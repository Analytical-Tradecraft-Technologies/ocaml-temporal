#!/bin/sh
set -eu

# The installed consumer is linked from a Rust static archive built by Dune.
# Keep the native build-tool checks in every source-of-truth manifest so a
# source install diagnoses missing Cargo/Rust or protoc prerequisites while
# resolving dependencies, before the native bridge rule begins compiling.
root=${1:-.}
required_dependencies='conf-rust-2024 conf-protoc'

# Capture native output synchronously in workspace-local files before CRLF
# normalization. A pipeline would hide a failing producer behind tr's success;
# local paths also work for native Windows tools invoked from Cygwin.
metadata_dir=$(mktemp -d "./temporal-sdk-build-metadata.XXXXXX")
cleanup() {
  rm -rf -- "$metadata_dir"
}
trap cleanup EXIT HUP INT TERM

fail() {
  echo "install consumer metadata: $*" >&2
  exit 1
}

manifest_dependencies() {
  OPAMCLI=2.0 opam show --file="$1" --field=depends --normalise >"$2" ||
    fail "could not parse $1"
}

manifest_dependencies "$root/temporal-sdk.opam" "$metadata_dir/opam"
manifest_dependencies "$root/temporal-sdk.opam.locked" "$metadata_dir/locked"
# An explicit language version bypasses workspace/scheduler initialization.
# Otherwise concurrent formatters contend over _build/.lock just to format a
# file, even inside the parent Dune test action. Keep this at dune-project's
# language version.
dune format-dune-file --dune-version 3.18 "$root/dune-project" >"$metadata_dir/dune" ||
  fail "could not format $root/dune-project"
# Native Windows tools emit CRLF even when their input is checked out with LF.
# Normalize command output before applying the line-oriented metadata checks.
opam_dependencies=$(tr -d '\r' <"$metadata_dir/opam")
locked_dependencies=$(tr -d '\r' <"$metadata_dir/locked")
dune_dependencies=$(tr -d '\r' <"$metadata_dir/dune")

require_lf_attribute() {
  pattern=$1
  if ! awk -v pattern="$pattern" '
    { sub(/\r$/, "") }
    $1 == pattern {
      for (field = 2; field <= NF; field++) {
        if ($field == "text") has_text = 1
        if ($field == "eol=lf") has_lf = 1
      }
    }
    END { exit !(has_text && has_lf) }
  ' "$root/.gitattributes"; then
    fail ".gitattributes does not force LF checkout for $pattern"
  fi
}

for lf_pattern in \
  '.gitattributes' 'Dockerfile*' dune-project '*.opam' '*.opam.locked' \
  '.github/workflows/*.yml' scripts/opam-lock-overrides.txt; do
  require_lf_attribute "$lf_pattern"
done

# The OCaml base image contains a point-in-time clone of opam-repository. New
# conf packages declared by this project are not guaranteed to be present in
# that clone, so refresh it in the same layer that resolves dependencies. The
# image installs protoc through apt and copies the pinned Rust toolchain from
# the official Rust image, so opam must validate the conf packages without
# attempting or solving their distribution-level depexts. Release artifacts
# must be compiled against the audited lock, so the layer installs and checks
# the exact locked closure rather than re-solving temporal-sdk.opam.
if ! awk '
  before_previous == "RUN opam repository set-url default https://opam.ocaml.org \\" &&
    previous == "    && opam update \\" &&
    $0 == "    && sh scripts/opam-locked-deps.sh install --no-depexts" { found = 1 }
  { before_previous = previous; previous = $0 }
  END { exit !found }
' "$root/Dockerfile.dev"; then
  fail "Dockerfile.dev does not install the locked closure from the current HTTPS opam repository"
fi

if ! grep -Fx 'COPY --chown=opam:opam temporal-sdk.opam temporal-sdk.opam.locked dune-project ./' \
    "$root/Dockerfile.dev" >/dev/null ||
  ! grep -Fx 'COPY --chown=opam:opam scripts/opam-locked-deps.sh scripts/opam-lock-overrides.txt ./scripts/' \
    "$root/Dockerfile.dev" >/dev/null; then
  fail "Dockerfile.dev does not copy the lock and its installer into the dependency layer"
fi

if ! grep -F 'protobuf-compiler \' "$root/Dockerfile.dev" >/dev/null ||
  ! grep -F 'COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo' \
    "$root/Dockerfile.dev" >/dev/null; then
  fail "Dockerfile.dev does not install the native tools required by its conf packages"
fi

# Every compiler series in the CI matrix must build from its own ocaml/opam
# stage pinned to an immutable manifest digest, and no other base reference
# may name ocaml/opam by tag alone.
compilers=$(tr -d '\r' <"$root/scripts/ci-matrix.py" |
  sed -n 's/^COMPILERS = (\(.*\))$/\1/p' | tr -d '",')
[ -n "$compilers" ] || fail "could not read compiler series from scripts/ci-matrix.py"
for compiler in $compilers; do
  series=${compiler%.*}
  if ! grep -E "^FROM ocaml/opam:debian-12-ocaml-$series@sha256:[0-9a-f]{64} AS ocaml-$series\$" \
      "$root/Dockerfile.dev" >/dev/null; then
    fail "Dockerfile.dev has no digest-pinned ocaml-$series stage"
  fi
done
if grep -E 'ocaml/opam:' "$root/Dockerfile.dev" | grep -Ev '@sha256:[0-9a-f]{64}' >/dev/null; then
  fail "Dockerfile.dev references an ocaml/opam image without a digest"
fi

for workflow in "$root/.github/workflows/build.yml" "$root/.github/workflows/build-pr.yml"; do
  if grep -E 'opam install .*--deps-only' "$workflow" >/dev/null; then
    fail "$(basename "$workflow") installs unlocked OCaml dependencies"
  fi
  if grep -F 'opam-locked-deps.sh install' "$workflow" |
    grep -v -- '--assume-depexts' >/dev/null; then
    fail "$(basename "$workflow") allows conf packages to replace the pinned native toolchain"
  fi
done
if [ "$(grep -c 'run: sh scripts/opam-locked-deps.sh install --assume-depexts$' \
    "$root/.github/workflows/build-pr.yml")" -ne 2 ]; then
  fail "build-pr.yml native macOS/Windows lanes do not install the locked closure"
fi

for required_dependency in $required_dependencies; do
  case "$opam_dependencies" in
    *\"$required_dependency\"*) ;;
    *) fail "temporal-sdk.opam does not declare $required_dependency" ;;
  esac

  if ! printf '%s\n' "$dune_dependencies" |
    grep -E "^[[:space:]]*$required_dependency$" >/dev/null; then
    fail "dune-project does not declare $required_dependency"
  fi
done

require_locked_pin() {
  dependency=$1
  version=$2
  exact_pin="\"$dependency\" {= \"$version\"}"
  case "$locked_dependencies" in
    *"$exact_pin"*) ;;
    *) fail "temporal-sdk.opam.locked does not pin $dependency to version $version" ;;
  esac
}

require_locked_pin conf-rust-2024 1
require_locked_pin conf-protoc 4.4.0

printf '%s\n' "install consumer metadata: ok"

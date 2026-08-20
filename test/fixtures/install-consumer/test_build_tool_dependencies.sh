#!/bin/sh
set -eu

# The installed consumer is linked from a Rust static archive built by Dune.
# Keep the native build-tool checks in every source-of-truth manifest so a
# source install diagnoses missing Cargo/Rust or protoc prerequisites while
# resolving dependencies, before the native bridge rule begins compiling.
root=${1:-.}
required_dependencies='conf-rust-2024 conf-protoc'

fail() {
  echo "install consumer metadata: $*" >&2
  exit 1
}

manifest_dependencies() {
  OPAMCLI=2.0 opam show --file="$1" --field=depends --normalise
}

opam_dependencies=$(manifest_dependencies "$root/temporal-sdk.opam")
locked_dependencies=$(manifest_dependencies "$root/temporal-sdk.opam.locked")
dune_dependencies=$(dune format-dune-file "$root/dune-project")

# The OCaml base image contains a point-in-time clone of opam-repository. New
# conf packages declared by this project are not guaranteed to be present in
# that clone, so replace it with the canonical Git HTTPS remote and refresh it
# in the same layer that resolves dependencies.
if ! grep -F 'RUN opam repository set-url default git+https://github.com/ocaml/opam-repository.git \' \
  "$root/Dockerfile.dev" >/dev/null ||
  ! grep -F 'if ! opam install --deps-only --with-test --assume-depexts -y . \' \
    "$root/Dockerfile.dev" >/dev/null; then
  fail "Dockerfile.dev does not use the current HTTPS opam repository before installing dependencies"
fi

for workflow in "$root/.github/workflows/build.yml" "$root/.github/workflows/build-pr.yml"; do
  if grep -E 'opam install .*--deps-only.*--with-test' "$workflow" |
    grep -v -- '--assume-depexts' >/dev/null; then
    fail "$(basename "$workflow") allows conf packages to replace the pinned native toolchain"
  fi
done

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

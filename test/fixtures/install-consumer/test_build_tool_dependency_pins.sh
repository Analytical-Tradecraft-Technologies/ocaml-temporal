#!/bin/sh
set -eu

root=${1:-.}
checker="$root/test/fixtures/install-consumer/test_build_tool_dependencies.sh"
# Keep the fixture beneath the current workspace. Native Windows executables
# invoked from Cygwin cannot resolve Cygwin's /tmp path, but can resolve this
# relative path from their shared working directory.
fixture_root=$(mktemp -d "./temporal-sdk-build-dependency-pins.XXXXXX")

cleanup() {
  case "$(basename "$fixture_root")" in
    temporal-sdk-build-dependency-pins.*) rm -rf -- "$fixture_root" ;;
    *) echo "refusing to remove unexpected fixture path: $fixture_root" >&2 ;;
  esac
}
trap cleanup EXIT HUP INT TERM

prepare_fixture() {
  rm -rf -- "$fixture_root/docker"
  mkdir -p "$fixture_root/.github/workflows" "$fixture_root/scripts"
  for pin in "$root"/docker/ocaml-*/Dockerfile; do
    series_dir=$(basename "$(dirname "$pin")")
    mkdir -p "$fixture_root/docker/$series_dir"
    sed -n 'p' "$pin" >"$fixture_root/docker/$series_dir/Dockerfile"
  done
  sed -n 'p' "$root/.github/dependabot.yml" >"$fixture_root/.github/dependabot.yml"
  sed -n 'p' "$root/scripts/ci-matrix.py" >"$fixture_root/scripts/ci-matrix.py"
  sed -n 'p' "$root/.gitattributes" >"$fixture_root/.gitattributes"
  sed -n 'p' "$root/Dockerfile.dev" >"$fixture_root/Dockerfile.dev"
  sed -n 'p' "$root/dune-project" >"$fixture_root/dune-project"
  sed -n 'p' "$root/temporal-sdk.opam" >"$fixture_root/temporal-sdk.opam"
  sed -n 'p' "$root/temporal-sdk.opam.locked" \
    >"$fixture_root/temporal-sdk.opam.locked"
  sed -n 'p' "$root/.github/workflows/build.yml" \
    >"$fixture_root/.github/workflows/build.yml"
  sed -n 'p' "$root/.github/workflows/build-pr.yml" \
    >"$fixture_root/.github/workflows/build-pr.yml"
}

replace_pin_with_unrelated_sentinel() {
  dependency=$1
  version=$2
  sentinel=$3
  next_lock="$fixture_root/temporal-sdk.opam.locked.next"

  awk -v dependency="$dependency" -v version="$version" -v sentinel="$sentinel" '
    $0 == "  \"" dependency "\" {= \"" version "\"}" {
      print "  \"" dependency "\""
      print "  \"" sentinel "\" {= \"" version "\"}"
      replaced = 1
      next
    }
    { print }
    END { if (!replaced) exit 2 }
  ' "$fixture_root/temporal-sdk.opam.locked" >"$next_lock"
  mv "$next_lock" "$fixture_root/temporal-sdk.opam.locked"
}

assert_rejects_detached_pin() {
  dependency=$1
  version=$2
  sentinel=$3
  log="$fixture_root/check.log"

  prepare_fixture
  replace_pin_with_unrelated_sentinel "$dependency" "$version" "$sentinel"
  if sh "$checker" "$fixture_root" >"$log" 2>&1; then
    echo "install consumer metadata mutation: detached $dependency pin unexpectedly passed" >&2
    exit 1
  fi
  if ! grep -F "does not pin $dependency to version $version" "$log" >/dev/null; then
    cat "$log" >&2
    echo "install consumer metadata mutation: $dependency failed for the wrong reason" >&2
    exit 1
  fi
}

assert_accepts_crlf_metadata() {
  crlf_attributes="$fixture_root/.gitattributes.crlf"
  tool_bin="$fixture_root/tool-bin"
  real_dune=$(command -v dune)

  prepare_fixture
  awk '{ printf "%s\r\n", $0 }' "$fixture_root/.gitattributes" \
    >"$crlf_attributes"
  mv "$crlf_attributes" "$fixture_root/.gitattributes"
  mkdir -p "$tool_bin"
  cat >"$tool_bin/dune" <<'EOF'
#!/bin/sh
set -eu
"$REAL_DUNE" "$@" >"$DUNE_OUTPUT"
awk '{ printf "%s\r\n", $0 }' "$DUNE_OUTPUT"
EOF
  chmod +x "$tool_bin/dune"
  if ! PATH="$tool_bin:$PATH" REAL_DUNE="$real_dune" DUNE_OUTPUT="$fixture_root/dune-output" \
    sh "$checker" "$fixture_root"; then
    echo "install consumer metadata mutation: CRLF metadata was rejected" >&2
    exit 1
  fi
}

# A native tool can emit complete-looking output before failing. The checker
# must propagate that failure instead of accepting the following tr's status.
assert_rejects_failed_tool() {
  tool=$1
  diagnostic=$2
  real_tool=$(command -v "$tool")
  tool_bin="$fixture_root/failed-$tool"
  log="$fixture_root/failed-$tool.log"

  prepare_fixture
  mkdir -p "$tool_bin"
  cat >"$tool_bin/$tool" <<'EOF'
#!/bin/sh
"$REAL_TOOL" "$@"
exit 23
EOF
  chmod +x "$tool_bin/$tool"
  if PATH="$tool_bin:$PATH" REAL_TOOL="$real_tool" \
    sh "$checker" "$fixture_root" >"$log" 2>&1; then
    echo "install consumer metadata mutation: failed $tool unexpectedly passed" >&2
    exit 1
  fi
  if ! grep -F "$diagnostic" "$log" >/dev/null; then
    cat "$log" >&2
    echo "install consumer metadata mutation: $tool failed for the wrong reason" >&2
    exit 1
  fi
}

# Loading a project extension is unnecessary for formatting its metadata. An
# unavailable extension makes accidental workspace initialization observable
# without relying on the timing of concurrent Windows formatters.
assert_ignores_build_workspace() {
  prepare_fixture
  printf '%s\n' '(using metadata-test-unavailable-extension 1.0)' \
    >>"$fixture_root/dune-project"
  if ! sh "$checker" "$fixture_root"; then
    echo "install consumer metadata mutation: formatter initialized the build workspace" >&2
    exit 1
  fi
}

# A per-series base-image pin must stay tracked without retargeting (#1027):
# each mutation below must fail with the named diagnostic.
assert_rejects_base_image_mutation() {
  description=$1
  diagnostic=$2
  log="$fixture_root/base-image.log"

  if sh "$checker" "$fixture_root" >"$log" 2>&1; then
    echo "install consumer metadata mutation: $description unexpectedly passed" >&2
    exit 1
  fi
  if ! grep -F "$diagnostic" "$log" >/dev/null; then
    cat "$log" >&2
    echo "install consumer metadata mutation: $description failed for the wrong reason" >&2
    exit 1
  fi
}

# The resolver maps series selectors to their pin but passes explicit image
# references through unchanged, even when the repository name looks like a
# series (for example a local `ocaml-5.2:custom` build).
assert_resolves() {
  selector=$1
  expected=$2
  actual=$(sh "$root/scripts/ocaml-image.sh" "$selector")
  if [ "$actual" != "$expected" ]; then
    echo "ocaml-image: '$selector' resolved to '$actual', expected '$expected'" >&2
    exit 1
  fi
}
pinned_5_2=$(awk '$1 == "FROM" { print $2 }' "$root/docker/ocaml-5.2/Dockerfile")
assert_resolves 5.2 "$pinned_5_2"
assert_resolves ocaml-5.2 "$pinned_5_2"
assert_resolves 5.2.1 "$pinned_5_2"
assert_resolves ocaml-5.2:custom ocaml-5.2:custom
assert_resolves ocaml-5.2@sha256:0123 ocaml-5.2@sha256:0123
assert_resolves registry.example/ocaml:5.2 registry.example/ocaml:5.2

prepare_fixture
sed 's/>= 5[.]4/>= 5.9/' "$root/.github/dependabot.yml" \
  >"$fixture_root/.github/dependabot.yml"
assert_rejects_base_image_mutation 'a retargetable 5.3 Dependabot entry' \
  "no docker entry for /docker/ocaml-5.3 ignoring ocaml/opam '>= 5.4'"

prepare_fixture
sed 's/@sha256:[0-9a-f]*//' "$root/docker/ocaml-5.4/Dockerfile" \
  >"$fixture_root/docker/ocaml-5.4/Dockerfile"
assert_rejects_base_image_mutation 'an undigested 5.4 base image' \
  'docker/ocaml-5.4/Dockerfile must have exactly one FROM line'

prepare_fixture
mkdir -p "$fixture_root/docker/ocaml-5.9"
sed 's/5[.]5/5.9/g' "$root/docker/ocaml-5.5/Dockerfile" \
  >"$fixture_root/docker/ocaml-5.9/Dockerfile"
assert_rejects_base_image_mutation 'a pinned series outside the CI matrix' \
  'differ from scripts/ci-matrix.py'

prepare_fixture
printf '%s\n' 'FROM ocaml/opam:debian-12-ocaml-5.2 AS stale' >>"$fixture_root/Dockerfile.dev"
assert_rejects_base_image_mutation 'a second ocaml/opam pin in Dockerfile.dev' \
  'Dockerfile.dev must take its ocaml/opam base image'

assert_accepts_crlf_metadata
assert_rejects_failed_tool opam 'could not parse'
assert_rejects_failed_tool dune 'could not format'
assert_ignores_build_workspace

assert_rejects_detached_pin \
  conf-protoc 4.4.0 zz-conf-protoc-pin-sentinel
assert_rejects_detached_pin \
  conf-rust-2024 1 zz-conf-rust-pin-sentinel

printf '%s\n' "install consumer metadata pin mutations: ok"

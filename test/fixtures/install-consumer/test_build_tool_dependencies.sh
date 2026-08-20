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
# that clone, so refresh it in the same layer that resolves dependencies.
if ! awk '
  previous == "RUN opam update \\" &&
    $0 == "    && opam install --deps-only --with-test -y ." { found = 1 }
  { previous = $0 }
  END { exit !found }
' "$root/Dockerfile.dev"; then
  fail "Dockerfile.dev does not refresh opam-repository before installing dependencies"
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

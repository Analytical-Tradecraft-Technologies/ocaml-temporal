#!/bin/sh
set -eu

# The installed consumer is linked from a Rust static archive built by Dune.
# Keep the package-manager toolchain check in every source-of-truth manifest so
# a source install diagnoses a missing Cargo/Rust installation while resolving
# dependencies, before the native bridge rule begins compiling.
root=${1:-.}
required_dependency=conf-rust-2024

fail() {
  echo "install consumer metadata: $*" >&2
  exit 1
}

manifest_dependencies() {
  OPAMCLI=2.0 opam show --file="$1" --field=depends --normalise
}

opam_dependencies=$(manifest_dependencies "$root/temporal-sdk.opam")
case "$opam_dependencies" in
  *\"$required_dependency\"*) ;;
  *) fail "temporal-sdk.opam does not declare $required_dependency" ;;
esac

locked_dependencies=$(manifest_dependencies "$root/temporal-sdk.opam.locked")
case "$locked_dependencies" in
  *\"$required_dependency\"*'{= "1"}'*) ;;
  *) fail "temporal-sdk.opam.locked does not pin $required_dependency to version 1" ;;
esac

if ! dune format-dune-file "$root/dune-project" |
  grep -E "^[[:space:]]*$required_dependency$" >/dev/null; then
  fail "dune-project does not declare $required_dependency"
fi

printf '%s\n' "install consumer metadata: ok"

#!/bin/sh
# Run from the relocated consumer tree. It contains only the SDK installation
# and application fixture: no SDK source, Cargo tree, or original Dune state.
set -eu
test -s sdk/lib/temporal-sdk/temporal.cmxa
if find sdk -name '*.ml' -print | grep -q .; then
  echo 'SDK implementation source leaked into the binary-only consumer' >&2
  exit 1
fi
test ! -d /workspace/lib
library_path=$(pwd)/sdk/lib
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) library_path=$(cygpath -w "$library_path") ;;
esac
export OCAMLPATH=$library_path DUNE_CACHE=disabled
opam exec -- dune build --root . ./main.exe ./worker_environment.exe
./_build/default/main.exe
./_build/default/worker_environment.exe
printf '%s\n' 'Precompiled SDK consumer passed; no SDK OCaml, C, or Rust compilation.'

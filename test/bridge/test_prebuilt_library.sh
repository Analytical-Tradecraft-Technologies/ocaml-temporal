#!/bin/sh
# Run from the relocated consumer tree. It contains only the SDK installation
# and application fixture: no SDK source, Cargo tree, or original Dune state.
set -eu

# Runs `opam exec -- dune SUBCOMMAND ARGS...` and adds an explicit `-j` when
# DUNE_JOBS is non-empty. Make exports the limit (#851), but Dune reads the
# DUNE_JOBS environment variable only from 3.22, and the SDK supports 3.18, so
# the flag keeps this nested build bounded on every supported release.
# test/smoke/test_build_jobs_contract.sh runs this function with a stub Dune.
opam_dune() {
  dune_subcommand=$1
  shift
  if [ -n "${DUNE_JOBS:-}" ]; then
    opam exec -- dune "$dune_subcommand" -j "$DUNE_JOBS" "$@"
  else
    opam exec -- dune "$dune_subcommand" "$@"
  fi
}

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
opam_dune build --root . ./main.exe ./main.bc ./worker_environment.exe
./_build/default/main.exe
# Dune (modes byte) consumer of the relocated package (#1020): the forced
# custom runtime must find the static archives through $CAMLORIGIN.
./_build/default/main.bc
./_build/default/worker_environment.exe
printf '%s\n' 'Precompiled SDK consumer passed; no SDK OCaml, C, or Rust compilation.'

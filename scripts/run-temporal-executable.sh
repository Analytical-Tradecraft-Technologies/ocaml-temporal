#!/bin/sh
# CI runs only previously verified binaries. Local invocation retains Dune's
# isolated build trees so a driver can compile while its worker remains alive.
set -eu
build_dir=$1
shift
case "$build_dir" in --build-dir=*) ;; *) echo 'missing build directory' >&2; exit 2 ;; esac
if [ "${TEMPORAL_PREBUILT_SMOKE:-0}" = 1 ]; then
  target=$1
  shift
  case "$target" in test/integration/*.exe) ;; *) echo 'invalid smoke executable' >&2; exit 2 ;; esac
  case "$target" in *..*) echo 'invalid smoke executable path' >&2; exit 2 ;; esac
  binary="_build/default/$target"
  test -x "$binary" || { echo "missing prebuilt smoke executable: $binary" >&2; exit 1; }
  exec "$binary" "$@"
fi
exec opam exec -- dune exec "$build_dir" "$@"

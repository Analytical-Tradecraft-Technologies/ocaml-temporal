#!/bin/sh
# An artifact consumer checks its inputs without invoking a compiler. Source
# builds use the same targets in one Dune invocation, preserving local behavior.
set -eu
if [ "${TEMPORAL_PREBUILT_SMOKE:-0}" = 1 ]; then
  for target in "$@"; do
    test -x "_build/default/$target" || { echo "missing prebuilt executable: $target" >&2; exit 1; }
  done
else
  # DUNE_JOBS is a numeric limit supplied by Make/the developer, not shell text.
  if [ -n "${DUNE_JOBS:-}" ]; then
    exec opam exec -- dune build -j "$DUNE_JOBS" "$@"
  fi
  exec opam exec -- dune build "$@"
fi

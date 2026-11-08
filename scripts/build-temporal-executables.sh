#!/bin/sh
# An artifact consumer checks its inputs without invoking a compiler. Source
# builds use the same targets in one Dune invocation, preserving local behavior.
set -eu
if [ "${TEMPORAL_PREBUILT_SMOKE:-0}" = 1 ]; then
  for target in "$@"; do
    test -x "_build/default/$target" || { echo "missing prebuilt executable: $target" >&2; exit 1; }
  done
else
  # Link the live-test executables with lld when the image provides it: each
  # statically links the Rust bridge, and lld links the 44 CI smoke programs
  # in about two thirds of GNU ld's time (65 s against 98 s on a standard
  # runner). OCAMLPARAM reaches only ocamlopt's final link command. The SDK's
  # own tests and the install consumer keep the default linker, because lld
  # resolves symbols across archives in any order and could hide a link-order
  # mistake in the flags the SDK ships (test_build_jobs_contract.sh checks
  # that nothing else selects it). A caller's own OCAMLPARAM wins.
  if [ -z "${OCAMLPARAM:-}" ] && command -v ld.lld >/dev/null 2>&1; then
    OCAMLPARAM='_,ccopt=-fuse-ld=lld'
    export OCAMLPARAM
  fi
  # DUNE_JOBS is a numeric limit supplied by Make/the developer, not shell text.
  if [ -n "${DUNE_JOBS:-}" ]; then
    exec opam exec -- dune build -j "$DUNE_JOBS" "$@"
  fi
  # Make supplies an empty value when no limit was requested. Dune also reads
  # this variable itself, and rejects an empty value instead of using auto.
  unset DUNE_JOBS
  exec opam exec -- dune build "$@"
fi

#!/bin/sh
set -eu

# Installs the package that scripts/check-opam-source-archive.sh built from a
# release source archive into a fresh prefix, then builds and runs the
# installed-package consumer fixture against that prefix only (#510).
#
# Usage: check-source-archive-consumer.sh WORK_DIRECTORY
#
# WORK_DIRECTORY is the directory previously passed to
# check-opam-source-archive.sh, which leaves the unpacked archive and its
# sandboxed `dune build -p temporal-sdk` output in WORK_DIRECTORY/source. This
# script reuses that build instead of compiling the Rust bridge a second time:
#
# 1. `dune install -p temporal-sdk --prefix WORK_DIRECTORY/prefix`, run from
#    the unpacked archive with the same `-p` flags as the build, copies the
#    files listed in temporal-sdk.install, as `opam install` does into a
#    switch.
# 2. The consumer fixture is copied from the archive's own
#    test/fixtures/install-consumer (not from any checkout) into
#    WORK_DIRECTORY/consumer, a separate Dune root, and built with only the
#    prefix prepended to OCAMLPATH. The executables are the ones
#    test/bridge/test_install.sh builds from a checkout: the public API witness
#    as native code and as a Dune (modes byte) program, the worker-environment
#    probe, and offline replay of a frozen history-corpus capture.
#
# A switch that already contains temporal-sdk is refused: the consumer could
# then resolve the switch's copy instead of the archive's, and the check would
# prove nothing about the release. The script needs the same OCaml switch and
# locked opam dependencies as the build; it needs no Rust, network, or Docker,
# and writes only below WORK_DIRECTORY and the Dune cache.
package=temporal-sdk

if [ "$#" -ne 1 ]; then
  echo "usage: check-source-archive-consumer.sh WORK_DIRECTORY" >&2
  exit 2
fi
if [ ! -d "$1" ]; then
  echo "work directory not found: $1" >&2
  exit 1
fi
work_root=$(cd "$1" && pwd -P)

# check-opam-source-archive.sh unpacks the archive's single top-level
# directory, ocaml-temporal-<version>, below WORK_DIRECTORY/source.
set -- "$work_root"/source/*
if [ "$#" -ne 1 ] || [ ! -d "$1" ]; then
  echo "no unpacked source archive below $work_root/source; run check-opam-source-archive.sh first" >&2
  exit 1
fi
package_root=$1
if [ ! -f "$package_root/_build/default/$package.install" ]; then
  echo "$package_root has no sandboxed build; run check-opam-source-archive.sh first" >&2
  exit 1
fi

prefix=$work_root/prefix
consumer=$work_root/consumer
for fresh in "$prefix" "$consumer"; do
  if [ -e "$fresh" ]; then
    echo "work directory already contains $fresh" >&2
    exit 1
  fi
done

switch_prefix=${OPAM_SWITCH_PREFIX:-}
if [ -z "$switch_prefix" ] && command -v opam >/dev/null 2>&1; then
  switch_prefix=$(opam var prefix 2>/dev/null || true)
fi
if [ -n "$switch_prefix" ] && [ -e "$switch_prefix/lib/$package" ]; then
  echo "the opam switch already provides $package ($switch_prefix/lib/$package);" >&2
  echo "use a switch without it so the consumer can only resolve the archive's build" >&2
  exit 1
fi

jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)

# -p implies --root . and the release profile, matching the sandboxed build,
# so Dune reuses its output rather than rebuilding anything.
(
  cd "$package_root"
  dune install -p "$package" --prefix "$prefix"
)
installed=$prefix/lib/$package
if [ ! -f "$installed/META" ] ||
  [ ! -s "$installed/__private__/temporal_core_bridge/libocaml_temporal_core_bridge.a" ]; then
  echo "dune install did not place $package with its Rust bridge below $prefix/lib" >&2
  exit 1
fi

fixture=$package_root/test/fixtures/install-consumer
history=$package_root/test/fixtures/history-corpus/histories/live-2026-10-08-timer.pb
mkdir "$consumer"
for file in dune-project dune main.ml public_api.ml worker_environment.ml installed_replay.ml; do
  if [ ! -f "$fixture/$file" ]; then
    echo "source archive is missing test/fixtures/install-consumer/$file" >&2
    exit 1
  fi
  cp "$fixture/$file" "$consumer/"
done
if [ ! -f "$history" ]; then
  echo "source archive is missing the replay history ${history#"$package_root"/}" >&2
  exit 1
fi
cp "$history" "$consumer/timer-history.pb"

# The subshell confines the prefix's OCAMLPATH to the consumer build. The
# remaining entries, if any, are the caller's (for example the switch's
# dependency libraries); the guard above keeps temporal-sdk out of them.
(
  OCAMLPATH=$prefix/lib${OCAMLPATH:+:$OCAMLPATH}
  export OCAMLPATH
  dune build --root "$consumer" -j "$jobs" \
    ./main.exe ./main.bc ./worker_environment.exe ./installed_replay.exe
)
"$consumer/_build/default/main.exe"
"$consumer/_build/default/main.bc"
"$consumer/_build/default/worker_environment.exe"
# Replay the recorded history through the installed package: it must pass, and
# the fixture also checks that the same workflow without its timer is
# nondeterministic (#497).
(
  cd "$consumer"
  ./_build/default/installed_replay.exe timer-history.pb
)
echo "installed consumer of $(basename "$package_root") built and ran from $prefix"

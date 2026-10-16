#!/bin/sh
set -eu

# Builds a release source archive the way opam does, inside opam's
# network-denying build sandbox, and fails unless the package builds (#778).
#
# Usage: check-opam-source-archive.sh ARCHIVE [WORK_DIRECTORY]
#
# ARCHIVE is the output of scripts/create-source-archive.sh. The script
# unpacks it into a fresh directory below WORK_DIRECTORY (default: a new
# temporary directory, removed on exit), then runs the package's opam build
# command, `dune build -p temporal-sdk -j <jobs>`, from the unpacked root
# through the sandbox wrapper. Success proves that every Rust crate, including
# the pinned Temporal Core Git revision, comes from the archive's vendored
# sources: the wrapper denies all network access, so any Cargo fetch fails.
#
# Sandbox selection, in order:
# - opam's own build wrapper, `<opam root>/opam-init/hooks/sandbox.sh build`,
#   when opam is installed with sandboxing enabled (the default `opam init`
#   configuration on Linux and macOS). This is exactly what `opam install`
#   wraps each build command with.
# - On macOS without that hook (for example a CI switch initialised with
#   --disable-sandboxing), `sandbox-exec` with the same build policy as opam
#   2.5's macOS wrapper: network denied except Unix sockets, and writes denied
#   except below the build directory, the temporary directories, the Dune
#   cache, and /dev/null.
# Any other platform without the opam hook is reported as unsupported rather
# than silently building with network access.
#
# Before building, the script runs a network probe through the same wrapper
# and fails if it reaches crates.io, so a wrapper that does not actually deny
# the network can never produce a passing result. Cargo runs with a fresh,
# empty CARGO_HOME inside the writable build directory, as for a user who has
# never run Cargo: a default home warmed by `cargo vendor` (which created the
# archive) could otherwise satisfy an offline build for a source the archive's
# replacement omits. The variables that select a prebuilt bridge, a shared
# target directory, or offline mode are cleared, so the archive itself must
# select the vendored build.
#
# The script needs OCaml, the locked opam dependencies, Rust and protoc on
# PATH, as `opam install` would; run it under `opam exec --` so the switch's
# dune is found, because opam's wrapper resolves the build command on PATH. It writes only below WORK_DIRECTORY and the
# Dune cache.
archive=$1
work_root=${2:-}
package=temporal-sdk

case "$archive" in
  /*) ;;
  *) archive=$(pwd)/$archive ;;
esac
if [ ! -f "$archive" ]; then
  echo "source archive not found: $archive" >&2
  exit 1
fi

if [ -z "$work_root" ]; then
  work_root=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-opam-check.XXXXXX")
  # The unpacked tree and its release Rust build take several gigabytes;
  # remove them on every exit path when this script chose the location.
  trap 'rm -rf "$work_root"' EXIT HUP INT TERM
else
  mkdir -p "$work_root"
fi
work_root=$(cd "$work_root" && pwd -P)

source_root=$work_root/source
if [ -e "$source_root" ]; then
  echo "work directory already contains a source tree: $source_root" >&2
  exit 1
fi
mkdir "$source_root"
gzip -dc "$archive" | tar -xf - -C "$source_root"

# The archive has exactly one top-level directory, ocaml-temporal-<version>.
set -- "$source_root"/*
if [ "$#" -ne 1 ] || [ ! -d "$1" ]; then
  echo "source archive must contain exactly one top-level directory" >&2
  exit 1
fi
package_root=$1
for required in "$package.opam" rust/Cargo.lock rust/vendor.tar rust/vendor-config.toml; do
  if [ ! -f "$package_root/$required" ]; then
    echo "source archive is missing $required" >&2
    exit 1
  fi
done

# Run the package's own opam build command rather than a copy that could
# drift: the opam file must still declare exactly this release build line.
if ! grep -Fq '["dune" "build" "-p" name "-j" jobs]' "$package_root/$package.opam"; then
  echo "$package.opam no longer builds with dune build -p name -j jobs; update this check" >&2
  exit 1
fi
jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)

# opam sets OPAM_SWITCH_PREFIX for every build command and its wrapper makes
# that switch read-only, so a build that tried to write into the switch fails
# here as it would under `opam install`. Outside an opam environment the
# prefix stays empty and both wrappers skip that rule.
opam_hook=
if command -v opam >/dev/null 2>&1; then
  opam_root=$(opam var root 2>/dev/null || true)
  if [ -n "$opam_root" ] && [ -f "$opam_root/opam-init/hooks/sandbox.sh" ]; then
    opam_hook=$opam_root/opam-init/hooks/sandbox.sh
  fi
  if [ -z "${OPAM_SWITCH_PREFIX:-}" ]; then
    OPAM_SWITCH_PREFIX=$(opam var prefix 2>/dev/null || true)
  fi
fi

# Writes the macOS fallback policy for the build rooted at $1 to stdout. The
# rules mirror opam 2.5's hooks/sandbox.sh "build" command; later rules take
# precedence, so the read-only switch rule comes before the writable ones.
macos_build_policy() {
  policy='(version 1)(allow default)(deny network*)(deny file-write*)'
  policy="$policy"'(allow network* (remote unix))'
  policy="$policy"'(allow file-write* (literal "/dev/null") (literal "/dev/dtracehelper"))'
  policy="$policy"'(allow file-write* (regex #"^(/private)?(/var)?/tmp/"))'
  if [ -n "${OPAM_SWITCH_PREFIX:-}" ] && [ -d "$OPAM_SWITCH_PREFIX" ]; then
    policy="$policy"'(deny file-write* (subpath "'"$(cd "$OPAM_SWITCH_PREFIX" && pwd -P)"'"))'
  fi
  for writable in "${TMPDIR:-}" "$1" "${XDG_CACHE_HOME:-$HOME/.cache}/dune"; do
    if [ -n "$writable" ] && [ -d "$writable" ]; then
      policy="$policy"'(allow file-write* (subpath "'"$(cd "$writable" && pwd -P)"'"))'
    fi
  done
  printf '%s' "$policy"
}

# Runs "$@" from the current directory inside the selected build sandbox.
sandboxed() {
  if [ -n "$opam_hook" ]; then
    OPAM_SWITCH_PREFIX=${OPAM_SWITCH_PREFIX:-} "$opam_hook" build "$@"
  else
    sandbox-exec -p "$(macos_build_policy "$(pwd -P)")" "$@"
  fi
}

if [ -n "$opam_hook" ]; then
  echo "sandbox: opam build wrapper $opam_hook"
elif [ "$(uname -s)" = Darwin ] && command -v sandbox-exec >/dev/null 2>&1; then
  mkdir -p "${XDG_CACHE_HOME:-$HOME/.cache}/dune"
  echo "sandbox: sandbox-exec with opam's macOS build policy"
else
  echo "no opam build sandbox is available on this platform; enable opam sandboxing" >&2
  exit 1
fi

# Both wrappers look the build command up on PATH; fail here with the remedy
# rather than with the wrapper's bare "Command not found".
if ! command -v dune >/dev/null 2>&1; then
  echo "dune is not on PATH; run this script under opam exec --" >&2
  exit 1
fi

cd "$package_root" || exit 1

# The probe must fail inside the sandbox. A probe that reaches the registry
# means the wrapper is not isolating the network and the build proves nothing.
if command -v curl >/dev/null 2>&1; then
  if sandboxed curl --silent --show-error --max-time 20 --output /dev/null \
    https://index.crates.io/config.json 2>/dev/null; then
    echo "the build sandbox allowed a network request; refusing to trust it" >&2
    exit 1
  fi
  echo "sandbox: network probe denied as expected"
else
  echo "curl is unavailable; cannot prove the sandbox denies the network" >&2
  exit 1
fi

# Clear every input that would bypass or pre-empt the archive's own selection.
unset TEMPORAL_RUST_BRIDGE_DIR TEMPORAL_RUST_BRIDGE_KEY CARGO_TARGET_DIR CARGO_NET_OFFLINE
# A cold, writable Cargo home below the build directory (which every sandbox
# leaves writable). Dune ignores directories whose names start with "_", so
# it never becomes part of the package build. rustup locates toolchains
# through RUSTUP_HOME, which is unaffected.
CARGO_HOME=$(pwd -P)/_opam-check-cargo-home
if [ -e "$CARGO_HOME" ]; then
  echo "source archive unexpectedly contains $CARGO_HOME" >&2
  exit 1
fi
mkdir "$CARGO_HOME"
export CARGO_HOME
sandboxed dune build -p "$package" -j "$jobs"

install_file=_build/default/$package.install
if [ ! -f "$install_file" ] || ! grep -q 'libocaml_temporal_core_bridge.a' "$install_file"; then
  echo "the sandboxed build did not produce $install_file with the Rust bridge" >&2
  exit 1
fi
echo "sandboxed opam build of $(basename "$package_root") succeeded"

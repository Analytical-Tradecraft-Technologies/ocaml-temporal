#!/bin/sh
set -eu

target_dir=${CARGO_TARGET_DIR:-_build/rust}
# Test the archive of the same Cargo profile that scripts/build-rust-bridge.sh
# selects (#779): Dune's release profile links target/release, every other
# profile target/debug. The default matches Dune's default dev build.
case "${OCAML_TEMPORAL_BUILD_PROFILE:-dev}" in
  release) cargo_profile=release; profile_dir=release ;;
  *) cargo_profile=dev; profile_dir=debug ;;
esac
archive="$target_dir/$profile_dir/libocaml_temporal_core_bridge.a"
output_dir=_build/test/bridge
binary="$output_dir/abi_harness"
native_link_output=$(mktemp)

cleanup() {
  rm -f "$native_link_output"
}

trap cleanup EXIT HUP INT TERM

if [ -n "${TEMPORAL_RUST_BRIDGE_DIR:-}" ]; then
  sh scripts/rust-bridge-artifact.sh validate . "$TEMPORAL_RUST_BRIDGE_DIR" \
    "${TEMPORAL_RUST_BRIDGE_KEY:?artifact key is required}"
  archive="$TEMPORAL_RUST_BRIDGE_DIR/libocaml_temporal_core_bridge.a"
  native_link_flags=$(cat "$TEMPORAL_RUST_BRIDGE_DIR/native-static-libs")
else
# Ask the pinned Rust toolchain for the platform libraries pulled in by the
# static archive. Core's networking stack needs Apple frameworks on macOS and
# a different system-library set on Linux, so duplicating the list here would
# drift as the locked Cargo graph evolves.
CARGO_TERM_COLOR=never cargo rustc \
  --manifest-path rust/Cargo.toml \
  --package ocaml-temporal-core-bridge \
  --profile "$cargo_profile" \
  --locked \
  --lib \
  --crate-type staticlib \
  -- \
  --print=native-static-libs \
  2>"$native_link_output"

native_link_flags=$(sed -n 's/^note: native-static-libs: //p' "$native_link_output" | tail -n 1)
fi
if [ -z "$native_link_flags" ]; then
  cat "$native_link_output" >&2
  echo "rustc did not report native static-library link flags" >&2
  exit 1
fi

if [ ! -s "$archive" ]; then
  echo "missing Rust bridge archive: $archive" >&2
  exit 1
fi

mkdir -p "$output_dir"

# GCC ships no sanitizer runtimes for musl targets: Alpine has neither libasan
# nor libubsan, so the default flags cannot link there (#1165). On musl the
# harness keeps UndefinedBehaviorSanitizer in trap mode, which needs no
# runtime and aborts on the first finding, and drops only AddressSanitizer;
# the best-effort musl lane therefore checks the ABI and undefined behaviour
# but not memory errors, which the glibc and macOS lanes still cover.
case "$(cc -dumpmachine)" in
  *-musl*) sanitizer_flags='-fsanitize=undefined -fsanitize-trap=undefined' ;;
  *) sanitizer_flags='-fsanitize=address,undefined' ;;
esac

# rustc emits an ordered, whitespace-separated list of linker arguments, and
# sanitizer_flags is a fixed list of separate options.
# shellcheck disable=SC2086
cc \
  -std=c11 \
  -Wall \
  -Wextra \
  -Werror \
  -pedantic \
  $sanitizer_flags \
  -fno-omit-frame-pointer \
  -Irust/core-bridge/include \
  test/bridge/abi_harness.c \
  "$archive" \
  $native_link_flags \
  -o "$binary"

case "$(uname -s)" in
  Darwin) ASAN_OPTIONS=detect_leaks=0 "$binary" ;;
  *) ASAN_OPTIONS=detect_leaks=1 "$binary" ;;
esac

#!/bin/sh
set -eu

target_dir=${CARGO_TARGET_DIR:-_build/rust}
archive="$target_dir/debug/libocaml_temporal_core_bridge.a"
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

# rustc emits an ordered, whitespace-separated list of linker arguments.
# shellcheck disable=SC2086
cc \
  -std=c11 \
  -Wall \
  -Wextra \
  -Werror \
  -pedantic \
  -fsanitize=address,undefined \
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

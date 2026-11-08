#!/bin/sh
set -eu

# This native smoke gate proves both halves of the pinned-toolchain contract:
# the compiler version is exact, and that compiler can build the locked Rust
# bridge into the archive consumed by the OCaml build. It is a compatibility
# check, not a replacement for the bridge tests or the broader verification.
#
# rust/rust-toolchain.toml is the single Rust version pin: the gates, images
# and IDEs all select it, so this checks that the compiler actually running is
# the one the file names (#782). The script rejects a non-exact channel.
expected_rust_version=$(sh scripts/rust-version.sh)

actual_rust_version=$(rustc --version | awk '{ print $2 }')
if [ "$actual_rust_version" != "$expected_rust_version" ]; then
  echo "expected rustc $expected_rust_version, got $actual_rust_version" >&2
  exit 1
fi

cargo --version >/dev/null
cargo clippy --version >/dev/null
cargo fmt --version >/dev/null
cargo build --manifest-path rust/Cargo.toml --locked

archive=_build/rust/debug/libocaml_temporal_core_bridge.a
if [ ! -s "$archive" ]; then
  echo "expected non-empty Rust static library at $archive" >&2
  exit 1
fi

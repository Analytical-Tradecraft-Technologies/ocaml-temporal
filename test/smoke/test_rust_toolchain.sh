#!/bin/sh
set -eu

# This native smoke gate proves both halves of the pinned-toolchain contract:
# the compiler version is exact, and that compiler can build the locked Rust
# bridge into the archive consumed by the OCaml build. It is a compatibility
# check, not a replacement for the bridge tests or the broader verification.
expected_rust_version=1.99.0

# rust/rust-toolchain.toml is what contributors and IDEs running cargo inside
# rust/ use. Supported gates select the toolchain explicitly, so without this
# check the file could advertise an untested compiler (#782).
toolchain_file=rust/rust-toolchain.toml
declared_rust_version=$(sed -n 's/^channel[[:space:]]*=[[:space:]]*"\([^"]*\)"[[:space:]]*$/\1/p' "$toolchain_file")
if [ "$declared_rust_version" != "$expected_rust_version" ]; then
  echo "$toolchain_file declares $declared_rust_version, expected $expected_rust_version" >&2
  exit 1
fi

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

#!/bin/sh
# Prints the exact Rust compiler version that every gate uses.
#
# Usage: rust-version.sh [TOOLCHAIN_FILE]
#
# rust/rust-toolchain.toml is the single source of that version. The Makefile
# (NATIVE_RUST_VERSION), the native and producer workflows (RUSTUP_TOOLCHAIN),
# both Linux Rust images (Dockerfile.rust-ci and Dockerfile.dev's toolchain
# stage) and test/smoke/test_rust_toolchain.sh all read it through this
# script, so Dependabot's rust-toolchain update, which edits only that file,
# moves every gate at once. The `rust:` base image then only supplies rustup
# and Debian; its tag no longer has to match. Docker builds copy the file
# elsewhere and pass its path as TOOLCHAIN_FILE.
#
# The channel must be an exact release such as "1.99.1": a moving channel
# ("stable", "1.99") would let two gates test different compilers.
set -eu

file=${1:-$(CDPATH='' cd -- "$(dirname "$0")/.." && pwd)/rust/rust-toolchain.toml}
[ -f "$file" ] || {
  echo "missing Rust toolchain file $file" >&2
  exit 1
}
# Windows checkouts may use CRLF; the channel line is `channel = "X.Y.Z"`.
version=$(tr -d '\r' <"$file" |
  sed -n 's/^channel[[:space:]]*=[[:space:]]*"\([^"]*\)"[[:space:]]*$/\1/p')
if ! printf '%s\n' "$version" | grep -Eqx '[0-9]+\.[0-9]+\.[0-9]+'; then
  echo "$file must pin one exact channel such as \"1.99.1\", found '$version'" >&2
  exit 1
fi
printf '%s\n' "$version"

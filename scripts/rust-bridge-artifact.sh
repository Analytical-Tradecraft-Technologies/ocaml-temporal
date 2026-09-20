#!/bin/sh
set -eu

# CI bundles contain finished libraries, never Cargo's target tree. The key is
# supplied by the workflow and covers the Rust inputs and build environment.
# Consumers must receive that same key independently of the downloaded bundle;
# a missing, wrong-platform, or damaged bundle is an error, never a rebuild.
command=$1
workspace_root=$2

platform() {
  case "$(uname -s):$(uname -m)" in
    Linux:x86_64) printf 'linux-amd64\n' ;;
    Linux:aarch64) printf 'linux-arm64\n' ;;
    Darwin:arm64) printf 'macos-arm64\n' ;;
    MINGW*:x86_64 | MSYS*:x86_64 | CYGWIN*:x86_64) printf 'windows-amd64\n' ;;
    *) echo 'unsupported Rust bridge artifact platform' >&2; exit 1 ;;
  esac
}

# macOS supplies shasum; Linux and the Windows Cygwin environment supply
# sha256sum. Both use the same portable checksum-file format.
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$@"
  else
    shasum -a 256 "$@"
  fi
}

if [ "$command" = platform ]; then
  platform
  exit 0
fi

bundle=$3
expected_key=$4
test -n "$expected_key" || { echo 'Rust bridge artifact key is required' >&2; exit 1; }
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) bundle=$(cygpath -u "$bundle") ;;
esac

case "$command" in
  pack)
    test -z "${TEMPORAL_RUST_BRIDGE_DIR:-}" || {
      echo 'cannot build an artifact while consuming a prebuilt bridge' >&2; exit 1;
    }
    # Refuse to mix old and new outputs in one immutable bundle.
    test ! -e "$bundle" || { echo "artifact directory already exists: $bundle" >&2; exit 1; }
    mkdir -p "$bundle"
    sh "$workspace_root/scripts/build-rust-bridge.sh" "$workspace_root" \
      "$bundle/libocaml_temporal_core_bridge.a" "$bundle/bridge.dynamic" \
      "$bundle/build-link-flags.sexp" "$bundle"
    rm "$bundle/build-link-flags.sexp"
    printf '%s\n' "$expected_key" >"$bundle/key"
    platform >"$bundle/platform"
    (
      cd "$bundle"
      sha256 libocaml_temporal_core_bridge.a bridge.dynamic native-static-libs key platform >SHA256SUMS
      if [ -d import-libs ]; then
        sha256 import-libs/*.a >>SHA256SUMS
      fi
    )
    ;;
  validate | use)
    test "$(cat "$bundle/key")" = "$expected_key" || {
      echo 'Rust bridge artifact key mismatch' >&2; exit 1;
    }
    test "$(cat "$bundle/platform")" = "$(platform)" || {
      echo 'Rust bridge artifact platform mismatch' >&2; exit 1;
    }
    test -s "$bundle/libocaml_temporal_core_bridge.a"
    test -s "$bundle/bridge.dynamic"
    test -s "$bundle/native-static-libs"
    (cd "$bundle" && sha256 -c SHA256SUMS) >&2
    if [ "$command" = use ]; then
      cp "$bundle/libocaml_temporal_core_bridge.a" "$5"
      cp "$bundle/bridge.dynamic" "$6"
      # Windows import libraries must use the consumer's path. No absolute
      # Cargo-registry or producer-workspace path is embedded in the bundle.
      sh "$workspace_root/scripts/render-rust-link-flags.sh" \
        "$(uname -s)" "$bundle" "$7" "$(cat "$bundle/native-static-libs")"
      echo "Using prebuilt Rust bridge: $expected_key" >&2
    fi
    ;;
  *) echo "unknown Rust bridge artifact command: $command" >&2; exit 2 ;;
esac

#!/bin/sh
set -eu

# Exercise producer/consumer relocation with a tiny fake Cargo output, then
# make Cargo fail unconditionally. A consumer must use only the bundle, and a
# broken bundle must fail instead of invoking the compiler as a fallback.
workspace_root=$(cd "$1" && pwd)
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM
unset TEMPORAL_RUST_BRIDGE_DIR TEMPORAL_RUST_BRIDGE_KEY
mkdir -p "$temporary_root/bin" "$temporary_root/target/debug"
cat >"$temporary_root/bin/cargo" <<'EOF'
#!/bin/sh
set -eu
if [ "$1" = rustc ]; then
  case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*) echo 'note: native-static-libs: -lwinapi_ntdll -lbcrypt' >&2 ;;
    *) echo 'note: native-static-libs: -lpthread -ldl' >&2 ;;
  esac
fi
EOF
chmod +x "$temporary_root/bin/cargo"
export PATH="$temporary_root/bin:$PATH"
export CARGO_TARGET_DIR="$temporary_root/target"
printf 'archive fixture\n' >"$temporary_root/target/debug/libocaml_temporal_core_bridge.a"
case "$(uname -s)" in
  Darwin) dynamic=libocaml_temporal_core_bridge.dylib ;;
  MINGW* | MSYS* | CYGWIN*)
    dynamic=ocaml_temporal_core_bridge.dll
    mkdir -p "$temporary_root/registry" "$temporary_root/target/debug/build/winapi-x86_64-pc-windows-gnu-fixture"
    printf 'import fixture\n' >"$temporary_root/registry/libwinapi_ntdll.a"
    printf 'cargo:rustc-link-search=native=%s\n' "$temporary_root/registry" \
      >"$temporary_root/target/debug/build/winapi-x86_64-pc-windows-gnu-fixture/output"
    ;;
  *) dynamic=libocaml_temporal_core_bridge.so ;;
esac
printf 'shared fixture\n' >"$temporary_root/target/debug/$dynamic"
sh "$workspace_root/scripts/rust-bridge-artifact.sh" pack "$workspace_root" \
  "$temporary_root/producer" fixture-key
mv "$temporary_root/producer" "$temporary_root/consumer with spaces"
rm -rf "$temporary_root/target" "$temporary_root/registry"
printf '#!/bin/sh\necho unexpected Cargo invocation >&2\nexit 99\n' >"$temporary_root/bin/cargo"
export TEMPORAL_RUST_BRIDGE_DIR="$temporary_root/consumer with spaces"
export TEMPORAL_RUST_BRIDGE_KEY=fixture-key
consume() {
  sh "$workspace_root/scripts/build-rust-bridge.sh" "$workspace_root" \
    "$temporary_root/static.a" "$temporary_root/shared" "$temporary_root/flags.sexp"
}
consume
cmp "$temporary_root/static.a" "$TEMPORAL_RUST_BRIDGE_DIR/libocaml_temporal_core_bridge.a"
cmp "$temporary_root/shared" "$TEMPORAL_RUST_BRIDGE_DIR/bridge.dynamic"
if (TEMPORAL_RUST_BRIDGE_KEY=wrong-key consume) >"$temporary_root/error" 2>&1; then
  echo 'accepted mismatched Rust artifact key' >&2; exit 1
fi
grep -q 'key mismatch' "$temporary_root/error"
saved_platform=$(cat "$TEMPORAL_RUST_BRIDGE_DIR/platform")
printf 'wrong-platform\n' >"$TEMPORAL_RUST_BRIDGE_DIR/platform"
if consume >"$temporary_root/error" 2>&1; then
  echo 'accepted wrong Rust artifact platform' >&2; exit 1
fi
grep -q 'platform mismatch' "$temporary_root/error"
printf '%s\n' "$saved_platform" >"$TEMPORAL_RUST_BRIDGE_DIR/platform"
printf 'corrupted\n' >>"$TEMPORAL_RUST_BRIDGE_DIR/libocaml_temporal_core_bridge.a"
if consume >"$temporary_root/error" 2>&1; then
  echo 'accepted damaged Rust artifact' >&2; exit 1
fi
grep -q 'FAILED' "$temporary_root/error"
rm "$TEMPORAL_RUST_BRIDGE_DIR/bridge.dynamic"
if consume >"$temporary_root/error" 2>&1; then
  echo 'accepted incomplete Rust artifact' >&2; exit 1
fi
test ! -s "$temporary_root/error" || ! grep -q 'unexpected Cargo invocation' "$temporary_root/error"

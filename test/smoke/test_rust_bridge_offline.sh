#!/bin/sh
set -eu

# Verifies that scripts/build-rust-bridge.sh builds a release source archive
# without network access (#778). opam's build sandbox denies the network, so
# when the archive's rust/vendor.tar and rust/vendor-config.toml are present
# every Cargo invocation must be --frozen, use the shipped source replacement
# against freshly unpacked crates, and set CARGO_NET_OFFLINE. A development
# checkout, which has neither file, must keep the normal --locked build that
# may fetch. A half-present vendor set is a broken archive and must fail before
# Cargo runs. Stand-in rustc and cargo executables record what they were given,
# so no Rust toolchain or network is needed.
source_root=$(cd "$1" && pwd)
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM
unset TEMPORAL_RUST_BRIDGE_DIR TEMPORAL_RUST_BRIDGE_KEY CARGO_NET_OFFLINE

# A minimal SDK tree: the bridge scripts and the workspace manifest are all the
# build script reads before and after Cargo.
workspace=$temporary_root/workspace
mkdir -p "$workspace/scripts" "$workspace/rust"
for script in build-rust-bridge.sh copy-rust-bridge-artifacts.sh render-rust-link-flags.sh; do
  cp "$source_root/scripts/$script" "$workspace/scripts/$script"
done
chmod +x "$workspace/scripts/copy-rust-bridge-artifacts.sh"
cp "$source_root/rust/Cargo.toml" "$workspace/rust/Cargo.toml"

required=$(sed -n 's/^rust-version[[:space:]]*=[[:space:]]*"\([0-9][0-9.]*\)"[[:space:]]*$/\1/p' \
  "$workspace/rust/Cargo.toml" | head -n 1)
mkdir -p "$temporary_root/bin"
# A rustc reporting exactly the workspace minimum passes the version gate.
printf '#!/bin/sh\necho "rustc %s.0 (0000000 2025-01-01)"\n' "$required" \
  >"$temporary_root/bin/rustc"
# Records each invocation's arguments and CARGO_NET_OFFLINE, one line each.
# When given --config it also records whether the file is the shipped
# replacement and whether the crates it names were unpacked beside it, as Cargo
# resolves the relative directory against the parent of the .cargo directory.
cat >"$temporary_root/bin/cargo" <<'EOF'
#!/bin/sh
set -eu
printf 'args: %s\noffline: %s\n' "$*" "${CARGO_NET_OFFLINE:-unset}" >>"$CARGO_LOG"
config=
previous=
for argument in "$@"; do
  [ "$previous" = --config ] && config=$argument
  previous=$argument
done
if [ -n "$config" ]; then
  cmp "$config" "$EXPECTED_VENDOR_CONFIG"
  vendor_dir=$(dirname "$(dirname "$config")")/vendor
  test -f "$vendor_dir/fixture-crate-1.0.0/Cargo.toml"
  test -f "$vendor_dir/fixture-crate-1.0.0/.github/kept"
  test ! -e "$vendor_dir/stale-crate-0.1.0"
  echo "config: ok" >>"$CARGO_LOG"
fi
if [ "$1" = rustc ]; then
  case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*) echo 'note: native-static-libs: -lwinapi_ntdll -lbcrypt' >&2 ;;
    *) echo 'note: native-static-libs: -lpthread -ldl' >&2 ;;
  esac
fi
EOF
chmod +x "$temporary_root/bin/rustc" "$temporary_root/bin/cargo"
export PATH="$temporary_root/bin:$PATH"
export RUSTC="$temporary_root/bin/rustc"
export CARGO_TARGET_DIR="$temporary_root/target"
export CARGO_LOG="$temporary_root/cargo.log"
export EXPECTED_VENDOR_CONFIG="$workspace/rust/vendor-config.toml"
export OCAML_TEMPORAL_BUILD_PROFILE=dev

case "$(uname -s)" in
  Darwin) dynamic=libocaml_temporal_core_bridge.dylib ;;
  MINGW* | MSYS* | CYGWIN*) dynamic=ocaml_temporal_core_bridge.dll ;;
  *) dynamic=libocaml_temporal_core_bridge.so ;;
esac
mkdir -p "$CARGO_TARGET_DIR/debug"
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*)
    # Windows link flags need Cargo's winapi build-script metadata.
    metadata_dir=$CARGO_TARGET_DIR/debug/build/winapi-x86_64-pc-windows-gnu-fixture
    mkdir -p "$metadata_dir" "$temporary_root/registry"
    printf 'import fixture\n' >"$temporary_root/registry/libwinapi_ntdll.a"
    printf 'cargo:rustc-link-search=native=%s\n' "$temporary_root/registry" \
      >"$metadata_dir/output"
    ;;
esac
printf 'archive\n' >"$CARGO_TARGET_DIR/debug/libocaml_temporal_core_bridge.a"
printf 'shared\n' >"$CARGO_TARGET_DIR/debug/$dynamic"

# Runs the bridge build against the scratch workspace, logging Cargo calls.
run_build() {
  : >"$CARGO_LOG"
  sh "$workspace/scripts/build-rust-bridge.sh" "$workspace" \
    "$temporary_root/static.a" "$temporary_root/shared" "$temporary_root/flags.sexp"
}

# Fails unless Cargo ran exactly twice (build and the link-flag probe).
expect_two_invocations() {
  test "$(grep -c '^args: ' "$CARGO_LOG")" -eq 2 || {
    echo "expected cargo build and cargo rustc: $1" >&2
    cat "$CARGO_LOG" >&2
    exit 1
  }
}

# A development checkout keeps the online, lockfile-enforcing build.
run_build
expect_two_invocations "development checkout"
grep '^args: ' "$CARGO_LOG" | while IFS= read -r invocation; do
  case " $invocation " in
    *" --frozen "* | *" --offline "* | *" --config "*)
      echo "development build was forced offline: $invocation" >&2; exit 1 ;;
    *" --locked "*) ;;
    *) echo "development build dropped --locked: $invocation" >&2; exit 1 ;;
  esac
done
if grep -v '^offline: unset$' "$CARGO_LOG" | grep -q '^offline: '; then
  echo "development build set CARGO_NET_OFFLINE" >&2
  exit 1
fi

# A release source archive: a crate with a hidden directory, which Dune's
# source_tree copy would drop, and the replacement printed by cargo vendor.
mkdir -p "$temporary_root/vendor-input/vendor/fixture-crate-1.0.0/.github"
printf '[package]\nname = "fixture-crate"\n' \
  >"$temporary_root/vendor-input/vendor/fixture-crate-1.0.0/Cargo.toml"
: >"$temporary_root/vendor-input/vendor/fixture-crate-1.0.0/.github/kept"
tar -cf "$workspace/rust/vendor.tar" -C "$temporary_root/vendor-input" vendor
cat >"$workspace/rust/vendor-config.toml" <<'EOF'
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "vendor"
EOF
# A crate left over from an earlier archive must not survive unpacking.
mkdir -p "$CARGO_TARGET_DIR/vendored-sources/vendor/stale-crate-0.1.0"

run_build
expect_two_invocations "vendored archive"
test "$(grep -c '^config: ok$' "$CARGO_LOG")" -eq 2 || {
  echo "Cargo did not receive the unpacked vendored sources" >&2
  cat "$CARGO_LOG" >&2
  exit 1
}
grep '^args: ' "$CARGO_LOG" | while IFS= read -r invocation; do
  case " $invocation " in
    *" --frozen --config $CARGO_TARGET_DIR/vendored-sources/.cargo/config.toml "*) ;;
    *) echo "vendored build may reach the network: $invocation" >&2; exit 1 ;;
  esac
done
test "$(grep -c '^offline: true$' "$CARGO_LOG")" -eq 2 || {
  echo "vendored build did not set CARGO_NET_OFFLINE=true" >&2
  cat "$CARGO_LOG" >&2
  exit 1
}

# Either file alone is an incomplete archive, rejected before Cargo runs.
for missing in vendor.tar vendor-config.toml; do
  mv "$workspace/rust/$missing" "$temporary_root/$missing"
  : >"$CARGO_LOG"
  if run_build 2>"$temporary_root/stderr"; then
    echo "bridge build accepted an archive without rust/$missing" >&2
    exit 1
  fi
  grep -F 'source archive is incomplete' "$temporary_root/stderr" >/dev/null
  test ! -s "$CARGO_LOG" || {
    echo "Cargo ran for an archive without rust/$missing" >&2
    exit 1
  }
  mv "$temporary_root/$missing" "$workspace/rust/$missing"
done

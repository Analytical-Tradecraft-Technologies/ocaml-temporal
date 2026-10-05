#!/bin/sh
set -eu

# Verifies the Cargo profile selected by scripts/build-rust-bridge.sh (#779).
# Dune's release profile, used by [opam install] and [dune build -p], must build
# and copy Cargo's optimized release output; every other Dune profile must use
# Cargo's dev output. A stand-in Cargo records its arguments, and each profile
# directory holds distinguishable fixture archives, so the test proves that the
# copied archive comes from the profile that was built rather than from a stale
# build of the other profile. No real Rust toolchain is needed.
workspace_root=$(cd "$1" && pwd)
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM
unset TEMPORAL_RUST_BRIDGE_DIR TEMPORAL_RUST_BRIDGE_KEY

required=$(sed -n 's/^rust-version[[:space:]]*=[[:space:]]*"\([0-9][0-9.]*\)"[[:space:]]*$/\1/p' \
  "$workspace_root/rust/Cargo.toml" | head -n 1)
mkdir -p "$temporary_root/bin"
# A rustc reporting exactly the workspace minimum passes the version gate.
printf '#!/bin/sh\necho "rustc %s.0 (0000000 2025-01-01)"\n' "$required" \
  >"$temporary_root/bin/rustc"
# Records every Cargo invocation, one line each, and reports link flags for the
# native-static-libs probe as rustc would.
cat >"$temporary_root/bin/cargo" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$CARGO_LOG"
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

case "$(uname -s)" in
  Darwin) dynamic=libocaml_temporal_core_bridge.dylib ;;
  MINGW* | MSYS* | CYGWIN*) dynamic=ocaml_temporal_core_bridge.dll ;;
  *) dynamic=libocaml_temporal_core_bridge.so ;;
esac
for directory in debug release; do
  mkdir -p "$CARGO_TARGET_DIR/$directory"
  case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*)
      # Windows link flags need Cargo's winapi build-script metadata from the
      # selected profile directory; give each profile its own import library.
      metadata_dir=$CARGO_TARGET_DIR/$directory/build/winapi-x86_64-pc-windows-gnu-fixture
      mkdir -p "$metadata_dir" "$temporary_root/registry-$directory"
      printf 'import fixture\n' >"$temporary_root/registry-$directory/libwinapi_ntdll.a"
      printf 'cargo:rustc-link-search=native=%s\n' "$temporary_root/registry-$directory" \
        >"$metadata_dir/output"
      ;;
  esac
  printf '%s archive\n' "$directory" \
    >"$CARGO_TARGET_DIR/$directory/libocaml_temporal_core_bridge.a"
  printf '%s shared\n' "$directory" >"$CARGO_TARGET_DIR/$directory/$dynamic"
done

# Builds the bridge for Dune profile $1 and checks that every Cargo invocation
# used Cargo profile $2 and that the copied outputs came from directory $3.
check_profile() {
  : >"$CARGO_LOG"
  OCAML_TEMPORAL_BUILD_PROFILE=$1 sh "$workspace_root/scripts/build-rust-bridge.sh" \
    "$workspace_root" "$temporary_root/static.a" "$temporary_root/shared" \
    "$temporary_root/flags.sexp"
  test "$(wc -l <"$CARGO_LOG")" -eq 2 || {
    echo "expected cargo build and cargo rustc for Dune profile $1" >&2
    cat "$CARGO_LOG" >&2; exit 1;
  }
  while IFS= read -r invocation; do
    case " $invocation " in
      *" --profile $2 "*) ;;
      *) echo "Dune profile $1 ran Cargo without --profile $2: $invocation" >&2; exit 1 ;;
    esac
  done <"$CARGO_LOG"
  cmp "$temporary_root/static.a" "$CARGO_TARGET_DIR/$3/libocaml_temporal_core_bridge.a"
  cmp "$temporary_root/shared" "$CARGO_TARGET_DIR/$3/$dynamic"
  case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*)
      grep -F "registry-$3" "$temporary_root/flags.sexp" >/dev/null || {
        echo "Dune profile $1 did not use $3 winapi metadata" >&2
        cat "$temporary_root/flags.sexp" >&2; exit 1;
      }
      ;;
  esac
}

check_profile release release release
check_profile dev dev debug
# Custom Dune profiles keep the fast development build.
check_profile custom-profile dev debug

# A caller that does not state its profile is a defect, not a silent default.
if (unset OCAML_TEMPORAL_BUILD_PROFILE; sh "$workspace_root/scripts/build-rust-bridge.sh" \
  "$workspace_root" "$temporary_root/static.a" "$temporary_root/shared" \
  "$temporary_root/flags.sexp") 2>/dev/null; then
  echo 'bridge build accepted a missing OCAML_TEMPORAL_BUILD_PROFILE' >&2
  exit 1
fi

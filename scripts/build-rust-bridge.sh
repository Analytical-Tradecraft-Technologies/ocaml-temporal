#!/bin/sh
set -eu

workspace_root=$1
static_output=$2
dynamic_output=$3
link_flags_output=$4
bundle_output=${5:-}

if [ -n "${TEMPORAL_RUST_BRIDGE_DIR:-}" ]; then
  exec sh "$workspace_root/scripts/rust-bridge-artifact.sh" use "$workspace_root" \
    "$TEMPORAL_RUST_BRIDGE_DIR" "${TEMPORAL_RUST_BRIDGE_KEY:?artifact key is required}" \
    "$static_output" "$dynamic_output" "$link_flags_output"
fi

# Select the Cargo profile from the build profile requesting the bridge (#779).
# Dune passes its own profile: [opam install] and [dune build -p] select
# "release", which must link Cargo's optimized [profile.release] into the
# user's worker. Every other value, including Dune's default "dev" and any
# custom Dune profile, keeps Cargo's unoptimized dev profile for fast iteration
# and debug assertions. Cargo writes the two profiles to different output
# directories, so the copied archive and the Windows build-script metadata are
# always read from the directory of the profile that was just built, never from
# a stale build of the other profile. An unset value is a caller defect.
build_profile=${OCAML_TEMPORAL_BUILD_PROFILE:?set OCAML_TEMPORAL_BUILD_PROFILE to the requesting build profile}
case "$build_profile" in
  release)
    cargo_profile=release
    profile_dir=release
    ;;
  *)
    cargo_profile=dev
    profile_dir=debug
    ;;
esac

# Dune copy sandboxes expose the Rust source tree read-only. They set the
# private fallback below to a writable sibling, while callers such as Docker
# and the native Makefile set CARGO_TARGET_DIR directly. Keep the explicit
# target directory authoritative so those workflows share one Cargo cache.
if [ -n "${CARGO_TARGET_DIR:-}" ]; then
  target_root=$CARGO_TARGET_DIR
elif [ -n "${OCAML_TEMPORAL_RUST_TARGET_FALLBACK:-}" ]; then
  target_root=$OCAML_TEMPORAL_RUST_TARGET_FALLBACK
  # Cargo must receive the same fallback selected for artifact copying. This
  # assignment is deliberately limited to the unset case so an existing
  # CARGO_TARGET_DIR is never replaced.
  export CARGO_TARGET_DIR="$target_root"
else
  target_root=$workspace_root/rust/target
fi
artifact_root=$target_root

case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*)
    if command -v cygpath >/dev/null 2>&1; then
      artifact_root=$(cygpath -u "$target_root")
    fi
    ;;
esac

# The opam dependency conf-rust-2024 only proves that the host compiler accepts
# Rust edition 2024 (Rust 1.85), while the workspace declares a newer
# rust-version. Without this gate an opam user with an older distribution
# toolchain passes the conf package and then fails deep inside Cargo's
# dependency resolution with an error that does not name the real cause.
# Compare against the same compiler Cargo will invoke: Cargo honours RUSTC, and
# both commands run from this directory, so rustup toolchain selection agrees.
required_rust_version=$(sed -n 's/^rust-version[[:space:]]*=[[:space:]]*"\([0-9][0-9.]*\)"[[:space:]]*$/\1/p' \
  "$workspace_root/rust/Cargo.toml" | head -n 1)
if [ -z "$required_rust_version" ]; then
  echo "rust/Cargo.toml does not declare a numeric workspace rust-version" >&2
  exit 1
fi
rustc_command=${RUSTC:-rustc}
if ! rustc_banner=$("$rustc_command" --version 2>/dev/null); then
  echo "cannot run '$rustc_command --version'; install Rust $required_rust_version or newer (https://rustup.rs)" >&2
  exit 1
fi
# "rustc 1.98.1 (hash date)" or "rustc 1.99.0-nightly (...)": keep only the
# numeric MAJOR.MINOR.PATCH so prerelease channels compare by their release.
actual_rust_version=$(printf '%s\n' "$rustc_banner" |
  sed -n 's/^rustc \([0-9][0-9]*\.[0-9][0-9]*\(\.[0-9][0-9]*\)\{0,1\}\).*$/\1/p')
if [ -z "$actual_rust_version" ]; then
  echo "cannot parse the Rust compiler version from: $rustc_banner" >&2
  exit 1
fi
# Prints MAJOR MINOR PATCH with absent components treated as zero.
rust_version_fields() {
  printf '%s\n' "$1" | awk -F. '{ printf "%d %d %d\n", $1, $2, $3 }'
}
# Succeeds when version $1 is at least version $2, comparing numerically.
rust_version_at_least() {
  set -- $(rust_version_fields "$1") $(rust_version_fields "$2")
  [ "$1" -gt "$4" ] && return 0
  [ "$1" -lt "$4" ] && return 1
  [ "$2" -gt "$5" ] && return 0
  [ "$2" -lt "$5" ] && return 1
  [ "$3" -ge "$6" ]
}
if ! rust_version_at_least "$actual_rust_version" "$required_rust_version"; then
  echo "the Temporal SDK Rust bridge requires Rust $required_rust_version or newer, but '$rustc_command' is $actual_rust_version." >&2
  echo "Install a newer toolchain (for example 'rustup update stable') or set RUSTC to a compatible compiler." >&2
  exit 1
fi

# Select where Cargo's dependency sources come from (#778). A release source
# archive (scripts/create-source-archive.sh) ships every locked crate, including
# the pinned Temporal Core Git checkout, as rust/vendor.tar together with the
# matching source-replacement configuration rust/vendor-config.toml. opam's
# build sandbox denies network access, so that build must never contact a
# registry or Git host: Cargo runs with --frozen (--locked plus --offline) and
# CARGO_NET_OFFLINE, and the replacement routes crates.io and the Git source to
# the extracted directory source, whose per-file checksums Cargo verifies.
#
# The crates stay archived because Dune's source_tree dependency omits
# directories whose names start with "." or "_", and many crates package such
# directories (for example .github), so a copied vendor tree would fail Cargo's
# checksum verification. The archive is unpacked afresh into the writable
# Cargo target directory on every run so no stale crate survives a source
# update. Development checkouts have neither file and keep fetching the locked
# graph normally with --locked.
vendor_archive=$workspace_root/rust/vendor.tar
vendor_config=$workspace_root/rust/vendor-config.toml
if [ -f "$vendor_archive" ] || [ -f "$vendor_config" ]; then
  if [ ! -f "$vendor_archive" ] || [ ! -f "$vendor_config" ]; then
    echo "an offline source build needs both rust/vendor.tar and rust/vendor-config.toml; the source archive is incomplete" >&2
    exit 1
  fi
  # Shell tools use the MSYS spelling of the target directory; Cargo receives
  # the caller's spelling, which is relative or native on every platform.
  vendor_root=$artifact_root/vendored-sources
  rm -rf "$vendor_root"
  mkdir -p "$vendor_root/.cargo"
  tar -xf "$vendor_archive" -C "$vendor_root"
  if [ ! -d "$vendor_root/vendor" ]; then
    echo "rust/vendor.tar does not contain the vendor directory" >&2
    exit 1
  fi
  # Cargo resolves the relative directory = "vendor" entry against the parent
  # of this .cargo directory, which is the extracted vendor_root.
  cp "$vendor_config" "$vendor_root/.cargo/config.toml"
  export CARGO_NET_OFFLINE=true
  set -- --frozen --config "$target_root/vendored-sources/.cargo/config.toml"
else
  set -- --locked
fi

cargo build \
  --manifest-path "$workspace_root/rust/Cargo.toml" \
  --package ocaml-temporal-core-bridge \
  --profile "$cargo_profile" \
  "$@"

native_link_output=$(mktemp)
trap 'rm -f "$native_link_output"' EXIT HUP INT TERM

if ! CARGO_TERM_COLOR=never cargo rustc \
  --manifest-path "$workspace_root/rust/Cargo.toml" \
  --package ocaml-temporal-core-bridge \
  --profile "$cargo_profile" \
  "$@" \
  --lib \
  --crate-type staticlib \
  -- \
  --print=native-static-libs \
  2>"$native_link_output"
then
  cat "$native_link_output" >&2
  exit 1
fi

native_link_flags=$(sed -n 's/^note: native-static-libs: //p' "$native_link_output" | tail -n 1)
if [ -z "$native_link_flags" ]; then
  cat "$native_link_output" >&2
  echo "rustc did not report native static-library link flags" >&2
  exit 1
fi

if [ -n "$bundle_output" ]; then
  printf '%s\n' "$native_link_flags" >"$bundle_output/native-static-libs"
fi

# rustc owns the platform-specific library list and its ordering. On Windows,
# also preserve the Cargo build-script search path needed to resolve winapi's
# bundled MinGW import archives from OCaml's foreign linker.
sh "$workspace_root/scripts/render-rust-link-flags.sh" \
  "$(uname -s)" \
  "$artifact_root/$profile_dir" \
  "$link_flags_output" \
  "$native_link_flags" \
  "${bundle_output:+$bundle_output/import-libs}"

"$workspace_root/scripts/copy-rust-bridge-artifacts.sh" \
  "$artifact_root/$profile_dir" "$static_output" "$dynamic_output"

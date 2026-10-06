#!/bin/sh
set -eu

# Creates the release source archive that opam installs from (#778).
#
# Usage: create-source-archive.sh REPOSITORY_ROOT RELEASE_TAG OUTPUT [NOTICES]
#
# The archive is the committed tree at HEAD (git archive, so ignored and
# uncommitted files never leak in) below the directory
# ocaml-temporal-<version>/, plus everything Cargo needs to build the Rust
# bridge without network access:
#
# - rust/vendor.tar: `cargo vendor --locked` output for the exact Cargo.lock
#   graph, including the pinned Temporal Core Git revision. Crates keep their
#   own licence files and Cargo's per-file checksums.
# - rust/vendor-config.toml: the source replacement printed by cargo vendor,
#   rewritten to the relative directory "vendor" so it is valid wherever
#   scripts/build-rust-bridge.sh unpacks the archive.
# - THIRD-PARTY-NOTICES.txt, when NOTICES is given: the audited licence texts
#   for the redistributed Rust packages, including the reviewed texts for
#   crates that publish none.
#
# opam's build sandbox denies network access; scripts/build-rust-bridge.sh
# detects the two rust/ files and runs Cargo with --frozen against them. This
# script itself needs network access (or a warm Cargo cache) and runs in the
# release workflow, never inside an opam build. It adds no dependency: the
# vendored set is exactly the locked graph already covered by the licence
# audit. GNU tar output is normalised (sorted names, fixed owner and mtime) so
# repeated runs of one commit produce identical bytes; other tar
# implementations still produce a valid but not byte-reproducible archive.
repository_root=$(cd "$1" && pwd)
release_tag=$2
output=$3
notices=${4:-}

case "$output" in
  /*) ;;
  *) output=$(pwd)/$output ;;
esac
if [ -n "$notices" ] && [ ! -f "$notices" ]; then
  echo "third-party notices file not found: $notices" >&2
  exit 1
fi

prefix=ocaml-temporal-${release_tag#v}
staging=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-source.XXXXXX")
# The staging tree holds several hundred megabytes of crate sources; remove it
# on every exit path.
trap 'rm -rf "$staging"' EXIT HUP INT TERM

tree=$staging/$prefix
git -C "$repository_root" archive --format=tar --prefix="$prefix/" HEAD |
  tar -xf - -C "$staging"
test -f "$tree/rust/Cargo.lock"
if [ -e "$tree/rust/vendor.tar" ] || [ -e "$tree/rust/vendor-config.toml" ]; then
  echo "the committed tree must not contain rust/vendor.tar or rust/vendor-config.toml" >&2
  exit 1
fi

# Vendor from the archived tree so the crate set matches the committed
# Cargo.lock rather than any uncommitted edit in the working copy.
cargo vendor --manifest-path "$tree/rust/Cargo.toml" --locked --versioned-dirs \
  "$staging/vendor" >"$staging/cargo-vendor.out"

# Keep only the TOML that cargo vendor prints after its human-readable preface,
# and point the directory source at the relative vendor directory.
sed -n '/^\[source/,$p' "$staging/cargo-vendor.out" |
  sed 's|^directory = .*$|directory = "vendor"|' >"$tree/rust/vendor-config.toml"
if [ "$(grep -c '^directory = "vendor"$' "$tree/rust/vendor-config.toml")" -ne 1 ] ||
  ! grep -q '^replace-with = "vendored-sources"$' "$tree/rust/vendor-config.toml"; then
  echo "unexpected cargo vendor configuration:" >&2
  cat "$staging/cargo-vendor.out" >&2
  exit 1
fi

if tar --version 2>/dev/null | grep -q 'GNU tar'; then
  set -- --sort=name --owner=0 --group=0 --numeric-owner --mtime=@0
else
  set --
fi
tar -cf "$tree/rust/vendor.tar" "$@" -C "$staging" vendor
if [ -n "$notices" ]; then
  cp "$notices" "$tree/THIRD-PARTY-NOTICES.txt"
fi

mkdir -p "$(dirname "$output")"
tar -cf - "$@" -C "$staging" "$prefix" | gzip -n >"$output"

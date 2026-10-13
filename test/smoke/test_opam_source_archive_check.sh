#!/bin/sh
set -eu

# Contract test for scripts/check-opam-source-archive.sh (#778), the CI lane
# that builds the release source archive inside opam's network-denying build
# sandbox. The real lane needs a full optimized Rust build, so this test
# replaces opam's sandbox hook, curl and dune with recording stand-ins and
# checks the script's own decisions without a toolchain or network:
# - a complete archive is built with the package's opam build command through
#   the opam hook's "build" mode, with every variable that would bypass the
#   archive's vendored sources cleared and a fresh, empty CARGO_HOME inside
#   the build directory in place of the caller's warm one;
# - a sandbox that lets the network probe through is refused before building;
# - an archive without the vendored crates is rejected; and
# - an opam file whose build command changed is reported instead of being
#   silently replaced by this check's copy.
source_root=$(cd "$1" && pwd)
check=$source_root/scripts/check-opam-source-archive.sh
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM

# Stand-in opam root whose sandbox hook records its mode and command, then
# runs the command unsandboxed.
fake_opam_root=$temporary_root/opam-root
mkdir -p "$fake_opam_root/opam-init/hooks" "$temporary_root/switch" "$temporary_root/bin"
cat >"$fake_opam_root/opam-init/hooks/sandbox.sh" <<'EOF'
#!/bin/sh
set -eu
mode=$1
shift
printf '%s %s\n' "$mode" "$1" >>"$CHECK_LOG"
exec "$@"
EOF
chmod +x "$fake_opam_root/opam-init/hooks/sandbox.sh"

cat >"$temporary_root/bin/opam" <<'EOF'
#!/bin/sh
set -eu
case "$1 $2" in
  "var root") printf '%s\n' "$FAKE_OPAM_ROOT" ;;
  "var prefix") printf '%s\n' "$FAKE_OPAM_PREFIX" ;;
  *) exit 2 ;;
esac
EOF
# The probe outcome is chosen per case: 7 is curl's "failed to connect".
cat >"$temporary_root/bin/curl" <<'EOF'
#!/bin/sh
printf 'curl\n' >>"$CHECK_LOG"
exit "$FAKE_CURL_STATUS"
EOF
# Records its arguments and the bypass variables, then produces the install
# file a successful `dune build -p` leaves behind.
cat >"$temporary_root/bin/dune" <<'EOF'
#!/bin/sh
set -eu
printf 'dune %s\n' "$*" >>"$CHECK_LOG"
printf 'env CARGO_TARGET_DIR=%s TEMPORAL_RUST_BRIDGE_DIR=%s CARGO_NET_OFFLINE=%s\n' \
  "${CARGO_TARGET_DIR-unset}" "${TEMPORAL_RUST_BRIDGE_DIR-unset}" \
  "${CARGO_NET_OFFLINE-unset}" >>"$CHECK_LOG"
# The Cargo home must be the cold directory the check created in the build
# directory, never the caller's.
if [ "${CARGO_HOME-}" = "$(pwd -P)/_opam-check-cargo-home" ] && [ -d "$CARGO_HOME" ] &&
  [ -z "$(ls -A "$CARGO_HOME")" ]; then
  printf 'cargo-home cold\n' >>"$CHECK_LOG"
else
  printf 'cargo-home %s\n' "${CARGO_HOME-unset}" >>"$CHECK_LOG"
fi
test -f rust/vendor.tar
mkdir -p _build/default
printf '"lib/libocaml_temporal_core_bridge.a"\n' >_build/default/temporal-sdk.install
EOF
chmod +x "$temporary_root/bin/opam" "$temporary_root/bin/curl" "$temporary_root/bin/dune"

# A Cargo home already populated, as `cargo vendor` leaves the default one
# when it creates the archive.
warm_cargo_home=$temporary_root/warm-cargo-home
mkdir -p "$warm_cargo_home/registry/cache"
: >"$warm_cargo_home/registry/cache/crate.crate"

# Builds a source archive at $1 shipping opam file $2 and the empty rust/
# files named by the remaining arguments.
make_archive() {
  archive_output=$1
  tree=$temporary_root/archive-tree
  rm -rf "$tree"
  mkdir -p "$tree/ocaml-temporal-0.0.0/rust"
  cp "$2" "$tree/ocaml-temporal-0.0.0/temporal-sdk.opam"
  shift 2
  for file in "$@"; do
    : >"$tree/ocaml-temporal-0.0.0/rust/$file"
  done
  tar -cf - -C "$tree" ocaml-temporal-0.0.0 | gzip -n >"$archive_output"
}

# Runs the check against archive $1 with curl exiting $2; the exit status and
# combined output are left in $status and $temporary_root/output.
run_check() {
  rm -rf "$temporary_root/work" "$temporary_root/check.log"
  : >"$temporary_root/check.log"
  set +e
  PATH=$temporary_root/bin:$PATH \
    FAKE_OPAM_ROOT=$fake_opam_root FAKE_OPAM_PREFIX=$temporary_root/switch \
    FAKE_CURL_STATUS=$2 CHECK_LOG=$temporary_root/check.log \
    CARGO_TARGET_DIR=$temporary_root/shared-target \
    TEMPORAL_RUST_BRIDGE_DIR=$temporary_root/prebuilt CARGO_NET_OFFLINE=false \
    CARGO_HOME=$warm_cargo_home \
    sh "$check" "$1" "$temporary_root/work" >"$temporary_root/output" 2>&1
  status=$?
  set -e
}

# Fails the test with the check's output when $1 is not in it.
expect_output() {
  if ! grep -Fq "$1" "$temporary_root/output"; then
    echo "expected check output to contain: $1" >&2
    cat "$temporary_root/output" >&2
    exit 1
  fi
}

# A complete archive is built with the opam command through the hook.
make_archive "$temporary_root/complete.tar.gz" "$source_root/temporal-sdk.opam" \
  Cargo.lock vendor.tar vendor-config.toml
run_check "$temporary_root/complete.tar.gz" 7
if [ "$status" -ne 0 ]; then
  cat "$temporary_root/output" >&2
  exit 1
fi
expect_output 'sandboxed opam build of ocaml-temporal-0.0.0 succeeded'
grep -Fqx 'build curl' "$temporary_root/check.log"
grep -Fqx 'build dune' "$temporary_root/check.log"
grep -Eq '^dune build -p temporal-sdk -j [1-9][0-9]*$' "$temporary_root/check.log"
grep -Fqx 'env CARGO_TARGET_DIR=unset TEMPORAL_RUST_BRIDGE_DIR=unset CARGO_NET_OFFLINE=unset' \
  "$temporary_root/check.log"
grep -Fqx 'cargo-home cold' "$temporary_root/check.log"

# A sandbox that lets the probe reach the network proves nothing.
run_check "$temporary_root/complete.tar.gz" 0
test "$status" -ne 0
expect_output 'allowed a network request'
if grep -q '^dune' "$temporary_root/check.log"; then
  echo "the build ran although the sandbox allowed the network" >&2
  exit 1
fi

# An archive without the vendored crates is not a release source archive.
make_archive "$temporary_root/unvendored.tar.gz" "$source_root/temporal-sdk.opam" \
  Cargo.lock vendor-config.toml
run_check "$temporary_root/unvendored.tar.gz" 7
test "$status" -ne 0
expect_output 'missing rust/vendor.tar'

# A changed opam build command must be noticed, not replaced by this copy.
sed 's/"-p" name/"--profile" "release" "-p" name/' "$source_root/temporal-sdk.opam" \
  >"$temporary_root/changed.opam"
make_archive "$temporary_root/changed.tar.gz" "$temporary_root/changed.opam" \
  Cargo.lock vendor.tar vendor-config.toml
run_check "$temporary_root/changed.tar.gz" 7
test "$status" -ne 0
expect_output 'update this check'

echo "opam source archive check contract: passed"

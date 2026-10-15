#!/bin/sh
set -eu

# Contract test for scripts/check-source-archive-consumer.sh (#510), which
# installs the package built from a release source archive into a fresh prefix
# and builds the installed-package consumer against it. The real check needs
# the sandboxed optimized build, so this test replaces dune and opam with
# recording stand-ins and checks the script's own decisions:
# - a complete build is installed from the unpacked archive with the build's
#   `-p` flags into WORK/prefix, the consumer is copied from the archive's own
#   fixture into WORK/consumer, built with the prefix first on OCAMLPATH, and
#   every consumer executable runs (replay with the archive's history);
# - a switch that already provides temporal-sdk is refused before installing;
# - a work directory without the sandboxed build is refused;
# - an archive without the consumer fixture is refused; and
# - an install that does not place the Rust bridge is refused.
source_root=$(cd "$1" && pwd)
check=$source_root/scripts/check-source-archive-consumer.sh
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM
mkdir -p "$temporary_root/bin" "$temporary_root/switch/lib"

# Stand-in opam: only the switch prefix query is used.
cat >"$temporary_root/bin/opam" <<'EOF'
#!/bin/sh
set -eu
case "$1 $2" in
  "var prefix") printf '%s\n' "$FAKE_OPAM_PREFIX" ;;
  *) exit 2 ;;
esac
EOF
# Stand-in dune. `install` records its directory and arguments and creates
# the installed layout unless FAKE_INSTALL_BROKEN is set; `build` records its
# arguments and OCAMLPATH and creates consumer executables that log each run.
cat >"$temporary_root/bin/dune" <<'EOF'
#!/bin/sh
set -eu
case "$1" in
  install)
    printf 'install-dir %s\n' "$(pwd -P)" >>"$CHECK_LOG"
    printf 'dune %s\n' "$*" >>"$CHECK_LOG"
    prefix=
    while [ "$#" -gt 0 ]; do
      if [ "$1" = --prefix ]; then prefix=$2; fi
      shift
    done
    mkdir -p "$prefix/lib/temporal-sdk/__private__/temporal_core_bridge"
    : >"$prefix/lib/temporal-sdk/META"
    if [ -z "${FAKE_INSTALL_BROKEN:-}" ]; then
      printf 'archive' >"$prefix/lib/temporal-sdk/__private__/temporal_core_bridge/libocaml_temporal_core_bridge.a"
    fi
    ;;
  build)
    printf 'dune %s\n' "$*" >>"$CHECK_LOG"
    printf 'ocamlpath %s\n' "${OCAMLPATH-unset}" >>"$CHECK_LOG"
    shift
    root=
    while [ "$#" -gt 0 ]; do
      if [ "$1" = --root ]; then root=$2; fi
      shift
    done
    mkdir -p "$root/_build/default"
    for program in main.exe main.bc worker_environment.exe installed_replay.exe; do
      cat >"$root/_build/default/$program" <<PROGRAM
#!/bin/sh
printf 'run $program %s in %s\n' "\$*" "\$(pwd -P)" >>"$CHECK_LOG"
PROGRAM
      chmod +x "$root/_build/default/$program"
    done
    ;;
  *) exit 2 ;;
esac
EOF
chmod +x "$temporary_root/bin/opam" "$temporary_root/bin/dune"

# Creates a work directory at $1 as check-opam-source-archive.sh leaves it:
# the unpacked archive with its install file, consumer fixture and history.
# Further arguments name pieces to leave out: "build" or "fixture".
make_work() {
  work=$1
  shift
  package_root=$work/source/ocaml-temporal-0.0.0
  rm -rf "$work"
  mkdir -p "$package_root/test/fixtures/install-consumer" \
    "$package_root/test/fixtures/history-corpus/histories"
  case " $* " in
    *" build "*) ;;
    *)
      mkdir -p "$package_root/_build/default"
      : >"$package_root/_build/default/temporal-sdk.install"
      ;;
  esac
  case " $* " in
    *" fixture "*) ;;
    *)
      for file in dune-project dune main.ml public_api.ml worker_environment.ml installed_replay.ml; do
        printf '(* archive copy of %s *)\n' "$file" \
          >"$package_root/test/fixtures/install-consumer/$file"
      done
      ;;
  esac
  : >"$package_root/test/fixtures/history-corpus/histories/live-2026-10-08-timer.pb"
}

# Runs the check on work directory $1 with switch prefix $2; the exit status
# and combined output are left in $status and $temporary_root/output.
run_check() {
  : >"$temporary_root/check.log"
  set +e
  env -u OPAM_SWITCH_PREFIX PATH="$temporary_root/bin:$PATH" \
    FAKE_OPAM_PREFIX="$2" CHECK_LOG="$temporary_root/check.log" \
    OCAMLPATH=/caller/lib \
    sh "$check" "$1" >"$temporary_root/output" 2>&1
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

# Fails the test with the check log when no line equals $1.
expect_log() {
  if ! grep -Fqx "$1" "$temporary_root/check.log"; then
    echo "expected check log line: $1" >&2
    cat "$temporary_root/check.log" >&2
    exit 1
  fi
}

# A complete build is installed into the prefix and consumed from it.
work=$temporary_root/work
make_work "$work"
run_check "$work" "$temporary_root/switch"
if [ "$status" -ne 0 ]; then
  cat "$temporary_root/output" >&2
  exit 1
fi
work=$(cd "$work" && pwd -P)
expect_output 'installed consumer of ocaml-temporal-0.0.0 built and ran'
expect_log "install-dir $work/source/ocaml-temporal-0.0.0"
expect_log "dune install -p temporal-sdk --prefix $work/prefix"
grep -Eq "^dune build --root $work/consumer -j [1-9][0-9]* ./main.exe ./main.bc ./worker_environment.exe ./installed_replay.exe$" \
  "$temporary_root/check.log"
expect_log "ocamlpath $work/prefix/lib:/caller/lib"
grep -Fqx '(* archive copy of main.ml *)' "$work/consumer/main.ml"
test -f "$work/consumer/timer-history.pb"
expect_log "run main.exe  in $(pwd -P)"
expect_log "run main.bc  in $(pwd -P)"
expect_log "run worker_environment.exe  in $(pwd -P)"
expect_log "run installed_replay.exe timer-history.pb in $work/consumer"

# A second run must not reuse the first run's prefix or consumer.
run_check "$work" "$temporary_root/switch"
test "$status" -ne 0
expect_output 'already contains'

# A switch that already has the package could satisfy the consumer itself.
make_work "$work"
mkdir -p "$temporary_root/switch-with-sdk/lib/temporal-sdk"
run_check "$work" "$temporary_root/switch-with-sdk"
test "$status" -ne 0
expect_output 'already provides temporal-sdk'
if grep -q '^dune' "$temporary_root/check.log"; then
  echo "dune ran although the switch already provides the package" >&2
  exit 1
fi

# Without the sandboxed build there is nothing from the archive to install.
make_work "$work" build
run_check "$work" "$temporary_root/switch"
test "$status" -ne 0
expect_output 'run check-opam-source-archive.sh first'

# The consumer must come from the archive, never from a checkout.
make_work "$work" fixture
run_check "$work" "$temporary_root/switch"
test "$status" -ne 0
expect_output 'source archive is missing test/fixtures/install-consumer/dune-project'

# An install without the Rust bridge is not a usable package.
make_work "$work"
: >"$temporary_root/check.log"
set +e
env -u OPAM_SWITCH_PREFIX PATH="$temporary_root/bin:$PATH" \
  FAKE_OPAM_PREFIX="$temporary_root/switch" CHECK_LOG="$temporary_root/check.log" \
  FAKE_INSTALL_BROKEN=1 sh "$check" "$work" >"$temporary_root/output" 2>&1
status=$?
set -e
test "$status" -ne 0
expect_output 'did not place temporal-sdk with its Rust bridge'

echo "source archive consumer check contract: passed"

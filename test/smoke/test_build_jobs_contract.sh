#!/bin/sh
set -eu

# Docker-free regression for #851. Dune links many executables against the
# large Rust static library in parallel, which OOM-kills a default Docker
# Desktop VM. The DUNE_JOBS bound only helps if it reaches every Make-owned
# Dune invocation, including `dune runtest` and `dune exec`, so this contract
# checks the Makefile source and the recipes Make actually prints.

# The probes choose their own variables; a caller's recursive Make overrides
# must not leak in through MAKEFLAGS or the environment.
unset MAKEFLAGS MFLAGS MAKEOVERRIDES DUNE_JOBS CARGO_BUILD_JOBS

source_root=${1:-.}
makefile=$source_root/Makefile
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM

# Source check: every non-comment Makefile line that starts Dune passes
# DUNE_BUILD_ARGS, or hands DUNE_JOBS to scripts/build-temporal-executables.sh,
# which converts it into `-j`. CRLF is stripped for Windows checkouts.
tr -d '\015' <"$makefile" | awk '
  /^[[:space:]]*#/ { next }
  /dune (build|runtest|test|exec)([^-[:alnum:]]|$)/ &&
    index($0, "$(DUNE_BUILD_ARGS)") == 0 {
    printf "Makefile:%d: Dune invocation without $(DUNE_BUILD_ARGS): %s\n", NR, $0
    missing = 1
  }
  /build-temporal-executables\.sh/ && index($0, "DUNE_JOBS=\"$(DUNE_JOBS)\"") == 0 {
    printf "Makefile:%d: executable build without DUNE_JOBS: %s\n", NR, $0
    missing = 1
  }
  END { exit missing }
' >&2

# Prints every Dune command that Make would run for the Docker and native
# build/test gates. MAKE=true keeps recursive recipes from expanding further;
# each target that a gate delegates to is listed explicitly instead.
dry_run() {
  make --no-print-directory -n -C "$source_root" -f Makefile "$@" \
    MAKE=true TEMPORAL_RUST_BRIDGE_DIR= \
    build build-examples lint test test-unit test-runtime \
    test-workflow-task-failure native-build native-lint native-test \
    build-smoke-executables >"$temporary_root/plan" 2>&1 || {
    cat "$temporary_root/plan" >&2
    exit 1
  }
}

# Asserts that the dry run contains Dune commands and that each one carries
# the expected `-j` argument (or, for the executable helper, DUNE_JOBS value).
expect_jobs() {
  label=$1
  expected=$2
  grep -E 'dune (build|runtest|exec)' "$temporary_root/plan" \
    >"$temporary_root/dune" || {
    echo "$label: dry run printed no Dune commands" >&2
    exit 1
  }
  grep -q 'dune runtest' "$temporary_root/dune" || {
    echo "$label: dry run printed no dune runtest command" >&2
    exit 1
  }
  if grep -Ev -- "dune (build|runtest|exec) ([^ ]+ )*-j $expected( |\$)" \
    "$temporary_root/dune" >"$temporary_root/unbounded"; then
    echo "$label: Dune commands without -j $expected:" >&2
    cat "$temporary_root/unbounded" >&2
    exit 1
  fi
  grep -Fq "DUNE_JOBS=\"$expected\"" "$temporary_root/plan" || {
    echo "$label: executable builds do not receive DUNE_JOBS=$expected" >&2
    exit 1
  }
}

# A local run gets a bounded default; GitHub Actions keeps Dune's own count;
# an explicit developer value wins in both environments.
CI= dry_run
expect_jobs 'local default' 2
CI=true dry_run
expect_jobs 'CI default' auto
CI= dry_run DUNE_JOBS=1
expect_jobs 'explicit local limit' 1
CI=true dry_run DUNE_JOBS=3
expect_jobs 'explicit CI limit' 3

# Cargo's test-only serial default must not slow the bridge build, while an
# explicit CARGO_BUILD_JOBS reaches the Cargo build that Dune's bridge rule and
# `make build` start inside the container.
CI= dry_run
if grep -E 'dune (build|runtest)|cargo build' "$temporary_root/plan" |
  grep -Fq CARGO_BUILD_JOBS; then
  echo 'default bridge builds were serialized by CARGO_BUILD_JOBS' >&2
  exit 1
fi
CI= dry_run CARGO_BUILD_JOBS=3
for command in 'opam exec -- dune build -j 2' 'opam exec -- dune runtest -j 2' 'cargo build --manifest-path'; do
  grep -F "$command" "$temporary_root/plan" | grep -Fq 'env CARGO_BUILD_JOBS=3' || {
    echo "explicit CARGO_BUILD_JOBS did not reach: $command" >&2
    exit 1
  }
done

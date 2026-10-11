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

# Dune processes that Make does not start itself (the nested builds in the
# install, prebuilt-consumer and vendored-build scripts, and long-lived Compose
# workers' `dune exec`) read DUNE_JOBS from their environment. A probe target
# appended to the Makefile prints the value its recipe receives; Dune rejects
# an empty value, so an explicitly empty limit must arrive unset.
cat >"$temporary_root/probe.mk" <<'PROBE'
.PHONY: print-dune-jobs-environment
print-dune-jobs-environment:
	@printf '%s\n' "$${DUNE_JOBS-unset}"
PROBE

# Runs the probe recipe with CI set to the third argument and any remaining
# arguments as Make command-line variables, and fails unless the recipe's
# DUNE_JOBS matches the expected value.
expect_exported() {
  label=$1
  expected=$2
  ci_value=$3
  shift 3
  actual=$(CI="$ci_value" make --no-print-directory -s -C "$source_root" \
    -f Makefile -f "$temporary_root/probe.mk" "$@" \
    print-dune-jobs-environment) || {
    echo "$label: probe recipe failed" >&2
    exit 1
  }
  if [ "$actual" != "$expected" ]; then
    echo "$label: recipes receive DUNE_JOBS=$actual, expected $expected" >&2
    exit 1
  fi
}
expect_exported 'exported local default' 2 ''
expect_exported 'exported CI default' auto true
expect_exported 'exported command-line limit' 1 '' DUNE_JOBS=1
expect_exported 'empty command-line limit' unset '' DUNE_JOBS=
expect_exported 'empty CI limit' unset true DUNE_JOBS=
# Subshells keep these environment values away from the later probes.
(DUNE_JOBS='' && export DUNE_JOBS && expect_exported 'empty environment limit' unset '')
(DUNE_JOBS=3 && export DUNE_JOBS && expect_exported 'environment limit' 3 '')

# Compose copies the exported value into every service that extends dev, which
# includes `compose run` containers and the long-lived workers. The key must
# have no value: an interpolated default would hand Dune an empty string.
compose_file=$source_root/test/integration/temporal/compose.yaml
tr -d '\015' <"$compose_file" | awk '
  /^  [[:alnum:]_-]+:/ { in_dev = ($1 == "dev:") }
  in_dev && /^      DUNE_JOBS:[[:space:]]*$/ { found = 1 }
  END { exit !found }
' || {
  echo "$compose_file: dev service does not pass DUNE_JOBS through" >&2
  exit 1
}

# Dune reads DUNE_JOBS from its environment only from 3.22, while the SDK
# supports 3.18, so each script that starts Dune itself must also turn a
# non-empty DUNE_JOBS into an explicit `-j`. Stub `opam` and `dune` commands
# record the Dune arguments without building anything.
stub_bin=$temporary_root/stub-bin
mkdir -p "$stub_bin"
cat >"$stub_bin/opam" <<'STUB'
#!/bin/sh
# Stub `opam exec -- COMMAND...`: runs COMMAND from the stub PATH.
[ "$1" = exec ] && [ "$2" = -- ] || { echo "unexpected opam call: $*" >&2; exit 2; }
shift 2
exec "$@"
STUB
cat >"$stub_bin/dune" <<'STUB'
#!/bin/sh
# Stub Dune: appends its argument list to DUNE_STUB_LOG.
printf '%s\n' "$*" >>"$DUNE_STUB_LOG"
STUB
chmod +x "$stub_bin/opam" "$stub_bin/dune"
dune_log=$temporary_root/dune-arguments

# Fails unless the stub Dune recorded exactly the expected argument line.
expect_dune_arguments() {
  label=$1
  expected=$2
  actual=$(cat "$dune_log")
  if [ "$actual" != "$expected" ]; then
    printf '%s: Dune received "%s", expected "%s"\n' "$label" "$actual" "$expected" >&2
    exit 1
  fi
  rm -f "$dune_log"
}

for script in test/bridge/test_install.sh test/bridge/test_prebuilt_library.sh \
  test/smoke/test_vendored_build.sh; do
  # Every Dune start in the script goes through its opam_dune helper.
  if tr -d '\015' <"$source_root/$script" | grep -v '^[[:space:]]*#' |
    grep -E '(^|[^_[:alnum:]])dune (build|runtest|test|exec)([^-[:alnum:]]|$)' >&2; then
    echo "$script: Dune started without the opam_dune job-limit helper" >&2
    exit 1
  fi
  grep -q 'opam_dune build' "$source_root/$script" || {
    echo "$script: no Dune build goes through opam_dune" >&2
    exit 1
  }
  tr -d '\015' <"$source_root/$script" |
    awk '/^opam_dune\(\) \{/ { copy = 1 } copy { print } copy && /^}/ { exit }' \
      >"$temporary_root/helper.sh"
  grep -q '^}' "$temporary_root/helper.sh" || {
    echo "$script: opam_dune helper not found" >&2
    exit 1
  }
  (
    PATH=$stub_bin:$PATH DUNE_STUB_LOG=$dune_log DUNE_JOBS=3
    export PATH DUNE_STUB_LOG DUNE_JOBS
    . "$temporary_root/helper.sh"
    opam_dune build --root . ./main.exe
  )
  expect_dune_arguments "$script with DUNE_JOBS" 'build -j 3 --root . ./main.exe'
  (
    PATH=$stub_bin:$PATH DUNE_STUB_LOG=$dune_log
    export PATH DUNE_STUB_LOG
    unset DUNE_JOBS
    . "$temporary_root/helper.sh"
    opam_dune build --root . ./main.exe
  )
  expect_dune_arguments "$script without DUNE_JOBS" 'build --root . ./main.exe'
done

# The long-lived Compose workers start through run-temporal-executable.sh,
# which is short enough to run unchanged against the stubs.
for jobs in 3 ''; do
  (
    PATH=$stub_bin:$PATH DUNE_STUB_LOG=$dune_log DUNE_JOBS=$jobs
    export PATH DUNE_STUB_LOG DUNE_JOBS
    unset TEMPORAL_PREBUILT_SMOKE
    sh "$source_root/scripts/run-temporal-executable.sh" --build-dir=/tmp/worker \
      test/integration/temporal/worker/smoke_worker.exe check
  )
done
expect_dune_arguments 'run-temporal-executable.sh' "$(printf '%s\n%s' \
  'exec -j 3 --build-dir=/tmp/worker test/integration/temporal/worker/smoke_worker.exe check' \
  'exec --build-dir=/tmp/worker test/integration/temporal/worker/smoke_worker.exe check')"

# Cargo's test-only serial default must not slow the bridge build, while an
# explicit CARGO_BUILD_JOBS is passed into every Compose container, so it
# reaches the Cargo build that Dune's bridge rule, `make build`, executable
# batches, and benchmarks start inside the container.
CI= dry_run
if grep -E 'dune (build|runtest)|cargo build|build-temporal-executables' "$temporary_root/plan" |
  grep -Fq CARGO_BUILD_JOBS; then
  echo 'default bridge builds were serialized by CARGO_BUILD_JOBS' >&2
  exit 1
fi
CI= dry_run CARGO_BUILD_JOBS=3
for command in 'opam exec -- dune build -j 2' 'opam exec -- dune runtest -j 2' \
  'cargo build --manifest-path' 'build-temporal-executables.sh'; do
  # Native targets run on the host, where Make already exports a command-line
  # or environment value to recipes; only container commands need the flag.
  grep -F "$command" "$temporary_root/plan" | grep -F ' run --rm ' \
    >"$temporary_root/matches" || {
    echo "dry run printed no container command matching: $command" >&2
    exit 1
  }
  if grep -Fv -- '-e CARGO_BUILD_JOBS=3' "$temporary_root/matches" | grep -q .; then
    echo "explicit CARGO_BUILD_JOBS did not reach: $command" >&2
    exit 1
  fi
done
# Every container started through COMPOSE_RUN, including direct Dune paths
# such as `make bench`, carries the explicit value.
make --no-print-directory -n -C "$source_root" -f Makefile MAKE=true CARGO_BUILD_JOBS=3 bench \
  >"$temporary_root/bench" 2>&1 || { cat "$temporary_root/bench" >&2; exit 1; }
if grep -F ' run --rm ' "$temporary_root/bench" | grep -Fv -- '-e CARGO_BUILD_JOBS=3' | grep -q .; then
  echo 'explicit CARGO_BUILD_JOBS did not reach every bench container' >&2
  exit 1
fi

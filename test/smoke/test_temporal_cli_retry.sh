#!/bin/sh
set -eu

# Exercises temporal_cli_read_only (#1285) with a fake CLI so the bounded
# retry of read-only admin queries is checked without Docker or a server.
root=${1:-$(CDPATH='' cd -- "$(dirname "$0")/../.." && pwd)}
. "$root/test/integration/temporal/scripts/temporal-cli-retry.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
trap 'exit 1' HUP INT TERM

deadline='Error: failed connecting to Temporal server at temporal:7233: context deadline exceeded'
TEMPORAL_CLI_RETRY_SLEEP_SECONDS=0
export TEMPORAL_CLI_RETRY_SLEEP_SECONDS

fail() {
  echo "temporal CLI retry test: $*" >&2
  exit 1
}

# fake_cli FAILURES MESSAGE: a shell function standing in for the Compose
# wrapper. It fails FAILURES times with MESSAGE on stderr and "partial" on
# stdout, then prints the final JSON. The count lives in a file because
# temporal_cli_read_only runs the command with redirected output.
fake_cli() {
  count=$(($(cat "$work/count") + 1))
  printf '%s\n' "$count" > "$work/count"
  if [ "$count" -le "$1" ]; then
    echo partial
    printf '%s\n' "$2" >&2
    return 3
  fi
  echo '{"ok":true}'
}

# run_case FAILURES MESSAGE: resets the counter and runs one wrapped call,
# leaving stdout, stderr and the exit status in $work.
run_case() {
  echo 0 > "$work/count"
  case_status=0
  temporal_cli_read_only fake_cli "$1" "$2" >"$work/out" 2>"$work/err" || case_status=$?
}

expect_calls() {
  [ "$(cat "$work/count")" -eq "$1" ] || fail "$2: expected $1 calls, got $(cat "$work/count")"
}

# A transport failure is retried, and only the final stdout is written.
run_case 2 'rpc error: code = Unavailable desc = frontend restarting'
[ "$case_status" -eq 0 ] || fail "transient: status $case_status"
expect_calls 3 transient
[ "$(cat "$work/out")" = '{"ok":true}' ] || fail "transient: stdout carried a failed attempt"
grep -q 'attempt 2 of 5' "$work/err" || fail "transient: no retry note"

# The CLI deadline still counts after Compose progress lines and with the
# diagnostics that current CLI releases append.
run_case 1 "$(printf ' Container admin-tools-run Creating\n%s\n  Connection checks for temporal:7233\n' "$deadline")"
[ "$case_status" -eq 0 ] || fail "deadline: status $case_status"
expect_calls 2 deadline

# Non-transport failures return at once with their status and stderr intact.
for message in 'rpc error: code = NotFound desc = workflow execution not found' \
  "$(printf 'Error: authentication failed: invalid API key\n%s' "$deadline")"; do
  run_case 1 "$message"
  [ "$case_status" -eq 3 ] || fail "fatal: status $case_status"
  expect_calls 1 fatal
  [ "$(cat "$work/out")" = partial ] || fail "fatal: stdout not passed through"
  grep -q 'code = NotFound\|authentication failed' "$work/err" || fail "fatal: stderr lost"
  if grep -q 'retrying' "$work/err"; then fail "fatal: retried"; fi
done

# A persistent transport failure stops after the configured bound.
TEMPORAL_CLI_RETRY_ATTEMPTS=3
export TEMPORAL_CLI_RETRY_ATTEMPTS
run_case 99 "$deadline"
[ "$case_status" -eq 3 ] || fail "bounded: status $case_status"
expect_calls 3 bounded
[ "$(cat "$work/out")" = partial ] || fail "bounded: final stdout missing"
[ "$(grep -c 'retrying' "$work/err")" -eq 2 ] || fail "bounded: expected two retry notes"
grep -qF "$deadline" "$work/err" || fail "bounded: final diagnostic lost"
unset TEMPORAL_CLI_RETRY_ATTEMPTS

# The library must not overwrite the caller's ordinary variable names.
attempt=caller status=caller first_line=caller
run_case 1 "$deadline"
[ "$attempt$status$first_line" = callercallercaller ] || fail "caller variables were changed"

# Invalid settings and a missing command are usage errors.
for setting in TEMPORAL_CLI_RETRY_ATTEMPTS=0 TEMPORAL_CLI_RETRY_ATTEMPTS=x \
  TEMPORAL_CLI_RETRY_SLEEP_SECONDS=-1; do
  usage_status=0
  env "$setting" sh -c '. "$1"; temporal_cli_read_only true' sh \
    "$root/test/integration/temporal/scripts/temporal-cli-retry.sh" 2>/dev/null || usage_status=$?
  [ "$usage_status" -eq 2 ] || fail "$setting: status $usage_status"
done
usage_status=0
temporal_cli_read_only 2>/dev/null || usage_status=$?
[ "$usage_status" -eq 2 ] || fail "missing command: status $usage_status"

echo 'Temporal CLI retry tests: ok'

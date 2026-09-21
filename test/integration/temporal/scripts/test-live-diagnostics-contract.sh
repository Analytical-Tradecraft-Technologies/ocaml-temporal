#!/usr/bin/env bash
set -eu

# Exercise the actual wrapper/collector in an isolated copy with a Docker
# protocol fixture. No daemon, SDK build or credentials are required; passing,
# failing and deadline-killed controllers all destroy their original files.
root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
fixture="$temporary/repository/test/integration/temporal"
scripts="$fixture/scripts"
mkdir -p "$scripts" "$temporary/bin" "$temporary/repository/rust"
for file in collect-live-diagnostics.sh diagnostic-filter.jq record-live-log.sh run-with-live-diagnostics.sh summarize-live-diagnostics.sh; do
  cp "$root/test/integration/temporal/scripts/$file" "$scripts/$file"
done
cp "$root/rust/Cargo.lock" "$temporary/repository/rust/Cargo.lock"
cp "$root/test/integration/temporal/compose.yaml" "$fixture/compose.yaml"
cp -R "$root/test/integration/temporal/fixtures/restart-replay" "$temporary/evidence"

# This stub asserts scope and emits secret canaries exactly as container logs
# could. Docker failures and slow reads are explicit, independently exercised
# protocol cases; they may not affect the controller's result.
cat >"$temporary/bin/docker" <<'EOF'
#!/bin/sh
case "$*" in
  *'ps --all'*)
    case "$*" in *'label=com.docker.compose.project=diagnostic-contract'*) ;; *) exit 99 ;; esac
    printf 'abcdef\tworker\tfixture-image\tUp 2 seconds\n' ;;
  *'logs --no-color'*)
    [ "${DIAGNOSTIC_TEST_DOCKER_FAILURE:-}" != yes ] || exit 42
    if [ "${DIAGNOSTIC_TEST_DOCKER_SLEEP:-}" = yes ]; then sleep 30; fi
    printf '2026-09-20T00:00:00Z worker ready\npassword=PASSWORD_CANARY\nAuthorization: Bearer BEARER_CANARY\npostgres://alice:URL_CANARY@db\n'
    printf '2026-09-20T00:00:01Z driver waiting %s\n' "$DIAGNOSTIC_TEST_SECRET"
    printf '%s\n' '-----BEGIN PRIVATE KEY-----' 'AABBCCDDEEFFAABBCCDDEEFFAABBCCDDEEFF' '-----END PRIVATE KEY-----' ;;
  *'config --images'*) printf 'temporalio/server:1.32.0\npostgres:18.6\n' ;;
  'version '*) printf 'fixture-client / fixture-server\n' ;;
  'top '*) printf 'PID PPID STAT COMMAND\n12 1 Sl smoke_worker\n' ;;
  'inspect '*) printf 'abcdef running=true exit_code=0 oom_killed=false\n' ;;
  *) exit 98 ;;
esac
EOF
chmod +x "$temporary/bin/docker"
export PATH="$temporary/bin:$PATH"
export TEMPORAL_COMPOSE_PROJECT=diagnostic-contract
export DIAGNOSTIC_TEST_SECRET=ENV_CANARY_unsafe_value
export DIAGNOSTIC_TEST_FIXTURE="$fixture"
export DIAGNOSTIC_TEST_EVIDENCE="$temporary/evidence"
export TEMPORAL_DIAGNOSTICS_SCENARIO=restart

cat >"$temporary/controller.sh" <<'EOF'
#!/bin/sh
set -eu
fixture=$DIAGNOSTIC_TEST_FIXTURE
# Preserve the original failure status while proving files survive deletion.
cleanup() {
  status=$?
  trap - EXIT
  sh "$fixture/scripts/collect-live-diagnostics.sh" cleanup
  rm -f "$fixture"/.restart-replay-*
  exit "$status"
}
trap cleanup EXIT
cp "$DIAGNOSTIC_TEST_EVIDENCE/controller.json" "$fixture/.restart-replay-controller.json"
cp "$DIAGNOSTIC_TEST_EVIDENCE/history.terminal.json" "$fixture/.restart-replay-history.terminal.json"
cp "$DIAGNOSTIC_TEST_EVIDENCE/diagnostics.json" "$fixture/.restart-replay-diagnostics.json"
if [ "$1" != pass ]; then
  # A failed or killed producer never fabricates a successful terminal event.
  jq '.events = .events[:3]' "$fixture/.restart-replay-controller.json" >"$fixture/temp"
  mv "$fixture/temp" "$fixture/.restart-replay-controller.json"
  rm -f "$fixture/.restart-replay-history.terminal.json"
  cp "$DIAGNOSTIC_TEST_EVIDENCE/history.initial.json" "$fixture/.restart-replay-history.initial.json"
fi
jq '.payload="PAYLOAD_CANARY" | .events[0].credentials="NESTED_CANARY"' "$fixture/.restart-replay-controller.json" >"$fixture/temp"
mv "$fixture/temp" "$fixture/.restart-replay-controller.json"
printf 'RAW_CANARY\n' >"$fixture/.restart-replay-history.terminal.json.raw"
printf 'DESCRIBE_CANARY\n' >"$fixture/.restart-replay-history.terminal.json.describe.json"
printf 'NEIGHBOUR_CANARY\n' >"$fixture/.restart-replay-unexpected.json"
printf 'phase=fixture_started\n'
printf 'partial driver output before controller exit\n' >"$fixture/.restart-replay-driver.log"
case "$1" in
  pass) printf 'phase=fixture_complete\n' ;;
  fail) exit 23 ;;
  timeout)
    # This is a real killed producer, not a forged timeout exit: the controller
    # retains partial output after its one-second watchdog stops the wait.
    sleep 30 & producer=$!
    (sleep 1; kill "$producer") & watchdog=$!
    wait "$producer" 2>/dev/null || true
    wait "$watchdog"
    exit 124 ;;
esac
EOF

# Returns an assertion failure with a useful name rather than leaking fixture
# content into CI when the privacy boundary regresses.
check() {
  if ! "$@"; then echo "live diagnostic contract failed: $*" >&2; exit 1; fi
}

for outcome in pass fail timeout; do
  export TEMPORAL_DIAGNOSTICS_DIR="$temporary/bundles/$outcome"
  status=0
  bash "$scripts/run-with-live-diagnostics.sh" restart sh "$temporary/controller.sh" "$outcome" >"$temporary/output" 2>&1 || status=$?
  case "$outcome" in pass) expected=0 ;; fail) expected=23 ;; timeout) expected=124 ;; esac
  check test "$status" -eq "$expected"
  check test ! -e "$fixture/.restart-replay-controller.json"
  bundle="$TEMPORAL_DIAGNOSTICS_DIR/restart"
  # jq variable interpolation is intentionally performed by jq, not Bash.
  # shellcheck disable=SC2016
  check jq -e --argjson expected "$expected" '.exit_code == $expected' "$bundle/result.json"
  check jq -e '.workflow_id == "two-binary-worker-restart-replay" and .run_id != "" and (has("payload") | not)' "$bundle/snapshot-00-cleanup/restart-replay-controller.json"
  if [ "$outcome" = pass ]; then
    check jq -e 'any(.events[]; .step == "driver_completed" and .outcome == "completed")' "$bundle/snapshot-00-cleanup/restart-replay-controller.json"
  else
    check jq -e 'all(.events[]; .step != "driver_completed")' "$bundle/snapshot-00-cleanup/restart-replay-controller.json"
    check test ! -e "$bundle/snapshot-00-cleanup/restart-replay-history.terminal.json"
  fi
  check grep -q 'partial driver output' "$bundle/snapshot-00-cleanup/restart-replay-driver.log"
  # Console recording is concurrent with the controller. It is complete after
  # the wrapper reaps the pipeline, not necessarily at the cleanup snapshot.
  check grep -q 'fixture_started' "$bundle/controller.log"
  if grep -ERq 'PASSWORD_CANARY|BEARER_CANARY|URL_CANARY|ENV_CANARY|PAYLOAD_CANARY|NESTED_CANARY|RAW_CANARY|DESCRIBE_CANARY|NEIGHBOUR_CANARY|AABBCCDDEEFF' "$bundle"; then
    echo 'sensitive content crossed the publication boundary' >&2; exit 1
  fi
done

# Killing the wrapper itself cannot run EXIT. Its already-sanitized rolling
# log must be inside the upload tree before any snapshot/final result exists.
export TEMPORAL_DIAGNOSTICS_DIR="$temporary/bundles/killed-wrapper"
bash "$scripts/run-with-live-diagnostics.sh" restart sh -c '
  printf "hard-kill checkpoint\noutput = KILLED_CANARY\nUNLABELLED_KILLED_CANARY\n"
  sleep 5
' >"$temporary/killed-output" 2>&1 &
wrapper_pid=$!
bundle="$TEMPORAL_DIAGNOSTICS_DIR/restart"
for _attempt in $(seq 1 100); do
  if grep -q 'redacted sensitive record' "$bundle/controller.log" 2>/dev/null; then break; fi
  sleep 0.02
done
check grep -q 'hard-kill checkpoint' "$bundle/controller.log"
kill -KILL "$wrapper_pid"
status=0
wait "$wrapper_pid" 2>/dev/null || status=$?
check test "$status" -eq 137
check test ! -e "$bundle/result.json"
check grep -q 'hard-kill checkpoint' "$bundle/controller.log"
if grep -q CANARY "$bundle/controller.log"; then exit 1; fi
GITHUB_STEP_SUMMARY="$temporary/killed-summary.md" LIVE_ARTIFACT_URL=https://example.invalid/diagnostic-artifact bash "$scripts/summarize-live-diagnostics.sh"
check grep -q '| restart | unfinished; no final result | unknown |' "$temporary/killed-summary.md"
check grep -q 'https://example.invalid/diagnostic-artifact' "$temporary/killed-summary.md"

# An instrumentation failure must drain the pipe so a controller writing more
# than a pipe buffer can finish and retain its own exit status without SIGPIPE.
mkdir "$temporary/failure-bin"
export DIAGNOSTIC_TEST_REAL_JQ
DIAGNOSTIC_TEST_REAL_JQ=$(command -v jq)
cat >"$temporary/failure-bin/jq" <<'EOF'
#!/bin/sh
case "$*" in *'--arg mode stream'*) exit 42 ;; esac
exec "$DIAGNOSTIC_TEST_REAL_JQ" "$@"
EOF
chmod +x "$temporary/failure-bin/jq"
export TEMPORAL_DIAGNOSTICS_DIR="$temporary/filter-failure"
status=0
PATH="$temporary/failure-bin:$PATH" bash "$scripts/run-with-live-diagnostics.sh" restart sh -c '
  i=0
  while [ "$i" -lt 10000 ]; do printf "controller still running\n"; i=$((i + 1)); done
  exit 23
' >"$temporary/filter-output" 2>&1 || status=$?
check test "$status" -eq 23
check grep -q 'console filtering failed' "$TEMPORAL_DIAGNOSTICS_DIR/restart/collection-error.txt"
check jq -e '.exit_code == 23' "$TEMPORAL_DIAGNOSTICS_DIR/restart/result.json"

# Corrupt/oversized JSON and long log tails exercise the byte/record limits,
# including a sensitive line crossing the tail boundary and symlink refusal.
export TEMPORAL_DIAGNOSTICS_DIR="$temporary/limits"
awk 'BEGIN { for (i=0;i<10000;i++) print "bounded log line" }' >"$fixture/.restart-replay-driver.log"
printf '\npassword=' >>"$fixture/.restart-replay-driver.log"
awk 'BEGIN { for (i=0;i<70000;i++) printf "Z"; print "\nlast safe line" }' >>"$fixture/.restart-replay-driver.log"
printf '{"events":[' >"$fixture/.restart-replay-controller.json"
cp "$fixture/.restart-replay-driver.log" "$fixture/.restart-replay-history.initial.json"
ln -s "$temporary/evidence/diagnostics.json" "$fixture/.restart-replay-diagnostics.json"
sh "$scripts/collect-live-diagnostics.sh" limits
snapshot="$TEMPORAL_DIAGNOSTICS_DIR/restart/snapshot-00-limits"
check test ! -f "$snapshot/restart-replay-controller.json"
check test ! -f "$snapshot/restart-replay-history.initial.json"
check test ! -f "$snapshot/restart-replay-diagnostics.json"
check grep -q omitted-invalid-or-oversize-json "$snapshot/collection.tsv"
check grep -q 'redacted sensitive record' "$snapshot/restart-replay-driver.log"
if grep -q ZZZZZ "$snapshot/restart-replay-driver.log"; then exit 1; fi
for file in "$snapshot"/*; do check test "$(wc -c <"$file")" -le 65536; done
rm -f "$fixture"/.restart-replay-*

# Quoted/spaced fields and multiline continuations do not have to repeat a
# sensitive key. Each case must suppress all subsequent unlabelled content.
for sensitive in '"input": "QUOTED_CANARY"' 'result = SPACED_CANARY' '"output" : "OUTPUT_CANARY"' '-----BEGIN CERTIFICATE-----'; do
  printf 'safe prefix\n%s\nUNLABELLED_CANARY\n' "$sensitive" >"$temporary/privacy.log"
  jq -Rnr --arg mode text -f "$scripts/diagnostic-filter.jq" "$temporary/privacy.log" >"$temporary/privacy.filtered"
  check grep -q 'safe prefix' "$temporary/privacy.filtered"
  if grep -Eq 'CANARY|CERTIFICATE' "$temporary/privacy.filtered"; then exit 1; fi
done
# Projection preserves the existing normalizers' actual correlation fields.
for normalized in "$root/test/integration/temporal/fixtures/restart-replay/history.terminal.json" "$root/test/integration/temporal/fixtures/parent-child-restart-replay/parent.history.terminal.json" "$root/test/integration/temporal/fixtures/parent-child-restart-replay/child.history.terminal.json"; do
  jq -c --arg mode json -f "$scripts/diagnostic-filter.jq" "$normalized" >"$temporary/projected.json"
  original_ids=$(jq -c '[.events[].event_id]' "$normalized")
  projected_ids=$(jq -c '[.events[].event_id]' "$temporary/projected.json")
  check test "$original_ids" = "$projected_ids"
done
printf '%s\n' '{"events":[{"event_id":"17","parent_initiated_event_id":"11"}]}' | jq -c --arg mode json -f "$scripts/diagnostic-filter.jq" >"$temporary/projected.json"
check jq -e '.events[0].parent_initiated_event_id == "11"' "$temporary/projected.json"

# Driver phases remain usable after free-text suppression, but only complete
# closed machine records qualify. Extra payload fields invalidate a line.
cat >"$fixture/.restart-replay-driver.log" <<'EOF'
output = PHASE_CANARY
UNLABELLED_PHASE_CANARY
two-binary phase=wait:two-binary-smoke status=completed workflow_id=two-binary-smoke run_id=01234567-89ab-cdef-0123-456789abcdef duration_ms=12.3
cache eviction phase=wait_b status=cancelled workflow_id=two-binary-cache-b run_id=01234567-89ab-cdef-0123-456789abcdef
two-binary phase=wait:two-binary-smoke status=completed workflow_id=two-binary-smoke run_id=01234567-89ab-cdef-0123-456789abcdef output=PHASE_CANARY
EOF
sh "$scripts/collect-live-diagnostics.sh" phases
phase_snapshot="$TEMPORAL_DIAGNOSTICS_DIR/restart/snapshot-01-phases"
check jq -e '(.events | length) == 2 and .events[0].status == "completed" and .events[1].status == "cancelled" and (.truncated | not)' "$phase_snapshot/restart-replay-driver.log.executions.json"
if grep -Rq CANARY "$phase_snapshot"; then exit 1; fi
jq -n '{events:[range(257) | {event_id:.}]}' >"$fixture/.restart-replay-controller.json"
sh "$scripts/collect-live-diagnostics.sh" record-limit
check test ! -e "$TEMPORAL_DIAGNOSTICS_DIR/restart/snapshot-02-record-limit/restart-replay-controller.json"
check grep -q omitted-invalid-or-oversize-json "$TEMPORAL_DIAGNOSTICS_DIR/restart/snapshot-02-record-limit/collection.tsv"
rm -f "$fixture"/.restart-replay-*

# A Docker command failure is diagnostic metadata, not a scenario failure.
export DIAGNOSTIC_TEST_DOCKER_FAILURE=yes
sh "$scripts/collect-live-diagnostics.sh" docker-failure
unset DIAGNOSTIC_TEST_DOCKER_FAILURE
check grep -q command-exit-42 "$TEMPORAL_DIAGNOSTICS_DIR/restart/snapshot-03-docker-failure/collection.tsv"
export DIAGNOSTIC_TEST_DOCKER_SLEEP=yes
before=$SECONDS
sh "$scripts/collect-live-diagnostics.sh" docker-timeout
check test "$((SECONDS - before))" -lt 12
unset DIAGNOSTIC_TEST_DOCKER_SLEEP
check grep -q command-exit-137 "$TEMPORAL_DIAGNOSTICS_DIR/restart/snapshot-04-docker-timeout/collection.tsv"
for _iteration in $(seq 1 12); do sh "$scripts/collect-live-diagnostics.sh" cap; done
check test "$(find "$TEMPORAL_DIAGNOSTICS_DIR/restart" -maxdepth 1 -type d -name 'snapshot-*' | wc -l)" -eq 12
check test -f "$TEMPORAL_DIAGNOSTICS_DIR/restart/collection-limit.txt"

# Keep the useful, sanitized regression artifacts in the CI upload so the
# failing/deadline-killed producer evidence is inspectable after cleanup.
if [ -n "${GITHUB_ACTIONS:-}" ]; then
  mkdir -p "$root/_build/live-diagnostics/collector-contract"
  cp -R "$temporary/bundles/." "$root/_build/live-diagnostics/collector-contract/"
fi
echo 'live diagnostics: passing, failing, timeout, redaction and bounds contracts passed'

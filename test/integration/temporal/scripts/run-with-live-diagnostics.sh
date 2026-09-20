#!/usr/bin/env bash
set -eu

# Wrap one synthetic scenario without replacing its exit status. The live
# controller snapshots before destructive cleanup; this outer wrapper retains
# bounded console output and a final process outcome even if cleanup fails.
root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
scripts="$root/test/integration/temporal/scripts"
export TEMPORAL_DIAGNOSTICS_SCENARIO=${1:?expected scenario then command}
shift
case "$TEMPORAL_DIAGNOSTICS_SCENARIO" in
  integration|restart|crash|cache-eviction|patching|parent-child-restart|child-failure-replay) ;;
  *) echo 'unsupported synthetic diagnostic scenario' >&2; exit 2 ;;
esac
export TEMPORAL_DIAGNOSTICS_DIR=${TEMPORAL_DIAGNOSTICS_DIR:-$root/_build/live-diagnostics}
scenario="$TEMPORAL_DIAGNOSTICS_DIR/$TEMPORAL_DIAGNOSTICS_SCENARIO"
mkdir -p "$scenario"
# The rolling console is filtered before truncation, so a partial multiline
# payload cannot lose its sensitive key at the beginning of the window. Keep
# it in the upload tree from the first record, even if EXIT never gets to run.
export TEMPORAL_DIAGNOSTICS_CONTROLLER_LOG
TEMPORAL_DIAGNOSTICS_CONTROLLER_LOG="$scenario/controller.log"
: >"$TEMPORAL_DIAGNOSTICS_CONTROLLER_LOG"
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# EXIT is the only finalizer; signal traps preserve conventional shell status.
# Collection errors are visible in a separate file and never mask a failure.
# shellcheck disable=SC2329
finish() {
  status=$?
  trap - EXIT HUP INT TERM
  sh "$scripts/collect-live-diagnostics.sh" finished || printf '%s\n' 'final diagnostic collection failed' >"$scenario/collection-error.txt"
  case "$status" in
    0) outcome=passed ;;
    124|137) outcome=timed_out_or_killed ;;
    129|130|143) outcome=cancelled ;;
    *) outcome=failed ;;
  esac
  jq -n --arg scenario "$TEMPORAL_DIAGNOSTICS_SCENARIO" --arg started "$started" --arg ended "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg outcome "$outcome" --argjson exit_code "$status" '{schema_version:1,scenario:$scenario,started_at:$started,ended_at:$ended,process_outcome:$outcome,exit_code:$exit_code,durable_outcomes:"see preserved controller/history documents and driver phases; a process exit alone does not prove completion"}' >"$scenario/result.json" || true
  exit "$status"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
set +e
"$@" 2>&1 | {
  jq --unbuffered -Rnr --arg mode stream -f "$scripts/diagnostic-filter.jq" || {
    printf '%s\n' 'console filtering failed' >"$scenario/collection-error.txt"
    # Keep the read side alive if instrumentation fails: SIGPIPE must not
    # abort the controller or replace its independently obtained exit status.
    cat >/dev/null
  }
} | {
  LC_ALL=C bash "$scripts/record-live-log.sh" "$TEMPORAL_DIAGNOSTICS_CONTROLLER_LOG" || {
    printf '%s\n' 'console recording failed' >"$scenario/collection-error.txt"
    cat >/dev/null
  }
}
status=${PIPESTATUS[0]}
exit "$status"

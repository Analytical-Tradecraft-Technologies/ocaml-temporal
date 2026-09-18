#!/bin/sh
# Exercise the real process wrapper and marker wait with an absent marker. A
# reset/full internal budget would be killed with 124 before printing its error.
set -eu
wrapper=$1
executable=$2
log=$(mktemp)
trap 'rm -f "$log"' EXIT HUP INT TERM
status=0
SMOKE_CACHE_EVICTION_TIMEOUT_SECONDS=2 sh "$wrapper" "$executable" --watchdog-child >"$log" 2>&1 || status=$?
cat "$log"
[ "$status" -eq 1 ] || { echo "expected marker failure before watchdog, got $status" >&2; exit 1; }
grep -q 'marker diagnostic:' "$log"
# Invalid budgets must be rejected rather than disabling the watchdog.
for budget in 0 1 08 nan inf -2 999999999999999999999999; do
  status=0
  SMOKE_CACHE_EVICTION_TIMEOUT_SECONDS=$budget sh "$wrapper" "$executable" --watchdog-child >"$log" 2>&1 || status=$?
  [ "$status" -eq 2 ] || { echo "invalid budget $budget returned $status" >&2; exit 1; }
done

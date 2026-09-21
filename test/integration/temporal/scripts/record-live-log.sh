#!/usr/bin/env bash
set -eu

# Relay already-filtered controller output and rewrite the last 64 lines after
# each record, capped to 768 bytes each (the caller fixes LC_ALL=C). Bash reads
# each line immediately; some awk implementations buffer pipe input even when
# their output is flushed, losing the current tail when a wrapper is killed.
destination=${1:?expected rolling log path}
lines=()
while IFS= read -r line || [ -n "$line" ]; do
  printf '%s\n' "$line"
  lines+=("${line:0:768}")
  if [ "${#lines[@]}" -gt 64 ]; then lines=("${lines[@]:1}"); fi
  printf '%s\n' "${lines[@]}" >"$destination"
done

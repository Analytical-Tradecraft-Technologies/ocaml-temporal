#!/usr/bin/env bash
set -eu

# The Actions summary links the immutable upload and reports collection
# independently from acceptance. Only closed result/manifest fields are read;
# fixture logs never become executable Markdown or workflow commands.
root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
output=${GITHUB_STEP_SUMMARY:-/dev/stdout}
directory=${TEMPORAL_DIAGNOSTICS_DIR:-$root/_build/live-diagnostics}
{
  printf '### Live diagnostic evidence\n\n'
  if [[ -n "${LIVE_ARTIFACT_URL:-}" ]]; then
    printf '[Download diagnostic bundle](%s) (7-day retention).\n\n' "$LIVE_ARTIFACT_URL"
  else
    printf 'No downloadable artifact URL was returned; upload outcome: %s.\n\n' "${LIVE_UPLOAD_OUTCOME:-unavailable}"
  fi
  printf '| Scenario | Process outcome | Collection warnings |\n|---|---|---|\n'
  shopt -s nullglob
  for scenario in integration restart crash cache-eviction patching parent-child-restart child-failure-replay; do
    scenario_dir="$directory/$scenario"
    [ -d "$scenario_dir" ] || continue
    manifests=("$scenario_dir"/snapshot-*/manifest.json)
    warnings=unknown
    if [ "${#manifests[@]}" -gt 0 ]; then
      warnings=$(jq -s '[.[].collection_warnings] | add // 0' "${manifests[@]}" 2>/dev/null || printf unknown)
    fi
    # A killed wrapper may leave only its rolling log. Show the attempted
    # scenario explicitly rather than dropping it from the status table.
    if [ -s "$scenario_dir/result.json" ]; then
      jq -r --arg warnings "$warnings" '[.scenario,.process_outcome,$warnings] | "| " + join(" | ") + " |"' "$scenario_dir/result.json" 2>/dev/null || printf '| %s | invalid final result | %s |\n' "$scenario" "$warnings"
    else
      printf '| %s | unfinished; no final result | %s |\n' "$scenario" "$warnings"
    fi
  done
  printf '\nProcess outcomes are separate from durable workflow results. Inspect the retained controller, normalized histories and exact-run driver phases. Missing snapshots or collection-error/collection-limit files mean evidence is incomplete.\n\n'
  printf 'Retrieval and timeline runbook: docs/reference/live-diagnostic-artifacts.md. Cancellation/SIGKILL or runner loss may prevent final collection or upload; already captured files are retained when Actions permits.\n'
} >>"$output"

#!/bin/sh
set -eu

# Checks only the controller's exact synthetic run IDs. History payloads retain
# their wire representation; describe decodes memo but leaves indexed payloads
# encoded. Validate both surfaces before treating a live run as evidence.
evidence=${1:?expected metadata evidence directory}
[ "$(wc -l < "$evidence/runs.tsv" | tr -d ' ')" -eq 5 ]
[ "$(wc -l < "$evidence/runs.tsv.roots" | tr -d ' ')" -eq 4 ]
[ "$(wc -l < "$evidence/cache-runs.tsv" | tr -d ' ')" -eq 3 ]
[ "$(wc -l < "$evidence/cache-snapshots.tsv" | tr -d ' ')" -eq 3 ]

# Verifies the initial and terminal history and visibility of every run listed
# in $1. $2 must have completed a workflow task before the initial capture and
# $3 must have completed one by the terminal capture; the replacement scenario
# passes its two worker generations, the cache scenario its single worker.
verify_runs() {
  runs=$1
  first=$2
  second=$3
  while IFS="$(printf '\t')" read -r id run expected; do
    for phase in initial terminal; do
      jq -e --arg id "$id" --arg run "$run" --arg expected "$expected" --arg phase "$phase" '
        def decoded: if type == "object" then .data | @base64d | fromjson else . end;
        ($expected | split(":")) as $values
        | .workflowExecutionInfo as $info
        | $info.execution.workflowId == $id and $info.execution.runId == $run
        and $info.status == (if $phase == "initial" then "WORKFLOW_EXECUTION_STATUS_RUNNING" else "WORKFLOW_EXECUTION_STATUS_COMPLETED" end)
        and (($info.memo.fields.note // null | decoded) == (if $values[0] == "-" then null else $values[0] end))
        and (($info.searchAttributes.indexedFields.MetadataKeyword // null | decoded) == (if $values[1] == "-" then null else $values[1] end))
        and (if $values[1] == "-" then true else
          ($info.searchAttributes.indexedFields.MetadataKeyword.metadata.type | @base64d) == "Keyword" end)
      ' "$evidence/$id.describe.$phase.json" >/dev/null
      jq -e --arg id "$id" --arg expected "$expected" --arg phase "$phase" \
        --arg first "$first" --arg second "$second" '
        ($expected | split(":")) as $values
        | .events[0].workflowExecutionStartedEventAttributes as $start
        | $start.workflowId == $id
        and (($start.memo.fields.note.data // null | if . == null then null else @base64d | fromjson end)
          == (if $values[0] == "-" then null else $values[0] end))
        and (($start.searchAttributes.indexedFields.MetadataKeyword.data // null | if . == null then null else @base64d | fromjson end)
          == (if $values[1] == "-" then null else $values[1] end))
        and (if $values[1] == "-" then true else
          ($start.searchAttributes.indexedFields.MetadataKeyword.metadata.type | @base64d) == "Keyword" end)
        and (if $values[2] == "deadline" then
          $start.workflowExecutionTimeout == "3600s" and $start.workflowRunTimeout == "1800s"
          and $start.workflowTaskTimeout == "10s" and ($start.workflowExecutionExpirationTime | type) == "string"
          else ($start.workflowExecutionExpirationTime // null) == null end)
        and any(.events[]; .eventType == "EVENT_TYPE_TIMER_FIRED")
        and any(.events[]; .workflowTaskCompletedEventAttributes.identity == $first)
        and (if $phase == "initial" then
          all(.events[]; (.eventType | test("WORKFLOW_EXECUTION_(COMPLETED|FAILED|TIMED_OUT|TERMINATED|CANCELED|CONTINUED_AS_NEW)$")) | not)
          else
          .events[-1].eventType == "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED"
          and (.events[-1].workflowExecutionCompletedEventAttributes.result.payloads[0].data | @base64d | fromjson) == $expected
          and any(.events[]; .workflowTaskCompletedEventAttributes.identity == $second) end)
      ' "$evidence/$id.$phase.json" >/dev/null
    done
    jq -e --slurpfile initial "$evidence/$id.initial.json" '
      ($initial[0].events | length) as $count | .events[:$count] == $initial[0].events
    ' "$evidence/$id.terminal.json" >/dev/null
    case "$id" in
      *-continue)
        jq -e --arg run "$run" '
          .events[-1].eventType == "EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW"
          and .events[-1].workflowExecutionContinuedAsNewEventAttributes.newExecutionRunId == $run
        ' "$evidence/$id.root.json" >/dev/null
        root_run=$(awk -F '\t' -v id="$id" '$1 == id {print $2}' "$evidence/runs.tsv.roots")
        [ "$root_run" != "$run" ]
        jq -e --arg root "$root_run" '
          .events[0].workflowExecutionStartedEventAttributes.continuedExecutionRunId == $root
          and .events[0].workflowExecutionStartedEventAttributes.initiator == "CONTINUE_AS_NEW_INITIATOR_WORKFLOW"
        ' "$evidence/$id.terminal.json" >/dev/null
        ;;
    esac
  done < "$runs"
}

verify_runs "$evidence/runs.tsv" metadata-worker-one metadata-worker-two
verify_runs "$evidence/cache-runs.tsv" metadata-worker-cache metadata-worker-cache

# Cache-full evidence. Each cache-events.tsv line is: kind, run ID, workflow
# ID or "-", replay flag, history length, cache-removal reason or "-". Every
# cache run needs an acknowledged completion with reason cache_full, followed
# later by a replaying activation of the same run: the run was evicted for
# capacity and then rebuilt from history.
while IFS="$(printf '\t')" read -r id run _expected; do
  if ! awk -F '\t' -v run="$run" '
    $1 == "completion" && $2 == run && $6 == "cache_full" && !evicted { evicted = NR }
    evicted && NR > evicted && $1 == "activation" && $2 == run && $4 == "true" { replayed = 1 }
    END { exit !(evicted && replayed) }
  ' "$evidence/cache-events.tsv"; then
    printf 'cache run %s (%s) lacks a cache_full eviction followed by a replay\n' "$id" "$run" >&2
    exit 1
  fi
done < "$evidence/cache-runs.tsv"

# The driver already required each run's exact snapshot to be identical before
# eviction and after replay. Here that snapshot's expiration must also equal,
# to the nanosecond, the expiration recorded on the run's start event.
while IFS="$(printf '\t')" read -r id snapshot; do
  jq -e --arg snapshot "$snapshot" '
    def exact: (sub("Z$"; "") | split(".")) as $parts
      | (($parts[0] + "Z") | fromdateiso8601 | tostring)
        + "." + ((($parts[1] // "") + "000000000")[:9]);
    (.events[0].workflowExecutionStartedEventAttributes.workflowExecutionExpirationTime // null) as $time
    | ($snapshot | split("|")[1]) == (if $time == null then "none" else ($time | exact) end)
  ' "$evidence/$id.terminal.json" >/dev/null
done < "$evidence/cache-snapshots.tsv"
printf 'Verified eight exact metadata histories, visibility values, continuation, worker replacement, and cache-full eviction with replay.\n'

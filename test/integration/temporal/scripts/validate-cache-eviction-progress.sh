#!/bin/sh
set -eu

# Binds the cache-full removal of exact run A to its later normal completion.
# Both history inputs are the payload-free projection from normalize-history.sh;
# the marker and B's run ID come from the live worker and client respectively.

if [ "$#" -ne 4 ]; then
  echo 'usage: validate-cache-eviction-progress.sh MARKER A_HISTORY B_HISTORY B_RUN_ID' >&2
  exit 2
fi

marker=$1
a_history=$2
b_history=$3
b_run_id=$4

for path in "$marker" "$a_history" "$b_history"; do
  if [ ! -s "$path" ]; then
    echo "cache eviction progress evidence is missing: $path" >&2
    exit 1
  fi
done
if [ -z "$b_run_id" ]; then
  echo 'cache eviction run B has no exact run ID' >&2
  exit 1
fi

if ! jq -e '
  type == "object" and (keys | sort) == ["reason", "run_id", "workflow_id"]
  and .workflow_id == "two-binary-cache-eviction-a"
  and .reason == "cache_full" and (.run_id | type == "string" and length > 0)
' "$marker" >/dev/null; then
  echo 'cache eviction marker has the wrong identity or reason' >&2
  exit 1
fi
a_run_id=$(jq -er '.run_id' "$marker")

if ! jq -e --arg run_id "$a_run_id" '
  .workflow_id == "two-binary-cache-eviction-a" and .run_id == $run_id
  and (.events | type == "array" and length >= 7)
  and ([.events[].type] as $types
       | $types[0] == "WorkflowExecutionStarted"
       and $types[-1] == "WorkflowExecutionCompleted"
       and ($types | index("WorkflowTaskCompleted")) != null
       and ($types | index("WorkflowExecutionSignaled")) != null
       and ($types | index("WorkflowTaskCompleted")) < ($types | index("WorkflowExecutionSignaled"))
       and ($types | map(select(. == "WorkflowExecutionSignaled")) | length) == 1
       and ($types | index("TimerStarted")) == null
       and ($types | index("WorkflowExecutionCanceled")) == null)
' "$a_history" >/dev/null; then
  echo 'evicted run A did not resume after its signal and complete the exact history' >&2
  exit 1
fi

if ! jq -e --arg run_id "$b_run_id" '
  .workflow_id == "two-binary-cache-eviction-b" and .run_id == $run_id
  and (.events | type == "array" and length >= 7)
  and ([.events[].type] as $types
       | $types[0] == "WorkflowExecutionStarted"
       and $types[-1] == "WorkflowExecutionCanceled"
       and ($types | index("TimerStarted")) != null
       and ($types | index("WorkflowExecutionCancelRequested")) != null
       and ($types | index("TimerStarted")) < ($types | index("WorkflowExecutionCancelRequested"))
       and ($types | index("WorkflowExecutionCompleted")) == null)
' "$b_history" >/dev/null; then
  echo 'outstanding run B did not reach its exact cancellation history' >&2
  exit 1
fi

echo 'cache eviction progress histories: ok'

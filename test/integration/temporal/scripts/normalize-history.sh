#!/bin/sh
set -eu

# Converts the Temporal CLI's machine-readable history response into the
# payload-free, closed history document consumed by the restart/replay
# validator. Temporal keeps protobuf field names stable but the CLI has used
# both camelCase and snake_case JSON spellings across releases, so this small
# adapter accepts only those known spellings and rejects everything else.

usage() {
  echo "usage: normalize-history.sh --workflow-id ID --run-id ID --output FILE" >&2
  exit 2
}

fail() {
  echo "history normalization failed: $*" >&2
  exit 1
}

workflow_id=''
run_id=''
output=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --workflow-id)
      [ "$#" -ge 2 ] || usage
      workflow_id=$2
      shift 2
      ;;
    --run-id)
      [ "$#" -ge 2 ] || usage
      run_id=$2
      shift 2
      ;;
    --output)
      [ "$#" -ge 2 ] || usage
      output=$2
      shift 2
      ;;
    --help|-h)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

[ -n "$workflow_id" ] || usage
[ -n "$run_id" ] || usage
[ -n "$output" ] || usage

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required"

tmp="${output}.tmp.$$"
trap 'rm -f "$tmp"' EXIT HUP INT TERM

# The CLI output is intentionally projected to event IDs and event types only.
# Payloads, timestamps, headers, and failure attributes are never copied to
# the acceptance document. Unknown event types fail closed instead of allowing
# a future server event to be silently misclassified as a successful replay.
if ! "$jq_bin" -e \
  --arg expected_workflow "$workflow_id" \
  --arg expected_run "$run_id" \
  '
    def compact:
      ascii_downcase | gsub("[^a-z0-9]"; "");
    def field($object; $camel; $snake):
      ($object[$camel] // $object[$snake]);
    def event_type:
      (field(.; "eventType"; "event_type") // .type // "")
      | compact
      # Protobuf JSON renders Temporal enum values with the EVENT_TYPE_ prefix;
      # the normalized contract intentionally keeps the stable semantic name.
      | sub("^eventtype"; "")
      | sub("^historyeventtype"; "")
      | if . == "workflowexecutionstarted" then "WorkflowExecutionStarted"
        elif . == "workflowexecutionsignaled" then "WorkflowExecutionSignaled"
        elif . == "workflowtaskscheduled" then "WorkflowTaskScheduled"
        elif . == "workflowtaskstarted" then "WorkflowTaskStarted"
        elif . == "workflowtaskcompleted" then "WorkflowTaskCompleted"
        elif . == "workflowtaskfailed" then "WorkflowTaskFailed"
        elif . == "workflowtasktimedout" then "WorkflowTaskTimedOut"
        elif . == "timerstarted" then "TimerStarted"
        elif . == "timerfired" then "TimerFired"
        elif . == "timercanceled" then "TimerCanceled"
        elif . == "activitytaskscheduled" then "ActivityTaskScheduled"
        elif . == "activitytaskstarted" then "ActivityTaskStarted"
        elif . == "activitytaskcompleted" then "ActivityTaskCompleted"
        elif . == "activitytaskfailed" then "ActivityTaskFailed"
        elif . == "activitytaskcanceled" then "ActivityTaskCanceled"
        elif . == "workflowexecutioncancelrequested" then "WorkflowExecutionCancelRequested"
        elif . == "workflowexecutioncompleted" then "WorkflowExecutionCompleted"
        elif . == "workflowexecutionfailed" then "WorkflowExecutionFailed"
        elif . == "workflowexecutioncanceled" then "WorkflowExecutionCanceled"
        elif . == "workflowexecutionterminated" then "WorkflowExecutionTerminated"
        elif . == "workflowexecutiontimedout" then "WorkflowExecutionTimedOut"
        elif . == "workflowexecutioncontinuedasnew" then "WorkflowExecutionContinuedAsNew"
        else error("unknown Temporal event type")
        end;
    def event_id:
      (field(.; "eventId"; "event_id") // .id)
      # Temporal protobuf JSON mapping emits int64 event IDs as decimal
      # strings. Reject JSON numbers instead of converting them: jq numbers
      # are IEEE-754 values and can silently lose ordering precision above
      # 2^53.
      | if type == "string" then .
        else error("Temporal event ID must be a decimal string")
        end;
    def event_list:
      (.history.events // .history_events // .events // .workflowExecutionHistory.events
       // .workflow_execution_history.events);
    def execution:
      (.workflowExecution // .workflow_execution // .execution // {});
    (event_list) as $events
    | (execution) as $execution
    | ($events | if type == "array" and length >= 1 then . else error("history has no events") end) as $raw
    | ($raw[0]
       | (.workflowExecutionStartedEventAttributes
          // .workflow_execution_started_event_attributes // {})) as $started
    | (field($execution; "workflowId"; "workflow_id")
       // field($started; "workflowId"; "workflow_id")
       // .workflowId // .workflow_id) as $raw_workflow
    | (if ($raw_workflow | type) == "string" and ($raw_workflow | length) > 0
       then $raw_workflow
       else error("history has no workflow ID")
       end) as $workflow
    # HistoryEvent does not carry the current run ID. The live controller
    # validates that identity separately with `workflow describe`; a nested
    # fixture may still provide it and is rejected if it disagrees.
    | (field($execution; "runId"; "run_id") // .runId // .run_id // $expected_run) as $raw_run
    | ($raw_run | tostring) as $run
    | {
        workflow_id: $workflow,
        run_id: $run,
        events: [$raw[] | {event_id: event_id, type: event_type}]
      }
    | select(.workflow_id == $expected_workflow and .run_id == $expected_run)
    | if all(.events[]; (.event_id | test("^[1-9][0-9]{0,18}$"))) then . else error("invalid event ID") end
  ' >"$tmp"; then
  fail "CLI response was not a supported Temporal history document"
fi

mv "$tmp" "$output"
trap - EXIT HUP INT TERM

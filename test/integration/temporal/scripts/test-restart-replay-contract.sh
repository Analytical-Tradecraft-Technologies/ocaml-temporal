#!/bin/sh
set -eu

# This is the Docker-free regression gate for the restart/replay acceptance
# contract. It exercises the same validator used by the live Compose controller,
# including rejection paths for identity mismatches, premature timer
# completion, malformed ordering, and missing replay evidence.

root=$(CDPATH= cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal/fixtures/restart-replay"
validator="$root/test/integration/temporal/scripts/validate-restart-replay.sh"
controller_validator="$root/test/integration/temporal/scripts/validate-restart-replay-controller.sh"
normalizer="$root/test/integration/temporal/scripts/normalize-history.sh"
identity_validator="$root/test/integration/temporal/scripts/validate-restart-replay-identity.sh"
workflow_id=two-binary-worker-restart-replay
run_id=11111111-1111-4111-8111-111111111111

[ -r "$validator" ]
[ -r "$controller_validator" ]
[ -r "$normalizer" ]
[ -r "$identity_validator" ]
[ -r "$fixture/history.initial.json" ]
[ -r "$fixture/history.terminal.json" ]
[ -r "$fixture/diagnostics.json" ]
[ -r "$fixture/controller.json" ]

# Runs one deliberately invalid invocation and fails the test if it is
# accepted. Keeping the negative assertion as a helper makes each rejection
# case below read as the invariant it protects.
expect_failure() {
  if "$@" >/dev/null 2>&1; then
    echo "expected command to fail: $*" >&2
    exit 1
  fi
}

# Invoke repository shell scripts through the platform shell explicitly. This
# avoids relying on executable-bit and macOS provenance behavior while keeping
# the same script entry points used by the live Make target.
sh "$validator" \
  --history "$fixture/history.initial.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage initial >/dev/null

sh "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --diagnostics "$fixture/diagnostics.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal \
  --require-replay >/dev/null

# Temporal compacts activity retries in workflow history, so the normalized
# terminal fixture cannot assert an intermediate ActivityTaskFailed event.
# The live driver proves the retry instead by requiring the exact
# SMOKE:AFTER-REPLAY:ATTEMPT:2 result; this fixture still proves that the
# terminal history contains the activity command and its successful completion.
jq -e '[.events[].type] | index("ActivityTaskScheduled") != null
  and index("ActivityTaskStarted") != null
  and index("ActivityTaskCompleted") != null' \
  "$fixture/history.terminal.json" >/dev/null

# Terminal validation cannot be used without retaining the initial snapshot;
# otherwise the cross-document event-prefix check would be bypassed.
expect_failure sh "$validator" \
  --history "$fixture/history.terminal.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal

expect_failure sh "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --workflow-id "$workflow_id" \
  --run-id wrong-run-id \
  --stage terminal

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

# Keep the live controller's exact-run identity check on a Docker-free test
# path. The CLI envelope is intentionally larger than this assertion; only
# the documented execution identifiers are relevant to the acceptance result.
jq -n --arg workflow_id "$workflow_id" --arg run_id "$run_id" \
  '{workflowExecutionInfo:{execution:{workflowId:$workflow_id,runId:$run_id}}}' \
  >"$tmp/describe.json"
sh "$identity_validator" --input "$tmp/describe.json" \
  --workflow-id "$workflow_id" --run-id "$run_id" >/dev/null
expect_failure sh "$identity_validator" --input "$tmp/describe.json" \
  --workflow-id "$workflow_id" --run-id 22222222-2222-4222-8222-222222222222

# Exercise the adapter with the protobuf-JSON spellings emitted by the
# Temporal CLI. In particular, enum values have an EVENT_TYPE_ prefix and
# int64 event IDs are decimal strings; the normalizer must project only the
# closed, payload-free contract used by the validator.
jq -n \
  --arg workflow_id "$workflow_id" --arg run_id "$run_id" \
  '{workflowExecution:{workflowId:$workflow_id,runId:$run_id},history:{events:[
    {eventId:"1",eventType:"EVENT_TYPE_WORKFLOW_EXECUTION_STARTED"},
    {eventId:"2",eventType:"EVENT_TYPE_WORKFLOW_TASK_COMPLETED"},
    {eventId:"3",eventType:"EVENT_TYPE_TIMER_STARTED"}
  ]}}' >"$tmp/cli-history.json"
sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
  --output "$tmp/normalized-history.json" <"$tmp/cli-history.json"
jq -e --arg workflow_id "$workflow_id" --arg run_id "$run_id" \
  '.workflow_id == $workflow_id
   and .run_id == $run_id
   and ([.events[].type] == ["WorkflowExecutionStarted", "WorkflowTaskCompleted", "TimerStarted"])
   and ([.events[].event_id] == ["1", "2", "3"])' \
  "$tmp/normalized-history.json" >/dev/null

# Replacing a worker can produce a non-terminal WorkflowTaskTimedOut event when
# the previous worker's sticky task expires. It is a valid replay boundary, so
# the normalizer must retain it as a known semantic event instead of failing
# closed as an unknown future event.
jq '.history.events[1].eventType = "EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT"' \
  "$tmp/cli-history.json" >"$tmp/cli-history-sticky-timeout.json"
sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
  --output "$tmp/normalized-sticky-timeout.json" <"$tmp/cli-history-sticky-timeout.json"
jq -e '.events[1].type == "WorkflowTaskTimedOut"' \
  "$tmp/normalized-sticky-timeout.json" >/dev/null

# A post-eviction signal is a durable server event, but its input payload must
# not enter the retained history projection. Accept the canonical protobuf
# enum and reject a misspelling rather than broadening the closed event map.
jq '.history.events += [{eventId:"4",eventType:"EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED",
      workflowExecutionSignaledEventAttributes:{signalName:"resume",input:"private-signal-input"}}]' \
  "$tmp/cli-history.json" >"$tmp/cli-history-signaled.json"
sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
  --output "$tmp/normalized-signaled.json" <"$tmp/cli-history-signaled.json"
jq -e '.events[3] == {event_id:"4",type:"WorkflowExecutionSignaled"}
  and ([.events[] | keys | sort] | all(. == ["event_id", "type"]))' \
  "$tmp/normalized-signaled.json" >/dev/null
if grep -F 'private-signal-input' "$tmp/normalized-signaled.json" >/dev/null; then
  echo 'signal input leaked into normalized history' >&2
  exit 1
fi
jq '.history.events[3].eventType = "EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALLED"' \
  "$tmp/cli-history-signaled.json" >"$tmp/cli-history-misspelled-signal.json"
expect_failure sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
  --output "$tmp/normalized-misspelled-signal.json" \
  <"$tmp/cli-history-misspelled-signal.json"
[ ! -e "$tmp/normalized-misspelled-signal.json" ]

# A client cancellation request also appears in B's exact server history.
# Preserve that event name while keeping its reason outside the projection;
# an unknown near-match must still fail without publishing an output file.
jq '.history.events += [{eventId:"4",eventType:"EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED",
      workflowExecutionCancelRequestedEventAttributes:{cause:"private-cancel-reason"}}]' \
  "$tmp/cli-history.json" >"$tmp/cli-history-cancel-requested.json"
sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
  --output "$tmp/normalized-cancel-requested.json" \
  <"$tmp/cli-history-cancel-requested.json"
jq -e '.events[3] == {event_id:"4",type:"WorkflowExecutionCancelRequested"}
  and ([.events[] | keys | sort] | all(. == ["event_id", "type"]))' \
  "$tmp/normalized-cancel-requested.json" >/dev/null
if grep -F 'private-cancel-reason' "$tmp/normalized-cancel-requested.json" >/dev/null; then
  echo 'cancellation reason leaked into normalized history' >&2
  exit 1
fi
jq '.history.events[3].eventType = "EVENT_TYPE_WORKFLOW_EXECUTION_CANCELLATION_REQUESTED"' \
  "$tmp/cli-history-cancel-requested.json" >"$tmp/cli-history-misspelled-cancel.json"
expect_failure sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
  --output "$tmp/normalized-misspelled-cancel.json" \
  <"$tmp/cli-history-misspelled-cancel.json"
[ ! -e "$tmp/normalized-misspelled-cancel.json" ]

# The real Temporal CLI emits this top-level shape, without workflow/run
# metadata around the history. The first event carries the workflow ID in its
# started-event attributes; the controller obtains the exact run ID from the
# separate `workflow describe --output json` identity check.
jq -n \
  --arg workflow_id "$workflow_id" \
  '{events:[
    {eventId:"1",eventType:"EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
      workflowExecutionStartedEventAttributes:{workflowId:$workflow_id}},
    {eventId:"2",eventType:"EVENT_TYPE_WORKFLOW_TASK_COMPLETED"},
    {eventId:"3",eventType:"EVENT_TYPE_TIMER_STARTED"}
  ]}' >"$tmp/cli-top-level-history.json"
sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
  --output "$tmp/normalized-top-level-history.json" <"$tmp/cli-top-level-history.json"
jq -e --arg workflow_id "$workflow_id" --arg run_id "$run_id" \
  '.workflow_id == $workflow_id
   and .run_id == $run_id
   and ([.events[].type] == ["WorkflowExecutionStarted", "WorkflowTaskCompleted", "TimerStarted"])' \
  "$tmp/normalized-top-level-history.json" >/dev/null
expect_failure sh -c \
  'jq ".history.events[1].eventType = \"EVENT_TYPE_NOT_REAL\"" "$1" \
  | sh "$2" --workflow-id "$3" --run-id "$4" --output "$5"' \
  sh "$tmp/cli-history.json" "$normalizer" "$workflow_id" "$run_id" \
  "$tmp/unknown-event.json"
expect_failure sh -c \
  'jq ".history.events[0].eventId = 9007199254740993" "$1" \
  | sh "$2" --workflow-id "$3" --run-id "$4" --output "$5"' \
  sh "$tmp/cli-history.json" "$normalizer" "$workflow_id" "$run_id" \
  "$tmp/numeric-event-id.json"

# Keep the validator's configurable jq executable path quoted end to end. A
# path containing spaces is valid on the host and catches command substitutions
# that accidentally split [JQ_BIN] after the structural checks have passed.
jq_path=$(command -v jq)
space_jq="$tmp/jq wrapper"
ln -s "$jq_path" "$space_jq"
JQ_BIN="$space_jq" sh "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --diagnostics "$fixture/diagnostics.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal \
  --require-replay >/dev/null

# The controller record is accepted only when its run identity and exact
# lifecycle order agree with the same worker-restart scenario. Reusing the
# configurable jq path here keeps both validators covered by the quoted-path
# regression instead of proving that only the history checker handles it.
JQ_BIN="$space_jq" sh "$controller_validator" \
  --controller "$fixture/controller.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" >/dev/null

# A terminal event before the timer is observed must not satisfy the initial
# stop boundary. jq is used only to create an ephemeral negative fixture; it is
# not part of the production worker or the Temporal history adapter.
jq '.events += [{"event_id": "6", "type": "TimerFired"}]' \
  "$fixture/history.initial.json" >"$tmp/history-fired.json"
expect_failure sh "$validator" \
  --history "$tmp/history-fired.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage initial

jq '.records[1].is_replaying = false' \
  "$fixture/diagnostics.json" >"$tmp/diagnostics-without-replay.json"
expect_failure sh "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --diagnostics "$tmp/diagnostics-without-replay.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal \
  --require-replay

jq '.records[1].history_length = "9223372036854775808"' \
  "$fixture/diagnostics.json" >"$tmp/diagnostics-out-of-range.json"
expect_failure sh "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --diagnostics "$tmp/diagnostics-out-of-range.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal \
  --require-replay

jq '.events[10].event_id = "9"' \
  "$fixture/history.terminal.json" >"$tmp/history-nonmonotonic.json"
expect_failure sh "$validator" \
  --history "$tmp/history-nonmonotonic.json" \
  --initial-history "$fixture/history.initial.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal

# JSON numbers are not used for event IDs, but the decimal-string contract
# still has to reject values outside Temporal's signed 64-bit range.
jq '.events[4].event_id = "9223372036854775808"' \
  "$fixture/history.initial.json" >"$tmp/history-out-of-range.json"
expect_failure sh "$validator" \
  --history "$tmp/history-out-of-range.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage initial

# A failed workflow event inserted between the timer and activity must not be
# hidden by a later successful-looking completion event.
jq '.events = .events[0:5]
  + [{"event_id": "6", "type": "WorkflowExecutionFailed"}]
  + (.events[5:] | map(.event_id = ((.event_id | tonumber) + 1 | tostring)))' \
  "$fixture/history.terminal.json" >"$tmp/history-early-terminal.json"
expect_failure sh "$validator" \
  --history "$tmp/history-early-terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal

# Temporal compacts intermediate activity retry events. A synthetic failure
# inserted into the normalized terminal history must therefore be rejected
# rather than mistaken for live retry evidence.
jq '.events = .events[0:10]
  + [{"event_id": "11", "type": "ActivityTaskFailed"}]
  + (.events[10:] | map(.event_id = ((.event_id | tonumber) + 1 | tostring)))' \
  "$fixture/history.terminal.json" >"$tmp/history-uncompacted-retry.json"
expect_failure sh "$validator" \
  --history "$tmp/history-uncompacted-retry.json" \
  --initial-history "$fixture/history.initial.json" \
  --diagnostics "$fixture/diagnostics.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal \
  --require-replay

# The baseline must be the exact initial event prefix, not merely a valid
# pending-timer history for the same workflow/run identity. This mutation keeps
# the baseline valid on its own while changing one already-observed event.
jq '.events[2].type = "WorkflowTaskScheduled"' \
  "$fixture/history.initial.json" >"$tmp/history-prefix-mismatch.json"
sh "$validator" \
  --history "$tmp/history-prefix-mismatch.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage initial >/dev/null
expect_failure sh "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$tmp/history-prefix-mismatch.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal

# A controller cannot claim replacement when the old worker container is still
# present. The history and replay documents remain valid; this mutation must
# be rejected by the lifecycle validator itself.
jq '.events[5].remaining_worker_containers = 1' \
  "$fixture/controller.json" >"$tmp/controller-worker-retained.json"
expect_failure sh "$controller_validator" \
  --controller "$tmp/controller-worker-retained.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id"

# A readiness marker from the stopped generation must not be accepted as
# generation-2 readiness. The IDs are compared across lifecycle records so a
# reused Compose container cannot satisfy the contract.
jq '.events[6].container_id = .events[4].container_id' \
  "$fixture/controller.json" >"$tmp/controller-reused-container.json"
expect_failure sh "$controller_validator" \
  --controller "$tmp/controller-reused-container.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id"

# Cleanup is part of the acceptance result, not an optional post-test action.
# Leaving the project volume behind must fail even when every workflow marker
# and terminal result looks successful.
jq '.events[12].remaining_project_volumes = 1' \
  "$fixture/controller.json" >"$tmp/controller-retained-volume.json"
expect_failure sh "$controller_validator" \
  --controller "$tmp/controller-retained-volume.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id"

# Unknown fields are rejected at the controller boundary. The live adapter must
# update the schema and validator deliberately rather than silently widening
# the acceptance protocol when a Docker command changes its output shape.
jq '.events[3].unexpected = true' "$fixture/controller.json" \
  >"$tmp/controller-unknown-field.json"
expect_failure sh "$controller_validator" \
  --controller "$tmp/controller-unknown-field.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id"

echo 'restart/replay contract: ok'

#!/bin/sh
set -eu

# This contract protects the live child-start-failure acceptance shape when
# Docker or Temporal Server is unavailable. Only the Compose run can prove
# that Temporal rejected the duplicate child ID; these checks keep the
# scenario tied to a real server conflict instead of a synthetic activation,
# an unregistered workflow type, or an activity failure.
root=${1:-.}
fixture="$root/test/integration/temporal"
definitions="$fixture/common/smoke_definitions.ml"
driver="$fixture/driver/smoke_driver.ml"
worker="$fixture/worker/smoke_worker.ml"

# Checks one source file for the required fixture fragment. The contract does
# not claim to observe Temporal's child-start state machine without Compose.
require_text() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "child-start-failure acceptance contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

require_text "$definitions" \
  'let child_start_conflict_id = "two-binary-long-running-cancellation"'
require_text "$definitions" 'let parent_observes_child_start_failure ='
require_text "$definitions" \
  'Temporal.Child_workflow.execute ~id:child_start_conflict_id'
require_text "$definitions" 'view.category = `Child_workflow'
require_text "$definitions" 'view.non_retryable'
require_text "$definitions" 'SMOKE:CHILD:START_FAILED'

# The parent must be registered, while the conflicting workflow remains the
# already-started top-level cancellation execution. This makes the live test
# exercise Temporal's duplicate-ID start failure rather than a local mock.
require_text "$worker" \
  'Worker.workflow Definitions.parent_observes_child_start_failure'
require_text "$driver" \
  'id:"two-binary-parent-observes-child-start-failure"'
require_text "$driver" \
  'SMOKE:CHILD:START_FAILED'
require_text "$driver" 'let* child_start_failure_result ='
require_text "$driver" 'wait_workflow child_start_failure_handle'

cancellation_start_line=$(grep -n -F \
  'id:"two-binary-long-running-cancellation"' "$driver" \
  | head -n 1 | cut -d: -f1)
child_start_failure_line=$(grep -n -F \
  'id:"two-binary-parent-observes-child-start-failure"' "$driver" \
  | head -n 1 | cut -d: -f1)
first_terminal_wait_line=$(grep -n -F \
  'let* fan_result = wait_workflow fan_handle' "$driver" \
  | head -n 1 | cut -d: -f1)
if [ -z "$cancellation_start_line" ] || [ -z "$child_start_failure_line" ] \
  || [ -z "$first_terminal_wait_line" ] \
  || [ "$cancellation_start_line" -ge "$child_start_failure_line" ] \
  || [ "$child_start_failure_line" -ge "$first_terminal_wait_line" ]; then
  echo "child-start-failure parent must start after the conflicting run and before the first wait" >&2
  exit 1
fi

# The duplicate child ID is held only while the cancellation run is open, so
# the driver must observe the parent's terminal start-failure result before it
# cancels that run. Waiting afterwards reintroduces the race in which the
# cancellation closes the holder before the parent issues its child command.
child_start_failure_wait_line=$(grep -n -F \
  'wait_workflow child_start_failure_handle' "$driver" \
  | head -n 1 | cut -d: -f1)
cancellation_cancel_line=$(grep -n -F \
  'cancel_workflow cancellation_handle' "$driver" \
  | head -n 1 | cut -d: -f1)
if [ -z "$child_start_failure_wait_line" ] || [ -z "$cancellation_cancel_line" ] \
  || [ "$child_start_failure_line" -ge "$child_start_failure_wait_line" ] \
  || [ "$child_start_failure_wait_line" -ge "$cancellation_cancel_line" ]; then
  echo "child-start-failure result must be awaited before the conflicting run is cancelled" >&2
  exit 1
fi

# Preserve the two-process boundary: only the worker registers definitions,
# while only the driver starts and waits for the exact run.
if grep -F 'Worker.create' "$driver" >/dev/null \
  || grep -F 'Worker.run' "$driver" >/dev/null; then
  echo "child-start-failure driver must not become a worker" >&2
  exit 1
fi
if grep -F 'Client.start' "$worker" >/dev/null \
  || grep -F 'Client.wait' "$worker" >/dev/null; then
  echo "child-start-failure worker must not become a client driver" >&2
  exit 1
fi

echo "temporal child-start-failure acceptance contract: ok"

#!/bin/sh
set -eu

# This contract is deliberately source-only: it verifies that the live
# acceptance fixture still contains a server-visible non-retryable activity
# case when Docker or Temporal Server is unavailable. The workflow catches the
# activity error, so a top-level client result cannot accidentally hide the
# activity category behind the public Workflow terminal category.
root=${1:-.}
fixture="$root/test/integration/temporal"
definitions="$fixture/common/smoke_definitions.ml"
driver="$fixture/driver/smoke_driver.ml"
worker="$fixture/worker/smoke_worker.ml"

require_file() {
  if [ ! -f "$1" ]; then
    echo "non-retryable activity acceptance contract is missing: $1" >&2
    exit 1
  fi
}

require_text() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "non-retryable activity acceptance contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

require_file "$definitions"
require_file "$driver"
require_file "$worker"

# The callback is retryable by itself; the policy's application error type is
# what makes this a useful live test of Temporal's non-retryable matching. The
# type is user-defined (not a category label) so the test proves that
# Error.make ~error_type reaches ApplicationFailureInfo.type on the wire.
require_text "$definitions" \
  'let non_retryable_activity_policy ='
require_text "$definitions" \
  '~non_retryable_error_types:[ "SmokeInvalidInput" ] ()'
require_text "$definitions" \
  '(Temporal.Error.make ~error_type:"SmokeInvalidInput"'
require_text "$definitions" \
  '|| view.error_type <> Some "SmokeInvalidInput"'
require_text "$definitions" \
  'Temporal.Activity.define ~name:"smoke.non_retryable_activity"'
require_text "$definitions" \
  'Temporal.Codec.encode Temporal.Codec.string'
require_text "$definitions" \
  '~details:[ detail ]'
require_text "$definitions" \
  'SMOKE:ACTIVITY_NON_RETRYABLE:ATTEMPT:1'
require_text "$definitions" \
  'let activity_non_retryable_failure ='
require_text "$definitions" \
  'view.category <> `Activity || not view.non_retryable'
require_text "$definitions" \
  'view.non_retryable'
require_text "$definitions" \
  'Temporal.Codec.decode Temporal.Codec.string detail'
require_text "$definitions" \
  'SMOKE:ACTIVITY_NON_RETRYABLE:OBSERVED'
require_text "$definitions" \
  'non-retryable activity unexpectedly retried'

# Both executable sides must participate in this assertion. A driver-only
# marker or an unregistered activity would not exercise the real task path.
require_text "$worker" \
  'Worker.workflow Definitions.activity_non_retryable_failure'
require_text "$worker" \
  'Worker.activity Definitions.non_retryable_activity'
require_text "$driver" \
  'id:"two-binary-activity-non-retryable-failure"'
require_text "$driver" \
  'let* activity_non_retryable_result ='
require_text "$driver" \
  'wait_workflow activity_non_retryable_handle'
require_text "$driver" \
  'require_completed "smoke.activity_non_retryable_failure"'
require_text "$driver" \
  'SMOKE:ACTIVITY_NON_RETRYABLE:OBSERVED'

# Keep this workflow in the initial fan-out. Starting it before the first
# terminal wait makes a retry, if incorrectly scheduled, observable alongside
# the other live tasks rather than turning the test into a local unit call.
first_terminal_wait_line=$(grep -n -F \
  'let* fan_result = wait_workflow fan_handle' "$driver" \
  | head -n 1 | cut -d: -f1)
start_line=$(grep -n -F \
  'id:"two-binary-activity-non-retryable-failure"' "$driver" \
  | head -n 1 | cut -d: -f1)
if [ -z "$start_line" ] || [ -z "$first_terminal_wait_line" ] \
  || [ "$start_line" -ge "$first_terminal_wait_line" ]; then
  echo "non-retryable activity workflow must start before the first terminal wait" >&2
  exit 1
fi

# Preserve the two-process boundary: only the worker owns executable callbacks
# and only the driver starts and waits for the top-level workflow.
if grep -F 'Worker.create' "$driver" >/dev/null \
  || grep -F 'Worker.run' "$driver" >/dev/null; then
  echo "non-retryable activity driver must not become a worker" >&2
  exit 1
fi
if grep -F 'Client.start' "$worker" >/dev/null \
  || grep -F 'Client.wait' "$worker" >/dev/null; then
  echo "non-retryable activity worker must not become a client driver" >&2
  exit 1
fi

echo "temporal non-retryable activity acceptance contract: ok"

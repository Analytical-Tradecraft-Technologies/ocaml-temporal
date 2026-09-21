#!/bin/sh
set -eu

# Runs one real parent and its timer-delayed failing child across a complete
# OCaml worker replacement. Evidence is bound to both exact Temporal run IDs;
# the child run is learned only from the parent's ChildWorkflowExecutionStarted
# event, never from a latest-run lookup. Generation two must replay both runs,
# propagate the child's non-retryable failure, and let the parent handle it.

root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal"
compose_file="$fixture/compose.yaml"
project=${TEMPORAL_COMPOSE_PROJECT:-ocaml-temporal-integration}
parent_workflow_id=two-binary-parent-child-failure-replay
child_workflow_id=two-binary-parent-child-failure-replay-child-smoke
child_workflow_type=smoke.parent_child_failure_replay_child

accepted="$fixture/.child-failure-replay-accepted"
result="$fixture/.child-failure-replay-result"
diagnostics="$fixture/.child-failure-replay-diagnostics.json"
driver_log="$fixture/.child-failure-replay-driver.log"
controller="$fixture/.child-failure-replay-controller.json"
# The failure-replay scenario reuses the parent-child-restart Compose worker
# services. Their stopped-marker paths are part of that service contract, so
# the controller must read those exact bind-mounted files rather than a
# scenario-specific pathname that the worker never receives.
worker_one_stopped="$fixture/.parent-child-restart-worker-one-stopped"
worker_two_stopped="$fixture/.parent-child-restart-worker-two-stopped"
parent_raw="$fixture/.child-failure-replay-parent.raw.json"
child_raw="$fixture/.child-failure-replay-child.raw.json"
parent_describe="$fixture/.child-failure-replay-parent.describe.json"
child_describe="$fixture/.child-failure-replay-child.describe.json"
parent_initial="$fixture/.child-failure-replay-parent.initial.json"
child_initial="$fixture/.child-failure-replay-child.initial.json"
parent_post_removal="$fixture/.child-failure-replay-parent.post-removal.json"
child_post_removal="$fixture/.child-failure-replay-child.post-removal.json"
parent_terminal="$fixture/.child-failure-replay-parent.terminal.json"
child_terminal="$fixture/.child-failure-replay-child.terminal.json"
container_fixture=/workspace/test/integration/temporal
diagnostics_container="$container_fixture/.child-failure-replay-diagnostics.json"
accepted_container="$container_fixture/.child-failure-replay-accepted"
result_container="$container_fixture/.child-failure-replay-result"

normalizer="$fixture/scripts/normalize-parent-child-restart-replay-history.sh"
validator="$fixture/scripts/validate-parent-child-restart-replay.sh"
controller_validator="$fixture/scripts/validate-parent-child-restart-replay-controller.sh"
identity_validator="$fixture/scripts/validate-restart-replay-identity.sh"
driver_container="$project-parent-child-restart-driver"
worker_one=parent-child-restart-worker-one
worker_two=parent-child-restart-worker-two
driver_pid=''
initial_deadline_epoch=''
parent_run_id=''
child_run_id=''
producer_status=0

# Normalizes every Compose invocation so the bind-mounted build tree has one
# numeric owner and every process joins the same isolated Temporal project.
compose() {
  OCAML_IMAGE=${OCAML_IMAGE:-ocaml/opam:debian-12-ocaml-5.2} \
    HOST_UID=${HOST_UID:-$(id -u)} HOST_GID=${HOST_GID:-$(id -g)} \
    SMOKE_PARENT_CHILD_RESTART_TIMEOUT_SECONDS=${SMOKE_PARENT_CHILD_RESTART_TIMEOUT_SECONDS:-900} \
    SMOKE_PARENT_CHILD_REPLAY_SCENARIO=failure \
    SMOKE_PARENT_CHILD_REPLAY_PARENT_WORKFLOW_ID="$parent_workflow_id" \
    SMOKE_PARENT_CHILD_REPLAY_CHILD_WORKFLOW_ID="$child_workflow_id" \
    SMOKE_PARENT_CHILD_REPLAY_DIAGNOSTICS_FILE="$diagnostics_container" \
    SMOKE_PARENT_CHILD_RESTART_ACCEPTED_FILE="$accepted_container" \
    SMOKE_PARENT_CHILD_RESTART_RESULT_FILE="$result_container" \
    SMOKE_PARENT_CHILD_REPLAY_PARENT_RUN_ID=${SMOKE_PARENT_CHILD_REPLAY_PARENT_RUN_ID:-not-configured} \
    SMOKE_PARENT_CHILD_REPLAY_CHILD_RUN_ID=${SMOKE_PARENT_CHILD_REPLAY_CHILD_RUN_ID:-not-configured} \
    docker compose --project-directory "$fixture" --file "$compose_file" \
      --project-name "$project" --profile temporal "$@"
}

# Counts only volumes created by this Compose project. The live gate starts
# and ends at zero so no PostgreSQL history can satisfy a later run.
project_volume_count() {
  # Keep the Docker query outside a pipeline: POSIX [set -e] observes the
  # assignment's failure, whereas a failed first pipeline stage could
  # otherwise be hidden by a successful [wc].
  project_volumes=$(docker volume ls -q \
    --filter "label=com.docker.compose.project=$project")
  printf '%s\n' "$project_volumes" | sed '/^$/d' | wc -l | tr -d ' '
}

# Removes every bounded coordination/history file produced by this scenario.
remove_artifacts() {
  rm -f "$accepted" "$result" "$diagnostics" "$driver_log" "$controller" \
    "$worker_one_stopped" "$worker_two_stopped" "$parent_raw" "$child_raw" \
    "$parent_describe" "$child_describe" "$parent_initial" "$child_initial" \
    "$parent_post_removal" "$child_post_removal" "$parent_terminal" \
    "$child_terminal"
}

# Waits for the background Compose CLI exactly once and disarms its PID before
# returning. Clearing first means an EXIT trap can never signal a recycled PID,
# including when the child exits nonzero and [set -e] would otherwise interrupt
# the caller immediately after [wait].
reap_driver() {
  [ -n "$driver_pid" ] || {
    producer_status=0
    return 0
  }
  reaped_pid=$driver_pid
  driver_pid=''
  if wait "$reaped_pid"; then
    producer_status=0
  else
    producer_status=$?
  fi
  [ "$producer_status" -eq 0 ]
}

# Always terminates the client and destroys the stack and PostgreSQL volume.
# Failure output is deliberately bounded to the relevant three OCaml roles.
cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  sh "$fixture/scripts/collect-live-diagnostics.sh" cleanup || echo "live diagnostic snapshot failed" >&2
  if [ -n "$driver_pid" ] && kill -0 "$driver_pid" 2>/dev/null; then
    # The Compose CLI may still be building, before the named container exists.
    # In that case there is nothing for [docker stop] to terminate, so signal
    # the CLI itself and bound the grace period before reaping it.
    if ! docker stop --time 10 "$driver_container" >/dev/null 2>&1; then
      kill -TERM "$driver_pid" 2>/dev/null || true
    fi
    for _attempt in $(seq 1 100); do
      if ! kill -0 "$driver_pid" 2>/dev/null; then break; fi
      sleep 0.1
    done
    if kill -0 "$driver_pid" 2>/dev/null; then
      kill -KILL "$driver_pid" 2>/dev/null || true
    fi
  fi
  if [ -n "$driver_pid" ]; then
    reap_driver || true
  fi
  if [ "$status" -ne 0 ]; then
    tail -n 200 "$driver_log" 2>/dev/null >&2 || true
    docker inspect --format \
      'child failure replay driver running={{.State.Running}} exit_code={{.State.ExitCode}} oom_killed={{.State.OOMKilled}} error={{json .State.Error}}' \
      "$driver_container" >&2 2>/dev/null || true
    report_history_timeline "$parent_workflow_id" "$parent_run_id" \
      "$parent_raw" parent >&2 || true
    report_history_timeline "$child_workflow_id" "$child_run_id" \
      "$child_raw" child >&2 || true
    compose logs --no-color --tail 200 temporal "$worker_one" "$worker_two" \
      >&2 2>/dev/null || true
  fi
  docker rm -f "$driver_container" >/dev/null 2>&1 || true
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  remove_artifacts
  exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Polls for a complete marker and fails early if its producer exits.
wait_for_marker() {
  marker=$1
  producer_pid=${2:-}
  label=$3
  deadline_epoch=${4:-}
  marker_timeout=${SMOKE_PARENT_CHILD_RESTART_TIMEOUT_SECONDS:-900}
  for _attempt in $(seq 1 "$marker_timeout"); do
    if [ -s "$marker" ]; then return 0; fi
    if [ -n "$producer_pid" ] && ! kill -0 "$producer_pid" 2>/dev/null; then
      reap_driver || true
      echo "$label exited with status $producer_status before publishing $marker" >&2
      return 1
    fi
    if [ -n "$deadline_epoch" ] && [ "$(date +%s)" -ge "$deadline_epoch" ]; then
      echo "timed out waiting for $label before the generation-one safety deadline" >&2
      return 1
    fi
    sleep 1
  done
  echo "timed out waiting for $label marker: $marker" >&2
  return 1
}

# Docker reports a container as stopped as soon as its process exits, while a
# bind-mounted marker can become visible to the host just after that status
# transition. Poll the complete, exact shutdown marker for a short bounded
# interval so the teardown assertion checks the worker's published contract
# rather than an intermediate filesystem observation. The marker is removed
# before each stop request, so accepting it here cannot reuse a prior run.
wait_for_shutdown_marker() {
  marker=$1
  generation=$2
  marker_timeout=${SMOKE_PARENT_CHILD_RESTART_STOP_MARKER_TIMEOUT_SECONDS:-15}
  for _attempt in $(seq 1 "$marker_timeout"); do
    if [ "$(cat "$marker" 2>/dev/null || true)" = "worker-stopped" ]; then
      return 0
    fi
    sleep 1
  done
  echo "generation $generation did not publish graceful shutdown marker" >&2
  return 1
}

# Checks the client's closed three-line marker before using its parent run ID.
read_accepted_marker() {
  [ "$(wc -l <"$accepted" | tr -d ' ')" -eq 3 ] || return 1
  [ "$(grep -c '^workflow_id=' "$accepted")" -eq 1 ] || return 1
  [ "$(grep -c '^run_id=' "$accepted")" -eq 1 ] || return 1
  [ "$(grep -c '^child_workflow_id=' "$accepted")" -eq 1 ] || return 1
  [ "$(sed -n 's/^workflow_id=//p' "$accepted")" = "$parent_workflow_id" ] \
    || return 1
  [ "$(sed -n 's/^child_workflow_id=//p' "$accepted")" = "$child_workflow_id" ] \
    || return 1
  parent_run_id=$(sed -n 's/^run_id=//p' "$accepted")
  [ -n "$parent_run_id" ]
}

# Retrieves one exact history. Temporal CLI output is retained only until the
# payload-free normalizer has projected the fields required by the contract.
fetch_raw_history() {
  workflow_id=$1
  run_id=$2
  destination=$3
  compose run --rm --no-deps temporal-admin-tools temporal workflow show \
    --workflow-id "$workflow_id" --run-id "$run_id" \
    --namespace temporal-sdk-test --output json >"$destination" 2>/dev/null
}

# Prints a payload-free event timeline for a failed live run. This diagnostic
# is deliberately generated before Compose teardown and includes only event
# IDs, event types, and workflow-task failure causes; workflow payloads and
# server-controlled failure messages never enter the CI log.
report_history_timeline() {
  workflow_id=$1
  run_id=$2
  destination=$3
  role=$4
  [ -n "$run_id" ] || return 0
  fetch_raw_history "$workflow_id" "$run_id" "$destination" || return 0
  jq -r --arg role "$role" '
    def events: (.history.events // .history_events // .events
      // .workflowExecutionHistory.events // .workflow_execution_history.events);
    events[]
    | (.workflowTaskFailedEventAttributes
       // .workflow_task_failed_event_attributes // {}) as $failed
    | ["child failure replay history", $role,
       (.eventId // .event_id // "unknown"),
       (.eventType // .event_type // "unknown"),
       ($failed.cause // "-")]
    | @tsv
  ' "$destination"
}

# Extracts exactly one child run and initiation event from the parent's raw
# server history. Both known protobuf JSON field spellings are accepted, but
# missing, duplicated, or mismatched relationships fail closed.
derive_child_identity() {
  jq -er --arg expected_child "$child_workflow_id" '
    def field($object; $camel; $snake): ($object[$camel] // $object[$snake]);
    def events: (.history.events // .history_events // .events
      // .workflowExecutionHistory.events // .workflow_execution_history.events);
    [events[]
      | (.childWorkflowExecutionStartedEventAttributes
         // .child_workflow_execution_started_event_attributes // empty) as $a
      | (field($a; "workflowExecution"; "workflow_execution") // {}) as $e
      | select(field($e; "workflowId"; "workflow_id") == $expected_child)
      | {
          run_id: field($e; "runId"; "run_id"),
          initiated_event_id: field($a; "initiatedEventId"; "initiated_event_id")
        }]
    | if length == 1
         and (.[0].run_id | type) == "string"
         and .[0].run_id != ""
         and (.[0].initiated_event_id | type) == "string"
         and (.[0].initiated_event_id | test("^[1-9][0-9]{0,18}$"))
      then .[0]
      else error("parent history does not identify exactly one expected child run")
      end
  ' "$parent_raw"
}

# Normalizes one already-fetched exact history with the bilateral identity
# context required to retain parent/child linkage but no payload data.
normalize_history() {
  role=$1
  workflow_id=$2
  run_id=$3
  counterpart_workflow_id=$4
  counterpart_run_id=$5
  raw=$6
  output=$7
  sh "$normalizer" --role "$role" --workflow-id "$workflow_id" \
    --run-id "$run_id" --counterpart-workflow-id "$counterpart_workflow_id" \
    --counterpart-run-id "$counterpart_run_id" --output "$output" <"$raw"
}

# Validates an exact workflow/run pair with a separate describe RPC. History
# output may omit the current run ID, so this check precedes normalization.
check_execution_identity() {
  workflow_id=$1
  run_id=$2
  destination=$3
  compose run --rm --no-deps temporal-admin-tools temporal workflow describe \
    --workflow-id "$workflow_id" --run-id "$run_id" \
    --namespace temporal-sdk-test --output json >"$destination" 2>/dev/null
  sh "$identity_validator" --input "$destination" --workflow-id "$workflow_id" \
    --run-id "$run_id"
}

# Fetches and normalizes both histories, retrying while the server has not yet
# reached the stage-specific invariant. Validation is performed separately so
# callers can supply the appropriate initial/post-removal evidence.
capture_histories() {
  parent_output=$1
  child_output=$2
  fetch_raw_history "$parent_workflow_id" "$parent_run_id" "$parent_raw" || return 1
  fetch_raw_history "$child_workflow_id" "$child_run_id" "$child_raw" || return 1
  normalize_history parent "$parent_workflow_id" "$parent_run_id" \
    "$child_workflow_id" "$child_run_id" "$parent_raw" "$parent_output" || return 1
  normalize_history child "$child_workflow_id" "$child_run_id" \
    "$parent_workflow_id" "$parent_run_id" "$child_raw" "$child_output"
}

# Stops one worker through its public shutdown path and proves that the marker
# was emitted after the process exited successfully, then removes its container.
stop_and_remove_worker() {
  service=$1
  stopped_file=$2
  generation=$3
  container_id=$4
  rm -f "$stopped_file"
  # Native Core may consume its full 30-second graceful shutdown interval.
  # Leave ten additional seconds for the signal watcher, OCaml cleanup, and
  # atomic stopped-marker publication before Docker is allowed to force-kill.
  compose stop --timeout 40 "$service" >/dev/null
  wait_for_shutdown_marker "$stopped_file" "$generation" || return 1
  exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$container_id")
  [ "$exit_code" -eq 0 ] || return 1
  sh "$fixture/scripts/collect-live-diagnostics.sh" pre-worker-removal || echo "live diagnostic snapshot failed" >&2
  compose rm --force "$service" >/dev/null
  remaining_containers=$(docker ps -aq \
    --filter "label=com.docker.compose.project=$project" \
    --filter "label=com.docker.compose.service=$service")
  remaining=$(printf '%s\n' "$remaining_containers" \
    | sed '/^$/d' | wc -l | tr -d ' ')
  [ "$remaining" -eq 0 ]
}

remove_artifacts
compose down --volumes --remove-orphans >/dev/null 2>&1 || true
remaining_before_start=$(project_volume_count)
[ "$remaining_before_start" -eq 0 ] || {
  echo "parent/child replay project retained a PostgreSQL volume" >&2
  exit 1
}

compose up -d --wait --wait-timeout 600 postgresql temporal
compose exec -T postgresql pg_isready -U temporal -d postgres >/dev/null
compose run --rm --no-deps --entrypoint /bin/sh temporal-admin-tools \
  /scripts/check-temporal-stack.sh >/dev/null

compose up -d --build --force-recreate --wait --wait-timeout 600 "$worker_one"
generation_one_container=$(compose ps -q "$worker_one")
[ -n "$generation_one_container" ]

docker rm -f "$driver_container" >/dev/null 2>&1 || true
compose run --build --name "$driver_container" --no-deps \
  parent-child-restart-driver >"$driver_log" 2>&1 &
driver_pid=$!
wait_for_marker "$accepted" "$driver_pid" "child failure replay driver"
read_accepted_marker
# The child's timer is 60 seconds. Fail the scenario rather than allowing a
# slow controller to leave generation one alive beyond 45 seconds after the
# parent start was accepted; this preserves at least 75 seconds for graceful
# stop/removal before the durable timer can make either execution terminal.
initial_deadline_epoch=$(( $(date +%s) + 45 ))

# The parent history is the only authority for the child run ID. Poll until
# Temporal has durably recorded ChildWorkflowExecutionStarted.
child_identity=''
while [ "$(date +%s)" -lt "$initial_deadline_epoch" ]; do
  if fetch_raw_history "$parent_workflow_id" "$parent_run_id" "$parent_raw"; then
    child_identity=$(derive_child_identity 2>/dev/null || true)
    if [ -n "$child_identity" ]; then break; fi
  fi
  sleep 1
done
[ -n "$child_identity" ] || {
  echo "parent never recorded the expected child execution" >&2
  exit 1
}
child_run_id=$(printf '%s' "$child_identity" | jq -er '.run_id')
initiated_event_id=$(printf '%s' "$child_identity" | jq -er '.initiated_event_id')

wait_for_marker "$diagnostics" "$driver_pid" "generation-one diagnostics" \
  "$initial_deadline_epoch"

initial_valid=false
while [ "$(date +%s)" -lt "$initial_deadline_epoch" ]; do
  if capture_histories "$parent_initial" "$child_initial" \
    && sh "$validator" --stage initial \
      --parent-history "$parent_initial" --child-history "$child_initial" \
      --parent-workflow-id "$parent_workflow_id" --parent-run-id "$parent_run_id" \
      --child-workflow-id "$child_workflow_id" --child-run-id "$child_run_id" \
      --diagnostics "$diagnostics" --outcome failure \
      --child-workflow-type "$child_workflow_type" >/dev/null 2>&1; then
    initial_valid=true
    break
  fi
  sleep 1
done
[ "$initial_valid" = true ] || {
  echo "parent and child never reached the required pending histories" >&2
  exit 1
}

[ "$(date +%s)" -lt "$initial_deadline_epoch" ] || {
  echo "generation one exceeded the safety deadline before replacement" >&2
  exit 1
}

stop_and_remove_worker "$worker_one" "$worker_one_stopped" 1 \
  "$generation_one_container"

# Exact describe RPCs do not require a worker. Perform them only after the
# time-sensitive generation-one stop so slow CLI container startup cannot
# consume the durable-timer safety margin.
check_execution_identity "$parent_workflow_id" "$parent_run_id" "$parent_describe"
check_execution_identity "$child_workflow_id" "$child_run_id" "$child_describe"

# Capture both exact runs again with no worker present. These snapshots prove
# the histories remained nonterminal after generation one was fully removed.
post_removal_valid=false
for _attempt in $(seq 1 30); do
  if capture_histories "$parent_post_removal" "$child_post_removal" \
    && sh "$validator" --stage post-removal \
      --parent-history "$parent_post_removal" --child-history "$child_post_removal" \
      --parent-workflow-id "$parent_workflow_id" --parent-run-id "$parent_run_id" \
      --child-workflow-id "$child_workflow_id" --child-run-id "$child_run_id" \
      --diagnostics "$diagnostics" --parent-initial-history "$parent_initial" \
      --child-initial-history "$child_initial" --outcome failure \
      --child-workflow-type "$child_workflow_type" >/dev/null 2>&1; then
    post_removal_valid=true
    break
  fi
  sleep 1
done
[ "$post_removal_valid" = true ] || {
  echo "post-removal parent/child histories were not valid and nonterminal" >&2
  exit 1
}

SMOKE_PARENT_CHILD_REPLAY_PARENT_RUN_ID="$parent_run_id" \
  SMOKE_PARENT_CHILD_REPLAY_CHILD_RUN_ID="$child_run_id" \
  compose up -d --build --force-recreate --wait --wait-timeout 600 "$worker_two"
generation_two_container=$(compose ps -q "$worker_two")
[ -n "$generation_two_container" ]
[ "$generation_two_container" != "$generation_one_container" ]

# The replacement publishes the four-record document only after it has seen
# replay activations for both exact roles. A one-role observation cannot pass.
replay_valid=false
for _attempt in $(seq 1 180); do
  if jq -e --arg parent_run "$parent_run_id" --arg child_run "$child_run_id" '
      .parent.run_id == $parent_run and .child.run_id == $child_run
      and ([.records[] | [.role,.phase,.generation,.is_replaying]] ==
        [["parent","initial",1,false],["child","initial",1,false],
         ["parent","replay",2,true],["child","replay",2,true]])
    ' "$diagnostics" >/dev/null 2>&1; then
    replay_valid=true
    break
  fi
  if ! kill -0 "$driver_pid" 2>/dev/null && [ ! -s "$result" ]; then
    reap_driver || true
    tail -n 200 "$driver_log" 2>/dev/null >&2 || true
    echo "child failure replay driver exited with status $producer_status before both replay checkpoints" >&2
    exit 1
  fi
  sleep 1
done
[ "$replay_valid" = true ] || {
  echo "replacement worker did not publish both replay checkpoints" >&2
  exit 1
}

wait_for_marker "$result" "$driver_pid" "child failure replay result"
reap_driver
[ "$(cat "$result")" = "SMOKE:PARENT:CHILD:FAILURE_RECOVERED" ]
docker rm "$driver_container" >/dev/null

terminal_valid=false
for _attempt in $(seq 1 120); do
  if capture_histories "$parent_terminal" "$child_terminal" \
    && sh "$validator" --stage terminal \
      --parent-history "$parent_terminal" --child-history "$child_terminal" \
      --parent-workflow-id "$parent_workflow_id" --parent-run-id "$parent_run_id" \
      --child-workflow-id "$child_workflow_id" --child-run-id "$child_run_id" \
      --diagnostics "$diagnostics" --parent-initial-history "$parent_initial" \
      --child-initial-history "$child_initial" \
      --parent-post-removal-history "$parent_post_removal" \
      --child-post-removal-history "$child_post_removal" --outcome failure \
      --child-workflow-type "$child_workflow_type" >/dev/null 2>&1; then
    terminal_valid=true
    break
  fi
  sleep 1
done
[ "$terminal_valid" = true ] || {
  echo "terminal parent/child histories did not satisfy replay recovery" >&2
  exit 1
}

stop_and_remove_worker "$worker_two" "$worker_two_stopped" 2 \
  "$generation_two_container"
sh "$fixture/scripts/collect-live-diagnostics.sh" pre-stack-removal || echo "live diagnostic snapshot failed" >&2
compose down --volumes --remove-orphans >/dev/null
remaining_volumes=$(project_volume_count)
[ "$remaining_volumes" -eq 0 ]

parent_initial_count=$(jq -r '.events | length' "$parent_initial")
child_initial_count=$(jq -r '.events | length' "$child_initial")
parent_post_count=$(jq -r '.events | length' "$parent_post_removal")
child_post_count=$(jq -r '.events | length' "$child_post_removal")
parent_terminal_count=$(jq -r '.events | length' "$parent_terminal")
child_terminal_count=$(jq -r '.events | length' "$child_terminal")
parent_replay_length=$(jq -r '.records[] | select(.role == "parent" and .phase == "replay") | .history_length' "$diagnostics")
child_replay_length=$(jq -r '.records[] | select(.role == "child" and .phase == "replay") | .history_length' "$diagnostics")

jq -n --arg parent_workflow_id "$parent_workflow_id" \
  --arg parent_run_id "$parent_run_id" --arg child_workflow_id "$child_workflow_id" \
  --arg child_run_id "$child_run_id" --arg initiated_event_id "$initiated_event_id" \
  --arg generation_one_container "$generation_one_container" \
  --arg generation_two_container "$generation_two_container" \
  --arg parent_replay_length "$parent_replay_length" \
  --arg child_replay_length "$child_replay_length" \
  --argjson remaining_before_start "$remaining_before_start" \
  --argjson parent_initial_count "$parent_initial_count" \
  --argjson child_initial_count "$child_initial_count" \
  --argjson parent_post_count "$parent_post_count" \
  --argjson child_post_count "$child_post_count" \
  --argjson parent_terminal_count "$parent_terminal_count" \
  --argjson child_terminal_count "$child_terminal_count" \
  --argjson remaining_volumes "$remaining_volumes" '
  {
    parent_workflow_id:$parent_workflow_id,
    parent_run_id:$parent_run_id,
    child_workflow_id:$child_workflow_id,
    child_run_id:$child_run_id,
    initiated_event_id:$initiated_event_id,
    events:[
      {step:"stack_ready",status:"ok",temporal_healthy:true,remaining_project_volumes_before_start:$remaining_before_start},
      {step:"parent_driver_accepted",status:"ok",workflow_id:$parent_workflow_id,run_id:$parent_run_id},
      {step:"child_execution_observed",status:"ok",workflow_id:$child_workflow_id,run_id:$child_run_id,parent_workflow_id:$parent_workflow_id,parent_run_id:$parent_run_id,initiated_event_id:$initiated_event_id},
      {step:"history_checked",status:"ok",role:"parent",stage:"initial",event_count:$parent_initial_count},
      {step:"history_checked",status:"ok",role:"child",stage:"initial",event_count:$child_initial_count},
      {step:"worker_generation_stopped",status:"ok",generation:1,container_id:$generation_one_container,exit_code:0,shutdown_marker:true},
      {step:"worker_generation_removed",status:"ok",generation:1,container_id:$generation_one_container,remaining_worker_containers:0},
      {step:"history_checked",status:"ok",role:"parent",stage:"post_removal",event_count:$parent_post_count},
      {step:"history_checked",status:"ok",role:"child",stage:"post_removal",event_count:$child_post_count},
      {step:"worker_generation_ready",status:"ok",generation:2,container_id:$generation_two_container,readiness_generation:2,fresh_container:true},
      {step:"replay_observed",status:"ok",role:"parent",generation:2,is_replaying:true,history_length:$parent_replay_length},
      {step:"replay_observed",status:"ok",role:"child",generation:2,is_replaying:true,history_length:$child_replay_length},
      {step:"parent_driver_completed",status:"ok",workflow_id:$parent_workflow_id,run_id:$parent_run_id,outcome:"child_failure_recovered"},
      {step:"history_checked",status:"ok",role:"parent",stage:"terminal",event_count:$parent_terminal_count},
      {step:"history_checked",status:"ok",role:"child",stage:"terminal",event_count:$child_terminal_count},
      {step:"worker_generation_stopped",status:"ok",generation:2,container_id:$generation_two_container,exit_code:0,shutdown_marker:true},
      {step:"worker_generation_removed",status:"ok",generation:2,container_id:$generation_two_container,remaining_worker_containers:0},
      {step:"postgres_volume_removed",status:"ok",remaining_project_volumes:$remaining_volumes}
    ]
  }' >"$controller"

sh "$controller_validator" --controller "$controller" \
  --parent-workflow-id "$parent_workflow_id" --parent-run-id "$parent_run_id" \
  --child-workflow-id "$child_workflow_id" --child-run-id "$child_run_id" \
  --initiated-event-id "$initiated_event_id" \
  --expected-outcome child_failure_recovered

sh "$fixture/scripts/collect-live-diagnostics.sh" validated || echo "live diagnostic snapshot failed" >&2
trap - EXIT HUP INT TERM
remove_artifacts

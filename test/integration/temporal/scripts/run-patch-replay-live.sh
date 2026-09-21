#!/bin/sh
set -eu

# Runs the real-server workflow-patch lifecycle compatibility gate. Four
# physically different OCaml worker executables provide the legacy, active,
# deprecated, and removed source generations. The three scenarios prove the
# safe patch-in, deprecation, and removal transitions against durable history.

root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal"
compose_file="$fixture/compose.yaml"
project=${TEMPORAL_COMPOSE_PROJECT:-ocaml-temporal-integration}
patch_id=smoke.patch_replay_history.activity.v1
legacy_workflow_id=two-binary-patch-replay-legacy
new_workflow_id=two-binary-patch-replay-new
container_fixture=/workspace/test/integration/temporal

legacy_accepted="$fixture/.patch-replay-legacy-accepted"
legacy_result="$fixture/.patch-replay-legacy-result"
legacy_diagnostics="$fixture/.patch-replay-legacy-diagnostics.json"
legacy_initial="$fixture/.patch-replay-legacy-history.initial.json"
legacy_terminal="$fixture/.patch-replay-legacy-history.terminal.json"
legacy_raw="$fixture/.patch-replay-legacy-history.raw.json"
legacy_describe="$fixture/.patch-replay-legacy-describe.json"
legacy_log="$fixture/.patch-replay-legacy-driver.log"
legacy_stopped="$fixture/.patch-replay-legacy-worker-stopped"

new_accepted="$fixture/.patch-replay-new-accepted"
new_result="$fixture/.patch-replay-new-result"
new_diagnostics="$fixture/.patch-replay-new-diagnostics.json"
new_initial="$fixture/.patch-replay-new-history.initial.json"
new_terminal="$fixture/.patch-replay-new-history.terminal.json"
new_raw="$fixture/.patch-replay-new-history.raw.json"
new_describe="$fixture/.patch-replay-new-describe.json"
new_log="$fixture/.patch-replay-new-driver.log"
new_stopped="$fixture/.patch-replay-new-worker-stopped"

removal_workflow_id=two-binary-patch-replay-removal
removal_accepted="$fixture/.patch-replay-removal-accepted"
removal_result="$fixture/.patch-replay-removal-result"
removal_diagnostics="$fixture/.patch-replay-removal-diagnostics.json"
removal_initial="$fixture/.patch-replay-removal-history.initial.json"
removal_terminal="$fixture/.patch-replay-removal-history.terminal.json"
removal_raw="$fixture/.patch-replay-removal-history.raw.json"
removal_describe="$fixture/.patch-replay-removal-describe.json"
removal_log="$fixture/.patch-replay-removal-driver.log"
removal_stopped="$fixture/.patch-replay-removal-worker-stopped"

controller="$fixture/.patch-replay-controller.json"
driver_container="$project-patch-replay-driver"
normalizer="$fixture/scripts/normalize-patch-replay-history.sh"
validator="$fixture/scripts/validate-patch-replay.sh"
controller_validator="$fixture/scripts/validate-patch-replay-controller.sh"
identity_validator="$fixture/scripts/validate-restart-replay-identity.sh"

driver_pid=''

# Applies one normalized Compose invocation everywhere. The image and numeric
# identity are explicit so bind-mounted Dune output has the same ownership in
# every worker and client process.
compose() {
  OCAML_IMAGE=${OCAML_IMAGE:-ocaml/opam:debian-12-ocaml-5.2} \
    HOST_UID=${HOST_UID:-$(id -u)} HOST_GID=${HOST_GID:-$(id -g)} \
    SMOKE_DRIVER_TIMEOUT_SECONDS=${SMOKE_DRIVER_TIMEOUT_SECONDS:-300} \
    docker compose --project-directory "$fixture" --file "$compose_file" \
      --project-name "$project" --profile temporal "$@"
}

# Counts only volumes owned by this Compose project. The final zero assertion
# ensures the PostgreSQL state cannot leak into a later acceptance run.
project_volume_count() {
  docker volume ls -q --filter "label=com.docker.compose.project=$project" \
    | wc -l | tr -d ' '
}

# Removes host coordination files. These are bounded test artifacts rather
# than build caches, and deleting them on both success and failure prevents a
# stale marker from satisfying a later controller poll.
remove_artifacts() {
  rm -f "$legacy_accepted" "$legacy_result" "$legacy_diagnostics" \
    "$legacy_initial" "$legacy_terminal" "$legacy_raw" "$legacy_describe" \
    "$legacy_log" \
    "$legacy_stopped" "$new_accepted" "$new_result" "$new_diagnostics" \
    "$new_initial" "$new_terminal" "$new_raw" "$new_describe" "$new_log" \
    "$new_stopped" "$removal_accepted" "$removal_result" \
    "$removal_diagnostics" "$removal_initial" "$removal_terminal" \
    "$removal_raw" "$removal_describe" "$removal_log" "$removal_stopped" \
    "$controller"
}

# Prints only bounded process/server tails on failure, then always removes the
# stack, PostgreSQL volume, one-off driver, and coordination files.
cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  sh "$fixture/scripts/collect-live-diagnostics.sh" cleanup || echo "live diagnostic snapshot failed" >&2
  if [ -n "$driver_pid" ] && kill -0 "$driver_pid" 2>/dev/null; then
    kill -TERM "$driver_pid" 2>/dev/null || true
  fi
  docker rm -f "$driver_container" >/dev/null 2>&1 || true
  if [ "$status" -ne 0 ]; then
    printf '%s\n' '--- patch replay legacy driver ---' >&2
    tail -n 200 "$legacy_log" 2>/dev/null >&2 || true
    printf '%s\n' '--- patch replay new driver ---' >&2
    tail -n 200 "$new_log" 2>/dev/null >&2 || true
    printf '%s\n' '--- patch replay removal driver ---' >&2
    tail -n 200 "$removal_log" 2>/dev/null >&2 || true
    compose logs --no-color --tail 200 temporal patch-replay-legacy-worker \
      patch-replay-patched-worker patch-replay-deprecated-worker \
      patch-replay-removed-worker >&2 2>/dev/null || true
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  remove_artifacts
  exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Waits until one exact marker path exists, failing early if its producer has
# exited. This avoids spending the full CI timeout after a typed client error.
wait_for_marker() {
  marker=$1
  producer_pid=${2:-}
  label=$3
  for _attempt in $(seq 1 180); do
    if [ -s "$marker" ]; then return 0; fi
    if [ -n "$producer_pid" ] && ! kill -0 "$producer_pid" 2>/dev/null; then
      echo "$label exited before publishing $marker" >&2
      return 1
    fi
    sleep 1
  done
  echo "timed out waiting for $label marker: $marker" >&2
  return 1
}

# Reads the two-line accepted marker and binds the exact server run identity.
# Duplicate keys, a wrong workflow ID, or extra lines are rejected.
accepted_run_id() {
  marker=$1
  expected_workflow_id=$2
  [ "$(wc -l <"$marker" | tr -d ' ')" -eq 2 ] || return 1
  [ "$(sed -n 's/^workflow_id=//p' "$marker")" = "$expected_workflow_id" ] \
    || return 1
  run_id=$(sed -n 's/^run_id=//p' "$marker")
  [ -n "$run_id" ] || return 1
  [ "$(grep -c '^run_id=' "$marker")" -eq 1 ] || return 1
  printf '%s\n' "$run_id"
}

# Starts the selected source-version worker with an exact execution target and
# diagnostic generation. Compose's health gate accepts the container only
# after the OCaml executable has created its public Worker and marker.
start_worker() {
  service=$1
  workflow_id=$2
  generation=$3
  diagnostics_container=$4
  stopped_container=$5
  PATCH_REPLAY_WORKFLOW_ID=$workflow_id \
    PATCH_REPLAY_GENERATION=$generation \
    PATCH_REPLAY_DIAGNOSTICS_FILE=$diagnostics_container \
    PATCH_REPLAY_WORKER_STOPPED_FILE=$stopped_container \
    compose up -d --build --force-recreate --wait --wait-timeout 600 "$service"
  started_container=$(compose ps -q "$service")
  [ -n "$started_container" ] || {
    echo "$service has no container after readiness" >&2
    return 1
  }
}

# Requests graceful public Worker shutdown, verifies its post-shutdown marker,
# and removes the container before another source generation may poll the same
# queue. Counting every lifecycle worker service after removal proves that no
# source generation remains able to poll before its replacement starts.
stop_worker() {
  service=$1
  stopped_file=$2
  container_id=$3
  rm -f "$stopped_file"
  compose stop --timeout 30 "$service" >/dev/null
  [ "$(cat "$stopped_file" 2>/dev/null || true)" = "worker-stopped" ] || {
    echo "$service did not publish graceful shutdown" >&2
    return 1
  }
  exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$container_id")
  [ "$exit_code" -eq 0 ] || {
    echo "$service exited with $exit_code" >&2
    return 1
  }
  sh "$fixture/scripts/collect-live-diagnostics.sh" pre-worker-removal || echo "live diagnostic snapshot failed" >&2
  compose rm --force "$service" >/dev/null
  remaining=$(compose ps -aq patch-replay-legacy-worker \
    patch-replay-patched-worker patch-replay-deprecated-worker \
    patch-replay-removed-worker | wc -l | tr -d ' ')
  [ "$remaining" -eq 0 ] || {
    echo "a patch lifecycle worker still exists after removing $service" >&2
    return 1
  }
}

# Launches the independent OCaml client in the background so the controller
# can replace its worker while Client.wait remains attached to the exact run.
start_driver() {
  workflow_id=$1
  expected_result=$2
  accepted_container=$3
  result_container=$4
  log_file=$5
  docker rm -f "$driver_container" >/dev/null 2>&1 || true
  PATCH_REPLAY_EXECUTION_ID=$workflow_id \
    PATCH_REPLAY_EXPECTED_RESULT=$expected_result \
    PATCH_REPLAY_ACCEPTED_FILE=$accepted_container \
    PATCH_REPLAY_RESULT_FILE=$result_container \
    compose run --build --rm --name "$driver_container" --no-deps \
      patch-replay-driver >"$log_file" 2>&1 &
  driver_pid=$!
}

# Binds the driver's accepted run ID to a server response before history
# normalization. `workflow show` is allowed to omit run ID metadata, so its
# normalizer safely falls back to the requested value only after this separate
# exact-identity check has rejected an unrelated execution.
check_execution_identity() {
  workflow_id=$1
  run_id=$2
  describe_file=$3
  if ! compose run --rm --no-deps temporal-admin-tools \
    temporal workflow describe --workflow-id "$workflow_id" --run-id "$run_id" \
      --namespace temporal-sdk-test --output json >"$describe_file" 2>/dev/null; then
    echo "could not describe patch replay workflow/run identity" >&2
    return 1
  fi
  sh "$identity_validator" --input "$describe_file" --workflow-id "$workflow_id" \
    --run-id "$run_id"
}

# Captures the Temporal CLI's machine history and delegates all closed-shape,
# marker, activity-type, ordering, prefix, and replay checks to the dedicated
# strict normalizer/validator pair. The caller must already have established
# the exact workflow/run identity through [check_execution_identity].
capture_history() {
  mode=$1
  workflow_id=$2
  run_id=$3
  raw_file=$4
  output_file=$5
  initial_file=${6:-}
  diagnostics_file=${7:-}
  if ! compose run --rm --no-deps temporal-admin-tools \
    temporal workflow show --workflow-id "$workflow_id" --run-id "$run_id" \
      --namespace temporal-sdk-test --output json >"$raw_file" 2>/dev/null; then
    return 1
  fi
  if ! sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
    --output "$output_file" <"$raw_file"; then
    return 1
  fi
  set -- --mode "$mode" --history "$output_file" --workflow-id "$workflow_id" \
    --run-id "$run_id" --patch-id "$patch_id"
  if [ -n "$initial_file" ]; then set -- "$@" --initial-history "$initial_file"; fi
  if [ -n "$diagnostics_file" ]; then set -- "$@" --diagnostics "$diagnostics_file"; fi
  sh "$validator" "$@" >/dev/null 2>&1
}

# Polls until an initial history has reached its durable timer boundary or a
# terminal driver failure makes further waiting pointless.
wait_initial_history() {
  mode=$1
  workflow_id=$2
  run_id=$3
  raw_file=$4
  output_file=$5
  diagnostics_file=$6
  for _attempt in $(seq 1 120); do
    if capture_history "$mode" "$workflow_id" "$run_id" "$raw_file" \
      "$output_file" '' "$diagnostics_file"; then
      return 0
    fi
    if ! kill -0 "$driver_pid" 2>/dev/null; then return 1; fi
    sleep 1
  done
  return 1
}

# Requires the existing Native_worker diagnostic observer to show one initial
# generation and a replaying replacement for this exact run.
wait_replay_diagnostics() {
  diagnostics=$1
  run_id=$2
  for _attempt in $(seq 1 180); do
    if jq -e --arg run_id "$run_id" \
      '.run_id == $run_id
       and ([.records[].phase] == ["initial", "replay"])
       and .records[0].generation == 1
       and .records[0].is_replaying == false
       and .records[1].generation == 2
       and .records[1].is_replaying == true
       and .records[1].history_length != "0"' "$diagnostics" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$driver_pid" 2>/dev/null; then return 1; fi
    sleep 1
  done
  return 1
}

# Waits for exact completion, joins the driver, and validates its closed marker
# against the expected run and branch result.
finish_driver() {
  result_file=$1
  log_file=$2
  workflow_id=$3
  run_id=$4
  expected_result=$5
  wait_for_marker "$result_file" "$driver_pid" "patch replay driver"
  wait "$driver_pid"
  driver_pid=''
  [ "$(wc -l <"$result_file" | tr -d ' ')" -eq 3 ]
  [ "$(sed -n 's/^workflow_id=//p' "$result_file")" = "$workflow_id" ]
  [ "$(sed -n 's/^run_id=//p' "$result_file")" = "$run_id" ]
  [ "$(sed -n 's/^result=//p' "$result_file")" = "$expected_result" ]
  grep -F 'patch replay driver completed' "$log_file" >/dev/null
}

command -v jq >/dev/null 2>&1 || {
  echo "jq is required for patch replay acceptance" >&2
  exit 1
}

stale_project_volumes_before_cleanup=$(project_volume_count)
compose down --volumes --remove-orphans >/dev/null 2>&1 || true
remove_artifacts
remaining_project_volumes_before_start=$(project_volume_count)
[ "$remaining_project_volumes_before_start" -eq 0 ]

compose up -d --build --wait --wait-timeout 600 postgresql temporal
compose run --rm --no-deps --entrypoint /bin/sh temporal-admin-tools \
  /scripts/check-temporal-stack.sh >/dev/null

# Scenario one: create a genuine marker-free history with the legacy binary,
# then replay and complete it with the patched binary's false/old branch.
start_worker patch-replay-legacy-worker "$legacy_workflow_id" 1 \
  "$container_fixture/.patch-replay-legacy-diagnostics.json" \
  "$container_fixture/.patch-replay-legacy-worker-stopped"
legacy_generation_one_container=$started_container
start_driver "$legacy_workflow_id" PATCH_REPLAY:OLD_HISTORY \
  "$container_fixture/.patch-replay-legacy-accepted" \
  "$container_fixture/.patch-replay-legacy-result" "$legacy_log"
wait_for_marker "$legacy_accepted" "$driver_pid" "legacy driver"
legacy_run_id=$(accepted_run_id "$legacy_accepted" "$legacy_workflow_id")
check_execution_identity "$legacy_workflow_id" "$legacy_run_id" "$legacy_describe"
wait_initial_history legacy-initial "$legacy_workflow_id" "$legacy_run_id" \
  "$legacy_raw" "$legacy_initial" "$legacy_diagnostics"
legacy_initial_count=$(jq -r '.events | length' "$legacy_initial")
stop_worker patch-replay-legacy-worker "$legacy_stopped" \
  "$legacy_generation_one_container"

start_worker patch-replay-patched-worker "$legacy_workflow_id" 2 \
  "$container_fixture/.patch-replay-legacy-diagnostics.json" \
  "$container_fixture/.patch-replay-legacy-worker-stopped"
legacy_generation_two_container=$started_container
wait_replay_diagnostics "$legacy_diagnostics" "$legacy_run_id"
finish_driver "$legacy_result" "$legacy_log" "$legacy_workflow_id" \
  "$legacy_run_id" PATCH_REPLAY:OLD_HISTORY
capture_history legacy-terminal "$legacy_workflow_id" "$legacy_run_id" \
  "$legacy_raw" "$legacy_terminal" "$legacy_initial" "$legacy_diagnostics"
legacy_terminal_count=$(jq -r '.events | length' "$legacy_terminal")
legacy_replay_history=$(jq -r '.records[1].history_length' "$legacy_diagnostics")
legacy_marker_count=$(jq -r '[.events[] | select(.type == "MarkerRecorded")] | length' \
  "$legacy_terminal")
legacy_marker_deprecated=$(jq -c \
  '[.events[] | select(.type == "MarkerRecorded") | .deprecated]
   | if length == 0 then null else .[0] end' "$legacy_terminal")
stop_worker patch-replay-patched-worker "$legacy_stopped" \
  "$legacy_generation_two_container"

# Scenario two: create an active marker with patched code, then deploy the
# deprecation source. Replay must keep the original false marker unchanged
# while the new source runs the patched behavior without a branch decision.
start_worker patch-replay-patched-worker "$new_workflow_id" 1 \
  "$container_fixture/.patch-replay-new-diagnostics.json" \
  "$container_fixture/.patch-replay-new-worker-stopped"
new_generation_one_container=$started_container
start_driver "$new_workflow_id" PATCH_REPLAY:NEW_HISTORY \
  "$container_fixture/.patch-replay-new-accepted" \
  "$container_fixture/.patch-replay-new-result" "$new_log"
wait_for_marker "$new_accepted" "$driver_pid" "new-history driver"
new_run_id=$(accepted_run_id "$new_accepted" "$new_workflow_id")
check_execution_identity "$new_workflow_id" "$new_run_id" "$new_describe"
wait_initial_history new-initial "$new_workflow_id" "$new_run_id" \
  "$new_raw" "$new_initial" "$new_diagnostics"
new_initial_count=$(jq -r '.events | length' "$new_initial")
stop_worker patch-replay-patched-worker "$new_stopped" \
  "$new_generation_one_container"

start_worker patch-replay-deprecated-worker "$new_workflow_id" 2 \
  "$container_fixture/.patch-replay-new-diagnostics.json" \
  "$container_fixture/.patch-replay-new-worker-stopped"
new_generation_two_container=$started_container
wait_replay_diagnostics "$new_diagnostics" "$new_run_id"
finish_driver "$new_result" "$new_log" "$new_workflow_id" "$new_run_id" \
  PATCH_REPLAY:NEW_HISTORY
capture_history new-terminal "$new_workflow_id" "$new_run_id" "$new_raw" \
  "$new_terminal" "$new_initial" "$new_diagnostics"
new_terminal_count=$(jq -r '.events | length' "$new_terminal")
new_replay_history=$(jq -r '.records[1].history_length' "$new_diagnostics")
new_marker_count=$(jq -r '[.events[] | select(.type == "MarkerRecorded")] | length' \
  "$new_terminal")
new_marker_deprecated=$(jq -c \
  '[.events[] | select(.type == "MarkerRecorded") | .deprecated]
   | if length == 0 then null else .[0] end' "$new_terminal")
stop_worker patch-replay-deprecated-worker "$new_stopped" \
  "$new_generation_two_container"

# Scenario three: start a fresh history with the deprecation source so Core
# records a true marker, then remove all patch calls from generation two. The
# retained marker and exact initial prefix prove that removal is replay-safe.
start_worker patch-replay-deprecated-worker "$removal_workflow_id" 1 \
  "$container_fixture/.patch-replay-removal-diagnostics.json" \
  "$container_fixture/.patch-replay-removal-worker-stopped"
removal_generation_one_container=$started_container
start_driver "$removal_workflow_id" PATCH_REPLAY:NEW_HISTORY \
  "$container_fixture/.patch-replay-removal-accepted" \
  "$container_fixture/.patch-replay-removal-result" "$removal_log"
wait_for_marker "$removal_accepted" "$driver_pid" "removal driver"
removal_run_id=$(accepted_run_id "$removal_accepted" "$removal_workflow_id")
check_execution_identity "$removal_workflow_id" "$removal_run_id" \
  "$removal_describe"
wait_initial_history removal-initial "$removal_workflow_id" "$removal_run_id" \
  "$removal_raw" "$removal_initial" "$removal_diagnostics"
removal_initial_count=$(jq -r '.events | length' "$removal_initial")
stop_worker patch-replay-deprecated-worker "$removal_stopped" \
  "$removal_generation_one_container"

start_worker patch-replay-removed-worker "$removal_workflow_id" 2 \
  "$container_fixture/.patch-replay-removal-diagnostics.json" \
  "$container_fixture/.patch-replay-removal-worker-stopped"
removal_generation_two_container=$started_container
wait_replay_diagnostics "$removal_diagnostics" "$removal_run_id"
finish_driver "$removal_result" "$removal_log" "$removal_workflow_id" \
  "$removal_run_id" PATCH_REPLAY:NEW_HISTORY
capture_history removal-terminal "$removal_workflow_id" "$removal_run_id" \
  "$removal_raw" "$removal_terminal" "$removal_initial" "$removal_diagnostics"
removal_terminal_count=$(jq -r '.events | length' "$removal_terminal")
removal_replay_history=$(jq -r '.records[1].history_length' \
  "$removal_diagnostics")
removal_marker_count=$(jq -r \
  '[.events[] | select(.type == "MarkerRecorded")] | length' \
  "$removal_terminal")
removal_marker_deprecated=$(jq -c \
  '[.events[] | select(.type == "MarkerRecorded") | .deprecated]
   | if length == 0 then null else .[0] end' "$removal_terminal")
stop_worker patch-replay-removed-worker "$removal_stopped" \
  "$removal_generation_two_container"

sh "$fixture/scripts/collect-live-diagnostics.sh" pre-stack-removal || echo "live diagnostic snapshot failed" >&2
compose down --volumes --remove-orphans >/dev/null
remaining_project_volumes=$(project_volume_count)
[ "$remaining_project_volumes" -eq 0 ]

# The controller document contains only lifecycle metadata already validated
# above. Its closed schema makes the final CI artifact independently auditable
# without copying workflow inputs, activity payloads, or arbitrary history.
jq -n \
  --arg legacy_workflow_id "$legacy_workflow_id" --arg legacy_run_id "$legacy_run_id" \
  --arg new_workflow_id "$new_workflow_id" --arg new_run_id "$new_run_id" \
  --arg removal_workflow_id "$removal_workflow_id" \
  --arg removal_run_id "$removal_run_id" \
  --arg legacy_one "$legacy_generation_one_container" \
  --arg legacy_two "$legacy_generation_two_container" \
  --arg new_one "$new_generation_one_container" \
  --arg new_two "$new_generation_two_container" \
  --arg removal_one "$removal_generation_one_container" \
  --arg removal_two "$removal_generation_two_container" \
  --arg legacy_replay_history "$legacy_replay_history" \
  --arg new_replay_history "$new_replay_history" \
  --arg removal_replay_history "$removal_replay_history" \
  --argjson legacy_initial_count "$legacy_initial_count" \
  --argjson legacy_terminal_count "$legacy_terminal_count" \
  --argjson new_initial_count "$new_initial_count" \
  --argjson new_terminal_count "$new_terminal_count" \
  --argjson removal_initial_count "$removal_initial_count" \
  --argjson removal_terminal_count "$removal_terminal_count" \
  --argjson legacy_marker_count "$legacy_marker_count" \
  --argjson new_marker_count "$new_marker_count" \
  --argjson removal_marker_count "$removal_marker_count" \
  --argjson legacy_marker_deprecated "$legacy_marker_deprecated" \
  --argjson new_marker_deprecated "$new_marker_deprecated" \
  --argjson removal_marker_deprecated "$removal_marker_deprecated" \
  --argjson stale_volumes "$stale_project_volumes_before_cleanup" \
  --argjson starting_volumes "$remaining_project_volumes_before_start" \
  --argjson remaining_volumes "$remaining_project_volumes" \
  '{legacy_workflow_id:$legacy_workflow_id,legacy_run_id:$legacy_run_id,
    new_workflow_id:$new_workflow_id,new_run_id:$new_run_id,
    removal_workflow_id:$removal_workflow_id,removal_run_id:$removal_run_id,events:[
    {step:"stack_ready",status:"ok",stale_project_volumes_before_cleanup:$stale_volumes,remaining_project_volumes_before_start:$starting_volumes,temporal_healthy:true},
    {step:"driver_accepted",status:"ok",scenario:"legacy",workflow_id:$legacy_workflow_id,run_id:$legacy_run_id},
    {step:"history_checked",status:"ok",scenario:"legacy",stage:"initial",event_count:$legacy_initial_count},
    {step:"worker_generation_stopped",status:"ok",scenario:"legacy",generation:1,container_id:$legacy_one,worker_version:"legacy",exit_code:0,shutdown_marker:true},
    {step:"worker_generation_removed",status:"ok",scenario:"legacy",generation:1,container_id:$legacy_one,remaining_worker_containers:0},
    {step:"worker_generation_ready",status:"ok",scenario:"legacy",generation:2,container_id:$legacy_two,worker_version:"patched",readiness_generation:2,fresh_container:true},
    {step:"replay_observed",status:"ok",scenario:"legacy",generation:2,is_replaying:true,history_length:$legacy_replay_history,branch:"old",marker_count:$legacy_marker_count,marker_deprecated:$legacy_marker_deprecated},
    {step:"driver_completed",status:"ok",scenario:"legacy",outcome:"completed"},
    {step:"history_checked",status:"ok",scenario:"legacy",stage:"terminal",event_count:$legacy_terminal_count},
    {step:"worker_generation_stopped",status:"ok",scenario:"legacy",generation:2,container_id:$legacy_two,worker_version:"patched",exit_code:0,shutdown_marker:true},
    {step:"worker_generation_removed",status:"ok",scenario:"legacy",generation:2,container_id:$legacy_two,remaining_worker_containers:0},
    {step:"driver_accepted",status:"ok",scenario:"new",workflow_id:$new_workflow_id,run_id:$new_run_id},
    {step:"history_checked",status:"ok",scenario:"new",stage:"initial",event_count:$new_initial_count},
    {step:"worker_generation_stopped",status:"ok",scenario:"new",generation:1,container_id:$new_one,worker_version:"patched",exit_code:0,shutdown_marker:true},
    {step:"worker_generation_removed",status:"ok",scenario:"new",generation:1,container_id:$new_one,remaining_worker_containers:0},
    {step:"worker_generation_ready",status:"ok",scenario:"new",generation:2,container_id:$new_two,worker_version:"deprecated",readiness_generation:2,fresh_container:true},
    {step:"replay_observed",status:"ok",scenario:"new",generation:2,is_replaying:true,history_length:$new_replay_history,branch:"new",marker_count:$new_marker_count,marker_deprecated:$new_marker_deprecated},
    {step:"driver_completed",status:"ok",scenario:"new",outcome:"completed"},
    {step:"history_checked",status:"ok",scenario:"new",stage:"terminal",event_count:$new_terminal_count},
    {step:"worker_generation_stopped",status:"ok",scenario:"new",generation:2,container_id:$new_two,worker_version:"deprecated",exit_code:0,shutdown_marker:true},
    {step:"worker_generation_removed",status:"ok",scenario:"new",generation:2,container_id:$new_two,remaining_worker_containers:0},
    {step:"driver_accepted",status:"ok",scenario:"removal",workflow_id:$removal_workflow_id,run_id:$removal_run_id},
    {step:"history_checked",status:"ok",scenario:"removal",stage:"initial",event_count:$removal_initial_count},
    {step:"worker_generation_stopped",status:"ok",scenario:"removal",generation:1,container_id:$removal_one,worker_version:"deprecated",exit_code:0,shutdown_marker:true},
    {step:"worker_generation_removed",status:"ok",scenario:"removal",generation:1,container_id:$removal_one,remaining_worker_containers:0},
    {step:"worker_generation_ready",status:"ok",scenario:"removal",generation:2,container_id:$removal_two,worker_version:"removed",readiness_generation:2,fresh_container:true},
    {step:"replay_observed",status:"ok",scenario:"removal",generation:2,is_replaying:true,history_length:$removal_replay_history,branch:"new",marker_count:$removal_marker_count,marker_deprecated:$removal_marker_deprecated},
    {step:"driver_completed",status:"ok",scenario:"removal",outcome:"completed"},
    {step:"history_checked",status:"ok",scenario:"removal",stage:"terminal",event_count:$removal_terminal_count},
    {step:"worker_generation_stopped",status:"ok",scenario:"removal",generation:2,container_id:$removal_two,worker_version:"removed",exit_code:0,shutdown_marker:true},
    {step:"worker_generation_removed",status:"ok",scenario:"removal",generation:2,container_id:$removal_two,remaining_worker_containers:0},
    {step:"postgres_volume_removed",status:"ok",remaining_project_volumes:$remaining_volumes}
  ]}' >"$controller"

sh "$controller_validator" --controller "$controller" \
  --legacy-workflow-id "$legacy_workflow_id" --legacy-run-id "$legacy_run_id" \
  --new-workflow-id "$new_workflow_id" --new-run-id "$new_run_id" \
  --removal-workflow-id "$removal_workflow_id" --removal-run-id "$removal_run_id"

printf '%s\n' 'workflow patch replay acceptance: ok'

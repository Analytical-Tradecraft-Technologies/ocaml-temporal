#!/bin/sh
set -eu

# Runs the shipped example application (examples/README.md) end to end against
# the already-started Compose Temporal stack (#798). The activity worker and
# the workflow worker are separate long-lived containers sharing one task
# queue; the client is a one-shot container. The caller owns the server
# lifecycle; this script owns, and always removes, only its three example
# containers.
#
# Assertions:
# - a valid name completes and the client prints exactly the documented
#   two-line result on stdout and exits 0;
# - a blank name fails the workflow (not only its task) and the client exits 1
#   with the typed business failure, proving it does not hang on retries;
# - both workers stay alive while serving, then exit 0 with their clean-stop
#   line after SIGTERM, which exercises the public graceful shutdown path.
#
# The executables must already exist under _build/default: the Makefile target
# compiles them locally, and CI unpacks the verified smoke artifact. Running
# the binaries directly avoids `dune exec`, whose build lock would serialize
# concurrently started processes.
root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
project=${TEMPORAL_COMPOSE_PROJECT:-ocaml-temporal-integration}
image=${TEMPORAL_EXAMPLES_IMAGE:-$project-dev}
# Seconds. The client bound covers two activities, a 250 ms timer and worker
# startup; the worker bound only guarantees a stuck process cannot outlive CI.
client_timeout=${TEMPORAL_EXAMPLES_CLIENT_TIMEOUT_SECONDS:-180}
worker_timeout=${TEMPORAL_EXAMPLES_WORKER_TIMEOUT_SECONDS:-900}
evidence="$root/_build/examples-live"
# A per-run queue and workflow IDs keep a reused namespace from returning a
# previous run's result (the client's default workflow ID is fixed).
suffix="$(date +%s)-$$"
task_queue="ocaml-temporal-example-live-$suffix"
activity_worker="$project-example-activity-worker"
workflow_worker="$project-example-workflow-worker"
client="$project-example-client"
activity_executable=examples/activity_worker/activity_worker.exe
workflow_executable=examples/workflow_worker/workflow_worker.exe
client_executable=examples/client/client.exe

rm -rf "$evidence"
mkdir -p "$evidence"

for executable in "$activity_executable" "$workflow_executable" "$client_executable"; do
  test -x "$root/_build/default/$executable" || {
    echo "missing compiled example executable: $executable" >&2
    exit 1
  }
done

# Runs one example executable in the development image on the Compose network
# with the environment documented in examples/README.md. Arguments are: the
# container name, a timeout in seconds, the repository-relative executable,
# then any further `docker run` options, a literal `--`, and program arguments.
# `--init` forwards `docker stop`'s SIGTERM through `timeout` to the program.
example() {
  container=$1
  limit=$2
  executable=$3
  shift 3
  options=''
  while [ "$#" -gt 0 ] && [ "$1" != -- ]; do
    options="$options $1"
    shift
  done
  [ "$#" -gt 0 ] && shift
  # The collected options contain no whitespace-bearing values by construction.
  # shellcheck disable=SC2086
  docker run $options --name "$container" --init \
    --network "${project}_temporal-network" \
    --user "${HOST_UID:-$(id -u)}:${HOST_GID:-$(id -g)}" \
    --volume "$root:/workspace" --workdir /workspace \
    --env TEMPORAL_ADDRESS=http://temporal:7233 \
    --env TEMPORAL_NAMESPACE=temporal-sdk-test \
    --env TEMPORAL_TASK_QUEUE="$task_queue" \
    --entrypoint timeout "$image" \
    --signal=TERM --kill-after=10s "$limit" \
    "/workspace/_build/default/$executable" "$@"
}

# Preserves worker logs for diagnosis and removes only this script's
# containers, whatever the exit path.
cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  for container in "$activity_worker" "$workflow_worker"; do
    docker logs "$container" >"$evidence/$container.log" 2>&1 || true
  done
  if [ "$status" -ne 0 ]; then
    for log in "$evidence"/*.log "$evidence"/*.stdout "$evidence"/*.stderr; do
      [ -f "$log" ] || continue
      printf '%s\n' "--- ${log##*/} ---"
      tail -n 100 "$log"
    done
  fi
  docker rm -f "$activity_worker" "$workflow_worker" "$client" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
docker rm -f "$activity_worker" "$workflow_worker" "$client" >/dev/null 2>&1 || true

# Fails when a worker container has exited, so a startup error is reported as
# such rather than as an unrelated client timeout.
require_running() {
  for container in "$activity_worker" "$workflow_worker"; do
    running=$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || echo false)
    if [ "$running" != true ]; then
      echo "example worker exited early: $container" >&2
      exit 1
    fi
  done
}

# Same startup order as examples/README.md.
example "$activity_worker" "$worker_timeout" "$activity_executable" --detach -- >/dev/null
example "$workflow_worker" "$worker_timeout" "$workflow_executable" --detach -- >/dev/null

# Success path: exact documented output on stdout and a zero exit status.
status=0
example "$client" "$client_timeout" "$client_executable" --rm \
  --env "TEMPORAL_WORKFLOW_ID=example-live-success-$suffix" -- "Ada Lovelace" \
  >"$evidence/client-success.stdout" 2>"$evidence/client-success.stderr" || status=$?
if [ "$status" -ne 0 ]; then
  echo "example client exited with status $status for a valid name" >&2
  require_running
  exit 1
fi
printf 'Workflow completed:\nHello, Ada Lovelace!\nNext: review the Temporal result for Ada Lovelace.\n' \
  >"$evidence/client-success.expected"
if ! cmp -s "$evidence/client-success.expected" "$evidence/client-success.stdout"; then
  echo "example client output differs from the documented result" >&2
  diff "$evidence/client-success.expected" "$evidence/client-success.stdout" >&2 || true
  exit 1
fi
require_running

# Failure path: invalid input must fail the workflow execution with a typed,
# non-retryable business error; a retried workflow task would hit the timeout.
status=0
example "$client" "$client_timeout" "$client_executable" --rm \
  --env "TEMPORAL_WORKFLOW_ID=example-live-invalid-$suffix" -- " " \
  >"$evidence/client-invalid.stdout" 2>"$evidence/client-invalid.stderr" || status=$?
if [ "$status" -ne 1 ]; then
  echo "example client exited with status $status for a blank name; expected 1" >&2
  exit 1
fi
if ! grep -F 'Workflow failed: a name is required' "$evidence/client-invalid.stderr" >/dev/null; then
  echo "example client did not report the workflow's invalid-input failure" >&2
  exit 1
fi
require_running

# Graceful teardown: SIGTERM must reach Worker shutdown and exit 0.
for container in "$activity_worker" "$workflow_worker"; do
  docker stop --time 30 "$container" >/dev/null
  exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$container")
  docker logs "$container" >"$evidence/$container.log" 2>&1
  if [ "$exit_code" != 0 ]; then
    echo "example worker $container exited with status $exit_code after SIGTERM" >&2
    exit 1
  fi
done
grep -Fx 'activity worker stopped cleanly' "$evidence/$activity_worker.log" >/dev/null || {
  echo "activity worker did not report a clean stop" >&2
  exit 1
}
grep -Fx 'workflow worker stopped cleanly' "$evidence/$workflow_worker.log" >/dev/null || {
  echo "workflow worker did not report a clean stop" >&2
  exit 1
}

cat "$evidence/client-success.stdout"
printf 'Example application acceptance passed. Logs: %s\n' "$evidence"

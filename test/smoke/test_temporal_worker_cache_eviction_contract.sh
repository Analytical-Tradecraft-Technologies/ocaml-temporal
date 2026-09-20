#!/bin/sh
set -eu

# Protects the focused live cache-eviction gate without requiring Docker. The
# contract checks the public configuration seam, the separate client driver,
# the one-slot controller, and the closed payload-free marker shape used after
# a real Core RemoveFromCache activation.
root=${1:-$(CDPATH="" cd -- "$(dirname "$0")/../.." && pwd)}
makefile="$root/Makefile"
worker="$root/test/integration/temporal/worker/smoke_worker.ml"
driver="$root/test/integration/temporal/driver/cache_eviction_driver.ml"
driver_dune="$root/test/integration/temporal/driver/dune"
fixture="$root/test/integration/temporal/fixtures/cache-eviction/marker.json"
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-cache-eviction.XXXXXX")
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM

require_source() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "cache eviction contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

# B is diagnostic only. Pinned Core ordering buffers B until it has delivered
# and received A's cache-full removal acknowledgement, so B's normal
# completion can never prove that eviction occurred. A's completion marker is
# the admission barrier; it is written after Core acknowledges the first
# activation and avoids making this cache test depend on query-task routing.
reject_source() {
  path=$1
  needle=$2
  if grep -F -- "$needle" "$path" >/dev/null; then
    echo "cache eviction contract retained forbidden source: $needle ($path)" >&2
    exit 1
  fi
}

require_source "$makefile" 'test-temporal-worker-cache-eviction:'
require_source "$makefile" 'SMOKE_WORKER_MAX_CACHED_WORKFLOWS=1'
require_source "$makefile" 'SMOKE_CACHE_EVICTION_TIMEOUT_SECONDS'
require_source "$makefile" 'SMOKE_WORKER_CACHE_EVICTION_FILE='
require_source "$makefile" 'SMOKE_WORKER_CACHE_EVICTION_READY_FILE='
require_source "$makefile" 'SMOKE_WORKER_CACHE_EVICTION_SECOND_READY_FILE='
require_source "$makefile" 'SMOKE_CACHE_EVICTION_READY_FILE'
require_source "$makefile" 'SMOKE_CACHE_EVICTION_SECOND_READY_FILE'
require_source "$makefile" 'SMOKE_REPLAY_WORKFLOW_ID=two-binary-cache-eviction-a'
require_source "$makefile" 'smoke-cache-eviction-driver'
require_source "$makefile" 'cleanup_cache_worker()'
require_source "$worker" 'Worker.create ?max_cached_workflows'
require_source "$driver_dune" '(name cache_eviction_driver)'
require_source "$driver" 'wait_for_eviction_with_second_diagnostic'
require_source "$driver" 'cache_settling'
require_source "$driver" 'wait_for_marker ~path:ready'
require_source "$driver" 'SMOKE_CACHE_EVICTION_READY_FILE'
require_source "$driver" 'SMOKE_CACHE_EVICTION_SECOND_READY_FILE'
require_source "$driver" 'second workflow was acknowledged but A cache-full eviction marker was not published'
reject_source "$driver" 'phase "ready_b"'
require_source "$driver" 'two-binary-cache-eviction-a'
require_source "$driver" 'two-binary-cache-eviction-b'
require_source "$driver" 'Client.cancel ~request_id ~reason:'
require_source "$driver" 'Client.wait first'
require_source "$driver" 'Client.wait second'
require_source "$root/lib/public/worker.mli" '?max_cached_workflows:int'
require_source "$root/test/integration/temporal/observer/acceptance_observer.ml" '| Some "" -> Ok None'
require_source "$root/test/integration/temporal/observer/acceptance_observer.ml" 'SMOKE_WORKER_CACHE_EVICTION_READY_FILE'
require_source "$root/test/integration/temporal/observer/acceptance_observer.ml" '| Some "cache_full" ->'
require_source "$root/test/integration/temporal/observer/acceptance_observer.ml" 'let cache_fixture_replay ='
require_source "$root/test/integration/temporal/observer/acceptance_observer.ml" 'if cache_fixture_replay then begin'
require_source "$root/test/integration/temporal/observer/acceptance_observer.ml" 'remember_target_identity info'
reject_source "$root/test/integration/temporal/observer/acceptance_observer.ml" \
  '| None when not info.is_replaying ->'
require_source "$root/lib/runtime/native_worker_execution.ml" 'cache_removal_reason'
require_source "$root/lib/runtime/native_worker_execution.ml" 'on_completion'
require_source "$root/test/integration/temporal/common/smoke_definitions.ml" \
  'Temporal.Workflow.sleep (Temporal.Duration.of_ms 3_600_000L)'
require_source "$root/test/integration/temporal/common/smoke_definitions.ml" \
  'smoke.cache_eviction_residency'
reject_source "$root/test/integration/temporal/common/smoke_definitions.ml" \
  'fun () -> Ok false'

expected='{"workflow_id":"two-binary-cache-eviction-a","run_id":"22222222-2222-4222-8222-222222222222","reason":"cache_full"}'
normalized=$(tr -d '[:space:]' <"$fixture")
if [ "$normalized" != "$expected" ]; then
  echo "cache eviction fixture does not match its strict marker schema" >&2
  exit 1
fi

invalid="$temporary_directory/invalid-marker.json"
sed 's/cache_full/lang_requested/' "$fixture" >"$invalid"
if [ "$(tr -d '[:space:]' <"$invalid")" = "$expected" ]; then
  echo "cache eviction marker contract accepted the wrong eviction reason" >&2
  exit 1
fi

echo "temporal worker cache eviction contract: ok"

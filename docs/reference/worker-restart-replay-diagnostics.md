# Worker restart/replay diagnostic contract

**Status: implemented and live-verified for graceful replacement and retry in the [PR #298 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291), and for forced crash recovery in the [PR #306 CI run](https://github.com/mfow/ocaml-temporal/actions/runs/29356904816).** This document defines the small, payload-free records used by the real-Temporal restart test to coordinate a worker replacement. The private worker activation callback and the machine-readable Temporal CLI adapter are implemented. The contract target and the Compose controller both passed: the run observed the exact run, replacement, replay marker, retrying activity's attempt-two result, terminal result, normalized history, and volume cleanup. Crash mode additionally requires generation one to exit with status 137 and leave no graceful-shutdown marker. The earlier [PR #253 run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471) remains historical evidence for the original two-generation path. A deliberately failing run for bounded diagnostic preservation remains separate follow-up work.

## Why there is a normalized document

The Temporal CLI's normal history output is intended for people, not a stable
controller protocol. The controller never greps display text. Its admin-tools
container requests JSON, and
[`normalize-history.sh`](../../test/integration/temporal/scripts/normalize-history.sh)
converts the response into the normalized JSON document described by
[`restart-replay-history.schema.json`](../schemas/acceptance/restart-replay-history.schema.json).
The script preserves event IDs and order, omits payload bytes, rejects unknown
event types and unsafe numeric event IDs, and writes the document atomically
before the validator reads it. `workflow describe` separately validates the
exact workflow/run identity because a history event does not carry the current
run ID in every CLI shape.

The normalized history document contains only the workflow ID, the exact run ID
returned by `Client.start`, and a bounded list of event IDs and event type
names. It is a projection, not a replacement for Temporal history: event
attributes are intentionally excluded because the acceptance test only needs
ordering and identity at this synchronization boundary. Event IDs are
canonical decimal strings rather than JSON numbers. This avoids the precision
loss that JavaScript-style JSON parsers introduce for Temporal's signed 64-bit
values.

The separate worker diagnostics document (`diagnostics.json`) carries each
generation's `history_length`, also as a canonical decimal string. Its shape is
defined by
[`restart-replay-diagnostics.schema.json`](../schemas/acceptance/restart-replay-diagnostics.schema.json),
not by the normalized history schema.

## Validation stages

The validator is
[`validate-restart-replay.sh`](../../test/integration/temporal/scripts/validate-restart-replay.sh).
It takes `--stage initial` or `--stage terminal`:

```sh
validate-restart-replay.sh \
  --history history.json \
  --initial-history initial-history.json \
  --workflow-id two-binary-worker-restart-replay \
  --run-id <exact-start-run-id> \
  --stage terminal
```

The initial stage requires the ordered subsequence
`WorkflowExecutionStarted`, `WorkflowTaskCompleted`, and `TimerStarted`. It
also rejects `TimerFired`, activity, and terminal events, so the controller
cannot mistake an already-completed workflow for a safe pending-timer
boundary. The terminal stage takes the initial document through
`--initial-history`; it validates that document with the same initial-stage
rules and requires the terminal document to begin with the exact same ordered
event prefix. Matching workflow and run IDs alone is not enough because a
history adapter that returned a different event list for the same IDs could
otherwise turn an unrelated completion into restart/replay evidence.

The terminal stage requires the ordered subsequence
`WorkflowExecutionStarted`, `WorkflowTaskCompleted`, `TimerStarted`,
`TimerFired`, `ActivityTaskScheduled`, `ActivityTaskStarted`,
`ActivityTaskCompleted`, and `WorkflowExecutionCompleted`, with the
workflow-completed event last. Temporal compacts intermediate activity retry
events, so the final `ActivityTaskStarted`/`ActivityTaskCompleted` pair is the
persisted outcome of the retrying activity. The driver's exact
`SMOKE:AFTER-REPLAY:ATTEMPT:2` result supplies the attempt number that the
payload-free history projection intentionally omits; other known
scheduling/started events may appear between these boundaries.
Event IDs must be positive canonical decimal strings in strict ascending order
in both documents, and the document's workflow/run IDs must exactly match the
driver record. The JSON Schemas describe each document independently; the
validator enforces this cross-document prefix relationship because JSON Schema
cannot compare two separate files.

## Replay diagnostics

The worker diagnostics document follows
[`restart-replay-diagnostics.schema.json`](../schemas/acceptance/restart-replay-diagnostics.schema.json):

```json
{
  "workflow_id": "two-binary-worker-restart-replay",
  "run_id": "<exact-start-run-id>",
  "records": [
    {"phase": "initial", "generation": 1, "is_replaying": false, "history_length": "5"},
    {"phase": "replay", "generation": 2, "is_replaying": true, "history_length": "5"}
  ]
}
```

The first record proves only that generation 1 observed the execution. The
second record is required for a replay claim: it must use generation 2, repeat
the exact run identity, set `is_replaying` to `true`, and report a positive
history length. The record contains no input, output, payload, timestamp, or
process identifier. The validator also checks that each record has exactly the
documented fields and that generation/flag combinations cannot be swapped.

The private callback writes this JSON document directly using an atomic
same-directory replacement; no human-readable worker log is parsed for replay
proof. If the worker cannot provide the replay bit, a successful terminal
result may be described as **worker restarted and continued** only; it must not
be reported as replay evidence.

## Failure interpretation

The replacement lifecycle is not successful merely because generation two
starts or because Temporal returns a workflow-task response. A workflow-task
failure, including a nondeterminism failure, is a failed acceptance outcome:
it means that the replacement worker did not produce a command sequence that
Temporal could apply to the recorded history. The controller must not turn
that failure into `replay_observed`, `history_checked` at `terminal`, or
`driver_completed` evidence.

When a replay attempt fails, preserve the exact workflow/run identity, worker
generation, normalized history, and server failure message together. Inspect
the first history event at which the replayed command sequence diverged, then
fix the workflow or bridge contract before updating any positive fixture. A
cache-eviction activation followed by a nondeterminism failure is therefore
not evidence that eviction or replacement replay worked; it is a failed
scenario that still requires investigation.

The current controller schema intentionally describes only successful
lifecycle records, so it does not encode a server failure as a successful
phase. The live harness must still perform its normal cleanup and return a
nonzero status. A future bounded-diagnostics extension may add a closed
failure record, but until that exists the raw server and worker diagnostics
are the authoritative evidence for a failed run.

## Controller lifecycle evidence

The controller also needs to prove that it replaced a worker while the exact
workflow run was still pending. That evidence is separate from the history
projection and the worker replay records: history describes what Temporal
recorded, diagnostics describe what a worker observed, and the controller
document describes what the test harness did. Its closed, ordered shape is
defined by
[`restart-replay-controller.schema.json`](../schemas/acceptance/restart-replay-controller.schema.json).

The checked-in fixture contains exactly thirteen records, in this order:

1. `stack_ready` records how many project volumes were found before cleanup,
   then proves that cleanup left zero volumes before Temporal passed its health
   check. A nonzero stale count is visible evidence of prior interrupted state,
   not a reason to hide that cleanup occurred.
2. `driver_accepted` records the workflow ID and the exact run ID returned by
   `Client.start`.
3. `history_checked` at `initial` records that the pending run reached the
   timer boundary before replacement.
4. `driver_waiting` proves that the one-shot assertion process had not exited
   before the worker was replaced.
5. `generation_one_replaced` records the generation-1 container identity,
   replacement mode, exit code, and whether the graceful shutdown marker was
   observed. Graceful mode requires exit code 0 and the marker; crash mode
   requires exit code 137 and no marker.
6. `generation_one_removed` proves that the replaced container was removed,
   rather than silently reused by Compose.
7. `generation_two_ready` records a different container and a fresh
   generation-aware readiness observation.
8. `replay_observed` records generation 2 seeing the same run with
   `is_replaying=true` and a positive history length.
9. `history_checked` at `terminal` records that the same run reached the
   required completion event sequence.
10. `driver_completed` proves that the driver received the expected completed
    result.
11. `generation_two_stopped` records a graceful generation-2 shutdown.
12. `generation_two_removed` proves that the replacement container was also
    removed.
13. `postgres_volume_removed` proves that teardown removed the test data.

Every record has a closed field set and a successful status. The validator
cross-checks the workflow/run identity, requires generation-2 container
replacement and removal, and requires zero project volumes at both the start
and end. The live Make target generates this record from real process, history,
replay, and cleanup observations; `controller.json` remains the deterministic
fixture for the validator and rejection tests.

## Local gate and limits

Run the Docker-free contract check with:

```sh
make test-temporal-worker-restart-contract
```

This contract target does not start Docker, PostgreSQL, Temporal Server, an
OCaml worker, or a client. It runs positive fixtures and negative cases through
the same validators used by the live controller. To run the real stack, use:

```sh
OCAML_VERSION=5.5 DUNE_JOBS=1 make test-temporal-worker-restart-live
```

The forced-termination companion gate uses the same history and replay
validator while requiring a real generation-one crash:

```sh
OCAML_VERSION=5.5 DUNE_JOBS=1 make test-temporal-worker-crash-recovery
```

Its controller record requires exit status 137 and explicitly rejects a
graceful worker-stop marker before accepting generation two's replay result.

The live target starts the two OCaml binaries, replaces and removes generation 1,
starts generation 2, validates replay, the retrying activity's exact attempt-two
result, and the exact terminal result, then removes the PostgreSQL volume on
both success and failure. The [PR #298 Actions
run](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291) passed
the graceful replacement sequence, and the [PR #306 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29356904816) passed the
forced-crash companion gate. The earlier [PR #253
run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471) passed the
original sequence. An earlier local attempt was stopped by Docker
storage/daemon failure before readiness; that infrastructure failure is not
part of the live evidence.

## Cache-eviction deadline diagnostics

The cache-eviction driver retains its existing 900-second process budget.
`run-cache-eviction-driver.sh` establishes one absolute marker deadline before
compilation/startup, reserving 10% of that budget (at least one second, at most
30 seconds) for reporting and shutdown. Both marker waits inherit this deadline;
advancing to another phase never resets it. Timeout overrides must be integer
seconds between 2 and 86400. A direct invocation without the wrapper retains the
local wait budget for manual diagnosis.

The driver prints both exact execution identities and reports its marker error
before attempting client shutdown. The diagnostic distinguishes a missing
cache-full eviction with B acknowledged from neither observation arriving.
The outer watchdog remains a failure if startup, an RPC, or shutdown hangs.
Fixing the ordering does not establish the cause of a cache-eviction stall.
The Docker-free marker tests and Linux wrapper regression run with `dune runtest
test/integration/temporal/cache_wait`; the live gate remains
`make test-temporal-worker-cache-eviction OCAML_VERSION=5.5`.

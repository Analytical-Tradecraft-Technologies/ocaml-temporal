# Worker restart and replay acceptance design

**Status: the original path is live-verified in the [PR #253 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471), the retry-after-restart extension is live-verified in [PR #298](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291), forced crash recovery is live-verified in [PR #306](https://github.com/mfow/ocaml-temporal/actions/runs/29355426605), the repaired one-slot sticky-cache eviction gate is live-verified in the complete [PR #438 run](https://github.com/mfow/ocaml-temporal/actions/runs/29805397413), and the separate bilateral parent/child restart-replay path is live-verified in the complete [PR #351 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013).** The earlier [PR #322 run](https://github.com/mfow/ocaml-temporal/actions/runs/29402103748) remains historical evidence for the original eviction gate. The private Rust bridge validates and feeds one history at a time into a workflow-only Temporal Core replay worker. The public worker reports bounded activation metadata through a private OCaml callback, and the Compose fixture replaces generation 1 with a fresh generation 2 while an independent OCaml driver waits for the exact run. The extension requires generation 2 to complete the retrying activity at attempt two, proven by the exact result marker; Temporal compacts intermediate activity retry events out of workflow history. `make test-temporal-worker-crash-recovery` adds the same exact-run evidence after a forced generation-one process kill and now has a green live result. `make test-temporal-worker-cache-eviction` exercises the separate one-slot eviction scenario and requires Core's real `RemoveFromCache` activation, an empty acknowledgement, and continued workflow progress. The bridge format and ownership rules are documented in the [internal replay bridge reference](replay-bridge.md).

This restart/replay result is not evidence for workflow-code versioning. The
separate [`make test-temporal-workflow-patching`](workflow-patching.md#live-replay-acceptance)
target proves that a marker-free legacy history and a marker-bearing new
history select their respective branches after worker replacement. That
separate result is live-verified by the complete [PR #348 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29411260374); it is not
part of the restart/replay evidence described here.

It is also not bilateral parent/child recovery evidence. The dedicated
[`make test-temporal-parent-child-restart`](parent-child-restart-replay-acceptance.md)
target records and validates the exact parent and child run identities before
and after worker replacement, then requires both roles to report generation-two
replay checkpoints before the parent may complete. That separate topology is
live-verified in the complete [PR #351 Actions
run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013); it is not
part of the single-workflow restart evidence described here.

The separate
[child-failure-after-replay acceptance](child-failure-replay-acceptance.md)
also requires both replacement-worker replay observations before the child
returns its typed failure and the parent records recovery. It is live-verified
by the complete [PR #361 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29475615866), not by
the single-workflow restart gate described here.

`make test-temporal-worker-restart-contract` is the fast Docker-free contract
gate. `make test-temporal-worker-restart-live` runs the real PostgreSQL,
Temporal Server, two-generation OCaml worker, and OCaml driver sequence, while
`make test-temporal-worker-restart` runs both. The standalone CI integration job
invokes this target after the baseline smoke. The [current evidence audit](live-acceptance-coverage.md)
records the tested source and successful job; PR #253 is the original evidence.
An earlier cold ARM64 attempt did
not reach the acceptance assertions because the Docker daemon ran out of
storage during the native build, which was an infrastructure failure rather
than replay evidence.

The separate `make test-temporal-worker-cache-eviction` target implements the
one-slot `RemoveFromCache` acceptance described by the live coverage matrix.
The repaired isolated-worker topology passed against the real server in the
complete [PR #438 run](https://github.com/mfow/ocaml-temporal/actions/runs/29805397413);
the earlier [PR #322 run](https://github.com/mfow/ocaml-temporal/actions/runs/29402103748)
is historical evidence for the original gate. Worker restart alone is not used
as eviction evidence.

## What this scenario proves

The existing two-process acceptance fixture has historical live evidence for
the baseline success paths while one worker remains alive. It does not stop or
replace a worker, inspect server history, or expose an `is_replaying` marker.
The live restart target proves a narrower recovery contract:

1. an OCaml test-driver starts one workflow through `Temporal.Client` and waits
   for that exact workflow/run pair;
2. the OCaml worker receives the initial activation and records a durable timer
   before it is stopped;
3. a fresh instance of the same OCaml worker is started with the same
   namespace and task queue;
4. Temporal/Core delivers the pending execution to the new worker, which
   replays the recorded history, waits for the timer, runs the follow-up
   activity, observes its first retryable failure, completes the second
   attempt, and completes the workflow; and
5. the one-shot driver asserts the exact terminal payload and exits nonzero if
   any result, terminal class, or timeout is unexpected.

This is a worker-SDK test, not a client-only test. A worker process that merely
starts, or a driver that merely receives a workflow ID, is never a pass.

## Roles remain asymmetric

The restart test keeps the topology used by the existing acceptance fixture.

| Process | Responsibility | What it must not do |
| --- | --- | --- |
| `smoke-driver` | One-shot OCaml test runner. Starts the workflow, waits for its exact run, and checks the expected terminal result. | Register a worker, poll task queues, execute workflow code, or manufacture a success value. |
| `smoke-worker` generation 1 | Long-lived OCaml Temporal worker. Registers the workflow/activity definitions and processes the initial activation. | Start the test workflow or decide whether the assertion passed. |
| `smoke-worker` generation 2 | The replacement long-lived OCaml worker. Uses the same registration, namespace, task queue, and library, but a new process and native Core graph. | Reuse generation 1's native handles or process-local continuations. |
| Compose/Make controller | Orchestrates the stop/start boundary and collects diagnostic evidence. | Produce workflow results, bypass the public OCaml API, or act as a second worker. |

The controller may run `docker compose stop`/`up` and use a fixture-only
Temporal history query to know when the first timer has been recorded. It is
not an application worker and does not change the user's rule that the OCaml
container which starts workflows is just a test runner asserting a result.

## Workflow under test

The fixture adds a deterministic workflow named
`smoke.worker_restart_replay`. Its body contains only replay-safe SDK
operations and is equivalent to:

```ocaml
Temporal.Workflow.define ~name:"smoke.worker_restart_replay"
  ~input:Temporal.Codec.string ~output:Temporal.Codec.string
  (fun _seed ->
    match Temporal.Workflow.sleep (Temporal.Duration.of_ms 60_000L) with
    | Error error -> Error error
    | Ok () ->
        let open Temporal.Result_syntax in
        let* policy = retry_policy in
        let* transformed =
          Temporal.Activity.execute ~retry_policy:policy retry_once_activity
            "after-replay"
        in
        Ok ("SMOKE:" ^ transformed))
```

This is the current fixture implementation. Its behavioral contract is:

- the initial workflow task emits one durable timer before the worker is
  stopped;
- the timer is long enough that the controller can observe `TimerStarted` and
  stop generation 1 without racing the timer firing;
- the follow-up activity is not scheduled until the timer fires, so its first
  live task must be handled by generation 2;
- the first activity attempt returns a retryable typed error, and the second
  attempt returns a marker ending in `ATTEMPT:2`; and
- the activity result is deterministic for a given attempt and input. The
  worker generation may be written to a diagnostic log by the activity, but it
  must never be included in the workflow result or used to choose commands.

For input `smoke`, the driver must require the exact completed payload
`SMOKE:AFTER-REPLAY:ATTEMPT:2`. It must reject failed, cancelled, terminated,
timed-out, continued-as-new, or differently encoded results through the
existing typed `result` path. The fixed workflow ID should be
`two-binary-worker-restart-replay`; the driver must retain and wait on the run
ID returned by `Client.start` rather than looking up a later run by workflow ID.

## Synchronization protocol

The restart boundary needs evidence that generation 1 actually committed the
timer. A fixed sleep in the controller is not sufficient: it can stop the
worker before the first workflow task, or after the timer has already fired.
The live target uses the machine-readable Temporal CLI response, the exact run
identity from `workflow describe`, and the strict normalized-history validator.
It emits the ordered controller record defined by the checked-in schema and
uses this bounded sequence:

1. The live restart target invokes the same clean-stack setup as the existing
   integration target. `temporal-clean` runs before startup and in an exit
   trap.
2. The controller starts Temporal/PostgreSQL, runs the normal lifecycle check,
   and starts generation 1 of `smoke-worker`.
3. The one-shot driver starts the named workflow and writes its existing
   metadata-only `accepted` phase (workflow ID and run ID, never payload
   bytes). It then waits normally; it must not exit while the workflow is
   pending.
4. The controller waits for a stable history observation showing, in order,
   `WorkflowExecutionStarted`, the initial `WorkflowTaskCompleted`, and
   `TimerStarted` for the exact run ID. The query must use a machine-readable
   Temporal/admin-tools response or a fixture helper with an explicitly tested
   parser; scraping human-formatted output is not acceptable. A timeout here is
   a failed initial dispatch, not permission to guess that the timer exists.
5. The controller stops generation 1 and asserts that the container exits
   within its configured graceful-shutdown bound. It records the container ID
   and worker logs before replacing the container. The driver must still be
   waiting and the workflow must still be open.
6. The controller removes the stopped generation-1 container with
   `docker compose rm --force smoke-worker` and asserts that no stopped
   `smoke-worker` container remains. This removal is required: Compose
   `stop`/`up` can otherwise reuse the same container filesystem and its old
   `/tmp/ocaml-temporal-two-binary-worker.ready` marker. The controller then
   starts generation 2 with the same task queue and registrations using
   `docker compose up --detach --build --force-recreate --wait smoke-worker`,
   a fresh readiness marker, and a distinct diagnostic worker identity (for
   example, a generation suffix). If a platform-specific equivalent is used,
   it must both remove or generation-check the old marker and prove that a new
   worker process reached `Worker.create` readiness; a reused container without
   that proof is a test failure.
7. The controller waits for generation-2 readiness before allowing the driver
   to finish, then waits for a replay marker and the exact result. The marker
   is diagnostic evidence; the driver's exact attempt-two result and the
   logical terminal history remain the acceptance oracle. Temporal's compact
   retry history is intentional, so an intermediate failure event is not
   required in the normalized document.
8. On success, the controller verifies the ordered history and the exact
   driver assertion, then runs `temporal-clean`. On failure, it first preserves
   the driver output, both worker generations' bounded logs, and the workflow
   history metadata, then removes the containers and PostgreSQL volume.

The driver remains one process and one assertion runner throughout this
sequence. Restarting the worker is external lifecycle orchestration, not a
second OCaml worker hidden inside the test client.

## Replay evidence and limits

The internal semantic activation retains Core's `is_replaying` bit, history
length, run ID, and cache-removal metadata. The private worker callback writes a
privacy-safe diagnostic record when it receives the initial and post-restart
activations, containing only:

```text
phase=replay
generation=2
workflow_id=two-binary-worker-restart-replay
run_id=<exact-started-run-id>
is_replaying=true
history_length=<positive-value>
```

The initial activation records `is_replaying=false`; the post-restart
activation for the same run records `is_replaying=true`. Payload bytes,
workflow inputs, and activity output are not written to the diagnostic file. A
live run is accepted only when this marker is present and the exact terminal
result also passes; a terminal result without it is not replay evidence.

A worker restart is not the same thing as a Temporal `RemoveFromCache`
activation. Losing an in-memory sticky execution while replacing a worker is
expected; it does not prove that the server sent an explicit cache-eviction
job or that the OCaml eviction acknowledgement path is correct. Sticky-cache
eviction therefore remains a separate, live-verified scenario requiring an
observed `remove_from_cache` activation and an empty completion; the current
isolated-worker proof is recorded in [PR #438](https://github.com/mfow/ocaml-temporal/actions/runs/29805397413).
This design must not fold that claim into the restart result.

## Exact pass criteria

The acceptance command may exit zero only when all of the following are true:

- the stack passed PostgreSQL schema, Temporal frontend, namespace, and Core
  lifecycle readiness checks;
- generation 1 became healthy and the exact driver run was accepted;
- the exact run's history contained `TimerStarted` before generation 1 stopped;
- generation 1 stopped through the public worker shutdown path within the
  grace period, with no second worker overlapping it on the same test queue;
- the generation-1 container was removed before generation 2 started, so its
  readiness marker and writable process state could not be reused;
- generation 2 became healthy and emitted the same run ID with
  `is_replaying=true` before the timer-result activity completed;
- the history contains the ordered timer firing, logical activity scheduling,
  final activity start and completion, and workflow completion events for that
  run; event IDs may vary, but their order and run identity must not. The
  driver's exact `SMOKE:AFTER-REPLAY:ATTEMPT:2` result proves that this final
  activity event represents the retry's second attempt;
- `smoke-driver` returned zero only after `Client.wait` reported
  `Completed` for that exact run and its decoded payload equaled
  `SMOKE:AFTER-REPLAY:ATTEMPT:2`; and
- teardown removed the Compose project and its PostgreSQL data volume.

Any missing marker, wrong run ID, unexpected terminal variant, activity
failure, timeout, stale worker readiness file, or retained volume is a failed
test. A green worker health check alone is never sufficient evidence.

## Failure evidence

The failure trap should print bounded, metadata-only evidence before cleanup:

- the driver's phase log, including start/wait boundaries, run ID, terminal
  class, typed error kind, and latency;
- generation 1 and generation 2 worker phase logs, worker identities, process
  exit status, and replay markers;
- the machine-readable history summary for the exact workflow/run pair;
- `docker compose ps` and the last 200 lines of PostgreSQL, Temporal Server,
  schema, and worker logs; and
- a check that the driver was still waiting when generation 1 stopped.

The history summary should distinguish at least these failures without
guessing:

| Evidence | Likely boundary |
| --- | --- |
| No `TimerStarted` before stop | Initial worker dispatch or completion failed. |
| Timer exists, but generation 1 does not stop | Worker shutdown/drain or native wait lifecycle bug. |
| Generation-1 container remains, its ID is reused, or its readiness marker is present before `Worker.create` | Replacement orchestration is invalid; generation 2 must not be accepted. |
| Generation 2 is healthy, but no `is_replaying=true` marker | Replacement worker did not receive/replay the pending execution, or replay instrumentation is missing. |
| Replay marker exists, but the driver does not return the exact attempt-two marker | Timer resolution, generation-2 activity dispatch, Temporal retry policy delivery, Core retry translation, or activity polling failed. |
| Final activity history is missing or incomplete | The replacement worker did not persist the retry's terminal activity outcome, or the history query/normalizer lost it. |
| Second activity completes, but driver reports a non-completed/mismatched result | Workflow determinism, payload decoding, or exact-run waiting failed. |
| History belongs to another run or is missing | Driver/run identity or server persistence/query synchronization failed. |
| Cleanup leaves a PostgreSQL volume | Fixture lifecycle is incorrect; the result must not be accepted. |

Do not delete the diagnostic history before these checks run. Cleanup still
must remove the named volume in both success and failure paths; preserving the
volume for a later run would make replay evidence non-repeatable.

## Determinism and ownership constraints

- Workflow code may use only replay-safe SDK operations. It must not read
  environment variables, wall-clock time, randomness, files, sockets, or
  process-global mutable state to select a command or result.
- The activity may record a generation marker for diagnostics because activity
  code is outside deterministic workflow replay, but its returned payload must
  depend only on its typed input.
- Generation 2 must construct a new public `Temporal.Worker` and new private
  supervisor/Core graph. No Rust handle, OCaml continuation, or workflow
  scheduler may cross the process boundary.
- The driver owns only its client and exact-run handles. It must shut down the
  client after the assertion even when a typed failure occurs.
- The worker stop path must use `Temporal.Worker.shutdown`; sending SIGKILL or
  deleting a live container before graceful shutdown would test process loss,
  not the SDK's lifecycle contract. A separate crash-recovery scenario may
  deliberately use an ungraceful stop later.

## Implementation sequence

The implementation should land as a separate acceptance slice, in this order:

1. Add the deterministic workflow and a test-only activity diagnostic marker;
   add unit/runtime tests proving that the workflow emits the same timer and
   activity commands when replayed with identical activation history. **Done.**
2. Add a bounded, machine-readable history synchronizer and generation-aware
   worker readiness/replay markers to the fixture. Have the live controller
   emit the ordered records defined by
   [`restart-replay-controller.schema.json`](../schemas/acceptance/restart-replay-controller.schema.json),
   and keep all payload logging disabled. **Done; the offline contract passes.**
3. Add a dedicated live Make target and CI job (one supported OCaml version)
   separate from the Docker-free contract target. It should start the driver in
   the background, perform the controlled worker replacement, wait for the
   exact assertion, and always remove the PostgreSQL volume. **Done; the CI
   job now invokes it.**
4. Run the acceptance from a clean stack in the dedicated Linux `amd64` live
   job; the separate Linux `arm64` and native Windows/macOS jobs must not run
   this Linux Compose test. **Done for the success path in the [PR #253
   Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471).**
   A deliberately failing run that proves diagnostics appear before cleanup is
   still follow-up work.
5. After a green CI run, update
   [`live-acceptance-coverage.md`](live-acceptance-coverage.md),
   [`feature-coverage.md`](feature-coverage.md), and `docs/progress.md` with
   the commit and run URL. **Done:** the current evidence is recorded in those
   references and this document.
6. Extend the replacement-worker activity path with the bounded retry policy
   and require the normalized history to contain the first failure and second
   activity schedule. **Live-verified in the [PR #298 Actions
   run](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291).**

This sequencing keeps a passing terminal result from being mistaken for
replay evidence and keeps the current one-shot driver/long-lived-worker
contract intact.

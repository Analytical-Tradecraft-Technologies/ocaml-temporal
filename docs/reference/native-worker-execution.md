# Native workflow execution adapter

This document describes the private OCaml loop that sits between typed native
supervisor operations and the deterministic workflow runtime. It is not a
public worker API. Public workflow definitions remain ordinary values; the
adapter hides the heterogeneous input/output types and the mutable execution
registry behind a private functor result.

## Boundary and ownership

The adapter consumes the typed operations below:

```text
try_poll_workflow : supervisor -> (activation option, error) result
complete_workflow : supervisor -> completion -> (unit, error) result
```

The concrete `Sdk_supervisor.Native` module instantiates this signature with
operations on its owner Domain. The public worker loop also uses two private,
bounded readiness operations (`Wait_workflow` and `Wait_activity`). They do not
consume a task or return one; they wait for a wake-up from the corresponding
Rust/Core poll lane. The loop always retries the nonblocking `try_poll_*`
operation after a wake-up, because readiness is only a hint and another
consumer may have taken the task first. The C boundary releases the OCaml
runtime lock during the wait. Its 100 ms bound means a quiet lane returns
ordinary `Not_ready` and the supervisor can still admit shutdown promptly.
Keeping waits outside this adapter means `poll` remains nonblocking, the
registry remains usable with deterministic fake sources, and execution
translation does not depend on a particular scheduling primitive.

When an activity adapter retains a completion after a native call explicitly
reports a retryable transport outcome, the loop uses a separate
`Wait_activity_completion_retry_backoff` operation. That operation is a fixed
10 ms timer on the supervisor's owner Domain, and its C stub releases the
OCaml runtime lock while the timer runs. It is intentionally not an activity
readiness wait: unrelated queued work must not make the retained completion
spin. The workflow adapter does not currently produce a retry-pending result;
its branch remains on ordinary readiness as a safe extension point.

The supervisor remains the sole owner of the Rust runtime, client, worker, and
native task ledger. Rust leases a Core task before handing it to a poll lane,
and the protocol adapter on the owner Domain strictly decodes and validates
the returned JSON before constructing a typed activation. If Rust cannot
convert or encode a task before that hand-off, it generates a Core failure and
retires the exact lease. If OCaml rejects a hand-off after it has crossed the
boundary, the private rejection operation decodes the submitted document and
checks semantic equality with Rust's retained activation before retiring that
same lease; JSON formatting is not part of the identity check. The worker
adapter sees only typed activations; it propagates those lower-layer errors and
never pretends that a completion was submitted.

The adapter owns only OCaml values:

- an immutable map from workflow type name to an existentially typed local
  definition;
- a mutable map from Temporal run ID to its matching typed `Execution.t`;
- a mutable map of workflow completions whose native acknowledgement has not
  yet been proven, with every mutable payload buffer copied into adapter-owned
  storage;
- one mutex that serializes polling, execution, and completion submission.

No native pointer, Rust future, or continuation is stored in the maps. The
pending completion map owns copied semantic payload bytes until the supervisor
confirms acknowledgement and then releases them with the ordinary OCaml value
lifetime. The Rust ledger remains the authority for the native lease until
Core accepts or rejects that completion. The mutex protects OCaml scheduler
state in addition to the supervisor's own owner-Domain serialization, so two
ordinary producer Domains cannot execute the same run concurrently. Workflow
fibers must not call the adapter directly because supervisor operations may
block their producer Domain.

## Worker configuration validation

`Make.create` validates the worker's implicit activity queue before it stores
any workflow definitions or accepts an activation. The queue must be non-empty,
contain no NUL byte, fit within 65,536 bytes, and be valid UTF-8. Invalid input
returns `Error { code = "invalid_configuration"; path = "$.task_queue"; ... }`;
it does not call the supervisor or wait until the first workflow starts. The
same predicate is used when an execution context is created, so a malformed
queue cannot turn into a late `Invalid_argument` after a Temporal lease has
already been accepted.

## One poll transaction

`Native_worker_execution.Make` processes at most one activation per `poll`:

1. A nonblocking empty lane returns `Ok Not_ready`.
2. A typed activation is validated again by
   `Native_execution.translate_activation`. This applies the same semantic
   checks for identifiers, sequence relationships, and payload shape to fake
   supervisors and future alternate sources. It is not a second JSON
   round-trip; JSON syntax and encoding metadata have already been checked by
   the native supervisor's protocol adapter.
3. A first job must be exactly one `Initialize_workflow`. Its workflow type is
   looked up in the immutable registration map. Duplicate run IDs, unknown
   workflow types, remote-only definitions, and invalid input argument counts
   become typed bridge failures.
4. The input payload is copied into the runtime representation. Binary metadata
   is rejected because runtime codec metadata is text; no replacement encoding
   is attempted. Zero arguments are decoded as the canonical `binary/null`
   unit payload, one argument is decoded normally, and additional arguments
   are rejected rather than dropped.
5. A typed `Execution.t` is inserted under the run ID before activation jobs
   run. The existing deterministic scheduler applies jobs in order and emits
   commands in creation order.
6. `Native_execution` converts the command batch to a checked semantic
   completion. Activity commands retain their complete Core fields and child
   starts retain their workflow identity, input payload, and optional retry
   policy before submission. Core child options that the current OCaml runtime
   does not expose stay at
   explicit defaults. When Core later sends a child start acknowledgment, the
   adapter stores the returned run ID and keeps the parent future pending; a
   separate terminal child resolution then completes that future.
7. The completion is copied into an adapter-owned pending record before it is
   submitted through the same supervisor. The supervisor canonical-encodes
   and reparses the completion, checks its leased run ID against Rust's ledger,
   and retires that lease only after Core accepts it. The run entry is removed
   only after the supervisor confirms completion retirement. Terminal commands
   remove the run; a cache-removal activation also removes it after its
   required empty acknowledgement. Pending timer, activity, and child work
   keeps the run entry. A child start failure retires its future immediately; a
   successful start keeps it until the matching terminal resolution arrives.
   A terminal resolution before its start acknowledgment, or a
   duplicate/unknown child sequence, is a typed bridge failure.

Activations without initialization must identify a run already in the map.
Unknown run IDs are completed with a non-retryable bridge failure, which
retires the native lease instead of silently ignoring it.

`poll` is deliberately nonblocking: `Not_ready` records only that this lane
was empty at that instant. `Temporal.Worker.run` owns the fairness policy and
the bounded readiness waits described above; callers that need a blocking
worker loop should use that public API rather than waiting in this adapter.

## Rejection and failures

An activation that is valid JSON but cannot be represented by the current
runtime receives a typed failed-task completion with no commands. The
workflow execution remains open; unsafe cached state is stopped immediately
and its ownership record is removed after acknowledgement. `poll` returns
`Ok (Rejected ...)` only after that completion has been accepted, and marks
`lease_retired = true`. If the native completion operation fails, `poll` returns
an error and leaves the exact completion in the pending map without claiming
retirement. The production source marks it retryable only when the bridge
returns the explicit `Retryable` status, which is reserved for a future
Core/client path that proves the lease was not consumed. Generic `Connection`,
`Not_ready`, and `Worker` statuses are not retryable: the pinned Core
completion API removes the task before internally logging/suppressing network
failures, so blindly submitting the same completion could duplicate it. A
retry never reruns the workflow implementation. `drain` retries a retained
completion only while that explicit classification remains true; otherwise it
returns a terminal error and leaves the worker closed. Before returning that
terminal error, the worker invokes the supervisor's `Native.shutdown` path.
That path always reaches `runtime_close`, even if Core reports outstanding
tasks while its graceful worker step runs; runtime disposal force-retires those
native leases and releases Tokio/Core. The original adapter error remains the
public result, while any native cleanup diagnostic is logged. The pending map
still owns copied bytes until the caller's result records either
acknowledgement or this terminal failure. If `Native.shutdown` returns
`Error`, that result is still release-complete by contract and the adapter maps
are then discarded. If it raises before returning, the worker keeps the maps,
marks terminal cleanup pending, and schedules a detached retry; no copied
completion is discarded merely because the public worker has closed admission.

Malformed JSON is rejected below this module by the supervisor's protocol
adapter. A native task that cannot be converted before hand-off is rejected by
Rust using its retained Core value. A typed activation that reaches this
module but fails `Native_execution.translate_activation` follows the typed
failure-completion path above. The lower layer owns the raw lease token and is
the only layer able to retire it safely when no trustworthy run ID can be
decoded. The worker adapter exposes the typed source error with its bounded
code/path/message and performs no unsafe best-effort parsing.

Expected operational failures use `result` values. Unexpected exceptions from
workflow translation, codec execution, or completion are contained as typed
`ocaml_exception`/`completion_failed` diagnostics. The adapter retains the
exact original completion when its acknowledgement raises, including when Core
may already have accepted it. It never submits replacement failure commands or
reruns user code to recover from an uncertain acknowledgement.
The mutex is still released by `Fun.protect`, so a producer Domain cannot lose
the registry lock or strand a second caller.

## Public worker wiring

`Temporal.Worker.create` validates all registration definitions before opening a
native resource. A `mock://` target selects the deterministic in-memory backend
used by unit tests; an `http://` or `https://` target creates one private
supervisor, connects the Core client, starts one workflow/remote-activity
worker, and installs the two typed adapters described above. The application
still owns the final executable: Rust remains a static implementation detail
behind the private supervisor and no native handle is exposed through the
public API.

`Temporal.Worker.run` takes the worker mutex and, on each iteration, polls the
workflow lane once and then the activity lane once. A task-level
workflow/activity failure is completed through Core and the loop continues; a
transport, protocol, or lifecycle error returns a typed `Temporal.Error.t`.
When both nonblocking polls return `Not_ready`, the loop waits on exactly one
bounded readiness operation and alternates the lane on the next empty
iteration. A readiness result is only a wake-up, so the next iteration drains
both lanes again. This keeps an activity-only load from starving behind
workflow readiness (or the reverse) without making the adapter's `poll`
operation block.

`shutdown` first closes admission, then waits for an active loop to leave the
adapters by taking the same worker mutex. It drains the workflow and activity
pending-completion maps in that order. Only when both maps are empty does the
supervisor close its readiness signals and native admission, join the Rust
poll lanes, verify that no native leases remain, and release worker, client,
and runtime state in reverse ownership order. If an activity drain reports the
explicit retryable status, native teardown is not started and both layers
reopen admission so the exact retained completion can be retried. A workflow
drain or permanent activity error first invokes `Native.shutdown`/`runtime_close`
to reclaim the graph, then leaves both private and public worker state
terminal; reopening after either error could duplicate a completion or conceal
an ownership defect. A returned native teardown `Error` is terminal because
the supervisor has consumed the graph and its defensive runtime close is
release-complete; the adapter maps are discarded only after that result. If
native teardown raises before returning, the worker remains terminal for new
work but retains its maps and schedules a detached retry, with the finalizer
as a further last-resort path. A same-Domain shutdown call is different: no
teardown has started, so it returns a retryable defect without closing the
private graph; a later call from another Domain can wait for the run mutex and
complete shutdown. That branch must not write the shared stop flag: a
concurrent shutdown on another Domain may already have set it to stop the run
loop, and any write here would race that caller and could strand the loop,
holding the run mutex forever. It therefore only marks the failure retryable
and leaves the stop flag exactly as observed. Repeated successful shutdown
calls are idempotent.

The semantic translator accepts child-start commands with the workflow identity,
input, and optional retry policy represented by the protocol. Core child options
not yet exposed by the OCaml runtime remain explicit defaults, but the two child
resolution activations are decoded and validated losslessly. Start and
terminal events share one Core sequence; only that exact pair is accepted. The
complete [PR #289 Compose run](https://github.com/mfow/ocaml-temporal/actions/runs/29339077368)
live-verifies the initial workflow/activity success path, exact-run
cancellation, heartbeat and timeout behavior, one parent awaiting a successful
child result, child failure/cancellation, one server-managed activity retry,
and one typed non-retryable workflow-failure path. The remaining
terminal/recovery scenarios still require their corresponding real-server
evidence.
Activity commands are accepted only when their required identifiers, payloads,
timeout policies, and cancellation options are present; a missing field is
rejected in the same typed way.

## Verification

`test/runtime/test_native_worker_execution.ml` uses a fake semantic queue to
verify:

- first-activation initialization and terminal completion;
- durable timer suspension and resumption through a matching sequence;
- cancellation and cache-eviction removal of suspended runs;
- complete activity-command scheduling and lease retirement;
- child-start translation plus start acknowledgment and terminal child
  resolution, including start failure, final-before-start, duplicate, and
  lease-retirement behavior;
- retention and retry of a rejected workflow completion without rerunning the
  workflow, including an explicit adapter drain;
- unknown run rejection and lease retirement;
- exact completion retention when acknowledgement raises, including after
  source acceptance, without replacement commands or workflow re-execution;
- typed propagation of lower-layer malformed-activation errors; and
- duplicate and remote-only registration rejection before worker publication;
- rejection of empty, NUL-containing, oversized, and non-UTF-8 worker queues
  before worker publication.

`test/runtime/test_native_worker_lifecycle.ml` is a separate focused regression
file for the shutdown-sensitive path. It rejects the same completion twice:
the initial poll fails, the first drain fails, and the second drain succeeds.
The workflow implementation runs once, the copied completion is submitted
once, and the fake native lease remains present until that final acknowledgement.
This is the contract that lets public worker shutdown retry a transient
completion transport failure safely.

The fake tests do not by themselves claim live Temporal compatibility. The
focused supervisor tests cover operation admission, bounded waits, and
idempotent shutdown; bridge tests cover the C/Rust readiness and null/error
paths; and the Rust task-ledger tests cover exact lease identity, conversion
rejection, and retirement ordering. The complete [PR #289 Compose
run](https://github.com/mfow/ocaml-temporal/actions/runs/29339077368) also
verifies timer, remote-activity, parent/child success, propagated child
failure, and child cancellation against a real Temporal Server. Dedicated
restart/replay, cache-eviction, patching, and parent/child recovery gates add
their own live evidence; the remaining activity, terminal, and recovery cases
still require their corresponding real-server evidence.

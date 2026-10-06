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
complete_workflow :
  supervisor -> completion:completion -> Encoded_workflow_completion.t ->
  (unit, error) result
```

`complete_workflow` receives the canonical bytes from the completion's single
encoder pass (`Encoded_workflow_completion.t`, which only that encoder can
produce) and submits exactly those bytes; it never encodes the completion
again (issue #846). The labelled `completion` is the typed value those bytes
were encoded from. It is passed read-only so fake sources, integration
controllers, and the cold-replay benchmark can inspect submitted commands
without parsing JSON a second time; the production supervisor ignores it.
Because `completion` may alias workflow-owned payload buffers, only the
encoded bytes are a snapshot and authoritative.

The concrete `Sdk_supervisor.Native` module instantiates this signature with
operations on its owner Domain. The public worker loop also uses one private,
bounded readiness operation, `Wait_any`. It does not consume a task or return
one; it waits for a wake-up from *either* Rust/Core poll lane. At most one of
the two idle execution lanes enters this native wait at a time, alternating the
preferred lane after each wait. Because the wait observes both lanes, which
lane holds it does not matter for latency: a task arriving on the other lane
ends the wait at once, and that lane's poll, queued behind the wait in the
supervisor mailbox, runs next. An earlier lane-specific wait held the sole
owner for the full bounded timeout while a task sat on the other lane, so every
step of a sequential activity workflow paid a dead 100 ms wait (#806). The
lane-specific `Wait_workflow` and `Wait_activity` operations remain for
bridge tests and replay. A
nonpreferred lane may claim the free wait after one local deferral if staggered
polls keep the preferred lane from observing both lanes idle. If its sibling is
busy or owns the native wait, a lane yields locally for 10 ms and then retries
its nonblocking `try_poll_*` operation. This prevents an idle lane
from repeatedly occupying the sole supervisor owner during work on the other
lane. The busy check is a snapshot taken after the lane's own poll: a sibling
that becomes busy between that decision and the native wait simply queues its
poll behind the wait, which `Wait_any` ends as soon as that task is ready.
Rust poll-lane failures are also reported by `try_poll_*` when no task is
queued, so local yields cannot conceal a fatal producer error. Native
readiness remains the both-idle steady-state path. The C boundary releases the
OCaml runtime lock during its 100 ms native wait, after which the supervisor
can admit shutdown promptly.
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
  yet been proven, each held as the immutable canonical JSON produced by its
  single encoder pass (`Encoded_workflow_completion.t`) beside the typed
  completion it was encoded from. A retry resubmits those bytes unchanged, so
  later mutation of a workflow-owned payload buffer cannot alter what is
  sent; the typed value is kept only to pass to the source read-only;
- one mutex that serializes polling, execution, and completion submission.

No native pointer, Rust future, or continuation is stored in the maps. The
pending completion map owns those immutable completion bytes until the
supervisor confirms acknowledgement and then releases them with the ordinary OCaml value
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
   supervisors and future alternate sources. JSON syntax and encoding
   metadata have already been checked by the native supervisor's protocol
   adapter. The activation is translated exactly once per poll: the private
   `translated_activation` record it returns is used for observer metadata
   and registry lookup and is then passed to
   `Native_execution.activate_translated`, which does not translate or
   validate it again (issue #846).
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
6. `Native_execution` converts the command batch to a semantic completion
   and runs the canonical encoder over it exactly once. That pass is the
   completion's validation, and its output bytes are returned with the typed
   value. Activity commands retain their complete Core fields and child
   starts retain their workflow identity, input payload, and optional retry
   policy before submission. Core child options that the current OCaml runtime
   does not expose stay at
   explicit defaults. When Core later sends a child start acknowledgment, the
   adapter stores the returned run ID and keeps the parent future pending; a
   separate terminal child resolution then completes that future.
7. The encoded bytes are stored in an adapter-owned pending record before
   they are submitted through the same supervisor. The supervisor copies them
   into the C call without encoding the completion again; Rust checks the
   leased run ID against its ledger and retires that lease only after Core
   accepts it. A failure completion or eviction acknowledgement built by the
   adapter is encoded once by the adapter in the same way. If the encoder
   rejects such a completion, nothing is submitted and the entry fails closed
   with `completion_failed`, as a non-retryable supervisor rejection does. The run entry is removed
   only after the supervisor confirms completion retirement. Terminal commands
   shut down fibers and pending operations but retain final workflow-local
   state for inline queries. Core owns cache lifetime: its cache-removal
   activation removes the run after the required empty acknowledgement.
   Completed query handlers can emit only their query response; the sealed
   scheduler never resumes. Pending timer, activity, and child work
   keeps the run entry. A child start failure retires its future immediately; a
   successful start keeps it until the matching terminal resolution arrives.
   A child the workflow cancelled between start initiation and start
   acknowledgment may be resolved by Core as cancelled with no start
   resolution; that cancelled result completes the future normally. Any other
   terminal resolution before its start acknowledgment, or a
   duplicate/unknown child sequence, is a typed bridge failure.

Activations without initialization must identify a run already in the map.
Unknown run IDs are completed with a non-retryable bridge failure (a query
failure for a query-only activation), which retires the native lease instead
of silently ignoring it. After eviction or worker replacement, Core replays
history into a new execution before dispatching queries, including queries of
already-completed runs.

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
retirement. The adapter's source signature classifies each failure through
`error_is_retryable`/`exception_is_retryable`, and the production workflow
source returns `false` for both: the bridge defines its `Retryable` status
only for activity completion, pinned Core reports only deterministic
validation failures from `complete_workflow_activation`, and a lifecycle,
mailbox, or lost-acknowledgement failure cannot prove that the run's lease is
still outstanding. Resubmitting after such a failure could duplicate the
completion or, once Core has issued the run's next activation, attach it to
that activation. The first non-retryable failure is therefore recorded on the
pending entry, and from then on neither `poll` nor `drain` calls the
supervisor for it again; both return the recorded error (issue #843). A retry
never reruns the workflow implementation. `drain` retries a retained
completion only while that explicit classification remains true; otherwise it
returns a terminal error and leaves the worker closed. Before returning that
terminal error, the worker invokes the supervisor's `Native.shutdown` path.
That path always reaches `runtime_close`, even if Core reports outstanding
tasks while its graceful worker step runs; runtime disposal force-retires those
native leases and releases Tokio/Core. The original adapter error remains the
public result, while any native cleanup diagnostic is logged. The pending map
still owns the encoded completion bytes until the caller's result records either
acknowledgement or this terminal failure. If `Native.shutdown` returns
`Error`, that result is still release-complete by contract and the adapter maps
are then discarded. If it raises before returning, the worker keeps the maps,
marks terminal cleanup pending, and schedules a detached retry; no retained
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
supervisor, connects the Core client, starts one Core worker, and installs the
two typed adapters described above. The worker polls only the task kinds it
registered (#805): with no activities, Core does not poll remote activity tasks
(local activities scheduled by its own workflows still run in-process); with no
workflows, Core runs no workflow poller. Registering neither is rejected as a
defect before any native allocation. This keeps a workflow-only and an
activity-only worker on the same task queue from taking, and failing as
unregistered, tasks meant for each other. For an activity-only worker the run
loop skips the idle workflow lane; the combined readiness wait never wakes for
that lane, so it cannot hold the supervisor on a lane that has no poller. The
application
still owns the final executable: Rust remains a static implementation detail
behind the private supervisor and no native handle is exposed through the
public API.

`Temporal.Worker.run` holds the worker lifecycle mutex while two OCaml Domains
run: the calling Domain polls and executes workflow activations, and one
dedicated Domain polls and executes activity tasks. Both Domains send native
operations through the same owner-Domain supervisor mailbox. Each lane waits
in turn on the bounded combined readiness operation when both are idle, and
uses a short local yield while its sibling is busy. A slow activity callback
therefore cannot hold up an unrelated workflow activation; the activity lane
has capacity one and cannot poll another activity while its callback or exact
completion retry is pending. A task-level workflow/activity failure is
completed through Core and the loop continues. A transport, protocol, or
lifecycle error stops both lanes and returns a typed `Temporal.Error.t` after
the activity Domain joins. The first fatal result is retained independently of
the worker's shutdown flag, so an explicit later shutdown still owns cleanup.

`shutdown` first closes admission, then waits for both execution Domains to
leave the adapters and for the activity Domain to join before taking the same
worker lifecycle mutex. It drains the workflow and activity
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
as a further last-resort path. A shutdown call from an execution lane's own
system thread (a workflow or activity callback, or a signal handler the
runtime runs there) is different: no teardown has started, so it returns a
retryable defect without closing the private graph. It does post a stop
request, so the loop returns, and a later call from any thread, including the
one that ran the loop, can take the run mutex and complete shutdown (#830). Lane identity is tracked per system thread (Domain plus
`Thread.id`), not per Domain, so a sibling thread on the run loop's Domain is
an ordinary caller (#763). That branch must not write the shared stop flag: a
concurrent shutdown on another thread may already have set it to stop the run
loop, and any write here would race that caller and could strand the loop,
holding the run mutex forever. It therefore only marks the failure retryable
and leaves the stop flag exactly as observed. The stop request is a separate,
sticky atomic that is only ever set to `true`, so posting it cannot undo
another caller's request.

`Temporal.Worker.request_shutdown` posts the same stop request without the
admission check or any lock. Both lanes treat it like the shutdown flag at
their next stop check, so an idle loop returns within one bounded native
readiness wait. It does not admit teardown: the shutdown flag's
compare-and-set is still free for the later `shutdown`, which performs the
ordinary drain and native release. Because the request is one atomic write,
an OCaml signal handler may make it on any Domain or thread, including the
run loop's own thread in a single-Domain program. Re-entrant shutdown from either
execution thread is rejected before the public shutdown mutex is acquired;
otherwise a callback could deadlock against a concurrent shutdown waiting for
its lane to return (#764). Admitted callers are serialized by that mutex and
return the first caller's cached terminal result. Repeated successful shutdown
calls are idempotent. A
callback that never returns still makes the join and shutdown unbounded; the
overall deadline and escalation policy are tracked in
[#495](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/495).

The semantic translator accepts child-start commands with the workflow identity,
input, and optional retry policy represented by the protocol. Core child options
not yet exposed by the OCaml runtime remain explicit defaults, but the two child
resolution activations are decoded and validated losslessly. Start and
terminal events share one Core sequence; only that exact pair is accepted. The
complete [PR #289 Compose run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29339077368)
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
The workflow implementation runs once, the retained completion bytes are
accepted once, and the fake native lease remains present until that final acknowledgement.
This is the contract that lets public worker shutdown retry a transient
completion transport failure safely.

The fake tests do not by themselves claim live Temporal compatibility. The
focused supervisor tests cover operation admission, bounded waits, and
idempotent shutdown; bridge tests cover the C/Rust readiness and null/error
paths; and the Rust task-ledger tests cover exact lease identity, conversion
rejection, and retirement ordering. The complete [PR #289 Compose
run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29339077368) also
verifies timer, remote-activity, parent/child success, propagated child
failure, and child cancellation against a real Temporal Server. Dedicated
restart/replay, cache-eviction, patching, and parent/child recovery gates add
their own live evidence; the remaining activity, terminal, and recovery cases
still require their corresponding real-server evidence.

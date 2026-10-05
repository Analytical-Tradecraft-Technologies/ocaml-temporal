# Private native activity execution adapter

`Temporal_runtime.Native_activity_execution` is the private OCaml layer that
turns a decoded Temporal activity task into a typed OCaml function call. It is
connected to the native `Temporal.Worker` path through the one owner-Domain
supervisor. The supervisor provides these typed operations after Rust/Core has
polled and validated the native JSON envelope:

```ocaml
try_poll_activity :
  supervisor -> (Activity_protocol.task option, native_error) result

complete_activity :
  supervisor -> Activity_protocol.completion -> (unit, native_error) result

record_activity_heartbeat :
  supervisor -> Activity_protocol.heartbeat -> (unit, native_error) result

complete_async_activity :
  supervisor -> Activity_protocol.completion -> (unit, native_error) result

record_async_activity_heartbeat :
  supervisor -> Activity_protocol.heartbeat -> (unit, native_error) result
```

For a `Start` task, the native bridge has already leased the opaque task token
and decoded the JSON envelope into the semantic `Activity_protocol.task` value.
This adapter then performs the language-side work: it looks up the activity
type, decodes its one input value, calls the registered OCaml function, and
submits exactly one terminal completion for that token. A `Cancel` task skips
the user function and submits a cancelled completion. Polling and dispatch are
synchronous in this layer; Rust worker threads do not call OCaml callbacks.

The adapter is deliberately independent of the concrete Rust supervisor.  A
deterministic fake supervisor can therefore test every lease and completion
path without a Temporal Server, while the production supervisor remains the
only owner of Rust handles and network state.

## Asynchronous completion

An activity that needs to finish after its worker callback returns uses the
explicit asynchronous definition:

```ocaml
let fetch_embedding =
  Temporal.Activity.define_async
    ~name:"fetch_embedding"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun context prompt ->
      let handle = Temporal.Activity.Async_context.handle context in
      (* Retry while the error is retryable: the handoff may not be accepted
         yet, or the RPC outcome may be uncertain. Production code should back
         off between attempts and bound the retry budget. *)
      let rec deliver submit =
        match submit () with
        | Error error when not (Temporal.Error.view error).non_retryable ->
            Unix.sleepf 0.05;
            deliver submit
        | result -> result
      in
      start_external_request prompt (fun result ->
        let outcome =
          match result with
          | Ok embedding ->
              deliver (fun () ->
                  Temporal.Activity.Async_handle.complete handle embedding)
          | Error error ->
              deliver (fun () -> Temporal.Activity.Async_handle.fail handle error)
        in
        Result.iter_error report_undelivered_result outcome);
      Temporal.Activity.Will_complete_async handle)
```

`Completed` and `Failed` keep completion in the worker callback. Returning
`Will_complete_async` acknowledges the worker lease first; only after that
acknowledgement does the adapter activate the opaque handle and move the copied
binary task token into its asynchronous-lease registry. The callback cannot use
the handle synchronously before the handoff is accepted.

External code can nevertheless race the handoff: the completer may finish
before the callback returns, or while the worker acknowledgement is still
waiting on the supervisor or on a retry after an uncertain submission. Such a
call returns a retryable error (`non_retryable = false`) without contacting
Temporal or reserving a request, so the caller simply retries the same
operation (#766). The handle neither buffers the operation nor blocks the
caller: a buffered request could only report its later native outcome to a
caller that has already returned, and blocking could deadlock when the caller
is the dispatch thread itself. A retry loop always terminates, because every
handle that can no longer be activated is closed and then returns a
non-retryable error: the callback returned `Completed` or `Failed`, raised, ran
as a local activity, or returned another attempt's handle; or the worker
discarded the unaccepted handoff during terminal cleanup. Calling the handle
synchronously from inside the callback is a programmer error; retrying there
cannot succeed because the handoff only starts after the callback returns.

This definition is executable only on the native worker path, where the Rust
bridge can acknowledge the Core handoff and own the later client operation. A
deterministic mock backend rejects asynchronous definitions during worker
construction instead of pretending that it can retain a Temporal task token.

The four handle methods are typed and return `(unit, Error.t) result`:

- `Async_handle.complete` encodes the output codec paired with the definition
  and sends a terminal client completion.
- `Async_handle.fail` sends a structured application failure without rerunning
  the activity callback.
- `Async_handle.cancel` sends a canceled completion with ordered detail
  payloads.
- `Async_handle.heartbeat` sends non-terminal progress through the
  namespace-bound client operation. It currently returns acknowledgement only;
  Core cancellation, pause, and reset flags are not yet represented in the
  public result.

All payloads and task tokens are copied before crossing a boundary. The
handle's private state allows one operation at a time and rejects a different
operation while a retryable request remains in flight or unresolved. Terminal
state is retired after native acceptance or a terminal native rejection. The
adapter validates each complete request through the strict activity-protocol
encoder before calling the supervisor, including metadata uniqueness and size
limits. For a new request, local codec or payload validation failures preserve
the handle and its asynchronous lease, and release the invalid operation key.
The caller may then submit corrected data or choose a different operation;
shutdown still reports the outstanding lease until a terminal operation completes.
If an earlier submission is still uncertain, a local rejection of its retry
cannot clear the original operation key or allow a conflicting request.

On the native worker path, Core worker completions retain the conservative
bilateral policy: only the dedicated `Retryable` bridge status authorizes an
exact-request retry, because Core consumes the worker lease. Late async
operations (heartbeat, complete, fail, cancel) are namespace-bound
`RespondActivityTask*ByToken`/`RecordActivityTaskHeartbeat` RPCs that never
consume that lease, and the server applies at most one terminal response per
activity, answering any later one with `NotFound`. They share one
classification (`Native_worker_policy.async_operation_disposition`):

- An uncertain RPC status such as `Unavailable`, `DeadlineExceeded`, or
  `ResourceExhausted` maps to `Connection`. The handle and adapter lease stay
  tracked and the caller receives a retryable error. A terminal operation
  keeps its key, so only the same byte-identical request may follow until it
  receives a definitive answer. A heartbeat key is released instead: a
  heartbeat is non-terminal and superseded by the next one, so the uncertain
  heartbeat is reported and forgotten and never blocks a newer heartbeat or a
  terminal operation.
- A definitive non-`NotFound` RPC rejection such as `InvalidArgument`,
  `PermissionDenied`, or `FailedPrecondition` maps to
  `Async_heartbeat_rejected` (the status name predates its use for terminal
  operations). It releases the request key, even when it answers the exact
  retry of an earlier uncertain request, and keeps the activity handle and
  adapter lease live. The caller may send corrected details or a different
  terminal operation, for example `fail` after a rejected `cancel`. An
  oversized `complete` result is not rejected this way: the server records a
  terminal activity failure and acknowledges the RPC, so the handle closes.
- `NotFound` maps to `Invalid_state` and closes the handle because the server
  has discarded the token. After an uncertain terminal request, this can also
  mean the earlier attempt was applied.

The activity callback is never rerun for a submission retry. The handle is not a retained activity context: ordinary
`Activity.Context` values are still invalidated when their callback returns.

## Registration and dispatch

An executable activity is registered with its normal typed definition:

```ocaml
let summarize =
  Temporal.Activity.define
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun text -> Ok (String.uppercase_ascii text))
```

The private adapter stores heterogeneous definitions behind an existential
wrapper, indexed by Temporal activity type name.  Each definition keeps its
input and output codecs next to its implementation, so a function cannot be
called with one codec and completed with another.  Duplicate names and
`Temporal.Activity.remote` definitions are rejected before polling begins.

For a `Start` task the adapter performs this sequence while holding its
adapter mutex:

1. Copy the opaque task token.
2. Find the activity type in the immutable registry.
3. Decode zero arguments as the canonical unit payload, one argument with the
   registered input codec, or reject more than one argument.
4. Build an attempt context from the server's heartbeat details and timeout,
   then invoke the implementation and convert its typed `result` into either an
   encoded payload or a structured application failure. Application failure
   retryability and each detail body supplied in `Error.t` are copied into the
   Temporal failure without text conversion; metadata still follows the
   runtime's strict UTF-8 key/value rules rather than becoming an unvalidated
   side channel.
5. Validate the entire completion through the strict activity-protocol encoder
   before admitting it to the pending-completion map. Invalid application
   metadata (including duplicate or oversized keys) becomes a bounded,
   non-retryable application failure with no details, retaining the exact task
   token. Validate that replacement before admitting it too.
6. Submit the completion to the supervisor and remove the token only after the
   supervisor returns `Ok ()`.

The adapter mutex covers this whole transaction, including the user
implementation and the native completion call. The production worker's
dedicated activity Domain therefore executes one OCaml activity callback at a
time and cannot poll a second activity until that callback has returned and
its immediate completion or asynchronous handoff has been acknowledged.
Workflow activations run concurrently on the
calling Domain; both lanes use the same serialized supervisor mailbox for
native operations. This preserves the adapter's token-ledger ownership while
allowing unrelated workflow progress. It does not add parallel OCaml activity
callbacks, and cancellation tasks behind a blocked callback cannot be polled
until that callback returns.

The context-aware form is authored with `Temporal.Activity.define_with_context`:

```ocaml
let summarize =
  Temporal.Activity.define_with_context
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun context text ->
      match Temporal.Activity.Context.heartbeat context Temporal.Codec.string text with
      | Error error -> Error error
      | Ok () -> Ok (String.uppercase_ascii text))
```

`Temporal.Activity.Context.details` returns the ordered payloads from the
previous attempt's last recorded heartbeat (empty on a first attempt). The
value is fixed for the whole attempt: heartbeats sent by the current attempt
are recorded for the next attempt and never replace what `details` returns,
matching the other Temporal SDKs. `heartbeat_timeout` returns the server's
configured interval when one was supplied. `Context.heartbeat` and
`Context.heartbeat_payloads` copy their payloads, validate them through the
same strict activity JSON codec as completions, and send them through the
supervisor mailbox. The mailbox serializes heartbeats with polling,
completion, and shutdown; an arbitrary Rust thread never calls an OCaml
callback.

`Context.info` returns the attempt's task metadata as an abstract
`Temporal.Activity.Info.t`: namespace, the scheduling workflow (ID, run ID,
and type; `None` for a standalone activity), activity ID and type, the 1-based
attempt, whether Core runs it as a local activity, and the first-scheduled,
current-attempt-scheduled, and started timestamps that Core reported. The
adapter copies these values from the validated start task, so the projection
cannot fail; a context from a backend without a Temporal task, such as the
in-process test backend, returns a typed defect instead. Combining the
workflow and activity IDs (plus the attempt when each retry must be distinct)
gives the standard idempotency key for at-least-once side effects. The
metadata is immutable and stays readable after the attempt ends. Asynchronous
activity contexts do not expose it yet.

Before constructing the context, the adapter validates the server timeout: it
rejects negative, sub-millisecond, or out-of-range values instead of rounding
or overflowing them. An accepted timeout is therefore exposed as an exact
whole-millisecond `Duration.t`.

Heartbeat-context conversion failures follow the same task-rejection path as
input codec failures, for both synchronous and asynchronous definitions. For
example, binary heartbeat metadata which cannot be represented by runtime
strings, or a sub-millisecond timeout, produces a bounded non-retryable failure
for the exact leased token without invoking the callback. The adapter retains
that validated failure until native acknowledgement, so a transient submission
failure remains visible to polling and shutdown drain. Once acknowledged,
unrelated queued activities can run normally. The deterministic adapter tests
cover both context failures, both definition styles, and unchanged completion
retries through polling and drain.

The context is valid only while its activity attempt is executing. The adapter
invalidates it before returning from dispatch, including exceptional and
completion-error paths. A retained context therefore returns a typed error
instead of retaining a native pointer or accidentally heartbeating a later
task. A successful heartbeat does not acknowledge or retire the activity lease;
the terminal completion retry map remains the sole owner of completion debt.

`heartbeat_timeout` is copied server metadata, not a local deadline. The
adapter exposes it but does not start a timer or synthesize timeout/retry
behavior; Temporal Core owns timeout decisions and subsequent task delivery.
If Core has already timed out an attempt, the synchronous adapter has no stale
completion recovery. An asynchronous handle remains owned by the SDK until a
terminal client operation is accepted or a confirmed terminal bridge failure
closes it. Neither an uncertain heartbeat RPC outcome nor a definitive
non-`NotFound` heartbeat rejection can retire that lease. A shutdown attempt
that finds an admitted asynchronous lease returns a
retryable outstanding-lease error and leaves the worker graph and handle
usable; the caller must finish the handle and retry shutdown. Only terminal
cleanup after a non-retryable failure closes an admitted handle.

### Heartbeat-timeout retry ownership

Heartbeat-timeout retry is a Temporal state-machine decision, not a second
activity callback that the OCaml adapter should create. The pinned Temporal
Core revision (recorded in [`rust/Cargo.toml`](../../rust/Cargo.toml)) owns
heartbeat aggregation, its local activity watchdog, and the server's retry
policy. When Core learns that an attempt is no longer live, it can deliver one
`ActivityTask::Cancel` with `reason = TimedOut` and independent
`is_not_found`/`is_timed_out` details. When the cancellation says that the
token is not found, Core marks the task as already unknown and suppresses a
duplicate terminal RPC; if the retry policy permits another attempt, Temporal
later supplies a new `Start` task with a new token and attempt number.

The native bridge therefore keeps the boundary deliberately one-way for a
heartbeat: [`record_activity_heartbeat`](activity-protocol.md#heartbeat-document) checks
the leased token and forwards the owned value to Core, whose API is
fire-and-forget. It does not invent synchronous cancellation flags. The flags
and reason arrive asynchronously in the later `Cancel` envelope, and the
adapter preserves them as private outcome metadata while submitting the one
`Cancelled` completion required for that token. Remapping a timed-out cancel
to an OCaml `Failed` result, or submitting a locally generated retry, would
race Core's ownership of the expired token and could send a duplicate or
incorrect completion. The same rule protects worker-shutdown, pause, reset,
and ordinary cancellation paths.

The bilateral tests
[`test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml)
and
[`activity_protocol.rs`](../../rust/core-bridge/tests/activity_protocol.rs)
cover the copied heartbeat context and cancellation details. The live
acceptance contract
[`test_temporal_activity_timeout_contract.sh`](../../test/smoke/test_temporal_activity_timeout_contract.sh)
proves the analogous **start-to-close** timeout retry. The dedicated
[`test_temporal_heartbeat_timeout_contract.sh`](../../test/smoke/test_temporal_heartbeat_timeout_contract.sh)
protects the two-process registration and marker contract for the separate
server-timeout scenario. The complete [PR #276 Compose run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29315361326)
then live-verifies that a no-heartbeat attempt reaches Temporal's heartbeat
timeout and that a new attempt is subsequently delivered. A local OCaml timer
would compete with Core and would not provide that evidence.

An implementation exception is caught at this boundary and becomes a typed
non-retryable failure.  Exceptions are therefore a last-resort defect guard,
not the normal way an activity reports an expected failure.

## Local activities

`Temporal.Activity.start_local` and `execute_local` use the same typed activity
definition and callback registry as remote activities, but emit Core's
`ScheduleLocalActivity` command. Core runs the callback on its local-activity
lane in the worker process and records the result in a history marker; no
activity task is sent to a Temporal task queue. The bridge enables that lane,
accepts the local token (`is_local = true`), and routes its completion through
the existing copied-token ledger. A local resolution therefore reaches the
workflow as the ordinary `ResolveActivity` job and resumes the same typed
future used by remote activities.

Local commands carry attempt, optional original schedule time, the three local
timeouts, retry policy, local retry threshold, and cancellation policy. The
public API deliberately omits remote-only queue, heartbeat, priority, and
eager-execution controls. Core remains the authority for whether an attempt is
retried and for the retry policy; it may either retry immediately or return a
`DoBackoff` resolution when the retry should be delayed. For `DoBackoff`, the
Rust bridge validates Core's attempt, duration, and original schedule time, and
the OCaml supervisor starts a deterministic workflow timer. When that timer
fires it re-emits `ScheduleLocalActivity` with the same activity sequence and
the attempt/schedule metadata supplied by Core. OCaml therefore never chooses
retry policy or invents attempt numbers, and a local completion is never
treated as a remote RPC. Local activities remain experimental. The live baseline executes
`smoke.local_activity` and requires `LOCAL`; the [audited live evidence](live-acceptance-coverage.md)
records its successful run. Local backoff/retry, cancellation and replay/restart
combinations still need dedicated live cases, and interceptors remain absent.

## Cancellation

A `Cancel` task has no activity type or input.  The adapter returns a
`Cancelled` completion with a standard Temporal `Canceled` failure and copies
the exact token unchanged.  The closed cancellation-reason variant is rendered
with stable labels (`not_found`, `cancelled`, `timed_out`, `worker_shutdown`,
`paused`, or `reset`).  Cancellation details remain task metadata owned by the
native protocol; they are not re-encoded into an unrelated application
payload.

The Rust task ledger treats cancellation as an update to the original `Start`,
not as another completion lease.  The update is still delivered to this
adapter while its token remains tracked, including after the start has been
handed to OCaml.  If the start completed before the owner drained the queued
update, the token is gone and the update is stale, so it is discarded without
submitting a duplicate completion.

The callback adapter is serialized: this cancellation task handling does not
preempt a callback already running under its lock. The public activity context
exposes heartbeat/details operations, but no cooperative cancellation probe at
the audited baseline. Workflow-side [scope hooks](workflow-scopes.md) buffer
activity/child cancellation commands; they do not interrupt activity code.
Worker shutdown is a separate lifecycle/drain operation. Focused lifecycle
tests and the live stop marker do not establish a callback-duration or
operational termination bound.

## Completion retry and ownership

The adapter keeps a small token-keyed map of pending completions.  The map is
needed because a native completion call can fail after the activity function
has already run.  Before polling a new task, `poll` retries one pending
completion.  It never invokes the activity implementation again for that
token.  A typed supervisor error or an exception leaves the completion in the
map and returns an error to the caller because lease retirement is not proven.

Only locally validated completions enter this map. A malformed application
payload therefore cannot permanently block later activities or make
`Worker.run` fail with a local protocol error. Once submission begins, even a
replacement failure remains unchanged across uncertain transport outcomes;
the adapter retires its token only after confirmed acceptance. The
`smoke.activity_invalid_failure_details` live acceptance scenario exercises
duplicate keys, oversized keys, and malformed UTF-8 through the public worker,
requiring a normal activity to complete after each rejected failure.

The token is copied on receipt, copied again into the completion, and never
converted to a string.  This preserves arbitrary binary tokens, prevents
caller-owned mutable buffers from being retained, and keeps tokens out of
diagnostics and logs.  The map is protected by one mutex around poll,
dispatch, and completion so two OCaml Domains cannot execute or retire the
same lease concurrently.  The mutex is an OCaml state guard; it does not hold
the OCaml runtime lock while Rust waits.  The concrete supervisor is
responsible for releasing that runtime lock in its C boundary.

The private worker shutdown path calls the adapter's `drain` operation before
closing native Core. It retries retained completions while holding the same
mutex and starts teardown only after the token map is empty. The public
worker reopens admission only when the drain failure is explicitly classified
as `Retryable`. Generic `Connection`, `Not_ready`, and other failures are
fail-closed because this Core revision may already have consumed the lease;
the native graph is cleaned up rather than blindly resubmitting the same
completion. The lease records its first non-retryable failure (including an
async handle admission that fails after Core accepted `WillCompleteAsync`),
and neither a later `poll`, a second `Worker.run`, nor `drain` submits that
completion again: each returns the recorded error without a native call
until terminal `discard` (issue #843). An explicitly retryable failure
preserves the exact completion and the native graph for a later attempt. An
admitted asynchronous handle is such a case: the adapter marks the
outstanding-lease diagnostic retryable, so normal shutdown cannot
force-discard the handle while user code still owns its completion
capability.

### Worker-loop retry policy

The adapter and the worker loop deliberately use two different signals for a
completion transport failure. `Native_activity_execution.poll` returns a
typed error with `retryable = true` only when the supervisor explicitly marks
the source failure as transient; it never searches an error message for words
such as `timeout` or `temporary`. A raised completion is classified by a
separate private exception classifier. An unexpected exception, protocol
failure, invalid state, configuration error, or worker error remains
non-retryable.

`Native_worker.poll_activity` converts that explicit transient result to
`Retry_pending`. `Temporal_runtime.Native_worker_loop` then invokes the public
worker's retry callback, which uses the dedicated bounded 10 ms native retry
backoff before polling again. The C bridge releases the OCaml runtime lock
during that delay; it never blocks the workflow lane or holds the adapter
mutex. Once the same copied completion is accepted, the next loop iteration
is free to poll a new activity. Thus a lost completion acknowledgement cannot
rerun user code, terminate an otherwise healthy worker, or create a busy spin.

The production source currently marks only the explicit bilateral `Retryable`
status as safe for a completion retry. Generic `Connection` and `Not_ready`
statuses are fail-closed because they do not prove that the lease remains
pending. This intentionally conservative policy keeps permanent and protocol
errors visible. The fake-source regressions in
[`test/runtime/test_native_worker_loop.ml`](../../test/runtime/test_native_worker_loop.ml)
cover one transient rejection followed by a successful retry and a permanent
protocol error that stops immediately; the activity execution regression also
covers a specifically classified transient completion exception.

`test/runtime/test_native_activity_lifecycle.ml` keeps this shutdown contract
in a separate focused test. It forces one completion rejection during polling
and another during the first drain, then verifies that the second drain retires
the original binary-token lease. The activity implementation is called once
and the completion is submitted once; retrying never repeats user work.

## Current boundary and deliberate limits

This slice implements typed local activity dispatch, failure/cancellation
completions, strict completion and heartbeat validation, transport retry, and
public worker wiring. The native heartbeat path is covered by focused tests in
[`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml),
[`test/runtime/test_native_activity_lifecycle.ml`](../../test/runtime/test_native_activity_lifecycle.ml),
and [`rust/core-bridge/tests/activity_protocol.rs`](../../rust/core-bridge/tests/activity_protocol.rs),
including binary detail preservation, prior-attempt detail delivery, lease
retention, copied context payloads, callback-exception classification, and
context invalidation. The complete [PR #253 Compose run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29286560471)
also live-verifies server-delivered heartbeat detail/retry, delayed
asynchronous activity completion, and start-to-close timeout retry. The
complete [PR #276 Compose run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29315361326)
also live-verifies heartbeat-timeout-triggered retry, driven by Temporal's
timeout decision rather than a local timer. The companion
[`test_temporal_non_retryable_activity_contract.sh`](../../test/smoke/test_temporal_non_retryable_activity_contract.sh)
protects activity error-type policy matching, and the complete [PR #277
Compose run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29318684069)
live-verifies that a public `Activity` error named by
`non_retryable_error_types` is observed without an unintended second attempt.
The later complete [PR #279 Compose run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29331237061)
re-verified these activity paths together in the prior sixteen-result gate.
The complete [PR #289 Compose run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29339077368)
records the seventeen-result baseline, including the child-retry and
duplicate-ID child-start-failure scenarios that share the same worker and
activity adapter. The [PR #302 Compose
run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29351689638) first
live-verifies a server-delivered, non-immediate retry under the later
two-second-backoff policy and requires the exact
`SMOKE:BACKOFF:RETRIED:SMOKE` result. The timing guard rejects delivery in
under one second; it does not prove the full configured delay. The complete
[PR #439 Compose
run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29824441578) retains
that activity path in its historical baseline. The [current evidence audit](live-acceptance-coverage.md) records the later successful source snapshot.

The worker handoff uses `Will_complete_async` only for `define_async` callbacks.
The later client endpoint rejects that marker and accepts only completed,
failed, or canceled terminal results. This prevents a retained handle from
accidentally re-entering the worker task ledger.

The semantic wire shape already carries the full decoded Temporal activity
context (headers, heartbeat details, timeouts, retry policy, priority, and
standalone run ID).  The adapter currently consumes only the fields required
for typed dispatch; retaining the complete protocol value keeps future
features additive without inventing a second payload format.

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
      start_external_request prompt (fun result ->
        match result with
        | Ok embedding ->
            ignore (Temporal.Activity.Async_handle.complete handle embedding)
        | Error error ->
            ignore (Temporal.Activity.Async_handle.fail handle error));
      Temporal.Activity.Will_complete_async handle)
```

`Completed` and `Failed` keep completion in the worker callback. Returning
`Will_complete_async` acknowledges the worker lease first; only after that
acknowledgement does the adapter activate the opaque handle and move the copied
binary task token into its asynchronous-lease registry. The callback cannot use
the handle synchronously before the handoff is accepted.

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
state is retired only after native acceptance. On the native worker path, an
operation key is retained after a failed submission only when the supervisor
explicitly classifies the failure as retryable; the current bilateral policy
uses the dedicated `Retryable` bridge status for that classification. Generic
`Connection` failures, `NotFound`, and other non-retryable bridge failures
close the handle and remove the pending operation because they do not prove
that replay is safe. Callers may retry only the same byte-identical operation
after an explicitly retryable result; they must not retry a generic async RPC
failure or issue a different operation for the same handle. The activity
callback is never rerun for a completion-submission retry. The handle is not a
retained activity context: ordinary `Activity.Context` values are still
invalidated when their callback returns.

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
5. Validate the completion through the strict activity-protocol encoder.
6. Submit the completion to the supervisor and remove the token only after the
   supervisor returns `Ok ()`.

The adapter mutex covers this whole transaction, including the user
implementation and the native completion call. The production worker's run
loop therefore executes one OCaml activity attempt at a time and cannot poll a
second activity until the first attempt has produced an acknowledged terminal
completion. This deliberate serialization keeps the token ledger and the
supervisor mailbox race-free; it is separate from Rust/Core's own network
concurrency.

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
previous attempt's heartbeat, and `heartbeat_timeout` returns the server's
configured interval when one was supplied. `Context.heartbeat` and
`Context.heartbeat_payloads` copy their payloads, validate them through the
same strict activity JSON codec as completions, and send them through the
supervisor mailbox. The mailbox serializes heartbeats with polling,
completion, and shutdown; an arbitrary Rust thread never calls an OCaml
callback.

Before constructing the context, the adapter validates the server timeout: it
rejects negative, sub-millisecond, or out-of-range values instead of rounding
or overflowing them. An accepted timeout is therefore exposed as an exact
whole-millisecond `Duration.t`.

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
terminal client operation is accepted or a non-retryable bridge failure closes
it. A shutdown attempt that finds an admitted asynchronous lease returns a
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
server-timeout scenario. The complete [PR #276 Compose run](https://github.com/mfow/ocaml-temporal/actions/runs/29315361326)
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
treated as a remote RPC. Local activities are experimental while live Compose
coverage and interceptors remain future work.

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

## Completion retry and ownership

The adapter keeps a small token-keyed map of pending completions.  The map is
needed because a native completion call can fail after the activity function
has already run.  Before polling a new task, `poll` retries one pending
completion.  It never invokes the activity implementation again for that
token.  A typed supervisor error or an exception leaves the completion in the
map and returns an error to the caller because lease retirement is not proven.

The token is copied on receipt, copied again into the completion, and never
converted to a string.  This preserves arbitrary binary tokens, prevents
caller-owned mutable buffers from being retained, and keeps tokens out of
diagnostics and logs.  The map is protected by one mutex around poll,
dispatch, and completion so two OCaml Domains cannot execute or retire the
same lease concurrently.  The mutex is an OCaml state guard; it does not hold
the OCaml runtime lock while Rust waits.  The concrete supervisor is
responsible for releasing that runtime lock in its C boundary.

The private worker shutdown path calls the adapter's `drain` operation before
closing native Core. It retries every retained completion while holding the
same mutex and starts teardown only after the token map is empty. The public
worker reopens admission only when the drain failure is explicitly classified
as `Retryable`. Generic `Connection`, `Not_ready`, and other failures are
fail-closed because this Core revision may already have consumed the lease;
the native graph is cleaned up rather than blindly resubmitting the same
completion. An explicitly retryable failure preserves the exact completion and
the native graph for a later attempt. An admitted asynchronous handle is such a
case: the adapter marks the outstanding-lease diagnostic retryable, so normal
shutdown cannot force-discard the handle while user code still owns its
completion capability.

### Worker-loop retry policy

The adapter and the worker loop deliberately use two different signals for a
completion transport failure. `Native_activity_execution.poll` returns a
typed error with `retryable = true` only when the supervisor explicitly marks
the source failure as transient; it never searches an error message for words
such as `timeout` or `temporary`. A raised completion is classified by a
separate private exception classifier. An unexpected exception, protocol
failure, invalid state, configuration error, or worker error remains
non-retryable.

`Temporal_runtime.Native_worker_loop` converts that explicit transient result
to `Retry_pending`. The production `Temporal.Worker.run` then waits on the
bounded native activity-readiness operation before polling again. The wait is
performed by the blocking worker Domain, while the C bridge releases the OCaml
runtime lock; it never blocks a workflow effect scheduler or holds the adapter
mutex. Once the same copied completion is accepted, the next loop iteration is
free to poll a new activity. Thus a lost completion acknowledgement cannot
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
context invalidation. The complete [PR #253 Compose run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
also live-verifies server-delivered heartbeat detail/retry, delayed
asynchronous activity completion, and start-to-close timeout retry. The
complete [PR #276 Compose run](https://github.com/mfow/ocaml-temporal/actions/runs/29315361326)
also live-verifies heartbeat-timeout-triggered retry, driven by Temporal's
timeout decision rather than a local timer. The companion
[`test_temporal_non_retryable_activity_contract.sh`](../../test/smoke/test_temporal_non_retryable_activity_contract.sh)
protects activity error-type policy matching, and the complete [PR #277
Compose run](https://github.com/mfow/ocaml-temporal/actions/runs/29318684069)
live-verifies that a public `Activity` error named by
`non_retryable_error_types` is observed without an unintended second attempt.
The later complete [PR #279 Compose run](https://github.com/mfow/ocaml-temporal/actions/runs/29331237061)
re-verified these activity paths together in the prior sixteen-result gate.
The complete [PR #289 Compose run](https://github.com/mfow/ocaml-temporal/actions/runs/29339077368)
records the seventeen-result baseline, including the child-retry and
duplicate-ID child-start-failure scenarios that share the same worker and
activity adapter. The [PR #302 Compose
run](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638) first
live-verifies a server-delivered, non-immediate retry under the later
two-second-backoff policy and requires the exact
`SMOKE:BACKOFF:RETRIED:SMOKE` result. The timing guard rejects delivery in
under one second; it does not prove the full configured delay. The complete
[PR #439 Compose
run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578) retains
that activity path in the current baseline.

The worker handoff uses `Will_complete_async` only for `define_async` callbacks.
The later client endpoint rejects that marker and accepts only completed,
failed, or canceled terminal results. This prevents a retained handle from
accidentally re-entering the worker task ledger.

The semantic wire shape already carries the full decoded Temporal activity
context (headers, heartbeat details, timeouts, retry policy, priority, and
standalone run ID).  The adapter currently consumes only the fields required
for typed dispatch; retaining the complete protocol value keeps future
features additive without inventing a second payload format.

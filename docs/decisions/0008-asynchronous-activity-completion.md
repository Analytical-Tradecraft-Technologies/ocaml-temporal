# ADR 0008: Asynchronous activity completion boundary

- Status: implemented; normal delayed completion is live-verified in the
  complete [PR #279 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29331237061),
  while Core response-flag handling and broader edge cases remain pending
- Date: 2026-07-13
- Decision owners: OCaml Temporal maintainers

## Context

The activity worker invokes one OCaml callback at a time, submits a terminal
completion through the Temporal Core worker operation, and invalidates the
attempt context before dispatch returns. If the native completion call is
temporarily unavailable, the adapter retains an owned copy in its retry ledger
and retries it without running the callback again. The ordinary
`Temporal.Activity.define` and `Temporal.Activity.define_with_context` APIs
remain synchronous, while `define_async` provides an explicit capability for
work that finishes after the worker callback returns.

The private activity protocol has a closed `will_complete_async` result. The
OCaml adapter now emits it only for remote `define_async` callbacks that return
`Will_complete_async`, and the Rust bridge exposes separate namespace-bound
client operations for the later terminal completion or heartbeat. The worker
lease and the retained asynchronous lease are deliberately different state
machines.

Temporal Core deliberately treats these two operations differently. Accepting
`WillCompleteAsync` through the worker tells Core that the worker has handed
the activity to external code and removes the worker's outstanding-task
record. A later successful, failed, cancelled, or heartbeat response must use
the namespace-bound Temporal client service, not the worker operation. The
pinned Core client exposes this as `AsyncActivityHandle`.

The implementation must also respect the current ownership boundary. The
activity adapter holds its mutex over dispatch, including the user callback,
and the one SDK supervisor Domain owns all native graph operations. A handle
which recursively acquires the adapter mutex from inside the callback could
deadlock or re-enter activity dispatch. A retained activity context is not a
valid workaround: it is intentionally invalidated when the callback returns.

## Decision

Keep the existing synchronous API unchanged and add an explicit asynchronous
activity definition with a typed outcome. The public shape is:

```ocaml
type ('output) async_result =
  | Completed of 'output
  | Failed of Error.t
  | Will_complete_async of ('output) Async_handle.t

type ('input, 'output) async_implementation =
  Async_context.t -> 'input -> ('output) async_result

val define_async :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) async_implementation ->
  ('input, 'output) t
```

`Async_handle.t` is opaque. Its operations accept and encode the output type
paired with the activity definition and return `(unit, Error.t) result`, never
an exception for an expected Temporal or transport failure. A context helper
returns the opaque handle, but the callback must return the explicit
`Will_complete_async` case. There is no dummy successful output, mutable
context flag, or implicit defer caused by dropping an output value. The
existing `define` and `define_with_context` wrappers remain ordinary
result-returning functions.

The defer handoff is linearized only after a remote activity callback has returned.
At that point the adapter submits exactly one `WillCompleteAsync` completion
to Core. If Core accepts it, the adapter publishes the opaque handle and moves
the token into a separate asynchronous-lease registry. If Core rejects it,
the original worker lease remains in the ordinary retry map and the activity
is not silently abandoned. A handle is never usable before this handoff is
accepted, so a callback cannot synchronously call back through the mutex it is
already holding. If a local activity callback returns `Will_complete_async`,
the adapter instead closes its dormant handle and submits a non-retryable
failure for the same worker token; Core cannot accept a deferred local result.

## Native boundary

Add a distinct supervisor operation and C ABI entry point for terminal
completion of an accepted asynchronous lease. Do not overload the existing
`Complete_activity` operation:

| Operation | Temporal owner | Rust action | OCaml state transition |
| --- | --- | --- | --- |
| `WillCompleteAsync` handoff | Core worker | `Worker::complete_activity_task` with the async status; no server RPC | ordinary worker lease -> async lease, only after acceptance |
| later `Completed`/`Failed`/`Cancelled` | namespace-bound client | `Client::get_async_activity_handle(...).complete/fail/report_cancelation` | async lease remains pending until the client result proves acceptance |
| later heartbeat | namespace-bound client | `AsyncActivityHandle::heartbeat` | async lease remains pending; the current OCaml API exposes acknowledgement only |

The new operation consumes the same strict activity-completion JSON shape but
rejects `will_complete_async`; that variant is valid only for the worker
handoff. Rust must validate the token and payloads again, construct a temporary
Core client handle, and keep all Tokio work on the existing runtime. It must
not consult or retire `Runtime.activity_tasks` for the later client RPC: Core
already removed that worker ledger entry when the handoff succeeded.

The runtime retains the validated worker namespace needed to construct
`ClientOptions` for the async handle. Callers do not supply a second namespace
that could disagree with the worker configuration. The initial implementation
uses the copied task token as the identifier; workflow/run/activity-ID based
handles can be added later only with the same strict identity validation.

## Ownership and concurrency

- The OCaml adapter owns an asynchronous lease record containing an immutable
  copy of the binary token, the activity's output codec, and its lifecycle
  state. Public handles expose neither the token nor a Rust pointer.
- The supervisor owns the native runtime, connection, and client construction.
  Every bridge call is serialized through its owner Domain. Rust/Tokio owns
  network concurrency and never calls an OCaml closure.
- Handle methods may be called from any application Domain after the handoff.
  They enqueue typed supervisor operations; they must not acquire the adapter
  dispatch mutex while an activity callback is running. The implementation
  must document and test its lock order (`handle state` -> `supervisor`, never
  `adapter dispatch mutex` -> callback -> `handle state`).
- The asynchronous handle is independent of `Activity_context`. Context
  invalidation after dispatch therefore cannot invalidate a deliberately
  retained handle, and retaining a context cannot extend a native token's
  lifetime.
- A handle has one terminal transition. Concurrent terminal calls are
  serialized by its lease state: one request is admitted, another request
  while it is in flight returns a typed busy error, and a conflicting or
  repeated terminal outcome after the request settles returns a typed state
  error. No result is silently discarded when a response is uncertain.

## Retry and shutdown

The adapter distinguishes local rejection, explicitly retryable submission,
and terminal native rejection at the private handle boundary. Conversion and
strict protocol validation happen before the supervisor call. A local error
keeps the asynchronous lease and clears a newly reserved invalid request key,
allowing a corrected or different operation without rerunning the activity
callback. A local rejection during a retry preserves any earlier unresolved
submission and its original key.

After native submission, an explicitly retryable result retains the copied
token and operation key; a terminal result, including `NotFound`, closes the
handle and retires its adapter lease. Generic connection failures remain
fail-closed. The state machine retains the key rather than the original
operation value, so a permitted retry must reconstruct the byte-identical
request. Callers must not issue a different operation while that request is
unresolved. Focused regressions cover local metadata conversion errors,
duplicate and oversized metadata caught by full request validation, corrected
requests through all four methods, and retained versus terminal native errors.

Worker shutdown first stops new polling, then drains ordinary worker leases as
it does today. It also accounts for asynchronous leases: if an admitted client
request remains, the adapter returns a retryable typed outstanding-async-leases
error and keeps the worker graph and admitted handles usable for an explicit
retry after the caller finishes them. A non-retryable drain or
native teardown failure invokes the native force-release contract first, then
closes retained async handles and clears their adapter maps; it never sends a
hidden completion. Finalizer cleanup follows the same ownership rule and may
use the existing dedicated cleanup thread, but it cannot run user callbacks or
invent a completion after Core has retired the lease.

## Verification plan

Implementation is staged so each ownership boundary is independently
reviewable:

1. **OCaml adapter tests.** Use a fake supervisor to cover the explicit async
   outcome, callback-return handoff, token copying, context invalidation,
   handle-before-handoff rejection, duplicate/conflicting terminal calls,
   concurrent callers, transport uncertainty, retry without rerunning user
   code, and shutdown with outstanding async leases.
2. **Rust client tests.** Use a mock `WorkflowService` to verify namespace and
   identity construction, task-token completion/failure/cancellation and
   heartbeat conversion, strict malformed JSON rejection, and the fact that
   client completion never retires the Core worker ledger. Test the pinned
   `AsyncActivityHandle` behavior for accepted and already-resolved responses.
3. **Bilateral protocol tests.** Add OCaml and Rust fixtures for terminal-only
   client requests, rejection of `will_complete_async` on that endpoint,
   duplicate/unknown fields, malformed tokens, payload limits, and canonical
   re-encoding.
4. **Live Compose acceptance.** The complete [PR #279 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29331237061)
   verifies the two-OCaml-binary stack's delayed asynchronous completion,
   timeout, cancellation, and graceful-shutdown integration paths. Focused
   fake-supervisor tests remain authoritative for duplicate completion and
   other adapter-only state transitions; broader Core response-flag and
   recovery cases remain follow-up work.

## Current implementation boundary and heartbeat limits

The local implementation now includes the separate namespace-bound async lease
and client terminal-operation path. `Temporal.Activity.define_async` callbacks
can return `Completed`, `Failed`, or `Will_complete_async`; the latter is
activated only after Core accepts the worker handoff. `Async_handle.complete`,
`fail`, `cancel`, and `heartbeat` return typed results and copy payload bytes at
each public boundary. The adapter tests cover handoff ordering, copied tokens,
retry keys, lifecycle errors, and shutdown accounting.

The native heartbeat operation currently acknowledges only successful
submission. Core's cancellation/pause/reset response flags are not yet
represented in the public OCaml result. A dedicated
`AsyncActivityError::NotFound` mapping now becomes a typed non-retryable bridge
error and retires the adapter lease; the focused fake-supervisor test covers
that terminal transition. `heartbeat_timeout` remains copied context metadata;
the adapter does not run a local timeout timer or synthesize retry behavior.
The normal delayed-completion, timeout-retry, cancellation, and worker-shutdown
paths are covered by the local OCaml 5.5 Compose acceptance run; broader Core
response-flag and heartbeat-timeout scenarios remain required.

## Consequences

- Synchronous activities retain their simple, idiomatic API and existing
  behavior.
- Asynchronous activities make the suspension and ownership transfer explicit
  in the OCaml type instead of hiding it in exceptions or mutable context.
- The bridge gains one additional, clearly separated client-RPC operation and
  a second lease state, but the Core worker ledger and the client's server
  protocol cannot be accidentally conflated.
- A handle may outlive the callback, but it cannot outlive the SDK instance
  without returning a typed closed/outstanding error; callers must retain it
  until completion is acknowledged.
- This is a correctness-first design. Throughput optimizations such as
  parallel JSON encoding or multiple activity dispatch Domains are deferred
  until the serialized handoff, retry, and shutdown tests pass.

## Evidence

The current synchronous boundary and lease rules are documented in
[`native-activity-execution.md`](../reference/native-activity-execution.md).
The existing closed protocol representation is specified in
[`activity-protocol.md`](../reference/activity-protocol.md) and implemented in
`lib/protocol/activity_protocol.ml` and
`rust/core-bridge/src/activity_protocol.rs`.

The adapter implementation is in
`lib/runtime/native_activity_execution.ml`; public activity registration is in
`lib/public/activity.ml`. The low-level lifecycle state machine is isolated in
`lib/base/async_activity.ml`, and the native client operations are declared in
`lib/core_bridge/native_bridge.ml` and implemented by the Rust ABI.

The worker/client split and one-owner lifecycle are recorded in
[`ADR 0004`](0004-sdk-instance-supervisor.md). The pinned Temporal Core client
implementation is
[`async_activity_handle.rs`](https://github.com/temporalio/sdk-core/blob/95e97686a079dcfe6c42e3254b2f3f5e3d97408f/crates/client/src/async_activity_handle.rs),
which provides the namespace-bound `AsyncActivityHandle` operations wrapped by
the current bridge. The pinned Core error and heartbeat-response behavior
remain the reference for the follow-up status/response work.

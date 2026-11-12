# Initial Nexus caller and handler contract

Status: **proposed design for review (#532)**. Nothing here is implemented.
The v1 support policy keeps Nexus deferred. This document defines the first
slice that #533 (caller) and #534 (handler) implement, so that both can proceed
against one set of types and one protocol contract.

Baseline: master at the time of writing, Temporal Core
`8fe478ee179ed754614d230f39586f3edb57da71` (v1.1.0). **No Core upgrade is
needed.** Every Core capability this slice uses exists at that pin, and none of
it is behind a feature flag or marked experimental.

## What exists today

- **Bridge.** Worker Nexus polling is hard-disabled.
  `rust/core-bridge/src/worker_bridge.rs` (`bridge_task_types`) sets
  `enable_nexus: false`.
- **Activation jobs.** Nexus activation jobs reach the generic "Core
  activation job kind is not supported" branch of
  `rust/core-bridge/src/workflow_protocol.rs`. That fails the workflow task
  instead of misinterpreting it.
- **Commands.** The OCaml command protocol has no Nexus command.
- **Failures.** Only Nexus failure *decoding* exists. `FailureInfo::NexusOperation`
  and `NexusHandler` convert to `Workflow_protocol.Nexus_operation` and
  `Nexus_handler` and surface as the `` `Nexus `` error cause.

## Core surface at the pin

| Purpose | Core item |
| --- | --- |
| Caller starts an operation | Workflow command `ScheduleNexusOperation { seq, endpoint, service, operation, input, schedule_to_close_timeout, schedule_to_start_timeout, start_to_close_timeout, nexus_header, cancellation_type }` |
| Caller cancels | Workflow command `RequestCancelNexusOperation { seq }` |
| Start outcome | Activation job `ResolveNexusOperationStart { seq, operation_token \| started_sync \| failed }` |
| Final outcome | Activation job `ResolveNexusOperation { seq, result: completed \| failed \| cancelled \| timed_out }` |
| Caller cancellation policy | `NexusOperationCancellationType`: `WAIT_CANCELLATION_COMPLETED` (default), `ABANDON`, `TRY_CANCEL`, `WAIT_CANCELLATION_REQUESTED` |
| Caller durable state | `nexus_operation_state_machine.rs` and `cancel_nexus_op_state_machine.rs` (Core-owned, replayed by Core) |
| Handler polling | `Worker::poll_nexus_task` (one poller at a time) returning `NexusTask { task \| cancel_task, request_deadline, endpoint }` |
| Handler completion | `Worker::complete_nexus_task(NexusTaskCompletion { task_token, completed \| ack_cancel \| failure })`; safe to call concurrently |
| Handler configuration | `WorkerTaskTypes.enable_nexus`, `nexus_task_poller_behavior`, `max_outstanding_nexus_tasks` |
| Handler cancellation | `cancel_task` with reason `TIMED_OUT` (the request deadline) or `WORKER_SHUTDOWN` (after the grace period), answered with `ack_cancel` |
| Endpoints | OperatorService `create/get/update/delete/list_nexus_endpoint` through Core's client |

Core already validates handler completions: the response kind must match the
request kind, and a start failure must carry application or cancelled failure
info. It also downgrades failures for servers that lack
`temporal_failure_responses`.

## Example used by both sides

One endpoint, `ocaml-nexus-example`, routes to the task queue of the handler
worker. It serves one service, `greeting`, with two operations:

- `say_hello`: synchronous, `string -> string`. Returns `"Hello, <name>"` and
  fails non-retryably on an empty name.
- `slow_hello`: synchronous, but its handler waits for a supplied number of
  milliseconds before answering. It exercises the request deadline,
  `TIMED_OUT` cancel tasks and caller timeouts.

Both directions are tested against the pinned official Python SDK, which the
`sdk_interop` regression already runs:

- **#533 (caller):** an OCaml workflow calls a Python handler.
- **#534 (handler):** a Python workflow calls the OCaml handler.

Neither issue therefore waits for the other. The endpoint is created in the
live stack with the pinned admin-tools CLI (`temporal operator nexus endpoint
create`), in the same way the live tests already register search attributes.

## Public OCaml surface

The names are proposals; the shapes follow the existing `Activity` and
`Child_workflow` APIs. All Nexus types live in a new `Temporal.Nexus` module.

```ocaml
module Nexus : sig
  (** A typed operation reference: endpoint-independent service and
      operation names plus the codecs both sides share. *)
  type ('input, 'output) operation

  val operation :
    service:string -> name:string ->
    input:'input Codec.t -> output:'output Codec.t ->
    ('input, 'output) operation

  (** Caller side, inside workflow code only. *)
  type cancellation_type =
    | Wait_cancellation_completed  (* Core default *)
    | Wait_cancellation_requested
    | Try_cancel
    | Abandon

  type 'output handle

  val start_handle :
    ?scope:Scope.t ->
    ?schedule_to_close_timeout:Duration.t ->
    ?schedule_to_start_timeout:Duration.t ->
    ?start_to_close_timeout:Duration.t ->
    ?headers:(string * string) list ->
    ?cancellation_type:cancellation_type ->
    endpoint:string ->
    ('input, 'output) operation -> 'input -> 'output handle

  (** Resolves when Core reports the start outcome: [Ok None] for a
      synchronous start, [Ok (Some token)] for an asynchronous one. *)
  val started : 'output handle -> (string option, Error.t) Future.t
  val future : 'output handle -> ('output, Error.t) Future.t
  val cancel : 'output handle -> (unit, Error.t) result

  val execute : (* start_handle then await the result *) ...

  (** Handler side, registered on a worker. *)
  module Handler : sig
    type context  (* service, operation, request ID, headers, deadline,
                     cancellation flag; no Temporal client in the first slice *)
    type t

    val sync :
      ('input, 'output) operation ->
      (context -> 'input -> ('output, Error.t) result) -> t
  end
end
```

`Worker.create` gains `?nexus:Nexus.Handler.t list`. A worker with an empty
list keeps `enable_nexus = false`, so existing workers poll nothing new.

**Failures stay typed.** A handler returns
`Error (Error.application ~non_retryable ...)` for an operation failure, and
`Error.retryable_handler`, a new constructor, for a transient handler error.
A raised exception is a handler defect: it is reported as a retryable handler
failure with the exception text, never as an operation failure.

## Caller lifecycle and failure mapping (#533)

1. `start_handle` validates options and encodes the input. An invalid option
   returns a ready failed handle and emits no command, as for activities. It
   then allocates the next private sequence and emits
   `ScheduleNexusOperation`.
2. `ResolveNexusOperationStart` resolves `started`:
   - `operation_token` gives `Ok (Some token)`;
   - `started_sync` gives `Ok None`, and `ResolveNexusOperation` follows in
     the same activation;
   - `failed` resolves both `started` and `future` with the converted
     failure, and no resolve follows.
3. `ResolveNexusOperation` resolves `future`:
   - `completed` is decoded with the output codec. A decode failure is a
     typed `` `Codec `` error and not a workflow-task failure, matching
     activities.
   - `failed`, `cancelled` and `timed_out` become `Error.t` with cause
     `` `Nexus ``, the existing `Nexus_operation` failure details (endpoint,
     service, operation, operation token, scheduled event ID), the inner
     cause chain, and `Error.origin` of `` `Cancelled `` or
     `` `Timeout `Schedule_to_close `` / `` `Start_to_close `` /
     `` `Schedule_to_start `` as Core reports it.
4. **Cancellation.** `cancel` (or cancelling the attached `scope`) emits
   `RequestCancelNexusOperation` at most once. Core's state machine applies
   the cancellation type. `Abandon` resolves at once without a server
   command; the others resolve when Core reports the outcome the type waits
   for.

Determinism: the command carries only the private sequence and the user's
inputs, and Core owns the durable state machine and its replay. No new
language-side state survives an activation except the future table that
activities already use. `Temporal.Testing` does not simulate Nexus in this
slice; a call there fails the run with a typed "Nexus is not simulated"
defect.

## Handler lifecycle (#534)

The worker gains a third lane next to the workflow and activity lanes, with
the same ownership rules:

- **Ownership.** The supervisor owns the Core worker. A dedicated Nexus
  Domain, admitted under the runtime's Domain budget like the activity
  Domain, runs handler callbacks.
- **Polling.** The Rust bridge runs `poll_nexus_task` on its Tokio runtime
  and hands each task to OCaml through the existing native event mechanism.
  It never calls OCaml from a Rust thread.
- **Tasks.** Each task is decoded into a typed request: start or cancel,
  plus the deadline. The bridge keeps the task token. The OCaml side holds
  only an opaque, owner-checked completion reference that is released
  exactly once, by completion, `ack_cancel`, or a shutdown abandonment
  report.
- **Starts.** A start dispatches to the registered operation by
  `(service, operation)`. An unknown pair completes with a non-retryable
  `NOT_FOUND` handler failure. The input is decoded with the operation's
  codec, and a decode failure is a non-retryable `BAD_REQUEST` handler
  failure. The callback result becomes `sync_success` (an encoded payload)
  or a failure.
- **Cancel requests.** A user cancel request, a separate
  `CancelOperationRequest`, has nothing to cancel for synchronous operations
  in this slice. It is answered with an empty `CancelOperationResponse`
  (accepted, no-op).
- **Core cancel tasks.** A `cancel_task` (`TIMED_OUT` or `WORKER_SHUTDOWN`)
  sets the running callback's cooperative cancellation flag, in the same way
  as activity cancellation (#494), and is answered with `ack_cancel`. A
  callback that ignores the flag is reported as abandoned by
  `Worker.shutdown_with_report`.
- **Limits.** `Worker.Options` gains `max_concurrent_nexus_tasks` (mapped to
  `max_outstanding_nexus_tasks`) and a Nexus poller count. Neither may be
  combined with a tuner, as Core requires.

All task tokens must be answered before Core can finish shutdown. The bounded
shutdown path therefore treats outstanding Nexus tasks like outstanding
activity tasks: the grace period, then abandonment, never an unbounded join.

## Protocol and bridge contract

- **Commands and jobs.** Two workflow commands (`schedule_nexus_operation`,
  `request_cancel_nexus_operation`) and two activation jobs
  (`resolve_nexus_operation_start`, `resolve_nexus_operation`) are added to
  the strict JSON workflow protocol, its schema under `docs/schemas/`, and the
  Rust converters. The existing catch-all rejections then stop applying to
  them.
- **Handler messages.** Nexus tasks and completions get their own message
  family, mirroring the activity task protocol. They are validated at the
  boundary, and a malformed completion is a typed bridge error, not a Core
  `MalformedNexusCompletion` surprise.
- **Bilateral tests.** Each new message gets OCaml and Rust encode/decode
  tests and decoder-fuzz corpus entries.

## Exclusions in the first slice

These are explicitly not part of #533 or #534 and need their own issues:

- **Asynchronous handler operations**, including workflow-run-backed ones.
  These need operation-token generation, `completion_callbacks` and `links`
  on `Client.start`, and routing of user cancel requests to the backing run.
  The caller side already handles asynchronous operations, because Core
  resolves them, so OCaml callers can use async operations served by other
  SDKs.
- **A Temporal client inside handler callbacks.**
- **Standalone Nexus operation RPCs** (`start_nexus_operation_execution` and
  similar) and endpoint management in the OCaml client. Tests use the CLI.
- **Nexus in `Temporal.Testing`** and replay-corpus histories. #503 will
  capture caller histories once #533 lands.
- **Nexus metrics beyond Core's own.**
- **Interceptors and headers beyond the plain `nexus_header` map** (#1162).

## Tests that close the follow-up issues

- **#533:**
  - live: an OCaml workflow calls `say_hello` (success and non-retryable
    failure) and `slow_hello` with a short `start_to_close_timeout` (typed
    timeout) on the Python handler;
  - cancellation under each cancellation type;
  - a worker restart between start and resolution, replayed from history
    with the same result.
- **#534:**
  - live: a Python workflow calls the OCaml handler;
  - success, application failure, unknown operation, codec failure, and the
    deadline cancel task with `ack_cancel`;
  - bounded shutdown with a stuck handler.
- **Both:**
  - bilateral protocol tests and decoder-fuzz corpus entries;
  - an entry in `docs/reference/live-acceptance-coverage.md`.

## Open questions for review

1. Does Temporal Server 1.32.2 in the Compose fixture enable Nexus endpoints
   by default, or does the dynamic config need a flag? Check this first in
   #533.
2. Should the caller API live in `Workflow.Nexus` or a top-level
   `Temporal.Nexus` with workflow-only functions, as above? The proposal
   follows `Child_workflow`'s top-level placement.
3. Is the "accept and ignore" answer to a user cancel request for synchronous
   operations acceptable, or should it be a non-retryable `NOT_IMPLEMENTED`
   handler failure?

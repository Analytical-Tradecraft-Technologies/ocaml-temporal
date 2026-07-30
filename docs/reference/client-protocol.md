# Native client JSON protocol

This document describes the private JSON messages used between the OCaml
client adapter and the Rust Temporal Core bridge. It is an implementation
boundary, not an API that workflow authors need to construct. Rust owns the
Temporal connection and protobuf types; OCaml owns the typed records and the
decision about how a workflow result is exposed.

The public `Temporal.Client` module does not expose these JSON documents,
start tickets, or native status codes. On an HTTP(S) client, `start` returns a
typed exact-run handle, `wait` hides the bounded polling loop and returns a
typed terminal value, and the exact-run control operations (`cancel`,
`terminate`, `reset`, `signal`, `query`, and `query_with_input`) expose typed
results.
`list_visibility` returns one bounded page. The sections below describe the
private steps that make those public operations safe.

## Why this protocol exists

Temporal Core is Rust code and the final executable is an OCaml executable.
The boundary therefore needs a representation that is easy to inspect,
validate, test, and free without sharing Rust or OCaml pointers. JSON is used
only for this private boundary. Temporal itself still receives protobuf over
gRPC through the official Rust client implementation.

Every operation copies input bytes before Rust releases the OCaml runtime lock.
Rust copies its output into an owned result buffer. The OCaml C stub copies that
buffer into an OCaml `bytes` value and frees the native result in a protected
cleanup path. Synchronous operations do not retain a JSON string or payload
pointer after the call. The asynchronous start operation retains only a
Rust-owned typed request inside its bounded Tokio task; its ticket and final
outcome cross the same copied JSON result boundary.

## Start a workflow

The OCaml side sends one closed object:

```json
{
  "request_id": "start-summarize-1",
  "namespace": "default",
  "workflow_id": "summarize-1",
  "workflow_type": "summarize_document",
  "task_queue": "agents",
  "input": [
    {
      "metadata": {
        "encoding": {
          "encoding": "base64",
          "data": "anNvbi9wbGFpbg=="
        }
      },
      "data": {
        "encoding": "base64",
        "data": "eyJ0ZXh0IjoiSGkifQ=="
      }
    }
  ]
}
```

The example's `data` value is illustrative; the actual encoder emits valid
base64 without whitespace. `input` is ordered and may be empty. Payload
metadata and bytes use the shared workflow payload codec, so binary values are
never treated as UTF-8 text.

Rust validates every identifier, rejects NUL bytes, rejects duplicate or
unknown members, validates payloads, and then calls Core's raw
`WorkflowService::start_workflow_execution`. The first slice deliberately
uses Temporal Server's documented defaults for optional start policies; it does
not invent OCaml-side defaults. The public `Temporal.Client.start` function
accepts an optional `request_id`. When it is supplied, that caller-owned value
is sent unchanged to Temporal; callers should reuse it when retrying a start
whose outcome is uncertain. When it is omitted, the adapter allocates one fresh
ID for that call. The resulting protocol request is created once and reused by
the bounded ticket polls, so polling does not accidentally change the
idempotency key. A request ID identifies one logical start and must not be
reused for unrelated workflow starts.

The direct `start_workflow_json` ABI can return the successful response shown
above, but the public HTTP(S) client uses the asynchronous ticket path. It
begins the request, waits for the ticket to become terminal, and converts the
accepted execution into the typed handle; rejected and unknown outcomes become
typed `Error.t` results. The ticket never leaves the private supervisor.

On success Rust returns:

```json
{
  "execution": {
    "namespace": "default",
    "workflow_id": "summarize-1",
    "run_id": "server-assigned-run-id"
  }
}
```

OCaml checks that the returned namespace and workflow ID still match the
request before exposing the run ID. The complete shape is documented by
[`client-start-request.schema.json`](../schemas/bridge/client-start-request.schema.json)
and [`client-start-response.schema.json`](../schemas/bridge/client-start-response.schema.json).

### Asynchronous start tickets

The owner supervisor can submit the same request through the private
`begin_start_workflow_json` operation when it must keep servicing other
messages while Temporal performs the RPC. Rust returns an opaque ticket:

```json
{"ticket":"4a7c3e0e-3e3d-4b9f-9df2-6e55d3b2b4b7"}
```

The supervisor supplies that object to either `poll_start_workflow_json` or
`wait_start_workflow_json`. Poll returns immediately; wait blocks for at most
the bridge's short bounded interval and then returns `STATUS_NOT_READY`, so a
mailbox loop can handle shutdown and other lifecycle messages between waits.
When the RPC is terminal, the ticket is retired and Rust returns one of these
closed values:

```json
{"kind":"accepted","execution":{"namespace":"default","workflow_id":"summarize-1","run_id":"run-1"}}
```

```json
{"kind":"rejected","error":{"kind":"already_started","workflow_id":"summarize-1","existing_run_id":null}}
```

```json
{"kind":"unknown","request_id":"start-summarize-1","workflow_id":"summarize-1"}
```

`accepted` is proof that Temporal allocated the run. `rejected` is used only
when the returned status proves the start was not accepted. `unknown` is
deliberately not a retry instruction: a timeout, transport failure, or
response-conversion failure may have happened after Temporal accepted the
request. The caller must reconcile that logical request using its stable
`request_id` and workflow identity before deciding what to do next. The ticket
and outcome schemas are
[`client-start-ticket.schema.json`](../schemas/bridge/client-start-ticket.schema.json)
and
[`client-start-outcome.schema.json`](../schemas/bridge/client-start-outcome.schema.json).

The runtime owns the ticket's receiver, validated request, and Tokio task
until one terminal read retires the ticket. If a caller abandons the ticket,
shutdown drains the ticket registry, aborts every remaining task, and joins
each handle before the client or Core runtime is released. A task that has
already placed a result in its receiver is still joined exactly once; the
queued result is then dropped with the receiver rather than being delivered to
an absent caller. This is the cancellation boundary for asynchronous starts:
Rust tasks never call OCaml, and no task is detached while it retains a Core
connection clone.

## Request cancellation of one exact run

The public `Temporal.Client.cancel` operation sends a control-plane request for
the exact execution retained by a workflow handle. The OCaml side supplies the
client namespace and these five fields to Rust:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "run_id": "server-assigned-run-id",
  "request_id": "cancel-summarize-1",
  "reason": "operator requested shutdown"
}
```

`run_id` is required. There is no implicit “latest run” form, so a cancellation
cannot accidentally target a continued-as-new successor or another execution
with the same workflow ID. `request_id` is the Temporal idempotency key for the
logical cancellation operation. If a transport timeout leaves the outcome
uncertain, the caller should retry with the same request ID and exact handle.
If the public `Temporal.Client.cancel` caller omits `request_id`, OCaml derives
a deterministic ID from that handle's workflow ID and run ID, so repeated
attempts for the same exact handle still identify one logical cancellation.
The optional `reason` is copied as operator context and may be empty; it is
bounded and NUL-free like all bridge strings.

Rust calls Temporal's official `RequestCancelWorkflowExecution` RPC and returns
only this positive acknowledgement:

```json
{"acknowledged":true}
```

The acknowledgement means that Temporal accepted the request, not that the
workflow has already stopped. The request is bounded to a short native RPC
deadline so a stalled server cannot hold the single supervisor owner forever.
On timeout the operation returns a typed bridge failure; retrying the same
`request_id` is safe. The caller then uses `Temporal.Client.wait handle` to
observe the eventual `Cancelled` terminal value. Cancellation errors use the
same closed `rpc` and `protocol` error documents as other client operations;
`already_started` is rejected as impossible for this operation.

The request and acknowledgement shapes are defined by
[`client-cancel-request.schema.json`](../schemas/bridge/client-cancel-request.schema.json)
and
[`client-cancel-response.schema.json`](../schemas/bridge/client-cancel-response.schema.json).
Both OCaml and Rust validate every field, reject unknown/duplicate members,
and validate the positive acknowledgement before it crosses the FFI boundary.
The exact-run cancellation path is covered by local mock, supervisor, OCaml
bridge, and Rust protocol tests. The live driver contains the same scenario,
and the complete [PR #289 run](https://github.com/mfow/ocaml-temporal/actions/runs/29339077368)
verified exact-run cancellation, the eventual typed cancelled result, and
graceful shutdown with outstanding work against a real Temporal Server as part
of the recorded seventeen-result baseline. The [PR #302
run](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638) first
verifies the later long-backoff extension, and the complete [PR #439
run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578) retains
exact-run cancellation and graceful shutdown in the current 26-start
baseline. The earlier [PR #277 run](https://github.com/mfow/ocaml-temporal/actions/runs/29318684069)
remains evidence for the prior fifteen-result slice, [PR #253 run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
for the prior twelve-result slice, and [PR #210](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859)
for the original nine-workflow slice. See the [live acceptance coverage](live-acceptance-coverage.md)

## Terminate one exact run

`Temporal.Client.terminate` requests immediate termination of the exact run
held by a typed client handle:

```ocaml
Temporal.Client.terminate ~reason:"operator requested termination" handle
```

The call returns after Temporal acknowledges `TerminateWorkflowExecution`; it
does not wait for workflow code. A later `Temporal.Client.wait handle` returns
`Terminated` with a non-retryable typed error. The request carries namespace,
workflow ID, run ID, and bounded reason text. It deliberately has no
`request_id`: Temporal's terminate RPC has no idempotency-key field. The
deterministic mock preserves this exact-run and terminal-history contract.

The native call has a bounded control-plane deadline so a stalled server cannot
hold the supervisor owner indefinitely. If that deadline expires, the server
may already have accepted the command, so the bridge returns the explicit
`rpc` code `termination_outcome_uncertain` rather than pretending that the
termination was rejected or that a retry is safe. Reconcile this result by
calling `wait handle` (or by checking visibility) before deciding what to do;
there is no idempotency key that can make a blind retry equivalent to the first
request.

The closed documents are specified by
[`client-terminate-request.schema.json`](../schemas/bridge/client-terminate-request.schema.json)
and
[`client-terminate-response.schema.json`](../schemas/bridge/client-terminate-response.schema.json).
Both OCaml and Rust reject unknown or duplicate fields and validate the
acknowledgement before it crosses the FFI boundary. Focused mock, supervisor,
OCaml bridge, and Rust protocol tests cover the exact-run request, terminal
mapping, and validation failures; live acceptance of this operator path remains
the next evidence boundary.

## Reset one exact run from a workflow-task boundary

`Temporal.Client.reset` asks Temporal to create a new run by replaying the
exact execution up to a supplied workflow-task finish event. It is an
operator-facing recovery operation: it does not mutate the existing run and
it never means “reset whichever run is latest”. The public function requires
the original run handle and a non-negative `workflow_task_finish_event_id`,
then returns a new exact-run handle on success.

The private request is a closed object:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "run_id": "server-assigned-run-id",
  "request_id": "reset-summarize-1-4",
  "reason": "replay after deploying a workflow fix",
  "workflow_task_finish_event_id": 4
}
```

The event ID is serialized as a JSON integer literal and remains a signed
64-bit value in OCaml, Rust, and the Temporal protobuf request. This avoids
loss of precision for histories whose event IDs exceed the exact integer range
of a JavaScript number. `request_id` is the idempotency key for one logical
reset; if the caller omits it, OCaml derives a deterministic value from the
exact run and event boundary. Retrying an uncertain transport result with the
same request ID is therefore safe.

Temporal returns the new run ID. The bridge wraps it in the same execution
object used by `start`, and OCaml verifies that namespace and workflow ID still
match the original handle before exposing it:

```json
{
  "execution": {
    "namespace": "default",
    "workflow_id": "summarize-1",
    "run_id": "new-server-assigned-run-id"
  }
}
```

Both sides reject missing, duplicate, or unknown members; empty or NUL-
containing identifiers; oversized reasons; negative event IDs; and responses
whose identity does not correlate to the request. A successful response means
Temporal accepted the reset and supplied a new run identity, not that the new
run has completed. Call `Temporal.Client.wait` with the returned handle to
observe it. The request and response schemas are
[`client-reset-request.schema.json`](../schemas/bridge/client-reset-request.schema.json)
and
[`client-reset-response.schema.json`](../schemas/bridge/client-reset-response.schema.json).

## Send one signal to an exact run

The public `Temporal.Client.signal` operation sends a typed, fire-and-forget
message to the exact workflow run retained by a client handle. It does not
start a workflow, wait for a handler, or follow a continued-as-new successor.
The signal definition supplies the stable Temporal name and its input codec;
the caller receives `Ok ()` only after Temporal acknowledges the RPC.

The private request is a closed object with the same exact-run identity fields
used by cancellation, plus the signal name, idempotency key, and ordered
payload list:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "run_id": "server-assigned-run-id",
  "signal_name": "add_document",
  "request_id": "signal-summarize-1-1",
  "input": [
    {
      "metadata": {
        "encoding": {
          "encoding": "base64",
          "data": "anNvbi9wbGFpbg=="
        }
      },
      "data": {
        "encoding": "base64",
        "data": "eyJ0ZXh0IjoiSGkifQ=="
      }
    }
  ]
}
```

OCaml validates the signal name when `Temporal.Signal.define` constructs the
definition and encodes the input before transport. Rust validates the exact
identifiers, request ID, signal name, payload conversions, and closed JSON
shape again before constructing Temporal's official
`SignalWorkflowExecutionRequest` protobuf. The connected client's identity is
used for the RPC; callers cannot provide a second identity or redirect the
request to another namespace.

When `request_id` is omitted, OCaml allocates a fresh process-wide ID shared by
all `Temporal.Client.t` values in that process. This keeps two independent
handles from accidentally presenting the same signal as a retry of an earlier
delivery. Supply an explicit ID when retrying an uncertain transport result so
Temporal can deduplicate the same logical signal. An idempotency key must not be
reused for a different signal name or payload: the deterministic mock accepts
an exact retry, but returns a typed workflow error when the same ID is paired
with different signal data. The native transport passes the key to Temporal,
whose server-side idempotency behavior remains authoritative. A successful
native response is exactly:

```json
{"acknowledged":true}
```

This acknowledgement says only that Temporal accepted the signal request; a
workflow task may process it later or the run may already be closing. Signal
failures use the closed `rpc` and `protocol` client error documents, while the
start-only `already_started` category is rejected as impossible. Both sides
reject unknown or duplicate members and validate the positive acknowledgement.
The bridge bounds this control-plane RPC to one second, matching cancellation:
an unavailable server cannot hold the supervisor's single owner Domain
indefinitely, and a timeout is returned as a typed `deadline_exceeded` error.
Callers that retry an uncertain result should reuse the same `request_id`.
The request and response shapes are defined by
[`client-signal-request.schema.json`](../schemas/bridge/client-signal-request.schema.json)
and
[`client-signal-response.schema.json`](../schemas/bridge/client-signal-response.schema.json).

The first focused [PR #266 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29311239247)
live-verified the signal path against Temporal Server: the driver waited for
the worker-visible readiness marker before sending the typed signal, then
observed the handler's value after the condition resumed. The recorded
seventeen-result baseline is covered by the [PR #289 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29339077368),
which includes the same signal/condition path. The acknowledgement therefore
remains distinct from handler execution, while the two runs together preserve
the focused and complete live evidence for this client operation.

## Query one exact run

The public `Temporal.Client.query` operation asks Temporal to evaluate a
read-only output-only query handler for the exact workflow/run identity retained
by a typed client handle. `Temporal.Client.query_with_input` uses the same
transport for a `Temporal.Query.define_with_input` definition and sends exactly
one encoded argument. Query handlers remain synchronous and non-suspending; a
successful call returns the handler's typed value after decoding the single
result payload with the query definition's codec.

The private request is a closed JSON object:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "run_id": "server-assigned-run-id",
  "query_type": "current_state",
"input": []
}
```

`run_id` is mandatory, so a query cannot accidentally inspect a different
execution after continued-as-new. OCaml and Rust validate every identifier,
reject unknown and duplicate members, and validate each payload in `input`
before entering the FFI; output-only queries send an empty list and typed
queries send exactly one payload. Rust wraps the list in Temporal's
`WorkflowQuery.query_args`, sets the
non-rejecting query condition, and calls the official `QueryWorkflow` RPC.

On success Rust returns:

```json
{"result":[{"metadata":{},"data":{"encoding":"base64","data":"..."}}]}
```

The result list is preserved through the bridge and the public adapter accepts
exactly one payload for the output codec. A server-side `query_rejected` value,
an RPC failure, or a malformed response is returned as a typed client error;
server diagnostic text does not cross the JSON boundary. The request and
response schemas are
[`client-query-request.schema.json`](../schemas/bridge/client-query-request.schema.json)
and
[`client-query-response.schema.json`](../schemas/bridge/client-query-response.schema.json).

The deterministic mock transport validates the exact execution identity but
does not run workflow code, so mock queries fail with a typed workflow error.
The complete [PR #434 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29684113836)
live-verifies both the output-only and exactly-one-input query handlers against
parked exact runs, including the missing-handler rejection. This slice also
proves the public API, strict protocol, supervisor serialization, ABI state
guards, and official Rust RPC mapping; query deadlines and behavior across
replay or cache eviction remain separate live scenarios.

## Complete a handed-off asynchronous activity

The private client bridge also owns the two operations used after a worker has
accepted `will_complete_async`. They are not ordinary workflow control-plane
requests: the Rust side creates a namespace-bound Temporal client from the
worker configuration and addresses the admitted activity by its opaque task
token. The namespace is therefore supplied by the connected runtime rather
than repeated in these JSON documents.

The terminal request reuses the activity completion semantic record:

```json
{
  "task_token": "AAEC/v8=",
  "result": {"kind": "completed", "result": null}
}
```

`result` may instead be `failed` or `cancelled`, with the same structured
failure shape used by the worker completion protocol. A second
`will_complete_async` marker is rejected: it is a worker-to-client handoff,
not a terminal operation. For a cancelled result, the failure must carry the
standard Temporal `Canceled` info so the ordered cancellation details can be
passed to Core. The endpoint returns an empty successful response after Core
accepts the terminal request.

The corresponding heartbeat request is non-terminal:

```json
{
  "task_token": "AAEC/v8=",
  "details": []
}
```

Heartbeat details use the same ordered binary payload representation as task
inputs and completions. A successful heartbeat only acknowledges submission;
it does not retire the activity or report cancellation, pause, or reset flags.
Those Core outcomes remain asynchronous and are not synthesized into this
client response.

Neither endpoint reads or retires the worker's activity-task ledger. The
worker lease was already handed off, and the OCaml async-activity state machine
keeps its copied token while a client operation is in flight. A successful
terminal request retires that lease; a terminal non-retryable bridge failure
closes the handle and removes the lease. A `NotFound` response is treated as a
terminal inactive-handle condition because retrying cannot make that token
valid again. Other async-client RPC failures are returned as generic
`Connection` bridge errors. The current public worker policy is deliberately
fail-closed: only the explicit bilateral `Retryable` status authorizes replay,
and these async-client endpoints do not produce that status, so callers must
not issue a different operation for the same handle after a generic RPC
failure.

The request shapes are defined by
[`activity-async-completion.schema.json`](../schemas/bridge/activity-async-completion.schema.json)
and
[`activity-heartbeat.schema.json`](../schemas/bridge/activity-heartbeat.schema.json).
The shared task-token, payload, and failure constraints are documented in the
[activity protocol reference](activity-protocol.md); the worker-only
completion schema intentionally remains broader because it includes the
`will_complete_async` handoff marker.

## Shut down the client graph

`Temporal.Client.shutdown` is a lifecycle operation rather than another
client JSON request. It closes the private supervisor graph that owns the
transport, Core client, and runtime; it does not send a cancellation or other
terminal command to any workflow execution on the server. A workflow that is
still running remains a server-side execution and must be observed or
cancelled through a client that is still open.

Shutdown has one linearization point at the public client. The first caller
serializes with other shutdown callers, closes admission before native
teardown, and then caches the exact `(unit, Error.t) result`. Calls that race
with that transition but have already entered the supervisor are allowed to
finish in supervisor mailbox order. Calls that have not entered it fail with
the typed bridge error `client is shut down`; this applies to `start`, `wait`,
`cancel`, `signal`, and `follow`, including calls made through handles retained
before shutdown.

For an HTTP(S) client, the supervisor admits one terminal shutdown request,
waits for earlier admitted operations, and joins its owner Domain. The native
backend then attempts the reverse ownership sequence: replay worker disposal,
worker shutdown, client disconnect, and runtime close. It preserves the first
failure while still running the defensive cleanup steps. Consequently, a
returned teardown error is terminal evidence that the native graph was
consumed or invalidated, not an invitation to retry an operation on the old
client. Repeating `Temporal.Client.shutdown` returns the same cached result,
including that error, without entering native teardown again. The
deterministic `mock://` transport follows the same public closed-state and
idempotency contract, although its cleanup only releases the in-memory
service.

## Wait for one exact run

The wait request contains exactly the three identity fields:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "run_id": "server-assigned-run-id"
}
```

There is intentionally no `follow_runs` member. Rust performs a close-event
history long poll with the equivalent of `follow_runs = false`, bounded to
100 ms per native call. When the run is still open, the call returns
`STATUS_NOT_READY` and no response object; the caller or a later orchestration
loop can retry the same request through its mailbox. A timeout is therefore a
pending observation, not a workflow failure. A terminal response always names
the exact run requested.

The public `Temporal.Client.wait handle` performs that retry loop internally:
it resubmits the same exact-run request after each bounded `NOT_READY` result
and yields the calling Domain between attempts. Code using the private bridge
directly may handle the status itself, but ordinary client callers receive only
a terminal `Ok` value or an outer typed `Error.t`.

For example, a terminal response has this shape:

```json
{
  "execution": {
    "namespace": "default",
    "workflow_id": "summarize-1",
    "run_id": "server-assigned-run-id"
  },
  "outcome": {
    "kind": "completed",
    "result": [],
    "successor": null
  }
}
```

The closed outcome variants are:

| `kind` | Additional members | Meaning |
| --- | --- | --- |
| `completed` | `result`, nullable `successor` | The run completed normally. |
| `failed` | `failure`, nullable `successor` | The run failed with a structured Temporal failure. |
| `cancelled` | `details` | The run was cancelled. |
| `terminated` | `details` | The run was terminated. |
| `timed_out` | nullable `successor` | The run timed out. |
| `continued_as_new` | required `successor` | The requested run ended and created a new run. |

Whenever a successor is present, both sides enforce the same three invariants:

1. successor namespace equals the waited execution's namespace;
2. successor workflow ID equals the waited execution's workflow ID; and
3. successor run ID differs from the waited run ID.

This prevents a malformed response from changing which execution a caller is
observing. OCaml returns `continued_as_new` to the caller; it never follows the
successor implicitly. See
[`client-wait-request.schema.json`](../schemas/bridge/client-wait-request.schema.json)
and [`client-wait-response.schema.json`](../schemas/bridge/client-wait-response.schema.json).

The public client exposes the successor as an opaque-to-codec
`Temporal.Client.execution` value containing the validated workflow and run
identity and its namespace. `Temporal.Client.follow client ~workflow successor`
combines that identity with the caller's existing client and workflow
definition to produce a typed exact-run handle, but first requires the
successor namespace to equal the client's configured namespace. This is not
another protocol message: no start or lookup is sent to Temporal, and no
successor is selected implicitly. The operation only checks the local lifecycle
bit and the same non-empty, NUL-free, 65,536-byte identifier limits used by
`start` and `wait`; malformed, cross-namespace, or shut-down-client input is
returned as an ordinary `Error.t` result.

## Start and await a workflow update

`Temporal.Client.start_update` sends a typed update to an exact workflow run
and returns after a Temporal worker has accepted it. The returned update handle retains
the update name, exact run, caller-supplied or generated update ID, and output
codec. It contains no native pointer and can be held while other updates or
workflow operations are started. `Temporal.Client.wait_update` polls the same
ID until the server returns a completed value or an application failure.

Acceptance and completion are deliberately separate: an accepted update may
still be waiting behind workflow code, and a pending poll is not a failure.
The OCaml supervisor serializes both requests through the one native owner;
Rust owns the Temporal protobuf and gRPC state and returns only copied JSON.
No Rust task calls an OCaml closure, and update failures are typed `Error.t`
values rather than exceptions.

The start request uses this closed object:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "run_id": "server-assigned-run-id",
  "update_id": "update-1",
  "update_name": "set_state",
  "input": []
}
```

Rust validates every identity, update ID/name, payload, and duplicate/unknown
member before invoking Temporal's `UpdateWorkflowExecution` RPC. The response
echoes the update ID and exact execution. Its `outcome` is `null` while the
update is accepted but not yet complete, or a closed `completed`/`failed`
object when Core already has a terminal result. Poll requests contain only the
namespace, exact execution, and update ID; poll responses contain only the
optional outcome. OCaml rejects an outcome or execution that does not match the
handle, so a response for another update cannot be mistaken for success.

The normative schemas are
[`client-update-request.schema.json`](../schemas/bridge/client-update-request.schema.json),
[`client-update-response.schema.json`](../schemas/bridge/client-update-response.schema.json),
[`client-poll-update-request.schema.json`](../schemas/bridge/client-poll-update-request.schema.json),
and [`client-poll-update-response.schema.json`](../schemas/bridge/client-poll-update-response.schema.json).
The OCaml protocol test is
[`test_ocaml_client_update_protocol.ml`](../../test/bridge/test_ocaml_client_update_protocol.ml);
Rust tests cover strict validation and Core response conversion.

## Structured failures

Routine transport and Core failures do not raise OCaml exceptions. Rust emits
one of these closed error documents in the native result's error buffer:

```json
{"kind":"already_started","workflow_id":"summarize-1","existing_run_id":null}
```

`already_started` is used only for a start operation rejected by Temporal's
AlreadyExists status. Other gRPC failures contain only one stable status code,
such as `deadline_exceeded`, `unavailable`, or `permission_denied`; server text
is intentionally discarded because it may contain user data. Core conversion
failures use `{"kind":"protocol","code":"core_invalid"}` or
`core_unsupported`. The complete code vocabulary is enumerated in the JSON
schema and checked by both Rust and OCaml decoders.

Status details are server input too. Rust includes an existing run ID only
when it is non-empty, within the protocol string limit, and free of NUL bytes.
If the optional detail is malformed, the error remains `already_started` but
its `existing_run_id` is `null`. This keeps the status category and JSON body
consistent with the same identifier validation used for OCaml-originated
requests.

The OCaml protocol exposes an abstract `error` and a small `error_view` with a
code, JSON path, and safe message. Payload bytes and raw input documents never
appear in that view. Public API conversion is a later layer; this private
codec does not decide whether a workflow failure is retryable.

At the public boundary, a bridge or codec problem is the outer `Error.t` from
`Client.start`, `Client.wait`, or `Client.cancel`. A workflow that reached a
Temporal terminal state instead remains inside the successful result: for
example, `Client.wait` returns `Ok (Failed error)` or `Ok (Cancelled error)`.

## Validation and ownership checklist

Both implementations validate their own outgoing representation and strictly
decode incoming data. In particular, they reject:

- missing, unknown, or duplicate object members;
- empty, oversized, or NUL-containing identifiers;
- non-canonical payload wrappers, invalid base64, and payload size violations;
- unknown outcome and error variants;
- successor identities that do not remain in the same execution chain; and
- status or protocol error codes outside the documented vocabulary.

The OCaml codec tests live in
[`test/bridge/test_ocaml_client_protocol.ml`](../../test/bridge/test_ocaml_client_protocol.ml).
Rust protocol unit tests live beside
[`rust/core-bridge/src/client_protocol.rs`](../../rust/core-bridge/src/client_protocol.rs),
and ABI-level client tests live in
[`rust/core-bridge/tests/client_bridge.rs`](../../rust/core-bridge/tests/client_bridge.rs).
The machine-readable schemas are
normative documentation for the object shapes, while runtime checks remain
authoritative for duplicate keys, byte limits, and cross-field invariants.

The current milestone wires these messages through private OCaml/C/Rust
bindings and the single-owner supervisor. Public `Temporal.Client` uses this
native path for `http://` and `https://` targets, including asynchronous start
and exact-run wait. The deterministic `mock://` transport remains available
only as a private unit-test seam. The complete [PR #277 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29318684069)
live-verified native client starts and exact-run waits alongside the current
heartbeat-detail retry and exact-run cancellation assertions against a public
worker and real Temporal Server. The signal-specific readiness and handler
assertion is documented above with the focused [PR #266](https://github.com/mfow/ocaml-temporal/actions/runs/29311239247)
run; the complete recorded seventeen-result signal evidence is in [PR #289](https://github.com/mfow/ocaml-temporal/actions/runs/29339077368).
The later complete [PR #279 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29331237061)
re-verified the client start, exact-run wait, cancellation, and graceful
shutdown paths in the prior sixteen-result gate. The complete [PR #289 Actions
run](https://github.com/mfow/ocaml-temporal/actions/runs/29339077368) is the
recorded seventeen-result baseline evidence for those paths.
The boundary and remaining cases are tracked in the
[`two-OCaml-binary acceptance design`](two-ocaml-binary-e2e-acceptance.md).

## List workflow executions through visibility

`Temporal.Client.list_visibility` requests one bounded page from Temporal's
visibility service:

```ocaml
Temporal.Client.list_visibility client
  ~query:"WorkflowType = 'summarize_document'" ~page_size:100 ()
```

The OCaml request contains the connected namespace, the caller's query, a page
size from 1 through 1,000, and an optional opaque continuation token. OCaml
validates the query and token metadata, then serializes one closed JSON object;
Rust validates it again, decodes the token as base64 protobuf bytes, and calls
the official Temporal visibility RPC. Rust reduces each server row to workflow
ID, run ID, workflow type, task queue, and a closed status string before
encoding the response. OCaml strictly rejects unknown fields, missing row
fields, empty identifiers, malformed tokens, and unexpected status values.

The token is never interpreted by OCaml and must be passed unchanged to fetch
the next page. The private request and response shapes are defined by
[`client-visibility-request.schema.json`](../schemas/bridge/client-visibility-request.schema.json)
and
[`client-visibility-response.schema.json`](../schemas/bridge/client-visibility-response.schema.json).
Temporal's protobuf/gRPC communication remains entirely inside Rust; JSON is
only the ownership-safe OCaml/Rust boundary.

# Internal replay worker bridge

This reference describes the bounded replay slice behind the private Rust
bridge. It is an implementation component, not a public OCaml API and not a
standalone live acceptance fixture. Deterministic tests drive the bridge
directly by feeding recorded histories to Temporal Core's replay
implementation. The separate two-generation restart/replay acceptance uses it
indirectly through the private worker path and is live-verified as an
integration; that evidence does not make this bridge a caller-facing replay
API. The public worker still executes normal server activations.

## Why this layer exists

Temporal Core already owns the difficult replay state machine: it turns a
recorded history into workflow activations and checks the workflow's commands
against that history. The bridge should not duplicate that state machine or
make OCaml parse Temporal's generated protobuf types. Instead, Rust validates a
small JSON document, converts its history body to Core's `HistoryForReplay`,
and owns the Core replay worker until it is finalized.

The history feeder is deliberately bounded to one queued history. A caller
that submits a second history waits until Core has consumed the first one.
This keeps replay order explicit and prevents malformed or slow input from
creating an unbounded native allocation. The feeder is FIFO, and dropping its
sender is the documented end-of-input signal.

## Document format

One history is represented by the closed document in
[`replay-history.schema.json`](../schemas/bridge/replay-history.schema.json):

```json
{
  "workflow_id": "workflow-replay-test",
  "history": {
    "encoding": "base64",
    "data": "<canonical padded standard-base64 protobuf>"
  }
}
```

`workflow_id` is separate because Temporal's `History` protobuf carries event
data but does not carry the workflow identity needed to construct a replay
task. `history.data` is the official Temporal Core `History` protobuf encoded
as padded standard base64. JSON is only the private OCaml/Rust representation;
the Temporal Server never receives this document.

The schema describes the shape, but it is not the complete security or
correctness check. The OCaml sender first applies the shared strict JSON and
canonical-payload validator. The Rust decoder then repeats the checks at the
FFI boundary and also:

- rejects duplicate members before Serde decoding and rejects unknown members
  in both the outer and nested objects;
- rejects empty, NUL-containing, or over-limit workflow IDs;
- accepts only `encoding: "base64"`, decodes at most the bridge payload limit,
  and compares the input with a canonical re-encoding; and
- decodes the protobuf and runs Core's `HistoryInfo` invariant validation
  before constructing `HistoryForReplay`.

The encoder uses the same invariant gate and immediately decodes its own JSON
output. This makes a future OCaml adapter fail at the Rust boundary instead
of handing a partially checked history to Core. Parser and Core diagnostics
are mapped to stable private categories; workflow-controlled bytes and server
error text are not copied into an ABI diagnostic.

## OCaml supervisor operation

The public `Temporal` module does not expose a replay handle. Internally,
`Sdk_supervisor.Native_backend` sends these typed operations to the one
supervisor Domain that owns the runtime:

1. `Start_replay_worker` creates the workflow-only Core worker. It is mutually
   exclusive with the live worker and does not require a client connection.
2. `Feed_replay_history` validates and queues one history. The feeder has one
   slot, so a second feed waits for Core to consume the first history.
3. `Wait_replay_workflow` wakes the supervisor when Core has an activation or
   has reached end-of-input. `Try_poll_replay_workflow` then converts the
   activation to the normal typed OCaml workflow protocol and retains its
   completion lease.
4. `Complete_replay_workflow` encodes the typed completion and submits it for
   the exact retained run. If OCaml cannot decode a handoff, the supervisor
   sends the untouched document to `Reject_replay_workflow` so Core's debt is
   retired rather than stranded.
5. `Finish_replay_input` closes the feeder. After all activations are completed
   and `Wait_replay_workflow` has observed `Shutdown`, `Finalize_replay` joins
   the lanes. `Dispose_replay` is the explicit abandonment path used by
   shutdown and error cleanup.

The C stubs copy every OCaml `bytes` value before releasing the OCaml runtime
lock. Rust owns the Core worker and all poll tasks; no OCaml continuation,
pointer, or future is retained by the feeder. This keeps replay communication
on the same single-owner mailbox path as live worker operations while allowing
Rust/Tokio to run Core's internal tasks concurrently.

### Validation, rejection, and status semantics

The sender-side OCaml check and the Rust check intentionally have different
responsibilities. OCaml validates the complete JSON tree, the closed
`workflow_id`/`history` shape, canonical base64, and the decoded payload-size
limit before it makes the C call. Rust repeats those checks from the copied
bytes, then performs the protobuf decode and Core `HistoryInfo` validation.
The OCaml check is therefore an early rejection and allocation guard; it is
not a substitute for the Rust/Core invariant gate.

The private C status names below are exposed in OCaml as
`Temporal_core_bridge.Native_bridge.status`. A successful `wait` is only a
wake signal: the caller must poll again to learn whether the lane became ready
or reached replay shutdown.

| Operation | Success means | Expected failure and owner action |
| --- | --- | --- |
| `Feed_replay_history` | The history entered the one-slot feeder. A full slot applies backpressure; the C stub releases the OCaml runtime lock while the Rust future waits. | `PROTOCOL` (11) means the document or Core history is invalid and no history was admitted. `INVALID_STATE` (5) means the feeder is closed or the worker is absent; do not retry the same input after `Finish_replay_input`. |
| `Try_poll_replay_workflow` | One activation was copied into OCaml and one completion lease was retained. | `NOT_READY` (10) means the queue was empty and no lease exists. If OCaml cannot decode successful bytes, it passes the original byte string to `Reject_replay_workflow`; it never invents a run ID. |
| `Wait_replay_workflow` | The lane either has work or has reached natural shutdown; it does not consume an activation. | `NOT_READY` (10) is the bounded 100 ms timeout and means “service the mailbox, then retry”. `INVALID_STATE` (5) means no replay worker exists. |
| `Complete_replay_workflow` | Core accepted the completion for the exact leased run. The OCaml lease is removed only after that success. | `PROTOCOL` (11) covers malformed JSON or a completion for a different run. A Core/lane failure is `WORKER` (8); use disposal/cleanup rather than silently dropping the retained native graph. |
| `Reject_replay_workflow` | Rust decoded the supplied document, confirmed that its semantic activation equals the retained activation, reported a bounded failure to Core, and retired that lease. | `PROTOCOL` (11) means the document is malformed, decodes to a different activation, or does not identify a retained lease. JSON formatting changes that preserve the same semantic activation are accepted. The original OCaml decode error remains the primary diagnostic. |
| `Finish_replay_input` | The feeder sender was closed; already queued histories remain drainable. Repeating it is harmless. | There is no “history complete” claim here: `Finalize_replay` must still observe shutdown and an empty completion ledger. |
| `Finalize_replay` | Input is closed, Core reported workflow-lane `Shutdown`, every activation was completed/rejected, and the native graph was joined and finalized. | `OUTSTANDING_TASKS` (9) is `ReplayNotDrained`; the worker remains owned and can be drained before retrying. `WORKER` (8) retains the graph when lane or Core finalization fails. |
| `Dispose_replay` | The caller explicitly abandoned replay; queued and leased activations were acknowledged with Core's shutdown-safe empty completion, the replay lane drained any follow-up eviction, and finalization succeeded. | `WORKER` (8) means the worker is still retained for another disposal attempt. Disposal is cleanup evidence only, never replay-success evidence. |

There are two rejection paths after a poll. If the OCaml semantic decoder
rejects Rust-produced bytes, `Protocol_adapter.workflow_poll_result` returns
the original byte string to the private rejection operation. Rust decodes that
document and compares the resulting semantic activation with the retained
lease; it does not compare JSON whitespace or member ordering. If Rust itself
cannot convert a Core activation into the semantic model, no JSON is exposed
to OCaml; Rust rejects that leased run directly with a constant reason. Both
paths retire the native obligation without echoing workflow-controlled bytes in
an error or log.

## Ownership and shutdown

`ReplayWorker` owns two values:

1. a workflow-only `PollLanes` instance, which owns Core's worker and the
   guarded poll/join state; and
2. an optional `HistoryFeeder` sender, whose single owner controls when input
   ends.

Core's Tokio runtime owns network-independent replay tasks. The bridge enters
that runtime only while constructing the worker or driving a feeder send. No
OCaml pointer, callback, continuation, or Rust future is stored in the feeder.
The replay worker uses `PollLanes::start_workflow_only`, so no activity poller
is started for a worker that cannot receive activities.

Normal finalization is intentionally stricter than live-worker disposal. The
caller closes the feeder, continues taking and completing every workflow
activation, and calls `wait_workflow` until it observes `Shutdown`. Only then
does `finalize` join the already-terminating poll lane and consume Core's
terminal worker future. A feeder close by itself is not enough: if a queued
history or completion debt remains, `finalize` returns a typed
`ReplayNotDrained` error together with the still-owned worker. This prevents a
queued history from being cancelled while looking like a successful replay.

When abandonment is intentional, `dispose` is the separate destructive path.
It acknowledges queued or leased replay activations with Core's empty
completion, initiates Core shutdown, and joins the workflow lane while
draining any follow-up eviction activation. A replay eviction has no live
workflow task to fail; sending the live-worker failure completion in this
state can panic inside Core, while an empty completion is explicitly safe even
when shutdown races the local workflow stream. The ledger removes every
activation before its asynchronous acknowledgement, and the join loop handles
identities published after that snapshot, so no replay completion debt is
silently dropped. Disposal must never be used as replay success evidence. A
poll-lane or finalization failure returns the retained worker and a typed error
so the caller can retry; the bridge never silently drops the unfinalized native
graph.

## Current evidence and limits

The focused Rust tests in
[`tests/support/replay_bridge.rs`](../../rust/core-bridge/tests/support/replay_bridge.rs)
cover:

- valid history encode/decode round trips;
- duplicate and unknown JSON field rejection;
- base64 and malformed-protobuf rejection;
- construction and clean shutdown without a Temporal client; and
- admission, activation completion, natural shutdown, and finalization of one
  valid history through the bounded feeder; and
- rejection of finalization after the feeder is closed but before its queued
  history is drained, with explicit disposal of the retained worker; and
- explicit disposal of a leased activation, including the follow-up eviction
  Core emits after its empty replay completion; and
- retention and reporting of a still-shared Core worker during disposal,
  reporting of a joined poll-lane failure, and successful retry after each
  retained owner is safe to release; and
- a deterministic guard that the shared history fixture initializes the
  workflow rather than delivering a fatal-machines-error eviction, so an
  invalid fixture cannot silently reintroduce the shutdown-race panic below.

The ABI-focused integration test in
[`tests/replay_abi.rs`](../../rust/core-bridge/tests/replay_abi.rs) adds null
handle, missing-worker, malformed-document, semantic lease matching, natural
shutdown, and idempotent-disposal coverage.

Two OCaml 5.2 replay-lifecycle CI failures shaped this coverage, both surfacing
as Core's “A non-empty completion was not processed” panic. The first was
bridge-originated: a live-worker failure completion was sent after Core had
already closed the workflow stream. Replay disposal now uses an empty
acknowledgement and drains the eviction that follows it, protected by the
leased-disposal regression.

The second was Core-originated and intermittent. The shared unit-test history
fixture omitted the mandatory `WorkflowTaskStarted` timestamp. Core's
structural replay-invariant validator accepted the document, but its workflow
machines hit a fatal error while applying the task. That fatal error is raised
outside any language completion, so Core auto-fails the workflow task by
submitting a **non-empty** completion through its own internal poll loop. With
`ignore_evicts_on_shutdown` enabled, Core's workflow stream can reach terminal
shutdown during disposal before that in-flight completion is processed, tripping
the same panic. The bridge cannot intercept Core's internal auto-fail, so the
fixture is now a genuinely valid history (every event carries a timestamp,
matching the ABI fixture). A valid history produces no autonomous non-empty
completion, so the race cannot occur, and
`replay_history_first_activation_initializes_workflow` fails deterministically
if the invalid fixture is reintroduced.

That fixture-validation test stops after inspecting the first activation and
uses the explicit replay-disposal path to release its leased activation. It
does not submit an empty completion against the already-completed history:
doing so deliberately creates a replay nondeterminism and a follow-up eviction,
which is useful in the dedicated completion-lifecycle tests but made this
unrelated guard depend on an asynchronous Core shutdown. A scheduled macOS
ARM64 run on 2026-08-28 exposed that dependency by waiting in the test until
the native job timeout. The focused fixture guard now tests only the invariant
named by the test, while the separate replay completion and disposal tests
retain coverage of both cleanup paths.

The OCaml bridge test in
[`test_ocaml_bridge.ml`](../../test/bridge/test_ocaml_bridge.ml) proves that
sender-side canonical-payload validation rejects malformed replay input before
it reaches Rust.

This remains **unit-tested native and supervisor plumbing** for the private
replay handle; it is not a public OCaml replay API. The Rust ABI exports,
OCaml supervisor operations, strict sender/receiver validation, and lifecycle
cleanup paths have focused tests. The separate live restart design is in
[`worker-restart-replay-acceptance.md`](worker-restart-replay-acceptance.md),
and its two-generation Compose test now observes the exact run, replay marker,
retrying activity's attempt-two result, terminal result, and volume cleanup in
the [PR #298 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291).
The [PR #306 CI run](https://github.com/mfow/ocaml-temporal/actions/runs/29356904816)
reuses that replay path in a forced-crash companion gate: generation one must
exit with status 137 and leave no graceful-shutdown marker before generation
two is accepted. This proves worker-process recovery through the replay bridge,
not a public replay API or a separate child-workflow recovery guarantee.
The earlier [PR #253 run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
is historical evidence for the original path. These runs are evidence for the
integrated worker/replay scenario, not for exposing this private bridge
directly.

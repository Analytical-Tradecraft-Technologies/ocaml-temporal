# Internal replay worker bridge

This reference describes the bounded replay slice behind the private Rust
bridge. The bridge itself is an implementation component; applications reach
it only through the public [`Temporal.Replay`](#public-offline-replay-api)
module, which wraps it without exposing handles, documents, or Core types.
Deterministic tests drive the bridge directly by feeding recorded histories to
Temporal Core's replay implementation. The separate two-generation
restart/replay acceptance uses it indirectly through the private worker path
and is live-verified as an integration. The public worker still executes
normal server activations.

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

`workflow_id` is separate because Core's replay worker takes the run's
workflow identity from its caller, not from the history: the ID becomes
`Workflow.info().workflow_id` and the input to every ID-derived command.
Histories recorded by Temporal Server 1.21 or later also store it in
`WorkflowExecutionStartedEventAttributes.workflow_id` (field 28); when that
field is non-empty, the bridge requires it to equal the document's
`workflow_id` (#1046). Histories from older servers leave it empty, and the
supplied ID is then used as given. `history.data` is the official Temporal
Core `History` protobuf encoded as padded standard base64. JSON is only the private OCaml/Rust representation;
the Temporal Server never receives this document.

The schema describes the shape, but it is not the complete security or
correctness check. The OCaml sender first applies the shared strict JSON and
canonical-payload validator. The Rust decoder then repeats the checks at the
FFI boundary and also:

- rejects duplicate members before Serde decoding and rejects unknown members
  in both the outer and nested objects;
- rejects empty, NUL-containing, or over-limit workflow IDs;
- accepts only `encoding: "base64"`, decodes at most the bridge payload limit,
  and compares the input with a canonical re-encoding;
- decodes the protobuf and runs Core's `HistoryInfo` invariant validation
  before constructing `HistoryForReplay`; and
- adds the checks `HistoryInfo` lacks but Core relies on later
  (`validate_replay_preconditions`): a non-empty recorded workflow ID must
  equal the supplied one; the start event must have a task queue, because
  Core's replay mock poll unwraps it and would otherwise panic inside a Tokio
  task and stall the replay for the 30-second idle bound (#1047); event IDs
  must run 1, 2, 3, ... without gaps; and every `WorkflowTaskStarted` event
  must carry `event_time`. The last two are fatal machine errors in Core
  ("History is out of order", "Workflow task started event must contain
  timestamp"); inside a run's first task Core reports them only through its
  autonomous auto-fail path (see [verdicts](#public-offline-replay-api)), so
  rejecting them at admission gives a precise message instead. The history is
  rejected, not normalized: a server never writes such a history, so the
  input is not a faithful export.

The encoder uses the same invariant gate and immediately decodes its own JSON
output. This makes a future OCaml adapter fail at the Rust boundary instead
of handing a partially checked history to Core. Parser and Core diagnostics
are mapped to stable private categories; workflow-controlled bytes and server
error text are not copied into an ABI diagnostic. Malformed documents and
Core's own invariant failures share the message "Temporal replay history
failed strict validation"; each of the four precondition failures above has
its own constant sentence (`ReplayInputError::abi_message`), for example
"Temporal replay history records a different workflow ID than the one
supplied", so the user knows what to fix.

## OCaml supervisor operation

The public `Temporal` module does not expose a replay handle; `Temporal.Replay`
creates and releases one per call. Internally,
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
4. `Complete_replay_workflow` submits the canonical bytes of an
   `Encoded_workflow_completion.t`, produced once by the worker adapter's
   encoder, for the exact retained run; the supervisor does not encode the
   completion again. If OCaml cannot decode a handoff, the supervisor
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
| `Feed_replay_history` | The history entered the one-slot feeder. A full slot applies backpressure; the C stub releases the OCaml runtime lock while the Rust future waits. | `PROTOCOL` (11) means the document or Core history is invalid and no history was admitted. `INVALID_STATE` (5) means the feeder is closed or the worker is absent; do not retry the same input after `Finish_replay_input`. `WORKER` (8) means the slot did not drain within the 60-second replay bound and the history was not admitted. |
| `Try_poll_replay_workflow` | One activation was copied into OCaml and one completion lease was retained. | `NOT_READY` (10) means the queue was empty and no lease exists. If OCaml cannot decode successful bytes, it passes the original byte string to `Reject_replay_workflow`; it never invents a run ID. |
| `Wait_replay_workflow` | The lane either has work or has reached natural shutdown; it does not consume an activation. | `NOT_READY` (10) is the bounded 100 ms timeout and means “service the mailbox, then retry”. `INVALID_STATE` (5) means no replay worker exists. |
| `Complete_replay_workflow` | Core accepted the completion for the exact leased run, or the completion was an empty acknowledgement that Core's already-terminated replay stream can no longer process (see [Bounded Core waits](#bounded-core-waits)). The OCaml lease is removed only after that success. | `PROTOCOL` (11) covers malformed JSON or a completion for a different run. A Core/lane failure is `WORKER` (8), including a non-empty completion stranded by Core's shutdown and a Core reply missing for 60 seconds; use disposal/cleanup rather than silently dropping the retained native graph. |
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

## Public offline replay API

[`Temporal.Replay`](../../lib/public/replay.mli) (#515) is the supported
application entry point. It adds no replay engine: one `replay` call performs
exactly the operation sequence above, in the same order as the private
cold-replay benchmark.

1. `History.of_protobuf ~workflow_id bytes` checks the schema-free bounds
   (non-empty, NUL-free, valid UTF-8 workflow ID of at most 65,536 bytes;
   non-empty history of at most the 128 MiB bridge payload limit) and builds
   the strict replay document above once, with the shared canonical base64
   payload wrapper. The protobuf and Core invariants are checked by the
   bridge at feed time.
2. `replay` validates the registration list exactly as `Worker.create` does,
   then creates a private supervisor (its own owner Domain and Rust runtime),
   starts the workflow-only replay worker, feeds the one history, and closes
   input.
3. The production workflow adapter (`Native_worker_execution.Make`) is
   instantiated with a replay-mode task source whose poll and completion use
   `Try_poll_replay_workflow` and `Complete_replay_workflow`. Workflow code,
   codecs, and handlers therefore run through the same conversion as a live
   worker. The source observes, without altering, each activation and
   completion and records the first refusal.
4. The caller drains until `Finalize_replay` succeeds, waiting through
   `Wait_replay_workflow` while Core reports outstanding tasks. About 300
   consecutive idle waits (30 seconds without an activation) abandon the
   replay as `Replay_error`.
5. On every path, `Supervisor.shutdown` runs next. It disposes an undrained
   replay worker, joins the owner Domain, and frees the runtime; only then are
   the adapter's OCaml executions discarded.

The typed verdict is derived from bridge and Core signals, never from parsing
text:

| Signal | Public result |
| --- | --- |
| Finalization succeeds with no refusal recorded, after an `Initialize_workflow` job was delivered | `Ok ()` |
| Finalization succeeds with no refusal recorded, but no `Initialize_workflow` job was ever delivered | `Invalid_history`: Core never started the run, so no workflow code was checked |
| `Remove_from_cache` with reason `Nondeterminism` | `Nondeterminism { run_id; message; mismatch }` with Core's mismatch text |
| A completion carrying `task_failure`, an adapter rejection that retired its lease (for example an unregistered workflow type), or eviction reason `Lang_fail`/`Unhandled_command` | `Workflow_task_failed` |
| `Feed_replay_history` returns `PROTOCOL` | `Invalid_history` with the bridge's constant category message |
| Eviction reason `Fatal` or `Unspecified` | `Invalid_history` with Core's eviction message |
| A replay activation the OCaml protocol cannot represent (poll `PROTOCOL`) | `Unsupported_history` |
| Registration, options, runtime start, or any other supervisor failure | `Replay_error` |

Eviction reason `Unspecified` is a refusal, not housekeeping (#1045). A
machines error raised while Core applies a run's **first** workflow task
happens before any language completion exists, so Core auto-fails the task
itself (`ActivationOrAuto::AutoFail`) and its workflow stream evicts the run
with `EvictionReason::Unspecified` when the failed completion is an
auto-completion (`workflow_stream.rs` at the pinned revision). The same error
in a later task keeps its `Fatal` or `Nondeterminism` reason. No other replay
path at the pinned revision produces `Unspecified`. Before this rule, such a
history was acknowledged as routine cache housekeeping and replayed as
`Ok ()` without running any workflow code. The `Initialize_workflow` rule
above is an independent second guard: whatever Core's eviction says, `Ok ()`
requires that the registered workflow actually received the run.

The first refusal wins; later evictions are consequences of it. Every copied
message is truncated to 4,096 bytes on a UTF-8 boundary. Core's
nondeterminism text names history events and command kinds, not payloads.
Core also logs each failed workflow task at `WARN` through the runtime's
standard-error logger, as it does for a live worker.

### Nondeterminism diagnostics

The verdict above never depends on text, but a developer still needs to know
which workflow, event, and command diverged. `Nondeterminism` therefore also
carries a `mismatch` record (#529):

| Field | Source |
| --- | --- |
| `workflow_id` | The ID passed to `History.of_protobuf` |
| `workflow_type` | The `Initialize_workflow` job Core delivered for the run, before any eviction |
| `event_id`, `event_type` | Core's `HistoryEvent(id: N, Type)` rendering of the first unmatched recorded event |
| `command` | The `<M> machine does not handle this event` / `cannot handle this event` state machine Core names, normally the command the changed code produced |
| `reason` | The `message` field of the failure inside Core's `Debug` envelope, or the whole message without an envelope |

At the pinned Core revision a mismatch reaches the eviction as the `Debug`
rendering of the workflow-task failure, for example `Workflow activation
completion failed: Failure { failure: Some(Failure { message: "[TMPRL1100]
Nondeterminism error: Complete workflow machine does not handle this event:
HistoryEvent(id: 5, TimerStarted)", ... }), force_cause:
NonDeterministicError }`. This wording is not a protocol, so extraction is
best effort and fails closed: every field that Core's text does not supply in
the expected shape is `None`, and `message` still carries the complete bounded
text. For example, Core reports a recorded patch marker that no `patched` call
claimed as `Non-deprecated patch marker encountered for change <id>, but
there is no corresponding change command!`, with no event ID or command, and
those fields stay `None`. Core reports no OCaml source location, so none is
offered. A Core upgrade must recheck these shapes with the replay diagnostics
test below.

`failure_message` keeps its `nondeterminism (run RUN_ID): ` prefix and then
renders the workflow type and ID, `recorded event N (Type) does not match the
current code's M command` when both are known, Core's `reason`, and a reminder
to guard intentional changes with `Temporal.Workflow.patched`. The line never
includes payload bytes because Core's text names event and command kinds,
not payloads.

Every `failure_message` is one line of at most about 3 KB, even for a
hostile history: `History.of_protobuf` accepts workflow IDs of up to 64 KiB
that may contain line breaks, and a history can carry any workflow type. Each
interpolated value is escaped (`\\`, `\n`, `\r`, `\t`, and `\xHH` for other
control bytes, DEL, and invalid UTF-8) and bounded after escaping: 256 bytes
for the run ID, workflow ID and type, event type, and command, and 1,024
bytes for Core's reason or any other diagnostic message. A longer value ends
in `...(N bytes truncated)` without splitting a UTF-8 character. Only the
rendering is bounded; the `mismatch` fields keep their exact values.

A live worker needs no SDK change to surface the same mismatch. Core fails the
workflow task with cause `NonDeterministicError` and the same `[TMPRL1100]`
message, which the Temporal UI and `temporal workflow show` display on the
`WorkflowTaskFailed` event, and logs it at `WARN` with the run ID through the
runtime logger. The run stays open and the server retries the workflow task
until compatible code is deployed.

The Temporal CLI exports histories as protobuf JSON. The pinned Core revision
derives a Rust-specific JSON form instead of accepting that one, so the public
API accepts the binary protobuf and documents conversion rather than adding a
JSON-history converter to production; the
[task-failure fixtures](../../test/integration/temporal/task_failure/histories/README.md)
were converted that way. An application that can reach the server needs no
export at all: `Temporal.Client.fetch_history` (#1137) returns the complete
binary history of a run as a `Replay.History.t`, with every page fetched and
concatenated inside the bridge (see the
[client protocol](client-protocol.md#fetch-the-complete-history-of-a-run-1137)).

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

### Bounded Core waits

Every replay wait on Core either has a bound or is shown to terminate, so a
Core or scheduler defect becomes a typed `Replay_error` instead of a
supervisor Domain that blocks forever (issue #965):

| Wait | Bound | Outcome when the bound is reached |
| --- | --- | --- |
| Feeder send (`feed_json`) | `REPLAY_CORE_WAIT_TIMEOUT`, 60 s | `ReplayWorkerError::TimedOut`; the history is dropped unsent. |
| Completion and rejection replies | 60 s, or 250 ms after Core's workflow stream has ended | Empty acknowledgement: success. Non-empty: `CompletionStranded`. No reply and no stream end: `CoreWaitTimedOut`. The lease stays retired in every case, because Core may already have consumed the completion. |
| Disposal acknowledgements | Same as completions | Outcome ignored; disposal continues. |
| Replay lane join (finalize and dispose) | 60 s for the whole join, including every acknowledgement it awaits | The lane task is aborted and awaited, then reported as a `PollLane` error with the worker retained. The worker is marked as having timed out its join; see below. |
| Core `finalize_shutdown` | Not timed | Runs only after a lane join that succeeded. It then terminates: Core's workflow thread has left its stream loop, the mock client answers in-process, and Core caps its final slot-permit wait at five seconds. A timeout would have to drop the consumed Core worker mid-finalization. |

A lane join that times out means Core never confirmed shutdown, so Core's
workflow thread may never stop and `finalize_shutdown`, which joins that
thread, could block forever. The worker therefore records the timeout, and
the lane slot being empty afterwards no longer counts as a successful join:

- a later `finalize` returns a `PollLane` error with the worker retained,
  because finalization is replay-success evidence; and
- a later `dispose`, including the two attempts made by `drop_runtime_graph`,
  releases the graph by **deliberately leaking the Core worker** and returns
  success.

The leak is the chosen trade-off. Detaching the finalizer into a Tokio task is
not safe, because Core joins its thread through `spawn_blocking` and dropping
the Tokio runtime waits for blocking tasks, which would move the hang into the
runtime drop. A leak is bounded to that one worker. Its allocation, its Core
workflow thread if the thread never exits, and its registration with the
in-process replay client stay alive until the process exits. Core's Tokio
tasks are still cancelled when the runtime is dropped. The poll lane has
already been aborted, the ledger and queues are dropped, and no OCaml value
refers to the worker, so the leaked worker can never be used again. Each leak
increments a process-wide counter (`replay_workers_leaked`) and writes one
static line to stderr. The first attempt has already returned the typed lane
error that OCaml reports as `Replay_error`.

The completion bound fixes the hang in issue #965. Core's
`complete_workflow_activation` sends the completion and a oneshot reply sender
into its workflow stream's unbounded Tokio channel, then awaits the reply.
When the stream ends, Core relies on the channel receiver's drop to discard
queued messages, which also drops the reply sender, and it treats a dropped
reply to an empty completion as "ignored". Tokio's receiver drain cannot see a
message whose send passed the channel's closed check before the receiver
closed but finished its push after the drain. That message, with its reply
sender, then lives until the last channel sender is dropped. Core's
`Workflows` owns that sender, and the bridge keeps the worker alive while it
waits, so the completion waits forever.

In replay this interleaving is routine. With `ignore_evicts_on_shutdown`,
Core's replay stream reaches its natural shutdown while the final eviction
acknowledgement is being sent. Most runs log Core's "Tried to interact with
workflow state after it shut down" warning and continue. Rarely, the send
lands in the race window. The workflow lane then reports `PollError::ShutDown`
only after Core's processing thread has dropped its receiver, so a reply still
missing after that point can never arrive. The lane records this as a
Core-originated close, separate from a bridge-initiated shutdown, and the
bounded await uses it to release the stranded completion.

Dropping Core's completion future after its first poll is safe. The message
was sent synchronously on that poll, and the future then owns only the reply
receiver. Live workers keep the unbounded await, because their whole shutdown
is already bounded by `drain_and_join_for_shutdown`.

## Current evidence and limits

The focused Rust tests in
[`tests/support/replay_bridge.rs`](../../rust/core-bridge/tests/support/replay_bridge.rs)
cover:

- valid history encode/decode round trips;
- duplicate and unknown JSON field rejection;
- base64 and malformed-protobuf rejection;
- the admission preconditions: a recorded workflow ID that differs from the
  supplied one, a start event without a task queue, event IDs with a gap or
  not starting at 1, and timestamp-free `WorkflowTaskStarted` events are each
  rejected with their own category and distinct static ABI message, while a
  history that records no workflow ID accepts any;
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

`replay_worker_accepts_one_history_document` runs under a 30-second test
deadline that names the step it was blocked in, so a regression of issue #965
fails fast instead of consuming the CI job timeout. The
`stranded_completion_tests` module in
[`worker_bridge.rs`](../../rust/core-bridge/src/worker_bridge.rs) checks each
outcome of the bounded completion await with model reply futures: a pending
reply after Core's shutdown is stranded, a bridge-only close is not, and a
ready or late-but-within-grace reply is returned. Its
`lane_join_bound_includes_a_stuck_acknowledgement` test injects a 200 ms join
bound and an acknowledgement that never completes, and requires the join to
time out near that bound with the lane aborted.
`replay_dispose_after_join_timeout_leaks_instead_of_finalizing` installs a
lane that never stops, with the same short bound. It requires the first
disposal to return the typed lane error, finalization to refuse, and the
retried disposal (the `drop_runtime_graph` path) to return through the leak
without entering Core's finalizer.

The ABI-focused integration test in
[`tests/replay_abi.rs`](../../rust/core-bridge/tests/replay_abi.rs) adds null
handle, missing-worker, malformed-document, semantic lease matching, natural
shutdown, and idempotent-disposal coverage.

The [replay history corpus](history-corpus.md) drives the OCaml supervisor
operations above with real workflow definitions. Its Docker-free test replays
every recorded corpus history and requires either natural finalization with
the recorded run identity or, for the negative controls, Core's nondeterminism
eviction.

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

[`test_public_replay.ml`](../../test/bridge/test_public_replay.ml) uses only
the public `Temporal.Replay` API against the five retained live task-failure
histories. Compatible code replays all five (including histories with earlier
failed tasks and deliberately failed executions); a removed timer and an
activity scheduled in place of a timer are `Nondeterminism`; a defect and an
unregistered type are `Workflow_task_failed`; non-protobuf, truncated, and
event-free inputs are `Invalid_history`; the `body` history replayed under
another execution's ID is `Invalid_history` while its recorded ID, or any ID
once the recorded one is removed, replays cleanly; histories derived from
`body` by a small test-only protobuf wire editor (all `eventTime`s scrubbed,
an event-ID gap, no start-event task queue, and a `WorkflowTaskScheduled`
event without attributes, which Core auto-fails inside the first task with
eviction reason `Unspecified`) are `Invalid_history` against both the
compatible and a deliberately changed definition, never `Ok ()`; invalid registrations and options are
`Replay_error`; and 140 alternating successful and nondeterministic replays,
more than OCaml's 128 simultaneous Domains, prove that each call releases its
supervisor Domain and native graph. Its nondeterminism cases also assert the
`mismatch` fields: a removed timer is event 5 `TimerStarted` against a
`Complete workflow` command, an activity in its place is the same event
against an `Activity` command, and an added second timer is event 16
`WorkflowExecutionCompleted` against a `Timer` command. Replayed against
`body` with its recorded workflow ID removed, a workflow ID with
line breaks, a tab, a backslash, an escape byte, a multi-byte character, and
60 KB of padding must render as one valid UTF-8 line of at most 3,072 bytes
with the escapes and truncation marker, while `mismatch.workflow_id` stays
byte-for-byte equal to the input. A history one byte longer than
`Replay.History.max_bytes` is rejected by `History.of_protobuf` with the
documented size message.

The installed-package consumer
[`installed_replay.ml`](../../test/fixtures/install-consumer/installed_replay.ml)
runs `Temporal.Replay` from outside the SDK workspace, both after
`make test-install` and in the relocated binary-only consumer. It replays the
corpus history `live-2026-10-08-timer.pb`: the recorded `corpus.timer` code
must pass, and the same type with its timer removed must be `Nondeterminism`
at the recorded `TimerStarted` event. The
[replay command's process test](../../test/replay_cli/test_replay_cli.ml)
feeds a sparse file of `max_bytes + 1` bytes and requires exit status 3.

[`test_replay_diagnostics.ml`](../../test/bridge/test_replay_diagnostics.ml)
replays the [history corpus](history-corpus.md)'s two negative controls
through the public API. `negative-timer-removed` must report workflow type
`corpus.timer`, workflow ID `history-corpus-timer`, event 5 `TimerStarted`,
command `Complete workflow`, and Core's exact reason, on one
`failure_message` line without the recorded result payload.
`negative-patch-active-on-legacy` must name the patch ID while leaving the
event and command `None`, because Core's text has neither.

Beneath that API, the bridge remains **unit-tested native and supervisor
plumbing** for the private replay handle. The Rust ABI exports,
OCaml supervisor operations, strict sender/receiver validation, and lifecycle
cleanup paths have focused tests. The separate live restart design is in
[`worker-restart-replay-acceptance.md`](worker-restart-replay-acceptance.md),
and its two-generation Compose test now observes the exact run, replay marker,
retrying activity's attempt-two result, terminal result, and volume cleanup in
the [PR #298 Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29346853291).
The [PR #306 CI run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29356904816)
reuses that replay path in a forced-crash companion gate: generation one must
exit with status 137 and leave no graceful-shutdown marker before generation
two is accepted. This proves worker-process recovery through the replay bridge,
not a public replay API or a separate child-workflow recovery guarantee.
The earlier [PR #253 run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29286560471)
is historical evidence for the original path. These runs are evidence for the
integrated worker/replay scenario, not for exposing this private bridge
directly.

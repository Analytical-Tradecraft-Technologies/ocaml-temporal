# Native Core Bridge ABI

This document is for contributors changing the OCaml/C/Rust boundary. Workflow
authors do not call this interface directly.

The product is an OCaml Temporal SDK, not only a Temporal service client. It
implements workflow workers and the deterministic workflow runtime as well as
client operations that start and observe workflows. The final worker process
is owned and launched by OCaml. The public OCaml library calls private C stubs,
which call the versioned Rust ABI documented here. Rust links the official
Temporal Core library and never invokes arbitrary OCaml functions from its
background threads.

## Version and symbols

ABI version 4 uses only symbols beginning with `ocaml_temporal_core_v4_`.
Before using the bridge, OCaml asks Rust which ABI version it implements and
checks that it matches `OCAML_TEMPORAL_CORE_ABI_VERSION`. The bridge represents
the Rust runtime with one opaque handle. Client connection and worker state are
subordinate Rust-owned state in that runtime; they are not separate handles
passed through the C ABI. “Opaque” means OCaml can pass the runtime handle back
to Rust but cannot inspect the Rust object it refers to. A connection is only
one internal part of the SDK; the public package is not merely a service client.

Version 2 is intentionally incompatible with version 1. The worker
configuration document now contains a required, strict `versioning` object;
the versioned symbols and negotiation constant therefore change together so
an OCaml object and Rust archive built from different contracts fail during
startup negotiation instead of reaching worker construction with ambiguous
JSON semantics.

Version 3 is intentionally incompatible with version 2 for the same reason.
The client start request carries a strict `id_conflict_policy` field that a
version 2 Rust decoder rejects as unknown, and the start response and start
outcome documents carry a required `started` flag that a version 2 Rust
archive never emits and a version 2 OCaml decoder rejects as unknown. Without
a bump, either mixed pairing would pass negotiation and then fail every
`Client.start` with a protocol error. Renaming the symbol prefix to
`ocaml_temporal_core_v4_` together with the constant makes a stale archive or
stale OCaml object fail at link time or during startup negotiation instead.

Version 4 is intentionally incompatible with version 3. The closed client
error document gained a `query_failed` kind that carries the bounded message
of a workflow query handler failure (issue #823). A version 3 OCaml decoder
rejects that kind as unknown, so a stale OCaml object paired with a version 4
archive would turn every query handler failure into a protocol error, and a
version 3 archive never emits it. The symbol prefix therefore moves to
`ocaml_temporal_core_v4_` together with the constant.

Version 4 also carries the current-run selector (#791) without a bump.
Client requests after start may send an empty `run_id`, which a version 4
archive built before that change rejects as an invalid identifier, and a
current-run wait response echoes that empty run ID. Neither direction can
silently change meaning: an older OCaml object never sends an empty run ID,
so every document it exchanges with a newer archive is byte-for-byte
unchanged, and a newer OCaml object paired with an older archive gets a typed
protocol error for current-run operations only, before any RPC, while every
exact-run operation keeps working. No release has shipped a version 4
archive, and renaming every symbol would collide with concurrent bridge work,
so the selector is recorded here as an additive version 4 capability rather
than a version 5 contract. The completed-run successor exposed by the OCaml
client in the same change (#837) was already part of the version 4 wait
response.

The canonical header is
`rust/core-bridge/include/ocaml_temporal_core.h`. Both Rust and C compile-time
assertions protect the status width, every numeric status value, and field
ordering/size of the documented `repr(C)` structures. This is intentional:
the C header is consumed by OCaml's private stubs and by downstream native
executables, so a seemingly harmless enum renumbering or padding change must
fail during compilation instead of becoming a delayed, cross-language memory
or error-handling defect. C11 and C++11 consumers get equivalent checks.

## Semantic workflow adapter

`rust/core-bridge/src/workflow_protocol.rs` is the Rust-only protobuf boundary
for the first activation/completion slice. It converts pinned official Core
types to a closed semantic model, serializes that model as strict JSON, and
performs the inverse conversion for workflow commands. The private OCaml module
`Temporal_protocol.Workflow_protocol` implements the same model and validation
without importing protobuf definitions.

Both encoders check their own output against the receiver's rules before it can
cross the native boundary. Rust reparses its output; OCaml applies the
decoder's semantic rules to the validated tree and the parser's raw-text
preflight to the serialized bytes, so payload bytes are not parsed and base64
decoded a second time (#846).
Both decoders reject duplicate or unknown fields, unknown variants, numeric
range violations, non-canonical base64, invalid workflow invariants, and
oversized values. Core fields not represented by the current semantic slice are
accepted only at their documented default; a non-default value returns a typed
`Unsupported` conversion error rather than being lost. See the
[protocol reference](core-protocol.md), machine-readable schemas, and
[ADR 0006](../decisions/0006-first-workflow-semantic-protocol.md).

The [structured parser fuzz smoke](bridge-parser-fuzz.md) mutates committed
valid and rejected fixtures against the independently callable Rust envelope,
payload, activation, and completion decoders. Its limits and instrumentation
scope are stated separately from the fixed-fixture and C ABI sanitizer tests.

The [lifecycle stress](bridge-lifecycle-stress.md) runs seeded,
reproducible operation sequences against the ABI. The sequences interleave
runtime, replay-worker, and live-worker operations: create, poll, complete,
reject, shutdown, finalize, free, and GC-fallback dispose, including calls
that misuse the ABI. A model and two ledgers check every handle and lease
against them. A companion OCaml test runs the custom-block finalizer and
supervisor cycles under GC pressure.

There is one normal-start compatibility default in the initializer: Temporal
Core maps the server's `first_workflow_task_backoff` to
`cron_schedule_to_schedule_interval`, and Temporal Server sends an explicit
zero duration for an ordinary non-cron start. The bridge accepts exactly that
zero value because it carries no scheduling meaning. A non-zero duration (or a
cron schedule) remains `Unsupported` until the semantic protocol models the
delay explicitly.

After protocol decoding, the private pure-OCaml
[`Native_execution`](native-execution-translation.md) adapter translates jobs
into the deterministic execution kernel and translates its commands back into
the checked completion model. It preserves activation metadata, initialization
records, sequence ordering, cancellation reasons, eviction details, and copied
payload bytes without exposing Rust state. Activity commands now carry every
Core-required field and are accepted only after exact validation. Child-start
commands now retain their workflow identity and input payload. Rust injects the
worker's already validated namespace at the Core boundary because Core copies
it into child failure metadata; the other options not yet exposed by the OCaml
runtime remain at explicit Core defaults and are rejected if a reverse
conversion encounters non-default values. Core child-start and
terminal-resolution jobs are also converted losslessly. The OCaml runtime
stores the start run ID, retires a failed start immediately, and accepts a
terminal result only after that start acknowledgment. No field is silently
dropped.
See the translation reference for the complete mapping table and test coverage.

## Private replay worker plumbing

`rust/core-bridge/src/replay_bridge.rs` contains the first bounded replay slice.
It is Rust-internal and is not a public C symbol or an OCaml workflow API. A
caller supplies one strict JSON document per recorded history; Rust decodes
the canonical base64 `History` protobuf, validates it with Core's
`HistoryInfo`, and constructs `HistoryForReplay`. Temporal Core then owns the
replay state machine and produces the same workflow activations it would
produce while replaying server history.

The document shape is defined by
[`replay-history.schema.json`](../schemas/bridge/replay-history.schema.json)
and explained in the [replay bridge reference](replay-bridge.md). Runtime
validation is stricter than the schema: duplicate and unknown members,
non-canonical base64, oversized values, malformed protobuf, and histories that
violate Core event invariants are rejected before the feeder sees them.

`ReplayWorker` owns a Core workflow-only worker and a one-slot
`HistoryFeeder`. The one-slot bound preserves FIFO ordering and applies
backpressure instead of accumulating histories in an unbounded native queue.
Dropping the feeder closes input. A normal finalization is allowed only after
the caller has completed every activation and observed the workflow lane's
natural `Shutdown`; this avoids cancelling a queued history while reporting
success. If that precondition is not met, the typed error retains the worker
for another drain attempt. The explicit `dispose` path is destructive: it
initiates shutdown, force-completes unfinished work, joins the lane, and
attempts Core finalization twice. Each force-completed workflow run ID and
activity token is retained as a bounded retired tombstone until both poll lanes
have joined. A poll that was already in flight can therefore be discarded as a
duplicate instead of being admitted as a new completion obligation; the
tombstones are then cleared. If both terminal attempts fail, it returns the
still-owned worker with a typed `Finalization` error so the caller can retry
after releasing a competing owner; it never silently drops an unfinalized
native graph. The replay path owns no OCaml pointer or callback and starts no
activity poller. Its focused Rust tests are kept in
`rust/core-bridge/tests/support/replay_bridge.rs` so production and test code
remain separate.

This plumbing is unit-tested native evidence plus the implemented acceptance
controller. The public C/OCaml replay operation remains separate work. The
two-generation Docker Compose restart target now proves the exact run, replay
marker, terminal result, and fresh PostgreSQL-volume cleanup in the [PR #253
Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29286560471).

## Native client start and exact-run wait

The private Rust client adapter exposes strict JSON operations for synchronous
workflow starts, asynchronous start admission and bounded ticket
polling/waiting, exact-run waits, and exact-run cancellation. The operations
are deliberately lower-level than the public OCaml `Client` module. The
adapter uses Core's raw `WorkflowService` trait because workflow type names and
payloads are dynamic at the OCaml boundary; it does not instantiate Rust's
statically typed workflow definitions.

The start request contains the stable idempotency key `request_id`,
`namespace`, `workflow_id`, `workflow_type`, `task_queue`, and an ordered
`input` payload array. The request ID is copied into Core's
`StartWorkflowExecution` request and remains unchanged across bounded ticket
polls. Optional start policies are not silently invented in this first slice:
Core receives its documented server defaults. The successful response contains
the namespace, workflow ID, and run ID allocated by Temporal. Both request and
response are strictly decoded, re-encoded, and reparsed before crossing the
boundary.

Starts invoke `WorkflowService` on Core's `Connection`, which applies Core's
retry policy to transient transport failures. Calling the connection's
underlying `workflow_service()` stub bypasses that policy. Retries preserve
the entire request, including `request_id`, and share the existing ten-second
overall deadline. A definitive rejection remains terminal; an unanswered
request remains uncertain when the deadline expires. Callback-transport tests
under `tests/support/client_start.rs` cover recovery, request identity,
non-retryable rejection, cancellation of a hung request, and the 64-ticket
admission bound.

Every other client RPC (wait, signal, query, cancel, terminate, reset,
update, update poll, and visibility listing) goes through the same retrying
`Connection` (#820). Core decides which statuses are transient (`unavailable`,
`resource_exhausted`, `unknown`, `internal`, `aborted`, `out_of_range`,
`data_loss`, and transport-level cancellation) and re-sends the identical
request, so `request_id` or the update ID deduplicates a retried mutation.
Each bounded RPC replaces Core's ten-second retry window with its own budget
(three seconds for the signal, cancel, reset, and terminate control RPCs, ten
for visibility, thirty for query, update acceptance, and update polling) and
uses that budget as every attempt's gRPC deadline; the outer timeout still
caps an attempt in flight when the budget ends. The pinned Core accepts a
`resource_exhausted` retry using its ordinary backoff and then replaces the
wait with a separate throttle backoff (1 s, 2 s, 4 s, ... up to 10 s, each
+/-20%) without rechecking the retry window, so the control budget is three
seconds rather than one: one throttled re-send fits, while a throttle wait
still pending when the budget ends is cut off by the outer timeout and
reported as the RPC's deadline error. Terminate has no idempotency key, so
Core re-sends it only after `resource_exhausted`; `unavailable`, a
per-attempt `deadline_exceeded` or `cancelled`, and the outer deadline may
follow an applied termination and are reported as
`termination_outcome_uncertain` without a re-send. The `wait` history long poll has no
total budget: Core measures its retry window from the start of a call, which
would forbid retrying a long poll that failed after ten healthy seconds, so the
wait instead allows up to thirty consecutive attempts per long poll (roughly
two minutes of outage with Core's default backoff). Tests under
`tests/support/client_retry.rs` cover recovery of each RPC, identical
re-sends, non-retried rejections, the terminate restriction and its
uncertain `unavailable` result, a throttled signal re-send inside the control
budget, and that budget's bound on a persistent outage.

The wait request names `namespace`, `workflow_id`, and one concrete `run_id`.
There is no `follow_runs` escape hatch in the document: the operation always
uses a close-event history long poll for that exact run, but each native call
is bounded to 100 ms. If no close event arrives during that interval, the ABI
returns status `10` (`NOT_READY`) without a terminal response; the OCaml
caller (or a later orchestration loop) can resume through the supervisor
mailbox. The runtime retains the same Rust future, including its connection
and pagination state: 100 ms bounds owner occupancy, not RPC lifetime. Slow
successful requests therefore make progress across calls while the owner can
still admit shutdown and other lifecycle messages.

At most 64 distinct exact-run observations may be retained per runtime. Calls
with the same namespace, workflow ID, and run ID share an in-flight future;
admitting a new identity at capacity returns status `15`
(`RESOURCE_EXHAUSTED`). That status is reserved for a full bounded
client-operation registry: nothing was sent to Temporal and the client stays
connected, so it is distinct from the `INVALID_STATE` returned for a closed
client or runtime. The same status and bound of 64 apply to outstanding
asynchronous start tickets. The public adapter maps it to a retryable
`bridge` error with `error_type` `resource_exhausted`, which
`Client.is_at_capacity` recognizes. A terminal
result or error removes its entry before returning to OCaml. A later wait may
observe the same closed run again through a fresh request; this table is not
a result cache. Client disconnect and both explicit and finalizer runtime
close cancel all retained futures before releasing the connection and Core.
These futures are polled only by the owner, so cancellation drops their RPCs
directly without spawning or detaching tasks.

Completed, failed, and timed-out close events retain any successor run ID
exposed by Core. A continued-as-new close is returned as a terminal
`continued_as_new` outcome with a required successor execution reference; the
bridge never follows that run implicitly. A wait request with an empty run ID
observes whichever run is current when Temporal handles the history poll and
echoes the empty run ID in its response; only the public OCaml client decides,
for a handle obtained by workflow ID, to follow successors with further
exact-run waits. This prevents an exact-run caller
from accidentally observing a different execution identity. Any successor
must retain the waited namespace and workflow ID and must identify a different
run. Both language validators enforce that cross-object relationship because
Draft 2020-12 JSON Schema cannot express equality between those fields.

Temporal AlreadyStarted responses use status `12` and a closed JSON error body
(`kind`, `workflow_id`, `existing_run_id`) rather than copying gRPC server text.
Other RPC failures use a closed JSON body containing only a stable `kind` and
status/code value. Core payload and failure conversion errors use a `protocol`
error kind with a closed conversion code; the only values are
`core_unsupported` and `core_invalid`. RPC codes use the closed lowercase tonic
status vocabulary, and never include payload bytes or server diagnostics.
Machine-readable
schemas for these documents live under
`docs/schemas/bridge/client-*.schema.json`.

Worker and poll-lane failures use the same closed-category rule. Rust may keep
the original Core error inside its private state machine long enough to decide
which transition failed, but the ABI maps it to a constant message before
allocating the C result. OCaml repeats that mapping for worker statuses before
an error can reach the public worker API or its logs. Consequently Core/gRPC
status text, endpoint details, task identifiers, and payload data cannot cross
the Rust/C/OCaml boundary. This is intentionally a discard, not a redacted
copy: the current logging policy does not expose those private diagnostics.
Lifecycle configuration parser failures follow the same fail-closed rule. Rust
returns only `invalid lifecycle configuration JSON`; Serde's syntax, location,
and unknown-field details are kept inside Rust so application-controlled input
cannot become a diagnostic at the C/OCaml boundary.

Client connection failures are the documented exception (#833). Because a
constant message made DNS, refused, TLS, and timeout failures
indistinguishable, `STATUS_CONNECTION` from client connect carries a closed
cause category plus the bounded (512-byte), control-character-escaped local
transport error chain. A failed `GetSystemInfo` contributes only its gRPC code
name, never the server's status message. Core's own rejection of connection
options maps to `STATUS_CONFIGURATION` with a constant message, because that
text may echo configured headers. `rust/core-bridge/src/diagnostics.rs` owns
this reduction; the message format is described in
[observability](observability.md).

Runtime creation configures Core telemetry with a push logger at the level
selected by `OCAML_TEMPORAL_CORE_LOG` (default `warn`, `off` disables it).
Core invokes the consumer synchronously on whichever thread emitted the
record, including Tokio workers driving network progress and the supervisor
thread inside a blocking bridge call, so the consumer performs no I/O. It
formats one bounded line and offers it to a bounded queue
(`CORE_LOG_QUEUE_CAPACITY`, 1,024 lines) with a non-blocking `try_send`. When
the queue is full the line is dropped and counted, never waited for. The
consumer holds no OCaml value, never calls back into OCaml, and contains
formatting panics.

Each runtime with logging enabled owns exactly one Core log writer thread
(`diagnostics::CoreLogWriter`), spawned before Core and stored in the runtime
handle. The writer drains the queue to stderr, ignores write errors, contains
sink panics, and never calls OCaml. Before the next line, after one idle
second, and at shutdown it writes one `N Core log records dropped` summary
for any drops. Its single release path runs on the runtime cleanup thread
after Core has been dropped, so Core's shutdown records are still flushed.
That path disconnects the queue, waits up to `CORE_LOG_CLOSE_TIMEOUT`
(500 ms) for the writer to drain and exit, and then joins it. A writer still
blocked in a stderr write after that bound means the stderr reader has
stalled. Waiting longer could hang runtime close indefinitely, so the thread
is detached instead. A detached writer owns only the queue receiver, the drop
counter, and stderr: no Core, Tokio, or OCaml state. It exits once its write
returns, or ends with the process. A stalled stderr therefore delays runtime
close by at most the bound. If runtime construction fails after the writer
was spawned, dropping it runs the same bounded close.

Core also installs the subscriber as the thread-local default on the thread
that creates the runtime (the owning supervisor thread). That guard is removed
only if the runtime is dropped on the same thread; otherwise it stays with
that thread and keeps one reference to the consumer. Because the writer's
close takes the queue sender explicitly, rather than waiting for Core to drop
its consumer, records reaching such a stale subscriber are discarded without
blocking.

All client-operation identifiers are nonempty and NUL-free. The schemas state the
65,536-character necessary bound, while the bilateral runtime validators apply
the authoritative 65,536-byte UTF-8 limit, reject duplicate members, and
check encoded output against the receiver's limits. JSON Schema counts Unicode characters rather than
encoded bytes, so schema validation alone is not a substitute for the runtime
checks.

## Result and buffer ownership

Every fallible operation that returns a result document accepts a writable
result pointer and returns the same status stored in that result. Runtime
close/dispose and result disposal have no result document and return their
status directly. Status zero is success. Nonzero statuses cover invalid
arguments, ABI mismatch, a contained Rust panic, internal bridge failure,
invalid lifecycle state, configuration, connection, worker, outstanding-task,
not-ready, protocol, already-started, explicit retryable-completion, and
async-heartbeat-rejected failures. The last status is a definitive rejection of
one nonterminal heartbeat request; it does not say the activity token is gone.
Worker polling and exact-run
client waits use the expected `NOT_READY` status. For a worker lane it means no
task is queued; for a client wait it means the 100 ms owner interval elapsed
without a terminal result. In both cases the caller or a later orchestration
loop can resume through the supervisor mailbox.
For live worker shutdown, `OUTSTANDING_TASKS` means a task leased to the
language side was never completed: the bridge force-completed it, finalized
the worker, and released it, and reports the abandoned work so the caller can
surface it. For replay it means recorded input was not fully drained.

A result has one success buffer and one error buffer. At most one owns memory:

- success may place arbitrary binary bytes in `value`;
- failure may place a UTF-8 diagnostic in `error`; worker and poll-lane
  failures use bounded constant categories, while client operation failures
  use the closed JSON documents described above;
- an empty allocation is always represented as `{ NULL, 0 }`.

Rust owns both allocations. The caller may copy their bytes but must never
mutate or directly free their fields. It must call
`ocaml_temporal_core_v4_result_free` exactly once after consuming an initialized
result. That function clears the object, so accidentally calling it again on
the same object is safe. Copying a live result structure creates no new
ownership; freeing both copies is invalid.

An output object may be uninitialized, but it must not contain a live owned
result when passed to another operation. Free the previous result first.

### OCaml ownership guard

The private C stubs allocate an OCaml custom block before entering Rust. That
block is the sole owner of the ABI result and has a finalizer which calls
`ocaml_temporal_core_v4_result_free`. The OCaml wrapper also uses
`Fun.protect` to release the result deterministically after copying its bytes.
This gives every path two compatible safeguards: normal operation frees
immediately, while an OCaml allocation failure or other exception leaves a
rooted/finalizable owner rather than orphaning Rust memory. Disposal is
idempotent, so a later finalizer after deterministic disposal is harmless.

Returned bytes are copied once, directly from the live Rust buffer into the
OCaml string/bytes allocation. The C binding first reads the buffer length
under a counted borrow, releases that borrow before allocating OCaml storage,
then borrows the result again for the allocation-free byte copy. Explicit free
closes the borrow gate and waits for admitted copies before calling Rust's
`result_free`. If another Domain frees the result between allocation and the
second borrow, the copy raises a closed-owner error without dereferencing
released bytes. No pointer into the movable OCaml custom block is retained
across the OCaml allocation. For the canonical empty `{ NULL, 0 }` case, the
C binding allocates an empty OCaml value without passing the null pointer to a
copy operation. A nonempty null span is rejected before dereference as an ABI
defect. Inputs that must survive a blocking call are copied to temporary C
storage before the runtime lock is released, then freed immediately after the
lock is reacquired. Neither side directly frees an allocation made by the
other side.

### Bounded replay ownership audit (#523)

This table traces one replay activation from a client-free runtime through
handoff, completion, and abandonment. The references name the owning path and
the test that observes it; they do not qualify every Core or FFI lifecycle.

| Stage | Owner and cleanup contract | Instrumentation |
| --- | --- | --- |
| Start and feed | `Runtime::replay_worker` owns the Core worker and one-slot feeder; `Runtime::dispose_replay` or runtime close releases the graph. C's `owned_runtime.active_calls` keeps the runtime alive during a released-lock call. | `rust/core-bridge/src/abi.rs` (`start_replay_worker`, `dispose_replay`, `drop_runtime_graph`), `lib/core_bridge/native_stubs.c` (`acquire_runtime`, `release_runtime`), `rust/core-bridge/tests/replay_abi.rs` (`new_replay_runtime`). |
| Poll and handoff | The Rust workflow ledger retains the activation lease. The ABI result owns its encoded bytes until C's `owned_response` copies and frees them; the copy/free gate permits either a complete copy or a closed-owner error when Domains race. In the OCaml binding, explicit free and finalization converge on `release_response`, which alone calls Rust's `result_free`; raw ABI callers call `result_free` directly. | `rust/core-bridge/src/abi.rs` (`try_poll_replay_workflow`, `invoke`, `result_free`), `lib/core_bridge/native_stubs.c` (`copy_response_buffer`, `release_response`), `lib/core_bridge/response_borrow_gate.h` (shared gate), `test/bridge/test_response_borrow_gate.ml` (admitted-read interleaving), `test/bridge/test_ocaml_response_buffers.ml` (success/error Domain races). |
| Malformed activation handoff | An OCaml decode failure sends the original bytes to native rejection. A mismatched or malformed rejection does not consume the lease; a semantically matching rejection reports the failure to Core and retires the lease. Each ABI response owns its diagnostic until the OCaml binding copies and frees it. | `rust/core-bridge/src/abi.rs` (`reject_replay_workflow_delivery`), `rust/core-bridge/tests/replay_abi.rs` (`replay_abi_rejects_only_semantically_matching_lease`), `test/sdk_supervisor/test_native_worker_operations.ml` (`test_decode_failure_retires_native_lease`). |
| Malformed completion or panic | Strict decoding rejects malformed completion before consuming the lease. `invoke` catches a Rust panic in its operation closure and returns an owned `STATUS_PANIC` error; the synthetic probe does not inject a panic into Core or after replay-worker ownership is taken. | `rust/core-bridge/src/abi.rs` (`complete_replay_workflow`, `invoke`, `test_invoke_panic`), `rust/core-bridge/tests/replay_abi.rs` (`replay_abi_retains_lease_after_malformed_completion`), `rust/core-bridge/tests/abi.rs` (`contains_rust_panics_as_owned_errors`). |
| Accepted completion and natural finalization | Core acceptance retires the semantic lease; a duplicate completion fails. Natural finalization requires closed input, observed shutdown, and no outstanding native debt, then clears the runtime's replay worker. | `rust/core-bridge/src/abi.rs` (`complete_replay_workflow`, `finalize_replay`), `rust/core-bridge/src/replay_bridge.rs` (`finalize`), `rust/core-bridge/tests/replay_abi.rs` (`replay_abi_retains_lease_after_malformed_completion`). |
| Abandonment | Explicit replay disposal acknowledges leased work with Core's replay-safe empty completion. Runtime disposal transfers the graph to its cleanup thread; the process-local created/cleaned counters observe eventual destructor completion with a leased activation. | `rust/core-bridge/src/replay_bridge.rs` (`dispose`), `rust/core-bridge/src/abi.rs` (`drop_runtime_graph`), `rust/core-bridge/tests/replay_abi.rs` (`replay_abi_disposes_a_leased_activation_without_core_failure`), `rust/core-bridge/tests/runtime_cleanup.rs`. |

The panic probe proves containment and result-buffer release at the common ABI
wrapper. It does not prove recovery from an arbitrary panic inside Temporal
Core or after `finalize_replay`/`dispose_replay` takes the worker. Those cases
remain outside this finite audit and must not be treated as production
qualification of the full FFI.

### Private OCaml worker operations

`Temporal_core_bridge.Native_bridge` exposes nine private wrappers over the
poll, readiness, completion, and rejection symbols. They are used by the
native worker adapter and are not part of the public workflow-authoring API:

| OCaml operation | Native behavior | Successful value |
| --- | --- | --- |
| `worker_try_poll_workflow` | Drain one already-ready workflow activation without waiting | semantic workflow JSON bytes |
| `worker_wait_workflow` | Wait for workflow readiness without consuming a task | `unit` wake signal |
| `worker_complete_workflow_json` | Validate and complete one leased workflow activation | `unit` |
| `worker_reject_workflow_json` | Retire the lease when OCaml cannot decode the exact Rust-produced activation document | `unit` |
| `worker_try_poll_activity` | Drain one already-ready remote or local activity task without waiting | semantic activity JSON bytes |
| `worker_wait_activity` | Wait for activity readiness without consuming a task | `unit` wake signal |
| `worker_wait_any` | Wait for readiness on either lane without consuming a task | `unit` wake signal |
| `worker_record_activity_heartbeat_json` | Validate and record progress for an outstanding activity lease without completing it; Core reports cancellation, pause, and reset asynchronously in a later `Cancel` task | `unit` acknowledgement |
| `worker_complete_activity_json` | Validate and complete one leased activity task | `unit` |
| `worker_reject_activity_json` | Retire the lease when OCaml cannot decode the exact Rust-produced activity document | `unit` |

The two poll functions return `Error { status = Not_ready; _ }` when their
independent Rust ready queues are empty. This is normal scheduling state, not a
worker defect. A readiness wait returns immediately when its queue is already
populated, wakes when its lane publishes a task or fatal error, and returns an
invalid-state error after normal shutdown has drained queued messages. A quiet
lane returns `Not_ready` after a bounded 100 ms wait. That bound is intentional:
the supervisor mailbox must regain control periodically so a queued shutdown
operation cannot be stranded behind a blocking readiness handler. Completion
functions copy the
caller-provided OCaml `bytes` into temporary C storage, release the OCaml
runtime lock for the synchronous Rust submission, then free that copy before
returning. Rust validates the complete JSON document and checks the run ID or
opaque activity token against its ownership ledger, so a duplicate or stale
completion cannot silently reach Core.

If OCaml rejects poll bytes, the supervisor returns that same byte document to
the corresponding rejection operation. Rust bounds and strictly decodes it
again; callers never supply a guessed run ID or task token. A workflow document
must equal the complete semantic activation retained at handoff. An activity
document must equal one complete semantic task retained under its canonical
opaque token; cancellation updates using the same token are retained alongside
the earlier task rather than overwriting it. A rejected Start owns the one
ledger obligation for that token, so its rejection reports a bridge failure to
Core and clears every retained document for the token. A rejected Cancel is
only a malformed or unsupported update; Rust drops that one retained document
without touching the Start's native lease, allowing the original activity
owner to complete normally. Changed identity or content is a protocol failure
and cannot retire the real lease. The malformed-byte case is defensive:
successful Rust poll encoding cannot produce malformed JSON, but both language
decoders and both rejection entry points still validate it.

`Sdk_supervisor.Native` is the private OCaml adapter for these ABI version 4
operations. It exposes a typed GADT rather than raw JSON bytes:

| Supervisor operation | Result and boundary behavior |
| --- | --- |
| `Try_poll_workflow` | `Workflow_protocol.activation option`; `None` means the workflow lane was empty at that instant |
| `Wait_workflow` | bounded native readiness wait; it does not consume an activation and releases the OCaml runtime lock |
| `Complete_workflow encoded` | an `Encoded_workflow_completion.t` holding the canonical JSON from the worker adapter's single encoder pass; the supervisor copies those bytes into the native completion call without encoding or reparsing them, and Rust strictly decodes them |
| `Try_poll_activity` | `Activity_protocol.task option`; `None` means the activity lane was empty at that instant |
| `Wait_activity` | bounded native readiness wait; it does not consume an activity task and releases the OCaml runtime lock |
| `Wait_any` | bounded native readiness wait that any queued workflow activation or activity task ends; it consumes nothing and releases the OCaml runtime lock |
| `Record_activity_heartbeat heartbeat` | canonical strict heartbeat JSON is validated and recorded for the outstanding activity lease without retiring it; the acknowledgement carries no cancellation flags, which arrive later on the activity poll lane |
| `Complete_activity completion` | the opaque token and result are validated before the native completion call |

All eight operations enter the same bounded mailbox as client and worker
lifecycle changes. A poll, completion, worker shutdown, and runtime shutdown
therefore cannot race native graph state. The pure protocol conversion module
is visible only from the private supervisor library so both serialization
directions can be tested without constructing a Core worker.

The two Rust readiness signals use one mutex-protected pending count per lane.
Each signal also notifies a worker-wide combined wake after releasing its lane
mutex; `worker_wait_any` holds that wake's mutex while it checks both lane
predicates, so a task published on either lane after the check still wakes it
(#806). Queued work on either lane wins over a fatal error, which wins over
closure.
The poll task holds that mutex while it sends a message and increments the
count; the supervisor holds it while receiving and decrementing. This makes a
send and its wake notification one linearizable operation and prevents a
notification-before-wait or send/receive reordering race. Shutdown closes both
signals before asking Core to wake its polls, while in-flight poll results may
still be queued and are always drained before the terminal state is reported.

These wrappers deliberately return the same owned-response shape as lifecycle
operations. The OCaml `decode` helper copies success or diagnostic bytes and
always calls `response_free` under `Fun.protect`; the C custom-block finalizer
therefore remains a fallback for allocation failures or exceptions. No Rust
poll lane calls an OCaml closure, and no bridge result retains an OCaml heap
pointer after the C call returns.

## Pointer and panic contract

Null output/result pointers return `INVALID_ARGUMENT` without being
dereferenced. A null input pointer is valid only when its length is zero.
As with any C byte-span API, a non-null input pointer must identify a readable
allocation of the stated length and must not overlap the output object.

Every fallible exported operation contains Rust panics before they can unwind
through C. A contained panic becomes `STATUS_PANIC` and an owned diagnostic.
The Rust integration suite invokes the common wrapper with a deliberate panic;
the panic test hook is not exported in the C header and is not part of the
stable ABI.

## Stateful handle ownership

The reserved runtime, client, and worker types are opaque references to
Rust-owned SDK state, not OS handles and not public OCaml values. Only the
runtime pointer (and, for an application-shared Core, the shared-runtime
pointer described under [Shared runtime](#shared-runtime-832)) crosses the
current C ABI; client and worker state are fields within that Rust runtime:

- a runtime owns Tokio and shared Core infrastructure;
- a client owns one cluster connection and its authentication/configuration;
- a worker owns polling and completion state for a task queue configuration.

A normal process is expected to have one runtime, usually one client, and one
or a small number of workers. The intended OCaml design is therefore one
supervisor actor per SDK instance, not one actor per handle. A dedicated OCaml
Domain owns the entire runtime/client/worker graph. Calls from other Domains
enter a synchronized MPSC mailbox and receive typed one-shot `result` replies.
The supervisor serializes lifecycle transitions and destroys workers before
clients and the runtime. Rust retains internal Tokio concurrency; workflow
executions retain their separate deterministic effect schedulers.

### Runtime thread budget (#832)

Each runtime builds its own multi-thread Tokio executor. Tokio's default of
one worker per core made every client and worker cost dozens of idle threads
on a large host, so runtime creation takes an explicit worker count through
`ocaml_temporal_core_v4_runtime_new_with_worker_threads`. `0` selects the
bridge default, `min(available parallelism, DEFAULT_RUNTIME_WORKER_THREADS_CAP)`
(4), falling back to one thread when parallelism cannot be queried.
`1..=OCAML_TEMPORAL_CORE_MAX_RUNTIME_WORKER_THREADS` (256) is used unchanged,
and a larger value returns `STATUS_INVALID_ARGUMENT` before anything is
allocated, leaving the runtime slot null. `ocaml_temporal_core_v4_runtime_new`
is the same call with `0`. The count is resolved before Core is built, so it
adds no owner or release path: the Tokio pool remains owned by Core inside the
runtime handle and is shut down by the existing runtime destruction path.

OCaml exposes the bound as the implementation-neutral `?io_threads` on
`Client.create` and `Worker.create`: the public documentation promises only an
upper bound on network and server-communication threads, so the Tokio mapping
stays private and could change without a public API migration. The SDK
validates the same range as a typed defect before any supervisor or native
allocation, and passes it through
`Sdk_supervisor.Native.create` to `Native_bridge.runtime_create`. The C stub
maps a negative or oversized OCaml integer to `UINT32_MAX` so Rust rejects it
rather than truncating. Each instance still owns a separate runtime, its
cleanup thread, and its supervisor Domain unless it is attached to a shared
runtime (below).

### Shared runtime (#832)

Several SDK instances can share one Core runtime, and so one Tokio pool,
through an explicit, application-owned value. The native graph is split at
one seam: Core moves out of the per-instance graph into a reference-counted
`SharedCore` (Core plus its optional log writer), and every holder owns one
`Arc` reference to it.

- `ocaml_temporal_core_v4_shared_runtime_new(worker_threads, &shared, out)`
  builds one `SharedCore` behind an opaque `SharedRuntime` handle that owns
  one reference and its own cleanup thread. It carries no client or worker.
- `ocaml_temporal_core_v4_runtime_new_attached(shared, &runtime, out)`
  creates an ordinary instance graph (one client, one worker, its own cleanup
  thread, released by `runtime_free`/`runtime_dispose`) that holds a cloned
  reference instead of building its own Core. A null `shared` is
  `INVALID_ARGUMENT`; concurrent attaches on one live handle are permitted.
- `ocaml_temporal_core_v4_shared_runtime_free` releases the handle's
  reference and waits; `..._shared_runtime_dispose` is the non-waiting GC
  fallback. Both clear the slot first and are idempotent on a null slot.

Ownership rules. Each graph still has exactly one owner (its supervisor's
owner Domain) and one release path; supervisors never share mutable native
state, so "one supervisor actor per SDK instance owns its graph" still holds,
with Core moved out of the graph. Core is destroyed exactly once, by
whichever holder drops the last reference, in any order: releasing the
shared handle first never frees Core under a live graph, it only defers
destruction to that graph's `runtime_free`. Every holder drops its reference
on a plain OS thread (a cleanup thread, or an OCaml thread with the runtime
lock released), never on a Tokio worker, where dropping a Tokio runtime
panics. Field order in `SharedCore` drops Core before closing the log writer,
so Core's shutdown records are still flushed. A Core worker finalizer that
was detached after a bounded timeout ends when the last reference is dropped,
which for a shared runtime is the shared runtime's release rather than the
graph's.

The OCaml side adds a stricter, typed ordering rule on top of that memory
safety. `Sdk_shared_runtime` (private) owns the `Native_bridge.shared_runtime`
custom block, whose C owner has the same counted-borrow gate and malloc'd
layout as the per-instance runtime owner. Each attachment is a lease acquired
before any graph exists; `Sdk_supervisor.Native.create ?runtime:lease` takes
ownership of the lease, releases it if creation fails, and otherwise releases
it in backend shutdown only after `runtime_close` has returned. `shutdown`
refuses with `Still_attached n` while any lease is outstanding and otherwise
holds its mutex across the native free, so every `Ok` return happens after
Core and its threads are gone. The public `Temporal.Runtime` maps that refusal
to a `Defect`. If an instance is abandoned without shutdown its lease stays
held, so `Runtime.shutdown` keeps refusing; the Rust references still
guarantee Core is never freed early.

Tests: `rust/core-bridge/tests/shared_runtime.rs` proves two attached graphs
share one Core and Tokio pool and run independent replay workers, that
releasing the shared handle first leaves graphs usable, and the invalid
argument paths; `shared_runtime_cleanup.rs` (its own process) proves Core is
destroyed exactly once by the last holder in both orders and through the GC
fallback; the C harness covers the symbols under ASan/UBSan; and
`test/bridge/test_shared_runtime.ml` covers the bridge, the lease ledger, two
real supervisors on one runtime, the public ordering errors with `mock://`
and refused `http://` targets, and a Linux thread-count leak check.

The implemented private supervisor owns the real runtime, one official client
connection, and one Core worker for workflows and remote activities. Its backend protocol exposes
typed GADT operations but never the owner-confined state, preventing a raw
handle from escaping through an otherwise convenient callback. See
[ADR 0004](../decisions/0004-sdk-instance-supervisor.md) for its lifecycle,
failure, and scheduler contracts.

### Lifecycle configuration JSON

The OCaml wrapper constructs two private JSON documents; applications never
assemble these strings themselves. The client document contains exactly
`target_url` and `identity`. The worker document contains exactly `namespace`,
`task_queue`, `build_id`, `versioning`, `max_cached_workflows`,
`max_outstanding_workflow_tasks`, `max_concurrent_workflow_task_polls`, and
`graceful_shutdown_timeout_ms`, plus `task_types`. Closed Draft 2020-12
schemas live under `docs/schemas/bridge/`.

`task_types` is a closed `{ "workflows": bool, "activities": bool }` object
that the OCaml worker derives from its registrations: a kind with no
registered implementation is `false` (#805). Rust maps it onto Core's
`WorkerTaskTypes` in `worker_bridge::bridge_task_types`: `workflows` enables
workflow polling and in-process local activities, and `activities` enables
remote activity polling; Nexus is always off. A worker that registers no
activities therefore never takes an activity task from a shared task queue
that a sibling activity worker could have executed, and an activity-only
worker never takes workflow tasks. A document with both `false` is rejected
with `STATUS_CONFIGURATION`. The member may be omitted for compatibility, in
which case both kinds are polled; the OCaml encoder always sends it.
`PollLanes::start` derives its lanes from the configuration Core actually
received: no workflow poll lane runs without workflows (Core's workflow poll
would otherwise report `ShutDown` and shut the whole worker down), and the
activity lane runs whenever local or remote activities are enabled. A lane
that is not started stays open and idle — polls report no work and readiness
waits time out — until shutdown closes it, so the OCaml run loop never sees a
spurious lane shutdown.

`versioning` is a closed object: `{ "kind": "none" }` preserves the existing
unversioned worker behavior, while `{ "kind": "legacy_build_id", "build_id":
"..." }` selects Temporal Core's legacy whole-worker build-ID routing. In the
legacy form the nested build ID must exactly match the top-level `build_id`;
both OCaml and Rust validate that invariant before worker construction.
The modern deployment form is:

```json
{
  "kind": "deployment_based",
  "deployment_name": "agents",
  "build_id": "agent-worker-2026-07-16",
  "use_worker_versioning": true,
  "default_versioning_behavior": "pinned"
}
```

Its nested `build_id` is subject to the same equality check. The deployment
name identifies the Temporal deployment, while `use_worker_versioning` and
`default_versioning_behavior` (`"auto_upgrade"` or `"pinned"`) are passed
to Core's `WorkerDeploymentBased` strategy. A behavior is rejected when worker
versioning is disabled and required when it is enabled, because completions
never carry a per-workflow behavior and Core substitutes only the configured
worker default for `UNSPECIFIED`. Registration, rollout, and compatibility
set management remain server-side responsibilities.

Temporal Core requires at least two workflow-task pollers when
`max_cached_workflows` is non-zero. The OCaml validator, the Rust validator,
and the JSON Schema all enforce that relationship before worker construction;
the public native worker default is two pollers.

For the pinned Core revision `95e9768`, cache-enabled workers also cap effective
workflow-task permits at `max(max_cached_workflows, 2)`. Core's poller balancer
reads the slot supplier's capacity rather than that independent cap. The bridge
therefore passes the smaller of the requested task limit and this cache-derived
capacity to Core. Otherwise, a one-entry cache with the default 1,000-task limit
advertises 1,000 slots while allowing only two permits, so sticky polls can
reserve capacity needed by the normal queue. This adjustment preserves the
capacity Core already enforces, caller limits below the cap, and uncached
workers. Reassess it when updating Core's permit dealer or poller balancer.

The regression in `rust/core-bridge/tests/support/worker_slot_limits.rs` checks
the one-entry/default-limit combination, larger cache bounds, stricter caller
limits, uncached workers, and rejection of a one-task cached worker. Live
qualification uses `make test-temporal-worker-cache-eviction`, which must
observe A's `cache_full` eviction after starting B and typed cancellation of
both runs. Increasing the watchdog or accepting a successful retry is not a
substitute for those observations. CI artifact retention and release
qualification remain tracked by issues #490 and #501.

The public `Temporal.Worker.create` accepts validated `Temporal.Worker.Options`
for routing and resource policy. `Options.make ~versioning:(Legacy_build_id
"build-v2") ()` enables legacy build-ID routing. It also accepts an optional
`max_cached_workflows` bound for applications that need to tune sticky
workflow memory. Omitting it retains the bounded default of 1,000 cached
workflows. A zero value disables the Core cache, while a positive value can
produce explicit `RemoveFromCache` activations when the bound is reached; the
worker acknowledges those activations with an empty completion.

Both sides reject missing, unknown, wrongly typed, empty required, and
out-of-range values. The whole document and each individual string have a
65,536-byte private transport-safety ceiling. That ceiling is not a Temporal
identifier policy: Core and Server retain semantic authority. JSON Schema
measures characters rather than encoded bytes, so the bilateral runtime
validators enforce the byte ceiling.

Client connection and worker namespace validation are synchronous ABI calls.
The C stub copies input before releasing the OCaml runtime lock, Rust performs
the Tokio wait, and the stub reacquires the lock only to copy the result. A
failed connection publishes no client. A failed worker construction or
validation releases the temporary worker and leaves the client available for a
corrected retry.

The validation-failure path (for example, a namespace the server reports as
missing) is the sole owner of the unpublished Core worker. Core's
`finalize_shutdown` completes only after both `poll_workflow_activation` and
`poll_activity_task` have returned `ShutDown`, and no poll lane exists yet, so
awaiting the finalizer directly hangs forever (issue #770). Rust therefore
initiates shutdown, drives both poll APIs to `ShutDown` itself (force-failing
any task Core unexpectedly hands out), and then finalizes, all in one task
on the Core runtime that owns the worker until `finalize_shutdown` finishes.
The worker is never dropped mid-release, because only `finalize_shutdown`
performs Core's `finalize_unregister`; a dropped worker would stay in the
client's worker registry and keep the client alive. `Worker.create` waits at
most `UNVALIDATED_WORKER_RELEASE_TIMEOUT` (10 s) for that task, so it always
returns the typed `Temporal workflow worker validation failed` error instead
of wedging the supervisor Domain; after the bound the task completes the
release in the background. `rust/core-bridge/tests/worker_validation_cleanup.rs`
covers this path with a plaintext HTTP/2 gRPC double rather than a server.

The worker owns two Tokio poll lanes: exactly one calls Core's workflow poll and
exactly one calls its activity poll. The activity lane admits both remote and
local activity tokens; Core marks local tokens and records their results as
workflow markers rather than sending them to the service. Nexus remains
disabled. Each lane writes without waiting to its own ready queue. Core's
configured outstanding-task permits bound the number of queued tasks; using a
second bounded send would deadlock shutdown if the supervisor joined a lane
while that lane waited for the supervisor to drain its full queue. The OCaml
supervisor takes ready work through non-blocking ABI operations and uses the
bounded readiness waits only from its owner-domain mailbox handler; the C stubs
release the OCaml runtime lock while Rust waits. No Tokio thread enters OCaml
and no long Core poll occupies the supervisor Domain. Keeping the lanes
independent prevents an idle activity poll from delaying workflow completion,
or vice versa.

ABI version 4 includes private readiness-wait symbols for the two independent
poll lanes and one combined wait over both. The supervisor may invoke them
only from the owner-domain mailbox handler; the C boundary releases the OCaml
runtime lock while Rust waits and reacquires it before returning. Callers must
not turn a readiness wait into a blocking condition wait on a workflow scheduler
fiber or allow a second owner to access the native worker graph.

One mutex-protected ledger is the authority for every task Core expects the
language runtime to complete. A task enters the ledger before its ready message
is queued, changes from Rust-owned ready state to OCaml-leased state at the
non-blocking handoff, and leaves only after Core accepts the exact matching run
ID or opaque activity token. Activity cancellation reuses the original token
and therefore does not create a second completion debt.

There is one deliberate pre-handoff exception. If a leased Core value cannot
be converted to the closed semantic JSON protocol, OCaml never receives its
run ID or task token and therefore cannot complete it. Rust generates exactly
one workflow-task or activity failure for Core and retires the inaccessible
lease on every outcome. For an activity cancellation, however, the task is an
update to a previously leased Start and does not own another completion debt;
an unrepresentable cancellation is dropped without completing the shared
token. The generated activity failure is a non-retryable application failure
of type `UnrepresentableActivityTask` with a static conversion category as its
message: representability is deterministic, so a retryable failure would only
make the server redeliver the task to be rejected again. Once Core accepts the
generated completion, the poll returns `NOT_READY` and the worker keeps
polling, exactly as for a rejected workflow activation; one task such as a
standalone activity or a header key another SDK allowed therefore cannot end
`Worker.run` for the whole task queue (issue #801). A rejected generated
completion remains a fatal worker error, but it cannot also leave a fabricated
language-side debt that blocks shutdown forever. Regression tests cover this
rule independently for workflow and activity conversion failures, including
the cancellation classification, and
`rust/core-bridge/tests/activity_task_rejection.rs` drives the activity case
through the ABI against a gRPC double.

The activity poll lane applies the same reasoning to cancellations it cannot
attach to a live Start. An unknown cancellation (its Start completed between
Core's poll returning and admission), a repeated or retired one, or one
polled while draining owns no completion debt, so the lane drops it without a
completion and without publishing a lane error.

There is also a post-handoff decode-failure path for version or implementation
drift between the two strict decoders. OCaml keeps its original protocol
error, returns the exact Rust-produced bytes to the private rejection ABI, and
never reflects those bytes in diagnostics. Rust accepts rejection only after
full semantic equality with retained handoff state. For a Start, it then
generates the Core failure and retires both the ledger debt and retained
semantic state even if Core reports that generated failure as unsuccessful.
For a Cancel update, it retires only that retained semantic state: the shared
Start debt remains owned by the activity implementation. This prevents
shutdown from waiting forever without turning a malformed cancellation into a
spurious `UnknownActivity` completion failure. When the rejection succeeds on
a live worker, the task is handled: Rust writes the static diagnostic (and,
for a workflow activation, applies the 100 ms redelivery backoff) and the
OCaml supervisor reports an empty poll so `Worker.run` continues (issue #801).
A replay instead returns the protocol error, because it must not report
history it never checked as compatible. If the rejection itself fails, the
original decode failure stays primary with the rejection category appended.

Shutdown first closes ledger admission and both readiness signals, then asks
Core to wake both polls. From that point the supervisor Domain is blocked in
the shutdown call and OCaml has already stopped its run loop and drained its
retained completions, so no language completion can arrive. Core, however,
returns `ShutDown` from a poll only after every task it produced has been
completed, so joining the lanes without completing those tasks hung forever
(issue #769). `PollLanes::drain_and_join_for_shutdown` therefore completes each
outstanding debt exactly once while it joins both lanes: a lease taken by
OCaml is removed from the ledger and failed (or acknowledged empty if it was a
pure cache eviction, which owns no workflow task); a queued handoff is removed
and completed from its own queue message; activity cancellations and lane
diagnostics own no debt and are dropped. It keeps draining both queues while
the lanes run, because polls in flight and Core's follow-up evictions publish
new tasks until each lane sees `ShutDown`. Each identity leaves the ledger
before its Core completion is awaited, and no tombstone is written, so a
same-run eviction that answers a failed workflow task is admitted and
acknowledged. The drain is bounded by `WORKER_SHUTDOWN_DRAIN_TIMEOUT` (90 s,
above one server long poll); on timeout the unjoined lanes stay owned by the
graph, the ledger is marked as having lost a lease, and the call returns a
worker failure so runtime close disposes the worker. Core finalization then
runs in a Tokio task that owns the worker until `finalize_shutdown` returns,
and the caller waits at most `WORKER_FINALIZE_TIMEOUT` (30 s); after that the
task finishes the release in the background, so a worker is never dropped
between the lane join and Core's `finalize_unregister`. Tasks that never
reached OCaml are retired silently and shutdown returns `OK`; if a leased task
had to be force-completed, the worker is still released but shutdown returns
`OUTSTANDING_TASKS`. The garbage-collection fallback cannot obtain missing
language completions. On the dedicated cleanup thread it force-fails
outstanding Core tasks, joins the poll lanes with the same bounded drain, and
attempts the same bounded finalization. Its force-completion acknowledges a
pure cache eviction (leased or queued) empty and fails every other activation,
and it tombstones each completed run ID until the lanes join. The workflow
lane records the eviction bit in the same ledger critical section that admits
the run, so dispose classifies an entry correctly even when the lane has
admitted it but not yet enqueued its message. Core answers
each such failure with a same-run eviction; the workflow poll lane
acknowledges a retired run's eviction empty instead of dropping it as a
duplicate, because Core keeps at most one activation outstanding per run and
reports `ShutDown` only after that eviction is completed. Dropping it made
runtime close wait out the whole drain bound and release an unfinalized
worker (issue #775);
it drops an undrained worker only if finalization still fails. This preserves
memory ownership and collector progress, while explicit supervisor shutdown
remains the required graceful path.

### Runtime destruction

Creating an SDK runtime starts its Tokio executor and a small Rust cleanup
thread dedicated to that owner. Explicit OCaml shutdown atomically detaches the
opaque pointer, releases the OCaml runtime lock, transfers Core to the cleanup
thread, and waits until Core's destructor has returned. This makes orderly
shutdown observable without preventing other OCaml Domains from running.

The OCaml custom-block finalizer is a fallback for abandoned runtime values. It
atomically detaches the pointer, waits only on the C-side borrow counter, and
then transfers Core to Rust's cleanup thread. It never enters or leaves an
OCaml blocking section: custom-block finalizers must not call OCaml runtime
operations. Every admitted C primitive keeps the runtime value rooted and
releases its borrow before reacquiring the OCaml lock, so this defensive wait
cannot deadlock behind a caller that is returning from Rust. The normal path
therefore destroys Core on the dedicated cleanup thread; if that thread has
already failed, Rust uses its defensive synchronous fallback to reclaim the
graph on the caller thread rather than leak it. In either case the finalizer
itself never invokes OCaml runtime operations. Both paths clear the handle
before transfer and are idempotent; exactly one path can own the native graph.
Cleanup finalizes worker, drops client, then drops the graph's Core reference
even when callers did not explicitly close the children; Core itself is
destroyed there unless a shared runtime or another attached graph still holds
it. A shared-runtime handle has the same two paths (`shared_runtime_free` and
the finalizer's `shared_runtime_dispose`) and its own cleanup thread.

## Verification

Rust integration tests cover version negotiation, status propagation, binary
and zero-length buffers, invalid null pointers, bounded blocking, repeated
result disposal, panic containment, explicit runtime closure, and completion of
the asynchronous finalizer fallback. The latter runs in an isolated test
process and observes monotonic cleanup counters only after Core's destructor
returns, preventing a parallel test from producing a false positive. A C11
harness compiles against the public header, links the actual static archive,
exercises the ownership contract, and runs with Address Sanitizer and
UndefinedBehavior Sanitizer in the development container. An OCaml two-Domain
test calls the linked Rust archive and proves another Domain progresses during
a native wait. An install
smoke test builds a fresh OCaml executable from the staged package and invokes
the negotiated ABI through the public `Temporal.Runtime_info` module.

The isolated `runtime_cleanup_idempotence.rs` integration test calls native
runtime disposal twice and waits for exactly one cleanup-counter increment.
The no-detached-start-task regression remains in the private `abi.rs` test
context (`tests/support/pending_start_cleanup.rs`): it observes a task-owned
drop marker after nonblocking finalization, proving aborted Tokio handles are
joined by the cleanup thread rather than detached. Keeping the counter and
task-drop assertions in separate test sources prevents a future lifecycle
change from being hidden by the broad ABI test binary.

The client wait regressions in `tests/support/client_wait.rs` use Core's
callback gRPC transport to delay history responses beyond 100 ms. They count
actual RPCs, verify retained pagination and exact-run identity, and exercise
terminal errors, capacity, disconnect, and both runtime close paths. This
proves that yielding the owner does not restart a slow successful request.

The lifecycle regression corpus also covers the two less visible ownership
edges. The mailbox test abandons an admitted terminal reply while the owner is
still processing earlier work and proves that the owner settles the reply and
joins without a waiting caller. The pending-start transition test publishes a
terminal result and then races cancellation; shutdown must drain the ticket,
join the still-running Tokio task, and release Core only after that task is
gone. The ABI suite repeats disposal for an error diagnostic as well as a
success value, proving that both result buffers share the same idempotent
cleanup rule. Its malformed-heartbeat regression then reuses the same result
slot after a protocol error, proving that error cleanup cannot poison a later
ABI call. The activity protocol ownership test drops the source JSON string
after decoding and verifies that the task token and payload bytes remain
owned by the decoded OCaml/Rust value rather than by borrowed input storage.

The Dune rule asks `rustc --print=native-static-libs` for the exact native
libraries required by the static archive and consumes the resulting ordered
flags from a generated S-expression file. This keeps platform linker knowledge
owned by the pinned Rust compiler instead of duplicating a fragile Linux,
macOS, and Windows library list in the OCaml build.

The C binding is a Dune `foreign_library`, so Dune first compiles it into a
plain static archive without applying Rust's system-library flags. The OCaml
library then references both that C archive and the Rust archive, and applies
the generated flags only when linking a consumer. The workspace also disables
dynamically linked foreign archives. The internal OCaml library uses
`no_dynlink`, because a native plugin (`.cmxs`) would be another dynamic bridge
artifact and is neither supported nor needed by the final executable.

### Supported link modes

The supported deployment artifact is an OCaml-owned native executable; the
project does not need a separately loadable bridge DLL. This distinction is
important on Windows: Rust correctly reports GNU linker tokens for the final
native link, but FlexDLL cannot reinterpret all of those tokens while
constructing an intermediate OCaml stub DLL. Keeping the C and Rust inputs as
static foreign archives removes that unnecessary link step without changing
the installed OCaml API or final executable.

Because only the static archives are installed, bytecode consumers must link
with `-custom` (or as a Dune `(modes byte)` executable); dynamically loaded
bytecode and the toplevel cannot load the bridge. The user-facing statement of
this constraint, together with the related `Unix.fork` restriction, is in the
[workflow guide](../guides/workflows.md#process-and-linking-constraints).

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

ABI version 2 uses only symbols beginning with `ocaml_temporal_core_v2_`.
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

Both encoders reparse their own output before it can cross the native boundary.
Both decoders reject duplicate or unknown fields, unknown variants, numeric
range violations, non-canonical base64, invalid workflow invariants, and
oversized values. Core fields not represented by the current semantic slice are
accepted only at their documented default; a non-default value returns a typed
`Unsupported` conversion error rather than being lost. See the
[protocol reference](core-protocol.md), machine-readable schemas, and
[ADR 0006](../decisions/0006-first-workflow-semantic-protocol.md).

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
Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471).

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

The wait request names `namespace`, `workflow_id`, and one concrete `run_id`.
There is no `follow_runs` escape hatch in the document: the operation always
uses a close-event history long poll for that exact run, but each native call
is bounded to 100 ms. If no close event arrives during that interval, the ABI
returns status `10` (`NOT_READY`) without a terminal response; the OCaml
caller (or a later orchestration loop) can retry through the supervisor
mailbox. Bounding each call lets the single owner Domain admit shutdown and
other lifecycle messages instead of being held indefinitely by a server long
poll.

Completed, failed, and timed-out close events retain any successor run ID
exposed by Core. A continued-as-new close is returned as a terminal
`continued_as_new` outcome with a required successor execution reference; the
bridge never follows that run implicitly. This prevents an exact-run caller
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

All client-operation identifiers are nonempty and NUL-free. The schemas state the
65,536-character necessary bound, while the bilateral runtime validators apply
the authoritative 65,536-byte UTF-8 limit, reject duplicate members, and
reparse encoded output. JSON Schema counts Unicode characters rather than
encoded bytes, so schema validation alone is not a substitute for the runtime
checks.

## Result and buffer ownership

Every fallible operation that returns a result document accepts a writable
result pointer and returns the same status stored in that result. Runtime
close/dispose and result disposal have no result document and return their
status directly. Status zero is success. Nonzero statuses cover invalid
arguments, ABI mismatch, a contained Rust panic, internal bridge failure,
invalid lifecycle state, configuration, connection, worker, outstanding-task,
not-ready, protocol, and already-started failures. Worker polling and exact-run
client waits use the expected `NOT_READY` status. For a worker lane it means no
task is queued; for a client wait it means the 100 ms history wait elapsed
before a close event. In both cases the caller or a later orchestration loop
can retry through the supervisor mailbox.
`OUTSTANDING_TASKS` means shutdown cannot finalize until the language side
completes leased work.

A result has one success buffer and one error buffer. At most one owns memory:

- success may place arbitrary binary bytes in `value`;
- failure may place a UTF-8 diagnostic in `error`; worker and poll-lane
  failures use bounded constant categories, while client operation failures
  use the closed JSON documents described above;
- an empty allocation is always represented as `{ NULL, 0 }`.

Rust owns both allocations. The caller may copy their bytes but must never
mutate or directly free their fields. It must call
`ocaml_temporal_core_v2_result_free` exactly once after consuming an initialized
result. That function clears the object, so accidentally calling it again on
the same object is safe. Copying a live result structure creates no new
ownership; freeing both copies is invalid.

An output object may be uninitialized, but it must not contain a live owned
result when passed to another operation. Free the previous result first.

### OCaml ownership guard

The private C stubs allocate an OCaml custom block before entering Rust. That
block is the sole owner of the ABI result and has a finalizer which calls
`ocaml_temporal_core_v2_result_free`. The OCaml wrapper also uses
`Fun.protect` to release the result deterministically after copying its bytes.
This gives every path two compatible safeguards: normal operation frees
immediately, while an OCaml allocation failure or other exception leaves a
rooted/finalizable owner rather than orphaning Rust memory. Disposal is
idempotent, so a later finalizer after deterministic disposal is harmless.

Returned bytes are copied once, directly from the live Rust buffer into the
OCaml string/bytes allocation. For the canonical empty `{ NULL, 0 }` case, the
C binding allocates an empty OCaml value without passing the null pointer to
the runtime's initialized-string primitive. A nonempty null span is rejected
before dereference as an ABI defect. Inputs that must survive a blocking call are
copied to temporary C storage before the runtime lock is released, then freed
immediately after the lock is reacquired. Neither side directly frees an
allocation made by the other side.

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

`Sdk_supervisor.Native` is the private OCaml adapter for these ABI version 2
operations. It exposes a typed GADT rather than raw JSON bytes:

| Supervisor operation | Result and boundary behavior |
| --- | --- |
| `Try_poll_workflow` | `Workflow_protocol.activation option`; `None` means the workflow lane was empty at that instant |
| `Wait_workflow` | bounded native readiness wait; it does not consume an activation and releases the OCaml runtime lock |
| `Complete_workflow completion` | canonical strict JSON is generated and reparsed before the native completion call |
| `Try_poll_activity` | `Activity_protocol.task option`; `None` means the activity lane was empty at that instant |
| `Wait_activity` | bounded native readiness wait; it does not consume an activity task and releases the OCaml runtime lock |
| `Record_activity_heartbeat heartbeat` | canonical strict heartbeat JSON is validated and recorded for the outstanding activity lease without retiring it; the acknowledgement carries no cancellation flags, which arrive later on the activity poll lane |
| `Complete_activity completion` | the opaque token and result are validated before the native completion call |

All seven operations enter the same bounded mailbox as client and worker
lifecycle changes. A poll, completion, worker shutdown, and runtime shutdown
therefore cannot race native graph state. The pure protocol conversion module
is visible only from the private supervisor library so both serialization
directions can be tested without constructing a Core worker.

The two Rust readiness signals use one mutex-protected pending count per lane.
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
runtime pointer crosses the current C ABI; client and worker state are fields
within that Rust runtime:

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
`graceful_shutdown_timeout_ms`. Closed Draft 2020-12 schemas live under
`docs/schemas/bridge/`.

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
the optional `default_versioning_behavior` (`"auto_upgrade"` or `"pinned"`)
are passed to Core's `WorkerDeploymentBased` strategy. A behavior is rejected
when worker versioning is disabled. Registration, rollout, and compatibility
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
validation gracefully finalizes the temporary worker and leaves the client
available for a corrected retry.

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

ABI version 2 includes private readiness-wait symbols for the two independent
poll lanes. The supervisor may invoke them only from the owner-domain mailbox
handler; the C boundary releases the OCaml runtime lock while Rust waits and
reacquires it before returning. Callers must not turn a readiness wait into a
blocking condition wait on a workflow scheduler fiber or allow a second owner
to access the native worker graph.

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
token. A rejected generated completion remains a fatal worker error, but it
cannot also leave a fabricated language-side debt that blocks shutdown
forever. Regression tests cover this rule independently for workflow and
activity conversion failures, including the cancellation classification.

There is also a post-handoff decode-failure path for version or implementation
drift between the two strict decoders. OCaml preserves its original protocol
error, returns the exact Rust-produced bytes to the private rejection ABI, and
never reflects those bytes in diagnostics. Rust accepts rejection only after
full semantic equality with retained handoff state. For a Start, it then
generates the Core failure and retires both the ledger debt and retained
semantic state even if Core reports that generated failure as unsuccessful.
For a Cancel update, it retires only that retained semantic state: the shared
Start debt remains owned by the activity implementation. This prevents
shutdown from waiting forever without turning a malformed cancellation into a
spurious `UnknownActivity` completion failure, while keeping the original
decode failure primary.

Shutdown first closes ledger admission and both readiness signals, then asks
Core to wake both polls and joins the lane tasks. Existing ready and leased
work remains completable while the worker drains. Core finalization is refused
until the ledger is empty, and only then consumes the worker before client and
runtime destruction. The garbage-collection fallback cannot obtain missing
language completions. On the dedicated cleanup thread it force-fails
outstanding Core tasks, joins the poll lanes, and attempts normal finalization;
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
Cleanup finalizes worker, drops client, then drops Core even when callers did
not explicitly close the children.

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

The supported deployment artifact is an OCaml-owned native executable; the
project does not need a separately loadable bridge DLL. This distinction is
important on Windows: Rust correctly reports GNU linker tokens for the final
native link, but FlexDLL cannot reinterpret all of those tokens while
constructing an intermediate OCaml stub DLL. Keeping the C and Rust inputs as
static foreign archives removes that unnecessary link step without changing
the installed OCaml API or final executable.

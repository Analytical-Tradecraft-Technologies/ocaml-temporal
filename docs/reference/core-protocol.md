# Private JSON Control Protocol

This contributor reference defines messages exchanged between the private
OCaml runtime and the project-owned Rust Temporal Core bridge. Workflow authors
never construct these documents, and this protocol is not a public API.

## Direction and lifecycle

Either side may send a `request`; the operation defines its direction. The
receiver returns exactly one terminal `response` or `error`, copying the
request's `correlation_id` and `operation`. A request remains pending until that
terminal message arrives or the owning SDK instance shuts down.

This foundation validates transport structure. The first semantic worker slice
also defines closed workflow activation and completion documents in both
languages. Rust alone reads and writes Temporal/Core protobuf; OCaml sees
validated semantic records, variants, exact time components, and opaque payload
bytes. Native worker poll/completion operations carry these documents through
independent Rust-owned ready lanes. They are non-blocking at the OCaml boundary:
`not_ready` is an expected status rather than a thread wait. A shared ownership
ledger validates the run ID or opaque task token before Core sees a completion.

## Startup compatibility

Compatibility number `1` covers the C layout and JSON contract. The bridge
checks it once before creating an SDK runtime. It is absent from messages
because OCaml and Rust are compiled and shipped together. A different number
means a stale or partial build and fails startup; per-message negotiation and
mixed versions are unsupported.

## Two JSON layers

There are two related uses of the word “protocol” in this repository. They are
not nested inside one another:

1. The generic control envelope below has `kind`, `correlation_id`,
   `operation`, and `body`. It is the reusable transport foundation for a
   request/response exchange and is covered by `control_protocol` tests.
2. The live client and worker C ABI operations pass one direct,
   operation-specific JSON object. For example, a workflow poll returns a
   `workflow-activation` object, and a workflow completion accepts a
   `workflow-completion` object. The native ABI status (`not_ready`, protocol,
   connection, and so on) is returned separately; it is not another JSON
   envelope around that object.

The operation-specific documents therefore start at their own schema root:

| Native operation family | Direct JSON document | Reference |
| --- | --- | --- |
| Client start, ticket poll/wait | start request, ticket, or start outcome | [client protocol](client-protocol.md) |
| Client exact-run wait/cancel | wait or cancellation request and response | [client protocol](client-protocol.md) |
| Client asynchronous activity completion/heartbeat | async completion or heartbeat request | [client protocol](client-protocol.md) |
| Workflow worker poll/complete/reject | activation or completion | This document's workflow sections |
| Remote activity worker poll/complete/heartbeat | task, completion, or heartbeat | [activity protocol](activity-protocol.md) |

Use the envelope shape only when an operation explicitly declares an envelope;
do not put an activation, completion, task, or client request inside a second
`body` object merely because both layers use JSON. This distinction also
explains why the schemas in `docs/schemas/bridge` describe several different
top-level documents rather than one universal message schema.

## Envelope and correlation

Requests and successful responses have exactly four fields:

```json
{"kind":"request","correlation_id":"0123456789abcdef0123456789abcdef","operation":"worker.poll","body":{}}
```

`kind` is exactly `request`, `response`, or `error`. A correlation identifier is
32 lowercase hexadecimal characters. It is opaque, not a workflow/run ID,
tracing secret, or database key. Operation names are 1 to 64 ASCII characters,
start with a lowercase letter, and otherwise contain lowercase letters, digits,
`_`, or `.`.

`body` is a JSON object. The foundation enforces global resource and JSON rules;
each future operation must close its body schema and reject its unknown fields.
Arrays, primitives, and opaque serialized protobuf are not envelope bodies.

Failed responses replace `body` with a closed error object:

```json
{"kind":"error","correlation_id":"0123456789abcdef0123456789abcdef","operation":"worker.poll","error":{"code":"invalid_message","message":"request body was invalid","retryable":false}}
```

Codes are `invalid_message`, `unsupported_message`, or `internal_bridge`.
`message` is non-empty UTF-8 of at most 1,024 bytes. `retryable` says whether an
unchanged retry might succeed. Expected failures are structured `result`
values; exceptions, panics, raw parser diagnostics, and Core failures do not
escape their boundaries.

## Strict rules and finite limits

Both incoming and outgoing paths enforce:

Required-nullable fields must be explicitly present. Omission is malformed and
is never normalized into JSON `null`; only fields documented as compatibility
extensions may be absent.

| Resource | Limit |
|---|---:|
| Complete UTF-8 document | 201,326,592 bytes (192 MiB) |
| Object/array nesting | 128 levels, outer value included |
| One decoded string or object key | 65,536 UTF-8 bytes |
| Members in one object | 201,326,592 (document-derived guard) |
| Elements in one array | 201,326,592 (document-derived guard) |
| Values in the complete tree | 201,326,592 (document-derived guard) |
| One decoded opaque byte field | 134,217,728 bytes (128 MiB) |
| Error message | 1,024 UTF-8 bytes |

The 65,536-byte string limit applies to control envelopes and semantic text.
Closed payload byte wrappers are the only exception: each `data` field may
contain up to 178,956,972 ASCII base64 bytes, the canonical padded encoding of
the 128 MiB per-field ceiling. The 192 MiB document ceiling applies to the
whole JSON document, so several byte fields remain collectively bounded rather
than each receiving a separate document allowance.

These are private bridge safety limits, not Temporal semantic limits. Temporal
Server applies namespace-configurable identifier and blob policies. The
per-field ceiling follows pinned Core's default 128 MiB incoming gRPC decoding
limit, while the document ceiling provides base64 and structural headroom for
that transport boundary. The parser rejects an oversized document before JSON
tree construction, checks encoded base64 length before decoding, and checks
Core payload byte lengths before cloning them into semantic values. Making
these bridge ceilings configurable per SDK instance remains future work.

The collection and parsed-node numbers are no-op consistency guards derived
from the document byte ceiling: every member, element, and value necessarily
occupies at least one source byte. They do not add a smaller job, command,
header, or payload-count policy, and post-parse node validation is not treated
as memory protection; the pre-parse document check owns that role. The
128-level nesting limit instead matches serde_json's enabled default recursion
guard, keeping both implementations within the Rust parser's stack-safety
boundary. It permits realistic recursive failure chains beyond the former
16-level limit. An iterative parser or configurable depth policy is a future
option; unbounded recursive parsing is deliberately not enabled.

Numbers must be integral and fit the common signed 64-bit range. Domains that
may exceed portable JSON integers must use validated decimal strings in their
future operation schema. Fractional numbers, trailing input, and Yojson
extensions are invalid.

Closed objects reject missing and unknown fields. Every object, including body
objects, rejects duplicate member names while the entry sequence still exists;
decoding directly into a map would silently lose that evidence. JSON Schema
cannot prove this property because many tools receive a map after duplicates
have been discarded.

## Normalized output and outgoing validation

Output is UTF-8 JSON without insignificant whitespace. Envelope and error fields
use the order shown above. Body keys are sorted recursively, arrays retain order,
and integers use shortest decimal form. Maintained serde_json and Yojson
encoders own string escaping.

Before sending, each side validates the typed value, serializes it, and passes
the bytes through its strict incoming decoder. The receiver repeats strict
parsing and validation. Normalization supports review and golden tests; it is
not a cryptographic signing format.

## Opaque payload bytes and privacy

Payloads use a separate closed object:

```json
{"encoding":"base64","data":"AAEC/v8="}
```

`encoding` is exactly `base64`; `data` is canonical padded RFC 4648 base64.
Unpadded, over-padded, non-alphabet, non-canonical, or decoded values over the
limit are rejected. This control encoding does not require workflow authors to
choose a JSON Temporal payload codec.

Protocol errors and logs may contain kind, operation, correlation identifier,
path, limit name, and stable error code. They must never contain source JSON,
payload base64, decoded payload bytes, workflow inputs/results, auth material,
or Core diagnostics that might embed those values.

## Client start and exact-run wait semantics

The private client operations are separate from the worker activation protocol.
`client_start_workflow_json` accepts a closed object with the stable
idempotency key `request_id`, `namespace`, `workflow_id`, `workflow_type`,
`task_queue`, and an ordered array of the canonical Temporal payload objects.
The bridge passes those fields to Core's raw `StartWorkflowExecution` request
and returns an execution reference with the server-assigned run ID. The
public client may generate `request_id` when its caller omits one, but one
logical start and every retry of its asynchronous ticket use the same key.
Start options not represented by this first schema are left at Core's
documented defaults rather than being guessed by the language layer.

`client_wait_workflow_json` accepts exactly one execution identity. Each ABI
call issues a close-event history long poll with `wait_new_event = true` and a
fixed `follow_runs = false` policy for at most 100 ms. If the run remains open,
the call returns status `10` (`NOT_READY`) and no terminal JSON; the OCaml
caller (or a later orchestration loop) can retry the same exact identity
through its mailbox. This bounded operation is important because the
supervisor owner Domain also serializes lifecycle messages and must regain
control to process shutdown.

A completed, failed, or timed-out event carries an optional successor
reference when Core provides one. A continued-as-new event is itself terminal
for the requested run and must contain the successor reference; the bridge does
not follow the successor and therefore cannot silently change the run returned
to the caller. Cancelled and terminated outcomes preserve their ordered detail
payloads.

Every non-null successor remains in the same execution chain: its namespace
and workflow ID must equal the waited execution, while its run ID must differ.
The Rust encoder and OCaml decoder enforce this relationship directly because
standard JSON Schema cannot compare values held in separate objects.

The ABI error buffer uses a closed `client-error.schema.json` body for
AlreadyStarted, stable RPC-code failures, and stable Core protocol categories.
It never contains raw gRPC status text, which can include workflow identifiers
or payload-derived diagnostics. Core conversion codes are exactly
`core_unsupported` and `core_invalid`. RPC codes use the closed, privacy-safe
snake-case tonic status vocabulary instead of reflecting diagnostic text.
The Rust validator checks duplicate object members before typed serde parsing,
canonical base64, NUL-free identifier byte limits, and output round trips. The
OCaml adapter applies the same identifier and successor checks before requests
or responses cross its typed boundary. These documents remain private bridge
protocol rather than a public workflow-authoring API.

## Workflow activation and completion semantics

An activation is a closed object sent from Rust to OCaml. It identifies one
`run_id`, carries exact `timestamp` seconds and nanoseconds for ordinary work,
and retains Core's replay state, history state, and job order. Core's synthetic
cache-eviction constructor deliberately omits the timestamp, represented as
JSON `null`; no other activation may omit it. The supported job slice is initialization,
remote-activity resolution, timer firing, workflow cancellation, and cache
eviction, and both stages of child-workflow resolution. An eviction must be the
only job in its activation. Initialization, when present, must occur exactly
once and as the first job. Sequence numbers
and history length are unsigned 32-bit integers. Sequence zero is valid because
Core defines the value as language-SDK supplied and its pinned tests exercise
zero for timers and activities. Randomness seeds and history
sizes are canonical unsigned 64-bit decimal strings so no JSON implementation
can round them through a floating-point number.

Workflow initialization preserves arguments, headers, identity, parent and root
execution identity for children, execution/run/task timeouts, first execution
run ID, start time, priority, attempt, and randomness seed. Priority's `f32`
fairness weight is carried as its unsigned IEEE-754 bit pattern so strict
integral JSON preserves every value exactly. Activation-wide internal flags, history size, continue-as-new
suggestions, deployment identity, SDK version, and target-deployment change are
also preserved. Optional `context` and `metadata` objects may be absent for
older synthetic fixtures, but when present their nested shapes are closed.
Other Core initialization fields outside this first slice are rejected as
`unsupported`; they are never discarded silently.

When Core starts a successor run, `context.continuation` is a
required-nullable nested object. It preserves the previous execution run ID,
the closed `initiator` enum (`unspecified`, `workflow`, `retry`, or
`cron_schedule`), and independent nullable values for the previous run's
failure and completion payloads. Ordinary first-run initialization encodes
`continuation: null`. The bridge validates and deep-copies these fields before
the workflow runtime retains them, so a continuation's terminal metadata
cannot be lost or alias native payload storage.

A child resolution is deliberately split into two activation jobs. A
`resolve_child_workflow_start` job carries either the assigned run ID, a typed
start cause (`workflow_already_exists` or `unspecified`), or a cancellation
failure. A later `resolve_child_workflow` job carries a nullable successful
payload, a structured child failure, or a cancellation failure. Both jobs use
the sequence from the original start command. The OCaml runtime accepts that
sequence twice only for this start/terminal pair; duplicate events and
cross-kind collisions are invalid. A terminal job before a successful start is
rejected by the runtime so a parent cannot observe a child that Core has not
started.

Both language decoders validate the complete child-resolution object before an
activation reaches the runtime. Required identifiers must be nonempty and
within the UTF-8 safety ceiling; outcome discriminators, start causes, nullable
payloads, and recursive failure objects are closed and type-checked; child
failure event IDs cannot be negative. One Temporal Core edge state is explicit:
when cancellation is reported before `ChildWorkflowExecutionStarted`, Core
does not know a child run ID and sends `run_id: ""` with
`started_event_id: 0`. The bilateral validators preserve that empty value only
for this pre-start state; a child failure after start must carry a nonempty run
ID. A malformed document returns the typed `invalid_message` protocol error and
has no lifecycle side effect. Runtime ordering checks happen only after this
parse boundary: a terminal-before-start, duplicate start, duplicate terminal,
or unknown sequence returns a typed bridge defect and leaves the existing
resolver state unchanged.

A completion is a closed object sent from OCaml to Rust. Optional
`task_failure` distinguishes a failed workflow task from successful commands.
Absent or null retains the original successful format; a structured failure
requires an empty `commands` array and maps to Core failed status with
`WorkflowWorkerUnhandledFailure`. It never closes the workflow execution.
Query-only activations still require query answers, and eviction requires
successful empty acknowledgement. See [workflow failures](workflow-failures.md).

For successful completions, ordered commands
cover scheduling and requesting cancellation of remote activities, starting and
cancelling a child workflow, signalling or requesting cancellation of an
external workflow, starting and cancelling timers, and completing,
failing, or cancelling the workflow. A child start includes an explicit
cancellation policy, and a later cancel command carries a validated reason;
Core applies that policy while preserving command order for replay. The child
command deliberately omits namespace, task queue, timeout, retry, header,
memo, search-attribute, versioning, and priority fields because the current
OCaml runtime does not expose them. For a live or replay worker, Rust injects
the worker's already-validated namespace into Core's child-start command before
submission; this is worker configuration, not workflow input. The remaining
omitted Core fields receive explicit defaults and non-default values are
rejected on reverse conversion. Injecting the namespace is important because
Core copies it into child failure metadata, including cancellation before the
child has a run ID; leaving it at Core's empty protobuf default would make that
otherwise valid activation fail the semantic protocol validator.
Scheduled activities require at least a schedule-to-close or start-to-close
timeout. They may also carry a closed retry-policy object with positive initial
and nondecreasing maximum intervals, a finite backoff coefficient at least 1.0,
and a signed 32-bit maximum-attempt count. The coefficient is an unsigned
decimal IEEE-754 bit string rather than a JSON float, and an omitted policy is
represented by the required JSON null member. The completion schema records the
timeout requirement as an `anyOf` constraint in addition to the
per-field nullable types; runtime validation remains
authoritative for duplicate members and UTF-8 byte limits. A terminal workflow
command may occur at most once and must be last.
When acknowledging an eviction, the completion command list must be empty and
the run ID must match the activation.

### External workflow operations

`signal_external_workflow` and `request_cancel_external_workflow` are
asynchronous workflow commands. OCaml allocates a sequence number, emits the
command, and returns a workflow future; it does not call Temporal directly or
block a scheduler thread. Core later sends a matching activation job named
`resolve_signal_external_workflow` or
`resolve_request_cancel_external_workflow`. The job is either
`{"kind":"succeeded"}` or contains a structured `failure`. The runtime removes
the one resolver for that sequence before waking the future, so duplicate,
unknown, or cross-operation sequence numbers are rejected as bridge defects.

An external signal contains a non-empty workflow ID, an optional exact run ID,
an identifier-safe signal name, ordered payloads, and a bounded header map.
When `child_workflow_only` is true, Rust constructs Core's child-workflow target;
otherwise it constructs a namespaced workflow execution using the worker's
validated namespace. External cancellation retains the same target fields and
requires a non-empty reason. Payload bytes are copied at the OCaml/Rust
boundary, and both language validators reject unknown fields, duplicate keys,
invalid UTF-8, NUL bytes, and oversized values before Core sees a command.
The complete shapes are defined by
[`workflow-activation.schema.json`](../schemas/bridge/workflow-activation.schema.json)
and [`workflow-completion.schema.json`](../schemas/bridge/workflow-completion.schema.json).

### Patch notification and marker

Core reports durable patch history with a passive activation job:

```json
{"kind":"notify_has_patch","patch_id":"orders.v2"}
```

The activation decoder validates `patch_id` as a non-empty, NUL-free UTF-8
identifier within the 65,536-byte boundary. The runtime installs every
notification before running workflow fibers. Core prepares query jobs in their
own activation, so both language validators reject a query mixed with this
notification.

Each `Temporal.Workflow.patched` call returns a per-execution decision and
emits an active completion command:

```json
{"kind":"set_patch_marker","patch_id":"orders.v2","deprecated":false}
```

`Temporal.Workflow.deprecate_patch` emits the same command with
`deprecated:true` and returns `unit`, because deprecation is a lifecycle marker
rather than a branch decision. The command is a closed record and `deprecated`
is required. Repeated commands in the same mode are preserved in order; Core,
rather than the OCaml runtime or JSON bridge, owns durable marker
deduplication. Both language validators reject active and deprecated commands
for the same ID in one completion, preventing Core's first-command-wins rule
from making durable history depend on ambiguous source order.

Workflow code can also merge indexed search attributes with a deterministic
completion command:

```json
{
  "kind": "upsert_search_attributes",
  "search_attributes": {
    "agent_status": {
      "metadata": {"encoding": "json/plain"},
      "data": "InJlYWR5"
    }
  }
}
```

The OCaml API validates and copies keys and payloads before buffering this
command. Rust validates the same limits before converting the map to Core's
`UpsertWorkflowSearchAttributes` command. Core applies the map as one merge,
and the new values become visible only after the workflow task is accepted;
the bridge does not perform a visibility query itself.

### Continue-as-new

The public OCaml operation `Temporal.Workflow.continue_as_new` emits this
terminal command shape:

```json
{
  "kind": "continue_as_new",
  "workflow_type": "counter",
  "input": [
    {"data":{"data":"","encoding":"base64"},"metadata":{}}
  ]
}
```

`workflow_type` is a validated non-empty Temporal identifier. `input` keeps
the successor's ordered argument payloads, even though the current public
helper normally supplies one encoded workflow input. The command must be the
only terminal command and must be the final command in the completion. Both
the OCaml and Rust decoders reject missing or unknown fields, invalid
identifiers, malformed payloads, and a terminal command followed by another
command.

Rust converts this record to Core's
`ContinueAsNewWorkflowExecution`. The current semantic protocol intentionally
does not expose a task queue, timeout, memo, headers, search attributes,
retry policy, or versioning controls for this command. The Rust reverse
conversion accepts only Core's explicit defaults for those fields and returns
`core_unsupported` for a non-default value, preventing silent loss of workflow
semantics. The successor run is not followed by the bridge or by
`Temporal.Client.wait` automatically.

Temporal identifiers must be nonempty but use the protocol's 65,536-byte text
safety ceiling rather than an invented 255-byte server policy; the server's
identifier policy is configurable. The one intentional exception is the
pre-start child failure `run_id` described above. Application failure `type` is
bounded text and may be empty. Activity failure event IDs are nonnegative and
worker identity is bounded text. Durations use nonnegative seconds plus 0
through 999,999,999 nanoseconds;
timestamps allow signed seconds with the same nanosecond range. Payload metadata
and initialization header maps normalize keys lexicographically on both sides.
Payload values preserve opaque data and metadata bytes using the canonical
base64 wrapper. Supported structured failures are application, cancellation,
activity, child-workflow, and timeout failures, including recursive causes,
child execution identity, event IDs, and retry state. A timeout failure keeps
Core's exact `timeout_type` (`unspecified`, `start_to_close`,
`schedule_to_start`, `schedule_to_close`, or `heartbeat`) and the ordered
`last_heartbeat_details` payload list. Unknown
protobuf oneofs, enum values, external payload references, unsupported failure
variants, or omitted Core fields with non-default values fail conversion.

The Rust conversion functions are the only protobuf boundary. They convert
official pinned Core activations to semantic values and semantic completions
back to official Core values. Core's `is_local` activity-resolution flag is not
represented because the pinned Core contract explicitly says language SDKs do
not need to distinguish it; every other omitted value is checked before
conversion.

## Remote activity task and completion semantics

Remote activity tasks use a separate closed document. The opaque Core task
token is canonical padded base64 and must be nonempty. A start task preserves
workflow and activity identity, headers, inputs, heartbeat details, exact
timestamps and timeouts, attempt, effective retry policy, priority, and the
standalone activity run ID. Retry backoff uses its unsigned IEEE-754 bit pattern
as a decimal string. A cancellation preserves its reason and independent detail
flags. Local activity tasks are rejected because this worker does not enable
their distinct lifecycle.

Completions contain the same task token and exactly one result: completed with
an optional payload, failed, cancelled, or will-complete-asynchronously.
Structured failures reuse the workflow protocol's validated failure model.
Both directions validate and normalize JSON before handoff; only Rust converts
to and from official Core protobuf values.

## Schemas and fixtures

Draft 2020-12 schemas live under [`docs/schemas/bridge`](../schemas/bridge/).
Shared positive and malformed fixtures under `test/bridge/fixtures/protocol`
drive the envelope tests. Bilateral activation/completion fixtures live under
`test/bridge/fixtures/workflow-protocol`; their schemas are
`workflow-activation.schema.json` and `workflow-completion.schema.json`.
Remote activity documents are described by `activity-task.schema.json` and
`activity-completion.schema.json`. Both protocol families reference
`temporal-payload.schema.json` and `temporal-failure.schema.json`. The OCaml
adapter's ownership, typed representation, and validation behavior are described
in the [activity protocol reference](activity-protocol.md). Runtime
validators remain authoritative for
duplicate keys, decoded payload length, aggregate node count, normalization,
privacy-safe errors, and every UTF-8 byte limit. JSON Schema `maxLength` counts
Unicode characters rather than encoded UTF-8 bytes, so it documents a useful
upper bound but cannot enforce the byte-count contract; schema validation alone
is insufficient.

# OCaml activity protocol adapter

`Temporal_protocol.Activity_protocol` is the private OCaml representation of
activity-task and completion JSON exchanged with the Rust Temporal Core bridge.
The same document shape carries remote server activities and local-activity
lane tasks; the required `is_local` start field preserves Core's locality bit
because local activities cannot hand off completion asynchronously. It is an
internal worker building block, not an API that workflow or
activity authors construct directly. Rust remains the only protobuf boundary;
this module sees ordinary OCaml records, variants, byte strings, and typed
validation failures.

## Direction and ownership

Rust sends one `task` after Core has leased an activity attempt. The task
contains an opaque binary `task_token` and either a complete start context or a
cancellation update. The native OCaml activity adapter retains the token and
copies it unchanged into a terminal completion obligation. If native transport
rejects that completion, the adapter retries the same copied value without
invoking the activity implementation again. This keeps the opaque correlation
value and the user-side execution aligned while Rust's outstanding-task ledger
decides whether the lease can be retired. The adapter never interprets token
bytes.

Start context retains whether Core leased a local activity, the scheduling
workflow identity, activity identity,
headers, ordered arguments, heartbeat details, timestamps, timeouts, one-based
attempt supplied by Core, normalized retry policy, task priority, and optional
standalone activity run ID. Cancellation context retains both Core's primary
reason and its independent detail flags. The completion result is a closed
variant: completed with an optional payload, failed, cancelled, or
will-complete-asynchronously.

The `will-complete-asynchronously` variant is the remote worker-to-client
handoff for `Temporal.Activity.define_async`. The adapter sends it through the
ordinary worker completion path exactly once, then activates the opaque handle
only after Core accepts it. For a local task, the adapter instead closes the
dormant handle and submits a non-retryable activity failure for the same token;
Core cannot accept a deferred local completion. Later remote completion,
failure, cancellation, and heartbeat
operations use separate namespace-bound client endpoints. Activity authors
cannot construct a task token or retain the ordinary attempt context; they can
only retain the typed handle returned by `Async_context.handle`.

The module has no native handle, mutable global registry, Domain, fiber, or
callback. Decoding allocates OCaml-owned values. Encoding reads those values
and returns a new JSON string. Native worker lifecycle and serialization remain
the supervisor's responsibility, so this adapter adds no race or
cross-language memory-ownership path.

## Shared semantic codecs

Activity JSON deliberately reuses the workflow protocol's canonical codecs for
Temporal payloads, recursive failures, timestamps, durations, workflow
executions, and task priority. This prevents the two OCaml paths from acquiring
different base64, nanosecond, failure-depth, identifier, or floating-point-bit
rules. The shared functions remain inside the `temporal-sdk` package's private
dependency tree and are not re-exported from the public `Temporal` library.

The typed activity token is `bytes`, although the JSON field is canonical
padded RFC 4648 base64. This avoids treating correlation data as text while
ensuring encoding always uses the audited binary codec. Retry backoff bits stay
as canonical unsigned 64-bit decimal text because all bit patterns do not fit
OCaml's signed `int64`. Activity attempt stays `int64` so the complete unsigned
32-bit protobuf domain is representable without depending on OCaml platform
integer width.

## Validation contract

`decode_task`, `decode_completion`, and `decode_heartbeat` first pass the
complete document through the duplicate-aware, bounded JSON foundation. Every
nested object is closed:
missing, unknown, and duplicate fields fail. Fields documented as nullable must
still be present with either their value or JSON `null`.

Semantic validation then enforces:

- nonempty canonical task tokens within the opaque-byte ceiling;
- nonempty workflow, run, namespace, activity, and activity-type identifiers;
- nonempty unique header keys, with maps normalized lexicographically;
- a required boolean `is_local` on start tasks, retained from Core;
- signed timestamp seconds and nonnegative duration seconds, both with
  nanoseconds from 0 through 999,999,999;
- unsigned 32-bit attempts and priority-weight bits, signed 32-bit priority and
  maximum-attempt fields, and Core's 64-byte fairness-key limit;
- canonical unsigned 64-bit decimal retry backoff bits, validated retry
  intervals, and bounded non-retryable failure-type strings; and
- the same bounded recursive structured failure and payload semantics used by
  workflow activations and completions.

The OCaml encoders reparse their own output through the corresponding strict
decoders. The Rust bridge validates completion and heartbeat JSON again at the
worker ABI, so a value accepted by one language still crosses a second
validation boundary before reaching Core.

## Heartbeat document

An activity heartbeat is a progress message, not a completion. Its exact JSON
shape is:

```json
{
  "task_token": "AAEC/v8=",
  "details": [
    {
      "metadata": {
        "encoding": {"encoding": "base64", "data": "YmluYXJ5L3BsYWlu"}
      },
      "data": {"encoding": "base64", "data": "cHJvZ3Jlc3M="}
    }
  ]
}
```

The token must be the canonical padded base64 representation of the currently
leased activity token. Details preserve their order and use the same binary
payload representation as task inputs and completion outputs. The OCaml and
Rust validators reject unknown or duplicate object members, malformed base64,
empty tokens, and payloads outside the shared size and metadata rules. The
Rust worker bridge checks a normal task token against its outstanding-task
ledger before handing a heartbeat to Temporal Core; it does not retire that
lease. The pinned Core `record_activity_heartbeat` API is fire-and-forget, so
the normal worker operation returns only an acknowledgement. If Core reports
`cancel_requested`, `activity_paused`, or `activity_reset`, the worker lane
delivers those facts asynchronously in a later `ActivityTask::Cancel`; no
synchronous status is invented at the heartbeat call boundary. The normal
adapter must later submit a terminal completion, including a `Cancelled`
result when it receives that cancellation task, and only that terminal path
removes the worker token from its ledger. An asynchronous heartbeat instead
uses the namespace-bound client path and is checked by Core's async activity
handle; its separate async lease remains non-terminal until an async terminal
operation is accepted or a non-retryable bridge failure closes and removes it.

The schema is [`activity-heartbeat.schema.json`](../schemas/bridge/activity-heartbeat.schema.json).
The focused bilateral tests are
`test/bridge/test_ocaml_activity_protocol.ml` and
`rust/core-bridge/tests/activity_protocol.rs`. The Rust ownership regression
also drops the source JSON buffer after decoding, demonstrating that task
tokens, heartbeat details, and metadata survive without borrowing caller-owned
input memory.

Encoding performs the reverse conversion, normalizes object keys, then decodes
the produced JSON again. Invalid typed outgoing records therefore fail before
they can cross the C/Rust boundary. `error_view` exposes only a stable code,
path, and payload-safe message; source JSON, base64 data, token bytes, inputs,
results, and failure details are never included.

## Completion and cancellation

`encode_completion` validates the token and the closed result union before
emitting JSON. The Rust worker ABI decodes and validates that document again
before calling Temporal Core. It first checks that the token is currently
leased, then retires the ledger entry only after Core accepts the completion.
If transport or Core rejects the call, the lease remains outstanding so the
OCaml adapter can retry the same pending completion; it never reruns the user
activity merely because submission failed.

A task with `variant.kind = "cancel"` does not invoke user activity code. The
adapter maps its stable reason to a `Cancelled` completion with the standard
Temporal `Canceled` failure and retains the independent flags on its private
OCaml outcome metadata for instrumentation. The flags are not copied into an
application payload or derived from the heartbeat acknowledgement. A heartbeat
is non-terminal and therefore cannot acknowledge cancellation or completion by
itself.

Cancellation is an update on the start task's token, not a second completion
obligation. The Rust poll lane may enqueue that update while the owner Domain
is handing off or completing the start. If the owner later sees
`AlreadyLeased` (the start is still owned) or `UnknownActivity` (the start has
already completed), it drops the stale cancellation without sending another
completion. This mirrors Temporal Core's own orphan-cancellation handling and
prevents a duplicate-token completion race; start-shaped delivery failures
continue to use the normal force-failure path.

`will_complete_async` is accepted only as the worker handoff. The
namespace-bound client endpoint rejects a second defer marker and accepts only
completed, failed, or canceled terminal results. The adapter keeps the copied
async lease while a request is in flight and retains it for retry only when the
native supervisor explicitly returns the bilateral `Retryable` status. A
successful terminal request retires the lease; a generic `Connection`,
`NotFound`, or other non-retryable bridge failure closes and removes it. No
transport result reruns the activity callback.

## Schemas and tests

The Draft 2020-12
[`activity-task.schema.json`](../schemas/bridge/activity-task.schema.json),
[`activity-completion.schema.json`](../schemas/bridge/activity-completion.schema.json),
[`activity-async-completion.schema.json`](../schemas/bridge/activity-async-completion.schema.json),
and [`activity-heartbeat.schema.json`](../schemas/bridge/activity-heartbeat.schema.json)
files document the closed wire shapes and link to the existing payload and
failure schemas. The ordinary completion schema includes the worker-only
`will_complete_async` handoff; the async-completion schema intentionally
excludes it and documents the later client endpoint. Runtime validation remains
authoritative for duplicate keys,
UTF-8 byte counts, decoded byte sizes, exact unsigned 64-bit range, aggregate
document limits, and normalized output because JSON Schema cannot prove all of
those properties.

Separated OCaml tests in
`test/bridge/test_ocaml_activity_protocol.ml` cover all task and completion
variants, binary token preservation, required-nullable members, closed nested
objects, identifiers, headers, time and duration domains, attempt and retry
numeric ranges, priority bits, and sender-side duplicate-map rejection.

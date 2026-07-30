# Signals, queries, and updates

This page explains the first public slice of workflow interactions in the
OCaml API. It is written for someone defining a workflow or a test for the
first time, so it separates three things that are easy to confuse:

| Piece | What it describes | What it does not do yet |
| --- | --- | --- |
| `Signal.t`, `Query.t`, or `Update.t` | A name and the codecs for its typed values | Contact Temporal by itself |
| A `Handler.t` | The OCaml function that handles one definition | Register a handler for a different interaction kind |
| `Temporal.Interaction.t` | An immutable, in-memory dispatcher for local tests | Deliver a message from a live Temporal Server |

The definitions and handler types are available in `Temporal.Signal`,
`Temporal.Query`, and `Temporal.Update`. `Temporal.Interaction` provides the
deterministic local routing path for tests. Native workflow signal delivery is
now available when a handler is attached to `Temporal.Worker.workflow`. Native
query delivery is also implemented at the bridge boundary. Native
updates have an experimental typed slice: a registered one-input handler may
 suspend on a workflow future; acceptance is emitted before it parks and
 completion is emitted when it resumes. PR #266 provides the first focused live proof of the typed
signal/condition path; the recorded seventeen-result Compose baseline is
also covered by the [PR #289 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29339077368).
The [PR #302 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638)
first verifies the later long-backoff extension, and the complete [PR #439
Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578)
retains both paths in the current 26-start baseline.
The [PR #406 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29557704643)
also proves an output-only client query against the exact signal-condition run
while it is parked. The complete [PR #434 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29684113836)
also proves the typed-input query against that parked run, while the [PR #428
Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29676120429)
proves typed update admission and completion. Suspended update recovery,
query deadlines and replay/cache-eviction behavior, and broader interaction
coverage remain future work.

`Temporal.Client.query` is a separate control-plane operation: it asks an
already registered workflow for an output-only query through an exact
workflow/run handle and returns the typed result. `Temporal.Client.query_with_input`
does the same for a query defined with `Query.define_with_input`; it encodes
exactly one argument before transport. Neither method registers or invokes a
handler locally. See the [native client JSON protocol](client-protocol.md) for
the request/response shape and exact-run semantics; output-only query
acceptance is recorded in PR #406 and both query forms are live-verified by PR
#434. Typed update admission/completion is live-verified by PR #428; rejected
unknown updates are covered by PR #432.

## Current status: local handlers and a partial native boundary

`Temporal.Interaction` remains the public, deterministic path for exercising
all three interaction kinds locally. The native bridge accepts a Core
`SignalWorkflow` activation, validates and copies its ordered payloads,
identity, and headers, and dispatches it to the matching handler registered on
the workflow. The callback is queued on that execution's scheduler, so it can
use deterministic workflow APIs and follows the same source ordering as root,
timer, activity, and child continuations.

The native public handler currently accepts exactly one payload. An activation
with zero or multiple payloads is completed as a non-retryable workflow failure
instead of dropping data or choosing an arbitrary element. A signal with no
matching handler follows the same fail-closed path. Identity and headers are
validated and retained by the runtime, but the first public handler API exposes
only the typed payload; a later API can add those metadata fields without
changing the transport contract.

Query activations are delivered to a registered handler on the execution owner
Domain and produce a matching query result. `Query.define` creates an
output-only handler; `Query.define_with_input` creates a handler that decodes
exactly one argument. Zero or multiple arguments for a typed handler, and any
non-empty argument list for an output-only handler, produce a typed
non-retryable query failure rather than silently dropping data. Query callbacks
are synchronous and non-suspending; they do not enter the workflow scheduler.

Native update activations are validated and copied in the same way, then routed
by update name to a typed `Temporal.Update.Handler.t`. The adapter requires
exactly one input payload. It runs the validator when Core requests it, skips
validation on replay, and emits accepted before invoking the handler. A handler
that awaits a workflow future retains its continuation in the execution-owned
pending map; a later activation emits completed or rejected and removes that
entry. Codec failures, missing handlers, unsupported input arity, duplicate
pending protocol IDs, and callback errors become typed rejections. Focused
native tests prove this behavior; live typed admission/completion and the
missing-handler rejection are verified by PRs #428 and #432.

## The three-step model

An interaction is assembled in three steps:

1. Define a stable name and the codec for each value that crosses the
   interaction boundary.
2. Pair that definition with a typed OCaml callback to make a handler. Update
   callbacks may use ordinary workflow helpers such as `Future.await`.
3. Attach signal, typed query, and/or update handlers to a worker workflow, or
   put handlers in an `Interaction` dispatcher when writing a local test.

The definition and handler preserve the relationship between a Temporal name,
its codec, and its OCaml type. A caller cannot accidentally pass a string to a
handler that was defined for bytes without receiving a typed codec error. The
native transport and the remaining handler/response lifecycles are described
in the [native interaction design](../design/native-interactions.md). Signal
and typed query activation delivery plus two-phase update responses are
implemented. Output-only query acceptance is live-verified by PR #406, both
query forms by PR #434, and typed update admission/completion by PR #428. The
remaining live boundaries are suspended update recovery, query deadlines and
replay/cache-eviction behavior, and broader handler policies.

## Definitions

Each definition pairs a validated Temporal name with the codec for values that
cross the interaction boundary:

```ocaml
let approval =
  Temporal.Signal.define
    ~name:"approval"
    ~input:Temporal.Codec.string

let status =
  Temporal.Query.define
    ~name:"status"
    ~output:Temporal.Codec.string

let status_for_user =
  Temporal.Query.define_with_input
    ~name:"status-for-user"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let add_tool =
  Temporal.Update.define
    ~name:"add-tool"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
```

The public shapes are:

```text
Signal.define : name:string -> input:'a Codec.t -> 'a Signal.t
Query.define  : name:string -> output:'b Codec.t -> 'b Query.t
Query.define_with_input : name:string -> input:'a Codec.t ->
                          output:'b Codec.t -> ('a, 'b) Query.typed
Update.define : name:string -> input:'a Codec.t -> output:'b Codec.t
                -> ('a, 'b) Update.t
```

The name must be non-empty, NUL-free, valid UTF-8, and no longer than the
bridge's 65,536-byte identifier limit. Constructing a definition does not
register a worker or contact Temporal. It creates an immutable typed
description that can be reused by ordinary helper functions and handler
registration. An invalid name raises `Invalid_argument` because it is a
programmer or configuration defect detected while building the workflow,
not a routine runtime failure.

Signals carry one input and do not return a result. Output-only queries return
one typed value; typed queries receive exactly one typed input and return one
typed value. Updates carry one input, run an optional validator, and return one
typed result.

### Codecs and payloads

A codec is the explicit boundary between an OCaml value and the payload that
the interaction dispatcher routes. On the way into a handler, the dispatcher
encodes the caller's value and the handler decodes that payload. On the way
out, it encodes the handler's result and the caller decodes it with its
definition. Encoding metadata is checked before a decoder runs, so a caller
using a definition with a different encoding receives `Error` instead of
silently interpreting the bytes as another type.

`Temporal.Codec.string` uses Temporal's `json/plain` payload encoding. That is
one payload choice, not a requirement that Temporal or the private OCaml/Rust
bridge use JSON for every message. Applications can define another payload
codec with `Temporal.Codec.make` when the value needs a different encoding.

## Handlers and ordinary helper functions

Handlers are ordinary OCaml functions returning `result` values. They can be
wrapped by normal helpers without a registration-specific class or callback
interface:

```ocaml
let approvals = ref []

let approve state value =
  state := value :: !state;
  Ok ()

let approval_handler =
  Temporal.Signal.Handler.make approval (approve approvals)

let status_handler =
  Temporal.Query.Handler.make status (fun () ->
      Ok (String.concat "," !approvals))

let non_empty_tool_handler update run =
  Temporal.Update.Handler.make
    ~validator:(fun value ->
      if String.equal value "" then
        Error (Temporal.Error.defect ~message:"tool name is empty")
      else Ok ())
    update run

let add_tool_handler =
  non_empty_tool_handler add_tool (fun value ->
    Ok (String.uppercase_ascii value))
```

To deliver a signal through a native workflow worker, attach its handler when
registering the workflow:

```ocaml
let workflow =
  Temporal.Workflow.define ~name:"agent" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () -> Ok "ready")

let registered =
  Temporal.Worker.workflow
    ~signals:[ approval_handler ]
    ~queries:[ status_handler ]
    ~updates:[ add_tool_handler ]
    workflow
```

The same `Temporal.Worker.workflow` registration accepts all three handler
lists independently. Each list is optional, and a handler is private to this
workflow registration; a name is not made available to every workflow in the
worker. The execution mode depends on the interaction kind:

- A matching signal activation is queued on the workflow's owner scheduler,
  never called from Rust or a native callback thread. Signal handlers return
  `(unit, Temporal.Error.t) result` and can call deterministic workflow
  helpers, including operations that suspend. Their commands are included in
  the containing workflow-task completion.
- A query handler is invoked synchronously on the execution owner Domain with
  that execution's context installed, so it can read execution-local state
  through `Temporal.Workflow_context.Local.get`. It does not enter the
  workflow scheduler, and its temporary mode disables the deterministic random
  stream; handlers must remain read-only and non-suspending. They cannot await
  a future, schedule an activity, or mutate durable workflow state. Their
  encoded result is returned as the query response rather than as a
  workflow-task command.
- An update handler currently has one input. Its validator runs before the
  implementation for a live request and is skipped when Core replays an
  already-validated update. The runtime emits acceptance before a handler can
  suspend, then retains only the suspended continuation keyed by Core's
  `protocol_instance_id` until a later activation completes it.

The `ref` above is deliberately small synthetic-test state. It demonstrates
that a handler can close over ordinary OCaml values; it is not a substitute
for replay-safe workflow state and it is not synchronized for concurrent
Domains. Keep a dispatcher and its callback-owned mutable state on one owning
Domain until native worker scheduling supplies the corresponding ownership
boundary.

For state that belongs to each workflow execution, use
`Temporal.Workflow_context.Local` instead of a module-level mutable value:

```ocaml
let approval_state = Temporal.Workflow_context.Local.create ()

let approval_handler =
  Temporal.Signal.Handler.make approval (fun value ->
    Temporal.Workflow_context.Local.set approval_state value)
```

The key is created alongside the workflow definition, but its value is stored
in the current execution context. `Local.get` and `Local.set` return typed
errors when called outside workflow execution. Values must remain deterministic
and replay-safe: do not use a local slot to hide wall-clock reads, randomness,
I/O, or process-global state.

The definition and callback are existentially paired inside each handler. A
registry can therefore store handlers for different OCaml types without an
unsafe cast, while each handler still decodes with the codec that belongs to
its definition.

## Dispatch order and results

The local dispatcher follows a fixed sequence. Native signal delivery preserves
the same name lookup, codec, callback, and typed-error rules while adding
scheduler ownership:

| Operation | Local sequence | Result |
| --- | --- | --- |
| Signal | Encode input, find the named handler, decode input, invoke the callback | `(unit, Error.t) result` |
| Query | Find the named handler, invoke its read-only callback, encode its output, decode it with the caller's definition | `('output, Error.t) result` |
| Update | Encode input, find the named handler, decode input, run the validator, invoke the implementation, encode output, decode it with the caller's definition | `('output, Error.t) result` |

An update validator runs after input decoding and before the implementation. A
validator that returns `Error` prevents the implementation from running, so
the rejected request cannot perform an update-side state change. Validators
are intended to be read-only and non-suspending. Query callbacks have the same
read-only, non-suspending contract. The dispatcher cannot make a callback safe
by blocking an OS thread or by silently moving it to another scheduler.

Expected operational failures are returned as `Error error` values. Callback
exceptions are caught at the handler boundary and converted to a non-retryable
`Defect`; they must not escape through the dispatch loop.

## Building and calling a dispatcher

`Interaction.create` copies each handler list into a persistent map. It either
returns a complete dispatcher or returns an error; it never returns a partial
registry:

```ocaml
let run_local_test () =
  match Temporal.Interaction.create
          ~signals:[ approval_handler ]
          ~queries:[ status_handler ]
          ~updates:[ add_tool_handler ]
          () with
  | Error error -> Error error
  | Ok interactions ->
      let accepted = Temporal.Interaction.signal interactions approval "yes" in
      let current = Temporal.Interaction.query interactions status in
      let changed = Temporal.Interaction.update interactions add_tool "search" in
      Ok (accepted, current, changed)
```

In a real test, match each result and assert the expected value or inspect
`Temporal.Error.view error`. The example returns results rather than using
`failwith`, because routine interaction failures belong in the typed error
channel. Exceptions are reserved for programmer defects and violated internal
invariants.

Registration and routing have these rules:

- Names must be unique within signals, within queries, and within updates.
  The same string may be used once in each kind because Temporal treats the
  three kinds as separate namespaces.
- A call made with a definition whose name has no local handler returns a
  non-retryable `Workflow` error for signals and queries, or an `Update` error
  for updates.
- A name match with incompatible encoding metadata returns a typed `Codec`
  error before the callback is invoked.
- A duplicate registration returns a non-retryable `Defect` from
  `Interaction.create`.
- A validator's own `Error` is returned unchanged and its implementation is
  not called. A callback exception is translated to a non-retryable `Defect`.

Calls on one owning Domain are synchronous and are observed in submission
order. `Interaction` does not add a hidden queue, background thread, fiber, or
lock. Its maps are immutable after construction, but callback-owned mutable
state is not synchronized; do not invoke one dispatcher concurrently from
multiple Domains. This local module is a deterministic routing primitive, not
the SDK's supervisor actor or a replacement for the future workflow scheduler.

## Native delivery boundary

The first native signal-handler slice is implemented. `Worker.workflow` carries
its handler list into the private runtime registration, and each
`SignalWorkflow` activation is validated before the matching callback is queued
on the execution scheduler. The handler sees exactly one decoded payload in the
public API; malformed arity, a missing name, a codec failure, or a callback
error produces a typed non-retryable workflow failure. The handler's ordinary
workflow commands are returned in that activation's completion.

The supervisor remains the sole owner of the Rust handle graph, and native
readiness is observed through its scheduler-safe boundary. Rust never calls an
OCaml closure. The focused runtime tests prove scheduler delivery, metadata
retention, and fail-closed handling. PR #266 established the first focused
live signal/condition acceptance, and the recorded seventeen-result baseline
is covered by the [PR #289 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29339077368).
Native query delivery now uses the same private registration path:
query IDs, repeated arguments, and headers are retained, handlers run
synchronously on the owner Domain, and argument arity is checked by the
registered output-only or typed handler rather than discarded. The remaining native
interaction work is:

- live acceptance scenarios for update handlers that suspend, including
  recovery and shutdown/eviction cleanup;
- query deadlines and query behavior across replay or cache eviction;
- Docker Compose acceptance scenarios for updates, including workflow-side
  assertions through Temporal Server.

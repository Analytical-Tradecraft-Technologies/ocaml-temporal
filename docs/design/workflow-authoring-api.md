# Phase 1 Workflow Authoring API Design

## Purpose and scope

This phase makes the existing direct-style workflow surface complete enough to
express the essential orchestration shapes: schedule activities and child
workflows, start durable timers, start several operations before waiting, await
one or all, and select the first completed operation. It extends the existing
`Activity`, `Workflow`, and `Future` modules instead of introducing a second
authoring abstraction.

This phase exercises the in-memory activation interpreter. It does not add
Temporal Core polling or Protobuf translation. Cancellation, retries, command
options, and structured concurrency remain incomplete parity work; they are not
excluded from the eventual SDK.

## Public OCaml surface

The existing `Activity.start`, `Activity.execute`, `Future.await`, and
`Workflow.sleep` functions remain the basic vocabulary. This phase adds:

```ocaml
module Child_workflow : sig
  val start :
    id:string ->
    ('input, 'output) Workflow.t ->
    'input ->
    ('output, Error.t) Future.t

  val execute :
    id:string ->
    ('input, 'output) Workflow.t ->
    'input ->
    ('output, Error.t) result
end

module Workflow : sig
  val start_sleep : Duration.t -> (unit, Error.t) Future.t
  val sleep : Duration.t -> (unit, Error.t) result
end

module Future : sig
  type ('left, 'right) race = Left of 'left | Right of 'right

  val all :
    ('value, Error.t) t list ->
    ('value list, Error.t) t

  val race :
    ('left, Error.t) t ->
    ('right, Error.t) t ->
    (('left, 'right) race, Error.t) t

  val first :
    ('value, Error.t) t ->
    ('value, Error.t) t list ->
    ('value, Error.t) t
end
```

`first` takes a mandatory first argument, so an empty race is unrepresentable.
`all []` is immediately successful. When called during workflow execution, its
ready future belongs to that execution so it can be combined with a timer,
activity, or child without a false ownership defect. The public aggregation
functions use the SDK's structured `Error.t`; this lets ownership mistakes be
returned as typed defects instead of raising an exception.

Child workflow IDs are explicit because Temporal records them as durable
identity and Temporal Core requires one when starting a child. An ID must be
non-empty, valid UTF-8, and at most 65,536 UTF-8 bytes so it fits the strict
bridge string boundary. Invalid identity produces a typed failed future (or
failed `execute` result) before input encoding, sequence allocation, or command
emission. Rich child options remain later parity work; the SDK does not invent
process-local `child-N` identifiers that could collide with durable history.

## Completion and ordering semantics

`race` and `first` settle on the first completion, whether it is `Ok` or
`Error`. They do not cancel losing operations. If inputs are already complete
when the aggregate is created, argument and observer-registration order decides
the winner. Otherwise, the deterministic scheduler's FIFO callback order
decides the winner. Later callbacks observe that the aggregate is settled and
do nothing.

`all` observes every input until all have completed. Successful values retain
input order, independent of completion order. If any input fails, `all` still
waits for every sibling and then returns the first error in input order.

All inputs to `both`, `all`, `race`, and `first` must belong to the same
workflow execution. A mismatch produces an already-completed future containing
a structured defect owned by the first input's scheduler. It is an API error
value, not routine control flow through an exception. Impossible internal
double-resolution remains an internal invariant defect and is contained at the
runtime boundary.

## Runtime boundary and ownership

Effect constructors, continuations, mutable resolver tables, and sequence
numbers remain private. Child workflows receive monotonically increasing
command sequence numbers from the same per-execution context as activities and
timers. The context owns exactly one resolver for each pending child and removes
it before resolving, which rejects unknown or duplicate completion jobs without
double-resolving a future.

`Workflow.start_sleep` records a timer and returns immediately. `Workflow.sleep`
is exactly `start_sleep` followed by `Future.await`. A zero duration produces an
already-ready future and no history command.

## Ordinary helper composition

SDK operations remain ordinary OCaml functions, so application helpers need no
registration or framework type:

```ocaml
let fastest left right input =
  Temporal.Future.race (left input) (right input)
  |> Temporal.Future.await

let summarize_and_review document =
  let open Temporal.Result_syntax in
  let summary = Temporal.Activity.start summarize document in
  let review =
    Temporal.Child_workflow.start ~id:"document-review" review_workflow document
  in
  let* summary, review = Temporal.Future.await (Temporal.Future.both summary review) in
  Ok (summary, review)
```

## Determinism and replay contract

Scheduling order is history-visible. Workflow code must make the same SDK calls
in the same order during replay. It must not use wall-clock time, random values,
network or filesystem I/O, process environment changes, cross-Domain races, or
unordered mutable global state to choose commands. Non-deterministic work
belongs in activities until replay-safe SDK alternatives exist.

Starting several Temporal operations before awaiting them is deterministic
concurrency: commands are emitted in call order, while completion order comes
from recorded workflow history. The SDK never serializes OCaml continuations;
replay reconstructs the same suspension points from history.

## Verification

Tests must prove explicit child ID emission and empty-ID rejection, child
command and result decoding, zero and nonzero timer behavior, command emission
order, input-order `all`, deterministic ready and pending `race`/`first`, first
completion errors, no implicit loser cancellation, typed cross-scheduler
failures for every aggregate including `both`, and helper-function composition.
Existing scheduler, activation, format, lint, Rust, and installation tests
remain regression gates.

# Deterministic workflow time

`Temporal.Workflow.now ()` returns the timestamp that Temporal attached to the
activation currently executing a workflow. It is an exact pair of signed Unix
seconds and a normalized nanosecond fraction:

```ocaml
let workflow_timestamp () =
  match Temporal.Workflow.now () with
  | Ok instant ->
      Ok (Printf.sprintf "%Ld.%09d"
        (Temporal.Time.seconds instant)
        (Temporal.Time.nanoseconds instant))
  | Error error ->
      (* Return the typed defect to the caller; report it outside the workflow. *)
      Error error
```

Formatting a value into a string is deterministic, but printing or logging is
I/O. Keep that reporting outside workflow execution: have the workflow return
the formatted value (or its typed error), then let the worker or application
boundary decide how to display it. This keeps the example compatible with
replay as well as live execution.

The value is supplied by the activation protocol and is installed before the
workflow function runs. Live execution and replay therefore observe the same
clock value for the same history event. The OCaml implementation does not call
`Unix.time`, inspect the host timezone, or use floating-point conversion.

`Temporal.Time.of_unix` can validate an application-provided timestamp when a
helper needs to compare or store one. Its nanosecond component must be in the
range `0` through `999_999_999`; invalid values return a typed non-retryable
`Temporal.Error.t` with category `` `Defect `` rather than raising.
`Temporal.Time.compare` and `Temporal.Time.equal` compare the integer
representation exactly.

## Time and duration arithmetic

`Temporal.Time.add instant duration` moves an instant forward by a
`Temporal.Duration.t`, and `Temporal.Time.diff later earlier` returns the
elapsed `Duration.t` between two instants, truncated to whole milliseconds.
`Temporal.Duration` adds `of_days` (a fixed 86,400 seconds, never a calendar
day), `add`, `compare`, `to_seconds` (truncating), and `maximum`. All of them
are pure integer arithmetic on replay-stable values, so they are safe in
workflow code:

```ocaml
(* Sleeps until 24 hours after the run started, or not at all if that
   moment has already passed. *)
let sleep_until_one_day_after_start () =
  let open Temporal.Result_syntax in
  let* info = Temporal.Workflow.info () in
  let* now = Temporal.Workflow.now () in
  match Temporal.Workflow.Info.start_time info with
  | None -> Ok ()
  | Some start ->
      let deadline = Temporal.Time.add start (Temporal.Duration.of_hours 24L) in
      Temporal.Workflow.sleep (Temporal.Time.diff deadline now)

(* True once seven days have elapsed since [since]. *)
let week_elapsed ~since =
  let open Temporal.Result_syntax in
  let* now = Temporal.Workflow.now () in
  Ok
    (Temporal.Duration.compare
       (Temporal.Time.diff now since)
       (Temporal.Duration.of_days 7L)
     >= 0)
```

The arithmetic is total and saturating (#1145). `Duration.add` stops at
`Duration.maximum`, the protobuf limit of 10,000 years that every SDK
duration already respects; `Time.add` stops at the last representable
instant (`Int64.max_int` seconds); and `Time.diff` is zero when `later` is
not after `earlier` and stops at `Duration.maximum`. Saturation was chosen
over a `result` because overflow cannot happen for any value Temporal can
use: its timestamps end at year 9999 and its durations at 10,000 years, so
a `result` would force every caller to handle an unreachable error, while
raising would turn a harmless bound into a workflow task failure. The
whole-unit constructors, including `of_days`, keep their existing contract
of raising `Invalid_argument` for a negative or out-of-range literal, since
that is a programming error in the caller's constants. Because `diff`
clamps at zero, use `Time.compare` when the order of two instants matters.

Outside workflow execution, or for a synthetic activation that has no Temporal
timestamp (such as cache eviction), `Temporal.Workflow.now ()` returns a typed
defect. It never silently falls back to local wall-clock time, because doing so
would make replay behavior depend on the worker host.

## Durable timers and cancellation

`Temporal.Workflow.start_sleep duration` emits one `Start_timer` command and
returns a future that resolves when Core delivers the matching firing job;
`Temporal.Workflow.sleep` starts and awaits it. A zero duration is ready at
once and records no timer. Both accept `~scope` (#1134): cancelling the scope
while the timer is pending emits one `Cancel_timer` (history records
`TimerCanceled`) and settles the timer future with a `Cancelled` error, while a
timer whose firing job was already applied, in the same or an earlier
activation, keeps its `Ok ()` and emits nothing. Core folds a cancellation
buffered in the same activation as the start into no history events at all,
and it never delivers a firing job for a cancelled timer. Unscoped sleeps emit
exactly the commands they did before, so histories recorded without timer
cancellation replay unchanged. See the [scope contract](workflow-scopes.md)
for the `Scope.with_scope` pattern that cancels the losing timer of a race.

`Temporal.Condition.wait_until ?timeout` and `wait_until_result ?timeout` use
the same scoped timer: the condition store's predicate also reports true once
the timer has fired, so the firing activation wakes the waiter; the
application predicate is then re-read, satisfaction wins over a timeout in the
same activation, and the private scope is cancelled so a satisfied wait emits
`Cancel_timer` for its still-pending timer. A timeout returns a `` `Timeout ``
error. A zero bound records no timer. The
[timer cancellation regression](../../test/integration/timer_cancellation/README.md)
checks both cancellation paths and a fired bound against a live server,
including replay after a worker replacement.

## Deterministic pseudo-random values

`Temporal.Workflow.random_int ~bound` draws an integer in `[0, bound)` from a
workflow-local stream seeded by Temporal when the run is initialized:

```ocaml
let choose_model models =
  match Temporal.Workflow.random_int ~bound:(List.length models) with
  | Ok index -> Ok (List.nth models index)
  | Error error -> Error error
```

The seed and generator state belong to one execution context. Replaying the
same history therefore produces the same sequence, while two workflow runs do
not share mutable random state. The implementation uses an integer-only
xorshift generator and rejection sampling; it never calls `Random`, reads host
entropy, or consults the wall clock. `bound` must be positive. The function
returns a typed defect for an invalid bound or when called outside workflow
execution.

Use this API only when a pseudo-random choice is part of deterministic workflow
logic. It does not make external randomness replay-safe: secrets, cryptographic
nonces, and entropy-dependent values belong in an activity whose result is
recorded by Temporal.

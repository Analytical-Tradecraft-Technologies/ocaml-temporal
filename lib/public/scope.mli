(** Workflow-local structured cancellation boundaries.

    A scope provides a deterministic cancellation signal for code that waits
    on public futures. Cancelling a scope wakes [await] callers and invokes
    every action registered with [on_cancel], allowing attached activity and
    child-workflow handles to emit their real Temporal cancellation commands.
    Every operation on a scope is owner-checked, so a handle cannot be read or
    mutated from another Domain or after its workflow scheduler has shut down. *)
type t

(** Creates a scope for the workflow execution currently running on this
    Domain. Outside workflow execution this returns a typed defect. *)
val create : unit -> (t, Error.t) result

(** Creates a scope, runs [body], and cancels the scope during cleanup. The
    body should use [await] for every operation whose observation belongs to
    this scope. When the body succeeds, a typed cancellation-hook failure is
    returned instead of the body's value. A body error or unexpected exception
    remains primary after cleanup is attempted. *)
val with_scope :
  (t -> ('value, Error.t) result) ->
  ('value, Error.t) result

(** Requests cancellation of [scope]. The operation is idempotent. It must be
    called by the owning workflow scheduler while the scope is active; an
    active scope called between scheduler runs, from another Domain, or after
    scheduler shutdown returns a typed defect. Repeating cancellation after
    it has already been requested is [Ok ()] when called by that scheduler. *)
val cancel : t -> (unit, Error.t) result

(** Registers one action to run when [scope] is cancelled. Actions are invoked
    once, in registration order, on the owning workflow scheduler. A callback
    should request cancellation of one or more Temporal operations and return
    [Ok ()] after the command has been buffered. All callbacks are attempted;
    if several fail, [cancel] returns the first structured error after the
    scope's own cancellation signal has still been delivered. Registering on
    an already-cancelled scope runs the action immediately. The callback must
    not block, perform nondeterministic I/O, or retain the scope indefinitely. *)
val on_cancel : t -> (unit -> (unit, Error.t) result) -> (unit, Error.t) result

(** Reports whether cancellation has been requested for [scope]. The query is
    owner-checked just like [cancel], so a foreign Domain or a retained scope
    queried after scheduler shutdown receives a typed defect instead of racing
    the workflow's mutable state. *)
val is_cancelled : t -> (bool, Error.t) result

(** Checks the scope without waiting. An active scope returns [Ok ()]; a
    cancelled scope returns a typed [`Cancelled] error; and a foreign Domain
    or stale handle returns a typed ownership defect. *)
val check : t -> (unit, Error.t) result

(** Awaits [future] while observing [scope]. A cancellation requested before
    the future completes returns a typed [`Cancelled] error. Errors from the
    future itself pass through unchanged. The future must belong to the same
    workflow execution as the scope. *)
val await :
  t ->
  ('value, Error.t) Future.t ->
  ('value, Error.t) result

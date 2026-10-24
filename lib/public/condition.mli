(** Workflow-local waits for replay-safe OCaml state.

    A condition does not contact Temporal and does not add a command or history
    event.  The predicate is checked immediately and again after the owning
    activation has run its queued workflow work.  A false predicate suspends
    only the current workflow fiber; it never blocks an OS thread. *)

(** A predicate that can report an expected, typed failure while deciding
    whether the condition is satisfied.  It must be deterministic, quick, and
    non-suspending: use activities or ordinary futures for external work. *)
type predicate = unit -> (bool, Error.t) result

(** Waits until a pure boolean predicate is true.  Exceptions raised by the
    predicate are caught and returned as non-retryable [`Defect] errors.

    [timeout] bounds the wait with a durable workflow timer (#1134). If the
    timer fires while the predicate is still false, the result is a typed
    [`Timeout] error ("workflow condition timed out"); if the predicate is
    true when the waiter wakes, the result is [Ok ()] even if the timer fired
    in the same activation. A wait that is satisfied first cancels its timer,
    so history records [TimerCanceled] and no timer is left pending. A zero
    [timeout] records no timer: the predicate is checked once and a false
    predicate times out at once. Without [timeout] no timer is created and
    nothing is recorded in history. *)
val wait_until : ?timeout:Duration.t -> (unit -> bool) -> (unit, Error.t) result

(** Result-aware form of [wait_until], with the same [timeout].  [Error
    error] from the predicate is returned unchanged; an exception is
    converted to a typed non-retryable defect. *)
val wait_until_result :
  ?timeout:Duration.t -> predicate -> (unit, Error.t) result

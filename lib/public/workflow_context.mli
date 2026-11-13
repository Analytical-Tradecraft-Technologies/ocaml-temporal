(** Workflow-execution-scoped state for SDK helpers.

    Values stored here belong to one workflow execution and are discarded
    with it, which makes them safe to use from replayed workflow code. *)

(** Returns [true] only while the current Domain is running workflow code under
    an activation. It is intended for diagnostics and internal guard checks. *)
val is_active : unit -> bool

(** Workflow-local mutable values are scoped to one execution context. They
    are useful for deterministic state shared by a workflow body and its
    scheduler-owned interaction handlers; callers must still derive every
    value from workflow history and must not use them for wall-clock, random,
    I/O, or process-global state. *)
module Local : sig
  (** An opaque key whose value is independent for every workflow run. *)
  type 'a t

  (** Allocates a key. Create it once alongside a workflow definition so the
      workflow body and its registered handlers can use the same key. *)
  val create : unit -> 'a t

  (** Reads the current run's value, or [None] when that run has not set it. *)
  val get : 'a t -> ('a option, Error.t) result

  (** Sets the current run's value. Outside workflow execution, and in a
      read-only update validator or query handler, it returns a typed defect
      and leaves the value unchanged: replay skips validators and never runs
      queries, so a write there would exist on the live path only. *)
  val set : 'a t -> 'a -> (unit, Error.t) result
end

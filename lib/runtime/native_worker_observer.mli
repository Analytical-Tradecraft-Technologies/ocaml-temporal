(** Private, scoped construction seam for workflow metadata observers.

    The installed SDK supplies no callbacks. This module contains no environment
    parsing, artifact I/O, or scenario policy. It is not in [Temporal]'s public
    interface and cannot be imported by an installed-package consumer. *)

type t = {
  on_activation : (Native_worker_execution.activation_info -> unit) option;
  on_completion : (Native_worker_execution.activation_info -> unit) option;
}
(** Callbacks captured by one worker at construction. The adapter invokes them
    synchronously under its mutex on the polling Domain. Activation exceptions
    follow the existing typed failure-completion path; completion exceptions
    are contained after Core acknowledgement and cannot resubmit a lease.
    Callbacks must not re-enter worker operations or retain native resources. *)

(** Returns the calling thread's construction scope, empty for ordinary workers.
    Other systhreads and spawned Domains never inherit an injection. *)
val current : unit -> t

(** Supplies callbacks while [create] constructs one worker, restoring the
    preceding scope on success, error, or exception. The worker captures the
    callbacks independently of this scope; its adapter owns their lifetime.
    Nested scopes restore in stack order; both Domains and systhreads are isolated. *)
val with_callbacks : t -> (unit -> 'a) -> 'a

(** Thread-granular lane ownership for the private native worker. The
    interface documents the re-entrancy contract this supports. *)

(** One system thread. [Thread.id] is not documented as unique across Domains,
    so the owning Domain is part of the identity. *)
type identity = { domain : Domain.id; thread : int }

(** The current owner of each execution lane, or [None] outside a run. *)
type t = { run : identity option Atomic.t; activity : identity option Atomic.t }

(** Identifies the calling system thread. *)
let current () = { domain = Domain.self (); thread = Thread.id (Thread.self ()) }

(** Allocates empty owner slots. *)
let create () = { run = Atomic.make None; activity = Atomic.make None }

(** Publishes the calling thread as the workflow-lane owner. *)
let enter_run owner = Atomic.set owner.run (Some (current ()))

(** Publishes the calling thread as the activity-lane owner. *)
let enter_activity owner = Atomic.set owner.activity (Some (current ()))

(** Clears both owners once neither lane can execute user code. *)
let leave owner =
  Atomic.set owner.run None;
  Atomic.set owner.activity None

(** Clears only the workflow-lane slot, leaving a detached activity lane's
    identity published. *)
let leave_run owner = Atomic.set owner.run None

(** Whether a published slot names exactly [self]. *)
let owns self = function
  | None -> false
  | Some { domain; thread } ->
      (domain :> int) = (self.domain :> int) && thread = self.thread

(** Compares the calling thread with both published lane owners. *)
let is_execution_thread owner =
  let self = current () in
  owns self (Atomic.get owner.run) || owns self (Atomic.get owner.activity)

(** Thread-granular identity of a native worker's execution lanes.

    A native worker executes workflow activations on the system thread that
    called [run] and activity callbacks on the main thread of a dedicated
    activity Domain. Shutdown and a nested [run] must reject a call made from
    either of those exact threads, because such a call would wait on the run
    mutex that its own lane is holding. A different system thread is never
    re-entrant, even when it shares a Domain with a lane: it can block on the
    run mutex with the OCaml runtime lock released while the lane observes the
    stop flag and exits. Tracking only the Domain would reject that legitimate
    caller on every retry.

    Identities are published with atomics so any Domain or thread may query
    them without a lock. They own no native resource. *)

type t
(** The workflow-lane and activity-lane owner slots of one worker. Both slots
    are empty while no run loop is active. *)

val create : unit -> t
(** Allocates empty owner slots. *)

val enter_run : t -> unit
(** Records the calling thread as the workflow-lane owner. The caller must hold
    the worker run mutex until it calls [leave]. *)

val enter_activity : t -> unit
(** Records the calling thread as the activity-lane owner. The activity lane
    calls it before each poll, because a poll may execute an activity callback
    on that thread. *)

val leave : t -> unit
(** Clears both slots after the run loop has joined its activity Domain and
    before the run mutex is released. *)

val leave_run : t -> unit
(** Clears only the workflow-lane slot. A run loop that returns without
    joining a detached activity Domain (#495) calls this instead of [leave],
    so the callback still running on that Domain keeps being recognized as
    an execution thread. *)

val is_execution_thread : t -> bool
(** Returns [true] only when the calling system thread currently owns a lane,
    which is exactly the context in which a blocking lifecycle call would
    deadlock. A helper thread that a callback spawns and then joins cannot be
    distinguished from an unrelated caller; that remains a user-level
    deadlock outside this check. *)

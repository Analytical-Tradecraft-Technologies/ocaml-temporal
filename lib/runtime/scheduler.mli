(** Runs the lightweight OCaml fibers belonging to one workflow execution. It
    runs one fiber at a time in a fixed first-in, first-out order. *)
type t

(** The result after running everything currently able to run. [Blocked] means
    at least one future is still waiting for a later activation. [Complete]
    means there is no runnable or pending work. *)
type status = Complete | Failed of exn | Blocked

(** Internal non-local control flow used by a terminal workflow operation. The
    scheduler consumes it without treating it as an application defect. *)
type _ Effect.t += Abort_workflow : 'value Effect.t

(** Raised when a terminal abort settles the current fiber. User-code try/with
    wrappers must re-raise it so it is not reported as a defect. *)
exception Workflow_aborted

(** Creates an empty active scheduler with an identity distinct from every
    other scheduler in this process. *)
val create : unit -> t

(** Returns the process-local identity of this scheduler. *)
val id : t -> int

(** Reports whether this scheduler is in a [run] drain on its owner Domain. *)
val is_running : t -> bool

(** Reports whether this scheduler has not yet been shut down. *)
val is_active : t -> bool

(** Creates a pending future owned by this scheduler and the function that will
    provide its result. *)
val promise :
  t ->
  outside_error:(unit -> 'error) ->
  ('value, 'error) Future_store.t * ('value, 'error) Future_store.resolver

(** Adds a new workflow fiber after work already waiting in the queue. *)
val spawn : t -> (unit -> unit) -> unit

(** Runs queued fibers in order until none can continue. Returns the earliest
    uncaught exception as [Failed]. Calling [run] from inside itself raises
    [Invalid_argument]. *)
val run : t -> status

(** Runs the scheduler and returns a stable diagnostic label. *)
val run_label : t -> string

(** Permanently closes the scheduler and releases pending futures, paused
    fibers, and queued functions. Calling it more than once is safe. *)
val shutdown : t -> unit

(** Performs the private terminal control effect. The polymorphic result gives
    public terminal helpers a natural non-returning type. *)
val abort_workflow : unit -> 'value

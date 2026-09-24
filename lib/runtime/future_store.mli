(** The scheduler operations a future needs. Each future keeps this value so
    its callbacks are queued by the workflow execution that created it and can
    be cleaned up when that execution ends. *)
type owner

(** Builds the scheduler connection used by futures. [on_create] is called once
    for each new pending future; [on_settled] is called once when that future
    receives a result or is closed during shutdown. [callbacks_live] is
    separate from [is_running]: it permits immediate delivery for the inert
    owner used by already-resolved outside-workflow futures, while a scheduler
    can reject callbacks after shutdown even though its queue is still drained
    for continuation cleanup. [register_teardown] adds a shutdown cleanup and
    returns an idempotent function that removes it when the future settles. *)
val make_owner :
  id:int ->
  enqueue:((unit -> unit) -> unit) ->
  is_running:(unit -> bool) ->
  callbacks_live:(unit -> bool) ->
  on_create:(unit -> unit) ->
  on_settled:(unit -> unit) ->
  register_teardown:((unit -> unit) -> (unit -> unit)) ->
  owner

(** A result that starts pending and can become ready exactly once. *)
type ('value, 'error) t

(** A function that supplies a future's result. Calling it twice raises
    [Invalid_argument], because one Temporal operation cannot have two results. *)
type ('value, 'error) resolver = ('value, 'error) result -> unit

(** The result of a heterogeneous two-way race. *)
type ('left, 'right) race = Left of 'left | Right of 'right

(** Internal OCaml 5 effect used when [await] needs to pause a workflow fiber.
    Only [Scheduler] handles it; it is not exposed by the public SDK. *)
type _ Effect.t +=
  | Await : ('value, 'error) t -> ('value, 'error) result Effect.t

(** Internal control exception used only to release paused fibers during
    scheduler teardown. It is not a workflow defect; the scheduler must ignore
    it when discontinuing waiters. *)
exception Scheduler_shutdown

(** Creates a pending future and the function that will provide its result. It
    also registers cleanup for workflow completion, eviction, or shutdown. *)
val create :
  owner:owner ->
  outside_error:(unit -> 'error) ->
  ('value, 'error) t * ('value, 'error) resolver

(** Creates an already-completed future when no workflow scheduler is active,
    for example when input encoding fails before a command can be scheduled. *)
val resolved :
  outside_error:(unit -> 'error) ->
  ('value, 'error) result ->
  ('value, 'error) t

(** Returns the identity of the workflow scheduler that created the future. *)
val owner_id : ('value, 'error) t -> int

(** Extracts the owner's liveness predicate without retaining this future's
    result. Adapters must pass this callback directly, without wrapping a call
    that captures the source future. *)
val callback_liveness : ('value, 'error) t -> (unit -> bool)

(** Reports whether callbacks queued for the future's owner may still run. *)
val callbacks_live : ('value, 'error) t -> bool

(** Runs [action] with [id] published as the Domain-local current scheduler
    owner. The scheduler installs this around each fiber so [await] can reject
    foreign-owner futures even when another scheduler is running elsewhere. *)
val with_current_owner_id : int option -> (unit -> 'a) -> 'a

(** Reports whether this Domain is currently running scheduler [id]. Used by
    derived public futures to reject outside await without mutating foreign
    scheduler state. *)
val current_owner_matches : int -> bool

(** Queues [thunk] on the scheduler that owns [future]. The callback is never
    run inline for an active workflow, which keeps completion ordering
    deterministic. Partial application retains only the owner callback. *)
val enqueue : ('value, 'error) t -> (unit -> unit) -> unit

(** Returns the result when ready. If called by the future's active workflow
    scheduler, pauses the current fiber until the result arrives. Otherwise,
    returns the error produced by [outside_error]. *)
val await : ('value, 'error) t -> ('value, 'error) result

(** Saves the paused fiber so it can resume when the future completes. If the
    result arrived first, queues the fiber for immediate resumption. Only the
    scheduler's effect handler calls this function. *)
val add_waiter :
  ('value, 'error) t ->
  (('value, 'error) result, unit) Effect.Deep.continuation ->
  unit

(** Registers an observer that receives the future result through the owning
    scheduler. Ready and closed futures are delivered using the same scheduler
    queue, so callers never need to race a direct callback against completion.
    A callback still queued when the owner shuts down is skipped after runtime
    teardown; public derived wrappers do not receive a synthetic terminal
    result and must not schedule more work from a skipped callback. Observers
    are internal and should not capture longer-lived resources. *)
val observe :
  ('value, 'error) t -> (('value, 'error) result -> unit) -> unit

(** Suspends the current workflow fiber until [register] invokes its signal.
    The signal is single-use; duplicate calls are ignored. This is a
    scheduler-aware gate for internal combinators and never blocks an OS
    thread. Partial application extracts only the owner, so retaining the gate
    does not retain the source result. *)
val await_gate :
  ('value, 'error) t -> (((unit -> unit) -> unit) -> unit)

(** Creates a future that transforms a successful result without waiting. *)
val map : ('value -> 'mapped) -> ('value, 'error) t -> ('mapped, 'error) t

(** Creates a future that transforms an error without waiting. *)
val map_error :
  ('error -> 'mapped_error) ->
  ('value, 'error) t ->
  ('value, 'mapped_error) t

(** Completes after both inputs have results. If both fail, returns the left
    error. Both futures must belong to the same workflow scheduler. *)
val both :
  ownership_error:(unit -> 'error) ->
  ('left, 'error) t ->
  ('right, 'error) t ->
  ('left * 'right, 'error) t

(** Completes after every input. Successful values and a selected error retain
    input order. The empty list completes immediately with [[]]. *)
val all :
  ownership_error:(unit -> 'error) ->
  ('value, 'error) t list ->
  ('value list, 'error) t

(** Completes with the first observed result from two differently typed inputs.
    A completion error wins just like a successful value. *)
val race :
  ownership_error:(unit -> 'error) ->
  ('left, 'error) t ->
  ('right, 'error) t ->
  (('left, 'right) race, 'error) t

(** Completes with the first observed result from a non-empty homogeneous
    collection. It does not cancel or stop observing losing inputs. *)
val first :
  ownership_error:(unit -> 'error) ->
  ('value, 'error) t ->
  ('value, 'error) t list ->
  ('value, 'error) t

(** Reports whether a result is available without suspending. *)
val is_ready : ('value, 'error) t -> bool

(** Returns a ready result or [None]. A future closed during shutdown also
    returns [None], because it has no result that application code may use. *)
val peek : ('value, 'error) t -> ('value, 'error) result option

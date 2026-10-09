(** Opaque execution context supplied to one local activity attempt.

    The context deliberately lives in the private base library. The public
    [Temporal.Activity.Context] module exposes only safe operations; native
    task tokens and supervisor handles never become OCaml values. *)

(** The opaque context for one activity attempt. Public documentation renders
    it through its supported alias.

    @canonical Temporal.Activity.context *)
type t

(** An exact protobuf timestamp copied from the activity task. The adapter has
    already validated [nanoseconds] to lie in 0 inclusive to 1_000_000_000
    exclusive. *)
type timestamp = { seconds : int64; nanoseconds : int }

(** Immutable identity and scheduling facts Temporal Core delivered with one
    activity attempt. [workflow_id], [workflow_run_id], and [workflow_type]
    are empty for a standalone activity that no workflow scheduled. An absent
    timestamp means that Core omitted the protobuf field.

    The three timeouts are the effective values Core resolved for the task,
    or [None] when Core omitted them. They are converted to whole
    milliseconds by rounding any sub-millisecond remainder up, so a positive
    timeout never reads as zero and conversion can never reject a task.
    [task_heartbeat_timeout] is named apart from the context's validated
    heartbeat interval, which rejects sub-millisecond values. *)
type info = {
  namespace : string;
  workflow_id : string;
  workflow_run_id : string;
  workflow_type : string;
  activity_id : string;
  activity_type : string;
  attempt : int;
  is_local : bool;
  scheduled_time : timestamp option;
  current_attempt_scheduled_time : timestamp option;
  started_time : timestamp option;
  schedule_to_close_timeout : Duration.t option;
  start_to_close_timeout : Duration.t option;
  task_heartbeat_timeout : Duration.t option;
}

(** Why Temporal Core asked a running activity attempt to stop. The
    constructors mirror Core's [ActivityCancelReason] one to one.

    @canonical Temporal.Activity.Cancellation.reason *)
type cancel_reason =
  | Requested
      (** Workflow code or a client requested cancellation of the activity. *)
  | Timed_out
      (** Temporal timed out the attempt, for example a missed heartbeat. *)
  | Not_found
      (** The server no longer knows the attempt: it already completed, timed
          out, or was retried elsewhere. *)
  | Worker_shutdown  (** Core is shutting the worker down. *)
  | Paused  (** An operator paused the activity. *)
  | Reset  (** An operator reset the activity. *)

(** One observed cancellation request. [reason] is Core's primary reason and
    [reasons] lists every independent fact Core reported, primary first and
    without duplicates. The value is immutable. *)
type cancellation = { reason : cancel_reason; reasons : cancel_reason list }

(** A single-assignment cell through which the native adapter publishes a
    cancellation to the context of the attempt it is executing.

    Ownership and threading: the adapter that runs the attempt creates the
    cell and is its only writer; it writes from whichever Domain delivered the
    Core cancellation task (the activity executor Domain, or a Domain on
    which the callback itself called heartbeat). Any Domain may read it. The
    cell is an [Atomic.t], so a read never blocks and never observes a
    partially written value, and the first published cancellation wins. *)
type cancellation_signal

(** Allocates an empty cancellation cell for one activity attempt. *)
val cancellation_signal : unit -> cancellation_signal

(** Publishes [cancellation] if the cell is still empty and returns [true];
    returns [false] and keeps the first value when one was already
    published. *)
val signal_cancellation : cancellation_signal -> cancellation -> bool

(** Returns the cancellation published to the cell, if any. *)
val observed_cancellation : cancellation_signal -> cancellation option

(** Creates an active context without Core task metadata. Test doubles and
    synthetic contexts use it; {!val-info} then returns [None]. The callback
    receives owned payload copies. The context is never cancelled and never
    reports a worker shutdown. *)
val create :
  heartbeat:(Payload.t list -> (unit, Error.t) result) ->
  details:Payload.t list ->
  heartbeat_timeout:Duration.t option ->
  t

(** Creates an active context after the native adapter has validated all
    values received from Temporal Core, retaining [info] for {!val-info}. Like
    {!create}, it is never cancelled and never reports a worker shutdown; the
    in-process test backend uses it. *)
val create_with_info :
  info:info ->
  heartbeat:(Payload.t list -> (unit, Error.t) result) ->
  details:Payload.t list ->
  heartbeat_timeout:Duration.t option ->
  t

(** Creates the context of one native worker attempt. It is {!create_with_info}
    plus the two cooperative stop signals: [cancellation] is the cell the
    adapter publishes Core cancellations to, and [worker_shutting_down]
    reports whether the owning worker has started to stop. The probe must be
    non-blocking and safe to call from any Domain. *)
val create_for_task :
  cancellation:cancellation_signal ->
  worker_shutting_down:(unit -> bool) ->
  info:info ->
  heartbeat:(Payload.t list -> (unit, Error.t) result) ->
  details:Payload.t list ->
  heartbeat_timeout:Duration.t option ->
  t

(** Creates a context for a backend that cannot submit native heartbeats. It
    carries no Core task metadata, is never cancelled, and never reports a
    worker shutdown. *)
val unavailable :
  details:Payload.t list -> heartbeat_timeout:Duration.t option -> t

(** Returns the task metadata supplied when the context was created, or [None]
    for a synthetic context. The record holds only immutable values, so it
    remains readable after invalidation. *)
val info : t -> info option

(** Submits copied heartbeat details while the attempt is active. Success does
    not change {!details}.

    The owner's heartbeat callback may also deliver a pending Core
    cancellation to this context. Once a cancellation has been observed,
    either before or during this call, the result is
    [Error (cancelled_error cancellation)] even though the details were
    submitted, so a callback that propagates heartbeat errors stops
    cooperatively. A context that is no longer active returns its lifecycle
    error instead. *)
val heartbeat : t -> Payload.t list -> (unit, Error.t) result

(** Returns the cancellation observed for this attempt, if any. It reads one
    atomic cell and never calls into the native runtime; cancellations reach
    the cell when the attempt heartbeats. Safe from any Domain and still
    readable after invalidation. *)
val cancellation : t -> cancellation option

(** Reports whether the owning worker has started to stop, by calling the
    probe supplied at construction. Safe from any Domain. *)
val worker_shutting_down : t -> bool

(** Builds the non-retryable [`Cancelled] error that reports [cancellation].
    An activity callback that returns an error in the [`Cancelled] category
    after a cancellation was observed completes the attempt as cancelled. *)
val cancelled_error : cancellation -> Error.t

(** Returns the stable lowercase label of a reason, as used in diagnostics and
    in the Temporal cancellation failure message. *)
val cancel_reason_label : cancel_reason -> string

(** Returns copied heartbeat details delivered with this task, i.e. the last
    details recorded by the previous attempt (empty on a first attempt). The
    value is stable for the whole attempt: heartbeats sent through {!heartbeat}
    are recorded for the next attempt and never change it. *)
val details : t -> Payload.t list

(** Returns the server-supplied heartbeat interval for this attempt. *)
val heartbeat_timeout : t -> Duration.t option

(** Invalidates the context after terminal completion or failure. The call
    waits for a callback already in flight before later calls fail. *)
val invalidate : t -> unit

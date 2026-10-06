(** Opaque execution context supplied to one local activity attempt.

    The context deliberately lives in the private base library. The public
    [Temporal.Activity.Context] module exposes only safe operations; native
    task tokens and supervisor handles never become OCaml values. *)
type t

(** An exact protobuf timestamp copied from the activity task. The adapter has
    already validated [nanoseconds] to lie in [0, 1_000_000_000). *)
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

(** Creates an active context without Core task metadata. Test doubles and
    synthetic contexts use it; {!info} then returns [None]. The callback
    receives owned payload copies. *)
val create :
  heartbeat:(Payload.t list -> (unit, Error.t) result) ->
  details:Payload.t list ->
  heartbeat_timeout:Duration.t option ->
  t

(** Creates an active context after the native adapter has validated all
    values received from Temporal Core, retaining [info] for {!info}. *)
val create_with_info :
  info:info ->
  heartbeat:(Payload.t list -> (unit, Error.t) result) ->
  details:Payload.t list ->
  heartbeat_timeout:Duration.t option ->
  t

(** Creates a context for a backend that cannot submit native heartbeats. It
    carries no Core task metadata. *)
val unavailable :
  details:Payload.t list -> heartbeat_timeout:Duration.t option -> t

(** Returns the task metadata supplied when the context was created, or [None]
    for a synthetic context. The record holds only immutable values, so it
    remains readable after invalidation. *)
val info : t -> info option

(** Submits copied heartbeat details while the attempt is active. Success does
    not change {!details}. *)
val heartbeat : t -> Payload.t list -> (unit, Error.t) result

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

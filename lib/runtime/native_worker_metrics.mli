(** SDK-side worker metrics (#500).

    Temporal Core instruments its workers itself (polls, slots, task
    latency, failures). A few worker states exist only in this SDK, and this
    module records them into the runtime's Prometheus registry through the
    bridge's metric meter, next to Core's own series:

    - activity-lane occupancy and callback duration;
    - activity tasks admitted to a callback, and tasks the SDK refused;
    - asynchronous activity operations the bridge did not admit because its
      client-call registry was full (#1104);
    - retained activity completions retried after a retryable status;
    - asynchronous activity leases expired by the adapter (#1043);
    - activity lanes detached after the grace period, and the outcome of
      every bounded worker shutdown (#495).

    Names and units are documented in [docs/reference/observability.md].
    Every series carries only [namespace] and [task_queue] (both fixed per
    worker) and, where stated, one label from a closed enumeration below.
    Workflow, run and activity IDs, activity types (a refused task's type
    is chosen by whoever scheduled it, so it is unbounded), and error text
    are never labels.

    Recording is a no-op on {!disabled}, which a worker without a metrics
    exporter uses, so the cost when metrics are off is one pattern match.
    When enabled, recording calls one allocation-free bridge function that
    never blocks on I/O. Every function is safe from any Domain or thread,
    never raises, and never affects worker behavior: a recording the bridge
    refuses is dropped. *)

(** The recorder of one worker. Immutable; share it freely. *)
type t

(** Records nothing. *)
val disabled : t

(** [create meter ~namespace ~task_queue] registers this worker's series on
    [meter]. Workers on the same runtime, namespace and task queue share
    their series, so their values add up. A registration the bridge refuses
    returns its error; callers fall back to {!disabled}. *)
val create :
  Temporal_core_bridge.Native_bridge.metric_meter ->
  namespace:string ->
  task_queue:string ->
  (t, Temporal_core_bridge.Native_bridge.error) result

(** Whether recording reaches a meter. *)
val enabled : t -> bool

(** Why the SDK refused an activity task (label [reason]). *)
type refusal =
  | Unrunnable
      (** [unrunnable]: unregistered activity type or an input this build
          cannot decode; failed retryably so another worker may run it. *)
  | Callback_raised  (** [callback_raised]: the callback raised. *)
  | Invalid
      (** [invalid]: an output codec or completion validation error, an
          unrepresentable context, or a protocol defect. *)

(** Which asynchronous activity operation was not admitted (label
    [operation]). *)
type async_operation = Heartbeat | Completion

(** How a bounded worker shutdown ended (label [outcome]). *)
type shutdown_outcome =
  | Clean  (** [clean]: lanes stopped and teardown completed. *)
  | Abandoned_work
      (** [abandoned_work]: a lane was abandoned at the end of the grace
          period; teardown completed. *)
  | Teardown_detached
      (** [teardown_detached]: teardown outlived the teardown timeout and
          continues in the background. *)
  | Completion_lost
      (** [completion_lost]: a retained completion could not be delivered. *)
  | Release_failed  (** [release_failed]: the native release reported an error. *)
  | Release_unproven
      (** [release_unproven]: the release raised; a cleanup retry was
          scheduled. *)

(** What a shutdown abandoned (label [kind]). *)
type abandoned_work = Activity_callback | Workflow_activation | Native_call

(** An activity callback was dispatched: counts one admitted task and
    raises the occupancy gauge. Pair with {!activity_callback_finished}. *)
val activity_callback_started : t -> unit

(** The callback started [elapsed_ns] nanoseconds ago returned or raised:
    lowers the occupancy gauge and observes its duration. *)
val activity_callback_finished : t -> elapsed_ns:int64 -> unit

(** The SDK refused an activity task and acknowledged it with a failure. *)
val activity_task_refused : t -> refusal -> unit

(** The bridge refused to admit an asynchronous activity operation because
    its client-call registry was full; the caller may retry. *)
val async_activity_not_admitted : t -> async_operation -> unit

(** The adapter closed an asynchronous lease the server must already have
    timed out. *)
val async_activity_lease_expired : t -> unit

(** Submission of a retained activity completion returned the retryable
    status; it will be resubmitted unchanged. *)
val activity_completion_retry : t -> unit

(** A run returned without joining its activity lane because a callback
    outlived the grace period. *)
val activity_lane_detached : t -> unit

(** One bounded shutdown finished with [outcome]. *)
val worker_shutdown : t -> shutdown_outcome -> unit

(** One bounded shutdown abandoned work of this kind. *)
val worker_shutdown_abandoned : t -> abandoned_work -> unit

(** Classifies one bounded shutdown report: its [outcome] label and every
    kind of work it abandoned. [failure], the outcome of a shutdown that
    returned an error, takes precedence; otherwise a detached teardown is
    [Teardown_detached], an abandoned lane [Abandoned_work], and anything
    else [Clean]. When a lane was abandoned, each kind with a positive count
    is listed (both when both lanes outlived the grace period); a lane
    blocked in a native call is counted in neither, so [Native_call] is
    listed only when no count is positive. Pure. *)
val classify_shutdown :
  ?failure:shutdown_outcome ->
  Native_worker_shutdown.report ->
  shutdown_outcome * abandoned_work list

(** Records one shutdown as classified by {!classify_shutdown}: one
    [outcome] increment and one increment per abandoned kind. *)
val record_shutdown :
  t -> ?failure:shutdown_outcome -> Native_worker_shutdown.report -> unit

(** The [kind] label value of an abandoned-work kind, also used as the
    [temporal.error_kind] of the matching lifecycle record. *)
val abandoned_work_label : abandoned_work -> string

(** Exported metric names (with Core's [temporal_] prefix), for tests and
    documentation checks. *)
val metric_names : string list

(** The type of an OCaml function that implements an activity. It receives the
    decoded input and returns either its output or a structured error. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** Opaque context for one activity attempt. Calls after terminal completion
    return typed errors rather than using a released task token. *)
type context = Temporal_base.Activity_context.t

(** Context-aware implementation form for activity progress and prior details. *)
type ('input, 'output) contextual_implementation =
  context -> 'input -> ('output, Error.t) result

(** Opaque capability retained by an asynchronous activity after it returns
    [Will_complete_async]. *)
type 'output async_handle =
  'output Temporal_base.Async_activity.handle

(** Attempt-scoped context used to obtain an asynchronous completion handle. *)
type 'output async_context =
  'output Temporal_base.Async_activity.context

(** Explicit callback outcome for asynchronous activities. *)
type 'output async_result =
  | Completed of 'output
  | Failed of Error.t
  | Will_complete_async of 'output async_handle

(** Callback form for an activity that may complete after worker dispatch has
    returned. *)
type ('input, 'output) async_implementation =
  'output async_context -> 'input -> 'output async_result

(** A description of an activity and the OCaml types it accepts and returns.
    The definition stores the activity's Temporal name and its input and output
    codecs. It contains either a plain local implementation, a context-aware
    implementation, or neither when it is a remote scheduling reference. *)
type ('input, 'output) t

(** Scheduling metadata for an activity task.  Temporal uses the lower
    positive [priority_key] first, then applies [fairness_key] and
    [fairness_weight] as best-effort queue fairness controls.  The value is
    immutable and validated before it can enter workflow history. *)
module Priority : sig
  (** Opaque, immutable priority configuration. *)
  type t

  (** Validates and constructs priority metadata.  At least one field must be
      supplied.  [priority_key] is a positive integer; [fairness_key] is at
      most 64 UTF-8 bytes; and [fairness_weight] is zero (the server default)
      or finite in [0.001, 1000.0]. *)
  val make :
    ?priority_key:int ->
    ?fairness_key:string ->
    ?fairness_weight:float ->
    unit ->
    (t, Error.t) result

  (** Alias for [make]. *)
  val create :
    ?priority_key:int ->
    ?fairness_key:string ->
    ?fairness_weight:float ->
    unit ->
    (t, Error.t) result

  (** Returns the configured priority key, if present. *)
  val priority_key : t -> int option

  (** Returns the configured fairness key, if present. *)
  val fairness_key : t -> string option

  (** Returns the configured fairness weight, if present. *)
  val fairness_weight : t -> float option
end

(** Creates a definition for an activity that this OCaml worker will run. The
    name is validated immediately; it must be non-empty, valid UTF-8, NUL-free,
    and no more than 65,536 bytes because it crosses the native protocol into
    Temporal history. Violations raise [Invalid_argument] as construction
    defects.

    Exceptions are treated as programmer defects. An exception escaping the
    implementation is reported as a {b non-retryable} Temporal application
    failure of type ["ocaml_exception"], so the activity's retry policy does
    not apply and the first raise fails the activity. Return an [Error] (for
    example a retryable [Error.make]) for expected or transient failures,
    including errors from I/O libraries that raise, such as [Unix.Unix_error].
    The failure's stack trace contains the OCaml backtrace when
    [Printexc.record_backtrace] is enabled. The same applies to local and
    asynchronous activity callbacks. *)
val define :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) implementation ->
  ('input, 'output) t

(** Creates a local activity that receives an opaque attempt context. *)
val define_with_context :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) contextual_implementation ->
  ('input, 'output) t

(** Creates an activity whose callback may return a retained completion
    handle. The callback must return exactly one of [Completed], [Failed], or
    [Will_complete_async]. Deferred completion is supported for remote
    activities only; a local activity returning [Will_complete_async] fails
    that attempt. A remote handle becomes usable only after the worker accepts
    the handoff. *)
val define_async :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) async_implementation ->
  ('input, 'output) t

(** Creates a typed reference to an activity run by another worker. The name
    has the same non-empty, valid UTF-8, NUL-free, 65,536-byte contract as
    [define]. The name and codecs must agree with that worker's activity
    definition; the returned value has no executable callback and must not be
    registered as local code. *)
val remote :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) t

(** Returns the activity type name sent to Temporal when it is scheduled. *)
val name : ('input, 'output) t -> string

(** Returns the codec used to decode activity inputs while keeping the activity
    definition itself opaque. *)
val input : ('input, 'output) t -> 'input Codec.t

(** Returns the codec used to encode successful activity outputs. *)
val output : ('input, 'output) t -> 'output Codec.t

(** Returns executable code for a local definition, or [None] for a remote
    reference. *)
val implementation :
  ('input, 'output) t -> ('input, 'output) implementation option

(** Returns the context-aware callback, if present. *)
val implementation_with_context :
  ('input, 'output) t ->
  ('input, 'output) contextual_implementation option

(** Returns the asynchronous callback, if present. *)
val implementation_async :
  ('input, 'output) t ->
  ('input, 'output) async_implementation option

(** Operations on the opaque asynchronous completion capability. The handle
    may be shared between Domains; one operation is in flight at a time, and a
    call that collides with another Domain's in-flight operation returns a
    retryable error.

    Until the worker has accepted the [Will_complete_async] handoff, every
    operation returns a retryable error and sends nothing, so external code
    that finishes before (or while) the handoff completes should retry the
    same call; the result is not lost. Do not retry from inside the activity
    callback itself: the handoff only starts after the callback returns. A
    handle that can never become active (the callback returned [Completed] or
    [Failed], raised, or the worker discarded the unaccepted handoff) returns
    a non-retryable error instead, so retry loops terminate.

    A local codec or payload-validation error leaves the handle active, so the
    caller may correct the data or choose a different operation. After
    submission:
    - A transient RPC failure (for example the server is unavailable) returns
      a retryable error ([Error.view]'s [non_retryable] is [false]) and keeps
      the handle live. For [complete], [fail], and [cancel], only the
      identical call may follow until it receives a definitive answer; a
      failed [heartbeat] is forgotten and never blocks later calls.
    - A definitive server rejection of the request (for example [cancel]
      without a cancellation request) returns a non-retryable error but keeps
      the handle live, so a corrected or different operation such as [fail]
      may follow. A result above the server's blob-size limit is not such a
      rejection: Temporal records a terminal failure for the activity and
      acknowledges the call, so [complete] returns [Ok] and closes the
      handle.
    - Acceptance of [complete], [fail], or [cancel], or the server reporting
      that the activity no longer exists, closes the handle. After an
      uncertain terminal call, "no longer exists" can mean that call was
      applied. *)
module Async_handle : sig
  (** The handle type paired with one asynchronous activity output. *)
  type 'output t = 'output async_handle

  (** Completes the activity with a typed output. *)
  val complete : 'output t -> 'output -> (unit, Error.t) result

  (** Fails the activity with a structured error. *)
  val fail : 'output t -> Error.t -> (unit, Error.t) result

  (** Reports cancellation and optional detail payloads. *)
  val cancel : 'output t -> Payload.t list -> (unit, Error.t) result

  (** Sends heartbeat detail payloads. *)
  val heartbeat : 'output t -> Payload.t list -> (unit, Error.t) result
end

(** Access to the capability carried by an asynchronous callback context. *)
module Async_context : sig
  (** The context type passed to an asynchronous implementation. *)
  type 'output t = 'output async_context

  (** Returns the opaque handle that can be retained after the callback. *)
  val handle : 'output t -> 'output Async_handle.t
end

(** Encodes and submits one typed heartbeat value for the current activity. The
    payload is copied before crossing into the private runtime, and stale or
    unavailable contexts return typed errors rather than retaining a released
    task token. *)
val heartbeat : context -> 'a Codec.t -> 'a -> (unit, Error.t) result

(** Read-only identity and scheduling facts for one activity attempt, copied
    from the task Temporal delivered. The type is abstract so later releases
    can add fields compatibly. A common use is an idempotency key for
    at-least-once side effects, for example combining {!Info.workflow},
    {!Info.activity_id}, and, when each retry must be distinct,
    {!Info.attempt}. *)
module Info : sig
  (** Metadata for one activity attempt. *)
  type t

  (** The workflow execution that scheduled this activity. *)
  type workflow = { workflow_id : string; run_id : string; workflow_type : string }

  (** Returns the Temporal namespace of the scheduling workflow. *)
  val namespace : t -> string

  (** Returns the workflow execution that scheduled this activity. Standalone
      activities (scheduled without a workflow) are not supported: the bridge
      fails such a task back to Temporal before any activity code runs, so
      every context that reaches an implementation has a scheduling
      workflow. *)
  val workflow : t -> workflow

  (** Returns the activity ID. Temporal keeps it stable across retries of the
      same scheduled activity. *)
  val activity_id : t -> string

  (** Returns the registered activity type name that this attempt runs. *)
  val activity_type : t -> string

  (** Returns the 1-based attempt number; retries increase it. *)
  val attempt : t -> int

  (** Returns [true] when Temporal Core is running this attempt as a local
      activity rather than through a task queue. *)
  val is_local : t -> bool

  (** Returns when the activity was first scheduled, if Core reported it. *)
  val scheduled_time : t -> Time.t option

  (** Returns when this attempt was scheduled, if Core reported it. It differs
      from {!scheduled_time} after a retry. *)
  val current_attempt_scheduled_time : t -> Time.t option

  (** Returns when the server recorded this attempt as started, if Core
      reported it. *)
  val started_time : t -> Time.t option
end

(** Operations available to a contextual activity attempt. *)
module Context : sig
  (** The attempt-scoped context passed to contextual activity helpers. *)
  type t = context

  (** Sends one typed heartbeat value and returns a typed error if this attempt
      is no longer active. *)
  val heartbeat : t -> 'a Codec.t -> 'a -> (unit, Error.t) result

  (** Sends already encoded detail payloads in order. *)
  val heartbeat_payloads : t -> Payload.t list -> (unit, Error.t) result

  (** Returns a copied list of the last heartbeat details recorded by the
      previous attempt of this activity, or [[]] on a first attempt or when
      the previous attempt never heartbeated. The value is fixed for the whole
      attempt: details sent with {!heartbeat} or {!heartbeat_payloads} are
      delivered to the next attempt and do not change what this attempt
      reads. *)
  val details : t -> Payload.t list

  (** Returns the configured heartbeat interval, if one was supplied. *)
  val heartbeat_timeout : t -> Duration.t option

  (** Returns this attempt's task metadata. Contexts created by a native worker
      always carry it; a context from a backend without a Temporal task, such
      as the in-process test backend, returns a typed [Defect] error. The value
      is immutable and remains readable after the attempt completes. *)
  val info : t -> (Info.t, Error.t) result
end

(** The cancellation policy sent with an activity command. [Try_cancel] asks
    the activity worker to stop when possible, [Wait_cancellation_completed]
    waits for acknowledgement, and [Abandon] leaves the activity running. The
    policy affects the parent future and is deterministic during replay. *)
type cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** An opaque typed activity operation. The handle keeps the result future and
    the owner-checked cancellation operation together; callers cannot forge the
    private runtime sequence used by the cancellation command. *)
type 'output handle

(** A deterministic retry policy for one scheduled activity.

    [initial_interval] and [maximum_interval] are positive whole-millisecond
    durations, with the maximum at least as large as the initial delay.
    [backoff_coefficient] must be finite and at least [1.0].  A
    [maximum_attempts] value of [0] means that Temporal imposes no attempt
    count limit; positive values include the initial attempt.
    [non_retryable_error_types] lists application error types that stop
    retries; Temporal matches them against the failure type set by
    [Error.make ~error_type] (or the category name, {!Error.kind}, for an
    untyped error).  The constructor returns a typed defect instead of
    raising so callers can validate policy configuration while assembling a
    workflow definition. *)
module Retry_policy : sig
  (** Opaque immutable retry policy validated before command construction. *)
  type t

  (** Validates and constructs an immutable retry policy. *)
  val make :
    initial_interval:Duration.t ->
    backoff_coefficient:float ->
    maximum_interval:Duration.t ->
    maximum_attempts:int ->
    ?non_retryable_error_types:string list ->
    unit ->
    (t, Error.t) result

  (** Alias for [make] for callers that prefer constructor terminology. *)
  val create :
    initial_interval:Duration.t ->
    backoff_coefficient:float ->
    maximum_interval:Duration.t ->
    maximum_attempts:int ->
    ?non_retryable_error_types:string list ->
    unit ->
    (t, Error.t) result

  (** Returns the exact initial retry delay. *)
  val initial_interval : t -> Duration.t

  (** Returns the finite multiplier applied between retry attempts. *)
  val backoff_coefficient : t -> float

  (** Returns the cap applied to the retry delay. *)
  val maximum_interval : t -> Duration.t

  (** Returns the maximum number of attempts; [0] means unlimited. *)
  val maximum_attempts : t -> int

  (** Returns the immutable list of Temporal error type names that must not be
      retried. *)
  val non_retryable_error_types : t -> string list
end

(** Schedules an activity and retains a handle for explicit cancellation. The
    command is emitted immediately and [future] remains pending until Core
    reports a terminal result. Invalid options or input encoding return a ready
    failed handle without emitting a command or consuming a sequence. The
    default policy is [Try_cancel]. When [scope] is supplied, cancellation of
    that scope emits the activity's Core cancellation command exactly once;
    the scope must belong to the same workflow execution. *)
val start_handle :
  ?scope:Scope.t ->
  ?activity_id:string ->
  ?task_queue:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?heartbeat_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  ?priority:Priority.t ->
  ?cancellation_type:cancellation_type ->
  ?do_not_eagerly_execute:bool ->
  ('input, 'output) t ->
  'input ->
  'output handle

(** Returns the typed future owned by an activity operation handle. *)
val future : 'output handle -> ('output, Error.t) Future.t

(** Requests cancellation of the exact activity represented by [handle].
    Repeated calls are idempotent, including a call after natural completion
    or activity-start failure. Calls made outside the owning workflow return a
    typed lifecycle error and emit no command. The underlying Temporal Core
    command identifies the activity by its private sequence and carries no
    user-supplied reason. *)
val cancel : 'output handle -> (unit, Error.t) result

(** Schedules the activity and returns immediately with a future for its
    result. Start several independent activities before awaiting them to let
    Temporal run them concurrently. Optional labels make the activity command
    explicit; omitted IDs are deterministic, omitted queues use the worker's
    queue, and omitted timeouts use a 60-second start-to-close timeout because
    Temporal requires at least one activity timeout. An explicit zero
    schedule-to-close or start-to-close timeout is an option error, because
    Temporal would treat it as unset. If input or option validation fails, the returned future contains a typed error and no command
    is emitted. [do_not_eagerly_execute] controls whether Core may run the
    activity inline with the scheduling activation; it defaults to [false]. If
    [scope] is supplied, cancelling the scope requests cancellation of this
    activity at the Temporal server as well as cancelling its local
    observation. *)
val start :
  ?scope:Scope.t ->
  ?activity_id:string ->
  ?task_queue:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?heartbeat_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  ?priority:Priority.t ->
  ?cancellation_type:cancellation_type ->
  ?do_not_eagerly_execute:bool ->
  ('input, 'output) t ->
  'input ->
  ('output, Error.t) Future.t

(** Schedules the activity and waits for its result. This is equivalent to
    calling [start] followed by [Future.await]. A supplied [scope] also
    attaches the activity's server-side cancellation command to that scope. *)
val execute :
  ?scope:Scope.t ->
  ?activity_id:string ->
  ?task_queue:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?heartbeat_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  ?priority:Priority.t ->
  ?cancellation_type:cancellation_type ->
  ?do_not_eagerly_execute:bool ->
  ('input, 'output) t -> 'input -> ('output, Error.t) result

(** Schedules the activity on Temporal Core's local activity lane. The
    callback executes in this worker process and Core records the result as a
    history marker. Local activities support activity IDs, retry policies, and
    local timeout controls, but do not accept a remote task queue, heartbeat,
    priority, or eager-execution option.

    [cancellation_type] defaults to [Wait_cancellation_completed], unlike the
    [Try_cancel] default of [start]. The policy is recorded with the schedule
    command, but this API returns only a future and accepts no [scope], so
    workflow code currently has no way to request cancellation of a local
    activity and the option has no observable effect. It is retained so a
    future local cancellation API can honour it without a signature change. *)
val start_local :
  ?activity_id:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  ?cancellation_type:cancellation_type ->
  ('input, 'output) t ->
  'input ->
  ('output, Error.t) Future.t

(** Schedules a local activity and waits for its result. It has the same
    contract as [start_local], including the [Wait_cancellation_completed]
    default for [cancellation_type] and the fact that the option currently has
    no observable effect, because no local activity can be cancelled. *)
val execute_local :
  ?activity_id:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  ?cancellation_type:cancellation_type ->
  ('input, 'output) t ->
  'input ->
  ('output, Error.t) result

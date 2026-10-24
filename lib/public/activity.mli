(** Typed activity definitions, worker-side activity contexts, and the
    workflow commands that schedule activities.

    An activity is ordinary OCaml code that may perform I/O; Temporal records
    only its input and result. Define an activity with {!define} (or a
    context-aware or asynchronous variant), register it with a worker, and
    call it from workflow code with {!execute} or {!execute_local}. Expected
    failures are returned as [result] values carrying {!Error.t}. *)

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
    asynchronous activity callbacks.

    A worker that receives a task it cannot run fails the attempt without
    calling any implementation: an activity type it does not register (an
    application failure of type ["NotFoundError"] naming the registered
    types, as in the other Temporal SDKs), or an input that [input] cannot
    decode. For an activity scheduled on a task queue the failure is
    retryable, so the activity's retry policy applies and a later attempt can
    reach a worker that has the type or a compatible codec, for example
    during a rolling deploy. A local activity always runs on the worker that
    scheduled it, so there the failure is non-retryable. *)
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
    - When the client the worker uses already has [max_in_flight_calls]
      calls in flight (see [Client.connect]), the call is refused
      before anything is sent. It returns a retryable [`Bridge] error whose
      message names [resource_exhausted], and keeps the handle live. Nothing
      reached the server, so retry the same call (or, if no earlier
      [complete], [fail], or [cancel] is still awaiting an answer, a
      different one) once other calls finish. The handle still expires as
      described below if it is never finished.
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
      applied.

    Every handle must eventually be completed, failed, or cancelled: until
    then the worker keeps a lease for it, and shutting the worker down waits
    for it for the whole graceful shutdown period and then reports it as
    lost. A handle that is never finished expires when Temporal must already
    have timed out its attempt (a few seconds after the attempt's
    start-to-close or schedule-to-close timeout, or its heartbeat timeout
    since the last {!Async_handle.heartbeat}); the worker then closes it
    without contacting the server, and later calls return a non-retryable
    error.

    Each operation is one request to the Temporal server, made while the
    calling thread waits for its answer (up to the client's RPC timeout and
    retries). The worker's own polling and completions do not wait for it,
    and operations on different handles proceed independently, but a call
    blocks its caller: do not call it from workflow code. *)
module Async_handle : sig
  (** The handle type paired with one asynchronous activity output. *)
  type 'output t = 'output async_handle

  (** Completes the activity with a typed output. *)
  val complete : 'output t -> 'output -> (unit, Error.t) result

  (** Fails the activity with a structured error. *)
  val fail : 'output t -> Error.t -> (unit, Error.t) result

  (** Reports cancellation and optional detail payloads. *)
  val cancel : 'output t -> Payload.t list -> (unit, Error.t) result

  (** Sends heartbeat detail payloads, throttled per activity the way
      Temporal Core throttles {!Context.heartbeat}. The first call is sent
      to the server at once and returns its result. Later calls within the
      throttle interval return at once without a server round trip: [Ok]
      then means the details were recorded locally, and only the latest
      details are sent when the interval expires. The interval is 80% of
      the activity's heartbeat timeout, or [default_heartbeat_throttle_interval]
      without one, capped by [max_heartbeat_throttle_interval] (see
      [Worker.Options]), so heartbeating per item of work is cheap and the
      server still hears from the activity within its heartbeat timeout.

      Each call reports the activity's state as of the latest send, which
      for a throttled call may be a send that happened after an earlier
      call returned. When Temporal has requested cancellation, the result
      is a non-retryable [Error] in the [`Cancelled] category; when the
      activity was paused or reset, it is a retryable [`Cancelled] error of
      type ["ActivityPause"] or ["ActivityReset"]. The heartbeat was still
      recorded and the handle stays live: stop the work and finish the
      handle with {!cancel}, or with {!fail} and that error. A failed send
      ends the throttle interval, so the next call is sent at once and
      returns the server's answer, for example a non-retryable error once
      the activity no longer exists. *)
  val heartbeat : 'output t -> Payload.t list -> (unit, Error.t) result
end

(** Read-only identity and scheduling facts for one activity attempt, copied
    from the task Temporal delivered. The type is abstract so later releases
    can add fields compatibly. A common use is an idempotency key for
    at-least-once side effects, for example combining {!Info.val-workflow},
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

  (** Returns the effective schedule-to-close timeout of the activity, if
      Temporal reported one. Temporal resolves defaults when the activity is
      scheduled, so this may differ from the option the workflow passed.
      Temporal reports an unset timeout as a zero duration; that is returned
      as [None], never as a zero [Duration.t], for every timeout here and for
      {!Context.heartbeat_timeout}. Temporal carries nanosecond precision; a sub-millisecond remainder is
      rounded up to the next whole millisecond, so a positive timeout is never
      reported as zero. The result is clamped to 315,576,000,000,999 ms, the
      largest value {!Duration.of_ms} accepts, so the protobuf maximum
      (which would round up past it) is reported as that bound. *)
  val schedule_to_close_timeout : t -> Duration.t option

  (** Returns the effective start-to-close timeout of this attempt, if
      Temporal reported one, rounded up to whole milliseconds like
      {!schedule_to_close_timeout}. *)
  val start_to_close_timeout : t -> Duration.t option

  (** Returns the heartbeat timeout Temporal reported for this attempt,
      rounded up to whole milliseconds like {!schedule_to_close_timeout}.
      Unlike {!Context.heartbeat_timeout}, it is also available to
      asynchronous activities. *)
  val heartbeat_timeout : t -> Duration.t option
end

(** Access to the capability carried by an asynchronous callback context. *)
module Async_context : sig
  (** The context type passed to an asynchronous implementation. *)
  type 'output t = 'output async_context

  (** Returns the opaque handle that can be retained after the callback. *)
  val handle : 'output t -> 'output Async_handle.t

  (** Returns this attempt's task metadata, as {!Context.info} does for a
      synchronous activity. Contexts created by a native worker always carry
      it; any other context returns a typed [Defect] error. The value is
      immutable, so external code may keep it after the callback returns, for
      example to log which activity a later completion belongs to. *)
  val info : 'output t -> (Info.t, Error.t) result
end

(** A request from Temporal that a running activity attempt stop.

    Cancellation is cooperative: the worker never interrupts activity code. A
    synchronous activity defined with {!define_with_context} observes it at
    its heartbeats, the same channel through which Temporal delivers it to the
    worker. Once a cancellation has been delivered, {!Context.cancellation}
    returns it and every later {!Context.heartbeat} returns an [Error] in the
    [`Cancelled] category. Returning such an error from the activity (for
    example by propagating the heartbeat's error, or [to_error]) completes the
    attempt as cancelled, which a workflow using [Wait_cancellation_completed]
    observes as a [`Cancelled] activity error. Returning [Ok] still completes
    the attempt successfully.

    An operator pause or reset ([Paused] or [Reset] as the primary
    {!Cancellation.type-reason}) is delivered the same way, but Temporal
    accepts a cancelled completion only after a cancellation request.
    Returning the [`Cancelled] error for a pause or reset therefore fails the
    attempt with a retryable application failure of type ["ActivityPause"] or
    ["ActivityReset"], as the Python SDK does: Temporal holds a paused activity's next attempt until it is unpaused,
    and schedules a reset activity afresh. When a cancellation was also
    requested, the primary reason is [Requested] and the attempt is
    cancelled.

    A cancellation can only reach an activity that heartbeats: Temporal
    returns it in the response to a heartbeat, and the worker delivers it
    when the activity next heartbeats. Configure a [heartbeat_timeout] when
    scheduling an activity that must react promptly.

    An asynchronous activity has no context to read. Its completer learns of
    the same requests from the result of {!Async_handle.heartbeat} and
    reports the outcome itself, with {!Async_handle.cancel} or
    {!Async_handle.fail}. It is not notified when its worker stops: shutdown
    waits for the handle for the graceful shutdown period and then closes it
    as a lost completion, after which its calls return a non-retryable error,
    and a handle that is never finished expires once Temporal must have timed
    its attempt out. Temporal then retries the attempt according to its
    heartbeat, start-to-close and schedule-to-close timeouts. The
    repository's [examples/cancellation] shows both forms releasing an
    external reservation idempotently on cancellation, and the synchronous
    form doing so on worker shutdown.

    {[
      let process_batch =
        Temporal.Activity.define_with_context ~name:"process_batch"
          ~input:Temporal.Codec.int ~output:Temporal.Codec.int
          (fun context count ->
            let open Temporal.Result_syntax in
            let rec loop index =
              if index = count then Ok count
              else
                (* Returns [Error] in the [`Cancelled] category once the
                   workflow cancels the activity. *)
                let* () =
                  Temporal.Activity.Context.heartbeat context
                    Temporal.Codec.int index
                in
                process_item index;
                loop (index + 1)
            in
            match loop 0 with
            | Error error when Temporal.Activity.Context.cancellation context <> None ->
                (* Undo partial work, then report the cancellation. Cleanup
                   must be idempotent: the attempt may be redelivered. *)
                release_reservations ();
                Error error
            | result -> result)
    ]} *)
module Cancellation : sig
  (** Why Temporal asked the attempt to stop. *)
  type reason = Temporal_base.Activity_context.cancel_reason =
    | Requested
        (** Workflow code or a client requested cancellation of the
            activity. *)
    | Timed_out
        (** Temporal timed out the attempt, for example after a missed
            heartbeat. *)
    | Not_found
        (** The server no longer knows the attempt: it already completed,
            timed out, or was retried. The attempt's result is discarded. *)
    | Worker_shutdown  (** Temporal Core is shutting the worker down. *)
    | Paused  (** An operator paused the activity. *)
    | Reset  (** An operator reset the activity. *)

  (** One observed cancellation request. Immutable. *)
  type t

  (** Returns the primary reason Temporal reported. *)
  val reason : t -> reason

  (** Returns every independent reason Temporal reported, primary first and
      without duplicates. Newer servers can report more than one, for example
      a pause together with a cancellation request. *)
  val reasons : t -> reason list

  (** Builds the non-retryable [`Cancelled] error that an activity returns to
      acknowledge this cancellation. For a [Paused] or [Reset] reason the
      worker reports the acknowledged attempt as a retryable
      ["ActivityPause"] or ["ActivityReset"] failure rather than as
      cancelled; see the module description. *)
  val to_error : t -> Error.t
end

(** Operations available to a contextual activity attempt. *)
module Context : sig
  (** The attempt-scoped context passed to contextual activity helpers. *)
  type t = context

  (** Sends one typed heartbeat value. The details are submitted (Temporal
      Core throttles what reaches the server) and any cancellation already
      waiting for this attempt is delivered. Returns an [Error] in the
      [`Cancelled] category once a cancellation has been delivered, a
      [`Bridge] error if this attempt is no longer active or the heartbeat
      could not be recorded, and a [`Codec] error if the value cannot be
      encoded. *)
  val heartbeat : t -> 'a Codec.t -> 'a -> (unit, Error.t) result

  (** Sends already encoded detail payloads in order, with the same result
      contract as {!heartbeat}. *)
  val heartbeat_payloads : t -> Payload.t list -> (unit, Error.t) result

  (** Returns the cancellation delivered to this attempt, or [None]. It is a
      non-blocking read that never contacts the server: cancellations arrive
      through {!heartbeat}, so poll this between heartbeats rather than in
      place of them. The first delivered cancellation stays fixed for the
      attempt. It may be called from any Domain and remains readable after
      the attempt completes. Contexts outside a native worker, such as the
      in-process test backend, are never cancelled. *)
  val cancellation : t -> Cancellation.t option

  (** Returns [true] once the worker running this attempt has begun to stop,
      because [Temporal.Worker.shutdown] or [Temporal.Worker.request_shutdown]
      was called. The worker waits for the running callback only until its
      graceful shutdown period ends (see [Temporal.Worker.Options.make]);
      after that the callback is abandoned, its attempt is failed
      retryably, and whatever it later returns is discarded. An activity
      that checks this flag can therefore stop early and decide its own
      outcome, for example by returning a retryable error so that Temporal
      retries the attempt on another worker. It never contacts the server,
      may be called from any Domain, and is always [false] outside a native
      worker. *)
  val is_worker_shutting_down : t -> bool

  (** Returns a copied list of the last heartbeat details recorded by the
      previous attempt of this activity, or [[]] on a first attempt or when
      the previous attempt never heartbeated. The value is fixed for the whole
      attempt: details sent with {!heartbeat} or {!heartbeat_payloads} are
      delivered to the next attempt and do not change what this attempt
      reads. *)
  val details : t -> Payload.t list

  (** Returns the configured heartbeat interval, if one was supplied. An
      unset (zero) interval is [None]; a sub-millisecond remainder is rounded
      up to the next whole millisecond, as in {!Info.heartbeat_timeout}. *)
  val heartbeat_timeout : t -> Duration.t option

  (** Returns this attempt's task metadata. Contexts created by a native
      worker or by [Temporal.Testing] always carry it ([Temporal.Testing]
      reports its virtual clock as the attempt's scheduled and started
      times); a context from a backend without a Temporal task, such as the
      [mock://] worker backend, returns a typed [Defect] error. The value is
      immutable and remains readable after the attempt completes. *)
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
    [Try_cancel] default of [start]; it governs a cancellation requested
    through [scope] or {!start_local_handle}. With [scope], cancelling the
    scope requests cancellation of this local activity: Core records the
    request and resolves the future with a typed [`Cancelled] error as the
    policy dictates ([Try_cancel] and [Abandon] at once,
    [Wait_cancellation_completed] once the running attempt has stopped). A
    local activity waiting in a language-owned retry backoff is cancelled
    immediately under every policy, because no attempt is running. A scope
    that is already cancelled schedules nothing. *)
val start_local :
  ?scope:Scope.t ->
  ?activity_id:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  ?cancellation_type:cancellation_type ->
  ('input, 'output) t ->
  'input ->
  ('output, Error.t) Future.t

(** Schedules a local activity and retains a handle for explicit
    cancellation with {!cancel}, with the same options and cancellation
    semantics as [start_local]. Repeated cancellation is idempotent, including
    after the activity has completed; cancellation outside the owning workflow
    returns a typed lifecycle error and emits no command. *)
val start_local_handle :
  ?scope:Scope.t ->
  ?activity_id:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  ?cancellation_type:cancellation_type ->
  ('input, 'output) t ->
  'input ->
  'output handle

(** Schedules a local activity and waits for its result. It has the same
    contract as [start_local], including the [Wait_cancellation_completed]
    default for [cancellation_type] and the cancellation a [scope] attaches. *)
val execute_local :
  ?scope:Scope.t ->
  ?activity_id:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  ?cancellation_type:cancellation_type ->
  ('input, 'output) t ->
  'input ->
  ('output, Error.t) result

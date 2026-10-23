(** Private production worker wiring.

    This module is kept behind the public library's [private_modules] boundary.
    It connects the typed workflow and activity execution adapters to the one
    owner-Domain supervisor, while [Temporal.Worker] remains responsible for
    the user-facing registration API. *)

(** Heterogeneous executable workflow registrations accepted by the private
    native adapter. The constructor is intentionally hidden. The type equation
    lets [Replay] hand the same registrations to its replay-mode instance of
    the adapter, so live and replayed workflows share one conversion path. *)
type workflow_registration =
  Temporal_sdk_kernel.Native_worker_execution.registered_workflow

(** Heterogeneous executable activity registrations accepted by the private
    native adapter. The constructor is intentionally hidden. *)
type activity_registration

(** Packs one typed workflow definition without exposing the existential
    constructor used by the runtime adapter. [signals] attaches typed signal
    handlers to the same workflow execution; handlers run on the deterministic
    workflow scheduler when native SignalWorkflow activations arrive. [updates]
    attaches typed update handlers for native DoUpdate jobs; a handler is
    acknowledged after validation and may then suspend on workflow futures. *)
val register_workflow :
  ?signals:Signal.Handler.t list ->
  ?queries:Query.Handler.t list ->
  ?updates:Update.Handler.t list ->
  ('input, 'output,
   'input -> ('output, Temporal_base.Error.t) result)
  Temporal_base.Definition.t ->
  workflow_registration

(** Packs one typed activity definition without exposing the existential
    constructor used by the runtime adapter. *)
val register_activity :
  ('input, 'output,
   Temporal_base.Activity_context.t ->
   'input -> ('output, Temporal_base.Error.t) result)
  Temporal_base.Definition.t ->
  activity_registration

(** Packs an asynchronous activity definition. Its callback may return a
    retained completion handle; the adapter activates that handle only after
    the native worker acknowledges [WillCompleteAsync]. *)
val register_async_activity :
  ('input, 'output,
   'output Temporal_base.Async_activity.context ->
   'input -> 'output Temporal_base.Async_activity.async_result)
  Temporal_base.Definition.t ->
  activity_registration

(** An opaque native worker containing the supervisor and both typed adapters.
    No Rust pointer, task token, or protocol buffer is exposed. *)
type t

(** Creates a real Temporal worker for an HTTP(S) endpoint.

    Registration validation happens before the native graph is published. If
    connection or worker startup fails after a graph exists, the graph is
    synchronously shut down before the typed error is returned. The optional
    cache bound is passed to Temporal Core; omitting it preserves the default
    bound, while a small positive bound makes Core cache eviction observable
    through the worker's empty-completion path. [versioning] selects
    Temporal's worker routing strategy; omitting it preserves the unversioned
    behavior. [io_threads] is the public network-thread bound, mapped
    to the native runtime's Tokio worker pool; omitting it selects the bridge
    default. With [runtime], the graph is attached to that shared Core
    runtime instead (#832); the attachment lasts until the supervisor has
    closed the graph, and a shut-down runtime is a defect. The caller
    validates that [io_threads] and [runtime] are not both given.
    [activation_deadline_ms] enables the non-yielding workflow
    watchdog with that positive deadline; omitting it disables the watchdog
    (the public [Worker.Options] layer supplies the default).

    [max_outstanding_workflow_tasks] (default 1000),
    [max_concurrent_workflow_task_polls] (default 2),
    [graceful_shutdown_timeout_ms] (default 30000) and [tuning] (default
    all-Core-defaults) are the public worker resource options of #498. They
    are validated by {!Temporal_sdk_kernel.Bridge.worker_config} before any
    native resource is allocated. Activity slot counts are deliberately not
    parameters: the bridge pins them to the serial OCaml executor (#777).

    The grace period also bounds how long [run] and [shutdown] wait for a
    stopping lane, and [shutdown_teardown_timeout_ms] (default 60000) how
    long [shutdown] then waits for the native release (#495). The caller
    validates both; neither reaches the bridge except the grace period. *)
val create :
  ?max_cached_workflows:int ->
  ?max_outstanding_workflow_tasks:int ->
  ?max_concurrent_workflow_task_polls:int ->
  ?graceful_shutdown_timeout_ms:int64 ->
  ?shutdown_teardown_timeout_ms:int64 ->
  ?tuning:Temporal_sdk_kernel.Bridge.worker_tuning ->
  ?io_threads:int ->
  ?runtime:Temporal_sdk_kernel.Shared_runtime.t ->
  ?versioning:Temporal_sdk_kernel.Bridge.worker_versioning ->
  ?activation_deadline_ms:int ->
  target_url:string ->
  namespace:string ->
  identity:string ->
  task_queue:string ->
  workflows:workflow_registration list ->
  activities:activity_registration list ->
  unit ->
  (t, Temporal_base.Error.t) result

(** Polls and executes both native workflow and activity lanes until [shutdown]
    is requested. A successful task-level failure is acknowledged by Temporal
    and does not terminate this loop. When the activation watchdog is enabled
    it runs on its own Domain for the duration of this call; an activation
    abandoned by the watchdog is reported as a rejected task when its code
    eventually returns, so the loop continues. The loop itself cannot return
    while workflow code refuses to yield. Once a stop is observed it waits
    for an activity callback only until the grace period has elapsed, then
    returns and leaves that callback running on its detached Domain (#495). *)
val run : t -> (unit, Temporal_base.Error.t) result

(** The first workflow activation the watchdog abandoned because it ran past
    its deadline without yielding, or [None]. Lock-free and safe from any
    Domain or thread, including while the workflow lane is stuck. The report
    is sticky until the worker value is discarded. *)
val stuck_activation :
  t -> Temporal_sdk_kernel.Native_worker_execution.stuck_activation option

(** Whether the calling system thread is executing this worker's workflow or
    activity lane, and therefore a workflow or activity callback. Public
    shutdown checks this before its own mutex so a callback cannot deadlock
    against a concurrent external shutdown. Another system thread on the same
    Domain as a lane is not an execution thread and may wait for shutdown. *)
val is_execution_thread : t -> bool

(** Asks an active or future [run] to return [Ok ()] at its next stop check,
    without waiting and without releasing anything (#830). The request is
    sticky. It is a single atomic write, so an OCaml signal handler may call it
    from any Domain or thread, including the run loop's own thread. The caller
    must still call [shutdown] once [run] has returned to drain completions and
    release the native graph. *)
val request_stop : t -> unit

(** Requests stop and runs the bounded shutdown of
    {!Temporal_sdk_kernel.Native_worker_shutdown}: it waits at most the grace
    period for the run loop to leave the adapters, drains retained
    completions, and releases the supervisor's worker, client, and Rust
    runtime graph exactly once, waiting at most the teardown timeout for that
    release. [Ok report] says what was abandoned and whether the release was
    still in progress on its own thread when the call returned. [Error] means
    a retained completion was lost or the release failed; the graph is still
    released. Repeated calls are idempotent. A call from an execution thread
    cannot wait for its own loop: it posts [request_stop] and returns a
    retryable defect without starting teardown. *)
val shutdown :
  t ->
  ( Temporal_sdk_kernel.Native_worker_shutdown.report,
    Temporal_base.Error.t )
  result

(** Returns [true] only when the most recent shutdown failure was the
    execution-thread admission defect. In that state teardown has not
    started and another thread may retry; every admitted shutdown is
    terminal and returns [false]. *)
val shutdown_retryable : t -> bool

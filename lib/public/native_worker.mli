(** Private production worker wiring.

    This module is kept behind the public library's [private_modules] boundary.
    It connects the typed workflow and activity execution adapters to the one
    owner-Domain supervisor, while [Temporal.Worker] remains responsible for
    the user-facing registration API. *)

(** Heterogeneous executable workflow registrations accepted by the private
    native adapter. The constructor is intentionally hidden. *)
type workflow_registration

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
    default. [activation_deadline_ms] enables the non-yielding workflow
    watchdog with that positive deadline; omitting it disables the watchdog
    (the public [Worker.Options] layer supplies the default). *)
val create :
  ?max_cached_workflows:int ->
  ?io_threads:int ->
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
    while workflow code refuses to yield. *)
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

(** Requests stop, waits for an active run loop to leave the adapter, and then
    releases the supervisor's worker, client, and Rust runtime graph exactly
    once. Repeated calls are idempotent. A call from an execution thread
    cannot wait for its own loop: it posts [request_stop] and returns a
    retryable defect without starting teardown. *)
val shutdown : t -> (unit, Temporal_base.Error.t) result

(** Returns [true] only when the most recent shutdown failure occurred while
    retrying an OCaml-owned completion. In that state native teardown has not
    started and the caller may retry; a native teardown error returns [false]
    because the supervisor graph is already terminal. *)
val shutdown_retryable : t -> bool

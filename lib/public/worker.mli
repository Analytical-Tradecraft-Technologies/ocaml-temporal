(** Worker registration and execution for OCaml workflow and activity code.

    Definitions are packed existentially only at the registration boundary;
    workflow bodies and activity bodies remain ordinary typed OCaml functions. *)

(** A heterogeneous workflow registration item. The existential package keeps
    each definition's input and output codecs paired with its implementation. *)
type registered_workflow

(** Immutable worker construction options. The record is abstract so callers
    can only obtain validated values through [make] or [default]. *)
module Options : sig
  (** Worker routing mode. [No_versioning] keeps the build ID as metadata;
      [Legacy_build_id] enables Temporal's whole-worker build-ID versioning;
      [Deployment_based] selects Temporal Core's deployment/version routing
      for the named deployment and build. When [use_worker_versioning] is
      [true], [default_versioning_behavior] must be [Some]: the SDK has no
      per-workflow behavior, so this default is what every workflow-task
      completion reports. When it is [false], the default must be [None]. *)
  type versioning =
    | No_versioning
    | Legacy_build_id of string
    | Deployment_based of {
        deployment_name : string;
        build_id : string;
        use_worker_versioning : bool;
        default_versioning_behavior : [ `Auto_upgrade | `Pinned ] option;
      }

  (** Deadline for one workflow activation to return control to the worker.
      [`After d] enables the non-yielding-code watchdog: when workflow code
      (including its codecs and signal, query, and update handlers) runs for
      at least [d] without returning or awaiting a workflow operation, the
      worker fails that workflow task so Temporal retries it, logs one
      bounded diagnostic, and reports {!Health.Stuck_workflow_activation}
      from {!health}. [`Disabled] turns the watchdog off, for example while
      stepping through workflow code in a debugger. The watchdog never
      interrupts the stuck code; see {!health} for the recovery contract. *)
  type activation_deadline = [ `After of Duration.t | `Disabled ]

  (** How many concurrent workflow-task polls the worker keeps open against
      the server. While the sticky cache is enabled the worker polls two
      queues, its own sticky queue and the shared normal queue, and the two
      variants treat them differently:

      - [Fixed n] is a total of [n] polls that Temporal Core splits between
        the queues: [max 1 (n / 5)] on the normal queue and the rest, at
        least one, on the sticky queue. A caching worker therefore needs
        [n >= 2]. Core applies the same normal-queue share without the
        cache, so an uncached worker keeps [max 1 (n / 5)] polls open.
      - [Autoscaling] bounds apply to {e each} queue separately: Core scales
        every queue between [minimum] and [maximum] polls from server
        feedback, starting at [initial], and requires
        [1 <= minimum <= initial <= maximum]. A caching worker can thus
        keep up to [2 * maximum] polls open, and at least [2 * minimum].
        Without the cache there is only the normal queue.

      A poll is only issued when a workflow-task slot is free, so polls
      beyond [max_concurrent_workflow_tasks] just wait. *)
  type workflow_task_pollers =
    | Fixed of int
    | Autoscaling of { minimum : int; maximum : int; initial : int }

  (** A validated set of optional worker resource and routing settings. *)
  type t

  (** Existing worker defaults: no routing versioning, the standard sticky
      cache bound, a two-second workflow activation deadline, and the
      resource defaults listed on [make]. *)
  val default : t

  (** Validates and constructs options. Every check happens here, before any
      native resource exists; the private bridge repeats them at its own
      boundary. Each invalid value returns a defect whose message names the
      option. Counts are capped at 1,000,000 and durations at one day.

      Routing and liveness:
      - [versioning] defaults to [No_versioning]. Legacy build IDs must be
        non-empty, NUL-free, and within the bridge transport limit.
        Deployment versioning returns a defect when [use_worker_versioning]
        and [default_versioning_behavior] disagree as described on
        [versioning].
      - [workflow_activation_deadline] defaults to [`After] two seconds (the
        Python SDK's deadlock timeout; Go uses one second); a zero deadline or
        one above one hour returns a defect.

      Workflow resources (#498):
      - [max_cached_workflows] (default 1000) bounds the sticky workflow
        cache; [0] disables sticky caching.
      - [max_concurrent_workflow_tasks] (default 1000, at least 1) bounds the
        workflow tasks Core holds at once. While the cache is enabled it must
        be at least 2, and Core admits at most [max 2 max_cached_workflows]
        tasks, so a smaller cache also lowers the effective limit. The OCaml
        workflow lane still runs one activation at a time; this limit bounds
        tasks waiting for it, not parallel execution.
      - [workflow_task_pollers] defaults to [Fixed 2]; see
        {!type-workflow_task_pollers}.
      - [sticky_queue_schedule_to_start_timeout] (default 10 s, 1 ms to one
        day) is how long a task may wait on this worker's sticky queue before
        the server offers it to any worker. It has no effect when the cache
        is disabled.

      Shutdown (#495; see {!shutdown_with_report} for the whole contract):
      - [graceful_shutdown_period] (default 30 s, zero to one day) is how long
        work already running may continue after shutdown begins. Activity
        callbacks see {!Activity.Context.is_worker_shutting_down} turn [true]
        at once. A callback or workflow activation still running when the
        period ends is abandoned, and Temporal retries its task elsewhere.
        Temporal Core receives the same period for the activity tasks it
        still holds.
      - [shutdown_teardown_timeout] (default 60 s, zero to one day) is how
        long {!shutdown} then waits for native teardown: retiring abandoned
        tasks with the server, deregistering the worker, and releasing the
        runtime. When it elapses first, {!shutdown} returns and the teardown
        finishes in the background. The default covers an ordinary
        teardown, including one server long poll.
      Together they bound {!shutdown}: it returns within
      [graceful_shutdown_period + shutdown_teardown_timeout] plus a fraction
      of a second.

      Activities:
      - [max_heartbeat_throttle_interval] (default 60 s) and
        [default_heartbeat_throttle_interval] (default 30 s), each 1 ms to one
        day, bound how often activity heartbeats are sent to the server.
        Activities with a heartbeat timeout use 80% of it, capped by the
        maximum; others use the default. An explicit default above an
        explicit maximum returns a defect.
      - [max_worker_activities_per_second] limits how many remote activity
        tasks this worker polls per second. It must be at least one per day
        ([1. /. 86400.]), because Core turns its reciprocal into the
        interval between polls. Core applies it to polled tasks
        only; an activity dispatched eagerly with its workflow task
        bypasses it unless the activity sets [~do_not_eagerly_execute:true].
        [max_task_queue_activities_per_second] asks the server to limit
        dispatch for the whole task queue: workers that set different values
        overwrite each other, and setting it disables eager activity
        execution. Both must be positive finite numbers and are unset by
        default.

      Remote and local activity slots are deliberately not configurable. The
      OCaml activity executor runs one callback at a time, so the worker
      grants Temporal Core exactly one remote and one local activity slot.
      A larger value would let the server start activity timeouts for tasks
      that can only wait in a queue. The rate limits above can only lower
      throughput and therefore cannot contradict that executor. *)
  val make :
    ?versioning:versioning ->
    ?max_cached_workflows:int ->
    ?workflow_activation_deadline:activation_deadline ->
    ?max_concurrent_workflow_tasks:int ->
    ?workflow_task_pollers:workflow_task_pollers ->
    ?sticky_queue_schedule_to_start_timeout:Duration.t ->
    ?graceful_shutdown_period:Duration.t ->
    ?shutdown_teardown_timeout:Duration.t ->
    ?max_heartbeat_throttle_interval:Duration.t ->
    ?default_heartbeat_throttle_interval:Duration.t ->
    ?max_worker_activities_per_second:float ->
    ?max_task_queue_activities_per_second:float ->
    unit ->
    (t, Error.t) result

  (** Reads the validated routing mode without exposing the internal record. *)
  val versioning : t -> versioning

  (** Returns the explicit cache override, or [None] when worker defaults apply. *)
  val max_cached_workflows : t -> int option

  (** Returns the workflow activation watchdog setting. *)
  val workflow_activation_deadline : t -> activation_deadline

  (** The configured workflow-task limit, or its default. *)
  val max_concurrent_workflow_tasks : t -> int

  (** The configured workflow poller behavior, or its default. *)
  val workflow_task_pollers : t -> workflow_task_pollers

  (** The configured sticky-queue timeout, or Core's default. *)
  val sticky_queue_schedule_to_start_timeout : t -> Duration.t

  (** The configured shutdown grace period, or its default. *)
  val graceful_shutdown_period : t -> Duration.t

  (** The configured native teardown timeout, or its default. *)
  val shutdown_teardown_timeout : t -> Duration.t

  (** The configured maximum heartbeat throttle interval, or Core's default. *)
  val max_heartbeat_throttle_interval : t -> Duration.t

  (** The default heartbeat throttle interval Core applies: the configured
      value or Core's default, capped by the effective maximum. *)
  val default_heartbeat_throttle_interval : t -> Duration.t

  (** The per-worker activity rate limit, or [None] when unlimited. *)
  val max_worker_activities_per_second : t -> float option

  (** The task-queue activity rate limit, or [None] when unset. *)
  val max_task_queue_activities_per_second : t -> float option
end

(** Worker liveness as observed by the workflow activation watchdog. *)
module Health : sig
  (** How the watchdog released a stuck activation, and whether Temporal
      acknowledged it.
      - [`Task_failed]: the workflow task was failed, so Temporal retries it,
        normally on another worker.
      - [`Queries_failed]: the activation only delivered queries; each query
        was answered with a failure and the workflow task was not failed.
      - [`Eviction_acknowledged]: the activation only evicted the run from the
        sticky cache; it was acknowledged with an empty completion and no
        workflow task was failed.
      - [`Not_acknowledged]: the replacement completion could not be
        delivered, so the task, query, or eviction will instead time out. *)
  type abandonment =
    [ `Task_failed | `Queries_failed | `Eviction_acknowledged | `Not_acknowledged ]

  (** The first workflow activation that exceeded the activation deadline.
      [workflow_type] and [workflow_id] are [None] only when the activation
      could not be matched to a run. [elapsed] is a lower bound on how long the
      activation had run when it was detected. [abandoned] reports what the
      watchdog submitted in place of the stuck activation's completion. No
      payload is included. *)
  type stuck_workflow_activation = {
    workflow_type : string option;
    workflow_id : string option;
    run_id : string;
    is_replaying : bool;
    elapsed : Duration.t;
    abandoned : abandonment;
  }

  (** [Healthy] until the watchdog detects a stuck activation; afterwards
      [Stuck_workflow_activation] for the life of the worker. *)
  type t = Healthy | Stuck_workflow_activation of stuck_workflow_activation
end

(** What a bounded {!shutdown_with_report} left behind (#495). *)
module Shutdown_report : sig
  (** Whether native teardown finished before {!shutdown_with_report}
      returned. [`Detached] means the teardown timeout elapsed first: the
      teardown continues on an SDK-owned background thread, which still
      releases every native resource exactly once. A process that exits
      right away may cut it short; Temporal then recovers the worker's
      tasks through its ordinary timeouts. *)
  type teardown = [ `Completed | `Detached ]

  (** Counts are taken when the grace period ended.
      - [elapsed] is how long the call that performed shutdown took.
        Repeated calls return the same report.
      - [lanes_stopped] is [true] when the workflow and activity lanes both
        returned within the grace period. [false] means at least one was
        abandoned: it was running the user code counted below, or was
        blocked in a call into Temporal Core on that code's behalf.
      - [abandoned_activity_callbacks] counts activity callbacks still
        running. The worker fails each one's task retryably, so Temporal
        schedules the next attempt under the activity's retry policy,
        normally on another worker.
      - [abandoned_workflow_activations] counts workflow activations still
        running. Their workflow tasks are failed, so Temporal retries them,
        normally on another worker. *)
  type t = {
    elapsed : Duration.t;
    lanes_stopped : bool;
    abandoned_activity_callbacks : int;
    abandoned_workflow_activations : int;
    native_teardown : teardown;
  }

  (** [true] when nothing was abandoned and teardown completed. *)
  val is_clean : t -> bool
end

(** Packs a typed workflow definition for a worker registration list. [signals]
    attach scheduler handlers for matching native signal activations; [queries]
    attach synchronous read-only handlers for matching query requests; [updates]
    attach typed update handlers for matching update activations. An update
    handler runs on the workflow scheduler once validation, if any, accepts
    the request and may suspend on workflow futures; see [Temporal.Update]. *)
val workflow :
  ?signals:Signal.Handler.t list ->
  ?queries:Query.Handler.t list ->
  ?updates:Update.Handler.t list ->
  ('input, 'output) Workflow.t -> registered_workflow

(** A heterogeneous activity registration item. *)
type registered_activity

(** Packs a typed activity definition for a worker registration list. *)
val activity : ('input, 'output) Activity.t -> registered_activity

(** An opaque worker instance owning one supervisor/backend graph and two
    deterministic registration maps. *)
type t

(** Creates and validates a worker. Duplicate names and remote-only definitions
    return typed defects before any backend graph is allocated. A [mock://]
    target selects an in-memory backend for testing registration and dispatch
    plumbing only: it queues one synthetic task per registered definition with
    an empty [binary/null] input, unrelated to any mock client start, and calls
    each implementation whose input codec accepts that payload (for example
    [Codec.unit]) outside a workflow context. Callback side effects therefore
    still run, while workflow operations such as [Activity.start] or
    [Workflow.sleep] return defects. It is not a
    workflow test environment. An [http://] or [https://]
    target creates the OCaml-owned native Core worker and its private Rust
    bridge. [max_cached_workflows] optionally bounds Core's sticky workflow
    cache; omitting it preserves the default, while a small positive bound can
    cause explicit cache-eviction activations that the worker acknowledges with
    an empty completion. Passing both [options] and [max_cached_workflows] is
    a typed defect. Every other resource and shutdown setting comes from
    [options] and is validated by {!Options.make}; {!options} reports the
    effective values. An explicit [identity] is used unchanged and must be
    non-empty and NUL-free. When omitted, the
    identity defaults to [<pid>@<hostname>], matching the official Temporal
    SDKs, computed once when the worker is created so pollers from different
    processes are distinguishable in Temporal.

    [io_threads] is an upper bound on the background threads this worker
    uses for network I/O and server communication; the SDK may use fewer.
    When omitted it is the host's available parallelism capped at 4.
    Workflow and activity code never runs on these threads. Each worker and
    each client has its own threads, so the bound applies per instance. It
    must be between 1 and 256; any other value returns a typed defect before
    anything is allocated, for every target including [mock://].

    [runtime] instead runs the worker on a shared {!Runtime.t}, whose
    background threads it shares with every other client and worker
    attached to it. The worker stays attached until {!shutdown} has
    completed its native teardown, and {!Runtime.shutdown} fails while it
    is. Passing both [runtime] and [io_threads] is a typed defect (set the
    bound on {!Runtime.create}), and so is a runtime that was already shut
    down. A [mock://] worker attaches too. *)
val create :
  ?identity:string ->
  ?options:Options.t ->
  ?max_cached_workflows:int ->
  ?io_threads:int ->
  ?runtime:Runtime.t ->
  target_url:string ->
  namespace:string ->
  task_queue:string ->
  workflows:registered_workflow list ->
  activities:registered_activity list ->
  unit ->
  (t, Error.t) result

(** Runs the workflow and activity poll loops until [shutdown] or
    [request_shutdown] is requested. After [request_shutdown], [run] returns
    [Ok ()] once both lanes have finished their current task, and a later
    [run] returns [Ok ()] without polling. An activity callback still
    running when the grace period ends ({!Options.make}) no longer delays
    [run]: it returns and leaves that callback running on its own Domain,
    which {!shutdown_with_report} then reports as abandoned. A workflow
    activation runs on the thread that called [run], so a non-yielding one
    keeps [run] from returning (see {!health}).
    Each accepted task is decoded, dispatched to its registered OCaml function,
    encoded, and completed before the next task is admitted. This is a blocking
    call: invoke it from an ordinary dedicated Domain or system thread, not
    directly on a cooperative Eio/Lwt scheduler fiber. Native readiness waits
    release the OCaml runtime lock and return periodically so shutdown cannot
    be stranded, but releasing that lock does not make [run] non-blocking. *)
val run : t -> (unit, Error.t) result

(** The options the worker was created with. The [Options] accessors report
    each effective setting, defaults included, and none of them carries a
    credential, so the result is safe to log for diagnostics. A worker
    created with [~max_cached_workflows] alone reports options holding that
    bound. *)
val options : t -> Options.t

(** Reports whether the workflow activation watchdog has detected workflow
    code that stopped yielding (see {!Options.activation_deadline}).

    OCaml code cannot be safely interrupted, so the watchdog only fails the
    stuck workflow task (Temporal then retries it, normally on another worker)
    and marks this worker unhealthy. The stuck code keeps the workflow lane
    busy until it returns on its own, so no other workflow task on this worker
    makes progress meanwhile, and [run] cannot return while it is stuck.
    {!shutdown}, called from another thread, still returns within its bound
    and reports the activation as abandoned; the stuck code itself keeps
    running. Recovery is a process restart by an external supervisor
    (Kubernetes, systemd, or similar). Once reported, the state is sticky:
    even if the code later returns, the process may hold inconsistent state
    and should be replaced.

    The call is lock-free and does not touch the worker's lanes or the native
    graph, so it is safe from any Domain or thread at any time, including a
    liveness-probe handler. A probe should fail when this returns
    [Stuck_workflow_activation], and should also be driven by a periodic
    heartbeat (for example a file or endpoint timestamp refreshed by a thread
    that calls [health]), so a process whose whole runtime is blocked fails
    the probe too. The mock backend always reports [Healthy]. *)
val health : t -> Health.t

(** Shuts the worker down within a bound and reports what it abandoned
    (#495). Admission closes at once: both lanes stop polling and activity
    contexts report {!Activity.Context.is_worker_shutting_down}. Then:

    + Work already running may finish until the grace period
      ({!Options.make}) elapses. Retained completions are delivered.
    + Anything still running at that point is abandoned rather than awaited,
      because OCaml cannot interrupt it. An activity callback is detached:
      it keeps running on its activity Domain, its task is failed
      retryably so Temporal schedules the next attempt (normally on another
      worker), and the result it eventually returns is discarded without
      reaching Temporal. A non-yielding workflow activation keeps running on
      the thread that called {!run}; its workflow task is failed. The
      abandoned code keeps any OCaml values it uses alive until it returns;
      the worker's native resources do not depend on it.
    + Native teardown then runs for at most the teardown timeout. It may
      instead be left to finish in the background, as the report states.

    The call therefore returns within the grace period plus the teardown
    timeout, plus a fraction of a second, even when a callback ignores
    cancellation or the server is unreachable. [Ok report] describes a
    shutdown that delivered every completion it could; a process supervisor
    should restart a process whose report shows abandoned work, because that
    code is still running. [Error] means a completion was lost (a retained
    completion could not be delivered, or failed permanently) or native
    teardown reported a failure; native resources are still released.
    Abandoned work that Temporal retries is not an error.

    Repeated calls are safe and return the same cached terminal result.
    A call from an execution thread is the only retryable failure (below).

    [shutdown_with_report] may be called from any Domain or system thread
    other than the one running a workflow or activity callback of this
    worker, including a sibling system thread on the Domain that hosts
    [run]. Concurrent callers are serialized: one performs the shutdown and
    the others wait for and return the same cached result. A call from the
    thread running [run], such as a workflow or activity callback of this
    worker or an OCaml signal handler that the runtime happens to execute on
    that thread, cannot wait for its own loop to stop. It calls
    [request_shutdown], so [run] returns, and returns a defect [Error]
    immediately without releasing anything; call it again after [run]
    returns.

    [shutdown_with_report] takes locks and blocks, so do not call it from a
    signal handler; use [request_shutdown] there. The mock backend has
    nothing to abandon and reports a clean shutdown. *)
val shutdown_with_report : t -> (Shutdown_report.t, Error.t) result

(** {!shutdown_with_report} without the report: the same bounded shutdown
    and the same cached result. *)
val shutdown : t -> (unit, Error.t) result

(** Asks [run] to stop and returns immediately. The request is sticky and
    idempotent: an active [run] returns [Ok ()] once each lane finishes its
    current task (within the bounded native readiness wait when idle), or
    once the grace period has elapsed for an activity callback that has not
    finished, and a later [run] returns [Ok ()] without polling. Nothing is
    drained or released, so call [shutdown_with_report] after [run]
    returns. The grace period starts when the run loop observes this
    request, so the following shutdown does not grant the callback a second
    one.

    This is the function to call from a [SIGTERM] or [SIGINT] handler. It
    performs only atomic writes, with no lock, I/O, logging, or native call,
    so it is safe whichever Domain or thread runs the handler, including the
    thread blocked in [run] itself. Signal handlers are process-global, so
    restore the previous ones when the worker stops; otherwise a later signal
    would be swallowed by this already shut-down worker:

    {[
      let serve worker =
        let stop _signal = Temporal.Worker.request_shutdown worker in
        let previous_term = Sys.signal Sys.sigterm (Sys.Signal_handle stop) in
        let previous_int = Sys.signal Sys.sigint (Sys.Signal_handle stop) in
        Fun.protect
          ~finally:(fun () ->
            Sys.set_signal Sys.sigterm previous_term;
            Sys.set_signal Sys.sigint previous_int)
          (fun () ->
            let run_result = Temporal.Worker.run worker in
            let shutdown_result = Temporal.Worker.shutdown_with_report worker in
            (match shutdown_result with
            | Ok report when not (Temporal.Worker.Shutdown_report.is_clean report)
              ->
                (* Abandoned code is still running: exit, so the process
                   supervisor replaces this process. *)
                prerr_endline "worker shutdown abandoned in-flight work"
            | Ok _ | Error _ -> ());
            Result.bind run_result (fun () -> Result.map ignore shutdown_result))
    ]}

    A service behind a load balancer or orchestrator should first stop
    advertising readiness (for example fail its readiness probe), then call
    [request_shutdown], and keep its liveness probe on {!health} until the
    process exits. *)
val request_shutdown : t -> unit

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

  (** A validated set of optional worker resource and routing settings. *)
  type t

  (** Existing worker defaults: no routing versioning, the standard sticky
      cache bound, and a two-second workflow activation deadline. *)
  val default : t

  (** Validates and constructs options. A supplied cache value overrides the
      normal worker default; [0] disables sticky workflow caching. Legacy build
      IDs must be non-empty, NUL-free, and within the bridge transport limit.
      Deployment versioning returns a defect when [use_worker_versioning] and
      [default_versioning_behavior] disagree as described on [versioning].
      [workflow_activation_deadline] defaults to [`After] two seconds (the
      Python SDK's deadlock timeout; Go uses one second); a zero deadline or
      one above one hour returns a defect. *)
  val make :
    ?versioning:versioning ->
    ?max_cached_workflows:int ->
    ?workflow_activation_deadline:activation_deadline ->
    unit ->
    (t, Error.t) result

  (** Reads the validated routing mode without exposing the internal record. *)
  val versioning : t -> versioning

  (** Returns the explicit cache override, or [None] when worker defaults apply. *)
  val max_cached_workflows : t -> int option

  (** Returns the workflow activation watchdog setting. *)
  val workflow_activation_deadline : t -> activation_deadline
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
    an empty completion. An explicit [identity] is used unchanged and must be
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
    anything is allocated, for every target including [mock://]. *)
val create :
  ?identity:string ->
  ?options:Options.t ->
  ?max_cached_workflows:int ->
  ?io_threads:int ->
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
    [run] returns [Ok ()] without polling.
    Each accepted task is decoded, dispatched to its registered OCaml function,
    encoded, and completed before the next task is admitted. This is a blocking
    call: invoke it from an ordinary dedicated Domain or system thread, not
    directly on a cooperative Eio/Lwt scheduler fiber. Native readiness waits
    release the OCaml runtime lock and return periodically so shutdown cannot
    be stranded, but releasing that lock does not make [run] non-blocking. *)
val run : t -> (unit, Error.t) result

(** Reports whether the workflow activation watchdog has detected workflow
    code that stopped yielding (see {!Options.activation_deadline}).

    OCaml code cannot be safely interrupted, so the watchdog only fails the
    stuck workflow task (Temporal then retries it, normally on another worker)
    and marks this worker unhealthy. The stuck code keeps the workflow lane
    busy until it returns on its own, so no other workflow task on this worker
    makes progress meanwhile, and {!shutdown} cannot complete while it is
    stuck. Recovery is a process restart by an external supervisor
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

(** Initiates graceful worker shutdown. Repeated calls are safe and return the
    same cached terminal result. A permanent native teardown error is retained
    so later callers observe [Error] rather than a spurious [Ok]. Retryable
    failures leave the worker open for another attempt.

    [shutdown] may be called from any Domain or system thread other than the
    one running a workflow or activity callback of this worker, including a
    sibling system thread on the Domain that hosts [run]. It blocks until the
    run loop has stopped and the worker is released. Concurrent callers are
    serialized: one performs the teardown and the others wait for and return
    the same cached result. A call from the thread running [run], such as a
    workflow or activity callback of this worker or an OCaml signal handler
    that the runtime happens to execute on that thread, cannot wait for its
    own loop to stop. It calls [request_shutdown], so [run] returns, and
    returns a defect [Error] immediately without releasing anything; call
    [shutdown] again after [run] returns.

    [shutdown] takes locks and blocks, so do not call it from a signal
    handler; use [request_shutdown] there. *)
val shutdown : t -> (unit, Error.t) result

(** Asks [run] to stop and returns immediately. The request is sticky and
    idempotent: an active [run] returns [Ok ()] once each lane finishes its
    current task (within the bounded native readiness wait when idle), and a
    later [run] returns [Ok ()] without polling. Nothing is drained or
    released, so call [shutdown] after [run] returns.

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
            let shutdown_result = Temporal.Worker.shutdown worker in
            Result.bind run_result (fun () -> shutdown_result))
    ]} *)
val request_shutdown : t -> unit

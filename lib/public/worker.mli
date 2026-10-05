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

  (** A validated set of optional worker resource and routing settings. *)
  type t

  (** Existing worker defaults: no routing versioning and the standard sticky
      cache bound. *)
  val default : t

  (** Validates and constructs options. A supplied cache value overrides the
      normal worker default; [0] disables sticky workflow caching. Legacy build
      IDs must be non-empty, NUL-free, and within the bridge transport limit.
      Deployment versioning returns a defect when [use_worker_versioning] and
      [default_versioning_behavior] disagree as described on [versioning]. *)
  val make :
    ?versioning:versioning ->
    ?max_cached_workflows:int ->
    unit ->
    (t, Error.t) result

  (** Reads the validated routing mode without exposing the internal record. *)
  val versioning : t -> versioning

  (** Returns the explicit cache override, or [None] when worker defaults apply. *)
  val max_cached_workflows : t -> int option
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
    processes are distinguishable in Temporal. *)
val create :
  ?identity:string ->
  ?options:Options.t ->
  ?max_cached_workflows:int ->
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
    thread blocked in [run] itself:

    {[
      let serve worker =
        let stop _signal = Temporal.Worker.request_shutdown worker in
        Sys.set_signal Sys.sigterm (Sys.Signal_handle stop);
        Sys.set_signal Sys.sigint (Sys.Signal_handle stop);
        let run_result = Temporal.Worker.run worker in
        let shutdown_result = Temporal.Worker.shutdown worker in
        Result.bind run_result (fun () -> shutdown_result)
    ]} *)
val request_shutdown : t -> unit

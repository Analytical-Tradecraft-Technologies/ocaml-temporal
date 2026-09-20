(** Private production wiring for the public OCaml worker.

    The public [Temporal.Worker] module owns registration ergonomics and the
    deterministic mock seam used by unit tests. This module owns the real
    integration: one [Temporal_sdk_kernel.Supervisor] instance, one workflow
    adapter, and one activity adapter. Rust/Core remains behind the supervisor;
    this module never stores a native pointer or a raw JSON document. *)

module Native = Temporal_sdk_kernel.Supervisor
module Bridge = Temporal_sdk_kernel.Bridge
module Base_error = Temporal_base.Error
module Observability = Temporal_base.Observability
module Workflow_adapter = Temporal_sdk_kernel.Native_worker_execution
module Activity_adapter = Temporal_sdk_kernel.Native_activity_execution
module Worker_loop = Temporal_sdk_kernel.Native_worker_loop
module Worker_policy = Temporal_sdk_kernel.Native_worker_policy
module Observer = Temporal_sdk_kernel.Native_worker_observer

(** Result-bind notation keeps expected startup and lifecycle failures typed. *)
let ( let* ) = Result.bind

(** A bounded diagnostic protects logs and public errors from an unexpectedly
    verbose native exception or server response. Payload bytes are never copied
    into this diagnostic. *)
let bounded_message value =
  let maximum = 1_024 in
  if String.length value <= maximum then value
  else String.sub value 0 (maximum - 3) ^ "..."

(** Converts a bridge status to the stable lowercase label used in diagnostics.
    The mapping is intentionally local because the bridge keeps this helper
    private to avoid exposing Rust-specific naming in the public API. *)
let bridge_status = function
  | Bridge.Invalid_argument -> "invalid_argument"
  | Abi_mismatch -> "abi_mismatch"
  | Panic -> "panic"
  | Internal -> "internal"
  | Invalid_state -> "invalid_state"
  | Configuration -> "configuration"
  | Connection -> "connection"
  | Worker -> "worker"
  | Outstanding_tasks -> "outstanding_tasks"
  | Not_ready -> "not_ready"
  | Protocol -> "protocol"
  | Already_started -> "already_started"
  | Retryable -> "retryable"
  | Unknown code -> Printf.sprintf "unknown(%d)" code

(** Converts the supervisor's opaque error into a bounded worker diagnostic.
    [Supervisor_failed] is an internal defect; it is still represented as a
    result so callers do not need to catch an exception during shutdown.

    Worker and outstanding-task statuses are closed categories at the public
    boundary. The Rust bridge normally supplies constant messages for them, but
    repeating the mapping here also protects callers from a stale native library
    or a malformed test double that still carries Core/gRPC prose. *)
let native_error_view (error : Native.error) =
  match error with
  | Native.Backend ({ Bridge.status; message } : Bridge.error) ->
      let message =
        match status with
        | Bridge.Worker -> "native worker operation failed"
        | Bridge.Outstanding_tasks -> "native worker has outstanding tasks"
        | _ -> bounded_message message
      in
      (bridge_status status, message)
  | Native.Closed -> ("closed", "native supervisor is shut down")
  | Native.Supervisor_failed exception_ ->
      let message =
        try Printexc.to_string exception_
        with _ -> "unprintable supervisor exception"
      in
      ("supervisor_failed", bounded_message message)

(** Converts a supervisor failure into the broad public bridge category while
    retaining the operation and native classification in one readable message.
*)
let public_native_error operation error =
  let code, message = native_error_view error in
  Base_error.make ~category:`Bridge
    ~message:(Printf.sprintf "%s failed (%s): %s" operation code message)
    ()

(** Converts a configuration error produced before the supervisor exists. The
    bridge configuration helpers use their lower-level error record rather than
    the supervisor's lifecycle variant. *)
let public_bridge_error operation ({ Bridge.status; message } : Bridge.error) =
  Base_error.make ~category:`Bridge
    ~message:
      (Printf.sprintf "%s failed (%s): %s" operation (bridge_status status)
         (bounded_message message))
    ()

(** Converts an adapter diagnostic into a public bridge error without exposing
    the adapter's private record type or any task-token bytes. *)
let public_adapter_error operation
    ({ code; path; message } : Workflow_adapter.error_view) =
  Base_error.make ~category:`Bridge
    ~message:
      (Printf.sprintf "%s failed at %s (%s): %s" operation path code message)
    ()

(** Activity adapter diagnostics have the same shape as workflow diagnostics but
    remain a distinct private type, so this conversion is explicit. *)
let public_activity_error operation
    ({ code; path; message; _ } : Activity_adapter.error_view) =
  Base_error.make ~category:`Bridge
    ~message:
      (Printf.sprintf "%s failed at %s (%s): %s" operation path code message)
    ()

(** The adapter functors need only the two typed operations below. Each call
    still enters the supervisor mailbox, so workflow and activity operations
    cannot race native lifecycle changes. *)
module Workflow_source = struct
  type t = Native.t
  type error = Native.error

  (** Drains one ready workflow activation through the supervisor mailbox. *)
  let try_poll_workflow supervisor =
    Native.perform supervisor Native.Try_poll_workflow

  (** Submits one semantic workflow completion through the supervisor mailbox.
  *)
  let complete_workflow supervisor completion =
    Native.perform supervisor (Native.Complete_workflow completion)

  (** Returns the stable classification used in adapter diagnostics. *)
  let error_code error = fst (native_error_view error)

  (** Returns the bounded diagnostic used in adapter diagnostics. *)
  let error_message error = snd (native_error_view error)
end

(** Activity operations use the same supervisor instance as workflow operations;
    a separate source module preserves the adapter's typed signatures. *)
module Activity_source = struct
  type t = Native.t
  type error = Native.error

  (** Drains one ready activity task through the supervisor mailbox. *)
  let try_poll_activity supervisor =
    Native.perform supervisor Native.Try_poll_activity

  (** Submits one semantic activity completion through the supervisor mailbox.
  *)
  let complete_activity supervisor completion =
    Native.perform supervisor (Native.Complete_activity completion)

  (** Completes an admitted asynchronous activity through the namespace-bound
      client path, never the worker task-token ledger. *)
  let complete_async_activity supervisor completion =
    Native.perform supervisor (Native.Complete_async_activity completion)

  (** Records progress for the currently leased activity through the same
      supervisor mailbox as polling and completion. *)
  let record_activity_heartbeat supervisor heartbeat =
    Native.perform supervisor (Native.Record_activity_heartbeat heartbeat)

  (** Records progress for an admitted asynchronous activity. *)
  let record_async_activity_heartbeat supervisor heartbeat =
    Native.perform supervisor (Native.Record_async_activity_heartbeat heartbeat)

  (** Returns the stable classification used in adapter diagnostics. *)
  let error_code error = fst (native_error_view error)

  (** Returns the bounded diagnostic used in adapter diagnostics. *)
  let error_message error = snd (native_error_view error)

  (** Only the bilateral retryable-completion status may authorize replaying a
      retained activity completion. The pinned Temporal Core revision consumes
      the activity lease before it reports generic completion transport errors,
      so [Connection] and [Not_ready] cannot safely be retried here: doing so
      could submit a completion twice. The pure policy deliberately fails closed
      for every status that does not prove the lease is still pending. *)
  let error_is_retryable = function
    | Native.Backend { Bridge.status; _ } ->
        Worker_policy.activity_completion_retryable status
    | _ -> false

  (** Unexpected supervisor exceptions are defects, not evidence of a safe
      transient transport failure. The adapter therefore retains them but the
      worker loop treats them as fatal unless a private test/source explicitly
      overrides this classification. *)
  let exception_is_retryable _exception = false
end

module Workflow = Workflow_adapter.Make (Workflow_source)
(** Instantiates the workflow adapter with the production supervisor source. *)

module Activity = Activity_adapter.Make (Activity_source)
(** Instantiates the activity adapter with the production supervisor source. *)

type workflow_registration = Workflow_adapter.registered_workflow
(** The hidden existential registrations retain each definition's codecs next to
    its implementation. This prevents a completion from being encoded through a
    different type witness than the input that was decoded. *)

type activity_registration = Activity_adapter.registered_activity

(** Converts one public signal handler into a private scheduler callback. The
    public handler intentionally accepts one typed value, while Temporal Core
    carries a repeated payload list; zero or multiple payloads therefore fail
    the workflow non-retryably instead of silently changing the input. *)
let runtime_signal_handler (handler : Signal.Handler.t) =
  let name = Signal.Handler.name handler in
  Workflow_adapter.make_signal_handler ~name ~dispatch:(fun signal ->
      match Workflow_adapter.signal_input signal with
      | [ payload ] ->
          Signal.Handler.dispatch handler (Payload_private.of_base payload)
          |> Result.map_error Error_private.to_base
      | _ ->
          Error
            (Base_error.make ~non_retryable:true ~category:`Workflow
               ~message:
                 (Printf.sprintf
                    "signal %s must contain exactly one payload for its \
                     registered OCaml handler"
                    name)
               ()))

(** Converts one public query handler into the private synchronous callback
    package. The handler owns the typed payload decoding boundary, so both
    output-only and one-input queries reject malformed arity without dropping
    data. The callback is executed inline on the worker owner Domain and
    cannot retain a workflow continuation. *)
let runtime_query_handler (handler : Query.Handler.t) =
  let name = Query.Handler.name handler in
  Workflow_adapter.make_query_handler ~name ~dispatch:(fun query ->
      Query.Handler.dispatch_payloads handler
        (List.map Payload_private.of_base
           (Workflow_adapter.query_arguments query))
      |> Result.map Payload_private.to_base
      |> Result.map_error Error_private.to_base)

(** Converts one public update handler into the private runtime callback. The
    public API currently accepts one input payload; an update activation with
    another arity is rejected without silently discarding Core data. *)
let runtime_update_handler (handler : Update.Handler.t) =
  let name = Update.Handler.name handler in
  Workflow_adapter.make_update_handler ~name
    ~dispatch:(fun ~run_validator ~on_validated update ->
      match Workflow_adapter.update_input update with
      | [ payload ] ->
          Update.Handler.dispatch ~run_validator ~on_validated handler
            (Payload_private.of_base payload)
          |> Result.map Payload_private.to_base
          |> Result.map_error Error_private.to_base
      | _ ->
          Error
            (Base_error.make ~non_retryable:true ~category:`Workflow
               ~message:
                 (Printf.sprintf
                    "update %s must contain exactly one payload for its \
                     registered OCaml handler"
                    name)
               ()))

(** Packs a workflow definition and its handlers for the private runtime
    adapter. The public [Worker] module validates names and duplicates before
    native resources are allocated. *)
let register_workflow ?(signals = []) ?(queries = []) ?(updates = []) definition
    =
  let signal_handlers = List.map runtime_signal_handler signals in
  let query_handlers = List.map runtime_query_handler queries in
  let update_handlers = List.map runtime_update_handler updates in
  Workflow_adapter.register ~signal_handlers ~query_handlers ~update_handlers
    definition

(** Packs an activity definition for [Activity.create]. *)
let register_activity definition = Activity_adapter.register definition

(** Packs an asynchronous activity definition for the deferred-completion
    adapter. Keeping this constructor separate prevents a synchronous callback
    from accidentally returning a handle that has no accepted lease. *)
let register_async_activity definition =
  Activity_adapter.register_async definition

(** Default native worker resource settings. They are deliberately explicit and
    stable so every worker has bounded Core resource usage even before a richer
    public options record is added. *)
let default_build_id = "ocaml-temporal"

let default_max_cached_workflows = 1_000
let default_max_outstanding_workflow_tasks = 1_000

(* Temporal Core requires at least two workflow-task pollers when workflow
   caching is enabled; the bridge validates the same invariant on both sides. *)
let default_max_concurrent_workflow_task_polls = 2
let default_graceful_shutdown_timeout_ms = 30_000L
let supervisor_capacity = 32

type t = {
  supervisor : Native.t;
  workflows : Workflow.t;
  activities : Activity.t;
  closed : bool Atomic.t;
  shutdown_retryable : bool Atomic.t;
      (** [true] while terminal native shutdown has not returned. Adapter maps
          and continuations must remain retained until that call returns [Ok] or
          [Error], because either result means the Rust runtime reached its
          force-release contract. *)
  terminal_cleanup_pending : bool Atomic.t;
      (** Prevents two finalizer or fallback threads from performing the same
          best-effort terminal retry concurrently. Native shutdown is
          idempotent, but serializing these retries keeps adapter discard
          ordering obvious. *)
  terminal_cleanup_scheduled : bool Atomic.t;
  run_mutex : Mutex.t;
      (** [Some domain] while [run] holds [run_mutex] on that Domain. Used to
          reject re-entrant [run]/[shutdown] from the same Domain (for example
          an activity implementation calling back into the worker) which would
          otherwise deadlock on the non-recursive mutex. *)
  run_domain : Domain.id option Atomic.t;
}
(** Native worker lifecycle state. The atomic flag is the only state observed by
    the polling loop from [shutdown]; adapter maps remain owner-confined to the
    run loop. [shutdown_retryable] distinguishes a failed adapter drain (where
    the native graph is still usable) from a native teardown failure (where
    reopening the public worker would only hide a terminal graph). *)

(** Reports worker lifecycle events without allowing a logging backend defect to
    alter lease ownership or shutdown ordering. *)
let report level ~operation ?error_kind () =
  try
    let tags = Observability.tags ~operation ?error_kind () in
    Observability.report ~src:Observability.Source.lifecycle level ~tags
      "native public worker event"
  with _ -> ()

(** Returns [true] only for the bounded readiness timeout. Other native errors
    must propagate because they may indicate a lost worker or connection. *)
let is_not_ready = function
  | Native.Backend { Bridge.status = Bridge.Not_ready; _ } -> true
  | _ -> false

(** Converts a successful adapter summary into progress. A rejected task has
    already been acknowledged with a failure completion and therefore must not
    stop the worker loop. *)
type progress = Worker_loop.progress = Progress | Not_ready | Retry_pending

(** Maps one workflow adapter poll and keeps only the scheduling information the
    outer loop needs. The adapter's detailed rejection is logged without copying
    a run ID into an error message. *)
let poll_workflow worker =
  match Workflow.poll worker.workflows with
  | Ok Workflow_adapter.Not_ready -> Ok Not_ready
  | Ok (Workflow_adapter.Completed _) -> Ok Progress
  | Ok (Workflow_adapter.Rejected { error; lease_retired = true; _ }) ->
      report Logs.Warning ~operation:"workflow_task_rejected"
        ~error_kind:error.code ();
      Ok Progress
  | Ok (Workflow_adapter.Rejected { error; lease_retired = false; _ }) ->
      Error (public_adapter_error "workflow task completion" error)
  | Error error -> Error (public_adapter_error "workflow task poll" error)

(** Maps one activity adapter poll using the same lease-retirement rule as the
    workflow path. An acknowledged activity failure is ordinary progress. *)
let poll_activity worker =
  match Activity.poll worker.activities with
  | Ok Activity_adapter.Not_ready -> Ok Not_ready
  | Ok (Activity_adapter.Completed _) -> Ok Progress
  | Ok (Activity_adapter.Rejected { error; lease_retired = true; _ }) ->
      report Logs.Warning ~operation:"activity_task_rejected"
        ~error_kind:error.code ();
      Ok Progress
  | Ok (Activity_adapter.Rejected { error; lease_retired = false; _ }) ->
      Error (public_activity_error "activity task completion" error)
  | Error { retryable = true; code; _ } ->
      report Logs.Warning ~operation:"activity_completion_retry"
        ~error_kind:code ();
      (* The adapter has retained the exact completion. Returning a scheduling
         result, rather than a fatal worker error, lets the generic loop apply
         its bounded activity-lane wait before retrying it. *)
      Ok Retry_pending
  | Error error -> Error (public_activity_error "activity task poll" error)

(** Waits on one bounded native readiness lane. The C bridge releases the OCaml
    runtime lock during this operation, and the bounded result lets [shutdown]
    regain the supervisor mailbox without waiting forever. *)
let wait_for_lane worker ~workflow_lane =
  let operation : unit Native.operation =
    if workflow_lane then Native.Wait_workflow else Native.Wait_activity
  in
  match Native.perform worker.supervisor operation with
  | Ok () -> Ok ()
  | Error error when is_not_ready error -> Ok ()
  | Error error -> Error (public_native_error "worker readiness wait" error)

(** Applies the bounded delay used after a retained activity completion. The
    native supervisor owns the timer operation and its C stub releases the OCaml
    runtime lock while sleeping, so this callback cannot block a workflow
    scheduler or let a ready-but-unrelated activity lane spin. A workflow retry
    is not currently produced by the workflow adapter; keeping that branch on
    the ordinary readiness path preserves a safe fallback if a future adapter
    adds one without also adding a workflow-specific native timer. *)
let retry_pending worker ~workflow_lane =
  if workflow_lane then wait_for_lane worker ~workflow_lane
  else
    match
      Native.perform worker.supervisor
        Native.Wait_activity_completion_retry_backoff
    with
    | Ok () -> Ok ()
    | Error _error when Atomic.get worker.closed -> Ok ()
    | Error error ->
        Error (public_native_error "activity completion retry backoff" error)

(** Runs one serialized worker loop. It alternates readiness lanes when both
    queues are empty so an activity-only workload cannot be starved by workflow
    waits (and vice versa). *)
let run worker =
  let self = Domain.self () in
  match Atomic.get worker.run_domain with
  | Some domain_id when domain_id = self ->
      Error
        (Base_error.defect
           ~message:
             "worker run is re-entrant on the same Domain; activity or host \
              code must not call Worker.run while a run loop is active")
  | _ ->
      Mutex.lock worker.run_mutex;
      Atomic.set worker.run_domain (Some self);
      Fun.protect
        ~finally:(fun () ->
          Atomic.set worker.run_domain None;
          Mutex.unlock worker.run_mutex)
        (fun () ->
          report Logs.Info ~operation:"worker_run_started" ();
          let result =
            Worker_loop.run
              ~closed:(fun () -> Atomic.get worker.closed)
              ~poll_workflow:(fun () -> poll_workflow worker)
              ~poll_activity:(fun () -> poll_activity worker)
              ~wait_for_lane:(fun ~workflow_lane ->
                wait_for_lane worker ~workflow_lane)
              ~retry_pending:(fun ~workflow_lane ->
                retry_pending worker ~workflow_lane)
          in
          report Logs.Info ~operation:"worker_run_finished" ();
          result)

(** Performs one best-effort terminal native cleanup attempt. A returned [Error]
    is still considered completion of the native release protocol:
    [Native.shutdown] always asks the supervisor to run [runtime_close], and the
    Rust bridge invalidates the runtime pointer even when Core reports an
    outstanding-task diagnostic. Only an exception before a result is returned
    leaves that guarantee unknown; in that case adapter maps stay retained and
    the pending flag keeps a later finalizer or retry thread responsible. *)
let terminal_cleanup_once worker =
  try
    let result = Native.shutdown worker.supervisor in
    (match result with
    | Ok () -> report Logs.Info ~operation:"worker_terminal_cleanup" ()
    | Error error ->
        let error_kind, _ = native_error_view error in
        report Logs.Error ~operation:"worker_terminal_cleanup_failed"
          ~error_kind ());
    (* The result, including [Error], proves the native graph has reached the
       force-release boundary. Only now may copied completions and paused
       workflow continuations be discarded. *)
    Workflow.discard worker.workflows;
    Activity.discard worker.activities;
    Atomic.set worker.terminal_cleanup_pending false;
    true
  with _ ->
    report Logs.Error ~operation:"worker_terminal_cleanup_failed"
      ~error_kind:"exception" ();
    false

(** Schedules a terminal cleanup retry without blocking the caller or a GC
    finalizer Domain. The worker value is captured by the helper thread, so its
    supervisor and adapter maps remain alive until the attempt returns. A failed
    thread creation leaves [terminal_cleanup_pending] set; the worker finalizer
    can make another attempt when the value is eventually abandoned. The pending
    flag is intentionally not cleared after an exception. *)
let schedule_terminal_cleanup worker =
  if Atomic.compare_and_set worker.terminal_cleanup_scheduled false true then
    match
      Thread.create
        (fun instance ->
          ignore (terminal_cleanup_once instance);
          Atomic.set instance.terminal_cleanup_scheduled false)
        worker
    with
    | _thread -> ()
    | exception _ -> Atomic.set worker.terminal_cleanup_scheduled false

(** Stops polling first, then waits for the loop mutex so no adapter-held lease
    remains when native worker shutdown begins. Adapter completion maps are
    drained while that mutex is held; native teardown is started only after both
    maps prove empty. If a drain fails, the graph remains usable and the caller
    can retry only when the activity adapter explicitly proved that the exact
    pending completion is still safe to submit. Other failures mark the public
    worker terminal and immediately force-release the native graph; this
    preserves the original adapter error without retaining Tokio/Core resources
    behind a worker value that can no longer be retried. A same-Domain admission
    defect is the exception: no teardown has started, so it remains retryable
    for a later call from another Domain. *)
let shutdown worker =
  let self = Domain.self () in
  match Atomic.get worker.run_domain with
  | Some domain_id when domain_id = self ->
      (* A same-Domain call cannot wait for [run_mutex] without deadlocking the
         loop that is making the call. Leave the private graph open and mark
         this admission failure retryable: the public wrapper reopens its
         admission flag, and a later call from another Domain can perform the
         ordinary drain-then-native-shutdown path once the active loop exits.

         Critically, this branch must NOT write [worker.closed]. It never set
         [closed] to [true] (it returns before the gate below), so any write
         could only undo a [true] published by a concurrent [shutdown] on
         another Domain -- clearing the stop request and stranding the loop,
         which then holds [run_mutex] forever and deadlocks that caller. The
         policy fixes the action to [Leave_unchanged] for exactly this reason. *)
      let closed_action, shutdown_retryable =
        Worker_policy.reentrant_same_domain_shutdown
      in
      (match closed_action with
      | Worker_policy.Leave_unchanged -> ()
      | Worker_policy.Write value -> Atomic.set worker.closed value);
      Atomic.set worker.shutdown_retryable shutdown_retryable;
      Error
        (Base_error.defect
           ~message:
             "cannot shut down a worker from inside its run loop on the same \
              Domain; that would deadlock the run mutex")
  | _ ->
      if Atomic.compare_and_set worker.closed false true then begin
        Mutex.lock worker.run_mutex;
        let drained =
          Fun.protect
            ~finally:(fun () -> Mutex.unlock worker.run_mutex)
            (fun () ->
              match Workflow.drain worker.workflows with
              | Error error ->
                  Error
                    ( Worker_policy.Workflow_drain,
                      public_adapter_error "workflow completion drain" error )
              | Ok () -> (
                  match Activity.drain worker.activities with
                  | Ok () -> Ok ()
                  | Error ({ retryable; _ } as error) ->
                      Error
                        ( Worker_policy.Activity_drain retryable,
                          public_activity_error "activity completion drain"
                            error )))
        in
        match drained with
        | Error (failure_kind, error) ->
            (* The native graph has not been touched. Reopen admission only when
           the activity adapter proved that the retained completion is safe to
           retry. A workflow drain or permanent activity error cannot be
           retried safely, so close public admission and dispose the native
           graph immediately. [Native.shutdown] force-completes any leases
           still held by Core before dropping Tokio and the runtime; the
           adapter's original error remains the result returned to the caller. *)
            let retryable = Worker_policy.shutdown_retryable failure_kind in
            Atomic.set worker.shutdown_retryable retryable;
            Atomic.set worker.closed (not retryable);
            if Worker_policy.needs_native_cleanup failure_kind then begin
              Atomic.set worker.terminal_cleanup_pending true;
              let report_cleanup_error native_error =
                let error_kind, _ = native_error_view native_error in
                report Logs.Error ~operation:"worker_terminal_cleanup_failed"
                  ~error_kind ()
              in
              let report_cleanup_exception _exception =
                report Logs.Error ~operation:"worker_terminal_cleanup_failed"
                  ~error_kind:"exception" ()
              in
              let cleanup_returned, _original_error =
                Worker_policy.retain_original_error
                  ~cleanup:(fun () -> Native.shutdown worker.supervisor)
                  ~on_cleanup_error:report_cleanup_error
                  ~on_cleanup_exception:report_cleanup_exception error
              in
              if cleanup_returned then begin
                (* Native shutdown has force-retired every Core lease before these
               adapter maps are cleared. Keeping this ordering means a copied
               completion is never silently discarded while Rust still expects
               its acknowledgement. Both adapter mutexes are acquired only
               after [run_mutex] was released above, so no run can race this
               terminal disposal. *)
                Workflow.discard worker.workflows;
                Activity.discard worker.activities;
                Atomic.set worker.terminal_cleanup_pending false
              end
              else
                (* An exception means the supervisor contract did not return its
               release result. Keep every adapter lease and arrange a detached
               retry; the worker remains closed to new polling, but cleanup is
               still live rather than being hidden behind [closed]. *)
                schedule_terminal_cleanup worker
            end;
            Error error
        | Ok () -> (
            Atomic.set worker.shutdown_retryable false;
            try
              let native_result = Native.shutdown worker.supervisor in
              (* [Native.shutdown] always asks the supervisor to run
              [runtime_close], and the bridge invalidates the runtime pointer
              on both [Ok] and [Error] (Core force-retires every lease even
              when it reports an outstanding-task diagnostic). That includes
              any execution still blocked awaiting an activity or timer, which
              lives in [adapter.runs], not [adapter.pending], so the earlier
              drain above never touched it. Discarding here -- on either
              result -- shuts down every remaining scheduler and one-shot
              continuation deterministically instead of leaving them for a
              later GC cycle, matching [terminal_cleanup_once] below. Only the
              exception path leaves the release outcome unproven and must keep
              the adapters retained. *)
              Workflow.discard worker.workflows;
              Activity.discard worker.activities;
              match native_result with
              | Ok () as result ->
                  report Logs.Info ~operation:"worker_shutdown" ();
                  result
              | Error error ->
                  Error (public_native_error "worker shutdown" error)
            with _ ->
              (* [Native.shutdown] normally contains owner-domain and bridge
              failures in its typed result. If an unexpected mailbox or
              mutex exception escapes before that result is returned, retain
              the already-drained adapters and make the same detached native
              cleanup path responsible for the retry. *)
              Atomic.set worker.terminal_cleanup_pending true;
              report Logs.Error ~operation:"worker_shutdown_failed"
                ~error_kind:"exception" ();
              schedule_terminal_cleanup worker;
              Error
                (Base_error.defect
                   ~message:
                     "native worker shutdown raised before releasing the \
                      runtime; a cleanup retry was scheduled"))
      end
      else Ok ()

(** Schedules forgotten-worker cleanup off the GC finalizer thread. A finalizer
    must not block on [run_mutex] or the supervisor mailbox; the detached thread
    runs the ordinary drain-then-shutdown path. If an earlier terminal cleanup
    raised before returning a native result, the pending flag instead schedules
    the narrow native retry path and keeps adapter maps retained until that path
    returns. If a system thread cannot be created during process teardown, the
    native custom-block finalizer remains the last-resort reclaim mechanism
    rather than discarding a still-owned lease. *)
let cleanup_abandoned worker =
  if Atomic.get worker.terminal_cleanup_pending then
    schedule_terminal_cleanup worker
  else if not (Atomic.get worker.closed) then
    (* Keep [worker] (and therefore [supervisor]) reachable for the lifetime of
       the cleanup thread so the supervisor's own finalizer cannot tear the
       native graph down before drain completes. The thread owns this root. *)
    match
      Thread.create
        (fun instance ->
          try ignore (shutdown instance)
          with _ ->
            Atomic.set instance.closed true;
            Atomic.set instance.terminal_cleanup_pending true;
            schedule_terminal_cleanup instance)
        worker
    with
    | _thread -> ()
    | exception _ ->
        (* Cannot spawn a helper. Do not block the finalizer Domain on the
           mailbox owner. Mark closed and request a terminal retry path without
           awaiting drain; residual native reclaim still goes through the
           runtime custom-block finalizer. *)
        Atomic.set worker.closed true;
        Atomic.set worker.terminal_cleanup_pending true;
        schedule_terminal_cleanup worker

(** Builds the native graph and both OCaml registries. Every failure after
    [Native.create] enters [cleanup], which joins the supervisor owner Domain
    and closes all native resources before returning. Successful construction
    attaches a GC finalizer so abandoned workers still drain leases. *)
let create ?max_cached_workflows ?(versioning = Bridge.No_versioning) ~target_url ~namespace
    ~identity ~task_queue ~workflows ~activities () =
  let max_cached_workflows =
    Option.value max_cached_workflows ~default:default_max_cached_workflows
  in
  let build_id =
    match versioning with
    | Bridge.No_versioning -> default_build_id
    | Bridge.Legacy_build_id build_id -> build_id
    | Bridge.Deployment_based { build_id; _ } -> build_id
  in
  let { Observer.on_activation; on_completion } = Observer.current () in
  let* client_config =
    Native.client_config ~target_url ~identity
    |> Result.map_error (public_bridge_error "client configuration")
  in
  let* worker_config =
    Native.worker_config ~namespace ~task_queue ~build_id ~versioning
      ~max_cached_workflows
      ~max_outstanding_workflow_tasks:default_max_outstanding_workflow_tasks
      ~max_concurrent_workflow_task_polls:
        default_max_concurrent_workflow_task_polls
      ~graceful_shutdown_timeout_ms:default_graceful_shutdown_timeout_ms ()
    |> Result.map_error (public_bridge_error "worker configuration")
  in
  let* supervisor =
    Native.create ~capacity:supervisor_capacity ()
    |> Result.map_error (public_native_error "native runtime creation")
  in
  let cleanup error =
    ignore (Native.shutdown supervisor);
    Error error
  in
  let setup =
    let* () =
      Native.perform supervisor (Native.Connect_client client_config)
      |> Result.map_error (public_native_error "client connection")
    in
    let* () =
      Native.perform supervisor (Native.Start_worker worker_config)
      |> Result.map_error (public_native_error "worker startup")
    in
    let* workflows =
      Workflow.create ?on_activation ?on_completion ~task_queue ~supervisor
        ~workflows ()
      |> Result.map_error (public_adapter_error "workflow registration")
    in
    let* activities =
      Activity.create ~supervisor ~activities
      |> Result.map_error (public_activity_error "activity registration")
    in
    Ok
      {
        supervisor;
        workflows;
        activities;
        closed = Atomic.make false;
        shutdown_retryable = Atomic.make false;
        terminal_cleanup_pending = Atomic.make false;
        terminal_cleanup_scheduled = Atomic.make false;
        run_mutex = Mutex.create ();
        run_domain = Atomic.make None;
      }
  in
  match setup with
  | Ok worker ->
      (* Explicit [shutdown] is the supported path. The finalizer is a last
         resort for abandoned workers: it schedules the same drain-then-native
         teardown so GC of a live [t] cannot leave Core leases without an
         OCaml completion document. *)
      Gc.finalise cleanup_abandoned worker;
      Ok worker
  | Error error -> cleanup error

(** Reports whether the most recent shutdown failure occurred before native
    teardown. The public wrapper uses this private state to reopen its own
    admission flag only for a safe adapter-drain retry. *)
let shutdown_retryable worker = Atomic.get worker.shutdown_retryable

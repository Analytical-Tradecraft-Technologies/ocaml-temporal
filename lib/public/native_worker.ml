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
module Owner = Temporal_sdk_kernel.Native_worker_owner
module Observer = Temporal_sdk_kernel.Native_worker_observer
module Watchdog = Temporal_sdk_kernel.Native_worker_watchdog
module Bounded_shutdown = Temporal_sdk_kernel.Native_worker_shutdown

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
  | Async_heartbeat_rejected -> "async_heartbeat_rejected"
  | Resource_exhausted -> "resource_exhausted"
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
  | Native.Owner_unavailable exception_ ->
      let message =
        try Printexc.to_string exception_
        with _ -> "unprintable Domain spawn exception"
      in
      ("owner_unavailable", bounded_message message)
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

  (** Submits one workflow completion, already encoded once by the adapter,
      through the supervisor mailbox. Only the encoded bytes are submitted; the
      typed [completion] exists for test sources and is ignored here. *)
  let complete_workflow supervisor ~completion:_ encoded =
    Native.perform supervisor (Native.Complete_workflow encoded)

  (** Returns the stable classification used in adapter diagnostics. *)
  let error_code error = fst (native_error_view error)

  (** Returns the bounded diagnostic used in adapter diagnostics. *)
  let error_message error = snd (native_error_view error)

  (** No workflow completion failure is retryable. The Rust bridge defines no
      retryable workflow-completion status: pinned Core reports only
      deterministic validation failures from [complete_workflow_activation],
      which an identical resubmission would repeat, and every other failure
      (lifecycle, mailbox, or an acknowledgement lost after acceptance) cannot
      prove that the run's lease is still outstanding. A retained workflow
      completion is therefore never resubmitted (issue #843). *)
  let error_is_retryable (_ : error) = false

  (** A raised completion is an uncertain acknowledgement; see
      [error_is_retryable]. *)
  let exception_is_retryable (_ : exn) = false
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

  (** Namespace-bound async heartbeats and complete/fail/cancel address the
      server by task token and never consume a Core worker lease, so they do
      not use [error_is_retryable]'s fail-closed worker-completion policy. A
      rejected request leaves its activity live and an uncertain RPC keeps the
      lease; only token loss or an unusable native graph retires the handle. *)
  let async_operation_error_disposition = function
    | Native.Backend { Bridge.status; _ } ->
        Worker_policy.async_operation_disposition status
    | _ -> Worker_policy.Retired

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

(** Converts one public signal handler into a private scheduler callback.
    [Signal.Handler.dispatch_payloads] owns the payload-arity policy: zero
    payloads decode as unit and multiple payloads are a [`Codec] error, which
    fails the workflow task (the v1 fail-closed signal policy, #811) instead
    of silently changing the input or closing the run. *)
let runtime_signal_handler (handler : Signal.Handler.t) =
  let name = Signal.Handler.name handler in
  Workflow_adapter.make_signal_handler ~name ~dispatch:(fun signal ->
      Workflow_adapter.signal_input signal
      |> List.map Payload_private.of_base
      |> Signal.Handler.dispatch_payloads handler
      |> Result.map_error Error_private.to_base)

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

(** Converts one public update handler into the private runtime callback.
    [Update.Handler.dispatch_payloads] owns the payload-arity policy: zero
    payloads (a no-argument update from the CLI, Web UI, or another SDK)
    decode as the canonical unit payload, and more than one is rejected
    without silently discarding Core data. *)
let runtime_update_handler (handler : Update.Handler.t) =
  let name = Update.Handler.name handler in
  Workflow_adapter.make_update_handler ~name
    ~dispatch:(fun ~run_validator ~on_validated update ->
      Workflow_adapter.update_input update
      |> List.map Payload_private.of_base
      |> Update.Handler.dispatch_payloads ~run_validator ~on_validated handler
      |> Result.map Payload_private.to_base
      |> Result.map_error Error_private.to_base)

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
    stable so every worker has bounded Core resource usage; the public
    [Worker.Options] layer documents the same values as its defaults and may
    override each one (#498). *)
let default_build_id = "ocaml-temporal"

let default_max_cached_workflows = 1_000
let default_max_outstanding_workflow_tasks = 1_000

(* Temporal Core requires at least two workflow-task pollers when workflow
   caching is enabled; the bridge validates the same invariant on both sides. *)
let default_max_concurrent_workflow_task_polls = 2
let default_graceful_shutdown_timeout_ms = 30_000L

(* How long [shutdown] waits for the native release after the grace period
   (#495). It covers a normal teardown, including one server long poll, while
   the bridge's own fixed bounds still end a pathological one later. *)
let default_shutdown_teardown_timeout_ms = 60_000L
let supervisor_capacity = 32

type t = {
  supervisor : Native.t;
  workflows : Workflow.t;
  activities : Activity.t;
  workflow_tasks : bool;
      (** [false] for an activity-only worker. Core then runs no workflow
          poller and the Rust workflow lane stays permanently idle, so the run
          loop skips polling it (#805). The combined readiness wait never
          wakes for that idle lane, so it needs no special case there. *)
  closed : bool Atomic.t;
  stop_requested : bool Atomic.t;
      (** Sticky, non-blocking request for the run loop to return (#830). It
          is separate from [closed] because it does not admit teardown: the
          loop exits, but drain and native release still belong to a later
          [shutdown], whose [closed] compare-and-set must still succeed. Only
          [request_stop] writes it, with a single [Atomic.set], so a signal
          handler may set it on any Domain, including the run loop's own
          thread. *)
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
      (** Held until both execution Domains leave their adapters. Native
          teardown must not overtake an activity callback or workflow task. *)
  owner : Owner.t;
      (** The system threads executing workflow activations and activity
          callbacks. Tracking the exact threads, not their Domains, rejects a
          callback that would wait for its own lane while still admitting a
          sibling system thread on the same Domain (#763). *)
  activation_deadline_ms : int option;
      (** Deadline for one workflow activation to return to the adapter, or
          [None] when the non-yielding-code watchdog is disabled (#493). *)
  graceful_shutdown_s : float;
      (** Seconds a stopping lane may keep running user code before shutdown
          abandons it (#495); the same period Core receives. *)
  teardown_timeout_s : float;
      (** Seconds [shutdown] waits for the native release after the grace
          period before reporting it detached (#495). *)
  lanes_deadline : float option Atomic.t;
      (** The absolute time, on [Unix.gettimeofday], by which every lane must
          have returned once a stop was observed. Published once, by
          whichever of [shutdown] and a stopping [run] computes it first, so
          both use the same deadline. *)
  activity_detached : bool Atomic.t;
      (** Set when a [run] returned without joining its activity Domain
          because a callback outlived [lanes_deadline]. That callback still
          holds the activity adapter's lock. *)
  discard_pending : Bounded_shutdown.Deferred_discard.t;
      (** Raised once the native release returned, cleared by the first
          successful discard of both adapters' copied state. When abandoned
          code still held an adapter lock, the discard is retried by the next
          thread to release one: the detached activity lane after its
          callback returns (see [run_lanes]), or [run] after its workflow
          lane returns. *)
}
(** Native worker lifecycle state. The [closed] and [stop_requested] atomics
    are the only state observed by the polling lanes from [shutdown] and
    [request_stop]. Each adapter protects its own maps; the
    lifecycle mutex prevents their drain from overlapping either execution
    lane. [shutdown_retryable] distinguishes a failed adapter drain (where the
    native graph is still usable) from a native teardown failure (where
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
  (* An activity-only worker has no workflow poller in Core; the Rust lane can
     never produce work, so avoid a supervisor round trip per loop turn. *)
  if not worker.workflow_tasks then Ok Not_ready
  else
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

(** Uses native event readiness only for the one idle lane holding the wait
    token. When its sibling is busy or already waiting, a 10 ms local yield
    avoids occupying the sole supervisor owner. The next poll checks the Rust
    queue; when both lanes become idle, they take turns holding the token.

    The native wait itself observes both lanes ([Wait_any]), whichever lane
    holds the token. A lane-specific wait held the sole supervisor owner for
    the full bounded timeout while a task sat on the other lane, so every step
    of a sequential activity workflow paid one dead wait (#806). A sibling
    lane's poll queued in the mailbox during the wait runs as soon as it ends.
    The wait does not consume the task, so the owning lane still takes it; this
    also covers an activity-only worker, whose idle workflow lane never wakes
    it. [workflow_lane] therefore matters only for the local yield. *)
let wait_for_lane worker ~workflow_lane:_ ~native_wait =
  if not native_wait then begin
    Thread.delay 0.01;
    Ok ()
  end
  else
    match Native.perform worker.supervisor Native.Wait_any with
    | Ok () -> Ok ()
    | Error error when is_not_ready error -> Ok ()
    | Error error -> Error (public_native_error "worker readiness wait" error)

(** Applies the bounded delay used after a retained activity completion. The
    native supervisor owns the timer operation and its C stub releases the OCaml
    runtime lock while sleeping, so this callback cannot block a workflow
    scheduler or let a ready-but-unrelated activity lane spin. A workflow retry
    is not currently produced by the workflow adapter; its bounded local yield
    remains a fallback if a future adapter adds one without a dedicated timer. *)
let retry_pending worker ~workflow_lane =
  if workflow_lane then wait_for_lane worker ~workflow_lane ~native_wait:false
  else
    match
      Native.perform worker.supervisor
        Native.Wait_activity_completion_retry_backoff
    with
    | Ok () -> Ok ()
    | Error _error when Atomic.get worker.closed -> Ok ()
    | Error error ->
        Error (public_native_error "activity completion retry backoff" error)

(** Detects a call from either execution lane's system thread before a
    lifecycle mutex is acquired. A callback could otherwise wait on a shutdown
    which in turn waits for that callback's lane to return. Another system
    thread, including one on a lane's Domain, is not re-entrant: it blocks on
    [run_mutex] with the runtime lock released while the lane exits. *)
let is_execution_thread worker = Owner.is_execution_thread worker.owner

(** Asks the run loop to return at its next stop check without waiting for it
    (#830). This is one [Atomic.set] on a preallocated cell: it takes no lock,
    performs no I/O, and does not touch the supervisor, so it is safe from an
    OCaml signal handler running at a safe point on any Domain, including the
    workflow lane's own thread. Teardown is deliberately not started here; the
    caller runs [shutdown] after [run] returns. *)
let request_stop worker = Atomic.set worker.stop_requested true

(** The lanes' stop predicate: an admitted shutdown or a stop request. *)
let stop_observed worker =
  Atomic.get worker.closed || Atomic.get worker.stop_requested

(** Starts the activation watchdog for one run, or returns [None] when it is
    disabled or this worker polls no workflow tasks. The watchdog only reads
    the adapter's running epoch and, past the deadline, fails that activation's
    workflow task through the supervisor; it never enters workflow code. *)
let start_watchdog worker =
  match worker.activation_deadline_ms with
  | None -> Ok None
  | Some _ when not worker.workflow_tasks -> Ok None
  | Some deadline_ms -> (
      match
        Watchdog.start ~deadline_ms
          ~running_epoch:(fun () -> Workflow.running_epoch worker.workflows)
          ~abandon:(fun ~epoch ~elapsed_ms ->
            ignore
              (Workflow.abandon_activation worker.workflows ~epoch ~elapsed_ms))
      with
      | Ok watchdog -> Ok (Some watchdog)
      | Error exception_ ->
          let message =
            try Printexc.to_string exception_
            with _ -> "unprintable Domain spawn exception"
          in
          Error
            (Base_error.make ~category:`Bridge
               ~message:
                 ("workflow activation watchdog could not start: "
                 ^ bounded_message message)
               ()))

(** Returns the shared lanes deadline, publishing [now + grace period] if no
    caller has published one yet. The compare-and-set makes the first writer
    win, so [shutdown] and a stopping [run] agree on one deadline. *)
let lanes_deadline worker =
  match Atomic.get worker.lanes_deadline with
  | Some deadline -> deadline
  | None ->
      let deadline = Unix.gettimeofday () +. worker.graceful_shutdown_s in
      if Atomic.compare_and_set worker.lanes_deadline None (Some deadline) then
        deadline
      else
        Option.value (Atomic.get worker.lanes_deadline) ~default:deadline

(** Bounds the run loop's join of its activity Domain (#495). After a stop,
    the deadline is the shared lanes deadline. After a lane failure without a
    stop, a private deadline one grace period away is used and not
    published, so it cannot shorten the grace period of a later shutdown. *)
let activity_detach worker =
  {
    Worker_loop.now = Unix.gettimeofday;
    deadline =
      (fun () ->
        if stop_observed worker then lanes_deadline worker
        else Unix.gettimeofday () +. worker.graceful_shutdown_s);
    on_detached =
      (fun () ->
        Atomic.set worker.activity_detached true;
        report Logs.Warning ~operation:"activity_lane_detached" ());
  }

(** Discards both adapters' copied state if neither lock is held. Returns
    [true] when both were discarded. Never waits, so it is safe while an
    abandoned callback or activation still holds an adapter lock. *)
let try_discard_adapters worker =
  let workflow_discarded = Workflow.try_discard worker.workflows in
  let activity_discarded = Activity.try_discard worker.activities in
  workflow_discarded && activity_discarded

(** Starts the deferred discard after the native release returned. *)
let request_discard worker =
  Bounded_shutdown.Deferred_discard.request worker.discard_pending
    ~try_discard:(fun () -> try_discard_adapters worker)

(** Retries a pending discard after the calling lane released its adapter
    lock; a no-op unless the release left one pending. *)
let retry_discard worker =
  Bounded_shutdown.Deferred_discard.retry worker.discard_pending
    ~try_discard:(fun () -> try_discard_adapters worker)

(** Runs both lanes through the generic loop and converts an escaped lane
    exception into a defect result. *)
let run_lanes worker =
  try
    Worker_loop.run ~detach:(Some (activity_detach worker))
      ~closed:(fun () -> stop_observed worker)
      ~poll_workflow:(fun () -> poll_workflow worker)
      ~poll_activity:(fun () ->
        Owner.enter_activity worker.owner;
        (* The poll holds the activity adapter lock while it runs a callback.
           Once it returns, that lock is free, so a discard the native
           release had to skip because this callback was abandoned can now
           run, here on the lane's own Domain (#495). *)
        Fun.protect
          ~finally:(fun () -> retry_discard worker)
          (fun () -> poll_activity worker))
      ~wait_for_lane:(fun ~workflow_lane ~native_wait ->
        wait_for_lane worker ~workflow_lane ~native_wait)
      ~retry_pending:(fun ~workflow_lane -> retry_pending worker ~workflow_lane)
  with _ ->
    Error (Base_error.defect ~message:"native worker execution lane failed")

(** Runs workflow execution on this Domain and capacity-one activity execution
    on a dedicated Domain. Both adapters continue to use the same serialized
    supervisor mailbox. [run_mutex] remains held until the activity Domain is
    joined, so later shutdown can drain and release the graph safely, unless
    a callback outlives the lanes deadline after a stop: the Domain is then
    detached (#495) and shutdown treats the activity adapter as busy. When
    enabled, a watchdog Domain observes the workflow lane for the duration of
    the run and is joined before [run_mutex] is released. A configured
    watchdog that cannot start fails the run rather than silently running
    unguarded. *)
let run worker =
  if is_execution_thread worker then
      Error
        (Base_error.defect
           ~message:
             "worker run is re-entrant on an execution thread; activity or \
              workflow code must not call Worker.run while its run loop is \
              active")
  else begin
      Mutex.lock worker.run_mutex;
      Owner.enter_run worker.owner;
      Fun.protect
        ~finally:(fun () ->
          (* A detached activity Domain is still an execution thread. *)
          if Atomic.get worker.activity_detached then Owner.leave_run worker.owner
          else Owner.leave worker.owner;
          (* The workflow lane has returned, so a discard the native release
             had to skip can now proceed for it (and for the activity
             adapter too, unless its callback is still detached). *)
          retry_discard worker;
          Mutex.unlock worker.run_mutex)
        (fun () ->
          report Logs.Info ~operation:"worker_run_started" ();
          let result =
            match start_watchdog worker with
            | Error _ as error -> error
            | Ok watchdog ->
                Fun.protect
                  ~finally:(fun () -> Option.iter Watchdog.stop watchdog)
                  (fun () -> run_lanes worker)
          in
          report Logs.Info ~operation:"worker_run_finished" ();
          result)
  end

(** The sticky watchdog report of the first workflow activation abandoned for
    exceeding its deadline, or [None] while the worker is healthy. *)
let stuck_activation worker = Workflow.stuck worker.workflows

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
       workflow continuations be discarded; an adapter still held by
       abandoned code is left to the next [run] exit. *)
    request_discard worker;
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

(** Releases the native graph on the bounded-shutdown thread and classifies
    the result for {!Bounded_shutdown.run}. [Native.shutdown] always asks the
    supervisor to run [runtime_close], and the bridge invalidates the runtime
    pointer on both [Ok] and [Error] (Core force-retires every lease,
    including one still held by an abandoned callback or activation).
    Discarding after either result shuts down every remaining scheduler and
    continuation deterministically; an adapter whose lock abandoned code
    still holds is left pending in [discard_pending]. Only an
    exception leaves the release outcome unproven: the adapters are then
    retained and the detached terminal-cleanup path becomes responsible for
    the retry. *)
let release_native worker =
  match Native.shutdown worker.supervisor with
  | exception exception_ ->
      Atomic.set worker.terminal_cleanup_pending true;
      report Logs.Error ~operation:"worker_shutdown_failed"
        ~error_kind:"exception" ();
      schedule_terminal_cleanup worker;
      raise exception_
  | result -> (
      request_discard worker;
      match result with
      | Ok () -> Bounded_shutdown.Released
      | Error
          (Native.Backend { Bridge.status = Bridge.Outstanding_tasks; _ } as
           error) ->
          Bounded_shutdown.Released_retiring_leases
            (public_native_error "worker shutdown" error)
      | Error error ->
          let error_kind, _ = native_error_view error in
          report Logs.Error ~operation:"worker_shutdown_failed" ~error_kind ();
          Bounded_shutdown.Release_failed
            (public_native_error "worker shutdown" error))

(** Maps a non-blocking workflow drain. A workflow drain failure is never
    retried at shutdown: the adapter only resubmits a completion whose own
    failure was classified retryable, and reports anything else unchanged. *)
let drain_workflow worker () =
  match Workflow.try_drain worker.workflows with
  | None -> Bounded_shutdown.Busy
  | Some (Ok ()) -> Bounded_shutdown.Drained
  | Some (Error error) ->
      Bounded_shutdown.Drain_failed
        {
          error = public_adapter_error "workflow completion drain" error;
          retryable = false;
        }

(** Maps a non-blocking activity drain, keeping the adapter's own
    retryability classification for the exact retained completion. *)
let drain_activity worker () =
  match Activity.try_drain worker.activities with
  | None -> Bounded_shutdown.Busy
  | Some (Ok ()) -> Bounded_shutdown.Drained
  | Some (Error ({ retryable; _ } as error)) ->
      Bounded_shutdown.Drain_failed
        {
          error = public_activity_error "activity completion drain" error;
          retryable;
        }

(** The worker's effects for {!Bounded_shutdown.run}. The lifecycle lock is
    [run_mutex], taken and released by the shutdown thread. The probes read
    atomics or the activity adapter's short delivery lock only. *)
let shutdown_operations worker =
  {
    Bounded_shutdown.try_acquire_lanes =
      (fun () -> Mutex.try_lock worker.run_mutex);
    release_lanes = (fun () -> Mutex.unlock worker.run_mutex);
    activity_lane_detached = (fun () -> Atomic.get worker.activity_detached);
    activity_callback_running =
      (fun () -> Activity.callback_running worker.activities);
    workflow_activation_in_flight =
      (fun () -> Workflow.activation_in_flight worker.workflows);
    drain_workflow = drain_workflow worker;
    drain_activity = drain_activity worker;
    outstanding_async_leases =
      (fun () ->
        Option.value ~default:1
          (Activity.outstanding_async_leases worker.activities));
    async_leases_error =
      (fun count ->
        Base_error.make ~category:`Bridge
          ~message:
            (Printf.sprintf
               "activity completion drain failed: %d asynchronous activity \
                completion(s) remained admitted when the grace period ended; \
                their handles were closed with the worker"
               count)
          ());
    release = (fun () -> release_native worker);
    exception_error =
      (fun _exception ->
        Base_error.defect ~message:"worker completion drain raised");
  }

(** The report for a call that found shutdown already admitted elsewhere.
    The public wrapper caches the admitted call's own report, so only a
    racing finalizer-scheduled cleanup can observe this value. *)
let already_shut_down =
  {
    Bounded_shutdown.elapsed_s = 0.;
    lanes_stopped = true;
    abandoned_activity_callbacks = 0;
    abandoned_workflow_activations = 0;
    teardown = Bounded_shutdown.Completed;
  }

(** Logs what a bounded shutdown left behind. No identifier is included:
    the abandonment kind and the teardown state are the whole diagnostic. *)
let report_shutdown (shutdown_report : Bounded_shutdown.report) =
  if not shutdown_report.lanes_stopped then
    report Logs.Warning ~operation:"worker_shutdown_abandoned_work"
      ~error_kind:
        (if shutdown_report.abandoned_activity_callbacks > 0 then
           "activity_callback"
         else if shutdown_report.abandoned_workflow_activations > 0 then
           "workflow_activation"
         else "native_call")
      ();
  match shutdown_report.teardown with
  | Bounded_shutdown.Completed ->
      report Logs.Info ~operation:"worker_shutdown" ()
  | Bounded_shutdown.Detached ->
      report Logs.Warning ~operation:"worker_shutdown_teardown_detached" ()

(** Closes admission, then runs the bounded shutdown sequence (#495): wait
    until the lanes deadline for the run loop to stop, drain retained
    completions, and release the native graph, all on one dedicated thread
    that owns those steps, while this caller waits at most the teardown
    timeout for the release. The caller therefore returns within the grace
    period plus the teardown timeout (plus a fraction of a second) whatever
    user code or the server does; see {!Bounded_shutdown} for the ownership
    rules and the outcome classification.

    Every admitted call is terminal: the graph is released, or its release is
    owned by the shutdown thread or the detached terminal-cleanup path. An
    execution-thread admission defect is the exception: no teardown has
    started, so it remains retryable for a later call from any other thread.
    That call also posts a stop request so the loop returns and the same
    thread, or any other, can then complete shutdown (#830). *)
let shutdown worker =
  if is_execution_thread worker then begin
    (* A call from a lane's own thread cannot wait for its own loop. Leave the
       private graph open and mark this admission failure retryable: the
       public wrapper reopens its admission flag, and a later call from any
       other thread (on this or another Domain) can perform the ordinary
       bounded shutdown once the active loop exits.

       Critically, this branch must NOT write [worker.closed]. It never set
       [closed] to [true] (it returns before the gate below), so any write
       could only undo a [true] published by a concurrent [shutdown] on
       another thread -- clearing the stop request and stranding the loop.
       The policy fixes the action to [Leave_unchanged] for exactly this
       reason. The separate [stop_requested] flag is only ever set to [true],
       so posting it here cannot undo another caller's request (#830). *)
    request_stop worker;
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
           "cannot shut down a worker from inside its own run loop thread; a \
            stop was requested instead, so call shutdown again after run \
            returns")
  end
  else if Atomic.compare_and_set worker.closed false true then begin
    Atomic.set worker.shutdown_retryable false;
    let outcome =
      Bounded_shutdown.run ~lanes_deadline:(lanes_deadline worker)
        ~teardown_timeout_s:worker.teardown_timeout_s
        (shutdown_operations worker)
    in
    match outcome with
    | Bounded_shutdown.Shut_down shutdown_report ->
        report_shutdown shutdown_report;
        Ok shutdown_report
    | Bounded_shutdown.Completion_lost { error; report = shutdown_report }
    | Bounded_shutdown.Release_error { error; report = shutdown_report } ->
        report_shutdown shutdown_report;
        Error error
    | Bounded_shutdown.Release_unproven _ ->
        Error
          (Base_error.defect
             ~message:
               "native worker shutdown raised before releasing the runtime; a \
                cleanup retry was scheduled")
  end
  else Ok already_shut_down

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
let create ?max_cached_workflows
    ?(max_outstanding_workflow_tasks = default_max_outstanding_workflow_tasks)
    ?(max_concurrent_workflow_task_polls =
      default_max_concurrent_workflow_task_polls)
    ?(graceful_shutdown_timeout_ms = default_graceful_shutdown_timeout_ms)
    ?(shutdown_teardown_timeout_ms = default_shutdown_teardown_timeout_ms)
    ?tuning ?io_threads ?runtime ?(versioning = Bridge.No_versioning)
    ?activation_deadline_ms ~target_url ~namespace ~identity ~task_queue
    ~workflows ~activities () =
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
  (* Core polls only the task kinds this worker can execute (#805). A worker
     that registers no activities must not take activity tasks from a shared
     task queue, where a sibling worker could have run them, only to fail them
     as unregistered; likewise for workflows. A worker with neither could
     never make progress, so it is rejected before any native allocation. *)
  let workflow_tasks = workflows <> [] in
  let activity_tasks = activities <> [] in
  let* client_config =
    Native.client_config ~target_url ~identity
    |> Result.map_error (public_bridge_error "client configuration")
  in
  let* () =
    if workflow_tasks || activity_tasks then Ok ()
    else
      Error
        (Base_error.defect
           ~message:"worker must register at least one workflow or activity")
  in
  let* worker_config =
    Native.worker_config ~namespace ~task_queue ~build_id ~versioning
      ~max_cached_workflows ~max_outstanding_workflow_tasks
      ~max_concurrent_workflow_task_polls ~graceful_shutdown_timeout_ms ?tuning
      ~workflow_tasks ~activity_tasks ()
    |> Result.map_error (public_bridge_error "worker configuration")
  in
  (* The lease is acquired only after every local validation, so no
     earlier failure can strand it; [Native.create] owns it from here and
     releases it on failure or after closing the graph (#832). *)
  let* lease =
    match runtime with
    | None -> Ok None
    | Some runtime -> (
        match Temporal_sdk_kernel.Shared_runtime.acquire runtime with
        | Some lease -> Ok (Some lease)
        | None ->
            Error
              (Base_error.defect
                 ~message:
                   "the runtime passed as ~runtime has already been shut down"))
  in
  let* supervisor =
    Native.create ?runtime_threads:io_threads ?runtime:lease
      ~capacity:supervisor_capacity ()
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
      Workflow.create ?on_activation ?on_completion ~task_queue ~namespace
        ~supervisor ~workflows ()
      |> Result.map_error (public_adapter_error "workflow registration")
    in
    let closed = Atomic.make false in
    let stop_requested = Atomic.make false in
    (* Activity contexts report a worker shutdown as soon as either lifecycle
       flag is set, i.e. while a running callback still holds the activity
       lane and [shutdown] waits for it (#494). *)
    let worker_shutting_down () =
      Atomic.get closed || Atomic.get stop_requested
    in
    let* activities =
      Activity.create ~supervisor ~activities ~worker_shutting_down
      |> Result.map_error (public_activity_error "activity registration")
    in
    Ok
      {
        supervisor;
        workflows;
        activities;
        workflow_tasks;
        closed;
        stop_requested;
        shutdown_retryable = Atomic.make false;
        terminal_cleanup_pending = Atomic.make false;
        terminal_cleanup_scheduled = Atomic.make false;
        run_mutex = Mutex.create ();
        owner = Owner.create ();
        activation_deadline_ms;
        graceful_shutdown_s =
          Int64.to_float graceful_shutdown_timeout_ms /. 1_000.;
        teardown_timeout_s =
          Int64.to_float shutdown_teardown_timeout_ms /. 1_000.;
        lanes_deadline = Atomic.make None;
        activity_detached = Atomic.make false;
        discard_pending = Bounded_shutdown.Deferred_discard.create ();
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

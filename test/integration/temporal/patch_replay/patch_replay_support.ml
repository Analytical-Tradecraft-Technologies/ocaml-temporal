(** Shared, private infrastructure for the live workflow-patch lifecycle replay
    acceptance fixture.

    This compilation unit intentionally contains no [Temporal.Workflow.patched]
    call and no workflow implementation. Each worker executable owns or selects
    exactly one source-version definition, so the legacy and removed binaries
    cannot accidentally link the active patch gate. Shared code is restricted to fixture
    constants, activity helpers, client references, and worker lifecycle
    support. *)

(** Public SDK modules are given short local aliases so the fixture's boundary
    code stays readable without exposing any private Runtime or Core types. *)
module Activity = Temporal.Activity
module Client = Temporal.Client
module Error = Temporal.Error
module Worker = Temporal.Worker
module Workflow = Temporal.Workflow

(** Dedicated task queue for this isolated acceptance. Keeping it separate
    from the ordinary smoke queue prevents a manually running smoke worker
    from accepting work intended for the lifecycle source-replacement test. *)
let task_queue = "ocaml-temporal-patch-replay"

(** Stable Temporal workflow type shared by every lifecycle source
    definition. A replacement worker must register this exact name so it can
    replay the execution accepted by the legacy worker. *)
let workflow_type = "smoke.patch_replay_history"

(** Stable history key for the one behavior change under test. It is a durable
    identifier rather than a build or deployment version, so changing this
    string would create a different Temporal patch decision. *)
let patch_id = "smoke.patch_replay_history.activity.v1"

(** The timer creates a durable, observable history boundary. Sixty seconds is
    long enough for the controller to stop generation one after [TimerStarted]
    and before [TimerFired], while keeping an accidentally unbounded live run
    within the driver timeout used by the surrounding fixture. *)
let replacement_timer = Temporal.Duration.of_ms 60_000L

(** Activity type used only by the historical, pre-patch branch. Its distinct
    name makes the terminal history show which branch actually scheduled work;
    it is not inferred from a process-local flag or worker log. *)
let legacy_activity_name = "smoke.patch_replay_history.legacy_activity"

(** Activity type used by the migrated behavior. Active code selects it via
    the patch decision; deprecated and removed generations schedule it
    unconditionally after their lifecycle gates. Its distinct durable name
    remains the terminal-history branch oracle. *)
let patched_activity_name = "smoke.patch_replay_history.patched_activity"

(** Exact activity output for the legacy branch. The result is intentionally a
    small fixture marker rather than workflow input or arbitrary payload data,
    allowing the driver to assert the branch without logging user values. *)
let legacy_activity_result = "PATCH_REPLAY:LEGACY_ACTIVITY"

(** Exact activity output for the newly patched branch. It differs from the
    legacy value so a controller cannot accidentally accept either branch. *)
let patched_activity_result = "PATCH_REPLAY:PATCHED_ACTIVITY"

(** Exact workflow result for an old history replayed by the replacement
    worker. This is kept distinct from the activity result to catch a workflow
    that schedules the correct activity but returns the wrong branch result. *)
let legacy_result = "PATCH_REPLAY:OLD_HISTORY"

(** Exact workflow result for an execution first started with patched code.
    The driver accepts only this value for the new-history phase. *)
let patched_result = "PATCH_REPLAY:NEW_HISTORY"

(** Activity implementation registered by both workers for the old branch.
    It does no I/O and has no process-local counter: the test is about durable
    workflow command selection, not activity-side nondeterminism. *)
let legacy_activity =
  Activity.define ~name:legacy_activity_name ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () -> Ok legacy_activity_result)

(** Activity implementation registered by active, deprecated, and removed code. Its separate
    Temporal name lets the final history and driver distinguish a successful
    new branch from a fallback to the old branch. *)
let patched_activity =
  Activity.define ~name:patched_activity_name ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () -> Ok patched_activity_result)

(** Fails closed if a local activity implementation ever returns a value other
    than its fixture contract. This turns an accidental fixture edit into a
    typed workflow failure instead of silently changing the acceptance oracle. *)
let require_activity_result ~branch ~expected ~result actual =
  if String.equal actual expected then Ok result
  else
    Error
      (Error.defect
         ~message:
           (Printf.sprintf
              "patch replay %s activity returned an unexpected fixture marker"
              branch))

(** Schedules and waits for the historical activity after the durable timer has
    fired. This helper is used by both source versions so the old branch stays
    byte-for-byte equivalent at the workflow-authoring level when patched code
    replays a legacy history. *)
let run_legacy_activity () =
  let open Temporal.Result_syntax in
  let* actual =
    Activity.execute ~do_not_eagerly_execute:true legacy_activity ()
  in
  require_activity_result ~branch:"legacy" ~expected:legacy_activity_result
    ~result:legacy_result actual

(** Schedules and waits for the migrated activity. Active code invokes it after
    the patch decision; deprecated and removed generations invoke it
    unconditionally after the lifecycle safety gates have been satisfied. *)
let run_patched_activity () =
  let open Temporal.Result_syntax in
  let* actual =
    Activity.execute ~do_not_eagerly_execute:true patched_activity ()
  in
  require_activity_result ~branch:"patched" ~expected:patched_activity_result
    ~result:patched_result actual

(** Client-only reference to the common workflow type. It deliberately has no
    implementation, so the assertion driver proves that it is a client and
    cannot accidentally execute the workflow in its own process. *)
let workflow_reference =
  Workflow.remote ~name:workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string

(** Environment validated before either dedicated worker allocates a native
    worker graph. Paths identify host-bind-mounted coordination files only;
    workflow inputs, activity payloads, native handles, and logs are excluded. *)
type worker_configuration = {
  target_url : string;
  namespace : string;
  ready_file : string;
  stopped_file : string;
  replay_diagnostics_file : string;
  generation : int;
}

(** Returns a required non-empty environment setting as a typed configuration
    error rather than raising during process startup. Callers may validate more
    specific constraints, such as absolute-file paths, afterwards. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Validates a file path used by a live fixture coordination marker. Requiring
    an absolute, NUL-free path limits writes to the controller-selected bind
    mount and avoids accidentally treating a relative working directory as a
    durable acceptance contract. *)
let required_absolute_path_env name =
  let open Temporal.Result_syntax in
  let* path = required_env name in
  if String.contains path '\000' then
    Error (Error.defect ~message:(name ^ " must not contain NUL"))
  else if Filename.is_relative path then
    Error (Error.defect ~message:(name ^ " must be an absolute path"))
  else Ok path

(** Parses a positive generation number. Generation one owns stale-diagnostic
    cleanup; a replacement generation must preserve and extend that file so
    the Native worker's strict observer can validate the exact prior run. *)
let required_positive_int_env name =
  let open Temporal.Result_syntax in
  let* value = required_env name in
  match int_of_string_opt value with
  | Some parsed when parsed >= 1 -> Ok parsed
  | Some _ -> Error (Error.defect ~message:(name ^ " must be positive"))
  | None -> Error (Error.defect ~message:(name ^ " must be an integer"))

(** Removes a stale marker before a new worker or driver publishes it. A failed
    removal is a typed defect because accepting a previous run's marker would
    make an old process or run look ready to the controller. *)
let clear_file_before_start ~label path =
  try
    if Sys.file_exists path then Sys.remove path;
    if Sys.file_exists path then
      Error
        (Error.defect
           ~message:(Printf.sprintf "cannot remove stale %s file" label))
    else Ok ()
  with exception_ ->
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot remove stale %s file: %s" label
              (Printexc.to_string exception_)))

(** Removes a marker after a worker exits without letting cleanup hide the
    worker's primary result. This is intentionally best-effort because the
    controller will still reject a stale file before its next launch. *)
let clear_file_best_effort path =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Atomically publishes a bounded text marker. The staging file is created in
    the destination directory, so [Sys.rename] is atomic on the shared fixture
    mount; readers observe either no marker or the complete contents. Any
    staging file is removed on failure to avoid confusing later test runs. *)
let publish_marker ~path ~contents =
  let temporary = ref None in
  try
    let generated =
      Filename.temp_file ~temp_dir:(Filename.dirname path)
        (Filename.basename path ^ ".tmp.") ""
    in
    temporary := Some generated;
    let channel = open_out_bin generated in
    Fun.protect
      ~finally:(fun () -> close_out_noerr channel)
      (fun () ->
        output_string channel contents;
        flush channel);
    Sys.rename generated path;
    temporary := None;
    Ok ()
  with exception_ ->
    Option.iter
      (fun generated -> try Sys.remove generated with _ -> ())
      !temporary;
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot publish patch replay marker: %s"
              (Printexc.to_string exception_)))

(** Requires the standard opt-in live-test flag. This prevents a direct local
    executable invocation from selecting a mock backend and being mistaken for
    the intended Temporal Server source-replacement acceptance. *)
let require_live_gate () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" -> Ok ()
  | _ ->
      Error
        (Error.defect
           ~message:
             "patch replay acceptance is disabled; set TEMPORAL_TWO_BINARY_LIVE=1")

(** Loads the dedicated worker configuration and requires the public Native
    worker's optional diagnostic observer to receive a non-empty target
    *execution* ID. [SMOKE_REPLAY_WORKFLOW_ID] is compared with
    [activation_info.workflow_id], not the Temporal workflow type; the later
    controller supplies the exact execution ID used by the driver. The observer
    itself owns JSON parsing, strict record validation, atomic writes, and
    preservation of generation-one records. *)
let worker_configuration () =
  let open Temporal.Result_syntax in
  let* () = require_live_gate () in
  let* target_url = required_env "TEMPORAL_ADDRESS" in
  let* namespace = required_env "TEMPORAL_NAMESPACE" in
  let* ready_file = required_absolute_path_env "PATCH_REPLAY_WORKER_READY_FILE" in
  let* stopped_file =
    required_absolute_path_env "PATCH_REPLAY_WORKER_STOPPED_FILE"
  in
  let* replay_diagnostics_file =
    required_absolute_path_env "SMOKE_WORKER_REPLAY_DIAGNOSTICS_FILE"
  in
  let* generation = required_positive_int_env "SMOKE_WORKER_GENERATION" in
  let* _target_execution_id = required_env "SMOKE_REPLAY_WORKFLOW_ID" in
  if String.equal ready_file stopped_file then
    Error
      (Error.defect
         ~message:
           "PATCH_REPLAY_WORKER_READY_FILE and PATCH_REPLAY_WORKER_STOPPED_FILE must differ")
  else if
    String.equal replay_diagnostics_file ready_file
    || String.equal replay_diagnostics_file stopped_file
  then
    Error
      (Error.defect
         ~message:
           "SMOKE_WORKER_REPLAY_DIAGNOSTICS_FILE must not share a worker marker path")
  else
    Ok
      {
        target_url;
        namespace;
        ready_file;
        stopped_file;
        replay_diagnostics_file;
        generation;
      }

(** Prepares the diagnostic file according to its ownership protocol. The
    generation-one process removes stale evidence before Core can load it;
    later generations require a prior file and rely on Native worker validation
    to reject malformed identity, record ordering, or replay metadata. *)
let prepare_replay_diagnostics configuration =
  if configuration.generation = 1 then
    clear_file_before_start ~label:"patch replay diagnostics"
      configuration.replay_diagnostics_file
  else if Sys.file_exists configuration.replay_diagnostics_file then Ok ()
  else
    Error
      (Error.defect
         ~message:
           "replacement patch replay worker requires generation-one diagnostics")

(** Invokes the public shutdown API without letting an unexpected exception
    bypass the fixture's typed-error boundary. The public [Worker.shutdown]
    implementation caches terminal results, so this helper is safe both for
    the signal watcher that first requests shutdown and for the unconditional
    post-body cleanup that follows it. It deliberately does not choose between
    a body failure and a cleanup failure; [run_worker] preserves the former
    when both occur. *)
let shutdown_worker_safely worker =
  try Worker.shutdown worker with
  | exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "patch replay worker shutdown raised: %s"
                (Printexc.to_string exception_)))

(** Runs a public worker until Compose sends SIGTERM or SIGINT. The signal
    handlers only set an atomic flag; a control Domain performs the potentially
    blocking [Worker.shutdown] call outside signal context, avoiding a lock or
    FFI transition being interrupted halfway through. *)
let run_with_signal_shutdown worker =
  let stop_requested = Atomic.make false in
  let watcher_finished = Atomic.make false in
  let request_shutdown _signal = Atomic.set stop_requested true in
  let previous_term = Sys.signal Sys.sigterm (Sys.Signal_handle request_shutdown) in
  let previous_int = Sys.signal Sys.sigint (Sys.Signal_handle request_shutdown) in
  let watcher =
    Domain.spawn (fun () ->
        while not (Atomic.get watcher_finished) do
          if Atomic.get stop_requested then begin
            let shutdown_result = shutdown_worker_safely worker in
            match shutdown_result with
            | Ok () -> Atomic.set watcher_finished true
            | Error _ -> Unix.sleepf 0.05
          end
          else Unix.sleepf 0.05
        done)
  in
  let run_result =
    try
      Fun.protect
        ~finally:(fun () -> Atomic.set watcher_finished true)
        (fun () -> Worker.run worker)
    with exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "patch replay worker run raised: %s"
                (Printexc.to_string exception_)))
  in
  Domain.join watcher;
  Sys.set_signal Sys.sigterm previous_term;
  Sys.set_signal Sys.sigint previous_int;
  let shutdown_result = shutdown_worker_safely worker in
  let open Temporal.Result_syntax in
  let* () = run_result in
  shutdown_result

(** Creates, advertises, runs, and tears down one of the dedicated worker
    source versions. The caller supplies only public registrations; all native
    lifecycle ownership remains inside [Temporal.Worker], while the private
    fixture observer owns diagnostic policy and artifacts. *)
let run_worker ~workflows ~activities =
  let open Temporal.Result_syntax in
  let* configuration = worker_configuration () in
  let* () =
    clear_file_before_start ~label:"patch replay worker readiness"
      configuration.ready_file
  in
  let* () =
    clear_file_before_start ~label:"patch replay worker shutdown"
      configuration.stopped_file
  in
  let* () = prepare_replay_diagnostics configuration in
  let* worker =
    Acceptance_observer.with_worker (fun () ->
      Worker.create ~target_url:configuration.target_url
        ~namespace:configuration.namespace ~identity:"ocaml-temporal-patch-replay"
        (* This fixture isolates workflow patch replay from sticky-cache
           hand-off. Sticky eviction and replacement are covered by the
           dedicated cache-eviction and restart gates; keeping Core's cache
           disabled here means a replacement generation receives the durable
           timer task from the ordinary queue instead of a queue owned by the
           stopped generation. *)
        ~max_cached_workflows:0 ~task_queue ~workflows ~activities ())
  in
  (* Worker creation is the ownership boundary: from this point onward every
     result path must invoke [Worker.shutdown], even if readiness publication
     itself fails.  Keep the body result separate from teardown so a failed
     cleanup cannot hide the more useful failure that caused the worker to
     leave its normal run path. *)
  let body_result =
    try
      Fun.protect
        ~finally:(fun () -> clear_file_best_effort configuration.ready_file)
        (fun () ->
          let* () =
            publish_marker ~path:configuration.ready_file ~contents:"worker-ready\n"
          in
          let* () = run_with_signal_shutdown worker in
          publish_marker ~path:configuration.stopped_file ~contents:"worker-stopped\n")
    with exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "patch replay worker body raised: %s"
                (Printexc.to_string exception_)))
  in
  let shutdown_result = shutdown_worker_safely worker in
  match body_result with
  | Error _ as error -> error
  | Ok () -> shutdown_result

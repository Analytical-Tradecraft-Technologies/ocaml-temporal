(** Worker process for the two-OCaml-binary live acceptance test.

    The worker registers the same shared workflow definitions as the driver,
    including the context-aware heartbeat/retry, timeout-triggered-retry,
    heartbeat-timeout-triggered-retry, and activity-level non-retryable
    scenarios, and keeps the public native
    worker loop alive until graceful shutdown. The executable remains
    guarded by [TEMPORAL_TWO_BINARY_LIVE] so a local run cannot accidentally
    connect to a developer's Temporal endpoint; the Compose job is the only
    place that enables the live gate. *)

module Worker = Temporal.Worker
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Records a worker-process phase using metadata only. This executable never
    logs workflow/activity payloads; the markers exist to show whether a live
    acceptance stall occurred during construction, readiness publication, or
    the long-running worker loop. *)
let phase operation status =
  Printf.eprintf "two-binary worker phase=%s status=%s\n%!" operation status

(** Reads a required environment setting as a typed configuration error. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Reads the optional cache bound used by the focused live eviction fixture.
    Leaving it unset preserves the production worker default; an explicit zero
    is retained because it is a meaningful Core configuration. *)
let optional_cache_bound () =
  match Sys.getenv_opt "SMOKE_WORKER_MAX_CACHED_WORKFLOWS" with
  | None | Some "" -> Ok None
  | Some value -> (
      try
        let parsed = int_of_string value in
        if parsed >= 0 then Ok (Some parsed)
        else
          Error
            (Error.defect
               ~message:
                 "SMOKE_WORKER_MAX_CACHED_WORKFLOWS must be non-negative")
      with _ ->
        Error
          (Error.defect
             ~message:
               "SMOKE_WORKER_MAX_CACHED_WORKFLOWS must be an integer"))

(** Writes a marker through a temporary file and atomic rename. The Compose
    health and teardown checks therefore observe either the complete marker or
    no marker, never a partially written file. [Filename.temp_file] creates the
    staging file exclusively, so separate containers with independent PID
    namespaces cannot accidentally share a staging pathname. *)
let publish_marker path contents =
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
        (* Flush before the rename so a successful marker always represents
           bytes that reached the operating-system file boundary. *)
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
           (Printf.sprintf "cannot publish worker marker %s: %s" path
              (Printexc.to_string exception_)))

(** Publishes readiness only after [Worker.create] has completed successfully.
    This marker is consumed by the Compose health check and is deliberately
    separate from the worker's human-readable phase logs. *)
let publish_ready path = publish_marker path "worker-ready\n"

(** Publishes the exact per-run shutdown marker after both public worker loops
    have returned successfully. The Makefile removes this file before every
    stop request, so an old container log or old marker cannot satisfy a new
    teardown assertion. *)
let publish_stopped path = publish_marker path "worker-stopped\n"

(** Removes the replay diagnostics only for generation one. Generation two
    must read and extend this file; deleting it there would erase the only
    evidence that the original worker observed the timer boundary. *)
let clear_replay_diagnostics_before_start path generation =
  if generation = 1 then
    try
      if Sys.file_exists path then Sys.remove path;
      if Sys.file_exists path then
        Error
          (Error.defect
             ~message:
               (Printf.sprintf
                  "cannot remove stale replay diagnostics file %s" path))
      else Ok ()
    with exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "cannot remove stale replay diagnostics file %s: %s"
                path (Printexc.to_string exception_)))
  else Ok ()

(** Removes the readiness marker during every normal result path. Compose also
    removes the container on failure, but explicit cleanup prevents a local
    rerun from inheriting stale readiness state. *)
let clear_ready path =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Removes a stale readiness marker before worker construction and refuses to
    continue if the path is still present. Final shutdown uses [clear_ready]
    because cleanup must not replace the worker's original result, but startup
    must fail closed: a marker that could not be removed would otherwise make
    Compose report a previous run as healthy. *)
let clear_ready_before_start path =
  try
    clear_ready path;
    if Sys.file_exists path then
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "cannot remove stale worker readiness marker %s"
                path))
    else Ok ()
  with exception_ ->
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot remove stale worker readiness marker %s: %s"
              path (Printexc.to_string exception_)))

(** Runs the blocking worker loop while a small control Domain translates
    Compose's SIGTERM/SIGINT into the public shutdown operation. Signal handlers
    only flip atomics; all SDK and native calls stay outside the signal context,
    so teardown cannot interrupt a mutex or a JSON/FFI conversion halfway
    through. *)
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
            (* A bounded native wait lets this call join the worker loop rather
               than terminating the process while Core still owns a lease. A
               transient retryable drain error must not end the watcher: doing
               so would leave [Worker.run] alive after the signal and make the
               host-side stop wait until Compose forcibly kills the process. *)
            let shutdown_result =
              try Worker.shutdown worker with _ ->
                Error
                  (Error.defect
                     ~message:"worker shutdown watcher raised an exception")
            in
            match shutdown_result with
            | Ok () -> Atomic.set watcher_finished true
            | Error _ -> Unix.sleepf 0.05
          end
          else Unix.sleepf 0.05
        done)
  in
  let run_result =
    try
      phase "worker_run" "begin";
      Fun.protect
        ~finally:(fun () -> Atomic.set watcher_finished true)
        (fun () -> Worker.run worker)
    with exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "worker run raised: %s"
                (Printexc.to_string exception_)))
  in
  Domain.join watcher;
  Sys.set_signal Sys.sigterm previous_term;
  Sys.set_signal Sys.sigint previous_int;
  phase "worker_shutdown" "begin";
  let shutdown_result = Worker.shutdown worker in
  phase "worker_run"
    (match run_result with Ok () -> "stopped" | Error _ -> "error");
  phase "worker_shutdown"
    (match shutdown_result with Ok () -> "ok" | Error _ -> "error");
  let open Temporal.Result_syntax in
  let* () = run_result in
  shutdown_result

(** Prevents a normal local invocation from silently running the mock backend
    and being reported as a real Temporal worker. *)
let require_live_gate () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" -> Ok ()
  | _ ->
      Error
        (Error.defect
           ~message:
             "two-binary live acceptance is not enabled; set \
              TEMPORAL_TWO_BINARY_LIVE=1 only in its Compose job")

(** Reports a structured SDK error without exposing bridge JSON or payload bytes
    in process logs. *)
let fail operation error =
  phase operation ("error:" ^ Error.kind error);
  Printf.eprintf "%s failed (%s): %s\n" operation (Error.kind error)
    (Error.message error);
  1

(** Creates, runs, and shuts down the worker using the public API only. *)
let run () =
  match require_live_gate () with
  | Error error -> Error error
  | Ok () ->
      let open Temporal.Result_syntax in
      let* ready_file = required_env "SMOKE_WORKER_READY_FILE" in
      (* Remove readiness before constructing the worker. A reused Compose
         container can retain the old marker after an interrupted process. Do
         this as soon as the readiness path itself is validated, before any
         later configuration lookup can return an error; failing closed if it
         cannot be removed prevents the health check from accepting stale
         readiness while this run's [Worker.create] is pending or has already
         failed. The finalizer below repeats best-effort cleanup for normal
         shutdown and error paths after creation. *)
      let* () = clear_ready_before_start ready_file in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* stopped_file = required_env "SMOKE_WORKER_STOPPED_FILE" in
      let* replay_diagnostics_file =
        required_env "SMOKE_WORKER_REPLAY_DIAGNOSTICS_FILE"
      in
      let* generation =
        match Sys.getenv_opt "SMOKE_WORKER_GENERATION" with
        | Some value -> (
            try
              let parsed = int_of_string value in
              if parsed >= 1 then Ok parsed
              else Error (Error.defect ~message:"SMOKE_WORKER_GENERATION must be positive")
            with _ ->
              Error (Error.defect ~message:"SMOKE_WORKER_GENERATION must be an integer"))
        | None -> Error (Error.defect ~message:"SMOKE_WORKER_GENERATION must be set")
      in
      let* max_cached_workflows = optional_cache_bound () in
      let* () = clear_replay_diagnostics_before_start replay_diagnostics_file generation in
      let* cancellation_ready_file = Definitions.cancellation_ready_file () in
      let* signal_condition_ready_file =
        Definitions.signal_condition_ready_file ()
      in
      (* Clear any marker left by a manually interrupted local run before the
         worker can advertise readiness. The driver performs the same cleanup
         immediately before starting workflows, closing the stale-marker race
         without allowing this test-only activity to touch workflow state. *)
      let () = Definitions.clear_cancellation_ready_file cancellation_ready_file in
      let () =
        Definitions.clear_signal_condition_ready_file signal_condition_ready_file
      in
      let worker_result =
        Worker.create ?max_cached_workflows ~target_url ~namespace
          ~identity:"ocaml-temporal-two-binary-worker"
          ~task_queue:Definitions.task_queue
          ~workflows:
            [
              Worker.workflow ~signals:[ Definitions.signal_value_handler ]
                ~queries:
                  [ Definitions.signal_condition_status_handler;
                    Definitions.signal_value_echo_handler ]
                ~updates:[ Definitions.signal_value_update_handler ]
                Definitions.signal_condition_workflow;
              Worker.workflow Definitions.external_signal_parent;
              Worker.workflow Definitions.external_signal_completed_parent;
              Worker.workflow Definitions.external_cancellation_wrong_run_parent;
              Worker.workflow Definitions.external_cancellation_parent;
              Worker.workflow Definitions.fan_out;
              Worker.workflow Definitions.timer_then_activity;
              Worker.workflow Definitions.local_activity_workflow;
              Worker.workflow Definitions.continue_as_new;
              Worker.workflow Definitions.activity_retry;
              Worker.workflow Definitions.activity_long_backoff_retry;
              Worker.workflow Definitions.activity_heartbeat_retry;
              Worker.workflow Definitions.async_activity_completion;
              Worker.workflow Definitions.activity_timeout_retry;
              Worker.workflow Definitions.activity_heartbeat_timeout_retry;
              Worker.workflow Definitions.activity_non_retryable_failure;
              Worker.workflow Definitions.child_retryable_failure;
              Worker.workflow Definitions.parent_retries_child;
              Worker.workflow Definitions.child_after_timer;
              Worker.workflow Definitions.parent_awaits_child;
              Worker.workflow Definitions.parent_observes_child_start_failure;
              Worker.workflow Definitions.child_non_retryable_failure;
              Worker.workflow Definitions.parent_awaits_failed_child;
              Worker.workflow Definitions.child_long_running;
              Worker.workflow Definitions.parent_cancels_child;
              Worker.workflow Definitions.non_retryable_failure;
              Worker.workflow Definitions.long_running_cancellation;
              Worker.workflow Definitions.worker_restart_replay;
              Worker.workflow
                ~queries:[ Definitions.cache_eviction_residency_handler ]
                Definitions.cache_eviction;
            ]
          ~activities:
            [
              Worker.activity Definitions.mock_transform;
              Worker.activity Definitions.retry_once_activity;
              Worker.activity Definitions.long_backoff_retry_activity;
              Worker.activity Definitions.heartbeat_retry_activity;
              Worker.activity Definitions.async_delayed_completion_activity;
              Worker.activity Definitions.timeout_retry_activity;
              Worker.activity Definitions.heartbeat_timeout_retry_activity;
              Worker.activity Definitions.non_retryable_activity;
              Worker.activity Definitions.child_retry_activity;
              Worker.activity Definitions.cancellation_ready_activity;
              Worker.activity Definitions.signal_condition_ready_activity;
            ]
          ()
      in
      let* worker =
        match worker_result with
        | Ok worker ->
            phase "worker_create" "ok";
            Ok worker
        | Error error ->
            phase "worker_create" ("error:" ^ Error.kind error);
            Error error
      in
      Fun.protect
        ~finally:(fun () ->
          clear_ready ready_file;
          Definitions.clear_cancellation_ready_file cancellation_ready_file)
        (fun () ->
          phase "worker_ready" "begin";
          match publish_ready ready_file with
          | Error error ->
              phase "worker_ready" ("error:" ^ Error.kind error);
              Error error
          | Ok () ->
              phase "worker_ready" "published";
              let result = run_with_signal_shutdown worker in
              (match result with
              | Error _ -> result
              | Ok () ->
                  let stopped_result = publish_stopped stopped_file in
                  (match stopped_result with
                  | Ok () -> phase "worker_stopped" "published"
                  | Error error ->
                      phase "worker_stopped" ("error:" ^ Error.kind error));
                  stopped_result))

(** Reports the final typed result and converts it to a process exit code. *)
let () =
  match run () with
  | Ok () -> Printf.printf "two-binary worker stopped cleanly\n%!"
  | Error error -> exit (fail "two-binary worker" error)

(** Dedicated worker for the live parent/child replay acceptances.

    This binary registers the success and failure-recovery parent/child pairs.
    Keeping the set limited to these four definitions makes each recovery proof
    attributable to the relevant workflow definitions rather than to the broad
    smoke worker's unrelated activities and workflow types. *)

module Worker = Temporal.Worker
(** Public worker lifecycle and registration API. *)

module Error = Temporal.Error
(** Structured SDK errors used for configuration and lifecycle failures. *)

module Definitions = Smoke_definitions
(** Shared typed workflow definitions and stable restart-fixture identities. *)

(** Emits bounded process-phase information without serializing workflow input,
    output, or native bridge values. These messages distinguish lifecycle stalls
    during acceptance while leaving the client driver's exact result as the
    pass/fail oracle. *)
let phase (operation : string) (status : string) : unit =
  Printf.eprintf "parent/child restart worker phase=%s status=%s\n%!" operation
    status

(** Reads a required non-empty environment setting as a typed configuration
    result. The dedicated Compose service supplies these values only for the
    live acceptance path, so a missing setting must fail before worker creation.
*)
let required_env (name : string) : (string, Error.t) result =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Writes a marker through a same-directory temporary file and atomic rename.
    The health check and external controller therefore cannot observe a partial
    readiness or shutdown marker. *)
let publish_marker (path : string) (contents : string) : (unit, Error.t) result
    =
  let temporary = ref None in
  try
    let generated =
      Filename.temp_file ~temp_dir:(Filename.dirname path)
        (Filename.basename path ^ ".tmp.")
        ""
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
           (Printf.sprintf
              "cannot publish parent/child restart worker marker %s: %s" path
              (Printexc.to_string exception_)))

(** Publishes readiness after [Worker.create] has completed successfully. *)
let publish_ready (path : string) : (unit, Error.t) result =
  publish_marker path "worker-ready\n"

(** Publishes the shutdown marker only after the worker loops have returned
    through the public graceful-shutdown path. *)
let publish_stopped (path : string) : (unit, Error.t) result =
  publish_marker path "worker-stopped\n"

(** Removes a readiness marker on normal shutdown or after a failed creation.
    Cleanup is best effort because it must not overwrite the original lifecycle
    error being returned to the caller. *)
let clear_ready (path : string) : unit =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Clears a stale readiness marker before worker construction and refuses to
    continue if it remains. A retained marker could otherwise make a reused
    container look healthy before the new worker has created its Core graph. *)
let clear_ready_before_start (path : string) : (unit, Error.t) result =
  try
    clear_ready path;
    if Sys.file_exists path then
      Error
        (Error.defect
           ~message:
             (Printf.sprintf
                "cannot remove stale parent/child restart readiness marker %s"
                path))
    else Ok ()
  with exception_ ->
    Error
      (Error.defect
         ~message:
           (Printf.sprintf
              "cannot remove parent/child restart readiness marker %s: %s" path
              (Printexc.to_string exception_)))

(** Invokes public shutdown without allowing an unexpected exception to bypass
    deterministic worker ownership. [Worker.shutdown] caches its terminal
    result, so the signal watcher and the unconditional owner cleanup may both
    call this helper safely. *)
let shutdown_worker_safely (worker : Worker.t) : (unit, Error.t) result =
  try Worker.shutdown worker
  with exception_ ->
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "parent/child restart worker shutdown raised: %s"
              (Printexc.to_string exception_)))

(** Converts an exception from signal installation, Domain creation/join, or
    handler restoration into the fixture's typed lifecycle error. *)
let lifecycle_exception (operation : string) (exception_ : exn) : Error.t =
  Error.defect
    ~message:
      (Printf.sprintf "parent/child restart worker %s raised: %s" operation
         (Printexc.to_string exception_))

(** Runs the blocking worker loop while a control Domain converts SIGTERM and
    SIGINT into [Worker.shutdown]. Signal handlers merely set an atomic flag;
    SDK calls remain outside signal context, and the blocking public loop is
    never run on a cooperative scheduler fiber. *)
let run_with_signal_shutdown (worker : Worker.t) : (unit, Error.t) result =
  let stop_requested = Atomic.make false in
  let watcher_finished = Atomic.make false in
  let request_shutdown _signal = Atomic.set stop_requested true in
  let previous_term = ref None in
  let previous_int = ref None in
  let watcher = ref None in
  let run_result =
    try
      previous_term :=
        Some (Sys.signal Sys.sigterm (Sys.Signal_handle request_shutdown));
      previous_int :=
        Some (Sys.signal Sys.sigint (Sys.Signal_handle request_shutdown));
      watcher :=
        Some
          (Domain.spawn (fun () ->
               while not (Atomic.get watcher_finished) do
                 if Atomic.get stop_requested then
                   (* A transient drain failure may be retryable. Keep the
                      watcher alive so Compose does not replace graceful stop
                      with its forced-termination deadline. *)
                   begin match shutdown_worker_safely worker with
                   | Ok () -> Atomic.set watcher_finished true
                   | Error _ -> Unix.sleepf 0.05
                   end
                 else Unix.sleepf 0.05
               done));
      phase "worker_run" "begin";
      Worker.run worker
    with exception_ -> Error (lifecycle_exception "run setup/body" exception_)
  in
  Atomic.set watcher_finished true;
  let join_result =
    match !watcher with
    | None -> Ok ()
    | Some domain -> (
        try
          Domain.join domain;
          Ok ()
        with exception_ ->
          Error (lifecycle_exception "watcher join" exception_))
  in
  (* Restore every handler that was successfully installed, even when the
     second installation or Domain creation failed. Attempt both restorations
     before choosing which cleanup error to report. *)
  let restore_int_result =
    match !previous_int with
    | None -> Ok ()
    | Some handler -> (
        try
          Sys.set_signal Sys.sigint handler;
          Ok ()
        with exception_ ->
          Error (lifecycle_exception "SIGINT handler restoration" exception_))
  in
  let restore_term_result =
    match !previous_term with
    | None -> Ok ()
    | Some handler -> (
        try
          Sys.set_signal Sys.sigterm handler;
          Ok ()
        with exception_ ->
          Error (lifecycle_exception "SIGTERM handler restoration" exception_))
  in
  phase "worker_run"
    (match run_result with Ok () -> "stopped" | Error _ -> "error");
  let open Temporal.Result_syntax in
  let* () = run_result in
  let* () = join_result in
  let* () = restore_int_result in
  restore_term_result

(** Prevents a direct local invocation from silently using a mock endpoint and
    being mistaken for the live Temporal/PostgreSQL recovery acceptance. *)
let require_live_gate () : (unit, Error.t) result =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" -> Ok ()
  | _ ->
      Error
        (Error.defect
           ~message:
             "parent/child restart acceptance is not enabled; set \
              TEMPORAL_TWO_BINARY_LIVE=1 only in its Compose job")

(** Formats a final typed worker error without exposing bridge JSON, task
    tokens, or workflow payloads in the process log. *)
let fail (operation : string) (error : Error.t) : int =
  phase operation ("error:" ^ Error.kind error);
  Printf.eprintf "%s failed (%s): %s\n" operation (Error.kind error)
    (Error.message error);
  1

(** Creates the dedicated worker, registers the parent and child together on the
    shared test queue, and keeps it alive until graceful shutdown. Both
    definitions must be local registrations: the parent emits the child start
    command, while the child receives the timer firing after replacement. *)
let run () : (unit, Error.t) result =
  match require_live_gate () with
  | Error error -> Error error
  | Ok () -> (
      let open Temporal.Result_syntax in
      let* ready_file = required_env "SMOKE_WORKER_READY_FILE" in
      let* () = clear_ready_before_start ready_file in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* stopped_file = required_env "SMOKE_WORKER_STOPPED_FILE" in
      let worker_result =
        Acceptance_observer.with_worker (fun () ->
          Worker.create ~target_url ~namespace
            ~identity:"ocaml-temporal-parent-child-restart-worker"
            ~task_queue:Definitions.task_queue
            ~workflows:
              [
                Worker.workflow Definitions.parent_child_restart_child;
                Worker.workflow Definitions.parent_child_restart_parent;
                Worker.workflow Definitions.parent_child_failure_replay_child;
                Worker.workflow Definitions.parent_child_failure_replay_parent;
              ]
            ~activities:[] ())
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
      (* [Worker.create] transfers a native handle graph to this scope. Keep
         the body result separate from unconditional shutdown so readiness,
         Domain-creation, run, marker, and unexpected-exception failures all
         release that graph, while the original failure remains primary. *)
      let body_result =
        try
          Fun.protect
            ~finally:(fun () -> clear_ready ready_file)
            (fun () ->
              phase "worker_ready" "begin";
              let* () =
                match publish_ready ready_file with
                | Ok () ->
                    phase "worker_ready" "published";
                    Ok ()
                | Error error ->
                    phase "worker_ready" ("error:" ^ Error.kind error);
                    Error error
              in
              let* () = run_with_signal_shutdown worker in
              let stopped_result = publish_stopped stopped_file in
              (match stopped_result with
              | Ok () -> phase "worker_stopped" "published"
              | Error error ->
                  phase "worker_stopped" ("error:" ^ Error.kind error));
              stopped_result)
        with exception_ -> Error (lifecycle_exception "owned body" exception_)
      in
      phase "worker_shutdown" "begin";
      let shutdown_result = shutdown_worker_safely worker in
      phase "worker_shutdown"
        (match shutdown_result with Ok () -> "ok" | Error _ -> "error");
      match body_result with
      | Error _ as error -> error
      | Ok () -> shutdown_result)

(** Converts the final typed worker result into the process exit status used by
    Compose. *)
let () =
  match run () with
  | Ok () -> Printf.printf "parent/child restart worker stopped cleanly\n%!"
  | Error error -> exit (fail "parent/child restart worker" error)

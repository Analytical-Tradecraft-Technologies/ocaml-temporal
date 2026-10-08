(** Driver for the live two-generation worker restart/replay acceptance.

    The process starts exactly one workflow and waits on that exact run while
    the shell controller replaces the worker. It communicates only bounded
    identity/result markers through the repository bind mount; workflow input,
    output payload bytes, and native handles never enter those files. *)

module Client = Temporal.Client
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Reads a required non-empty environment value as a typed configuration
    result rather than raising while the executable is initializing. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Replaces a marker atomically so the controller can never observe a partial
    workflow/run identity while the driver is publishing its start result. *)
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
           (Printf.sprintf "cannot publish restart driver marker %s: %s" path
              (Printexc.to_string exception_)))

(** Extracts the exact terminal class needed by this acceptance fixture. A
    failure or cancellation is returned as a typed defect with no payload
    details, because the expected success marker is the public contract. *)
let require_completed = function
  | Client.Completed { output = value; _ } when value = "SMOKE:AFTER-REPLAY:ATTEMPT:2" -> Ok ()
  | Client.Completed { output = value; _ } ->
      Error
        (Error.defect
           ~message:(Printf.sprintf "restart workflow returned %S" value))
  | Client.Failed { error; _ }
  | Client.Cancelled error
  | Client.Terminated error
  | Client.Timed_out { error; _ } ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "restart workflow ended with %s: %s"
                (Error.kind error) (Error.message error)))
  | Client.Continued_as_new execution ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "restart workflow continued as new at run %s"
                execution.run_id))

(** Performs the one start-and-wait transaction and always shuts down the
    client after the exact run has reached a terminal result. *)
let run () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" ->
      let open Temporal.Result_syntax in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* accepted_file = required_env "SMOKE_RESTART_ACCEPTED_FILE" in
      let* result_file = required_env "SMOKE_RESTART_RESULT_FILE" in
      let* client =
        Client.create ~target_url ~namespace
          ~identity:"ocaml-temporal-restart-driver" ()
      in
      let finish result =
        match Client.shutdown client with
        | Ok () -> result
        | Error error -> Error error
      in
      let result =
        let* handle =
          Client.start client ~workflow:Definitions.worker_restart_replay
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-worker-restart-replay" ~input:"smoke" ()
        in
        let* () =
          publish_marker accepted_file
            (Printf.sprintf "workflow_id=%s\nrun_id=%s\n"
               (Client.workflow_id handle) (Option.get (Client.run_id handle)))
        in
        let* outcome = Client.wait handle in
        let* () = require_completed outcome in
        publish_marker result_file "completed\n"
      in
      finish result
  | _ ->
      Error
        (Error.defect
           ~message:
             "restart acceptance is not enabled; set TEMPORAL_TWO_BINARY_LIVE=1")

(** Converts the typed process result into a useful command status without
    exposing payload data in the controller log. *)
let () =
  match run () with
  | Ok () -> Printf.printf "restart/replay driver completed\n%!"
  | Error error ->
      Printf.eprintf "restart/replay driver failed (%s): %s\n%!" (Error.kind error)
        (Error.message error);
      exit 1

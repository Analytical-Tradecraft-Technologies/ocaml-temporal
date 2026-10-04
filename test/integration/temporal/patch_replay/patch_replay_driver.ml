(** Client-only assertion driver for the live patch-lifecycle history fixture.

    The controller launches this process beside either worker generation. It
    starts exactly one run of the common workflow type, publishes that returned
    workflow/run identity atomically, waits on the same typed handle, and then
    publishes the exact expected result. It never registers workflow or
    activity code, so it cannot make a source-replacement test pass locally. *)

(** Short aliases keep the process-boundary code readable while preserving the
    public [Temporal] API as the only interface used by this test binary. *)
module Client = Temporal.Client
module Error = Temporal.Error
module Support = Patch_replay_support

(** Driver settings supplied by the later Compose controller. The expected
    result is deliberately constrained to the two fixture constants so a
    malformed environment cannot turn this into a generic payload recorder. *)
type configuration = {
  target_url : string;
  namespace : string;
  execution_id : string;
  expected_result : string;
  accepted_file : string;
  result_file : string;
}

(** Reads a required bounded identifier-like environment setting. The public
    client repeats its own Temporal validation before network use; this early
    check keeps marker formatting and error reporting deterministic. *)
let required_identifier_env name =
  let open Temporal.Result_syntax in
  let* value = Support.required_env name in
  if String.contains value '\000' then
    Error (Error.defect ~message:(name ^ " must not contain NUL"))
  else Ok value

(** Loads the driver configuration and rejects marker-path aliases. Different
    files are required so a completed result cannot be mistaken for the start
    acknowledgement used by the controller to discover the exact run ID. *)
let configuration () =
  let open Temporal.Result_syntax in
  let* () = Support.require_live_gate () in
  let* target_url = Support.required_env "TEMPORAL_ADDRESS" in
  let* namespace = Support.required_env "TEMPORAL_NAMESPACE" in
  let* execution_id = required_identifier_env "PATCH_REPLAY_EXECUTION_ID" in
  let* expected_result = required_identifier_env "PATCH_REPLAY_EXPECTED_RESULT" in
  let* accepted_file = Support.required_absolute_path_env "PATCH_REPLAY_ACCEPTED_FILE" in
  let* result_file = Support.required_absolute_path_env "PATCH_REPLAY_RESULT_FILE" in
  if
    not
      (String.equal expected_result Support.legacy_result
      || String.equal expected_result Support.patched_result)
  then
    Error
      (Error.defect
         ~message:
           "PATCH_REPLAY_EXPECTED_RESULT must be one of the fixture result markers")
  else if String.equal accepted_file result_file then
    Error
      (Error.defect
         ~message:
           "PATCH_REPLAY_ACCEPTED_FILE and PATCH_REPLAY_RESULT_FILE must differ")
  else
    Ok
      {
        target_url;
        namespace;
        execution_id;
        expected_result;
        accepted_file;
        result_file;
      }

(** Clears both driver markers before requesting a new workflow. Failing closed
    prevents a controller from pairing the current server run with a marker
    written by an interrupted earlier driver process. *)
let clear_markers configuration =
  let open Temporal.Result_syntax in
  let* () =
    Support.clear_file_before_start ~label:"patch replay driver acceptance"
      configuration.accepted_file
  in
  Support.clear_file_before_start ~label:"patch replay driver result"
    configuration.result_file

(** Converts a terminal client outcome into the fixture's exact success
    assertion. Non-success terminal values remain typed errors rather than
    exceptions, and their payload details are not copied into marker files. *)
let require_expected_completion expected = function
  | Client.Completed actual when String.equal actual expected -> Ok ()
  | Client.Completed _ ->
      Error
        (Error.defect
           ~message:"patch replay workflow completed with an unexpected branch marker")
  | Client.Failed { error; _ }
  | Client.Cancelled error
  | Client.Terminated error
  | Client.Timed_out { error; _ } ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "patch replay workflow ended with %s: %s"
                (Error.kind error) (Error.message error)))
  | Client.Continued_as_new execution ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf
                "patch replay workflow unexpectedly continued as new at run %s"
                execution.run_id))

(** Requests client teardown without allowing an unexpected boundary exception
    to bypass the driver's typed error contract. The client is the only owner
    of this process's native client graph, so this helper is called exactly
    once after successful construction. *)
let shutdown_client_safely client =
  try Client.shutdown client with
  | exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "patch replay client shutdown raised: %s"
                (Printexc.to_string exception_)))

(** Starts one exact workflow run, publishes the returned server identity, and
    waits for the configured branch result. Once [Client.create] succeeds,
    every exit path requests teardown so the client-side Rust graph has one
    deterministic release path. A body failure takes precedence over a later
    shutdown failure: it identifies why the acceptance failed, while shutdown
    is still attempted and its error is returned when the body succeeded. *)
let run () =
  let open Temporal.Result_syntax in
  let* configuration = configuration () in
  let* () = clear_markers configuration in
  let* client =
    Client.create ~target_url:configuration.target_url ~namespace:configuration.namespace
      ~identity:"ocaml-temporal-patch-replay-driver" ()
  in
  let body_result =
    try
      let* handle =
        Client.start client ~workflow:Support.workflow_reference
          ~task_queue:Support.task_queue ~id:configuration.execution_id ~input:()
          ()
      in
      let* () =
        Support.publish_marker ~path:configuration.accepted_file
          ~contents:
            (Printf.sprintf "workflow_id=%s\nrun_id=%s\n"
               (Client.workflow_id handle) (Client.run_id handle))
      in
      let* outcome = Client.wait handle in
      let* () = require_expected_completion configuration.expected_result outcome in
      Support.publish_marker ~path:configuration.result_file
        ~contents:
          (Printf.sprintf "workflow_id=%s\nrun_id=%s\nresult=%s\n"
             (Client.workflow_id handle) (Client.run_id handle)
             configuration.expected_result)
    with exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "patch replay driver body raised: %s"
                (Printexc.to_string exception_)))
  in
  let shutdown_result = shutdown_client_safely client in
  match body_result with
  | Error _ as error -> error
  | Ok () -> shutdown_result

(** Reports the typed driver outcome as a useful command status without
    printing input, protocol JSON, or arbitrary activity payloads. *)
let () =
  match run () with
  | Ok () -> Printf.printf "patch replay driver completed\n%!"
  | Error error ->
      Printf.eprintf "patch replay driver failed (%s): %s\n%!" (Error.kind error)
        (Error.message error);
      exit 1

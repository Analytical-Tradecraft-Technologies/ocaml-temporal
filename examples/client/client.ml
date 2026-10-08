(** One-shot client example that starts the example workflow, waits for that
    exact run, and prints its terminal result before releasing its connection.
*)

(** Selects the optional name argument, using a deterministic demonstration
    value when no command-line argument is supplied. *)
let input_name () =
  match Array.to_list Sys.argv with
  | _program :: name :: [] -> Ok name
  | [ _program ] -> Ok "Ada Lovelace"
  | _ ->
      Error
        (Temporal.Error.defect
           ~message:"usage: client.exe [name]")

(** Selects a workflow ID, preserving an explicit environment value for users
    who need to retry the same logical client request. The default is intended
    for a local demonstration and may need changing in a shared namespace. *)
let workflow_id () =
  match Sys.getenv_opt "TEMPORAL_WORKFLOW_ID" with
  | None -> Ok "ocaml-temporal-example-message"
  | Some "" ->
      Error
        (Temporal.Error.defect
           ~message:"TEMPORAL_WORKFLOW_ID must not be empty")
  | Some id -> Ok id

(** Displays one terminal workflow outcome and turns non-success terminal
    states into an expected error so the command exits nonzero after cleanup. *)
let display_terminal = function
  | Temporal.Client.Completed { output = message; _ } ->
      Printf.printf "Workflow completed:\n%s\n%!" message;
      Ok ()
  | Temporal.Client.Failed { error; _ } ->
      Printf.eprintf "Workflow failed: %s\n%!" (Temporal.Error.message error);
      Error error
  | Temporal.Client.Cancelled error ->
      Printf.eprintf "Workflow was cancelled: %s\n%!" (Temporal.Error.message error);
      Error error
  | Temporal.Client.Terminated error ->
      Printf.eprintf "Workflow was terminated: %s\n%!" (Temporal.Error.message error);
      Error error
  | Temporal.Client.Timed_out { error; _ } ->
      Printf.eprintf "Workflow timed out: %s\n%!" (Temporal.Error.message error);
      Error error
  | Temporal.Client.Continued_as_new { workflow_id; run_id; _ } ->
      Printf.printf "Workflow continued as new: workflow_id=%s run_id=%s\n%!"
        workflow_id run_id;
      Error
        (Temporal.Error.defect
           ~message:"the example client does not automatically follow continue-as-new")

(** Connects, starts one workflow with a stable idempotency request ID, waits
    for the exact returned run, and displays its typed terminal outcome. *)
let run () =
  let open Temporal.Result_syntax in
  let* config = Example_support.Config.from_environment () in
  let* name = input_name () in
  let* id = workflow_id () in
  Example_support.Lifecycle.with_client config (fun client ->
      let* handle =
        Temporal.Client.start client
          ~request_id:("ocaml-temporal-example-start:" ^ id)
          ~workflow:Example_support.Definitions.remote_compose_message
          ~task_queue:config.task_queue ~id ~input:name ()
      in
      let* terminal = Temporal.Client.wait handle in
      display_terminal terminal)

(** Reports failures only after [run] has requested client cleanup. *)
let () =
  match run () with
  | Ok () -> ()
  | Error error ->
      Example_support.report_error "example client" error;
      exit 1

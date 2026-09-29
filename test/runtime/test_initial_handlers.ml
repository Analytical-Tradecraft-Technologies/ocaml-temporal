(** Initial handlers must start before the workflow body, including through
    native validation and public callback dispatch. *)
module T = Temporal
module Execution = Temporal_runtime.Execution
module Native = Temporal_runtime.Native_execution
module Protocol = Temporal_protocol.Workflow_protocol
module Activation = Temporal_runtime.Activation

(** Compares observable results with a scenario-specific diagnostic. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Copies a public payload into the runtime-owned representation. *)
let base_payload (payload : T.Payload.t) : Temporal_base.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Copies an activation argument before public codec dispatch. *)
let public_payload (payload : Temporal_base.Payload.t) : T.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Preserves callback failures across the public/private boundary. *)
let base_error error =
  let view = T.Error.view error in
  Temporal_base.Error.make ~category:view.category ~message:view.message
    ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ()

(** Turns an unexpected public error into a useful assertion failure. *)
let require = function
  | Ok value -> value
  | Error error -> failwith (T.Error.message error)

(** Encodes a typed value for the real native activation validator. *)
let payload codec value : Protocol.payload =
  let payload = require (T.Codec.encode codec value) in
  { metadata = List.map (fun (key, value) -> key, Bytes.of_string value)
      payload.metadata;
    data = Bytes.copy payload.data }

(** Initializes the same workflow type used by all focused scenarios. *)
let initialize =
  Protocol.Initialize_workflow
    { workflow_id = "initial-handlers-id"; workflow_type = "initial-handlers";
      arguments = [ payload T.Codec.unit () ]; randomness_seed = "1";
      attempt = 1; context = None }

(** Sends one string argument through the public signal decoder. *)
let signal value =
  Protocol.Signal_workflow
    { signal_name = "record"; input = [ payload T.Codec.string value ];
      identity = "test"; headers = [] }

(** Constructs an execution with the production public signal dispatch boundary. *)
let execution ?(update_handlers = []) ~handler root =
  let definition = Temporal_base.Definition.make ~name:"initial-handlers"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.string
      ~implementation:(Some (fun () -> Result.map_error base_error (root ()))) in
  let signal = T.Signal.define ~name:"record" ~input:T.Codec.string in
  let public_handler = T.Signal.Handler.make signal handler in
  let runtime_handler = Execution.make_signal_handler ~name:"record"
      ~dispatch:(fun (signal : Execution.signal) ->
        match signal.input with
        | [ argument ] -> T.Signal.Handler.dispatch public_handler
            (public_payload argument) |> Result.map_error base_error
        | _ -> failwith "unexpected signal arity") in
  Execution.start ~signal_handlers:[ runtime_handler ] ~update_handlers definition ()

(** Exercises native validation, context installation, translation, and execution. *)
let activate ~is_replaying execution jobs =
  let activation : Protocol.activation =
    { run_id = "initial-handlers-run";
      timestamp = Some { seconds = 1L; nanoseconds = 0 };
      is_replaying; history_length = 7L; jobs; metadata = None } in
  match Native.activate execution activation with
  | Ok completion -> completion
  | Error error -> failwith (Native.error_view error).message

(** Checks both the exact command batch and successful task status. *)
let expect_completion expected (completion : Protocol.completion) =
  expect "workflow task failure" None completion.task_failure;
  expect "workflow completion"
    [ Protocol.Complete_workflow { result = Some (payload T.Codec.string expected) } ]
    completion.commands

(** Multiple signals must affect the root's first decision, even if it never
    yields. Repeating with a root timer detects stale pre-suspension decisions. *)
let test_initial_signals ~is_replaying ~root_suspends =
  let trace = ref [] in
  let record value = trace := !trace @ [ value ] in
  let execution = execution
      ~handler:(fun value -> record value; Ok ())
      (fun () ->
        let observed = String.concat "," !trace in
        record "root";
        if root_suspends then require (T.Workflow.sleep (T.Duration.of_ms 10L));
        Ok observed) in
  Fun.protect ~finally:(fun () -> Execution.shutdown execution) (fun () ->
    let initial = activate ~is_replaying execution
        [ initialize; signal "first"; signal "second" ] in
    expect "initial signal invocation order" [ "first"; "second"; "root" ] !trace;
    let final = if root_suspends then (
      expect "root suspension" [ Protocol.Start_timer { seq = 1L;
        start_to_fire_timeout = { seconds = 0L; nanoseconds = 10_000_000 } } ]
        initial.commands;
      activate ~is_replaying execution [ Protocol.Fire_timer { seq = 1L } ])
      else initial in
    expect_completion "first,second" final)

(** A suspended initial handler must not hold up later handlers or the root.
    Its continuation still resumes normally in the following activation. *)
let test_suspending_signal ~is_replaying =
  let trace = ref [] in
  let finished = ref false in
  let record value = trace := !trace @ [ value ] in
  let execution = execution
      ~handler:(fun value ->
        record value;
        if value = "first" then (
          require (T.Workflow.sleep (T.Duration.of_ms 10L));
          record "resumed";
          finished := true);
        Ok ())
      (fun () ->
        record "root";
        require (T.Condition.wait_until (fun () -> !finished));
        Ok (String.concat "," !trace)) in
  Fun.protect ~finally:(fun () -> Execution.shutdown execution) (fun () ->
    let initial = activate ~is_replaying execution
        [ initialize; signal "first"; signal "second" ] in
    expect "suspended handler does not block root" [ "first"; "second"; "root" ] !trace;
    expect "handler timer" [ Protocol.Start_timer { seq = 1L;
      start_to_fire_timeout = { seconds = 0L; nanoseconds = 10_000_000 } } ]
      initial.commands;
    expect "initial task failure" None initial.task_failure;
    expect_completion "first,second,root,resumed"
      (activate ~is_replaying execution [ Protocol.Fire_timer { seq = 1L } ]))

(** Initial updates have the same invocation ordering as signals. Validation
    remains a live-only operation, and acceptance precedes handler completion. *)
let test_initial_update ~is_replaying =
  let value = ref "before-update" in
  let trace = ref [] in
  let record event = trace := !trace @ [ event ] in
  let update = T.Update.define ~name:"set-value" ~input:T.Codec.unit
      ~output:T.Codec.unit in
  let public_handler = T.Update.Handler.make update
      ~validator:(fun () -> record "validate"; Ok ())
      (fun () -> record "update"; value := "after-update"; Ok ()) in
  let runtime_handler = Execution.make_update_handler ~name:"set-value"
      ~dispatch:(fun ~run_validator ~on_validated (update : Execution.update) ->
        match update.input with
        | [ argument ] ->
            T.Update.Handler.dispatch ~run_validator ~on_validated public_handler
              (public_payload argument)
            |> Result.map base_payload |> Result.map_error base_error
        | _ -> failwith "unexpected update arity") in
  let execution = execution ~update_handlers:[ runtime_handler ]
      ~handler:(fun _ -> record "signal"; Ok ())
      (fun () -> record "root"; Ok !value) in
  Fun.protect ~finally:(fun () -> Execution.shutdown execution) (fun () ->
    let completion = activate ~is_replaying execution
        [ initialize; signal "first";
          Protocol.Do_update
            { id = "update"; protocol_instance_id = "protocol"; name = "set-value";
              input = [ payload T.Codec.unit () ]; headers = [];
              meta = { identity = "test"; update_id = "update" };
              run_validator = not is_replaying } ] in
    expect "initial update invocation order"
      (if is_replaying then [ "signal"; "update"; "root" ]
       else [ "signal"; "validate"; "update"; "root" ]) !trace;
    expect "update task failure" None completion.task_failure;
    expect "initial update results"
      [ Protocol.Update_response { protocol_instance_id = "protocol"; response = Update_accepted };
        Protocol.Update_response { protocol_instance_id = "protocol";
          response = Update_completed (payload T.Codec.unit ()) };
        Protocol.Complete_workflow { result = Some (payload T.Codec.string "after-update") } ]
      completion.commands)

(** A handler failure must stop the queued root, including the public exception
    boundary that classifies defects as retryable workflow-task failures. *)
let test_initial_failure ~is_replaying ~defect =
  let root_ran = ref false in
  let execution = execution
      ~handler:(fun _ ->
        if defect then failwith "initial handler defect"
        else Error (T.Error.make ~category:`Workflow ~message:"initial handler failed" ()))
      (fun () -> root_ran := true; Ok "unexpected") in
  Fun.protect ~finally:(fun () -> Execution.shutdown execution) (fun () ->
    let completion = activate ~is_replaying execution [ initialize; signal "first" ] in
    expect "failed handler prevents root" false !root_ran;
    if defect then (
      expect "defect discards commands" [] completion.commands;
      expect "defect fails workflow task" true (Option.is_some completion.task_failure))
    else (
      expect "application failure task status" None completion.task_failure;
      match completion.commands with
      | [ Protocol.Fail_workflow _ ] -> ()
      | _ -> failwith "expected application failure command"))

(** Recording initialization early must retain duplicate-start validation and
    prevent cancellation or eviction from starting a deferred root. *)
let test_start_guards () =
  List.iter (fun ending ->
    let root_ran = ref false in
    let execution = execution ~handler:(fun _ -> Ok ())
        (fun () -> root_ran := true; Ok "unexpected") in
    Fun.protect ~finally:(fun () -> Execution.shutdown execution) (fun () ->
      let commands = Execution.activate execution [ Activation.Start_workflow; ending ] in
      expect "initial terminal prevents root" false !root_ran;
      match ending with
      | Activation.Start_workflow ->
          expect "duplicate initialization discards commands" [] commands;
          expect "duplicate initialization task failure" true
            (Option.is_some (Execution.task_failure execution))
      | Activation.Cancel_workflow ->
          expect "initial cancellation" [ Activation.Cancel_workflow_execution ] commands
      | Activation.Remove_from_cache -> expect "initial eviction" [] commands
      | _ -> failwith "invalid terminal fixture"))
    [ Activation.Start_workflow; Activation.Cancel_workflow; Activation.Remove_from_cache ]

let () =
  List.iter (fun is_replaying ->
    List.iter (fun root_suspends -> test_initial_signals ~is_replaying ~root_suspends)
      [ false; true ];
    test_suspending_signal ~is_replaying;
    test_initial_update ~is_replaying;
    List.iter (fun defect -> test_initial_failure ~is_replaying ~defect) [ false; true ])
    [ false; true ];
  test_start_guards ()

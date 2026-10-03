(** Public handler dispatch must preserve private scheduler control flow when
    a terminal operation releases suspended or already-queued continuations. *)
module T = Temporal
module Execution = Temporal_runtime.Execution
module Activation = Temporal_runtime.Activation

(** The two asynchronous public handler boundaries share the same lifecycle. *)
type handler_kind = Signal | Update

(** Ways an execution can stop while another handler still owns a fiber. *)
type ending = Complete | Fail | Cancel | Continue | Evict | Shutdown

(** Compares observable activation results with a useful scenario label. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Copies a public payload across the same boundary as native registration. *)
let base_payload (payload : T.Payload.t) : Temporal_base.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Copies an activation payload before handing it to the public decoder. *)
let public_payload (payload : Temporal_base.Payload.t) : T.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Preserves typed callback errors at the private runtime boundary. *)
let base_error error =
  let view = T.Error.view error in
  Temporal_base.Error.make ~category:view.category ~message:view.message
    ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ()

(** Supplies one valid encoded input, as required by native registration. *)
let unit_payload =
  match T.Codec.encode T.Codec.unit () with
  | Ok payload -> base_payload payload
  | Error error -> failwith (T.Error.message error)

(** Builds a runtime execution with real public signal or update dispatch. *)
let execution kind ~root ~handler =
  let definition =
    Temporal_base.Definition.make ~name:"handler_teardown"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Result.map_error base_error (root ())))
  in
  match kind with
  | Signal ->
      let signal = T.Signal.define ~name:"handler" ~input:T.Codec.unit in
      let public_handler = T.Signal.Handler.make signal handler in
      let runtime_handler =
        Execution.make_signal_handler ~name:"handler"
          ~dispatch:(fun (signal : Execution.signal) ->
            match signal.input with
            | [ payload ] ->
                T.Signal.Handler.dispatch public_handler (public_payload payload)
                |> Result.map_error base_error
            | _ -> failwith "invalid signal fixture input")
      in
      (Execution.start ~signal_handlers:[ runtime_handler ] definition (),
       Activation.Signal_workflow
         { signal_name = "handler"; input = [ unit_payload ];
           identity = "test"; headers = [] })
  | Update ->
      let update =
        T.Update.define ~name:"handler" ~input:T.Codec.unit ~output:T.Codec.unit
      in
      let public_handler = T.Update.Handler.make update handler in
      let runtime_handler =
        Execution.make_update_handler ~name:"handler"
          ~dispatch:(fun ~run_validator ~on_validated (update : Execution.update) ->
            match update.input with
            | [ payload ] ->
                T.Update.Handler.dispatch ~run_validator ~on_validated
                  public_handler (public_payload payload)
                |> Result.map base_payload |> Result.map_error base_error
            | _ -> failwith "invalid update fixture input")
      in
      (Execution.start ~update_handlers:[ runtime_handler ] definition (),
       Activation.Do_update
         { id = "update"; protocol_instance_id = "protocol"; name = "handler";
           input = [ unit_payload ]; headers = []; identity = "test";
           update_id = "update"; run_validator = true })

(** Starts a real timer so the root and handler can be resumed independently. *)
let sleep () = T.Workflow.sleep (T.Duration.of_ms 100L)

(** Names the successor without introducing native resources or a server. *)
let successor =
  T.Workflow.remote ~name:"successor" ~input:T.Codec.unit ~output:T.Codec.unit

(** Returns the deliberate application failure used by the terminal matrix. *)
let application_error () =
  T.Error.make ~category:`Workflow ~message:"intentional workflow failure" ()

(** Verifies terminal commands and exactly-once cleanup with either a parked
    handler, an already-queued handler continuation, or a finished handler. *)
let test_teardown kind ending ~queued ~finished =
  let finalizers = ref 0 in
  let returned = ref false in
  let handler () =
    Fun.protect ~finally:(fun () -> incr finalizers) (fun () ->
        match sleep () with
        | Error _ as error -> error
        | Ok () -> returned := true; Ok ())
  in
  let root () =
    match sleep () with
    | Error _ as error -> error
    | Ok () ->
        match ending with
        | Fail -> Error (application_error ())
        | Continue -> T.Workflow.continue_as_new successor ()
        | Complete | Cancel | Evict | Shutdown -> Ok ()
  in
  let execution, job = execution kind ~root ~handler in
  expect "root timer" [ Activation.Start_timer { seq = 1L; milliseconds = 100L } ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  let acceptance =
    match kind with
    | Signal -> []
    | Update ->
        [ Activation.Update_response
            { protocol_instance_id = "protocol"; response = `Accepted } ]
  in
  expect "handler suspension"
    (acceptance @ [ Activation.Start_timer { seq = 2L; milliseconds = 100L } ])
    (Execution.activate execution [ job ]);
  if finished then (
    let completion =
      match kind with
      | Signal -> []
      | Update ->
          [ Activation.Update_response
              { protocol_instance_id = "protocol";
                response = `Completed unit_payload } ]
    in
    expect "handler finished first" completion
      (Execution.activate execution [ Activation.Fire_timer { seq = 2L } ]));
  let commands =
    match ending with
    | Cancel -> Execution.activate execution [ Activation.Cancel_workflow ]
    | Evict -> Execution.activate execution [ Activation.Remove_from_cache ]
    | Shutdown -> Execution.shutdown execution; []
    | Complete | Fail | Continue ->
        let jobs = [ Activation.Fire_timer { seq = 1L } ] in
        let jobs = if queued then jobs @ [ Activation.Fire_timer { seq = 2L } ] else jobs in
        Execution.activate execution jobs
  in
  let expected =
    match ending with
    | Complete -> [ Activation.Complete_workflow unit_payload ]
    | Fail -> [ Activation.Fail_workflow (base_error (application_error ())) ]
    | Cancel -> [ Activation.Cancel_workflow_execution ]
    | Continue ->
        [ Activation.Continue_as_new { workflow_type = "successor"; input = unit_payload } ]
    | Evict | Shutdown -> []
  in
  expect "teardown preserves terminal command" expected commands;
  expect "teardown is not a task failure" None (Execution.task_failure execution);
  expect "handler normal return" finished !returned;
  expect "handler cleanup" 1 !finalizers;
  Execution.shutdown execution;
  expect "repeated shutdown cleanup" 1 !finalizers

(** A handler can itself abort the current run with continue-as-new; neither
    public dispatch nor the runtime update wrapper may turn that into a defect. *)
let test_handler_continue kind =
  let finalizers = ref 0 in
  let handler () =
    Fun.protect ~finally:(fun () -> incr finalizers) (fun () ->
        T.Workflow.continue_as_new successor ())
  in
  let execution, job = execution kind ~root:sleep ~handler in
  ignore (Execution.activate execution [ Activation.Start_workflow ]);
  let commands = Execution.activate execution [ job ] in
  expect "handler continue command" true
    (List.exists (function Activation.Continue_as_new _ -> true | _ -> false) commands);
  expect "handler abort is not a task failure" None (Execution.task_failure execution);
  expect "aborted handler cleanup" 1 !finalizers;
  expect "continued run is sealed" []
    (Execution.activate execution [ Activation.Cancel_workflow ])

(** The control-flow exception exemption must not hide real callback defects. *)
let test_handler_defect kind =
  let execution, job =
    execution kind ~root:sleep ~handler:(fun () -> failwith "handler defect")
  in
  ignore (Execution.activate execution [ Activation.Start_workflow ]);
  expect "defect discards command batch" [] (Execution.activate execution [ job ]);
  match Execution.task_failure execution with
  | Some error -> expect "callback defect category" `Defect (Temporal_base.Error.view error).category
  | None -> failwith "real callback defect was suppressed"

let () =
  List.iter
    (fun kind ->
      List.iter
        (fun ending -> test_teardown kind ending ~queued:false ~finished:false)
        [ Complete; Fail; Cancel; Continue; Evict; Shutdown ];
      test_teardown kind Complete ~queued:true ~finished:false;
      test_teardown kind Complete ~queued:false ~finished:true;
      test_handler_continue kind;
      test_handler_defect kind)
    [ Signal; Update ]

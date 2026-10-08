module Protocol = Temporal_protocol.Client_protocol
module Workflow = Temporal_protocol.Workflow_protocol

(** Converts a protocol error into a useful test failure without printing any
    server-controlled payload bytes. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      let view = Protocol.error_view error in
      failwith (Printf.sprintf "%s at %s: %s" view.code view.path view.message)

(** Requires strict validation to reject the supplied malformed JSON or
    request. *)
let require_error = function
  | Error _ -> ()
  | Ok _ -> failwith "expected update protocol validation to fail"

let payload data : Protocol.payload =
  { Workflow.metadata = [ ("encoding", Bytes.of_string "binary/plain") ]; data }

let execution : Protocol.execution =
  { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }

let update_request : Protocol.update_request =
  {
    execution;
    update_id = "update-1";
    update_name = "set_state";
    input = [ payload (Bytes.of_string "input") ];
    rpc_deadline = None;
  }

let poll_request : Protocol.poll_update_request =
  { execution; update_id = update_request.update_id }

let failed_update_json =
  "{\"outcome\":{\"kind\":\"failed\",\"failure\":{\"message\":\"failed\",\"source\":\"worker\",\"stack_trace\":\"\",\"encoded_attributes\":null,\"cause\":null,\"info\":{\"kind\":\"application\",\"type\":\"Failure\",\"non_retryable\":true,\"details\":[]}}}}"

let test_update_protocol () =
  ignore (unwrap (Protocol.encode_update_request update_request));
  ignore (unwrap (Protocol.encode_poll_update_request poll_request));
  let prefix =
    "{\"update_id\":\"update-1\",\"execution\":{\"namespace\":\"default\",\"workflow_id\":\"workflow-1\",\"run_id\":\"run-1\"},\"outcome\":"
  in
  let pending =
    unwrap
      (Protocol.decode_update_response ~request:update_request
         (prefix ^ "null}"))
  in
  if pending.outcome <> None then failwith "pending update was not preserved";
  let completed =
    unwrap
      (Protocol.decode_update_response ~request:update_request
         (prefix ^ "{\"kind\":\"completed\",\"result\":[]}}"))
  in
  (match completed.outcome with
  | Some (Protocol.Update_completed { result = [] }) -> ()
  | _ -> failwith "completed update result changed shape");
  let failed = unwrap (Protocol.decode_poll_update_response failed_update_json) in
  (match failed.outcome with
  | Some (Protocol.Update_failed { failure = { message = "failed"; _ } }) -> ()
  | _ -> failwith "failed update result changed shape");
  List.iter
    (fun document -> require_error (Protocol.decode_poll_update_response document))
    [
      "{\"outcome\":{\"kind\":\"completed\",\"result\":[],\"extra\":true}}";
      "{\"outcome\":{\"kind\":\"unknown\",\"result\":[]}}";
      "{\"outcome\":{}}";
    ];
  require_error
    (Protocol.decode_update_response ~request:update_request
       "{\"update_id\":\"other\",\"execution\":{\"namespace\":\"default\",\"workflow_id\":\"workflow-1\",\"run_id\":\"run-1\"},\"outcome\":null}");
  List.iter
    (fun request -> require_error (Protocol.encode_update_request request))
    [
      { update_request with update_id = "" };
      { update_request with update_name = "contains\000nul" };
      { update_request with execution = { execution with workflow_id = "" } };
    ];
  List.iter
    (fun request -> require_error (Protocol.encode_poll_update_request request))
    [
      { poll_request with update_id = "" };
      { poll_request with execution = { execution with namespace = "" } };
    ]

let () =
  test_update_protocol ();
  print_endline "PASS client update protocol"

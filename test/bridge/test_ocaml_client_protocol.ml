(* Keep the fixture code focused on the two protocol modules under test; these
   aliases also make every assertion explicit about which JSON vocabulary it
   exercises. *)
module Protocol = Temporal_protocol.Client_protocol
module Workflow = Temporal_protocol.Workflow_protocol

(** Extracts a valid protocol value or fails with the privacy-safe diagnostic. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      let view = Protocol.error_view error in
      failwith (Printf.sprintf "%s at %s: %s" view.code view.path view.message)

(** Requires a decoder or encoder to reject the supplied malformed value. *)
let require_error = function
  | Error _ -> ()
  | Ok _ -> failwith "expected client protocol validation to fail"

(** Requires malformed input to retain the strict decoder's precise safe path. *)
let check_error_path label expected = function
  | Error error ->
      let actual = (Protocol.error_view error).path in
      if not (String.equal expected actual) then
        failwith
          (Printf.sprintf "%s path differed: expected %s, got %s" label expected
             actual)
  | Ok _ -> failwith (label ^ " unexpectedly passed validation")

(** Checks that canonical JSON contains a small structural marker without
    coupling this test to association-list member ordering. *)
let require_fragment label fragment value =
  let fragment_length = String.length fragment in
  (* Search all byte offsets instead of assuming JSON member ordering or UTF-8
     character boundaries; the fixture marker is intentionally structural. *)
  let rec contains offset =
    if offset + fragment_length > String.length value then false
    else if String.sub value offset fragment_length = fragment then true
    else contains (offset + 1)
  in
  if not (contains 0) then
    failwith (label ^ " did not contain its expected JSON marker")

(** Creates a binary-safe payload used by start requests and terminal results. *)
let payload data : Protocol.payload =
  { Workflow.metadata = [ ("encoding", Bytes.of_string "binary/plain") ]; data }

(** Shared execution identity used by all exact-run response fixtures. *)
let execution : Protocol.execution =
  { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }

(** Cancellation requests retain a stable operation identifier so a caller can
    retry a transport timeout without creating a second logical request. *)
let cancel_request : Protocol.cancel_request =
  { execution; request_id = "cancel-1"; reason = "operator requested cancellation" }

(** Reset requests identify the exact workflow-task boundary from which
    Temporal should rebuild a new run. The event ID is intentionally kept as an
    [int64] all the way to the JSON integer literal. *)
let reset_request : Protocol.reset_request =
  {
    execution;
    request_id = "reset-1";
    reason = "replay from workflow task";
    workflow_task_finish_event_id = 4L;
  }

(** Signal requests carry the exact run, stable operation ID, and ordered
    payloads that the Rust bridge forwards to Temporal's signal RPC. *)
let signal_request : Protocol.signal_request =
  {
    execution;
    signal_name = "add_document";
    request_id = "signal-1";
    input = [];
  }

(** Output-only query request used to exercise exact-run and query-name
    validation in the OCaml half of the closed bridge protocol. *)
let query_request : Protocol.query_request =
  { execution; query_type = "current_state"; input = [] }

(** Start requests use the same workflow identity as the response fixtures. *)
let start_request : Protocol.start_request =
  {
    request_id = "request-1";
    namespace = execution.namespace;
    workflow_id = execution.workflow_id;
    workflow_type = "Smoke";
    task_queue = "queue";
    input = [];
    memo = [];
    search_attributes = [];
  }

(** The canonical payload wrapper for the bytes [ok]. *)
let ok_payload_json =
  {|{"metadata":{"encoding":{"encoding":"base64","data":"YmluYXJ5L3BsYWlu"}},"data":{"encoding":"base64","data":"b2s="}}|}

(** Decodes every terminal outcome supported by the closed native client ABI. *)
let test_terminal_outcomes () =
  let prefix =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"outcome":|}
  in
  let suffix = "}" in
  let documents =
    [
      (prefix
      ^ {|{"kind":"completed","result":|}
      ^ "[" ^ ok_payload_json ^ "]"
      ^ {|,"successor":null}|}
      ^ suffix);
      (prefix
      ^ {|{"kind":"failed","failure":{"message":"failed","source":"worker","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"application","type":"Failure","non_retryable":true,"details":[]}},"successor":null}|}
      ^ suffix);
      (prefix ^ {|{"kind":"cancelled","details":[]}|} ^ suffix);
      (prefix ^ {|{"kind":"terminated","details":[]}|} ^ suffix);
      (prefix ^ {|{"kind":"timed_out","successor":null}|} ^ suffix);
      (prefix
      ^ {|{"kind":"continued_as_new","successor":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}}|}
      ^ suffix);
    ]
  in
  List.iter
    (fun document ->
      ignore (unwrap (Protocol.decode_wait_response ~request:execution document)))
    documents;
  let completed =
    unwrap (Protocol.decode_wait_response ~request:execution (List.hd documents))
  in
  match completed.outcome with
  | Protocol.Completed { result = [ value ]; successor = None } ->
      if not (Bytes.equal value.data (Bytes.of_string "ok")) then
        failwith "completed result payload changed"
  | _ -> failwith "completed response did not retain its result shape"

(** Rejects successor identities that would silently leave or repeat the exact
    run selected by the wait request. *)
let test_successor_identity_validation () =
  let response suffix =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"outcome":{"kind":"continued_as_new","successor":|}
    ^ suffix ^ "}}"
  in
  List.iter
    (fun suffix ->
      require_error
        (Protocol.decode_wait_response ~request:execution (response suffix)))
    [
      {|{"namespace":"other","workflow_id":"workflow-1","run_id":"run-2"}|};
      {|{"namespace":"default","workflow_id":"other","run_id":"run-2"}|};
      {|{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"}|};
    ]

(** Confirms request encoders validate identifiers before sending bytes to Rust. *)
let test_request_validation () =
  let valid_start : Protocol.start_request =
    { start_request with input = [ payload (Bytes.of_string "input") ] }
  in
  let encoded_start = unwrap (Protocol.encode_start_request valid_start) in
  require_fragment "start request" "{" encoded_start;
  require_fragment "start request ID" "request-1" encoded_start;
  let encoded_wait = unwrap (Protocol.encode_wait_request execution) in
  require_fragment "wait request" "{" encoded_wait;
  let encoded_cancel = unwrap (Protocol.encode_cancel_request cancel_request) in
  require_fragment "cancel request ID" "cancel-1" encoded_cancel;
  require_fragment "cancel request reason" "operator requested cancellation"
    encoded_cancel;
  List.iter
    (fun request -> require_error (Protocol.encode_start_request request))
    [
      { valid_start with request_id = "" };
      { valid_start with namespace = "" };
      { valid_start with workflow_id = "contains\000nul" };
      { valid_start with workflow_type = String.make 65_537 'x' };
      { valid_start with task_queue = "" };
    ];
  List.iter
    (fun request -> require_error (Protocol.encode_wait_request request))
    [
      { execution with namespace = "" };
      { execution with workflow_id = "contains\000nul" };
      { execution with run_id = String.make 65_537 'x' };
    ];
  List.iter
    (fun request -> require_error (Protocol.encode_cancel_request request))
    [
      { cancel_request with execution = { execution with namespace = "" } };
      { cancel_request with execution = { execution with run_id = "" } };
      { cancel_request with request_id = "contains\000nul" };
      { cancel_request with reason = String.make 65_537 'x' };
      { cancel_request with reason = "contains\000nul" };
    ];
  (* An empty reason is valid operator context; it is not an omitted field. *)
  ignore
    (unwrap
       (Protocol.encode_cancel_request
          { cancel_request with reason = "" }))
  ;
  let encoded_signal = unwrap (Protocol.encode_signal_request signal_request) in
  require_fragment "signal name" "add_document" encoded_signal;
  require_fragment "signal request ID" "signal-1" encoded_signal;
  List.iter
    (fun request -> require_error (Protocol.encode_signal_request request))
    [
      { signal_request with execution = { execution with namespace = "" } };
      { signal_request with execution = { execution with run_id = "" } };
      { signal_request with signal_name = "" };
      { signal_request with request_id = "contains\000nul" };
    ]

(** Checks the cancellation acknowledgement's closed shape. A false value is
    rejected because the native bridge exposes only positively acknowledged
    control operations, while unknown and duplicate members must fail closed. *)
let test_cancellation_protocol () =
  let encoded = unwrap (Protocol.encode_cancel_request cancel_request) in
  require_fragment "cancel request workflow ID" "workflow-1" encoded;
  let acknowledged =
    unwrap (Protocol.decode_cancel_response {|{"acknowledged":true}|})
  in
  if not acknowledged.acknowledged then
    failwith "positive cancellation acknowledgement changed shape";
  List.iter
    (fun document -> require_error (Protocol.decode_cancel_response document))
    [
      {|{"acknowledged":false}|};
      {|{"acknowledged":true,"unexpected":true}|};
      {|{"acknowledged":true,"acknowledged":false}|};
      {|{"acknowledged":"true"}|};
    ]

(** Checks exact-run reset encoding, response identity correlation, and the
    fail-closed handling of invalid event boundaries or response shapes. *)
let test_reset_protocol () =
  let encoded = unwrap (Protocol.encode_reset_request reset_request) in
  require_fragment "reset request ID" "reset-1" encoded;
  require_fragment "reset event boundary" "workflow_task_finish_event_id" encoded;
  require_fragment "reset event value" "4" encoded;
  let response =
    unwrap
      (Protocol.decode_reset_response ~request:reset_request
         {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}}|})
  in
  if response.execution.run_id <> "run-2" then
    failwith "reset response did not expose the new run identity";
  List.iter
    (fun request -> require_error (Protocol.encode_reset_request request))
    [
      { reset_request with workflow_task_finish_event_id = -1L };
      { reset_request with request_id = "" };
      { reset_request with reason = "contains\000nul" };
    ];
  List.iter
    (fun document ->
      require_error
        (Protocol.decode_reset_response ~request:reset_request document))
    [
      {|{"execution":{"namespace":"other","workflow_id":"workflow-1","run_id":"run-2"}}|};
      {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"},"unexpected":true}|};
      {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":""}}|};
    ]

(** Checks the exact-run termination request and its positive acknowledgement.
    The operation intentionally has no cancellation request ID because
    Temporal's terminate RPC does not expose one. *)
let test_termination_protocol () =
  let request : Protocol.terminate_request =
    { execution; reason = "operator test" }
  in
  let encoded = unwrap (Protocol.encode_terminate_request request) in
  require_fragment "terminate reason" "operator test" encoded;
  let acknowledged =
    unwrap (Protocol.decode_terminate_response {|{"acknowledged":true}|})
  in
  if not acknowledged.acknowledged then
    failwith "positive termination acknowledgement changed shape";
  List.iter
    (fun document -> require_error (Protocol.decode_terminate_response document))
    [ {|{"acknowledged":false}|}; {|{"acknowledged":true,"extra":1}|} ]

(** Checks that signal acknowledgement and operation-specific error decoding
    fail closed in the same way as cancellation. *)
let test_signal_protocol () =
  let acknowledged =
    unwrap (Protocol.decode_signal_response {|{"acknowledged":true}|})
  in
  if not acknowledged.acknowledged then
    failwith "positive signal acknowledgement changed shape";
  List.iter
    (fun document -> require_error (Protocol.decode_signal_response document))
    [
      {|{"acknowledged":false}|};
      {|{"acknowledged":true,"unexpected":true}|};
      {|{"acknowledged":"true"}|};
    ];
  require_error
    (Protocol.decode_signal_error
       {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}|});
  (match
     unwrap
       (Protocol.decode_signal_error {|{"kind":"rpc","code":"unavailable"}|})
   with
  | Protocol.Rpc { code = "unavailable" } -> ()
  | _ -> failwith "signal RPC error was not typed")

(** Checks the output-only query request/response contract and rejects the
    start-only error category before any native operation is attempted. *)
let test_query_protocol () =
  let encoded = unwrap (Protocol.encode_query_request query_request) in
  require_fragment "query type" "current_state" encoded;
  let response = unwrap (Protocol.decode_query_response {|{"result":[]}|}) in
  if response.result <> [] then failwith "empty query result changed shape";
  List.iter
    (fun document -> require_error (Protocol.decode_query_response document))
    [ {|{"result":[],"unexpected":true}|}; {|{"result":{}}|} ];
  List.iter
    (fun request -> require_error (Protocol.encode_query_request request))
    [
      { query_request with execution = { execution with namespace = "" } };
      { query_request with execution = { execution with run_id = "" } };
      { query_request with query_type = "" };
    ];
  require_error
    (Protocol.decode_query_error
       {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}|});
  match
    unwrap (Protocol.decode_query_error {|{"kind":"rpc","code":"failed_precondition"}|})
  with
  | Protocol.Rpc { code = "failed_precondition" } -> ()
  | _ -> failwith "query rejection was not typed as failed_precondition"

(** Visibility pages use an exact row schema and preserve opaque continuation
    tokens without allowing unknown fields or malformed rows through. *)
let test_visibility_protocol () =
  let request : Protocol.visibility_request =
    {
      namespace = "default";
      query = "WorkflowType = 'Smoke'";
      page_size = 25;
      next_page_token = None;
    }
  in
  let encoded = unwrap (Protocol.encode_visibility_request request) in
  require_fragment "visibility query" "WorkflowType" encoded;
  require_fragment "visibility page size" "25" encoded;
  let response =
    {|{"executions":[{"workflow_id":"workflow-1","run_id":"run-1","workflow_type":"Smoke","task_queue":"queue","status":"running"}],"next_page_token":"dG9rZW4="}|}
  in
  (match unwrap (Protocol.decode_visibility_response response) with
  | { executions = [ row ]; next_page_token = Some "dG9rZW4=" } ->
      assert (String.equal row.workflow_id "workflow-1");
      assert (String.equal row.status "running")
  | _ -> failwith "visibility response changed shape");
  List.iter
    (fun malformed -> require_error (Protocol.decode_visibility_response malformed))
    [
      {|{"executions":[],"next_page_token":null,"extra":true}|};
      {|{"executions":[{"workflow_id":"","run_id":"run-1","workflow_type":"Smoke","task_queue":"queue","status":"running"}],"next_page_token":null}|};
      {|{"executions":[{"workflow_id":"workflow-1","run_id":"run-1","workflow_type":"Smoke","task_queue":"queue","status":"future_status"}],"next_page_token":null}|};
    ]

(** Exercises the asynchronous-start capability and all three terminal outcome
    variants. The request-bound ticket is deliberately opaque: this test can
    recover the retained request only through the protocol accessor, never by
    inspecting or constructing the native ticket string. *)
let test_async_start_protocol () =
  let ticket_json = {|{"ticket":"ticket-1"}|} in
  let ticket =
    unwrap (Protocol.decode_start_ticket ~request:start_request ticket_json)
  in
  if Protocol.start_ticket_request ticket <> start_request then
    failwith "start ticket lost its originating request";
  let encoded_ticket = unwrap (Protocol.encode_start_ticket ticket) in
  require_fragment "start ticket" "ticket-1" encoded_ticket;
  require_error
    (Protocol.decode_start_ticket ~request:start_request
       {|{"ticket":"ticket-1","extra":true}|});
  require_error
    (Protocol.decode_start_ticket ~request:start_request {|{"ticket":""}|});
  let accepted_json =
    {|{"kind":"accepted","execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}}|}
  in
  let accepted =
    unwrap (Protocol.decode_start_outcome ~request:start_request accepted_json)
  in
  (match accepted with
  | Protocol.Accepted { execution = { run_id = "run-2"; _ } } -> ()
  | _ -> failwith "accepted start outcome changed shape");
  let rejected_json =
    {|{"kind":"rejected","error":{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":"run-existing"}}|}
  in
  let rejected =
    unwrap (Protocol.decode_start_outcome ~request:start_request rejected_json)
  in
  (match rejected with
  | Protocol.Rejected
      (Protocol.Already_started
        { workflow_id = "workflow-1"; existing_run_id = Some "run-existing" }) ->
      ()
  | _ -> failwith "rejected start outcome changed shape");
  let unknown_json =
    {|{"kind":"unknown","request_id":"request-1","workflow_id":"workflow-1"}|}
  in
  let unknown =
    unwrap (Protocol.decode_start_outcome ~request:start_request unknown_json)
  in
  (match unknown with
  | Protocol.Unknown { request_id = "request-1"; workflow_id = "workflow-1" } ->
      ()
  | _ -> failwith "unknown start outcome changed shape");
  List.iter
    (fun outcome ->
      ignore (unwrap (Protocol.encode_start_outcome outcome)))
    [
      accepted;
      rejected;
      unknown;
    ];
  require_error
    (Protocol.decode_start_outcome ~request:start_request
       {|{"kind":"unknown","request_id":"other-request","workflow_id":"workflow-1"}|});
  require_error
    (Protocol.decode_start_outcome ~request:start_request
       {|{"kind":"accepted","execution":{"namespace":"other","workflow_id":"workflow-1","run_id":"run-2"}}|});
  require_error
    (Protocol.decode_start_outcome ~request:start_request
       {|{"kind":"rejected","error":{"kind":"already_started","workflow_id":"other-workflow","existing_run_id":null}}|})

(** Keeps duplicate and unknown response fields from being accepted by either
    the JSON foundation or the operation-specific decoder. *)
let test_closed_response_shape () =
  let valid =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"}}|}
  in
  ignore (unwrap (Protocol.decode_start_response ~request:start_request valid));
  require_error
    (Protocol.decode_start_response ~request:start_request
       {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"extra":true}|});
  check_error_path "nested execution duplicate" "$.execution"
    (Protocol.decode_start_response ~request:start_request
       {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","run_id":"run-2"}}|});
  require_error
    (Protocol.decode_wait_response ~request:execution
       {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"outcome":{"kind":"cancelled","details":[],"extra":true}}|})

(** Ensures successful responses cannot be attributed to a different execution,
    even when their JSON shape and identifiers are individually valid. *)
let test_response_execution_correlation () =
  let response =
    {|{"execution":{"namespace":"default","workflow_id":"other-workflow","run_id":"run-1"}}|}
  in
  require_error (Protocol.decode_start_response ~request:start_request response);
  let wait_response =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"},"outcome":{"kind":"cancelled","details":[]}}|}
  in
  require_error
    (Protocol.decode_wait_response ~request:execution wait_response)

(** Decodes structured native errors and rejects categories or codes outside
    the bilateral closed vocabulary. *)
let test_client_errors () =
  (match
     unwrap
       (Protocol.decode_client_error
          {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}|})
   with
  | Protocol.Already_started { existing_run_id = None; _ } -> ()
  | _ -> failwith "already-started body changed shape");
  (match
     unwrap
       (Protocol.decode_client_error
          {|{"kind":"rpc","code":"deadline_exceeded"}|})
   with
  | Protocol.Rpc { code = "deadline_exceeded" } -> ()
  | _ -> failwith "rpc error code was not retained");
  (match
     unwrap
       (Protocol.decode_client_error
          {|{"kind":"rpc","code":"termination_outcome_uncertain"}|})
   with
  | Protocol.Rpc { code = "termination_outcome_uncertain" } -> ()
  | _ -> failwith "termination uncertainty code was not retained");
  (match
     unwrap
       (Protocol.decode_client_error
          {|{"kind":"protocol","code":"core_invalid"}|})
   with
  | Protocol.Protocol { code = "core_invalid" } -> ()
  | _ -> failwith "protocol error code was not retained");
  (* The schema and the Rust encoder both allow the rpc "ok" code (tonic's
     Code::Ok, mapped with no filter in map_rpc_status), so the OCaml decoder
     must accept it too even though it is near-unreachable on an error path. *)
  (match
     unwrap (Protocol.decode_client_error {|{"kind":"rpc","code":"ok"}|})
   with
  | Protocol.Rpc { code = "ok" } -> ()
  | _ -> failwith "rpc ok error code was not retained");
  List.iter
    (fun document -> require_error (Protocol.decode_client_error document))
    [
      {|{"kind":"rpc","code":"not-a-real-code"}|};
      {|{"kind":"protocol","code":"unsupported-future-code"}|};
      {|{"kind":"unknown","code":"internal"}|};
      {|{"kind":"rpc","code":"internal","extra":true}|};
    ]

(** Operation-specific error decoders retain the closed error body while
    correlating identities and rejecting impossible categories. *)
let test_operation_error_correlation () =
  let already_started =
    {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}|}
  in
  ignore (unwrap (Protocol.decode_start_error ~request:start_request already_started));
  require_error
    (Protocol.decode_start_error ~request:start_request
       {|{"kind":"already_started","workflow_id":"other-workflow","existing_run_id":null}|});
  require_error
    (Protocol.decode_wait_error ~request:execution already_started)

(** Runs one protocol test with a stable CI-visible name. *)
let run name test =
  try
    test ();
    Printf.printf "PASS %s\n%!" name
  with exn ->
    Printf.eprintf "FAIL %s: %s\n%!" name (Printexc.to_string exn);
    exit 1

(** Runs every client-protocol encoder/decoder assertion as one bridge test. *)
let () =
  run "client terminal outcomes" test_terminal_outcomes;
  run "client successor identity" test_successor_identity_validation;
  run "client request validation" test_request_validation;
  run "client cancellation protocol" test_cancellation_protocol;
  run "client reset protocol" test_reset_protocol;
  run "client termination protocol" test_termination_protocol;
  run "client signal protocol" test_signal_protocol;
  run "client query protocol" test_query_protocol;
  run "client visibility protocol" test_visibility_protocol;
  run "client asynchronous starts" test_async_start_protocol;
  run "client closed response shape" test_closed_response_shape;
  run "client response correlation" test_response_execution_correlation;
  run "client structured errors" test_client_errors;
  run "client operation error correlation" test_operation_error_correlation

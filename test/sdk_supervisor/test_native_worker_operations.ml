module Supervisor = Sdk_supervisor.Native
module Bridge = Temporal_core_bridge.Native_bridge
module Client = Temporal_protocol.Client_protocol
module Workflow = Temporal_protocol.Workflow_protocol
module Activity = Temporal_protocol.Activity_protocol

(** Requires two structural values to be equal and identifies the violated
    native-worker contract if they differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Reports whether [needle] occurs in [source] without requiring a
    standard-library substring helper newer than the oldest supported OCaml. *)
let contains_substring source needle =
  let source_length = String.length source in
  let needle_length = String.length needle in
  let rec loop offset =
    offset + needle_length <= source_length
    &&
    (String.equal (String.sub source offset needle_length) needle
    || loop (offset + 1))
  in
  needle_length = 0 || loop 0

(** Extracts a successful private adapter result for positive serialization
    cases while retaining a useful bridge diagnostic on failure. *)
let require_bridge = function
  | Ok value -> value
  | Error { Bridge.status; message } ->
      let status =
        match status with
        | Invalid_argument -> "invalid_argument"
        | Abi_mismatch -> "abi_mismatch"
        | Panic -> "panic"
        | Internal -> "internal"
        | Invalid_state -> "invalid_state"
        | Configuration -> "configuration"
        | Connection -> "connection"
        | Worker -> "worker"
        | Outstanding_tasks -> "outstanding_tasks"
        | Not_ready -> "not_ready"
        | Protocol -> "protocol"
        | Already_started -> "already_started"
        | Retryable -> "retryable"
        | Unknown code -> Printf.sprintf "unknown(%d)" code
      in
      failwith (Printf.sprintf "%s: %s" status message)

(** A minimal ordinary activation proves that polling bytes are validated and
    converted to the private typed workflow model before leaving the owner
    subsystem. *)
let workflow_activation_json =
  {|{"run_id":"run-1","timestamp":{"seconds":1,"nanoseconds":0},"is_replaying":false,"history_length":1,"jobs":[]}|}

(** A cancellation task keeps the activity fixture compact while still
    proving that opaque task-token bytes cross the adapter losslessly. *)
let activity_task_json =
  {|{"task_token":"AAEC","variant":{"kind":"cancel","reason":"worker_shutdown","details":null}}|}

(** Empty native lanes are an ordinary nonblocking readiness result, while a
    real bridge failure must remain distinguishable from an idle poll. *)
let test_nonblocking_readiness_results () =
  let reject _ = failwith "empty and failed polls must not reject a lease" in
  expect "empty workflow lane" (Ok None)
    (Supervisor.Protocol_adapter.workflow_poll_result ~reject
       (Error { Bridge.status = Not_ready; message = "lane empty" }));
  expect "empty activity lane" (Ok None)
    (Supervisor.Protocol_adapter.activity_poll_result ~reject
       (Error { Bridge.status = Not_ready; message = "lane empty" }));
  let failure = { Bridge.status = Worker; message = "poll lane stopped" } in
  let sanitized_failure =
    { Bridge.status = Worker; message = "native worker operation failed" }
  in
  expect "workflow poll failure" (Error sanitized_failure)
    (Supervisor.Protocol_adapter.workflow_poll_result ~reject (Error failure));
  let hostile_failure =
    {
      Bridge.status = Worker;
      message = "tonic::Status { message: secret-core-diagnostic }";
    }
  in
  (match
     Supervisor.Protocol_adapter.activity_poll_result ~reject
       (Error hostile_failure)
   with
  | Error { Bridge.status = Worker; message } ->
      if contains_substring message "secret-core-diagnostic" then
        failwith "activity worker error exposed Core diagnostic";
      expect "sanitized activity worker error" "native worker operation failed"
        message
  | _ -> failwith "hostile activity worker error changed status")

(** If OCaml rejects bytes that Rust already leased, the adapter returns those
    exact bytes to the native rejection path before exposing the protocol
    error. A rejection failure is appended without losing the original
    [Protocol] classification or copying source JSON into the diagnostic. *)
let test_decode_failure_retires_native_lease () =
  let malformed = Bytes.of_string {|{"run_id":"private-run"}|} in
  let rejected = ref None in
  let reject input =
    rejected := Some input;
    Ok ()
  in
  (match
     Supervisor.Protocol_adapter.workflow_poll_result ~reject (Ok malformed)
   with
  | Error { Bridge.status = Protocol; message } ->
      if contains_substring message "private-run"
      then failwith "workflow rejection error exposed source JSON"
  | _ -> failwith "workflow decode failure did not remain Protocol");
  expect "workflow rejection input" (Some malformed) !rejected;
  let rejection_failure =
    { Bridge.status = Worker; message = "native rejection failed safely" }
  in
  (match
     Supervisor.Protocol_adapter.activity_poll_result
       ~reject:(fun _ -> Error rejection_failure)
       (Ok (Bytes.of_string {|{"task_token":"c2VjcmV0"}|}))
   with
  | Error { Bridge.status = Protocol; message } ->
      if contains_substring message "native rejection failed safely" then
        failwith "activity rejection failure exposed native prose";
      if not (contains_substring message "native worker operation failed") then
        failwith "activity rejection failure was not categorized";
      if contains_substring message "c2VjcmV0"
      then failwith "activity rejection error exposed task bytes"
  | _ -> failwith "activity decode/rejection failure lost Protocol status")

(** Valid poll documents become typed values, and typed completions become the
    exact canonical JSON documents accepted by the Rust bridge. *)
let test_protocol_serialization () =
  let activation =
    require_bridge
      (Supervisor.Protocol_adapter.decode_workflow_activation
         (Bytes.of_string workflow_activation_json))
  in
  expect "workflow run id" "run-1" activation.run_id;
  let task =
    require_bridge
      (Supervisor.Protocol_adapter.decode_activity_task
         (Bytes.of_string activity_task_json))
  in
  if not (Bytes.equal task.task_token (Bytes.of_string "\000\001\002")) then
    failwith "activity token changed while decoding";
  let workflow_completion : Workflow.completion =
    { run_id = "run-1"; task_failure = None; commands = [] }
  in
  expect "workflow completion JSON"
    (Bytes.of_string {|{"commands":[],"run_id":"run-1"}|})
    (require_bridge
       (Supervisor.Protocol_adapter.encode_workflow_completion
          workflow_completion));
  let activity_completion : Activity.completion =
    { task_token = Bytes.of_string "\000\001\002"; result = Will_complete_async }
  in
  expect "activity completion JSON"
    (Bytes.of_string
       {|{"result":{"kind":"will_complete_async"},"task_token":"AAEC"}|})
    (require_bridge
       (Supervisor.Protocol_adapter.encode_activity_completion
          activity_completion))

(** Invalid incoming and outgoing semantic documents are converted to the
    stable bridge [Protocol] status without including source payload bytes in
    the diagnostic. *)
let test_protocol_failures_are_typed () =
  (match
     Supervisor.Protocol_adapter.decode_workflow_activation
       (Bytes.of_string {|{"run_id":"secret-payload"}|})
   with
  | Error { Bridge.status = Protocol; message } ->
      if String.length message = 0 then failwith "empty workflow protocol error";
      if contains_substring message "secret-payload"
      then failwith "workflow protocol error exposed source JSON"
  | _ -> failwith "invalid workflow activation was not a protocol error");
  let invalid_completion : Workflow.completion =
    { run_id = ""; task_failure = None; commands = [] }
  in
  match
    Supervisor.Protocol_adapter.encode_workflow_completion invalid_completion
  with
  | Error { Bridge.status = Protocol; message } ->
      if String.length message = 0 then failwith "empty completion protocol error"
  | _ -> failwith "invalid workflow completion was not a protocol error"

(** Exercises the typed client adapter without a Temporal server. The native
    result shapes below model the already-owned bytes returned by the private
    bridge, allowing this test to cover OCaml response/error validation and
    status correlation independently from network availability. *)
let test_client_protocol_adapter () =
  let start_request : Client.start_request =
    {
      request_id = "request-1";
      namespace = "default";
      workflow_id = "workflow-1";
      workflow_type = "Smoke";
      task_queue = "queue";
      input = [];
      memo = [];
      search_attributes = [];
    }
  in
  let wait_request : Client.wait_request =
    { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }
  in
  let cancel_request : Client.cancel_request =
    {
      execution = wait_request;
      request_id = "cancel-request-1";
      reason = "operator requested shutdown";
    }
  in
  let signal_request : Client.signal_request =
    {
      execution = wait_request;
      signal_name = "add_document";
      request_id = "signal-request-1";
      input = [];
    }
  in
  let query_request : Client.query_request =
    {
      execution = wait_request;
      query_type = "current_state";
      input = [];
    }
  in
  let start_json =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}}|}
  in
  let wait_json =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"outcome":{"kind":"cancelled","details":[]}}|}
  in
  let start_bytes =
    require_bridge
      (Supervisor.Protocol_adapter.encode_client_start_request start_request)
  in
  if not (contains_substring (Bytes.to_string start_bytes) "workflow-1") then
    failwith "typed start request was not encoded";
  let wait_bytes =
    require_bridge
      (Supervisor.Protocol_adapter.encode_client_wait_request wait_request)
  in
  if not (contains_substring (Bytes.to_string wait_bytes) "run-1") then
    failwith "typed wait request was not encoded";
  let cancel_bytes =
    require_bridge
      (Supervisor.Protocol_adapter.encode_client_cancel_request cancel_request)
  in
  if not (contains_substring (Bytes.to_string cancel_bytes) "cancel-request-1")
  then failwith "typed cancellation request was not encoded";
  let signal_bytes =
    require_bridge
      (Supervisor.Protocol_adapter.encode_client_signal_request signal_request)
  in
  if not (contains_substring (Bytes.to_string signal_bytes) "signal-request-1")
  then failwith "typed signal request was not encoded";
  let query_bytes =
    require_bridge
      (Supervisor.Protocol_adapter.encode_client_query_request query_request)
  in
  if not (contains_substring (Bytes.to_string query_bytes) "current_state") then
    failwith "typed query request was not encoded";
  (match
     Supervisor.Protocol_adapter.decode_client_cancel_result
       (Ok (Bytes.of_string {|{"acknowledged":true}|}))
   with
  | Ok (Ok ()) -> ()
  | _ -> failwith "positive cancellation acknowledgement was not typed");
  (match
     Supervisor.Protocol_adapter.decode_client_signal_result
       (Ok (Bytes.of_string {|{"acknowledged":true}|}))
   with
  | Ok (Ok ()) -> ()
  | _ -> failwith "positive signal acknowledgement was not typed");
  (match
     Supervisor.Protocol_adapter.decode_client_query_result
       (Ok (Bytes.of_string {|{"result":[]}|}))
   with
  | Ok (Ok []) -> ()
  | _ -> failwith "positive query response was not typed");
  (match
     Supervisor.Protocol_adapter.decode_client_start_result start_request
       (Ok (Bytes.of_string start_json))
   with
  | Ok (Ok { Client.execution = { run_id = "run-2"; _ } }) -> ()
  | _ -> failwith "valid start response was not typed");
  let start_ticket =
    match
      Supervisor.Protocol_adapter.decode_client_start_ticket start_request
        (Ok (Bytes.of_string {|{"ticket":"ticket-1"}|}))
    with
    | Ok (Ok ticket) -> ticket
    | _ -> failwith "valid start ticket was not typed"
  in
  if Client.start_ticket_request start_ticket <> start_request then
    failwith "typed start ticket lost its request correlation";
  let ticket_bytes =
    require_bridge
      (Supervisor.Protocol_adapter.encode_client_start_ticket start_ticket)
  in
  if not (contains_substring (Bytes.to_string ticket_bytes) "ticket-1") then
    failwith "typed start ticket was not encoded";
  let accepted_outcome_json =
    {|{"kind":"accepted","execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}}|}
  in
  (match
     Supervisor.Protocol_adapter.decode_client_start_outcome start_ticket
       (Ok (Bytes.of_string accepted_outcome_json))
   with
  | Ok (Some (Client.Accepted { execution = { run_id = "run-2"; _ } })) -> ()
  | _ -> failwith "valid asynchronous start outcome was not typed");
  (match
     Supervisor.Protocol_adapter.decode_client_start_outcome start_ticket
       (Error { Bridge.status = Not_ready; message = "retry" })
   with
  | Ok None -> ()
  | _ -> failwith "asynchronous start readiness was not mapped to None");
  (match
     Supervisor.Protocol_adapter.decode_client_wait_result wait_request
       (Ok (Bytes.of_string wait_json))
   with
  | Ok (Ok { Client.execution = { run_id = "run-1"; _ }; outcome = Cancelled _ }) ->
      ()
  | _ -> failwith "valid wait response was not typed");
  let already_started =
    Error
      {
        Bridge.status = Already_started;
        message =
          {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":"run-existing"}|};
      }
  in
  (match
     Supervisor.Protocol_adapter.decode_client_start_result start_request
       already_started
   with
  | Ok (Error (Client.Already_started { workflow_id = "workflow-1"; _ })) -> ()
  | _ -> failwith "structured already-started error was not typed");
  let rpc_failure =
    Error
      {
        Bridge.status = Connection;
        message = {|{"kind":"rpc","code":"unavailable"}|};
      }
  in
  (match
     Supervisor.Protocol_adapter.decode_client_query_result
       (Error
          {
            Bridge.status = Connection;
            message = {|{"kind":"rpc","code":"failed_precondition"}|};
          })
   with
  | Ok (Error (Client.Rpc { code = "failed_precondition" })) -> ()
  | _ -> failwith "structured query RPC error was not typed");
  (match
     Supervisor.Protocol_adapter.decode_client_wait_result wait_request
       rpc_failure
   with
  | Ok (Error (Client.Rpc { code = "unavailable" })) -> ()
  | _ -> failwith "structured RPC error was not typed");
  (match
     Supervisor.Protocol_adapter.decode_client_wait_result wait_request
       (Error { Bridge.status = Not_ready; message = "retry" })
   with
  | Error { Bridge.status = Not_ready; _ } -> ()
  | _ -> failwith "wait readiness status was converted into a terminal value");
  (match
     Supervisor.Protocol_adapter.decode_client_cancel_result
       (Error
          {
            Bridge.status = Connection;
            message = {|{"kind":"rpc","code":"deadline_exceeded"}|};
          })
   with
  | Ok (Error (Client.Rpc { code = "deadline_exceeded" })) -> ()
  | _ -> failwith "structured cancellation RPC error was not typed");
  (match
     Supervisor.Protocol_adapter.decode_client_signal_result
       (Error
          {
            Bridge.status = Connection;
            message = {|{"kind":"rpc","code":"deadline_exceeded"}|};
          })
   with
  | Ok (Error (Client.Rpc { code = "deadline_exceeded" })) -> ()
  | _ -> failwith "structured signal RPC error was not typed");
  (match
     Supervisor.Protocol_adapter.decode_client_cancel_result
       (Error
          {
            Bridge.status = Already_started;
            message =
              {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}|};
          })
   with
  | Error { Bridge.status = Protocol; _ } -> ()
  | _ -> failwith "impossible cancellation error status was accepted");
  (match
     Supervisor.Protocol_adapter.decode_client_signal_result
       (Error
          {
            Bridge.status = Already_started;
            message =
              {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}|};
          })
   with
  | Error { Bridge.status = Protocol; _ } -> ()
  | _ -> failwith "impossible signal error status was accepted");
  (match
     Supervisor.Protocol_adapter.decode_client_start_result start_request
       (Ok
          (Bytes.of_string
             {|{"execution":{"namespace":"other","workflow_id":"workflow-1","run_id":"run-2"}}|}))
   with
  | Error { Bridge.status = Protocol; _ } -> ()
  | _ -> failwith "mismatched start response was accepted");
  (match
     Supervisor.Protocol_adapter.decode_client_wait_result wait_request
       (Error
          {
            Bridge.status = Already_started;
            message = {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}|};
          })
   with
  | Error { Bridge.status = Protocol; _ } -> ()
  | _ -> failwith "impossible wait error status was accepted");
  (match
     Supervisor.Protocol_adapter.decode_client_start_result start_request
       (Error { Bridge.status = Connection; message = "secret native text" })
   with
  | Error { Bridge.status = Protocol; message } ->
      if contains_substring message "secret native text" then
        failwith "malformed native error exposed raw text"
  | _ -> failwith "malformed native error was not rejected")

(** The production supervisor rejects polling before worker construction,
    validates completions before entering Rust, and closes every worker
    operation at the mailbox admission boundary after shutdown. *)
let test_native_lifecycle_guards () =
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 ()) in
  (match Supervisor.perform supervisor Supervisor.Try_poll_workflow with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "workflow poll without worker was accepted");
  (match Supervisor.perform supervisor Supervisor.Try_poll_activity with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "activity poll without worker was accepted");
  (match Supervisor.perform supervisor Supervisor.Wait_workflow with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "workflow readiness wait without worker was accepted");
  (match Supervisor.perform supervisor Supervisor.Wait_activity with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "activity readiness wait without worker was accepted");
  let invalid_completion : Workflow.completion =
    { run_id = ""; task_failure = None; commands = [] }
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Complete_workflow invalid_completion)
   with
  | Error (Supervisor.Backend { Bridge.status = Protocol; _ }) -> ()
  | _ -> failwith "invalid completion reached the native worker");
  let workflow_completion : Workflow.completion =
    { run_id = "run-1"; task_failure = None; commands = [] }
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Complete_workflow workflow_completion)
   with
  | Error (Supervisor.Backend { Bridge.status = Protocol; _ }) -> ()
  | _ -> failwith "unleased workflow completion was accepted");
  let invalid_activity_completion : Activity.completion =
    { task_token = Bytes.empty; result = Will_complete_async }
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Complete_activity invalid_activity_completion)
   with
  | Error (Supervisor.Backend { Bridge.status = Protocol; _ }) -> ()
  | _ -> failwith "invalid activity completion reached the native worker");
  let activity_completion : Activity.completion =
    { task_token = Bytes.of_string "token"; result = Will_complete_async }
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Complete_activity activity_completion)
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "activity completion without worker was accepted");
  expect "native shutdown" (Ok ()) (Supervisor.shutdown supervisor);
  expect "poll after shutdown" (Error Supervisor.Closed)
    (Supervisor.perform supervisor Supervisor.Try_poll_workflow)

(** Client start and exact-run wait share the same owner lifecycle guard as
    worker operations. Calling either operation before a client connection is
    established must return a typed native state error rather than touching an
    uninitialized Rust handle. *)
let test_native_client_lifecycle_guards () =
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 ()) in
  (* These are semantically complete documents so the native adapter reaches
     its connection-state guard instead of stopping at request validation. *)
  let start_request : Client.start_request =
    {
      request_id = "request-1";
      namespace = "default";
      workflow_id = "workflow-1";
      workflow_type = "Smoke";
      task_queue = "queue";
      input = [];
      memo = [];
      search_attributes = [];
    }
  in
  let wait_request : Client.wait_request =
    { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Client_start_workflow start_request)
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "client start without connection was accepted");
  (match
     Supervisor.perform supervisor
       (Supervisor.Client_begin_start_workflow start_request)
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "client asynchronous start without connection was accepted");
  let ticket =
    match
      Client.decode_start_ticket ~request:start_request
        {|{"ticket":"ticket-before-connect"}|}
    with
    | Ok ticket -> ticket
    | Error _ -> failwith "test asynchronous ticket was invalid"
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Client_poll_start_workflow ticket)
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "client asynchronous poll without connection was accepted");
  (match
     Supervisor.perform supervisor
       (Supervisor.Client_wait_start_workflow ticket)
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "client asynchronous wait without connection was accepted");
  (match
     Supervisor.perform supervisor
       (Supervisor.Client_wait_workflow wait_request)
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "client wait without connection was accepted");
  (match
     Supervisor.perform supervisor
       (Supervisor.Client_cancel_workflow
          {
            execution = wait_request;
            request_id = "cancel-before-connect";
            reason = "test";
          })
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "client cancellation without connection was accepted");
  (match
     Supervisor.perform supervisor
       (Supervisor.Client_signal_workflow
          {
            execution = wait_request;
            signal_name = "add_document";
            request_id = "signal-before-connect";
            input = [];
          })
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "client signal without connection was accepted");
  expect "client lifecycle shutdown" (Ok ()) (Supervisor.shutdown supervisor)

let () =
  test_nonblocking_readiness_results ();
  test_decode_failure_retires_native_lease ();
  test_protocol_serialization ();
  test_protocol_failures_are_typed ();
  test_client_protocol_adapter ();
  test_native_lifecycle_guards ();
  test_native_client_lifecycle_guards ()

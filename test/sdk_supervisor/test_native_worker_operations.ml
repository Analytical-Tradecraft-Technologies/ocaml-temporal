module Supervisor = Sdk_supervisor.Native
module Bridge = Temporal_core_bridge.Native_bridge
module Client = Temporal_protocol.Client_protocol
module Workflow = Temporal_protocol.Workflow_protocol
module Activity = Temporal_protocol.Activity_protocol
module Encoded_completion = Temporal_protocol.Encoded_workflow_completion

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
        | Async_heartbeat_rejected -> "async_heartbeat_rejected"
        | Resource_exhausted -> "resource_exhausted"
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
    exact bytes to the native rejection path. On a live worker a successful
    rejection is local task progress and reads as an empty poll, so one
    undecodable task cannot end [Worker.run] (issue #801). A replay keeps the
    [Protocol] error instead of silently skipping history. A rejection failure
    is appended without losing the original [Protocol] classification or
    copying source JSON into the diagnostic. *)
let test_decode_failure_retires_native_lease () =
  let malformed = Bytes.of_string {|{"run_id":"private-run"}|} in
  let rejected = ref None in
  let reject input =
    rejected := Some input;
    Ok ()
  in
  expect "live workflow rejection keeps polling" (Ok None)
    (Supervisor.Protocol_adapter.workflow_poll_result ~reject (Ok malformed));
  expect "workflow rejection input" (Some malformed) !rejected;
  rejected := None;
  (match
     Supervisor.Protocol_adapter.replay_workflow_poll_result ~reject
       (Ok malformed)
   with
  | Error { Bridge.status = Protocol; message } ->
      if contains_substring message "private-run"
      then failwith "replay rejection error exposed source JSON"
  | _ -> failwith "replay decode failure did not remain Protocol");
  expect "replay rejection input" (Some malformed) !rejected;
  let malformed_task = Bytes.of_string {|{"task_token":"c2VjcmV0"}|} in
  rejected := None;
  expect "live activity rejection keeps polling" (Ok None)
    (Supervisor.Protocol_adapter.activity_poll_result ~reject
       (Ok malformed_task));
  expect "activity rejection input" (Some malformed_task) !rejected;
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
  (* Workflow completions are encoded once, before they enter the supervisor
     mailbox; the supervisor copies exactly these bytes into the C call. *)
  expect "workflow completion JSON"
    (Bytes.of_string {|{"commands":[],"run_id":"run-1"}|})
    (match Encoded_completion.encode workflow_completion with
    | Ok encoded -> Encoded_completion.to_bytes encoded
    | Error _ -> failwith "valid workflow completion was not encoded");
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
  (* An invalid outgoing completion is rejected by the single encoder pass, so
     no value exists that could be handed to [Complete_workflow]. *)
  match Encoded_completion.encode invalid_completion with
  | Error error ->
      if String.length (Workflow.error_view error).message = 0 then
        failwith "empty completion protocol error"
  | Ok _ -> failwith "invalid workflow completion was encoded"

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
      id_conflict_policy = Client.Fail;
      id_reuse_policy = Client.Allow_duplicate;
      execution_timeout_ms = None;
      run_timeout_ms = None;
      task_timeout_ms = None;
      retry_policy = None;
      rpc_deadline = None;
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
      rpc_deadline = None;
    }
  in
  let signal_request : Client.signal_request =
    {
      execution = wait_request;
      signal_name = "add_document";
      request_id = "signal-request-1";
      input = [];
      rpc_deadline = None;
    }
  in
  let query_request : Client.query_request =
    {
      execution = wait_request;
      query_type = "current_state";
      input = [];
      rpc_deadline = None;
    }
  in
  let start_json =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"},"started":true}|}
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
  | Ok (Ok { Client.execution = { run_id = "run-2"; _ }; started = true }) -> ()
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
    {|{"kind":"accepted","execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"},"started":true}|}
  in
  (match
     Supervisor.Protocol_adapter.decode_client_start_outcome start_ticket
       (Ok (Bytes.of_string accepted_outcome_json))
   with
  | Ok (Some (Client.Accepted { execution = { run_id = "run-2"; _ }; started = true })) -> ()
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
  (* Issue #823: a failed query handler shares the RPC status but keeps its
     own JSON kind and the handler's message. *)
  (match
     Supervisor.Protocol_adapter.decode_client_query_result
       (Error
          {
            Bridge.status = Connection;
            message = {|{"kind":"query_failed","message":"no such query"}|};
          })
   with
  | Ok (Error (Client.Query_failed { message = "no such query" })) -> ()
  | _ -> failwith "structured query handler failure was not typed");
  (match
     Supervisor.Protocol_adapter.decode_client_query_result
       (Error
          {
            Bridge.status = Protocol;
            message = {|{"kind":"query_failed","message":"no such query"}|};
          })
   with
  | Error { Bridge.status = Protocol; _ } -> ()
  | _ -> failwith "query failure with a mismatched status was accepted");
  (match
     Supervisor.Protocol_adapter.decode_client_signal_result
       (Error
          {
            Bridge.status = Connection;
            message = {|{"kind":"query_failed","message":"no such query"}|};
          })
   with
  | Error { Bridge.status = Protocol; _ } -> ()
  | _ -> failwith "query-only failure was accepted for a signal");
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
             {|{"execution":{"namespace":"other","workflow_id":"workflow-1","run_id":"run-2"},"started":true}|}))
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
  (match Supervisor.perform supervisor Supervisor.Wait_any with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "combined readiness wait without worker was accepted");
  let invalid_completion : Workflow.completion =
    { run_id = ""; task_failure = None; commands = [] }
  in
  (* The encoder is the only constructor of a submittable completion, so an
     invalid one cannot reach the native worker at all. *)
  (match Encoded_completion.encode invalid_completion with
  | Error _ -> ()
  | Ok _ -> failwith "invalid completion was encoded for the native worker");
  let workflow_completion =
    match
      Encoded_completion.encode
        ({ run_id = "run-1"; task_failure = None; commands = [] }
          : Workflow.completion)
    with
    | Ok encoded -> encoded
    | Error _ -> failwith "valid workflow completion was not encoded"
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
      id_conflict_policy = Client.Fail;
      id_reuse_policy = Client.Allow_duplicate;
      execution_timeout_ms = None;
      run_timeout_ms = None;
      task_timeout_ms = None;
      retry_policy = None;
      rpc_deadline = None;
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
            rpc_deadline = None;
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
            rpc_deadline = None;
          })
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "client signal without connection was accepted");
  expect "client lifecycle shutdown" (Ok ()) (Supervisor.shutdown supervisor)

(** A deadline that expired one second ago on the monotonic clock, with a
    one-millisecond budget, so every dispatch finds it expired. *)
let expired_deadline () =
  Some
    (Client.rpc_deadline
       ~now_ns:(Int64.sub (Bridge.monotonic_now_ns ()) 1_000_000_000L)
       ~timeout_ms:1L)

(** A deadline with a full minute remaining. *)
let live_deadline () =
  Some
    (Client.rpc_deadline ~now_ns:(Bridge.monotonic_now_ns ()) ~timeout_ms:60_000L)

(** A caller deadline that has already expired when the owner Domain
    dispatches a request (#499) fails with the typed [deadline_exceeded] RPC
    error, and the request never reaches the bridge: on this unconnected
    supervisor a sent request would fail with [Invalid_state] instead, as the
    same requests with a live deadline still do. An expired start is a
    definite rejection, never an uncertain outcome. *)
let test_expired_rpc_deadline_is_not_sent () =
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 ()) in
  let execution : Client.execution =
    { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }
  in
  let start_request rpc_deadline : Client.start_request =
    {
      request_id = "request-1";
      namespace = "default";
      workflow_id = "workflow-1";
      workflow_type = "Smoke";
      task_queue = "queue";
      input = [];
      memo = [];
      search_attributes = [];
      id_conflict_policy = Client.Fail;
      id_reuse_policy = Client.Allow_duplicate;
      execution_timeout_ms = None;
      run_timeout_ms = None;
      task_timeout_ms = None;
      retry_policy = None;
      rpc_deadline;
    }
  in
  let deadline_exceeded = Client.Rpc { code = "deadline_exceeded" } in
  (* Each check returns [`Expired] for the typed deadline error, [`Sent] for
     the unconnected bridge's [Invalid_state], and fails otherwise. *)
  let classify label = function
    | Ok (Error error) when error = deadline_exceeded -> `Expired
    | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> `Sent
    | _ -> failwith (label ^ " returned an unexpected result")
  in
  let operations =
    [
      ( "start",
        fun rpc_deadline ->
          classify "start"
            (Result.map
               (Result.map ignore)
               (Supervisor.perform supervisor
                  (Supervisor.Client_start_workflow (start_request rpc_deadline)))) );
      ( "begin start",
        fun rpc_deadline ->
          classify "begin start"
            (Result.map
               (Result.map ignore)
               (Supervisor.perform supervisor
                  (Supervisor.Client_begin_start_workflow
                     (start_request rpc_deadline)))) );
      ( "cancel",
        fun rpc_deadline ->
          classify "cancel"
            (Supervisor.perform supervisor
               (Supervisor.Client_cancel_workflow
                  { execution; request_id = "c"; reason = ""; rpc_deadline })) );
      ( "terminate",
        fun rpc_deadline ->
          classify "terminate"
            (Supervisor.perform supervisor
               (Supervisor.Client_terminate_workflow
                  { execution; reason = ""; rpc_deadline })) );
      ( "reset",
        fun rpc_deadline ->
          classify "reset"
            (Result.map
               (Result.map ignore)
               (Supervisor.perform supervisor
                  (Supervisor.Client_reset_workflow
                     {
                       execution;
                       request_id = "r";
                       reason = "";
                       workflow_task_finish_event_id = 3L;
                       rpc_deadline;
                     }))) );
      ( "signal",
        fun rpc_deadline ->
          classify "signal"
            (Supervisor.perform supervisor
               (Supervisor.Client_signal_workflow
                  {
                    execution;
                    signal_name = "s";
                    request_id = "s";
                    input = [];
                    rpc_deadline;
                  })) );
      ( "query",
        fun rpc_deadline ->
          classify "query"
            (Result.map
               (Result.map ignore)
               (Supervisor.perform supervisor
                  (Supervisor.Client_query_workflow
                     { execution; query_type = "q"; input = []; rpc_deadline }))) );
      ( "update",
        fun rpc_deadline ->
          classify "update"
            (Result.map
               (Result.map ignore)
               (Supervisor.perform supervisor
                  (Supervisor.Client_update_workflow
                     {
                       execution;
                       update_id = "u";
                       update_name = "n";
                       input = [];
                       rpc_deadline;
                     }))) );
      ( "visibility",
        fun rpc_deadline ->
          (* Visibility has no structured error channel, so the expired
             deadline arrives as the same native failure Rust reports. *)
          match
            Supervisor.perform supervisor
              (Supervisor.Client_list_visibility_workflows
                 {
                   namespace = "default";
                   query = "";
                   page_size = 10;
                   next_page_token = None;
                   rpc_deadline;
                 })
          with
          | Error (Supervisor.Backend error)
            when error = Supervisor.Protocol_adapter.expired_rpc_deadline_error ->
              `Expired
          | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) ->
              `Sent
          | _ -> failwith "visibility returned an unexpected result" );
    ]
  in
  List.iter
    (fun (label, perform) ->
      if perform (expired_deadline ()) <> `Expired then
        failwith (label ^ " sent a request whose deadline had expired");
      if perform (live_deadline ()) <> `Sent then
        failwith (label ^ " did not send a request with a live deadline");
      if perform None <> `Sent then
        failwith (label ^ " did not send a request without a deadline"))
    operations;
  expect "expired deadline shutdown" (Ok ()) (Supervisor.shutdown supervisor)

(** Submitted client calls (#807) through the real native supervisor. An
    expired deadline completes the call on the owner without reaching Rust
    (an unconnected runtime would otherwise refuse it with [Invalid_state]),
    and awaiting it yields the typed [deadline_exceeded] result; an expired
    start is a definite rejection. A live or absent deadline reaches Rust,
    which refuses the unconnected submission synchronously, and a wait (no
    deadline) does too. After shutdown, submission reports [Closed]. *)
let test_submitted_client_calls () =
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 ()) in
  let execution : Client.execution =
    { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }
  in
  let deadline_exceeded = Client.Rpc { code = "deadline_exceeded" } in
  let signal rpc_deadline =
    Supervisor.Rpc_signal
      { execution; signal_name = "s"; request_id = "s"; input = []; rpc_deadline }
  in
  (match Supervisor.call supervisor (signal (expired_deadline ())) with
  | Ok (Error error) when error = deadline_exceeded -> ()
  | _ -> failwith "an expired submitted signal was not completed with its deadline");
  (match Supervisor.perform supervisor (Supervisor.Client_submit (signal (expired_deadline ()))) with
  | Ok (Sdk_supervisor.Client_call.Completed _) -> ()
  | _ -> failwith "an expired submitted signal reached the bridge");
  List.iter
    (fun rpc_deadline ->
      match Supervisor.call supervisor (signal rpc_deadline) with
      | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
      | _ -> failwith "an unconnected submitted signal was accepted")
    [ live_deadline (); None ];
  let start rpc_deadline : Client.start_request =
    {
      request_id = "request-1";
      namespace = "default";
      workflow_id = "workflow-1";
      workflow_type = "Smoke";
      task_queue = "queue";
      input = [];
      memo = [];
      search_attributes = [];
      id_conflict_policy = Client.Fail;
      id_reuse_policy = Client.Allow_duplicate;
      execution_timeout_ms = None;
      run_timeout_ms = None;
      task_timeout_ms = None;
      retry_policy = None;
      rpc_deadline;
    }
  in
  (match Supervisor.call supervisor (Supervisor.Rpc_start (start (expired_deadline ()))) with
  | Ok (Client.Rejected error) when error = deadline_exceeded -> ()
  | _ -> failwith "an expired submitted start was not a definite rejection");
  (match Supervisor.call supervisor (Supervisor.Rpc_start (start None)) with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "an unconnected submitted start was accepted");
  (match Supervisor.call supervisor (Supervisor.Rpc_wait execution) with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "an unconnected submitted wait was accepted");
  expect "submitted call shutdown" (Ok ()) (Supervisor.shutdown supervisor);
  match Supervisor.call supervisor (signal None) with
  | Error Supervisor.Closed -> ()
  | _ -> failwith "a submission after shutdown was not closed"

(** A fake supervisor backend whose client signal uses the production
    deadline resolution, so a test can hold the owner Domain busy with a
    long operation and observe what a queued request does at dispatch. *)
module Queued_backend = struct
  type config = unit

  (** Budgets of the signals this backend sent, most recent first; [None]
      marks a request sent without a deadline. Module-level so the test can
      read it after the owner Domain has finished. *)
  let sent : int64 option list Atomic.t = Atomic.make []

  (** The fake graph has no owner-confined state. *)
  type state = unit

  type error = Bridge.error

  type _ operation =
    | Hold : (unit -> unit) -> unit operation
        (** Occupies the owner Domain until the supplied wait returns. *)
    | Signal :
        Client.signal_request
        -> (unit, Client.client_error) result operation
        (** Resolves the deadline as the native supervisor does, then
            records the send instead of performing an RPC. *)

  let create () = Ok ()

  let perform : type value. state -> value operation -> (value, error) result =
   fun () -> function
    | Hold wait ->
        wait ();
        Ok ()
    | Signal request ->
        Supervisor.Protocol_adapter.with_rpc_deadline
          ~now_ns:(Bridge.monotonic_now_ns ())
          request.rpc_deadline
          ~expired:(fun () ->
            Supervisor.Protocol_adapter.decode_client_signal_result
              (Error Supervisor.Protocol_adapter.expired_rpc_deadline_error))
          ~live:(fun deadline ->
            let budget =
              Option.map (fun (value : Client.rpc_deadline) -> value.timeout_ms) deadline
            in
            let rec record () =
              let current = Atomic.get sent in
              if not (Atomic.compare_and_set sent current (budget :: current))
              then record ()
            in
            record ();
            Ok (Ok ()))

  let shutdown _ = Ok ()
end

module Queued = Sdk_supervisor.Make (Queued_backend)

(** The caller's RPC deadline covers the time a request waits in the
    supervisor mailbox (#499). While the owner Domain is held by a long
    operation, a signal with a 50 ms deadline is queued behind it for about
    300 ms: when finally dispatched it fails with the typed
    [deadline_exceeded] error and is never sent. A signal with a two-second
    deadline queued the same way is sent with only the budget that remains,
    never its full original budget. *)
let test_queued_rpc_deadline_counts_wait () =
  let queued = Result.get_ok (Queued.create ~capacity:4 ()) in
  let holding = Atomic.make false and release = Atomic.make false in
  let hold () =
    Atomic.set holding true;
    while not (Atomic.get release) do
      Thread.delay 0.001
    done
  in
  let signal timeout_ms : Client.signal_request =
    {
      execution = { namespace = "default"; workflow_id = "w"; run_id = "r" };
      signal_name = "s";
      request_id = "s";
      input = [];
      rpc_deadline =
        Some
          (Client.rpc_deadline ~now_ns:(Bridge.monotonic_now_ns ())
             ~timeout_ms);
    }
  in
  let holder = Domain.spawn (fun () -> Queued.perform queued (Queued_backend.Hold hold)) in
  while not (Atomic.get holding) do
    Thread.delay 0.001
  done;
  (* Both deadlines start now, while the owner is already busy. *)
  let short = signal 50L and long = signal 2_000L in
  let short_result =
    Domain.spawn (fun () -> Queued.perform queued (Queued_backend.Signal short))
  in
  let long_result =
    Domain.spawn (fun () -> Queued.perform queued (Queued_backend.Signal long))
  in
  Thread.delay 0.3;
  Atomic.set release true;
  expect "held operation" (Ok ()) (Domain.join holder);
  (match Domain.join short_result with
  | Ok (Error (Client.Rpc { code = "deadline_exceeded" })) -> ()
  | _ -> failwith "a queued expired deadline was not reported as deadline_exceeded");
  (match Domain.join long_result with
  | Ok (Ok ()) -> ()
  | _ -> failwith "a queued live deadline was not sent");
  (* Only the long signal was sent, with the budget left after its wait. *)
  (match Atomic.get Queued_backend.sent with
  | [ Some budget ] when budget >= 1L && budget <= 1_750L -> ()
  | [ Some _ ] -> failwith "a queued request was sent with its full budget"
  | _ -> failwith "an expired queued request was sent");
  expect "queued shutdown" (Ok ()) (Queued.shutdown queued)

let () =
  test_nonblocking_readiness_results ();
  test_decode_failure_retires_native_lease ();
  test_protocol_serialization ();
  test_protocol_failures_are_typed ();
  test_client_protocol_adapter ();
  test_native_lifecycle_guards ();
  test_native_client_lifecycle_guards ();
  test_expired_rpc_deadline_is_not_sent ();
  test_submitted_client_calls ();
  test_queued_rpc_deadline_counts_wait ()

(** Regression for successor identities crossing the validated native protocol
    into the private client backend. No live Temporal server is needed: the
    protocol decoder supplies the same typed response accepted by the native
    supervisor after an exact-run wait. *)

module Protocol = Temporal_protocol.Client_protocol
module Backend = Temporal__Backend

(** The exact run requested by the caller; successors use another run ID in
    this same namespace and workflow chain. *)
let requested : Protocol.execution =
  { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }

(** Decodes one synthetic close event through the strict production protocol. *)
let response kind extra successor =
  let json =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"outcome":{"kind":"|}
    ^ kind ^ {|",|} ^ extra ^ {|"successor":|} ^ successor ^ "}}"
  in
  match Protocol.decode_wait_response ~request:requested json with
  | Ok response -> response
  | Error error ->
      let view = Protocol.error_view error in
      failwith (Printf.sprintf "%s at %s" view.code view.path)

(** Checks the backend retains a distinct successor run only when the close
    event supplied one; both workflow and run IDs must survive unchanged. *)
let require_successor label expect = function
  | None when not expect -> ()
  | Some (value : Backend.successor)
    when expect
         && value.workflow_id = requested.workflow_id
         && value.run_id = "run-2" ->
      ()
  | _ -> failwith (label ^ " lost or changed the successor run")

(** Failed and timed-out terminal conversions both retain optional successors
    that the public client's [wait] can turn into typed executions. Before the
    fix, the backend discarded failed successors and buried timeout successors
    in an error message, so these assertions could not pass. *)
let test_failure_and_timeout_successors () =
  let successor =
    {|{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}|}
  in
  let failure =
    {|"failure":{"message":"failed","source":"worker","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"application","type":"Failure","non_retryable":true,"details":[]}},|}
  in
  let failed_without = response "failed" failure "null" in
  let failed_with = response "failed" failure successor in
  let timeout_without = response "timed_out" "" "null" in
  let timeout_with = response "timed_out" "" successor in
  (match Backend.native_terminal_result failed_without with
  | Ok (Backend.Failed { successor; _ }) ->
      require_successor "failed without" false successor
  | _ -> failwith "failed close event changed terminal kind");
  (match Backend.native_terminal_result failed_with with
  | Ok (Backend.Failed { successor; _ }) ->
      require_successor "failed with" true successor
  | _ -> failwith "failed close event changed terminal kind");
  (match Backend.native_terminal_result timeout_without with
  | Ok (Backend.Timed_out { successor; _ }) ->
      require_successor "timed out without" false successor
  | _ -> failwith "timed-out close event changed terminal kind");
  match Backend.native_terminal_result timeout_with with
  | Ok (Backend.Timed_out { successor; _ }) ->
      require_successor "timed out with" true successor
  | _ -> failwith "timed-out close event changed terminal kind"

(** Runs the backend regression as an independent Dune test executable. *)
let () = test_failure_and_timeout_successors ()

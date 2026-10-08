(** Regression for successor identities crossing the validated native protocol
    into the private client backend. No live Temporal server is needed: the
    protocol decoder supplies the same typed response accepted by the native
    supervisor after an exact-run wait. *)

module Protocol = Temporal_protocol.Client_protocol
module Backend = Temporal__Backend
module Client = Temporal.Client

let unwrap label = function
  | Ok value -> value
  | Error error ->
      failwith (label ^ ": " ^ Temporal.Error.message error)

(** The exact run requested by the caller; successors use another run ID in
    this same namespace and workflow chain. *)
let requested : Protocol.execution =
  { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }

(** Decodes one synthetic close event through the strict production protocol. *)
let response ?(requested = requested) kind extra successor =
  let json =
    Printf.sprintf
      {|{"execution":{"namespace":"%s","workflow_id":"%s","run_id":"%s"},"outcome":{"kind":"|}
      requested.namespace requested.workflow_id requested.run_id
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

(** A completed close event keeps the cron or retry successor Temporal links
    to it (#837); before the fix the backend discarded it, so a completed run
    in a chain could not be followed. *)
let test_completed_successor () =
  let successor =
    {|{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}|}
  in
  (match
     Backend.native_terminal_result (response "completed" {|"result":[],|} "null")
   with
  | Ok (Backend.Completed { successor; _ }) ->
      require_successor "completed without" false successor
  | _ -> failwith "completed close event changed terminal kind");
  match
    Backend.native_terminal_result (response "completed" {|"result":[],|} successor)
  with
  | Ok (Backend.Completed { successor; _ }) ->
      require_successor "completed with" true successor
  | _ -> failwith "completed close event changed terminal kind"

(** A client-observed workflow failure exposes the application failure type,
    including when the workflow's failure wraps an activity failure whose
    application cause carries the type; the public category stays [Workflow]. *)
let test_failed_result_error_type () =
  let error_of failure_json =
    match
      Backend.native_terminal_result
        (response "failed" ({|"failure":|} ^ failure_json ^ ",") "null")
    with
    | Ok (Backend.Failed { error; _ }) -> error
    | _ -> failwith "failed close event changed terminal kind"
  in
  let flat =
    error_of
      {|{"message":"failed","source":"worker","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"application","type":"Failure","non_retryable":true,"details":[]}}|}
  in
  assert (Temporal.Error.error_type flat = Some "Failure");
  assert ((Temporal.Error.view flat).category = `Workflow);
  let nested =
    error_of
      {|{"message":"activity failed","source":"core","stack_trace":"","encoded_attributes":null,"cause":{"message":"bad","source":"PythonSDK","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"application","type":"InvalidInput","non_retryable":false,"details":[]}},"info":{"kind":"activity","scheduled_event_id":5,"started_event_id":6,"identity":"worker","activity_type":"activity","activity_id":"activity-1","retry_state":"non_retryable_failure"}}|}
  in
  assert (Temporal.Error.error_type nested = Some "InvalidInput");
  assert ((Temporal.Error.view nested).category = `Workflow)

(** The mock supplies two real exact runs; a private hook substitutes the
    protocol-decoded close event for the first. The public successor returned by
    [wait], not the reset fixture, is passed directly to [follow]. *)
let test_public_wait_follow kind =
  let target_url = "mock://terminal-follow-" ^ kind in
  let namespace = "default" in
  let workflow_id = "workflow-1" in
  let workflow =
    Temporal.Workflow.define ~name:"test.successor"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
        Ok input)
  in
  let client =
    unwrap "create public client"
      (Client.create ~target_url ~namespace ())
  in
  let backend =
    let config : Backend.config =
      { target_url; namespace; identity = "test-fixture"; task_queue = None }
    in
    unwrap "create mock fixture" (Backend.client_create config)
  in
  let original =
    unwrap "start original run"
      (Client.start client ~workflow ~task_queue:"test" ~id:workflow_id
         ~input:"successor output" ())
  in
  let fixture_successor =
    unwrap "allocate successor run"
      (Client.reset ~workflow_task_finish_event_id:4L original)
  in
  let requested : Protocol.execution =
    { namespace; workflow_id; run_id = Option.get (Client.run_id original) }
  in
  let successor_json =
    Printf.sprintf
      {|{"namespace":"%s","workflow_id":"%s","run_id":"%s"}|}
      fixture_successor.namespace fixture_successor.workflow_id
      fixture_successor.run_id
  in
  let failure =
    {|"failure":{"message":"failed","source":"worker","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"application","type":"Failure","non_retryable":true,"details":[]}},|}
  in
  let extra =
    match kind with
    | "failed" -> failure
    | "completed" -> {|"result":[],|}
    | _ -> ""
  in
  let terminal =
    response ~requested kind extra successor_json
    |> Backend.native_terminal_result
    |> unwrap "decode terminal result"
  in
  let wait_request : Backend.wait_request =
    { workflow_id; run_id = Option.get (Client.run_id original) }
  in
  unwrap "script exact-run close event"
    (Backend.mock_set_wait_outcome_for_test backend wait_request terminal);
  let successor =
    match (kind, unwrap "wait on original run" (Client.wait original)) with
    | "failed", Client.Failed { successor = Some successor; _ }
    | "timed_out", Client.Timed_out { successor = Some successor; _ }
    | "completed", Client.Completed { successor = Some successor; _ }
    | "continued_as_new", Client.Continued_as_new successor ->
        successor
    | _ -> failwith (kind ^ " wait lost its typed successor")
  in
  assert (successor.namespace = namespace);
  assert (successor.workflow_id = workflow_id);
  assert (successor.run_id = fixture_successor.run_id);
  let followed =
    unwrap "follow wait successor" (Client.follow client ~workflow successor)
  in
  assert (Client.workflow_id followed = workflow_id);
  assert (Option.get (Client.run_id followed) = fixture_successor.run_id);
  (match Client.wait followed with
  | Ok (Client.Completed { output = "successor output"; _ }) -> ()
  | Ok _ -> failwith "follow did not wait on the successor run"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap "shutdown mock fixture" (Backend.client_shutdown backend);
  unwrap "shutdown public client" (Client.shutdown client)

(** A current-run handle's [wait] (#791) follows every kind of successor link
    to the end of the chain, as the official SDKs' result methods do, while an
    exact-run handle for the same run returns the link without following it.

    The mock has two real runs: the original, completed by an exact wait, and
    the reset successor, which becomes the workflow's current run. The current
    run is scripted to close with [kind] linking back to the completed
    original, so a following wait must end with the original's completion. *)
let test_current_run_wait_follows kind =
  let target_url = "mock://terminal-current-run-" ^ kind in
  let namespace = "default" in
  let workflow_id = "workflow-chain" in
  let workflow =
    Temporal.Workflow.define ~name:"test.chain" ~input:Temporal.Codec.string
      ~output:Temporal.Codec.string (fun input -> Ok input)
  in
  let client =
    unwrap "create public client" (Client.create ~target_url ~namespace ())
  in
  let backend =
    let config : Backend.config =
      { target_url; namespace; identity = "test-fixture"; task_queue = None }
    in
    unwrap "create mock fixture" (Backend.client_create config)
  in
  let original =
    unwrap "start original run"
      (Client.start client ~workflow ~task_queue:"test" ~id:workflow_id
         ~input:"chain end" ())
  in
  (match Client.wait original with
  | Ok (Client.Completed { output = "chain end"; successor = None }) -> ()
  | _ -> failwith "original run did not complete");
  let current_run =
    unwrap "reset to a current run"
      (Client.reset ~workflow_task_finish_event_id:4L original)
  in
  let requested : Protocol.execution =
    { namespace; workflow_id; run_id = current_run.run_id }
  in
  let link =
    Printf.sprintf {|{"namespace":"%s","workflow_id":"%s","run_id":"%s"}|}
      namespace workflow_id
      (Option.get (Client.run_id original))
  in
  let failure =
    {|"failure":{"message":"failed","source":"worker","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"application","type":"Failure","non_retryable":false,"details":[]}},|}
  in
  let extra =
    match kind with
    | "failed" -> failure
    | "completed" -> {|"result":[],|}
    | _ -> ""
  in
  let terminal =
    response ~requested kind extra link
    |> Backend.native_terminal_result
    |> unwrap "decode terminal result"
  in
  unwrap "script current-run close event"
    (Backend.mock_set_wait_outcome_for_test backend
       { workflow_id; run_id = current_run.run_id }
       terminal);
  let exact = unwrap "follow current run" (Client.follow client ~workflow current_run) in
  (match unwrap "exact wait" (Client.wait exact) with
  | Client.Completed { successor = Some _; _ }
  | Client.Failed { successor = Some _; _ }
  | Client.Timed_out { successor = Some _; _ }
  | Client.Continued_as_new _ ->
      ()
  | _ -> failwith (kind ^ " exact wait followed or lost its link"));
  let by_id =
    unwrap "get current-run handle"
      (Client.get_handle client ~workflow ~id:workflow_id ())
  in
  (match unwrap "current-run wait" (Client.wait by_id) with
  | Client.Completed { output = "chain end"; successor = None } -> ()
  | _ -> failwith (kind ^ " current-run wait did not follow the chain"));
  unwrap "shutdown mock fixture" (Backend.client_shutdown backend);
  unwrap "shutdown public client" (Client.shutdown client)

(** Runs both the private adapter and public wait/follow regressions without a
    live Temporal server. *)
let () =
  test_failure_and_timeout_successors ();
  test_completed_successor ();
  test_failed_result_error_type ();
  List.iter test_public_wait_follow
    [ "failed"; "timed_out"; "completed"; "continued_as_new" ];
  List.iter test_current_run_wait_follows
    [ "failed"; "timed_out"; "completed"; "continued_as_new" ]

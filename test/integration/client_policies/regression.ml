(** Live regression for client execution policies and RPC deadlines (#499).

    Against a disposable Temporal server it shows that each policy exposed by
    [Client.start] reaches the server and changes its behaviour: workflow
    execution and run timeouts end runs as [Timed_out] and are recorded with
    the task timeout in the execution config, the workflow ID reuse policies
    refuse or allow a closed ID, and a workflow retry policy starts a retry
    run. It also separates RPC deadlines from workflow timeouts: an expired
    query deadline is a typed [`Deadline_exceeded] error while the workflow
    keeps running, and a start whose deadline expired is reconciled with its
    request ID into exactly one run. The parent owns and reaps its worker,
    uses a unique task queue, and terminates every execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("client policies: " ^ label)

(** Milliseconds as a public duration. *)
let ms = Duration.of_ms

(** Waits forever, so only a workflow timeout or termination can end it. *)
let blocked =
  Workflow.define ~name:"client-policies-blocked" ~input:Codec.string
    ~output:Codec.string (fun _ ->
      Result.map (fun () -> "unreachable") (Condition.wait_until (fun () -> false)))

(** Completes at once, so its ID has a successfully completed closed run. *)
let quick =
  Workflow.define ~name:"client-policies-quick" ~input:Codec.string
    ~output:Codec.string (fun input -> Ok input)

(** Always fails with a retryable workflow error naming its retry attempt, so
    a retry policy's successor run is observable in the failure message. *)
let failing =
  Workflow.define ~name:"client-policies-failing" ~input:Codec.string
    ~output:Codec.string (fun _ ->
      Result.bind (Workflow.info ()) (fun info ->
          Error
            (Error.make ~category:`Workflow ~error_type:"PolicyFailure"
               ~message:(Printf.sprintf "attempt %d" (Workflow.Info.attempt info))
               ())))

(** Answers whether the blocked workflow is still running. *)
let alive = Query.define ~name:"alive" ~output:Codec.string

(** Runs the fixture definitions on the test's unique queue. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[]
         ~workflows:
           [
             Worker.workflow blocked
               ~queries:[ Query.Handler.make alive (fun () -> Ok "alive") ];
             Worker.workflow quick;
             Worker.workflow failing;
           ]
         ())
  in
  get (Worker.run worker)

(** One check's client, unique task queue, and cleanup list. Cleanups are
    closures because the tracked handles have different workflow types. *)
type context = {
  client : Client.t;
  queue : string;
  cleanups : (unit -> unit) list ref;
}

(** Derives a workflow ID that is unique to this run of the regression. *)
let workflow_id context suffix = context.queue ^ "-" ^ suffix

(** Registers [handle] for termination when the regression ends, including
    after an assertion failure, and returns it. *)
let track context handle =
  context.cleanups := (fun () -> ignore (Client.terminate handle)) :: !(context.cleanups);
  handle

(** Runs the official CLI and returns its standard output. Only this
    fixture's own executions are described. *)
let cli_output cli address args =
  let address = String.sub address 7 (String.length address - 7) in
  let argv = Array.of_list (cli :: "--address" :: address :: args) in
  let channel = Unix.open_process_args_in cli argv in
  let output = In_channel.input_all channel in
  match Unix.close_process_in channel with
  | Unix.WEXITED 0 -> output
  | _ -> fail ("temporal CLI failed: " ^ String.concat " " args)

(** Reports whether [needle] occurs in [haystack]. *)
let contains haystack needle =
  let length = String.length needle in
  let rec search index =
    index + length <= String.length haystack
    && (String.sub haystack index length = needle || search (index + 1))
  in
  search 0

(** Requires the CLI's description of [handle]'s run to contain each
    fragment, proving a policy reached the server rather than only passing
    local validation. *)
let expect_recorded cli address handle fragments =
  let description =
    cli_output cli address
      [ "workflow"; "describe"; "--namespace"; "default"; "--output"; "json";
        "--workflow-id"; Client.workflow_id handle;
        "--run-id"; Option.get (Client.run_id handle) ]
  in
  List.iter
    (fun fragment ->
      if not (contains description fragment) then
        fail (Printf.sprintf "execution config lacks %S:\n%s" fragment description))
    fragments

(** Requires an already-started refusal naming [expected]'s run. *)
let expect_refused label expected = function
  | Ok _ -> fail (label ^ ": start was not refused")
  | Error error -> (
      if Error.error_type error <> Some "WorkflowExecutionAlreadyStarted" then
        fail (label ^ ": unexpected error " ^ Error.message error);
      match Client.already_started error with
      | Some { Client.run_id; _ } when Some run_id = Client.run_id expected -> ()
      | Some _ | None -> fail (label ^ ": refusal did not name the expected run"))

(** Execution and run timeouts end runs as [Timed_out] and, with the task
    timeout, are recorded in the server's execution config. While the first
    run is open, a 1 ms query deadline is a typed deadline error and the same
    query then succeeds: the RPC deadline bounded the call, not the workflow. *)
let check_timeouts context ~cli address =
  let execution =
    track context
      (get
         (Client.start context.client ~workflow:blocked
            ~execution_timeout:(ms 4_000L) ~task_timeout:(ms 3_000L)
            ~task_queue:context.queue
            ~id:(workflow_id context "execution-timeout") ~input:"" ()))
  in
  expect_recorded cli address execution
    [ {|"workflowExecutionTimeout": "4s"|}; {|"defaultWorkflowTaskTimeout": "3s"|} ];
  (match Client.query ~rpc_timeout:(ms 1L) execution ~query:alive with
  | Error error when Client.rpc_status error = Some `Deadline_exceeded -> ()
  | Error error -> fail ("query deadline was not typed: " ^ Error.message error)
  | Ok _ -> fail "a 1 ms query deadline did not expire");
  if get (Client.query execution ~query:alive) <> "alive" then
    fail "workflow did not survive an expired query deadline";
  let run =
    track context
      (get
         (Client.start context.client ~workflow:blocked
            ~run_timeout:(ms 1_500L) ~task_queue:context.queue
            ~id:(workflow_id context "run-timeout") ~input:"" ()))
  in
  expect_recorded cli address run [ {|"workflowRunTimeout": "1.500s"|} ];
  List.iter
    (fun (label, handle) ->
      match Client.wait handle with
      | Ok (Client.Timed_out { successor = None; _ }) -> ()
      | Ok _ -> fail (label ^ " did not end as Timed_out")
      | Error error -> fail (label ^ ": " ^ Error.message error))
    [ ("execution timeout", execution); ("run timeout", run) ]

(** The reuse policy governs only closed runs. After a successful completion,
    [`Reject_duplicate] and [`Allow_duplicate_failed_only] refuse the ID and
    the default reuses it; after a failure, [`Reject_duplicate] still refuses
    it and [`Allow_duplicate_failed_only] reuses it. *)
let check_reuse_policies context =
  let start ?id_reuse_policy ?id_conflict_policy workflow suffix =
    Client.start context.client ?id_reuse_policy ?id_conflict_policy ~workflow
      ~task_queue:context.queue ~id:(workflow_id context suffix) ~input:"" ()
  in
  let completed = track context (get (start quick "reuse-completed")) in
  (match Client.wait completed with
  | Ok (Client.Completed _) -> ()
  | _ -> fail "quick workflow did not complete");
  expect_refused "reject after completion" completed
    (start ~id_reuse_policy:`Reject_duplicate quick "reuse-completed");
  expect_refused "failed-only after completion" completed
    (start ~id_reuse_policy:`Allow_duplicate_failed_only quick "reuse-completed");
  let reused = track context (get (start quick "reuse-completed")) in
  if Client.run_id reused = Client.run_id completed then
    fail "default reuse policy did not start a new run";
  let failed = track context (get (start failing "reuse-failed")) in
  (match Client.wait failed with
  | Ok (Client.Failed { successor = None; _ }) -> ()
  | _ -> fail "failing workflow did not fail without a retry");
  expect_refused "reject after failure" failed
    (start ~id_reuse_policy:`Reject_duplicate quick "reuse-failed");
  let after_failure =
    track context
      (get (start ~id_reuse_policy:`Allow_duplicate_failed_only quick "reuse-failed"))
  in
  if not (Client.started after_failure) then
    fail "failed-only reuse did not start a new run";
  (* The reuse policy never affects an open run: with [`Use_existing] even
     [`Reject_duplicate] attaches to it. *)
  let open_run = track context (get (start blocked "reuse-open")) in
  let attached =
    get
      (start ~id_conflict_policy:`Use_existing ~id_reuse_policy:`Reject_duplicate
         blocked "reuse-open")
  in
  if Client.started attached || Client.run_id attached <> Client.run_id open_run
  then fail "use_existing with reject_duplicate did not attach to the open run"

(** A workflow retry policy makes Temporal retry a failed run as a new run of
    the same workflow ID: the first run reports attempt 1 and links the
    successor, whose own failure reports attempt 2 and ends the chain. A
    current-run handle is tracked so cleanup reaches whichever run is open.

    The pinned server reports a retried run's close event to this client as
    continue-as-new rather than as a failure with a successor, presumably
    because the client does not advertise Temporal's follows-next-run-id
    feature, so both forms of the link are accepted here; the attempt numbers
    still prove that the server applied the retry policy. *)
let check_retry_policy context =
  let retry_policy =
    get
      (Activity.Retry_policy.make ~initial_interval:(ms 500L)
         ~backoff_coefficient:1.0 ~maximum_interval:(ms 500L)
         ~maximum_attempts:2 ())
  in
  let id = workflow_id context "retry" in
  let first =
    get
      (Client.start context.client ~workflow:failing ~retry_policy
         ~task_queue:context.queue ~id ~input:"" ())
  in
  ignore (track context (get (Client.get_handle context.client ~workflow:failing ~id ())));
  let successor =
    match Client.wait first with
    | Ok (Client.Failed { successor = Some successor; error }) ->
        if not (String.starts_with ~prefix:"attempt 1 " (Error.message error)) then
          fail ("first run reported " ^ Error.message error);
        successor
    | Ok (Client.Continued_as_new successor) -> successor
    | Ok (Client.Failed { successor = None; _ }) ->
        fail "retry policy did not start a retry run"
    | Ok _ -> fail "first run did not fail"
    | Error error -> fail ("waiting for the first run failed: " ^ Error.message error)
  in
  if Some successor.Client.run_id = Client.run_id first then
    fail "retry run reused the first run";
  let retried = get (Client.follow context.client ~workflow:failing successor) in
  match Client.wait retried with
  | Ok (Client.Failed { successor = None; error })
    when String.starts_with ~prefix:"attempt 2 " (Error.message error) -> ()
  | Ok (Client.Failed { error; _ }) ->
      fail ("retry run reported " ^ Error.message error)
  | _ -> fail "retry run did not end the chain with a failure"

(** A start whose 1 ms deadline expires after it was sent is reported as an
    uncertain outcome: Temporal may or may not have created the run. If it
    expired before it was sent, it is a typed [`Deadline_exceeded]
    rejection instead. Retrying with the same
    request ID and the default deadline returns the run as started either
    way, and a separate start then finds exactly that run open. A signal
    with an expired deadline keeps the typed RPC classification. *)
let check_uncertain_start context =
  let request_id = workflow_id context "uncertain-request" in
  let id = workflow_id context "uncertain" in
  let start ?rpc_timeout ?request_id () =
    Client.start context.client ?rpc_timeout ?request_id ~workflow:blocked
      ~task_queue:context.queue ~id ~input:"" ()
  in
  (match start ~rpc_timeout:(ms 1L) ~request_id () with
  | Ok handle -> ignore (track context handle)
  | Error error when Client.is_start_outcome_uncertain error -> ()
  (* The deadline can also expire before the owner Domain sends the start,
     which is a definite, typed rejection rather than an uncertain one. *)
  | Error error when Client.rpc_status error = Some `Deadline_exceeded -> ()
  | Error error -> fail ("expired start was not typed: " ^ Error.message error));
  let reconciled = track context (get (start ~request_id ())) in
  if not (Client.started reconciled) then
    fail "request-ID retry did not report the run it created";
  expect_refused "single run after reconciliation" reconciled (start ());
  match
    Client.signal ~rpc_timeout:(ms 1L) reconciled
      ~signal:(Signal.define ~name:"unhandled" ~input:Codec.string)
      ~input:""
  with
  | Ok () -> ()
  | Error error when
      Client.rpc_status error = Some `Deadline_exceeded
      || Client.rpc_status error = Some `Unavailable -> ()
  | Error error -> fail ("signal deadline was not typed: " ^ Error.message error)

(** Starts the worker process, runs every check, and terminates all fixture
    executions and the worker even when a check fails. *)
let check address cli =
  let queue = "client-policies-" ^ Temporal_base.Client_request_id.create () in
  let pid =
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "worker"; address; queue |]
      Unix.stdin Unix.stdout Unix.stderr
  in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let context = { client; queue; cleanups = ref [] } in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun cleanup -> cleanup ()) !(context.cleanups);
      ignore (Client.shutdown client);
      Unix.kill pid Sys.sigterm;
      ignore (Unix.waitpid [] pid))
    (fun () ->
      check_timeouts context ~cli address;
      check_reuse_policies context;
      check_retry_policy context;
      check_uncertain_start context;
      print_endline "client policies live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address; cli ] -> check address cli
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"

(** Live regression for workflow start metadata read back by workflow code
    (#512).

    Against a disposable Temporal server it starts workflows with the start
    policies [Client.start] exposes (execution, run, and task timeouts, a
    workflow retry policy, and a memo) and requires the workflow to report,
    through [Workflow.info] and [Workflow.start_metadata], exactly what the
    client sent. Three chains are covered: a root run started without
    options, the retry run a failing first attempt produces, and the
    successor of an explicit continue-as-new. For each reporting run the
    report is read with a query, the worker process is replaced so a fresh
    worker must rebuild the run by replaying its history, the report is read
    again and must be identical, and the run then completes with the same
    report. The parent owns and reaps its worker, uses a unique task queue,
    and terminates every execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("workflow start info: " ^ label)

(** Milliseconds as a public duration. *)
let ms = Duration.of_ms

(** Signal that lets a reporting run complete. *)
let finish = Signal.define ~name:"finish" ~input:Codec.unit

(** Query returning the run's start report. *)
let report_query = Query.define ~name:"start-report" ~output:Codec.string

(** Workflow-local gate set by [finish]; an unset gate is closed. *)
let finished = Workflow_context.Local.create ()

(** Condition predicate over [finished]. *)
let is_finished () =
  match Workflow_context.Local.get finished with
  | Ok (Some true) -> true
  | Ok _ | Error _ -> false

(** Formats an optional duration as milliseconds, or [-] when absent. *)
let show_duration = Option.fold ~none:"-" ~some:(fun value -> Int64.to_string (Duration.to_ms value))

(** Renders everything the current run can read about how it was started as
    one comparable string. It reads only [Workflow.info] and
    [Workflow.start_metadata], which are deterministic history values, so a
    replaying worker must render the same string. Query handlers may call it
    because both readers are side-effect free. *)
let report () =
  let open Result_syntax in
  let* info = Workflow.info () in
  let* metadata = Workflow.start_metadata () in
  let* note =
    match Option.bind metadata.memo (List.assoc_opt "note") with
    | None -> Ok "-"
    | Some payload -> Codec.decode Codec.string payload
  in
  let module Info = Workflow.Info in
  let module Policy = Activity.Retry_policy in
  let initiator =
    match Info.continuation_initiator info with
    | None -> "-"
    | Some `Continue_as_new -> "continue-as-new"
    | Some `Retry -> "retry"
    | Some `Cron_schedule -> "cron"
  in
  let retry =
    Option.fold ~none:"-"
      ~some:(fun policy ->
        Printf.sprintf "%Ld/%g/%Ld/%d/%s"
          (Duration.to_ms (Policy.initial_interval policy))
          (Policy.backoff_coefficient policy)
          (Duration.to_ms (Policy.maximum_interval policy))
          (Policy.maximum_attempts policy)
          (String.concat "," (Policy.non_retryable_error_types policy)))
      (Info.retry_policy info)
  in
  Ok
    (Printf.sprintf
       "attempt=%d first=%s from=%s by=%s exec=%s run=%s task=%s retry=%s memo=%s"
       (Info.attempt info)
       (Option.value ~default:"-" (Info.first_execution_run_id info))
       (Option.value ~default:"-" (Info.continued_from_run_id info))
       initiator
       (show_duration (Info.execution_timeout info))
       (show_duration (Info.run_timeout info))
       (show_duration (Info.task_timeout info))
       retry note)

(** The continue-as-new target of [reporting], named by type so the
    definition can refer to its own workflow type. *)
let reporting_target =
  Workflow.remote ~name:"start-info-reporting" ~input:Codec.string
    ~output:Codec.string

(** Mode ["retry"] fails its first attempt so the retry policy starts a
    retry run; mode ["continue"] continues as new once. Every other run waits
    for [finish] and completes with its start report. *)
let reporting =
  Workflow.define ~name:"start-info-reporting" ~input:Codec.string
    ~output:Codec.string (fun mode ->
      let open Result_syntax in
      let* info = Workflow.info () in
      match mode with
      | "retry" when Workflow.Info.attempt info = 1 ->
          Error
            (Error.make ~category:`Workflow ~error_type:"StartInfoRetry"
               ~message:"first attempt fails to exercise the retry policy" ())
      | "continue" -> Workflow.continue_as_new reporting_target "continued"
      | _ ->
          let* () = Condition.wait_until is_finished in
          report ())

(** Runs the fixture definition on the test's unique queue until killed. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[]
         ~workflows:
           [
             Worker.workflow reporting
               ~signals:
                 [ Signal.Handler.make finish (fun () ->
                       Workflow_context.Local.set finished true) ]
               ~queries:[ Query.Handler.make report_query report ];
           ]
         ())
  in
  get (Worker.run worker)

(** Starts a worker process for [queue]. *)
let spawn address queue =
  Unix.create_process Sys.executable_name
    [| Sys.executable_name; "worker"; address; queue |]
    Unix.stdin Unix.stdout Unix.stderr

(** Kills and reaps a worker process; an already exited one is fine. *)
let stop pid =
  (try Unix.kill pid Sys.sigterm with Unix.Unix_error _ -> ());
  ignore (Unix.waitpid [] pid)

(** The client, queue, current worker process, and cleanup list shared by
    every check. *)
type context = {
  client : Client.t;
  address : string;
  queue : string;
  mutable worker : int;
  cleanups : (unit -> unit) list ref;
}

(** Derives a workflow ID unique to this run of the regression. *)
let workflow_id context suffix = context.queue ^ "-" ^ suffix

(** Registers the current run of [id] for termination when the regression
    ends, including after an assertion failure. *)
let track context id =
  let handle = get (Client.get_handle context.client ~workflow:reporting ~id ()) in
  context.cleanups := (fun () -> ignore (Client.terminate handle)) :: !(context.cleanups)

(** Replaces the worker process. The new worker has an empty cache, so its
    next task for any open run replays that run's history from the start. *)
let replace_worker context =
  stop context.worker;
  context.worker <- spawn context.address context.queue

(** Requires [handle]'s report to equal [expected] before and after worker
    replacement, then releases the run and requires it to complete with the
    same report. *)
let expect_report context label handle expected =
  let read stage =
    match Client.query handle ~query:report_query with
    | Ok report when report = expected -> ()
    | Ok report ->
        fail (Printf.sprintf "%s %s report:\n  got  %s\n  want %s" label stage report expected)
    | Error error -> fail (Printf.sprintf "%s %s query: %s" label stage (Error.message error))
  in
  read "live";
  replace_worker context;
  read "replayed";
  get (Client.signal handle ~signal:finish ~input:());
  match Client.wait handle with
  | Ok (Client.Completed { output; successor = None }) when output = expected -> ()
  | Ok (Client.Completed { output; _ }) ->
      fail (Printf.sprintf "%s completed with %s" label output)
  | Ok _ -> fail (label ^ " did not complete")
  | Error error -> fail (label ^ " wait: " ^ Error.message error)

(** The retry policy every option-bearing chain is started with. *)
let retry_policy =
  get
    (Activity.Retry_policy.make ~initial_interval:(ms 500L)
       ~backoff_coefficient:1.5 ~maximum_interval:(ms 2_000L)
       ~maximum_attempts:3 ~non_retryable_error_types:[ "StartInfoFatal" ] ())

(** The rendered retry policy, as [report] formats it. *)
let retry_report = "500/1.5/2000/3/StartInfoFatal"

(** Starts [mode] with every start option, returning the first run. *)
let start_with_options context mode suffix =
  let id = workflow_id context suffix in
  let memo = [ ("note", get (Codec.encode Codec.string "start-info-memo")) ] in
  let handle =
    get
      (Client.start context.client ~workflow:reporting ~memo
         ~execution_timeout:(ms 60_000L) ~run_timeout:(ms 30_000L)
         ~task_timeout:(ms 7_000L) ~retry_policy ~task_queue:context.queue ~id
         ~input:mode ())
  in
  track context id;
  handle

(** Returns a run's ID, which a started handle always has. *)
let run_id handle =
  match Client.run_id handle with Some id -> id | None -> fail "started run has no run ID"

(** A run started without options reports no continuation, no execution or
    run limit, no retry policy, no memo, and Temporal's recorded 10 second
    default task timeout. *)
let check_root context =
  let id = workflow_id context "root" in
  let handle =
    get
      (Client.start context.client ~workflow:reporting ~task_queue:context.queue ~id
         ~input:"report" ())
  in
  track context id;
  expect_report context "root" handle
    (Printf.sprintf
       "attempt=1 first=%s from=- by=- exec=- run=- task=10000 retry=- memo=-"
       (run_id handle))

(** The retry run of a failed first attempt reports attempt 2, the failed
    run as its predecessor and chain origin, the [`Retry] initiator, and the
    timeouts, retry policy, and memo the client set. *)
let check_retry context =
  let first = start_with_options context "retry" "retry" in
  let successor =
    match Client.wait first with
    | Ok (Client.Failed { successor = Some successor; _ }) -> successor
    | Ok _ -> fail "first attempt did not fail with a retry successor"
    | Error error -> fail ("waiting for the first attempt: " ^ Error.message error)
  in
  let retried = get (Client.follow context.client ~workflow:reporting successor) in
  let first_run = run_id first in
  expect_report context "retry" retried
    (Printf.sprintf
       "attempt=2 first=%s from=%s by=retry exec=60000 run=30000 task=7000 retry=%s memo=start-info-memo"
       first_run first_run retry_report)

(** The successor of an explicit continue-as-new reports its predecessor, the
    [`Continue_as_new] initiator, the chain-wide execution timeout, and the
    run timeout, task timeout, retry policy, and memo Temporal carried over
    from the previous run (the SDK's continue-as-new command overrides none
    of them). *)
let check_continue_as_new context =
  let first = start_with_options context "continue" "continue" in
  let successor =
    match Client.wait first with
    | Ok (Client.Continued_as_new successor) -> successor
    | Ok _ -> fail "first run did not continue as new"
    | Error error -> fail ("waiting for the continuing run: " ^ Error.message error)
  in
  let next = get (Client.follow context.client ~workflow:reporting successor) in
  let first_run = run_id first in
  expect_report context "continue-as-new" next
    (Printf.sprintf
       "attempt=1 first=%s from=%s by=continue-as-new exec=60000 run=30000 task=7000 retry=%s memo=start-info-memo"
       first_run first_run retry_report)

(** Starts the worker process, runs every check, and terminates all fixture
    executions and the worker even when a check fails. *)
let check address =
  let queue = "start-info-" ^ Temporal_base.Client_request_id.create () in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let context =
    { client; address; queue; worker = spawn address queue; cleanups = ref [] }
  in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun cleanup -> cleanup ()) !(context.cleanups);
      ignore (Client.shutdown client);
      stop context.worker)
    (fun () ->
      check_root context;
      check_retry context;
      check_continue_as_new context;
      print_endline "workflow start info live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address ] -> check address
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233"

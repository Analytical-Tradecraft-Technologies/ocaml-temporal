(** Live regression for child workflow start options (#1135).

    Against a disposable Temporal server it checks that the timeouts and ID
    reuse policy a parent passes to [Child_workflow] reach Temporal:

    - A child started with execution, run, and task timeouts reports them
      through [Workflow.info]. The parent exposes the child's report through
      a query, the worker process is replaced so a fresh worker must replay
      the parent's history (including the start-child command carrying the
      options), and the report must be read back identically.
    - A child that never finishes, started with a short run timeout, is ended
      by Temporal: the parent observes a failed child, and an exact-run
      [Client.wait] on the child reports [Timed_out].
    - Reusing a closed child's workflow ID is refused under
      [`Reject_duplicate] (the parent's [started] future fails) and allowed
      under [`Allow_duplicate].

    The parent owns and reaps its worker, uses a unique task queue, and
    terminates every execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("child start options: " ^ label)

(** Milliseconds as a public duration. *)
let ms = Duration.of_ms

(** Signal that lets a reporting parent complete. *)
let finish = Signal.define ~name:"finish" ~input:Codec.unit

(** Query returning what the parent learned from its child. *)
let report_query = Query.define ~name:"child-report" ~output:Codec.string

(** Workflow-local gate set by [finish]; an unset gate is closed. *)
let finished = Workflow_context.Local.create ()

(** Workflow-local copy of the parent's result, read by [report_query]. *)
let outcome = Workflow_context.Local.create ()

(** Condition predicate over [finished]. *)
let is_finished () =
  match Workflow_context.Local.get finished with
  | Ok (Some true) -> true
  | Ok _ | Error _ -> false

(** Formats an optional duration as milliseconds, or [-] when absent. *)
let show_duration =
  Option.fold ~none:"-" ~some:(fun value -> Int64.to_string (Duration.to_ms value))

(** The child. Mode ["report"] returns the timeouts Temporal recorded for
    it; mode ["stall"] waits for a condition that never holds, so only its
    run timeout can end it. *)
let child =
  Workflow.define ~name:"child-options-child" ~input:Codec.string
    ~output:Codec.string (fun mode ->
      let open Result_syntax in
      match mode with
      | "stall" ->
          let* () = Condition.wait_until (fun () -> false) in
          Ok "unreachable"
      | _ ->
          let* info = Workflow.info () in
          let module Info = Workflow.Info in
          Ok
            (Printf.sprintf "exec=%s run=%s task=%s"
               (show_duration (Info.execution_timeout info))
               (show_duration (Info.run_timeout info))
               (show_duration (Info.task_timeout info))))

(** Renders a child error by category, so the expected string does not
    depend on server wording. *)
let show_error error = "error:" ^ Error.kind error

(** The parent. Its input is ["<mode>:<child id>"]:
    - ["report"] runs the child with all three timeouts and records the
      child's report;
    - ["timeout"] runs a stalling child with a three-second run timeout and
      records the error it observes;
    - ["reuse"] runs a child to completion, then starts the same child ID
      again under [`Reject_duplicate] and under [`Allow_duplicate],
      recording whether each start was refused.
    Every mode then waits for [finish] so the test can query the recorded
    outcome before and after a worker replacement. *)
let parent =
  Workflow.define ~name:"child-options-parent" ~input:Codec.string
    ~output:Codec.string (fun input ->
      let open Result_syntax in
      let mode, child_id =
        match String.index_opt input ':' with
        | Some index ->
            (String.sub input 0 index,
             String.sub input (index + 1) (String.length input - index - 1))
        | None -> (input, input)
      in
      let* recorded =
        match mode with
        | "report" ->
            Child_workflow.execute ~id:child_id
              ~execution_timeout:(ms 60_000L) ~run_timeout:(ms 30_000L)
              ~task_timeout:(ms 7_000L) child "report"
        | "timeout" -> (
            match
              Child_workflow.execute ~id:child_id ~run_timeout:(ms 3_000L)
                child "stall"
            with
            | Ok value -> Ok ("unexpected success:" ^ value)
            | Error error -> Ok (show_error error))
        | "reuse" ->
            let* _ = Child_workflow.execute ~id:child_id child "report" in
            let attempt policy =
              let handle =
                Child_workflow.start_handle ~id:child_id ~id_reuse_policy:policy
                  child "report"
              in
              match Future.await (Child_workflow.started handle) with
              | Error error -> show_error error
              | Ok _ -> (
                  match Future.await (Child_workflow.future handle) with
                  | Ok _ -> "started"
                  | Error error -> "started-then-" ^ show_error error)
            in
            let rejected = attempt `Reject_duplicate in
            let allowed = attempt `Allow_duplicate in
            Ok (Printf.sprintf "reject=%s allow=%s" rejected allowed)
        | _ -> Error (Error.defect ~message:("unknown mode " ^ mode))
      in
      let* () = Workflow_context.Local.set outcome recorded in
      let* () = Condition.wait_until is_finished in
      Ok recorded)

(** Reads the recorded outcome for [report_query]. *)
let read_outcome () =
  match Workflow_context.Local.get outcome with
  | Ok (Some value) -> Ok value
  | Ok None -> Ok "-"
  | Error error -> Error error

(** Runs the fixture definitions on the test's unique queue until killed. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[]
         ~workflows:
           [
             Worker.workflow parent
               ~signals:
                 [ Signal.Handler.make finish (fun () ->
                       Workflow_context.Local.set finished true) ]
               ~queries:[ Query.Handler.make report_query read_outcome ];
             Worker.workflow child;
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

(** Registers the current run of [id] of [workflow] for termination when the
    regression ends, including after an assertion failure. *)
let track context workflow id =
  let handle = get (Client.get_handle context.client ~workflow ~id ()) in
  context.cleanups := (fun () -> ignore (Client.terminate handle)) :: !(context.cleanups)

(** Replaces the worker process. The new worker has an empty cache, so its
    next task for any open run replays that run's history from the start. *)
let replace_worker context =
  stop context.worker;
  context.worker <- spawn context.address context.queue

(** Polls the parent's query until it reports an outcome, since the child
    work it waits on takes a few seconds. *)
let await_outcome label handle =
  let deadline = Unix.gettimeofday () +. 60. in
  let rec loop () =
    match Client.query handle ~query:report_query with
    | Ok "-" | Error _ when Unix.gettimeofday () < deadline ->
        Unix.sleepf 0.5;
        loop ()
    | Ok "-" -> fail (label ^ " recorded no outcome within 60 s")
    | Ok value -> value
    | Error error -> fail (label ^ " query: " ^ Error.message error)
  in
  loop ()

(** Starts the parent in [mode], requires its recorded outcome to equal
    [expected] before and after a worker replacement, then releases it and
    requires it to complete with the same outcome. *)
let check_parent context label mode expected =
  let id = workflow_id context label in
  let child_id = workflow_id context (label ^ "-child") in
  let handle =
    get
      (Client.start context.client ~workflow:parent ~task_queue:context.queue
         ~id ~input:(mode ^ ":" ^ child_id) ())
  in
  track context parent id;
  track context child child_id;
  let live = await_outcome label handle in
  if live <> expected then
    fail (Printf.sprintf "%s live outcome:\n  got  %s\n  want %s" label live expected);
  replace_worker context;
  let replayed = await_outcome label handle in
  if replayed <> expected then
    fail (Printf.sprintf "%s replayed outcome:\n  got  %s\n  want %s" label replayed expected);
  get (Client.signal handle ~signal:finish ~input:());
  (match Client.wait handle with
  | Ok (Client.Completed { output; _ }) when output = expected -> ()
  | Ok (Client.Completed { output; _ }) -> fail (label ^ " completed with " ^ output)
  | Ok _ -> fail (label ^ " did not complete")
  | Error error -> fail (label ^ " wait: " ^ Error.message error));
  child_id

(** The child reports the three timeouts its parent set. *)
let check_report context =
  ignore (check_parent context "report" "report" "exec=60000 run=30000 task=7000")

(** A stalling child is ended by its run timeout: the parent sees a failed
    child and an exact-run wait on the child reports [Timed_out]. *)
let check_timeout context =
  let child_id = check_parent context "timeout" "timeout" "error:child_workflow" in
  let child_handle = get (Client.get_handle context.client ~workflow:child ~id:child_id ()) in
  match Client.wait child_handle with
  | Ok (Client.Timed_out _) -> ()
  | Ok _ -> fail "the stalled child did not end as Timed_out"
  | Error error -> fail ("waiting for the stalled child: " ^ Error.message error)

(** A closed child's ID is refused under [`Reject_duplicate] and reused
    under [`Allow_duplicate]. *)
let check_reuse context =
  ignore
    (check_parent context "reuse" "reuse" "reject=error:child_workflow allow=started")

(** Starts the worker process, runs every check, and terminates all fixture
    executions and the worker even when a check fails. *)
let check address =
  let queue = "child-options-" ^ Temporal_base.Client_request_id.create () in
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
      check_report context;
      check_timeout context;
      check_reuse context;
      print_endline "child start options live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address ] -> check address
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233"

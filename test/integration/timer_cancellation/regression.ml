(** Live regression for durable-timer cancellation (#1134).

    Against a disposable Temporal server it runs three workflows and checks
    both their outcome and the timer events Temporal recorded:

    - ["satisfied"]: [Condition.wait_until ~timeout] is satisfied by a
      signal before its 60 s bound, so the bound's timer is cancelled
      ([TIMER_CANCELED]) and the wait succeeds.
    - ["timeout"]: the same wait with a 2 s bound is never satisfied, so the
      timer fires ([TIMER_FIRED]), nothing is cancelled, and the wait reports
      a [`Timeout] error.
    - ["scope"]: [Workflow.sleep ~scope] on a 60 s timer is cancelled by a
      signal handler cancelling the scope, so the timer is cancelled and the
      sleep returns [`Cancelled].

    Each workflow records its outcome and then waits for [finish]. The
    outcome is read with a query, the worker process is replaced so a fresh
    worker must replay the history (including the cancel or fire), and the
    outcome is read again and must be identical before the workflow
    completes with it. The timer events are read with the official CLI. The
    parent owns and reaps its worker, uses a unique task queue, and
    terminates every execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("timer cancellation: " ^ label)

(** Signal satisfying the ["satisfied"] wait. *)
let go = Signal.define ~name:"go" ~input:Codec.unit

(** Signal whose handler cancels the ["scope"] workflow's scope. *)
let cancel_scope = Signal.define ~name:"cancel-scope" ~input:Codec.unit

(** Signal that lets a workflow complete after its outcome was read. *)
let finish = Signal.define ~name:"finish" ~input:Codec.unit

(** Query returning the recorded outcome, or ["-"] before there is one. *)
let outcome_query = Query.define ~name:"outcome" ~output:Codec.string

(** Workflow-local flags and values; an unset flag is false. *)
let went = Workflow_context.Local.create ()
let finished = Workflow_context.Local.create ()
let outcome = Workflow_context.Local.create ()
let scope_slot = Workflow_context.Local.create ()

(** Reads a workflow-local boolean flag. *)
let flag key =
  match Workflow_context.Local.get key with Ok (Some true) -> true | Ok _ | Error _ -> false

(** Renders a result by outcome and error category only, so the expected
    strings do not depend on diagnostic wording. *)
let show = function Ok () -> "ok" | Error error -> "error:" ^ Error.kind error

(** The fixture workflow; its input names the scenario. *)
let fixture =
  Workflow.define ~name:"timer-cancellation" ~input:Codec.string
    ~output:Codec.string (fun mode ->
      let open Result_syntax in
      let recorded =
        match mode with
        | "satisfied" ->
            show
              (Condition.wait_until ~timeout:(Duration.of_seconds 60L) (fun () ->
                   flag went))
        | "timeout" ->
            show
              (Condition.wait_until ~timeout:(Duration.of_seconds 2L) (fun () ->
                   flag went))
        | "scope" ->
            show
              (Scope.with_scope (fun scope ->
                   match Workflow_context.Local.set scope_slot scope with
                   | Error _ as error -> error
                   | Ok () -> Workflow.sleep ~scope (Duration.of_seconds 60L)))
        | other -> "unknown-mode:" ^ other
      in
      let* () = Workflow_context.Local.set outcome recorded in
      let* () = Condition.wait_until (fun () -> flag finished) in
      Ok recorded)

(** Cancels the scope the ["scope"] workflow stored, from a signal handler
    running on the workflow's own scheduler. *)
let cancel_stored_scope () =
  match Workflow_context.Local.get scope_slot with
  | Ok (Some scope) -> Scope.cancel scope
  | Ok None -> Error (Error.defect ~message:"no scope stored yet")
  | Error _ as error -> error

(** Reads the recorded outcome for [outcome_query]. *)
let read_outcome () =
  match Workflow_context.Local.get outcome with
  | Ok (Some value) -> Ok value
  | Ok None -> Ok "-"
  | Error error -> Error error

(** Runs the fixture definition on the test's unique queue until killed. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[]
         ~workflows:
           [
             Worker.workflow fixture
               ~signals:
                 [
                   Signal.Handler.make go (fun () -> Workflow_context.Local.set went true);
                   Signal.Handler.make cancel_scope cancel_stored_scope;
                   Signal.Handler.make finish (fun () ->
                       Workflow_context.Local.set finished true);
                 ]
               ~queries:[ Query.Handler.make outcome_query read_outcome ];
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

(** The client, CLI, queue, current worker process, and cleanup list shared
    by every check. *)
type context = {
  client : Client.t;
  address : string;
  cli : string;
  queue : string;
  mutable worker : int;
  cleanups : (unit -> unit) list ref;
}

(** Retries [attempt] for up to 60 s, for state that appears asynchronously. *)
let await label attempt =
  let deadline = Unix.gettimeofday () +. 60. in
  let rec poll () =
    match attempt () with
    | Ok value -> value
    | Error _ when Unix.gettimeofday () < deadline ->
        Unix.sleepf 0.2;
        poll ()
    | Error message -> fail (Printf.sprintf "timed out waiting for %s: %s" label message)
  in
  poll ()

(** Runs the official CLI's [workflow show] for one exact run and returns
    its event types without the [EVENT_TYPE_] prefix, without a shell. *)
let history context handle =
  let args =
    [| context.cli; "--address"; context.address; "--namespace"; "default";
       "--command-timeout"; "30s"; "workflow"; "show"; "--workflow-id";
       Client.workflow_id handle; "--run-id"; Option.get (Client.run_id handle);
       "--output"; "json" |]
  in
  await ("history of " ^ Client.workflow_id handle) (fun () ->
      let input = Unix.open_process_args_in context.cli args in
      let document =
        match Yojson.Basic.from_channel input with
        | document -> Ok document
        | exception Yojson.Json_error message -> Error message
      in
      match (Unix.close_process_in input, document) with
      | Unix.WEXITED 0, Ok document -> (
          try
            Ok
              Yojson.Basic.Util.(
                document |> member "events" |> to_list
                |> List.map (fun event ->
                       let name = event |> member "eventType" |> to_string in
                       let prefix = "EVENT_TYPE_" in
                       if String.starts_with ~prefix name then
                         String.sub name (String.length prefix)
                           (String.length name - String.length prefix)
                       else name))
          with Yojson.Basic.Util.Type_error (message, _) -> Error message)
      | Unix.WEXITED 0, Error message -> Error ("unparsable CLI output: " ^ message)
      | Unix.WEXITED code, _ -> Error (Printf.sprintf "CLI exited with %d" code)
      | (Unix.WSIGNALED _ | Unix.WSTOPPED _), _ -> Error "CLI was killed")

(** Counts the timer events of [kind] in [events]. *)
let count kind events = List.length (List.filter (String.equal kind) events)

(** Replaces the worker process. The new worker has an empty cache, so its
    next task for the open run replays that run's history from the start. *)
let replace_worker context =
  stop context.worker;
  context.worker <- spawn context.address context.queue

(** Polls the outcome query until the workflow has recorded one. *)
let await_outcome handle =
  await ("outcome of " ^ Client.workflow_id handle) (fun () ->
      match Client.query handle ~query:outcome_query with
      | Ok "-" -> Error "no outcome yet"
      | Ok value -> Ok value
      | Error error -> Error (Error.message error))

(** Runs one scenario: starts [mode], applies [drive] once the workflow is
    waiting, and requires the [expected] outcome before and after a worker
    replacement, exactly [started], [cancelled] and [fired] timer events,
    and completion with the same outcome. *)
let check_scenario context mode ~drive ~expected ~started ~cancelled ~fired =
  let id = context.queue ^ "-" ^ mode in
  let handle =
    get
      (Client.start context.client ~workflow:fixture ~task_queue:context.queue ~id
         ~input:mode ())
  in
  context.cleanups := (fun () -> ignore (Client.terminate handle)) :: !(context.cleanups);
  (* The waiting workflow has started its bounded timer once the timer is
     in history, so [drive] cannot race the timer's creation. *)
  ignore
    (await (mode ^ " timer started") (fun () ->
         if count "TIMER_STARTED" (history context handle) > 0 then Ok ()
         else Error "timer not started"));
  drive handle;
  let live = await_outcome handle in
  if live <> expected then
    fail (Printf.sprintf "%s live outcome: got %s, want %s" mode live expected);
  replace_worker context;
  let replayed = await_outcome handle in
  if replayed <> expected then
    fail (Printf.sprintf "%s replayed outcome: got %s, want %s" mode replayed expected);
  get (Client.signal handle ~signal:finish ~input:());
  (match Client.wait handle with
  | Ok (Client.Completed { output; _ }) when output = expected -> ()
  | Ok (Client.Completed { output; _ }) -> fail (mode ^ " completed with " ^ output)
  | Ok _ -> fail (mode ^ " did not complete")
  | Error error -> fail (mode ^ " wait: " ^ Error.message error));
  let events = history context handle in
  let actual =
    (count "TIMER_STARTED" events, count "TIMER_CANCELED" events, count "TIMER_FIRED" events)
  in
  if actual <> (started, cancelled, fired) then (
    let s, c, f = actual in
    fail
      (Printf.sprintf "%s timer events: started=%d canceled=%d fired=%d, want %d/%d/%d"
         mode s c f started cancelled fired));
  Printf.printf "%s: outcome=%s timers started=%d canceled=%d fired=%d\n%!" mode
    expected started cancelled fired

(** Starts the worker process, runs every scenario, and terminates all
    fixture executions and the worker even when a check fails. *)
let check address cli =
  let queue = "timer-cancel-" ^ Temporal_base.Client_request_id.create () in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let context =
    { client; address; cli; queue; worker = spawn address queue; cleanups = ref [] }
  in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun cleanup -> cleanup ()) !(context.cleanups);
      ignore (Client.shutdown client);
      stop context.worker)
    (fun () ->
      check_scenario context "satisfied"
        ~drive:(fun handle -> get (Client.signal handle ~signal:go ~input:()))
        ~expected:"ok" ~started:1 ~cancelled:1 ~fired:0;
      check_scenario context "timeout" ~drive:ignore ~expected:"error:timeout"
        ~started:1 ~cancelled:0 ~fired:1;
      check_scenario context "scope"
        ~drive:(fun handle -> get (Client.signal handle ~signal:cancel_scope ~input:()))
        ~expected:"error:cancelled" ~started:1 ~cancelled:1 ~fired:0;
      print_endline "timer cancellation live regression: ok")

(** The supplied URL must identify a disposable test namespace/server; the
    second argument is the official Temporal CLI. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address; cli ] -> check address cli
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"

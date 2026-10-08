(** Live regression for issue #530: queries and an accepted, suspended update
    recovered after sticky-cache eviction and after worker replacement.

    One process plays two roles. [worker] runs a public [Temporal.Worker] on a
    unique task queue, optionally with a one-entry sticky cache. [check] owns
    the client, spawns and reaps every worker process, and synchronizes only on
    observable server state: client query results, update outcomes, and exact
    run histories read through the official Temporal CLI. Waits are bounded
    polls with named failures; no step relies on a fixed sleep.

    Three runs of the same workflow are compared. The eviction run and the
    restart run are interleaved with queries, a validator-rejected update, a
    re-attached update handle, and cache eviction or worker replacement. The
    control run receives only the command-producing interactions. Their
    durable, non-workflow-task events, including each event's meaningful
    attributes such as payloads and the timer duration, must be identical, proving that queries
    and rejected updates did not alter later commands, and each recovered
    update must be accepted and completed exactly once on its original run. *)
open Temporal

(** Reports setup and client failures through their public diagnostic. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Test-only, per-process counters keyed by (counter name, workflow ID).

    This is deliberately process-global mutation inside workflow callbacks,
    which ordinary workflow code must not do. It is safe here because the
    counters are only ever read by the [probe] query, which emits no commands,
    so they cannot influence replayed decisions. A fresh worker process starts
    every counter at zero, which is what lets the driver prove that state was
    rebuilt by replay and that replay did not re-run a validator. The mutex
    guards against the worker's lanes touching the table concurrently. *)
module Probe = struct
  (** Counter storage; see the module comment for its ownership rules. *)
  let counts : (string * string, int) Hashtbl.t = Hashtbl.create 8

  (** Serializes every access to [counts]. *)
  let lock = Mutex.create ()

  (** Keys a counter by the current workflow ID. Workflow info is read-only, so
      this is also valid inside update validators and queries. *)
  let key name = Result.map (fun info -> (name, Workflow.Info.workflow_id info)) (Workflow.info ())

  (** Returns the current count for [name] in the active workflow. *)
  let read name = Result.map (fun key ->
    Mutex.protect lock (fun () -> Option.value ~default:0 (Hashtbl.find_opt counts key)))
    (key name)

  (** Increments the count for [name] in the active workflow. *)
  let bump name = Result.map (fun key ->
    Mutex.protect lock (fun () ->
      Hashtbl.replace counts key (1 + Option.value ~default:0 (Hashtbl.find_opt counts key))))
    (key name)
end

(** Workflow-local state: the counter and the two signal-controlled gates. *)
let value = Workflow_context.Local.create ()
let released = Workflow_context.Local.create ()
let finished = Workflow_context.Local.create ()

(** Reads the counter without creating commands. *)
let read () = Result.map (Option.value ~default:0) (Workflow_context.Local.get value)

(** Condition predicate over a boolean gate; an unset gate is closed. *)
let is_open gate () = match Workflow_context.Local.get gate with
  | Ok (Some true) -> true
  | Ok _ | Error _ -> false

(** Counts body starts per process, then parks until [finish]. A fresh
    activation of the body happens on the first task and on every replay. *)
let workflow = Workflow.define ~name:"interaction-recovery" ~input:Codec.int ~output:Codec.int
  (fun initial ->
    let open Result_syntax in
    let* () = Probe.bump "body" in
    let* () = Workflow_context.Local.set value initial in
    let* () = Condition.wait_until (is_open finished) in
    read ())

(** A filler workflow with no handlers. On the one-slot worker its first task
    can only be processed after the target run is evicted. *)
let filler = Workflow.define ~name:"interaction-recovery-filler" ~input:Codec.unit
  ~output:Codec.unit (fun () -> Condition.wait_until (fun () -> false))

(** Opens the gate the suspended update handler waits on. *)
let release = Signal.define ~name:"release" ~input:Codec.unit

(** Opens the gate that lets the workflow body return its counter. *)
let finish = Signal.define ~name:"finish" ~input:Codec.unit

(** Adds a non-negative amount once released; the result is the new counter. *)
let update = Update.define ~name:"add" ~input:Codec.int ~output:Codec.int

(** Reports reconstructed state and this process's instrumentation in one
    read-only round trip. *)
let probe = Query.define ~name:"probe" ~output:Codec.string

(** Renders the [probe] response; the driver compares whole strings so a
    failure shows every field at once. *)
let render ~value ~body ~validator =
  Printf.sprintf "value=%d body_starts=%d validator_calls=%d" value body validator

(** Answers [probe] from workflow-local state and the process counters. *)
let probe_handler () =
  let open Result_syntax in
  let* value = read () in
  let* body = Probe.read "body" in
  let* validator = Probe.read "validator" in
  Ok (render ~value ~body ~validator)

(** Counts every invocation, then rejects negative amounts. Core skips
    validators for updates replayed from history, so the count must not grow
    when a run is rebuilt. *)
let validator amount =
  let open Result_syntax in
  let* () = Probe.bump "validator" in
  if amount < 0 then Error (Error.make ~category:`Update ~non_retryable:true
    ~message:"NEGATIVE_REJECTED" ())
  else Ok ()

(** Suspends after acceptance until [release], then starts a short durable
    timer so the recovered handler must also emit a new command, and finally
    applies the amount. *)
let handler amount =
  let open Result_syntax in
  let* () = Condition.wait_until (is_open released) in
  let* () = Workflow.sleep (Duration.of_ms 50L) in
  let* current = read () in
  let* () = Workflow_context.Local.set value (current + amount) in
  read ()

(** Runs one worker until it is terminated. [cache] is ["default"] or a
    positive sticky-cache bound. *)
let worker address queue cache =
  let max_cached_workflows = match cache with
    | "default" -> None
    | bound -> Some (int_of_string bound) in
  let worker = get (Worker.create ?max_cached_workflows ~target_url:address
    ~namespace:"default" ~task_queue:queue ~activities:[]
    ~workflows:[
      Worker.workflow workflow
        ~queries:[Query.Handler.make probe probe_handler]
        ~signals:[Signal.Handler.make release (fun () -> Workflow_context.Local.set released true);
                  Signal.Handler.make finish (fun () -> Workflow_context.Local.set finished true)]
        ~updates:[Update.Handler.make ~validator update handler];
      Worker.workflow filler] ()) in
  get (Worker.run worker)

(** Spawns a separate worker process so a replacement shares no OCaml state. *)
let spawn address queue cache = Unix.create_process Sys.executable_name
  [|Sys.executable_name; "worker"; address; queue; cache|]
  Unix.stdin Unix.stdout Unix.stderr

(** Terminates and reaps one worker process; no fixture worker survives. *)
let stop pid = (try Unix.kill pid Sys.sigterm with Unix.Unix_error _ -> ());
  ignore (Unix.waitpid [] pid)

(** Seconds allowed for any one observation to become true. Replacing a
    worker can make Temporal wait for the old sticky queue to time out before
    it redispatches, so this is deliberately generous. *)
let observation_seconds = 90.

(** Polls [attempt] until it returns [Ok], logging each transient error, and
    fails with [label] and the last error when the deadline passes. *)
let await label attempt =
  let deadline = Unix.gettimeofday () +. observation_seconds in
  let rec poll () = match attempt () with
    | Ok value -> value
    | Error message when Unix.gettimeofday () < deadline ->
        Printf.eprintf "interaction recovery: waiting for %s: %s\n%!" label message;
        Unix.sleepf 0.1;
        poll ()
    | Error message -> failwith (Printf.sprintf "timed out waiting for %s: %s" label message) in
  poll ()

(** Waits until [probe] answers with exactly [expected]. A query error (for
    example while Temporal redispatches away from a dead sticky queue) and a
    different answer are both retried, and the last one is reported. *)
let await_probe label handle expected =
  await label (fun () -> match Client.query handle ~query:probe with
    | Ok actual when actual = expected -> Ok ()
    | Ok actual -> Error (Printf.sprintf "probe=%S, expected %S" actual expected)
    | Error error -> Error (Error.message error))

(** Requires [probe] to answer [expected] now. Used where the workflow is
    already settled, so any difference is a defect rather than a race. *)
let expect_probe label handle expected =
  match Client.query handle ~query:probe with
  | Ok actual when actual = expected -> ()
  | Ok actual -> failwith (Printf.sprintf "%s: probe=%S, expected %S" label actual expected)
  | Error error -> failwith (Printf.sprintf "%s: query failed: %s" label (Error.message error))

(** Deadline for one CLI invocation, matching the other history fixtures. *)
let cli_timeout = "30s"

(** Reads one exact run's history as JSON events through the official CLI,
    without a shell. A failed read is an [Error] so callers can retry it. *)
let history cli address handle =
  let args = [|cli; "--address"; address; "--namespace"; "default";
    "--command-timeout"; cli_timeout; "workflow"; "show";
    "--workflow-id"; Client.workflow_id handle; "--run-id"; Client.run_id handle;
    "--output"; "json"|] in
  let input = Unix.open_process_args_in cli args in
  let document = match Yojson.Basic.from_channel input with
    | document -> Ok document
    | exception Yojson.Json_error message -> Error message
    | exception exn -> ignore (Unix.close_process_in input); raise exn in
  match Unix.close_process_in input, document with
  | Unix.WEXITED 0, Ok document ->
      (try Ok Yojson.Basic.Util.(document |> member "events" |> to_list)
       with Yojson.Basic.Util.Type_error (message, _) -> Error ("unexpected history: " ^ message))
  | Unix.WEXITED 0, Error message -> Error ("unparsable history: " ^ message)
  | Unix.WEXITED code, _ -> Error (Printf.sprintf "history read exited with %d" code)
  | (Unix.WSIGNALED _ | Unix.WSTOPPED _), _ -> Error "history read was killed"

(** Returns an event's type without its [EVENT_TYPE_] prefix. *)
let event_type event =
  let name = Yojson.Basic.Util.(event |> member "eventType" |> to_string) in
  let prefix = "EVENT_TYPE_" in
  if String.starts_with ~prefix name then
    String.sub name (String.length prefix) (String.length name - String.length prefix)
  else name

(** Reads a history, retrying transient CLI failures within the deadline. *)
let read_history cli address handle =
  await ("history of " ^ Client.workflow_id handle) (fun () -> history cli address handle)

(** Counts events of one type. *)
let count kind events = List.length (List.filter (fun event -> event_type event = kind) events)

(** Allowlist of compared attributes for each non-workflow-task event type
    this workflow can record: the attribute object's key and the paths inside
    it. They carry the meaning of each event (workflow type, input and result,
    update ID, name, arguments and outcome, signal name and payload, timer ID
    and duration). Everything else is deliberately not compared because it
    legitimately differs between executions: event IDs and times, task
    queues, workflow, run and request IDs, worker identities, references to
    workflow-task and other event IDs, and server bookkeeping such as
    workflow-task timeouts and attempt counters. An event type without an
    entry fails closed, so new behavior cannot silently escape comparison. *)
let compared_attributes = function
  | "WORKFLOW_EXECUTION_STARTED" ->
      "workflowExecutionStartedEventAttributes", [["workflowType"; "name"]; ["input"]]
  | "WORKFLOW_EXECUTION_UPDATE_ACCEPTED" ->
      "workflowExecutionUpdateAcceptedEventAttributes",
      [["acceptedRequest"; "meta"; "updateId"]; ["acceptedRequest"; "input"; "name"];
       ["acceptedRequest"; "input"; "args"]]
  | "WORKFLOW_EXECUTION_SIGNALED" ->
      "workflowExecutionSignaledEventAttributes", [["signalName"]; ["input"]]
  | "TIMER_STARTED" -> "timerStartedEventAttributes", [["timerId"]; ["startToFireTimeout"]]
  | "TIMER_FIRED" -> "timerFiredEventAttributes", [["timerId"]]
  | "WORKFLOW_EXECUTION_UPDATE_COMPLETED" ->
      "workflowExecutionUpdateCompletedEventAttributes", [["meta"; "updateId"]; ["outcome"]]
  | "WORKFLOW_EXECUTION_COMPLETED" -> "workflowExecutionCompletedEventAttributes", [["result"]]
  | kind -> failwith ("no compared attributes are defined for history event " ^ kind)

(** Renders one non-workflow-task event as its type followed by every
    allowlisted attribute, in a canonical (key-sorted) JSON form. A missing
    allowlisted attribute is a failure rather than a silent [null]. *)
let render_event event =
  let kind = event_type event in
  let key, paths = compared_attributes kind in
  let attributes = Yojson.Basic.Util.member key event in
  let field path =
    let value = List.fold_left (fun json name -> Yojson.Basic.Util.member name json)
      attributes path in
    if value = `Null then
      failwith (Printf.sprintf "%s event has no %s.%s" kind key (String.concat "." path));
    Printf.sprintf "%s=%s" (String.concat "." path)
      (Yojson.Basic.to_string (Yojson.Basic.sort value)) in
  String.concat " " (kind :: List.map field paths)

(** The run's durable, command- and interaction-driven events with their
    meaningful attributes (see {!compared_attributes}). Workflow-task events
    are dropped because their number legitimately differs when a sticky task
    times out after a worker is replaced, or when a task is retried after
    eviction; they carry no workflow decision of their own. *)
let decisions events = List.filter_map (fun event ->
  if String.starts_with ~prefix:"WORKFLOW_TASK_" (event_type event) then None
  else Some (render_event event)) events

(** Collects every string under an [updateId] key anywhere in [json]. *)
let rec update_ids json = match json with
  | `Assoc fields -> List.concat_map (fun (key, field) ->
      match key, field with
      | "updateId", `String id -> [id]
      | _ -> update_ids field) fields
  | `List items -> List.concat_map update_ids items
  | _ -> []

(** Requires the update to be accepted but not yet completed. *)
let expect_accepted_only label events =
  let accepted = count "WORKFLOW_EXECUTION_UPDATE_ACCEPTED" events in
  let completed = count "WORKFLOW_EXECUTION_UPDATE_COMPLETED" events in
  if accepted <> 1 || completed <> 0 then
    failwith (Printf.sprintf "%s: expected one accepted and no completed update, found %d/%d"
      label accepted completed)

(** Requires exactly one acceptance followed by one completion, both naming
    [update_id]; a rejected update must leave no event. *)
let expect_completed_once label update_id events =
  let only kind = match List.filter (fun event -> event_type event = kind) events with
    | [event] -> event
    | found -> failwith (Printf.sprintf "%s: expected one %s, found %d" label kind
        (List.length found)) in
  let accepted = only "WORKFLOW_EXECUTION_UPDATE_ACCEPTED" in
  let completed = only "WORKFLOW_EXECUTION_UPDATE_COMPLETED" in
  let ids event = List.sort_uniq String.compare (update_ids event) in
  if ids accepted <> [update_id] || ids completed <> [update_id] then
    failwith (label ^ ": update events do not name the accepted update ID");
  let position kind = let rec find index = function
      | [] -> failwith (label ^ ": missing " ^ kind)
      | event :: rest -> if event_type event = kind then index else find (index + 1) rest in
    find 0 events in
  if position "WORKFLOW_EXECUTION_UPDATE_ACCEPTED"
     >= position "WORKFLOW_EXECUTION_UPDATE_COMPLETED" then
    failwith (label ^ ": update completed before it was accepted")

(** Returns the worker identity on the last completed workflow task before
    the first event of [kind]: the worker whose task produced that event. *)
let completing_identity label kind events =
  let rec scan identity = function
    | [] -> failwith (label ^ ": missing " ^ kind)
    | event :: rest ->
        let current = event_type event in
        if current = kind then identity
        else if current = "WORKFLOW_TASK_COMPLETED" then
          scan Yojson.Basic.Util.(event
            |> member "workflowTaskCompletedEventAttributes" |> member "identity"
            |> to_string_option) rest
        else scan identity rest in
  scan None events

(** Requires [identity] to be the default identity of worker process [pid],
    which the SDK renders as [<pid>@<hostname>]. *)
let expect_worker label pid identity =
  let prefix = string_of_int pid ^ "@" in
  match identity with
  | Some identity when String.starts_with ~prefix identity -> ()
  | Some identity -> failwith (Printf.sprintf "%s: produced by %S, expected worker %d"
      label identity pid)
  | None -> failwith (label ^ ": no completing worker identity in history")

(** Requires a validator rejection at admission. *)
let expect_rejected label = function
  | Error error when Error.kind error = "update" -> ()
  | Error error -> failwith (label ^ ": wrong rejection: " ^ Error.message error)
  | Ok _ -> failwith (label ^ ": a negative update was accepted")

(** Requires the run to complete with [expected]. *)
let expect_completed label handle expected =
  match get (Client.wait handle) with
  | Client.Completed actual when actual = expected -> ()
  | Client.Completed actual -> failwith (Printf.sprintf "%s: result %d, expected %d" label actual expected)
  | _ -> failwith (label ^ ": run did not complete")

(** Waits for the first workflow task of a run to complete in history. *)
let await_first_task cli address handle =
  await ("first workflow task of " ^ Client.workflow_id handle) (fun () ->
    match history cli address handle with
    | Ok events when count "WORKFLOW_TASK_COMPLETED" events > 0 -> Ok ()
    | Ok _ -> Error "no completed workflow task yet"
    | Error message -> Error message)

(** Registers a just-started run for termination during cleanup and returns
    its handle. [track] receives a monomorphic thunk so runs of different
    workflow types share one cleanup list. *)
let started track handle =
  track (fun () -> ignore (Client.terminate handle));
  handle

(** Re-attaches to an already accepted update by its ID. Temporal must return
    the in-flight update rather than admitting a second one, so the validator
    is not consulted again. *)
let reattach handle = get (Client.start_update ~update_id:"suspended" handle ~update ~input:5 ())

(** Waits for both handles of the recovered update and checks the result. *)
let expect_update_result label handles =
  List.iter (fun pending -> match Client.wait_update pending with
    | Ok 15 -> ()
    | Ok actual -> failwith (Printf.sprintf "%s: update result %d, expected 15" label actual)
    | Error error -> failwith (label ^ ": update failed: " ^ Error.message error)) handles

(** Eviction scenario on a one-slot worker. Every count is per worker process,
    which is the single long-lived worker for this queue. *)
let eviction_scenario ~cli ~address ~client ~queue ~track =
  let label = "eviction" in
  let run = started track (get (Client.start client ~workflow ~task_queue:queue
    ~id:(queue ^ "-target") ~input:10 ())) in
  await_probe "eviction target start" run (render ~value:10 ~body:1 ~validator:0);
  let pending = get (Client.start_update ~update_id:"suspended" run ~update ~input:5 ()) in
  expect_probe "accepted update is suspended" run (render ~value:10 ~body:1 ~validator:1);
  expect_rejected "eviction rejected update"
    (Client.start_update ~update_id:"negative-1" run ~update ~input:(-1) ());
  expect_accepted_only label (read_history cli address run);
  (* The filler's first task can only run once Core has evicted the target. *)
  let filler_run = started track (get (Client.start client ~workflow:filler ~task_queue:queue
    ~id:(queue ^ "-filler") ~input:() ())) in
  await_first_task cli address filler_run;
  (* Answering this query requires replaying the evicted run: the body runs a
     second time, while the accepted update's validator is not re-run. *)
  expect_probe "query after eviction" run (render ~value:10 ~body:2 ~validator:2);
  expect_accepted_only (label ^ " after replay") (read_history cli address run);
  let reattached = reattach run in
  expect_probe "re-attached update" run (render ~value:10 ~body:2 ~validator:2);
  expect_rejected "rejected update after replay"
    (Client.start_update ~update_id:"negative-2" run ~update ~input:(-1) ());
  expect_probe "validator runs for live requests" run (render ~value:10 ~body:2 ~validator:3);
  (* Querying the filler evicts the target again, so the release signal must
     rebuild it and resume the suspended handler from history. *)
  (* The filler registers no query, so a typed handler failure proves a worker
     replayed it and answered, rather than a transport error. *)
  (match Client.query filler_run ~query:probe with
   | Error error when Client.is_query_failed error -> ()
   | Error error -> failwith ("filler query did not reach a worker: " ^ Error.message error)
   | Ok _ -> failwith "filler unexpectedly answered a probe");
  get (Client.signal ~request_id:"release" run ~signal:release ~input:());
  expect_update_result label [pending; reattached];
  expect_probe "after release" run (render ~value:15 ~body:3 ~validator:3);
  get (Client.signal ~request_id:"finish" run ~signal:finish ~input:());
  expect_completed label run 15;
  let events = read_history cli address run in
  expect_completed_once label "suspended" events;
  decisions events

(** Worker-replacement scenario. The first process accepts the update and is
    terminated; a fresh process must rebuild the run from history. [address]
    is the CLI's host:port form and [target_url] the SDK URL for the
    replacement worker. *)
let restart_scenario ~cli ~address ~target_url ~client ~queue ~worker_pid ~track =
  let label = "restart" in
  let run = started track (get (Client.start client ~workflow ~task_queue:queue
    ~id:(queue ^ "-target") ~input:10 ())) in
  await_probe "restart target start" run (render ~value:10 ~body:1 ~validator:0);
  let pending = get (Client.start_update ~update_id:"suspended" run ~update ~input:5 ()) in
  expect_probe "accepted update before restart" run (render ~value:10 ~body:1 ~validator:1);
  expect_rejected "rejected update before restart"
    (Client.start_update ~update_id:"negative-1" run ~update ~input:(-1) ());
  expect_accepted_only label (read_history cli address run);
  let original = Option.get !worker_pid in
  stop original;
  worker_pid := None;
  let replacement = spawn target_url queue "default" in
  worker_pid := Some replacement;
  (* Fresh counters prove the answer comes from a replay in the new process,
     and a zero validator count proves replay did not re-validate. *)
  await_probe "query after restart" run (render ~value:10 ~body:1 ~validator:0);
  expect_accepted_only (label ^ " after replay") (read_history cli address run);
  let reattached = reattach run in
  expect_rejected "rejected update after restart"
    (Client.start_update ~update_id:"negative-2" run ~update ~input:(-1) ());
  expect_probe "validator runs for live requests" run (render ~value:10 ~body:1 ~validator:1);
  get (Client.signal ~request_id:"release" run ~signal:release ~input:());
  expect_update_result label [pending; reattached];
  expect_probe "after release" run (render ~value:15 ~body:1 ~validator:1);
  get (Client.signal ~request_id:"finish" run ~signal:finish ~input:());
  expect_completed label run 15;
  let events = read_history cli address run in
  expect_completed_once label "suspended" events;
  (* The durable history itself shows the hand-over: the original process
     accepted the update and the replacement completed it. *)
  expect_worker "update acceptance" original
    (completing_identity label "WORKFLOW_EXECUTION_UPDATE_ACCEPTED" events);
  expect_worker "update completion" replacement
    (completing_identity label "WORKFLOW_EXECUTION_UPDATE_COMPLETED" events);
  decisions events

(** Control run with only the interactions that produce commands, in the same
    order, with no queries, rejected updates, eviction, or replacement. *)
let control_scenario ~cli ~address ~client ~queue ~track =
  let run = started track (get (Client.start client ~workflow ~task_queue:queue
    ~id:(queue ^ "-control") ~input:10 ())) in
  let pending = get (Client.start_update ~update_id:"suspended" run ~update ~input:5 ()) in
  get (Client.signal ~request_id:"release" run ~signal:release ~input:());
  expect_update_result "control" [pending];
  get (Client.signal ~request_id:"finish" run ~signal:finish ~input:());
  expect_completed "control" run 15;
  let events = read_history cli address run in
  expect_completed_once "control" "suspended" events;
  decisions events

(** Requires a recovered run's decisions to equal the control run's. *)
let expect_same_decisions label control actual =
  if actual <> control then
    failwith (Printf.sprintf "%s decisions differ from the control run:\n  control:\n    %s\n  %s:\n    %s"
      label (String.concat "\n    " control) label (String.concat "\n    " actual))

(** Runs the three scenarios with protected cleanup of workers and runs. *)
let check address cli =
  if not (String.starts_with ~prefix:"http://" address) then
    failwith "this fixture requires an HTTP development server";
  let cli_address = String.sub address 7 (String.length address - 7) in
  let tag = Printf.sprintf "interaction-recovery-%d-%d" (Unix.getpid ())
    (Random.State.bits (Random.State.make_self_init ())) in
  let evict_queue = tag ^ "-evict" and restart_queue = tag ^ "-restart" in
  let evict_pid = ref (Some (spawn address evict_queue "1")) in
  let restart_pid = ref (Some (spawn address restart_queue "default")) in
  Fun.protect ~finally:(fun () -> Option.iter stop !evict_pid; Option.iter stop !restart_pid)
    (fun () ->
      let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
      Fun.protect ~finally:(fun () -> ignore (Client.shutdown client)) (fun () ->
        (* Every started run is terminated on exit; terminating a completed
           run is a harmless typed error. *)
        let cleanup = ref [] in
        let track terminate = cleanup := terminate :: !cleanup in
        Fun.protect ~finally:(fun () -> List.iter (fun terminate -> terminate ()) !cleanup)
          (fun () ->
            let evicted = eviction_scenario ~cli ~address:cli_address ~client
              ~queue:evict_queue ~track in
            print_endline "interaction recovery: cache eviction ok";
            let restarted = restart_scenario ~cli ~address:cli_address ~target_url:address ~client
              ~queue:restart_queue ~worker_pid:restart_pid ~track in
            print_endline "interaction recovery: worker restart ok";
            let control = control_scenario ~cli ~address:cli_address ~client
              ~queue:restart_queue ~track in
            expect_same_decisions "eviction" control evicted;
            expect_same_decisions "restart" control restarted;
            print_endline "interaction recovery live regression: ok")))

(** An overall alarm bounds the driver; protected cleanup reaps the workers.
    The CLI path is explicit so the test cannot invoke an unrelated wrapper. *)
let () = match Array.to_list Sys.argv with
  | [_; "worker"; address; queue; cache] -> worker address queue cache
  | [_; "check"; address; cli] ->
      Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ -> failwith "fixture timed out"));
      ignore (Unix.alarm 170);
      Fun.protect ~finally:(fun () -> ignore (Unix.alarm 0)) (fun () -> check address cli)
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"

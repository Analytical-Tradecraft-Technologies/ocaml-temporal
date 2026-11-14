(** Live regression for [Client.signal_with_start] and
    [Client.update_with_start] (#1151).

    Against a disposable Temporal server this check:

    - signals an absent workflow with [signal_with_start]: a new run is
      started ([started] is [true]) and its history records the signal before
      the first workflow task starts, so the first activation delivers it;
    - signals the same workflow ID while that run is open: the call only
      signals it, returns the same exact run ID, and reports [started] as
      [false]; a retry with the same request ID delivers the signal once;
    - signals it again after the run completed: another run is started, and
      [`Terminate_existing] then replaces that open run with a new one;
    - starts a workflow with [update_with_start]: the handle names the new run
      and the accepted update completes; a second call on the open run only
      admits the update there; a validator rejection, on an open run and on
      an absent workflow, is reported beside the run handle, and the rejected
      start still created its run; a [`Fail] call on the open run is the
      typed already-started error naming that run.

    Every expected run is checked by its exact run ID, and each run's final
    output lists the signals and updates its workflow code received. The
    parent owns and reaps its worker, uses a unique task queue, and
    terminates every execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("with-start: " ^ label)

(** Items the workflow received from signals and accepted updates, newest
    first. *)
let items = Workflow_context.Local.create ()

(** Set by [finish]; an unset flag is false. *)
let finished = Workflow_context.Local.create ()

(** Reads the received items, newest first. *)
let received () = Option.value ~default:[] (get (Workflow_context.Local.get items))

(** Records one received item. *)
let add item = Workflow_context.Local.set items (item :: received ())

(** Appends its string input to the run's items. *)
let deliver = Signal.define ~name:"deliver" ~input:Codec.string

(** Lets the run complete. *)
let finish = Signal.define ~name:"finish" ~input:Codec.unit

(** Appends its input to the run's items and returns how many it now has. *)
let apply = Update.define ~name:"apply" ~input:Codec.string ~output:Codec.int

(** Rejects the input ["reject"] before the handler runs. *)
let validator input =
  if String.equal input "reject" then
    Error (Error.make ~category:`Update ~non_retryable:true ~message:"REJECTED" ())
  else Ok ()

(** Records an accepted update and returns the new item count. *)
let handler input = Result.map (fun () -> List.length (received ())) (add input)

(** Waits for [finish] and completes with every received item in order,
    comma-separated, so the output proves which messages reached this run. *)
let entity =
  Workflow.define ~name:"with-start-entity" ~input:Codec.string ~output:Codec.string
    (fun _ ->
      let open Result_syntax in
      let* () =
        Condition.wait_until (fun () ->
            Option.value ~default:false (get (Workflow_context.Local.get finished)))
      in
      Ok (String.concat "," (List.rev (received ()))))

(** Runs the entity workflow on the test's unique queue until killed. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[]
         ~workflows:
           [
             Worker.workflow entity
               ~signals:
                 [
                   Signal.Handler.make deliver add;
                   Signal.Handler.make finish (fun () ->
                       Workflow_context.Local.set finished true);
                 ]
               ~updates:[ Update.Handler.make ~validator apply handler ];
           ]
         ())
  in
  get (Worker.run worker)

(** Deadline of one CLI history read. *)
let cli_timeout = "30s"

(** Reads one exact run's history event types through the official CLI,
    retrying a failed read a few times: history is append-only, so a retry
    cannot weaken the assertions made about it. *)
let event_types cli address handle =
  let address = String.sub address 7 (String.length address - 7) in
  let args =
    [|
      cli; "--address"; address; "--command-timeout"; cli_timeout; "workflow"; "show";
      "--workflow-id"; Client.workflow_id handle; "--run-id";
      Option.get (Client.run_id handle); "--output"; "json";
    |]
  in
  let read () =
    let input = Unix.open_process_args_in cli args in
    let document =
      match Yojson.Basic.from_channel input with
      | document -> Some document
      | exception Yojson.Json_error _ -> None
    in
    match (Unix.close_process_in input, document) with
    | Unix.WEXITED 0, Some document ->
        Some
          Yojson.Basic.Util.(
            document |> member "events" |> to_list
            |> List.map (fun event -> event |> member "eventType" |> to_string))
    | _ -> None
  in
  let rec attempt remaining =
    match read () with
    | Some types -> types
    | None when remaining > 0 ->
        Unix.sleepf 0.5;
        attempt (remaining - 1)
    | None -> fail "reading a run's history failed"
  in
  attempt 3

(** Requires [handle]'s run to have recorded its signal before its first
    workflow task started, which is what makes signal-with-start deliver the
    signal in the run's first activation. *)
let expect_signal_before_first_task cli address label handle =
  let rec scan = function
    | "EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED" :: _ -> ()
    | "EVENT_TYPE_WORKFLOW_TASK_STARTED" :: _ ->
        fail (label ^ ": the first workflow task started before the signal")
    | _ :: rest -> scan rest
    | [] -> fail (label ^ ": the run recorded no signal")
  in
  match event_types cli address handle with
  | "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED" :: rest -> scan rest
  | _ -> fail (label ^ ": the history does not begin with the start")

(** Requires [handle] to name exactly [run_id] with the given [started]. *)
let expect_run label ~run_id ~started handle =
  if Client.run_id handle <> Some run_id then fail (label ^ ": another run was addressed");
  if Client.started handle <> started then
    fail (Printf.sprintf "%s: started was %b" label (Client.started handle))

(** Lets [handle]'s run finish and requires its output to be [expected]. *)
let expect_finished label handle expected =
  get (Client.signal handle ~signal:finish ~input:());
  match Client.wait ~timeout:(Duration.of_seconds 30L) handle with
  | Ok (Client.Completed { output; _ }) when String.equal output expected -> ()
  | Ok (Client.Completed { output; _ }) ->
      fail (Printf.sprintf "%s: the run received %S, not %S" label output expected)
  | Ok _ -> fail (label ^ ": the run did not complete")
  | Error error -> fail (label ^ ": waiting failed: " ^ Error.message error)

(** Requires an update result to be the validator's typed rejection, whose
    message starts with the validator's own. *)
let expect_rejected label = function
  | Ok _ -> fail (label ^ ": the rejected update was accepted")
  | Error error
    when Error.kind error = "update"
         && String.starts_with ~prefix:"REJECTED" (Error.message error) ->
      ()
  | Error error -> fail (label ^ ": wrong update error: " ^ Error.message error)

(** Runs every check against [address] with one worker process; [cli] is the
    official Temporal CLI used only to read run histories. *)
let check address cli =
  let queue = "with-start-" ^ Temporal_base.Client_request_id.create () in
  let pid =
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "worker"; address; queue |]
      Unix.stdin Unix.stdout Unix.stderr
  in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let cleanups = ref [] in
  let track handle =
    cleanups := (fun () -> ignore (Client.terminate handle)) :: !cleanups;
    handle
  in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun cleanup -> cleanup ()) !cleanups;
      ignore (Client.shutdown client);
      (try Unix.kill pid Sys.sigterm with Unix.Unix_error _ -> ());
      ignore (Unix.waitpid [] pid))
    (fun () ->
      (* Signal-with-start. *)
      let id = queue ^ "-signalled" in
      let signal_with_start ?request_id ?id_conflict_policy item =
        track
          (get
             (Client.signal_with_start client ?request_id ?id_conflict_policy
                ~workflow:entity ~task_queue:queue ~id ~input:"ignored" ~signal:deliver
                ~signal_input:item ()))
      in
      let first = signal_with_start "first" in
      let first_run = Option.get (Client.run_id first) in
      expect_run "absent workflow" ~run_id:first_run ~started:true first;
      expect_signal_before_first_task cli address "absent workflow" first;
      expect_run "open run" ~run_id:first_run ~started:false (signal_with_start "second");
      expect_finished "signalled run" first "first,second";
      let restarted = signal_with_start "third" in
      let restarted_run = Option.get (Client.run_id restarted) in
      if String.equal restarted_run first_run then fail "a closed run was signalled";
      expect_run "closed workflow" ~run_id:restarted_run ~started:true restarted;
      expect_signal_before_first_task cli address "closed workflow" restarted;
      let replaced = signal_with_start ~id_conflict_policy:`Terminate_existing "fourth" in
      let replaced_run = Option.get (Client.run_id replaced) in
      if String.equal replaced_run restarted_run then fail "terminate_existing kept the open run";
      expect_run "terminate_existing" ~run_id:replaced_run ~started:true replaced;
      (match Client.wait ~timeout:(Duration.of_seconds 30L) restarted with
      | Ok (Client.Terminated _) -> ()
      | Ok _ | Error _ -> fail "terminate_existing did not terminate the open run");
      let request_id = queue ^ "-retried" in
      expect_run "request ID" ~run_id:replaced_run ~started:false
        (signal_with_start ~request_id "once");
      expect_run "request ID retry" ~run_id:replaced_run ~started:false
        (signal_with_start ~request_id "once");
      expect_finished "replacement run" replaced "fourth,once";
      (* Update-with-start. *)
      let id = queue ^ "-updated" in
      let update_with_start ?(id = id) ~id_conflict_policy item =
        Client.update_with_start client ~id_conflict_policy ~workflow:entity
          ~task_queue:queue ~id ~input:"ignored" ~update:apply ~update_input:item ()
      in
      let started, accepted =
        get (update_with_start ~id_conflict_policy:`Use_existing "one")
      in
      let started = track started in
      let updated_run = Option.get (Client.run_id started) in
      expect_run "update-with-start" ~run_id:updated_run ~started:true started;
      if get (Client.wait_update (get accepted)) <> 1 then fail "the first update was lost";
      let attached, accepted =
        get (update_with_start ~id_conflict_policy:`Use_existing "two")
      in
      expect_run "update on the open run" ~run_id:updated_run ~started:false attached;
      if get (Client.wait_update (get accepted)) <> 2 then fail "the second update was lost";
      let rejected_on_open, rejection =
        get (update_with_start ~id_conflict_policy:`Use_existing "reject")
      in
      expect_run "rejection on the open run" ~run_id:updated_run ~started:false
        rejected_on_open;
      expect_rejected "rejection on the open run" rejection;
      (match update_with_start ~id_conflict_policy:`Fail "three" with
      | Ok _ -> fail "a fail-policy update-with-start used the open run"
      | Error error -> (
          match Client.already_started error with
          | Some { run_id; _ } when String.equal run_id updated_run -> ()
          | Some _ | None -> fail ("fail policy: wrong error: " ^ Error.message error)));
      expect_finished "updated run" started "one,two";
      let fresh, rejection =
        get
          (update_with_start ~id:(queue ^ "-rejected") ~id_conflict_policy:`Use_existing
             "reject")
      in
      let fresh = track fresh in
      expect_run "rejection on a new run" ~run_id:(Option.get (Client.run_id fresh))
        ~started:true fresh;
      expect_rejected "rejection on a new run" rejection;
      expect_finished "rejected update's run" fresh "";
      print_endline "with-start live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address; cli ] -> check address cli
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"

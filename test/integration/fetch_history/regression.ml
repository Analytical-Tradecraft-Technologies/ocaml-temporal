(** Live regression for [Client.fetch_history] (#1137).

    A replay gate used to need the Temporal CLI's JSON export plus a
    protobuf conversion in Python. Against a disposable Temporal server this
    check runs a workflow that records several durable timers, then fetches
    its history with the SDK alone and replays it with [Temporal.Replay]:

    - while the run is still open, the events recorded so far replay;
    - after it completes, the complete history fetched with the server's
      default page size, with one event per page and with three events per
      page all replay, so a multi-page history is reassembled exactly;
    - a current-run handle from [Client.get_handle] fetches the same run;
    - a variant registered under the same name that changes only the code
      after [finish] replays the open run's history but fails every
      complete history with [Nondeterminism], so each fetch, whatever its
      page size, includes the events of the last pages: a replay of a
      truncated prefix alone would still succeed;
    - an unknown workflow ID is a [Not_found] RPC error.

    The parent owns and reaps its worker, uses a unique task queue, and
    terminates every execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("fetch history: " ^ label)

(** Durable timers the fixture records before and after [finish]. *)
let timers_per_phase = 3

(** Signal that lets the fixture record its second phase and complete. *)
let finish = Signal.define ~name:"finish" ~input:Codec.unit

(** Set by [finish]; an unset flag is false. *)
let finished = Workflow_context.Local.create ()

(** Set once the first phase's timers have fired. *)
let first_phase_done = Workflow_context.Local.create ()

(** Reads a workflow-local boolean flag; an unset flag is false. *)
let flag key =
  match Workflow_context.Local.get key with Ok (Some true) -> true | Ok _ | Error _ -> false

(** Reads the [finished] flag. *)
let is_finished () = flag finished

(** Reports whether the first phase's timers have all fired. *)
let first_phase = Query.define ~name:"first-phase" ~output:Codec.bool

(** Sleeps [count] times for 10 ms, recording one durable timer each. *)
let rec sleeps count =
  if count = 0 then Ok ()
  else Result.bind (Workflow.sleep (Duration.of_ms 10L)) (fun () -> sleeps (count - 1))

(** Name shared by the fixture and its incompatible replacement. *)
let fixture_name = "fetch-history"

(** Records [timers_per_phase] timers, waits for [finish], records as many
    again, and completes with its input. Every phase adds several events,
    so the completed history spans many one-event pages. *)
let fixture =
  Workflow.define ~name:fixture_name ~input:Codec.string ~output:Codec.string
    (fun input ->
      let open Result_syntax in
      let* () = sleeps timers_per_phase in
      let* () = Workflow_context.Local.set first_phase_done true in
      let* () = Condition.wait_until is_finished in
      let* () = sleeps timers_per_phase in
      Ok input)

(** The same workflow type with the fixture's first phase but no timers
    after [finish]: a history that stops before the second phase replays
    against it, and one that contains the second phase diverges at its first
    timer. *)
let changed_tail =
  Workflow.define ~name:fixture_name ~input:Codec.string ~output:Codec.string
    (fun input ->
      let open Result_syntax in
      let* () = sleeps timers_per_phase in
      let* () = Workflow_context.Local.set first_phase_done true in
      let* () = Condition.wait_until is_finished in
      Ok input)

(** Signal handler registration shared by the worker and every replay. *)
let signals = [ Signal.Handler.make finish (fun () -> Workflow_context.Local.set finished true) ]

(** Query handler registration of the worker. *)
let queries = [ Query.Handler.make first_phase (fun () -> Ok (flag first_phase_done)) ]

(** Runs the fixture on the test's unique queue until killed. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[] ~workflows:[ Worker.workflow fixture ~signals ~queries ] ())
  in
  get (Worker.run worker)

(** Replays [history] against [workflow] under the worker's namespace and
    queue. *)
let replay ~queue workflow history =
  Replay.replay ~namespace:"default" ~task_queue:queue
    ~workflows:[ Replay.workflow workflow ~signals ]
    history

(** Fetches [handle]'s history with [page_size] and requires it to replay
    against the fixture. With [~complete:true] it must also hold the second
    phase, so [changed_tail] diverges from it; otherwise [changed_tail]
    replays it too. *)
let expect_replays label ~queue ~complete ?page_size handle =
  let history = get (Client.fetch_history ?page_size handle) in
  if Replay.History.workflow_id history <> Client.workflow_id handle then
    fail (label ^ ": the history has another workflow ID");
  (match replay ~queue fixture history with
  | Ok () -> ()
  | Error failure -> fail (label ^ ": " ^ Replay.failure_message failure));
  match (complete, replay ~queue changed_tail history) with
  | true, Error (Replay.Nondeterminism _) | false, Ok () -> ()
  | true, Ok () -> fail (label ^ ": the history lacks the events after finish")
  | _, Error failure ->
      fail (label ^ ": the changed tail failed with " ^ Replay.failure_message failure)

(** Retries [attempt] for up to 30 s, for state that appears asynchronously. *)
let await label attempt =
  let deadline = Unix.gettimeofday () +. 30. in
  let rec poll () =
    if attempt () then ()
    else if Unix.gettimeofday () < deadline then (
      Unix.sleepf 0.2;
      poll ())
    else fail ("timed out waiting for " ^ label)
  in
  poll ()

(** Runs every check against [address] with one worker process. *)
let check address =
  let queue = "fetch-history-" ^ Temporal_base.Client_request_id.create () in
  let pid =
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "worker"; address; queue |]
      Unix.stdin Unix.stdout Unix.stderr
  in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let cleanups = ref [] in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun cleanup -> cleanup ()) !cleanups;
      ignore (Client.shutdown client);
      (try Unix.kill pid Sys.sigterm with Unix.Unix_error _ -> ());
      ignore (Unix.waitpid [] pid))
    (fun () ->
      let handle =
        get
          (Client.start client ~workflow:fixture ~task_queue:queue
             ~id:(queue ^ "-run") ~input:"done" ())
      in
      cleanups := (fun () -> ignore (Client.terminate handle)) :: !cleanups;
      (* Once the first phase's timers have fired the open run's history
         holds them; the events recorded so far must replay. *)
      await "the first phase" (fun () ->
          match Client.query handle ~query:first_phase with
          | Ok done_ -> done_
          | Error _ -> false);
      expect_replays "open run" ~queue ~complete:false handle;
      get (Client.signal handle ~signal:finish ~input:());
      (match Client.wait ~timeout:(Duration.of_seconds 30L) handle with
      | Ok (Client.Completed { output = "done"; _ }) -> ()
      | Ok _ -> fail "the fixture did not complete with its input"
      | Error error -> fail ("waiting for the fixture failed: " ^ Error.message error));
      expect_replays "default page size" ~queue ~complete:true handle;
      expect_replays "one event per page" ~queue ~complete:true ~page_size:1 handle;
      expect_replays "three events per page" ~queue ~complete:true ~page_size:3 handle;
      let current =
        get (Client.get_handle client ~workflow:fixture ~id:(Client.workflow_id handle) ())
      in
      expect_replays "current-run handle" ~queue ~complete:true current;
      let missing =
        get (Client.get_handle client ~workflow:fixture ~id:(queue ^ "-missing") ())
      in
      (match Client.fetch_history missing with
      | Error error when Client.rpc_status error = Some `Not_found -> ()
      | Error error -> fail ("an unknown workflow failed with " ^ Error.message error)
      | Ok _ -> fail "an unknown workflow returned a history");
      print_endline "fetch history live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address ] -> check address
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233"

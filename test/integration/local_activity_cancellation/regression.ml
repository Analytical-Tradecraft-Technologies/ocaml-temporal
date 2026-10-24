(** Live local-retry cancellation and fresh-worker history replay, through
    the public local-activity cancellation API (#1156): an explicit
    [Activity.start_local_handle] cancelled with [Activity.cancel] for each
    Core cancellation policy, and an [Activity.start_local ~scope] whose scope
    is cancelled. Client, worker, signal, and query paths use the public API. *)
open Temporal

(** Reports setup and client failures through their public diagnostic. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Each execution retains its own cancellation operation and query result. *)
let cancellation = Workflow_context.Local.create ()
let outcome = Workflow_context.Local.create ()

(** Converts recorded input to a public cancellation policy. The ["scope"]
    execution keeps the local default, [Wait_cancellation_completed]. *)
let policy = function
  | "try" -> Some Activity.Try_cancel
  | "wait" -> Some Activity.Wait_cancellation_completed
  | "abandon" -> Some Activity.Abandon
  | "scope" -> None
  | _ -> failwith "unknown cancellation policy"

(** A delay longer than Core's local threshold must become a durable timer. *)
let retry_policy =
  get
    (Retry_policy.make ~initial_interval:(Duration.of_ms 120_000L)
       ~backoff_coefficient:1.0 ~maximum_interval:(Duration.of_ms 120_000L)
       ~maximum_attempts:3 ())

(** The signal arrives only after the driver sees a committed backoff timer. *)
let cancel_signal = Signal.define ~name:"cancel-local" ~input:Codec.unit
let result_query = Query.define ~name:"cancellation-result" ~output:Codec.string

(** The workflow's view of the worker-registered activity below. *)
let failing_activity =
  Activity.remote ~name:"fail-for-local-backoff" ~input:Codec.string ~output:Codec.unit

(** Repeated cancellation must remain harmless before and after resolution. *)
let cancel () =
  let cancel = Option.get (get (Workflow_context.Local.get cancellation)) in
  get (cancel ());
  get (cancel ());
  Ok ()

(** Queries read the terminal state reconstructed by the workflow on replay. *)
let read () =
  Result.map (Option.value ~default:"pending") (Workflow_context.Local.get outcome)

(** Waits on the original local activity future, not a separate cancel
    future. A policy name schedules through [start_local_handle] and keeps
    [Activity.cancel] for the signal; ["scope"] starts inside a scope and
    keeps [Scope.cancel] instead. Both are invoked twice by the signal, and
    once more after resolution, to prove repeated cancellation is harmless. *)
let workflow = Workflow.define ~name:"local-backoff-cancellation"
  ~input:Codec.string ~output:Codec.string (fun name ->
    let start_to_close_timeout = Duration.of_ms 30_000L in
    let future, cancel_operation =
      match policy name with
      | Some cancellation_type ->
          let handle =
            Activity.start_local_handle ~start_to_close_timeout ~retry_policy
              ~cancellation_type failing_activity name
          in
          (Activity.future handle, fun () -> Activity.cancel handle)
      | None ->
          let scope = get (Scope.create ()) in
          ( Activity.start_local ~scope ~start_to_close_timeout ~retry_policy
              failing_activity name,
            fun () -> Scope.cancel scope )
    in
    get (Workflow_context.Local.set cancellation cancel_operation);
    (match Future.await future with
    | Error error when Error.kind error = "cancelled" -> ()
    | Error error -> failwith (Error.message error)
    | Ok () -> failwith "cancelled activity unexpectedly succeeded");
    get (cancel_operation ());
    get (Workflow_context.Local.set outcome "cancelled");
    Ok "cancelled")

(** Activity I/O records real attempts; replay must never invoke this callback. *)
let activity log = Activity.define ~name:"fail-for-local-backoff"
  ~input:Codec.string ~output:Codec.unit (fun name ->
    let output = open_out_gen [Open_wronly; Open_append; Open_creat; Open_text] 0o600 log in
    Fun.protect ~finally:(fun () -> close_out output)
      (fun () -> output_string output (name ^ "\n"));
    Error (Error.make ~category:`Activity ~message:"retryable fixture failure" ()))

(** One isolated process serves all fixture executions on a unique task queue. *)
let worker address queue log =
  let worker = get (Worker.create ~target_url:address ~namespace:"default"
    ~task_queue:queue ~activities:[Worker.activity (activity log)]
    ~workflows:[Worker.workflow workflow
      ~signals:[Signal.Handler.make cancel_signal cancel]
      ~queries:[Query.Handler.make result_query read]] ()) in
  get (Worker.run worker)

(** Process replacement discards every cached continuation before replay. *)
let spawn address queue log = Unix.create_process Sys.executable_name
  [|Sys.executable_name; "worker"; address; queue; log|]
  Unix.stdin Unix.stdout Unix.stderr

(** Stops and reaps only the worker created by this fixture. *)
let stop pid = Unix.kill pid Sys.sigterm; ignore (Unix.waitpid [] pid)

(** Deadline for one CLI invocation: a fresh dial plus one history read. The CLI
    reports an expired [--command-timeout] only as "program interrupted", and
    the original 5 seconds was exceeded on a loaded CI runner. The fixture's
    120-second alarm and the controller's outer timeout still bound the run. *)
let cli_timeout = "30s"

(** Names a CLI exit status for failure diagnostics. *)
let describe_status = function
  | Unix.WEXITED code -> Printf.sprintf "exited with status %d" code
  | Unix.WSIGNALED signal -> Printf.sprintf "killed by signal %d" signal
  | Unix.WSTOPPED signal -> Printf.sprintf "stopped by signal %d" signal

(** Fetches this execution's history through the official CLI without a shell.
    A failed invocation is an [Error] naming its exit status rather than an
    exception from cleanup, so callers decide whether the read may be retried.
    The CLI's own diagnostic goes to the inherited standard error. *)
let history cli address handle =
  let args = [|cli; "--address"; address; "--command-timeout"; cli_timeout;
    "workflow"; "show"; "--workflow-id"; Client.workflow_id handle;
    "--run-id"; Option.get (Client.run_id handle); "--output"; "json"|] in
  let input = Unix.open_process_args_in cli args in
  let document = match Yojson.Basic.from_channel input with
    | document -> Ok document
    | exception Yojson.Json_error message -> Error message
    | exception exn ->
        (* An alarm or other defect still reaps the child before propagating. *)
        ignore (Unix.close_process_in input);
        raise exn in
  match Unix.close_process_in input, document with
  | Unix.WEXITED 0, Ok document ->
      Ok Yojson.Basic.Util.(document |> member "events" |> to_list)
  | Unix.WEXITED 0, Error message -> Error ("unparsable history: " ^ message)
  | status, _ -> Error ("history fetch " ^ describe_status status)

(** Selects durable event kinds without depending on timestamps or event IDs. *)
let events kind history = List.filter (fun event ->
  Yojson.Basic.Util.(event |> member "eventType" |> to_string) = kind) history

(** Waits for server evidence of the retry timer, avoiding signal timing races.
    Polling is observation rather than assertion, so a failed read is reported
    and retried until the deadline (seconds) expires. *)
let await_backoff cli address handle ~seconds =
  let deadline = Unix.gettimeofday () +. seconds in
  let rec poll () =
    let result = history cli address handle in
    match result with
    | Ok recorded when events "EVENT_TYPE_TIMER_STARTED" recorded <> [] -> ()
    | Ok _ | Error _ when Unix.gettimeofday () < deadline ->
        Result.iter_error (Printf.eprintf "%s: %s; retrying\n%!"
          (Client.workflow_id handle)) result;
        Unix.sleepf 0.05;
        poll ()
    | Ok _ -> failwith "local retry never created a durable timer"
    | Error message -> failwith ("awaiting the retry timer: " ^ message) in
  poll ()

(** Reads a completed execution's history. Completed history is immutable, so
    retrying a failed read tolerates transport failures without weakening any
    assertion made about the events themselves. *)
let completed_history cli address handle =
  let rec fetch attempts =
    match history cli address handle with
    | Ok recorded -> recorded
    | Error message when attempts > 1 ->
        Printf.eprintf "%s: %s; retrying\n%!" (Client.workflow_id handle) message;
        Unix.sleepf 0.5;
        fetch (attempts - 1)
    | Error message -> failwith ("reading completed history: " ^ message) in
  fetch 3

(** Requires a cancelled retry timer, one failed attempt marker, and completion. *)
let verify_history cli address handle =
  let recorded = completed_history cli address handle in
  let one kind = match events kind recorded with
    | [event] -> event | _ -> failwith ("expected exactly one " ^ kind) in
  let started = one "EVENT_TYPE_TIMER_STARTED" in
  let cancelled = one "EVENT_TYPE_TIMER_CANCELED" in
  let marker = one "EVENT_TYPE_MARKER_RECORDED" in
  let field event attributes name = Yojson.Basic.Util.(event |> member attributes |> member name) in
  if field started "timerStartedEventAttributes" "timerId"
     <> field cancelled "timerCanceledEventAttributes" "timerId" then
    failwith "cancelled the wrong timer";
  if field marker "markerRecordedEventAttributes" "markerName" <> `String "core_local_activity" then
    failwith "missing local activity marker";
  ignore (one "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED");
  if events "EVENT_TYPE_TIMER_FIRED" recorded <> []
     || events "EVENT_TYPE_WORKFLOW_TASK_FAILED" recorded <> [] then
    failwith "retry fired or a workflow task failed";
  Printf.printf "%s: one local attempt marker, cancelled retry timer, completed\n%!"
    (Client.workflow_id handle)

(** Confirms that neither cancellation nor replacement-worker replay ran retries. *)
let verify_attempts log =
  let input = open_in log in
  let rec lines result = match input_line input with
    | line -> lines (line :: result) | exception End_of_file -> result in
  let actual = Fun.protect ~finally:(fun () -> close_in input) (fun () -> lines []) in
  if List.sort String.compare actual <> ["abandon"; "scope"; "try"; "wait"] then
    failwith "unexpected local activity attempts"

(** Runs all policies, replaces the worker, and reconstructs terminal query state. *)
let check address cli =
  if not (String.starts_with ~prefix:"http://" address) then
    failwith "this fixture requires an HTTP development server";
  let cli_address = String.sub address 7 (String.length address - 7) in
  let queue = Printf.sprintf "local-cancel-%d-%d" (Unix.getpid ())
    (Random.State.bits (Random.State.make_self_init ())) in
  let log = Filename.temp_file "local-cancel-attempts-" ".log" in
  let pid = ref (Some (spawn address queue log)) in
  Fun.protect ~finally:(fun () -> Option.iter stop !pid; Sys.remove log) (fun () ->
    let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
    Fun.protect ~finally:(fun () -> ignore (Client.shutdown client)) (fun () ->
      let handles = ref [] in
      Fun.protect ~finally:(fun () -> List.iter (fun handle ->
        ignore (Client.terminate handle)) !handles) (fun () ->
        List.iter (fun name ->
          let handle = get (Client.start client ~workflow ~task_queue:queue
            ~id:(queue ^ "-" ^ name) ~input:name ()) in
          handles := handle :: !handles;
          await_backoff cli cli_address handle ~seconds:60.0;
          get (Client.signal handle ~signal:cancel_signal ~input:());
          (match get (Client.wait handle) with
          | Client.Completed { output = "cancelled"; _ } -> () | _ -> failwith "cancellation did not settle");
          verify_history cli cli_address handle) ["try"; "wait"; "abandon"; "scope"];
        verify_attempts log;
        Option.iter stop !pid;
        pid := None;
        pid := Some (spawn address queue log);
        List.iter (fun handle ->
          if get (Client.query handle ~query:result_query) <> "cancelled" then
            failwith "replayed cancellation result changed") !handles;
        verify_attempts log;
        print_endline "local activity cancellation: all policies, scope, and fresh-worker replay passed")))

(** An overall alarm bounds client waits; protected cleanup reaps the worker. *)
let () = match Array.to_list Sys.argv with
  | [_; "worker"; address; queue; log] -> worker address queue log
  | [_; "check"; address; cli] ->
      Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ -> failwith "fixture timed out"));
      ignore (Unix.alarm 120);
      Fun.protect ~finally:(fun () -> ignore (Unix.alarm 0)) (fun () -> check address cli)
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"

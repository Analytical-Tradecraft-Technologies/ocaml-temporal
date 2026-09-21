(** Live local-retry cancellation and fresh-worker history replay. Explicit
    local handles are private today, so only scheduling/cancellation uses the
    runtime adapter; client, worker, signal, and query paths use the public API. *)
open Temporal

module Context = Temporal_runtime.Workflow_context_store
module Activation = Temporal_runtime.Activation

(** Reports setup and client failures through their public diagnostic. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Preserves the private runtime diagnostic at the fixture adapter boundary. *)
let get_base = function
  | Ok value -> value
  | Error error -> failwith (Temporal_base.Error.message error)

(** Each execution retains its own cancellation operation and query result. *)
let cancellation = Workflow_context.Local.create ()
let outcome = Workflow_context.Local.create ()

(** Converts recorded input to the three Core cancellation policies. *)
let policy = function
  | "try" -> Activation.Try_cancel
  | "wait" -> Activation.Wait_cancellation_completed
  | "abandon" -> Activation.Abandon
  | _ -> failwith "unknown cancellation policy"

(** A delay longer than Core's local threshold must become a durable timer. *)
let retry_policy = {
  Activation.initial_interval = 120_000L;
  maximum_interval = 120_000L;
  backoff_coefficient_bits = Int64.to_string (Int64.bits_of_float 1.0);
  maximum_attempts = 3;
  non_retryable_error_types = [];
}

(** The signal arrives only after the driver sees a committed backoff timer. *)
let cancel_signal = Signal.define ~name:"cancel-local" ~input:Codec.unit
let result_query = Query.define ~name:"cancellation-result" ~output:Codec.string

(** Repeated cancellation must remain harmless before and after resolution. *)
let cancel () =
  let cancel = Option.get (get (Workflow_context.Local.get cancellation)) in
  get_base (cancel ());
  get_base (cancel ());
  Ok ()

(** Queries read the terminal state reconstructed by the workflow on replay. *)
let read () =
  Result.map (Option.value ~default:"pending") (Workflow_context.Local.get outcome)

(** Waits on the original local activity future, not a separate cancel future. *)
let workflow = Workflow.define ~name:"local-backoff-cancellation"
  ~input:Codec.string ~output:Codec.string (fun name ->
    let context = Option.get (Context.current ()) in
    let future, cancel_operation = Context.schedule_local_activity context
      ~name:"fail-for-local-backoff"
      ~input:(get_base (Temporal_base.Codec.encode Temporal_base.Codec.string name))
      ~start_to_close_timeout:30_000L ~retry_policy ~cancellation_type:(policy name)
      ~decode:(Temporal_base.Codec.decode Temporal_base.Codec.unit) () in
    get (Workflow_context.Local.set cancellation cancel_operation);
    (match Temporal_runtime.Future_store.await future with
    | Error error when (Temporal_base.Error.view error).category = `Cancelled -> ()
    | Error error -> failwith (Temporal_base.Error.message error)
    | Ok () -> failwith "cancelled activity unexpectedly succeeded");
    get_base (cancel_operation ());
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

(** Fetches this execution's history through the official CLI without a shell. *)
let history cli address handle =
  let args = [|cli; "--address"; address; "--command-timeout"; "5s";
    "workflow"; "show"; "--workflow-id"; Client.workflow_id handle;
    "--run-id"; Client.run_id handle; "--output"; "json"|] in
  let input = Unix.open_process_args_in cli args in
  let document = Fun.protect
    ~finally:(fun () -> match Unix.close_process_in input with
      | Unix.WEXITED 0 -> () | _ -> failwith "history fetch failed")
    (fun () -> Yojson.Basic.from_channel input) in
  Yojson.Basic.Util.(document |> member "events" |> to_list)

(** Selects durable event kinds without depending on timestamps or event IDs. *)
let events kind history = List.filter (fun event ->
  Yojson.Basic.Util.(event |> member "eventType" |> to_string) = kind) history

(** Waits for server evidence of the retry timer, avoiding signal timing races. *)
let rec await_backoff cli address handle remaining =
  let recorded = history cli address handle in
  if events "EVENT_TYPE_TIMER_STARTED" recorded <> [] then ()
  else if remaining = 0 then failwith "local retry never created a durable timer"
  else (Unix.sleepf 0.05; await_backoff cli address handle (remaining - 1))

(** Requires a cancelled retry timer, one failed attempt marker, and completion. *)
let verify_history cli address handle =
  let recorded = history cli address handle in
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
  if List.sort String.compare actual <> ["abandon"; "try"; "wait"] then
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
          await_backoff cli cli_address handle 200;
          get (Client.signal handle ~signal:cancel_signal ~input:());
          (match get (Client.wait handle) with
          | Client.Completed "cancelled" -> () | _ -> failwith "cancellation did not settle");
          verify_history cli cli_address handle) ["try"; "wait"; "abandon"];
        verify_attempts log;
        Option.iter stop !pid;
        pid := None;
        pid := Some (spawn address queue log);
        List.iter (fun handle ->
          if get (Client.query handle ~query:result_query) <> "cancelled" then
            failwith "replayed cancellation result changed") !handles;
        verify_attempts log;
        print_endline "local activity cancellation: all policies and fresh-worker replay passed")))

(** An overall alarm bounds client waits; protected cleanup reaps the worker. *)
let () = match Array.to_list Sys.argv with
  | [_; "worker"; address; queue; log] -> worker address queue log
  | [_; "check"; address; cli] ->
      Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ -> failwith "fixture timed out"));
      ignore (Unix.alarm 120);
      Fun.protect ~finally:(fun () -> ignore (Unix.alarm 0)) (fun () -> check address cli)
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"

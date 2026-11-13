(** Live regression for cron schedules and delayed starts on [Client.start]
    and in OCaml workers (#1158).

    Against a disposable Temporal server this check:

    - starts a run with [~start_delay] and requires its first workflow task
      to run no earlier than the delay after the start: workflow code reads
      the delay from [Workflow.Info.first_workflow_task_backoff], and its
      first [Workflow.now] is at least that much later than
      [Workflow.Info.start_time];
    - does the same through [Client.signal_with_start ~start_delay], whose
      signal is delivered with the delayed first task;
    - starts a cron chain with [Client.start ~cron_schedule] and requires the
      first run to report the schedule and no predecessor, its exact-run
      wait to complete with the next run as successor, and that second run
      to report [`Cron_schedule] from [Workflow.Info.continuation_initiator]
      and the first run as its predecessor; a current-run wait on the chain
      must not end, and the chain is then terminated;
    - starts a second cron chain through the official Temporal CLI
      ([temporal workflow start --cron]) and requires the same of its first
      two runs;
    - requires Temporal to reject an invalid cron expression with an
      [`Invalid_argument] RPC error; and
    - replays the recorded histories of the delayed run and of both runs of
      the client-started cron chain.

    The parent owns and reaps its worker, uses a unique task queue, and
    terminates every chain it starts, also after an assertion failure. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("cron/start delay: " ^ label)

(** The cron schedule both chains use: a run every two seconds, so the
    first two runs of a chain close within a few seconds. *)
let schedule = "@every 2s"

(** The start delay of the delayed runs, in milliseconds. *)
let delay_ms = 3_000L

(** Delivered by signal-with-start; the [signalled] run waits for it. *)
let deliver = Signal.define ~name:"deliver" ~input:Codec.unit

(** Set by [deliver]; an unset flag is false. *)
let delivered = Workflow_context.Local.create ()

(** Reads the [delivered] flag. *)
let is_delivered () =
  match Workflow_context.Local.get delivered with Ok (Some true) -> true | Ok _ | Error _ -> false

(** Milliseconds from [earlier] to [later], negative when [later] precedes
    it. *)
let milliseconds_between earlier later =
  Int64.add
    (Int64.mul (Int64.sub (Time.seconds later) (Time.seconds earlier)) 1_000L)
    (Int64.of_int ((Time.nanoseconds later - Time.nanoseconds earlier) / 1_000_000))

(** Describes the current run as
    [label|cron schedule|backoff ms|initiator|predecessor|lag ms], with [-]
    for an absent value. The lag is the first activation's workflow time
    minus [Workflow.Info.start_time]. *)
let observe label =
  let open Result_syntax in
  let* info = Workflow.info () in
  let* now = Workflow.now () in
  let module Info = Workflow.Info in
  let initiator =
    match Info.continuation_initiator info with
    | None -> "-"
    | Some `Cron_schedule -> "cron"
    | Some `Retry -> "retry"
    | Some `Continue_as_new -> "continue-as-new"
  in
  let lag =
    match Info.start_time info with
    | None -> "-"
    | Some start -> Int64.to_string (milliseconds_between start now)
  in
  Ok
    (String.concat "|"
       [ label;
         Option.value (Info.cron_schedule info) ~default:"-";
         (match Info.first_workflow_task_backoff info with
         | None -> "-"
         | Some backoff -> Int64.to_string (Duration.to_ms backoff));
         initiator;
         Option.value (Info.continued_from_run_id info) ~default:"-";
         lag ])

(** Returns the observation of its first activation. The [signalled] input
    first waits for [deliver], which signal-with-start delivers with that
    activation. *)
let fixture =
  Workflow.define ~name:"cron-start-delay" ~input:Codec.string ~output:Codec.string
    (fun label ->
      let open Result_syntax in
      let* observation = observe label in
      let* () =
        if String.equal label "signalled" then Condition.wait_until is_delivered else Ok ()
      in
      Ok observation)

(** Signal registration of the worker. *)
let signals = [ Signal.Handler.make deliver (fun () -> Workflow_context.Local.set delivered true) ]

(** Runs the fixture on [queue] until killed. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[]
         ~workflows:[ Worker.workflow fixture ~signals ]
         ())
  in
  get (Worker.run worker)

(** Runs the official CLI against the server and returns its standard output
    and whether it exited successfully. *)
let cli_run cli address args =
  let address = String.sub address 7 (String.length address - 7) in
  let argv = Array.of_list (cli :: "--address" :: address :: args) in
  let channel = Unix.open_process_args_in cli argv in
  let output = In_channel.input_all channel in
  (output, Unix.close_process_in channel = Unix.WEXITED 0)

(** Splits an observation into its six fields, after printing it as
    evidence in the suite log. *)
let fields label observation =
  Printf.printf "%s: %s\n%!" label observation;
  match String.split_on_char '|' observation with
  | [ _; cron; backoff; initiator; predecessor; lag ] -> (cron, backoff, initiator, predecessor, lag)
  | _ -> fail (Printf.sprintf "%s returned a malformed observation %S" label observation)

(** Waits for the exact run [handle] to complete and returns its output and
    successor. *)
let completed label handle =
  match Client.wait ~timeout:(Duration.of_seconds 60L) handle with
  | Ok (Client.Completed { output; successor }) -> (output, successor)
  | Ok _ -> fail (label ^ " did not complete")
  | Error error -> fail ("waiting for " ^ label ^ " failed: " ^ Error.message error)

(** Requires a delayed run's observation: no cron schedule or predecessor, a
    recorded backoff equal to the delay, and a first activation at least the
    delay after the start time. *)
let expect_delayed label observation =
  let cron, backoff, initiator, predecessor, lag = fields label observation in
  if cron <> "-" || initiator <> "-" || predecessor <> "-" then
    fail (Printf.sprintf "%s observed cron/predecessor metadata: %s" label observation);
  if backoff <> Int64.to_string delay_ms then
    fail (Printf.sprintf "%s observed backoff %s, expected %Ld" label backoff delay_ms);
  match Int64.of_string_opt lag with
  | Some lag when Int64.compare lag delay_ms >= 0 -> ()
  | _ -> fail (Printf.sprintf "%s ran its first task too early: %s" label observation)

(** Checks the first two runs of the cron chain whose first run is [first]:
    the first run reports the schedule, a backoff and no predecessor; its
    exact wait completes with the second run as successor; the second run
    reports the schedule, [`Cron_schedule] and the first run. Returns the
    exact handle of the second run. *)
let expect_cron_chain client label first =
  let first_output, successor = completed (label ^ " first run") first in
  let cron, backoff, initiator, predecessor, _ = fields label first_output in
  if cron <> schedule || backoff = "-" || initiator <> "-" || predecessor <> "-" then
    fail (Printf.sprintf "%s first run observed %s" label first_output);
  let successor =
    match successor with
    | Some successor -> successor
    | None -> fail (label ^ " first run completed without a scheduled successor")
  in
  let second = get (Client.follow client ~workflow:fixture successor) in
  let second_output, next = completed (label ^ " second run") second in
  let cron, _, initiator, predecessor, _ = fields label second_output in
  let first_run = Option.get (Client.run_id first) in
  if cron <> schedule || initiator <> "cron" || predecessor <> first_run then
    fail (Printf.sprintf "%s second run observed %s" label second_output);
  if Option.is_none next then fail (label ^ " second run completed without a successor");
  second

(** Requires the closed run [handle] to replay against the fixture. *)
let expect_replay handle label queue =
  let history = get (Client.fetch_history handle) in
  match
    Replay.replay ~namespace:"default" ~task_queue:queue
      ~workflows:[ Replay.workflow fixture ~signals ]
      history
  with
  | Ok () -> ()
  | Error failure -> fail (label ^ " did not replay: " ^ Replay.failure_message failure)

(** Extracts the [runId] member of the CLI's JSON start output. *)
let cli_run_id output =
  let key = "\"runId\"" in
  let length = String.length output in
  let rec find index =
    if index + String.length key > length then fail ("no run ID in CLI output: " ^ output)
    else if String.sub output index (String.length key) = key then index + String.length key
    else find (index + 1)
  in
  let after_key = find 0 in
  let opening = String.index_from output after_key '"' in
  let closing = String.index_from output (opening + 1) '"' in
  String.sub output (opening + 1) (closing - opening - 1)

(** Runs every check against [address] with one worker; [cli] is the
    official Temporal CLI used for the CLI cron start. *)
let check address cli =
  let base = "cron-start-delay-" ^ Temporal_base.Client_request_id.create () in
  let queue = base ^ "-queue" in
  let worker_pid =
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "worker"; address; queue |]
      Unix.stdin Unix.stdout Unix.stderr
  in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let ids =
    [ base ^ "-delayed"; base ^ "-signalled"; base ^ "-cron"; base ^ "-cli-cron"; base ^ "-invalid" ]
  in
  let id suffix = base ^ "-" ^ suffix in
  Fun.protect
    ~finally:(fun () ->
      List.iter
        (fun id ->
          match Client.get_handle client ~workflow:fixture ~id () with
          | Ok current -> ignore (Client.terminate current)
          | Error _ -> ())
        ids;
      ignore (Client.shutdown client);
      (try Unix.kill worker_pid Sys.sigterm with Unix.Unix_error _ -> ());
      ignore (Unix.waitpid [] worker_pid))
    (fun () ->
      let delay = Duration.of_ms delay_ms in
      (* A delayed start. *)
      let delayed =
        get
          (Client.start client ~workflow:fixture ~task_queue:queue ~id:(id "delayed")
             ~start_delay:delay ~input:"delayed" ())
      in
      let output, successor = completed "the delayed run" delayed in
      expect_delayed "the delayed run" output;
      if Option.is_some successor then fail "the delayed run had a successor";
      (* A delayed signal-with-start: the signal arrives with the first task. *)
      let signalled =
        get
          (Client.signal_with_start client ~workflow:fixture ~task_queue:queue
             ~id:(id "signalled") ~start_delay:delay ~input:"signalled" ~signal:deliver
             ~signal_input:() ())
      in
      let output, _ = completed "the signalled run" signalled in
      expect_delayed "the signalled run" output;
      (* A cron chain started by this client. *)
      let cron =
        get
          (Client.start client ~workflow:fixture ~task_queue:queue ~id:(id "cron")
             ~cron_schedule:schedule ~input:"cron" ())
      in
      let cron_second = expect_cron_chain client "the client cron chain" cron in
      let current = get (Client.get_handle client ~workflow:fixture ~id:(id "cron") ()) in
      (match Client.wait ~timeout:(Duration.of_seconds 5L) current with
      | Error error when Client.rpc_status error = Some `Deadline_exceeded -> ()
      | Error error -> fail ("the current-run wait failed: " ^ Error.message error)
      | Ok _ -> fail "a current-run wait on a cron chain ended");
      get (Client.terminate current);
      (* A cron chain started by the official CLI. *)
      let output, ok =
        cli_run cli address
          [ "workflow"; "start"; "--namespace"; "default"; "--task-queue"; queue;
            "--type"; "cron-start-delay"; "--workflow-id"; id "cli-cron"; "--cron"; schedule;
            "--input"; "\"cli\""; "--output"; "json" ]
      in
      if not ok then fail ("the CLI cron start failed: " ^ output);
      let cli_first =
        get
          (Client.get_handle client ~workflow:fixture ~id:(id "cli-cron")
             ~run_id:(cli_run_id output) ())
      in
      ignore (expect_cron_chain client "the CLI cron chain" cli_first);
      get (Client.terminate (get (Client.get_handle client ~workflow:fixture ~id:(id "cli-cron") ())));
      (* Temporal parses the expression and rejects an invalid one. *)
      (match
         Client.start client ~workflow:fixture ~task_queue:queue ~id:(id "invalid")
           ~cron_schedule:"not a cron schedule" ~input:"invalid" ()
       with
      | Error error when Client.rpc_status error = Some `Invalid_argument -> ()
      | Error error -> fail ("an invalid schedule failed differently: " ^ Error.message error)
      | Ok _ -> fail "Temporal accepted an invalid cron schedule");
      expect_replay delayed "the delayed run" queue;
      expect_replay cron "the first client cron run" queue;
      expect_replay cron_second "the second client cron run" queue;
      print_endline "cron/start delay live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address; cli ] -> check address cli
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"

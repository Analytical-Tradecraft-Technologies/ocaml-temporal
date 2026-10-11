(** Live #493 regression: detect, report, and recover from workflow code that
    stops yielding.

    One process plays two worker processes on one fresh task queue:

    - worker A runs the broken build. Its implementation of
      [non-yielding.workflow] spins on the CPU without awaiting any workflow
      operation, and its activation deadline is 500 ms;
    - worker B, created only after A has shut down, runs the corrected build:
      the same workflow type returns at once, and its 3 s deadline also lets
      a legitimately heavy (but finite) activation complete.

    The fixture checks, in order:

    + a control workflow completes on A while A is healthy;
    + the non-yielding run is detected within the deadline plus one watchdog
      tick and a second of slack: [Worker.health] reports the exact workflow
      type, workflow ID and run ID, a non-replaying activation, an elapsed
      time of at least the deadline, and a failed workflow task;
    + exactly one [workflow_activation_deadline_exceeded] log record names the
      same run, and neither it nor the server's [WorkflowTaskFailed] event
      contains the run's secret input;
    + [Worker.shutdown_with_report] on A, while the code is still spinning,
      returns within the grace period plus the teardown timeout (plus slack)
      and reports exactly one abandoned workflow activation, and A's [run]
      has not returned because the spinning code owns its thread;
    + on B, a second control workflow and the heavy workflow complete and B
      stays healthy, and Temporal's retry of the failed task completes the
      original run with the corrected code. Its final history holds exactly
      one failed workflow task (from A), a completed task from B, one
      completion, and nothing the abandoned code could have produced;
    + once the spinning code is released, A's late completion is dropped
      (one [workflow_activation_late_completion_dropped] record) and A's
      [run] returns.

    The spinning implementation reads a process-global atomic, which real
    workflow code must never do: it is the fixture's stand-in for an
    unbounded loop that it can later stop.

    Run against a disposable Temporal Server only:
    [dune exec test/integration/non_yielding_workflow/regression.exe -- check
    http://127.0.0.1:7233 /path/to/temporal]. The second argument is the
    official Temporal CLI, used to read the run's history.
    [TEMPORAL_NAMESPACE] defaults to [temporal-sdk-test]. CI runs it through
    [make test-temporal-live-regressions]. *)
open Temporal

(** Converts the public result boundary into a short fixture failure. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the fixture with [message] unless [condition] holds. *)
let require condition message = if not condition then failwith message

let namespace =
  Sys.getenv_opt "TEMPORAL_NAMESPACE"
  |> Option.value ~default:"temporal-sdk-test"

(** Workflow type shared by the broken and the corrected implementation. *)
let workflow_type = "non-yielding.workflow"

(** Worker A's activation deadline, in milliseconds. *)
let deadline_ms = 500L

(** Worker B's activation deadline, in milliseconds; above [heavy_seconds]. *)
let b_deadline_ms = 3_000L

(** Wall-clock CPU time the heavy workflow's single activation uses. It is
    above A's deadline and below B's, so only the configured deadline
    distinguishes it from a stuck activation. *)
let heavy_seconds = 1.0

(** Grace period and teardown timeout given to worker A, in milliseconds. *)
let grace_ms = 1_000L

let teardown_ms = 20_000L

(** Identities recorded by Temporal for the two workers' task completions. *)
let identity_a = "non-yielding-a"

let identity_b = "non-yielding-b"

(** Wall-clock time at which A's spinning code started, or [0.] before. *)
let spin_entered_at = Atomic.make 0.

(** Set by the fixture to let A's spinning code return. *)
let spin_released = Atomic.make false

(** Set by A's spinning code when it finally returns. *)
let spin_returned = Atomic.make false

(** Worker A's broken implementation: busy-waits without performing any
    workflow effect until the fixture releases it (or after 150 s, a safety
    net below the process alarm). Its eventual result must never reach
    Temporal. *)
let spinning =
  Workflow.define ~name:workflow_type ~input:Codec.string ~output:Codec.string
    (fun _secret ->
      Atomic.set spin_entered_at (Unix.gettimeofday ());
      let safety = Unix.gettimeofday () +. 150. in
      while
        (not (Atomic.get spin_released)) && Unix.gettimeofday () < safety
      do
        Domain.cpu_relax ()
      done;
      Atomic.set spin_returned true;
      Ok "late-result-from-a")

(** Worker B's corrected implementation of the same type. The output names
    only the input's length, so a leaked secret would be visible. *)
let corrected =
  Workflow.define ~name:workflow_type ~input:Codec.string ~output:Codec.string
    (fun secret -> Ok (Printf.sprintf "recovered:%d" (String.length secret)))

(** An ordinary workflow that yields on a durable timer, served by both
    workers to show that unaffected executions keep their behavior. *)
let control =
  Workflow.define ~name:"non-yielding.control" ~input:Codec.string
    ~output:Codec.string (fun label ->
      match Workflow.sleep (Duration.of_ms 100L) with
      | Ok () -> Ok ("control:" ^ label)
      | Error error -> Error error)

(** A legitimately long activation: [heavy_seconds] of CPU before returning.
    The duration only delays the result, it never changes a command. *)
let heavy =
  Workflow.define ~name:"non-yielding.heavy" ~input:Codec.string
    ~output:Codec.string (fun label ->
      let until = Unix.gettimeofday () +. heavy_seconds in
      while Unix.gettimeofday () < until do
        Domain.cpu_relax ()
      done;
      Ok ("heavy:" ^ label))

(** Polls [predicate] every 10 ms for at most [seconds]. *)
let wait_until ~seconds predicate =
  let deadline = Unix.gettimeofday () +. seconds in
  let rec loop () =
    if predicate () then true
    else if Unix.gettimeofday () > deadline then false
    else (
      Unix.sleepf 0.01;
      loop ())
  in
  loop ()

(** Log records captured by [capture_logs], newest first, each rendered as its
    message followed by its tags. Reporters run on worker and watchdog
    Domains, so the list is guarded by [records_mutex]. *)
let records : string list ref = ref []

let records_mutex = Mutex.create ()

(** Installs a [Logs] reporter that keeps every warning or error record in
    [records]. The SDK never installs a reporter itself, so this is the
    application-side view an operator would get. *)
let capture_logs () =
  let report _src _level ~over k msgf =
    msgf (fun ?header:_ ?(tags = Logs.Tag.empty) format ->
        Format.kasprintf
          (fun message ->
            let rendered =
              Format.asprintf "%s %a" message Logs.Tag.pp_set tags
            in
            Mutex.protect records_mutex (fun () ->
                records := rendered :: !records);
            over ();
            k ())
          format)
  in
  Logs.set_reporter { Logs.report };
  Logs.set_level (Some Logs.Warning)

(** Returns whether [needle] occurs in [haystack]. *)
let contains haystack needle =
  let length = String.length needle in
  let rec scan index =
    index + length <= String.length haystack
    && (String.sub haystack index length = needle || scan (index + 1))
  in
  length > 0 && scan 0

(** Captured records that mention [operation]. *)
let records_for operation =
  Mutex.protect records_mutex (fun () ->
      List.filter (fun record -> contains record operation) !records)

(** Deadline for one CLI invocation. *)
let cli_timeout = "30s"

(** Reads one exact run's history as JSON events through the official CLI,
    without a shell. A failed read is an [Error] so callers can retry it. *)
let history ~cli ~address handle =
  let args =
    [|
      cli; "--address"; address; "--namespace"; namespace; "--command-timeout";
      cli_timeout; "workflow"; "show"; "--workflow-id";
      Client.workflow_id handle; "--run-id"; Option.get (Client.run_id handle);
      "--output"; "json";
    |]
  in
  let input = Unix.open_process_args_in cli args in
  let document =
    match Yojson.Basic.from_channel input with
    | document -> Ok document
    | exception Yojson.Json_error message -> Error message
    | exception exn ->
        ignore (Unix.close_process_in input);
        raise exn
  in
  match (Unix.close_process_in input, document) with
  | Unix.WEXITED 0, Ok document -> (
      try Ok Yojson.Basic.Util.(document |> member "events" |> to_list)
      with Yojson.Basic.Util.Type_error (message, _) ->
        Error ("unexpected history: " ^ message))
  | Unix.WEXITED 0, Error message -> Error ("unparsable history: " ^ message)
  | Unix.WEXITED code, _ ->
      Error (Printf.sprintf "history read exited with %d" code)
  | (Unix.WSIGNALED _ | Unix.WSTOPPED _), _ -> Error "history read was killed"

(** Returns an event's type without its [EVENT_TYPE_] prefix. *)
let event_type event =
  let name = Yojson.Basic.Util.(event |> member "eventType" |> to_string) in
  let prefix = "EVENT_TYPE_" in
  if String.starts_with ~prefix name then
    String.sub name (String.length prefix)
      (String.length name - String.length prefix)
  else name

(** Events of one type, in history order. *)
let events_of kind events =
  List.filter (fun event -> event_type event = kind) events

(** Reads a string field of an event's attribute object, or [""]. *)
let attribute event ~attributes keys =
  let member = Yojson.Basic.Util.member in
  match
    List.fold_left (fun json key -> member key json) (member attributes event) keys
  with
  | `String value -> value
  | _ -> ""

(** Re-reads the history until [ready] accepts it or [seconds] elapse, and
    returns the last successful read. CLI failures are retried. *)
let await_history ~cli ~address ~seconds ~what handle ready =
  let last = ref (Error "history was never read") in
  let accepted () =
    last := history ~cli ~address handle;
    match !last with Ok events -> ready events | Error _ -> false
  in
  if wait_until ~seconds accepted then Result.get_ok !last
  else
    failwith
      (Printf.sprintf "%s: history of %s %s" what (Client.workflow_id handle)
         (match !last with
         | Ok events ->
             "held " ^ String.concat "," (List.map event_type events)
         | Error message -> "could not be read: " ^ message))

(** The only event types the recovered run may record: no command of the
    abandoned code may reach history. *)
let allowed_events =
  [
    "WORKFLOW_EXECUTION_STARTED"; "WORKFLOW_TASK_SCHEDULED";
    "WORKFLOW_TASK_STARTED"; "WORKFLOW_TASK_FAILED"; "WORKFLOW_TASK_TIMED_OUT";
    "WORKFLOW_TASK_COMPLETED"; "WORKFLOW_EXECUTION_COMPLETED";
  ]

(** Waits for a workflow and returns its output, failing on any other result. *)
let completed_output what handle =
  match get (Client.wait handle) with
  | Client.Completed { output; _ } -> output
  | _ -> failwith (what ^ " did not complete")

(** Runs one worker on its own Domain and records when [run] returns. *)
let start_worker worker =
  let returned = Atomic.make false in
  let domain =
    Domain.spawn (fun () ->
        Fun.protect
          ~finally:(fun () -> Atomic.set returned true)
          (fun () -> Worker.run worker))
  in
  (domain, returned)

(** Formats a shutdown report for the fixture log. *)
let describe_report (report : Worker.Shutdown_report.t) =
  Printf.sprintf
    "elapsed=%Ldms lanes_stopped=%b abandoned_activity_callbacks=%d \
     abandoned_workflow_activations=%d teardown=%s"
    (Duration.to_ms report.elapsed) report.lanes_stopped
    report.abandoned_activity_callbacks report.abandoned_workflow_activations
    (match report.native_teardown with
    | `Completed -> "completed"
    | `Detached -> "detached")

(** Checks A's health report against the stuck run and returns the detection
    latency in seconds, measured from the moment the spinning code started. *)
let check_detection worker_a handle =
  require
    (wait_until ~seconds:60. (fun () -> Atomic.get spin_entered_at > 0.))
    "the non-yielding workflow never started on worker A";
  let entered = Atomic.get spin_entered_at in
  let stuck () =
    match Worker.health worker_a with
    | Worker.Health.Stuck_workflow_activation stuck -> Some stuck
    | Worker.Health.Healthy -> None
  in
  let bound = (Int64.to_float deadline_ms /. 1_000.) +. 1. in
  require
    (wait_until ~seconds:(bound +. 5.) (fun () -> Option.is_some (stuck ())))
    "worker A never reported the stuck activation";
  let latency = Unix.gettimeofday () -. entered in
  let stuck = Option.get (stuck ()) in
  Printf.printf
    "detected after %.3fs: type=%s run=%s elapsed=%Ldms replaying=%b\n%!"
    latency
    (Option.value stuck.workflow_type ~default:"?")
    stuck.run_id (Duration.to_ms stuck.elapsed) stuck.is_replaying;
  require (latency <= bound)
    (Printf.sprintf "detection took %.3fs, above its %.3fs bound" latency bound);
  require
    (stuck.workflow_type = Some workflow_type)
    "the health report named the wrong workflow type";
  require
    (stuck.workflow_id = Some (Client.workflow_id handle))
    "the health report named the wrong workflow ID";
  require
    (Some stuck.run_id = Client.run_id handle)
    "the health report named the wrong run";
  require (not stuck.is_replaying) "a first execution was reported as replay";
  require
    (Duration.to_ms stuck.elapsed >= deadline_ms)
    "the reported elapsed time is below the deadline";
  require (stuck.abandoned = `Task_failed)
    "the stuck workflow task was not reported failed";
  require
    (Atomic.get spin_returned = false)
    "the spinning code returned before it was released"

(** Checks the operator-facing diagnostics: one bounded log record naming the
    run, and the server's failed-task event, neither carrying the secret. *)
let check_diagnostics ~cli ~address ~secret handle =
  let run_id = Option.get (Client.run_id handle) in
  let deadline_records = records_for "workflow_activation_deadline_exceeded" in
  require
    (List.length deadline_records = 1)
    (Printf.sprintf "expected one deadline record, saw %d"
       (List.length deadline_records));
  let record = List.hd deadline_records in
  Printf.printf "log record: %s\n%!" record;
  List.iter
    (fun expected ->
      require (contains record expected)
        ("the deadline record does not mention " ^ expected))
    [ run_id; Client.workflow_id handle; workflow_type; "workflow_task_failed" ];
  require (not (contains record secret)) "the deadline record leaked the input";
  let events =
    await_history ~cli ~address ~seconds:20. ~what:"task failure" handle
      (fun events -> events_of "WORKFLOW_TASK_FAILED" events <> [])
  in
  let failed = List.hd (events_of "WORKFLOW_TASK_FAILED" events) in
  let attributes = "workflowTaskFailedEventAttributes" in
  let message = attribute failed ~attributes [ "failure"; "message" ] in
  Printf.printf "workflow task failure: %s\n%!" message;
  require
    (contains message "activation deadline")
    "the failed task does not name the activation deadline";
  require
    (attribute failed ~attributes [ "identity" ] = identity_a)
    "the failed task was not reported by worker A";
  require
    (not (contains (Yojson.Basic.to_string failed) secret))
    "the failed-task event leaked the input"

(** Checks the recovered run's complete history. *)
let check_recovered_history ~cli ~address handle =
  let events =
    await_history ~cli ~address ~seconds:20. ~what:"recovery" handle
      (fun events -> events_of "WORKFLOW_EXECUTION_COMPLETED" events <> [])
  in
  Printf.printf "recovered history: %s\n%!"
    (String.concat "," (List.map event_type events));
  List.iter
    (fun event ->
      require
        (List.mem (event_type event) allowed_events)
        ("unexpected event in the recovered history: " ^ event_type event))
    events;
  require
    (List.length (events_of "WORKFLOW_TASK_FAILED" events) = 1)
    "the recovered run should record exactly one failed workflow task";
  require
    (List.length (events_of "WORKFLOW_EXECUTION_COMPLETED" events) = 1)
    "the recovered run should complete exactly once";
  let completions = events_of "WORKFLOW_TASK_COMPLETED" events in
  require
    (List.for_all
       (fun event ->
         attribute event ~attributes:"workflowTaskCompletedEventAttributes"
           [ "identity" ]
         = identity_b)
       completions
    && completions <> [])
    "the recovered run's workflow task was not completed by worker B"

(** Runs the scenario. Every started run is terminated and every worker shut
    down on exit; a process-wide alarm bounds a hung server or worker. *)
let check address cli =
  ignore (Unix.alarm 170);
  if not (String.starts_with ~prefix:"http://" address) then
    failwith "this fixture requires an HTTP development server";
  let cli_address = String.sub address 7 (String.length address - 7) in
  capture_logs ();
  let suffix =
    Printf.sprintf "%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  let queue = "non-yielding-" ^ suffix in
  let secret = "non-yielding-secret-" ^ suffix in
  let a_options =
    get
      (Worker.Options.make
         ~workflow_activation_deadline:(`After (Duration.of_ms deadline_ms))
         ~graceful_shutdown_period:(Duration.of_ms grace_ms)
         ~shutdown_teardown_timeout:(Duration.of_ms teardown_ms) ())
  in
  let worker_a =
    get
      (Worker.create ~options:a_options ~target_url:address ~namespace
         ~task_queue:queue ~identity:identity_a
         ~workflows:[ Worker.workflow spinning; Worker.workflow control ]
         ~activities:[] ())
  in
  let a_domain, a_returned = start_worker worker_a in
  let a_shut_down = ref false in
  let worker_b = ref None in
  let client = get (Client.create ~target_url:address ~namespace ()) in
  let cleanup = ref [] in
  let track handle =
    cleanup := (fun () -> ignore (Client.terminate handle)) :: !cleanup;
    handle
  in
  let start workflow ~id ~input =
    track
      (get
         (Client.start client ~workflow ~task_queue:queue
            ~task_timeout:(Duration.of_ms 3_000L) ~id:(id ^ "-" ^ suffix)
            ~input ()))
  in
  let body =
    try
      (* 1. A is healthy and serves ordinary workflows. *)
      let first = start control ~id:"non-yielding-control-a" ~input:"a" in
      require
        (completed_output "the first control workflow" first = "control:a")
        "the first control workflow returned the wrong output";
      require (Worker.health worker_a = Worker.Health.Healthy)
        "worker A was unhealthy before the non-yielding workflow";
      (* 2. The non-yielding run is detected, reported, and its task failed. *)
      let stuck_run = start spinning ~id:"non-yielding-run" ~input:secret in
      check_detection worker_a stuck_run;
      check_diagnostics ~cli ~address:cli_address ~secret stuck_run;
      (* 3. Shutting A down while the code still spins returns in bound. *)
      let started = Unix.gettimeofday () in
      let report = get (Worker.shutdown_with_report worker_a) in
      let elapsed = Unix.gettimeofday () -. started in
      a_shut_down := true;
      Printf.printf "worker A shutdown after %.3fs: %s\n%!" elapsed
        (describe_report report);
      let bound =
        ((Int64.to_float grace_ms +. Int64.to_float teardown_ms) /. 1_000.)
        +. 2.
      in
      require (elapsed <= bound)
        (Printf.sprintf "shutdown took %.1fs, above its %.1fs bound" elapsed
           bound);
      require
        (report.abandoned_workflow_activations = 1)
        "the stuck activation was not reported abandoned";
      require
        (report.abandoned_activity_callbacks = 0)
        "a workflow-only worker reported an activity callback";
      require
        (not (Worker.Shutdown_report.is_clean report))
        "an abandoned activation was reported as a clean shutdown";
      require
        (not (Atomic.get a_returned))
        "worker A's run returned while its thread was still spinning";
      require
        (not (Atomic.get spin_returned))
        "the spinning code returned before it was released";
      (* 4. The corrected build recovers the run; other work is unaffected. *)
      let b_options =
        get
          (Worker.Options.make
             ~workflow_activation_deadline:
               (`After (Duration.of_ms b_deadline_ms))
             ())
      in
      let b =
        get
          (Worker.create ~options:b_options ~target_url:address ~namespace
             ~task_queue:queue ~identity:identity_b
             ~workflows:
               [
                 Worker.workflow corrected; Worker.workflow control;
                 Worker.workflow heavy;
               ]
             ~activities:[] ())
      in
      worker_b := Some (b, start_worker b);
      let second = start control ~id:"non-yielding-control-b" ~input:"b" in
      let heavy_run = start heavy ~id:"non-yielding-heavy" ~input:"b" in
      require
        (completed_output "the second control workflow" second = "control:b")
        "the second control workflow returned the wrong output";
      require
        (completed_output "the heavy workflow" heavy_run = "heavy:b")
        "the heavy workflow returned the wrong output";
      let recovery_started = Unix.gettimeofday () in
      let output = completed_output "the recovered workflow" stuck_run in
      Printf.printf "recovered output %s after a further %.3fs\n%!" output
        (Unix.gettimeofday () -. recovery_started);
      require
        (output = Printf.sprintf "recovered:%d" (String.length secret))
        ("the recovered run completed as " ^ output);
      check_recovered_history ~cli ~address:cli_address stuck_run;
      require (Worker.health b = Worker.Health.Healthy)
        "worker B became unhealthy";
      (* 5. Releasing the code drops A's late completion and ends its run. *)
      Atomic.set spin_released true;
      require
        (wait_until ~seconds:15. (fun () -> Atomic.get a_returned))
        "worker A's run did not return after the code was released";
      require (Atomic.get spin_returned) "the spinning code did not return";
      Printf.printf "worker A run after release: %s\n%!"
        (match Domain.join a_domain with
        | Ok () -> "ok"
        | Error error -> "error: " ^ Error.message error);
      require
        (List.length (records_for "workflow_activation_late_completion_dropped")
        = 1)
        "the late completion was not dropped exactly once";
      require
        (List.length (records_for "workflow_activation_deadline_exceeded") = 1)
        "the watchdog reported more than once";
      Ok output
    with exception_ -> Error exception_
  in
  Atomic.set spin_released true;
  (match body with
  | Ok _ -> ()
  | Error _ -> List.iter (fun terminate -> terminate ()) !cleanup);
  ignore (Client.shutdown client);
  if not !a_shut_down then ignore (Worker.shutdown worker_a);
  if (not (Atomic.get a_returned)) && wait_until ~seconds:15. (fun () -> Atomic.get a_returned)
  then ignore (Domain.join a_domain);
  let b_clean =
    match !worker_b with
    | None -> true
    | Some (b, (domain, _)) -> (
        let report = Worker.shutdown_with_report b in
        ignore (Domain.join domain);
        match report with
        | Ok report -> Worker.Shutdown_report.is_clean report
        | Error _ -> false)
  in
  match body with
  | Ok output ->
      require b_clean "worker B did not shut down cleanly";
      Printf.printf
        "non-yielding workflow live regression: detected and failed the \
         stuck task within its bound, shut the stuck worker down within its \
         bound, and recovered the run on the corrected worker (%s)\n%!"
        output
  | Error exception_ -> raise exception_

let () =
  match Sys.argv with
  | [| _; "check"; address; cli |] -> check address cli
  | _ ->
      prerr_endline "usage: regression.exe check TEMPORAL_ADDRESS TEMPORAL_CLI";
      exit 2

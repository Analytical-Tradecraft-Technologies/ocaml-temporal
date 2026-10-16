(** Live regression for issue #519: a worker process is killed with [SIGKILL]
    while an activity callback is running, and a replacement process
    completes the retried attempt.

    One executable plays two roles. [worker] runs a public [Temporal.Worker]
    hosting both the workflow and the activity on one task queue; its
    generation argument selects the activity implementation. [check] owns the
    client, spawns and reaps every worker process, and synchronizes only on a
    marker file written by the running callback and on server state read
    through the official Temporal CLI. Every wait is a bounded poll with a
    named failure; no step relies on a fixed sleep.

    The scenario, in order:

    + Generation 1 starts. The workflow schedules one activity with a
      three-second heartbeat timeout, a long start-to-close timeout, and a
      one-second retry interval, so only the heartbeat timeout can end
      attempt 1.
    + Attempt 1 runs on generation 1. Its callback heartbeats a fixed detail
      every 200 ms and never returns. After its first accepted heartbeat it
      writes a marker file (barrier one).
    + The driver then waits until [workflow describe] shows the pending
      activity started as attempt 1 by generation 1, with recorded heartbeat
      details (barrier two), so the kill lands while the server considers the
      callback running.
    + The driver sends [SIGKILL] to generation 1 and requires that the
      process died of that signal. No graceful shutdown path runs.
    + With no worker on the queue, the driver waits until [describe] shows
      the activity rescheduled as attempt 2 with a heartbeat-timeout last
      failure while the run is still open (barrier three). This proves the
      server alone detected the lost attempt.
    + Generation 2 starts. Its callback reports its attempt number from
      {!Temporal.Activity.Info.attempt} and the previous attempt's last
      heartbeat detail, and returns at once.
    + The run must complete with the exact expected result, which names
      attempt 2, generation 2, and generation 1's heartbeat detail. The exact
      run's history must hold exactly one scheduled, started, and completed
      activity event and no activity failure or timeout event; the started
      event must record attempt 2, generation 2's identity, and a
      heartbeat-timeout last failure, and the completed event generation 2's
      identity.

    Evidence (the two [describe] snapshots, the raw exact-run history, and
    each worker generation's log) is written to the evidence directory: the
    optional third [check] argument, or a fresh temporary directory. The
    worker logs are also copied into the driver's output, which the live
    regression controller keeps as this suite's log.

    Run against a disposable Temporal Server with a [default] namespace only:
    [make test-activity-worker-crash-live RUN='opam exec --'
    TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233
    TEMPORAL_TEST_CLI=/absolute/path/to/temporal]. CI runs it through
    [make test-temporal-live-regressions]. *)
open Temporal

(** Converts a setup or client failure into a fixture failure. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** The namespace the live regression controller registers for CLI-backed
    fixtures. *)
let namespace = "default"

(** Activity type shared by the workflow's definition and both worker
    generations' implementations. *)
let activity_name = "activity-worker-crash.callback"

(** Explicit activity ID, so the evidence names one scheduled activity. *)
let activity_id = "crash-target"

(** Workflow input; it is echoed into the activity result. *)
let label = "activity-worker-crash"

(** The heartbeat detail generation 1 records; attempt 2 must read it back. *)
let heartbeat_detail = "generation-1-attempt-1"

(** Heartbeat timeout of the scheduled activity, in milliseconds. It is
    short so the server detects the killed attempt promptly, and well above
    the 200 ms heartbeat cadence (Core throttles sends to 80% of it). *)
let heartbeat_timeout_ms = 3_000L

(** The terminal result the run must produce: attempt 2 ran on generation 2
    and observed generation 1's last heartbeat. *)
let expected_result =
  Printf.sprintf "completed:%s:attempt=2:generation=2:previous_heartbeat=%s" label
    heartbeat_detail

(** Name of the marker file generation 1 writes after its first accepted
    heartbeat. *)
let marker_name = "attempt-1-heartbeating"

(** Worker identity for one generation. Temporal records it on the pending
    activity and on activity history events, so the driver can attribute
    each attempt to a process. *)
let identity generation = "activity-worker-crash-generation-" ^ generation

(** The attempt number of an activity context, or 0 if unavailable. *)
let attempt context =
  match Activity.Context.info context with
  | Ok info -> Activity.Info.attempt info
  | Error _ -> 0

(** Writes [contents] to [path] through a rename, so a reader never sees a
    partially written file. *)
let write_atomically path contents =
  let partial = path ^ ".partial" in
  Out_channel.with_open_bin partial (fun channel -> output_string channel contents);
  Sys.rename partial path

(** Generation 1's implementation: a callback that is still running when its
    process is killed. It heartbeats [heartbeat_detail] every 200 ms and
    writes the marker in [evidence] after the first accepted heartbeat. It
    returns only after ten minutes, a safety net far beyond the driver's
    alarm; its result must never reach Temporal. *)
let running_callback evidence =
  Activity.define_with_context ~name:activity_name ~input:Codec.string
    ~output:Codec.string (fun context _label ->
      let attempt = attempt context in
      Printf.printf "generation 1: attempt %d started in pid %d\n%!" attempt
        (Unix.getpid ());
      let deadline = Unix.gettimeofday () +. 600. in
      let rec loop marked =
        if Unix.gettimeofday () > deadline then
          Error (Error.make ~category:`Activity ~message:"generation 1 was never killed" ())
        else begin
          let marked =
            match Activity.Context.heartbeat context Codec.string heartbeat_detail with
            | Ok () when not marked ->
                write_atomically
                  (Filename.concat evidence marker_name)
                  (Printf.sprintf "pid=%d attempt=%d\n" (Unix.getpid ()) attempt);
                Printf.printf "generation 1: attempt %d heartbeating\n%!" attempt;
                true
            | Ok () -> marked
            | Error error ->
                Printf.printf "generation 1: heartbeat failed: %s\n%!" (Error.message error);
                marked
          in
          Unix.sleepf 0.2;
          loop marked
        end
      in
      loop false)

(** Generation 2's implementation: returns at once, naming its attempt and
    the previous attempt's last heartbeat detail (["none"] if absent). *)
let replacement_callback =
  Activity.define_with_context ~name:activity_name ~input:Codec.string
    ~output:Codec.string (fun context input ->
      let previous =
        match Activity.Context.details context with
        | [] -> "none"
        | payload :: _ -> (
            match Codec.decode Codec.string payload with
            | Ok detail -> detail
            | Error _ -> "undecodable")
      in
      let attempt = attempt context in
      Printf.printf "generation 2: attempt %d completing in pid %d\n%!" attempt
        (Unix.getpid ());
      Ok
        (Printf.sprintf "%s:attempt=%d:generation=2:previous_heartbeat=%s" input attempt
           previous))

(** Schedules the activity on the workflow's own queue. The start-to-close
    timeout is far beyond the test, so only the heartbeat timeout can end the
    killed attempt; the retry interval is one second, and a third attempt
    would show in the result rather than pass. *)
let workflow =
  Workflow.define ~name:"activity-worker-crash" ~input:Codec.string ~output:Codec.string
    (fun input ->
      let open Result_syntax in
      let* retry_policy =
        Activity.Retry_policy.make ~initial_interval:(Duration.of_ms 1_000L)
          ~backoff_coefficient:1.0 ~maximum_interval:(Duration.of_ms 1_000L)
          ~maximum_attempts:3 ()
      in
      let* output =
        Activity.execute ~activity_id ~start_to_close_timeout:(Duration.of_ms 600_000L)
          ~heartbeat_timeout:(Duration.of_ms heartbeat_timeout_ms) ~retry_policy
          replacement_callback input
      in
      Ok ("completed:" ^ output))

(** Runs one worker generation until it is killed or terminated. Generation
    ["1"] registers {!running_callback}; any other generation registers
    {!replacement_callback}. *)
let worker address queue generation evidence =
  let activity =
    if generation = "1" then Worker.activity (running_callback evidence)
    else Worker.activity replacement_callback
  in
  let worker =
    get
      (Worker.create ~identity:(identity generation) ~target_url:address ~namespace
         ~task_queue:queue ~workflows:[ Worker.workflow workflow ] ~activities:[ activity ] ())
  in
  Printf.printf "generation %s: worker running in pid %d\n%!" generation (Unix.getpid ());
  get (Worker.run worker)

(** Path of one generation's log in the evidence directory. *)
let log_path evidence generation =
  Filename.concat evidence (Printf.sprintf "generation-%s.log" generation)

(** Spawns a worker generation as a separate process, with its stdout and
    stderr in its own evidence log, so a replacement shares no OCaml state
    with the process it replaces. *)
let spawn address queue generation evidence =
  let log =
    Unix.openfile (log_path evidence generation)
      [ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC; Unix.O_CLOEXEC ]
      0o644
  in
  Fun.protect ~finally:(fun () -> Unix.close log) (fun () ->
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "worker"; address; queue; generation; evidence |]
      Unix.stdin log log)

(** Returns [true] while [pid] has not exited. It reaps the process if it
    has, so callers must not wait for it again after [false]. *)
let alive pid =
  match Unix.waitpid [ Unix.WNOHANG ] pid with
  | 0, _ -> true
  | _ -> false
  | exception Unix.Unix_error (Unix.ECHILD, _, _) -> false

(** Sends [signal] to a still-running worker and reaps it. Used only for
    cleanup; the scenario's own kill checks the exit status itself. *)
let stop signal pid =
  if alive pid then begin
    (try Unix.kill pid signal with Unix.Unix_error _ -> ());
    try ignore (Unix.waitpid [] pid) with Unix.Unix_error _ -> ()
  end

(** Seconds allowed for any one observation to become true. Temporal may
    wait for a dead worker's sticky queue to time out before redispatching
    the final workflow task, so this is deliberately generous. *)
let observation_seconds = 90.

(** Polls [attempt] until it returns [Ok], logging each distinct waiting
    reason, and fails with [label] and the last reason at the deadline. *)
let await label attempt =
  let deadline = Unix.gettimeofday () +. observation_seconds in
  let rec poll last =
    match attempt () with
    | Ok value -> value
    | Error message when Unix.gettimeofday () < deadline ->
        if Some message <> last then
          Printf.printf "activity worker crash: waiting for %s: %s\n%!" label message;
        Unix.sleepf 0.1;
        poll (Some message)
    | Error message -> failwith (Printf.sprintf "timed out waiting for %s: %s" label message)
  in
  poll None

(** Deadline for one CLI invocation, matching the other history fixtures. *)
let cli_timeout = "30s"

(** Runs one [temporal workflow] subcommand for the exact run and parses its
    JSON output, without a shell. A failed read is an [Error] so callers can
    retry it. *)
let cli_json cli address handle command =
  let args =
    Array.of_list
      ([ cli; "--address"; address; "--namespace"; namespace; "--command-timeout";
         cli_timeout; "workflow" ]
      @ command
      @ [ "--workflow-id"; Client.workflow_id handle; "--run-id";
          Option.get (Client.run_id handle); "--output"; "json" ])
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
  | Unix.WEXITED 0, Ok document -> Ok document
  | Unix.WEXITED 0, Error message -> Error ("unparsable CLI output: " ^ message)
  | Unix.WEXITED code, _ -> Error (Printf.sprintf "CLI exited with %d" code)
  | (Unix.WSIGNALED _ | Unix.WSTOPPED _), _ -> Error "CLI was killed"

(** Saves one JSON document in the evidence directory. *)
let save evidence name document =
  write_atomically (Filename.concat evidence name) (Yojson.Basic.pretty_to_string document ^ "\n")

(** Reports whether the lower-cased compact rendering of [json] contains
    [needle]. Enum values are matched this way so either protobuf JSON
    spelling (["TIMEOUT_TYPE_HEARTBEAT"] or a short form) is accepted. *)
let mentions json needle =
  let haystack = String.lowercase_ascii (Yojson.Basic.to_string json) in
  let length = String.length needle in
  let rec search index =
    index + length <= String.length haystack
    && (String.sub haystack index length = needle || search (index + 1))
  in
  search 0

(** Reads an integer field that protobuf JSON may render as a number or a
    string; a missing or malformed field is [None]. *)
let int_field name json =
  match Yojson.Basic.Util.member name json with
  | `Int value -> Some value
  | `String value -> int_of_string_opt value
  | _ -> None

(** Reads a string field; a missing or non-string field is [None]. *)
let string_field name json =
  match Yojson.Basic.Util.member name json with `String value -> Some value | _ -> None

(** Requires the run to still be open in a [describe] document. *)
let running document =
  let info = Yojson.Basic.Util.member "workflowExecutionInfo" document in
  if mentions (Yojson.Basic.Util.member "status" info) "running" then Ok ()
  else Error ("the run is not running: " ^ Yojson.Basic.to_string (Yojson.Basic.Util.member "status" info))

(** Returns the only pending activity of a [describe] document, which must
    be the scenario's activity. *)
let pending_activity document =
  match Yojson.Basic.Util.member "pendingActivities" document with
  | `List [ activity ] when string_field "activityId" activity = Some activity_id -> Ok activity
  | `List [ activity ] -> Error ("unexpected pending activity: " ^ Yojson.Basic.to_string activity)
  | `List [] | `Null -> Error "no pending activity"
  | other -> Error ("unexpected pending activities: " ^ Yojson.Basic.to_string other)

(** Barrier two: the server shows attempt 1 started by generation 1 with a
    recorded heartbeat. Returns the [describe] document. *)
let attempt_one_running cli address handle () =
  Result.bind (cli_json cli address handle [ "describe" ]) (fun document ->
    Result.bind (running document) (fun () ->
      Result.bind (pending_activity document) (fun activity ->
        let state = Yojson.Basic.Util.member "state" activity in
        if int_field "attempt" activity <> Some 1 then
          Error ("pending attempt is not 1: " ^ Yojson.Basic.to_string activity)
        else if not (mentions state "started") then
          Error ("attempt 1 is not started: " ^ Yojson.Basic.to_string state)
        else if string_field "lastWorkerIdentity" activity <> Some (identity "1") then
          Error ("attempt 1 is not on generation 1: " ^ Yojson.Basic.to_string activity)
        else if Yojson.Basic.Util.member "heartbeatDetails" activity = `Null then
          Error "no heartbeat recorded for attempt 1 yet"
        else Ok document)))

(** Barrier three: with no worker polling, the server has failed attempt 1
    on its heartbeat timeout and scheduled attempt 2 while the run is open.
    Returns the [describe] document. *)
let attempt_two_scheduled cli address handle () =
  Result.bind (cli_json cli address handle [ "describe" ]) (fun document ->
    Result.bind (running document) (fun () ->
      Result.bind (pending_activity document) (fun activity ->
        let last_failure = Yojson.Basic.Util.member "lastFailure" activity in
        if int_field "attempt" activity <> Some 2 then
          Error ("pending attempt is not 2 yet: " ^ Yojson.Basic.to_string activity)
        else if not (mentions last_failure "heartbeat") then
          Error ("attempt 1 did not fail on its heartbeat timeout: "
                 ^ Yojson.Basic.to_string last_failure)
        else Ok document)))

(** Returns an event's type without its [EVENT_TYPE_] prefix. *)
let event_type event =
  let name = Yojson.Basic.Util.(event |> member "eventType" |> to_string) in
  let prefix = "EVENT_TYPE_" in
  if String.starts_with ~prefix name then
    String.sub name (String.length prefix) (String.length name - String.length prefix)
  else name

(** The durable, non-workflow-task events the run must record, in order.
    Workflow-task events are excluded because a dead worker's sticky queue
    legitimately adds a workflow-task timeout before redispatch. *)
let expected_decisions =
  [ "WORKFLOW_EXECUTION_STARTED"; "ACTIVITY_TASK_SCHEDULED"; "ACTIVITY_TASK_STARTED";
    "ACTIVITY_TASK_COMPLETED"; "WORKFLOW_EXECUTION_COMPLETED" ]

(** Returns the attributes of the only event of [kind], stored under
    [attributes]. *)
let only_event events kind attributes =
  match List.filter (fun event -> event_type event = kind) events with
  | [ event ] -> Yojson.Basic.Util.member attributes event
  | found -> failwith (Printf.sprintf "expected one %s, found %d" kind (List.length found))

(** Checks the exact run's completed history: the decision sequence, then
    attempt 2's attribution to generation 2 and its heartbeat-timeout
    predecessor. *)
let check_history events =
  let decisions =
    List.filter_map
      (fun event ->
        let kind = event_type event in
        if String.starts_with ~prefix:"WORKFLOW_TASK_" kind then None else Some kind)
      events
  in
  Printf.printf "activity worker crash history: %s\n%!" (String.concat ", " decisions);
  if decisions <> expected_decisions then
    failwith
      (Printf.sprintf "history\n    %s\n  expected\n    %s" (String.concat "\n    " decisions)
         (String.concat "\n    " expected_decisions));
  let started = only_event events "ACTIVITY_TASK_STARTED" "activityTaskStartedEventAttributes" in
  let completed =
    only_event events "ACTIVITY_TASK_COMPLETED" "activityTaskCompletedEventAttributes"
  in
  if int_field "attempt" started <> Some 2 then
    failwith ("the started activity event is not attempt 2: " ^ Yojson.Basic.to_string started);
  if string_field "identity" started <> Some (identity "2") then
    failwith ("attempt 2 did not start on generation 2: " ^ Yojson.Basic.to_string started);
  if not (mentions (Yojson.Basic.Util.member "lastFailure" started) "heartbeat") then
    failwith ("attempt 2 does not record a heartbeat-timeout predecessor: "
              ^ Yojson.Basic.to_string started);
  if string_field "identity" completed <> Some (identity "2") then
    failwith ("attempt 2 did not complete on generation 2: " ^ Yojson.Basic.to_string completed)

(** Copies each generation's log into the driver output, so the suite log
    kept by the controller contains the worker side too. *)
let dump_logs evidence =
  List.iter
    (fun generation ->
      let path = log_path evidence generation in
      if Sys.file_exists path then begin
        Printf.printf "----- generation %s log (%s) -----\n" generation path;
        print_string (In_channel.with_open_bin path In_channel.input_all);
        Printf.printf "----- end of generation %s log -----\n%!" generation
      end)
    [ "1"; "2" ]

(** Creates the evidence directory if needed and removes a stale marker or
    log from an earlier run, so every barrier observes this run only. *)
let prepare_evidence = function
  | Some directory ->
      (try Unix.mkdir directory 0o755 with Unix.Unix_error (Unix.EEXIST, _, _) -> ());
      List.iter
        (fun name ->
          let path = Filename.concat directory name in
          if Sys.file_exists path then Sys.remove path)
        [ marker_name; "generation-1.log"; "generation-2.log"; "describe-before-kill.json";
          "describe-after-kill.json"; "history.json" ];
      directory
  | None -> Filename.temp_dir "activity-worker-crash-" ""

(** Runs the scenario with protected cleanup of both worker generations and
    the run. *)
let check address cli evidence =
  if not (String.starts_with ~prefix:"http://" address) then
    failwith "this fixture requires an HTTP development server";
  let cli_address = String.sub address 7 (String.length address - 7) in
  let evidence = prepare_evidence evidence in
  Printf.printf "activity worker crash: evidence in %s\n%!" evidence;
  let queue =
    Printf.sprintf "activity-worker-crash-%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  let first = spawn address queue "1" evidence in
  (* [Some pid] until the scenario reaps generation 1 itself, so cleanup
     never signals or waits for a reaped (and possibly reused) PID. *)
  let unreaped_first = ref (Some first) in
  let replacement = ref None in
  Fun.protect
    ~finally:(fun () ->
      Option.iter (stop Sys.sigkill) !unreaped_first;
      Option.iter (stop Sys.sigterm) !replacement;
      dump_logs evidence)
    (fun () ->
      let client = get (Client.create ~target_url:address ~namespace ()) in
      Fun.protect ~finally:(fun () -> ignore (Client.shutdown client)) (fun () ->
        let handle =
          get (Client.start client ~workflow ~task_queue:queue ~id:queue ~input:label ())
        in
        (* Terminating an already completed run is a harmless typed error. *)
        Fun.protect ~finally:(fun () -> ignore (Client.terminate handle)) (fun () ->
          (* Barrier one: the callback is running and heartbeating. *)
          let marker = Filename.concat evidence marker_name in
          await "attempt 1 to heartbeat on generation 1" (fun () ->
            if Sys.file_exists marker then Ok ()
            else if alive first then Error "no marker yet"
            else begin
              unreaped_first := None;
              failwith "generation 1 exited before attempt 1 heartbeated"
            end);
          let expected_marker = Printf.sprintf "pid=%d attempt=1\n" first in
          let marker_contents = In_channel.with_open_bin marker In_channel.input_all in
          if marker_contents <> expected_marker then
            failwith (Printf.sprintf "marker %S, expected %S" marker_contents expected_marker);
          (* Barrier two: the server agrees the callback is running. *)
          let before = await "server to record attempt 1 running"
            (attempt_one_running cli cli_address handle) in
          save evidence "describe-before-kill.json" before;
          if not (alive first) then begin
            unreaped_first := None;
            failwith "generation 1 exited before the kill"
          end;
          Unix.kill first Sys.sigkill;
          let status = snd (Unix.waitpid [] first) in
          unreaped_first := None;
          (match status with
           | Unix.WSIGNALED signal when signal = Sys.sigkill ->
               Printf.printf "activity worker crash: generation 1 (pid %d) killed by SIGKILL \
                              while attempt 1 was running\n%!" first
           | Unix.WEXITED code ->
               failwith (Printf.sprintf "generation 1 exited with %d instead of dying" code)
           | Unix.WSIGNALED _ | Unix.WSTOPPED _ ->
               failwith "generation 1 did not die of SIGKILL");
          (* Barrier three: the server alone retries the lost attempt. *)
          let after = await "server to schedule attempt 2 after the heartbeat timeout"
            (attempt_two_scheduled cli cli_address handle) in
          save evidence "describe-after-kill.json" after;
          replacement := Some (spawn address queue "2" evidence);
          (match get (Client.wait handle) with
           | Client.Completed { output; _ } when output = expected_result -> ()
           | Client.Completed { output; _ } ->
               failwith (Printf.sprintf "result %S, expected %S" output expected_result)
           | _ -> failwith "the run did not complete");
          let history = await "history of the completed run" (fun () ->
            cli_json cli cli_address handle [ "show" ]) in
          save evidence "history.json" history;
          check_history Yojson.Basic.Util.(history |> member "events" |> to_list);
          Printf.printf
            "activity worker crash live regression: ok (generation 1 killed during \
             attempt 1; generation 2 completed attempt 2 with %S)\n%!"
            expected_result)))

(** Dispatches the two roles. An overall alarm bounds the driver below the
    controller's per-suite limit; protected cleanup reaps the workers. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "worker"; address; queue; generation; evidence ] ->
      worker address queue generation evidence
  | [ _; "check"; address; cli ] | [ _; "check"; address; cli; _ ] as arguments ->
      let evidence = match arguments with [ _; _; _; _; directory ] -> Some directory | _ -> None in
      Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ -> failwith "fixture timed out"));
      ignore (Unix.alarm 170);
      Fun.protect ~finally:(fun () -> ignore (Unix.alarm 0)) (fun () -> check address cli evidence)
  | _ ->
      prerr_endline
        "usage: regression.exe check http://HOST:PORT /path/to/temporal [EVIDENCE_DIR]";
      exit 2

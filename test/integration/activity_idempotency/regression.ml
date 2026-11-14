(** Live regression for #504: an activity's external side effect is
    committed, and its worker process dies before the completion reaches
    Temporal. A replacement process receives the redelivered attempt, the
    external system's atomic idempotency contract answers it with the
    original posting, and the side effect happens once.

    The definitions under test are the shipped idempotent payments example
    ({!Idempotent_payments}, {!Payment_ledger}): a workflow that posts each
    line of an order to a file-backed ledger under the key
    ["<order ID>/line-<N>"]. The order has two identical lines, so the run
    also shows that a distinct operation with an identical payload gets a
    distinct key and is not suppressed.

    One executable plays two roles. [worker] runs a public [Temporal.Worker]
    hosting the workflow and the activity on one task queue, against the
    ledger directory shared by every process. [check] owns the client,
    spawns and reaps both worker generations, and observes the ledger
    directory, [Client.describe] and the exact run's history read through
    the official Temporal CLI. Every wait is a bounded poll with a named
    failure; no step relies on a fixed sleep.

    The scenario, in order:

    + Generation 1 starts and the order is posted. Line 1's first attempt
      commits its posting in the ledger and then sends [SIGKILL] to its own
      process, after the durable effect and before the activity returns, so
      no completion can reach Temporal.
    + The driver requires that generation 1 died of [SIGKILL], and that the
      ledger then holds exactly line 1's record, committed by attempt 1 of
      activity [post-line-1] in the exact workflow run, labelled with
      generation 1.
    + With no worker polling, the driver waits until [Client.describe] shows
      line 1 pending as attempt 2 while the run is open, after Temporal timed
      out the lost attempt (start-to-close timeout).
    + Generation 2 starts. Line 1's attempt 2 sends the same key; the ledger
      answers it as a duplicate of attempt 1's posting, and the activity
      returns the original receipt. Line 2, with an identical account and
      amount, gets its own key and is posted by its first attempt.
    + The run must complete with exactly the receipts of both lines. The
      ledger must hold exactly two records and its journal exactly three
      requests (line 1 committed by attempt 1 on generation 1, line 1
      answered as a duplicate to attempt 2 on generation 2, line 2
      committed by attempt 1 on generation 2), all naming the exact workflow
      and run. The run's history must hold exactly two scheduled, started
      and completed activities and no activity failure or timeout event;
      line 1's started event must record attempt 2 on generation 2 with a
      timeout as its last failure.

    Evidence is written to the evidence directory (the optional third
    [check] argument, or a fresh temporary directory): the ledger directory
    itself ([ledger/], with its record files and journal), a
    [correlation.json] summary naming the workflow, run, activity, attempt,
    worker generation and process of every ledger request, the
    [Client.describe] snapshots, the raw exact-run history and each worker
    generation's log. The worker logs are also copied into the driver's
    output, which the live regression controller keeps as this suite's log.

    Run against a disposable Temporal Server with a [default] namespace only:
    [make test-activity-idempotency-live RUN='opam exec --'
    TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233
    TEMPORAL_TEST_CLI=/absolute/path/to/temporal]. CI runs it through
    [make test-temporal-live-regressions]. *)
open Temporal

(** Converts a setup or client failure into a fixture failure. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Converts a ledger failure into a fixture failure. *)
let ledger_get = function Ok value -> value | Error message -> failwith ("ledger: " ^ message)

(** The namespace the live regression controller registers for CLI-backed
    fixtures. *)
let namespace = "default"

(** Worker identity for one generation. Temporal records it on activity
    history events, and the ledger records it with every request, so both
    attribute each attempt to a process. *)
let identity generation = "activity-idempotency-generation-" ^ generation

(** The order of the scenario: two lines with the same account and amount,
    under an order ID unique to this run of the fixture. *)
let order_of queue : Idempotent_payments.order =
  { order_id = queue; account = "acct-ada"; lines = [ 2500; 2500 ] }

(** Generation 1's hook: after line 1 is committed, kill this process at
    once with [SIGKILL], so no completion, failure or graceful shutdown path
    runs. Any other answer (which the scenario never produces on generation
    1) is let through and fails the checks later. *)
let crash_after_commit = function
  | Payment_ledger.Committed record ->
      Printf.printf "generation 1: %s committed by %s; killing pid %d before completion\n%!"
        record.key
        (Payment_ledger.describe_correlation record.committed_by)
        (Unix.getpid ());
      Unix.kill (Unix.getpid ()) Sys.sigkill;
      (* SIGKILL cannot be caught; this point is never reached. *)
      Ok ()
  | outcome ->
      Printf.printf "generation 1: unexpected %s answer\n%!"
        (Payment_ledger.string_of_outcome_kind (Payment_ledger.outcome_kind outcome));
      Ok ()

(** Runs one worker generation against the ledger in [ledger_directory]
    until it is killed or terminated. Generation ["1"] kills itself after
    its first committed posting. *)
let worker address queue generation ledger_directory =
  let ledger = ledger_get (Payment_ledger.open_ledger ledger_directory) in
  let after_commit = if generation = "1" then Some crash_after_commit else None in
  let activity =
    Idempotent_payments.post_line ~ledger ~worker:(identity generation) ?after_commit ()
  in
  let worker =
    get
      (Worker.create ~identity:(identity generation) ~target_url:address ~namespace
         ~task_queue:queue
         ~workflows:[ Worker.workflow Idempotent_payments.post_order_workflow ]
         ~activities:[ Worker.activity activity ] ())
  in
  Printf.printf "generation %s: worker running in pid %d\n%!" generation (Unix.getpid ());
  get (Worker.run worker)

(** Writes [contents] to [path] through a rename, so a reader never sees a
    partially written file. *)
let write_atomically path contents =
  let partial = path ^ ".partial" in
  Out_channel.with_open_bin partial (fun channel -> output_string channel contents);
  Sys.rename partial path

(** Path of one generation's log in the evidence directory. *)
let log_path evidence generation =
  Filename.concat evidence (Printf.sprintf "generation-%s.log" generation)

(** The shared ledger directory inside the evidence directory. *)
let ledger_path evidence = Filename.concat evidence "ledger"

(** Spawns a worker generation as a separate process, with its stdout and
    stderr in its own evidence log, so a replacement shares no OCaml state
    with the process it replaces; only the ledger directory is shared. *)
let spawn address queue generation evidence =
  let log =
    Unix.openfile (log_path evidence generation)
      [ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC; Unix.O_CLOEXEC ]
      0o644
  in
  Fun.protect ~finally:(fun () -> Unix.close log) (fun () ->
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "worker"; address; queue; generation; ledger_path evidence |]
      Unix.stdin log log)

(** Returns [Some status] once [pid] has exited, reaping it, and [None]
    while it runs. *)
let exited pid =
  match Unix.waitpid [ Unix.WNOHANG ] pid with
  | 0, _ -> None
  | _, status -> Some status

(** Sends [signal] to a still-running worker and reaps it. Used only for
    cleanup. *)
let stop signal pid =
  if exited pid = None then begin
    (try Unix.kill pid signal with Unix.Unix_error _ -> ());
    try ignore (Unix.waitpid [] pid) with Unix.Unix_error _ -> ()
  end

(** Seconds allowed for any one observation to become true. Temporal may
    wait for a dead worker's sticky queue to time out before redispatching
    a workflow task, so this is deliberately generous. *)
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
          Printf.printf "activity idempotency: waiting for %s: %s\n%!" label message;
        Unix.sleepf 0.1;
        poll (Some message)
    | Error message -> failwith (Printf.sprintf "timed out waiting for %s: %s" label message)
  in
  poll None

(** Renders a correlation as a JSON object. *)
let correlation_json (correlation : Payment_ledger.correlation) =
  `Assoc
    [ ("workflow_id", `String correlation.workflow_id); ("run_id", `String correlation.run_id);
      ("activity_id", `String correlation.activity_id); ("attempt", `Int correlation.attempt);
      ("worker", `String correlation.worker) ]

(** Renders a [Client.describe] result as JSON evidence: the run, its
    status, and its pending activities with their attempts and states. *)
let description_json (description : Client.execution_description) =
  let status =
    match description.status with
    | `Running -> "running" | `Completed -> "completed" | `Failed -> "failed"
    | `Canceled -> "canceled" | `Terminated -> "terminated"
    | `Continued_as_new -> "continued-as-new" | `Timed_out -> "timed-out"
    | `Paused -> "paused" | `Unknown value -> Printf.sprintf "unknown-%d" value
  in
  let state = function
    | `Scheduled -> "scheduled" | `Started -> "started"
    | `Cancel_requested -> "cancel-requested" | `Paused -> "paused"
    | `Pause_requested -> "pause-requested" | `Unknown value -> Printf.sprintf "unknown-%d" value
  in
  `Assoc
    [ ("workflow_id", `String description.execution.workflow_id);
      ("run_id", `String description.execution.run_id); ("status", `String status);
      ( "pending_activities",
        `List
          (List.map
             (fun (activity : Client.pending_activity) ->
               `Assoc
                 [ ("activity_id", `String activity.activity_id);
                   ("activity_type", `String activity.activity_type);
                   ("attempt", `Int activity.attempt); ("state", `String (state activity.state)) ])
             description.pending_activities) ) ]

(** Saves one JSON document in the evidence directory. *)
let save evidence name document =
  write_atomically (Filename.concat evidence name)
    (Yojson.Safe.pretty_to_string document ^ "\n")

(** The correlation the ledger must record for [attempt] of the line whose
    activity is [activity_id], in the exact run, on [generation]. *)
let expected_correlation handle ~activity_id ~attempt ~generation : Payment_ledger.correlation =
  { workflow_id = Client.workflow_id handle; run_id = Option.get (Client.run_id handle);
    activity_id; attempt; worker = identity generation }

(** Renders a journal for a failure message. *)
let describe_journal entries =
  String.concat "; "
    (List.map
       (fun (entry : Payment_ledger.journal_entry) ->
         Printf.sprintf "%s %s by %s" entry.journal_key
           (Payment_ledger.string_of_outcome_kind entry.answer)
           (Payment_ledger.describe_correlation entry.requested_by))
       entries)

(** Checks the ledger right after generation 1 died: exactly line 1's
    record, committed by attempt 1 on generation 1 in the exact run, and one
    journal entry for it. *)
let check_ledger_after_crash ledger handle (order : Idempotent_payments.order) =
  let line_1 = Idempotent_payments.line_key ~order_id:order.order_id ~line:1 in
  let committer =
    expected_correlation handle ~activity_id:(Idempotent_payments.line_activity_id 1) ~attempt:1
      ~generation:"1"
  in
  (match ledger_get (Payment_ledger.records ledger) with
   | [ record ] when record.key = line_1 && record.committed_by = committer -> ()
   | records ->
       failwith
         (Printf.sprintf "after the crash the ledger holds %d records, expected line 1 by %s"
            (List.length records) (Payment_ledger.describe_correlation committer)));
  match ledger_get (Payment_ledger.journal ledger) with
  | [ { journal_key; requested_by; answer = Committed_kind } ]
    when journal_key = line_1 && requested_by = committer -> ()
  | entries -> failwith ("unexpected journal after the crash: " ^ describe_journal entries)

(** Checks the final ledger: two records with identical postings under
    distinct keys, and exactly the three expected requests. *)
let check_final_ledger ledger handle (order : Idempotent_payments.order) =
  let key line = Idempotent_payments.line_key ~order_id:order.order_id ~line in
  let correlation ~line ~attempt ~generation =
    expected_correlation handle ~activity_id:(Idempotent_payments.line_activity_id line) ~attempt
      ~generation
  in
  let posting : Payment_ledger.posting = { account = order.account; amount_cents = 2500 } in
  let expected_records : Payment_ledger.record list =
    [ { key = key 1; posting; committed_by = correlation ~line:1 ~attempt:1 ~generation:"1" };
      { key = key 2; posting; committed_by = correlation ~line:2 ~attempt:1 ~generation:"2" } ]
  in
  let records = ledger_get (Payment_ledger.records ledger) in
  if records <> expected_records then
    failwith
      (Printf.sprintf "the final ledger holds %d records: %s" (List.length records)
         (String.concat "; "
            (List.map
               (fun (record : Payment_ledger.record) ->
                 record.key ^ " by " ^ Payment_ledger.describe_correlation record.committed_by)
               records)));
  let expected_journal : Payment_ledger.journal_entry list =
    [ { journal_key = key 1; requested_by = correlation ~line:1 ~attempt:1 ~generation:"1";
        answer = Committed_kind };
      { journal_key = key 1; requested_by = correlation ~line:1 ~attempt:2 ~generation:"2";
        answer = Duplicate_kind };
      { journal_key = key 2; requested_by = correlation ~line:2 ~attempt:1 ~generation:"2";
        answer = Committed_kind } ]
  in
  let journal = ledger_get (Payment_ledger.journal ledger) in
  if journal <> expected_journal then
    failwith ("unexpected final journal: " ^ describe_journal journal)

(** Barrier: with no worker polling, Temporal has timed out line 1's lost
    first attempt and scheduled attempt 2 while the run is open. Returns the
    description. *)
let attempt_two_pending handle () =
  match Client.describe handle with
  | Error error -> Error ("describe failed: " ^ Error.message error)
  | Ok description -> (
      match (description.status, description.pending_activities) with
      | `Running, [ { activity_id; attempt = 2; _ } ]
        when activity_id = Idempotent_payments.line_activity_id 1 ->
          Ok description
      | `Running, [ { activity_id; attempt; _ } ] ->
          Error (Printf.sprintf "pending %s is at attempt %d" activity_id attempt)
      | `Running, [] -> Error "no pending activity"
      | `Running, pending -> Error (Printf.sprintf "%d pending activities" (List.length pending))
      | _ -> Error "the run is not running")

(** Deadline for one CLI invocation, matching the other history fixtures. *)
let cli_timeout = "30s"

(** Runs [temporal workflow show] for the exact run and parses its JSON
    output, without a shell. A failed read is an [Error] so callers can
    retry it. *)
let history_json cli address handle =
  let args =
    [| cli; "--address"; address; "--namespace"; namespace; "--command-timeout"; cli_timeout;
       "workflow"; "show"; "--workflow-id"; Client.workflow_id handle; "--run-id";
       Option.get (Client.run_id handle); "--output"; "json" |]
  in
  let input = Unix.open_process_args_in cli args in
  let document =
    match Yojson.Safe.from_channel input with
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

(** Reads an integer field that protobuf JSON may render as a number or a
    string; a missing or malformed field is [None]. *)
let int_field name json =
  match Yojson.Safe.Util.member name json with
  | `Int value -> Some value
  | `String value -> int_of_string_opt value
  | _ -> None

(** Reads a string field; a missing or non-string field is [None]. *)
let string_field name json =
  match Yojson.Safe.Util.member name json with `String value -> Some value | _ -> None

(** Returns an event's type without its [EVENT_TYPE_] prefix. *)
let event_type event =
  let name = Option.value (string_field "eventType" event) ~default:"" in
  let prefix = "EVENT_TYPE_" in
  if String.starts_with ~prefix name then
    String.sub name (String.length prefix) (String.length name - String.length prefix)
  else name

(** The durable, non-workflow-task events the run must record, in order.
    Workflow-task events are excluded because a dead worker's sticky queue
    legitimately adds a workflow-task timeout before redispatch. A retried
    activity records only its final attempt's started event, so the lost
    attempt adds no event of its own. *)
let expected_decisions =
  [ "WORKFLOW_EXECUTION_STARTED"; "ACTIVITY_TASK_SCHEDULED"; "ACTIVITY_TASK_STARTED";
    "ACTIVITY_TASK_COMPLETED"; "ACTIVITY_TASK_SCHEDULED"; "ACTIVITY_TASK_STARTED";
    "ACTIVITY_TASK_COMPLETED"; "WORKFLOW_EXECUTION_COMPLETED" ]

(** Checks the exact run's history: the decision sequence, the scheduled
    activity IDs, and each started event's attempt, identity and (for line
    1) timeout predecessor. *)
let check_history events =
  let decisions =
    List.filter_map
      (fun event ->
        let kind = event_type event in
        if String.starts_with ~prefix:"WORKFLOW_TASK_" kind then None else Some kind)
      events
  in
  Printf.printf "activity idempotency history: %s\n%!" (String.concat ", " decisions);
  if decisions <> expected_decisions then
    failwith
      (Printf.sprintf "history\n    %s\n  expected\n    %s" (String.concat "\n    " decisions)
         (String.concat "\n    " expected_decisions));
  let attributes kind name =
    List.filter_map
      (fun event ->
        if event_type event = kind then Some (event, Yojson.Safe.Util.member name event) else None)
      events
  in
  let scheduled = attributes "ACTIVITY_TASK_SCHEDULED" "activityTaskScheduledEventAttributes" in
  let activity_of_scheduled id =
    List.find_map
      (fun (event, attributes) ->
        if int_field "eventId" event = Some id then string_field "activityId" attributes else None)
      scheduled
  in
  let started = attributes "ACTIVITY_TASK_STARTED" "activityTaskStartedEventAttributes" in
  let summary =
    List.map
      (fun (_, attributes) ->
        ( Option.bind (int_field "scheduledEventId" attributes) activity_of_scheduled,
          int_field "attempt" attributes,
          string_field "identity" attributes ))
      started
  in
  let line activity = Some (Idempotent_payments.line_activity_id activity) in
  if summary <> [ (line 1, Some 2, Some (identity "2")); (line 2, Some 1, Some (identity "2")) ]
  then
    failwith
      ("unexpected started activity events: "
      ^ Yojson.Safe.to_string (`List (List.map snd started)));
  let line_1_failure =
    match started with
    | (_, attributes) :: _ ->
        String.lowercase_ascii
          (Yojson.Safe.to_string (Yojson.Safe.Util.member "lastFailure" attributes))
    | [] -> ""
  in
  let contains needle =
    let length = String.length needle in
    let rec search index =
      index + length <= String.length line_1_failure
      && (String.sub line_1_failure index length = needle || search (index + 1))
    in
    search 0
  in
  if not (contains "timeout") then
    failwith ("line 1's attempt 2 does not record a timeout predecessor: " ^ line_1_failure);
  List.iter
    (fun (_, attributes) ->
      if string_field "identity" attributes <> Some (identity "2") then
        failwith ("an activity did not complete on generation 2: " ^ Yojson.Safe.to_string attributes))
    (attributes "ACTIVITY_TASK_COMPLETED" "activityTaskCompletedEventAttributes")

(** Writes [correlation.json]: the exact run, both generations' process IDs,
    and every ledger record and request with its full correlation. *)
let save_correlation evidence ledger handle ~first ~second =
  let records = ledger_get (Payment_ledger.records ledger) in
  let journal = ledger_get (Payment_ledger.journal ledger) in
  save evidence "correlation.json"
    (`Assoc
      [ ("workflow_id", `String (Client.workflow_id handle));
        ("run_id", `String (Option.get (Client.run_id handle)));
        ( "generations",
          `Assoc
            [ ("1", `Assoc [ ("identity", `String (identity "1")); ("pid", `Int first) ]);
              ("2", `Assoc [ ("identity", `String (identity "2")); ("pid", `Int second) ]) ] );
        ( "records",
          `List
            (List.map
               (fun (record : Payment_ledger.record) ->
                 `Assoc
                   [ ("key", `String record.key); ("account", `String record.posting.account);
                     ("amount_cents", `Int record.posting.amount_cents);
                     ("committed_by", correlation_json record.committed_by) ])
               records) );
        ( "requests",
          `List
            (List.map
               (fun (entry : Payment_ledger.journal_entry) ->
                 `Assoc
                   [ ("key", `String entry.journal_key);
                     ("answer", `String (Payment_ledger.string_of_outcome_kind entry.answer));
                     ("requested_by", correlation_json entry.requested_by) ])
               journal) ) ])

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

(** Creates the evidence directory if needed and removes the ledger, logs
    and snapshots of an earlier run, so every check observes this run
    only. *)
let prepare_evidence = function
  | Some directory ->
      (try Unix.mkdir directory 0o755 with Unix.Unix_error (Unix.EEXIST, _, _) -> ());
      let remove path = if Sys.file_exists path then Sys.remove path in
      let ledger = ledger_path directory in
      if Sys.file_exists ledger then begin
        Array.iter (fun name -> remove (Filename.concat ledger name)) (Sys.readdir ledger);
        Sys.rmdir ledger
      end;
      List.iter
        (fun name -> remove (Filename.concat directory name))
        [ "generation-1.log"; "generation-2.log"; "describe-after-crash.json";
          "describe-completed.json"; "history.json"; "correlation.json" ];
      directory
  | None -> Filename.temp_dir "activity-idempotency-" ""

(** Runs the scenario with protected cleanup of both worker generations and
    the run. *)
let check address cli evidence =
  if not (String.starts_with ~prefix:"http://" address) then
    failwith "this fixture requires an HTTP development server";
  let cli_address = String.sub address 7 (String.length address - 7) in
  let evidence = prepare_evidence evidence in
  Printf.printf "activity idempotency: evidence in %s\n%!" evidence;
  let ledger = ledger_get (Payment_ledger.open_ledger (ledger_path evidence)) in
  let queue =
    Printf.sprintf "activity-idempotency-%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  let order = order_of queue in
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
          get
            (Client.start client ~workflow:Idempotent_payments.post_order_workflow
               ~task_queue:queue ~id:queue ~input:order ())
        in
        (* Terminating an already completed run is a harmless typed error. *)
        Fun.protect ~finally:(fun () -> ignore (Client.terminate handle)) (fun () ->
          let status =
            await "generation 1 to die after committing line 1" (fun () ->
              match exited first with
              | Some status -> Ok status
              | None -> Error "generation 1 is still running")
          in
          unreaped_first := None;
          (match status with
           | Unix.WSIGNALED signal when signal = Sys.sigkill ->
               Printf.printf
                 "activity idempotency: generation 1 (pid %d) died of SIGKILL after its commit\n%!"
                 first
           | Unix.WEXITED code ->
               failwith (Printf.sprintf "generation 1 exited with %d instead of dying" code)
           | Unix.WSIGNALED _ | Unix.WSTOPPED _ ->
               failwith "generation 1 did not die of SIGKILL");
          check_ledger_after_crash ledger handle order;
          let pending =
            await "Temporal to schedule line 1's attempt 2" (attempt_two_pending handle)
          in
          save evidence "describe-after-crash.json" (description_json pending);
          let second = spawn address queue "2" evidence in
          replacement := Some second;
          (match get (Client.wait handle) with
           | Client.Completed { output; _ }
             when output = Idempotent_payments.expected_output order -> ()
           | Client.Completed { output; _ } -> failwith (Printf.sprintf "result %S" output)
           | _ -> failwith "the run did not complete");
          save evidence "describe-completed.json" (description_json (get (Client.describe handle)));
          save_correlation evidence ledger handle ~first ~second;
          check_final_ledger ledger handle order;
          let history =
            await "history of the completed run" (fun () -> history_json cli cli_address handle)
          in
          save evidence "history.json" history;
          check_history Yojson.Safe.Util.(history |> member "events" |> to_list);
          Printf.printf
            "activity idempotency live regression: ok (line 1 committed once by attempt 1 on \
             generation 1, redelivered as attempt 2 on generation 2 and answered as a \
             duplicate; identical line 2 posted under its own key)\n%!")))

(** Dispatches the two roles. An overall alarm bounds the driver below the
    controller's per-suite limit; protected cleanup reaps the workers. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "worker"; address; queue; generation; ledger_directory ] ->
      worker address queue generation ledger_directory
  | [ _; "check"; address; cli ] | [ _; "check"; address; cli; _ ] as arguments ->
      let evidence = match arguments with [ _; _; _; _; directory ] -> Some directory | _ -> None in
      Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ -> failwith "fixture timed out"));
      ignore (Unix.alarm 170);
      Fun.protect ~finally:(fun () -> ignore (Unix.alarm 0)) (fun () -> check address cli evidence)
  | _ ->
      prerr_endline
        "usage: regression.exe check http://HOST:PORT /path/to/temporal [EVIDENCE_DIR]";
      exit 2

(** Coverage for activity timeouts in [Temporal.Testing] (#1160). A
    registration's [durations] make each attempt take virtual time, and the
    environment then enforces start-to-close, schedule-to-close and
    heartbeat timeouts against it. Each test asserts the typed error the
    workflow observes, in the shape the native worker reports (#1143), and
    the virtual time that elapsed before the workflow saw it. *)

module T = Temporal
open T.Result_syntax

(** Fails the test with [label] when two values differ, printing both. *)
let expect label expected actual =
  if not (String.equal expected actual) then
    failwith (Printf.sprintf "%s: expected %S, got %S" label expected actual)

(** Fails the test with [label] when two virtual durations differ. *)
let expect_ms label expected actual =
  if not (Int64.equal expected actual) then
    failwith (Printf.sprintf "%s: expected %Ld ms, got %Ld ms" label expected actual)

(** Unwraps a successful result, failing the test with the error message. *)
let ok label = function
  | Ok value -> value
  | Error error -> failwith (label ^ " failed: " ^ T.Error.message error)

(** Milliseconds since the Unix epoch of a public instant. *)
let epoch_ms time =
  Int64.add
    (Int64.mul (T.Time.seconds time) 1_000L)
    (Int64.of_int (T.Time.nanoseconds time / 1_000_000))

(** The attempt numbers [slow] has run, most recent first. *)
let attempts = ref []

(** Heartbeats ["attempt-N"] and succeeds with ["done-N"] on attempt N, so a
    test can tell which attempt's heartbeat or result reached the workflow. *)
let slow =
  T.Activity.define_with_context ~name:"slow" ~input:T.Codec.unit
    ~output:T.Codec.string (fun context () ->
      let attempt =
        T.Activity.Info.attempt (ok "activity info" (T.Activity.Context.info context))
      in
      attempts := attempt :: !attempts;
      ok "heartbeat"
        (T.Activity.Context.heartbeat context T.Codec.string
           (Printf.sprintf "attempt-%d" attempt));
      Ok (Printf.sprintf "done-%d" attempt))

(** Fails retryably on every attempt. *)
let failing =
  T.Activity.define ~name:"failing" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () -> Error (T.Error.make ~category:`Activity ~message:"transient" ()))

(** Renders an activity outcome as one comparable line: the value, or the
    error's category, timeout type, retryability, application type and
    decoded heartbeat details. *)
let describe = function
  | Ok value -> "ok " ^ value
  | Error error ->
      let timeout =
        match T.Error.timeout_type error with
        | Some `Start_to_close -> "start_to_close"
        | Some `Schedule_to_close -> "schedule_to_close"
        | Some `Heartbeat -> "heartbeat"
        | Some `Schedule_to_start -> "schedule_to_start"
        | Some `Unspecified -> "unspecified"
        | None -> "none"
      in
      let heartbeats =
        List.map
          (fun payload -> ok "heartbeat decode" (T.Codec.decode T.Codec.string payload))
          (T.Error.last_heartbeat_details error)
      in
      let view = T.Error.view error in
      Printf.sprintf "%s timeout=%s non_retryable=%b type=%s heartbeats=[%s]"
        (T.Error.kind error) timeout view.non_retryable
        (Option.value view.error_type ~default:"-")
        (String.concat "," heartbeats)

(** A retry policy with a fixed initial interval, doubling backoff and an
    attempt budget. *)
let retry_policy ?non_retryable_error_types ~initial_ms ~maximum_attempts () =
  ok "retry policy"
    (T.Retry_policy.make ?non_retryable_error_types
       ~initial_interval:(T.Duration.of_ms initial_ms)
       ~backoff_coefficient:2.0 ~maximum_interval:(T.Duration.of_ms 60_000L)
       ~maximum_attempts ())

(** Defines a workflow named [name] that runs [run] and reports the
    activity's outcome through {!describe}. *)
let probe name run =
  T.Workflow.define ~name ~input:T.Codec.unit ~output:T.Codec.string (fun () ->
      Ok (describe (run ())))

(** Runs [workflow] in a fresh environment with [activities] and returns its
    report and the virtual milliseconds that elapsed. *)
let run_probe ~activities workflow =
  let environment =
    ok "create"
      (T.Testing.create ~workflows:[ T.Testing.workflow workflow ] ~activities ())
  in
  Fun.protect
    ~finally:(fun () -> T.Testing.shutdown environment)
    (fun () ->
      attempts := [];
      let started = epoch_ms (T.Testing.now environment) in
      let report = ok "execute" (T.Testing.execute environment workflow ()) in
      (report, Int64.sub (epoch_ms (T.Testing.now environment)) started))

let seconds = T.Duration.of_seconds
let minutes = T.Duration.of_minutes

(** The acceptance case: an attempt that outlasts its start-to-close timeout
    fails at the deadline with the server's typed timeout, after exactly
    that much virtual time, and an exhausted budget is non-retryable. *)
let test_start_to_close () =
  let workflow =
    probe "start_to_close" (fun () ->
        T.Activity.execute ~start_to_close_timeout:(minutes 1L)
          ~retry_policy:(retry_policy ~initial_ms:1_000L ~maximum_attempts:1 ())
          slow ())
  in
  let report, elapsed =
    run_probe ~activities:[ T.Testing.activity ~durations:[ minutes 5L ] slow ] workflow
  in
  expect "start-to-close error"
    "activity timeout=start_to_close non_retryable=true type=- heartbeats=[attempt-1]"
    report;
  expect_ms "start-to-close elapsed" 60_000L elapsed

(** A timed-out attempt is retried under the policy, and an attempt that
    finishes within its timeout succeeds after its own duration. *)
let test_retry_after_timeout () =
  let workflow =
    probe "retry_after_timeout" (fun () ->
        T.Activity.execute ~start_to_close_timeout:(minutes 1L)
          ~retry_policy:(retry_policy ~initial_ms:1_000L ~maximum_attempts:3 ())
          slow ())
  in
  let report, elapsed =
    run_probe
      ~activities:[ T.Testing.activity ~durations:[ minutes 5L; seconds 2L ] slow ]
      workflow
  in
  expect "retried attempt result" "ok done-2" report;
  expect_ms "timeout, backoff, then second attempt" 63_000L elapsed

(** A duration within every timeout delays the result without failing it. *)
let test_duration_without_timeout () =
  let workflow = probe "duration_only" (fun () -> T.Activity.execute slow ()) in
  let report, elapsed =
    run_probe ~activities:[ T.Testing.activity ~durations:[ seconds 59L ] slow ] workflow
  in
  expect "slow but in time" "ok done-1" report;
  expect_ms "attempt duration" 59_000L elapsed

(** Schedule-to-close is measured from the first scheduling: the second
    attempt's start-to-close deadline lies past it, so the activity ends
    with the retryable schedule-to-close timeout. *)
let test_schedule_to_close_during_attempt () =
  let workflow =
    probe "schedule_to_close_attempt" (fun () ->
        T.Activity.execute ~start_to_close_timeout:(minutes 1L)
          ~schedule_to_close_timeout:(seconds 90L)
          ~retry_policy:(retry_policy ~initial_ms:1_000L ~maximum_attempts:0 ())
          slow ())
  in
  let report, elapsed =
    run_probe ~activities:[ T.Testing.activity ~durations:[ minutes 5L ] slow ] workflow
  in
  expect "schedule-to-close error"
    "activity timeout=schedule_to_close non_retryable=false type=- heartbeats=[attempt-2]"
    report;
  expect_ms "schedule-to-close deadline" 90_000L elapsed;
  expect "two attempts ran" "2,1"
    (String.concat "," (List.map string_of_int !attempts))

(** A backoff that would end at or past the schedule-to-close deadline is
    cut short by the timeout at the deadline, even for an activity whose
    attempts take no time. *)
let test_schedule_to_close_during_backoff () =
  let workflow =
    probe "schedule_to_close_backoff" (fun () ->
        T.Activity.execute ~schedule_to_close_timeout:(seconds 10L)
          ~retry_policy:(retry_policy ~initial_ms:4_000L ~maximum_attempts:0 ())
          failing ())
  in
  let report, elapsed = run_probe ~activities:[ T.Testing.activity failing ] workflow in
  expect "schedule-to-close during backoff"
    "activity timeout=schedule_to_close non_retryable=false type=- heartbeats=[]"
    report;
  expect_ms "deadline, not the 12 s retry" 10_000L elapsed

(** Heartbeats happen at the attempt's start, so an attempt longer than its
    heartbeat timeout fails at that timeout with the heartbeat's details. *)
let test_heartbeat_timeout () =
  let workflow =
    probe "heartbeat" (fun () ->
        T.Activity.execute ~start_to_close_timeout:(minutes 1L)
          ~heartbeat_timeout:(seconds 5L)
          ~retry_policy:(retry_policy ~initial_ms:1_000L ~maximum_attempts:1 ())
          slow ())
  in
  let report, elapsed =
    run_probe ~activities:[ T.Testing.activity ~durations:[ seconds 30L ] slow ] workflow
  in
  expect "heartbeat error"
    "activity timeout=heartbeat non_retryable=true type=- heartbeats=[attempt-1]"
    report;
  expect_ms "heartbeat deadline" 5_000L elapsed

(** An ordinary final failure now carries the [`Application] origin, as the
    native worker classifies an activity's application failure. *)
let test_application_origin () =
  let workflow =
    probe "application_origin" (fun () ->
        match
          T.Activity.execute
            ~retry_policy:(retry_policy ~initial_ms:1_000L ~maximum_attempts:1 ())
            failing ()
        with
        | Error error when T.Error.origin error = Some `Application -> Ok "application"
        | Error error -> Error error
        | Ok value -> Ok value)
  in
  let report, _ = run_probe ~activities:[ T.Testing.activity failing ] workflow in
  expect "application origin" "ok application" report

(** A running attempt is cancelled at once under [Try_cancel], before its
    duration elapses. *)
let test_cancel_running_attempt () =
  let workflow =
    probe "cancel_running" (fun () ->
        let handle = T.Activity.start_handle slow () in
        let* () = T.Workflow.sleep (seconds 1L) in
        let* () = T.Activity.cancel handle in
        T.Future.await (T.Activity.future handle))
  in
  let report, elapsed =
    run_probe ~activities:[ T.Testing.activity ~durations:[ seconds 30L ] slow ] workflow
  in
  expect "cancelled attempt" "cancelled timeout=none non_retryable=false type=- heartbeats=[]"
    report;
  expect_ms "cancelled at the request" 1_000L elapsed

(** Under [Wait_cancellation_completed] the attempt cannot observe the
    request, so its own outcome arrives when the attempt finishes. *)
let test_wait_cancellation_completed () =
  let workflow =
    probe "cancel_waits" (fun () ->
        let handle =
          T.Activity.start_handle ~cancellation_type:T.Activity.Wait_cancellation_completed
            slow ()
        in
        let* () = T.Workflow.sleep (seconds 1L) in
        let* () = T.Activity.cancel handle in
        T.Future.await (T.Activity.future handle))
  in
  let report, elapsed =
    run_probe ~activities:[ T.Testing.activity ~durations:[ seconds 30L ] slow ] workflow
  in
  expect "waited for the attempt" "ok done-1" report;
  expect_ms "attempt duration" 30_000L elapsed

(** [mock_activity] accepts the same durations as {!T.Testing.activity}. *)
let test_mock_durations () =
  let workflow =
    probe "mock_durations" (fun () ->
        T.Activity.execute ~start_to_close_timeout:(seconds 10L)
          ~retry_policy:(retry_policy ~initial_ms:1_000L ~maximum_attempts:2 ())
          slow ())
  in
  let report, elapsed =
    run_probe
      ~activities:
        [
          T.Testing.mock_activity ~durations:[ seconds 20L; seconds 3L ] slow
            (fun () -> Ok "mocked");
        ]
      workflow
  in
  expect "mocked retry" "ok mocked" report;
  expect_ms "timeout, backoff, attempt" 14_000L elapsed

(** Fails with an error that claims a schedule-to-close timeout origin. *)
let claims_timeout =
  T.Activity.define ~name:"claims_timeout" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      attempts := 0 :: !attempts;
      Error
        (T.Error.make ~category:`Activity ~origin:(`Timeout `Schedule_to_close)
           ~message:"not really a timeout" ()))

(** An error a callback returns is an application failure whatever origin
    it carries (Codex review on #1206): it is retried under the policy and
    reaches the workflow with the [`Application] origin, as on a native
    worker, which does not transmit the origin. *)
let test_callback_origin_is_ignored () =
  let workflow =
    probe "callback_origin" (fun () ->
        match
          T.Activity.execute
            ~retry_policy:(retry_policy ~initial_ms:1_000L ~maximum_attempts:3 ())
            claims_timeout ()
        with
        | Error error when T.Error.origin error = Some `Application ->
            Ok "application"
        | Error error -> Error error
        | Ok value -> Ok value)
  in
  let report, elapsed =
    run_probe ~activities:[ T.Testing.activity claims_timeout ] workflow
  in
  expect "callback origin" "ok application" report;
  expect "all attempts ran" "3" (string_of_int (List.length !attempts));
  expect_ms "two backoffs" 3_000L elapsed

(** Non-retryable error types name application failures, so a timeout is
    retried even when the policy lists the category name ["activity"]
    (Codex review on #1206). *)
let test_timeout_ignores_error_types () =
  let workflow =
    probe "timeout_error_types" (fun () ->
        T.Activity.execute ~start_to_close_timeout:(minutes 1L)
          ~retry_policy:
            (retry_policy ~non_retryable_error_types:[ "activity" ]
               ~initial_ms:1_000L ~maximum_attempts:2 ())
          slow ())
  in
  let report, _ =
    run_probe
      ~activities:[ T.Testing.activity ~durations:[ minutes 5L; seconds 0L ] slow ]
      workflow
  in
  expect "timeout retried" "ok done-2" report

(** A zero heartbeat timeout means unset, as on a native worker, so it
    never expires (Codex review on #1206). *)
let test_zero_heartbeat_timeout_is_unset () =
  let workflow =
    probe "zero_heartbeat" (fun () ->
        T.Activity.execute ~heartbeat_timeout:(T.Duration.of_ms 0L) slow ())
  in
  let report, elapsed =
    run_probe ~activities:[ T.Testing.activity ~durations:[ seconds 5L ] slow ] workflow
  in
  expect "zero heartbeat timeout" "ok done-1" report;
  expect_ms "attempt duration" 5_000L elapsed

(** Runs every timeout scenario as one dune test. *)
let () =
  test_start_to_close ();
  test_retry_after_timeout ();
  test_duration_without_timeout ();
  test_schedule_to_close_during_attempt ();
  test_schedule_to_close_during_backoff ();
  test_heartbeat_timeout ();
  test_application_origin ();
  test_cancel_running_attempt ();
  test_wait_cancellation_completed ();
  test_mock_durations ();
  test_callback_origin_is_ignored ();
  test_timeout_ignores_error_types ();
  test_zero_heartbeat_timeout_is_unset ()

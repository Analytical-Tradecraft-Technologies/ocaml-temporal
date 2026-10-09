(** Public validation, defaults and inspection of the worker resource and
    shutdown options (#498). Every invalid value must be a typed defect
    returned by [Worker.Options.make], before any native resource exists, and
    every accessor must report the effective value a worker would use. *)

module Options = Temporal.Worker.Options

(** Milliseconds as a public duration. *)
let ms = Temporal.Duration.of_ms

(** Extracts a successful result or fails with its message. *)
let unwrap = function
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Expects a defect whose message contains [fragment]. *)
let expect_defect fragment = function
  | Ok _ -> failwith ("invalid worker options were accepted: " ^ fragment)
  | Error error ->
      let message = Temporal.Error.message error in
      if Temporal.Error.kind error <> "defect" then
        failwith ("expected a defect, got: " ^ message);
      let contains =
        let fragment_length = String.length fragment in
        let rec search index =
          index + fragment_length <= String.length message
          && (String.equal (String.sub message index fragment_length) fragment
             || search (index + 1))
        in
        search 0
      in
      if not contains then
        failwith (Printf.sprintf "expected %S in %S" fragment message)

(** Compares durations by their exact millisecond value. *)
let same_duration expected actual =
  Int64.equal (Temporal.Duration.to_ms expected) (Temporal.Duration.to_ms actual)

(** Omitted settings report the documented effective defaults, which match
    the values the native worker has always used and Core's own defaults. *)
let test_defaults () =
  let check options =
    assert (Options.max_cached_workflows options = None);
    assert (Options.max_concurrent_workflow_tasks options = 1_000);
    assert (Options.workflow_task_pollers options = Options.Fixed 2);
    assert (
      same_duration (ms 10_000L)
        (Options.sticky_queue_schedule_to_start_timeout options));
    assert (same_duration (ms 30_000L) (Options.graceful_shutdown_period options));
    assert (
      same_duration (ms 60_000L) (Options.max_heartbeat_throttle_interval options));
    assert (
      same_duration (ms 30_000L)
        (Options.default_heartbeat_throttle_interval options));
    assert (Options.max_worker_activities_per_second options = None);
    assert (Options.max_task_queue_activities_per_second options = None)
  in
  check Options.default;
  check (unwrap (Options.make ()))

(** Every explicit setting is retained and reported unchanged. *)
let test_explicit_settings () =
  let options =
    unwrap
      (Options.make ~max_cached_workflows:4 ~max_concurrent_workflow_tasks:8
         ~workflow_task_pollers:
           (Options.Autoscaling { minimum = 2; maximum = 6; initial = 3 })
         ~sticky_queue_schedule_to_start_timeout:(ms 2_500L)
         ~graceful_shutdown_period:(ms 0L)
         ~max_heartbeat_throttle_interval:(ms 20_000L)
         ~default_heartbeat_throttle_interval:(ms 5_000L)
         ~max_worker_activities_per_second:2.5
         ~max_task_queue_activities_per_second:40.5 ())
  in
  assert (Options.max_cached_workflows options = Some 4);
  assert (Options.max_concurrent_workflow_tasks options = 8);
  assert (
    Options.workflow_task_pollers options
    = Options.Autoscaling { minimum = 2; maximum = 6; initial = 3 });
  assert (
    same_duration (ms 2_500L)
      (Options.sticky_queue_schedule_to_start_timeout options));
  assert (same_duration (ms 0L) (Options.graceful_shutdown_period options));
  assert (
    same_duration (ms 20_000L) (Options.max_heartbeat_throttle_interval options));
  assert (
    same_duration (ms 5_000L) (Options.default_heartbeat_throttle_interval options));
  assert (Options.max_worker_activities_per_second options = Some 2.5);
  assert (Options.max_task_queue_activities_per_second options = Some 40.5)

(** Core caps the default heartbeat interval by the maximum, so lowering
    only the maximum lowers the reported effective default too. *)
let test_effective_heartbeat_default () =
  let options =
    unwrap (Options.make ~max_heartbeat_throttle_interval:(ms 10_000L) ())
  in
  assert (
    same_duration (ms 10_000L)
      (Options.default_heartbeat_throttle_interval options))

(** Uncached workers may use a single workflow task slot and poller, which a
    caching worker cannot: Core needs one sticky and one normal poll. *)
let test_uncached_single_slot () =
  let options =
    unwrap
      (Options.make ~max_cached_workflows:0 ~max_concurrent_workflow_tasks:1
         ~workflow_task_pollers:(Options.Fixed 1) ())
  in
  assert (Options.max_concurrent_workflow_tasks options = 1);
  expect_defect "max_concurrent_workflow_tasks must be at least 2"
    (Options.make ~max_concurrent_workflow_tasks:1 ());
  expect_defect "workflow_task_pollers must be at least 2"
    (Options.make ~max_cached_workflows:1 ~workflow_task_pollers:(Options.Fixed 1)
       ());
  expect_defect "workflow_task_pollers maximum must be at least 2"
    (Options.make
       ~workflow_task_pollers:
         (Options.Autoscaling { minimum = 1; maximum = 1; initial = 1 })
       ())

(** Zero, negative and overflowing counts are rejected. *)
let test_invalid_counts () =
  List.iter
    (fun value ->
      expect_defect "max_concurrent_workflow_tasks must be between"
        (Options.make ~max_concurrent_workflow_tasks:value ());
      expect_defect "workflow_task_pollers must be between"
        (Options.make ~workflow_task_pollers:(Options.Fixed value) ()))
    [ 0; -1; 1_000_001; max_int; min_int ];
  expect_defect "max_cached_workflows"
    (Options.make ~max_cached_workflows:max_int ())

(** Autoscaling bounds must satisfy [1 <= minimum <= initial <= maximum]. *)
let test_invalid_autoscaling () =
  let autoscaling minimum maximum initial =
    Options.make
      ~workflow_task_pollers:(Options.Autoscaling { minimum; maximum; initial })
      ()
  in
  expect_defect "minimum must be between" (autoscaling 0 4 2);
  expect_defect "maximum must be between" (autoscaling 1 max_int 2);
  expect_defect "maximum must be at least minimum" (autoscaling 5 4 4);
  expect_defect "initial must be between minimum and maximum"
    (autoscaling 2 4 1);
  expect_defect "initial must be between minimum and maximum"
    (autoscaling 2 4 5)

(** Durations are bounded to one day; only the grace period may be zero. *)
let test_invalid_durations () =
  let one_day_plus = ms 86_400_001L in
  expect_defect "sticky_queue_schedule_to_start_timeout"
    (Options.make ~sticky_queue_schedule_to_start_timeout:(ms 0L) ());
  expect_defect "sticky_queue_schedule_to_start_timeout"
    (Options.make ~sticky_queue_schedule_to_start_timeout:one_day_plus ());
  expect_defect "graceful_shutdown_period"
    (Options.make ~graceful_shutdown_period:one_day_plus ());
  expect_defect "max_heartbeat_throttle_interval"
    (Options.make ~max_heartbeat_throttle_interval:(ms 0L) ());
  expect_defect "default_heartbeat_throttle_interval"
    (Options.make ~default_heartbeat_throttle_interval:one_day_plus ());
  expect_defect
    "default_heartbeat_throttle_interval must not exceed \
     max_heartbeat_throttle_interval"
    (Options.make ~max_heartbeat_throttle_interval:(ms 1_000L)
       ~default_heartbeat_throttle_interval:(ms 1_001L) ());
  ignore
    (unwrap
       (Options.make ~graceful_shutdown_period:(ms 86_400_000L)
          ~max_heartbeat_throttle_interval:(ms 1_000L)
          ~default_heartbeat_throttle_interval:(ms 1_000L) ()))

(** Rates must be positive finite numbers. *)
let test_invalid_rates () =
  List.iter
    (fun value ->
      expect_defect "max_worker_activities_per_second"
        (Options.make ~max_worker_activities_per_second:value ());
      expect_defect "max_task_queue_activities_per_second"
        (Options.make ~max_task_queue_activities_per_second:value ()))
    [ 0.0; -0.0; -1.0; Float.nan; Float.infinity; Float.neg_infinity; 5e-324 ]

(** A worker reports the options it was created with, and the legacy
    [~max_cached_workflows] argument is folded into those options. *)
let test_worker_reports_options () =
  let workflow =
    Temporal.Workflow.define ~name:"unit.options-workflow"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  let create ?options ?max_cached_workflows () =
    unwrap
      (Temporal.Worker.create ?options ?max_cached_workflows
         ~target_url:"mock://options" ~namespace:"unit-test"
         ~task_queue:"unit-test"
         ~workflows:[ Temporal.Worker.workflow workflow ]
         ~activities:[] ())
  in
  let options =
    unwrap
      (Options.make ~graceful_shutdown_period:(ms 1_000L)
         ~max_worker_activities_per_second:3.0 ())
  in
  let worker = create ~options () in
  let reported = Temporal.Worker.options worker in
  assert (same_duration (ms 1_000L) (Options.graceful_shutdown_period reported));
  assert (Options.max_worker_activities_per_second reported = Some 3.0);
  unwrap (Temporal.Worker.shutdown worker);
  let legacy = create ~max_cached_workflows:7 () in
  assert (
    Options.max_cached_workflows (Temporal.Worker.options legacy) = Some 7);
  unwrap (Temporal.Worker.shutdown legacy)

(** Fully tuned options pass the private bridge's own validation: worker
    creation against an unreachable port reaches the client connection,
    which runs only after the native worker configuration was accepted. *)
let test_native_accepts_tuned_options () =
  let activity =
    Temporal.Activity.define ~name:"unit.options-activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  let options =
    unwrap
      (Options.make ~max_cached_workflows:4 ~max_concurrent_workflow_tasks:8
         ~workflow_task_pollers:
           (Options.Autoscaling { minimum = 2; maximum = 6; initial = 3 })
         ~sticky_queue_schedule_to_start_timeout:(ms 2_500L)
         ~graceful_shutdown_period:(ms 1_500L)
         ~max_heartbeat_throttle_interval:(ms 20_000L)
         ~default_heartbeat_throttle_interval:(ms 5_000L)
         ~max_worker_activities_per_second:2.5
         ~max_task_queue_activities_per_second:40.5 ())
  in
  match
    Temporal.Worker.create ~options ~target_url:"http://127.0.0.1:1"
      ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
      ~activities:[ Temporal.Worker.activity activity ] ()
  with
  | Ok worker ->
      ignore (Temporal.Worker.shutdown worker);
      failwith "worker connected to an unreachable port"
  | Error error ->
      let message = Temporal.Error.message error in
      if not (String.starts_with ~prefix:"client connection failed" message)
      then failwith ("tuned options failed before connecting: " ^ message)

let () =
  test_defaults ();
  test_explicit_settings ();
  test_effective_heartbeat_default ();
  test_uncached_single_slot ();
  test_invalid_counts ();
  test_invalid_autoscaling ();
  test_invalid_durations ();
  test_invalid_rates ();
  test_worker_reports_options ();
  test_native_accepts_tuned_options ();
  print_endline "worker options tests passed"

(** Live #498 regression: a worker constrained by the public resource and
    shutdown options respects them against a real Temporal Server and
    recovers after saturation.

    One worker runs with a one-workflow sticky cache, two workflow task slots
    and pollers, short sticky and shutdown timeouts, and a per-worker rate
    of one remote activity start per second. A burst of executions, each
    running one activity, saturates both limits: the cache forces evictions
    and replays, and the rate limit spaces the activity starts. Every
    execution must still complete with its own result, the observed activity
    starts must be spread out as the rate requires (an unlimited worker
    starts them all within well under a second), a later execution must
    complete once the burst has drained, and [Worker.options] must report the
    configuration the worker was created with.

    Run against a disposable Temporal Server only:
    [dune exec test/integration/worker_resource_options/regression.exe --
    check http://127.0.0.1:7233]. [TEMPORAL_NAMESPACE] defaults to
    [temporal-sdk-test]. CI runs it through
    [make test-temporal-live-regressions]. *)
open Temporal

(** Converts the public result boundary into a short fixture failure. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

let namespace =
  Sys.getenv_opt "TEMPORAL_NAMESPACE"
  |> Option.value ~default:"temporal-sdk-test"

(** Executions in the saturating burst. Six activities at one per second
    need several seconds, which an unlimited worker never takes. *)
let burst = 6

(** The configured per-worker activity rate, in starts per second. *)
let activity_rate = 1.0

(** Smallest accepted spread, in seconds, between the first and last
    activity start of the burst. Core lets the first one or two polls through
    immediately and spaces the rest one second apart, so [burst] activities
    take at least about four seconds; the margin absorbs scheduling noise
    while staying far above an unlimited worker's spread. *)
let minimum_spread_seconds = 2.5

(** Wall-clock activity start times. Activities run on the worker's own
    activity lane, outside any workflow, so reading the clock is safe. *)
let starts = ref []

(** Serializes [starts] between the activity lane and the checking thread. *)
let starts_mutex = Mutex.create ()

(** Records its start time and echoes its input. *)
let stamp =
  Activity.define ~name:"resource-options.stamp" ~input:Codec.string
    ~output:Codec.string (fun input ->
      Mutex.protect starts_mutex (fun () ->
          starts := Unix.gettimeofday () :: !starts);
      Ok ("stamped:" ^ input))

(** Runs one activity. Eager dispatch is disabled because Core applies the
    per-worker rate limit to polled activity tasks only. *)
let fanout =
  Workflow.define ~name:"resource-options.fanout" ~input:Codec.string
    ~output:Codec.string (fun input ->
      Activity.execute ~do_not_eagerly_execute:true
        ~start_to_close_timeout:(Duration.of_ms 30_000L) stamp input)

(** The constrained configuration under test. *)
let options () =
  get
    (Worker.Options.make ~max_cached_workflows:1
       ~max_concurrent_workflow_tasks:2
       ~workflow_task_pollers:(Worker.Options.Fixed 2)
       ~sticky_queue_schedule_to_start_timeout:(Duration.of_ms 1_000L)
       ~graceful_shutdown_period:(Duration.of_ms 2_000L)
       ~max_heartbeat_throttle_interval:(Duration.of_ms 5_000L)
       ~default_heartbeat_throttle_interval:(Duration.of_ms 2_000L)
       ~max_worker_activities_per_second:activity_rate ())

(** Fails unless the worker reports the configuration it was given. *)
let check_reported_options worker =
  let reported = Worker.options worker in
  let duration_ms read = Duration.to_ms (read reported) in
  if
    not
      (Worker.Options.max_cached_workflows reported = Some 1
      && Worker.Options.max_concurrent_workflow_tasks reported = 2
      && Worker.Options.workflow_task_pollers reported = Worker.Options.Fixed 2
      && duration_ms Worker.Options.sticky_queue_schedule_to_start_timeout
         = 1_000L
      && duration_ms Worker.Options.graceful_shutdown_period = 2_000L
      && duration_ms Worker.Options.max_heartbeat_throttle_interval = 5_000L
      && duration_ms Worker.Options.default_heartbeat_throttle_interval
         = 2_000L
      && Worker.Options.max_worker_activities_per_second reported
         = Some activity_rate)
  then failwith "worker did not report its configured options"

(** Starts executions [first .. first + count - 1] and waits for each exact
    run, checking that every one completed with its own activity result. *)
let run_executions client queue ~first ~count =
  let handles =
    List.init count (fun offset ->
        let input = string_of_int (first + offset) in
        ( input,
          get
            (Client.start client ~workflow:fanout ~task_queue:queue
               ~id:(Printf.sprintf "%s-%s" queue input) ~input ()) ))
  in
  List.iter
    (fun (input, handle) ->
      match get (Client.wait handle) with
      | Client.Completed { output; _ } when output = "stamped:" ^ input -> ()
      | Client.Completed _ -> failwith "constrained worker returned a wrong result"
      | _ -> failwith "constrained worker execution did not complete")
    handles

(** Spread in seconds between the earliest and latest recorded start. *)
let spread () =
  Mutex.protect starts_mutex (fun () ->
      match !starts with
      | [] -> 0.0
      | first :: rest ->
          let low = List.fold_left Float.min first rest in
          let high = List.fold_left Float.max first rest in
          high -. low)

(** Runs the burst and the recovery execution, always shutting the worker
    down. A process-wide alarm bounds a hung server or worker. *)
let check address =
  ignore (Unix.alarm 150);
  let queue =
    Printf.sprintf "resource-options-%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  let worker =
    get
      (Worker.create ~options:(options ()) ~target_url:address ~namespace
         ~task_queue:queue ~identity:"resource-options-worker"
         ~workflows:[ Worker.workflow fanout ]
         ~activities:[ Worker.activity stamp ] ())
  in
  let running = Domain.spawn (fun () -> Worker.run worker) in
  let client = get (Client.create ~target_url:address ~namespace ()) in
  let body =
    try
      check_reported_options worker;
      run_executions client queue ~first:0 ~count:burst;
      let burst_spread = spread () in
      if burst_spread < minimum_spread_seconds then
        failwith
          (Printf.sprintf
             "%d activity starts spanned %.2f s; the %.1f/s worker rate \
              limit was not applied"
             burst burst_spread activity_rate);
      (* Recovery: once the burst has drained, a new execution must still be
         admitted and completed by the same saturated worker. *)
      run_executions client queue ~first:burst ~count:1;
      Ok burst_spread
    with exception_ -> Error exception_
  in
  ignore (Client.shutdown client);
  let shutdown = Worker.shutdown worker in
  let run_result = Domain.join running in
  match body with
  | Error exception_ -> raise exception_
  | Ok burst_spread ->
      get run_result;
      get shutdown;
      Printf.printf
        "worker resource options live regression: %d executions completed, \
         activity starts spanned %.2f s, recovery execution completed\n%!"
        burst burst_spread

let () =
  match Sys.argv with
  | [| _; "check"; address |] -> check address
  | _ ->
      prerr_endline "usage: regression.exe check TEMPORAL_ADDRESS";
      exit 2

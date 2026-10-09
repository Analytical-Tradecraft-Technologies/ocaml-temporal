(** Live #495 regression for bounded worker shutdown with outstanding work.

    One process runs three workers on one fresh task queue:

    - a workflow-only worker that runs [workflow] for the whole test;
    - activity worker A, whose implementation of [bounded-shutdown.slow]
      never heartbeats, ignores cancellation and the worker-shutdown flag,
      and only returns when this fixture releases it;
    - activity worker B, created after A has shut down, whose implementation
      of the same activity type returns at once.

    The workflow runs the activity with a long start-to-close timeout and a
    one-second retry interval. Once attempt 1 is running on A, the fixture
    calls [Worker.shutdown_with_report] on A with a two-second grace period
    and a twenty-second teardown timeout. The call must return within that
    bound (plus slack) and no earlier than the grace period, report exactly
    one abandoned activity callback with the run loop not stopped and
    teardown completed, and A's [Worker.run] must already have returned.
    Temporal must then retry the attempt, and B must complete attempt 2,
    which the workflow result names. Finally the stuck callback is released;
    its late result must be discarded without disturbing anything, and B and
    the workflow worker must shut down cleanly.

    Run against a disposable Temporal Server only:
    [dune exec test/integration/bounded_shutdown/regression.exe -- check
    http://127.0.0.1:7233]. [TEMPORAL_NAMESPACE] defaults to
    [temporal-sdk-test]. CI runs it through
    [make test-temporal-live-regressions]. *)
open Temporal

(** Converts the public result boundary into a short fixture failure. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

let namespace =
  Sys.getenv_opt "TEMPORAL_NAMESPACE"
  |> Option.value ~default:"temporal-sdk-test"

(** Activity type shared by both implementations. *)
let activity_name = "bounded-shutdown.slow"

(** Grace period given to worker A, in milliseconds. *)
let grace_ms = 2_000L

(** Teardown timeout given to worker A, in milliseconds. *)
let teardown_ms = 20_000L

(** Set by A's implementation when attempt 1 starts running. *)
let stuck_started = Atomic.make false

(** Set by the fixture to let A's stuck callback return. *)
let stuck_released = Atomic.make false

(** Set by A's implementation when its callback finally returns. *)
let stuck_returned = Atomic.make false

(** The attempt number of an activity context, or 0 if unavailable. *)
let attempt context =
  match Activity.Context.info context with
  | Ok info -> Activity.Info.attempt info
  | Error _ -> 0

(** Worker A's implementation: non-cooperative user code. It never
    heartbeats and never reads the shutdown flag or the cancellation, and
    returns only when released (or after ten minutes, as a fixture safety
    net). Its eventual result must never reach Temporal. *)
let stuck =
  Activity.define_with_context ~name:activity_name ~input:Codec.string
    ~output:Codec.string (fun context _label ->
      Atomic.set stuck_started true;
      let deadline = Unix.gettimeofday () +. 600. in
      while
        (not (Atomic.get stuck_released)) && Unix.gettimeofday () < deadline
      do
        Unix.sleepf 0.05
      done;
      Atomic.set stuck_returned true;
      Ok (Printf.sprintf "late-result-from-a:attempt=%d" (attempt context)))

(** Worker B's implementation: completes at once and names its attempt. *)
let quick =
  Activity.define_with_context ~name:activity_name ~input:Codec.string
    ~output:Codec.string (fun context _label ->
      Ok (Printf.sprintf "completed-by-b:attempt=%d" (attempt context)))

(** Runs the activity on the activity queue (the workflow input) with a
    start-to-close timeout far beyond the test, so only the shutdown can end
    attempt 1, and a fast retry so attempt 2 follows promptly. *)
let workflow =
  Workflow.define ~name:"bounded-shutdown.workflow" ~input:Codec.string
    ~output:Codec.string (fun activity_queue ->
      let retry_policy =
        get
          (Activity.Retry_policy.make ~initial_interval:(Duration.of_ms 1_000L)
             ~backoff_coefficient:1.0 ~maximum_interval:(Duration.of_ms 1_000L)
             ~maximum_attempts:5 ())
      in
      Activity.execute ~task_queue:activity_queue
        ~start_to_close_timeout:(Duration.of_ms 600_000L) ~retry_policy quick
        "bounded-shutdown")

(** Runs one worker on its own Domain so the workers share this process. *)
let start_worker worker = Domain.spawn (fun () -> Worker.run worker)

(** Fails the fixture with [message] unless [condition] holds. *)
let require condition message = if not condition then failwith message

(** Polls [predicate] every 50 ms for at most [seconds]. *)
let wait_until ~seconds predicate =
  let deadline = Unix.gettimeofday () +. seconds in
  let rec loop () =
    if predicate () then true
    else if Unix.gettimeofday () > deadline then false
    else (
      Unix.sleepf 0.05;
      loop ())
  in
  loop ()

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

(** Runs the scenario and always shuts every worker down. A process-wide
    alarm bounds a hung server or worker. *)
let check address =
  ignore (Unix.alarm 170);
  let suffix =
    Printf.sprintf "%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  let workflow_queue = "bounded-shutdown-" ^ suffix in
  let activity_queue = "bounded-shutdown-activities-" ^ suffix in
  let workflow_worker =
    get
      (Worker.create ~target_url:address ~namespace ~task_queue:workflow_queue
         ~identity:"bounded-shutdown-workflows"
         ~workflows:[ Worker.workflow workflow ] ~activities:[] ())
  in
  let a_options =
    get
      (Worker.Options.make
         ~graceful_shutdown_period:(Duration.of_ms grace_ms)
         ~shutdown_teardown_timeout:(Duration.of_ms teardown_ms) ())
  in
  let worker_a =
    get
      (Worker.create ~options:a_options ~target_url:address ~namespace
         ~task_queue:activity_queue ~identity:"bounded-shutdown-a"
         ~workflows:[] ~activities:[ Worker.activity stuck ] ())
  in
  let workflow_domain = start_worker workflow_worker in
  let a_domain = start_worker worker_a in
  let a_shut_down = ref false in
  let worker_b = ref None in
  let client = get (Client.create ~target_url:address ~namespace ()) in
  let body =
    try
      let handle =
        get
          (Client.start client ~workflow ~task_queue:workflow_queue
             ~id:("bounded-shutdown-" ^ suffix) ~input:activity_queue ())
      in
      require
        (wait_until ~seconds:60. (fun () -> Atomic.get stuck_started))
        "attempt 1 never started on worker A";
      (* Shut A down while its callback ignores everything. *)
      let started = Unix.gettimeofday () in
      let report = get (Worker.shutdown_with_report worker_a) in
      let elapsed = Unix.gettimeofday () -. started in
      a_shut_down := true;
      Printf.printf "worker A shutdown: %s\n%!" (describe_report report);
      let bound =
        (Int64.to_float grace_ms +. Int64.to_float teardown_ms) /. 1_000. +. 2.
      in
      require (elapsed <= bound)
        (Printf.sprintf "shutdown took %.1fs, above its %.1fs bound" elapsed
           bound);
      require
        (elapsed >= (Int64.to_float grace_ms /. 1_000.) -. 0.5)
        (Printf.sprintf "shutdown returned after %.1fs, before the grace period"
           elapsed);
      require (not report.lanes_stopped) "the stuck lane was reported stopped";
      require
        (report.abandoned_activity_callbacks = 1)
        "the stuck callback was not reported abandoned";
      require
        (report.abandoned_workflow_activations = 0)
        "an activity-only worker reported a workflow activation";
      require
        (report.native_teardown = `Completed)
        "native teardown did not complete within its timeout";
      require
        (not (Worker.Shutdown_report.is_clean report))
        "an abandoned callback was reported as a clean shutdown";
      require (Worker.shutdown_with_report worker_a = Ok report)
        "a repeated shutdown did not return the cached report";
      (* [run] detached the stuck lane at the grace period, so it returned. *)
      require (get (Domain.join a_domain) = ()) "worker A run failed";
      require
        (not (Atomic.get stuck_returned))
        "the stuck callback returned before it was released";
      (* Attempt 2 must run on a second worker. *)
      let b =
        get
          (Worker.create ~target_url:address ~namespace
             ~task_queue:activity_queue ~identity:"bounded-shutdown-b"
             ~workflows:[] ~activities:[ Worker.activity quick ] ())
      in
      worker_b := Some (b, start_worker b);
      let output =
        match get (Client.wait handle) with
        | Client.Completed { output; _ } -> output
        | _ -> failwith "the workflow did not complete"
      in
      require
        (output = "completed-by-b:attempt=2")
        ("the retried activity completed as " ^ output);
      (* The late result from A is discarded: releasing it changes nothing. *)
      Atomic.set stuck_released true;
      require
        (wait_until ~seconds:10. (fun () -> Atomic.get stuck_returned))
        "the released callback did not return";
      Unix.sleepf 0.5;
      Ok output
    with exception_ -> Error exception_
  in
  Atomic.set stuck_released true;
  ignore (Client.shutdown client);
  if not !a_shut_down then begin
    ignore (Worker.shutdown worker_a);
    ignore (Domain.join a_domain)
  end;
  let b_clean =
    match !worker_b with
    | None -> true
    | Some (b, domain) ->
        let report = Worker.shutdown_with_report b in
        ignore (Domain.join domain);
        (match report with
        | Ok report -> Worker.Shutdown_report.is_clean report
        | Error _ -> false)
  in
  let workflow_clean =
    match Worker.shutdown_with_report workflow_worker with
    | Ok report -> Worker.Shutdown_report.is_clean report
    | Error _ -> false
  in
  ignore (Domain.join workflow_domain);
  match body with
  | Ok output ->
      require b_clean "worker B did not shut down cleanly";
      require workflow_clean "the workflow worker did not shut down cleanly";
      Printf.printf
        "bounded shutdown live regression: worker A returned within its \
         bound reporting one abandoned callback; Temporal retried the attempt \
         on worker B (%s)\n%!"
        output
  | Error exception_ -> raise exception_

let () =
  match Sys.argv with
  | [| _; "check"; address |] -> check address
  | _ ->
      prerr_endline "usage: regression.exe check TEMPORAL_ADDRESS";
      exit 2

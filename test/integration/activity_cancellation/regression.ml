(** Live #494 regression for cooperative activity cancellation and the
    worker-shutdown signal.

    Three scenarios share one process and one fresh task-queue pair:

    - [requested]: a workflow schedules a heartbeating activity with
      [Wait_cancellation_completed], sleeps, and cancels it. The activity must
      observe the cancellation through a heartbeat with reason [Requested]
      while it is still running, return the heartbeat's [`Cancelled] error,
      and the workflow must then see a [`Cancelled] activity error, which
      under [Wait_cancellation_completed] means Temporal recorded the attempt
      as cancelled rather than completed or failed.
    - [heartbeat_timeout]: a single-attempt activity stops heartbeating for
      longer than its heartbeat timeout and then resumes. It must observe a
      cancellation whose reason is [Timed_out] (Core's local heartbeat timer)
      or [Not_found] (the server rejected the late heartbeat), and the
      workflow must see an activity failure caused by a heartbeat timeout.
    - [worker_shutdown]: an activity runs on a separate activity-only worker
      and polls [Context.is_worker_shutting_down]. Shutting that worker down
      while the callback runs must make the flag [true]; the activity then
      returns normally, its completion is still accepted before the worker
      closes, and the workflow completes with that result.

    Run against a disposable Temporal Server only:
    [dune exec test/integration/activity_cancellation/regression.exe -- check
    http://127.0.0.1:7233]. [TEMPORAL_NAMESPACE] defaults to
    [temporal-sdk-test]. CI runs it through
    [make test-temporal-live-regressions]. *)
open Temporal

(** Converts the public result boundary into a short fixture failure. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

let namespace =
  Sys.getenv_opt "TEMPORAL_NAMESPACE"
  |> Option.value ~default:"temporal-sdk-test"

(** What each activity observed, keyed by the workflow-supplied label. The
    activities run on worker Domains of this process; the mutex makes the
    table safe to read from the checking Domain. *)
let observations : (string, Activity.Cancellation.reason list) Hashtbl.t =
  Hashtbl.create 4

let observations_mutex = Mutex.create ()

(** Records the reasons an activity observed, at most once per label. *)
let observe label reasons =
  Mutex.protect observations_mutex (fun () ->
      if not (Hashtbl.mem observations label) then
        Hashtbl.replace observations label reasons)

(** Reads what the activity labelled [label] observed. *)
let observed label =
  Mutex.protect observations_mutex (fun () -> Hashtbl.find_opt observations label)

(** Polls [observed] until the activity labelled [label] has recorded an
    observation or [seconds] elapse. *)
let wait_observed label ~seconds =
  let deadline = Unix.gettimeofday () +. seconds in
  let rec loop () =
    match observed label with
    | Some _ as found -> found
    | None when Unix.gettimeofday () > deadline -> None
    | None ->
        Unix.sleepf 0.1;
        loop ()
  in
  loop ()

(** Heartbeats every 200 ms until a heartbeat reports a cancellation, which
    the activity records and returns, or until [seconds] elapse. [stall]
    first sleeps without heartbeating, to exceed a heartbeat timeout. *)
let heartbeat_until_cancelled ~stall ~seconds context label =
  let deadline = Unix.gettimeofday () +. seconds in
  ignore (Activity.Context.heartbeat context Codec.int 0);
  if stall > 0. then Unix.sleepf stall;
  let rec loop index =
    if Unix.gettimeofday () > deadline then Ok "no cancellation observed"
    else
      match Activity.Context.heartbeat context Codec.int index with
      | Error error when (Error.view error).category = `Cancelled ->
          let reasons =
            match Activity.Context.cancellation context with
            | Some cancellation -> Activity.Cancellation.reasons cancellation
            | None -> []
          in
          observe label reasons;
          Error error
      | Error _ | Ok () ->
          Unix.sleepf 0.2;
          loop (index + 1)
  in
  loop 1

(** Long-running activity that a workflow cancels. *)
let cooperative =
  Activity.define_with_context ~name:"activity-cancellation.cooperative"
    ~input:Codec.string ~output:Codec.string
    (heartbeat_until_cancelled ~stall:0. ~seconds:60.)

(** Activity that misses its heartbeat timeout, then resumes heartbeating. *)
let stalling =
  Activity.define_with_context ~name:"activity-cancellation.stalling"
    ~input:Codec.string ~output:Codec.string
    (heartbeat_until_cancelled ~stall:4. ~seconds:40.)

(** Set by [watches_shutdown] once it is running, so the checker shuts the
    activity worker down only while the callback is in flight. *)
let shutdown_activity_started = Atomic.make false

(** Activity that returns as soon as its worker starts shutting down. *)
let watches_shutdown =
  Activity.define_with_context ~name:"activity-cancellation.watches-shutdown"
    ~input:Codec.string ~output:Codec.string (fun context label ->
      Atomic.set shutdown_activity_started true;
      let deadline = Unix.gettimeofday () +. 60. in
      let rec loop () =
        if Activity.Context.is_worker_shutting_down context then begin
          observe label [];
          Ok "shutdown-observed"
        end
        else if Unix.gettimeofday () > deadline then Ok "shutdown not observed"
        else (
          Unix.sleepf 0.1;
          loop ())
      in
      loop ())

(** Describes an activity result as one comparable string. *)
let describe = function
  | Ok output -> "completed:" ^ output
  | Error error -> "error:" ^ Error.kind error ^ "|" ^ Error.message error

(** Schedules [cooperative], waits until it is surely heartbeating, cancels
    it, and reports the result Temporal recorded for it. *)
let requested =
  Workflow.define ~name:"activity-cancellation.requested" ~input:Codec.string
    ~output:Codec.string (fun label ->
      let handle =
        Activity.start_handle ~heartbeat_timeout:(Duration.of_ms 2_000L)
          ~start_to_close_timeout:(Duration.of_ms 90_000L)
          ~cancellation_type:Activity.Wait_cancellation_completed cooperative
          label
      in
      let open Result_syntax in
      let* () = Workflow.sleep (Duration.of_ms 3_000L) in
      let* () = Activity.cancel handle in
      Ok (describe (Future.await (Activity.future handle))))

(** Runs [stalling] once, so its heartbeat timeout fails the activity. *)
let heartbeat_timeout =
  Workflow.define ~name:"activity-cancellation.heartbeat-timeout"
    ~input:Codec.string ~output:Codec.string (fun label ->
      let retry_policy =
        get
          (Activity.Retry_policy.make ~initial_interval:(Duration.of_ms 1_000L)
             ~backoff_coefficient:1.0 ~maximum_interval:(Duration.of_ms 1_000L)
             ~maximum_attempts:1 ())
      in
      Ok
        (describe
           (Activity.execute ~heartbeat_timeout:(Duration.of_ms 1_000L)
              ~start_to_close_timeout:(Duration.of_ms 90_000L) ~retry_policy
              stalling label)))

(** Runs [watches_shutdown], labelled ["shutdown"], on the activity-only
    worker's task queue, which is the workflow input. *)
let shutdown_workflow =
  Workflow.define ~name:"activity-cancellation.worker-shutdown"
    ~input:Codec.string ~output:Codec.string (fun activity_queue ->
      Ok
        (describe
           (Activity.execute ~task_queue:activity_queue
              ~start_to_close_timeout:(Duration.of_ms 90_000L) watches_shutdown
              "shutdown")))

(** Runs one worker on its own Domain so the workers share this process. *)
let start_worker worker = Domain.spawn (fun () -> Worker.run worker)

(** Waits for one exact run and returns its string result. *)
let wait_output handle =
  match get (Client.wait handle) with
  | Client.Completed { output; _ } -> output
  | _ -> failwith "a fixture workflow did not complete"

(** Reports whether [sub] occurs in [text]. *)
let contains ~sub text =
  let sub_length = String.length sub in
  let rec from index =
    index + sub_length <= String.length text
    && (String.sub text index sub_length = sub || from (index + 1))
  in
  from 0

(** Fails the fixture with [message] unless [condition] holds. *)
let require condition message = if not condition then failwith message

(** Runs the three scenarios and always shuts every worker down. A
    process-wide alarm bounds a hung server or worker. *)
let check address =
  ignore (Unix.alarm 150);
  let suffix =
    Printf.sprintf "%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  let queue = "activity-cancellation-" ^ suffix in
  let activity_queue = "activity-cancellation-shutdown-" ^ suffix in
  let main_worker =
    get
      (Worker.create ~target_url:address ~namespace ~task_queue:queue
         ~identity:"activity-cancellation-main"
         ~workflows:
           [
             Worker.workflow requested;
             Worker.workflow heartbeat_timeout;
             Worker.workflow shutdown_workflow;
           ]
         ~activities:[ Worker.activity cooperative; Worker.activity stalling ]
         ())
  in
  let shutdown_worker =
    get
      (Worker.create ~target_url:address ~namespace ~task_queue:activity_queue
         ~identity:"activity-cancellation-shutdown" ~workflows:[]
         ~activities:[ Worker.activity watches_shutdown ] ())
  in
  let main_domain = start_worker main_worker in
  let shutdown_domain = start_worker shutdown_worker in
  let shutdown_joined = ref false in
  let client = get (Client.create ~target_url:address ~namespace ()) in
  let start workflow id input =
    get
      (Client.start client ~workflow ~task_queue:queue
         ~id:(Printf.sprintf "%s-%s" queue id) ~input ())
  in
  let body =
    try
      (* Requested cancellation of a running, heartbeating activity. *)
      let started = Unix.gettimeofday () in
      let output = wait_output (start requested "requested" "requested") in
      require (String.starts_with ~prefix:"error:cancelled|" output)
        ("the cancelled activity was recorded as " ^ output);
      require
        (observed "requested" = Some [ Activity.Cancellation.Requested ])
        "the running activity did not observe a requested cancellation";
      require
        (Unix.gettimeofday () -. started < 45.)
        "cancellation was not observed cooperatively";
      (* Heartbeat timeout of a single-attempt activity. *)
      let output =
        wait_output (start heartbeat_timeout "heartbeat-timeout" "timeout")
      in
      require
        (String.starts_with ~prefix:"error:activity|" output
        && contains ~sub:"timeout type=heartbeat" output)
        ("the timed-out activity was recorded as " ^ output);
      (* The workflow learns of the server timeout at once, while the
         callback is still stalled; wait for it to resume and observe. *)
      begin match wait_observed "timeout" ~seconds:45. with
      | Some (Activity.Cancellation.(Timed_out | Not_found) :: _) -> ()
      | Some _ -> failwith "the timed-out activity observed the wrong reason"
      | None -> failwith "the timed-out activity observed no cancellation"
      end;
      (* Worker shutdown while an activity callback runs. *)
      let handle =
        start shutdown_workflow "worker-shutdown" activity_queue
      in
      while not (Atomic.get shutdown_activity_started) do
        Unix.sleepf 0.05
      done;
      ignore (get (Worker.shutdown shutdown_worker));
      ignore (get (Domain.join shutdown_domain));
      shutdown_joined := true;
      require (observed "shutdown" = Some [])
        "the activity did not observe its worker shutting down";
      let output = wait_output handle in
      require (output = "completed:shutdown-observed")
        ("the shutdown-observing activity was recorded as " ^ output);
      Ok ()
    with exception_ -> Error exception_
  in
  ignore (Client.shutdown client);
  if not !shutdown_joined then begin
    ignore (Worker.shutdown shutdown_worker);
    ignore (Domain.join shutdown_domain)
  end;
  ignore (get (Worker.shutdown main_worker));
  ignore (Domain.join main_domain);
  match body with
  | Ok () ->
      let label = function
        | Activity.Cancellation.Requested -> "requested"
        | Timed_out -> "timed_out"
        | Not_found -> "not_found"
        | Worker_shutdown -> "worker_shutdown"
        | Paused -> "paused"
        | Reset -> "reset"
      in
      Printf.printf
        "activity cancellation live regression: requested cancellation, \
         heartbeat-timeout cancellation (%s) and worker shutdown observed\n%!"
        (String.concat ","
           (List.map label (Option.value ~default:[] (observed "timeout"))))
  | Error exception_ -> raise exception_

let () =
  match Sys.argv with
  | [| _; "check"; address |] -> check address
  | _ ->
      prerr_endline "usage: regression.exe check TEMPORAL_ADDRESS";
      exit 2

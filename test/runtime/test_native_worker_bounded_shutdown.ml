(** Tests for bounded native worker shutdown (#495).

    The production orchestration ([Native_worker_shutdown.run]) and the real
    lane scheduler ([Native_worker_loop.run]) are driven by fake adapters:
    plain mutexes stand in for the adapter locks a running callback or
    activation holds, and a counted [release] closure stands in for the
    native graph. Each scenario checks the documented bound (grace period
    plus slack plus teardown timeout), the typed report, and that the release
    ran exactly once. Durations are a few hundred milliseconds; assertions on
    elapsed time allow generous scheduling slack, and a process watchdog
    fails a hung case promptly. *)

module Shutdown = Temporal_runtime.Native_worker_shutdown
module Loop = Temporal_runtime.Native_worker_loop

(** Terminates the process if the whole suite exceeds [seconds]; a hung
    shutdown would otherwise stall the test runner. *)
let start_watchdog seconds =
  ignore
    (Thread.create
       (fun () ->
         Thread.delay seconds;
         prerr_endline "bounded shutdown tests timed out";
         Unix._exit 2)
       ())

(** Fails with [message] unless [condition] holds. *)
let require condition message = if not condition then failwith message

(** Waits for a cross-thread observation, failing after five seconds. *)
let await label predicate =
  let deadline = Unix.gettimeofday () +. 5. in
  while not (predicate ()) do
    if Unix.gettimeofday () > deadline then failwith ("timed out: " ^ label);
    Thread.delay 0.005
  done

(** A gate a fake callback blocks on until the test opens it. *)
type gate = { opened : bool Atomic.t }

(** A closed gate. *)
let gate () = { opened = Atomic.make false }

(** Opens [gate] for every current and future waiter. *)
let open_gate gate = Atomic.set gate.opened true

(** Blocks the calling thread until [gate] opens, sleeping between checks
    so the runtime lock is released like a real blocking callback. *)
let block_on gate =
  while not (Atomic.get gate.opened) do
    Thread.delay 0.005
  done

(** The fake worker: the lifecycle lock, both adapter locks, and the
    observable facts the orchestration probes. *)
type fake = {
  run_mutex : Mutex.t;
  workflow_lock : Mutex.t;
  activity_lock : Mutex.t;
  closed : bool Atomic.t;
  callback_running : bool Atomic.t;
  activation_in_flight : bool Atomic.t;
  detached : bool Atomic.t;
  releases : int Atomic.t;
  release_finished : bool Atomic.t;
}

(** A fake worker with free locks and no observed activity. *)
let fake () =
  {
    run_mutex = Mutex.create ();
    workflow_lock = Mutex.create ();
    activity_lock = Mutex.create ();
    closed = Atomic.make false;
    callback_running = Atomic.make false;
    activation_in_flight = Atomic.make false;
    detached = Atomic.make false;
    releases = Atomic.make 0;
    release_finished = Atomic.make false;
  }

(** A non-blocking drain of an adapter guarded by [lock]: busy while a lane
    holds it, otherwise the result of [drained]. *)
let try_drain lock drained () =
  if Mutex.try_lock lock then
    Fun.protect ~finally:(fun () -> Mutex.unlock lock) drained
  else Shutdown.Busy

(** Operations over [worker]. [drain_activity], [release] and the count of
    admitted async leases can be replaced per scenario; the default release
    succeeds unless a lane was abandoned, in which case it reports retired
    leases, as the bridge does. *)
let operations ?drain_activity ?release ?(async_leases = fun () -> 0) worker =
  let release =
    match release with
    | Some release -> release
    | None ->
        fun () ->
          let held = Mutex.try_lock worker.workflow_lock in
          if held then Mutex.unlock worker.workflow_lock;
          if held && not (Atomic.get worker.callback_running) then
            Shutdown.Released
          else Shutdown.Released_retiring_leases "bridge retired leases"
  in
  {
    Shutdown.try_acquire_lanes = (fun () -> Mutex.try_lock worker.run_mutex);
    release_lanes = (fun () -> Mutex.unlock worker.run_mutex);
    activity_lane_detached = (fun () -> Atomic.get worker.detached);
    activity_callback_running = (fun () -> Atomic.get worker.callback_running);
    workflow_activation_in_flight =
      (fun () -> Atomic.get worker.activation_in_flight);
    drain_workflow = try_drain worker.workflow_lock (fun () -> Shutdown.Drained);
    drain_activity =
      Option.value drain_activity
        ~default:(try_drain worker.activity_lock (fun () -> Shutdown.Drained));
    outstanding_async_leases = async_leases;
    async_leases_error = Printf.sprintf "%d async leases";
    release =
      (fun () ->
        ignore (Atomic.fetch_and_add worker.releases 1);
        let result = release () in
        Atomic.set worker.release_finished true;
        result);
    exception_error = Printexc.to_string;
  }

(** The shutdown caller: closes admission and runs the bounded sequence
    with [grace] seconds for the lanes and [teardown] for the release. *)
let shutdown ?(grace = 0.3) ?(teardown = 0.3) ?lanes_deadline worker ops =
  Atomic.set worker.closed true;
  let started = Unix.gettimeofday () in
  let lanes_deadline = Option.value lanes_deadline ~default:(started +. grace) in
  let outcome =
    Shutdown.run ~lanes_deadline ~teardown_timeout_s:teardown ops
  in
  (outcome, Unix.gettimeofday () -. started)

(** Asserts the documented bound with half a second of scheduling slack. *)
let require_bound ~grace ~teardown elapsed =
  let bound = grace +. Shutdown.lanes_slack_s +. teardown +. 0.5 in
  require (elapsed <= bound)
    (Printf.sprintf "shutdown took %.3fs, above its %.3fs bound" elapsed bound)

(** Extracts a successful report or fails with the outcome's name. *)
let shut_down = function
  | Shutdown.Shut_down report -> report
  | Completion_lost _ -> failwith "unexpected Completion_lost"
  | Release_error _ -> failwith "unexpected Release_error"
  | Release_unproven _ -> failwith "unexpected Release_unproven"

(** An idle worker with no run loop shuts down at once and cleanly. *)
let test_idle () =
  let worker = fake () in
  let outcome, elapsed = shutdown worker (operations worker) in
  let report = shut_down outcome in
  require report.lanes_stopped "an idle worker reported abandoned lanes";
  require (report.abandoned_activity_callbacks = 0) "idle: activity count";
  require (report.abandoned_workflow_activations = 0) "idle: workflow count";
  require (report.teardown = Shutdown.Completed) "idle: teardown detached";
  require (elapsed < 0.2) "an idle shutdown waited for its deadline";
  require (Atomic.get worker.releases = 1) "idle: release count"

(** Runs the real lane scheduler on its own thread under [run_mutex], as
    the native worker does, with bounded activity detach at the shared
    deadline. Returns a flag set when the loop has returned. *)
let start_run ?(deadline = fun () -> Unix.gettimeofday () +. 0.3) worker
    ~poll_workflow ~poll_activity =
  let returned = Atomic.make false in
  let ready = Atomic.make false in
  ignore
    (Thread.create
       (fun () ->
         Mutex.lock worker.run_mutex;
         Atomic.set ready true;
         Fun.protect
           ~finally:(fun () ->
             Mutex.unlock worker.run_mutex;
             Atomic.set returned true)
           (fun () ->
             ignore
               (Loop.run
                  ~detach:
                    (Some
                       {
                         Loop.now = Unix.gettimeofday;
                         deadline;
                         on_detached = (fun () -> Atomic.set worker.detached true);
                       })
                  ~closed:(fun () -> Atomic.get worker.closed)
                  ~poll_workflow ~poll_activity
                  ~wait_for_lane:(fun ~workflow_lane:_ ~native_wait:_ ->
                    Thread.delay 0.005;
                    Ok ())
                  ~retry_pending:(fun ~workflow_lane:_ -> Ok ()))))
       ());
  await "run loop started" (fun () -> Atomic.get ready);
  returned

(** An idle lane poll. *)
let idle () = Ok Loop.Not_ready

(** A one-shot activity poll: the first poll runs [callback] under the
    activity lock, as the serial executor does; later polls are idle. *)
let activity_once worker callback =
  let taken = Atomic.make false in
  fun () ->
    if Atomic.compare_and_set taken false true then begin
      Mutex.lock worker.activity_lock;
      Atomic.set worker.callback_running true;
      Fun.protect
        ~finally:(fun () ->
          Atomic.set worker.callback_running false;
          Mutex.unlock worker.activity_lock)
        callback;
      Ok Loop.Progress
    end
    else idle ()

(** A cooperative callback returns soon after shutdown begins, so the
    shutdown is clean and does not wait for the grace period. *)
let test_cooperative_activity () =
  let worker = fake () in
  let entered = Atomic.make false in
  let returned =
    start_run worker ~poll_workflow:idle
      ~poll_activity:
        (activity_once worker (fun () ->
             Atomic.set entered true;
             while not (Atomic.get worker.closed) do
               Thread.delay 0.005
             done;
             Thread.delay 0.05))
  in
  await "callback entered" (fun () -> Atomic.get entered);
  let outcome, elapsed = shutdown ~grace:2. worker (operations worker) in
  let report = shut_down outcome in
  require report.lanes_stopped "a cooperative callback was abandoned";
  require (elapsed < 1.) "a cooperative shutdown waited for its grace period";
  require (Atomic.get returned) "the run loop did not return";
  require (not (Atomic.get worker.detached)) "a cooperative lane detached"

(** A callback that ignores cancellation: [run] detaches its Domain at the
    deadline and returns, shutdown reports one abandoned callback within
    the bound, the release runs once, and the callback finishing later
    triggers nothing further. *)
let test_stuck_activity_callback () =
  let worker = fake () in
  let release_callback = gate () in
  let entered = Atomic.make false in
  let started = Unix.gettimeofday () in
  let returned =
    start_run worker
      ~deadline:(fun () -> started +. 0.3)
      ~poll_workflow:idle
      ~poll_activity:
        (activity_once worker (fun () ->
             Atomic.set entered true;
             block_on release_callback))
  in
  await "callback entered" (fun () -> Atomic.get entered);
  let outcome, elapsed =
    shutdown ~lanes_deadline:(started +. 0.3) worker (operations worker)
  in
  let report = shut_down outcome in
  require_bound ~grace:0.3 ~teardown:0.3 elapsed;
  require (Atomic.get returned) "run did not return while the callback ran";
  require (Atomic.get worker.detached) "run did not detach the activity lane";
  require (not report.lanes_stopped) "a stuck callback was not reported";
  require (report.abandoned_activity_callbacks = 1) "abandoned callback count";
  require (report.abandoned_workflow_activations = 0) "workflow count";
  require (report.teardown = Shutdown.Completed) "teardown detached";
  require (Atomic.get worker.releases = 1) "release count";
  (* The late callback return must not release or complete anything. *)
  open_gate release_callback;
  await "callback returned" (fun () -> not (Atomic.get worker.callback_running));
  Thread.delay 0.05;
  require (Atomic.get worker.releases = 1) "a late callback caused a release"

(** A workflow activation that never yields keeps the run thread, so the
    lifecycle lock is never handed back. Shutdown still returns within the
    bound, reports the activation, and the release retires its lease. *)
let test_stuck_workflow_activation () =
  let worker = fake () in
  let release_activation = gate () in
  let entered = Atomic.make false in
  let taken = Atomic.make false in
  let poll_workflow () =
    if Atomic.compare_and_set taken false true then begin
      Mutex.lock worker.workflow_lock;
      Atomic.set worker.activation_in_flight true;
      Atomic.set entered true;
      Fun.protect
        ~finally:(fun () ->
          Atomic.set worker.activation_in_flight false;
          Mutex.unlock worker.workflow_lock)
        (fun () -> block_on release_activation);
      Ok Loop.Progress
    end
    else idle ()
  in
  let returned = start_run worker ~poll_workflow ~poll_activity:idle in
  await "activation entered" (fun () -> Atomic.get entered);
  let outcome, elapsed = shutdown worker (operations worker) in
  let report = shut_down outcome in
  require_bound ~grace:0.3 ~teardown:0.3 elapsed;
  require (not (Atomic.get returned)) "run returned during a stuck activation";
  require (not report.lanes_stopped) "a stuck activation was not reported";
  require (report.abandoned_workflow_activations = 1) "abandoned activation count";
  require (report.abandoned_activity_callbacks = 0) "activity count";
  require (Atomic.get worker.releases = 1) "release count";
  open_gate release_activation;
  await "run returned after the activation" (fun () -> Atomic.get returned);
  require (Atomic.get worker.releases = 1) "a late activation caused a release"

(** #495 review: an async lease admitted before shutdown belongs to external
    code, not to a callback that is abandoned meanwhile. The busy activity
    drain cannot see it, so it is probed separately: if it is still admitted
    when the grace period ends, the outcome is a lost completion naming it,
    never a clean report that silently closes its handle. *)
let test_async_lease_with_stuck_callback () =
  let worker = fake () in
  let release_callback = gate () in
  let entered = Atomic.make false in
  let started = Unix.gettimeofday () in
  let _returned =
    start_run worker
      ~deadline:(fun () -> started +. 0.3)
      ~poll_workflow:idle
      ~poll_activity:
        (activity_once worker (fun () ->
             Atomic.set entered true;
             block_on release_callback))
  in
  await "callback entered" (fun () -> Atomic.get entered);
  let outcome, elapsed =
    shutdown ~lanes_deadline:(started +. 0.3) worker
      (operations ~async_leases:(fun () -> 1) worker)
  in
  require_bound ~grace:0.3 ~teardown:0.3 elapsed;
  (match outcome with
  | Shutdown.Completion_lost { error = "1 async leases"; report } ->
      require (report.abandoned_activity_callbacks = 1)
        "the abandoned callback was not counted next to the async lease";
      require (not report.lanes_stopped) "async lease: lanes"
  | Shutdown.Shut_down _ ->
      failwith "an outstanding async lease was reported as a clean shutdown"
  | _ -> failwith "an outstanding async lease was not reported");
  require (Atomic.get worker.releases = 1) "async lease: release count";
  open_gate release_callback

(** An async lease that external code completes within the grace period is
    not lost, even while a callback is abandoned. *)
let test_async_lease_completed_in_grace () =
  let worker = fake () in
  let release_callback = gate () in
  let entered = Atomic.make false in
  let started = Unix.gettimeofday () in
  let _returned =
    start_run worker
      ~deadline:(fun () -> started +. 0.3)
      ~poll_workflow:idle
      ~poll_activity:
        (activity_once worker (fun () ->
             Atomic.set entered true;
             block_on release_callback))
  in
  await "callback entered" (fun () -> Atomic.get entered);
  let completes_at = started +. 0.4 in
  let async_leases () = if Unix.gettimeofday () < completes_at then 1 else 0 in
  let outcome, _elapsed =
    shutdown ~lanes_deadline:(started +. 0.3) worker
      (operations ~async_leases worker)
  in
  let report = shut_down outcome in
  require (report.abandoned_activity_callbacks = 1) "completed async: count";
  open_gate release_callback

(** #495 review: when the release runs while a detached callback still holds
    the activity adapter lock, the discard is left pending, and the lane
    itself performs it after the callback returns and the lock is free, on
    its own Domain. Nothing else needs to run for the copied state to go. *)
let test_detached_callback_performs_pending_discard () =
  let worker = fake () in
  let pending = Shutdown.Deferred_discard.create () in
  let discards = Atomic.make 0 in
  let try_discard () =
    if Mutex.try_lock worker.activity_lock then begin
      Mutex.unlock worker.activity_lock;
      ignore (Atomic.fetch_and_add discards 1);
      true
    end
    else false
  in
  let release_callback = gate () in
  let entered = Atomic.make false in
  let lane_exited = Atomic.make false in
  let started = Unix.gettimeofday () in
  let poll = activity_once worker (fun () ->
      Atomic.set entered true;
      block_on release_callback)
  in
  let returned =
    start_run worker
      ~deadline:(fun () -> started +. 0.2)
      ~poll_workflow:idle
      ~poll_activity:(fun () ->
        (* The native worker's lane wrapper. *)
        Fun.protect
          ~finally:(fun () ->
            Shutdown.Deferred_discard.retry pending ~try_discard;
            if Atomic.get worker.closed then Atomic.set lane_exited true)
          poll)
  in
  await "callback entered" (fun () -> Atomic.get entered);
  Atomic.set worker.closed true;
  await "run detached the lane" (fun () -> Atomic.get returned);
  (* The release has returned; the callback still holds the lock. *)
  Shutdown.Deferred_discard.request pending ~try_discard;
  require (Shutdown.Deferred_discard.pending pending)
    "a discard ran while the callback held the adapter lock";
  require (Atomic.get discards = 0) "discarded under a held lock";
  open_gate release_callback;
  await "the detached lane performed the discard" (fun () ->
      not (Shutdown.Deferred_discard.pending pending));
  require (Atomic.get discards = 1) "pending discard count";
  await "the lane observed the stop after its retry" (fun () ->
      Atomic.get lane_exited);
  (* A later retry finds nothing pending. *)
  Shutdown.Deferred_discard.retry pending ~try_discard;
  require (Atomic.get discards = 1) "a cleared discard ran again"

(** A retained completion that fails retryably is retried with the exact
    same completion until it is accepted; the shutdown is then clean. *)
let test_pending_completion_retried () =
  let worker = fake () in
  let attempts = Atomic.make 0 in
  let drain_activity () =
    if Atomic.fetch_and_add attempts 1 < 2 then
      Shutdown.Drain_failed { error = "transient"; retryable = true }
    else Shutdown.Drained
  in
  let outcome, elapsed =
    shutdown ~grace:2. worker (operations ~drain_activity worker)
  in
  let report = shut_down outcome in
  require report.lanes_stopped "retried completion: lanes";
  require (Atomic.get attempts = 3) "the retained completion was not retried";
  require (elapsed < 1.) "retries waited for the grace period";
  require (Atomic.get worker.releases = 1) "retried completion: release count"

(** A retained completion that keeps failing retryably is retried only
    until the grace period ends; it is then reported lost, and the native
    graph is still released once within the bound. *)
let test_pending_completion_exhausted () =
  let worker = fake () in
  let attempts = Atomic.make 0 in
  let drain_activity () =
    ignore (Atomic.fetch_and_add attempts 1);
    Shutdown.Drain_failed { error = "unreachable"; retryable = true }
  in
  let outcome, elapsed = shutdown worker (operations ~drain_activity worker) in
  (match outcome with
  | Shutdown.Completion_lost { error = "unreachable"; report } ->
      require report.lanes_stopped "exhausted completion: lanes"
  | _ -> failwith "an undeliverable completion was not reported lost");
  require_bound ~grace:0.3 ~teardown:0.3 elapsed;
  require (Atomic.get attempts > 1) "a retryable completion was not retried";
  require (Atomic.get worker.releases = 1) "exhausted completion: release count"

(** A permanent completion failure is never retried and is reported lost
    after the graph is released. *)
let test_permanent_completion_failure () =
  let worker = fake () in
  let attempts = Atomic.make 0 in
  let drain_activity () =
    ignore (Atomic.fetch_and_add attempts 1);
    Shutdown.Drain_failed { error = "rejected"; retryable = false }
  in
  let outcome, _elapsed =
    shutdown ~grace:2. worker (operations ~drain_activity worker)
  in
  (match outcome with
  | Shutdown.Completion_lost { error = "rejected"; _ } -> ()
  | _ -> failwith "a permanent completion failure was not reported");
  require (Atomic.get attempts = 1) "a permanent failure was retried";
  require (Atomic.get worker.releases = 1) "permanent failure: release count"

(** An unreachable server keeps the native release busy. The caller returns
    at the teardown timeout with a detached teardown; the shutdown thread
    still finishes the one release later. *)
let test_unreachable_server () =
  let worker = fake () in
  let server = gate () in
  let release () =
    block_on server;
    Shutdown.Released
  in
  let outcome, elapsed = shutdown worker (operations ~release worker) in
  let report = shut_down outcome in
  require_bound ~grace:0. ~teardown:0.3 elapsed;
  require (report.teardown = Shutdown.Detached) "a blocked release was not detached";
  require report.lanes_stopped "unreachable server: lanes";
  require (not (Atomic.get worker.release_finished)) "release finished early";
  open_gate server;
  await "detached release finished" (fun () -> Atomic.get worker.release_finished);
  require (Atomic.get worker.releases = 1) "unreachable server: release count"

(** A drain blocked inside a native call (a completion RPC to an
    unreachable server) still cannot hold the caller past the bound; the
    report then comes from the non-blocking probes. *)
let test_drain_blocked_in_native_call () =
  let worker = fake () in
  let server = gate () in
  let drain_activity () =
    block_on server;
    Shutdown.Drained
  in
  let outcome, elapsed = shutdown worker (operations ~drain_activity worker) in
  let report = shut_down outcome in
  require_bound ~grace:0.3 ~teardown:0.3 elapsed;
  require (report.teardown = Shutdown.Detached) "a blocked drain was not detached";
  require (not report.lanes_stopped) "a blocked drain reported stopped lanes";
  open_gate server;
  await "release after the blocked drain" (fun () ->
      Atomic.get worker.release_finished);
  require (Atomic.get worker.releases = 1) "blocked drain: release count"

(** Retired leases with nothing abandoned would be a false success, so they
    are reported as a release error. *)
let test_retired_leases_without_abandonment () =
  let worker = fake () in
  let release () = Shutdown.Released_retiring_leases "outstanding" in
  match shutdown worker (operations ~release worker) with
  | Shutdown.Release_error { error = "outstanding"; _ }, _ -> ()
  | _ -> failwith "retired leases without abandonment were reported as success"

(** A release that raises leaves its outcome unproven. *)
let test_release_raises () =
  let worker = fake () in
  let release () = raise Exit in
  match shutdown worker (operations ~release worker) with
  | Shutdown.Release_unproven _, _ -> ()
  | _ -> failwith "a raising release was not reported unproven"

(** A deadline that already passed (published by a run loop that stopped
    earlier) gives no further grace, and the lanes slack still lets an idle
    loop return. *)
let test_past_deadline () =
  let worker = fake () in
  let outcome, elapsed =
    shutdown
      ~lanes_deadline:(Unix.gettimeofday () -. 10.)
      worker (operations worker)
  in
  ignore (shut_down outcome);
  require (elapsed < 0.2) "a past deadline still waited"

(** Without [detach], [Loop.run] keeps the previous contract and joins the
    activity Domain however long its callback runs. *)
let test_loop_joins_without_detach () =
  let closed = Atomic.make false in
  let entered = Atomic.make false in
  let release_callback = gate () in
  let taken = Atomic.make false in
  let returned = Atomic.make false in
  ignore
    (Thread.create
       (fun () ->
         ignore
           (Loop.run ~detach:None
              ~closed:(fun () -> Atomic.get closed)
              ~poll_workflow:idle
              ~poll_activity:(fun () ->
                if Atomic.compare_and_set taken false true then begin
                  Atomic.set entered true;
                  block_on release_callback;
                  Ok Loop.Progress
                end
                else idle ())
              ~wait_for_lane:(fun ~workflow_lane:_ ~native_wait:_ ->
                Thread.delay 0.005;
                Ok ())
              ~retry_pending:(fun ~workflow_lane:_ -> Ok ()));
         Atomic.set returned true)
       ());
  await "callback entered" (fun () -> Atomic.get entered);
  Atomic.set closed true;
  Thread.delay 0.3;
  require (not (Atomic.get returned)) "run returned without joining";
  open_gate release_callback;
  await "joined run returned" (fun () -> Atomic.get returned)

let () =
  start_watchdog 60.;
  List.iter
    (fun (name, test) ->
      test ();
      Printf.printf "ok %s\n%!" name)
    [
      ("idle", test_idle);
      ("cooperative activity", test_cooperative_activity);
      ("stuck activity callback", test_stuck_activity_callback);
      ("stuck workflow activation", test_stuck_workflow_activation);
      ("async lease with stuck callback", test_async_lease_with_stuck_callback);
      ("async lease completed in grace", test_async_lease_completed_in_grace);
      ( "detached callback performs pending discard",
        test_detached_callback_performs_pending_discard );
      ("pending completion retried", test_pending_completion_retried);
      ("pending completion exhausted", test_pending_completion_exhausted);
      ("permanent completion failure", test_permanent_completion_failure);
      ("unreachable server", test_unreachable_server);
      ("drain blocked in native call", test_drain_blocked_in_native_call);
      ("retired leases without abandonment", test_retired_leases_without_abandonment);
      ("release raises", test_release_raises);
      ("past deadline", test_past_deadline);
      ("loop joins without detach", test_loop_joins_without_detach);
    ]

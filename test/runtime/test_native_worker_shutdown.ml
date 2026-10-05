(** Regression tests for native worker shutdown re-entrancy (#763, #764) and
    for stopping a run loop from a signal handler on its own thread (#830).

    The private native worker cannot be built without a Temporal server, so
    these tests compose the real lane scheduler ([Native_worker_loop]) and the
    real thread-identity tracker ([Native_worker_owner]) with a small model of
    the two shutdown layers: the public wrapper's admission check, mutex, and
    cached result, and the native layer's stop flag and [run_mutex] wait. The
    model keeps the production lock order, so a regression to Domain-granular
    identity or to locking before the re-entrancy check fails or deadlocks
    here. A process watchdog bounds every test so a deadlock fails promptly. *)

module Loop = Temporal_runtime.Native_worker_loop
module Owner = Temporal_runtime.Native_worker_owner

(** Lifecycle state shared by the model run loop and model shutdown callers. *)
type worker = {
  owner : Owner.t;  (** Lane threads, exactly as tracked by the native worker. *)
  run_mutex : Mutex.t;  (** Held by [run] until both lanes have returned. *)
  closed : bool Atomic.t;  (** The stop flag observed by both lanes. *)
  stop_requested : bool Atomic.t;
      (** The non-blocking stop request, like [Native_worker.stop_requested]:
          observed by both lanes but never admitting teardown. *)
  public_mutex : Mutex.t;
      (** Serializes admitted shutdown callers, like [Worker.shutdown_mutex]. *)
  mutable cached : (unit, string) result option;
      (** The first terminal result; guarded by [public_mutex]. *)
  teardowns : int Atomic.t;  (** Number of native teardowns performed. *)
  admitted : bool Atomic.t;
      (** Set once an external caller holds [public_mutex] and has published
          the stop request, immediately before it waits for [run_mutex]. *)
}

(** Allocates an idle worker model. *)
let worker () =
  {
    owner = Owner.create ();
    run_mutex = Mutex.create ();
    closed = Atomic.make false;
    stop_requested = Atomic.make false;
    public_mutex = Mutex.create ();
    cached = None;
    teardowns = Atomic.make 0;
    admitted = Atomic.make false;
  }

(** Terminates the process if a test does not finish within [seconds]. A
    deadlocked lifecycle test would otherwise hang the suite. [_exit] skips
    [at_exit] handlers that could themselves wait on a stuck thread. *)
let with_watchdog ?(seconds = 20.) label body =
  let finished = Atomic.make false in
  let _watchdog =
    Thread.create
      (fun () ->
        let deadline = Unix.gettimeofday () +. seconds in
        while (not (Atomic.get finished)) && Unix.gettimeofday () < deadline do
          Thread.delay 0.01
        done;
        if not (Atomic.get finished) then begin
          prerr_endline ("timed out (deadlock?) in " ^ label);
          Unix._exit 2
        end)
      ()
  in
  Fun.protect ~finally:(fun () -> Atomic.set finished true) body

(** Waits for a cross-thread observation with a deadline. [Thread.delay]
    releases the runtime lock so a sibling thread on this Domain can run. *)
let await label predicate =
  let deadline = Unix.gettimeofday () +. 10. in
  while not (predicate ()) do
    if Unix.gettimeofday () >= deadline then
      failwith ("timed out waiting for " ^ label);
    Thread.delay 0.001
  done

(** Mirrors [Native_worker.run]: reject a lane thread, then own [run_mutex] and
    the workflow lane for the whole loop. The activity lane records its thread
    before each poll, as the production adapter callback does. *)
let run worker ~poll_workflow ~poll_activity =
  if Owner.is_execution_thread worker.owner then Error "re-entrant run"
  else begin
    Mutex.lock worker.run_mutex;
    Owner.enter_run worker.owner;
    Fun.protect
      ~finally:(fun () ->
        Owner.leave worker.owner;
        Mutex.unlock worker.run_mutex)
      (fun () ->
        Loop.run
          ~closed:(fun () ->
            Atomic.get worker.closed || Atomic.get worker.stop_requested)
          ~poll_workflow
          ~poll_activity:(fun () ->
            Owner.enter_activity worker.owner;
            poll_activity ())
          ~wait_for_lane:(fun ~workflow_lane:_ ~native_wait:_ ->
            Thread.delay 0.001;
            Ok ())
          ~retry_pending:(fun ~workflow_lane:_ -> Ok ()))
  end

(** Mirrors [Worker.request_shutdown]: one atomic write, no lock, so it is
    safe from a signal handler on any thread. *)
let request_shutdown worker = Atomic.set worker.stop_requested true

(** Mirrors [Worker.shutdown] over [Native_worker.shutdown]: the execution
    thread check runs before the public mutex and posts a stop request (#830);
    an admitted caller publishes the stop flag, waits for [run_mutex], and
    caches the terminal result. *)
let shutdown worker =
  if Owner.is_execution_thread worker.owner then begin
    request_shutdown worker;
    Error "re-entrant shutdown"
  end
  else begin
    Mutex.lock worker.public_mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock worker.public_mutex)
      (fun () ->
        match worker.cached with
        | Some result -> result
        | None ->
            Atomic.set worker.closed true;
            Atomic.set worker.admitted true;
            Mutex.lock worker.run_mutex;
            Atomic.incr worker.teardowns;
            Mutex.unlock worker.run_mutex;
            let result = Ok () in
            worker.cached <- Some result;
            result)
  end

(** An idle lane poll. *)
let idle () = Ok Loop.Not_ready

(** Lane ownership is per system thread: the owner is an execution thread, a
    sibling thread on its Domain and a thread on another Domain are not, and
    [leave] clears both lanes. *)
let test_owner_identity () =
  let owner = Owner.create () in
  assert (not (Owner.is_execution_thread owner));
  Owner.enter_run owner;
  assert (Owner.is_execution_thread owner);
  let sibling = ref true in
  Thread.join
    (Thread.create (fun () -> sibling := Owner.is_execution_thread owner) ());
  assert (not !sibling);
  assert (not (Domain.join (Domain.spawn (fun () -> Owner.is_execution_thread owner))));
  let activity_owner, activity_sibling =
    Domain.join
      (Domain.spawn (fun () ->
           Owner.enter_activity owner;
           let sibling = ref true in
           Thread.join
             (Thread.create
                (fun () -> sibling := Owner.is_execution_thread owner)
                ());
           (Owner.is_execution_thread owner, !sibling)))
  in
  assert activity_owner;
  assert (not activity_sibling);
  Owner.leave owner;
  assert (not (Owner.is_execution_thread owner))

(** #763: a run loop hosted on a system thread is stopped by a shutdown from a
    sibling thread on the same Domain. Domain-granular identity rejected this
    caller on every retry, so the loop never stopped. *)
let test_sibling_thread_shutdown_stops_loop () =
  with_watchdog "sibling thread shutdown" (fun () ->
    let worker = worker () in
    let started = Atomic.make false in
    let run_result = ref (Error "not finished") in
    let runner =
      Thread.create
        (fun () ->
          run_result :=
            run worker
              ~poll_workflow:(fun () ->
                Atomic.set started true;
                idle ())
              ~poll_activity:idle)
        ()
    in
    await "run loop start" (fun () -> Atomic.get started);
    assert (shutdown worker = Ok ());
    Thread.join runner;
    assert (!run_result = Ok ());
    assert (Atomic.get worker.teardowns = 1);
    (* The cached result is returned again without a second teardown. *)
    assert (shutdown worker = Ok ());
    assert (Atomic.get worker.teardowns = 1))

(** #764: while an external caller holds the public mutex and waits for the
    loop, a callback on [lane] calls shutdown. It must receive the typed
    rejection immediately instead of waiting on the public mutex; the callback
    then returns, the loop stops, and the external caller completes. *)
let test_callback_shutdown_rejected_during_concurrent_shutdown ~workflow_lane ()
    =
  with_watchdog "re-entrant shutdown against concurrent shutdown" (fun () ->
    let worker = worker () in
    let callback_entered = Atomic.make false in
    let callback_result = Atomic.make None in
    let callback () =
      if Atomic.get callback_entered then idle ()
      else begin
        Atomic.set callback_entered true;
        await "external shutdown admission" (fun () ->
          Atomic.get worker.admitted);
        Atomic.set callback_result (Some (shutdown worker));
        Ok Loop.Progress
      end
    in
    let poll_workflow, poll_activity =
      if workflow_lane then (callback, idle) else (idle, callback)
    in
    let runner =
      Domain.spawn (fun () -> run worker ~poll_workflow ~poll_activity)
    in
    await "callback entry" (fun () -> Atomic.get callback_entered);
    let external_ = Domain.spawn (fun () -> shutdown worker) in
    assert (Domain.join external_ = Ok ());
    assert (Domain.join runner = Ok ());
    assert (Atomic.get callback_result = Some (Error "re-entrant shutdown"));
    assert (Atomic.get worker.teardowns = 1))

(** Several callers from sibling threads and other Domains shut down one
    running worker concurrently. All return, agree on the cached result, and
    exactly one teardown is performed. *)
let test_concurrent_shutdowns_agree () =
  with_watchdog "concurrent shutdowns" (fun () ->
    let worker = worker () in
    let started = Atomic.make false in
    let runner =
      Thread.create
        (fun () ->
          ignore
            (run worker
               ~poll_workflow:(fun () ->
                 Atomic.set started true;
                 idle ())
               ~poll_activity:idle))
        ()
    in
    await "run loop start" (fun () -> Atomic.get started);
    let thread_results = Array.make 3 (Error "not finished") in
    let threads =
      List.init 3 (fun index ->
        Thread.create
          (fun () -> thread_results.(index) <- shutdown worker)
          ())
    in
    let domains = List.init 2 (fun _ -> Domain.spawn (fun () -> shutdown worker)) in
    List.iter Thread.join threads;
    let domain_results = List.map Domain.join domains in
    Thread.join runner;
    List.iter (fun result -> assert (result = Ok ())) domain_results;
    Array.iter (fun result -> assert (result = Ok ())) thread_results;
    assert (Atomic.get worker.teardowns = 1))

(** #830: a callback on [lane] calls the blocking [shutdown], as a signal
    handler that the runtime runs on a lane thread would. It is rejected, but
    its stop request alone makes [run] return; no other thread calls shutdown.
    The thread that ran the loop then completes teardown itself. *)
let test_lane_shutdown_requests_stop ~workflow_lane () =
  with_watchdog "lane shutdown requests stop" (fun () ->
    let worker = worker () in
    let callback_result = Atomic.make None in
    let callback () =
      if Option.is_some (Atomic.get callback_result) then idle ()
      else begin
        Atomic.set callback_result (Some (shutdown worker));
        Ok Loop.Progress
      end
    in
    let poll_workflow, poll_activity =
      if workflow_lane then (callback, idle) else (idle, callback)
    in
    assert (run worker ~poll_workflow ~poll_activity = Ok ());
    assert (Atomic.get callback_result = Some (Error "re-entrant shutdown"));
    assert (Atomic.get worker.teardowns = 0);
    assert (shutdown worker = Ok ());
    assert (Atomic.get worker.teardowns = 1))

(** #830: a real [SIGUSR1] handler calling [request_shutdown] stops a run
    loop hosted on the main thread of the only application Domain, with no
    watcher Domain. A helper thread raises the signal with [Unix.kill] once the
    loop is polling; the runtime runs the handler at a safe point of whichever
    thread it picks, which here may be the run loop's own thread. [run] must
    return [Ok ()], and the same thread then completes shutdown. Windows has no
    [SIGUSR1] to deliver to itself, so there the handler is invoked directly. *)
let test_signal_handler_stops_main_thread_loop () =
  with_watchdog "signal handler stops loop" (fun () ->
    let worker = worker () in
    let started = Atomic.make false in
    let handled = Atomic.make false in
    let handler _signal =
      request_shutdown worker;
      Atomic.set handled true
    in
    let previous =
      if Sys.win32 then None
      else Some (Sys.signal Sys.sigusr1 (Sys.Signal_handle handler))
    in
    Fun.protect
      ~finally:(fun () ->
        Option.iter (Sys.set_signal Sys.sigusr1) previous)
      (fun () ->
        let raiser =
          Thread.create
            (fun () ->
              await "run loop start" (fun () -> Atomic.get started);
              if Sys.win32 then handler 0
              else Unix.kill (Unix.getpid ()) Sys.sigusr1)
            ()
        in
        let result =
          run worker
            ~poll_workflow:(fun () ->
              Atomic.set started true;
              idle ())
            ~poll_activity:idle
        in
        Thread.join raiser;
        assert (result = Ok ());
        assert (Atomic.get handled);
        assert (Atomic.get worker.teardowns = 0);
        assert (shutdown worker = Ok ());
        assert (Atomic.get worker.teardowns = 1)))

(** Runs every case. The concurrency cases repeat to expose interleavings. *)
let () =
  test_owner_identity ();
  for _ = 1 to 20 do
    test_sibling_thread_shutdown_stops_loop ();
    test_callback_shutdown_rejected_during_concurrent_shutdown
      ~workflow_lane:true ();
    test_callback_shutdown_rejected_during_concurrent_shutdown
      ~workflow_lane:false ();
    test_concurrent_shutdowns_agree ();
    test_lane_shutdown_requests_stop ~workflow_lane:true ();
    test_lane_shutdown_requests_stop ~workflow_lane:false ();
    test_signal_handler_stops_main_thread_loop ()
  done

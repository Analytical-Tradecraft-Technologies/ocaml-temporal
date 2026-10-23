(** Concurrent public [Worker.shutdown] callers on the deterministic mock
    backend (#764). Every caller, whether a sibling system thread or another
    Domain, must return and observe the same cached result. A signal handler
    calling [Worker.request_shutdown] must stop [run] between tasks (#830). The
    native
    re-entrancy cases live in [test/runtime/test_native_worker_shutdown.ml]
    because they need the private lane scheduler. *)

(** Terminates the process if the test does not finish in time, so a
    lifecycle deadlock fails instead of hanging the suite. *)
let with_watchdog label body =
  let finished = Atomic.make false in
  let _watchdog =
    Thread.create
      (fun () ->
        let deadline = Unix.gettimeofday () +. 20. in
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

(** Fails the test with the SDK diagnostic when a setup step fails. *)
let unwrap = function
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Starts shutdown concurrently from three threads on this Domain and two
    other Domains. All callers must succeed, and a later [run] must observe the
    terminal shutdown rather than polling a retired backend. *)
let test_concurrent_shutdowns_agree () =
  with_watchdog "concurrent mock shutdowns" (fun () ->
    let worker =
      unwrap
        (Temporal.Worker.create ~target_url:"mock://shutdown-concurrency"
           ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
           ~activities:[] ())
    in
    let go = Atomic.make false in
    let call () =
      while not (Atomic.get go) do
        Thread.yield ()
      done;
      Temporal.Worker.shutdown worker
    in
    let thread_results = Array.make 3 None in
    let threads =
      List.init 3 (fun index ->
        Thread.create (fun () -> thread_results.(index) <- Some (call ())) ())
    in
    let domains = List.init 2 (fun _ -> Domain.spawn call) in
    Atomic.set go true;
    List.iter Thread.join threads;
    let results =
      List.map Domain.join domains
      @ List.map Option.get (Array.to_list thread_results)
    in
    List.iter (fun result -> assert (Result.is_ok result)) results;
    assert (Result.is_ok (Temporal.Worker.shutdown worker));
    assert (Result.is_error (Temporal.Worker.run worker)))

(** Waits up to ten seconds for a flag set by a signal handler. The handler
    runs at a later safe point, which this loop's allocations provide. *)
let await_flag label flag =
  let deadline = Unix.gettimeofday () +. 10. in
  while not (Atomic.get flag) do
    if Unix.gettimeofday () >= deadline then
      failwith ("timed out waiting for " ^ label);
    Thread.yield ()
  done

(** #830: an activity callback on the thread running [Worker.run] raises
    [SIGUSR1]; the installed handler only calls [Worker.request_shutdown]. The
    mock backend queues one task per registered activity, so [run] must return
    [Ok ()] after the signalling task and before the other one. [shutdown] from
    the same thread then releases the worker, and a later [run] sees it shut
    down. Windows cannot deliver [SIGUSR1] to itself, so there the callback
    invokes the handler directly. *)
let test_signal_handler_request_stops_run () =
  with_watchdog "signal handler request_shutdown" (fun () ->
    let worker_cell = ref None in
    let handled = Atomic.make false in
    let handler _signal =
      Option.iter Temporal.Worker.request_shutdown !worker_cell;
      Atomic.set handled true
    in
    let calls = Atomic.make 0 in
    let activity name =
      Temporal.Activity.define ~name ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit (fun () ->
          if Atomic.fetch_and_add calls 1 = 0 then begin
            if Sys.win32 then handler 0
            else Unix.kill (Unix.getpid ()) Sys.sigusr1;
            await_flag "signal handler" handled
          end;
          Ok ())
    in
    let worker =
      unwrap
        (Temporal.Worker.create ~target_url:"mock://signal-request-shutdown"
           ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
           ~activities:
             [
               Temporal.Worker.activity (activity "unit.first");
               Temporal.Worker.activity (activity "unit.second");
             ]
           ())
    in
    worker_cell := Some worker;
    let previous =
      if Sys.win32 then None
      else Some (Sys.signal Sys.sigusr1 (Sys.Signal_handle handler))
    in
    Fun.protect
      ~finally:(fun () -> Option.iter (Sys.set_signal Sys.sigusr1) previous)
      (fun () ->
        assert (Temporal.Worker.run worker = Ok ());
        assert (Atomic.get calls = 1);
        (* The request is sticky: a second run returns without polling. *)
        assert (Temporal.Worker.run worker = Ok ());
        assert (Atomic.get calls = 1);
        assert (Result.is_ok (Temporal.Worker.shutdown worker));
        assert (Result.is_error (Temporal.Worker.run worker))))

(** The mock backend abandons nothing (#495): its report is clean, and a
    repeated call returns the same cached report through either entry
    point. *)
let test_mock_report_is_clean () =
  with_watchdog "mock shutdown report" (fun () ->
    let worker =
      unwrap
        (Temporal.Worker.create ~target_url:"mock://shutdown-report"
           ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
           ~activities:[] ())
    in
    let report = unwrap (Temporal.Worker.shutdown_with_report worker) in
    assert (Temporal.Worker.Shutdown_report.is_clean report);
    assert (report.lanes_stopped);
    assert (report.abandoned_activity_callbacks = 0);
    assert (report.abandoned_workflow_activations = 0);
    assert (report.native_teardown = `Completed);
    assert (Temporal.Worker.shutdown_with_report worker = Ok report);
    assert (Temporal.Worker.shutdown worker = Ok ());
    assert (
      not
        (Temporal.Worker.Shutdown_report.is_clean
           { report with abandoned_activity_callbacks = 1 }));
    assert (
      not
        (Temporal.Worker.Shutdown_report.is_clean
           { report with native_teardown = `Detached })))

(** Repeats the race to exercise different interleavings. *)
let () =
  test_mock_report_is_clean ();
  for _ = 1 to 50 do
    test_concurrent_shutdowns_agree ()
  done;
  test_signal_handler_request_stops_run ()

(** Concurrent public [Worker.shutdown] callers on the deterministic mock
    backend (#764). Every caller, whether a sibling system thread or another
    Domain, must return and observe the same cached result. The native
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

(** Repeats the race to exercise different interleavings. *)
let () =
  for _ = 1 to 50 do
    test_concurrent_shutdowns_agree ()
  done

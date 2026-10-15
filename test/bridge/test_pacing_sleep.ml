(** The bridge's worker idle pacing sleep (#1008) through the OCaml stub.

    The Rust suite checks the sleep's precision directly; this test covers the
    stub's seconds-to-nanoseconds conversion, its handling of requests that
    are not positive, and the release of the runtime lock. A wrong unit
    conversion would make a 2 ms request return at once or sleep for
    seconds. Precision is judged on the median of several sleeps, so one
    sleep delayed by a loaded runner cannot fail the test. *)

module Bridge = Temporal_core_bridge.Native_bridge

(** One default Windows system timer tick in seconds, the granularity that
    [Thread.delay] is bound to there and the pacing sleep must beat. *)
let windows_tick_s = 0.015625

(** Seconds on the bridge's monotonic clock. *)
let now () = Int64.to_float (Bridge.monotonic_now_ns ()) /. 1e9

(** Wall seconds taken by [Bridge.pacing_sleep seconds]. *)
let timed_sleep seconds =
  let started = now () in
  Bridge.pacing_sleep seconds;
  now () -. started

(** Requests that are not positive return at once. *)
let test_non_positive_requests () =
  List.iter
    (fun seconds -> assert (timed_sleep seconds < windows_tick_s))
    [ 0.; -1.; Float.nan; Float.neg_infinity ]

(** A request sleeps about as long as asked, and a 1 ms request typically
    lasts far less than one Windows timer tick. The 10% allowance only
    absorbs a timer that fires a few microseconds before the monotonic clock
    reaches the due time; a wrong unit would be off by orders of magnitude. *)
let test_precision () =
  assert (timed_sleep 0.002 >= 0.0018);
  let samples = Array.init 21 (fun _ -> timed_sleep 0.001) in
  Array.sort Float.compare samples;
  let median = samples.(Array.length samples / 2) in
  Printf.printf "median 1 ms pacing sleep: %.3f ms\n%!" (median *. 1000.);
  (* Enforced only on Windows, where the default timer tick is the
     regression this guards; elsewhere the sleep is nanosleep and a
     virtualised runner's scheduling delay is unrelated (#1008). *)
  if Sys.win32 && median >= windows_tick_s /. 2. then
    failwith
      (Printf.sprintf "median 1 ms pacing sleep took %.3f ms" (median *. 1000.))

(** A sibling system thread on the same Domain keeps running while one
    sleeps, so the stub released the runtime lock. Both threads share one
    Domain's runtime lock: a separate Domain has its own and would progress
    even if the stub held it. The observer spins in pure OCaml until a
    deadline well inside the 200 ms sleep; if the lock were held, it could
    only run after the sleep, so it records whether it finished first.
    [started] is never reset, so a late observer cannot wait forever. *)
let test_releases_runtime_lock () =
  let started = Atomic.make false in
  let sleeping = Atomic.make false in
  let finished_during_sleep = Atomic.make false in
  let sleeper =
    Thread.create
      (fun () ->
        Atomic.set sleeping true;
        Atomic.set started true;
        Bridge.pacing_sleep 0.2;
        Atomic.set sleeping false)
      ()
  in
  let observer =
    Thread.create
      (fun () ->
        while not (Atomic.get started) do
          Thread.yield ()
        done;
        let deadline = now () +. 0.05 in
        while now () < deadline do
          ()
        done;
        Atomic.set finished_during_sleep (Atomic.get sleeping))
      ()
  in
  Thread.join observer;
  Thread.join sleeper;
  if not (Atomic.get finished_during_sleep) then
    failwith "pacing_sleep held the runtime lock"

(** Runs the pacing sleep checks. *)
let () =
  test_non_positive_requests ();
  test_precision ();
  test_releases_runtime_lock ()

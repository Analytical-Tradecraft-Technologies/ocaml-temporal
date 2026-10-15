(** Regression tests for idle pacing between native lane polls (#994).

    The pure tests check the backoff schedule and the wait-joining rule
    without sleeping. The timed tests drive the real
    {!Temporal_runtime.Native_worker_loop} against a fake supervisor: one
    mutex stands for the single owner Domain, so a poll issued while the fake
    readiness wait runs queues behind it, as it does in the native mailbox.
    Their assertions are on poll order and on the pacing decisions (which
    delays the lanes request), never on wall time or on the total of
    requested delays, because those depend on the platform's sleep
    granularity. Every test wait has a deadline so a lost wake fails
    promptly. *)

(** Seconds on the native monotonic clock, which has sub-microsecond
    resolution on every supported platform. The simulated server round trips
    are sub-millisecond, finer than [Unix.gettimeofday] resolves on Windows.
    This alone did not fix the Windows failures; see {!sleep}. *)
let now () =
  Int64.to_float (Temporal_core_bridge.Native_bridge.monotonic_now_ns ())
  /. 1e9

module Loop = Temporal_runtime.Native_worker_loop
module Idle = Temporal_runtime.Native_worker_idle

(** Optional simulation of the Windows sleep, selected with
    [IDLE_TEST_SLEEP_MODEL=windows]. OCaml's Windows [Thread.delay] passes
    whole milliseconds to [Sleep], truncating: a request under 1 ms returns at
    once, and a longer one lasts until a later 15.625 ms timer tick. Sub-ms
    backoff steps are therefore free on Windows, and lanes take many more
    pacing decisions per unit of time than on macOS or Linux. Every fixture
    sleep, including the lanes' local yields, goes through {!sleep}, so the
    Windows CI interleaving can be reproduced locally. Unset, sleeps are
    passed through unchanged. *)
let windows_sleep_model =
  match Sys.getenv_opt "IDLE_TEST_SLEEP_MODEL" with
  | None | Some "" -> false
  | Some "windows" -> true
  | Some other -> invalid_arg ("unknown IDLE_TEST_SLEEP_MODEL " ^ other)

(** Every sleep in this fixture, in seconds. *)
let sleep seconds =
  if not windows_sleep_model then Thread.delay seconds
  else
    let whole_ms = Float.of_int (int_of_float (seconds *. 1000.)) in
    if whole_ms <= 0. then Thread.delay 0.
    else
      let tick_ms = 15.625 in
      Thread.delay (Float.ceil (whole_ms /. tick_ms) *. tick_ms /. 1000.)

(** Fails with [label] unless [condition] holds. *)
let check label condition = if not condition then failwith label

(** Waits for a cross-Domain observation with a test-only deadline. Sleeps in
    short slices so the observed Domains keep their CPU. *)
let await label predicate =
  let deadline = now () +. 5. in
  while not (predicate ()) do
    if now () >= deadline then
      failwith ("timed out waiting for " ^ label);
    sleep 0.0001
  done

(** The delay sequence starts at the first delay, doubles to the cap, and
    restarts at an immediate repoll after a completed task. *)
let test_backoff_schedule () =
  let idle = Idle.create ~first_delay_s:0.001 ~max_delay_s:0.008 () in
  let lane = Idle.lane idle in
  let delays = List.init 6 (fun _ -> Idle.next_delay lane) in
  check "backoff schedule"
    (delays = [ 0.001; 0.002; 0.004; 0.008; 0.008; 0.008 ]);
  Idle.note_work idle;
  let delays = List.init 3 (fun _ -> Idle.next_delay lane) in
  check "work event restarts the backoff" (delays = [ 0.; 0.001; 0.002 ])

(** A readiness wake restarts the sibling's backoff but not the waiter's. *)
let test_ready_restarts_only_sibling () =
  let idle = Idle.create ~first_delay_s:0.001 ~max_delay_s:0.008 () in
  let waiter = Idle.lane idle in
  let sibling = Idle.lane idle in
  for _ = 1 to 5 do
    ignore (Idle.next_delay waiter);
    ignore (Idle.next_delay sibling)
  done;
  check "wait reports readiness"
    (Idle.wait waiter ~native_wait:true (fun () -> Ok true) = Ok ());
  check "sibling repolls immediately" (Idle.next_delay sibling = 0.);
  check "waiter keeps its backoff" (Idle.next_delay waiter = 0.008);
  check "timed-out wait is not a work event"
    (Idle.wait waiter ~native_wait:true (fun () -> Ok false) = Ok ());
  check "sibling resumes backoff after a timeout"
    (Idle.next_delay sibling = 0.001)

(** A sibling joins an in-flight native wait with one immediate repoll per
    wait, never more, and the in-flight mark is cleared even on an
    exception. *)
let test_join_in_flight_wait_once () =
  let idle = Idle.create ~first_delay_s:0.001 ~max_delay_s:0.008 () in
  let waiter = Idle.lane idle in
  let sibling = Idle.lane idle in
  let inside =
    Idle.native_wait waiter (fun () ->
        let first = Idle.next_delay sibling in
        let second = Idle.next_delay sibling in
        (first, second))
  in
  check "joins the wait once" (inside = (0., 0.001));
  check "no join after the wait ended" (Idle.next_delay sibling = 0.002);
  let second_wait = Idle.native_wait waiter (fun () -> Idle.next_delay sibling) in
  check "joins each new wait" (second_wait = 0.);
  (match Idle.native_wait waiter (fun () -> failwith "wait raised") with
  | () -> failwith "native wait swallowed an exception"
  | exception Failure _ -> ());
  check "raised wait no longer in flight" (Idle.next_delay sibling > 0.);
  check "wait error is returned unchanged"
    (Idle.wait waiter ~native_wait:true (fun () -> Error "lost") = Error "lost")

(** Rejects a delay configuration that could spin or never back off. *)
let test_invalid_configuration () =
  let rejects f =
    match f () with
    | (_ : Idle.t) -> false
    | exception Invalid_argument _ -> true
  in
  check "zero first delay" (rejects (fun () -> Idle.create ~first_delay_s:0. ()));
  check "first delay above cap"
    (rejects (fun () -> Idle.create ~first_delay_s:0.02 ~max_delay_s:0.01 ()))

(** A joining poll served before the wait's mailbox admission returns empty
    while the wait is still in flight. The lane must then restart its
    backoff short for that wait, not resume a capped sleep, and only once per
    wait (PR #996 review). *)
let test_early_join_poll_restarts_backoff () =
  let idle = Idle.create ~first_delay_s:0.001 ~max_delay_s:0.008 () in
  let waiter = Idle.lane idle in
  let sibling = Idle.lane idle in
  for _ = 1 to 6 do
    ignore (Idle.next_delay sibling)
  done;
  check "sibling settled at the cap" (Idle.next_delay sibling = 0.008);
  let inside =
    Idle.native_wait waiter (fun () ->
        (* Join; the joining poll returns empty before admission; the next
           delays are taken while the same wait is still in flight. *)
        List.init 4 (fun _ -> Idle.next_delay sibling))
  in
  check "early join poll restarts the backoff once"
    (inside = [ 0.; 0.001; 0.002; 0.004 ]);
  check "backoff continues after the wait" (Idle.next_delay sibling = 0.008)

(** A fake supervisor with one owner mutex and per-lane task visibility
    times. A lane has a task when [now] has reached its
    [visible_at]; [infinity] means none. [on_take] runs inside the owner when
    a poll takes a task and returns the next visibility time of the other
    lane's task, or [None] to leave it unchanged.

    Per-lane arrays are indexed by {!lane_index}. [empty_polls] counts polls
    that returned no task; [queued] counts polls waiting for the owner;
    [sleeps] counts the local yields a lane requested and [slept_us] sums
    their requested microseconds over both lanes, so assertions do not depend
    on how precisely the OS honours a sleep. [capped_sleeps] counts requested
    yields at the {!Idle.default_max_delay_s} cap over both lanes. [max_sleep_s] is the largest
    yield a lane requested since the test last reset it. [taken_after] holds
    the taking lane's [empty_polls] and [sleeps] at its latest take.
    [wait_holding] is [true] while a fake wait holds the owner.
    [arm_admission_race] makes the next native wait hold back its owner
    admission until the sibling lane has returned two empty polls, forcing a
    joining poll to be served before the wait; [raced_waiter] then names the
    waiting lane ([true] for the workflow lane). The raced wait lasts up to
    {!raced_wait_timeout_s}, so it stays in flight until the test's task. *)
type fake = {
  owner : Mutex.t;
  workflow_at : float Atomic.t;
  activity_at : float Atomic.t;
  closed : bool Atomic.t;
  polls : int Atomic.t;
  native_waits : int Atomic.t;
  wait_timeout_s : float;
  wait_holding : bool Atomic.t;
  on_take : workflow_lane:bool -> float option;
  empty_polls : int Atomic.t array;
  queued : int Atomic.t array;
  sleeps : int Atomic.t array;
  slept_us : int Atomic.t;
  capped_sleeps : int Atomic.t;
  max_sleep_s : float Atomic.t array;
  taken_after : (int * int) Atomic.t;
  arm_admission_race : bool Atomic.t;
  raced_waiter : bool option Atomic.t;
}

(** Timeout of the wait armed by [arm_admission_race], in seconds. *)
let raced_wait_timeout_s = 2.

(** Array index of a lane in the per-lane counters. *)
let lane_index ~workflow_lane = if workflow_lane then 0 else 1

(** Makes a fake with no pending task. *)
let make_fake ?(closed = Atomic.make false) ?(wait_timeout_s = 0.1)
    ?(on_take = fun ~workflow_lane:_ -> None) () =
  let pair () = [| Atomic.make 0; Atomic.make 0 |] in
  {
    owner = Mutex.create ();
    workflow_at = Atomic.make infinity;
    activity_at = Atomic.make infinity;
    closed;
    polls = Atomic.make 0;
    native_waits = Atomic.make 0;
    wait_timeout_s;
    wait_holding = Atomic.make false;
    on_take;
    empty_polls = pair ();
    queued = pair ();
    sleeps = pair ();
    slept_us = Atomic.make 0;
    capped_sleeps = Atomic.make 0;
    max_sleep_s = [| Atomic.make 0.; Atomic.make 0. |];
    taken_after = Atomic.make (-1, -1);
    arm_admission_race = Atomic.make false;
    raced_waiter = Atomic.make None;
  }

(** Runs [f] as one supervisor operation, counting lane [lane] as queued
    while it waits for the owner. *)
let with_owner ?lane fake f =
  Option.iter (fun i -> ignore (Atomic.fetch_and_add fake.queued.(i) 1)) lane;
  Mutex.lock fake.owner;
  Option.iter
    (fun i -> ignore (Atomic.fetch_and_add fake.queued.(i) (-1)))
    lane;
  Fun.protect ~finally:(fun () -> Mutex.unlock fake.owner) f

(** One non-blocking lane poll through the fake owner. A take records the
    lane's empty-poll and sleep counts at that moment in [taken_after]. *)
let fake_poll fake ~workflow_lane =
  let i = lane_index ~workflow_lane in
  with_owner ~lane:i fake (fun () ->
      ignore (Atomic.fetch_and_add fake.polls 1);
      let mine, other =
        if workflow_lane then (fake.workflow_at, fake.activity_at)
        else (fake.activity_at, fake.workflow_at)
      in
      if now () >= Atomic.get mine then begin
        Atomic.set fake.taken_after
          (Atomic.get fake.empty_polls.(i), Atomic.get fake.sleeps.(i));
        Atomic.set mine infinity;
        Option.iter (Atomic.set other) (fake.on_take ~workflow_lane);
        Ok Loop.Progress
      end
      else begin
        ignore (Atomic.fetch_and_add fake.empty_polls.(i) 1);
        Ok Loop.Not_ready
      end)

(** The combined readiness wait: holds the owner until either lane has a
    visible task or [wait_timeout_s] elapses. Like the Rust wait, it does not
    observe the OCaml stop flag. The 50 us recheck stands in for the Rust
    condition variable. An armed race first lets the sibling's joining poll
    through, as a poll admitted to the mailbox just ahead of the wait. *)
let fake_wait fake ~workflow_lane =
  let raced = Atomic.compare_and_set fake.arm_admission_race true false in
  if raced then begin
    let sibling = lane_index ~workflow_lane:(not workflow_lane) in
    Atomic.set fake.max_sleep_s.(sibling) 0.;
    Atomic.set fake.raced_waiter (Some workflow_lane);
    (* A second empty poll after the wait was marked in flight must have
       started after it, so the sibling joined this wait before polling. *)
    let baseline = Atomic.get fake.empty_polls.(sibling) in
    await "the sibling's early joining poll" (fun () ->
        Atomic.get fake.empty_polls.(sibling) >= baseline + 2)
  end;
  with_owner fake (fun () ->
      ignore (Atomic.fetch_and_add fake.native_waits 1);
      Atomic.set fake.wait_holding true;
      Fun.protect ~finally:(fun () -> Atomic.set fake.wait_holding false)
      @@ fun () ->
      let timeout = if raced then raced_wait_timeout_s else fake.wait_timeout_s in
      let deadline = now () +. timeout in
      let ready () =
        let now = now () in
        now >= Atomic.get fake.workflow_at || now >= Atomic.get fake.activity_at
      in
      while (not (ready ())) && now () < deadline do
        sleep 0.00005
      done;
      Ok (ready ()))

(** Records one requested local yield of [delay] seconds for lane [i]. Only
    lane [i]'s thread writes [max_sleep_s.(i)] apart from a test reset. *)
let record_sleep fake i delay =
  if delay > 0. then begin
    ignore (Atomic.fetch_and_add fake.sleeps.(i) 1);
    ignore
      (Atomic.fetch_and_add fake.slept_us
         (int_of_float (Float.round (delay *. 1e6))));
    if delay >= Idle.default_max_delay_s then
      ignore (Atomic.fetch_and_add fake.capped_sleeps 1);
    if delay > Atomic.get fake.max_sleep_s.(i) then
      Atomic.set fake.max_sleep_s.(i) delay
  end

(** Runs the generic loop over [fake] with the production idle pacing wiring:
    a completed task is a work event, and each lane owns its [Idle.lane]. The
    local yield is [Idle.local_yield] spelled out, so the fixture can record
    each requested delay. *)
let run_fake fake =
  let idle = Idle.create () in
  let workflow_idle = Idle.lane idle in
  let activity_idle = Idle.lane idle in
  let poll ~workflow_lane () =
    let result = fake_poll fake ~workflow_lane in
    (match result with Ok Loop.Progress -> Idle.note_work idle | _ -> ());
    result
  in
  Loop.run ~activity_lane:true ~lane_unavailable:raise ~detach:None
    ~closed:(fun () -> Atomic.get fake.closed)
    ~poll_workflow:(poll ~workflow_lane:true)
    ~poll_activity:(poll ~workflow_lane:false)
    ~wait_for_lane:(fun ~workflow_lane ~native_wait ->
      let lane = if workflow_lane then workflow_idle else activity_idle in
      if native_wait then
        Idle.wait lane ~native_wait (fun () -> fake_wait fake ~workflow_lane)
      else begin
        let delay = Idle.next_delay lane in
        record_sleep fake (lane_index ~workflow_lane) delay;
        if delay > 0. then sleep delay;
        Ok ()
      end)
    ~retry_pending:(fun ~workflow_lane:_ ->
      failwith "idle pacing fixture entered completion retry")

(** Stops [fake], joins its runner, and re-raises a failed observation. *)
let finish fake runner observation =
  Atomic.set fake.closed true;
  let result = Domain.join runner in
  let value =
    match observation with Ok value -> value | Error e -> raise e
  in
  match result with
  | Ok () -> value
  | Error () -> failwith "idle pacing fixture returned an error"

(** Publishes a task for one lane and waits until it is taken. Returns the
    wall latency, for the log only, and checks the poll order: the lane took
    the task without one more empty poll or local yield after publication,
    so it was released by the readiness wait rather than by a backoff
    timer. *)
let publish_and_take fake ~workflow_lane =
  let i = lane_index ~workflow_lane in
  let cell = if workflow_lane then fake.workflow_at else fake.activity_at in
  let before = (Atomic.get fake.empty_polls.(i), Atomic.get fake.sleeps.(i)) in
  let published = now () in
  Atomic.set cell published;
  await "the ready task to be taken" (fun () -> Atomic.get cell = infinity);
  let latency = now () -. published in
  if Atomic.get fake.taken_after <> before then
    failwith "ready task was taken only after an extra poll or local yield";
  latency

(** A task that becomes ready on an idle worker is taken by the poll the
    readiness wait releases, on either lane. The task is published once a
    native wait has been in flight long enough for the sibling to settle,
    whether it joined the wait or was sleeping at its 10 ms cap (up to one
    15.6 ms timer tick on Windows); a long fake timeout keeps the wait in
    flight meanwhile. *)
let test_ready_lane_wakes_promptly () =
  let fake = make_fake ~wait_timeout_s:2. () in
  let runner = Domain.spawn (fun () -> run_fake fake) in
  let observation =
    try
      let worst = ref 0. in
      for step = 1 to 10 do
        let waits = Atomic.get fake.native_waits in
        await "a fresh native wait" (fun () ->
            Atomic.get fake.native_waits > waits);
        sleep 0.1;
        let latency = publish_and_take fake ~workflow_lane:(step mod 2 = 0) in
        worst := Float.max !worst latency
      done;
      Ok !worst
    with exception_ -> Error exception_
  in
  let worst = finish fake runner observation in
  Printf.printf "ready lane worst wake: %.3f ms\n%!" (worst *. 1000.)

(** Forces the admission race end to end: the sibling's joining poll is
    served before the native wait takes the owner. The sibling must requeue
    behind the wait after short yields only, never the 10 ms cap, and then
    take its task on the poll the wait releases. *)
let test_early_join_poll_requeues_behind_wait () =
  let fake = make_fake () in
  let runner = Domain.spawn (fun () -> run_fake fake) in
  let observation =
    try
      (* Let a few short idle waits pass so both lanes have backed off. How
         far each lane backed off depends on the platform's sleep
         granularity, so this test checks only the requeue yields, and
         [test_early_join_poll_restarts_backoff] checks deterministically
         that an early poll from a capped lane restarts short. *)
      let waits = Atomic.get fake.native_waits in
      await "a few idle native waits" (fun () ->
          Atomic.get fake.native_waits >= waits + 3);
      Atomic.set fake.arm_admission_race true;
      await "the raced native wait" (fun () ->
          Option.is_some (Atomic.get fake.raced_waiter));
      let sibling = not (Option.get (Atomic.get fake.raced_waiter)) in
      let i = lane_index ~workflow_lane:sibling in
      (* Queued while the raced wait holds the owner, so the poll cannot run
         before the wait ends. The raced wait is the only one in flight:
         the sibling's poll was admitted ahead of it. *)
      await "the sibling to queue behind the raced wait" (fun () ->
          Atomic.get fake.wait_holding && Atomic.get fake.queued.(i) > 0);
      let max_sleep = Atomic.get fake.max_sleep_s.(i) in
      if max_sleep >= Idle.default_max_delay_s then
        failwith
          (Printf.sprintf "early joining poll resumed a %.1f ms sleep"
             (max_sleep *. 1000.));
      ignore (publish_and_take fake ~workflow_lane:sibling);
      Ok max_sleep
    with exception_ -> Error exception_
  in
  let max_sleep = finish fake runner observation in
  Printf.printf "early join poll: longest requeue yield %.2f ms\n%!"
    (max_sleep *. 1000.)

(** A serial activity workflow alternates lanes on every step, with a
    simulated 0.5 ms server round trip before each follow-up task. The
    assertion is on the pacing decisions, not on time: the fixed 10 ms yield
    requested about two capped yields per step, while adaptive pacing, which
    restarts after every completed task, must request a capped yield in at
    most one step in two. Neither wall time nor the total of requested
    delays is bounded, because both depend on the platform's sleep
    granularity: on Windows a sub-millisecond [Thread.delay] returns at once,
    so the lanes take more backoff steps per round trip (about 6.5 ms of
    requested yields per step, against 0.2 ms on macOS, with the same
    decisions after each work event). *)
let test_serial_chain_avoids_fixed_sleep () =
  let steps = 100 in
  let rtt_s = 0.0005 in
  let activities = Atomic.make 0 in
  let closed = Atomic.make false in
  let on_take ~workflow_lane =
    if workflow_lane then
      if Atomic.get activities >= steps then begin
        Atomic.set closed true;
        None
      end
      else Some (now () +. rtt_s)
    else begin
      ignore (Atomic.fetch_and_add activities 1);
      Some (now () +. rtt_s)
    end
  in
  let fake = make_fake ~closed ~on_take () in
  let started = now () in
  Atomic.set fake.workflow_at started;
  let runner = Domain.spawn (fun () -> run_fake fake) in
  let observation =
    try
      let deadline = now () +. 60. in
      while not (Atomic.get fake.closed) do
        if now () >= deadline then
          failwith "timed out waiting for serial chain completion";
        sleep 0.001
      done;
      Ok (now () -. started)
    with exception_ -> Error exception_
  in
  let elapsed = finish fake runner observation in
  check "every activity completed" (Atomic.get activities = steps);
  let per_step ms = ms /. float_of_int steps in
  let wall_ms = per_step (elapsed *. 1000.) in
  let slept_ms = per_step (float_of_int (Atomic.get fake.slept_us) /. 1000.) in
  Printf.printf
    "serial chain per step: %.3f ms wall, %.3f ms of requested yields (%.1f \
     ms simulated round trips)\n\
     %!"
    wall_ms slept_ms (2. *. rtt_s *. 1000.);
  let capped = Atomic.get fake.capped_sleeps in
  Printf.printf "serial chain capped yields: %d in %d steps\n%!" capped steps;
  if capped * 2 > steps then
    failwith
      (Printf.sprintf "serial chain requested %d capped yields in %d steps"
         capped steps)

(** An idle worker does not spin: over one second the lanes make a bounded
    number of polls and native waits. A busy loop would make tens of
    thousands. *)
let test_idle_worker_does_not_spin () =
  let fake = make_fake () in
  let runner = Domain.spawn (fun () -> run_fake fake) in
  let observation =
    try
      sleep 0.2;
      let polls = Atomic.get fake.polls in
      let waits = Atomic.get fake.native_waits in
      sleep 1.;
      Ok (Atomic.get fake.polls - polls, Atomic.get fake.native_waits - waits)
    with exception_ -> Error exception_
  in
  let polls, waits = finish fake runner observation in
  Printf.printf "idle worker in 1 s: %d polls, %d native waits\n%!" polls waits;
  if polls > 250 || waits > 20 then
    failwith
      (Printf.sprintf "idle worker made %d polls and %d native waits in 1 s"
         polls waits)

(** A stop observed while idle ends the run within one native wait timeout
    plus one capped local yield, as before (#495). The assertion allows a
    loaded runner several times that bound; an unbounded wait would still
    fail it. *)
let test_stop_while_idle_is_bounded () =
  let fake = make_fake () in
  let runner = Domain.spawn (fun () -> run_fake fake) in
  sleep 0.2;
  let stopped = now () in
  Atomic.set fake.closed true;
  (match Domain.join runner with
  | Ok () -> ()
  | Error () -> failwith "stopped idle fixture returned an error");
  let elapsed = now () -. stopped in
  Printf.printf "idle stop returned in %.1f ms\n%!" (elapsed *. 1000.);
  if elapsed >= 1. then
    failwith (Printf.sprintf "idle stop took %.0f ms" (elapsed *. 1000.))

(** Runs the idle pacing regressions. *)
let () =
  test_backoff_schedule ();
  test_ready_restarts_only_sibling ();
  test_join_in_flight_wait_once ();
  test_early_join_poll_restarts_backoff ();
  test_invalid_configuration ();
  test_ready_lane_wakes_promptly ();
  test_early_join_poll_requeues_behind_wait ();
  test_serial_chain_avoids_fixed_sleep ();
  test_idle_worker_does_not_spin ();
  test_stop_while_idle_is_bounded ()

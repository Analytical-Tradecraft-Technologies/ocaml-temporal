(** Regression tests for idle pacing between native lane polls (#994).

    The pure tests check the backoff schedule and the wait-joining rule
    without sleeping. The timed tests drive the real
    {!Temporal_runtime.Native_worker_loop} against a fake supervisor: one
    mutex stands for the single owner Domain, so a poll issued while the fake
    readiness wait runs queues behind it, as it does in the native mailbox.
    Every test wait has a deadline so a lost wake fails promptly. *)

module Loop = Temporal_runtime.Native_worker_loop
module Idle = Temporal_runtime.Native_worker_idle

(** Fails with [label] unless [condition] holds. *)
let check label condition = if not condition then failwith label

(** Waits for a cross-Domain observation with a test-only deadline. Sleeps in
    short slices so the observed Domains keep their CPU. *)
let await label predicate =
  let deadline = Unix.gettimeofday () +. 5. in
  while not (predicate ()) do
    if Unix.gettimeofday () >= deadline then
      failwith ("timed out waiting for " ^ label);
    Thread.delay 0.0001
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

(** A fake supervisor with one owner mutex and per-lane task visibility
    times. A lane has a task when [Unix.gettimeofday] has reached its
    [visible_at]; [infinity] means none. [on_take] runs inside the owner when
    a poll takes a task and returns the next visibility time of the other
    lane's task, or [None] to leave it unchanged. *)
type fake = {
  owner : Mutex.t;
  workflow_at : float Atomic.t;
  activity_at : float Atomic.t;
  closed : bool Atomic.t;
  polls : int Atomic.t;
  native_waits : int Atomic.t;
  wait_timeout_s : float;
  on_take : workflow_lane:bool -> float option;
}

(** Makes a fake with no pending task. *)
let make_fake ?(closed = Atomic.make false) ?(wait_timeout_s = 0.1)
    ?(on_take = fun ~workflow_lane:_ -> None) () =
  {
    owner = Mutex.create ();
    workflow_at = Atomic.make infinity;
    activity_at = Atomic.make infinity;
    closed;
    polls = Atomic.make 0;
    native_waits = Atomic.make 0;
    wait_timeout_s;
    on_take;
  }

(** Runs [f] as one supervisor operation. *)
let with_owner fake f =
  Mutex.lock fake.owner;
  Fun.protect ~finally:(fun () -> Mutex.unlock fake.owner) f

(** One non-blocking lane poll through the fake owner. *)
let fake_poll fake ~workflow_lane =
  with_owner fake (fun () ->
      ignore (Atomic.fetch_and_add fake.polls 1);
      let mine, other =
        if workflow_lane then (fake.workflow_at, fake.activity_at)
        else (fake.activity_at, fake.workflow_at)
      in
      if Unix.gettimeofday () >= Atomic.get mine then begin
        Atomic.set mine infinity;
        Option.iter (Atomic.set other) (fake.on_take ~workflow_lane);
        Ok Loop.Progress
      end
      else Ok Loop.Not_ready)

(** The combined readiness wait: holds the owner until either lane has a
    visible task or [wait_timeout_s] elapses. Like the Rust wait, it does not
    observe the OCaml stop flag. The 50 us recheck stands in for the Rust
    condition variable. *)
let fake_wait fake =
  with_owner fake (fun () ->
      ignore (Atomic.fetch_and_add fake.native_waits 1);
      let deadline = Unix.gettimeofday () +. fake.wait_timeout_s in
      let ready () =
        let now = Unix.gettimeofday () in
        now >= Atomic.get fake.workflow_at || now >= Atomic.get fake.activity_at
      in
      while (not (ready ())) && Unix.gettimeofday () < deadline do
        Thread.delay 0.00005
      done;
      Ok (ready ()))

(** Runs the generic loop over [fake] with the production idle pacing wiring:
    a completed task is a work event, and each lane owns its [Idle.lane]. *)
let run_fake fake =
  let idle = Idle.create () in
  let workflow_idle = Idle.lane idle in
  let activity_idle = Idle.lane idle in
  let poll ~workflow_lane () =
    let result = fake_poll fake ~workflow_lane in
    (match result with Ok Loop.Progress -> Idle.note_work idle | _ -> ());
    result
  in
  Loop.run ~detach:None
    ~closed:(fun () -> Atomic.get fake.closed)
    ~poll_workflow:(poll ~workflow_lane:true)
    ~poll_activity:(poll ~workflow_lane:false)
    ~wait_for_lane:(fun ~workflow_lane ~native_wait ->
      Idle.wait
        (if workflow_lane then workflow_idle else activity_idle)
        ~native_wait
        (fun () -> fake_wait fake))
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

(** A task that becomes ready on an idle worker is taken well under the
    former 10 ms sleep, on either lane. The task is published only once a
    native wait has been in flight long enough for the sibling to settle,
    whether that sibling joined the wait or is sleeping at its 10 ms cap; a
    long fake timeout keeps the wait in flight meanwhile. *)
let test_ready_lane_wakes_promptly () =
  let fake = make_fake ~wait_timeout_s:2. () in
  let runner = Domain.spawn (fun () -> run_fake fake) in
  let observation =
    try
      let worst = ref 0. in
      for step = 1 to 10 do
        let workflow_lane = step mod 2 = 0 in
        let waits = Atomic.get fake.native_waits in
        await "a fresh native wait" (fun () ->
            Atomic.get fake.native_waits > waits);
        Thread.delay 0.03;
        let cell = if workflow_lane then fake.workflow_at else fake.activity_at in
        let polls = Atomic.get fake.polls in
        let published = Unix.gettimeofday () in
        Atomic.set cell published;
        await "the ready task to be taken" (fun () ->
            Atomic.get cell = infinity);
        let latency = Unix.gettimeofday () -. published in
        check "taken by a poll" (Atomic.get fake.polls > polls);
        worst := Float.max !worst latency
      done;
      Ok !worst
    with exception_ -> Error exception_
  in
  let worst = finish fake runner observation in
  Printf.printf "ready lane worst wake: %.3f ms\n%!" (worst *. 1000.);
  if worst >= 0.005 then
    failwith (Printf.sprintf "ready lane woke after %.2f ms" (worst *. 1000.))

(** A serial activity workflow alternates lanes on every step, with a
    simulated 0.5 ms server round trip before each follow-up task. The fixed
    10 ms sleep made each crossing cost several milliseconds; adaptive pacing
    must keep the mean step well below that floor. *)
let test_serial_chain_avoids_fixed_sleep () =
  let steps = 200 in
  let rtt_s = 0.0005 in
  let activities = Atomic.make 0 in
  let closed = Atomic.make false in
  let on_take ~workflow_lane =
    if workflow_lane then
      if Atomic.get activities >= steps then begin
        Atomic.set closed true;
        None
      end
      else Some (Unix.gettimeofday () +. rtt_s)
    else begin
      ignore (Atomic.fetch_and_add activities 1);
      Some (Unix.gettimeofday () +. rtt_s)
    end
  in
  let fake = make_fake ~closed ~on_take () in
  let started = Unix.gettimeofday () in
  Atomic.set fake.workflow_at started;
  let runner = Domain.spawn (fun () -> run_fake fake) in
  let observation =
    try
      await "serial chain completion" (fun () -> Atomic.get fake.closed);
      Ok (Unix.gettimeofday () -. started)
    with exception_ -> Error exception_
  in
  let elapsed = finish fake runner observation in
  check "every activity completed" (Atomic.get activities = steps);
  let per_step_ms = elapsed /. float_of_int steps *. 1000. in
  Printf.printf "serial chain mean step: %.3f ms (%.1f ms simulated round trips)\n%!"
    per_step_ms (2. *. rtt_s *. 1000.);
  if per_step_ms >= 4. then
    failwith (Printf.sprintf "serial step took %.2f ms on average" per_step_ms)

(** An idle worker does not spin: over one second the lanes make a bounded
    number of polls and native waits. A busy loop would make tens of
    thousands. *)
let test_idle_worker_does_not_spin () =
  let fake = make_fake () in
  let runner = Domain.spawn (fun () -> run_fake fake) in
  let observation =
    try
      Thread.delay 0.2;
      let polls = Atomic.get fake.polls in
      let waits = Atomic.get fake.native_waits in
      Thread.delay 1.;
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
    plus one capped local yield, as before (#495). *)
let test_stop_while_idle_is_bounded () =
  let fake = make_fake () in
  let runner = Domain.spawn (fun () -> run_fake fake) in
  Thread.delay 0.2;
  let stopped = Unix.gettimeofday () in
  Atomic.set fake.closed true;
  (match Domain.join runner with
  | Ok () -> ()
  | Error () -> failwith "stopped idle fixture returned an error");
  let elapsed = Unix.gettimeofday () -. stopped in
  Printf.printf "idle stop returned in %.1f ms\n%!" (elapsed *. 1000.);
  if elapsed >= 0.3 then
    failwith (Printf.sprintf "idle stop took %.0f ms" (elapsed *. 1000.))

(** Runs the idle pacing regressions. *)
let () =
  test_backoff_schedule ();
  test_ready_restarts_only_sibling ();
  test_join_in_flight_wait_once ();
  test_invalid_configuration ();
  test_ready_lane_wakes_promptly ();
  test_serial_chain_avoids_fixed_sleep ();
  test_idle_worker_does_not_spin ();
  test_stop_while_idle_is_bounded ()

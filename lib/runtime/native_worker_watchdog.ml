(** Private watchdog for workflow activations that do not yield. The
    interface documents the detection contract. *)

(** [observed] is the epoch seen at the previous tick, [elapsed_ms] how long it
    has been seen continuously, and [fired] the last epoch already reported, so
    a still-stuck activation is reported once. *)
type state = { observed : int option; elapsed_ms : int; fired : int option }

(** The state before the first sample. *)
let initial = { observed = None; elapsed_ms = 0; fired = None }

(** A quarter of the deadline keeps detection within 25% of it, while the
    bounds avoid busy sampling for tiny test deadlines and keep [stop] prompt
    for long ones. *)
let tick_ms ~deadline_ms = Int.min 250 (Int.max 5 (deadline_ms / 4))

(** Counts continuous ticks of one epoch. A different epoch, or none, restarts
    the count at zero because the new activation began at an unknown point
    within the previous tick. *)
let step state ~running ~tick_ms ~deadline_ms =
  match running with
  | None -> ({ state with observed = None; elapsed_ms = 0 }, None)
  | Some epoch ->
      let elapsed_ms =
        match state.observed with
        | Some previous when previous = epoch -> state.elapsed_ms + tick_ms
        | Some _ | None -> 0
      in
      let already_fired =
        match state.fired with Some fired -> fired = epoch | None -> false
      in
      let state = { state with observed = Some epoch; elapsed_ms } in
      if elapsed_ms >= deadline_ms && not already_fired then
        ({ state with fired = Some epoch }, Some (epoch, elapsed_ms))
      else (state, None)

(** [stop_requested] is written by [stop] on the run loop's thread and read
    by the sampling thread between sleep slices. [finished] is set, under
    [mutex], by the sampling thread as its last action and broadcast on
    [exited]; [stop] waits for it instead of [Thread.join] because the
    sampling thread usually lives on another Domain (the supervisor's). *)
type t = {
  stop_requested : bool Atomic.t;
  mutex : Mutex.t;
  exited : Condition.t;
  mutable finished : bool;
}

let stop_poll_s = 0.02

(** Sleeps for [seconds] in slices of at most {!stop_poll_s}, returning early
    once [should_stop ()] holds. A whole-tick sleep made [stop] wait up to
    250 ms, which delayed [Worker.run]'s return past the bounded-shutdown
    slack for short grace periods (#1023). The sliced sleep keeps a tick's
    length unchanged, so detection timing is unaffected. *)
let sleep_unless_stopped ~should_stop seconds =
  let rec loop remaining =
    if remaining > 0. && not (should_stop ()) then begin
      let slice = Float.min remaining stop_poll_s in
      Thread.delay slice;
      loop (remaining -. slice)
    end
  in
  loop seconds

(** Samples until [should_stop] holds. [Thread.delay] releases the host
    Domain's runtime lock while sleeping. Callback exceptions are contained:
    a diagnostic defect must not end detection. *)
let run ~should_stop ~deadline_ms ~running_epoch ~abandon =
  let tick_ms = tick_ms ~deadline_ms in
  let tick_seconds = Float.of_int tick_ms /. 1_000. in
  let rec loop state =
    if not (should_stop ()) then begin
      sleep_unless_stopped ~should_stop tick_seconds;
      if not (should_stop ()) then begin
        let running = try running_epoch () with _ -> None in
        let state, fire = step state ~running ~tick_ms ~deadline_ms in
        (match fire with
        | Some (epoch, elapsed_ms) -> (
            try abandon ~epoch ~elapsed_ms with _ -> ())
        | None -> ());
        loop state
      end
    end
  in
  loop initial

(** Publishes the sampling thread's exit to {!stop}. *)
let mark_finished watchdog =
  Mutex.lock watchdog.mutex;
  watchdog.finished <- true;
  Condition.broadcast watchdog.exited;
  Mutex.unlock watchdog.mutex

(** Starts the sampling thread through [spawn]. The watchdog no longer owns a
    Domain (#1042): the caller chooses a host whose runtime lock the workflow
    lane never holds, so a lane that never yields cannot starve the sampling
    thread. A raising [host_closing] counts as closing, so a defect there
    cannot keep the host's Domain from being joined. *)
let start ~spawn ~host_closing ~deadline_ms ~running_epoch ~abandon =
  let watchdog =
    {
      stop_requested = Atomic.make false;
      mutex = Mutex.create ();
      exited = Condition.create ();
      finished = false;
    }
  in
  let should_stop () =
    Atomic.get watchdog.stop_requested
    || (try host_closing () with _ -> true)
  in
  let body () =
    Fun.protect
      ~finally:(fun () -> mark_finished watchdog)
      (fun () ->
        try run ~should_stop ~deadline_ms ~running_epoch ~abandon with _ -> ())
  in
  match spawn body with
  | Ok () -> Ok watchdog
  | Error _ as error -> error

(** Requests exit and waits for it; the sampling thread observes the flag
    within one {!stop_poll_s} sleep slice, or at once if it already exited
    because its host began closing. [Condition.wait] releases the caller's
    runtime lock. *)
let stop watchdog =
  Atomic.set watchdog.stop_requested true;
  Mutex.lock watchdog.mutex;
  while not watchdog.finished do
    Condition.wait watchdog.exited watchdog.mutex
  done;
  Mutex.unlock watchdog.mutex

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

(** The stop flag is written by [stop] on the owner's Domain and read by the
    watchdog Domain between ticks. *)
type t = { stop_requested : bool Atomic.t; domain : unit Domain.t }

(** Samples until stopped. [Thread.delay] releases the Domain's runtime lock
    while sleeping. Callback exceptions are contained: a diagnostic defect must
    not end detection. *)
let run ~stop_requested ~deadline_ms ~running_epoch ~abandon =
  let tick_ms = tick_ms ~deadline_ms in
  let tick_seconds = Float.of_int tick_ms /. 1_000. in
  let rec loop state =
    if not (Atomic.get stop_requested) then begin
      Thread.delay tick_seconds;
      if not (Atomic.get stop_requested) then begin
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

(** Spawns the dedicated watchdog Domain. A separate Domain, rather than a
    system thread on the workflow lane's Domain, keeps sampling independent of
    whether the stuck code ever reaches a point where that Domain's runtime
    lock can be handed to another thread. *)
let start ~deadline_ms ~running_epoch ~abandon =
  let stop_requested = Atomic.make false in
  match
    Domain.spawn (fun () ->
        run ~stop_requested ~deadline_ms ~running_epoch ~abandon)
  with
  | domain -> Ok { stop_requested; domain }
  | exception exception_ -> Error exception_

(** Requests exit and joins; the Domain observes the flag after its current
    tick. *)
let stop watchdog =
  Atomic.set watchdog.stop_requested true;
  Domain.join watchdog.domain

(** Private idle pacing for the native worker lanes. See the interface for the
    two rules and why neither spins (#994). *)

(** Per-run state shared by both lanes.

    [work_epoch] advances on every {!note_work}. [wait_generation] advances
    when a native wait starts, and [waiting] is [true] while one is in flight.
    The loop's wait token guarantees at most one native wait at a time, so a
    plain flag suffices. *)
type t = {
  first_delay_s : float;
  max_delay_s : float;
  work_epoch : int Atomic.t;
  wait_generation : int Atomic.t;
  waiting : bool Atomic.t;
}

(** One lane's backoff. Mutable fields are touched only by the owning lane's
    thread, so they need no synchronization.

    [seen_epoch] is the work epoch the current backoff sequence started from.
    [joined_generation] is the last native wait this lane joined with a
    zero-delay poll. [early_generation] is the last joined wait during which
    this lane found its joining poll returned early and restarted its
    backoff. [delay_s] is the last delay returned, [0.] right after a
    reset. *)
type lane = {
  shared : t;
  mutable seen_epoch : int;
  mutable joined_generation : int;
  mutable early_generation : int;
  mutable delay_s : float;
}

let default_max_delay_s = 0.01
let default_first_delay_s = 0.0001

let create ?(first_delay_s = default_first_delay_s)
    ?(max_delay_s = default_max_delay_s) () =
  if not (first_delay_s > 0. && first_delay_s <= max_delay_s) then
    invalid_arg "Native_worker_idle.create: need 0 < first_delay_s <= max_delay_s";
  {
    first_delay_s;
    max_delay_s;
    work_epoch = Atomic.make 0;
    wait_generation = Atomic.make 0;
    waiting = Atomic.make false;
  }

(** A new lane starts from the current epoch and generation, so its first
    idle yield is [first_delay_s] rather than an immediate repoll: the poll
    that found it idle happened just before. *)
let lane shared =
  {
    shared;
    seen_epoch = Atomic.get shared.work_epoch;
    joined_generation = Atomic.get shared.wait_generation;
    early_generation = Atomic.get shared.wait_generation;
    delay_s = 0.;
  }

let note_work t = ignore (Atomic.fetch_and_add t.work_epoch 1)

let note_ready lane =
  (* Mark only this bump as seen. Should the sibling bump the epoch again in
     between, the mismatch restarts this lane's backoff too, as it should. *)
  let previous = Atomic.fetch_and_add lane.shared.work_epoch 1 in
  if lane.seen_epoch = previous then lane.seen_epoch <- previous + 1

let native_wait lane f =
  let t = lane.shared in
  (* Publish the generation before the flag, so a sibling that sees
     [waiting] also sees a generation it has not joined yet. *)
  ignore (Atomic.fetch_and_add t.wait_generation 1);
  Atomic.set t.waiting true;
  Fun.protect ~finally:(fun () -> Atomic.set t.waiting false) f

let next_delay lane =
  let shared = lane.shared in
  let generation = Atomic.get shared.wait_generation in
  let in_flight = Atomic.get shared.waiting in
  if in_flight && generation <> lane.joined_generation then begin
    (* The sibling holds the supervisor in a native wait this lane has not
       joined: the next poll queues behind it and returns when either lane
       has work. Joining once per generation bounds the zero-delay polls if
       the poll does not, in fact, queue behind the wait. *)
    lane.joined_generation <- generation;
    0.
  end
  else if in_flight && generation <> lane.early_generation then begin
    (* The joined wait is still in flight, yet the joining poll returned: a
       poll queued behind the wait cannot return before it ends, so this one
       was served before the wait's mailbox admission. Restart the backoff
       short, once per wait, rather than sleep up to the cap while a task for
       this lane may already wake the wait. A poll that never enters the
       supervisor costs only the usual doubling sequence per wait. A queued
       poll that returns just before the waiter clears [waiting] also lands
       here; it only shortens one sleep. *)
    lane.early_generation <- generation;
    lane.delay_s <- shared.first_delay_s;
    shared.first_delay_s
  end
  else
    let epoch = Atomic.get shared.work_epoch in
    if epoch <> lane.seen_epoch then begin
      lane.seen_epoch <- epoch;
      lane.delay_s <- 0.;
      0.
    end
    else begin
      let next =
        if lane.delay_s <= 0. then shared.first_delay_s
        else Float.min shared.max_delay_s (lane.delay_s *. 2.)
      in
      lane.delay_s <- next;
      next
    end

(** The bridge's pacing sleep rather than [Thread.delay], which on Windows
    rounds every backoff step to an immediate return or a whole 15.6 ms timer
    tick (#1008). *)
let local_yield lane =
  let delay = next_delay lane in
  if delay > 0. then Temporal_core_bridge.Native_bridge.pacing_sleep delay

let wait lane ~native_wait:use_native ready =
  if not use_native then begin
    local_yield lane;
    Ok ()
  end
  else
    match native_wait lane ready with
    | Ok true ->
        note_ready lane;
        Ok ()
    | Ok false -> Ok ()
    | Error _ as error -> error

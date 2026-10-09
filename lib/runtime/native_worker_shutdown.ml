(** Bounded shutdown orchestration for the private native worker (#495). The
    interface documents the phases, ownership and the overall bound. *)

type clock = { now : unit -> float; sleep : float -> unit }

let system_clock = { now = Unix.gettimeofday; sleep = Thread.delay }
let lanes_slack_s = 0.25

(** Interval between non-blocking lifecycle-lock attempts and between checks
    of the shutdown thread's progress. Short enough to add little latency to
    a prompt shutdown, long enough not to spin a core. *)
let poll_interval_s = 0.01

(** Pause between attempts to deliver an explicitly retryable retained
    completion, matching the order of the activity lane's own retry backoff. *)
let drain_retry_interval_s = 0.1

type teardown = Completed | Detached

type report = {
  elapsed_s : float;
  lanes_stopped : bool;
  abandoned_activity_callbacks : int;
  abandoned_workflow_activations : int;
  teardown : teardown;
}

type 'error drain =
  | Drained
  | Busy
  | Drain_failed of { error : 'error; retryable : bool }

type 'error release =
  | Released
  | Released_retiring_leases of 'error
  | Release_failed of 'error

type 'error operations = {
  try_acquire_lanes : unit -> bool;
  release_lanes : unit -> unit;
  activity_lane_detached : unit -> bool;
  activity_callback_running : unit -> bool;
  workflow_activation_in_flight : unit -> bool;
  drain_workflow : unit -> 'error drain;
  drain_activity : unit -> 'error drain;
  release : unit -> 'error release;
  exception_error : exn -> 'error;
}

type 'error outcome =
  | Shut_down of report
  | Completion_lost of { error : 'error; report : report }
  | Release_error of { error : 'error; report : report }
  | Release_unproven of report

(** What the lanes and drain phases established, before the release. *)
type lanes = {
  stopped : bool;
  activity_callbacks : int;
  workflow_activations : int;
}

(** How the release, and therefore the whole sequence, ended. *)
type 'error final =
  | Final_ok
  | Final_completion_lost of 'error
  | Final_release_error of 'error
  | Final_unproven

(** Progress shared by the shutdown thread (sole writer) and the caller
    (reader). [mutex] is held only to copy or replace a field, never across
    an operation, so the caller's wait can never block behind native work. *)
type 'error progress = {
  mutex : Mutex.t;
  mutable lanes : lanes option;
      (** Published when the drains end, just before the release. *)
  mutable drain_failure : 'error option;
      (** The first failed drain, published with [lanes]. *)
  mutable release_started_at : float option;
  mutable final : (lanes * 'error final) option;
}

(** Reads or writes [progress] under its mutex. *)
let locked progress f =
  Mutex.lock progress.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock progress.mutex) f

(** A non-blocking probe that may itself be faulty. A defect in a probe must
    not end shutdown, so it reads as [default]. *)
let probe default f = try f () with _ -> default

(** Waits until [try_acquire_lanes] succeeds or [give_up] passes. *)
let acquire_lanes ~clock ~give_up operations =
  let rec loop () =
    if probe false operations.try_acquire_lanes then true
    else if clock.now () >= give_up then false
    else begin
      clock.sleep poll_interval_s;
      loop ()
    end
  in
  loop ()

(** Drains one adapter. An explicitly retryable failure is retried, with the
    exact retained completion, until [give_up]; at least one attempt is
    always made so a worker shut down after its deadline still delivers what
    it can. A raised drain is a permanent defect. *)
let drain_until ~clock ~give_up operations drain =
  let rec loop () =
    let result =
      try drain ()
      with exception_ ->
        Drain_failed
          { error = operations.exception_error exception_; retryable = false }
    in
    match result with
    | Drain_failed { retryable = true; _ } when clock.now () < give_up ->
        clock.sleep drain_retry_interval_s;
        loop ()
    | Drained | Busy | Drain_failed _ -> result
  in
  loop ()

(** The first failed drain, workflow before activity, matching drain order. *)
let first_drain_failure workflow activity =
  match (workflow, activity) with
  | Drain_failed { error; _ }, _ | _, Drain_failed { error; _ } -> Some error
  | (Drained | Busy), (Drained | Busy) -> None

(** The complete sequence, run by the shutdown thread (or inline when none
    can be created). The lifecycle lock, when acquired, is released on this
    same thread before the native release begins: the release never needs
    it, and a stuck caller of [run] cannot be waiting for it. *)
let sequence ~clock ~lanes_deadline operations progress =
  let give_up = lanes_deadline +. lanes_slack_s in
  let acquired = acquire_lanes ~clock ~give_up operations in
  let drains () =
    let workflow =
      drain_until ~clock ~give_up operations operations.drain_workflow
    in
    let activity =
      drain_until ~clock ~give_up operations operations.drain_activity
    in
    (workflow, activity)
  in
  let workflow, activity =
    if acquired then
      Fun.protect ~finally:(fun () -> probe () operations.release_lanes) drains
    else drains ()
  in
  (* A busy adapter is held by abandoned code; the probes say whether that
     code is user code or a native call made on its behalf. *)
  let activity_callbacks =
    if activity = Busy && probe false operations.activity_callback_running
    then 1
    else 0
  in
  let workflow_activations =
    if workflow = Busy && probe false operations.workflow_activation_in_flight
    then 1
    else 0
  in
  let detached_and_running =
    probe false operations.activity_lane_detached
    && probe false operations.activity_callback_running
  in
  let lanes =
    {
      stopped =
        acquired && (not detached_and_running) && workflow <> Busy
        && activity <> Busy;
      activity_callbacks;
      workflow_activations;
    }
  in
  let drain_failure = first_drain_failure workflow activity in
  locked progress (fun () ->
      progress.lanes <- Some lanes;
      progress.drain_failure <- drain_failure;
      progress.release_started_at <- Some (clock.now ()));
  let final =
    match operations.release () with
    | exception _ -> Final_unproven
    | release -> (
        match drain_failure with
        | Some error -> Final_completion_lost error
        | None -> (
            match release with
            | Released -> Final_ok
            (* Leases the bridge retired belong to the abandoned lanes. *)
            | Released_retiring_leases _ when not lanes.stopped -> Final_ok
            | Released_retiring_leases error | Release_failed error ->
                Final_release_error error))
  in
  locked progress (fun () -> progress.final <- Some (lanes, final))

(** Builds the caller's outcome from what the shutdown thread published. *)
let outcome_of ~elapsed_s ~teardown lanes final =
  let report =
    {
      elapsed_s;
      lanes_stopped = lanes.stopped;
      abandoned_activity_callbacks = lanes.activity_callbacks;
      abandoned_workflow_activations = lanes.workflow_activations;
      teardown;
    }
  in
  match final with
  | Final_ok -> Shut_down report
  | Final_completion_lost error -> Completion_lost { error; report }
  | Final_release_error error -> Release_error { error; report }
  | Final_unproven -> Release_unproven report

let run ?(clock = system_clock) ~lanes_deadline ~teardown_timeout_s operations =
  let started = clock.now () in
  let hard_deadline =
    Float.max started lanes_deadline +. lanes_slack_s +. teardown_timeout_s
  in
  let progress =
    {
      mutex = Mutex.create ();
      lanes = None;
      drain_failure = None;
      release_started_at = None;
      final = None;
    }
  in
  let job () =
    try sequence ~clock ~lanes_deadline operations progress
    with _ ->
      (* Only a defect outside every guarded operation reaches here. The
         release outcome is then unknown, which is reported as such. *)
      locked progress (fun () ->
          let lanes =
            Option.value progress.lanes
              ~default:
                { stopped = false; activity_callbacks = 0; workflow_activations = 0 }
          in
          progress.final <- Some (lanes, Final_unproven))
  in
  let elapsed () = Float.max 0. (clock.now () -. started) in
  let threaded = match Thread.create job () with _ -> true | exception _ -> false in
  if not threaded then job ();
  (* Wait for the published outcome, or give up at the release deadline. *)
  let rec wait () =
    let final, lanes, drain_failure, release_started_at =
      locked progress (fun () ->
          ( progress.final,
            progress.lanes,
            progress.drain_failure,
            progress.release_started_at ))
    in
    match final with
    | Some (lanes, final) ->
        outcome_of ~elapsed_s:(elapsed ()) ~teardown:Completed lanes final
    | None ->
        let now = clock.now () in
        let deadline =
          match release_started_at with
          | Some at -> Float.min hard_deadline (at +. teardown_timeout_s)
          | None -> hard_deadline
        in
        if now < deadline then begin
          clock.sleep poll_interval_s;
          wait ()
        end
        else
          (* The shutdown thread keeps ownership of the release. If it has not
             even published the lanes outcome, a drain is blocked in a native
             call; report what the non-blocking probes show now. *)
          let lanes =
            match lanes with
            | Some lanes -> lanes
            | None ->
                {
                  stopped = false;
                  activity_callbacks =
                    (if probe false operations.activity_callback_running then 1
                     else 0);
                  workflow_activations =
                    (if probe false operations.workflow_activation_in_flight
                     then 1
                     else 0);
                }
          in
          let final =
            match drain_failure with
            | Some error -> Final_completion_lost error
            | None -> Final_ok
          in
          outcome_of ~elapsed_s:(elapsed ()) ~teardown:Detached lanes final
  in
  wait ()

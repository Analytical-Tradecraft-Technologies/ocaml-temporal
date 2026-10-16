(** Activities and workflows that show cooperative activity cancellation and
    cleanup on worker shutdown (#494).

    Each activity holds a reservation in an external system while it works,
    and must not leave it behind when it stops early. The worker never
    interrupts activity code: an activity learns that it should stop only
    when it asks, through a heartbeat result,
    [Temporal.Activity.Context.cancellation], or
    [Temporal.Activity.Context.is_worker_shutting_down]. Every external
    operation is idempotent and keyed by an identifier that is stable across
    retries, because Temporal delivers an activity at least once: a retry, or
    a redelivery after a worker crash, can repeat any step. *)

(** A stand-in for an external reservation service, such as a database table
    of seat holds or a lock service. A real application would call that
    service from the activity; this one keeps the state in memory so the
    example runs without another dependency and the driver in the same
    process can check what the activities did.

    Every operation is idempotent for a given key. A repeated [acquire] keeps
    the existing hold, [release] of a key that is not held does nothing, and
    neither undoes a commit, so an activity attempt that is retried or
    redelivered can safely repeat any of them. Operations are serialized by
    one mutex, because activity callbacks, the asynchronous completer thread
    and the driver run on different Domains and threads. *)
module Ledger = struct
  (** Why an activity attempt gave its reservation back. *)
  type release_reason =
    | Cancellation_requested
        (** The workflow, or a client, requested cancellation. *)
    | Worker_stopping
        (** The worker running the attempt began to shut down. *)
    | Operator_interrupted  (** An operator paused or reset the activity. *)
    | Attempt_lost
        (** Temporal timed the attempt out or no longer knows it, or a
            heartbeat or the asynchronous handle failed for another reason. *)

  (** One recorded change to a reservation. [attempt] is the activity attempt
      that made it, starting at 1. *)
  type event =
    | Acquired of { attempt : int }
    | Released of { attempt : int; reason : release_reason }
    | Committed of { attempt : int }

  (** The current state of one reservation. *)
  type state = Held | Released_state | Committed_state

  (** Per-key state and its history, newest event first. *)
  type entry = { mutable state : state; mutable history : event list }

  (** All reservations of this process, keyed by reservation key. *)
  let entries : (string, entry) Hashtbl.t = Hashtbl.create 8

  (** Serializes every access to [entries]. *)
  let mutex = Mutex.create ()

  (** Runs [f] on the entry for [key], creating an empty released entry for
      a key that was never seen, while holding the ledger lock. *)
  let with_entry key f =
    Mutex.protect mutex (fun () ->
        let entry =
          match Hashtbl.find_opt entries key with
          | Some entry -> entry
          | None ->
              let entry = { state = Released_state; history = [] } in
              Hashtbl.replace entries key entry;
              entry
        in
        f entry)

  (** Places a hold for [key]. Returns [`Already_committed] when an earlier
      attempt finished the work, so a redelivered attempt reports success
      instead of doing the work twice; otherwise the key is (or stays) held
      and [`Acquired] is returned. *)
  let acquire key ~attempt =
    with_entry key (fun entry ->
        match entry.state with
        | Committed_state -> `Already_committed
        | Held -> `Acquired
        | Released_state ->
            entry.state <- Held;
            entry.history <- Acquired { attempt } :: entry.history;
            `Acquired)

  (** Gives back the hold for [key]. Releasing a key that is not held, or
      that was already committed, changes nothing, so cleanup code can call
      it on every exit path, and more than once. *)
  let release key ~attempt reason =
    with_entry key (fun entry ->
        match entry.state with
        | Held ->
            entry.state <- Released_state;
            entry.history <- Released { attempt; reason } :: entry.history
        | Released_state | Committed_state -> ())

  (** Makes the held reservation for [key] permanent. Committing an already
      committed key changes nothing. A key that is not held is left alone and
      [false] is returned, because its hold was given back and the work must
      not be committed without one. *)
  let commit key ~attempt =
    with_entry key (fun entry ->
        match entry.state with
        | Held ->
            entry.state <- Committed_state;
            entry.history <- Committed { attempt } :: entry.history;
            true
        | Committed_state -> true
        | Released_state -> false)

  (** Returns the current state of [key], or [None] if it was never used. *)
  let state key =
    Mutex.protect mutex (fun () ->
        Option.map (fun entry -> entry.state) (Hashtbl.find_opt entries key))

  (** Returns the history of [key], oldest event first. *)
  let history key =
    Mutex.protect mutex (fun () ->
        match Hashtbl.find_opt entries key with
        | None -> []
        | Some entry -> List.rev entry.history)
end

(** The activity ID both workflows pass explicitly. A stable activity ID,
    together with the workflow ID, gives every attempt of the same scheduled
    activity the same reservation key. *)
let reservation_activity_id = "reservation"

(** Returns the reservation key for one scheduled activity: its workflow ID
    and activity ID, which stay the same across retries. Attempt-specific
    data, such as [Info.attempt], is deliberately left out, so a retry finds
    the hold of the attempt it replaces. *)
let reservation_key info =
  let workflow = Temporal.Activity.Info.workflow info in
  workflow.workflow_id ^ "/" ^ Temporal.Activity.Info.activity_id info

(** Seconds of simulated work per step. Each step is followed by a heartbeat,
    which is also how cancellation reaches the activity, so this bounds how
    long an activity takes to notice it. *)
let step_seconds = 0.1

(** The retryable failure an activity returns when its worker stops before
    the work is committed. Temporal then schedules another attempt, normally
    on a different worker, which reacquires the same reservation key. *)
let worker_stopping_error () =
  Temporal.Error.make ~category:`Activity ~error_type:"WorkerShutdown"
    ~message:"the worker stopped before the reservation was committed" ()

(** Classifies why a synchronous attempt stopped before finishing. A
    delivered cancellation wins over the shutdown flag, because Temporal
    reports a cancellation with its own reason, including
    [Worker_shutdown]. *)
let stop_reason context =
  match Temporal.Activity.Context.cancellation context with
  | Some cancellation -> (
      match Temporal.Activity.Cancellation.reason cancellation with
      | Requested -> Ledger.Cancellation_requested
      | Worker_shutdown -> Worker_stopping
      | Paused | Reset -> Operator_interrupted
      | Timed_out | Not_found -> Attempt_lost)
  | None ->
      if Temporal.Activity.Context.is_worker_shutting_down context then
        Worker_stopping
      else Attempt_lost

(** Returns the step to resume from: the step that the previous attempt last
    reported in a heartbeat, or [0] on a first attempt. That step may not
    have finished, so it is repeated; steps must be idempotent for the same
    reason as the ledger operations. *)
let resume_step context =
  match Temporal.Activity.Context.details context with
  | payload :: _ -> (
      match Temporal.Codec.decode Temporal.Codec.int payload with
      | Ok step -> step
      | Error _ -> 0)
  | [] -> 0

(** Runs steps [first] to [steps - 1], heartbeating before each one. It stops
    with the heartbeat's error as soon as a heartbeat reports a cancellation,
    and with {!worker_stopping_error} as soon as the worker begins to shut
    down. Checking the flag before each step lets the attempt release its
    reservation well within the worker's graceful shutdown period. *)
let run_steps context ~first ~steps =
  let rec step index =
    if index >= steps then Ok ()
    else if Temporal.Activity.Context.is_worker_shutting_down context then
      Error (worker_stopping_error ())
    else
      match Temporal.Activity.Context.heartbeat context Temporal.Codec.int index with
      | Error error -> Error error
      | Ok () ->
          Unix.sleepf step_seconds;
          step (index + 1)
  in
  step first

(** The message an activity returns once its reservation is committed. *)
let committed_message key = "reservation " ^ key ^ " committed"

(** A synchronous activity that holds a reservation while it works through
    [steps] steps, then commits it.

    If it stops early, it releases the reservation before returning, on
    every exit path, and then reports why it stopped:
    - for a requested cancellation it returns the heartbeat's [`Cancelled]
      error, so Temporal records the attempt as cancelled;
    - when its worker is stopping it returns a retryable error, so Temporal
      runs another attempt, which resumes from the last heartbeat details;
    - for a pause or reset it also returns the heartbeat's error, which the
      worker reports as a retryable ["ActivityPause"] or ["ActivityReset"]
      failure.

    The release happens before the result is returned, so a workflow that
    waits for the cancellation to complete knows the cleanup has run. A
    redelivered attempt after a commit finds the reservation committed and
    returns at once, without repeating the work. *)
let hold_reservation =
  Temporal.Activity.define_with_context
    ~name:"ocaml-temporal-example.hold-reservation" ~input:Temporal.Codec.int
    ~output:Temporal.Codec.string (fun context steps ->
      let open Temporal.Result_syntax in
      let* info = Temporal.Activity.Context.info context in
      let key = reservation_key info in
      let attempt = Temporal.Activity.Info.attempt info in
      match Ledger.acquire key ~attempt with
      | `Already_committed -> Ok (committed_message key)
      | `Acquired -> (
          match run_steps context ~first:(resume_step context) ~steps with
          | Ok () ->
              if Ledger.commit key ~attempt then Ok (committed_message key)
              else
                Error
                  (Temporal.Error.make ~category:`Activity
                     ~message:"the reservation was released before commit" ())
          | Error error ->
              let reason = stop_reason context in
              Ledger.release key ~attempt reason;
              Error
                (match reason with
                | Worker_stopping -> worker_stopping_error ()
                | Cancellation_requested | Operator_interrupted | Attempt_lost ->
                    error)))

(** Calls [submit] until it returns [Ok] or a non-retryable error, at most
    [attempts] times. A retryable error means the worker has not accepted
    the asynchronous handoff yet, or the RPC outcome was uncertain, and the
    identical call must be repeated. *)
let rec deliver ?(attempts = 100) submit =
  match submit () with
  | Error error
    when attempts > 1 && not (Temporal.Error.view error).non_retryable ->
      Thread.delay 0.05;
      deliver ~attempts:(attempts - 1) submit
  | result -> result

(** Encodes one step number as heartbeat details. *)
let step_details index =
  match Temporal.Codec.encode Temporal.Codec.int index with
  | Ok payload -> [ payload ]
  | Error _ -> []

(** Works through [steps] steps for an asynchronous attempt on the calling
    thread, heartbeating through [handle], and finishes the handle exactly
    once.

    An asynchronous activity has no context to poll; its cancellation signal
    is the result of {!Temporal.Activity.Async_handle.heartbeat}:
    - a non-retryable [`Cancelled] error is a requested cancellation: release
      the reservation, then acknowledge with [Async_handle.cancel];
    - a retryable [`Cancelled] error is a pause or reset: release the
      reservation, then fail the handle with that error, because Temporal
      accepts a cancelled completion only after a cancellation request;
    - another retryable error means the handoff is not accepted yet or a
      send failed transiently, so the same step is retried;
    - another non-retryable error means the handle is closed (for example
      the activity no longer exists), so the reservation is released and
      nothing more can be reported. *)
let drive_async handle ~key ~attempt ~steps =
  let module Handle = Temporal.Activity.Async_handle in
  let rec step index =
    if index >= steps then
      if Ledger.commit key ~attempt then
        deliver (fun () -> Handle.complete handle (committed_message key))
      else
        deliver (fun () ->
            Handle.fail handle
              (Temporal.Error.make ~category:`Activity
                 ~message:"the reservation was released before commit" ()))
    else
      match Handle.heartbeat handle (step_details index) with
      | Ok () ->
          Thread.delay step_seconds;
          step (index + 1)
      | Error error -> (
          let view = Temporal.Error.view error in
          match (view.category, view.non_retryable) with
          | `Cancelled, true ->
              Ledger.release key ~attempt Cancellation_requested;
              deliver (fun () -> Handle.cancel handle [])
          | `Cancelled, false ->
              Ledger.release key ~attempt Operator_interrupted;
              deliver (fun () -> Handle.fail handle error)
          | _, false ->
              Thread.delay step_seconds;
              step index
          | _, true ->
              Ledger.release key ~attempt Attempt_lost;
              Error error)
  in
  match step 0 with
  | Ok () -> ()
  | Error error ->
      Printf.eprintf "asynchronous reservation %s could not finish: %s\n%!" key
        (Temporal.Error.message error)
  | exception exception_ ->
      Ledger.release key ~attempt Attempt_lost;
      Printf.eprintf "asynchronous reservation %s raised: %s\n%!" key
        (Printexc.to_string exception_)

(** The asynchronous form of {!hold_reservation}. The callback acquires the
    reservation and hands the attempt to a system thread, which does the
    work and finishes the handle; see {!drive_async}. A real application
    would hand the work to an existing pool or event loop rather than start
    a thread per attempt. *)
let hold_reservation_async =
  Temporal.Activity.define_async
    ~name:"ocaml-temporal-example.hold-reservation-async"
    ~input:Temporal.Codec.int ~output:Temporal.Codec.string
    (fun context steps ->
      match Temporal.Activity.Async_context.info context with
      | Error error -> Temporal.Activity.Failed error
      | Ok info -> (
          let key = reservation_key info in
          let attempt = Temporal.Activity.Info.attempt info in
          match Ledger.acquire key ~attempt with
          | `Already_committed -> Completed (committed_message key)
          | `Acquired ->
              let handle = Temporal.Activity.Async_context.handle context in
              ignore
                (Thread.create
                   (fun () -> drive_async handle ~key ~attempt ~steps)
                   ());
              Will_complete_async handle))

(** Steps a cancelled activity is given: about a minute of work, far longer
    than the workflow waits before cancelling it. *)
let long_steps = 600

(** Builds a retry policy, which can only fail for invalid constants. *)
let retry_policy ~maximum_attempts =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 1_000L) ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 1_000L) ~maximum_attempts ()

(** The terminal workflow failure for an unknown [mode]. *)
let invalid_mode mode =
  Temporal.Error.make ~category:`Workflow ~non_retryable:true
    ~message:("unknown reservation mode: " ^ mode) ()

(** Starts a long reservation activity ([mode] is ["sync"] or ["async"]),
    lets it work for three seconds, cancels it, and returns ["cancelled"]
    once Temporal reports the cancellation as complete.

    [Wait_cancellation_completed] makes the activity future resolve only
    after the activity acknowledges the cancellation, which it does after
    its cleanup, so the workflow could safely start compensating work here.
    The two-second heartbeat timeout bounds how long Temporal takes to tell
    the activity. Any other outcome is returned as text, which the driver
    reports as a failure. *)
let reserve_then_cancel mode =
  let open Temporal.Result_syntax in
  let* activity =
    match mode with
    | "sync" -> Ok hold_reservation
    | "async" -> Ok hold_reservation_async
    | _ -> Error (invalid_mode mode)
  in
  let* retry_policy = retry_policy ~maximum_attempts:1 in
  let handle =
    Temporal.Activity.start_handle ~activity_id:reservation_activity_id
      ~heartbeat_timeout:(Temporal.Duration.of_ms 2_000L)
      ~start_to_close_timeout:(Temporal.Duration.of_ms 90_000L) ~retry_policy
      ~cancellation_type:Temporal.Activity.Wait_cancellation_completed activity
      long_steps
  in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 3_000L) in
  let* () = Temporal.Activity.cancel handle in
  match Temporal.Future.await (Temporal.Activity.future handle) with
  | Error error when (Temporal.Error.view error).category = `Cancelled ->
      Ok "cancelled"
  | Error error -> Ok ("failed: " ^ Temporal.Error.message error)
  | Ok output -> Ok ("completed: " ^ output)

(** The workflow that cancels a running reservation activity. *)
let reserve_then_cancel_workflow =
  Temporal.Workflow.define ~name:"ocaml-temporal-example.reserve-then-cancel"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string
    reserve_then_cancel

(** Runs a short reservation activity of [steps] steps to completion,
    retrying it after a one-second delay, and returns its result. A worker
    shutdown during the first attempt makes that attempt release its hold and
    fail retryably; the retry, on another worker, reacquires the same key
    and commits it. *)
let reserve_and_commit steps =
  let open Temporal.Result_syntax in
  let* retry_policy = retry_policy ~maximum_attempts:5 in
  Temporal.Activity.execute ~activity_id:reservation_activity_id
    ~heartbeat_timeout:(Temporal.Duration.of_ms 2_000L)
    ~start_to_close_timeout:(Temporal.Duration.of_ms 90_000L) ~retry_policy
    hold_reservation steps

(** The workflow that commits a reservation across a worker replacement. *)
let reserve_and_commit_workflow =
  Temporal.Workflow.define ~name:"ocaml-temporal-example.reserve-and-commit"
    ~input:Temporal.Codec.int ~output:Temporal.Codec.string reserve_and_commit

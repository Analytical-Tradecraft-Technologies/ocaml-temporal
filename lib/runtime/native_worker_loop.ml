(** The private native worker lane scheduler.

    Workflow activations stay on the calling Domain. One dedicated activity
    Domain polls and executes activity callbacks, so a slow activity cannot
    prevent an unrelated workflow activation from reaching Core. A worker that
    polls no activity tasks spawns no activity Domain (#1042). Both lanes
    still enter the same owner-Domain supervisor mailbox for native operations.
    The activity lane admits only one task at a time; a retained completion is
    retried after a bounded backoff before another activity is polled. *)

type progress =
  | Progress
  | Not_ready
  | Retry_pending

(** The first fatal lane outcome is published across Domains without changing
    the worker's shutdown flag. A later explicit shutdown must still own native
    teardown even if [run] returned an error. *)
type 'error lane_failure = Source_error of 'error | Raised of exn

(** Runs one lane until shutdown or a sibling-lane failure. A lane is busy from
    the start of its poll through callback/completion and any retained-completion
    backoff. Only a [Not_ready] result makes it idle. The shared wait token lets
    at most one idle lane occupy the native supervisor with a readiness wait;
    the other lane yields locally and keeps checking for its own work. The
    native wait must observe both lanes (see [run] in the interface), so the
    token bounds supervisor occupancy without deciding which lane is woken. *)
let run_lane ~stopped ~poll ~wait ~retry_pending ~busy ~sibling_busy
    ~has_sibling ~wait_token ~prefer_workflow ~wait_epoch ~workflow_lane =
  let deferred_epoch = ref (-1) in
  let rec loop () =
    if stopped () then Ok ()
    else begin
      Atomic.set busy true;
      match poll () with
      | Error _ when stopped () -> Ok ()
      | Error error -> Error error
      | Ok progress ->
          if stopped () then Ok ()
          else
            let wait_result =
              match progress with
              | Progress -> Ok ()
              | Not_ready ->
                  Atomic.set busy false;
                  let epoch = Atomic.get wait_epoch in
                  let sibling_is_busy = Atomic.get sibling_busy in
                  let preferred = Atomic.get prefer_workflow = workflow_lane in
                  let native_wait =
                    (not sibling_is_busy)
                    && (preferred || !deferred_epoch = epoch)
                    && Atomic.get wait_epoch = epoch
                    && not (Atomic.get sibling_busy)
                    && Atomic.compare_and_set wait_token false true
                  in
                  if native_wait then
                    Fun.protect
                      ~finally:(fun () ->
                        (* A lone lane keeps the preference, so it never
                           pays a local yield before its next native wait. *)
                        if has_sibling then
                          Atomic.set prefer_workflow (not workflow_lane);
                        ignore (Atomic.fetch_and_add wait_epoch 1);
                        Atomic.set wait_token false)
                      (fun () -> wait ~native_wait:true)
                  else begin
                    (* Preference is advisory: if the favored lane keeps
                       seeing its sibling busy, this lane may claim the next
                       free token after one local yield in the same epoch. *)
                    if not sibling_is_busy && not preferred then
                      deferred_epoch := epoch;
                    wait ~native_wait:false
                  end
              | Retry_pending -> retry_pending ()
            in
            match wait_result with
            | Error _ when stopped () -> Ok ()
            | Error error -> Error error
            | Ok () -> loop ()
    end
  in
  loop ()

type activity_detach = {
  now : unit -> float;
  deadline : unit -> float;
  on_detached : unit -> unit;
  on_stop : unit -> unit;
  callback_running : unit -> bool;
}

(** Interval at which a bounded join checks whether the activity lane has
    finished. [Thread.delay] releases the runtime lock meanwhile. *)
let join_poll_interval_s = 0.01

let idle_detach_allowance_s = 0.15

(** Whether the bounded join should stop waiting for the activity lane at
    [now]. A lane whose callback is still running past [deadline] is detached
    at once. Any other lane observes the stop by itself within one bounded
    wait, so it is given {!idle_detach_allowance_s} more: detaching it would
    log a spurious [activity_lane_detached] and report abandoned work after a
    short grace period (#1023). The allowance still bounds the join if that
    lane is itself stuck behind the supervisor.

    The lane's scheduling [busy] flag is deliberately not used: it is also
    set for each poll, including one that finds no task, so sampling it at
    the deadline detached an idle lane whenever the sample fell inside a
    poll (CI on #1081). *)
let detach_due ~now ~deadline ~callback_running =
  now >= deadline
  && (callback_running || now >= deadline +. idle_detach_allowance_s)

(** Runs the workflow and capacity-one activity lanes concurrently. The caller
    joins the activity Domain on every ordinary result before returning; its
    owner can therefore hold the worker lifecycle mutex until neither lane can
    use an adapter or the native supervisor. With [detach], an activity
    callback that outlives the deadline is left running on its Domain instead
    of blocking this join forever (#495); the [finished] flag is set by that
    Domain as its last action, so a [true] read means the join is immediate.
    [detach.on_stop] is started once, after the workflow lane returned
    because [closed] holds and before the activity lane is joined, so the
    caller can stop native polling while a callback still finishes (#1028).
    It runs on a detached system thread and is never awaited. *)
let run ~activity_lane ~lane_unavailable ~detach ~closed ~poll_workflow
    ~poll_activity ~wait_for_lane ~retry_pending =
  let stop = Atomic.make false in
  let first_failure = Atomic.make None in
  let workflow_busy = Atomic.make false in
  let activity_busy = Atomic.make false in
  let activity_finished = Atomic.make false in
  let wait_token = Atomic.make false in
  let prefer_workflow = Atomic.make true in
  let wait_epoch = Atomic.make 0 in
  let stopped () = Atomic.get stop || closed () in
  let publish failure =
    ignore (Atomic.compare_and_set first_failure None (Some failure));
    Atomic.set stop true
  in
  let guarded ~poll ~workflow_lane ~busy ~sibling_busy =
    try
      match
        run_lane ~stopped ~poll ~busy ~sibling_busy ~has_sibling:activity_lane
          ~wait_token
          ~prefer_workflow ~wait_epoch ~workflow_lane
          ~wait:(fun ~native_wait -> wait_for_lane ~workflow_lane ~native_wait)
          ~retry_pending:(fun () -> retry_pending ~workflow_lane)
      with
      | Ok () -> ()
      | Error error -> publish (Source_error error)
    with exception_ -> publish (Raised exception_)
  in
  let spawned =
    if not activity_lane then begin
      (* No activity lane: the join below is immediate and skipped. *)
      Atomic.set activity_finished true;
      Ok None
    end
    else
      match
        Domain.spawn (fun () ->
            Fun.protect
              ~finally:(fun () -> Atomic.set activity_finished true)
              (fun () ->
                guarded ~poll:poll_activity ~workflow_lane:false
                  ~busy:activity_busy ~sibling_busy:workflow_busy))
      with
      | domain -> Ok (Some domain)
      | exception exception_ -> Error (lane_unavailable exception_)
  in
  match spawned with
  | Error _ as error ->
      (* Nothing ran, so there is no lane to stop and no polling to end:
         the worker may be run again once a Domain is free (#1042). *)
      error
  | Ok activity_domain ->
        guarded ~poll:poll_workflow ~workflow_lane:true ~busy:workflow_busy
          ~sibling_busy:activity_busy;
        let joined =
          match detach with
          | None -> true
          | Some { now; deadline; on_detached; on_stop; callback_running } ->
              (* Only a genuine stop, never a lane failure, ends native polling:
                 after a failure without a stop the worker may still be run again.
                 The callback runs fire-and-forget on its own system thread: it may
                 wait for a supervisor mailbox occupied by the very activity-lane
                 native call this join must be able to abandon, so awaiting it here
                 could hold [run] (and the caller's lifecycle lock) past the
                 deadline. A defect in it, or a failed thread creation, must not
                 prevent the join below; the caller's shutdown path stops polling
                 in any case. *)
              if closed () then begin
                match
                  Thread.create (fun () -> try on_stop () with _ -> ()) ()
                with
                | _thread -> ()
                | exception _ -> ()
              end;
              if not (Atomic.get activity_finished) then begin
                let deadline = deadline () in
                while
                  (not (Atomic.get activity_finished))
                  && not
                       (detach_due ~now:(now ()) ~deadline
                          ~callback_running:
                            (try callback_running () with _ -> false))
                do
                  Thread.delay join_poll_interval_s
                done
              end;
              if Atomic.get activity_finished then true
              else begin
                (* The callback holds the activity lane past its deadline. The
                   Domain is not joined; it ends by itself after the callback,
                   because [stop] or [closed] is already set. *)
                on_detached ();
                false
              end
        in
        if joined then Option.iter Domain.join activity_domain;
        match Atomic.get first_failure with
        | None -> Ok ()
        | Some (Source_error error) -> Error error
        | Some (Raised exception_) -> raise exception_

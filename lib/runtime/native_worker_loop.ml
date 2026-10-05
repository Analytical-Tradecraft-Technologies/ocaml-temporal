(** The private native worker lane scheduler.

    Workflow activations stay on the calling Domain. One dedicated activity
    Domain polls and executes activity callbacks, so a slow activity cannot
    prevent an unrelated workflow activation from reaching Core. Both lanes
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
    ~wait_token ~prefer_workflow ~wait_epoch ~workflow_lane =
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

(** Runs the workflow and capacity-one activity lanes concurrently. The caller
    joins the activity Domain on every ordinary result before returning; its
    owner can therefore hold the worker lifecycle mutex until neither lane can
    use an adapter or the native supervisor. An activity callback that never
    returns also prevents this join and remains a bounded-shutdown limitation. *)
let run ~closed ~poll_workflow ~poll_activity ~wait_for_lane ~retry_pending =
  let stop = Atomic.make false in
  let first_failure = Atomic.make None in
  let workflow_busy = Atomic.make false in
  let activity_busy = Atomic.make false in
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
        run_lane ~stopped ~poll ~busy ~sibling_busy ~wait_token
          ~prefer_workflow ~wait_epoch ~workflow_lane
          ~wait:(fun ~native_wait -> wait_for_lane ~workflow_lane ~native_wait)
          ~retry_pending:(fun () -> retry_pending ~workflow_lane)
      with
      | Ok () -> ()
      | Error error -> publish (Source_error error)
    with exception_ -> publish (Raised exception_)
  in
  let activity_domain =
    Domain.spawn (fun () ->
      guarded ~poll:poll_activity ~workflow_lane:false ~busy:activity_busy
        ~sibling_busy:workflow_busy)
  in
  guarded ~poll:poll_workflow ~workflow_lane:true ~busy:workflow_busy
    ~sibling_busy:activity_busy;
  Domain.join activity_domain;
  match Atomic.get first_failure with
  | None -> Ok ()
  | Some (Source_error error) -> Error error
  | Some (Raised exception_) -> raise exception_

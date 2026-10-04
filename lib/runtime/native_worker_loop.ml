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

(** Runs one lane until shutdown or a sibling-lane failure. Polling and callback
    execution remain sequential within a lane. Every wait is bounded by its
    native implementation, so the sibling can observe a published stop. *)
let run_lane ~stopped ~poll ~wait ~retry_pending =
  let rec loop () =
    if stopped () then Ok ()
    else
      match poll () with
      | Error _ when stopped () -> Ok ()
      | Error error -> Error error
      | Ok progress ->
          if stopped () then Ok ()
          else
            let wait_result =
              match progress with
              | Progress -> Ok ()
              | Not_ready -> wait ()
              | Retry_pending -> retry_pending ()
            in
            match wait_result with
            | Error _ when stopped () -> Ok ()
            | Error error -> Error error
            | Ok () -> loop ()
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
  let stopped () = Atomic.get stop || closed () in
  let publish failure =
    ignore (Atomic.compare_and_set first_failure None (Some failure));
    Atomic.set stop true
  in
  let guarded ~poll ~workflow_lane =
    try
      match
        run_lane ~stopped ~poll
          ~wait:(fun () -> wait_for_lane ~workflow_lane)
          ~retry_pending:(fun () -> retry_pending ~workflow_lane)
      with
      | Ok () -> ()
      | Error error -> publish (Source_error error)
    with exception_ -> publish (Raised exception_)
  in
  let activity_domain =
    Domain.spawn (fun () -> guarded ~poll:poll_activity ~workflow_lane:false)
  in
  guarded ~poll:poll_workflow ~workflow_lane:true;
  Domain.join activity_domain;
  match Atomic.get first_failure with
  | None -> Ok ()
  | Some (Source_error error) -> Error error
  | Some (Raised exception_) -> raise exception_

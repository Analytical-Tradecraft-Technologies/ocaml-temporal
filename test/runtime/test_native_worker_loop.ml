(** Regression tests for the private native worker lane scheduler.

    These tests use callbacks instead of a native worker so they can make the
    liveness boundary deterministic: one activity completion rejection is
    followed by a dedicated bounded retry backoff, then the exact same completion is
    accepted and the loop can observe a subsequent task. A permanent protocol
    error is kept fatal, proving that retry support cannot hide malformed or
    unsafe boundary data. *)

module Loop = Temporal_runtime.Native_worker_loop

(** A small diagnostic type is enough for the scheduler because it does not
    inspect error contents. The [retryable] bit is produced by the adapter's
    explicit source classification, never inferred from message text. *)
type source_error = { code : string; retryable : bool }

(** A fake source records lane calls and exposes a single transient completion
    rejection. [closed] is set only after the retry succeeds so the test proves
    that the loop actually performed the retry before stopping. *)
type fake_source = {
  mutable activity_polls : int;
  mutable activity_rejections : int;
  mutable waits : bool list;
  mutable retry_waits : bool list;
  mutable closed : bool;
}

(** Creates the deterministic source used by the success-after-retry test. *)
let fake_source () =
  {
    activity_polls = 0;
    activity_rejections = 1;
    waits = [];
    retry_waits = [];
    closed = false;
  }

(** The workflow lane has no work in this focused test. *)
let idle_workflow () = Ok Loop.Not_ready

(** The first activity poll reports a retained completion rejection. The second
    poll accepts that exact completion, and the third poll represents a
    subsequent activity task. Only after that subsequent task does the fixture
    close, proving that retry does not strand the worker loop. *)
let activity_after_one_rejection source () =
  source.activity_polls <- source.activity_polls + 1;
  if source.activity_rejections > 0 then begin
    source.activity_rejections <- source.activity_rejections - 1;
    (* The adapter has already classified and retained the exact completion;
       the scheduler receives a scheduling outcome rather than a fatal error. *)
    Ok Loop.Retry_pending
  end
  else if source.activity_polls = 2 then
    Ok Loop.Progress
  else begin
    source.closed <- true;
    Ok Loop.Progress
  end

(** Records the requested lane without sleeping. Production supplies a native
    bounded readiness wait; the fake only proves that the retry selects the
    activity lane rather than spinning or waiting on workflow readiness. *)
let record_wait source ~workflow_lane =
  source.waits <- workflow_lane :: source.waits;
  Ok ()

(** Records the dedicated backoff callback separately from ordinary readiness.
    Production supplies a native timer for this callback; keeping the two
    traces distinct proves a retained completion does not accidentally use an
    idle-lane wait or spin through the polling loop. *)
let record_retry_wait source ~workflow_lane =
  source.retry_waits <- workflow_lane :: source.retry_waits;
  Ok ()

(** Proves one transient completion rejection is retried after exactly one
    dedicated bounded backoff and then allows the worker loop to finish
    normally. *)
let test_transient_completion_retries_and_progresses () =
  let source = fake_source () in
  begin match
    Loop.run ~closed:(fun () -> source.closed)
      ~poll_workflow:idle_workflow
      ~poll_activity:(activity_after_one_rejection source)
      ~wait_for_lane:(record_wait source)
      ~retry_pending:(record_retry_wait source)
  with
  | Ok () -> ()
  | Error _ -> failwith "transient completion rejection stopped worker loop"
  end;
  if source.activity_polls <> 3 then
    failwith
      "worker loop did not retry the retained completion and then process a subsequent task";
  begin match source.retry_waits with
  | [ false ] -> ()
  | _ -> failwith "worker loop did not apply dedicated activity-lane backoff"
  end;
  if source.waits <> [] then
    failwith "retry-pending completion used ordinary readiness wait"

(** A permanent/protocol failure must stop the loop immediately. In
    particular, the scheduler must not reinterpret a protocol error as a
    transport retry merely because both errors use the same result channel. *)
let test_permanent_protocol_error_remains_fatal () =
  let waits = ref [] in
  let error = { code = "protocol"; retryable = false } in
  begin match
    Loop.run ~closed:(fun () -> false)
      ~poll_workflow:idle_workflow
      ~poll_activity:(fun () -> Error error)
      ~wait_for_lane:(fun ~workflow_lane ->
        waits := workflow_lane :: !waits;
        Ok ())
      ~retry_pending:(fun ~workflow_lane ->
        waits := workflow_lane :: !waits;
        Ok ())
  with
  | Error returned
    when returned.code = error.code && returned.retryable = error.retryable ->
      ()
  | Error _ -> failwith "permanent protocol error was rewritten"
  | Ok () -> failwith "permanent protocol error did not stop worker loop"
  end;
  if !waits <> [] then
    failwith "permanent protocol error incorrectly entered retry wait"

(** A successful Core rejection is surfaced as [Not_ready] after the native
    bounded backoff. Repeated unsupported deliveries must leave this scheduler
    free to observe the next healthy workflow, rather than ending Worker.run. *)
let test_rejected_delivery_keeps_workflow_lane_live () =
  let workflow_polls = ref 0 in
  let waits = ref 0 in
  let closed = ref false in
  let poll_workflow () =
    incr workflow_polls;
    if !workflow_polls <= 2 then Ok Loop.Not_ready
    else begin
      closed := true;
      Ok Loop.Progress
    end
  in
  begin match
    Loop.run ~closed:(fun () -> !closed)
      ~poll_workflow
      ~poll_activity:(fun () -> Ok Loop.Not_ready)
      ~wait_for_lane:(fun ~workflow_lane:_ -> incr waits; Ok ())
      ~retry_pending:(fun ~workflow_lane:_ ->
        failwith "rejected workflow delivery entered activity retry path")
  with
  | Ok () -> ()
  | Error _ -> failwith "rejected workflow delivery stopped the worker loop"
  end;
  if !workflow_polls <> 3 || !waits <> 2 then
    failwith "worker did not advance beyond rejected workflow deliveries"

(** A native stop request must short-circuit before either readiness lane or
    retry callback is touched. This protects shutdown liveness from a future
    loop refactor that accidentally polls a retired supervisor once more. *)
let test_closed_loop_does_not_poll () =
  let calls = ref 0 in
  let unexpected_poll () =
    incr calls;
    failwith "closed worker loop polled a backend lane"
  in
  let unexpected_wait ~workflow_lane:_ =
    incr calls;
    failwith "closed worker loop waited on a backend lane"
  in
  match
    Loop.run ~closed:(fun () -> true)
      ~poll_workflow:unexpected_poll ~poll_activity:unexpected_poll
      ~wait_for_lane:unexpected_wait ~retry_pending:unexpected_wait
  with
  | Ok () when !calls = 0 -> ()
  | Ok () -> failwith "closed worker loop invoked a backend callback"
  | Error _ -> failwith "closed worker loop returned an unexpected error"

(** Runs the focused scheduler regressions. *)
let () =
  test_transient_completion_retries_and_progresses ();
  test_permanent_protocol_error_remains_fatal ();
  test_rejected_delivery_keeps_workflow_lane_live ();
  test_closed_loop_does_not_poll ()

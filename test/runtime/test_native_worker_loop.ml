(** Regression tests for the private native worker lane scheduler.

    Fake callbacks retain the scheduling contract without a Temporal server.
    Atomic gates hold one activity callback while the workflow lane continues,
    and every test wait has a deadline so a lost wake fails promptly. *)

module Loop = Temporal_runtime.Native_worker_loop

(** A diagnostic the scheduler must return without inspecting its contents. *)
type source_error = { code : string; retryable : bool }

(** Waits for a cross-Domain observation with a test-only deadline. *)
let await label predicate =
  let deadline = Unix.gettimeofday () +. 5. in
  while not (predicate ()) do
    if Unix.gettimeofday () >= deadline then
      failwith ("timed out waiting for " ^ label);
    Domain.cpu_relax ()
  done

(** Returns no workflow work until the activity test closes its source. *)
let idle_workflow () = Ok Loop.Not_ready

(** A blocked activity must not prevent unrelated workflow progress. A second
    activity is queued but cannot enter while the first holds the sole slot. *)
let test_blocked_activity_does_not_block_workflow_or_overadmit () =
  let closed = Atomic.make false in
  let first_entered = Atomic.make false in
  let release_first = Atomic.make false in
  let workflow_progressed = Atomic.make false in
  let second_entered = Atomic.make false in
  let activity_polls = Atomic.make 0 in
  let poll_workflow () =
    if Atomic.get first_entered && not (Atomic.get workflow_progressed) then begin
      Atomic.set workflow_progressed true;
      Ok Loop.Progress
    end
    else Ok Loop.Not_ready
  in
  let poll_activity () =
    match Atomic.fetch_and_add activity_polls 1 with
    | 0 ->
        Atomic.set first_entered true;
        await "release of first activity" (fun () -> Atomic.get release_first);
        Ok Loop.Progress
    | 1 ->
        Atomic.set second_entered true;
        Atomic.set closed true;
        Ok Loop.Progress
    | _ -> failwith "capacity-one lane polled beyond the queued activities"
  in
  let wait_for_lane ~workflow_lane ~native_wait =
    if workflow_lane then begin
      if Atomic.get first_entered then begin
        if native_wait then
          failwith "idle workflow used the owner while an activity was busy";
      end;
      if Atomic.get workflow_progressed then
        await "second activity completion" (fun () -> Atomic.get closed)
      else
        await "first activity admission" (fun () -> Atomic.get first_entered);
      Ok ()
    end
    else failwith "activity lane waited despite a queued task"
  in
  let runner =
    Domain.spawn (fun () ->
        Loop.run ~closed:(fun () -> Atomic.get closed) ~poll_workflow
          ~poll_activity ~wait_for_lane
          ~retry_pending:(fun ~workflow_lane:_ ->
            failwith "queued activities entered completion retry"))
  in
  let observation =
    try
      await "first activity callback" (fun () -> Atomic.get first_entered);
      await "workflow progress during blocked activity" (fun () ->
          Atomic.get workflow_progressed);
      if Atomic.get second_entered || Atomic.get activity_polls <> 1 then
        failwith "second activity ran before the first callback completed";
      Atomic.set release_first true;
      await "second queued activity" (fun () -> Atomic.get second_entered);
      Ok ()
    with exception_ -> Error exception_
  in
  (* Release the fixture even if an assertion fails so the activity Domain can
     leave its callback before the test reports that failure. *)
  Atomic.set release_first true;
  Atomic.set closed true;
  let run_result = Domain.join runner in
  begin match observation with
  | Ok () -> ()
  | Error exception_ -> raise exception_
  end;
  begin match run_result with
  | Ok () -> ()
  | Error _ -> failwith "concurrent worker lanes returned an unexpected error"
  end;
  if Atomic.get activity_polls <> 2 then
    failwith "capacity-one lane did not process both queued activities"

(** A busy workflow poll keeps the idle activity lane off the sole supervisor
    owner, rather than allowing repeated unrelated 100 ms native waits. *)
let test_busy_workflow_keeps_idle_activity_off_owner () =
  let closed = Atomic.make false in
  let workflow_entered = Atomic.make false in
  let release_workflow = Atomic.make false in
  let activity_local_yields = Atomic.make 0 in
  let activity_native_waits = Atomic.make 0 in
  let runner =
    Domain.spawn (fun () ->
      Loop.run ~closed:(fun () -> Atomic.get closed)
        ~poll_workflow:(fun () ->
          Atomic.set workflow_entered true;
          await "workflow release" (fun () -> Atomic.get release_workflow);
          Atomic.set closed true;
          Ok Loop.Progress)
        ~poll_activity:(fun () ->
          await "busy workflow poll" (fun () -> Atomic.get workflow_entered);
          Ok Loop.Not_ready)
        ~wait_for_lane:(fun ~workflow_lane ~native_wait ->
          if workflow_lane then failwith "busy workflow entered readiness wait";
          if native_wait then
            ignore (Atomic.fetch_and_add activity_native_waits 1)
          else
            ignore (Atomic.fetch_and_add activity_local_yields 1);
          Thread.delay 0.001;
          Ok ())
        ~retry_pending:(fun ~workflow_lane:_ ->
          failwith "busy workflow fixture entered completion retry"))
  in
  let observation =
    try
      await "activity local yields" (fun () -> Atomic.get activity_local_yields >= 3);
      if Atomic.get activity_native_waits <> 0 then
        failwith "idle activity repeatedly blocked the owner during workflow work";
      Ok ()
    with exception_ -> Error exception_
  in
  Atomic.set release_workflow true;
  Atomic.set closed true;
  let run_result = Domain.join runner in
  (match observation with Ok () -> () | Error exception_ -> raise exception_);
  (match run_result with
  | Ok () -> ()
  | Error _ -> failwith "busy workflow fixture returned an unexpected error")

(** When both lanes are idle, one native event wait at a time alternates
    between their readiness signals. The sibling takes bounded local yields. *)
let test_idle_native_waits_alternate () =
  let closed = Atomic.make false in
  let waits = Atomic.make [] in
  let runner =
    Domain.spawn (fun () ->
      Loop.run ~closed:(fun () -> Atomic.get closed)
        ~poll_workflow:idle_workflow
        ~poll_activity:(fun () -> Ok Loop.Not_ready)
        ~wait_for_lane:(fun ~workflow_lane ~native_wait ->
          if native_wait then begin
            let observed = Atomic.get waits in
            Atomic.set waits (workflow_lane :: observed);
            if List.length observed >= 3 then Atomic.set closed true
          end;
          Thread.delay 0.001;
          Ok ())
        ~retry_pending:(fun ~workflow_lane:_ ->
          failwith "idle lanes entered completion retry"))
  in
  let observation =
    try
      await "four alternating native waits" (fun () ->
        List.length (Atomic.get waits) >= 4);
      let sequence = List.rev (Atomic.get waits) in
      if sequence <> [true; false; true; false] then
        failwith "idle native readiness waits did not alternate lanes";
      Ok ()
    with exception_ -> Error exception_
  in
  Atomic.set closed true;
  let run_result = Domain.join runner in
  (match observation with Ok () -> () | Error exception_ -> raise exception_);
  (match run_result with
  | Ok () -> ()
  | Error _ -> failwith "idle readiness fixture returned an unexpected error")

(** One retained completion receives a backoff and does not rerun its callback.
    The third poll is a distinct task, proving the lane stays live afterward. *)
let test_transient_completion_retries_and_progresses () =
  let closed = Atomic.make false in
  let activity_polls = Atomic.make 0 in
  let callback_calls = Atomic.make 0 in
  let retry_waits = Atomic.make 0 in
  let poll_activity () =
    match Atomic.fetch_and_add activity_polls 1 with
    | 0 ->
        ignore (Atomic.fetch_and_add callback_calls 1);
        Ok Loop.Retry_pending
    | 1 ->
        if Atomic.get retry_waits <> 1 then
          failwith "retained completion was retried before its backoff";
        Ok Loop.Progress
    | 2 ->
        ignore (Atomic.fetch_and_add callback_calls 1);
        Atomic.set closed true;
        Ok Loop.Progress
    | _ -> failwith "activity lane polled after fixture completion"
  in
  begin match
    Loop.run ~closed:(fun () -> Atomic.get closed)
      ~poll_workflow:idle_workflow ~poll_activity
      ~wait_for_lane:(fun ~workflow_lane ~native_wait:_ ->
        if not workflow_lane then
          failwith "retained completion used ordinary activity readiness";
        await "activity completion retry" (fun () -> Atomic.get closed);
        Ok ())
      ~retry_pending:(fun ~workflow_lane ->
        if workflow_lane then
          failwith "activity completion used workflow retry backoff";
        ignore (Atomic.fetch_and_add retry_waits 1);
        Ok ())
  with
  | Ok () -> ()
  | Error _ -> failwith "transient completion rejection stopped worker loop"
  end;
  if Atomic.get activity_polls <> 3 || Atomic.get callback_calls <> 2 then
    failwith "retained completion retry redispatched a callback or lost a task";
  if Atomic.get retry_waits <> 1 then
    failwith "retained completion did not receive one activity backoff"

(** A fatal activity error stops the sibling and preserves the error without
    setting the external shutdown flag; explicit teardown still owns that flag. *)
let test_permanent_activity_error_stops_sibling_without_shutdown () =
  let closed = Atomic.make false in
  let workflow_started = Atomic.make false in
  let activity_failed = Atomic.make false in
  let activity_polls = Atomic.make 0 in
  let error = { code = "protocol"; retryable = false } in
  begin match
    Loop.run ~closed:(fun () -> Atomic.get closed)
      ~poll_workflow:(fun () ->
        Atomic.set workflow_started true;
        Ok Loop.Not_ready)
      ~poll_activity:(fun () ->
        ignore (Atomic.fetch_and_add activity_polls 1);
        await "workflow lane startup" (fun () -> Atomic.get workflow_started);
        Atomic.set activity_failed true;
        Error error)
      ~wait_for_lane:(fun ~workflow_lane ~native_wait:_ ->
        if not workflow_lane then
          failwith "fatal activity error entered activity readiness wait";
        await "fatal activity result" (fun () -> Atomic.get activity_failed);
        Ok ())
      ~retry_pending:(fun ~workflow_lane:_ ->
        failwith "permanent activity error entered retry backoff")
  with
  | Error returned
    when returned.code = error.code && returned.retryable = error.retryable ->
      ()
  | Error _ -> failwith "permanent activity error was rewritten"
  | Ok () -> failwith "permanent activity error did not stop worker loop"
  end;
  if Atomic.get closed then
    failwith "lane failure changed the worker shutdown admission flag";
  if Atomic.get activity_polls <> 1 then
    failwith "fatal activity error caused another activity poll"

(** Rejected workflow deliveries leave the lane free to observe a later healthy
    task while the idle activity Domain waits for completion. *)
let test_rejected_delivery_keeps_workflow_lane_live () =
  let workflow_polls = Atomic.make 0 in
  let workflow_waits = Atomic.make 0 in
  let closed = Atomic.make false in
  let poll_workflow () =
    if Atomic.fetch_and_add workflow_polls 1 < 2 then Ok Loop.Not_ready
    else begin
      Atomic.set closed true;
      Ok Loop.Progress
    end
  in
  begin match
    Loop.run ~closed:(fun () -> Atomic.get closed) ~poll_workflow
      ~poll_activity:(fun () -> Ok Loop.Not_ready)
      ~wait_for_lane:(fun ~workflow_lane ~native_wait:_ ->
        if workflow_lane then ignore (Atomic.fetch_and_add workflow_waits 1)
        else await "workflow completion" (fun () -> Atomic.get closed);
        Ok ())
      ~retry_pending:(fun ~workflow_lane:_ ->
        failwith "rejected workflow delivery entered activity retry path")
  with
  | Ok () -> ()
  | Error _ -> failwith "rejected workflow delivery stopped the worker loop"
  end;
  if Atomic.get workflow_polls <> 3 || Atomic.get workflow_waits <> 2 then
    failwith "workflow lane did not advance beyond rejected deliveries"

(** A native stop request short-circuits before either lane is touched. *)
let test_closed_loop_does_not_poll () =
  let calls = Atomic.make 0 in
  let unexpected_poll () =
    ignore (Atomic.fetch_and_add calls 1);
    failwith "closed worker loop polled a backend lane"
  in
  let unexpected_wait ~workflow_lane:_ ~native_wait:_ =
    ignore (Atomic.fetch_and_add calls 1);
    failwith "closed worker loop waited on a backend lane"
  in
  match
    Loop.run ~closed:(fun () -> true)
      ~poll_workflow:unexpected_poll ~poll_activity:unexpected_poll
      ~wait_for_lane:unexpected_wait
      ~retry_pending:(fun ~workflow_lane:_ ->
        ignore (Atomic.fetch_and_add calls 1);
        failwith "closed worker loop retried a completion")
  with
  | Ok () when Atomic.get calls = 0 -> ()
  | Ok () -> failwith "closed worker loop invoked a backend callback"
  | Error _ -> failwith "closed worker loop returned an unexpected error"

(** Runs the focused scheduler regressions. *)
let () =
  test_blocked_activity_does_not_block_workflow_or_overadmit ();
  test_busy_workflow_keeps_idle_activity_off_owner ();
  test_idle_native_waits_alternate ();
  test_transient_completion_retries_and_progresses ();
  test_permanent_activity_error_stops_sibling_without_shutdown ();
  test_rejected_delivery_keeps_workflow_lane_live ();
  test_closed_loop_does_not_poll ()

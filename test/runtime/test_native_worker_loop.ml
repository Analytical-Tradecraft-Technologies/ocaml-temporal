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
    activity is queued but cannot enter while the first holds the sole slot.

    Every workflow poll first waits until the activity callback has entered.
    The loop decides whether an idle lane takes the native wait from a sibling
    busy snapshot taken after that lane's poll, so this ordering guarantees the
    snapshot sees the activity lane busy and [native_wait] must be [false]. If
    the workflow lane could poll before the activity Domain started, the loop
    could legitimately claim the wait token from an idle snapshot and the
    activity could enter before [wait_for_lane] ran, failing the assertion
    without any scheduler defect. *)
let test_blocked_activity_does_not_block_workflow_or_overadmit () =
  let closed = Atomic.make false in
  let first_entered = Atomic.make false in
  let release_first = Atomic.make false in
  let workflow_progressed = Atomic.make false in
  let second_entered = Atomic.make false in
  let activity_polls = Atomic.make 0 in
  let poll_workflow () =
    await "first activity admission" (fun () -> Atomic.get first_entered);
    if not (Atomic.get workflow_progressed) then begin
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
      (* Every workflow poll returned after the activity lane became busy, and
         that lane stays busy until shutdown, so no wait here is a native one. *)
      if native_wait then
        failwith "idle workflow used the owner while an activity was busy";
      await "second activity completion" (fun () -> Atomic.get closed);
      Ok ()
    end
    else failwith "activity lane waited despite a queued task"
  in
  let runner =
    Domain.spawn (fun () ->
        Loop.run ~detach:None ~closed:(fun () -> Atomic.get closed) ~poll_workflow
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
      Loop.run ~detach:None ~closed:(fun () -> Atomic.get closed)
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

(** A strict wait preference can strand both lanes in local yields when polls
    are staggered: the preferred workflow lane always observes activity busy,
    and the activity lane sees workflow idle but declines the wait token. After
    one deferral in the same wait epoch, the activity lane must claim it. *)
let test_staggered_idle_polls_eventually_enter_native_wait () =
  let closed = Atomic.make false in
  let workflow_polls = Atomic.make 0 in
  let activity_polls = Atomic.make 0 in
  let workflow_first_yield = Atomic.make false in
  let activity_first_yield = Atomic.make false in
  let workflow_second_yield = Atomic.make false in
  let activity_native_wait = Atomic.make false in
  let runner =
    Domain.spawn (fun () ->
      Loop.run ~detach:None ~closed:(fun () -> Atomic.get closed)
        ~poll_workflow:(fun () ->
          match Atomic.fetch_and_add workflow_polls 1 with
          | 0 ->
              await "first activity poll" (fun () ->
                Atomic.get activity_polls >= 1);
              Ok Loop.Not_ready
          | 1 ->
              await "second activity poll" (fun () ->
                Atomic.get activity_polls >= 2);
              Ok Loop.Not_ready
          | _ ->
              await "staggered fixture close" (fun () -> Atomic.get closed);
              Ok Loop.Not_ready)
        ~poll_activity:(fun () ->
          match Atomic.fetch_and_add activity_polls 1 with
          | 0 ->
              await "first workflow local yield" (fun () ->
                Atomic.get workflow_first_yield);
              Ok Loop.Not_ready
          | 1 ->
              await "second workflow local yield" (fun () ->
                Atomic.get workflow_second_yield);
              Ok Loop.Not_ready
          | _ -> Ok Loop.Not_ready)
        ~wait_for_lane:(fun ~workflow_lane ~native_wait ->
          if workflow_lane then begin
            if native_wait then
              failwith "preferred workflow waited while activity polled";
            if not (Atomic.get workflow_first_yield) then begin
              Atomic.set workflow_first_yield true;
              await "first activity local yield" (fun () ->
                Atomic.get activity_first_yield)
            end
            else begin
              Atomic.set workflow_second_yield true;
              await "activity native wait" (fun () -> Atomic.get closed)
            end
          end
          else if native_wait then begin
            Atomic.set activity_native_wait true;
            Atomic.set closed true
          end
          else if not (Atomic.get activity_first_yield) then begin
            Atomic.set activity_first_yield true;
            await "second workflow poll" (fun () ->
              Atomic.get workflow_polls >= 2)
          end;
          Ok ())
        ~retry_pending:(fun ~workflow_lane:_ ->
          failwith "staggered idle fixture entered completion retry"))
  in
  let observation =
    try
      await "native event wait after staggered idle polls" (fun () ->
        Atomic.get activity_native_wait);
      Ok ()
    with exception_ -> Error exception_
  in
  Atomic.set closed true;
  let run_result = Domain.join runner in
  (match observation with Ok () -> () | Error exception_ -> raise exception_);
  (match run_result with
  | Ok () -> ()
  | Error _ -> failwith "staggered idle fixture returned an unexpected error")

(** When both lanes are idle, each eventually receives a native event wait and
    the single token never admits two waits concurrently. Advisory preference
    can be overtaken by the one-yield stagger fallback. *)
let test_idle_native_waits_share_one_token () =
  let closed = Atomic.make false in
  let workflow_polled = Atomic.make false in
  let activity_polled = Atomic.make false in
  let workflow_waited = Atomic.make false in
  let activity_waited = Atomic.make false in
  let active_waits = Atomic.make 0 in
  let runner =
    Domain.spawn (fun () ->
      Loop.run ~detach:None ~closed:(fun () -> Atomic.get closed)
        ~poll_workflow:(fun () ->
          Atomic.set workflow_polled true;
          idle_workflow ())
        ~poll_activity:(fun () ->
          Atomic.set activity_polled true;
          Ok Loop.Not_ready)
        ~wait_for_lane:(fun ~workflow_lane ~native_wait ->
          if native_wait then begin
            if Atomic.fetch_and_add active_waits 1 <> 0 then
              failwith "idle lanes entered simultaneous native waits";
            if workflow_lane then Atomic.set workflow_waited true
            else Atomic.set activity_waited true;
            if Atomic.get workflow_waited && Atomic.get activity_waited then
              Atomic.set closed true;
            Thread.delay 0.001;
            ignore (Atomic.fetch_and_add active_waits (-1))
          end;
          if not native_wait then Thread.delay 0.001;
          Ok ())
        ~retry_pending:(fun ~workflow_lane:_ ->
          failwith "idle lanes entered completion retry"))
  in
  let observation =
    try
      (* Give both Domains a chance to start before measuring wait fairness. *)
      await "both idle lanes to poll" (fun () ->
        Atomic.get workflow_polled && Atomic.get activity_polled);
      await "both idle lanes to enter a native readiness wait" (fun () ->
        Atomic.get workflow_waited && Atomic.get activity_waited);
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
    Loop.run ~detach:None ~closed:(fun () -> Atomic.get closed)
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
    Loop.run ~detach:None ~closed:(fun () -> Atomic.get closed)
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
    Loop.run ~detach:None ~closed:(fun () -> Atomic.get closed) ~poll_workflow
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

(** Readiness semantics a sequential-workflow fixture gives the native wait.
    [Combined] is the loop's contract: any queued task on either lane ends the
    wait. [Lane_specific] models the pre-#806 bridge, where the token holder
    waited only on its own lane. *)
type wait_model = Combined | Lane_specific

(** Counters observed by one simulated sequential activity workflow run. *)
type sequential_stats = {
  completed_activities : int;
  native_waits : int;
  dead_waits : int;
      (** Native waits that a real bridge would have spent sleeping for the
          whole bounded timeout although a task was already queued. *)
}

(** Runs a workflow that awaits [steps] activities one after another through
    the real lane scheduler. A mutex stands in for the sole supervisor owner:
    every poll and native wait holds it, so a sibling's poll queues behind a
    wait just as it does in the supervisor mailbox. Completing an activity
    enqueues the next workflow activation and that activation schedules the
    next activity, so a task is always queued whenever the owner is free until
    the workflow completes.

    Instead of sleeping for the bridge's 100 ms timeout, a wait that cannot see
    the queued task returns at once and is counted as dead. Counting rather
    than timing keeps the regression independent of host speed. *)
let run_sequential_activity_workflow ~model ~steps =
  let owner = Mutex.create () in
  let workflow_pending = ref 1 in
  let activity_pending = ref 0 in
  let completed = ref 0 in
  let closed = Atomic.make false in
  let native_waits = Atomic.make 0 in
  let dead_waits = Atomic.make 0 in
  let poll_workflow () =
    Mutex.protect owner (fun () ->
        if !workflow_pending = 0 then Ok Loop.Not_ready
        else begin
          decr workflow_pending;
          if !completed = steps then Atomic.set closed true
          else incr activity_pending;
          Ok Loop.Progress
        end)
  in
  let poll_activity () =
    Mutex.protect owner (fun () ->
        if !activity_pending = 0 then Ok Loop.Not_ready
        else begin
          decr activity_pending;
          incr completed;
          incr workflow_pending;
          Ok Loop.Progress
        end)
  in
  let wait_for_lane ~workflow_lane ~native_wait =
    if native_wait then
      Mutex.protect owner (fun () ->
          ignore (Atomic.fetch_and_add native_waits 1);
          let own_pending, sibling_pending =
            if workflow_lane then (!workflow_pending, !activity_pending)
            else (!activity_pending, !workflow_pending)
          in
          let observed =
            match model with
            | Combined -> own_pending + sibling_pending > 0
            | Lane_specific -> own_pending > 0
          in
          if (not observed) && own_pending + sibling_pending > 0 then
            ignore (Atomic.fetch_and_add dead_waits 1)
          else if (not observed) && not (Atomic.get closed) then
            (* Work is produced only under [owner], so an empty fixture here
               could never be woken and would stall the run. *)
            failwith "native wait found no queued work before completion")
    else Thread.delay 0.001;
    Ok ()
  in
  begin match
    Loop.run ~detach:None ~closed:(fun () -> Atomic.get closed) ~poll_workflow
      ~poll_activity ~wait_for_lane
      ~retry_pending:(fun ~workflow_lane:_ ->
        failwith "sequential workflow fixture entered completion retry")
  with
  | Ok () -> ()
  | Error _ -> failwith "sequential workflow fixture returned an error"
  end;
  {
    completed_activities = Mutex.protect owner (fun () -> !completed);
    native_waits = Atomic.get native_waits;
    dead_waits = Atomic.get dead_waits;
  }

(** Regression for #806: with the combined readiness contract, a workflow that
    awaits activities sequentially never has its idle native wait sleep through
    a task queued on the other lane. The lane-specific control run proves the
    fixture detects that pattern: there the alternating token holder repeatedly
    waits on the lane that has nothing queued. *)
let test_sequential_activities_have_no_dead_waits () =
  let steps = 20 in
  let combined = run_sequential_activity_workflow ~model:Combined ~steps in
  if combined.completed_activities <> steps then
    failwith "sequential workflow did not complete every activity";
  if combined.dead_waits <> 0 then
    failwith
      (Printf.sprintf "combined readiness left %d dead waits in %d native waits"
         combined.dead_waits combined.native_waits);
  let control = run_sequential_activity_workflow ~model:Lane_specific ~steps in
  if control.completed_activities <> steps then
    failwith "lane-specific control did not complete every activity";
  if control.dead_waits = 0 then
    failwith "lane-specific control no longer exhibits the #806 dead wait"

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
    Loop.run ~detach:None ~closed:(fun () -> true)
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
  test_staggered_idle_polls_eventually_enter_native_wait ();
  test_idle_native_waits_share_one_token ();
  test_transient_completion_retries_and_progresses ();
  test_permanent_activity_error_stops_sibling_without_shutdown ();
  test_rejected_delivery_keeps_workflow_lane_live ();
  test_sequential_activities_have_no_dead_waits ();
  test_closed_loop_does_not_poll ()

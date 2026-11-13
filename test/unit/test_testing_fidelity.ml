(** Coverage for the server behaviors [Temporal.Testing] simulates beyond
    activity timeouts (#1160): the [`Retry] workflow-task-failure mode,
    start memo and search attributes, workflow ID reuse and conflict
    policies (top-level and child), asynchronous activities completed
    through their handles, and workflow execution and run timeouts. Every
    test drives workflow code through the public API only and asserts the
    typed outcome and, where time matters, the virtual time that passed. *)

module T = Temporal
open T.Result_syntax

(** Fails the test with [label] when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Fails the test with [label] when two strings differ, printing both. *)
let expect_string label expected actual =
  if not (String.equal expected actual) then
    failwith (Printf.sprintf "%s: expected %S, got %S" label expected actual)

(** Unwraps a successful result, failing the test with the error message. *)
let ok label = function
  | Ok value -> value
  | Error error -> failwith (label ^ " failed: " ^ T.Error.message error)

(** Asserts that a result failed with [category] and returns the error. *)
let expect_error label category = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      if (T.Error.view error).category <> category then
        failwith
          (Printf.sprintf "%s: unexpected %s error: %s" label
             (T.Error.kind error) (T.Error.message error));
      error

(** Reports whether [needle] occurs in [haystack]. *)
let contains ~needle haystack =
  let needle_length = String.length needle in
  let rec search index =
    index + needle_length <= String.length haystack
    && (String.equal (String.sub haystack index needle_length) needle
       || search (index + 1))
  in
  search 0

(** Asserts that [needle] occurs in [haystack]. *)
let expect_contains label ~needle haystack =
  if not (contains ~needle haystack) then
    failwith (Printf.sprintf "%s: %S does not contain %S" label haystack needle)

(** Creates an environment, runs [body], and always shuts it down. *)
let with_environment ?workflow_task_failure ~workflows ~activities body =
  let environment =
    ok "create"
      (T.Testing.create ?workflow_task_failure ~workflows ~activities ())
  in
  Fun.protect
    ~finally:(fun () -> T.Testing.shutdown environment)
    (fun () -> body environment)

(** Milliseconds since the Unix epoch of the environment's virtual clock. *)
let now_ms environment =
  let time = T.Testing.now environment in
  Int64.add
    (Int64.mul (T.Time.seconds time) 1_000L)
    (Int64.of_int (T.Time.nanoseconds time / 1_000_000))

let seconds count = T.Duration.of_ms (Int64.mul count 1_000L)
let minutes count = seconds (Int64.mul count 60L)

(** A retry policy with a fixed attempt budget and a short backoff. *)
let retry_policy ~maximum_attempts =
  ok "retry policy"
    (T.Retry_policy.make ~initial_interval:(seconds 1L) ~backoff_coefficient:2.0
       ~maximum_interval:(seconds 10L) ~maximum_attempts ())

(* --- Workflow task failure retry mode ----------------------------------- *)

(** Whether [fragile_workflow] still has its bug. Workflow code must not
    read process state; this test does so on purpose to model a bug the
    test fixes between task retries. *)
let broken = ref true

(** How many times [step] ran, so replay is shown not to rerun activities. *)
let step_calls = ref 0

let step =
  T.Activity.define ~name:"fidelity_step" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      incr step_calls;
      Ok "stepped")

(** The run's progress marker, readable by [progress_query]. *)
let progress : string T.Workflow_context.Local.t = T.Workflow_context.Local.create ()

(** The last note a signal delivered. *)
let note : string T.Workflow_context.Local.t = T.Workflow_context.Local.create ()

let note_signal = T.Signal.define ~name:"note" ~input:T.Codec.string
let progress_query = T.Query.define ~name:"progress" ~output:T.Codec.string

(** Reads a run-local string, or [default] when unset. *)
let local_or local default =
  match T.Workflow_context.Local.get local with
  | Ok (Some value) -> value
  | Ok None | Error _ -> default

(** Runs [step], sleeps a minute, then fails its workflow task while
    [broken] is set; once fixed it reports the step and the latest note. *)
let fragile_workflow =
  T.Workflow.define ~name:"fragile" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () ->
      let* () = T.Workflow_context.Local.set progress "started" in
      let* stepped = T.Activity.execute step () in
      let* () = T.Workflow_context.Local.set progress stepped in
      let* () = T.Workflow.sleep (minutes 1L) in
      if !broken then failwith "fragile bug";
      Ok (Printf.sprintf "%s note=%s" stepped (local_or note "none")))

(** The fragile workflow's registration with its signal and query. *)
let fragile_registration =
  T.Testing.workflow fragile_workflow
    ~signals:
      [ T.Signal.Handler.make note_signal (T.Workflow_context.Local.set note) ]
    ~queries:
      [ T.Query.Handler.make progress_query (fun () -> Ok (local_or progress "unset")) ]

(** In [`Retry] mode a failing task keeps the run open: the test observes
    the failure, queries the state before it, queues a signal, fixes the
    bug, and retries; the replay does not rerun the completed activity. *)
let test_task_failure_retry () =
  broken := true;
  step_calls := 0;
  with_environment ~workflow_task_failure:`Retry
    ~workflows:[ fragile_registration ] ~activities:[ T.Testing.activity step ]
    (fun environment ->
      let started = now_ms environment in
      let handle = ok "start" (T.Testing.start environment fragile_workflow ()) in
      let error =
        expect_error "stuck result" `Defect (T.Testing.result handle)
      in
      expect_contains "task failure" ~needle:"fragile bug" (T.Error.message error);
      expect "timer fired first" 60_000L (Int64.sub (now_ms environment) started);
      (match T.Testing.workflow_task_failure handle with
      | Some failure ->
          expect_contains "reported failure" ~needle:"fragile bug"
            (T.Error.message failure)
      | None -> failwith "the failing task was not reported");
      expect_string "query sees the last completed task" "stepped"
        (ok "query" (T.Testing.query handle progress_query));
      ok "signal while failing" (T.Testing.signal handle note_signal "queued");
      (* Retrying with the bug still present fails again and stays open. *)
      ok "retry while broken" (T.Testing.retry_workflow_tasks environment);
      expect "still failing" true
        (Option.is_some (T.Testing.workflow_task_failure handle));
      broken := false;
      ok "retry after fix" (T.Testing.retry_workflow_tasks environment);
      expect "no longer failing" None (T.Testing.workflow_task_failure handle);
      expect_string "fixed result" "stepped note=queued"
        (ok "result" (T.Testing.result handle));
      expect "the replay reused the activity result" 1 !step_calls;
      ignore
        (expect_error "nothing to retry" `Defect
           (T.Testing.retry_workflow_tasks environment)));
  (* The default mode still ends the run with the task failure. *)
  broken := true;
  with_environment ~workflows:[ fragile_registration ]
    ~activities:[ T.Testing.activity step ] (fun environment ->
      let handle = ok "start" (T.Testing.start environment fragile_workflow ()) in
      ignore (expect_error "fail-run result" `Defect (T.Testing.result handle));
      expect "fail-run reports no failing task" None
        (T.Testing.workflow_task_failure handle);
      ignore
        (expect_error "closed run" `Defect
           (T.Testing.signal handle note_signal "late")))

(** Whether [patched_workflow] and [mutating_workflow] still fail their
    second workflow task (deliberate process state, as for [broken]). *)
let replay_broken = ref true

(** Takes the patched branch in its first task, then fails its second task
    while [replay_broken] is set and reports which branch the first task
    took. The retry replays the first task, which must take the same
    branch. *)
let patched_workflow =
  T.Workflow.define ~name:"patched_retry" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      let branch = if T.Workflow.patched ~id:"v2" then "new" else "old" in
      let* () = T.Workflow.sleep (minutes 1L) in
      if !replay_broken then failwith "patched bug";
      Ok branch)

(** Records its input, then mutates the decoded bytes in place before
    failing its second task while [replay_broken] is set. A rebuild must
    decode a fresh input, so the replayed first task records the original. *)
let mutating_workflow =
  T.Workflow.define ~name:"mutating_retry" ~input:T.Codec.bytes
    ~output:T.Codec.string (fun input ->
      let seen = Bytes.to_string input in
      Bytes.set input 0 'X';
      let* () = T.Workflow.sleep (minutes 1L) in
      if !replay_broken then failwith "mutating bug";
      Ok seen)

(** A retried run replays with the patch markers it recorded, so
    [Workflow.patched] keeps its answer, and with a freshly decoded input,
    so a run-local mutation does not leak into the rebuild (#1347). *)
let test_retry_replay_fidelity () =
  replay_broken := true;
  with_environment ~workflow_task_failure:`Retry
    ~workflows:
      [ T.Testing.workflow patched_workflow; T.Testing.workflow mutating_workflow ]
    ~activities:[] (fun environment ->
      let patched = ok "start patched" (T.Testing.start environment patched_workflow ()) in
      let mutating =
        ok "start mutating"
          (T.Testing.start environment mutating_workflow (Bytes.of_string "abc"))
      in
      ignore (expect_error "patched stuck" `Defect (T.Testing.result patched));
      ignore (expect_error "mutating stuck" `Defect (T.Testing.result mutating));
      replay_broken := false;
      ok "retry" (T.Testing.retry_workflow_tasks environment);
      expect_string "patched branch survives replay" "new"
        (ok "patched result" (T.Testing.result patched));
      expect_string "replay sees the original input" "abc"
        (ok "mutating result" (T.Testing.result mutating)))

(* --- Start memo and search attributes ----------------------------------- *)

(** Upserts one memo key and reports the memo keys the run started with. *)
let metadata_workflow =
  T.Workflow.define ~name:"metadata" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () ->
      let* metadata = T.Workflow.start_metadata () in
      let* added = T.Codec.encode T.Codec.string "added" in
      T.Workflow.upsert_memo [ ("b", added) ];
      Ok
        (String.concat ","
           (List.map fst (Option.value metadata.memo ~default:[]))))

(** A start's memo and search attributes reach the run and its readers. *)
let test_start_metadata () =
  with_environment ~workflows:[ T.Testing.workflow metadata_workflow ]
    ~activities:[] (fun environment ->
      let memo_value = ok "memo" (T.Codec.encode T.Codec.string "kept") in
      let search_attributes =
        ok "search attributes"
          (T.Search_attribute.encode
             [ T.Search_attribute.(set (keyword "CustomKeyword") "gold") ])
      in
      let handle =
        ok "start"
          (T.Testing.start ~memo:[ ("a", memo_value) ] ~search_attributes
             environment metadata_workflow ())
      in
      expect_string "workflow saw the start memo" "a"
        (ok "result" (T.Testing.result handle));
      expect "merged memo keys" [ "a"; "b" ] (List.map fst (T.Testing.memo handle));
      expect "search attributes kept" search_attributes
        (T.Testing.search_attributes handle))

(* --- Workflow ID policies -------------------------------------------- *)

(** Sleeps for a day, then completes. *)
let sleeper_workflow =
  T.Workflow.define ~name:"sleeper" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () ->
      let* () = T.Workflow.sleep (minutes 1440L) in
      Ok "slept")

(** Fails at once with an application failure. *)
let failing_workflow =
  T.Workflow.define ~name:"failing_workflow" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () ->
      Error (T.Error.make ~category:`Workflow ~message:"business failure" ()))

(** Asserts that [result] is the client's already-started error naming
    [run_id] of workflow ["order"]. *)
let expect_already_started label ~run_id result =
  let error = expect_error label `Workflow result in
  expect (label ^ " type") (Some "WorkflowExecutionAlreadyStarted")
    (T.Error.error_type error);
  match T.Client.already_started error with
  | Some execution ->
      expect_string (label ^ " workflow") "order" execution.workflow_id;
      expect_string (label ^ " run") run_id execution.run_id
  | None -> failwith (label ^ ": no conflicting execution")

(** Conflict policies decide a start against an open run, reuse policies
    one against a closed run, and a refused start returns the client's
    typed already-started error. *)
let test_id_policies () =
  with_environment
    ~workflows:
      [ T.Testing.workflow sleeper_workflow; T.Testing.workflow failing_workflow ]
    ~activities:[] (fun environment ->
      let first = ok "first" (T.Testing.start ~id:"order" environment sleeper_workflow ()) in
      expect "first started" true (T.Testing.started first);
      expect_already_started "fail conflict" ~run_id:(T.Testing.run_id first)
        (T.Testing.start ~id:"order" environment sleeper_workflow ());
      let attached =
        ok "use existing"
          (T.Testing.start ~id:"order" ~id_conflict_policy:`Use_existing
             environment sleeper_workflow ())
      in
      expect "attached, not started" false (T.Testing.started attached);
      expect_string "same run" (T.Testing.run_id first) (T.Testing.run_id attached);
      let replacement =
        ok "terminate existing"
          (T.Testing.start ~id:"order" ~id_conflict_policy:`Terminate_existing
             environment sleeper_workflow ())
      in
      if String.equal (T.Testing.run_id replacement) (T.Testing.run_id first) then
        failwith "terminate-existing kept the old run";
      ignore (expect_error "terminated run" `Terminated (T.Testing.result first));
      expect_string "replacement completes" "slept"
        (ok "replacement" (T.Testing.result replacement));
      let completed_run = T.Testing.run_id replacement in
      expect_already_started "reject duplicate" ~run_id:completed_run
        (T.Testing.start ~id:"order" ~id_reuse_policy:`Reject_duplicate
           environment sleeper_workflow ());
      expect_already_started "failed only after success" ~run_id:completed_run
        (T.Testing.start ~id:"order" ~id_reuse_policy:`Allow_duplicate_failed_only
           environment sleeper_workflow ());
      ignore (ok "default reuse" (T.Testing.start ~id:"order" environment sleeper_workflow ()));
      ignore
        (expect_error "business failure" `Workflow
           (T.Testing.execute ~id:"invoice" environment failing_workflow ()));
      ignore
        (ok "failed only after failure"
           (T.Testing.start ~id:"invoice" ~id_reuse_policy:`Allow_duplicate_failed_only
              environment failing_workflow ())))

(** Completes at once. *)
let quick_child =
  T.Workflow.define ~name:"quick_child" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () -> Ok "child done")

(** Runs [quick_child] as ["kid"], then tries again under
    [`Reject_duplicate] and reports how the second start ended. *)
let reusing_parent =
  T.Workflow.define ~name:"reusing_parent" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      let* first = T.Child_workflow.execute ~id:"kid" quick_child () in
      match
        T.Child_workflow.execute ~id_reuse_policy:`Reject_duplicate ~id:"kid"
          quick_child ()
      with
      | Ok _ -> Ok (first ^ "; reused")
      | Error error ->
          Ok
            (Printf.sprintf "%s; %s %s" first (T.Error.kind error)
               (if contains ~needle:"workflow_already_exists" (T.Error.message error)
                then "already exists"
                else "other")))

(** A child start applies its own reuse policy to the closed child. *)
let test_child_reuse_policy () =
  with_environment
    ~workflows:[ T.Testing.workflow reusing_parent; T.Testing.workflow quick_child ]
    ~activities:[] (fun environment ->
      expect_string "second child refused" "child done; child_workflow already exists"
        (ok "parent" (T.Testing.execute environment reusing_parent ())))

(* --- Asynchronous activities ------------------------------------------- *)

(** Handles the asynchronous [charge] activity handed off, newest first. *)
let handles : string T.Activity.async_handle list ref = ref []

(** Hands every attempt off for asynchronous completion. *)
let charge =
  T.Activity.define_async ~name:"charge" ~input:T.Codec.string
    ~output:T.Codec.string (fun context _order ->
      let handle = T.Activity.Async_context.handle context in
      handles := handle :: !handles;
      T.Activity.Will_complete_async handle)

(** The newest handed-off handle. *)
let latest_handle () =
  match !handles with
  | handle :: _ -> handle
  | [] -> failwith "no asynchronous attempt was handed off"

(** Runs [charge] with the given timeouts and a two-attempt policy and
    reports its outcome as a string, so the test sees timeouts as values. *)
let checkout_workflow =
  T.Workflow.define ~name:"checkout" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () ->
      match
        T.Activity.execute ~start_to_close_timeout:(minutes 5L)
          ~heartbeat_timeout:(seconds 10L)
          ~retry_policy:(retry_policy ~maximum_attempts:2)
          charge "order-1"
      with
      | Ok receipt -> Ok ("ok " ^ receipt)
      | Error error ->
          Ok
            (Printf.sprintf "error timeout=%s"
               (match T.Error.timeout_type error with
               | Some `Heartbeat -> "heartbeat"
               | Some `Start_to_close -> "start_to_close"
               | Some _ -> "other"
               | None -> "none")))

(** An asynchronous activity waits for its handle: heartbeats restart the
    heartbeat timeout, a failure is retried with a fresh handle, completion
    resolves it, and a finished handle is retired. *)
let test_async_completion () =
  handles := [];
  with_environment ~workflows:[ T.Testing.workflow checkout_workflow ]
    ~activities:[ T.Testing.activity charge ] (fun environment ->
      let handle = ok "start" (T.Testing.start environment checkout_workflow ()) in
      let first = latest_handle () in
      ignore
        (expect_error "cancel without a request" `Activity
           (T.Activity.Async_handle.cancel first []));
      ok "skip 8s" (T.Testing.skip environment (seconds 8L));
      ok "heartbeat" (T.Activity.Async_handle.heartbeat first []);
      ok "skip 8s more" (T.Testing.skip environment (seconds 8L));
      ok "fail first attempt"
        (T.Activity.Async_handle.fail first
           (T.Error.make ~category:`Activity ~message:"card declined" ()));
      ok "skip the backoff" (T.Testing.skip environment (seconds 1L));
      expect "second attempt handed off" 2 (List.length !handles);
      ok "complete" (T.Activity.Async_handle.complete (latest_handle ()) "receipt-7");
      expect_string "result" "ok receipt-7" (ok "result" (T.Testing.result handle));
      let error =
        match T.Activity.Async_handle.complete (latest_handle ()) "again" with
        | Ok () -> failwith "a finished handle accepted a second completion"
        | Error error -> error
      in
      expect "retired handle error is final" true (T.Error.view error).non_retryable);
  (* A handle nobody finishes times out by its heartbeat timeout. *)
  handles := [];
  with_environment ~workflows:[ T.Testing.workflow checkout_workflow ]
    ~activities:[ T.Testing.activity charge ] (fun environment ->
      let started = now_ms environment in
      expect_string "heartbeat timeout" "error timeout=heartbeat"
        (ok "result" (T.Testing.execute environment checkout_workflow ()));
      (* Two attempts, 10 s each, with a 1 s backoff between them. *)
      expect "virtual time" 21_000L (Int64.sub (now_ms environment) started))

(** Starts [charge] waiting for cancellation to complete, cancels it after a
    second, and reports how the activity ended. *)
let cancelling_checkout =
  T.Workflow.define ~name:"cancelling_checkout" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      let activity =
        T.Activity.start_handle ~start_to_close_timeout:(minutes 5L)
          ~cancellation_type:T.Activity.Wait_cancellation_completed charge "order-2"
      in
      let* () = T.Workflow.sleep (seconds 1L) in
      let* () = T.Activity.cancel activity in
      match T.Future.await (T.Activity.future activity) with
      | Ok receipt -> Ok ("completed " ^ receipt)
      | Error error -> Ok ("ended " ^ T.Error.kind error))

(** A cancellation request reaches the completer through its heartbeat,
    and the workflow waits for the completer to report the cancellation. *)
let test_async_cancellation () =
  handles := [];
  with_environment ~workflows:[ T.Testing.workflow cancelling_checkout ]
    ~activities:[ T.Testing.mock_async_activity charge (fun context _ ->
        let handle = T.Activity.Async_context.handle context in
        handles := handle :: !handles;
        T.Activity.Will_complete_async handle) ]
    (fun environment ->
      let handle = ok "start" (T.Testing.start environment cancelling_checkout ()) in
      ok "reach the cancel" (T.Testing.skip environment (seconds 1L));
      let pending = latest_handle () in
      let error =
        expect_error "heartbeat after the request" `Cancelled
          (T.Activity.Async_handle.heartbeat pending [])
      in
      expect "cancellation is final" true (T.Error.view error).non_retryable;
      ok "report the cancellation" (T.Activity.Async_handle.cancel pending []);
      expect_string "workflow saw the cancellation" "ended cancelled"
        (ok "result" (T.Testing.result handle)))

(** Runs [charge] as a local activity, which cannot complete asynchronously. *)
let local_checkout =
  T.Workflow.define ~name:"local_checkout" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      T.Activity.execute_local ~start_to_close_timeout:(seconds 5L) charge "order-3")

(** A local activity's [Will_complete_async] fails the attempt for good. *)
let test_async_local () =
  with_environment ~workflows:[ T.Testing.workflow local_checkout ]
    ~activities:[ T.Testing.activity charge ] (fun environment ->
      let error =
        expect_error "local async" `Activity
          (T.Testing.execute environment local_checkout ())
      in
      expect_contains "local async reason" ~needle:"cannot be completed asynchronously"
        (T.Error.message error))

(* --- Workflow timeouts ------------------------------------------------- *)

(** Reports the execution and task timeouts the run reads, in milliseconds. *)
let timeouts_workflow =
  T.Workflow.define ~name:"timeouts" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () ->
      let* info = T.Workflow.info () in
      let ms = function
        | Some duration -> Int64.to_string (T.Duration.to_ms duration)
        | None -> "none"
      in
      Ok
        (Printf.sprintf "execution=%s task=%s"
           (ms (T.Workflow.Info.execution_timeout info))
           (ms (T.Workflow.Info.task_timeout info))))

(** Sleeps two minutes on its first attempt and returns the attempt. *)
let slow_first_workflow =
  T.Workflow.define ~name:"slow_first" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () ->
      let* info = T.Workflow.info () in
      let attempt = T.Workflow.Info.attempt info in
      let* () = if attempt = 1 then T.Workflow.sleep (minutes 2L) else Ok () in
      Ok (Printf.sprintf "attempt=%d" attempt))

(** Starts [sleeper_workflow] as a child with a one-minute run timeout and
    reports the child failure's shape. *)
let timing_parent =
  T.Workflow.define ~name:"timing_parent" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      match
        T.Child_workflow.execute ~run_timeout:(minutes 1L) ~id:"slow-child"
          sleeper_workflow ()
      with
      | Ok _ -> Ok "child completed"
      | Error error ->
          Ok
            (Printf.sprintf "%s timeout=%b non_retryable=%b" (T.Error.kind error)
               (T.Error.timeout_type error = Some `Start_to_close)
               (T.Error.view error).non_retryable))

(** Execution and run timeouts expire in virtual time with the timeout
    error shape, a run timeout is retried under the retry policy, and a
    child's timeout reaches its parent as a child failure. *)
let test_workflow_timeouts () =
  with_environment
    ~workflows:
      [
        T.Testing.workflow sleeper_workflow;
        T.Testing.workflow timeouts_workflow;
        T.Testing.workflow slow_first_workflow;
        T.Testing.workflow timing_parent;
      ]
    ~activities:[] (fun environment ->
      expect_string "reported timeouts" "execution=3600000 task=10000"
        (ok "timeouts"
           (T.Testing.execute ~execution_timeout:(minutes 60L)
              ~task_timeout:(seconds 10L) environment timeouts_workflow ()));
      let started = now_ms environment in
      let error =
        expect_error "execution timeout" `Timeout
          (T.Testing.execute ~execution_timeout:(minutes 60L) environment
             sleeper_workflow ())
      in
      expect "timeout type" (Some `Start_to_close) (T.Error.timeout_type error);
      expect "expired after an hour" 3_600_000L (Int64.sub (now_ms environment) started);
      let started = now_ms environment in
      expect_string "run timeout retried" "attempt=2"
        (ok "run timeout"
           (T.Testing.execute ~run_timeout:(minutes 1L)
              ~retry_policy:(retry_policy ~maximum_attempts:3)
              environment slow_first_workflow ()));
      expect "timeout plus backoff" 61_000L (Int64.sub (now_ms environment) started);
      ignore
        (expect_error "invalid timeouts" `Defect
           (T.Testing.start ~execution_timeout:(minutes 1L) ~run_timeout:(minutes 2L)
              environment sleeper_workflow ()));
      let started = now_ms environment in
      expect_string "child timeout" "child_workflow timeout=true non_retryable=false"
        (ok "parent" (T.Testing.execute environment timing_parent ()));
      expect "child timed out after a minute" 60_000L
        (Int64.sub (now_ms environment) started))

let () =
  test_task_failure_retry ();
  test_retry_replay_fidelity ();
  test_start_metadata ();
  test_id_policies ();
  test_child_reuse_policy ();
  test_async_completion ();
  test_async_cancellation ();
  test_async_local ();
  test_workflow_timeouts ()

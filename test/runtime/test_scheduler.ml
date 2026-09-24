module Scheduler = Temporal_runtime.Scheduler

(** Compares values and includes a short scenario label on failure. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Creates the structured defect returned when a test future is used outside
    its owning scheduler. Keeping scheduler ownership failures in [Error.t]
    matches the public workflow API rather than relying on exceptions. *)
let outside_error () = Temporal.Error.defect ~message:"outside scheduler"

(** Adapts the scheduler's internal future through the package-private kernel.
    Runtime tests intentionally create low-level futures so they can resolve
    them directly; production code reaches the same kernel only through the
    public facade's internal adapters, and installed consumers cannot import
    this package-private module. *)
let public_future ~outside_error future =
  Temporal_future_kernel.make
    ~await:(fun () -> Temporal_runtime.Future_store.await future)
    ~await_gate:(fun register ->
      Temporal_runtime.Future_store.await_gate future register)
    ~subscribe:(Temporal_runtime.Future_store.subscribe future)
    ~is_ready:(fun () -> Temporal_runtime.Future_store.is_ready future)
    ~peek:(fun () -> Temporal_runtime.Future_store.peek future)
    ~owner_id:(Temporal_runtime.Future_store.owner_id future)
    ~outside_error
    ~callbacks_live:(fun () -> Temporal_runtime.Future_store.callbacks_live future)
    ~enqueue:(Temporal_runtime.Future_store.enqueue future)

(** Creates a test-controlled scheduler promise and exposes it through the
    public future interface. Resolvers stay internal because tests need to
    model native completion events explicitly. *)
let promise scheduler ~outside_error =
  let future, resolve = Scheduler.promise scheduler ~outside_error in
  (public_future ~outside_error future, resolve)

(** Compares an SDK error by its stable public message rather than by its
    private representation. *)
let expect_error_message label expected = function
  | Error error -> expect label expected (Temporal.Error.message error)
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")

(** Verifies that futures resume their waiting fibers in registration order. *)
let test_fifo_resume () =
  let scheduler = Scheduler.create () in
  let first, resolve_first =
    promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  let second, resolve_second =
    promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  let seen = ref [] in
  Scheduler.spawn scheduler (fun () ->
      seen := "first entered" :: !seen;
      match Temporal.Future.await first with
      | Ok value -> seen := value :: !seen
      | Error error -> failwith error);
  Scheduler.spawn scheduler (fun () ->
      seen := "second entered" :: !seen;
      match Temporal.Future.await second with
      | Ok value -> seen := value :: !seen
      | Error error -> failwith error);
  expect "blocked" "blocked" (Scheduler.run_label scheduler);
  resolve_second (Ok "second");
  resolve_first (Ok "first");
  expect "complete" "complete" (Scheduler.run_label scheduler);
  expect "FIFO roots and resolution-order resume"
    [ "first entered"; "second entered"; "second"; "first" ] (List.rev !seen)

(** Verifies that a future rejects a second result. *)
let test_double_resolution () =
  let scheduler = Scheduler.create () in
  let _, resolve =
    promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  resolve (Ok 1);
  match resolve (Ok 2) with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "future resolved twice"

(** Exercises successful [map] and [both] composition on scheduler futures. *)
let test_combinators () =
  let scheduler = Scheduler.create () in
  let left, resolve_left =
    promise scheduler ~outside_error
  in
  let right, resolve_right =
    promise scheduler ~outside_error
  in
  let both = Temporal.Future.both (Temporal.Future.map String.length left) right in
  let result = ref None in
  Scheduler.spawn scheduler (fun () -> result := Some (Temporal.Future.await both));
  expect "blocked combinator" "blocked" (Scheduler.run_label scheduler);
  resolve_right (Ok 42);
  resolve_left (Ok "agent");
  expect "complete combinator" "complete" (Scheduler.run_label scheduler);
  expect "both" (Some (Ok (5, 42))) !result

(** Confirms awaiting without the owning active scheduler returns the supplied
    structured error rather than performing an unhandled effect. *)
let test_outside_scheduler () =
  let scheduler = Scheduler.create () in
  let future, _ =
    promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  expect "outside await" (Error "outside scheduler") (Temporal.Future.await future)

(** Covers immediate results and several fibers waiting on the same future. *)
let test_immediate_and_multiple_waiters () =
  let scheduler = Scheduler.create () in
  let future, resolve =
    promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  resolve (Ok 7);
  let seen = ref [] in
  Scheduler.spawn scheduler (fun () ->
      seen := ("first", Temporal.Future.await future) :: !seen);
  Scheduler.spawn scheduler (fun () ->
      seen := ("second", Temporal.Future.await future) :: !seen);
  expect "immediate complete" "complete" (Scheduler.run_label scheduler);
  expect "immediate values and spawn order"
    [ ("first", Ok 7); ("second", Ok 7) ] (List.rev !seen)

(** Covers error mapping and rejection of futures from different schedulers. *)
let test_map_error_and_owner_check () =
  let first_scheduler = Scheduler.create () in
  let first, resolve_first =
    promise first_scheduler ~outside_error:(fun () -> "outside")
  in
  let mapped = Temporal.Future.map_error String.uppercase_ascii first in
  resolve_first (Error "failure");
  expect "mapped error processing" "complete" (Scheduler.run_label first_scheduler);
  expect "mapped error" (Some (Error "FAILURE")) (Temporal.Future.peek mapped)

(** An already-resolved future has an inert owner that delivers callbacks
    immediately without being a valid workflow-await owner. Derived futures
    must therefore still run their observer callbacks outside a scheduler. *)
let test_resolved_combinator_callback_delivery () =
  let source =
    Temporal_runtime.Future_store.resolved
      ~outside_error:(fun () -> outside_error ()) (Ok 4)
  in
  let mapped = Temporal_runtime.Future_store.map (fun value -> value + 1) source in
  expect "resolved map callback" (Some (Ok 5))
    (Temporal_runtime.Future_store.peek mapped)

(** Verifies aggregate ownership mistakes become ready structured errors for
    every public combinator, including the pre-existing [both]. *)
let test_aggregate_owner_errors_are_typed () =
  let first_scheduler = Scheduler.create () in
  let second_scheduler = Scheduler.create () in
  let first, _ = promise first_scheduler ~outside_error in
  let second, _ = promise second_scheduler ~outside_error in
  let expected =
    "Temporal future combinator received futures from different workflow executions"
  in
  let expect_defect label future =
    match Temporal.Future.peek future with
    | Some result -> expect_error_message label expected result
    | None -> failwith (label ^ " did not return a ready error")
  in
  expect_defect "both owner" (Temporal.Future.both first second);
  expect_defect "all owner" (Temporal.Future.all [ first; second ]);
  expect_defect "race owner" (Temporal.Future.race first second);
  expect_defect "first owner" (Temporal.Future.first first [ second ])

(** Confirms [all] waits for every input, retains input ordering, and selects
    the first error by input order rather than completion order. *)
let test_all_order_and_errors () =
  let scheduler = Scheduler.create () in
  let first, resolve_first = promise scheduler ~outside_error in
  let second, resolve_second = promise scheduler ~outside_error in
  let third, resolve_third = promise scheduler ~outside_error in
  let all = Temporal.Future.all [ first; second; third ] in
  resolve_third (Ok 3);
  resolve_first (Ok 1);
  expect "all waits for every input" "blocked" (Scheduler.run_label scheduler);
  resolve_second (Ok 2);
  expect "all completes" "complete" (Scheduler.run_label scheduler);
  expect "all input order" (Some (Ok [ 1; 2; 3 ])) (Temporal.Future.peek all);
  let first, resolve_first = promise scheduler ~outside_error in
  let second, resolve_second = promise scheduler ~outside_error in
  let failed = Temporal.Future.all [ first; second ] in
  resolve_second (Error (Temporal.Error.defect ~message:"second"));
  resolve_first (Error (Temporal.Error.defect ~message:"first"));
  expect "all errors complete" "complete" (Scheduler.run_label scheduler);
  match Temporal.Future.peek failed with
  | Some result -> expect_error_message "all first input error" "first" result
  | None -> failwith "all error remained pending"

(** Verifies an empty aggregate is immediately successful without allocating
    pending scheduler work. *)
let test_all_empty () =
  let empty = Temporal.Future.all [] in
  expect "empty all" (Some (Ok [])) (Temporal.Future.peek empty);
  let mapped = Temporal.Future.map List.length empty in
  expect "empty all derived callback" (Some (Ok 0))
    (Temporal.Future.peek mapped)

(** Verifies ready inputs use argument order, while pending inputs use
    deterministic scheduler callback order. Losing inputs remain observable. *)
let test_race_order_and_loser () =
  let scheduler = Scheduler.create () in
  let left, resolve_left = promise scheduler ~outside_error in
  let right, resolve_right = promise scheduler ~outside_error in
  resolve_right (Ok "right");
  resolve_left (Ok "left");
  let ready_race = Temporal.Future.race left right in
  expect "ready race processing" "complete" (Scheduler.run_label scheduler);
  expect "ready race argument order"
    (Some (Ok (Temporal.Future.Left "left")))
    (Temporal.Future.peek ready_race);
  let left, resolve_left = promise scheduler ~outside_error in
  let right, resolve_right = promise scheduler ~outside_error in
  let pending_race = Temporal.Future.race left right in
  resolve_right (Ok "right");
  expect "pending race first callback" "blocked" (Scheduler.run_label scheduler);
  expect "pending race completion order"
    (Some (Ok (Temporal.Future.Right "right")))
    (Temporal.Future.peek pending_race);
  resolve_left (Ok "left");
  expect "race loser can settle" "complete" (Scheduler.run_label scheduler);
  expect "race winner remains stable"
    (Some (Ok (Temporal.Future.Right "right")))
    (Temporal.Future.peek pending_race)

(** A race winner must not be retained by a pending loser after the caller
    drops the aggregate handle. The loser remains a valid operation, so its
    observer stays registered, but the observer must release the aggregate
    resolver and its resolved payload immediately after selecting a winner. *)
let test_race_releases_winner_from_pending_loser () =
  let scheduler, weak_payload =
    let scheduler = Scheduler.create () in
    let weak_payload = Weak.create 1 in
    let build_race () =
      let payload = Bytes.create 1_048_576 in
      Weak.set weak_payload 0 (Some payload);
      let left, resolve_left = promise scheduler ~outside_error in
      let _right, _resolve_right = promise scheduler ~outside_error in
      let _race = Temporal.Future.race left _right in
      resolve_left (Ok payload);
      expect "race winner processing" "blocked" (Scheduler.run_label scheduler)
    in
    build_race ();
    scheduler, weak_payload
  in
  ignore (Sys.opaque_identity scheduler);
  if Option.is_none (Weak.get weak_payload 0) then
    failwith "race payload was already gone before collection";
  Gc.full_major ();
  expect "race loser remains pending" "blocked" (Scheduler.run_label scheduler);
  if Option.is_some (Weak.get weak_payload 0) then
    failwith "pending race loser retained the resolved winner payload"

(** The homogeneous [first] combinator has the same retention contract as
    [race]: selecting one result must not keep that result alive solely through
    an observer attached to an unfinished losing operation. *)
let test_first_releases_winner_from_pending_loser () =
  let scheduler, weak_payload =
    let scheduler = Scheduler.create () in
    let weak_payload = Weak.create 1 in
    let build_first () =
      let payload = Bytes.create 1_048_576 in
      Weak.set weak_payload 0 (Some payload);
      let leading, resolve_leading = promise scheduler ~outside_error in
      let _loser, _resolve_loser = promise scheduler ~outside_error in
      let _first = Temporal.Future.first leading [ _loser ] in
      resolve_leading (Ok payload);
      expect "first winner processing" "blocked" (Scheduler.run_label scheduler)
    in
    build_first ();
    scheduler, weak_payload
  in
  ignore (Sys.opaque_identity scheduler);
  if Option.is_none (Weak.get weak_payload 0) then
    failwith "first payload was already gone before collection";
  Gc.full_major ();
  expect "first loser remains pending" "blocked" (Scheduler.run_label scheduler);
  if Option.is_some (Weak.get weak_payload 0) then
    failwith "pending first loser retained the resolved winner payload"

(** Confirms [first] settles on an error as a completion event and does not
    wait for or cancel later candidates. *)
let test_first_completion_error () =
  let scheduler = Scheduler.create () in
  let first, resolve_first = promise scheduler ~outside_error in
  let second, resolve_second = promise scheduler ~outside_error in
  let earliest = Temporal.Future.first first [ second ] in
  resolve_second (Error (Temporal.Error.defect ~message:"won with error"));
  expect "first error processing" "blocked" (Scheduler.run_label scheduler);
  (match Temporal.Future.peek earliest with
  | Some result -> expect_error_message "first completion error" "won with error" result
  | None -> failwith "first completion did not settle");
  resolve_first (Ok 1);
  expect "first loser settles" "complete" (Scheduler.run_label scheduler)

(** Confirms an exception raised by a mapping function becomes a scheduler
    failure instead of escaping the run loop. *)
let test_mapper_defect_is_contained () =
  let scheduler = Scheduler.create () in
  let source, resolve =
    promise scheduler ~outside_error:(fun () -> "outside")
  in
  let _mapped = Temporal.Future.map (fun _ -> failwith "mapper defect") source in
  resolve (Ok 1);
  match Scheduler.run scheduler with
  | Scheduler.Failed (Failure message) when message = "mapper defect" -> ()
  | _ -> failwith "mapper exception escaped or was not recorded"

(** A ready-owner-mismatch future ([ready_like]) that becomes the parent of a
    still-pending outer combinator must suspend the awaiting fiber until the
    outer future genuinely settles, and must then return the real settled
    result rather than a synchronous "outside its workflow scheduler" defect.
    Regression test for a bug where [ready_like] retained a real owner id but
    installed a no-op await gate, so [make_derived.await] fell through to its
    outside-scheduler fallback without ever suspending. *)
let test_ready_like_parent_suspends_pending_outer () =
  let first_scheduler = Scheduler.create () in
  let second_scheduler = Scheduler.create () in
  let f, resolve_f = promise first_scheduler ~outside_error in
  let g, resolve_g = promise second_scheduler ~outside_error in
  let h, resolve_h = promise first_scheduler ~outside_error in
  (* [inner] is an immediately ready ownership-error future owned by
     [first_scheduler] (f's owner), built from a genuine cross-scheduler
     mismatch between [f] and [g]. Resolve both inputs so their schedulers'
     pending counts can reach zero; [both]'s owner-mismatch branch captures
     the ownership error independently of whether [f]/[g] ever settle. *)
  let inner = Temporal.Future.both f g in
  resolve_f (Ok 1);
  resolve_g (Ok 1);
  (* [outer] shares [inner]'s owner with [h], so it proceeds past the owner
     guard and stays pending on [h]. *)
  let outer = Temporal.Future.both inner h in
  let observed = ref None in
  Scheduler.spawn first_scheduler (fun () ->
      observed := Some (Temporal.Future.await outer));
  expect "outer await suspends on pending sibling" "blocked"
    (Scheduler.run_label first_scheduler);
  expect "await does not return before the sibling settles" None !observed;
  resolve_h (Ok 2);
  expect "outer completes once the sibling resolves" "complete"
    (Scheduler.run_label first_scheduler);
  let expected =
    "Temporal future combinator received futures from different workflow executions"
  in
  match !observed with
  | Some result ->
      expect_error_message "await returns the real ownership error" expected
        result
  | None -> failwith "outer await never produced a result"

(** Confirms [both] continues observing the other future after one side fails. *)
let test_both_observes_sibling_after_error () =
  let scheduler = Scheduler.create () in
  let left, resolve_left =
    promise scheduler ~outside_error
  in
  let right, resolve_right =
    promise scheduler ~outside_error
  in
  let combined = Temporal.Future.both left right in
  resolve_left (Error (Temporal.Error.defect ~message:"left failed"));
  expect "sibling still pending" "blocked" (Scheduler.run_label scheduler);
  assert (not (Temporal.Future.is_ready combined));
  resolve_right (Ok 9);
  expect "both settled" "complete" (Scheduler.run_label scheduler);
  match Temporal.Future.peek combined with
  | Some result -> expect_error_message "left error retained" "left failed" result
  | None -> failwith "both remained pending"

(** Confirms shutdown releases paused fibers and ignores later results. *)
let test_shutdown_closes_pending_continuations () =
  let scheduler = Scheduler.create () in
  let future, resolve =
    promise scheduler ~outside_error:(fun () -> "outside")
  in
  Scheduler.spawn scheduler (fun () -> ignore (Temporal.Future.await future));
  expect "shutdown setup" "blocked" (Scheduler.run_label scheduler);
  Scheduler.shutdown scheduler;
  match resolve (Ok 1) with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "shutdown future remained resolvable"

(** Resolving a future while the scheduler is idle enqueues continue thunks.
    Shutdown must discontinue those thunks rather than [Queue.clear] them, so
    [Fun.protect] cleanups still run. *)
let test_shutdown_discontinues_queued_ready_continuations () =
  let scheduler = Scheduler.create () in
  let future, resolve =
    promise scheduler ~outside_error:(fun () -> "outside")
  in
  let cleaned = ref false in
  Scheduler.spawn scheduler (fun () ->
      Fun.protect
        ~finally:(fun () -> cleaned := true)
        (fun () -> ignore (Temporal.Future.await future)));
  expect "queue waiter" "blocked" (Scheduler.run_label scheduler);
  resolve (Ok 7);
  Scheduler.shutdown scheduler;
  if not !cleaned then
    failwith "queued ready continuation was dropped without discontinue"

(** Every waiter parked on a future must be discontinued when its scheduler
    shuts down. This guards the terminal invariant that no captured workflow
    continuation remains stranded when a pending future is closed. *)
let test_shutdown_discontinues_all_pending_continuations () =
  let scheduler = Scheduler.create () in
  let future, _resolve = promise scheduler ~outside_error in
  let cleanup_count = ref 0 in
  let spawn_waiter () =
    Scheduler.spawn scheduler (fun () ->
        Fun.protect
          ~finally:(fun () -> incr cleanup_count)
          (fun () -> ignore (Temporal.Future.await future)))
  in
  spawn_waiter ();
  spawn_waiter ();
  spawn_waiter ();
  expect "multiple cleanup setup" "blocked" (Scheduler.run_label scheduler);
  Scheduler.shutdown scheduler;
  expect "all pending cleanup count" 3 !cleanup_count

(** A resolved future should not remain reachable solely through the scheduler
    teardown ledger while the scheduler itself remains alive. This helper
    returns the live scheduler and only a weak reference after dropping the
    future and resolver, so the major collection below can detect accidental
    retention of the resolved payload. *)
let test_settled_future_releases_payload () =
  let settled_scheduler () =
    let weak = Weak.create 1 in
    let payload = Bytes.create 1024 in
    Weak.set weak 0 (Some payload);
    let scheduler = Scheduler.create () in
    let _future, resolve = promise scheduler ~outside_error in
    resolve (Ok payload);
    scheduler, weak
  in
  let scheduler, weak = settled_scheduler () in
  Gc.full_major ();
  ignore (Sys.opaque_identity scheduler);
  if Option.is_some (Weak.get weak 0) then
    failwith "settled future retained its payload through scheduler teardown"

(** A shutdown requested by one running thunk must not execute a later root
    thunk that was already admitted. The owner still drains the queue so
    future continuations can discontinue themselves instead of being dropped. *)
let test_shutdown_skips_queued_root_thunks () =
  let scheduler = Scheduler.create () in
  let ran_after_shutdown = ref false in
  Scheduler.spawn scheduler (fun () -> Scheduler.shutdown scheduler);
  Scheduler.spawn scheduler (fun () -> ran_after_shutdown := true);
  expect "shutdown drain" "complete" (Scheduler.run_label scheduler);
  if !ran_after_shutdown then
    failwith "queued root thunk ran after scheduler shutdown"

(** A derived future observer queued by a running thunk must become inert when
    a later thunk shuts down the scheduler before that observer is drained.
    Otherwise it could resolve a derived future that shutdown has already
    closed and turn normal teardown into a spurious scheduler defect. *)
let test_shutdown_skips_queued_observers () =
  let scheduler = Scheduler.create () in
  let source, resolve = promise scheduler ~outside_error:(fun () -> "outside") in
  let observer_ran = ref false in
  let derived =
    Temporal.Future.map
      (fun value ->
        observer_ran := true;
        value + 1)
      source
  in
  Scheduler.spawn scheduler (fun () -> resolve (Ok 1));
  Scheduler.spawn scheduler (fun () -> Scheduler.shutdown scheduler);
  expect "observer shutdown drain" "complete" (Scheduler.run_label scheduler);
  if !observer_ran then
    failwith "queued observer ran after scheduler shutdown";
  if Temporal.Future.is_ready derived then
    failwith "derived future became ready after scheduler shutdown"

(** A source resolved before the scheduler starts can queue its runtime
    observer ahead of a shutdown root. That observer may then queue callbacks
    for a second public derived future after the shutdown root is already in
    the queue; both mapper and observer callbacks must be inert when drained. *)
let test_shutdown_skips_public_derived_callbacks () =
  let scheduler = Scheduler.create () in
  let source, resolve = promise scheduler ~outside_error:(fun () -> "outside") in
  let derived = Temporal.Future.map (fun value -> value + 1) source in
  let mapper_ran = ref false in
  let observer_ran = ref false in
  let downstream =
    Temporal.Future.map
      (fun value ->
        mapper_ran := true;
        value + 1)
      derived
  in
  Temporal_future_kernel.observe derived (fun _ -> observer_ran := true);
  resolve (Ok 1);
  Scheduler.spawn scheduler (fun () -> Scheduler.shutdown scheduler);
  expect "public derived shutdown drain" "complete"
    (Scheduler.run_label scheduler);
  if !mapper_ran then
    failwith "queued public mapper ran after scheduler shutdown";
  if !observer_ran then
    failwith "queued public observer ran after scheduler shutdown";
  if Temporal.Future.is_ready downstream then
    failwith "downstream public future became ready after scheduler shutdown"

(** Awaiting an unresolved public derived future after re-entrant shutdown must
    return its typed outside-owner error without allocating a new gate future
    on a scheduler that is already closed. *)
let test_shutdown_rejects_derived_await () =
  let scheduler = Scheduler.create () in
  let source, _resolve = promise scheduler ~outside_error in
  let derived = Temporal.Future.map Fun.id source in
  let observed = ref None in
  Scheduler.spawn scheduler (fun () ->
      Scheduler.shutdown scheduler;
      observed := Some (Temporal.Future.await derived));
  expect "derived await after shutdown" "complete"
    (Scheduler.run_label scheduler);
  match !observed with
  | Some (Error _) -> ()
  | Some (Ok _) -> failwith "derived await succeeded after scheduler shutdown"
  | None -> failwith "derived await was not evaluated"

(** Draining callbacks must not retain a history proportional to elapsed work.
    Measure live words after full collections, not heap capacity or allocations.
    Keeping the scheduler live after each measurement prevents a false pass
    caused by collecting the scheduler itself. The allowance is deliberately
    much larger than fixed bookkeeping, but smaller than one trace batch. *)
let test_drained_callbacks_release_memory () =
  let scheduler = Scheduler.create () in
  Scheduler.spawn scheduler (fun () -> ());
  expect "memory warmup" "complete" (Scheduler.run_label scheduler);
  Gc.full_major ();
  let baseline = (Gc.stat ()).live_words in
  for batch = 1 to 3 do
    for _ = 1 to 10_000 do
      Scheduler.spawn scheduler (fun () -> ());
      ignore (Scheduler.run scheduler)
    done;
    Gc.full_major ();
    let retained = (Gc.stat ()).live_words - baseline in
    ignore (Sys.opaque_identity scheduler);
    if retained > 4_096 then
      failwith (Printf.sprintf "drained callbacks retained %d words after batch %d"
        retained batch)
  done;
  Scheduler.shutdown scheduler

let () =
  test_drained_callbacks_release_memory ();
  test_fifo_resume ();
  test_double_resolution ();
  test_combinators ();
  test_outside_scheduler ();
  test_immediate_and_multiple_waiters ();
  test_map_error_and_owner_check ();
  test_resolved_combinator_callback_delivery ();
  test_aggregate_owner_errors_are_typed ();
  test_ready_like_parent_suspends_pending_outer ();
  test_all_order_and_errors ();
  test_all_empty ();
  test_race_order_and_loser ();
  test_race_releases_winner_from_pending_loser ();
  test_first_releases_winner_from_pending_loser ();
  test_first_completion_error ();
  test_mapper_defect_is_contained ();
  test_both_observes_sibling_after_error ();
  test_shutdown_closes_pending_continuations ();
  test_shutdown_discontinues_queued_ready_continuations ();
  test_shutdown_discontinues_all_pending_continuations ();
  test_settled_future_releases_payload ();
  test_shutdown_skips_queued_root_thunks ();
  test_shutdown_skips_queued_observers ();
  test_shutdown_skips_public_derived_callbacks ();
  test_shutdown_rejects_derived_await ()

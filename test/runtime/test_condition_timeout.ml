(** Tests for [Temporal.Condition.wait_until ?timeout] (#1134).

    The fixtures run the public condition API on a deterministic scheduler
    whose idle hook rechecks predicates exactly as the activation loop does,
    and play Core's part by calling [fire_timer] between drains. They assert
    the command buffer: a bounded wait starts one durable timer, a wait that
    is satisfied first cancels it, a wait that times out leaves the fired
    timer alone, a zero bound records nothing, and an unbounded wait still
    records nothing at all. *)

module Activation = Temporal_runtime.Activation
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Fails with the scenario label when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Checks a public error's stable category and message. *)
let expect_error label expected_kind expected_message = function
  | Some (Error error) ->
      expect (label ^ " kind") expected_kind (Temporal.Error.kind error);
      expect (label ^ " message") expected_message (Temporal.Error.message error)
  | Some (Ok ()) -> failwith (label ^ " unexpectedly succeeded")
  | None -> failwith (label ^ " is still waiting")

(** Drains the scheduler with the activation loop's idle recheck of
    condition predicates, standing in for one activation's fiber pass. *)
let drain context scheduler =
  Workflow_context_store.with_context context (fun () ->
      ignore
        (Scheduler.run
           ~on_idle:(fun () ->
             ignore (Workflow_context_store.notify_conditions context))
           scheduler))

(** Spawns a workflow fiber running [action] and drains the scheduler. *)
let run_in context scheduler action =
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context action);
  drain context scheduler

(** A ten-second bound; its value only matters for the timer command. *)
let ten_seconds = Temporal.Duration.of_seconds 10L

(** Extracts the sequence of the single ten-second [Start_timer]. *)
let started_timer label = function
  | [ Activation.Start_timer { seq; milliseconds = 10_000L } ] -> seq
  | _ -> failwith (label ^ ": expected one ten-second Start_timer")

(** Starts one bounded wait on [flag] and returns the result cell and the
    timer sequence it recorded. *)
let start_bounded_wait context scheduler flag =
  let result = ref None in
  run_in context scheduler (fun () ->
      result :=
        Some (Temporal.Condition.wait_until ~timeout:ten_seconds (fun () -> !flag)));
  expect "bounded wait suspends" None !result;
  let seq =
    started_timer "bounded wait" (Workflow_context_store.take_commands context)
  in
  (result, seq)

(** Satisfied before the bound: the waiter resumes with [Ok ()] and its
    still-pending timer is cancelled, so no timer is left in history. *)
let test_satisfied_cancels_timer () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let flag = ref false in
  let result, seq = start_bounded_wait context scheduler flag in
  run_in context scheduler (fun () -> flag := true);
  expect "satisfied wait" (Some (Ok ())) !result;
  expect "satisfied wait cancels its timer"
    [ Activation.Cancel_timer { seq } ]
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** The bound elapses first: the firing job wakes the waiter through the
    usual recheck and it returns the typed [`Timeout] error; the fired timer
    needs no cancellation. *)
let test_timeout () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let flag = ref false in
  let result, seq = start_bounded_wait context scheduler flag in
  expect "fire job" (Ok ()) (Workflow_context_store.fire_timer context ~seq);
  drain context scheduler;
  expect_error "timed-out wait" "timeout" "workflow condition timed out" !result;
  expect "timeout emits nothing more" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** The timer fires and the state changes in the same activation: the
    predicate is re-read on wake, so satisfaction wins deterministically, and
    the fired timer is not cancelled. *)
let test_satisfaction_beats_same_activation_timeout () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let flag = ref false in
  let result, seq = start_bounded_wait context scheduler flag in
  expect "fire job" (Ok ()) (Workflow_context_store.fire_timer context ~seq);
  run_in context scheduler (fun () -> flag := true);
  expect "satisfied in the timeout's activation" (Some (Ok ())) !result;
  expect "fired timer is not cancelled" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** A zero bound records no timer and checks the predicate once; an
    unbounded wait records nothing either, as before #1134. *)
let test_zero_and_unbounded_record_nothing () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let zero_false = ref None and zero_true = ref None and unbounded = ref None in
  run_in context scheduler (fun () ->
      let zero = Temporal.Duration.of_ms 0L in
      zero_false :=
        Some (Temporal.Condition.wait_until ~timeout:zero (fun () -> false));
      zero_true := Some (Temporal.Condition.wait_until ~timeout:zero (fun () -> true));
      unbounded := Some (Temporal.Condition.wait_until (fun () -> true)));
  expect_error "zero bound, false predicate" "timeout"
    "workflow condition timed out" !zero_false;
  expect "zero bound, true predicate" (Some (Ok ())) !zero_true;
  expect "unbounded" (Some (Ok ())) !unbounded;
  expect "no timer commands" [] (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** A predicate error ends a bounded wait with that error and still cancels
    the wait's own timer. *)
let test_predicate_error_cancels_timer () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let broken = ref false in
  let result = ref None in
  run_in context scheduler (fun () ->
      result :=
        Some
          (Temporal.Condition.wait_until_result ~timeout:ten_seconds (fun () ->
               if !broken then Error (Temporal.Error.codec ~message:"bad state")
               else Ok false)));
  let seq =
    started_timer "predicate error" (Workflow_context_store.take_commands context)
  in
  run_in context scheduler (fun () -> broken := true);
  expect_error "predicate error" "codec" "bad state" !result;
  expect "predicate error cancels the timer" [ Activation.Cancel_timer { seq } ]
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Outside a workflow a bounded wait reports the condition's own defect. *)
let test_outside_workflow () =
  expect_error "bounded wait outside a workflow" "defect"
    "Temporal.Condition.wait_until used outside a workflow execution"
    (Some (Temporal.Condition.wait_until ~timeout:ten_seconds (fun () -> true)))

(** Runs the bounded condition contract as one dune test. *)
let () =
  test_satisfied_cancels_timer ();
  test_timeout ();
  test_satisfaction_beats_same_activation_timeout ();
  test_zero_and_unbounded_record_nothing ();
  test_predicate_error_cancels_timer ();
  test_outside_workflow ()

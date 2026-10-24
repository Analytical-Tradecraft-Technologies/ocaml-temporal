(** Tests for durable-timer cancellation through [Temporal.Scope] (#1134).

    The fixtures install the private workflow context on a deterministic
    scheduler, start timers through the public [Workflow.start_sleep] and
    [Workflow.sleep], and play Core's part by calling [fire_timer] between
    scheduler runs. Each scenario asserts the exact command buffer, because
    the observable contract is that a pending timer owned by a cancelled scope
    produces exactly one [Cancel_timer], while a timer that already fired, a
    zero-duration sleep, or a scope cancelled before the start produce none.
    Core's replay matching of those commands is covered by the native
    activation tests and the live suite. *)

module Activation = Temporal_runtime.Activation
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Fails with the scenario label when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Checks a public error's stable category and message. *)
let expect_error label expected_kind expected_message = function
  | Error error ->
      expect (label ^ " kind") expected_kind (Temporal.Error.kind error);
      expect (label ^ " message") expected_message (Temporal.Error.message error)
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")

(** Same as {!expect_error} for the runtime's base error type, used where a
    test calls the private context store directly. *)
let expect_base_error label expected_message = function
  | Error error ->
      expect (label ^ " message") expected_message
        (Temporal_base.Error.message error)
  | Ok () -> failwith (label ^ " unexpectedly succeeded")

(** Runs [action] as a new workflow fiber with [context] installed and then
    drives the scheduler until it is idle, returning the run label. *)
let run_in context scheduler action =
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context action);
  Scheduler.run_label scheduler

(** A one-second timer duration; the value only matters for the command. *)
let one_second = Temporal.Duration.of_ms 1_000L

(** Extracts the sequence of the single [Start_timer] the scenario emitted,
    so tests do not depend on the context's sequence allocation scheme. *)
let started_timer label = function
  | [ Activation.Start_timer { seq; milliseconds = 1_000L } ] -> seq
  | _ -> failwith (label ^ ": expected exactly one one-second Start_timer")

(** Creates a scope from inside the workflow, failing the test otherwise. *)
let create_scope () =
  match Temporal.Scope.create () with
  | Ok scope -> scope
  | Error error -> failwith (Temporal.Error.message error)

(** Cancel-before-fire: a scope cancelled by a second fiber emits one
    [Cancel_timer] for the pending timer, resumes the waiter with the scope's
    cancellation, and settles the timer future itself as cancelled. Repeating
    the cancellation emits nothing more. *)
let test_cancel_before_fire () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let scope_ref = ref None in
  let timer_ref = ref None in
  let slept = ref None in
  expect "sleep run" "blocked"
    (run_in context scheduler (fun () ->
         let scope = create_scope () in
         scope_ref := Some scope;
         let timer = Temporal.Workflow.start_sleep ~scope one_second in
         timer_ref := Some timer;
         slept := Some (Temporal.Scope.await scope timer)));
  let seq =
    started_timer "cancel-before-fire"
      (Workflow_context_store.take_commands context)
  in
  let first = ref None in
  let second = ref None in
  let timer_result = ref None in
  ignore
    (run_in context scheduler (fun () ->
         let scope = Option.get !scope_ref in
         first := Some (Temporal.Scope.cancel scope);
         second := Some (Temporal.Scope.cancel scope);
         timer_result := Temporal.Future.peek (Option.get !timer_ref)));
  expect "first cancel" (Some (Ok ())) !first;
  expect "repeated cancel" (Some (Ok ())) !second;
  expect_error "awaiting caller" "cancelled" "workflow scope cancelled"
    (Option.get !slept);
  expect_error "timer future" "cancelled" "workflow timer cancelled"
    (Option.get !timer_result);
  expect "one Cancel_timer" [ Activation.Cancel_timer { seq } ]
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Direct-style [Workflow.sleep ~scope] is cancelled by another fiber in the
    same way and returns the scope's cancellation error. *)
let test_direct_sleep_cancelled () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let scope_ref = ref None in
  let slept = ref None in
  ignore
    (run_in context scheduler (fun () ->
         let scope = create_scope () in
         scope_ref := Some scope;
         slept := Some (Temporal.Workflow.sleep ~scope one_second)));
  let seq =
    started_timer "direct sleep" (Workflow_context_store.take_commands context)
  in
  ignore
    (run_in context scheduler (fun () ->
         ignore (Temporal.Scope.cancel (Option.get !scope_ref))));
  expect_error "direct sleep" "cancelled" "workflow scope cancelled"
    (Option.get !slept);
  expect "direct sleep Cancel_timer" [ Activation.Cancel_timer { seq } ]
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Cancel-after-fire in the same activation: Core's firing job is applied
    before the cancelling fiber runs, so the timer keeps its [Ok ()] and no
    [Cancel_timer] is emitted even though the scope is then cancelled. *)
let test_cancel_after_fire_same_activation () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let scope_ref = ref None in
  let slept = ref None in
  ignore
    (run_in context scheduler (fun () ->
         let scope = create_scope () in
         scope_ref := Some scope;
         slept := Some (Temporal.Workflow.sleep ~scope one_second)));
  let seq =
    started_timer "same activation" (Workflow_context_store.take_commands context)
  in
  expect "fire job" (Ok ()) (Workflow_context_store.fire_timer context ~seq);
  let cancelled = ref None in
  ignore
    (run_in context scheduler (fun () ->
         cancelled := Some (Temporal.Scope.cancel (Option.get !scope_ref))));
  expect "fired sleep" (Some (Ok ())) !slept;
  expect "cancel after fire" (Some (Ok ())) !cancelled;
  expect "no command after fire" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Cancel-after-fire in a later activation: once the waiter has resumed, the
    scope no longer retains the timer and cancelling it emits nothing. *)
let test_cancel_after_fire_later_activation () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let scope_ref = ref None in
  let timer_ref = ref None in
  ignore
    (run_in context scheduler (fun () ->
         let scope = create_scope () in
         scope_ref := Some scope;
         timer_ref := Some (Temporal.Workflow.start_sleep ~scope one_second)));
  let seq =
    started_timer "later activation"
      (Workflow_context_store.take_commands context)
  in
  expect "fire job" (Ok ()) (Workflow_context_store.fire_timer context ~seq);
  ignore (Scheduler.run_label scheduler);
  let cancelled = ref None in
  let timer_result = ref None in
  ignore
    (run_in context scheduler (fun () ->
         cancelled := Some (Temporal.Scope.cancel (Option.get !scope_ref));
         timer_result := Temporal.Future.peek (Option.get !timer_ref)));
  expect "later cancel" (Some (Ok ())) !cancelled;
  expect "fired timer result" (Some (Ok ())) !timer_result;
  expect "no command in later activation" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** The standard timeout race: [Scope.with_scope] around a race that the work
    wins cancels the losing timer during cleanup, leaving no dangling timer. *)
let test_race_winner_cancels_losing_timer () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let outcome = ref None in
  ignore
    (run_in context scheduler (fun () ->
         outcome :=
           Some
             (Temporal.Scope.with_scope (fun scope ->
                  (* A zero-duration sleep is a ready future without a
                     command, standing in for work that finishes first. *)
                  let work =
                    Temporal.Future.map
                      (fun () -> "done")
                      (Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 0L))
                  in
                  let timeout =
                    Temporal.Workflow.start_sleep ~scope one_second
                  in
                  match Temporal.Future.await (Temporal.Future.race work timeout) with
                  | Ok (Temporal.Future.Left value) -> Ok value
                  | Ok (Temporal.Future.Right ()) -> Ok "timed out"
                  | Error _ as error -> error))));
  expect "race outcome" (Some (Ok "done")) !outcome;
  begin match Workflow_context_store.take_commands context with
  | [ Activation.Start_timer { seq = started; _ };
      Activation.Cancel_timer { seq = cancelled } ] ->
      expect "cancelled the started timer" started cancelled
  | _ -> failwith "race did not emit Start_timer followed by Cancel_timer"
  end;
  Workflow_context_store.shutdown context

(** A scope that is already cancelled starts no timer: the future is ready
    with the scope's cancellation and the command buffer stays empty. A
    zero-duration scoped sleep likewise records nothing, before or after its
    scope is cancelled. *)
let test_no_command_without_pending_timer () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let cancelled_start = ref None in
  let zero = ref None in
  ignore
    (run_in context scheduler (fun () ->
         let zero_scope = create_scope () in
         zero :=
           Some (Temporal.Workflow.sleep ~scope:zero_scope (Temporal.Duration.of_ms 0L));
         ignore (Temporal.Scope.cancel zero_scope);
         let scope = create_scope () in
         ignore (Temporal.Scope.cancel scope);
         cancelled_start :=
           Temporal.Future.peek (Temporal.Workflow.start_sleep ~scope one_second)));
  expect "zero sleep" (Some (Ok ())) !zero;
  expect_error "cancelled scope start" "cancelled" "workflow scope cancelled"
    (Option.get !cancelled_start);
  expect "no timer commands" [] (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Unscoped sleeps keep their pre-#1134 command stream, so histories recorded
    before this change replay unchanged. *)
let test_unscoped_sleep_unchanged () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let slept = ref None in
  ignore
    (run_in context scheduler (fun () ->
         slept := Some (Temporal.Workflow.sleep one_second)));
  let seq =
    started_timer "unscoped" (Workflow_context_store.take_commands context)
  in
  expect "fire job" (Ok ()) (Workflow_context_store.fire_timer context ~seq);
  ignore (Scheduler.run_label scheduler);
  expect "unscoped sleep" (Some (Ok ())) !slept;
  expect "no further command" [] (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** The runtime cancellation operation is owner-checked: called outside its
    workflow it returns a typed defect, emits nothing, and leaves the timer
    pending, so the owner can still cancel it exactly once. *)
let test_cancel_outside_workflow () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let handle = ref None in
  ignore
    (run_in context scheduler (fun () ->
         handle := Some (Workflow_context_store.start_timer context 1_000L)));
  let future, cancel = Option.get !handle in
  let seq =
    started_timer "outside" (Workflow_context_store.take_commands context)
  in
  expect_base_error "foreign cancel" "timer cancellation used outside its workflow"
    (cancel ());
  expect "foreign cancel emitted nothing" []
    (Workflow_context_store.take_commands context);
  if Temporal_runtime.Future_store.is_ready future then
    failwith "foreign cancel settled the timer";
  let owner_cancel = ref None in
  ignore
    (run_in context scheduler (fun () -> owner_cancel := Some (cancel ())));
  expect "owner cancel" (Some (Ok ())) !owner_cancel;
  expect "owner Cancel_timer" [ Activation.Cancel_timer { seq } ]
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** A scoped sleep outside any workflow execution is a ready typed defect. *)
let test_scoped_sleep_outside_workflow () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let scope_ref = ref None in
  ignore
    (run_in context scheduler (fun () -> scope_ref := Some (create_scope ())));
  expect_error "detached scoped sleep" "defect"
    "workflow sleep used outside a workflow execution"
    (Temporal.Workflow.sleep ~scope:(Option.get !scope_ref) one_second);
  expect "detached sleep emitted nothing" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Executes the timer-cancellation contract as one dune test. *)
let () =
  test_cancel_before_fire ();
  test_direct_sleep_cancelled ();
  test_cancel_after_fire_same_activation ();
  test_cancel_after_fire_later_activation ();
  test_race_winner_cancels_losing_timer ();
  test_no_command_without_pending_timer ();
  test_unscoped_sleep_unchanged ();
  test_cancel_outside_workflow ();
  test_scoped_sleep_outside_workflow ()

(** Runtime tests for workflow-local conditions.

    The first scenarios exercise the package-private store through its
    workflow-context adapter, so registration, FIFO notification, typed
    predicate failures, and teardown can be inspected without a native worker.
    The final scenario drives [Execution.activate] with a synthetic signal: a
    signal handler mutates workflow-local state and the activation loop must
    recheck the condition before returning commands. *)

module Activation = Temporal_runtime.Activation
module Execution = Temporal_runtime.Execution
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Compares values while retaining the scenario name in a failure. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Checks a base error's stable category and message. *)
let expect_base_error label expected_kind expected_message = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      expect (label ^ " kind") expected_kind
        (Temporal_base.Error.kind error);
      expect (label ^ " message") expected_message
        (Temporal_base.Error.message error)

(** Creates a scheduler-owned context used by the direct condition-store
    scenarios.  The caller is responsible for shutdown after each test. *)
let context () =
  let scheduler = Scheduler.create () in
  (scheduler, Workflow_context_store.create scheduler)

(** Verifies that an already true predicate returns immediately and does not
    leave the scheduler blocked on a private notification future. *)
let test_immediate_success () =
  let scheduler, context = context () in
  let result = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          result :=
            Some
              (Workflow_context_store.wait_until context ~predicate:(fun () -> Ok true))));
  expect "immediate condition status" "complete"
    (Scheduler.run_label scheduler);
  expect "immediate condition result" (Some (Ok ())) !result;
  Workflow_context_store.shutdown context

(** Verifies that a false predicate suspends one fiber, then wakes it only after
    [notify] observes the state change. *)
let test_wait_and_notify () =
  let scheduler, context = context () in
  let ready = ref false in
  let evaluations = ref 0 in
  let result = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          result :=
            Some
              (Workflow_context_store.wait_until context ~predicate:(fun () ->
                   incr evaluations;
                   Ok !ready))));
  expect "false condition status" "blocked" (Scheduler.run_label scheduler);
  expect "false condition result" None !result;
  expect "false condition initial evaluation count" 1 !evaluations;
  ready := true;
  expect "condition notification" true
    (Workflow_context_store.notify_conditions context);
  expect "condition notification evaluation count" 2 !evaluations;
  expect "woken condition status" "complete"
    (Scheduler.run_label scheduler);
  expect "woken condition result" (Some (Ok ())) !result;
  Workflow_context_store.shutdown context

(** Verifies that multiple satisfied predicates resume in registration order,
    regardless of the reversed list used for constant-time registration. *)
let test_fifo_notification () =
  let scheduler, context = context () in
  let ready = ref false in
  let seen = ref [] in
  let wait label =
    Scheduler.spawn scheduler (fun () ->
        Workflow_context_store.with_context context (fun () ->
            match
              Workflow_context_store.wait_until context ~predicate:(fun () -> Ok !ready)
            with
            | Ok () -> seen := label :: !seen
            | Error error ->
                failwith (Temporal_base.Error.message error)))
  in
  wait "first";
  wait "second";
  expect "FIFO initial status" "blocked" (Scheduler.run_label scheduler);
  ready := true;
  expect "FIFO notification" true
    (Workflow_context_store.notify_conditions context);
  expect "FIFO resumed status" "complete" (Scheduler.run_label scheduler);
  expect "FIFO waiter order" [ "first"; "second" ] (List.rev !seen);
  Workflow_context_store.shutdown context

(** Verifies that an explicit predicate error settles the wait as a typed value
    and does not retain a waiter for a later activation. *)
let test_predicate_error () =
  let scheduler, context = context () in
  let result = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          result :=
            Some
              (Workflow_context_store.wait_until context ~predicate:(fun () ->
                   Error
                     (Temporal_base.Error.defect
                        ~message:"predicate rejected state")))));
  expect "predicate error status" "complete" (Scheduler.run_label scheduler);
  expect_base_error "predicate error" "defect" "predicate rejected state"
    (Option.get !result);
  expect "predicate error notification" false
    (Workflow_context_store.notify_conditions context);
  Workflow_context_store.shutdown context

(** Verifies that an exception from a predicate is converted to a typed defect
    rather than escaping through the scheduler. *)
let test_predicate_exception () =
  let scheduler, context = context () in
  let result = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          result :=
            Some
              (Workflow_context_store.wait_until context ~predicate:(fun () ->
                   raise Exit))));
  expect "predicate exception status" "complete"
    (Scheduler.run_label scheduler);
  match Option.get !result with
  | Ok () -> failwith "predicate exception unexpectedly succeeded"
  | Error error ->
      expect "predicate exception kind" "defect"
        (Temporal_base.Error.kind error);
      if
        not
          (String.starts_with ~prefix:"Temporal condition predicate raised:"
             (Temporal_base.Error.message error))
      then failwith "predicate exception message was not contextual";
  Workflow_context_store.shutdown context

(** Verifies that teardown drops predicates and queued continuations.  A later
    state change cannot resurrect a condition after eviction or completion. *)
let test_teardown_removes_waiter () =
  let scheduler, context = context () in
  let evaluations = ref 0 in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          ignore
            (Workflow_context_store.wait_until context ~predicate:(fun () ->
                 incr evaluations;
                 Ok false))));
  expect "teardown setup status" "blocked" (Scheduler.run_label scheduler);
  let before_shutdown = !evaluations in
  Workflow_context_store.shutdown context;
  expect "teardown notification" false
    (Workflow_context_store.notify_conditions context);
  expect "teardown predicate count" before_shutdown !evaluations;
  (try
     ignore (Scheduler.run_label scheduler);
     failwith "teardown scheduler unexpectedly remained active"
   with Invalid_argument message ->
     expect "teardown scheduler error" "Temporal scheduler is shut down"
       message)

(** Runs one workflow whose condition is satisfied by a signal handler.  This
    proves the execution loop's post-drain notification rather than calling
    [notify] manually from the test. *)
let test_activation_rechecks_after_signal_mutation () =
  let ready = ref false in
  let implementation () =
    match Workflow_context_store.current () with
    | None ->
        Error
          (Temporal_base.Error.defect
             ~message:"condition test lost its workflow context")
    | Some context ->
        Result.map (fun () -> "condition satisfied")
          (Workflow_context_store.wait_until context ~predicate:(fun () -> Ok !ready))
  in
  let definition =
    Temporal_base.Definition.make ~name:"condition-test"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.string
      ~implementation:(Some implementation)
  in
  let handler =
    Execution.make_signal_handler ~name:"set-ready" ~dispatch:(fun _signal ->
        ready := true;
        Ok ())
  in
  let execution =
    Execution.start ~signal_handlers:[ handler ] definition ()
  in
  expect "condition activation starts blocked" []
    (Execution.activate execution [ Activation.Start_workflow ]);
  let commands =
    Execution.activate execution
      [ Activation.Signal_workflow
          {
            signal_name = "set-ready";
            input = [];
            identity = "test";
            headers = [];
          } ]
  in
  match commands with
  | [ Activation.Complete_workflow payload ] -> (
      match Temporal_base.Codec.decode Temporal_base.Codec.string payload with
      | Ok value -> expect "condition activation result" "condition satisfied" value
      | Error error ->
          failwith (Temporal_base.Error.message error))
  | _ -> failwith "signal mutation did not complete the condition workflow"

(** Runs a terminal signal in the same activation that could otherwise satisfy
    a parked condition.  [terminate] represents either continue-as-new or a
    failure command.  The condition continuation must not be resumed after
    that command is buffered: the context is already sealed, and running user
    code after a terminal transition would make the activation order depend on
    an implementation detail of the scheduler drain. *)
let run_terminal_condition_regression ~label ~terminate ~expect_command =
  let ready = ref false in
  let resumed = ref false in
  let implementation () =
    match Workflow_context_store.current () with
    | None ->
        Error
          (Temporal_base.Error.defect
             ~message:(label ^ " lost its workflow context"))
    | Some context ->
        Result.map
          (fun () ->
            resumed := true;
            "condition continuation ran")
          (Workflow_context_store.wait_until context ~predicate:(fun () ->
               Ok !ready))
  in
  let definition =
    Temporal_base.Definition.make ~name:(label ^ "-condition-terminal")
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.string
      ~implementation:(Some implementation)
  in
  let handler =
    Execution.make_signal_handler ~name:"terminate" ~dispatch:(fun _signal ->
        ready := true;
        match Workflow_context_store.current () with
        | None ->
            Error
              (Temporal_base.Error.defect
                 ~message:(label ^ " terminal handler lost its context"))
        | Some context ->
            terminate context;
            Ok ())
  in
  let execution =
    Execution.start ~signal_handlers:[ handler ] definition ()
  in
  expect (label ^ " starts blocked") []
    (Execution.activate execution [ Activation.Start_workflow ]);
  let commands =
    Execution.activate execution
      [ Activation.Signal_workflow
          {
            signal_name = "terminate";
            input = [];
            identity = "test";
            headers = [];
          } ]
  in
  expect_command commands;
  if !resumed then
    failwith (label ^ " resumed a condition after its terminal command");
  expect (label ^ " remains terminal") []
    (Execution.activate execution [ Activation.Start_workflow ])

(** A continue-as-new command must suppress condition rechecking in the same
    activation, even though its [terminate] path sets only the context's
    buffered-terminal state until [Execution.activate] finalizes the command. *)
let test_condition_skips_continue_as_new_terminal () =
  let successor_input : Temporal_base.Codec.payload =
    { metadata = []; data = Bytes.empty }
  in
  run_terminal_condition_regression ~label:"continue-as-new"
    ~terminate:(fun context ->
      let continue : unit -> unit =
        Workflow_context_store.continue_as_new context
          ~workflow_type:"condition-successor" ~input:successor_input
      in
      continue ())
    ~expect_command:(function
      | [ Activation.Continue_as_new { workflow_type; _ } ] ->
          expect "continue-as-new successor" "condition-successor" workflow_type
      | commands ->
          failwith
            ("continue-as-new expected one terminal command, got "
            ^ string_of_int (List.length commands)))

(** A workflow failure has the same buffered-terminal lifetime as
    continue-as-new and must likewise prevent a previously parked condition
    from being resumed by the terminal signal's state mutation. *)
let test_condition_skips_failure_terminal () =
  let failure = Temporal_base.Error.make ~category:`Workflow ~message:"terminal signal failure" () in
  run_terminal_condition_regression ~label:"failure"
    ~terminate:(fun context ->
      let fail : unit -> unit =
        Workflow_context_store.terminate context (Activation.Fail_workflow failure)
      in
      fail ())
    ~expect_command:(function
      | [ Activation.Fail_workflow error ] ->
          expect "failure message" "terminal signal failure"
            (Temporal_base.Error.message error)
      | commands ->
          failwith
            ("failure expected one terminal command, got "
            ^ string_of_int (List.length commands)))

(** Executes every deterministic condition-store and activation scenario. *)
let () =
  test_immediate_success ();
  test_wait_and_notify ();
  test_fifo_notification ();
  test_predicate_error ();
  test_predicate_exception ();
  test_teardown_removes_waiter ();
  test_activation_rechecks_after_signal_mutation ();
  test_condition_skips_continue_as_new_terminal ();
  test_condition_skips_failure_terminal ()

(** Deterministic coverage for public external-workflow operations.

    The test installs the same execution context used by workflow activation
    tests, then observes the public futures and private command buffer. This
    keeps the assertion independent of a Temporal server while proving the
    boundary contract that live acceptance tests cannot isolate: command
    sequence allocation, typed completion, operation ownership, and validation
    before command emission. *)

module Activation = Temporal_runtime.Activation
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Copies a public payload into the private representation carried by runtime
    commands, including its mutable byte buffer. *)
let private_payload (payload : Temporal.Payload.t) :
    Temporal_base.Codec.payload =
  {
    Temporal_base.Payload.metadata = List.map Fun.id payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Compares two values and reports which lifecycle assertion diverged. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Checks that a private resolver rejected a malformed operation identity. *)
let expect_bridge_error label = function
  | Error error ->
      expect (label ^ " category") "bridge" (Temporal_base.Error.kind error)
  | Ok () -> failwith (label ^ " unexpectedly succeeded")

(** Checks the public error preserved by a ready future. *)
let expect_public_error label expected_kind expected_message = function
  | Some (Error error) ->
      expect (label ^ " category") expected_kind (Temporal.Error.kind error);
      expect (label ^ " message") expected_message (Temporal.Error.message error)
  | Some (Ok ()) -> failwith (label ^ " unexpectedly succeeded")
  | None -> failwith (label ^ " remained pending")

(** Encodes the signal payload once so the command assertion checks the exact
    bytes retained by the private runtime rather than only the signal metadata. *)
let encoded_signal_payload () =
  match Temporal.Codec.encode Temporal.Codec.string "ready" with
  | Ok payload -> private_payload payload
  | Error error -> failwith ("signal payload encoding failed: " ^ Temporal.Error.message error)

(** Proves that both public helpers allocate ordered commands and remain
    pending until the matching Core resolution arrives. Wrong-operation
    resolutions must leave the original future pending, while a duplicate after
    successful resolution must be rejected as a bridge defect. *)
let test_external_operation_lifecycle () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let signal =
    Temporal.Signal.define ~name:"refresh"
      ~input:Temporal.Codec.string
  in
  let signal_future =
    Workflow_context_store.with_context context (fun () ->
        Temporal.Workflow.signal_external_workflow
          ~workflow_id:"target-workflow" ~run_id:"run-42" ~signal ~input:"ready")
  in
  expect "signal starts pending" None (Temporal.Future.peek signal_future);
  let signal_payload = encoded_signal_payload () in
  expect "signal command"
    [ Activation.Signal_external_workflow
        {
          seq = 1L;
          workflow_id = "target-workflow";
          run_id = "run-42";
          signal_name = "refresh";
          input = [ signal_payload ];
          child_workflow_only = false;
          headers = [];
        } ]
    (Workflow_context_store.take_commands context);
  expect_bridge_error "signal resolved as cancellation"
    (Workflow_context_store.resolve_external_workflow context
       ~operation:`Cancel ~seq:1L (Ok ()));
  expect "signal remains pending after mismatched resolution" None
    (Temporal.Future.peek signal_future);
  let signal_failure =
    Temporal_base.Error.defect ~message:"target rejected signal"
  in
  expect "signal resolution"
    (Ok ())
    (Workflow_context_store.resolve_external_workflow context
       ~operation:`Signal ~seq:1L (Error signal_failure));
  expect_public_error "signal failure" "defect" "target rejected signal"
    (Temporal.Future.peek signal_future);
  expect_bridge_error "duplicate signal resolution"
    (Workflow_context_store.resolve_external_workflow context
       ~operation:`Signal ~seq:1L (Ok ()));

  let cancel_future =
    Workflow_context_store.with_context context (fun () ->
        Temporal.Workflow.cancel_external_workflow
          ~workflow_id:"target-workflow" ~run_id:"run-42"
          ~reason:"no longer needed")
  in
  expect "cancellation starts pending" None (Temporal.Future.peek cancel_future);
  expect "cancellation command"
    [ Activation.Request_cancel_external_workflow
        {
          seq = 2L;
          workflow_id = "target-workflow";
          run_id = "run-42";
          reason = "no longer needed";
        } ]
    (Workflow_context_store.take_commands context);
  expect "cancellation resolution"
    (Ok ())
    (Workflow_context_store.resolve_external_workflow context
       ~operation:`Cancel ~seq:2L (Ok ()));
  expect "cancellation completes" (Some (Ok ()))
    (Temporal.Future.peek cancel_future);
  Workflow_context_store.shutdown context

(** Proves that detached calls and invalid target/reason fields fail as typed
    defects without allocating a command sequence or mutating the buffer. *)
let test_external_operation_validation () =
  let signal =
    Temporal.Signal.define ~name:"refresh"
      ~input:Temporal.Codec.string
  in
  let detached =
    Temporal.Workflow.signal_external_workflow
      ~workflow_id:"target-workflow" ~run_id:"run-42" ~signal ~input:"ready"
  in
  expect_public_error "detached signal" "defect"
    "external workflow signal used outside a workflow execution"
    (Temporal.Future.peek detached);

  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let invalid_signal =
    Workflow_context_store.with_context context (fun () ->
        Temporal.Workflow.signal_external_workflow
          ~workflow_id:"" ~run_id:"run-42" ~signal ~input:"ready")
  in
  expect_public_error "empty external workflow ID" "defect"
    "external workflow id must not be empty"
    (Temporal.Future.peek invalid_signal);
  let invalid_cancellation =
    Workflow_context_store.with_context context (fun () ->
        Temporal.Workflow.cancel_external_workflow
          ~workflow_id:"target-workflow" ~run_id:"run-42" ~reason:"")
  in
  expect_public_error "empty cancellation reason" "defect"
    "external cancellation reason must not be empty"
    (Temporal.Future.peek invalid_cancellation);
  expect "invalid operations emit no commands" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Compares every public error field, including codec detail payloads. *)
let expect_error_view label expected = function
  | Error actual -> expect label (Temporal.Error.view expected) (Temporal.Error.view actual)
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")

(** Surfaces an assertion raised inside a workflow fiber with its own label. *)
let expect_complete scheduler =
  match Scheduler.run scheduler with
  | Scheduler.Complete -> ()
  | Scheduler.Failed exn -> raise exn
  | Scheduler.Blocked -> failwith "ready external-operation composition remained blocked"

(** Exercises every local validation branch and a structured codec failure. *)
let failure_cases () =
  let signal = Temporal.Signal.define ~name:"refresh" ~input:Temporal.Codec.unit in
  let signal_call workflow_id run_id () =
    Temporal.Workflow.signal_external_workflow ~workflow_id ~run_id ~signal ~input:()
  in
  let cancel_call workflow_id run_id reason () =
    Temporal.Workflow.cancel_external_workflow ~workflow_id ~run_id ~reason
  in
  let invalid_identifiers =
    [ "", " must not be empty";
      "bad\000id", " must not contain NUL";
      String.make 65_537 'x', " exceeds 65536 bytes";
      "\255", " must be valid UTF-8" ]
  in
  let targets =
    List.concat_map
      (fun (invalid, suffix) ->
        let expected = Temporal.Error.defect ~message:("external workflow id" ^ suffix) in
        let workflows =
          [ "signal workflow id" ^ suffix, signal_call invalid "", expected;
            "cancel workflow id" ^ suffix, cancel_call invalid "" "stop", expected ]
        in
        if invalid = "" then workflows
        else
          let expected = Temporal.Error.defect ~message:("external run id" ^ suffix) in
          workflows @
          [ "signal run id" ^ suffix, signal_call "target" invalid, expected;
            "cancel run id" ^ suffix, cancel_call "target" invalid "stop", expected ])
      invalid_identifiers
  in
  let reasons =
    List.map
      (fun (invalid, suffix) ->
        "cancellation reason" ^ suffix, cancel_call "target" "" invalid,
        Temporal.Error.defect ~message:("external cancellation reason" ^ suffix))
      invalid_identifiers
  in
  let codec_error =
    Temporal.Error.make ~category:`Codec ~message:"cannot encode signal"
      ~non_retryable:false
      ~details:[ { Temporal.Payload.metadata = [ "encoding", "binary/plain" ];
                   data = Bytes.of_string "codec detail" } ] ()
  in
  let failing_codec =
    Temporal.Codec.make ~encoding:"binary/plain"
      ~encode:(fun () -> Error codec_error) ~decode:(fun _ -> Ok ())
  in
  let failing_signal = Temporal.Signal.define ~name:"refresh" ~input:failing_codec in
  targets @ reasons @
  [ "signal codec error",
    (fun () -> Temporal.Workflow.signal_external_workflow
        ~workflow_id:"target" ~run_id:"" ~signal:failing_signal ~input:()),
    codec_error ]

(** Failed operations retain their scheduler in both input positions. Ready
    selection follows input order, and joining a pending timer still waits for
    that timer before returning the original error. No external command or
    durable sequence is allocated by local validation. *)
let test_failure_composition (label, failed_operation, expected) =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          let failed = failed_operation () in
          let ready = Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 0L) in
          let error name future =
            expect_error_view (label ^ " " ^ name) expected (Temporal.Future.await future)
          in
          error "direct" failed;
          error "both left" (Temporal.Future.both failed ready);
          error "both right" (Temporal.Future.both ready failed);
          error "all first" (Temporal.Future.all [ failed; ready ]);
          error "all last" (Temporal.Future.all [ ready; failed ]);
          error "race error first" (Temporal.Future.race failed ready);
          error "first error first" (Temporal.Future.first failed [ ready ]);
          expect (label ^ " race ready first") (Ok (Temporal.Future.Left ()))
            (Temporal.Future.await (Temporal.Future.race ready failed));
          expect (label ^ " first ready first") (Ok ())
            (Temporal.Future.await (Temporal.Future.first ready [ failed ]))));
  expect_complete scheduler;
  expect (label ^ " emits no command") [] (Workflow_context_store.take_commands context);
  let result = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          let failed = failed_operation () in
          let pending = Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 1L) in
          result := Some (Temporal.Future.await (Temporal.Future.both failed pending))));
  expect (label ^ " pending join") Scheduler.Blocked (Scheduler.run scheduler);
  expect (label ^ " join has not returned early") None !result;
  expect (label ^ " retains first durable sequence")
    [ Activation.Start_timer { seq = 1L; milliseconds = 1L } ]
    (Workflow_context_store.take_commands context);
  expect (label ^ " fires timer") (Ok ()) (Workflow_context_store.fire_timer context ~seq:1L);
  expect_complete scheduler;
  expect_error_view (label ^ " pending join error") expected (Option.get !result);
  Workflow_context_store.shutdown context

(** Detached calls keep inert ready errors, while real cross-workflow inputs
    remain invalid even when the leading operation failed before scheduling. *)
let test_failure_owner_boundaries () =
  let signal = Temporal.Signal.define ~name:"refresh" ~input:Temporal.Codec.unit in
  let failed_operation () =
    Temporal.Workflow.signal_external_workflow ~workflow_id:"" ~run_id:"" ~signal ~input:()
  in
  let detached_signal = failed_operation () in
  let detached_cancel =
    Temporal.Workflow.cancel_external_workflow ~workflow_id:"" ~run_id:"" ~reason:""
  in
  let detached_error =
    Temporal.Error.defect ~message:"external workflow signal used outside a workflow execution"
  in
  expect_error_view "detached signal" detached_error (Temporal.Future.await detached_signal);
  expect_error_view "detached cancellation"
    (Temporal.Error.defect ~message:"external workflow cancellation used outside a workflow execution")
    (Temporal.Future.await detached_cancel);
  expect_error_view "detached futures compose" detached_error
    (Temporal.Future.await (Temporal.Future.both detached_signal detached_cancel));
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let foreign_scheduler = Scheduler.create () in
  let foreign_context = Workflow_context_store.create foreign_scheduler in
  let foreign =
    Workflow_context_store.with_context foreign_context (fun () ->
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 0L))
  in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          let failed = failed_operation () in
          let expected =
            Temporal.Error.defect
              ~message:"Temporal future combinator received futures from different workflow executions"
          in
          List.iter (fun other ->
              expect_error_view "foreign both" expected
                (Temporal.Future.await (Temporal.Future.both failed other));
              expect_error_view "foreign all" expected
                (Temporal.Future.await (Temporal.Future.all [ failed; other ]));
              expect_error_view "foreign race" expected
                (Temporal.Future.await (Temporal.Future.race failed other));
              expect_error_view "foreign first" expected
                (Temporal.Future.await (Temporal.Future.first failed [ other ])))
            [ foreign; detached_signal ]));
  expect_complete scheduler;
  Workflow_context_store.shutdown context;
  Workflow_context_store.shutdown foreign_context

(** Runs the isolated external-operation lifecycle and validation scenarios. *)
let () =
  test_external_operation_lifecycle ();
  test_external_operation_validation ();
  List.iter test_failure_composition (failure_cases ());
  test_failure_owner_boundaries ()

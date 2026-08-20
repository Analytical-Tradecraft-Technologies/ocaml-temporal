module Protocol = Temporal_protocol.Workflow_protocol
module Activation = Temporal_runtime.Activation
module Execution = Temporal_runtime.Execution
module Native_execution = Temporal_runtime.Native_execution

(** Returns an owned protocol payload containing the supplied bytes. *)
let protocol_payload text : Protocol.payload =
  { Protocol.metadata = []; data = Bytes.of_string text }

(** Returns the same bytes in the base payload representation consumed by the
    private execution state machine. This test intentionally bypasses the
    public opaque adapter because it exercises the low-level translator. *)
let runtime_payload text : Temporal_base.Codec.payload =
  { Temporal_base.Payload.metadata = []; data = Bytes.of_string text }

(** Returns a unit payload in the base representation used by the translator. *)
let runtime_unit_payload () =
  match Temporal_base.Codec.encode Temporal_base.Codec.unit () with
  | Ok payload -> payload
  | Error error -> failwith (Temporal_base.Error.message error)

(** Copies a public payload into the private representation consumed by the
    execution fixture. The native execution tests otherwise stay below the
    public worker adapter, so this explicit copy makes ownership at that seam
    visible and prevents a test from accidentally retaining mutable caller
    bytes. *)
let base_payload (payload : Temporal.Payload.t) : Temporal_base.Codec.payload =
  {
    Temporal_base.Payload.metadata =
      List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Copies a private payload back into the public record required by a codec.
    Keeping this conversion next to [base_payload] makes the test's ownership
    boundary symmetric in both directions. *)
let public_payload (payload : Temporal_base.Codec.payload) : Temporal.Payload.t =
  {
    Temporal.Payload.metadata =
      List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Converts a public structured error for a private test definition, copying
    every detail payload before it enters the low-level runtime. *)
let base_error (error : Temporal.Error.t) : Temporal_base.Error.t =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ~category:view.category
    ~message:view.message ()

(** Adapts a public codec to the private codec record expected by
    [Temporal_runtime.Execution]. Codec failures are translated to the same
    base error type as the production public/private adapter. *)
let base_codec (codec : 'a Temporal.Codec.t) : 'a Temporal_base.Codec.t =
  Temporal_base.Codec.of_payload
    ~encode:(fun value ->
      match Temporal.Codec.encode codec value with
      | Ok payload -> Ok (base_payload payload)
      | Error error -> Error (base_error error))
    ~decode:(fun payload ->
      match Temporal.Codec.decode codec (public_payload payload) with
      | Ok value -> Ok value
      | Error error -> Error (base_error error))

(** Rebuilds a public workflow as the private definition accepted by the
    low-level execution fixture. This helper is test-only; production callers
    use the opaque public worker registration path instead. *)
let base_workflow (definition : ('input, 'output) Temporal.Workflow.t) =
  let implementation =
    Option.map
      (fun implementation input ->
        Result.map_error base_error (implementation input))
      (Temporal.Workflow.implementation definition)
  in
  Temporal_base.Definition.make ~name:(Temporal.Workflow.name definition)
    ~input:(base_codec (Temporal.Workflow.input definition))
    ~output:(base_codec (Temporal.Workflow.output definition)) ~implementation

(** Returns a protocol payload using the standard JSON string encoding. Native
    update tests use this instead of an untagged byte payload so the public
    codec can prove that the input and output encoding names match. *)
let json_protocol_payload text : Protocol.payload =
  {
    Protocol.metadata = [ ("encoding", Bytes.of_string "json/plain") ];
    data = Bytes.of_string (Yojson.Safe.to_string (`String text));
  }

(** Fails with the stable translation diagnostic when a result was expected. *)
let unwrap label = function
  | Ok value -> value
  | Error error ->
      let view = Native_execution.error_view error in
      failwith
        (Printf.sprintf "%s: %s at %s (%s)" label view.message view.path
           view.code)

(** Creates a valid ordinary activation envelope around an ordered job list. *)
let default_timestamp : Protocol.timestamp = { seconds = 1L; nanoseconds = 0 }

let activation ?(timestamp = Some default_timestamp) ?(is_replaying = true)
    ?metadata jobs :
    Protocol.activation =
  {
    run_id = "run-native-translation";
    timestamp;
    is_replaying;
    history_length = 7L;
    jobs;
    metadata;
  }

(** Checks that one translation error has the expected stable classification. *)
let expect_error_code label code = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      let view = Native_execution.error_view error in
      if not (String.equal view.code code) then
        failwith
          (Printf.sprintf "%s returned %s instead of %s" label view.code code)

(** Confirms initialization, replay metadata, and source ordering survive the
    conversion even though the runtime job algebra uses a small start marker. *)
let test_activation_metadata_and_order () =
  let metadata =
    Some
      {
        Protocol.available_internal_flags = [ 2L; 5L ];
        history_size_bytes = "42";
        continue_as_new_suggested = false;
        deployment_version_for_current_task = None;
        last_sdk_version = "test";
        suggest_continue_as_new_reasons = [];
        target_worker_deployment_version_changed = false;
      }
  in
  let value =
    activation ?metadata
      [
        Protocol.Initialize_workflow
          {
            workflow_id = "workflow-native";
            workflow_type = "native";
            arguments = [ protocol_payload "input" ];
            randomness_seed = "1";
            attempt = 1;
            context = None;
          };
        Protocol.Resolve_activity
          {
            seq = 3L;
            result = Protocol.Completed (Some (protocol_payload "ok"));
          };
        Protocol.Notify_has_patch { patch_id = "native.patch" };
        Protocol.Fire_timer { seq = 4L };
      ]
  in
  let translated =
    unwrap "activation translation"
      (Native_execution.translate_activation value)
  in
  if
    translated.jobs
    <> [
         Activation.Start_workflow;
         Activation.Resolve_activity
           { seq = 3L; result = Ok (runtime_payload "ok") };
         Activation.Notify_has_patch { patch_id = "native.patch" };
         Activation.Fire_timer { seq = 4L };
       ]
  then failwith "activation job order or payload conversion differed";
  if not (translated.is_replaying && translated.history_length = 7L) then
    failwith "replay metadata was not retained";
  if translated.metadata <> metadata then
    failwith "activation metadata was not retained";
  match translated.initialization with
  | Some initialization
    when initialization.workflow_id = "workflow-native"
         && initialization.workflow_type = "native"
         && initialization.arguments = [ protocol_payload "input" ] ->
      ()
  | _ -> failwith "initialization metadata was not retained"

(** Confirms that retained protocol payloads are independent of mutable input
    bytes. This guards the Domain/lifetime boundary used by a later worker loop:
    validation must not turn caller-owned buffers into shared replay state. *)
let test_retained_payloads_are_copied () =
  let argument = protocol_payload "input" in
  let header = protocol_payload "header" in
  let continuation_attributes = protocol_payload "attributes" in
  let continuation_detail = protocol_payload "detail" in
  let continuation_result = protocol_payload "result" in
  let continuation_failure : Protocol.failure =
    {
      message = "continued failure";
      source = "core";
      stack_trace = "stack";
      encoded_attributes = Some continuation_attributes;
      cause = None;
      info =
        Protocol.Application
          {
            type_name = "continued";
            non_retryable = false;
            details = [ continuation_detail ];
          };
    }
  in
  let context : Protocol.initialize_context =
    {
      headers = [ ("request", header) ];
      identity = "worker";
      parent_workflow = None;
      workflow_execution_timeout = None;
      workflow_run_timeout = None;
      workflow_task_timeout = None;
      first_execution_run_id = "first-run";
      start_time = None;
      root_workflow = None;
      priority = None;
      retry_policy = None;
      continuation =
        Some
          {
            continued_from_execution_run_id = "previous-run";
            initiator = Protocol.Continue_as_new_workflow;
            continued_failure = Some continuation_failure;
            last_completion_result = Some [ continuation_result ];
          };
    }
  in
  let translated =
    unwrap "payload copy translation"
      (Native_execution.translate_activation
         (activation
            [
              Protocol.Initialize_workflow
                {
                  workflow_id = "workflow-copy";
                  workflow_type = "copy";
                  arguments = [ argument ];
                  randomness_seed = "1";
                  attempt = 1;
                  context = Some context;
                };
            ]))
  in
  Bytes.set argument.data 0 'X';
  Bytes.set header.data 0 'Y';
  Bytes.set continuation_attributes.data 0 'X';
  Bytes.set continuation_detail.data 0 'X';
  Bytes.set continuation_result.data 0 'X';
  match translated.initialization with
  | Some { arguments = [ argument_copy ]; context = Some context_copy; _ } ->
      let header_copy =
        match context_copy.headers with
        | [ (_, payload) ] -> payload
        | _ -> failwith "copied context header disappeared"
      in
      if Bytes.to_string argument_copy.data <> "input" then
        failwith "initialization argument retained caller-owned bytes";
      if Bytes.to_string header_copy.data <> "header" then
        failwith "initialization context retained caller-owned bytes";
      begin match context_copy.continuation with
      | Some
          {
            continued_failure =
              Some
                {
                  encoded_attributes = Some attributes;
                  info = Protocol.Application { details = [ detail ]; _ };
                  _;
                };
            last_completion_result = Some [ result ];
            _;
          }
        when Bytes.to_string attributes.data = "attributes"
             && Bytes.to_string detail.data = "detail"
             && Bytes.to_string result.data = "result" ->
          ()
      | _ -> failwith "continuation metadata retained caller-owned bytes"
      end
  | _ -> failwith "initialization payload copies were not retained"

(** Confirms that application details survive the common Temporal shape where an
    activity failure wraps the application failure in [cause]. *)
let test_activity_failure_details_are_preserved () =
  let failure : Protocol.failure =
    {
      message = "activity wrapper";
      source = "core";
      stack_trace = "";
      encoded_attributes = None;
      cause =
        Some
          {
            message = "application failure";
            source = "worker";
            stack_trace = "";
            encoded_attributes = None;
            cause = None;
            info =
              Protocol.Application
                {
                  type_name = "mock_failure";
                  non_retryable = false;
                  details = [ protocol_payload "failure-details" ];
                };
          };
      info =
        Protocol.Activity
          {
            scheduled_event_id = 1L;
            started_event_id = 2L;
            identity = "worker";
            activity_type = "mock";
            activity_id = "activity-1";
            retry_state = Protocol.Timeout;
          };
    }
  in
  let translated =
    unwrap "activity failure translation"
      (Native_execution.translate_activation
         (activation
            [ Protocol.Resolve_activity { seq = 6L; result = Failed failure } ]))
  in
  match translated.jobs with
  | [ Activation.Resolve_activity { result = Error error; _ } ] ->
      let view = Temporal_base.Error.view error in
      begin match view.details with
      | [ payload ] when Bytes.to_string payload.data = "failure-details" -> ()
      | _ -> failwith "wrapped activity failure details were discarded"
      end
  | _ -> failwith "activity failure did not become a typed runtime error"

(** Confirms child start and terminal resolutions remain distinct jobs while
    sharing one Core sequence number. A repeated start or repeated terminal
    result is still rejected, but the valid two-stage pair is accepted. *)
let test_child_resolution_translation () =
  let translated =
    unwrap "child resolution translation"
      (Native_execution.translate_activation
         (activation
            [
              Protocol.Resolve_child_workflow_start
                {
                  seq = 8L;
                  result = Protocol.Child_start_succeeded "child-run";
                };
              Protocol.Resolve_child_workflow
                {
                  seq = 8L;
                  result = Protocol.Child_completed (Some (protocol_payload "ok"));
                };
            ]))
  in
  if
    translated.jobs
    <> [
         Activation.Resolve_child_workflow_start
           { seq = 8L; result = Ok "child-run" };
         Activation.Resolve_child_workflow
           { seq = 8L; result = Ok (runtime_payload "ok") };
       ]
  then failwith "child start and terminal jobs were not preserved in order";
  expect_error_code "duplicate child start sequence" "invalid_message"
    (Native_execution.translate_activation
       (activation
          [
            Protocol.Resolve_child_workflow_start
              { seq = 8L; result = Protocol.Child_start_succeeded "run-1" };
            Protocol.Resolve_child_workflow_start
              { seq = 8L; result = Protocol.Child_start_succeeded "run-2" };
          ]));
  expect_error_code "duplicate child terminal sequence" "invalid_message"
    (Native_execution.translate_activation
       (activation
          [
            Protocol.Resolve_child_workflow
              { seq = 8L; result = Protocol.Child_completed None };
            Protocol.Resolve_child_workflow
              { seq = 8L; result = Protocol.Child_completed None };
          ]))

(** Converts Core's local-activity backoff result into the dedicated runtime
    job and rounds a sub-millisecond fraction upward so the retry timer cannot
    fire earlier than the requested delay. *)
let test_local_activity_backoff_translation () =
  let translated =
    unwrap "local activity backoff translation"
      (Native_execution.translate_activation
         (activation
            [ Protocol.Resolve_activity
                {
                  seq = 9L;
                  result =
                    Protocol.Backoff
                      {
                        attempt = 2L;
                        backoff_duration = { seconds = 1L; nanoseconds = 1 };
                        original_schedule_time = Some default_timestamp;
                      };
                } ]))
  in
  match translated.jobs with
  | [ Activation.Resolve_local_activity_backoff
        { seq = 9L; attempt = 2L; backoff_milliseconds = 1_001L; _ } ] -> ()
  | _ -> failwith "local activity backoff did not become a retry job"

(** Confirms that a Core signal keeps its complete payload envelope while it
    crosses the semantic bridge. An execution without a matching registration
    fails closed instead of silently acknowledging an event that workflow code
    could not observe; this protects replay by making the missing handler an
    explicit non-retryable workflow failure. *)
let test_signal_workflow_translation_and_activation () =
  let signal_input = protocol_payload "signal-input" in
  let signal_header = protocol_payload "signal-header" in
  let signal =
    Protocol.Signal_workflow
      {
        signal_name = "order_updated";
        input = [ signal_input ];
        identity = "sender";
        headers = [ ("trace", signal_header) ];
      }
  in
  let translated =
    unwrap "signal translation"
      (Native_execution.translate_activation (activation [ signal ]))
  in
  begin match translated.jobs with
  | [
   Activation.Signal_workflow
     {
       signal_name = "order_updated";
       input = [ input ];
       identity = "sender";
       headers = [ ("trace", header) ];
     } ] ->
      if input <> runtime_payload "signal-input" then
        failwith "signal input payload was changed during translation";
      if header <> runtime_payload "signal-header" then
        failwith "signal header payload was changed during translation"
  | _ -> failwith "signal activation did not retain its complete runtime job"
  end;
  let workflow =
    Temporal_base.Definition.make ~name:"native_signal"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Ok ()))
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "signal activation"
      (Native_execution.activate execution (activation [ signal ]))
  in
  begin match completion.commands with
  | [ Protocol.Fail_workflow
        {
          failure =
            {
              message;
              info = Protocol.Application { non_retryable = true; _ };
              _;
            };
        } ]
    when String.equal message "unhandled workflow signal: order_updated" ->
      ()
  | _ -> failwith "unhandled signal did not fail the workflow explicitly"
  end

(** Proves the native query slice preserves query identity and executes the
    handler synchronously without running the workflow scheduler. A second
    handler demonstrates that non-empty arguments become a failed query
    result, not a dropped field or workflow failure. *)
let test_query_workflow_translation_and_activation () =
  let query_protocol_payload text = protocol_payload text in
  let query_runtime_payload text = runtime_payload text in
  let query_job ?(arguments = []) ?(headers = []) query_id query_type =
    Protocol.Query_workflow { query_id; query_type; arguments; headers }
  in
  let translated =
    unwrap "query translation"
      (Native_execution.translate_activation
         (activation
            [
              query_job
                ~headers:[ ("trace", query_protocol_payload "header") ]
                "legacy_query"
                "current-state";
            ]))
  in
  begin match translated.jobs with
  | [
   Activation.Query_workflow
     {
       query_id = "legacy_query";
       query_type = "current-state";
       arguments = [];
       headers = [ ("trace", header) ];
     } ] when header = query_runtime_payload "header" ->
      ()
  | _ -> failwith "query activation did not preserve ID, type, or headers"
  end;
  let workflow =
    Temporal_base.Definition.make ~name:"native_query" ~input:Temporal_base.Codec.unit
      ~output:Temporal_base.Codec.unit ~implementation:(Some (fun () -> Ok ()))
  in
  let handler =
    Execution.make_query_handler ~name:"current-state" ~dispatch:(fun query ->
        match query.arguments with
        | [] -> Ok (query_runtime_payload "answer")
        | _ ->
            Error
              (Temporal_base.Error.make ~non_retryable:true ~category:`Workflow
                 ~message:"query arguments are unsupported" ()))
  in
  let execution = Execution.start ~query_handlers:[ handler ] workflow () in
  let completion =
    unwrap "query activation"
      (Native_execution.activate execution
         (activation [ query_job "query-1" "current-state" ]))
  in
  begin match completion.commands with
  | [ Protocol.Query_result { query_id = "query-1"; result = Query_succeeded payload } ]
    when payload = { Protocol.metadata = []; data = Bytes.of_string "answer" } ->
      ()
  | _ -> failwith "query handler did not return a successful query result"
  end;
  let rejected =
    unwrap "query argument rejection"
      (Native_execution.activate execution
         (activation
            [ query_job
                ~arguments:[ query_protocol_payload "unexpected" ] "query-2"
                "current-state" ]))
  in
  begin match rejected.commands with
  | [ Protocol.Query_result { query_id = "query-2"; result = Query_failed failure } ]
    when Protocol.failure_non_retryable failure ->
      ()
  | _ -> failwith "query arguments were not returned as a failed query result"
  end;
  let query_activation =
    activation [ query_job "query-expected" "current-state" ]
  in
  let query_completion query_id =
    Protocol.
      {
        run_id = "run-native-translation";
        commands =
          [ Query_result
              { query_id; result = Query_succeeded (protocol_payload "answer") } ];
      }
  in
  expect_error_code "stray query result" "invalid_message"
    (Native_execution.validate_completion_for_activation
       (activation [ Protocol.Fire_timer { seq = 1L } ])
       (query_completion "query-expected"));
  expect_error_code "missing query result" "invalid_message"
    (Native_execution.validate_completion_for_activation query_activation
       { Protocol.run_id = "run-native-translation"; commands = [] });
  expect_error_code "mismatched query result" "invalid_message"
    (Native_execution.validate_completion_for_activation query_activation
       (query_completion "query-other"));
  expect_error_code "mixed query activation" "invalid_message"
    (Native_execution.validate_completion_for_activation
       (activation
          [ query_job "query-expected" "current-state";
            Protocol.Fire_timer { seq = 9L } ])
       (query_completion "query-expected"));
  expect_error_code "duplicate query activation ID" "invalid_message"
    (Native_execution.validate_completion_for_activation
       (activation
          [ query_job "query-expected" "current-state";
            query_job "query-expected" "current-state" ])
       (query_completion "query-expected"));
  expect_error_code "extra query result" "invalid_message"
    (Native_execution.validate_completion_for_activation query_activation
       {
         Protocol.run_id = "run-native-translation";
         commands =
           [ Query_result
               {
                 query_id = "query-expected";
                 result = Query_succeeded (protocol_payload "one");
               };
             Query_result
               {
                 query_id = "query-expected";
                 result = Query_succeeded (protocol_payload "two");
               } ];
       })

(** Proves a native update is translated with its Core correlation fields and
    dispatched through the scheduler boundary. The two jobs exercise
    both validator modes: replay asks the handler to skip validation, while a
    live-style job requests it. The runtime must emit one accepted response and
    one completed response for each update, preserving protocol IDs and output
    payload bytes in source order. *)
let test_update_workflow_translation_and_activation () =
  let validator_runs = ref [] in
  let workflow =
    Temporal_base.Definition.make ~name:"native_update"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Ok ()))
  in
  let handler =
    Execution.make_update_handler ~name:"set_status"
      ~dispatch:(fun ~run_validator ~on_validated update ->
        on_validated ();
        validator_runs := (update.id, run_validator) :: !validator_runs;
        match update.input with
        | [ payload ] -> (
            match Temporal_base.Codec.decode Temporal_base.Codec.string payload with
            | Error error -> Error error
            | Ok value ->
                Temporal_base.Codec.encode Temporal_base.Codec.string
                  (String.uppercase_ascii value))
        | _ ->
            Error
              (Temporal_base.Error.make ~non_retryable:true ~category:`Workflow
                 ~message:"update input arity was not one" ()))
  in
  let execution = Execution.start ~update_handlers:[ handler ] workflow () in
  let update ~id ~protocol_instance_id ~run_validator =
    Protocol.Do_update
      {
        id;
        protocol_instance_id;
        name = "set_status";
        input = [ json_protocol_payload "ready" ];
        headers = [ ("trace", json_protocol_payload "header") ];
        meta = { Protocol.identity = "client"; update_id = id };
        run_validator;
      }
  in
  let completion =
    unwrap "update activation"
      (Native_execution.activate execution
         (activation
            [ update ~id:"update-live" ~protocol_instance_id:"protocol-live"
                ~run_validator:true;
              update ~id:"update-replay" ~protocol_instance_id:"protocol-replay"
                ~run_validator:false ]))
  in
  let expected_payload = json_protocol_payload "READY" in
  begin match completion.commands with
  | [ Protocol.Update_response
        { protocol_instance_id = "protocol-live"; response = Update_accepted };
      Protocol.Update_response
        {
          protocol_instance_id = "protocol-live";
          response = Update_completed live_payload;
        };
      Protocol.Update_response
        { protocol_instance_id = "protocol-replay"; response = Update_accepted };
      Protocol.Update_response
        {
          protocol_instance_id = "protocol-replay";
          response = Update_completed replay_payload;
        } ]
    when live_payload = expected_payload && replay_payload = expected_payload ->
      ()
  | _ -> failwith "update handler did not emit ordered accepted/completed pairs"
  end;
  let observed = List.sort compare !validator_runs in
  if observed <> [ ("update-live", true); ("update-replay", false) ] then
    failwith "update validator replay flag was not forwarded exactly"

(** Proves that an update is queued behind a resolver that appeared earlier in
    the same activation. The workflow resumes from its first activity, records
    that progress, and parks on a second activity; the update handler must then
    observe that progress and a live workflow context. Inline dispatch would
    see [resumed = false] and would run outside [Workflow_context]. *)
let test_update_dispatch_preserves_activation_order () =
  let resumed = ref false in
  let handler_saw_context = ref false in
  let activity =
    Temporal.Activity.remote ~name:"native_ordering_activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_update_order"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let open Temporal.Result_syntax in
        let first = Temporal.Activity.start activity () in
        let* () = Temporal.Future.await first in
        resumed := true;
        let second = Temporal.Activity.start activity () in
        let* () = Temporal.Future.await second in
        Ok ())
  in
  let handler =
    Execution.make_update_handler ~name:"observe_order"
      ~dispatch:(fun ~run_validator:_ ~on_validated _update ->
        on_validated ();
        handler_saw_context := Temporal.Workflow_context.is_active ();
        Temporal_base.Codec.encode Temporal_base.Codec.string "handled")
  in
  let execution =
    Execution.start ~update_handlers:[ handler ] (base_workflow workflow) ()
  in
  let initial =
    unwrap "ordering workflow start"
      (Native_execution.activate execution
         (activation
            [ Protocol.Initialize_workflow
                {
                  workflow_id = "workflow-order";
                  workflow_type = "native_update_order";
                  arguments = [];
                  randomness_seed = "1";
                  attempt = 1;
                  context = None;
                } ]))
  in
  begin match initial.commands with
  | [ Protocol.Schedule_activity { seq = 1L; activity_type; _ } ]
    when String.equal activity_type "native_ordering_activity" ->
      ()
  | _ -> failwith "ordering workflow did not schedule its first activity"
  end;
  if !resumed then failwith "workflow resumed before its first activity resolved";
  let update =
    Protocol.Do_update
      {
        id = "update-order";
        protocol_instance_id = "protocol-order";
        name = "observe_order";
        input = [];
        headers = [];
        meta = { Protocol.identity = "client"; update_id = "update-order" };
        run_validator = true;
      }
  in
  let next =
    unwrap "ordering workflow resolution"
      (Native_execution.activate execution
         (activation
            [ Protocol.Resolve_activity { seq = 1L; result = Protocol.Completed None };
              update ]))
  in
  if not !resumed then
    failwith "resolver continuation did not run before the update handler";
  if not !handler_saw_context then
    failwith "update handler ran outside the workflow context";
  begin match next.commands with
  | [ Protocol.Schedule_activity { seq = 2L; _ };
      Protocol.Update_response
        { protocol_instance_id = "protocol-order"; response = Update_accepted };
      Protocol.Update_response
        {
          protocol_instance_id = "protocol-order";
          response = Update_completed _;
        } ] ->
      ()
  | _ -> failwith "resolver and update commands were not emitted in order"
  end

(** Verifies the two-phase update lifecycle. Validation acknowledgement is
    emitted in the activation that starts a handler, while the handler's
    completion is held until a future it owns resolves in a later activation.
    This is the regression test for the scheduler-owned pending-update map. *)
let test_update_handler_can_suspend () =
  let activity =
    Temporal.Activity.remote ~name:"suspended_update_activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"suspended_update_workflow"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let open Temporal.Result_syntax in
        let* () = Temporal.Future.await (Temporal.Activity.start activity ()) in
        Ok ())
  in
  let handler =
    Execution.make_update_handler ~name:"wait_for_activity"
      ~dispatch:(fun ~run_validator:_ ~on_validated _update ->
        on_validated ();
        match Temporal.Future.await (Temporal.Activity.start activity ()) with
        | Error error -> Error (base_error error)
        | Ok () ->
            Temporal_base.Codec.encode Temporal_base.Codec.string "finished")
  in
  let execution =
    Execution.start ~update_handlers:[ handler ] (base_workflow workflow) ()
  in
  let initial =
    unwrap "suspended update workflow start"
      (Native_execution.activate execution
         (activation
            [ Protocol.Initialize_workflow
                {
                  workflow_id = "suspended-update-workflow";
                  workflow_type = "suspended_update_workflow";
                  arguments = [];
                  randomness_seed = "1";
                  attempt = 1;
                  context = None;
                } ]))
  in
  begin match initial.commands with
  | [ Protocol.Schedule_activity { seq = 1L; _ } ] -> ()
  | _ -> failwith "suspended update workflow did not schedule its first activity"
  end;
  let waiting =
    unwrap "suspended update acknowledgement"
      (Native_execution.activate execution
         (activation
            [ Protocol.Do_update
                {
                  id = "suspended-update";
                  protocol_instance_id = "suspended-update-protocol";
                  name = "wait_for_activity";
                  input = [];
                  headers = [];
                  meta =
                    { Protocol.identity = "client"; update_id = "suspended-update" };
                  run_validator = true;
                } ]))
  in
  begin match waiting.commands with
  | [ Protocol.Update_response
        { protocol_instance_id = "suspended-update-protocol";
          response = Update_accepted };
      Protocol.Schedule_activity { seq = 2L; _ } ] ->
      ()
  | _ ->
      failwith
        "suspended update did not emit activity and acceptance before parking"
  end;
  let duplicate =
    unwrap "duplicate suspended update rejection"
      (Native_execution.activate execution
         (activation
            [ Protocol.Do_update
                {
                  id = "suspended-update-duplicate";
                  protocol_instance_id = "suspended-update-protocol";
                  name = "wait_for_activity";
                  input = [];
                  headers = [];
                  meta =
                    { Protocol.identity = "client";
                      update_id = "suspended-update-duplicate" };
                  run_validator = true;
                } ]))
  in
  begin match duplicate.commands with
  | [ Protocol.Update_response
        { protocol_instance_id = "suspended-update-protocol";
          response = Update_rejected _ } ] ->
      ()
  | _ -> failwith "duplicate pending update protocol ID was not rejected"
  end;
  let resumed =
    unwrap "suspended update completion"
      (Native_execution.activate execution
         (activation
            [ Protocol.Resolve_activity { seq = 2L; result = Protocol.Completed None } ]))
  in
  begin match resumed.commands with
  | [ Protocol.Update_response
        { protocol_instance_id = "suspended-update-protocol";
          response = Update_completed payload } ]
    when payload = json_protocol_payload "finished" ->
      ()
  | _ -> failwith "suspended update did not complete after its future resolved"
  end

(** Confirms that both malformed identity forms are rejected before a signal
    can become runtime state.  OCaml strings may contain arbitrary bytes, so
    this exercises the boundary that JSON received from Rust cannot express
    for invalid UTF-8 but an in-process caller could otherwise bypass. *)
let test_signal_identity_validation () =
  let signal identity =
    Protocol.Signal_workflow
      {
        signal_name = "order_updated";
        input = [];
        identity;
        headers = [];
      }
  in
  let expect_rejected label identity =
    match Native_execution.translate_activation (activation [ signal identity ]) with
    | Ok _ -> failwith (label ^ " signal identity was accepted")
    | Error error ->
        let view = Native_execution.error_view error in
        if not (String.equal view.code "invalid_message") then
          failwith (label ^ " returned the wrong error code");
        (* Invalid UTF-8 is rejected by the lower-level JSON foundation, which
           retains the exact array element and field location. *)
        if not (String.equal view.path "$.jobs[0].identity") then
          failwith (label ^ " returned the wrong error path")
  in
  expect_rejected "NUL" "sender\000";
  expect_rejected "invalid UTF-8" (String.make 1 (Char.chr 0xff))

(** Confirms cancellation and cache-removal facts are retained instead of being
    silently lost by marker-only runtime jobs. *)
let test_cancellation_and_eviction () =
  let cancelled =
    activation
      [
        Protocol.Cancel_workflow { reason = "operator requested cancellation" };
      ]
  in
  let translated =
    unwrap "cancellation translation"
      (Native_execution.translate_activation cancelled)
  in
  if translated.jobs <> [ Activation.Cancel_workflow ] then
    failwith "cancellation job was not translated";
  if translated.cancellation_reason <> Some "operator requested cancellation"
  then failwith "cancellation reason was discarded";
  let eviction =
    activation ~timestamp:None
      [
        Protocol.Remove_from_cache
          { message = "cache pressure"; reason = Protocol.Cache_full };
      ]
  in
  let translated =
    unwrap "eviction translation"
      (Native_execution.translate_activation eviction)
  in
  if translated.jobs <> [ Activation.Remove_from_cache ] then
    failwith "eviction marker was not translated";
  match translated.cache_removal with
  | Some
      {
        Native_execution.message = "cache pressure";
        reason = Protocol.Cache_full;
      } ->
      ()
  | _ -> failwith "eviction details were discarded"

(** A completed activation through the runtime produces the protocol's nullable
    result for the canonical unit payload. *)
let test_activate_terminal_completion () =
  let workflow =
    Temporal_base.Definition.make ~name:"native_terminal"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Ok ()))
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "terminal activation"
      (Native_execution.activate execution
         (activation
            [
              Protocol.Initialize_workflow
                {
                  workflow_id = "workflow-native";
                  workflow_type = "native_terminal";
                  arguments = [];
                  randomness_seed = "1";
                  attempt = 1;
                  context = None;
                };
            ]))
  in
  match completion.commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "unit workflow did not produce nullable protocol completion"

(** Proves that the native activation adapter installs Core's deterministic
    timestamp before the workflow implementation runs. The implementation
    reads the public [Temporal.Workflow.now] API and compares both integer
    components, so a missing or stale context timestamp produces a workflow
    failure rather than a false-positive completion. *)
let test_activate_installs_workflow_time () =
  let timestamp : Protocol.timestamp =
    { seconds = 123L; nanoseconds = 456_789_012 }
  in
  let workflow =
    Temporal_base.Definition.make ~name:"native_workflow_time"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:
        (Some
           (fun () ->
             match Temporal.Workflow.now () with
             | Error error ->
                 Error
                   (Temporal_base.Error.defect
                      ~message:
                        ("workflow clock was unavailable: "
                        ^ Temporal.Error.message error))
             | Ok instant
               when Int64.equal (Temporal.Time.seconds instant) timestamp.seconds
                    && Int.equal (Temporal.Time.nanoseconds instant)
                         timestamp.nanoseconds ->
                 Ok ()
             | Ok _ ->
                 Error
                   (Temporal_base.Error.defect
                      ~message:
                        "workflow clock did not match the activation timestamp")))
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "workflow time activation"
      (Native_execution.activate execution
         (activation ~timestamp:(Some timestamp)
            [
              Protocol.Initialize_workflow
                {
                  workflow_id = "workflow-time";
                  workflow_type = "native_workflow_time";
                  arguments = [];
                  randomness_seed = "1";
                  attempt = 1;
                  context = None;
                };
            ]))
  in
  match completion.commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | [ Protocol.Fail_workflow { failure } ] ->
      failwith ("workflow time implementation failed: " ^ failure.message)
  | _ -> failwith "workflow time activation did not complete with unit result"

(** Proves the native adapter installs task-local worker deployment metadata
    before workflow code runs. *)
let test_activate_installs_deployment_metadata () =
  let metadata =
    Some
      {
        Protocol.available_internal_flags = [];
        history_size_bytes = "0";
        continue_as_new_suggested = false;
        deployment_version_for_current_task =
          Some { deployment_name = "orders"; build_id = "build-42" };
        last_sdk_version = "test";
        suggest_continue_as_new_reasons = [];
        target_worker_deployment_version_changed = false;
      }
  in
  let observed = ref [] in
  let workflow =
    Temporal_base.Definition.make ~name:"native_workflow_deployment"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:
        (Some
           (fun () ->
             observed :=
               !observed @ [ Temporal.Workflow.current_deployment_version () ];
             match Temporal.Workflow.sleep (Temporal.Duration.of_ms 1L) with
             | Ok () ->
                 observed :=
                   !observed
                   @ [ Temporal.Workflow.current_deployment_version () ];
                 Ok ()
             | Error error ->
                 Error
                   (Temporal_base.Error.defect
                      ~message:(Temporal.Error.message error))))
  in
  let execution = Execution.start workflow () in
  let first_completion =
    unwrap "workflow deployment activation"
      (Native_execution.activate execution
         (activation ?metadata
            [ Protocol.Initialize_workflow
                {
                  workflow_id = "workflow-deployment";
                  workflow_type = "native_workflow_deployment";
                  arguments = [];
                  randomness_seed = "1";
                  attempt = 1;
                  context = None;
                } ]))
  in
  begin match !observed with
  | [ Some { Temporal.Workflow.deployment_name = "orders"; build_id = "build-42" } ] -> ()
  | _ -> failwith "native adapter did not install deployment metadata"
  end;
  let second_completion =
    unwrap "workflow deployment timer activation"
      (Native_execution.activate execution
         (activation ?metadata:None [ Protocol.Fire_timer { seq = 1L } ]))
  in
  begin match first_completion.commands with
  | [ Protocol.Start_timer { seq = 1L; _ } ] -> ()
  | _ -> failwith "deployment metadata workflow did not schedule its timer"
  end;
  begin match second_completion.commands, !observed with
  | [ Protocol.Complete_workflow { result = None } ],
    [ Some { Temporal.Workflow.deployment_name = "orders"; build_id = "build-42" }; None ] -> ()
  | _ -> failwith "native adapter retained stale deployment metadata"
  end

(** Proves the native adapter installs replay state and patch notifications
    before entering workflow code. The replayed body must take the new branch
    only because Core supplied the marker, then return the marker command in
    the same activation completion. *)
let test_activate_installs_workflow_patch_state () =
  let observed = ref None in
  let workflow =
    Temporal_base.Definition.make ~name:"native_workflow_patch"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:
        (Some
           (fun () ->
             observed := Some (Temporal.Workflow.patched ~id:"native.patch");
             Ok ()))
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "workflow patch activation"
      (Native_execution.activate execution
         (activation ~is_replaying:true
            [ Protocol.Initialize_workflow
                {
                  workflow_id = "workflow-patch";
                  workflow_type = "native_workflow_patch";
                  arguments = [];
                  randomness_seed = "1";
                  attempt = 1;
                  context = None;
                };
              Protocol.Notify_has_patch { patch_id = "native.patch" } ]))
  in
  if !observed <> Some true then
    failwith "native adapter did not install replay patch state before workflow code";
  match completion.commands with
  | [ Protocol.Set_patch_marker
        { patch_id = "native.patch"; deprecated = false };
      Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "native patch activation emitted unexpected commands"

(** Proves the native adapter preserves the deprecated marker bit emitted by
    the public lifecycle API. A history notification may precede deprecation:
    it selects replay state without fixing which marker mode new code emits. *)
let test_activate_emits_deprecated_workflow_patch () =
  let workflow =
    Temporal_base.Definition.make ~name:"native_workflow_patch_deprecation"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:
        (Some
           (fun () ->
             Temporal.Workflow.deprecate_patch ~id:"native.patch";
             Ok ()))
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "workflow patch deprecation activation"
      (Native_execution.activate execution
         (activation ~is_replaying:true
            [ Protocol.Initialize_workflow
                {
                  workflow_id = "workflow-patch-deprecation";
                  workflow_type = "native_workflow_patch_deprecation";
                  arguments = [];
                  randomness_seed = "1";
                  attempt = 1;
                  context = None;
                };
              Protocol.Notify_has_patch { patch_id = "native.patch" } ]))
  in
  match completion.commands with
  | [ Protocol.Set_patch_marker
        { patch_id = "native.patch"; deprecated = true };
      Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "native patch deprecation emitted unexpected commands"

(** Cache eviction acknowledges Core without running or emitting workflow
    commands, while retaining the exact run identity. *)
let test_activate_eviction_completion () =
  let workflow =
    Temporal_base.Definition.make ~name:"native_eviction"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Ok ()))
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "eviction activation"
      (Native_execution.activate execution
         (activation ~timestamp:None
            [
              Protocol.Remove_from_cache
                { message = "cache miss"; reason = Protocol.Cache_miss };
            ]))
  in
  if completion.run_id <> "run-native-translation" || completion.commands <> []
  then failwith "eviction completion did not preserve the empty acknowledgement"

(** Confirms timer conversion keeps exact millisecond ordering and rejects
    negative or out-of-range sequence values without exceptions. *)
let test_command_order_and_validation () =
  let commands =
    Native_execution.completion_of_commands ~run_id:"run-native-translation"
      [
        Activation.Start_timer { seq = 9L; milliseconds = 1_234L };
        Activation.Cancel_timer { seq = 9L };
        Activation.Cancel_workflow_execution;
      ]
  in
  let completion = unwrap "timer command conversion" commands in
  begin match completion.commands with
  | [
   Protocol.Start_timer
     {
       seq = 9L;
       start_to_fire_timeout = { seconds = 1L; nanoseconds = 234_000_000 };
     };
   Protocol.Cancel_timer { seq = 9L };
   Protocol.Cancel_workflow_execution;
  ] ->
      ()
  | _ -> failwith "command order or duration conversion differed"
  end;
  expect_error_code "negative timer" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Start_timer { seq = 1L; milliseconds = -1L }));
  expect_error_code "too-large sequence" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Cancel_timer { seq = 4_294_967_296L }))

  ;
  let continue_input = runtime_unit_payload () in
  let continue_completion =
    unwrap "continue-as-new command conversion"
      (Native_execution.completion_of_commands ~run_id:"run-native-translation"
         [
           Activation.Continue_as_new
             { workflow_type = "native"; input = continue_input };
         ])
  in
  match continue_completion.commands with
  | [ Protocol.Continue_as_new { workflow_type = "native"; input = [ _ ] } ] ->
      ()
  | _ -> failwith "continue-as-new command was not translated as terminal"

(** Activity commands retain every Core-required field through translation, and
    malformed timeout/identity options fail before a completion is returned. *)
let test_activity_command_translation_and_validation () =
  let input = runtime_unit_payload () in
  let command =
    Activation.Schedule_activity
      {
        seq = 1L;
        activity_id = "lookup-1";
        activity_type = "lookup";
        task_queue = "activities";
        arguments = [ input ];
        schedule_to_close_timeout = Some 30_000L;
        schedule_to_start_timeout = None;
        start_to_close_timeout = Some 10_000L;
        heartbeat_timeout = None;
        retry_policy = None;
        priority = None;
        cancellation_type = Activation.Wait_cancellation_completed;
        do_not_eagerly_execute = true;
      }
  in
  let translated =
    unwrap "activity command translation"
      (Native_execution.command_to_protocol command)
  in
  begin match translated with
  | Protocol.Schedule_activity
      {
        seq = 1L;
        activity_id = "lookup-1";
        activity_type = "lookup";
        task_queue = "activities";
        arguments = [ argument ];
        schedule_to_close_timeout =
          Some { seconds = 30L; nanoseconds = 0 };
        schedule_to_start_timeout = None;
        start_to_close_timeout = Some { seconds = 10L; nanoseconds = 0 };
        heartbeat_timeout = None;
        retry_policy = None;
        priority = None;
        cancellation_type = Protocol.Wait_cancellation_completed;
        do_not_eagerly_execute = true;
      }
    when argument =
      {
        Protocol.metadata = [ ("encoding", Bytes.of_string "binary/null") ];
        data = Bytes.empty;
      } ->
      ()
  | _ -> failwith "activity command fields were not preserved"
  end;
  expect_error_code "activity timeout requirement" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 2L;
            activity_id = "lookup-2";
            activity_type = "lookup";
            task_queue = "activities";
            arguments = [ input ];
            schedule_to_close_timeout = None;
            schedule_to_start_timeout = None;
            start_to_close_timeout = None;
            heartbeat_timeout = None;
            retry_policy = None;
            priority = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  expect_error_code "negative activity timeout" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 3L;
            activity_id = "lookup-3";
            activity_type = "lookup";
            task_queue = "activities";
            arguments = [ input ];
            schedule_to_close_timeout = None;
            schedule_to_start_timeout = None;
            start_to_close_timeout = Some (-1L);
            heartbeat_timeout = None;
            retry_policy = None;
            priority = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  expect_error_code "empty activity queue" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 4L;
            activity_id = "lookup-4";
            activity_type = "lookup";
            task_queue = "";
            arguments = [ input ];
            schedule_to_close_timeout = Some 1L;
            schedule_to_start_timeout = None;
            start_to_close_timeout = None;
            heartbeat_timeout = None;
            retry_policy = None;
            priority = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  expect_error_code "invalid UTF-8 activity queue" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 5L;
            activity_id = "lookup-5";
            activity_type = "lookup";
            task_queue = String.make 1 (Char.chr 0xff);
            arguments = [ input ];
            schedule_to_close_timeout = Some 1L;
            schedule_to_start_timeout = None;
            start_to_close_timeout = None;
            heartbeat_timeout = None;
            retry_policy = None;
            priority = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  expect_error_code "overlong activity ID" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 6L;
            activity_id = String.make 65_537 'x';
            activity_type = "lookup";
            task_queue = "activities";
            arguments = [ input ];
            schedule_to_close_timeout = Some 1L;
            schedule_to_start_timeout = None;
            start_to_close_timeout = None;
            heartbeat_timeout = None;
            retry_policy = None;
            priority = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  begin match
    unwrap "child command translation"
      (Native_execution.command_to_protocol
         (Activation.Start_child_workflow
            {
              seq = 2L;
              id = "child/1";
              name = "child";
              input;
              retry_policy = None;
              cancellation_type = Activation.Child_abandon;
            }))
  with
  | Protocol.Start_child_workflow
      {
        seq = 2L;
        workflow_id = "child/1";
        workflow_type = "child";
        input = [ child_input ];
        retry_policy = None;
        cancellation_type = Protocol.Child_abandon;
      }
    when child_input =
      {
        Protocol.metadata = [ ("encoding", Bytes.of_string "binary/null") ];
        data = Bytes.empty;
      } ->
      ()
  | _ -> failwith "child command fields were not preserved"
  end;
  begin match
    unwrap "child cancellation translation"
      (Native_execution.command_to_protocol
         (Activation.Cancel_child_workflow
            { seq = 2L; reason = "stop child" }))
  with
  | Protocol.Cancel_child_workflow { seq = 2L; reason = "stop child" } -> ()
  | _ -> failwith "child cancellation fields were not preserved"
  end;
  expect_error_code "empty child cancellation reason" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Cancel_child_workflow { seq = 2L; reason = "" }));
  let invalid_metadata : Temporal_base.Codec.payload =
    {
      Temporal_base.Payload.metadata = [ ("encoding", "\255") ];
      data = Bytes.empty;
    }
  in
  expect_error_code "binary metadata" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Complete_workflow invalid_metadata));
  let duplicate_metadata : Temporal_base.Codec.payload =
    {
      Temporal_base.Payload.metadata =
        [ ("encoding", "binary/plain"); ("encoding", "binary/plain") ];
      data = Bytes.of_string "payload";
    }
  in
  (match Temporal_base.Codec.decode Temporal_base.Codec.bytes duplicate_metadata with
  | Error _ -> ()
  | Ok _ -> failwith "base codec accepted duplicate payload metadata")

(** Duplicate sequence numbers in one protocol activation are rejected before an
    execution can be mutated. *)
let test_duplicate_sequence_rejected () =
  expect_error_code "duplicate sequence" "invalid_message"
    (Native_execution.translate_activation
       (activation
          [
            Protocol.Fire_timer { seq = 5L };
            Protocol.Resolve_activity
              { seq = 5L; result = Protocol.Completed None };
          ]))

(** An unknown sequence delivered to an already-created runtime becomes one
    typed terminal bridge failure, preserving Core's completion semantics. *)
let test_unknown_sequence_becomes_failure () =
  let workflow =
    Temporal_base.Definition.make ~name:"native_unknown"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Ok ()))
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "unknown sequence activation"
      (Native_execution.activate execution
         (activation [ Protocol.Fire_timer { seq = 99L } ]))
  in
  match completion.commands with
  | [
   Protocol.Fail_workflow { failure = { info = Protocol.Application _; _ } };
  ] ->
      ()
  | _ -> failwith "unknown sequence did not become a protocol failure"

(** Runs every native-execution translation assertion. *)
let () =
  test_activation_metadata_and_order ();
  test_retained_payloads_are_copied ();
  test_activity_failure_details_are_preserved ();
  test_child_resolution_translation ();
  test_local_activity_backoff_translation ();
  test_signal_workflow_translation_and_activation ();
  test_query_workflow_translation_and_activation ();
  test_update_workflow_translation_and_activation ();
  test_update_dispatch_preserves_activation_order ();
  test_update_handler_can_suspend ();
  test_signal_identity_validation ();
  test_cancellation_and_eviction ();
  test_activate_terminal_completion ();
  test_activate_installs_workflow_time ();
  test_activate_installs_deployment_metadata ();
  test_activate_installs_workflow_patch_state ();
  test_activate_emits_deprecated_workflow_patch ();
  test_activate_eviction_completion ();
  test_command_order_and_validation ();
  test_activity_command_translation_and_validation ();
  test_duplicate_sequence_rejected ();
  test_unknown_sequence_becomes_failure ()

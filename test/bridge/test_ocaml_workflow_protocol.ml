module Protocol = Temporal_protocol.Workflow_protocol

(** Reads a complete shared semantic fixture and closes its descriptor on all
    paths. *)
let read_file path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in_noerr channel)
    (fun () -> really_input_string channel (in_channel_length channel))

(** Resolves a fixture under Dune's copied workflow-protocol source tree. *)
let fixture parts =
  List.fold_left Filename.concat "fixtures/workflow-protocol" parts |> read_file

(** Extracts a successful protocol result for positive fixtures. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      let view = Protocol.error_view error in
      failwith (Printf.sprintf "%s at %s: %s" view.code view.path view.message)

(** Requires malformed input to fail without rendering its potentially
    sensitive payload bytes. *)
let require_error = function
  | Error _ -> ()
  | Ok _ -> failwith "expected workflow protocol validation to fail"

(** Requires malformed input to retain the strict decoder's precise safe path. *)
let check_error_path label expected = function
  | Error error ->
      let actual = (Protocol.error_view error).path in
      if not (String.equal expected actual) then
        failwith
          (Printf.sprintf "%s path differed: expected %s, got %s" label expected
             actual)
  | Ok _ -> failwith (label ^ " unexpectedly passed validation")

(** Requires a malformed activation to return the stable semantic error record.
    Checking the code, path, and message guards the OCaml side against
    accidentally leaking parser exceptions or returning an unstructured
    failure that the Rust bridge could not classify. *)
let require_invalid_activation name =
  match
    Protocol.decode_activation
      (fixture [ "invalid"; name ^ ".json" ])
  with
  | Error error ->
      let view = Protocol.error_view error in
      if view.code <> "invalid_message" then
        failwith (name ^ " returned a non-protocol error code");
      if view.path = "" then failwith (name ^ " omitted its validation path");
      if view.message = "" then failwith (name ^ " omitted its safe diagnostic")
  | Ok _ -> failwith (name ^ " was accepted")

(** Compares normalized JSON output exactly. *)
let check_string label expected actual =
  if not (String.equal expected actual) then failwith (label ^ " differed")

(** Finds a short ASCII marker in encoded JSON without making the protocol
    test depend on a particular JSON parser representation. The helper is only
    used for checking the canonical policy spelling emitted by OCaml; semantic
    equality is checked separately by decoding the complete document. *)
let contains_substring ~needle value =
  let needle_length = String.length needle in
  let value_length = String.length value in
  let rec search offset =
    if offset + needle_length > value_length then false
    else if String.equal (String.sub value offset needle_length) needle then true
    else search (offset + 1)
  in
  search 0

(** Verifies activation variants, ordering, and outgoing self-validation using
    the same fixtures as Rust. *)
let test_valid_activations () =
  List.iter
    (fun name ->
      let value =
        unwrap
          (Protocol.decode_activation
             (fixture [ "valid"; name ^ ".input.json" ]))
      in
      let expected =
        String.trim (fixture [ "valid"; name ^ ".normalized.json" ])
      in
      check_string name expected (unwrap (Protocol.encode_activation value));
      ignore (unwrap (Protocol.decode_activation expected)))
    [
      "activation";
      "eviction";
      "realistic-initialize";
      "child-initialize";
      "child-resolution";
      "child-cancellation-before-start";
      "patch-activation";
    ]

(** Proves continuation initialization metadata remains typed through the OCaml
    encoder and decoder, including a nested terminal failure and the previous
    run's completion payload. This exercises the fields Core sends only for a
    successor execution rather than relying solely on the ordinary null case
    in shared fixtures. *)
let test_continuation_initialize_metadata () =
  let initialized =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "realistic-initialize.input.json" ]))
  in
  let payload : Protocol.payload =
    {
      metadata = [ ("encoding", Bytes.of_string "json/plain") ];
      data = Bytes.of_string "\"previous-result\"";
    }
  in
  let continued_failure : Protocol.failure =
    {
      message = "previous run failed";
      source = "core";
      stack_trace = "stack";
      encoded_attributes = None;
      cause = None;
      info =
        Application
          { type_name = "example"; non_retryable = false; details = [] };
    }
  in
  let continuation : Protocol.continuation =
    {
      continued_from_execution_run_id = "previous-run";
      initiator = Continue_as_new_workflow;
      continued_failure = Some continued_failure;
      last_completion_result = Some [ payload ];
    }
  in
  let retry_policy : Protocol.retry_policy =
    {
      initial_interval = { seconds = 1L; nanoseconds = 0 };
      backoff_coefficient_bits = "4611686018427387904";
      maximum_interval = { seconds = 5L; nanoseconds = 0 };
      maximum_attempts = 2;
      non_retryable_error_types = [ "PermanentChildFailure" ];
    }
  in
  let jobs =
    List.map
      (function
        | Protocol.Initialize_workflow
            {
              workflow_id;
              workflow_type;
              arguments;
              randomness_seed;
              attempt;
              context = Some context;
            } ->
            Protocol.Initialize_workflow
              {
                workflow_id;
                workflow_type;
                arguments;
                randomness_seed;
                attempt;
                context =
                  Some
                    {
                      context with
                      retry_policy = Some retry_policy;
                      continuation = Some continuation;
                    };
              }
        | _ -> failwith "realistic fixture did not contain initialization")
      initialized.jobs
  in
  let value = { initialized with jobs } in
  let encoded = unwrap (Protocol.encode_activation value) in
  if
    not
      (contains_substring ~needle:"continued_from_execution_run_id" encoded)
  then failwith "continuation run identity was omitted";
  if not (contains_substring ~needle:"backoff_coefficient_bits" encoded) then
    failwith "workflow retry policy was omitted";
  let decoded = unwrap (Protocol.decode_activation encoded) in
  if decoded <> value then failwith "continuation metadata did not round-trip"

(** Verifies every first-slice completion command and command ordering. *)
let test_valid_completion () =
  let value =
    unwrap
      (Protocol.decode_completion
         (fixture [ "valid"; "completion.input.json" ]))
  in
  let expected =
    String.trim (fixture [ "valid"; "completion.normalized.json" ])
  in
  check_string "completion" expected (unwrap (Protocol.encode_completion value));
  ignore (unwrap (Protocol.decode_completion expected))

(** Proves local-activity scheduling keeps attempt, original schedule time,
    retry threshold, and cancellation policy across the strict JSON boundary. *)
let test_local_activity_protocol_slice () =
  let payload : Protocol.payload = { metadata = []; data = Bytes.of_string "input" } in
  let command : Protocol.completion_command =
    Protocol.Schedule_local_activity
      {
        seq = 4L;
        activity_id = "local-activity-4";
        activity_type = "normalize";
        attempt = 1L;
        original_schedule_time = Some { seconds = 10L; nanoseconds = 20 };
        arguments = [ payload ];
        schedule_to_close_timeout = Some { seconds = 30L; nanoseconds = 0 };
        schedule_to_start_timeout = None;
        start_to_close_timeout = Some { seconds = 5L; nanoseconds = 0 };
        retry_policy = None;
        local_retry_threshold = Some { seconds = 60L; nanoseconds = 0 };
        cancellation_type = Wait_cancellation_completed;
      }
  in
  let completion =
    {
      Protocol.run_id = "local-run";
      commands = [ command; Protocol.Request_cancel_local_activity { seq = 4L } ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  if unwrap (Protocol.decode_completion encoded) <> completion then
    failwith "local activity command did not round-trip";
  let bad_command =
    match command with
    | Protocol.Schedule_local_activity value ->
        Protocol.Schedule_local_activity { value with attempt = 0L }
    | _ -> assert false
  in
  require_error
    (Protocol.encode_completion { completion with commands = [ bad_command ] })

(** Proves that Core's long local-activity retry delay is represented as a
    nonterminal activation result without losing attempt or schedule-time
    provenance. *)
let test_local_activity_backoff_protocol_slice () =
  let backoff : Protocol.activity_resolution =
    Protocol.Backoff
      {
        attempt = 2L;
        backoff_duration = { seconds = 3L; nanoseconds = 400_000_000 };
        original_schedule_time = Some { seconds = 10L; nanoseconds = 20 };
      }
  in
  let activation : Protocol.activation =
    {
      run_id = "local-run";
      timestamp = Some { seconds = 20L; nanoseconds = 0 };
      is_replaying = false;
      history_length = 2L;
      jobs = [ Protocol.Resolve_activity { seq = 4L; result = backoff } ];
      metadata = None;
    }
  in
  let encoded = unwrap (Protocol.encode_activation activation) in
  if unwrap (Protocol.decode_activation encoded) <> activation then
    failwith "local activity backoff did not round-trip";
  let invalid =
    Protocol.Backoff
      {
        attempt = 0L;
        backoff_duration = { seconds = 3L; nanoseconds = 400_000_000 };
        original_schedule_time = Some { seconds = 10L; nanoseconds = 20 };
      }
  in
  require_error
    (Protocol.encode_activation
       { activation with jobs = [ Protocol.Resolve_activity { seq = 4L; result = invalid } ] })

(** Proves the OCaml side normalizes the shared patch-marker fixtures and
    rejects an empty identifier, an unknown activation field, and a completion
    marker missing its required deprecation flag. Rust exercises these same
    fixtures and the pinned Core conversions in its separate protocol suite. *)
let test_patch_marker_protocol_slice () =
  let completion =
    unwrap
      (Protocol.decode_completion
         (fixture [ "valid"; "patch-completion.input.json" ]))
  in
  let expected =
    String.trim (fixture [ "valid"; "patch-completion.normalized.json" ])
  in
  check_string "patch completion" expected
    (unwrap (Protocol.encode_completion completion));
  ignore (unwrap (Protocol.decode_completion expected));
  let repeated : Protocol.completion =
    {
      run_id = "patch-run";
      commands =
        [ Protocol.Set_patch_marker
            { patch_id = "orders.v2"; deprecated = false };
          Protocol.Set_patch_marker
            { patch_id = "orders.v2"; deprecated = false };
          Protocol.Set_patch_marker
            { patch_id = "orders.v1"; deprecated = true };
          Protocol.Set_patch_marker
            { patch_id = "orders.v1"; deprecated = true } ];
    }
  in
  ignore (unwrap (Protocol.encode_completion repeated));
  require_error
    (Protocol.encode_completion
       {
         repeated with
         commands =
           [ Protocol.Set_patch_marker
               { patch_id = "orders.v2"; deprecated = false };
             Protocol.Set_patch_marker
               { patch_id = "orders.v2"; deprecated = true } ];
       });
  require_error
    (Protocol.decode_completion
       "{\"run_id\":\"patch-run\",\"commands\":[{\"kind\":\"set_patch_marker\",\"patch_id\":\"orders.v2\",\"deprecated\":true},{\"kind\":\"set_patch_marker\",\"patch_id\":\"orders.v2\",\"deprecated\":false}]}");
  let activation : Protocol.activation =
    {
      run_id = "patch-run";
      timestamp = Some { seconds = 1L; nanoseconds = 0 };
      is_replaying = true;
      history_length = 1L;
      jobs = [ Protocol.Notify_has_patch { patch_id = "orders.v2" } ];
      metadata = None;
    }
  in
  require_error
    (Protocol.encode_activation
       { activation with jobs = [ Protocol.Notify_has_patch { patch_id = "" } ] });
  require_error
    (Protocol.decode_activation
       "{\"run_id\":\"patch-run\",\"timestamp\":{\"seconds\":1,\"nanoseconds\":0},\"is_replaying\":true,\"history_length\":1,\"jobs\":[{\"kind\":\"notify_has_patch\",\"patch_id\":\"orders.v2\",\"extra\":true}]}");
  require_error
    (Protocol.decode_completion
       "{\"run_id\":\"patch-run\",\"commands\":[{\"kind\":\"set_patch_marker\",\"patch_id\":\"orders.v2\"}]}")

(** Proves that modern and legacy query IDs, repeated arguments, and headers
    survive the OCaml semantic JSON boundary. Query activations and
    completions are query-only—including rejection of a mixed patch
    notification—and duplicate IDs are rejected before bytes reach Rust. *)
let test_query_protocol_slice () =
  let payload text : Protocol.payload =
    { metadata = [ ("encoding", Bytes.of_string "binary/plain") ]; data = Bytes.of_string text }
  in
  let query query_id : Protocol.activation_job =
    Protocol.Query_workflow
      {
        query_id;
        query_type = "current-state";
        arguments = [ payload "first"; payload "second" ];
        headers = [ ("trace", payload "header") ];
      }
  in
  let activation : Protocol.activation =
    {
      run_id = "query-run";
      timestamp = Some { seconds = 2L; nanoseconds = 3 };
      is_replaying = true;
      history_length = 9L;
      jobs = [ query "query-1"; query "legacy_query" ];
      metadata = None;
    }
  in
  let encoded = unwrap (Protocol.encode_activation activation) in
  if unwrap (Protocol.decode_activation encoded) <> activation then
    failwith "query activation did not preserve repeated fields or IDs";
  let successful : Protocol.completion =
    {
      run_id = "query-run";
      commands =
        [
          Protocol.Query_result
            { query_id = "query-1"; result = Query_succeeded (payload "answer") };
          Protocol.Query_result
            {
              query_id = "legacy_query";
              result = Query_failed
                         {
                           message = "not available";
                           source = "test";
                           stack_trace = "";
                           encoded_attributes = None;
                           cause = None;
                           info = Application { type_name = "QueryFailure"; non_retryable = true; details = [] };
                         };
            };
        ];
    }
  in
  let completion_json = unwrap (Protocol.encode_completion successful) in
  if unwrap (Protocol.decode_completion completion_json) <> successful then
    failwith "query completion did not round-trip";
  require_error
    (Protocol.encode_activation
       { activation with jobs = [ query "query-1"; Protocol.Fire_timer { seq = 1L } ] });
  require_error
    (Protocol.encode_activation
       {
         activation with
         jobs =
           [ query "query-1";
             Protocol.Notify_has_patch { patch_id = "orders.v2" } ];
       });
  require_error
    (Protocol.encode_completion
       {
         successful with
         commands =
           [
             Protocol.Query_result
               { query_id = "query-1"; result = Query_succeeded (payload "answer") };
             Protocol.Query_result
               { query_id = "query-1"; result = Query_succeeded (payload "duplicate") };
           ];
       })
  ;
  require_error
    (Protocol.encode_completion
       {
         successful with
         commands =
           [
             Protocol.Query_result
               { query_id = "query-1"; result = Query_succeeded (payload "answer") };
             Protocol.Start_timer
               {
                 seq = 1L;
                 start_to_fire_timeout = { seconds = 0L; nanoseconds = 1_000_000 };
               };
           ];
       })

(** Proves workflow updates retain both Core identifiers, requester metadata,
    replay validator state, and the two-phase acceptance/completion response.
    A completed-only response is also accepted because Core may deliver it in a
    later activation after the acceptance event has been recorded. *)
let test_update_protocol_slice () =
  let payload text : Protocol.payload =
    { metadata = [ ("encoding", Bytes.of_string "binary/plain") ]; data = Bytes.of_string text }
  in
  let update : Protocol.activation_job =
    Protocol.Do_update
      {
        id = "update-1";
        protocol_instance_id = "update-1";
        name = "set-status";
        input = [ payload "ready" ];
        headers = [ ("trace", payload "header") ];
        meta = { Protocol.identity = "client"; update_id = "update-1" };
        run_validator = true;
      }
  in
  let activation : Protocol.activation =
    {
      run_id = "update-run";
      timestamp = Some { seconds = 1L; nanoseconds = 0 };
      is_replaying = false;
      history_length = 2L;
      jobs = [ update ];
      metadata = None;
    }
  in
  let encoded = unwrap (Protocol.encode_activation activation) in
  if unwrap (Protocol.decode_activation encoded) <> activation then
    failwith "update activation did not round-trip";
  let completion : Protocol.completion =
    {
      run_id = "update-run";
      commands =
        [
          Protocol.Update_response
            { protocol_instance_id = "update-1"; response = Update_accepted };
          Protocol.Update_response
            {
              protocol_instance_id = "update-1";
              response = Update_completed (payload "done");
            };
        ];
    }
  in
  let completion_json = unwrap (Protocol.encode_completion completion) in
  if unwrap (Protocol.decode_completion completion_json) <> completion then
    failwith "update completion did not round-trip";
  let later_completion =
    {
      Protocol.run_id = "update-run";
      commands =
        [
          Protocol.Update_response
            {
              protocol_instance_id = "update-1";
              response = Update_completed (payload "later");
            };
        ];
    }
  in
  ignore (unwrap (Protocol.encode_completion later_completion));
  require_error
    (Protocol.encode_completion
       {
         completion with
         commands =
           [
             Protocol.Update_response
               { protocol_instance_id = "update-1"; response = Update_accepted };
             Protocol.Update_response
               { protocol_instance_id = "update-1"; response = Update_accepted };
           ];
       });
  require_error
    (Protocol.encode_activation
       {
         activation with
         jobs =
           [
             Protocol.Do_update
               {
                 id = "update-1";
                 protocol_instance_id = "update-1";
                 name = "set-status";
                 input = [];
                 headers = [];
                 meta = { Protocol.identity = "client"; update_id = "different" };
                 run_validator = true;
               };
           ];
       })

(** Proves the child-workflow start command uses the same canonical payload
    representation as activities while retaining its workflow identity. *)
let test_start_child_workflow_command () =
  let input : Protocol.payload =
    {
      metadata = [ ("encoding", Bytes.of_string "binary/null") ];
      data = Bytes.empty;
    }
  in
  let completion : Protocol.completion =
    {
      run_id = "parent-run";
      commands =
        [
          Start_child_workflow
            {
              seq = 2L;
              workflow_id = "child/1";
              workflow_type = "child";
              input = [ input ];
              retry_policy = None;
              cancellation_type = Child_try_cancel;
            };
        ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  check_string "child command"
    {|{"commands":[{"cancellation_type":"try_cancel","input":[{"data":{"data":"","encoding":"base64"},"metadata":{"encoding":{"data":"YmluYXJ5L251bGw=","encoding":"base64"}}}],"kind":"start_child_workflow","retry_policy":null,"seq":2,"workflow_id":"child/1","workflow_type":"child"}],"run_id":"parent-run"}|}
    encoded;
  if unwrap (Protocol.decode_completion encoded) <> completion then
    failwith "child command did not round-trip"

(** Proves external signal and cancellation commands retain their target
    identity, payload bytes, and sequence numbers across the strict JSON
    boundary.  These commands are intentionally tested together because both
    suspend a workflow until Core sends a matching resolution job. *)
let test_external_workflow_commands () =
  let payload : Protocol.payload =
    {
      metadata = [ ("encoding", Bytes.of_string "json/plain") ];
      data = Bytes.of_string "\"hello\"";
    }
  in
  let completion : Protocol.completion =
    {
      run_id = "parent-run";
      commands =
        [ Protocol.Signal_external_workflow
            {
              seq = 7L;
              workflow_id = "target-workflow";
              run_id = "target-run";
              signal_name = "greeting";
              input = [ payload ];
              child_workflow_only = false;
              headers = [ ("trace-id", payload) ];
            };
          Protocol.Request_cancel_external_workflow
            {
              seq = 8L;
              workflow_id = "target-workflow";
              run_id = "target-run";
              reason = "parent shutdown";
            } ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  if unwrap (Protocol.decode_completion encoded) <> completion then
    failwith "external workflow commands did not round-trip";
  require_error
    (Protocol.encode_completion
       { completion with
         commands =
           [ Protocol.Signal_external_workflow
               { seq = 7L; workflow_id = ""; run_id = ""; signal_name = "greeting";
                 input = [ payload ]; child_workflow_only = false; headers = [] } ] });
  require_error
    (Protocol.encode_completion
       { completion with
         commands =
           [ Protocol.Request_cancel_external_workflow
               { seq = 8L; workflow_id = "target-workflow"; run_id = "target-run";
                 reason = "" } ] })

(** Proves every child cancellation policy has a distinct, stable JSON spelling
    and remains typed after decoding. This closes the gap where a serializer
    and decoder could agree on one accidentally collapsed policy while the
    Rust/Core bridge still expected four durable cancellation semantics. *)
let test_all_child_cancellation_policies () =
  let input : Protocol.payload =
    { metadata = [ ("encoding", Bytes.of_string "binary/null") ]; data = Bytes.empty }
  in
  List.iter
    (fun (cancellation_type, wire_name) ->
      let completion : Protocol.completion =
        {
          run_id = "parent-run";
          commands =
            [
              Start_child_workflow
                {
                  seq = 2L;
                  workflow_id = "child/1";
                  workflow_type = "child";
                  input = [ input ];
                  retry_policy = None;
                  cancellation_type;
                };
            ];
        }
      in
      let encoded = unwrap (Protocol.encode_completion completion) in
      if
        not
          (contains_substring
             ~needle:("\"cancellation_type\":\"" ^ wire_name ^ "\"")
             encoded)
      then failwith ("child policy was encoded with the wrong spelling: " ^ wire_name);
      if unwrap (Protocol.decode_completion encoded) <> completion then
        failwith ("child policy did not round-trip: " ^ wire_name))
    [
      (Child_try_cancel, "try_cancel");
      (Child_wait_cancellation_completed, "wait_cancellation_completed");
      (Child_abandon, "abandon");
      (Child_wait_cancellation_requested, "wait_cancellation_requested");
    ]

(** Proves the OCaml decoder applies the same cancellation reason, policy, and
    child-identifier boundaries as the Rust decoder. NUL checks use JSON
    escapes so the test exercises decoded semantic strings rather than only
    searching the serialized source. The final cases build values directly to
    cover the outgoing translator path as well. *)
let test_child_cancellation_validation () =
  let document reason : Yojson.Safe.t =
    `Assoc
      [
        ("run_id", `String "parent-run");
        ( "commands",
          `List
            [
              `Assoc
                [
                  ("kind", `String "cancel_child_workflow");
                  ("seq", `Int 7);
                  ("reason", `String reason);
                ];
            ] );
      ]
  in
  List.iter
    (fun reason ->
      require_error
        (Protocol.decode_completion (Yojson.Safe.to_string (document reason))))
    [ ""; String.make 1 '\000' ];
  require_error
    (Protocol.decode_completion
       (Yojson.Safe.to_string (document (String.make 65_537 'x'))));
  let start_document workflow_id workflow_type : Yojson.Safe.t =
    `Assoc
      [
        ("run_id", `String "parent-run");
        ( "commands",
          `List
            [
              `Assoc
                [
                  ("kind", `String "start_child_workflow");
                  ("seq", `Int 7);
                  ("workflow_id", `String workflow_id);
                  ("workflow_type", `String workflow_type);
                  ("input", `List []);
                  ("retry_policy", `Null);
                  ("cancellation_type", `String "try_cancel");
                ];
            ] );
      ]
  in
  List.iter
    (fun (workflow_id, workflow_type) ->
      require_error
        (Protocol.decode_completion
           (Yojson.Safe.to_string
              (start_document workflow_id workflow_type))))
    [ (String.make 1 '\000', "child"); ("child", String.make 1 '\000') ];
  let invalid_utf8 = String.make 1 (Char.chr 0xFF) in
  let completion : Protocol.completion =
    {
      run_id = "parent-run";
      commands =
        [
          Start_child_workflow
            {
              seq = 7L;
              workflow_id = invalid_utf8;
              workflow_type = "child";
              input = [];
              retry_policy = None;
              cancellation_type = Child_try_cancel;
            };
        ];
    }
  in
  require_error (Protocol.encode_completion completion);
  require_error
    (Protocol.decode_completion
       {|{"run_id":"parent-run","commands":[{"kind":"start_child_workflow","seq":7,"workflow_id":"child","workflow_type":"child","input":[],"retry_policy":null,"cancellation_type":"unknown"}]}|})

(** Proves a continue-as-new command is terminal, retains the target workflow
    identity and carries its encoded input through the bilateral JSON shape. *)
let test_continue_as_new_command () =
  let input : Protocol.payload =
    {
      metadata = [ ("encoding", Bytes.of_string "binary/null") ];
      data = Bytes.empty;
    }
  in
  let completion : Protocol.completion =
    {
      run_id = "current-run";
      commands =
        [
          Continue_as_new
            { workflow_type = "counter"; input = [ input ] };
        ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  check_string "continue-as-new command"
    {|{"commands":[{"input":[{"data":{"data":"","encoding":"base64"},"metadata":{"encoding":{"data":"YmluYXJ5L251bGw=","encoding":"base64"}}}],"kind":"continue_as_new","workflow_type":"counter"}],"run_id":"current-run"}|}
    encoded;
  if unwrap (Protocol.decode_completion encoded) <> completion then
    failwith "continue-as-new command did not round-trip"

(** Proves closed nested objects, numeric bounds, canonical binary data, and
    workflow semantic invariants are rejected identically by both languages. *)
let test_invalid_documents () =
  List.iter
    (fun name ->
      require_error
        (Protocol.decode_activation
           (fixture [ "invalid"; name ^ ".json" ])))
    [
      "activation-duplicate-field";
      "activation-unknown-job";
      "activation-seq-negative";
      "activation-seq-too-large";
      "activation-invalid-base64";
      "activation-missing-field";
      "activation-eviction-mixed";
    ];
  List.iter require_invalid_activation
    [
      "activation-child-start-missing-run-id";
      "activation-child-start-empty-run-id";
      "activation-child-start-invalid-cause";
      "activation-child-terminal-missing-payload";
      "activation-child-terminal-missing-failure-info";
      "activation-child-failure-empty-run-id-after-start";
      "activation-child-failure-empty-workflow-id";
      "activation-child-failure-negative-event-id";
      "activation-child-unknown-terminal-kind";
    ];
  List.iter
    (fun name ->
      require_error
        (Protocol.decode_completion
           (fixture [ "invalid"; name ^ ".json" ])))
    [
      "completion-unknown-command";
      "completion-terminal-not-last";
      "completion-invalid-duration";
      "completion-no-activity-timeout";
      "completion-unknown-nested";
      "completion-duplicate-field";
    ]

(** Proves every top-level workflow codec retains the nested strict-JSON path
    supplied by the control protocol. Decoders encounter duplicate members in
    peer documents, while encoders encounter invalid UTF-8 in directly
    assembled semantic values. *)
let test_strict_json_error_paths () =
  check_error_path "activation duplicate" "$.jobs[0]"
    (Protocol.decode_activation
       (fixture [ "invalid"; "activation-duplicate-field.json" ]));
  check_error_path "completion duplicate" "$.commands[0]"
    (Protocol.decode_completion
       (fixture [ "invalid"; "completion-duplicate-field.json" ]));
  let invalid_utf_8 = String.make 1 (Char.chr 0xff) in
  let activation =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "activation.input.json" ]))
  in
  check_error_path "activation encoding" "$.run_id"
    (Protocol.encode_activation { activation with run_id = invalid_utf_8 });
  let completion =
    unwrap
      (Protocol.decode_completion
         (fixture [ "valid"; "completion.input.json" ]))
  in
  check_error_path "completion encoding" "$.run_id"
    (Protocol.encode_completion { completion with run_id = invalid_utf_8 })

(** Proves the semantic parser admits payload data above the normal text limit
    while preserving that limit for ordinary user-visible text. *)
let test_large_nested_payload () =
  let payload : Protocol.payload =
    { metadata = []; data = Bytes.make 50_000 'x' }
  in
  let completion : Protocol.completion =
    {
      run_id = "run-large";
      commands = [ Complete_workflow { result = Some payload } ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  ignore (unwrap (Protocol.decode_completion encoded));
  let oversized_reason = String.make 65_537 'x' in
  require_error
    (Protocol.decode_activation
       (Yojson.Safe.to_string
          (`Assoc
            [
              ("run_id", `String "r");
              ("timestamp", `Assoc [ ("seconds", `Int 0); ("nanoseconds", `Int 0) ]);
              ("is_replaying", `Bool false);
              ("history_length", `Int 0);
              ( "jobs",
                `List
                  [
                    `Assoc
                      [
                        ("kind", `String "cancel_workflow");
                        ("reason", `String oversized_reason);
                      ];
                  ] );
            ])))

(** Proves association-list order cannot affect deterministic wire output. *)
let test_metadata_key_canonicalization () =
  let payload : Protocol.payload =
    {
      metadata = [ ("z-key", Bytes.of_string "z"); ("a-key", Bytes.of_string "a") ];
      data = Bytes.empty;
    }
  in
  let completion : Protocol.completion =
    {
      run_id = "run-map";
      commands = [ Complete_workflow { result = Some payload } ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  let find needle =
    let rec loop offset =
      if offset + String.length needle > String.length encoded then
        failwith ("missing normalized key " ^ needle)
      else if String.sub encoded offset (String.length needle) = needle then offset
      else loop (offset + 1)
    in
    loop 0
  in
  let a = find "\"a-key\"" in
  let z = find "\"z-key\"" in
  if a >= z then failwith "metadata keys were not normalized lexicographically";
  ignore (unwrap (Protocol.decode_completion encoded))

(** Proves the typed protocol encoder rejects duplicate metadata names before
    constructing a JSON object. The public codec performs the same validation
    earlier, while this test protects the bridge-facing representation used by
    client and worker command encoders. *)
let test_duplicate_metadata_rejected () =
  let payload : Protocol.payload =
    {
      metadata =
        [ ("encoding", Bytes.of_string "json/plain");
          ("encoding", Bytes.of_string "json/plain") ];
      data = Bytes.of_string "value";
    }
  in
  let completion : Protocol.completion =
    {
      run_id = "run-duplicate-metadata";
      commands = [ Complete_workflow { result = Some payload } ];
    }
  in
  require_error (Protocol.encode_completion completion)

(** Proves configurable Temporal identifiers are not constrained by an
    invented server-default limit while the protocol safety ceiling remains
    enforced. *)
let test_identifier_safety_limit () =
  let long_id = String.make 300 'i' in
  let completion : Protocol.completion = { run_id = long_id; commands = [] } in
  let encoded = unwrap (Protocol.encode_completion completion) in
  let decoded = unwrap (Protocol.decode_completion encoded) in
  check_string "long identifier" long_id decoded.run_id;
  require_error
    (Protocol.encode_completion
       { run_id = String.make 65_537 'i'; commands = [] })

(** Proves Core initialization ordering and nullable eviction timestamps are
    checked independently of structural JSON validation. *)
let test_activation_cross_field_invariants () =
  let initialized =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "realistic-initialize.input.json" ]))
  in
  let initialize = List.hd initialized.jobs in
  require_error
    (Protocol.encode_activation
       { initialized with jobs = [ initialize; initialize ] });
  require_error
    (Protocol.encode_activation
       {
         initialized with
         jobs = [ Fire_timer { seq = 0L }; initialize ];
       });
  require_error (Protocol.encode_activation { initialized with timestamp = None });
  let zero_sequence =
    {
      initialized with
      jobs = [ Fire_timer { seq = 0L } ];
    }
  in
  let encoded = unwrap (Protocol.encode_activation zero_sequence) in
  ignore (unwrap (Protocol.decode_activation encoded));
  let eviction =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "eviction.input.json" ]))
  in
  match eviction.timestamp with
  | None -> ()
  | Some _ -> failwith "official Core eviction timestamp must remain absent"

(** Proves collection accounting does not impose a smaller workflow-job limit
    than the aggregate document boundary itself. *)
let test_large_activation_job_batch () =
  let activation =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "realistic-initialize.input.json" ]))
  in
  let jobs = List.init 300 (fun seq -> Protocol.Fire_timer { seq = Int64.of_int seq }) in
  let value = { activation with jobs } in
  let encoded = unwrap (Protocol.encode_activation value) in
  let decoded = unwrap (Protocol.decode_activation encoded) in
  if List.length decoded.jobs <> 300 then failwith "large activation job batch was truncated"

(** Builds a failure whose nested info can vary independently of the common
    recursive failure fields. *)
let failure_with_info info : Protocol.failure =
  {
    message = "";
    source = "";
    stack_trace = "";
    encoded_attributes = None;
    cause = None;
    info;
  }

(** Builds a recursive application-failure chain used to exercise the shared
    parser's stack-safety depth boundary. *)
let nested_application_failure cause_count =
  let rec loop remaining cause =
    if remaining = 0 then cause
    else
      loop (remaining - 1)
        {
          (failure_with_info
             (Application
                { type_name = ""; non_retryable = false; details = [] }))
          with
          cause = Some cause;
        }
  in
  loop cause_count
    (failure_with_info
       (Application { type_name = ""; non_retryable = false; details = [] }))

(** Proves recursive failures can exceed the former 16-level limit while the
    serde_json-aligned stack-safety boundary still rejects hostile depth. *)
let test_recursive_failure_depth () =
  let completion cause_count : Protocol.completion =
    {
      run_id = "run-nested-failure";
      commands =
        [ Fail_workflow { failure = nested_application_failure cause_count } ];
    }
  in
  ignore (unwrap (Protocol.encode_completion (completion 32)));
  require_error (Protocol.encode_completion (completion 130))

(** Proves application types are bounded text and activity failures reject
    negative event IDs and oversized identities on outgoing validation. *)
let test_failure_field_semantics () =
  let application : Protocol.completion =
    {
      run_id = "run-failure";
      commands =
        [
          Fail_workflow
            {
              failure =
                failure_with_info
                  (Application
                     { type_name = ""; non_retryable = false; details = [] });
            };
        ];
    }
  in
  ignore (unwrap (Protocol.encode_completion application));
  let invalid_activity scheduled_event_id started_event_id identity =
    ({
       run_id = "run-failure";
       commands =
         [
           Fail_workflow
             {
               failure =
                 failure_with_info
                   (Activity
                      {
                        scheduled_event_id;
                        started_event_id;
                        identity;
                        activity_type = "activity";
                        activity_id = "activity-1";
                        retry_state = Unspecified;
                      });
             };
         ];
     }
      : Protocol.completion)
  in
  require_error (Protocol.encode_completion (invalid_activity (-1L) 0L ""));
  require_error (Protocol.encode_completion (invalid_activity 0L (-1L) ""));
  require_error
    (Protocol.encode_completion
       (invalid_activity 0L 0L (String.make 65_537 'i')))

(** Proves timeout failure metadata keeps both the exact timeout policy and
    ordered heartbeat payloads through OCaml encoding and decoding. The final
    case changes only the enum spelling in an otherwise valid document to
    ensure unknown future values fail closed. *)
let test_timeout_failure_info () =
  let heartbeat : Protocol.payload =
    {
      metadata = [ ("encoding", Bytes.of_string "binary/plain") ];
      data = Bytes.of_string "heartbeat";
    }
  in
  let completion : Protocol.completion =
    {
      run_id = "run-timeout";
      commands =
        [
          Fail_workflow
            {
              failure =
                failure_with_info
                  (Timeout_failure
                     {
                       timeout_type = Timeout_heartbeat;
                       last_heartbeat_details = [ heartbeat ];
                     });
            };
        ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  if
    not
      (contains_substring ~needle:{|"kind":"timeout"|} encoded
      && contains_substring ~needle:{|"timeout_type":"heartbeat"|} encoded)
  then failwith "timeout failure info was not encoded";
  if unwrap (Protocol.decode_completion encoded) <> completion then
    failwith "timeout failure info did not round-trip";
  let rec replace_timeout_type = function
    | `Assoc fields ->
        `Assoc
          (List.map
             (fun (key, value) ->
               if String.equal key "timeout_type" then
                 (key, `String "future_timeout")
               else (key, replace_timeout_type value))
             fields)
    | `List values -> `List (List.map replace_timeout_type values)
    | value -> value
  in
  let malformed =
    Yojson.Safe.to_string
      (replace_timeout_type (Yojson.Safe.from_string encoded))
  in
  require_error (Protocol.decode_completion malformed)

(** Proves retryability survives the Core wrapper used for activity and child
    failures. A nested application flag is authoritative when the wrapper has
    no retry policy of its own, while an explicit timeout remains retryable
    even if the nested application originally marked itself non-retryable. *)
let test_failure_retryability_inheritance () =
  let application non_retryable =
    failure_with_info
      (Application { type_name = "child_failure"; non_retryable; details = [] })
  in
  let child retry_state cause =
    {
      (failure_with_info
         (Child_workflow
            {
              namespace = "temporal-sdk-test";
              workflow_id = "child";
              run_id = "run-child";
              workflow_type = "child";
              initiated_event_id = 1L;
              started_event_id = 2L;
              retry_state;
            }))
      with
      cause = Some cause;
    }
  in
  let activity retry_state cause =
    {
      (failure_with_info
         (Activity
            {
              scheduled_event_id = 1L;
              started_event_id = 2L;
              identity = "worker";
              activity_type = "activity";
              activity_id = "activity";
              retry_state;
            }))
      with
      cause = Some cause;
    }
  in
  if not (Protocol.failure_non_retryable (application true)) then
    failwith "application non-retryable flag was lost";
  if Protocol.failure_non_retryable (application false) then
    failwith "retryable application was marked non-retryable";
  if
    not
      (Protocol.failure_non_retryable
         (child Retry_policy_not_set (application true)))
  then failwith "child application retryability was not inherited";
  if
    not
      (Protocol.failure_non_retryable
         (activity Unspecified (application true)))
  then failwith "activity application retryability was not inherited";
  if
    Protocol.failure_non_retryable (child Timeout (application true))
  then failwith "explicit child timeout was overridden by its cause";
  if
    Protocol.failure_non_retryable
      (activity In_progress (application true))
  then failwith "explicit activity retry state was overridden by its cause";
  if
    not
      (Protocol.failure_non_retryable
         (child Non_retryable_failure (application false)))
  then failwith "explicit child non-retryable state was lost";
  if
    not
      (Protocol.failure_non_retryable
         (activity Maximum_attempts_reached (application false)))
  then failwith "maximum-attempts state was lost"

(** Proves payload-aware parsing does not permit invalid initialization header
    keys through sender-side validation. *)
let test_initialize_header_keys () =
  let activation =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "realistic-initialize.input.json" ]))
  in
  let jobs =
    match activation.jobs with
    | Protocol.Initialize_workflow value :: rest ->
        let context = Option.get value.context in
        let payload : Protocol.payload = { metadata = []; data = Bytes.empty } in
        Protocol.Initialize_workflow
          {
            value with
            context = Some { context with headers = [ ("", payload) ] };
          }
        :: rest
    | _ -> failwith "fixture must start with initialization"
  in
  require_error (Protocol.encode_activation { activation with jobs })

(** Proves two server-default-sized payload byte fields, and their base64
    expansion, fit in one validated semantic document. *)
let test_batched_default_temporal_payloads () =
  let bytes = Bytes.make (2 * 1024 * 1024) 'x' in
  let payload : Protocol.payload =
    { metadata = [ ("second", Bytes.copy bytes) ]; data = bytes }
  in
  let completion : Protocol.completion =
    {
      run_id = "run-batched-payloads";
      commands = [ Complete_workflow { result = Some payload } ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  ignore (unwrap (Protocol.decode_completion encoded))

(** Proves required-nullable members are not interchangeable with omission on
    the OCaml side of the bilateral contract. *)
let test_required_nullable_fields () =
  require_error
    (Protocol.decode_completion
       {|{"run_id":"run-required-null","commands":[{"kind":"complete_workflow"}]}|});
  require_error
    (Protocol.decode_completion
       {|{"run_id":"run-required-null","commands":[{"kind":"schedule_activity","seq":0,"activity_id":"activity","activity_type":"activity","task_queue":"queue","arguments":[],"schedule_to_start_timeout":null,"start_to_close_timeout":null,"heartbeat_timeout":null,"retry_policy":null,"priority":null,"cancellation_type":"try_cancel","do_not_eagerly_execute":false}]}|})

(** Proves an explicit activity retry policy is represented with exact
    durations and coefficient bits, while [None] remains an explicit JSON
    null rather than being omitted or confused with a default policy. *)
let test_activity_retry_policy () =
  let policy : Protocol.retry_policy =
    {
      initial_interval = { seconds = 1L; nanoseconds = 0 };
      backoff_coefficient_bits = "4609434218613702656";
      maximum_interval = { seconds = 60L; nanoseconds = 0 };
      maximum_attempts = 3;
      non_retryable_error_types = [ "InvalidInput" ];
    }
  in
  let schedule retry_policy : Protocol.completion_command =
    Schedule_activity
      {
        seq = 1L;
        activity_id = "activity-1";
        activity_type = "example.activity";
        task_queue = "activities";
        arguments = [];
        schedule_to_close_timeout = Some { seconds = 60L; nanoseconds = 0 };
        schedule_to_start_timeout = None;
        start_to_close_timeout = Some { seconds = 30L; nanoseconds = 0 };
        heartbeat_timeout = None;
        retry_policy;
        priority =
          Some
            {
              priority_key = 2;
              fairness_key = "tenant-a";
              fairness_weight_bits = Int64.of_int32 (Int32.bits_of_float 1.5);
            };
        cancellation_type = Try_cancel;
        do_not_eagerly_execute = false;
      }
  in
  let command = schedule (Some policy) in
  let completion = { Protocol.run_id = "run-retry"; commands = [ command ] } in
  let encoded = unwrap (Protocol.encode_completion completion) in
  if not (String.contains encoded '"') then failwith "policy JSON was empty";
  let retry_policy_json =
    Yojson.Safe.Util.(
      Yojson.Safe.from_string encoded |> member "commands" |> index 0
      |> member "retry_policy")
  in
  let priority_json =
    Yojson.Safe.Util.(
      Yojson.Safe.from_string encoded |> member "commands" |> index 0
      |> member "priority")
  in
  if Yojson.Safe.Util.(priority_json |> member "priority_key" |> to_int) <> 2 then
    failwith "activity priority key was not encoded"
  else if
    Yojson.Safe.Util.(priority_json |> member "fairness_key" |> to_string)
    <> "tenant-a"
  then failwith "activity fairness key was not encoded";
  if
    not
      (String.equal
         (Yojson.Safe.to_string retry_policy_json)
         (Yojson.Safe.to_string
            (`Assoc
              [
                ("backoff_coefficient_bits", `String policy.backoff_coefficient_bits);
                ( "initial_interval",
                  `Assoc [ ("nanoseconds", `Int 0); ("seconds", `Int 1) ] );
                ( "maximum_attempts",
                  `Int policy.maximum_attempts );
                ( "maximum_interval",
                  `Assoc [ ("nanoseconds", `Int 0); ("seconds", `Int 60) ] );
                ( "non_retryable_error_types",
                  `List [ `String "InvalidInput" ] );
              ])))
  then failwith "retry policy JSON was not canonical"
  else if unwrap (Protocol.decode_completion encoded) <> completion then
    failwith "retry policy did not round-trip"
  else
    let no_policy =
      { Protocol.run_id = "run-default"; commands = [ schedule None ] }
    in
    let no_policy_json = unwrap (Protocol.encode_completion no_policy) in
    let no_retry_policy_json =
      Yojson.Safe.Util.(
        Yojson.Safe.from_string no_policy_json |> member "commands" |> index 0
        |> member "retry_policy")
    in
    if not (String.equal (Yojson.Safe.to_string no_retry_policy_json) "null") then
      failwith "omitted retry policy was not encoded as null";
    require_error
      (Protocol.encode_completion
         { completion with
           commands =
             [ schedule
                 (Some
                    { policy with backoff_coefficient_bits = "0" }) ] })
    ;
    require_error
      (Protocol.encode_completion
         { completion with
           commands =
             [ schedule
                 (Some
                    {
                      policy with
                      backoff_coefficient_bits = "18446744073709551615";
                    }) ] })

(** Runs one test with a stable name suitable for CI logs. *)
let run name test =
  try
    test ();
    Printf.printf "PASS %s\n%!" name
  with exn ->
    Printf.eprintf "FAIL %s: %s\n%!" name (Printexc.to_string exn);
    exit 1

let () =
  run "workflow activations" test_valid_activations;
  run "continuation initialization metadata" test_continuation_initialize_metadata;
  run "workflow completion" test_valid_completion;
  run "local activity protocol" test_local_activity_protocol_slice;
  run "local activity backoff protocol" test_local_activity_backoff_protocol_slice;
  run "workflow patch marker protocol" test_patch_marker_protocol_slice;
  run "query protocol slice" test_query_protocol_slice;
  run "update protocol slice" test_update_protocol_slice;
  run "start child workflow command" test_start_child_workflow_command;
  run "external workflow commands" test_external_workflow_commands;
  run "all child cancellation policies" test_all_child_cancellation_policies;
  run "child cancellation validation" test_child_cancellation_validation;
  run "continue-as-new command" test_continue_as_new_command;
  run "malformed workflow documents" test_invalid_documents;
  run "strict JSON error paths" test_strict_json_error_paths;
  run "large nested payload" test_large_nested_payload;
  run "metadata key canonicalization" test_metadata_key_canonicalization;
  run "duplicate metadata rejected" test_duplicate_metadata_rejected;
  run "identifier safety limit" test_identifier_safety_limit;
  run "activation cross-field invariants" test_activation_cross_field_invariants;
  run "large activation job batch" test_large_activation_job_batch;
  run "failure field semantics" test_failure_field_semantics;
  run "timeout failure info" test_timeout_failure_info;
  run "failure retryability inheritance" test_failure_retryability_inheritance;
  run "recursive failure depth" test_recursive_failure_depth;
  run "initialize header keys" test_initialize_header_keys;
  run "batched default Temporal payloads" test_batched_default_temporal_payloads;
  run "required nullable fields" test_required_nullable_fields;
  run "activity retry policy" test_activity_retry_policy

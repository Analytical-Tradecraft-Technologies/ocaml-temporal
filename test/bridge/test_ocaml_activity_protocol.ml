module Protocol = Temporal_protocol.Activity_protocol

(** Extracts a successful activity-protocol result for positive cases. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      let view = Protocol.error_view error in
      failwith (Printf.sprintf "%s at %s: %s" view.code view.path view.message)

(** Requires malformed input or an invalid typed value to be rejected. *)
let require_error = function
  | Error _ -> ()
  | Ok _ -> failwith "expected activity protocol validation to fail"

(** Requires malformed input to retain the strict decoder's precise safe path. *)
let check_error_path label expected = function
  | Error error ->
      let actual = (Protocol.error_view error).path in
      if not (String.equal expected actual) then
        failwith
          (Printf.sprintf "%s path differed: expected %s, got %s" label expected
             actual)
  | Ok _ -> failwith (label ^ " unexpectedly passed validation")

(** Compares normalized JSON output exactly. *)
let check_string label expected actual =
  if not (String.equal expected actual) then
    failwith (Printf.sprintf "%s differed:\nexpected %s\nactual   %s" label expected actual)

(** Finds an exact substring and fails with a useful fixture diagnostic when
    normalized output unexpectedly omits it. *)
let find_substring source needle =
  let needle_length = String.length needle in
  let rec find offset =
    if offset + needle_length > String.length source then
      failwith ("missing normalized fragment " ^ needle)
    else if String.sub source offset needle_length = needle then offset
    else find (offset + 1)
  in
  find 0

(** A complete start task exercises every nullable field and shared semantic
    codec without requiring an official protobuf on the OCaml side. *)
let valid_start_json =
  {|{"task_token":"AAEC/v8=","variant":{"kind":"start","workflow_namespace":"default","workflow_type":"example.workflow","workflow_execution":{"workflow_id":"workflow-1","run_id":"run-1"},"activity_id":"activity-1","activity_type":"example.activity","header_fields":{"trace":{"metadata":{},"data":{"encoding":"base64","data":"aGVhZGVy"}}},"input":[{"metadata":{"encoding":{"encoding":"base64","data":"anNvbi9wbGFpbg=="}},"data":{"encoding":"base64","data":"eyJ2YWx1ZSI6MX0="}}],"heartbeat_details":[],"scheduled_time":{"seconds":10,"nanoseconds":20},"current_attempt_scheduled_time":null,"started_time":{"seconds":11,"nanoseconds":0},"attempt":1,"schedule_to_close_timeout":{"seconds":60,"nanoseconds":0},"start_to_close_timeout":{"seconds":30,"nanoseconds":0},"heartbeat_timeout":null,"retry_policy":{"initial_interval":{"seconds":1,"nanoseconds":0},"backoff_coefficient_bits":"4611686018427387904","maximum_interval":null,"maximum_attempts":3,"non_retryable_error_types":["InvalidInput"]},"priority":{"priority_key":2,"fairness_key":"tenant","fairness_weight_bits":1065353216},"standalone_run_id":""}}|}

(** Decoding a start task produces binary tokens and typed execution context,
    while encoding returns the Rust contract's deterministic field order. *)
let test_start_round_trip () =
  let task = unwrap (Protocol.decode_task valid_start_json) in
  (match task.variant with
  | Start start ->
      if not (Bytes.equal task.task_token (Bytes.of_string "\000\001\002\254\255")) then
        failwith "task token bytes changed";
      if start.attempt <> 1L then failwith "attempt changed";
      if start.workflow_execution.workflow_id <> "workflow-1" then
        failwith "workflow execution changed";
      if start.retry_policy = None then failwith "retry policy was lost"
  | Cancel _ -> failwith "start task decoded as cancellation");
  let encoded = unwrap (Protocol.encode_task task) in
  ignore (unwrap (Protocol.decode_task encoded))

(** Cancellation tasks preserve both the stable primary reason and every
    independent cancellation fact supplied by newer Core versions. *)
let test_cancel_round_trip () =
  let json =
    {|{"task_token":"dG9rZW4=","variant":{"kind":"cancel","reason":"worker_shutdown","details":{"is_not_found":false,"is_cancelled":true,"is_paused":false,"is_timed_out":false,"is_worker_shutdown":true,"is_reset":false}}}|}
  in
  let normalized =
    {|{"task_token":"dG9rZW4=","variant":{"details":{"is_cancelled":true,"is_not_found":false,"is_paused":false,"is_reset":false,"is_timed_out":false,"is_worker_shutdown":true},"kind":"cancel","reason":"worker_shutdown"}}|}
  in
  let task = unwrap (Protocol.decode_task json) in
  check_string "cancel task" normalized (unwrap (Protocol.encode_task task))

(** Activity completion variants reuse the canonical payload and recursive
    failure codecs rather than interpreting their bytes independently. *)
let test_completion_variants () =
  let documents =
    [
      {|{"task_token":"AA==","result":{"kind":"completed","result":null}}|};
      {|{"task_token":"AA==","result":{"kind":"will_complete_async"}}|};
      {|{"task_token":"AA==","result":{"kind":"failed","failure":{"message":"bad input","source":"worker","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"application","type":"InvalidInput","non_retryable":true,"details":[]}}}}|};
      {|{"task_token":"AA==","result":{"kind":"cancelled","failure":{"message":"cancelled","source":"worker","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"canceled","details":[],"identity":"worker-1"}}}}|};
    ]
  in
  List.iter
    (fun json ->
      let completion = unwrap (Protocol.decode_completion json) in
      let encoded = unwrap (Protocol.encode_completion completion) in
      let reparsed = unwrap (Protocol.decode_completion encoded) in
      check_string "completion" encoded (unwrap (Protocol.encode_completion reparsed)))
    documents

(** Heartbeats retain ordered binary detail payloads and the exact opaque token
    while using the same closed-object validation as task completions. *)
let test_heartbeat_round_trip () =
  let heartbeat : Protocol.heartbeat =
    {
      task_token = Bytes.of_string "\000heartbeat\255";
      details =
        [
          {
            metadata = [ ("encoding", Bytes.of_string "binary/plain") ];
            data = Bytes.of_string "\000progress\255";
          };
        ];
    }
  in
  let encoded = unwrap (Protocol.encode_heartbeat heartbeat) in
  let decoded = unwrap (Protocol.decode_heartbeat encoded) in
  if not (Bytes.equal heartbeat.task_token decoded.task_token) then
    failwith "heartbeat task token changed";
  match decoded.details with
  | [ payload ] when Bytes.equal payload.data (Bytes.of_string "\000progress\255") -> ()
  | _ -> failwith "heartbeat detail payload changed"

(** Heartbeat documents reject drift in required fields, token encoding, and
    payload wrappers before they can reach the native supervisor. *)
let test_heartbeat_validation () =
  List.iter
    (fun json -> require_error (Protocol.decode_heartbeat json))
    [
      {|{"task_token":"AA==","details":[],"extra":true}|};
      {|{"task_token":"","details":[]}|};
      {|{"task_token":"AA","details":[]}|};
      {|{"task_token":"AA=="}|};
      {|{"task_token":"AA==","details":[{"metadata":{},"data":{"encoding":"raw","data":"AA=="}}]}|};
    ]

(** Closed-object and required-nullable validation rejects protocol drift at
    every nesting level before a future worker can act on the task. *)
let test_closed_documents () =
  List.iter
    (fun json -> require_error (Protocol.decode_task json))
    [
      {|{"task_token":"AA==","task_token":"AQ==","variant":{"kind":"cancel","reason":"cancelled","details":null}}|};
      {|{"task_token":"AA==","variant":{"kind":"cancel","reason":"cancelled"}}|};
      {|{"task_token":"AA==","variant":{"kind":"cancel","reason":"cancelled","details":null,"extra":true}}|};
      {|{"task_token":"AA==","variant":{"kind":"start","workflow_namespace":"default"}}|};
    ];
  require_error
    (Protocol.decode_completion
       {|{"task_token":"AA==","result":{"kind":"completed"}}|});
  check_error_path "nested completion duplicate" "$.result"
    (Protocol.decode_completion
       {|{"task_token":"AA==","result":{"kind":"will_complete_async","kind":"will_complete_async"}}|})

(** Tokens, identifiers, time values, attempts, retry-policy numbers, header
    keys, and raw bit strings retain the exact bilateral numeric domains. *)
let test_semantic_validation () =
  (* Replacing an exact fragment keeps each malformed document focused on one
     semantic invariant and fails loudly if the representative fixture moves. *)
  let replace_once source before after =
    let before_length = String.length before in
    let rec find offset =
      if offset + before_length > String.length source then
        failwith ("missing test fragment " ^ before)
      else if String.sub source offset before_length = before then
        String.sub source 0 offset ^ after
        ^ String.sub source (offset + before_length)
            (String.length source - offset - before_length)
      else find (offset + 1)
    in
    find 0
  in
  List.iter
    (fun json -> require_error (Protocol.decode_task json))
    [
      replace_once valid_start_json "AAEC/v8=" "AAEC/v8";
      replace_once valid_start_json "\"workflow_namespace\":\"default\"" "\"workflow_namespace\":\"\"";
      replace_once valid_start_json "\"nanoseconds\":20" "\"nanoseconds\":1000000000";
      replace_once valid_start_json "\"seconds\":60" "\"seconds\":-1";
      replace_once valid_start_json "\"attempt\":1" "\"attempt\":4294967296";
      replace_once valid_start_json "\"maximum_attempts\":3" "\"maximum_attempts\":2147483648";
      replace_once valid_start_json "\"backoff_coefficient_bits\":\"4611686018427387904\"" "\"backoff_coefficient_bits\":\"01\"";
      replace_once valid_start_json "\"trace\":" "\"\":";
      replace_once valid_start_json "\"fairness_weight_bits\":1065353216" "\"fairness_weight_bits\":4294967296";
    ];
  require_error
    (Protocol.decode_completion
       {|{"task_token":"","result":{"kind":"will_complete_async"}}|})

(** Typed outgoing maps cannot smuggle duplicate header keys through Yojson's
    association-list representation. *)
let test_outgoing_header_validation () =
  let task = unwrap (Protocol.decode_task valid_start_json) in
  match task.variant with
  | Cancel _ -> failwith "fixture must be a start task"
  | Start start ->
      let payload : Protocol.payload = { metadata = []; data = Bytes.empty } in
      require_error
        (Protocol.encode_task
           {
             task with
             variant =
               Start
                 {
                   start with
                   header_fields = [ ("duplicate", payload); ("duplicate", payload) ];
                 };
           })

(** Shared failure validation applies recursively to received and outgoing
    activity completions, including nonnegative service event identifiers. *)
let test_failure_validation () =
  let malformed =
    {|{"task_token":"AA==","result":{"kind":"failed","failure":{"message":"failed","source":"worker","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"activity","scheduled_event_id":-1,"started_event_id":0,"identity":"worker","activity_type":"activity","activity_id":"activity-1","retry_state":"unspecified"}}}}|}
  in
  require_error (Protocol.decode_completion malformed);
  let failure : Protocol.failure =
    {
      message = "failed";
      source = "worker";
      stack_trace = "";
      encoded_attributes = None;
      cause = None;
      info =
        Activity
          {
            scheduled_event_id = 0L;
            started_event_id = -1L;
            identity = "worker";
            activity_type = "activity";
            activity_id = "activity-1";
            retry_state = Unspecified;
          };
    }
  in
  require_error
    (Protocol.encode_completion
       {
         task_token = Bytes.of_string "token";
         result = Failed failure;
       })

(** Header-map normalization is deterministic and independent of OCaml
    association-list insertion order. *)
let test_header_normalization () =
  let task = unwrap (Protocol.decode_task valid_start_json) in
  match task.variant with
  | Cancel _ -> failwith "fixture must be a start task"
  | Start start ->
      let payload : Protocol.payload = { metadata = []; data = Bytes.empty } in
      let encoded =
        unwrap
          (Protocol.encode_task
             {
               task with
               variant =
                 Start
                   {
                     start with
                     header_fields = [ ("z-header", payload); ("a-header", payload) ];
                   };
             })
      in
      let a = find_substring encoded "\"a-header\"" in
      let z = find_substring encoded "\"z-header\"" in
      if a >= z then failwith "header keys were not normalized lexicographically"

(** Opaque task tokens use the binary-field ceiling rather than the much
    smaller limit reserved for human-readable protocol strings. *)
let test_large_task_token () =
  let task = unwrap (Protocol.decode_task valid_start_json) in
  let token = Bytes.make 50_000 '\255' in
  let encoded = unwrap (Protocol.encode_task { task with task_token = token }) in
  let decoded = unwrap (Protocol.decode_task encoded) in
  if not (Bytes.equal token decoded.task_token) then
    failwith "large opaque task token changed"

(** Runs one test with a stable name suitable for CI logs. *)
let run name test =
  try
    test ();
    Printf.printf "PASS %s\n%!" name
  with exn ->
    Printf.eprintf "FAIL %s: %s\n%!" name (Printexc.to_string exn);
    exit 1

let () =
  run "activity start round trip" test_start_round_trip;
  run "activity cancel round trip" test_cancel_round_trip;
  run "activity completion variants" test_completion_variants;
  run "activity heartbeat round trip" test_heartbeat_round_trip;
  run "activity heartbeat validation" test_heartbeat_validation;
  run "closed activity documents" test_closed_documents;
  run "activity semantic validation" test_semantic_validation;
  run "outgoing activity headers" test_outgoing_header_validation;
  run "activity failure validation" test_failure_validation;
  run "activity header normalization" test_header_normalization;
  run "large activity task token" test_large_task_token

module Control = Control_protocol
module Workflow = Workflow_protocol
module Shared = Workflow.Internal

type payload = Workflow.payload = { metadata : (string * bytes) list; data : bytes }
type timestamp = Workflow.timestamp = { seconds : int64; nanoseconds : int }
type duration = Workflow.duration = { seconds : int64; nanoseconds : int }
type workflow_execution = Workflow.workflow_execution = { workflow_id : string; run_id : string }
type workflow_priority = Workflow.workflow_priority = { priority_key : int; fairness_key : string; fairness_weight_bits : int64 }

type retry_state = Workflow.retry_state =
  | Unspecified
  | In_progress
  | Non_retryable_failure
  | Timeout
  | Maximum_attempts_reached
  | Retry_policy_not_set
  | Internal_server_error
  | Cancel_requested

type timeout_type = Workflow.timeout_type =
  | Timeout_unspecified
  | Timeout_start_to_close
  | Timeout_schedule_to_start
  | Timeout_schedule_to_close
  | Timeout_heartbeat

type failure_info = Workflow.failure_info =
  | Application of { type_name : string; non_retryable : bool; details : payload list }
  | Canceled of { details : payload list; identity : string }
  | Activity of {
      scheduled_event_id : int64;
      started_event_id : int64;
      identity : string;
      activity_type : string;
      activity_id : string;
      retry_state : retry_state;
    }
  | Child_workflow of {
      namespace : string;
      workflow_id : string;
      run_id : string;
      workflow_type : string;
      initiated_event_id : int64;
      started_event_id : int64;
      retry_state : retry_state;
    }
  | Timeout_failure of {
      timeout_type : timeout_type;
      last_heartbeat_details : payload list;
    }

type failure = Workflow.failure = {
  message : string;
  source : string;
  stack_trace : string;
  encoded_attributes : payload option;
  cause : failure option;
  info : failure_info;
}

type retry_policy = {
  initial_interval : duration option;
  backoff_coefficient_bits : string;
  maximum_interval : duration option;
  maximum_attempts : int;
  non_retryable_error_types : string list;
}

type activity_start = {
  workflow_namespace : string;
  workflow_type : string;
  workflow_execution : workflow_execution;
  activity_id : string;
  activity_type : string;
  header_fields : (string * payload) list;
  input : payload list;
  heartbeat_details : payload list;
  scheduled_time : timestamp option;
  current_attempt_scheduled_time : timestamp option;
  started_time : timestamp option;
  attempt : int64;
  schedule_to_close_timeout : duration option;
  start_to_close_timeout : duration option;
  heartbeat_timeout : duration option;
  retry_policy : retry_policy option;
  priority : workflow_priority option;
  standalone_run_id : string;
}

type cancel_reason =
  | Cancellation_not_found
  | Cancellation_requested
  | Cancellation_timed_out
  | Cancellation_worker_shutdown
  | Cancellation_paused
  | Cancellation_reset

type cancellation_details = {
  is_not_found : bool;
  is_cancelled : bool;
  is_paused : bool;
  is_timed_out : bool;
  is_worker_shutdown : bool;
  is_reset : bool;
}

type activity_cancel = { reason : cancel_reason; details : cancellation_details option }
type task_variant = Start of activity_start | Cancel of activity_cancel
type task = { task_token : bytes; variant : task_variant }

type completion_result =
  | Completed of payload option
  | Failed of failure
  | Cancelled of failure
  | Will_complete_async

type completion = { task_token : bytes; result : completion_result }

(** Progress details submitted for one currently leased activity token. Rust
    converts the opaque token back to the official Core protobuf. *)
type heartbeat = { task_token : bytes; details : payload list }
type error = Workflow.error
type error_view = Workflow.error_view = { code : string; path : string; message : string }

(** Copies a semantic error without exposing its private representation. *)
let error_view = Workflow.error_view

(** Converts a strict-JSON failure without flattening its safe nested path. *)
let of_control_error error =
  let view = Control.error_view error in
  Shared.of_control_error view.path error

(** Sequences strict structural and semantic validation without exceptions. *)
let ( let* ) = Result.bind

(** Reads a required object member after its enclosing object has been closed. *)
let required path name entries = Shared.field path name entries

(** Decodes one canonical padded-base64 task token into opaque bytes. *)
let task_token path json =
  match json with
  | `String encoded ->
      (* Tokens deliberately bypass the ordinary 64 KiB text limit. The shared
         binary codec enforces canonical base64 and the 128 MiB byte ceiling. *)
      let wrapper =
        `Assoc [ ("encoding", `String "base64"); ("data", `String encoded) ]
      in
      let* decoded = Shared.bytes_wrapper path wrapper in
      if Bytes.length decoded = 0 then
        Error (Shared.invalid path "activity task token must not be empty")
      else Ok decoded
  | _ -> Error (Shared.invalid path "expected JSON string")

(** Encodes opaque token bytes as the protocol's unwrapped canonical base64
    string. The shared binary codec remains authoritative for size limits. *)
let task_token_json path bytes =
  if Bytes.length bytes = 0 then
    Error (Shared.invalid path "activity task token must not be empty")
  else
    let* wrapper = Shared.bytes_wrapper_json bytes in
    match wrapper with
    | `Assoc entries -> required path "data" entries
    | _ -> Error (Shared.invalid path "canonical task token encoding failed")

(** Decodes a payload map, validates header keys as Temporal identifiers, and
    normalizes association-list order for deterministic typed values. *)
let payload_map path = function
  | `Assoc entries ->
      let rec loop decoded = function
        | [] ->
            Ok
              (List.sort
                 (fun (left, _) (right, _) -> String.compare left right)
                 decoded)
        | (key, value) :: rest ->
            let* _ = Shared.identifier (path ^ ".<key>") (`String key) in
            let* value = Shared.payload (path ^ "." ^ key) value in
            loop ((key, value) :: decoded) rest
      in
      loop [] entries
  | _ -> Error (Shared.invalid path "expected JSON object")

(** Encodes a payload map while rejecting keys that are invalid or repeated in
    OCaml's association-list representation. *)
let payload_map_json path values =
  let rec loop encoded = function
    | [] -> Ok (`Assoc encoded)
    | (key, value) :: rest ->
        let* _ = Shared.identifier (path ^ ".<key>") (`String key) in
        if List.exists (fun (existing, _) -> String.equal existing key) encoded then
          Error (Shared.invalid path "duplicate payload-map key")
        else
          let* value = Shared.payload_json value in
          loop ((key, value) :: encoded) rest
  in
  loop [] values

(** Decodes a list of ordinary bounded strings. *)
let strings path json = Shared.list path Shared.string json

(** Encodes an optional timestamp as an explicitly present null. *)
let optional_timestamp (value : timestamp option) =
  match value with
  | None -> `Null
  | Some value -> Shared.time_json value.seconds value.nanoseconds

(** Encodes an optional duration as an explicitly present null. *)
let optional_duration (value : duration option) =
  match value with
  | None -> `Null
  | Some value -> Shared.time_json value.seconds value.nanoseconds

(** Decodes the retry policy exactly as represented by Rust, including an
    unsigned decimal string for the raw [f64] backoff coefficient bits. *)
let retry_policy path json =
  let fields =
    [
      "initial_interval";
      "backoff_coefficient_bits";
      "maximum_interval";
      "maximum_attempts";
      "non_retryable_error_types";
    ]
  in
  let* entries = Shared.exact_object path fields json in
  let* initial_json = required path "initial_interval" entries in
  let* initial_interval = Shared.nullable (path ^ ".initial_interval") Shared.duration initial_json in
  let* backoff_json = required path "backoff_coefficient_bits" entries in
  let* backoff_coefficient_bits =
    Shared.uint64_decimal (path ^ ".backoff_coefficient_bits") backoff_json
  in
  let* maximum_json = required path "maximum_interval" entries in
  let* maximum_interval = Shared.nullable (path ^ ".maximum_interval") Shared.duration maximum_json in
  let* attempts_json = required path "maximum_attempts" entries in
  let* maximum_attempts = Shared.int32 (path ^ ".maximum_attempts") attempts_json in
  let* errors_json = required path "non_retryable_error_types" entries in
  let* non_retryable_error_types = strings (path ^ ".non_retryable_error_types") errors_json in
  Ok
    {
      initial_interval;
      backoff_coefficient_bits;
      maximum_interval;
      maximum_attempts;
      non_retryable_error_types;
    }

(** Encodes one retry policy; the complete document is reparsed so sender-side
    values receive exactly the same numeric validation as received JSON. *)
let retry_policy_json value =
  Ok
    (`Assoc
      [
        ("initial_interval", optional_duration value.initial_interval);
        ("backoff_coefficient_bits", `String value.backoff_coefficient_bits);
        ("maximum_interval", optional_duration value.maximum_interval);
        ("maximum_attempts", `Int value.maximum_attempts);
        ( "non_retryable_error_types",
          `List (List.map (fun value -> `String value) value.non_retryable_error_types) );
      ])

(** Renders one workflow execution identity using the shared record model. *)
let workflow_execution_json value =
  `Assoc
    [
      ("workflow_id", `String value.workflow_id);
      ("run_id", `String value.run_id);
    ]

(** Renders one workflow priority without converting its weight bits to float. *)
let workflow_priority_json value =
  `Assoc
    [
      ("priority_key", `Int value.priority_key);
      ("fairness_key", `String value.fairness_key);
      ("fairness_weight_bits", `Intlit (Int64.to_string value.fairness_weight_bits));
    ]

(** Decodes complete execution context for one start-task attempt. *)
let activity_start path json =
  let fields =
    [
      "kind";
      "workflow_namespace";
      "workflow_type";
      "workflow_execution";
      "activity_id";
      "activity_type";
      "header_fields";
      "input";
      "heartbeat_details";
      "scheduled_time";
      "current_attempt_scheduled_time";
      "started_time";
      "attempt";
      "schedule_to_close_timeout";
      "start_to_close_timeout";
      "heartbeat_timeout";
      "retry_policy";
      "priority";
      "standalone_run_id";
    ]
  in
  let* entries = Shared.exact_object path fields json in
  let get name = required path name entries in
  let* workflow_namespace_json = get "workflow_namespace" in
  let* workflow_namespace = Shared.identifier (path ^ ".workflow_namespace") workflow_namespace_json in
  let* workflow_type_json = get "workflow_type" in
  let* workflow_type = Shared.identifier (path ^ ".workflow_type") workflow_type_json in
  let* execution_json = get "workflow_execution" in
  let* workflow_execution = Shared.workflow_execution (path ^ ".workflow_execution") execution_json in
  let* activity_id_json = get "activity_id" in
  let* activity_id = Shared.identifier (path ^ ".activity_id") activity_id_json in
  let* activity_type_json = get "activity_type" in
  let* activity_type = Shared.identifier (path ^ ".activity_type") activity_type_json in
  let* headers_json = get "header_fields" in
  let* header_fields = payload_map (path ^ ".header_fields") headers_json in
  let* input_json = get "input" in
  let* input = Shared.list (path ^ ".input") Shared.payload input_json in
  let* heartbeat_json = get "heartbeat_details" in
  let* heartbeat_details = Shared.list (path ^ ".heartbeat_details") Shared.payload heartbeat_json in
  let nullable_time name decoder =
    let* json = get name in
    Shared.nullable (path ^ "." ^ name) decoder json
  in
  let* scheduled_time = nullable_time "scheduled_time" Shared.timestamp in
  let* current_attempt_scheduled_time = nullable_time "current_attempt_scheduled_time" Shared.timestamp in
  let* started_time = nullable_time "started_time" Shared.timestamp in
  let* attempt_json = get "attempt" in
  let* attempt = Shared.uint32 (path ^ ".attempt") attempt_json in
  let* schedule_to_close_timeout = nullable_time "schedule_to_close_timeout" Shared.duration in
  let* start_to_close_timeout = nullable_time "start_to_close_timeout" Shared.duration in
  let* heartbeat_timeout = nullable_time "heartbeat_timeout" Shared.duration in
  let* retry_json = get "retry_policy" in
  let* retry_policy = Shared.nullable (path ^ ".retry_policy") retry_policy retry_json in
  let* priority_json = get "priority" in
  let* priority = Shared.nullable (path ^ ".priority") Shared.workflow_priority priority_json in
  let* standalone_json = get "standalone_run_id" in
  let* standalone_run_id = Shared.string (path ^ ".standalone_run_id") standalone_json in
  Ok
    {
      workflow_namespace;
      workflow_type;
      workflow_execution;
      activity_id;
      activity_type;
      header_fields;
      input;
      heartbeat_details;
      scheduled_time;
      current_attempt_scheduled_time;
      started_time;
      attempt;
      schedule_to_close_timeout;
      start_to_close_timeout;
      heartbeat_timeout;
      retry_policy;
      priority;
      standalone_run_id;
    }

(** Encodes a payload list while preserving argument order. *)
let payloads_json values =
  let rec loop encoded = function
    | [] -> Ok (`List (List.rev encoded))
    | value :: rest ->
        let* value = Shared.payload_json value in
        loop (value :: encoded) rest
  in
  loop [] values

(** Encodes complete start-task context for strict sender-side reparsing. *)
let activity_start_json value =
  let* header_fields = payload_map_json "$.variant.header_fields" value.header_fields in
  let* input = payloads_json value.input in
  let* heartbeat_details = payloads_json value.heartbeat_details in
  let* retry_policy =
    match value.retry_policy with
    | None -> Ok `Null
    | Some value -> retry_policy_json value
  in
  let priority =
    match value.priority with None -> `Null | Some value -> workflow_priority_json value
  in
  Ok
    (`Assoc
      [
        ("kind", `String "start");
        ("workflow_namespace", `String value.workflow_namespace);
        ("workflow_type", `String value.workflow_type);
        ("workflow_execution", workflow_execution_json value.workflow_execution);
        ("activity_id", `String value.activity_id);
        ("activity_type", `String value.activity_type);
        ("header_fields", header_fields);
        ("input", input);
        ("heartbeat_details", heartbeat_details);
        ("scheduled_time", optional_timestamp value.scheduled_time);
        ("current_attempt_scheduled_time", optional_timestamp value.current_attempt_scheduled_time);
        ("started_time", optional_timestamp value.started_time);
        ("attempt", `Intlit (Int64.to_string value.attempt));
        ("schedule_to_close_timeout", optional_duration value.schedule_to_close_timeout);
        ("start_to_close_timeout", optional_duration value.start_to_close_timeout);
        ("heartbeat_timeout", optional_duration value.heartbeat_timeout);
        ("retry_policy", retry_policy);
        ("priority", priority);
        ("standalone_run_id", `String value.standalone_run_id);
      ])

(** Decodes the stable cancellation reason spelling. *)
let cancel_reason path = function
  | "not_found" -> Ok Cancellation_not_found
  | "cancelled" -> Ok Cancellation_requested
  | "timed_out" -> Ok Cancellation_timed_out
  | "worker_shutdown" -> Ok Cancellation_worker_shutdown
  | "paused" -> Ok Cancellation_paused
  | "reset" -> Ok Cancellation_reset
  | _ -> Error (Shared.invalid path "unknown activity cancellation reason")

(** Renders the stable cancellation reason spelling. *)
let cancel_reason_string = function
  | Cancellation_not_found -> "not_found"
  | Cancellation_requested -> "cancelled"
  | Cancellation_timed_out -> "timed_out"
  | Cancellation_worker_shutdown -> "worker_shutdown"
  | Cancellation_paused -> "paused"
  | Cancellation_reset -> "reset"

(** Decodes the independent cancellation facts as one closed object. *)
let cancellation_details path json =
  let fields =
    [
      "is_not_found";
      "is_cancelled";
      "is_paused";
      "is_timed_out";
      "is_worker_shutdown";
      "is_reset";
    ]
  in
  let* entries = Shared.exact_object path fields json in
  let read name =
    let* json = required path name entries in
    Shared.bool (path ^ "." ^ name) json
  in
  let* is_not_found = read "is_not_found" in
  let* is_cancelled = read "is_cancelled" in
  let* is_paused = read "is_paused" in
  let* is_timed_out = read "is_timed_out" in
  let* is_worker_shutdown = read "is_worker_shutdown" in
  let* is_reset = read "is_reset" in
  Ok
    {
      is_not_found;
      is_cancelled;
      is_paused;
      is_timed_out;
      is_worker_shutdown;
      is_reset;
    }

(** Encodes independent cancellation facts without deriving one flag from
    another; Core treats each field as separate information. *)
let cancellation_details_json value =
  `Assoc
    [
      ("is_not_found", `Bool value.is_not_found);
      ("is_cancelled", `Bool value.is_cancelled);
      ("is_paused", `Bool value.is_paused);
      ("is_timed_out", `Bool value.is_timed_out);
      ("is_worker_shutdown", `Bool value.is_worker_shutdown);
      ("is_reset", `Bool value.is_reset);
    ]

(** Decodes either a start or cancellation task from the closed tagged union. *)
let task_variant path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (Shared.invalid path "expected JSON object")
  in
  let* kind_json = required path "kind" entries in
  let* kind = Shared.string (path ^ ".kind") kind_json in
  match kind with
  | "start" ->
      let* value = activity_start path json in
      Ok (Start value)
  | "cancel" ->
      let* entries = Shared.exact_object path [ "kind"; "reason"; "details" ] json in
      let* reason_json = required path "reason" entries in
      let* reason_name = Shared.string (path ^ ".reason") reason_json in
      let* reason = cancel_reason (path ^ ".reason") reason_name in
      let* details_json = required path "details" entries in
      let* details = Shared.nullable (path ^ ".details") cancellation_details details_json in
      Ok (Cancel { reason; details })
  | _ -> Error (Shared.invalid (path ^ ".kind") "unknown activity task kind")

(** Encodes either task variant using its exact closed member set. *)
let task_variant_json = function
  | Start value -> activity_start_json value
  | Cancel value ->
      let details =
        match value.details with
        | None -> `Null
        | Some value -> cancellation_details_json value
      in
      Ok
        (`Assoc
          [
            ("kind", `String "cancel");
            ("reason", `String (cancel_reason_string value.reason));
            ("details", details);
          ])

(** Converts one strict task object into typed activity execution data. *)
let task_from_json json =
  let* entries = Shared.exact_object "$" [ "task_token"; "variant" ] json in
  let* token_json = required "$" "task_token" entries in
  let* task_token = task_token "$.task_token" token_json in
  let* variant_json = required "$" "variant" entries in
  let* variant = task_variant "$.variant" variant_json in
  Ok { task_token; variant }

(** Strictly decodes a task through the duplicate-aware JSON foundation. *)
let decode_task input =
  match Control.decode_payload_object input with
  | Error error -> Error (of_control_error error)
  | Ok json -> task_from_json json

(** Encodes and reparses a task so typed outgoing values obey every receiver
    invariant before crossing the native boundary. *)
let encode_task (value : task) =
  let* token = task_token_json "$.task_token" value.task_token in
  let* variant = task_variant_json value.variant in
  let json = `Assoc [ ("task_token", token); ("variant", variant) ] in
  match Control.encode_payload_object json with
  | Error error -> Error (of_control_error error)
  | Ok output ->
      let* _ = decode_task output in
      Ok output

(** Decodes one closed activity completion result. *)
let completion_result path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (Shared.invalid path "expected JSON object")
  in
  let* kind_json = required path "kind" entries in
  let* kind = Shared.string (path ^ ".kind") kind_json in
  match kind with
  | "completed" ->
      let* entries = Shared.exact_object path [ "kind"; "result" ] json in
      let* result_json = required path "result" entries in
      let* result = Shared.nullable (path ^ ".result") Shared.payload result_json in
      Ok (Completed result)
  | "failed" | "cancelled" ->
      let* entries = Shared.exact_object path [ "kind"; "failure" ] json in
      let* failure_json = required path "failure" entries in
      let* failure = Shared.failure (path ^ ".failure") failure_json in
      if String.equal kind "failed" then Ok (Failed failure)
      else Ok (Cancelled failure)
  | "will_complete_async" ->
      let* _ = Shared.exact_object path [ "kind" ] json in
      Ok Will_complete_async
  | _ -> Error (Shared.invalid (path ^ ".kind") "unknown activity completion kind")

(** Encodes one terminal completion result through the shared payload or
    failure codec as appropriate. *)
let completion_result_json = function
  | Completed result ->
      let* result =
        match result with None -> Ok `Null | Some value -> Shared.payload_json value
      in
      Ok (`Assoc [ ("kind", `String "completed"); ("result", result) ])
  | Failed failure ->
      let* failure = Shared.failure_json failure in
      Ok (`Assoc [ ("kind", `String "failed"); ("failure", failure) ])
  | Cancelled failure ->
      let* failure = Shared.failure_json failure in
      Ok (`Assoc [ ("kind", `String "cancelled"); ("failure", failure) ])
  | Will_complete_async -> Ok (`Assoc [ ("kind", `String "will_complete_async") ])

(** Converts one strict completion object into a typed terminal response. *)
let completion_from_json json =
  let* entries = Shared.exact_object "$" [ "task_token"; "result" ] json in
  let* token_json = required "$" "task_token" entries in
  let* task_token = task_token "$.task_token" token_json in
  let* result_json = required "$" "result" entries in
  let* result = completion_result "$.result" result_json in
  Ok { task_token; result }

(** Strictly decodes a completion through the duplicate-aware JSON foundation. *)
let decode_completion input =
  match Control.decode_payload_object input with
  | Error error -> Error (of_control_error error)
  | Ok json -> completion_from_json json

(** Encodes and reparses a completion before it can be submitted to Core. *)
let encode_completion (value : completion) =
  let* token = task_token_json "$.task_token" value.task_token in
  let* result = completion_result_json value.result in
  let json = `Assoc [ ("task_token", token); ("result", result) ] in
  match Control.encode_payload_object json with
  | Error error -> Error (of_control_error error)
  | Ok output ->
      let* _ = decode_completion output in
      Ok output

(** Decodes a heartbeat object after closing its exact two-member shape. *)
let heartbeat_from_json json =
  let* entries = Shared.exact_object "$" [ "task_token"; "details" ] json in
  let* token_json = required "$" "task_token" entries in
  let* task_token = task_token "$.task_token" token_json in
  let* details_json = required "$" "details" entries in
  let* details = Shared.list "$.details" Shared.payload details_json in
  Ok { task_token; details }

(** Strictly decodes one activity-heartbeat JSON document. *)
let decode_heartbeat input =
  match Control.decode_payload_object input with
  | Error error -> Error (of_control_error error)
  | Ok json -> heartbeat_from_json json

(** Encodes and reparses a heartbeat before sending it across the native
    boundary, so malformed metadata cannot reach Core. *)
let encode_heartbeat (value : heartbeat) =
  let* token = task_token_json "$.task_token" value.task_token in
  let* details = payloads_json value.details in
  let json = `Assoc [ ("task_token", token); ("details", details) ] in
  match Control.encode_payload_object json with
  | Error error -> Error (of_control_error error)
  | Ok output ->
      let* _ = decode_heartbeat output in
      Ok output

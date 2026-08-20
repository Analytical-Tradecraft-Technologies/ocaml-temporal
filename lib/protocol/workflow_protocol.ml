module Control = Control_protocol

type payload = { metadata : (string * bytes) list; data : bytes }
type timestamp = { seconds : int64; nanoseconds : int }
type duration = { seconds : int64; nanoseconds : int }
type workflow_execution = { workflow_id : string; run_id : string }
type namespaced_workflow_execution = { namespace : string; workflow_id : string; run_id : string }
type workflow_priority = { priority_key : int; fairness_key : string; fairness_weight_bits : int64 }

(** Requester metadata attached to a workflow update. The nested update ID is
    retained so strict validation can compare it with the enclosing ID. *)
type update_meta = { identity : string; update_id : string }

type worker_deployment_version = { deployment_name : string; build_id : string }

type suggest_continue_as_new_reason =
  | Suggest_unspecified
  | History_size_too_large
  | Too_many_history_events
  | Too_many_updates

type activation_metadata = {
  available_internal_flags : int64 list;
  history_size_bytes : string;
  continue_as_new_suggested : bool;
  deployment_version_for_current_task : worker_deployment_version option;
  last_sdk_version : string;
  suggest_continue_as_new_reasons : suggest_continue_as_new_reason list;
  target_worker_deployment_version_changed : bool;
}

type retry_state =
  | Unspecified
  | In_progress
  | Non_retryable_failure
  | Timeout
  | Maximum_attempts_reached
  | Retry_policy_not_set
  | Internal_server_error
  | Cancel_requested

(** The timeout policy that Temporal reports as having elapsed.  This is kept
    separate from [retry_state]'s [Timeout] constructor: retry state describes
    the outer activity/child wrapper, while this value preserves Core's
    TimeoutFailureInfo metadata. *)
type timeout_type =
  | Timeout_unspecified
  | Timeout_start_to_close
  | Timeout_schedule_to_start
  | Timeout_schedule_to_close
  | Timeout_heartbeat

type failure_info =
  | Application of {
      type_name : string;
      non_retryable : bool;
      details : payload list;
    }
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

type failure = {
  message : string;
  source : string;
  stack_trace : string;
  encoded_attributes : payload option;
  cause : failure option;
  info : failure_info;
}

type continue_as_new_initiator =
  | Continue_as_new_unspecified
  | Continue_as_new_workflow
  | Continue_as_new_retry
  | Continue_as_new_cron_schedule

(** A bridge-ready retry policy shared by workflow initialization and
    workflow commands. Durations and the coefficient bit string are validated
    before the value is serialized or handed to the Rust bridge. *)
type retry_policy = {
  initial_interval : duration;
  backoff_coefficient_bits : string;
  maximum_interval : duration;
  maximum_attempts : int;
  non_retryable_error_types : string list;
}

type continuation = {
  continued_from_execution_run_id : string;
  initiator : continue_as_new_initiator;
  continued_failure : failure option;
  last_completion_result : payload list option;
}

type initialize_context = {
  headers : (string * payload) list;
  identity : string;
  parent_workflow : namespaced_workflow_execution option;
  workflow_execution_timeout : duration option;
  workflow_run_timeout : duration option;
  workflow_task_timeout : duration option;
  first_execution_run_id : string;
  start_time : timestamp option;
  root_workflow : workflow_execution option;
  priority : workflow_priority option;
  retry_policy : retry_policy option;
  continuation : continuation option;
}

(** Computes the public retryability flag without discarding an application
    failure nested inside a Core activity or child-workflow wrapper. Explicit
    retry states are authoritative: a timeout or in-progress retry remains
    retryable, while Core's non-retryable and attempt-exhausted states do not.
    [Retry_policy_not_set] and [Unspecified] carry no retryability decision of
    their own, so the nested cause is consulted. The depth limit matches the
    protocol's bounded recursive failure representation. *)
let failure_non_retryable failure =
  let rec loop depth (value : failure) =
    let nested () =
      match value.cause with
      | Some cause when depth < 128 -> loop (depth + 1) cause
      | None | Some _ -> false
    in
    match value.info with
    | Application { non_retryable; _ } -> non_retryable
    | Canceled _ -> false
    | Activity { retry_state; _ } | Child_workflow { retry_state; _ } -> (
        match retry_state with
        | Non_retryable_failure | Maximum_attempts_reached -> true
        | Retry_policy_not_set | Unspecified -> nested ()
        | In_progress | Timeout | Internal_server_error | Cancel_requested ->
            false)
    | Timeout_failure _ -> nested ()
  in
  loop 0 failure

type activity_resolution =
  | Completed of payload option
  | Failed of failure
  | Cancelled of failure
  (** Core uses this nonterminal result when a local activity retry should be
      delayed by a workflow timer before the next attempt is scheduled. *)
  | Backoff of {
      attempt : int64;
      backoff_duration : duration;
      original_schedule_time : timestamp option;
    }

type child_workflow_start_failure_cause =
  | Child_start_unspecified
  | Child_start_workflow_already_exists

type child_workflow_start_resolution =
  | Child_start_succeeded of string
  | Child_start_failed of {
      workflow_id : string;
      workflow_type : string;
      cause : child_workflow_start_failure_cause;
    }
  | Child_start_cancelled of failure

type child_workflow_resolution =
  | Child_completed of payload option
  | Child_failed of failure
  | Child_cancelled of failure

type eviction_reason =
  | Eviction_unspecified
  | Cache_full
  | Cache_miss
  | Nondeterminism
  | Lang_fail
  | Lang_requested
  | Task_not_found
  | Unhandled_command
  | Fatal
  | Pagination_or_history_fetch
  | Workflow_execution_ending

type activation_job =
  | Initialize_workflow of {
      workflow_id : string;
      workflow_type : string;
      arguments : payload list;
      randomness_seed : string;
      attempt : int;
      context : initialize_context option;
    }
  | Resolve_activity of { seq : int64; result : activity_resolution }
  | Resolve_child_workflow_start of {
      seq : int64;
      result : child_workflow_start_resolution;
    }
  | Resolve_child_workflow of {
      seq : int64;
      result : child_workflow_resolution;
    }
  (** Reports whether Core delivered a signal to an external workflow. A
      structured failure is a workflow-level result, not a bridge exception. *)
  | Resolve_signal_external_workflow of {
      seq : int64;
      result : (unit, failure) result;
    }
  (** Reports whether Core accepted cancellation of an external workflow. *)
  | Resolve_request_cancel_external_workflow of {
      seq : int64;
      result : (unit, failure) result;
    }
  (** Delivers one synchronous query request. Query jobs carry no sequence and
      never resume a workflow fiber; preserving the repeated argument and
      header fields lets the runtime apply its typed handler policy without
      losing Core data at the bridge boundary. *)
  | Query_workflow of {
      query_id : string;
      query_type : string;
      arguments : payload list;
      headers : (string * payload) list;
    }
  (** Delivers one workflow update request. Core supplies both an enclosing
      ID and a metadata ID; the decoder proves they refer to the same update. *)
  | Do_update of {
      id : string;
      protocol_instance_id : string;
      name : string;
      input : payload list;
      headers : (string * payload) list;
      meta : update_meta;
      run_validator : bool;
    }
  (** Delivers one signal received by this workflow. Signal jobs have no
      sequence number: Core replays their name, payloads, sender identity, and
      headers as one ordinary workflow activation job. *)
  | Signal_workflow of {
      signal_name : string;
      input : payload list;
      identity : string;
      headers : (string * payload) list;
    }
  (** Reports that Core found the named patch marker while replaying this run. *)
  | Notify_has_patch of { patch_id : string }
  | Fire_timer of { seq : int64 }
  | Cancel_workflow of { reason : string }
  | Remove_from_cache of { message : string; reason : eviction_reason }

type activation = {
  run_id : string;
  timestamp : timestamp option;
  is_replaying : bool;
  history_length : int64;
  jobs : activation_job list;
  metadata : activation_metadata option;
}

type activity_cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** Controls how Core reports a child-workflow cancellation request to the
    parent.  The policy is part of the command so replay sees the same
    cancellation semantics on every activation. *)
type child_workflow_cancellation_type =
  | Child_try_cancel
  | Child_wait_cancellation_completed
  | Child_abandon
  | Child_wait_cancellation_requested

(** The result sent for one synchronous query. A failed query answers the
    request without failing or changing the workflow execution itself. *)
type query_result =
  | Query_succeeded of payload
  | Query_failed of failure

(** The first-phase decision or final value for one workflow update. *)
type update_response =
  | Update_accepted
  | Update_rejected of failure
  | Update_completed of payload

type completion_command =
  | Schedule_activity of {
      seq : int64;
      activity_id : string;
      activity_type : string;
      task_queue : string;
      arguments : payload list;
      schedule_to_close_timeout : duration option;
      schedule_to_start_timeout : duration option;
      start_to_close_timeout : duration option;
      heartbeat_timeout : duration option;
      retry_policy : retry_policy option;
      priority : workflow_priority option;
      cancellation_type : activity_cancellation_type;
      do_not_eagerly_execute : bool;
    }
  (** Schedules a local activity on the worker's local-activity lane. Local
      activities are still resolved through the normal activity sequence, but
      never create a server activity task; Core records their result in a
      marker so replay observes the same payload. *)
  | Schedule_local_activity of {
      seq : int64;
      activity_id : string;
      activity_type : string;
      attempt : int64;
      original_schedule_time : timestamp option;
      arguments : payload list;
      schedule_to_close_timeout : duration option;
      schedule_to_start_timeout : duration option;
      start_to_close_timeout : duration option;
      retry_policy : retry_policy option;
      local_retry_threshold : duration option;
      cancellation_type : activity_cancellation_type;
    }
  | Start_child_workflow of {
      seq : int64;
      workflow_id : string;
      workflow_type : string;
      input : payload list;
      retry_policy : retry_policy option;
      cancellation_type : child_workflow_cancellation_type;
    }
  | Cancel_child_workflow of { seq : int64; reason : string }
  (** Sends a signal to another workflow execution. Core resolves the command
      asynchronously; [child_workflow_only] preserves the safer child-target
      restriction when requested by the workflow author. *)
  | Signal_external_workflow of {
      seq : int64;
      workflow_id : string;
      run_id : string;
      signal_name : string;
      input : payload list;
      child_workflow_only : bool;
      headers : (string * payload) list;
    }
  (** Requests cancellation of another workflow execution and waits for Core's
      acknowledgement or structured failure before resolving the workflow
      future. *)
  | Request_cancel_external_workflow of {
      seq : int64;
      workflow_id : string;
      run_id : string;
      reason : string;
    }
  | Request_cancel_activity of { seq : int64 }
  (** Requests cancellation of a scheduled local activity.  Core keeps this
      separate from remote activity cancellation because local work has no
      server task token. *)
  | Request_cancel_local_activity of { seq : int64 }
  | Start_timer of { seq : int64; start_to_fire_timeout : duration }
  | Cancel_timer of { seq : int64 }
  (** Answers a query request identified by Core. The [query_id] is retained
      exactly, including Core's legacy-query identifier, because Core uses it
      to choose its legacy response path. *)
  | Query_result of { query_id : string; result : query_result }
  (** Answers one update protocol instance. An accepted or rejected response
      is a decision; a completed response carries the handler result. *)
  | Update_response of {
      protocol_instance_id : string;
      response : update_response;
    }
  (** Records one workflow patch lifecycle operation. [deprecated]
      distinguishes an active patch-in call from a deprecation call. *)
  | Set_patch_marker of { patch_id : string; deprecated : bool }
  (** Merges payload values into Temporal's indexed search-attribute map. *)
  | Upsert_search_attributes of { search_attributes : (string * payload) list }
  | Complete_workflow of { result : payload option }
  | Fail_workflow of { failure : failure }
  | Continue_as_new of { workflow_type : string; input : payload list }
  | Cancel_workflow_execution

type completion = { run_id : string; commands : completion_command list }
type error = { code : string; path : string; message : string }
type error_view = { code : string; path : string; message : string }

(** Copies a semantic error without exposing its representation. *)
let error_view (error : error) : error_view =
  { code = error.code; path = error.path; message = error.message }

(** Constructs a privacy-safe semantic validation failure. *)
let invalid path message : error = { code = "invalid_message"; path; message }

(** Converts a foundation error while retaining only its safe diagnostic. *)
let of_control_error path error : error =
  let view = Control.error_view error in
  { code = view.code; path; message = view.message }

(** Converts a foundation error without rebasing its already-contextual path. *)
let of_control_error_at_source error : error =
  let view = Control.error_view error in
  of_control_error view.path error

(** Sequences semantic validation without exceptions. *)
let ( let* ) = Result.bind

(** Matches the control protocol's ordinary decoded-string safety ceiling.
    Temporal identifier limits are server-configurable, so the bridge imposes
    no narrower identifier policy of its own. *)
let protocol_string_safety_limit = 65_536

(** Requires a closed object before individual fields are read. *)
let exact_object path expected = function
  | `Assoc entries
    when List.length entries = List.length expected
         && List.for_all (fun (name, _) -> List.mem name expected) entries ->
      Ok entries
  | `Assoc _ -> Error (invalid path "object has missing or unknown fields")
  | _ -> Error (invalid path "expected JSON object")

(** Reads one already-required object member. *)
let field path name entries =
  match List.assoc_opt name entries with
  | Some value -> Ok value
  | None -> Error (invalid path ("missing required field " ^ name))

(** Reads a JSON string. *)
let string path = function
  | `String value when String.length value <= 65_536 -> Ok value
  | `String _ -> Error (invalid path "decoded JSON string limit exceeded")
  | _ -> Error (invalid path "expected JSON string")

(** Checks every byte of a semantic string as one complete UTF-8 sequence.
    Incoming JSON normally receives this check from [Control], but semantic
    values can also be assembled directly by an OCaml translator. Keeping the
    check here closes that second path before an invalid string reaches the
    JSON encoder or the Rust bridge. *)
let valid_utf_8 value =
  let rec loop offset =
    if offset = String.length value then true
    else
      let decoded = String.get_utf_8_uchar value offset in
      Uchar.utf_decode_is_valid decoded
      && loop (offset + Uchar.utf_decode_length decoded)
  in
  loop 0

(** Validates text that may be empty but still crosses the Core/JSON boundary.
    Signal sender identities are opaque to the workflow, yet they are carried
    in Core's string field and later replayed.  Keeping the check separate from
    [identifier] allows an empty identity while rejecting values that cannot be
    represented deterministically by the bilateral protocol. *)
let bounded_text path value =
  let* value = string path value in
  if String.contains value '\000' then
    Error (invalid path "text must not contain NUL")
  else if not (valid_utf_8 value) then
    Error (invalid path "text must be valid UTF-8")
  else Ok value

(** Validates a cancellation reason before it becomes part of history.  A
    reason is human-readable text rather than an identifier, but it still must
    be non-empty, bounded, UTF-8, and free of embedded NUL bytes for every
    supported Core transport. *)
let cancellation_reason path value =
  let* value = string path value in
  if String.length value = 0 then Error (invalid path "reason must not be empty")
  else if String.contains value '\000' then
    Error (invalid path "reason must not contain NUL")
  else if not (valid_utf_8 value) then
    Error (invalid path "reason must be valid UTF-8")
  else Ok value

(** Reads a JSON Boolean. *)
let bool path = function
  | `Bool value -> Ok value
  | _ -> Error (invalid path "expected JSON boolean")

(** Reads an integral JSON value into the common signed 64-bit range. *)
let int64 path = function
  | `Int value -> Ok (Int64.of_int value)
  | `Intlit value -> (
      try Ok (Int64.of_string value)
      with _ -> Error (invalid path "integer is outside signed 64-bit range"))
  | _ -> Error (invalid path "expected JSON integer")

(** Reads a protobuf event identifier, whose signed wire representation must
    never carry a negative service event number. *)
let nonnegative_int64 path value =
  let* value = int64 path value in
  if Int64.compare value 0L < 0 then
    Error (invalid path "event identifier must not be negative")
  else Ok value

(** Reads an unsigned 32-bit protobuf number without narrowing it to OCaml
    [int] on platforms where that could be unsafe. *)
let uint32 path value =
  let* value = int64 path value in
  if Int64.compare value 0L < 0 || Int64.compare value 4_294_967_295L > 0 then
    Error (invalid path "integer is outside unsigned 32-bit range")
  else Ok value

(** Reads a bounded positive protobuf [int32]. *)
let positive_int32 path value =
  let* value = int64 path value in
  if Int64.compare value 1L < 0 || Int64.compare value 2_147_483_647L > 0 then
    Error (invalid path "integer is outside positive signed 32-bit range")
  else Ok (Int64.to_int value)

(** Reads a signed protobuf [int32] without accepting a wider JSON integer. *)
let int32 path value =
  let* value = int64 path value in
  if Int64.compare value (-2_147_483_648L) < 0 || Int64.compare value 2_147_483_647L > 0 then
    Error (invalid path "integer is outside signed 32-bit range")
  else Ok (Int64.to_int value)

(** Requires a non-empty bounded identifier that can safely cross the JSON
    bridge. Temporal names may contain punctuation and non-ASCII text, so the
    validator deliberately avoids a narrower character allow-list while still
    rejecting embedded NUL bytes and malformed UTF-8 assembled by an OCaml
    translator. *)
let identifier path value =
  let* value = string path value in
  if String.length value = 0 || String.length value > protocol_string_safety_limit then
    Error (invalid path "identifier is empty or exceeds the protocol string safety limit")
  else if String.contains value '\000' then
    Error (invalid path "identifier must not contain NUL")
  else if not (valid_utf_8 value) then
    Error (invalid path "identifier must be valid UTF-8")
  else Ok value

(** Validates the run ID carried by a child-workflow failure. Temporal Core
    reports cancellation while a child start is still in flight with an empty
    run ID and [started_event_id = 0]; once a child has started, the normal
    non-empty identifier contract applies. *)
let child_failure_run_id path ~started_event_id value =
  let* value = string path value in
  if String.equal value "" then
    if Int64.equal started_event_id 0L then Ok value
    else Error (invalid path "child failure run_id may be empty only before the child starts")
  else identifier path (`String value)

(** Validates a canonical unsigned 64-bit decimal representation. *)
let uint64_decimal path value =
  let* value = string path value in
  let maximum = "18446744073709551615" in
  if
    String.length value = 0
    || (String.length value > 1 && Char.equal value.[0] '0')
    || not (String.for_all (function '0' .. '9' -> true | _ -> false) value)
    || String.length value > String.length maximum
    || (String.length value = String.length maximum
       && String.compare value maximum > 0)
  then Error (invalid path "value is not canonical unsigned 64-bit decimal")
  else Ok value

(** Decodes a list while adding a stable index to nested error paths. *)
let list path decode = function
  | `List values ->
      let rec loop index decoded = function
        | [] -> Ok (List.rev decoded)
        | value :: rest ->
            let* value = decode (Printf.sprintf "%s[%d]" path index) value in
            loop (index + 1) (value :: decoded) rest
      in
      loop 0 [] values
  | _ -> Error (invalid path "expected JSON array")

(** Decodes nullable JSON with the supplied non-null decoder. *)
let nullable path decode = function
  | `Null -> Ok None
  | value ->
      let* value = decode path value in
      Ok (Some value)

(** Decodes one canonical base64 wrapper by delegating to the shared binary
    validator. *)
let bytes_wrapper path json =
  match Control.decode_payload (Yojson.Safe.to_string json) with
  | Ok value -> Ok value
  | Error error -> Error (of_control_error path error)

(** Builds the canonical base64 wrapper produced by the shared validator. *)
let bytes_wrapper_json bytes =
  match Control.encode_payload bytes with
  | Error error -> Error (of_control_error "$" error)
  | Ok encoded -> (
      match Control.decode_payload_object encoded with
      | Ok json -> Ok json
      | Error error -> Error (of_control_error "$" error))

(** Decodes one binary-safe Temporal payload, including arbitrary binary
    metadata values. *)
let payload path json =
  let* entries = exact_object path [ "metadata"; "data" ] json in
  let* metadata_json = field path "metadata" entries in
  let* metadata_entries =
    match metadata_json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid (path ^ ".metadata") "expected JSON object")
  in
  let rec decode_metadata decoded = function
    | [] -> Ok (List.rev decoded)
    | (key, value) :: rest ->
        if String.length key = 0 || String.length key > 65_536 then
          Error (invalid (path ^ ".metadata") "metadata key length is outside protocol limits")
        else
          let* bytes = bytes_wrapper (path ^ ".metadata." ^ key) value in
          decode_metadata ((key, bytes) :: decoded) rest
  in
  let* metadata = decode_metadata [] metadata_entries in
  let metadata =
    List.sort (fun (left, _) (right, _) -> String.compare left right) metadata
  in
  let* data_json = field path "data" entries in
  let* data = bytes_wrapper (path ^ ".data") data_json in
  Ok { metadata; data }

(** Encodes one payload without changing metadata key or byte content. *)
let payload_json (value : payload) =
  let rec metadata_json encoded = function
    | [] -> Ok (`Assoc (List.rev encoded))
    | (key, bytes) :: rest ->
        if String.length key = 0 || String.length key > 65_536 then
          Error (invalid "$.metadata" "metadata key length is outside protocol limits")
        else if List.exists (fun (existing, _) -> String.equal existing key) encoded
        then Error (invalid "$.metadata" "duplicate metadata key")
        else
          let* wrapped = bytes_wrapper_json bytes in
          metadata_json ((key, wrapped) :: encoded) rest
  in
  let* metadata = metadata_json [] value.metadata in
  let* data = bytes_wrapper_json value.data in
  Ok (`Assoc [ ("metadata", metadata); ("data", data) ])

(** Decodes a protobuf timestamp without applying wall-clock semantics. *)
let timestamp path json : (timestamp, error) result =
  let* entries = exact_object path [ "seconds"; "nanoseconds" ] json in
  let* seconds_json = field path "seconds" entries in
  let* seconds = int64 (path ^ ".seconds") seconds_json in
  let* nanos_json = field path "nanoseconds" entries in
  let* nanos = int64 (path ^ ".nanoseconds") nanos_json in
  if Int64.compare nanos 0L < 0 || Int64.compare nanos 999_999_999L > 0 then
    Error (invalid (path ^ ".nanoseconds") "nanoseconds are outside protobuf range")
  else
    let value : timestamp = { seconds; nanoseconds = Int64.to_int nanos } in
    Ok value

(** Decodes a normalized nonnegative protobuf duration. *)
let duration path json : (duration, error) result =
  let* value = timestamp path json in
  if Int64.compare value.seconds 0L < 0 then
    Error (invalid (path ^ ".seconds") "duration must not be negative")
  else
    let duration : duration =
      { seconds = value.seconds; nanoseconds = value.nanoseconds }
    in
    Ok duration

(** Encodes seconds/nanoseconds with no floating-point conversion. *)
let time_json seconds nanoseconds =
  `Assoc
    [
      ("seconds", `Intlit (Int64.to_string seconds));
      ("nanoseconds", `Int nanoseconds);
    ]

(** Maps the stable retry-state spelling used by the JSON contract. *)
let retry_state path = function
  | "unspecified" -> Ok Unspecified
  | "in_progress" -> Ok In_progress
  | "non_retryable_failure" -> Ok Non_retryable_failure
  | "timeout" -> Ok Timeout
  | "maximum_attempts_reached" -> Ok Maximum_attempts_reached
  | "retry_policy_not_set" -> Ok Retry_policy_not_set
  | "internal_server_error" -> Ok Internal_server_error
  | "cancel_requested" -> Ok Cancel_requested
  | _ -> Error (invalid path "unknown retry state")

(** Renders a retry state using the stable lowercase contract spelling. *)
let retry_state_string = function
  | Unspecified -> "unspecified"
  | In_progress -> "in_progress"
  | Non_retryable_failure -> "non_retryable_failure"
  | Timeout -> "timeout"
  | Maximum_attempts_reached -> "maximum_attempts_reached"
  | Retry_policy_not_set -> "retry_policy_not_set"
  | Internal_server_error -> "internal_server_error"
  | Cancel_requested -> "cancel_requested"

(** Decodes Core's exact timeout-policy spelling and rejects future values. *)
let timeout_type path = function
  | "unspecified" -> Ok Timeout_unspecified
  | "start_to_close" -> Ok Timeout_start_to_close
  | "schedule_to_start" -> Ok Timeout_schedule_to_start
  | "schedule_to_close" -> Ok Timeout_schedule_to_close
  | "heartbeat" -> Ok Timeout_heartbeat
  | _ -> Error (invalid path "unknown timeout type")

(** Renders a timeout policy using the stable lowercase protocol spelling. *)
let timeout_type_string = function
  | Timeout_unspecified -> "unspecified"
  | Timeout_start_to_close -> "start_to_close"
  | Timeout_schedule_to_start -> "schedule_to_start"
  | Timeout_schedule_to_close -> "schedule_to_close"
  | Timeout_heartbeat -> "heartbeat"

(** Decodes the supported closed failure-info union. *)
let failure_info path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "application" ->
      let* entries =
        exact_object path [ "kind"; "type"; "non_retryable"; "details" ] json
      in
      let* type_json = field path "type" entries in
      let* type_name = string (path ^ ".type") type_json in
      let* non_retryable_json = field path "non_retryable" entries in
      let* non_retryable = bool (path ^ ".non_retryable") non_retryable_json in
      let* details_json = field path "details" entries in
      let* details = list (path ^ ".details") payload details_json in
      Ok (Application { type_name; non_retryable; details })
  | "canceled" ->
      let* entries = exact_object path [ "kind"; "details"; "identity" ] json in
      let* details_json = field path "details" entries in
      let* details = list (path ^ ".details") payload details_json in
      let* identity_json = field path "identity" entries in
      let* identity = string (path ^ ".identity") identity_json in
      Ok (Canceled { details; identity })
  | "activity" ->
      let* entries =
        exact_object path
          [
            "kind";
            "scheduled_event_id";
            "started_event_id";
            "identity";
            "activity_type";
            "activity_id";
            "retry_state";
          ]
          json
      in
      let* scheduled_json = field path "scheduled_event_id" entries in
      let* scheduled_event_id =
        nonnegative_int64 (path ^ ".scheduled_event_id") scheduled_json
      in
      let* started_json = field path "started_event_id" entries in
      let* started_event_id =
        nonnegative_int64 (path ^ ".started_event_id") started_json
      in
      let* identity_json = field path "identity" entries in
      let* identity = string (path ^ ".identity") identity_json in
      let* activity_type_json = field path "activity_type" entries in
      let* activity_type = identifier (path ^ ".activity_type") activity_type_json in
      let* activity_id_json = field path "activity_id" entries in
      let* activity_id = identifier (path ^ ".activity_id") activity_id_json in
      let* retry_json = field path "retry_state" entries in
      let* retry_name = string (path ^ ".retry_state") retry_json in
      let* retry_state = retry_state (path ^ ".retry_state") retry_name in
      Ok
        (Activity
           {
             scheduled_event_id;
             started_event_id;
             identity;
             activity_type;
             activity_id;
             retry_state;
           })
  | "child_workflow" ->
      let* entries =
        exact_object path
          [
            "kind";
            "namespace";
            "workflow_id";
            "run_id";
            "workflow_type";
            "initiated_event_id";
            "started_event_id";
            "retry_state";
          ]
          json
      in
      let* namespace_json = field path "namespace" entries in
      let* namespace = identifier (path ^ ".namespace") namespace_json in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
      let* workflow_type_json = field path "workflow_type" entries in
      let* workflow_type = identifier (path ^ ".workflow_type") workflow_type_json in
      let* initiated_json = field path "initiated_event_id" entries in
      let* initiated_event_id = nonnegative_int64 (path ^ ".initiated_event_id") initiated_json in
      let* started_json = field path "started_event_id" entries in
      let* started_event_id = nonnegative_int64 (path ^ ".started_event_id") started_json in
      let* run_id_json = field path "run_id" entries in
      let* run_id =
        child_failure_run_id (path ^ ".run_id") ~started_event_id run_id_json
      in
      let* retry_json = field path "retry_state" entries in
      let* retry_name = string (path ^ ".retry_state") retry_json in
      let* retry_state = retry_state (path ^ ".retry_state") retry_name in
      Ok
        (Child_workflow
           {
             namespace;
             workflow_id;
             run_id;
             workflow_type;
             initiated_event_id;
             started_event_id;
             retry_state;
           })
  | "timeout" ->
      let* entries =
        exact_object path [ "kind"; "timeout_type"; "last_heartbeat_details" ]
          json
      in
      let* timeout_type_json = field path "timeout_type" entries in
      let* timeout_type_name = string (path ^ ".timeout_type") timeout_type_json in
      let* timeout_type = timeout_type (path ^ ".timeout_type") timeout_type_name in
      let* details_json = field path "last_heartbeat_details" entries in
      let* last_heartbeat_details =
        list (path ^ ".last_heartbeat_details") payload details_json
      in
      Ok (Timeout_failure { timeout_type; last_heartbeat_details })
  | _ -> Error (invalid (path ^ ".kind") "unsupported failure info kind")

(** Decodes a recursive failure. JSON depth limits bound cause recursion before
    this function executes. *)
let rec failure path json =
  let* entries =
    exact_object path
      [ "message"; "source"; "stack_trace"; "encoded_attributes"; "cause"; "info" ]
      json
  in
  let* message_json = field path "message" entries in
  let* message = string (path ^ ".message") message_json in
  let* source_json = field path "source" entries in
  let* source = string (path ^ ".source") source_json in
  let* stack_json = field path "stack_trace" entries in
  let* stack_trace = string (path ^ ".stack_trace") stack_json in
  let* encoded_json = field path "encoded_attributes" entries in
  let* encoded_attributes = nullable (path ^ ".encoded_attributes") payload encoded_json in
  let* cause_json = field path "cause" entries in
  let* cause = nullable (path ^ ".cause") failure cause_json in
  let* info_json = field path "info" entries in
  let* info = failure_info (path ^ ".info") info_json in
  Ok { message; source; stack_trace; encoded_attributes; cause; info }

(** Decodes the Core enum describing why a successor run was created. *)
let continuation_initiator path = function
  | "unspecified" -> Ok Continue_as_new_unspecified
  | "workflow" -> Ok Continue_as_new_workflow
  | "retry" -> Ok Continue_as_new_retry
  | "cron_schedule" -> Ok Continue_as_new_cron_schedule
  | _ -> Error (invalid path "unknown continue-as-new initiator")

(** Renders the Core continuation initiator using the closed JSON spelling. *)
let continuation_initiator_string = function
  | Continue_as_new_unspecified -> "unspecified"
  | Continue_as_new_workflow -> "workflow"
  | Continue_as_new_retry -> "retry"
  | Continue_as_new_cron_schedule -> "cron_schedule"

(** Decodes continuation provenance without collapsing optional failure or
    completion payloads into an absent value. *)
let continuation path json =
  let* entries =
    exact_object path
      [
        "continued_from_execution_run_id";
        "initiator";
        "continued_failure";
        "last_completion_result";
      ]
      json
  in
  let* run_id_json = field path "continued_from_execution_run_id" entries in
  let* continued_from_execution_run_id =
    identifier (path ^ ".continued_from_execution_run_id") run_id_json
  in
  let* initiator_json = field path "initiator" entries in
  let* initiator_name = string (path ^ ".initiator") initiator_json in
  let* initiator = continuation_initiator (path ^ ".initiator") initiator_name in
  let* failure_json = field path "continued_failure" entries in
  let* continued_failure = nullable (path ^ ".continued_failure") failure failure_json in
  let* result_json = field path "last_completion_result" entries in
  let* last_completion_result =
    nullable (path ^ ".last_completion_result")
      (fun list_path list_json -> list list_path payload list_json)
      result_json
  in
  Ok
    {
      continued_from_execution_run_id;
      initiator;
      continued_failure;
      last_completion_result;
    }

(** Encodes the supported failure-info union. *)
let rec failure_info_json = function
  | Application { type_name; non_retryable; details } ->
      let* details = payloads_json details in
      Ok
        (`Assoc
          [
            ("kind", `String "application");
            ("type", `String type_name);
            ("non_retryable", `Bool non_retryable);
            ("details", details);
          ])
  | Canceled { details; identity } ->
      let* details = payloads_json details in
      Ok
        (`Assoc
          [
            ("kind", `String "canceled");
            ("details", details);
            ("identity", `String identity);
          ])
  | Activity
      {
        scheduled_event_id;
        started_event_id;
        identity;
        activity_type;
        activity_id;
        retry_state;
      } ->
      Ok
        (`Assoc
          [
            ("kind", `String "activity");
            ("scheduled_event_id", `Intlit (Int64.to_string scheduled_event_id));
            ("started_event_id", `Intlit (Int64.to_string started_event_id));
            ("identity", `String identity);
            ("activity_type", `String activity_type);
            ("activity_id", `String activity_id);
            ("retry_state", `String (retry_state_string retry_state));
          ])
  | Child_workflow
      {
        namespace;
        workflow_id;
        run_id;
        workflow_type;
        initiated_event_id;
        started_event_id;
        retry_state;
      } ->
      Ok
        (`Assoc
          [
            ("kind", `String "child_workflow");
            ("namespace", `String namespace);
            ("workflow_id", `String workflow_id);
            ("run_id", `String run_id);
            ("workflow_type", `String workflow_type);
            ("initiated_event_id", `Intlit (Int64.to_string initiated_event_id));
            ("started_event_id", `Intlit (Int64.to_string started_event_id));
            ("retry_state", `String (retry_state_string retry_state));
          ])
  | Timeout_failure { timeout_type; last_heartbeat_details } ->
      let* last_heartbeat_details = payloads_json last_heartbeat_details in
      Ok
        (`Assoc
          [
            ("kind", `String "timeout");
            ("timeout_type", `String (timeout_type_string timeout_type));
            ("last_heartbeat_details", last_heartbeat_details);
          ])

(** Encodes a payload list in source order. *)
and payloads_json values =
  let rec loop encoded = function
    | [] -> Ok (`List (List.rev encoded))
    | value :: rest ->
        let* value = payload_json value in
        loop (value :: encoded) rest
  in
  loop [] values

(** Encodes a recursive failure without exposing it to generic serializers. *)
and failure_json value =
  let* encoded_attributes =
    match value.encoded_attributes with
    | None -> Ok `Null
    | Some value -> payload_json value
  in
  let* cause =
    match value.cause with None -> Ok `Null | Some value -> failure_json value
  in
  let* info = failure_info_json value.info in
  Ok
    (`Assoc
      [
        ("message", `String value.message);
        ("source", `String value.source);
        ("stack_trace", `String value.stack_trace);
        ("encoded_attributes", encoded_attributes);
        ("cause", cause);
        ("info", info);
      ])

(** Decodes the activity-resolution oneof. *)
let activity_resolution path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "completed" ->
      let* entries = exact_object path [ "kind"; "payload" ] json in
      let* payload_json = field path "payload" entries in
      let* value = nullable (path ^ ".payload") payload payload_json in
      Ok (Completed value)
  | "failed" | "cancelled" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* value = failure (path ^ ".failure") failure_json in
      if String.equal kind "failed" then Ok (Failed value) else Ok (Cancelled value)
  | "backoff" ->
      let* entries =
        exact_object path
          [ "kind"; "attempt"; "backoff_duration"; "original_schedule_time" ]
          json
      in
      let* attempt_json = field path "attempt" entries in
      let* attempt = uint32 (path ^ ".attempt") attempt_json in
      if Int64.equal attempt 0L then
        Error (invalid (path ^ ".attempt") "local activity backoff attempt must be positive")
      else
        let* duration_json = field path "backoff_duration" entries in
        let* backoff_duration = duration (path ^ ".backoff_duration") duration_json in
        let* original_json = field path "original_schedule_time" entries in
        let* original_schedule_time =
          nullable (path ^ ".original_schedule_time") timestamp original_json
        in
        Ok (Backoff { attempt; backoff_duration; original_schedule_time })
  | _ -> Error (invalid (path ^ ".kind") "unknown activity resolution kind")

(** Encodes the activity-resolution oneof. *)
let activity_resolution_json = function
  | Completed value ->
      let* payload =
        match value with None -> Ok `Null | Some value -> payload_json value
      in
      Ok (`Assoc [ ("kind", `String "completed"); ("payload", payload) ])
  | Failed value ->
      let* failure = failure_json value in
      Ok (`Assoc [ ("kind", `String "failed"); ("failure", failure) ])
  | Cancelled value ->
      let* failure = failure_json value in
      Ok (`Assoc [ ("kind", `String "cancelled"); ("failure", failure) ])
  | Backoff { attempt; backoff_duration; original_schedule_time } ->
      Ok
        (`Assoc
          [ ("kind", `String "backoff");
            ("attempt", `Intlit (Int64.to_string attempt));
            ("backoff_duration", time_json backoff_duration.seconds backoff_duration.nanoseconds);
            ( "original_schedule_time",
              match original_schedule_time with
              | None -> `Null
              | Some value -> time_json value.seconds value.nanoseconds ) ])

(** Decodes the closed query-answer union. Query success requires one payload;
    query failure retains Core's structured failure for exact error delivery. *)
let query_result path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "succeeded" ->
      let* entries = exact_object path [ "kind"; "payload" ] json in
      let* payload_json = field path "payload" entries in
      let* payload = payload (path ^ ".payload") payload_json in
      Ok (Query_succeeded payload)
  | "failed" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* failure = failure (path ^ ".failure") failure_json in
      Ok (Query_failed failure)
  | _ -> Error (invalid (path ^ ".kind") "unknown query result kind")

(** Encodes a query answer without changing the result payload or failure. *)
let query_result_json = function
  | Query_succeeded payload ->
      let* payload = payload_json payload in
      Ok (`Assoc [ ("kind", `String "succeeded"); ("payload", payload) ])
  | Query_failed failure ->
      let* failure = failure_json failure in
      Ok (`Assoc [ ("kind", `String "failed"); ("failure", failure) ])

(** Decodes metadata attached to one update request. Temporal repeats the
    update identifier in the enclosing message and in this nested object;
    comparing them is done by the enclosing activation-job decoder. *)
let update_meta path json =
  let* entries = exact_object path [ "identity"; "update_id" ] json in
  let* identity_json = field path "identity" entries in
  let* identity = bounded_text (path ^ ".identity") identity_json in
  let* update_id_json = field path "update_id" entries in
  let* update_id = identifier (path ^ ".update_id") update_id_json in
  Ok { identity; update_id }

(** Encodes update metadata with its stable field names and no extension
    members, matching the strict bridge schema. *)
let update_meta_json { identity; update_id } =
  Ok
    (`Assoc
      [ ("identity", `String identity); ("update_id", `String update_id) ])

(** Decodes one update response phase. The accepted response carries no
    payload; rejected and completed responses carry exactly one value. *)
let update_response path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "accepted" ->
      let* _ = exact_object path [ "kind" ] json in
      Ok Update_accepted
  | "rejected" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* failure = failure (path ^ ".failure") failure_json in
      Ok (Update_rejected failure)
  | "completed" ->
      let* entries = exact_object path [ "kind"; "payload" ] json in
      let* payload_json = field path "payload" entries in
      let* payload = payload (path ^ ".payload") payload_json in
      Ok (Update_completed payload)
  | _ -> Error (invalid (path ^ ".kind") "unknown update response kind")

(** Encodes one update response phase without silently dropping a failure or
    completion payload. *)
let update_response_json = function
  | Update_accepted -> Ok (`Assoc [ ("kind", `String "accepted") ])
  | Update_rejected failure ->
      let* failure = failure_json failure in
      Ok (`Assoc [ ("kind", `String "rejected"); ("failure", failure) ])
  | Update_completed payload ->
      let* payload = payload_json payload in
      Ok (`Assoc [ ("kind", `String "completed"); ("payload", payload) ])

(** Decodes the initial child-workflow start result. A successful start carries
    only the run identity; the child remains pending until a later terminal
    child resolution is delivered. *)
let child_workflow_start_failure_cause path = function
  | "unspecified" -> Ok Child_start_unspecified
  | "workflow_already_exists" -> Ok Child_start_workflow_already_exists
  | _ -> Error (invalid path "unknown child workflow start failure cause")

let child_workflow_start_failure_cause_string = function
  | Child_start_unspecified -> "unspecified"
  | Child_start_workflow_already_exists -> "workflow_already_exists"

let child_workflow_start_resolution path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "succeeded" ->
      let* entries = exact_object path [ "kind"; "run_id" ] json in
      let* run_id_json = field path "run_id" entries in
      let* run_id = identifier (path ^ ".run_id") run_id_json in
      Ok (Child_start_succeeded run_id)
  | "failed" ->
      let* entries =
        exact_object path [ "kind"; "workflow_id"; "workflow_type"; "cause" ] json
      in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
      let* workflow_type_json = field path "workflow_type" entries in
      let* workflow_type = identifier (path ^ ".workflow_type") workflow_type_json in
      let* cause_json = field path "cause" entries in
      let* cause_name = string (path ^ ".cause") cause_json in
      let* cause = child_workflow_start_failure_cause (path ^ ".cause") cause_name in
      Ok (Child_start_failed { workflow_id; workflow_type; cause })
  | "cancelled" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* failure = failure (path ^ ".failure") failure_json in
      Ok (Child_start_cancelled failure)
  | _ -> Error (invalid (path ^ ".kind") "unknown child workflow start resolution kind")

let child_workflow_start_resolution_json = function
  | Child_start_succeeded run_id ->
      Ok (`Assoc [ ("kind", `String "succeeded"); ("run_id", `String run_id) ])
  | Child_start_failed { workflow_id; workflow_type; cause } ->
      Ok
        (`Assoc
          [
            ("kind", `String "failed");
            ("workflow_id", `String workflow_id);
            ("workflow_type", `String workflow_type);
            ("cause", `String (child_workflow_start_failure_cause_string cause));
          ])
  | Child_start_cancelled failure ->
      let* failure = failure_json failure in
      Ok (`Assoc [ ("kind", `String "cancelled"); ("failure", failure) ])

(** Decodes a terminal child-workflow result. *)
let child_workflow_resolution path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "completed" ->
      let* entries = exact_object path [ "kind"; "payload" ] json in
      let* payload_json = field path "payload" entries in
      let* payload = nullable (path ^ ".payload") payload payload_json in
      Ok (Child_completed payload)
  | "failed" | "cancelled" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* failure = failure (path ^ ".failure") failure_json in
      if String.equal kind "failed" then Ok (Child_failed failure)
      else Ok (Child_cancelled failure)
  | _ -> Error (invalid (path ^ ".kind") "unknown child workflow resolution kind")

let child_workflow_resolution_json = function
  | Child_completed payload ->
      let* payload =
        match payload with None -> Ok `Null | Some value -> payload_json value
      in
      Ok (`Assoc [ ("kind", `String "completed"); ("payload", payload) ])
  | Child_failed failure ->
      let* failure = failure_json failure in
      Ok (`Assoc [ ("kind", `String "failed"); ("failure", failure) ])
  | Child_cancelled failure ->
      let* failure = failure_json failure in
      Ok (`Assoc [ ("kind", `String "cancelled"); ("failure", failure) ])

(** Decodes the nullable-failure result Core uses for external operations. *)
let external_workflow_resolution path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "succeeded" ->
      let* _ = exact_object path [ "kind" ] json in
      Ok (Ok ())
  | "failed" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* failure = failure (path ^ ".failure") failure_json in
      Ok (Error failure)
  | _ -> Error (invalid (path ^ ".kind") "unknown external workflow resolution kind")

(** Encodes one external operation acknowledgement without dropping Core's
    structured failure. *)
let external_workflow_resolution_json = function
  | Ok () -> Ok (`Assoc [ ("kind", `String "succeeded") ])
  | Error failure ->
      let* failure = failure_json failure in
      Ok (`Assoc [ ("kind", `String "failed"); ("failure", failure) ])

(** Maps a stable cache-eviction spelling. *)
let eviction_reason path = function
  | "unspecified" -> Ok Eviction_unspecified
  | "cache_full" -> Ok Cache_full
  | "cache_miss" -> Ok Cache_miss
  | "nondeterminism" -> Ok Nondeterminism
  | "lang_fail" -> Ok Lang_fail
  | "lang_requested" -> Ok Lang_requested
  | "task_not_found" -> Ok Task_not_found
  | "unhandled_command" -> Ok Unhandled_command
  | "fatal" -> Ok Fatal
  | "pagination_or_history_fetch" -> Ok Pagination_or_history_fetch
  | "workflow_execution_ending" -> Ok Workflow_execution_ending
  | _ -> Error (invalid path "unknown cache eviction reason")

(** Renders a cache-eviction reason. *)
let eviction_reason_string = function
  | Eviction_unspecified -> "unspecified"
  | Cache_full -> "cache_full"
  | Cache_miss -> "cache_miss"
  | Nondeterminism -> "nondeterminism"
  | Lang_fail -> "lang_fail"
  | Lang_requested -> "lang_requested"
  | Task_not_found -> "task_not_found"
  | Unhandled_command -> "unhandled_command"
  | Fatal -> "fatal"
  | Pagination_or_history_fetch -> "pagination_or_history_fetch"
  | Workflow_execution_ending -> "workflow_execution_ending"

(** Decodes a canonical object map whose values are Temporal payloads. *)
let payload_map path = function
  | `Assoc entries ->
      let rec loop decoded = function
        | [] -> Ok (List.rev decoded)
        | (key, value) :: rest ->
            let* key = identifier (path ^ ".key") (`String key) in
            if List.exists (fun (existing, _) -> String.equal existing key) decoded then
              Error (invalid path "duplicate payload-map key")
            else
              let* value = payload (path ^ "." ^ key) value in
              loop ((key, value) :: decoded) rest
      in
      let* values = loop [] entries in
      Ok (List.sort (fun (left, _) (right, _) -> String.compare left right) values)
  | _ -> Error (invalid path "expected JSON object")

(** Encodes a payload map and rejects duplicate keys before canonical sorting. *)
let payload_map_json path values =
  let rec loop encoded = function
    | [] -> Ok (`Assoc (List.rev encoded))
    | (key, value) :: rest ->
        let* key = identifier (path ^ ".key") (`String key) in
        if List.exists (fun (existing, _) -> String.equal existing key) encoded then
          Error (invalid path "duplicate payload-map key")
        else
          let* value = payload_json value in
          loop ((key, value) :: encoded) rest
  in
  loop [] values

(** Decodes a workflow/run pair used for a child workflow's root identity. *)
let workflow_execution path json =
  let* entries = exact_object path [ "workflow_id"; "run_id" ] json in
  let* workflow_id_json = field path "workflow_id" entries in
  let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
  let* run_id_json = field path "run_id" entries in
  let* run_id = identifier (path ^ ".run_id") run_id_json in
  Ok { workflow_id; run_id }

(** Decodes a parent workflow identity including its namespace. *)
let namespaced_workflow_execution path json =
  let* entries = exact_object path [ "namespace"; "workflow_id"; "run_id" ] json in
  let* namespace_json = field path "namespace" entries in
  let* namespace = identifier (path ^ ".namespace") namespace_json in
  let* workflow_id_json = field path "workflow_id" entries in
  let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
  let* run_id_json = field path "run_id" entries in
  let* run_id = identifier (path ^ ".run_id") run_id_json in
  Ok { namespace; workflow_id; run_id }

(** Decodes exact workflow priority, retaining the raw [f32] weight bits. *)
let workflow_priority path json =
  let* entries = exact_object path [ "priority_key"; "fairness_key"; "fairness_weight_bits" ] json in
  let* key_json = field path "priority_key" entries in
  let* priority_key = int32 (path ^ ".priority_key") key_json in
  if priority_key < 0 then
    Error (invalid (path ^ ".priority_key") "priority key cannot be negative")
  else
  let* fairness_json = field path "fairness_key" entries in
  let* fairness_key = string (path ^ ".fairness_key") fairness_json in
  if String.length fairness_key > 64 then Error (invalid (path ^ ".fairness_key") "fairness key exceeds Core's 64-byte limit")
  else if String.contains fairness_key '\000' then
    Error (invalid (path ^ ".fairness_key") "fairness key contains a NUL byte")
  else if not (valid_utf_8 fairness_key) then
    Error (invalid (path ^ ".fairness_key") "fairness key is not valid UTF-8")
  else
    let* bits_json = field path "fairness_weight_bits" entries in
    let* fairness_weight_bits = uint32 (path ^ ".fairness_weight_bits") bits_json in
    let weight = Int32.float_of_bits (Int64.to_int32 fairness_weight_bits) in
    if
      not (Float.is_finite weight)
      || (weight <> 0. && (weight < 0.001 || weight > 1000.))
    then
      Error
        (invalid (path ^ ".fairness_weight_bits")
           "fairness weight must be zero or finite in [0.001, 1000]")
    else Ok { priority_key; fairness_key; fairness_weight_bits }

(** Decodes the canonical unsigned decimal used for an IEEE-754 bit pattern.
    The two limbs keep every intermediate value below [2^64] while avoiding
    machine-sized integers and preserving patterns whose sign bit is set. *)
let uint64_bits path json =
  let* text = uint64_decimal path json in
  let base = 4_294_967_296L in
  let rec loop index high low =
    if index = String.length text then
      if Int64.compare high 0xffff_ffffL > 0 then
        Error (invalid path "value is outside unsigned 64-bit range")
      else
        Ok
          (Int64.logor (Int64.shift_left high 32)
             (Int64.logand low 0xffff_ffffL))
    else
      let digit = Char.code text.[index] - Char.code '0' in
      let product =
        Int64.add (Int64.mul low 10L) (Int64.of_int digit)
      in
      let next_low = Int64.rem product base in
      let carry = Int64.div product base in
      let next_high = Int64.add (Int64.mul high 10L) carry in
      loop (index + 1) next_high next_low
  in
  loop 0 0L 0L

(** Validates a coefficient's exact bit representation and returns the
    canonical decimal text retained by the semantic record. *)
let retry_coefficient_bits path json =
  let* text = uint64_decimal path json in
  let* bits = uint64_bits path json in
  let value = Int64.float_of_bits bits in
  match classify_float value with
  | FP_nan | FP_infinite ->
      Error (invalid path "backoff coefficient must be finite")
  | FP_zero | FP_subnormal | FP_normal when value < 1.0 ->
      Error (invalid path "backoff coefficient must be at least 1.0")
  | FP_zero | FP_subnormal | FP_normal ->
      Ok text

(** Compares normalized durations without converting to floating point. *)
let compare_duration left right =
  match Int64.compare left.seconds right.seconds with
  | 0 -> Int.compare left.nanoseconds right.nanoseconds
  | value -> value

(** Decodes and validates the retry policy attached to an activity command.
    The same semantic policy is also carried by workflow initialization, so
    this decoder must be defined before either protocol path uses it. *)
let retry_policy path json =
  let* entries =
    exact_object path
      [
        "initial_interval";
        "backoff_coefficient_bits";
        "maximum_interval";
        "maximum_attempts";
        "non_retryable_error_types";
      ] json
  in
  let* initial_json = field path "initial_interval" entries in
  let* initial_interval = duration (path ^ ".initial_interval") initial_json in
  let* maximum_json = field path "maximum_interval" entries in
  let* maximum_interval = duration (path ^ ".maximum_interval") maximum_json in
  if
    Int64.equal initial_interval.seconds 0L
    && initial_interval.nanoseconds = 0
  then Error (invalid (path ^ ".initial_interval") "duration must be positive")
  else if compare_duration maximum_interval initial_interval < 0 then
    Error
      (invalid (path ^ ".maximum_interval")
         "maximum interval must be at least initial interval")
  else
    let* coefficient_json = field path "backoff_coefficient_bits" entries in
    let* backoff_coefficient_bits =
      retry_coefficient_bits (path ^ ".backoff_coefficient_bits") coefficient_json
    in
    let* attempts_json = field path "maximum_attempts" entries in
    let* maximum_attempts = int64 (path ^ ".maximum_attempts") attempts_json in
    if
      Int64.compare maximum_attempts 0L < 0
      || Int64.compare maximum_attempts 2_147_483_647L > 0
    then
      Error
        (invalid (path ^ ".maximum_attempts")
           "maximum attempts is outside signed 32-bit range")
    else
      let* error_types_json =
        field path "non_retryable_error_types" entries
      in
      let* non_retryable_error_types =
        list (path ^ ".non_retryable_error_types") string error_types_json
      in
      let rec validate_error_types index = function
        | [] -> Ok ()
        | value :: rest ->
            let item_path =
              Printf.sprintf "%s.non_retryable_error_types[%d]" path index
            in
            if String.equal value "" then
              Error (invalid item_path "error type must not be empty")
            else if String.contains value '\000' then
              Error (invalid item_path "error type must not contain NUL")
            else if not (valid_utf_8 value) then
              Error (invalid item_path "error type must be valid UTF-8")
            else validate_error_types (index + 1) rest
      in
      let* () = validate_error_types 0 non_retryable_error_types in
      Ok
        {
          initial_interval;
          backoff_coefficient_bits;
          maximum_interval;
          maximum_attempts = Int64.to_int maximum_attempts;
          non_retryable_error_types;
        }

(** Decodes the normal fields attached to a first workflow initialization. *)
let initialize_context path json =
  let fields =
    [
      "headers";
      "identity";
      "parent_workflow";
      "workflow_execution_timeout";
      "workflow_run_timeout";
      "workflow_task_timeout";
      "first_execution_run_id";
      "start_time";
      "root_workflow";
      "priority";
      "retry_policy";
      "continuation";
    ]
  in
  let* entries = exact_object path fields json in
  let* headers_json = field path "headers" entries in
  let* headers = payload_map (path ^ ".headers") headers_json in
  let* identity_json = field path "identity" entries in
  let* identity = string (path ^ ".identity") identity_json in
  let* parent_json = field path "parent_workflow" entries in
  let* parent_workflow = nullable (path ^ ".parent_workflow") namespaced_workflow_execution parent_json in
  let timeout name =
    let* value = field path name entries in
    nullable (path ^ "." ^ name) duration value
  in
  let* workflow_execution_timeout = timeout "workflow_execution_timeout" in
  let* workflow_run_timeout = timeout "workflow_run_timeout" in
  let* workflow_task_timeout = timeout "workflow_task_timeout" in
  let* first_json = field path "first_execution_run_id" entries in
  let* first_execution_run_id = identifier (path ^ ".first_execution_run_id") first_json in
  let* start_json = field path "start_time" entries in
  let* start_time = nullable (path ^ ".start_time") timestamp start_json in
  let* root_json = field path "root_workflow" entries in
  let* root_workflow = nullable (path ^ ".root_workflow") workflow_execution root_json in
  let* priority_json = field path "priority" entries in
  let* priority = nullable (path ^ ".priority") workflow_priority priority_json in
  let* retry_policy_json = field path "retry_policy" entries in
  let* retry_policy = nullable (path ^ ".retry_policy") retry_policy retry_policy_json in
  let* continuation_json = field path "continuation" entries in
  let* continuation = nullable (path ^ ".continuation") continuation continuation_json in
  Ok
    {
      headers;
      identity;
      parent_workflow;
      workflow_execution_timeout;
      workflow_run_timeout;
      workflow_task_timeout;
      first_execution_run_id;
      start_time;
      root_workflow;
      priority;
      retry_policy;
      continuation;
    }

(** Encodes a retry policy and immediately validates its representation by
    passing the generated object through the same decoder used for input. *)
let retry_policy_json value =
  let json =
    `Assoc
      [
        ( "initial_interval",
          time_json value.initial_interval.seconds value.initial_interval.nanoseconds );
        ( "backoff_coefficient_bits",
          `String value.backoff_coefficient_bits );
        ( "maximum_interval",
          time_json value.maximum_interval.seconds value.maximum_interval.nanoseconds );
        ( "maximum_attempts",
          `Intlit (Int64.to_string (Int64.of_int value.maximum_attempts)) );
        ( "non_retryable_error_types",
          `List (List.map (fun value -> `String value) value.non_retryable_error_types) );
      ]
  in
  let* _ = retry_policy "$.retry_policy" json in
  Ok json

(** Encodes a first-workflow initialization context. *)
let initialize_context_json value =
  let* headers = payload_map_json "$.context.headers" value.headers in
  let optional_time = function
    | None -> `Null
    | Some value -> time_json value.seconds value.nanoseconds
  in
  let parent_workflow = match value.parent_workflow with
    | None -> `Null
    | Some value -> `Assoc [ ("namespace", `String value.namespace); ("workflow_id", `String value.workflow_id); ("run_id", `String value.run_id) ]
  in
  let root_workflow = match value.root_workflow with
    | None -> `Null
    | Some value -> `Assoc [ ("workflow_id", `String value.workflow_id); ("run_id", `String value.run_id) ]
  in
  let priority = match value.priority with
    | None -> `Null
    | Some value -> `Assoc [ ("priority_key", `Int value.priority_key); ("fairness_key", `String value.fairness_key); ("fairness_weight_bits", `Intlit (Int64.to_string value.fairness_weight_bits)) ]
  in
  let* retry_policy =
    match value.retry_policy with
    | None -> Ok `Null
    | Some value -> retry_policy_json value
  in
  let* continuation =
    match value.continuation with
    | None -> Ok `Null
    | Some value ->
        let* continued_failure =
          match value.continued_failure with
          | None -> Ok `Null
          | Some failure -> failure_json failure
        in
        let* last_completion_result =
          match value.last_completion_result with
          | None -> Ok `Null
          | Some values -> payloads_json values
        in
        Ok
          (`Assoc
            [
              ( "continued_from_execution_run_id",
                `String value.continued_from_execution_run_id );
              ( "initiator",
                `String (continuation_initiator_string value.initiator) );
              ("continued_failure", continued_failure);
              ("last_completion_result", last_completion_result);
            ])
  in
  Ok
    (`Assoc
      [
        ("headers", headers);
        ("identity", `String value.identity);
        ("parent_workflow", parent_workflow);
        ("workflow_execution_timeout", optional_time value.workflow_execution_timeout);
        ("workflow_run_timeout", optional_time value.workflow_run_timeout);
        ("workflow_task_timeout", optional_time value.workflow_task_timeout);
        ("first_execution_run_id", `String value.first_execution_run_id);
        ( "start_time",
          match value.start_time with
          | None -> `Null
          | Some value -> time_json value.seconds value.nanoseconds );
        ("root_workflow", root_workflow);
        ("priority", priority);
        ("retry_policy", retry_policy);
        ("continuation", continuation);
      ])

(** Maps a stable continue-as-new suggestion spelling. *)
let suggestion_reason path = function
  | "unspecified" -> Ok Suggest_unspecified
  | "history_size_too_large" -> Ok History_size_too_large
  | "too_many_history_events" -> Ok Too_many_history_events
  | "too_many_updates" -> Ok Too_many_updates
  | _ -> Error (invalid path "unknown continue-as-new suggestion reason")

(** Renders the stable continue-as-new suggestion spelling. *)
let suggestion_reason_string = function
  | Suggest_unspecified -> "unspecified"
  | History_size_too_large -> "history_size_too_large"
  | Too_many_history_events -> "too_many_history_events"
  | Too_many_updates -> "too_many_updates"

(** Decodes activation metadata required by normal SDK language layers. *)
let activation_metadata path json =
  let fields =
    [
      "available_internal_flags";
      "history_size_bytes";
      "continue_as_new_suggested";
      "deployment_version_for_current_task";
      "last_sdk_version";
      "suggest_continue_as_new_reasons";
      "target_worker_deployment_version_changed";
    ]
  in
  let* entries = exact_object path fields json in
  let* flags_json = field path "available_internal_flags" entries in
  let* available_internal_flags = list (path ^ ".available_internal_flags") uint32 flags_json in
  let* size_json = field path "history_size_bytes" entries in
  let* history_size_bytes = uint64_decimal (path ^ ".history_size_bytes") size_json in
  let* suggested_json = field path "continue_as_new_suggested" entries in
  let* continue_as_new_suggested = bool (path ^ ".continue_as_new_suggested") suggested_json in
  let deployment path json =
    let* entries = exact_object path [ "deployment_name"; "build_id" ] json in
    let* name_json = field path "deployment_name" entries in
    let* deployment_name = string (path ^ ".deployment_name") name_json in
    let* build_json = field path "build_id" entries in
    let* build_id = identifier (path ^ ".build_id") build_json in
    Ok { deployment_name; build_id }
  in
  let* deployment_json = field path "deployment_version_for_current_task" entries in
  let* deployment_version_for_current_task = nullable (path ^ ".deployment_version_for_current_task") deployment deployment_json in
  let* sdk_json = field path "last_sdk_version" entries in
  let* last_sdk_version = string (path ^ ".last_sdk_version") sdk_json in
  let* reasons_json = field path "suggest_continue_as_new_reasons" entries in
  let reason path json = let* value = string path json in suggestion_reason path value in
  let* suggest_continue_as_new_reasons = list (path ^ ".suggest_continue_as_new_reasons") reason reasons_json in
  let* changed_json = field path "target_worker_deployment_version_changed" entries in
  let* target_worker_deployment_version_changed = bool (path ^ ".target_worker_deployment_version_changed") changed_json in
  Ok { available_internal_flags; history_size_bytes; continue_as_new_suggested; deployment_version_for_current_task; last_sdk_version; suggest_continue_as_new_reasons; target_worker_deployment_version_changed }

(** Encodes activation metadata without narrowing uint64 history size. *)
let activation_metadata_json value =
  let deployment = match value.deployment_version_for_current_task with
    | None -> `Null
    | Some value -> `Assoc [ ("deployment_name", `String value.deployment_name); ("build_id", `String value.build_id) ]
  in
  Ok (`Assoc [
    ("available_internal_flags", `List (List.map (fun value -> `Intlit (Int64.to_string value)) value.available_internal_flags));
    ("history_size_bytes", `String value.history_size_bytes);
    ("continue_as_new_suggested", `Bool value.continue_as_new_suggested);
    ("deployment_version_for_current_task", deployment);
    ("last_sdk_version", `String value.last_sdk_version);
    ("suggest_continue_as_new_reasons", `List (List.map (fun value -> `String (suggestion_reason_string value)) value.suggest_continue_as_new_reasons));
    ("target_worker_deployment_version_changed", `Bool value.target_worker_deployment_version_changed)
  ])

(** Decodes one supported activation job after reading its discriminator. *)
let activation_job path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "initialize_workflow" ->
      let has_context = List.mem_assoc "context" entries in
      let* entries =
        exact_object path
          ([ "kind"; "workflow_id"; "workflow_type"; "arguments"; "randomness_seed"; "attempt" ]
          @ if has_context then [ "context" ] else [])
          json
      in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
      let* workflow_type_json = field path "workflow_type" entries in
      let* workflow_type = identifier (path ^ ".workflow_type") workflow_type_json in
      let* arguments_json = field path "arguments" entries in
      let* arguments = list (path ^ ".arguments") payload arguments_json in
      let* seed_json = field path "randomness_seed" entries in
      let* randomness_seed = uint64_decimal (path ^ ".randomness_seed") seed_json in
      let* attempt_json = field path "attempt" entries in
      let* attempt = positive_int32 (path ^ ".attempt") attempt_json in
      let* context =
        if has_context then
          let* value = field path "context" entries in
          let* value = initialize_context (path ^ ".context") value in
          Ok (Some value)
        else Ok None
      in
      Ok
        (Initialize_workflow
           { workflow_id; workflow_type; arguments; randomness_seed; attempt; context })
  | "resolve_activity" ->
      let* entries = exact_object path [ "kind"; "seq"; "result" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* result_json = field path "result" entries in
      let* result = activity_resolution (path ^ ".result") result_json in
      Ok (Resolve_activity { seq; result })
  | "resolve_child_workflow_start" ->
      let* entries = exact_object path [ "kind"; "seq"; "result" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* result_json = field path "result" entries in
      let* result =
        child_workflow_start_resolution (path ^ ".result") result_json
      in
      Ok (Resolve_child_workflow_start { seq; result })
  | "resolve_child_workflow" ->
      let* entries = exact_object path [ "kind"; "seq"; "result" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* result_json = field path "result" entries in
      let* result = child_workflow_resolution (path ^ ".result") result_json in
      Ok (Resolve_child_workflow { seq; result })
  | "resolve_signal_external_workflow" ->
      let* entries = exact_object path [ "kind"; "seq"; "result" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* result_json = field path "result" entries in
      let* result = external_workflow_resolution (path ^ ".result") result_json in
      Ok (Resolve_signal_external_workflow { seq; result })
  | "resolve_request_cancel_external_workflow" ->
      let* entries = exact_object path [ "kind"; "seq"; "result" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* result_json = field path "result" entries in
      let* result = external_workflow_resolution (path ^ ".result") result_json in
      Ok (Resolve_request_cancel_external_workflow { seq; result })
  | "query_workflow" ->
      let* entries =
        exact_object path [ "kind"; "query_id"; "query_type"; "arguments"; "headers" ]
          json
      in
      let* query_id_json = field path "query_id" entries in
      let* query_id = identifier (path ^ ".query_id") query_id_json in
      let* query_type_json = field path "query_type" entries in
      let* query_type = identifier (path ^ ".query_type") query_type_json in
      let* arguments_json = field path "arguments" entries in
      let* arguments = list (path ^ ".arguments") payload arguments_json in
      let* headers_json = field path "headers" entries in
      let* headers = payload_map (path ^ ".headers") headers_json in
      Ok (Query_workflow { query_id; query_type; arguments; headers })
  | "do_update" ->
      let* entries =
        exact_object path
          [ "kind"; "id"; "protocol_instance_id"; "name"; "input"; "headers";
            "meta"; "run_validator" ]
          json
      in
      let* id_json = field path "id" entries in
      let* id = identifier (path ^ ".id") id_json in
      let* protocol_json = field path "protocol_instance_id" entries in
      let* protocol_instance_id =
        identifier (path ^ ".protocol_instance_id") protocol_json
      in
      let* name_json = field path "name" entries in
      let* name = identifier (path ^ ".name") name_json in
      let* input_json = field path "input" entries in
      let* input = list (path ^ ".input") payload input_json in
      let* headers_json = field path "headers" entries in
      let* headers = payload_map (path ^ ".headers") headers_json in
      let* meta_json = field path "meta" entries in
      let* meta = update_meta (path ^ ".meta") meta_json in
      let* run_validator_json = field path "run_validator" entries in
      let* run_validator = bool (path ^ ".run_validator") run_validator_json in
      if not (String.equal id meta.update_id) then
        Error (invalid (path ^ ".meta.update_id") "must match update id")
      else Ok (Do_update { id; protocol_instance_id; name; input; headers; meta; run_validator })
  | "signal_workflow" ->
      let* entries =
        exact_object path [ "kind"; "signal_name"; "input"; "identity"; "headers" ]
          json
      in
      let* signal_name_json = field path "signal_name" entries in
      let* signal_name = identifier (path ^ ".signal_name") signal_name_json in
      let* input_json = field path "input" entries in
      let* input = list (path ^ ".input") payload input_json in
      let* identity_json = field path "identity" entries in
      let* identity = bounded_text (path ^ ".identity") identity_json in
      let* headers_json = field path "headers" entries in
      let* headers = payload_map (path ^ ".headers") headers_json in
      Ok (Signal_workflow { signal_name; input; identity; headers })
  | "notify_has_patch" ->
      let* entries = exact_object path [ "kind"; "patch_id" ] json in
      let* patch_id_json = field path "patch_id" entries in
      let* patch_id = identifier (path ^ ".patch_id") patch_id_json in
      Ok (Notify_has_patch { patch_id })
  | "fire_timer" ->
      let* entries = exact_object path [ "kind"; "seq" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      Ok (Fire_timer { seq })
  | "cancel_workflow" ->
      let* entries = exact_object path [ "kind"; "reason" ] json in
      let* reason_json = field path "reason" entries in
      let* reason = string (path ^ ".reason") reason_json in
      Ok (Cancel_workflow { reason })
  | "remove_from_cache" ->
      let* entries = exact_object path [ "kind"; "message"; "reason" ] json in
      let* message_json = field path "message" entries in
      let* message = string (path ^ ".message") message_json in
      let* reason_json = field path "reason" entries in
      let* reason_name = string (path ^ ".reason") reason_json in
      let* reason = eviction_reason (path ^ ".reason") reason_name in
      Ok (Remove_from_cache { message; reason })
  | _ -> Error (invalid (path ^ ".kind") "unsupported activation job kind")

(** Encodes one activation job. *)
let activation_job_json = function
  | Initialize_workflow
      { workflow_id; workflow_type; arguments; randomness_seed; attempt; context } ->
      let* arguments = payloads_json arguments in
      let* context = match context with None -> Ok [] | Some value -> let* value = initialize_context_json value in Ok [ ("context", value) ] in
      Ok
        (`Assoc
          ([
            ("kind", `String "initialize_workflow");
            ("workflow_id", `String workflow_id);
            ("workflow_type", `String workflow_type);
            ("arguments", arguments);
            ("randomness_seed", `String randomness_seed);
            ("attempt", `Int attempt);
          ] @ context))
  | Resolve_activity { seq; result } ->
      let* result = activity_resolution_json result in
      Ok
        (`Assoc
          [
            ("kind", `String "resolve_activity");
            ("seq", `Intlit (Int64.to_string seq));
            ("result", result);
          ])
  | Resolve_child_workflow_start { seq; result } ->
      let* result = child_workflow_start_resolution_json result in
      Ok
        (`Assoc
          [
            ("kind", `String "resolve_child_workflow_start");
            ("seq", `Intlit (Int64.to_string seq));
            ("result", result);
          ])
  | Resolve_child_workflow { seq; result } ->
      let* result = child_workflow_resolution_json result in
      Ok
        (`Assoc
          [
            ("kind", `String "resolve_child_workflow");
            ("seq", `Intlit (Int64.to_string seq));
            ("result", result);
          ])
  | Resolve_signal_external_workflow { seq; result } ->
      let* result = external_workflow_resolution_json result in
      Ok
        (`Assoc
          [
            ("kind", `String "resolve_signal_external_workflow");
            ("seq", `Intlit (Int64.to_string seq));
            ("result", result);
          ])
  | Resolve_request_cancel_external_workflow { seq; result } ->
      let* result = external_workflow_resolution_json result in
      Ok
        (`Assoc
          [
            ("kind", `String "resolve_request_cancel_external_workflow");
            ("seq", `Intlit (Int64.to_string seq));
            ("result", result);
          ])
  | Query_workflow { query_id; query_type; arguments; headers } ->
      let* arguments = payloads_json arguments in
      let* headers = payload_map_json "$.headers" headers in
      Ok
        (`Assoc
          [
            ("kind", `String "query_workflow");
            ("query_id", `String query_id);
            ("query_type", `String query_type);
            ("arguments", arguments);
            ("headers", headers);
          ])
  | Do_update { id; protocol_instance_id; name; input; headers; meta; run_validator } ->
      let* input = payloads_json input in
      let* headers = payload_map_json "$.headers" headers in
      let* meta = update_meta_json meta in
      Ok
        (`Assoc
          [ ("kind", `String "do_update");
            ("id", `String id);
            ("protocol_instance_id", `String protocol_instance_id);
            ("name", `String name);
            ("input", input);
            ("headers", headers);
            ("meta", meta);
            ("run_validator", `Bool run_validator) ])
  | Signal_workflow { signal_name; input; identity; headers } ->
      let* input = payloads_json input in
      let* headers = payload_map_json "$.headers" headers in
      Ok
        (`Assoc
          [
            ("kind", `String "signal_workflow");
            ("signal_name", `String signal_name);
            ("input", input);
            ("identity", `String identity);
            ("headers", headers);
          ])
  | Notify_has_patch { patch_id } ->
      Ok
        (`Assoc
          [ ("kind", `String "notify_has_patch"); ("patch_id", `String patch_id) ])
  | Fire_timer { seq } ->
      Ok
        (`Assoc
          [ ("kind", `String "fire_timer"); ("seq", `Intlit (Int64.to_string seq)) ])
  | Cancel_workflow { reason } ->
      Ok (`Assoc [ ("kind", `String "cancel_workflow"); ("reason", `String reason) ])
  | Remove_from_cache { message; reason } ->
      Ok
        (`Assoc
          [
            ("kind", `String "remove_from_cache");
            ("message", `String message);
            ("reason", `String (eviction_reason_string reason));
          ])

(** Encodes a job list while preserving scheduler-visible order. *)
let activation_jobs_json jobs =
  let rec loop encoded = function
    | [] -> Ok (`List (List.rev encoded))
    | job :: rest ->
        let* job = activation_job_json job in
        loop (job :: encoded) rest
  in
  loop [] jobs

(** Enforces Core's invariant that an eviction activation contains only its
    single remove-from-cache job. *)
let validate_eviction_jobs path jobs =
  match List.filter (function Remove_from_cache _ -> true | _ -> false) jobs with
  | [] -> Ok ()
  | [ _ ] when List.length jobs = 1 -> Ok ()
  | _ -> Error (invalid path "cache eviction must be the activation's only job")

(** Requires initialization, when present, to occur exactly once at the head of
    the activation. Core sends initialization before any consequences for that
    workflow run, so accepting another order would hide malformed Core traffic. *)
let validate_initialize_jobs path jobs =
  let rec loop index seen = function
    | [] -> Ok ()
    | Initialize_workflow _ :: _ when seen || index <> 0 ->
        Error (invalid path "initialize_workflow must occur at most once and first")
    | Initialize_workflow _ :: rest -> loop (index + 1) true rest
    | _ :: rest -> loop (index + 1) seen rest
  in
  loop 0 false jobs

(** Core delivers queries in a query-only activation. Keeping that invariant
    explicit prevents the runtime from accidentally resuming ordinary workflow
    continuations while answering a synchronous read-only request. *)
let validate_query_jobs path jobs =
  let query_count =
    List.fold_left
      (fun count -> function Query_workflow _ -> count + 1 | _ -> count)
      0 jobs
  in
  if query_count = 0 then Ok ()
  else if query_count <> List.length jobs then
    Error (invalid path "query_workflow jobs must be the activation's only jobs")
  else
    let rec loop seen = function
      | [] -> Ok ()
      | Query_workflow { query_id; _ } :: rest ->
          if List.mem query_id seen then
            Error (invalid (path ^ ".query_id") "duplicate query ID")
          else loop (query_id :: seen) rest
      | _ :: _ -> Error (invalid path "query activation contains a non-query job")
    in
    loop [] jobs

(** Ensures each update protocol instance is answered at most once per
    activation and that Core's two update identifiers remain unique. Updates
    may appear alongside ordinary jobs because they can resume a workflow
    handler; unlike queries they are not a query-only activation. *)
let validate_update_jobs path jobs =
  let rec loop seen_ids seen_protocols = function
    | [] -> Ok ()
    | Do_update { id; protocol_instance_id; meta; _ } :: rest ->
        if List.mem id seen_ids then
          Error (invalid (path ^ ".id") "duplicate update ID")
        else if List.mem protocol_instance_id seen_protocols then
          Error (invalid (path ^ ".protocol_instance_id") "duplicate update protocol ID")
        else if not (String.equal id meta.update_id) then
          Error (invalid (path ^ ".meta.update_id") "must match update id")
        else loop (id :: seen_ids) (protocol_instance_id :: seen_protocols) rest
    | _ :: rest -> loop seen_ids seen_protocols rest
  in
  loop [] [] jobs

(** Accepts a missing activation timestamp only for Core's synthetic eviction
    activation, whose official constructor deliberately sets [timestamp] to
    [None]. *)
let validate_activation_timestamp path timestamp jobs =
  match (timestamp, jobs) with
  | Some _, _ -> Ok ()
  | None, [ Remove_from_cache _ ] -> Ok ()
  | None, _ -> Error (invalid path "timestamp may be null only for cache eviction")

(** Converts a strict activation object to typed values. *)
let activation_from_json json =
  let raw_entries = match json with `Assoc entries -> entries | _ -> [] in
  let has_metadata = List.mem_assoc "metadata" raw_entries in
  let* entries =
    exact_object "$"
      ([ "run_id"; "timestamp"; "is_replaying"; "history_length"; "jobs" ]
      @ if has_metadata then [ "metadata" ] else [])
      json
  in
  let* run_json = field "$" "run_id" entries in
  let* run_id = identifier "$.run_id" run_json in
  let* timestamp_json = field "$" "timestamp" entries in
  let* timestamp = nullable "$.timestamp" timestamp timestamp_json in
  let* replay_json = field "$" "is_replaying" entries in
  let* is_replaying = bool "$.is_replaying" replay_json in
  let* history_json = field "$" "history_length" entries in
  let* history_length = uint32 "$.history_length" history_json in
  let* jobs_json = field "$" "jobs" entries in
  let* jobs = list "$.jobs" activation_job jobs_json in
  let* () = validate_eviction_jobs "$.jobs" jobs in
  let* () = validate_initialize_jobs "$.jobs" jobs in
  let* () = validate_query_jobs "$.jobs" jobs in
  let* () = validate_update_jobs "$.jobs" jobs in
  let* () = validate_activation_timestamp "$.timestamp" timestamp jobs in
  let* metadata =
    if has_metadata then
      let* json = field "$" "metadata" entries in
      let* value = activation_metadata "$.metadata" json in
      Ok (Some value)
    else Ok None
  in
  Ok { run_id; timestamp; is_replaying; history_length; jobs; metadata }

(** Strictly decodes one activation through the shared JSON foundation. *)
let decode_activation input =
  match Control.decode_payload_object input with
  | Error error -> Error (of_control_error_at_source error)
  | Ok json -> activation_from_json json

(** Encodes and semantically reparses one activation. *)
let encode_activation value =
  let* jobs = activation_jobs_json value.jobs in
  let* metadata =
    match value.metadata with
    | None -> Ok []
    | Some value ->
        let* value = activation_metadata_json value in
        Ok [ ("metadata", value) ]
  in
  let json =
    `Assoc
      ([
        ("run_id", `String value.run_id);
        ( "timestamp",
          match value.timestamp with
          | None -> `Null
          | Some value -> time_json value.seconds value.nanoseconds );
        ("is_replaying", `Bool value.is_replaying);
        ("history_length", `Intlit (Int64.to_string value.history_length));
        ("jobs", jobs);
      ] @ metadata)
  in
  match Control.encode_payload_object json with
  | Error error -> Error (of_control_error_at_source error)
  | Ok output ->
      let* _ = decode_activation output in
      Ok output

(** Maps the activity-cancellation spelling from Core. *)
let cancellation_type path = function
  | "try_cancel" -> Ok Try_cancel
  | "wait_cancellation_completed" -> Ok Wait_cancellation_completed
  | "abandon" -> Ok Abandon
  | _ -> Error (invalid path "unknown activity cancellation type")

(** Renders the activity-cancellation spelling expected by Core. *)
let cancellation_type_string = function
  | Try_cancel -> "try_cancel"
  | Wait_cancellation_completed -> "wait_cancellation_completed"
  | Abandon -> "abandon"

(** Decodes the child-workflow cancellation policy emitted by the OCaml
    runtime.  Child policies are deliberately separate from activity policies:
    Core supports an additional wait-for-requested state for children. *)
let child_cancellation_type path = function
  | "try_cancel" -> Ok Child_try_cancel
  | "wait_cancellation_completed" -> Ok Child_wait_cancellation_completed
  | "abandon" -> Ok Child_abandon
  | "wait_cancellation_requested" -> Ok Child_wait_cancellation_requested
  | _ -> Error (invalid path "unknown child workflow cancellation type")

(** Renders the canonical policy spelling used by the bridge JSON protocol. *)
let child_cancellation_type_string = function
  | Child_try_cancel -> "try_cancel"
  | Child_wait_cancellation_completed -> "wait_cancellation_completed"
  | Child_abandon -> "abandon"
  | Child_wait_cancellation_requested -> "wait_cancellation_requested"

(** Decodes a nullable duration. *)
let optional_duration path value = nullable path duration value

(** Encodes a nullable duration. *)
let optional_duration_json = function
  | None -> `Null
  | Some value -> time_json value.seconds value.nanoseconds

(** Validates a retry policy assembled by an OCaml translator without making
    callers construct a synthetic completion just to reach the canonical
    decoder. *)
let validate_retry_policy value =
  match retry_policy_json value with
  | Ok _ -> Ok ()
  | Error error -> Error error

(** Decodes one supported completion command. *)
let completion_command path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "schedule_activity" ->
      let* entries =
        exact_object path
          [
            "kind";
            "seq";
            "activity_id";
            "activity_type";
            "task_queue";
            "arguments";
            "schedule_to_close_timeout";
            "schedule_to_start_timeout";
            "start_to_close_timeout";
            "heartbeat_timeout";
            "retry_policy";
            "priority";
            "cancellation_type";
            "do_not_eagerly_execute";
          ]
          json
      in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* activity_id_json = field path "activity_id" entries in
      let* activity_id = identifier (path ^ ".activity_id") activity_id_json in
      let* activity_type_json = field path "activity_type" entries in
      let* activity_type = identifier (path ^ ".activity_type") activity_type_json in
      let* task_queue_json = field path "task_queue" entries in
      let* task_queue = identifier (path ^ ".task_queue") task_queue_json in
      let* arguments_json = field path "arguments" entries in
      let* arguments = list (path ^ ".arguments") payload arguments_json in
      let decode_timeout name =
        let* json = field path name entries in
        optional_duration (path ^ "." ^ name) json
      in
      let* schedule_to_close_timeout = decode_timeout "schedule_to_close_timeout" in
      let* schedule_to_start_timeout = decode_timeout "schedule_to_start_timeout" in
      let* start_to_close_timeout = decode_timeout "start_to_close_timeout" in
      let* heartbeat_timeout = decode_timeout "heartbeat_timeout" in
      let* retry_policy_json = field path "retry_policy" entries in
      let* retry_policy =
        nullable (path ^ ".retry_policy") retry_policy retry_policy_json
      in
      let* priority_json = field path "priority" entries in
      let* priority = nullable (path ^ ".priority") workflow_priority priority_json in
      let* cancellation_json = field path "cancellation_type" entries in
      let* cancellation_name = string (path ^ ".cancellation_type") cancellation_json in
      let* cancellation_type = cancellation_type (path ^ ".cancellation_type") cancellation_name in
      let* eager_json = field path "do_not_eagerly_execute" entries in
      let* do_not_eagerly_execute = bool (path ^ ".do_not_eagerly_execute") eager_json in
      if Option.is_none schedule_to_close_timeout && Option.is_none start_to_close_timeout then
        Error
          (invalid path
             "activity requires schedule-to-close or start-to-close timeout")
      else
        Ok
          (Schedule_activity
             {
               seq;
               activity_id;
               activity_type;
               task_queue;
               arguments;
               schedule_to_close_timeout;
               schedule_to_start_timeout;
               start_to_close_timeout;
               heartbeat_timeout;
               retry_policy;
               priority;
               cancellation_type;
            do_not_eagerly_execute;
          })
  | "schedule_local_activity" ->
      let* entries =
        exact_object path
          [
            "kind";
            "seq";
            "activity_id";
            "activity_type";
            "attempt";
            "original_schedule_time";
            "arguments";
            "schedule_to_close_timeout";
            "schedule_to_start_timeout";
            "start_to_close_timeout";
            "retry_policy";
            "local_retry_threshold";
            "cancellation_type";
          ]
          json
      in
      let* seq = uint32 (path ^ ".seq") (Option.get (List.assoc_opt "seq" entries)) in
      let* activity_id = identifier (path ^ ".activity_id") (Option.get (List.assoc_opt "activity_id" entries)) in
      let* activity_type = identifier (path ^ ".activity_type") (Option.get (List.assoc_opt "activity_type" entries)) in
      let* attempt = uint32 (path ^ ".attempt") (Option.get (List.assoc_opt "attempt" entries)) in
      let* original_schedule_time =
        nullable (path ^ ".original_schedule_time")
          (fun path json -> timestamp path json)
          (Option.get (List.assoc_opt "original_schedule_time" entries))
      in
      let* arguments = list (path ^ ".arguments") payload (Option.get (List.assoc_opt "arguments" entries)) in
      let decode_timeout name =
        nullable (path ^ "." ^ name) duration
          (Option.get (List.assoc_opt name entries))
      in
      let* schedule_to_close_timeout = decode_timeout "schedule_to_close_timeout" in
      let* schedule_to_start_timeout = decode_timeout "schedule_to_start_timeout" in
      let* start_to_close_timeout = decode_timeout "start_to_close_timeout" in
      let* retry_policy =
        nullable (path ^ ".retry_policy") retry_policy
          (Option.get (List.assoc_opt "retry_policy" entries))
      in
      let* local_retry_threshold = decode_timeout "local_retry_threshold" in
      let* cancellation_name =
        string (path ^ ".cancellation_type")
          (Option.get (List.assoc_opt "cancellation_type" entries))
      in
      let* cancellation_type = cancellation_type (path ^ ".cancellation_type") cancellation_name in
      if Int64.compare attempt 0L <= 0 then
        Error (invalid (path ^ ".attempt") "local activity attempt must be positive")
      else if Option.is_none schedule_to_close_timeout && Option.is_none start_to_close_timeout then
        Error (invalid path "local activity requires schedule-to-close or start-to-close timeout")
      else
        Ok
          (Schedule_local_activity
             {
               seq;
               activity_id;
               activity_type;
               attempt;
               original_schedule_time;
               arguments;
               schedule_to_close_timeout;
               schedule_to_start_timeout;
               start_to_close_timeout;
               retry_policy;
               local_retry_threshold;
               cancellation_type;
             })
  | "start_child_workflow" ->
      let* entries =
        exact_object path
          [
            "kind";
            "seq";
            "workflow_id";
            "workflow_type";
            "input";
            "retry_policy";
            "cancellation_type";
          ]
          json
      in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
      let* workflow_type_json = field path "workflow_type" entries in
      let* workflow_type = identifier (path ^ ".workflow_type") workflow_type_json in
      let* input_json = field path "input" entries in
      let* input = list (path ^ ".input") payload input_json in
      let* retry_policy_json = field path "retry_policy" entries in
      let* retry_policy =
        nullable (path ^ ".retry_policy") retry_policy retry_policy_json
      in
      let* cancellation_json = field path "cancellation_type" entries in
      let* cancellation_name =
        string (path ^ ".cancellation_type") cancellation_json
      in
      let* cancellation_type =
        child_cancellation_type (path ^ ".cancellation_type") cancellation_name
      in
      Ok
        (Start_child_workflow
           { seq; workflow_id; workflow_type; input; retry_policy;
             cancellation_type })
  | "cancel_child_workflow" ->
      let* entries = exact_object path [ "kind"; "seq"; "reason" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* reason_json = field path "reason" entries in
      let* reason = cancellation_reason (path ^ ".reason") reason_json in
      Ok (Cancel_child_workflow { seq; reason })
  | "signal_external_workflow" ->
      let* entries =
        exact_object path
          [
            "kind";
            "seq";
            "workflow_id";
            "run_id";
            "signal_name";
            "input";
            "child_workflow_only";
            "headers";
          ] json
      in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
      let* run_id_json = field path "run_id" entries in
      let* run_id = bounded_text (path ^ ".run_id") run_id_json in
      let* signal_name_json = field path "signal_name" entries in
      let* signal_name = identifier (path ^ ".signal_name") signal_name_json in
      let* input_json = field path "input" entries in
      let* input = list (path ^ ".input") payload input_json in
      let* child_workflow_only_json = field path "child_workflow_only" entries in
      let* child_workflow_only = bool (path ^ ".child_workflow_only") child_workflow_only_json in
      let* headers_json = field path "headers" entries in
      let* headers = payload_map (path ^ ".headers") headers_json in
      Ok (Signal_external_workflow { seq; workflow_id; run_id; signal_name; input; child_workflow_only; headers })
  | "request_cancel_external_workflow" ->
      let* entries =
        exact_object path [ "kind"; "seq"; "workflow_id"; "run_id"; "reason" ] json
      in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
      let* run_id_json = field path "run_id" entries in
      let* run_id = bounded_text (path ^ ".run_id") run_id_json in
      let* reason_json = field path "reason" entries in
      let* reason = cancellation_reason (path ^ ".reason") reason_json in
      Ok (Request_cancel_external_workflow { seq; workflow_id; run_id; reason })
  | "continue_as_new" ->
      let* entries =
        exact_object path [ "kind"; "workflow_type"; "input" ] json
      in
      let* workflow_type_json = field path "workflow_type" entries in
      let* workflow_type =
        identifier (path ^ ".workflow_type") workflow_type_json
      in
      let* input_json = field path "input" entries in
      let* input = list (path ^ ".input") payload input_json in
      Ok (Continue_as_new { workflow_type; input })
  | "request_cancel_activity" ->
      let* entries = exact_object path [ "kind"; "seq" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      Ok (Request_cancel_activity { seq })
  | "request_cancel_local_activity" ->
      let* entries = exact_object path [ "kind"; "seq" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      Ok (Request_cancel_local_activity { seq })
  | "start_timer" ->
      let* entries = exact_object path [ "kind"; "seq"; "start_to_fire_timeout" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* timeout_json = field path "start_to_fire_timeout" entries in
      let* start_to_fire_timeout = duration (path ^ ".start_to_fire_timeout") timeout_json in
      Ok (Start_timer { seq; start_to_fire_timeout })
  | "cancel_timer" ->
      let* entries = exact_object path [ "kind"; "seq" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      Ok (Cancel_timer { seq })
  | "query_result" ->
      let* entries = exact_object path [ "kind"; "query_id"; "result" ] json in
      let* query_id_json = field path "query_id" entries in
      let* query_id = identifier (path ^ ".query_id") query_id_json in
      let* result_json = field path "result" entries in
      let* result = query_result (path ^ ".result") result_json in
      Ok (Query_result { query_id; result })
  | "update_response" ->
      let* entries =
        exact_object path [ "kind"; "protocol_instance_id"; "response" ] json
      in
      let* protocol_json = field path "protocol_instance_id" entries in
      let* protocol_instance_id =
        identifier (path ^ ".protocol_instance_id") protocol_json
      in
      let* response_json = field path "response" entries in
      let* response = update_response (path ^ ".response") response_json in
      Ok (Update_response { protocol_instance_id; response })
  | "set_patch_marker" ->
      let* entries = exact_object path [ "kind"; "patch_id"; "deprecated" ] json in
      let* patch_id_json = field path "patch_id" entries in
      let* patch_id = identifier (path ^ ".patch_id") patch_id_json in
      let* deprecated_json = field path "deprecated" entries in
      let* deprecated = bool (path ^ ".deprecated") deprecated_json in
      Ok (Set_patch_marker { patch_id; deprecated })
  | "upsert_search_attributes" ->
      let* entries = exact_object path [ "kind"; "search_attributes" ] json in
      let* values_json = field path "search_attributes" entries in
      let* search_attributes = payload_map (path ^ ".search_attributes") values_json in
      Ok (Upsert_search_attributes { search_attributes })
  | "complete_workflow" ->
      let* entries = exact_object path [ "kind"; "result" ] json in
      let* result_json = field path "result" entries in
      let* result = nullable (path ^ ".result") payload result_json in
      Ok (Complete_workflow { result })
  | "fail_workflow" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* failure = failure (path ^ ".failure") failure_json in
      Ok (Fail_workflow { failure })
  | "cancel_workflow" ->
      let* _ = exact_object path [ "kind" ] json in
      Ok Cancel_workflow_execution
  | _ -> Error (invalid (path ^ ".kind") "unsupported completion command kind")

(** Encodes one completion command. *)
let completion_command_json = function
  | Schedule_activity value ->
      let* arguments = payloads_json value.arguments in
      let* retry_policy =
        match value.retry_policy with
        | None -> Ok `Null
        | Some value -> retry_policy_json value
      in
      let priority =
        match value.priority with
        | None -> `Null
        | Some value ->
            `Assoc
              [
                ("priority_key", `Int value.priority_key);
                ("fairness_key", `String value.fairness_key);
                ( "fairness_weight_bits",
                  `Intlit (Int64.to_string value.fairness_weight_bits) );
              ]
      in
      Ok
        (`Assoc
          [
            ("kind", `String "schedule_activity");
            ("seq", `Intlit (Int64.to_string value.seq));
            ("activity_id", `String value.activity_id);
            ("activity_type", `String value.activity_type);
            ("task_queue", `String value.task_queue);
            ("arguments", arguments);
            ("schedule_to_close_timeout", optional_duration_json value.schedule_to_close_timeout);
            ("schedule_to_start_timeout", optional_duration_json value.schedule_to_start_timeout);
            ("start_to_close_timeout", optional_duration_json value.start_to_close_timeout);
            ("heartbeat_timeout", optional_duration_json value.heartbeat_timeout);
            ("retry_policy", retry_policy);
            ("priority", priority);
            ("cancellation_type", `String (cancellation_type_string value.cancellation_type));
            ("do_not_eagerly_execute", `Bool value.do_not_eagerly_execute);
          ])
  | Schedule_local_activity value ->
      let* arguments = payloads_json value.arguments in
      let* retry_policy =
        match value.retry_policy with
        | None -> Ok `Null
        | Some value -> retry_policy_json value
      in
      Ok
        (`Assoc
          [
            ("kind", `String "schedule_local_activity");
            ("seq", `Intlit (Int64.to_string value.seq));
            ("activity_id", `String value.activity_id);
            ("activity_type", `String value.activity_type);
            ("attempt", `Intlit (Int64.to_string value.attempt));
            ( "original_schedule_time",
              match value.original_schedule_time with
              | None -> `Null
              | Some value -> time_json value.seconds value.nanoseconds );
            ("arguments", arguments);
            ("schedule_to_close_timeout", optional_duration_json value.schedule_to_close_timeout);
            ("schedule_to_start_timeout", optional_duration_json value.schedule_to_start_timeout);
            ("start_to_close_timeout", optional_duration_json value.start_to_close_timeout);
            ("retry_policy", retry_policy);
            ("local_retry_threshold", optional_duration_json value.local_retry_threshold);
            ("cancellation_type", `String (cancellation_type_string value.cancellation_type));
          ])
  | Request_cancel_activity { seq } ->
      Ok
        (`Assoc
          [
            ("kind", `String "request_cancel_activity");
            ("seq", `Intlit (Int64.to_string seq));
          ])
  | Request_cancel_local_activity { seq } ->
      Ok
        (`Assoc
          [
            ("kind", `String "request_cancel_local_activity");
            ("seq", `Intlit (Int64.to_string seq));
          ])
  | Start_child_workflow
      { seq; workflow_id; workflow_type; input; retry_policy; cancellation_type } ->
      let* input = payloads_json input in
      let* retry_policy =
        match retry_policy with
        | None -> Ok `Null
        | Some value -> retry_policy_json value
      in
      Ok
        (`Assoc
          [
            ("kind", `String "start_child_workflow");
            ("seq", `Intlit (Int64.to_string seq));
            ("workflow_id", `String workflow_id);
            ("workflow_type", `String workflow_type);
            ("input", input);
            ("retry_policy", retry_policy);
            ( "cancellation_type",
              `String (child_cancellation_type_string cancellation_type) );
          ])
  | Cancel_child_workflow { seq; reason } ->
      Ok
        (`Assoc
          [
            ("kind", `String "cancel_child_workflow");
            ("seq", `Intlit (Int64.to_string seq));
            ("reason", `String reason);
          ])
  | Signal_external_workflow
      { seq; workflow_id; run_id; signal_name; input; child_workflow_only; headers } ->
      let* input = payloads_json input in
      let* headers = payload_map_json "$.command.headers" headers in
      Ok
        (`Assoc
          [
            ("kind", `String "signal_external_workflow");
            ("seq", `Intlit (Int64.to_string seq));
            ("workflow_id", `String workflow_id);
            ("run_id", `String run_id);
            ("signal_name", `String signal_name);
            ("input", input);
            ("child_workflow_only", `Bool child_workflow_only);
            ("headers", headers);
          ])
  | Request_cancel_external_workflow
      { seq; workflow_id; run_id; reason } ->
      Ok
        (`Assoc
          [
            ("kind", `String "request_cancel_external_workflow");
            ("seq", `Intlit (Int64.to_string seq));
            ("workflow_id", `String workflow_id);
            ("run_id", `String run_id);
            ("reason", `String reason);
          ])
  | Continue_as_new { workflow_type; input } ->
      let* input = payloads_json input in
      Ok
        (`Assoc
          [
            ("kind", `String "continue_as_new");
            ("workflow_type", `String workflow_type);
            ("input", input);
          ])
  | Start_timer { seq; start_to_fire_timeout } ->
      Ok
        (`Assoc
          [
            ("kind", `String "start_timer");
            ("seq", `Intlit (Int64.to_string seq));
            ( "start_to_fire_timeout",
              time_json start_to_fire_timeout.seconds start_to_fire_timeout.nanoseconds );
          ])
  | Cancel_timer { seq } ->
      Ok
        (`Assoc
          [ ("kind", `String "cancel_timer"); ("seq", `Intlit (Int64.to_string seq)) ])
  | Query_result { query_id; result } ->
      let* result = query_result_json result in
      Ok
        (`Assoc
          [
            ("kind", `String "query_result");
            ("query_id", `String query_id);
            ("result", result);
          ])
  | Update_response { protocol_instance_id; response } ->
      let* response = update_response_json response in
      Ok
        (`Assoc
          [ ("kind", `String "update_response");
            ("protocol_instance_id", `String protocol_instance_id);
            ("response", response) ])
  | Set_patch_marker { patch_id; deprecated } ->
      Ok
        (`Assoc
          [ ("kind", `String "set_patch_marker");
            ("patch_id", `String patch_id);
            ("deprecated", `Bool deprecated) ])
  | Upsert_search_attributes { search_attributes } ->
      let* search_attributes =
        payload_map_json "$.commands.search_attributes" search_attributes
      in
      Ok
        (`Assoc
          [ ("kind", `String "upsert_search_attributes");
            ("search_attributes", search_attributes) ])
  | Complete_workflow { result } ->
      let* result = match result with None -> Ok `Null | Some value -> payload_json value in
      Ok (`Assoc [ ("kind", `String "complete_workflow"); ("result", result) ])
  | Fail_workflow { failure } ->
      let* failure = failure_json failure in
      Ok (`Assoc [ ("kind", `String "fail_workflow"); ("failure", failure) ])
  | Cancel_workflow_execution -> Ok (`Assoc [ ("kind", `String "cancel_workflow") ])

(** Encodes completion commands while preserving deterministic source order. *)
let completion_commands_json commands =
  let rec loop encoded = function
    | [] -> Ok (`List (List.rev encoded))
    | command :: rest ->
        let* command = completion_command_json command in
        loop (command :: encoded) rest
  in
  loop [] commands

(** Identifies commands that end a workflow execution. *)
let terminal = function
  | Complete_workflow _ | Fail_workflow _ | Continue_as_new _
  | Cancel_workflow_execution -> true
  | _ -> false

(** Requires a terminal command, if present, to be unique and final. *)
let validate_terminal_order path commands =
  let rec loop = function
    | [] -> Ok ()
    | [ command ] when terminal command -> Ok ()
    | command :: _ when terminal command ->
        Error (invalid path "terminal workflow command must be last")
    | _ :: rest -> loop rest
  in
  loop commands

(** Query answers are the only commands valid for a query activation. Enforcing
    this at the semantic layer avoids sending a query response alongside a
    workflow command that Core would reject or that could mutate history. *)
let validate_query_results path commands =
  let query_count =
    List.fold_left
      (fun count -> function Query_result _ -> count + 1 | _ -> count)
      0 commands
  in
  if query_count = 0 then Ok ()
  else if query_count <> List.length commands then
    Error (invalid path "query_result commands must be the completion's only commands")
  else
    let rec loop seen = function
      | [] -> Ok ()
      | Query_result { query_id; _ } :: rest ->
          if List.mem query_id seen then
            Error (invalid (path ^ ".query_id") "duplicate query ID")
          else loop (query_id :: seen) rest
      | _ :: _ -> Error (invalid path "query completion contains a non-query command")
    in
    loop [] commands

(** Ensures a completion does not emit two decisions for the same update
    protocol instance in one activation. Accepted, rejected, and completed
    responses may coexist with ordinary workflow commands. *)
let validate_update_responses path commands =
  let rec loop phases = function
    | [] -> Ok ()
    | Update_response { protocol_instance_id; response } :: rest ->
        let accepted, terminal =
          match List.assoc_opt protocol_instance_id phases with
          | None -> false, false
          | Some phases -> phases
        in
        let invalid_phase message =
          Error (invalid (path ^ ".protocol_instance_id") message)
        in
        begin match response with
        | Update_accepted when accepted || terminal ->
            invalid_phase
              "update acceptance must be the first response and may appear once"
        | Update_accepted ->
            loop
              ((protocol_instance_id, (true, terminal))
              :: List.remove_assoc protocol_instance_id phases)
              rest
        | Update_rejected _ when terminal ->
            invalid_phase
              "update rejection may appear once and must be terminal"
        | Update_rejected _ ->
            loop
              ((protocol_instance_id, (accepted, true))
              :: List.remove_assoc protocol_instance_id phases)
              rest
        | Update_completed _ when terminal ->
            invalid_phase "update completion may appear once"
        | Update_completed _ ->
            loop
              ((protocol_instance_id, (accepted, true))
              :: List.remove_assoc protocol_instance_id phases)
              rest
        end
    | _ :: rest -> loop phases rest
  in
  loop [] commands

(** Rejects two marker modes for one patch ID in a single completion. Temporal
    Core retains only the first marker command for an ID, so allowing a later
    conflicting value would make durable history depend silently on source
    order. Same-mode repetitions remain valid and ordered for Core to
    deduplicate. *)
let validate_patch_marker_modes path commands =
  let rec loop modes = function
    | [] -> Ok ()
    | Set_patch_marker { patch_id; deprecated } :: rest -> (
        match List.assoc_opt patch_id modes with
        | Some existing when existing <> deprecated ->
            Error
              (invalid (path ^ ".patch_id")
                 "one patch ID cannot use both active and deprecated marker modes")
        | Some _ -> loop modes rest
        | None -> loop ((patch_id, deprecated) :: modes) rest)
    | _ :: rest -> loop modes rest
  in
  loop [] commands

(** Converts a strict completion object to typed values. *)
let completion_from_json json =
  let* entries = exact_object "$" [ "run_id"; "commands" ] json in
  let* run_json = field "$" "run_id" entries in
  let* run_id = identifier "$.run_id" run_json in
  let* commands_json = field "$" "commands" entries in
  let* commands = list "$.commands" completion_command commands_json in
  let* () = validate_terminal_order "$.commands" commands in
  let* () = validate_query_results "$.commands" commands in
  let* () = validate_update_responses "$.commands" commands in
  let* () = validate_patch_marker_modes "$.commands" commands in
  Ok { run_id; commands }

(** Strictly decodes one completion through the shared JSON foundation. *)
let decode_completion input =
  match Control.decode_payload_object input with
  | Error error -> Error (of_control_error_at_source error)
  | Ok json -> completion_from_json json

(** Encodes and semantically reparses one outgoing completion. *)
let encode_completion value =
  let* commands = completion_commands_json value.commands in
  let json = `Assoc [ ("run_id", `String value.run_id); ("commands", commands) ] in
  match Control.encode_payload_object json with
  | Error error -> Error (of_control_error_at_source error)
  | Ok output ->
      let* _ = decode_completion output in
      Ok output

(** Private aliases make the canonical semantic codecs reusable without
    copying validation logic into each protocol adapter. *)
module Internal = struct
  let invalid = invalid
  let of_control_error = of_control_error
  let exact_object = exact_object
  let field = field
  let string = string
  let bool = bool
  let uint32 = uint32
  let int32 = int32
  let identifier = identifier
  let uint64_decimal = uint64_decimal
  let list = list
  let nullable = nullable
  let bytes_wrapper = bytes_wrapper
  let bytes_wrapper_json = bytes_wrapper_json
  let payload = payload
  let payload_json = payload_json
  let timestamp = timestamp
  let duration = duration
  let time_json = time_json
  let workflow_execution = workflow_execution
  let workflow_priority = workflow_priority
  let failure = failure
  let failure_json = failure_json
end

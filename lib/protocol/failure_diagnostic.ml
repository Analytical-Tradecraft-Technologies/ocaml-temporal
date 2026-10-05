(** Renders structured Temporal failure metadata for the OCaml layers.

    Core frequently wraps an application failure in an activity or
    child-workflow record. Keeping this traversal in the protocol library lets
    both the workflow runtime and public client preserve the same outer-to-inner
    diagnostic without exposing protocol records in the public API. *)

open Workflow_protocol

(** The JSON decoder enforces this depth; the second guard protects callers
    that construct protocol values directly in tests or adapter code. *)
let max_cause_depth = 128

(** Limits server-supplied text before it is copied into an OCaml error. The
    protocol already bounds each field, but this local cap keeps diagnostics
    safe if a value bypasses decoding.

    The cut backs off to a UTF-8 code-point boundary so that truncating valid
    text never yields invalid UTF-8: a split character would otherwise make the
    whole message unusable later, when [bounded_protocol_message] replaces
    invalid text with a generic diagnostic. Continuation bytes have the form
    [0b10xxxxxx]; at most three are skipped for well-formed input. *)
let bounded_text ~limit value =
  if String.length value <= limit then value
  else
    let is_continuation index = Char.code value.[index] land 0xC0 = 0x80 in
    let rec boundary length =
      if length > 0 && is_continuation length then boundary (length - 1)
      else length
    in
    String.sub value 0 (boundary limit) ^ "..."

(** Describes one semantic info variant while leaving binary payload details in
    the typed [Error.view] list. *)
let failure_info_summary = function
  | Application
      { type_name; non_retryable; details; category; next_retry_delay } ->
      let options =
        (match category with
        | Application_category_unspecified -> ""
        | Application_category_benign -> " category=benign")
        ^ (match next_retry_delay with
          | None -> ""
          | Some { seconds; nanoseconds } ->
              Printf.sprintf " next_retry_delay=%Ld.%09ds" seconds nanoseconds)
      in
      Printf.sprintf "application type=%s non_retryable=%b details=%d%s"
        type_name non_retryable (List.length details) options
  | Canceled { details; identity } ->
      Printf.sprintf "canceled identity=%s details=%d" identity
        (List.length details)
  | Terminated { identity } -> Printf.sprintf "terminated identity=%s" identity
  | Activity
      {
        scheduled_event_id;
        started_event_id;
        identity;
        activity_type;
        activity_id;
        retry_state;
      } ->
      let retry_state =
        match retry_state with
        | Unspecified -> "unspecified"
        | In_progress -> "in_progress"
        | Non_retryable_failure -> "non_retryable_failure"
        | Timeout -> "timeout"
        | Maximum_attempts_reached -> "maximum_attempts_reached"
        | Retry_policy_not_set -> "retry_policy_not_set"
        | Internal_server_error -> "internal_server_error"
        | Cancel_requested -> "cancel_requested"
      in
      Printf.sprintf
        "activity id=%s type=%s identity=%s scheduled_event_id=%Ld started_event_id=%Ld retry_state=%s"
        activity_id activity_type identity scheduled_event_id started_event_id
        retry_state
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
      let retry_state =
        match retry_state with
        | Unspecified -> "unspecified"
        | In_progress -> "in_progress"
        | Non_retryable_failure -> "non_retryable_failure"
        | Timeout -> "timeout"
        | Maximum_attempts_reached -> "maximum_attempts_reached"
        | Retry_policy_not_set -> "retry_policy_not_set"
        | Internal_server_error -> "internal_server_error"
        | Cancel_requested -> "cancel_requested"
      in
      Printf.sprintf
        "child_workflow namespace=%s id=%s run_id=%s type=%s initiated_event_id=%Ld started_event_id=%Ld retry_state=%s"
        namespace workflow_id run_id workflow_type initiated_event_id
        started_event_id retry_state
  | Timeout_failure { timeout_type; last_heartbeat_details } ->
      Printf.sprintf "timeout type=%s last_heartbeat_details=%d"
        (timeout_type_string timeout_type)
        (List.length last_heartbeat_details)
  | Server { non_retryable } ->
      Printf.sprintf "server non_retryable=%b" non_retryable
  | Reset_workflow { last_heartbeat_details } ->
      Printf.sprintf "reset_workflow last_heartbeat_details=%d"
        (List.length last_heartbeat_details)
  | Nexus_operation
      {
        scheduled_event_id;
        endpoint;
        service;
        operation;
        operation_id;
        operation_token;
      } ->
      Printf.sprintf
        "nexus_operation endpoint=%s service=%s operation=%s operation_id=%s operation_token=%s scheduled_event_id=%Ld"
        endpoint service operation operation_id operation_token
        scheduled_event_id
  | Nexus_handler { type_name; retry_behavior } ->
      let retry_behavior =
        match retry_behavior with
        | Nexus_retry_unspecified -> "unspecified"
        | Nexus_retry_retryable -> "retryable"
        | Nexus_retry_non_retryable -> "non_retryable"
      in
      Printf.sprintf "nexus_handler type=%s retry_behavior=%s" type_name
        retry_behavior
  | Absent -> "no_failure_info"

(** Renders one failure layer, keeping the same field order for stable logs and
    tests. The marker for encoded attributes confirms presence without copying
    arbitrary binary bytes into text. *)
let layer_text (value : failure) =
  let source =
    if String.equal value.source "" then []
    else [ "source=" ^ bounded_text ~limit:512 value.source ]
  in
  let stack_trace =
    if String.equal value.stack_trace "" then []
    else [ "stack_trace=" ^ bounded_text ~limit:1024 value.stack_trace ]
  in
  let attributes =
    match value.encoded_attributes with
    | None -> []
    | Some _ -> [ "encoded_attributes_present=true" ]
  in
  String.concat " "
    ((if String.equal value.message "" then []
      else [ bounded_text ~limit:2048 value.message ])
    @ source @ stack_trace
    @ [ failure_info_summary value.info ] @ attributes)

(** Walks [cause] from the outer wrapper to the innermost failure while
    retaining a deterministic marker instead of recursing without a bound. *)
let failure_diagnostic (failure : failure) =
  let rec loop depth reversed (value : failure) =
    let current = layer_text value in
    match value.cause with
    | None -> String.concat " | " (List.rev (current :: reversed))
    | Some _ when depth >= max_cause_depth ->
        String.concat " | "
          (List.rev ("cause_depth_limit_reached" :: current :: reversed))
    | Some cause -> loop (depth + 1) (current :: reversed) cause
  in
  loop 0 [] failure

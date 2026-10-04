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
    safe if a value bypasses decoding. *)
let bounded_text ~limit value =
  if String.length value <= limit then value
  else String.sub value 0 limit ^ "..."

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

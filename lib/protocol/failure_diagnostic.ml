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

(** Mirrors [Temporal_base.Error.max_error_type_bytes]. The protocol library
    does not depend on the base library, so the bound is repeated here and a
    regression test checks that every sanitized type is accepted by
    [Error.make]. *)
let max_inbound_error_type_encoded_length = 65_536

(** Marker appended when an inbound type is cut. It contains no control
    character, double quote or backslash, so it costs one encoded character
    per byte and cannot itself violate the [Error.make] rules. *)
let inbound_error_type_truncation_marker = "..."

(** Rewrites an inbound application failure type into text that
    [Temporal_base.Error.make] accepts without raising (#1011).

    The type was written by another SDK, worker or the server history, so it
    may legitimately contain control characters, invalid UTF-8, or more text
    than the SDK's outbound limit allows; the Rust bridge's own truncation
    marker even begins with a newline. Rejecting such a value would raise from
    [Error.make], escaping [Client.wait] and failing every replay of the
    workflow task, so instead:

    - each malformed UTF-8 sequence becomes U+FFFD;
    - each ASCII control character (below 0x20, or 0x7F) becomes the visible
      six-character escape [\\uXXXX] with upper-case hexadecimal digits;
    - the result is cut at a character or escape boundary so that its
      JSON-encoded length (a double quote or backslash counts two) stays within
      {!max_inbound_error_type_encoded_length}, and [...] marks the cut.

    The rewrite is a pure function of the input, so replaying the same history
    always yields the same type. Text that already satisfies the rules is
    returned unchanged. *)
let sanitize_inbound_error_type value =
  let limit = max_inbound_error_type_encoded_length in
  let marker = inbound_error_type_truncation_marker in
  let encoded_length text =
    String.fold_left
      (fun length character ->
        match character with '"' | '\\' -> length + 2 | _ -> length + 1)
      0 text
  in
  (* Phase one rewrites each decoded character into a replacement chunk,
     collected in reverse order with its encoded length. *)
  let rec chunks index reversed =
    if index >= String.length value then List.rev reversed
    else
      let decoded = String.get_utf_8_uchar value index in
      let chunk =
        if not (Uchar.utf_decode_is_valid decoded) then "\xEF\xBF\xBD"
        else
          let code = Uchar.to_int (Uchar.utf_decode_uchar decoded) in
          if code < 0x20 || code = 0x7f then Printf.sprintf "\\u%04X" code
          else String.sub value index (Uchar.utf_decode_length decoded)
      in
      chunks
        (index + Uchar.utf_decode_length decoded)
        ((chunk, encoded_length chunk) :: reversed)
  in
  let chunks = chunks 0 [] in
  let total = List.fold_left (fun sum (_, length) -> sum + length) 0 chunks in
  (* Phase two keeps everything when it fits; otherwise it keeps whole chunks
     while room for the marker remains, so a cut never splits a code point or
     an escape. *)
  if total <= limit then String.concat "" (List.map fst chunks)
  else
    let buffer = Buffer.create limit in
    let budget = limit - String.length marker in
    let rec take used = function
      | (chunk, length) :: rest when used + length <= budget ->
          Buffer.add_string buffer chunk;
          take (used + length) rest
      | _ -> ()
    in
    take 0 chunks;
    Buffer.add_string buffer marker;
    Buffer.contents buffer

(** Classifies the first non-wrapper layer (#1143). Wrappers only say which
    operation failed, so they defer to their cause, as for
    [application_failure_type]; a malformed chain deeper than the bound is
    [`Other]. *)
let failure_origin (failure : failure) =
  let rec loop depth (value : failure) =
    let inward () =
      match value.cause with
      | Some cause when depth < max_cause_depth -> loop (depth + 1) cause
      | None | Some _ -> `Other
    in
    match value.info with
    | Application _ -> `Application
    | Timeout_failure { timeout_type; _ } ->
        `Timeout
          (match timeout_type with
          | Timeout_start_to_close -> `Start_to_close
          | Timeout_schedule_to_start -> `Schedule_to_start
          | Timeout_schedule_to_close -> `Schedule_to_close
          | Timeout_heartbeat -> `Heartbeat
          | Timeout_unspecified -> `Unspecified)
    | Server _ -> `Server
    | Canceled _ -> `Cancelled
    | Terminated _ -> `Terminated
    | Activity _ | Child_workflow _ | Nexus_operation _ -> inward ()
    | Absent when Option.is_some value.cause -> inward ()
    | Absent | Reset_workflow _ | Nexus_handler _ -> `Other
  in
  loop 0 failure

(** Walks the bounded [cause] chain, appending the payloads [select] picks
    from each layer, outer to inner. *)
let collect_payloads select (failure : failure) =
  let rec loop depth reversed (value : failure) =
    let reversed = List.rev_append (select value.info) reversed in
    match value.cause with
    | Some cause when depth < max_cause_depth -> loop (depth + 1) reversed cause
    | None | Some _ -> List.rev reversed
  in
  loop 0 [] failure

(** Application and cancellation details only; heartbeat payloads are a
    timeout's or reset's own record, not error details (#1143). *)
let failure_details =
  collect_payloads (function
    | Application { details; _ } | Canceled { details; _ } -> details
    | Timeout_failure _ | Reset_workflow _ | Activity _ | Child_workflow _
    | Terminated _ | Server _ | Nexus_operation _ | Nexus_handler _ | Absent ->
        [])

(** The heartbeat payloads [failure_details] leaves out. *)
let last_heartbeat_details =
  collect_payloads (function
    | Timeout_failure { last_heartbeat_details; _ }
    | Reset_workflow { last_heartbeat_details } ->
        last_heartbeat_details
    | Application _ | Canceled _ | Activity _ | Child_workflow _ | Terminated _
    | Server _ | Nexus_operation _ | Nexus_handler _ | Absent ->
        [])

(** Searches outer-to-inner for the first [Application] layer. In the standard
    Temporal shapes that layer is the innermost meaningful failure: Core wraps
    an application failure as [Activity -> Application] or
    [Child_workflow -> Application], possibly with a timeout layer between.
    This mirrors how [Workflow_protocol.failure_non_retryable] lets wrappers
    defer to their cause, and uses the same bounded depth as
    [failure_diagnostic]. *)
let application_failure_type (failure : failure) =
  let rec loop depth (value : failure) =
    match value.info with
    | Application { type_name = ""; _ } -> None
    | Application { type_name; _ } ->
        Some (sanitize_inbound_error_type type_name)
    | Canceled _ | Terminated _ | Activity _ | Child_workflow _
    | Timeout_failure _ | Server _ | Reset_workflow _ | Nexus_operation _
    | Nexus_handler _ | Absent -> (
        match value.cause with
        | Some cause when depth < max_cause_depth -> loop (depth + 1) cause
        | None | Some _ -> None)
  in
  loop 0 failure

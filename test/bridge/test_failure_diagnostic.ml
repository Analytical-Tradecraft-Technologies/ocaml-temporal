(** Regression tests for the shared structured-failure diagnostic.

    These tests construct the same nested shape that Temporal sends for an
    activity timeout: an outer activity wrapper, a timeout record, and the
    application failure beneath it. No server is needed because the property
    under test is the deterministic protocol-to-text traversal. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Diagnostic = Temporal_protocol.Failure_diagnostic

(** Finds a stable fragment without depending on optional String extensions. *)
let contains source needle =
  let source_length = String.length source in
  let needle_length = String.length needle in
  let rec loop index =
    if index + needle_length > source_length then false
    else if String.sub source index needle_length = needle then true
    else loop (index + 1)
  in
  if needle_length = 0 then true else loop 0

(** Builds one failure layer with explicit metadata so the test remains
    independent of JSON decoding and exercises the closed protocol type. *)
let layer ~message ~source ~info ~cause : Protocol.failure =
  {
    message;
    source;
    stack_trace = "";
    encoded_attributes = None;
    cause;
    info;
  }

(** A timeout nested below an activity wrapper must remain visible in the
    public diagnostic instead of being replaced by the outer summary. *)
let test_nested_timeout_is_visible () =
  let detail : Protocol.payload =
    { metadata = []; data = Bytes.of_string "heartbeat" }
  in
  let application =
    layer ~message:"llm call failed" ~source:"activity-worker"
      ~info:(Protocol.Application
               {
                 type_name = "LlmUnavailable";
                 non_retryable = false;
                 details = [ detail ];
                 category = Protocol.Application_category_unspecified;
                 next_retry_delay = None;
               })
      ~cause:None
  in
  let timeout =
    layer ~message:"activity timed out" ~source:"temporal-core"
      ~info:(Protocol.Timeout_failure
               {
                 timeout_type = Protocol.Timeout_start_to_close;
                 last_heartbeat_details = [ detail ];
               })
      ~cause:(Some application)
  in
  let outer =
    layer ~message:"workflow failed" ~source:"temporal-core"
      ~info:(Protocol.Activity
               {
                 scheduled_event_id = 12L;
                 started_event_id = 13L;
                 identity = "worker-1";
                 activity_type = "llm.call";
                 activity_id = "activity-1";
                 retry_state = Protocol.Timeout;
               })
      ~cause:(Some timeout)
  in
  let diagnostic = Diagnostic.failure_diagnostic outer in
  assert (contains diagnostic "workflow failed source=temporal-core");
  assert (contains diagnostic "timeout type=start_to_close last_heartbeat_details=1");
  assert (contains diagnostic "application type=LlmUnavailable non_retryable=false details=1");
  assert (contains diagnostic " | ")

(** Failure kinds outside the application/activity/child family, including a
    failure with no info at all, still render a typed summary for every layer
    so a server-generated cause stays diagnosable. *)
let test_extended_kinds_are_visible () =
  let absent =
    layer ~message:"no info" ~source:"" ~info:Protocol.Absent ~cause:None
  in
  let handler =
    layer ~message:"handler failed" ~source:""
      ~info:(Protocol.Nexus_handler
               { type_name = "INTERNAL"; retry_behavior = Protocol.Nexus_retry_non_retryable })
      ~cause:(Some absent)
  in
  let server =
    layer ~message:"result too large" ~source:""
      ~info:(Protocol.Server { non_retryable = true })
      ~cause:(Some handler)
  in
  let diagnostic = Diagnostic.failure_diagnostic server in
  assert (contains diagnostic "result too large server non_retryable=true");
  assert (contains diagnostic "nexus_handler type=INTERNAL retry_behavior=non_retryable");
  assert (contains diagnostic "no info no_failure_info")

(** A recursively constructed value cannot make diagnostics grow without a
    bound, even though normal JSON decoding already rejects excessive depth. *)
let test_depth_is_bounded () =
  let rec make depth : Protocol.failure =
    let cause = if depth = 0 then None else Some (make (depth - 1)) in
    layer ~message:"nested" ~source:"test"
      ~info:(Protocol.Application
               { type_name = "Nested"; non_retryable = false; details = [];
                 category = Protocol.Application_category_unspecified;
                 next_retry_delay = None })
      ~cause
  in
  let diagnostic = Diagnostic.failure_diagnostic (make 130) in
  assert (contains diagnostic "cause_depth_limit_reached")

(** Public errors keep Core-only application options in a bounded diagnostic
    even when an activity wrapper is the visible failure layer. *)
let test_application_options_survive_cause_diagnostic () =
  let application =
    layer ~message:"expected rejection" ~source:"activity-worker"
      ~info:(Protocol.Application
        { type_name = "ExpectedError"; non_retryable = false; details = [];
          category = Protocol.Application_category_benign;
          next_retry_delay = Some { seconds = 3L; nanoseconds = 7 } })
      ~cause:None
  in
  let outer =
    layer ~message:"activity failed" ~source:"server"
      ~info:(Protocol.Activity
        { scheduled_event_id = 1L; started_event_id = 2L;
          identity = "worker"; activity_type = "lookup";
          activity_id = "lookup-1";
          retry_state = Protocol.Maximum_attempts_reached })
      ~cause:(Some application)
  in
  let diagnostic = Diagnostic.failure_diagnostic outer in
  assert (contains diagnostic "category=benign");
  assert (contains diagnostic "next_retry_delay=3.000000007s");
  assert (contains diagnostic "activity id=lookup-1")

(** Truncated messages, sources, and stack traces must stay valid UTF-8 (#773).
    Each field is filled with three-byte characters so that a byte cut
    at the field limit (2,048, 512, or 1,024 bytes) would split a code point;
    an invalid result would later be replaced wholesale by a generic workflow
    failure diagnostic. *)
let test_truncation_keeps_utf_8 () =
  let straddling limit =
    (* No limit is a multiple of three, so each byte cut lands mid-character. *)
    assert (limit mod 3 <> 0);
    String.concat "" (List.init ((limit / 3) + 2) (fun _ -> "\xe6\x97\xa5"))
  in
  let value =
    {
      (layer ~message:(straddling 2048) ~source:(straddling 512)
         ~info:(Protocol.Terminated { identity = "operator" })
         ~cause:None)
      with
      stack_trace = straddling 1024;
    }
  in
  let diagnostic = Diagnostic.failure_diagnostic value in
  assert (String.is_valid_utf_8 diagnostic);
  assert (contains diagnostic "\xe6\x97\xa5...");
  assert (String.length diagnostic < 2048 + 512 + 1024 + 256)

(** Runs the pure diagnostic regression cases. *)
let () =
  test_nested_timeout_is_visible ();
  test_depth_is_bounded ();
  test_application_options_survive_cause_diagnostic ();
  test_truncation_keeps_utf_8 ();
  test_extended_kinds_are_visible ()

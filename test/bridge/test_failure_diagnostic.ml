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

(** The application type is read through activity, child-workflow, and timeout
    wrappers; the first application layer wins; an empty type, a chain without
    an application layer, and a layer beyond the depth bound yield [None]. *)
let test_application_failure_type () =
  let application type_name cause =
    layer ~message:"app" ~source:"worker"
      ~info:(Protocol.Application
               { type_name; non_retryable = false; details = [];
                 category = Protocol.Application_category_unspecified;
                 next_retry_delay = None })
      ~cause
  in
  let activity cause =
    layer ~message:"activity" ~source:"core"
      ~info:(Protocol.Activity
               { scheduled_event_id = 1L; started_event_id = 2L;
                 identity = "w"; activity_type = "act"; activity_id = "a";
                 retry_state = Protocol.Non_retryable_failure })
      ~cause
  in
  let child cause =
    layer ~message:"child" ~source:"core"
      ~info:(Protocol.Child_workflow
               { namespace = "default"; workflow_id = "c"; run_id = "r";
                 workflow_type = "child"; initiated_event_id = 1L;
                 started_event_id = 2L;
                 retry_state = Protocol.Non_retryable_failure })
      ~cause
  in
  let timeout cause =
    layer ~message:"timeout" ~source:"core"
      ~info:(Protocol.Timeout_failure
               { timeout_type = Protocol.Timeout_start_to_close;
                 last_heartbeat_details = [] })
      ~cause
  in
  let type_of = Diagnostic.application_failure_type in
  assert (type_of (application "InvalidInput" None) = Some "InvalidInput");
  assert (
    type_of (activity (Some (application "InvalidInput" None)))
    = Some "InvalidInput");
  assert (
    type_of (child (Some (activity (Some (application "Deep" None)))))
    = Some "Deep");
  assert (
    type_of (activity (Some (timeout (Some (application "Late" None)))))
    = Some "Late");
  assert (
    type_of (application "Outer" (Some (application "Inner" None)))
    = Some "Outer");
  assert (type_of (activity (Some (application "" None))) = None);
  assert (type_of (activity (Some (timeout None))) = None);
  let rec wrap depth inner =
    if depth = 0 then inner else activity (Some (wrap (depth - 1) inner))
  in
  assert (type_of (wrap 128 (application "Bounded" None)) = Some "Bounded");
  assert (type_of (wrap 129 (application "TooDeep" None)) = None)

(** Inbound application failure types written by other SDKs may contain
    control characters, invalid UTF-8 or more than the outbound limit (#1011).
    The extracted type must be deterministic, must leave valid text unchanged,
    and must always be accepted by [Error.make], which raises on the raw
    values. *)
let test_inbound_failure_type_is_sanitized () =
  let application type_name =
    layer ~message:"app" ~source:"python"
      ~info:(Protocol.Application
               { type_name; non_retryable = false; details = [];
                 category = Protocol.Application_category_unspecified;
                 next_retry_delay = None })
      ~cause:None
  in
  let activity cause =
    layer ~message:"activity" ~source:"core"
      ~info:(Protocol.Activity
               { scheduled_event_id = 1L; started_event_id = 2L;
                 identity = "w"; activity_type = "act"; activity_id = "a";
                 retry_state = Protocol.Non_retryable_failure })
      ~cause:(Some cause)
  in
  let accepted raw =
    match Diagnostic.application_failure_type (activity (application raw)) with
    | None -> assert false
    | Some sanitized ->
        let error =
          Temporal_base.Error.make ~error_type:sanitized ~category:`Activity
            ~message:"inbound" ()
        in
        assert (Temporal_base.Error.error_type error = Some sanitized);
        assert (Diagnostic.sanitize_inbound_error_type raw = sanitized);
        sanitized
  in
  let raises raw =
    match
      Temporal_base.Error.make ~error_type:raw ~category:`Activity
        ~message:"raw" ()
    with
    | _ -> false
    | exception Invalid_argument _ -> true
  in
  assert (raises "Invalid\tInput");
  assert (accepted "Invalid\tInput" = "Invalid\\u0009Input");
  assert (accepted "a\nb\x7f" = "a\\u000Ab\\u007F");
  assert (accepted "nul\000x" = "nul\\u0000x");
  assert (accepted "bad\xffutf8" = "bad\xef\xbf\xbdutf8");
  assert (accepted "Valid \xe6\x97\xa5 \"quoted\"" = "Valid \xe6\x97\xa5 \"quoted\"");
  let limit = Diagnostic.max_inbound_error_type_encoded_length in
  let exact = String.make limit 'x' in
  assert (accepted exact = exact);
  (* The Rust bridge truncates an over-long type with a marker that starts
     with a newline; both the length and the marker must be repaired. *)
  let over_long = String.make (limit + 10) 'y' ^ "\n...[truncated]" in
  assert (raises over_long);
  let sanitized = accepted over_long in
  assert (String.length sanitized = limit);
  assert (String.ends_with ~suffix:"..." sanitized);
  (* A cut never splits a multi-byte character or an escape sequence, and a
     quote counts two encoded characters. *)
  let multibyte = String.concat "" (List.init limit (fun _ -> "\xe6\x97\xa5")) in
  let sanitized = accepted multibyte in
  assert (String.is_valid_utf_8 sanitized);
  let quoted = String.make limit '"' in
  let sanitized = accepted quoted in
  assert (String.length sanitized = ((limit - 3) / 2) + 3);
  let controls = String.make limit '\t' in
  let sanitized = accepted controls in
  assert (String.length sanitized = ((limit - 3) / 7 * 6) + 3)

(** One payload whose bytes name it, for order and membership checks. *)
let payload name : Protocol.payload = { metadata = []; data = Bytes.of_string name }

(** Names of [payloads] in order. *)
let names payloads =
  List.map (fun (value : Protocol.payload) -> Bytes.to_string value.data) payloads

(** An application failure layer carrying [details]. *)
let application ?(details = []) cause =
  layer ~message:"business" ~source:"worker"
    ~info:(Protocol.Application
             {
               type_name = "Business";
               non_retryable = false;
               details;
               category = Protocol.Application_category_unspecified;
               next_retry_delay = None;
             })
    ~cause

(** A timeout layer of [timeout_type] carrying [heartbeat] details. *)
let timeout ?(heartbeat = []) timeout_type cause =
  layer ~message:"timed out" ~source:"server"
    ~info:(Protocol.Timeout_failure { timeout_type; last_heartbeat_details = heartbeat })
    ~cause

(** An activity wrapper around [cause]. *)
let activity cause =
  layer ~message:"activity failed" ~source:"core"
    ~info:(Protocol.Activity
             {
               scheduled_event_id = 5L;
               started_event_id = 6L;
               identity = "worker";
               activity_type = "charge";
               activity_id = "1";
               retry_state = Protocol.Timeout;
             })
    ~cause

(** A child-workflow wrapper around [cause]. *)
let child cause =
  layer ~message:"child failed" ~source:"core"
    ~info:(Protocol.Child_workflow
             {
               namespace = "default";
               workflow_id = "child";
               run_id = "run";
               workflow_type = "child";
               initiated_event_id = 5L;
               started_event_id = 6L;
               retry_state = Protocol.Timeout;
             })
    ~cause

(** [failure_origin] (#1143) skips the activity, child and Nexus operation
    wrappers and an info-less layer that has a cause, and classifies the
    first other layer: a timeout of each type, including one that wraps the
    last attempt's application failure, an application failure, a server
    failure, cancellation and termination. A chain of wrappers only, a reset
    and a Nexus handler error are [`Other]. *)
let test_failure_origin () =
  let origin = Diagnostic.failure_origin in
  let bare info = layer ~message:"m" ~source:"s" ~info ~cause:None in
  List.iter
    (fun (timeout_type, expected) ->
      assert (origin (activity (Some (timeout timeout_type (Some (application None)))))
              = `Timeout expected))
    [
      (Protocol.Timeout_start_to_close, `Start_to_close);
      (Protocol.Timeout_schedule_to_start, `Schedule_to_start);
      (Protocol.Timeout_schedule_to_close, `Schedule_to_close);
      (Protocol.Timeout_heartbeat, `Heartbeat);
      (Protocol.Timeout_unspecified, `Unspecified);
    ];
  assert (origin (child (Some (timeout Protocol.Timeout_start_to_close None)))
          = `Timeout `Start_to_close);
  assert (origin (activity (Some (application None))) = `Application);
  assert (origin (child (Some (activity (Some (application None))))) = `Application);
  assert (origin (activity (Some (bare (Protocol.Server { non_retryable = true }))))
          = `Server);
  assert (origin (activity (Some (bare (Protocol.Canceled { details = []; identity = "" }))))
          = `Cancelled);
  assert (origin (child (Some (bare (Protocol.Terminated { identity = "" })))) = `Terminated);
  assert (origin
            (layer ~message:"m" ~source:"s" ~info:Protocol.Absent
               ~cause:(Some (application None)))
          = `Application);
  assert (origin (activity None) = `Other);
  assert (origin (bare Protocol.Absent) = `Other);
  assert (origin (bare (Protocol.Reset_workflow { last_heartbeat_details = [] })) = `Other)

(** Heartbeat payloads are a timeout's or reset's own record (#1143):
    [failure_details] keeps only application and cancellation details, and
    [last_heartbeat_details] returns the heartbeat payloads, both outer to
    inner. *)
let test_heartbeat_details_are_separate () =
  let failure =
    activity
      (Some
         (timeout ~heartbeat:[ payload "progress-42" ] Protocol.Timeout_heartbeat
            (Some (application ~details:[ payload "business" ] None))))
  in
  assert (names (Diagnostic.failure_details failure) = [ "business" ]);
  assert (names (Diagnostic.last_heartbeat_details failure) = [ "progress-42" ]);
  let reset =
    layer ~message:"reset" ~source:"server"
      ~info:(Protocol.Reset_workflow { last_heartbeat_details = [ payload "outer" ] })
      ~cause:(Some (timeout ~heartbeat:[ payload "inner" ] Protocol.Timeout_heartbeat None))
  in
  assert (names (Diagnostic.last_heartbeat_details reset) = [ "outer"; "inner" ]);
  assert (Diagnostic.failure_details reset = [])

(** Runs the pure diagnostic regression cases. *)
let () =
  test_inbound_failure_type_is_sanitized ();
  test_application_failure_type ();
  test_nested_timeout_is_visible ();
  test_depth_is_bounded ();
  test_application_options_survive_cause_diagnostic ();
  test_truncation_keeps_utf_8 ();
  test_extended_kinds_are_visible ();
  test_failure_origin ();
  test_heartbeat_details_are_separate ()

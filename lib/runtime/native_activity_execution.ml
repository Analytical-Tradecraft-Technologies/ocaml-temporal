(** Private typed execution of one native Temporal activity-task lease.

    Rust/Core owns polling, protobuf conversion, and network concurrency. This
    module owns only the OCaml half: a deterministic activity registry, codec
    conversion, implementation dispatch, and the small pending-completion map
    needed when the native completion call is temporarily unavailable. The map
    is deliberately protected by one mutex; no task can be executed twice merely
    because its completion transport failed. *)

module Protocol = Temporal_protocol.Activity_protocol
module Encoded_completion = Temporal_protocol.Encoded_activity_completion
module Encoded_heartbeat = Temporal_protocol.Encoded_activity_heartbeat
module Encoded_async_heartbeat = Temporal_protocol.Encoded_async_activity_heartbeat
module Definition = Temporal_base.Definition
module Codec = Temporal_base.Codec
module Base_error = Temporal_base.Error
module Observability = Temporal_base.Observability
module Activity_context = Temporal_base.Activity_context
module Async_activity = Temporal_base.Async_activity
module Worker_policy = Native_worker_policy
module Metrics = Native_worker_metrics

(** Result-bind notation keeps expected protocol and codec failures on typed
    paths rather than using exceptions as ordinary activity control flow. *)
let ( let* ) = Result.bind

(** The native operations required by the adapter. Keeping this signature
    independent of the concrete supervisor makes task ownership testable with a
    deterministic queue and avoids coupling this private layer to Rust ABI
    details. *)
module type SUPERVISOR = sig
  type t
  type error

  val try_poll_activity : t -> (Protocol.task option, error) result
  (* Both completion calls receive the canonical bytes from the adapter's
     single encoder pass (issue #995); implementations submit them without
     encoding the completion again. *)
  val complete_activity : t -> Encoded_completion.t -> (unit, error) result
  (* Submits a completion for an activity that has already returned
     [WillCompleteAsync]. This path uses Core's namespace-bound client and
     never consults the worker task-token ledger. It and
     [record_async_activity_heartbeat] are called with no adapter lock held
     and may wait for the server on the calling thread (#1025). *)
  val complete_async_activity :
    t -> Encoded_completion.t -> (unit, error) result
  (* Both heartbeat calls likewise receive the canonical bytes from the
     adapter's single encoder pass (issue #1001) and never encode again. *)
  val record_activity_heartbeat :
    t -> Encoded_heartbeat.t -> (unit, error) result
  (* Records heartbeat details through the namespace-bound async handle. The
     bridge throttles these per task token (#1025): a call may only record
     its details for a trailing send. The response is the activity state the
     token's latest send reported. *)
  val record_async_activity_heartbeat :
    t ->
    Encoded_async_heartbeat.t ->
    (Protocol.async_heartbeat_response, error) result
  val error_code : error -> string
  val error_message : error -> string
  val error_is_retryable : error -> bool
  val async_operation_error_disposition :
    error -> Worker_policy.async_operation_disposition
  val exception_is_retryable : exn -> bool
end

type error_view = {
  code : string;
  path : string;
  message : string;
  retryable : bool;
}
(** Stable diagnostics never contain payload bytes or opaque task-token data. *)

(** Public registration is existential so definitions with unrelated OCaml
    input/output types can share one name-indexed registry. *)
type registered_activity =
  | Activity :
      ( 'input,
        'output,
        Activity_context.t -> 'input -> ('output, Base_error.t) result )
      Definition.t
      -> registered_activity
  | Async_activity :
      ( 'input,
        'output,
        'output Temporal_base.Async_activity.context ->
        'input -> 'output Temporal_base.Async_activity.async_result )
      Definition.t
      -> registered_activity

(** Completion classes exposed in the small private outcome summary. *)
type completion_kind = Succeeded | Failed | Cancelled | Deferred

(** Cancellation facts copied from Core alongside a [Cancelled] completion.
    Keeping this optional metadata on the private outcome lets worker-loop
    instrumentation observe pause/reset/cancel distinctions without placing
    them in the Temporal failure payload or pretending that a heartbeat call
    returned synchronously. *)
type cancellation_details = Protocol.cancellation_details

(** One serialized adapter transaction result. The opaque token itself stays
    inside the supervisor and pending-lease map; only the activity type and
    terminal class are exposed for safe metrics and logs. *)
type outcome =
  | Not_ready
  | Completed of {
      activity_type : string option;
      kind : completion_kind;
      cancellation_details : cancellation_details option;
    }
  | Rejected of {
      activity_type : string option;
      error : error_view;
      lease_retired : bool;
    }

(** Heterogeneous definition registry values. *)
type registered_definition =
  | Registered_definition :
      ( 'input,
        'output,
        Activity_context.t -> 'input -> ('output, Base_error.t) result )
      Definition.t
      -> registered_definition
  | Registered_async_definition :
      ( 'input,
        'output,
        'output Temporal_base.Async_activity.context ->
        'input -> 'output Temporal_base.Async_activity.async_result )
      Definition.t
      -> registered_definition

(** Immutable comparison for copied opaque task-token bytes. The adapter never
    uses a token as a string, so embedded NUL bytes and non-UTF-8 bytes remain
    unchanged. *)
module Token_map = Map.Make (struct
  (* Keeping the key as [bytes] prevents accidental text decoding of the
     opaque token, while [Bytes.compare] gives the retry map a stable order. *)
  type t = bytes

  let compare = Bytes.compare
end)

module Name_map = Map.Make (String)
(** Name lookup is separate from token lookup because Temporal schedules by an
    activity type while Core correlates completion by an opaque token. *)

(** Identity of one activity attempt, copied from its start task so the
    adapter's lease records can be correlated with the workflow history and
    with application logs emitted inside the callback (#500). These are
    execution identifiers: they go only into log tags, never into metric
    labels, completion bytes, or error messages. The task token is
    deliberately absent because it is an opaque, credential-like value. *)
type correlation = {
  workflow_id : string;
  run_id : string;
  workflow_type : string;
  activity_id : string;
  activity_type : string;
  attempt : int;
}

(** Why a lease carries a failure completion. [Application_failure] is an
    ordinary [Error] returned by the activity implementation, which Temporal
    routinely retries; [Sdk_rejection] is an adapter-side refusal (unknown
    activity type, codec or protocol error, raised callback) that indicates a
    worker or caller defect. The distinction only selects the log operation and
    level (issue #1024); both are acknowledged with the same failure completion
    and reported to the caller as [Rejected]. *)
type rejection_origin = Application_failure | Sdk_rejection

(** The server-side timeouts of an asynchronously completed attempt, in
    seconds, copied from its start task; [None] means unset. They bound how
    long the adapter keeps the attempt's async lease (#1043).
    [heartbeat_timeout] is the start task's exact heartbeat timeout, which
    every async heartbeat carries so the bridge can derive its throttle
    interval (#1025). *)
type async_timeouts = {
  start_to_close_s : float option;
  schedule_to_close_s : float option;
  heartbeat_s : float option;
  heartbeat_timeout : Protocol.duration option;
}

(** What the caller should observe after a lease is acknowledged. A rejected
    task carries the original adapter diagnostic so the caller can identify a
    bad registration or payload without inspecting the completion bytes. An
    async handoff carries the attempt's timeouts so the admitted lease can
    expire. *)
type accepted_result =
  | Completed_result of {
      kind : completion_kind;
      cancellation_details : cancellation_details option;
    }
  | Rejected_result of { error : error_view; origin : rejection_origin }
  | Async_handoff : {
      handle : 'output Temporal_base.Async_activity.handle;
      timeouts : async_timeouts;
    }
      -> accepted_result

type lease = {
  token : bytes;
  correlation : correlation option;
  encoded : Encoded_completion.t;
  accepted_result : accepted_result;
  mutable retry_refusal : error_view option;
}
(** One completion that has not yet been proven accepted by native Core. The
    [token] is always an owned copy. [encoded] is the immutable canonical JSON
    from the completion's single encoder pass, which also embeds that same
    token; every submission and retry sends exactly these bytes, so no retry
    re-encodes the completion or observes a later buffer mutation
    (issue #995).

    [correlation] is the start task's identity when the adapter saw one; it is
    [None] only for a bare cancellation whose start this adapter never
    received. It is used for log correlation and the outcome's activity type,
    never for completion encoding.

    [retry_refusal] records the first failure that was not explicitly
    classified as retryable. Once it is [Some], the completion is never
    submitted again: pinned Core removes the activity from its outstanding set
    before suppressing completion transport errors, and an uncertain or
    post-acceptance failure may already have retired the lease, so a second
    submission could duplicate it. Later polls and drains return the recorded
    error unchanged and only terminal [discard] releases the lease
    (issue #843). *)

(** An async lease is the capability retained after Core accepts
    [WillCompleteAsync]. It is deliberately kept in a separate map from worker
    task leases because later operations use the namespace-bound Temporal
    client and must never be sent through the worker completion ledger.

    [uncertain_completion] holds the request key
    ([Async_activity.operation_key]) of a terminal operation whose last
    submission had an unknown native outcome ([Retryable_submission]),
    together with the bytes of its single encoder pass. The handle admits
    only a request with that same key until it is resolved, and equal keys
    mean the same wire request even when the operations differ structurally,
    so every admitted retry resends these physically identical bytes instead
    of preparing and encoding the completion again (issue #1001). Any other
    completion outcome clears it, and a request with a different key is
    prepared afresh, so it is never sent the retained bytes.

    [correlation] names the attempt in the expiry record. [attempt_deadline]
    and [heartbeat_s] bound the lease's life (#1043): once the server must
    have timed the attempt out, no terminal call can succeed, so the adapter
    closes the handle and drops the lease instead of letting it hold every
    later worker shutdown for the full grace period. Both are measured on the
    worker's monotonic clock, so a wall-clock step (NTP or manual) cannot
    make a live lease look expired, from moments that are no earlier than the server's own
    reference points: [attempt_deadline] adds the shorter of the
    start-to-close and schedule-to-close timeouts to the time of admission,
    which follows the attempt's start, and [last_progress] (admission, then
    the end of each async heartbeat call that may have reached the server or
    was recorded for a throttled trailing send) bounds the server's last
    recorded heartbeat to within one throttle interval and RPC after it (see
    [async_lease_expired]). The adapter therefore never expires a lease
    before the server has; it may only expire it later. [heartbeat_timeout]
    is the exact value every async heartbeat document carries (#1025). *)
type async_lease =
  | Async_lease :
      {
        token : bytes;
        handle : 'output Temporal_base.Async_activity.handle;
        correlation : correlation option;
        attempt_deadline : float option;
        heartbeat_s : float option;
        heartbeat_timeout : Protocol.duration option;
        mutable last_progress : float;
        mutable in_flight : bool;
        mutable uncertain_completion : (string * Encoded_completion.t) option;
      }
      -> async_lease

(** Extra time, in seconds, beyond the latest moment the server could still
    accept a terminal call for an async attempt. The deadlines above are
    already conservative; this margin also covers the delay before the
    server's timeout task runs. *)
let async_lease_expiry_slack_s = 5.0

(** Converts one optional task timeout to seconds, treating a zero duration
    as unset, as [configured_timeout] does for [Activity.Info]. *)
let timeout_seconds = function
  | None -> None
  | Some { Protocol.seconds = 0L; nanoseconds = 0 } -> None
  | Some { Protocol.seconds; nanoseconds } ->
      Some (Int64.to_float seconds +. (float_of_int nanoseconds /. 1e9))

(** Copies the timeouts that bound an asynchronous attempt from its start
    task. *)
let async_timeouts_of_start (start : Protocol.activity_start) =
  {
    start_to_close_s = timeout_seconds start.start_to_close_timeout;
    schedule_to_close_s = timeout_seconds start.schedule_to_close_timeout;
    heartbeat_s = timeout_seconds start.heartbeat_timeout;
    heartbeat_timeout = start.heartbeat_timeout;
  }

(** The attempt deadline of an async lease admitted at [admitted_at]: the
    shorter configured close timeout after admission, or [None] when the
    task reported neither (the server always sets one for a remote activity,
    so [None] only arises from a test double). *)
let async_attempt_deadline ~admitted_at timeouts =
  match (timeouts.start_to_close_s, timeouts.schedule_to_close_s) with
  | None, None -> None
  | Some timeout, None | None, Some timeout -> Some (admitted_at +. timeout)
  | Some first, Some second -> Some (admitted_at +. Float.min first second)

(** Whether the server must already have timed out the attempt behind
    [lease] at time [now].

    The heartbeat bound is twice the heartbeat timeout after the last
    progress, not once (#1025): the bridge throttles async heartbeats, so a
    call can return after only recording its details, and the trailing send
    that carries them follows within the throttle interval (at most 80% of
    the heartbeat timeout) plus one RPC. The server's last heartbeat can
    therefore be up to that much later than [last_progress]; the second
    timeout covers the interval and leaves the rest for the trailing RPC. *)
let async_lease_expired ~now (Async_lease lease) =
  let past deadline = now > deadline +. async_lease_expiry_slack_s in
  (match lease.attempt_deadline with
  | Some deadline -> past deadline
  | None -> false)
  ||
  match lease.heartbeat_s with
  | Some timeout -> past (lease.last_progress +. (2.0 *. timeout))
  | None -> false

(** Bounds diagnostics before they are sent to Logs or embedded in a
    non-retryable Temporal failure. Invalid UTF-8 is replaced because the
    protocol's string fields are strict UTF-8. *)
let bounded_text ?(maximum = 1_024) ~fallback value =
  if not (Codec.valid_utf_8 value) then fallback
  else if String.length value <= maximum then value
  else
    (* Start from the maximum useful prefix and back off until the cut lands
       on a UTF-8 boundary; diagnostics must remain valid protocol strings. *)
    let rec prefix length =
      if length <= 0 then fallback
      else
        let value = String.sub value 0 length in
        if Codec.valid_utf_8 value then value ^ "..." else prefix (length - 1)
    in
    prefix (maximum - 3)

(** Bounds a source classification separately from its human-readable text. *)
let bounded_code value = bounded_text ~fallback:"native_activity_error" value

(** Constructs one immutable privacy-safe diagnostic. *)
let make_error ?(path = "$") ?(retryable = false) code message : error_view =
  {
    code = bounded_code code;
    path;
    message = bounded_text ~fallback:"invalid activity diagnostic" message;
    retryable;
  }

(** Converts an unexpected OCaml exception into a typed boundary diagnostic.
    Exceptions are still defects; catching them here prevents a user activity
    from unwinding past the lease and leaving native Core without a response. *)
let exception_error ?(path = "$") ?(retryable = false) exception_ =
  let message =
    try Printexc.to_string exception_ with _ -> "unprintable OCaml exception"
  in
  make_error ~path ~retryable "ocaml_exception" message

(** Converts a supervisor error without trusting its diagnostic accessors to be
    exception-free. This guard keeps lifecycle failures on the typed path. *)
let supervisor_error ?(path = "$") ?(retryable = false) ~error_code
    ~error_message source_error =
  try make_error ~path ~retryable (error_code source_error) (error_message source_error)
  with exception_ -> exception_error ~path exception_

(** Converts the protocol's private diagnostic into this adapter's stable
    representation. *)
let protocol_error ?(path = "$") error =
  let view = Protocol.error_view error in
  make_error ~path view.code view.message

(** Converts a public structured error to an adapter diagnostic. Payload details
    remain available to callers of [Error.view] but are not copied into a wire
    failure or log message at this boundary. *)
let application_error ?(path = "$") error =
  let view = Base_error.view error in
  make_error ~path (Base_error.kind error) view.message

(** Builds the application failure used when a task cannot be dispatched
    locally. Empty details keep the failure independent of mutable application
    payloads and make it safe to retry completion submission.

    The default is a non-retryable failure of type
    ["ocaml_temporal_native_activity"], for defects that another attempt on
    any worker would repeat (an unrepresentable heartbeat context, an output
    that cannot be encoded). A task that another worker sharing the queue
    might be able to run, such as an unregistered activity type or an input
    this build cannot decode, passes [~non_retryable:false] (and, for an
    unknown type, [~type_name:"NotFoundError"]) so the activity's retry policy
    decides, as in the other Temporal SDKs (#1031). *)
let failure_of_error ?(type_name = "ocaml_temporal_native_activity")
    ?(non_retryable = true) (error : error_view) : Protocol.failure =
  Protocol.
    {
      message = error.message;
      source = "ocaml-temporal";
      stack_trace = "";
      encoded_attributes = None;
      cause = None;
      info =
        Application
          {
            type_name;
            non_retryable;
            details = [];
            category = Application_category_unspecified;
            next_retry_delay = None;
          };
    }

(** Builds the failure for an exception raised by application activity code.
    The SDK contract reserves exceptions for programmer defects
    ([Match_failure], [Assert_failure], [Invalid_argument], ...); expected and
    transient failures are returned as typed [Error] values, which keep their
    own retryability. Retrying a defect would rerun broken code, without bound
    under Temporal's default retry policy, so the failure is non-retryable.
    Unlike adapter dispatch failures it has the distinct [ocaml_exception]
    type and carries the OCaml backtrace when backtrace recording is enabled,
    bounded and valid UTF-8 to fit the protocol's strict string fields. *)
let callback_exception_failure (diagnostic : error_view) backtrace :
    Protocol.failure =
  Protocol.
    {
      message = diagnostic.message;
      source = "ocaml-temporal";
      stack_trace = bounded_text ~maximum:8_192 ~fallback:"" backtrace;
      encoded_attributes = None;
      cause = None;
      info =
        Application
          {
            type_name = "ocaml_exception";
            non_retryable = true;
            details = [];
            category = Application_category_unspecified;
            next_retry_delay = None;
          };
    }

(** Maps Core's protocol reason to the context's reason one to one. *)
let context_cancel_reason = function
  | Protocol.Cancellation_not_found -> Activity_context.Not_found
  | Cancellation_requested -> Requested
  | Cancellation_timed_out -> Timed_out
  | Cancellation_worker_shutdown -> Worker_shutdown
  | Cancellation_paused -> Paused
  | Cancellation_reset -> Reset

(** Uses a stable, closed set of cancellation labels so cancellation failures
    remain valid protocol strings even when the source task is malformed. The
    labels are shared with the activity context's cancellation errors. *)
let cancellation_reason reason =
  Activity_context.cancel_reason_label (context_cancel_reason reason)

(** Converts a Core cancellation task into the value published to a running
    attempt's context. The primary reason comes first; each independent detail
    flag adds its reason once, in Core's field order. *)
let context_cancellation (cancel : Protocol.activity_cancel) :
    Activity_context.cancellation =
  let primary = context_cancel_reason cancel.reason in
  let flagged =
    match cancel.details with
    | None -> []
    | Some details ->
        List.filter_map
          (fun (set, reason) -> if set then Some reason else None)
          [
            (details.is_cancelled, Activity_context.Requested);
            (details.is_timed_out, Timed_out);
            (details.is_not_found, Not_found);
            (details.is_worker_shutdown, Worker_shutdown);
            (details.is_paused, Paused);
            (details.is_reset, Reset);
          ]
  in
  {
    reason = primary;
    reasons = primary :: List.filter (fun reason -> reason <> primary) flagged;
  }

(** Converts a cancellation task into the standard Temporal canceled failure.
    Cancellation details are intentionally not copied into a second completion
    payload: Core already carries the exact reason and flags in the task. *)
let cancellation_failure ?(details = []) reason : Protocol.failure =
  Protocol.
    {
      message = "activity cancellation requested: " ^ cancellation_reason reason;
      source = "temporal";
      stack_trace = "";
      encoded_attributes = None;
      cause = None;
        info = Canceled { details; identity = "ocaml-temporal" };
    }

(** Converts binary protocol metadata to the runtime's string metadata without
    replacement decoding. Metadata values are arbitrary bytes in Temporal's
    protocol and an OCaml [string] holds bytes, so keys and values are carried
    losslessly instead of failing the task (#1048); the protocol decoder has
    already bounded them. Body and metadata bytes are copied so a task can be
    retained safely after the supervisor releases its source buffer. *)
let runtime_payload _path (payload : Protocol.payload) =
  let metadata =
    List.map (fun (key, bytes) -> (key, Bytes.to_string bytes)) payload.metadata
  in
  Ok { Temporal_base.Payload.metadata; data = Bytes.copy payload.data }

(** Converts a runtime payload into the binary-safe protocol representation,
    validating metadata keys before copying it across the ownership boundary.
    Keys follow the bridge protocol's rule (non-empty, valid UTF-8, at most
    65536 bytes; NUL is allowed) and values may be any bytes, so a payload
    received from Temporal can be returned or heartbeated unchanged. *)
let protocol_payload path (payload : Temporal_base.Codec.payload) =
  (* Use the same order-preserving construction as [runtime_payload], but turn
     validated strings back into freshly owned byte buffers. *)
  let rec metadata_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | (key, value) :: rest ->
        if String.length key = 0 || String.length key > 65_536 then
          Error
            (make_error ~path:(path ^ ".metadata") "invalid_message"
               "metadata key must be non-empty and at most 65536 bytes")
        else if not (Codec.valid_utf_8 key) then
          Error
            (make_error ~path:(path ^ ".metadata.key") "invalid_message"
               "metadata key must be valid UTF-8")
        else metadata_loop ((key, Bytes.of_string value) :: reversed) rest
  in
  let* metadata = metadata_loop [] payload.metadata in
  Ok Protocol.{ metadata; data = Bytes.copy payload.data }

(** Converts a list of protocol payloads while preserving order and copying
    every body. The indexed path makes malformed metadata diagnosable without
    exposing the payload bytes themselves. *)
let runtime_payloads path payloads =
  (* The index is part of the diagnostic path; the reversed accumulator keeps
     traversal tail-recursive while [List.rev] restores input order. *)
  let rec loop index reversed = function
    | [] -> Ok (List.rev reversed)
    | payload :: rest ->
        let* payload = runtime_payload (Printf.sprintf "%s[%d]" path index) payload in
        loop (index + 1) (payload :: reversed) rest
  in
  loop 0 [] payloads

(** Converts an activity implementation's structured error while preserving its
    retryability classification, its application error type (falling back to
    the category label when untyped; this is the value Temporal matches against
    a retry policy's [non_retryable_error_types]), and every
    application-supplied detail payload.
    Adapter-generated input/registry errors use [failure_of_error] and are
    always non-retryable; an application error may intentionally remain
    retryable so Temporal can apply its retry policy. Detail payloads are
    validated and copied through the same boundary helper as successful
    activity output. *)
let failure_of_application_error (diagnostic : error_view)
    (error : Base_error.t) : (Protocol.failure, error_view) result =
  let view = Base_error.view error in
  (* Validate and copy every detail in order. A failed conversion aborts before
     any completion is submitted, so a malformed detail cannot retire a lease
     with a partially constructed application failure. *)
  let rec details_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | payload :: rest ->
        let* payload = protocol_payload "$.completion.result.info.details" payload in
        details_loop (payload :: reversed) rest
  in
  let* details = details_loop [] view.details in
  Ok
    Protocol.
      {
        message = diagnostic.message;
        source = "ocaml-temporal";
        stack_trace = "";
        encoded_attributes = None;
        cause = None;
        info =
          Application
            {
              type_name = Base_error.application_failure_type error;
              non_retryable = view.non_retryable;
              details;
              category = Application_category_unspecified;
              next_retry_delay = None;
            };
      }

(** Decodes the argument-list shape used by activity tasks. The SDK accepts one
    typed value (or the canonical unit payload) and explicitly rejects extra
    values rather than silently dropping arguments. *)
let decode_input definition arguments =
  let payload_result =
    match arguments with
    | [] ->
        Ok
          {
            Temporal_base.Payload.metadata = [ ("encoding", "binary/null") ];
            data = Bytes.empty;
          }
    | [ payload ] -> runtime_payload "$.variant.input[0]" payload
    | _ ->
        Error
          (make_error ~path:"$.variant.input" "unsupported"
             "activity definitions currently accept exactly one input value")
  in
  let* payload = payload_result in
  match Codec.decode (Definition.input definition) payload with
  | Ok input -> Ok input
  | Error error -> Error (application_error ~path:"$.variant.input" error)

(** Which part of a start task could not be converted for the callback. The
    distinction selects the failure's retryability (#1031): an input that
    this build cannot decode may come from a newer caller and decode on an
    updated worker sharing the queue, so it is retried under the activity's
    retry policy, while an unrepresentable heartbeat context is a property of
    the recorded task itself and stays non-retryable. *)
type decode_failure =
  | Input_rejected of error_view
  | Context_rejected of error_view

(** Converts the callback input and heartbeat details shared by both
    definition styles. Callers must route any conversion error through
    [reject_decode_failure]: a wire-valid task already owns a native
    completion lease even when its input or heartbeat context is
    unsupported. *)
let decode_payloads definition (start : Protocol.activity_start) =
  match decode_input definition start.input with
  | Error error -> Error (Input_rejected error)
  | Ok input -> (
      match
        runtime_payloads "$.variant.heartbeat_details" start.heartbeat_details
      with
      | Error error -> Error (Context_rejected error)
      | Ok details -> Ok (input, details))

(** Converts a task timeout to whole milliseconds for
    [Temporal.Activity.Info] and [Context.heartbeat_timeout], rounding a
    sub-millisecond remainder up.

    The conversion is total: exposing metadata must never add a
    task-rejection path, so a sub-millisecond heartbeat interval scheduled by
    another SDK is accepted rather than failing the task (#1032). Rounding up
    keeps a positive timeout positive (zero would read as "already
    expired"). The result is clamped to 315,576,000,000,999 ms, the
    largest whole-millisecond protobuf duration and the maximum accepted by
    [Temporal.Duration.of_ms]: rounding up the valid protobuf maximum
    (315,576,000,000 s plus 999,999,999 ns) would otherwise exceed it by one
    millisecond. The strict decoder bounds seconds to the protobuf duration
    range and nanoseconds to [0, 1e9); the clamps and saturation below only
    make the function total should that invariant ever change. *)
let info_duration (duration : Protocol.duration) =
  let maximum_ms = 315_576_000_000_999L in
  let seconds = Int64.max 0L duration.seconds in
  let nanoseconds = max 0 (min 999_999_999 duration.nanoseconds) in
  let fraction_ms = Int64.of_int ((nanoseconds + 999_999) / 1_000_000) in
  (* [fraction_ms] is at most 1000, so any [seconds] below this bound keeps
     [seconds * 1000 + fraction_ms] within [Int64.max_int]. *)
  let maximum_seconds = Int64.pred (Int64.div Int64.max_int 1_000L) in
  let milliseconds =
    if Int64.compare seconds maximum_seconds > 0 then Int64.max_int
    else Int64.add (Int64.mul seconds 1_000L) fraction_ms
  in
  Temporal_base.Duration.of_ms (Int64.min milliseconds maximum_ms)

(** Projects an optional Core timeout into the public representation. Temporal
    uses a zero duration to mean "not set": Core fills an absent local
    schedule-to-close with zero, the server reports an unset heartbeat timeout
    as zero, and an unset remote schedule-to-close inherits a zero workflow run
    timeout. Exposing those as [Some 0ms] would make deadline arithmetic
    conclude the attempt already expired and heartbeat pacing at half the
    interval spin, so zero becomes [None] as in the Go and Python SDKs and in
    [Temporal.Testing] (#1032). Any positive value goes through
    [info_duration]. *)
let configured_timeout = function
  | None -> None
  | Some { Protocol.seconds = 0L; nanoseconds = 0 } -> None
  | Some duration -> Some (info_duration duration)

(** Converts all synchronous callback inputs before dispatch, including the
    heartbeat interval behind [Temporal.Activity.Context.heartbeat_timeout],
    which uses the same total [configured_timeout] projection as
    [Activity.Info]: an unset (zero) interval is [None] and a sub-millisecond
    one rounds up, so only the payloads can reject the task. Asynchronous
    definitions use [decode_payloads] instead because they have no synchronous
    heartbeat context. Error routing is the same as for [decode_payloads]. *)
let decode_start definition (start : Protocol.activity_start) =
  let* input, details = decode_payloads definition start in
  Ok (input, details, configured_timeout start.heartbeat_timeout)

(** Copies the identity and scheduling facts of a start task into the
    immutable record exposed through [Temporal.Activity.Context.info]. The
    strict protocol decoder has already bounded every string and validated
    timestamp fractions, and [configured_timeout] is total, so this projection
    cannot fail. Core's attempt is a uint32, which always fits a native OCaml
    [int] on supported 64-bit targets. *)
let task_info (start : Protocol.activity_start) : Activity_context.info =
  let timestamp =
    Option.map (fun (value : Protocol.timestamp) ->
        { Activity_context.seconds = value.seconds;
          nanoseconds = value.nanoseconds })
  in
  {
    namespace = start.workflow_namespace;
    workflow_id = start.workflow_execution.workflow_id;
    workflow_run_id = start.workflow_execution.run_id;
    workflow_type = start.workflow_type;
    activity_id = start.activity_id;
    activity_type = start.activity_type;
    attempt = Int64.to_int start.attempt;
    is_local = start.is_local;
    scheduled_time = timestamp start.scheduled_time;
    current_attempt_scheduled_time =
      timestamp start.current_attempt_scheduled_time;
    started_time = timestamp start.started_time;
    schedule_to_close_timeout =
      configured_timeout start.schedule_to_close_timeout;
    start_to_close_timeout = configured_timeout start.start_to_close_timeout;
    task_heartbeat_timeout = configured_timeout start.heartbeat_timeout;
  }

(** Projects the log-correlation identity of a start task. The strict decoder
    has bounded every string; [Observability.tags] bounds them again for the
    reporter. Core's attempt is a uint32, which fits a native [int] on the
    supported 64-bit targets. *)
let correlation_of_start (start : Protocol.activity_start) =
  {
    workflow_id = start.workflow_execution.workflow_id;
    run_id = start.workflow_execution.run_id;
    workflow_type = start.workflow_type;
    activity_id = start.activity_id;
    activity_type = start.activity_type;
    attempt = Int64.to_int start.attempt;
  }

(** The activity type exposed on a public outcome, when the lease knows it. *)
let lease_activity_type (lease : lease) =
  Option.map (fun (correlation : correlation) -> correlation.activity_type)
    lease.correlation

(** Finds an executable definition by the Temporal activity type. The
    rejection names the types this worker does register, as the Go and Python
    SDKs do, so an operator can spot a worker running an older build; the
    message is bounded later by [make_error]. *)
let find_definition definitions activity_type =
  match Name_map.find_opt activity_type definitions with
  | Some definition -> Ok definition
  | None ->
      let registered = List.map fst (Name_map.bindings definitions) in
      Error
        (make_error ~path:"$.variant.activity_type" "unknown_activity_type"
           (Printf.sprintf
              "activity type %s is not registered on this worker; registered \
               activity types: [%s]"
              activity_type
              (String.concat ", " registered)))

(** Adds one definition to the immutable name map and rejects remote-only
    references before the worker can claim a task for them. *)
let add_definition definitions registration =
  let name =
    match registration with
    | Activity definition -> Definition.name definition
    | Async_activity definition -> Definition.name definition
  in
  if Name_map.mem name definitions then
    Error
      (make_error ~path:"$.activities" "duplicate_activity"
         ("activity type is registered more than once: " ^ name))
  else
    match registration with
    | Activity definition ->
        if Option.is_none (Definition.implementation definition) then
          Error
            (make_error ~path:("$.activities." ^ name) "not_executable"
               "activity registration has no local implementation")
        else
          Ok
            (Name_map.add name (Registered_definition definition) definitions)
    | Async_activity definition ->
        if Option.is_none (Definition.implementation definition) then
          Error
            (make_error ~path:("$.activities." ^ name) "not_executable"
               "asynchronous activity registration has no local implementation")
        else
          Ok
            (Name_map.add name
               (Registered_async_definition definition) definitions)

(** Builds the complete definition map before publishing mutable lease state. *)
let build_definitions activities =
  List.fold_left
    (fun result activity ->
      let* definitions = result in
      add_definition definitions activity)
    (Ok Name_map.empty) activities

(** Reports lifecycle events without allowing an observability backend defect to
    affect lease ownership or activity execution. With [correlation], the
    record carries the attempt's workflow ID, run ID, workflow type, activity
    ID, activity type, and attempt (#500), so a reporter can join SDK lease
    records with application logs for the same attempt. *)
let report level ~operation ?error_kind ?(correlation : correlation option) ()
    =
  try
    let tags =
      match correlation with
      | None -> Observability.tags ~operation ?error_kind ()
      | Some correlation ->
          Observability.tags ~operation ?error_kind
            ~workflow_id:correlation.workflow_id ~run_id:correlation.run_id
            ~workflow_type:correlation.workflow_type
            ~activity_id:correlation.activity_id
            ~activity_type:correlation.activity_type
            ~attempt:correlation.attempt ()
    in
    Observability.report ~src:Observability.Source.lifecycle level ~tags
      "native activity worker adapter event"
  with _ -> ()

(** One of the two ways a native completion call may finish. Keeping raised
    exceptions distinct lets [poll] retain the pending lease in both cases. *)
type completion_attempt =
  | Accepted
  | Rejected_by_supervisor of error_view
  | Raised_by_supervisor of exn

(** Validates and serializes the typed completion in its one strict semantic
    encoder pass. The returned value is the only form a completion can take on
    its way to the supervisor, which copies its bytes into the C call without
    encoding them again (issue #995); an encoder rejection therefore keeps the
    completion away from the native bridge. The pass is timed as
    [activity_completion_encode] only while the bridge source is at [Debug]
    (#528). *)
let encode_completion completion =
  Result.map_error
    (fun error -> protocol_error ~path:"$.completion" error)
    (Observability.debug_timed ~src:Observability.Source.bridge
       ~operation:"activity_completion_encode" (fun () ->
         Encoded_completion.encode completion))

(** Validates and serializes one synchronous context's heartbeat in its
    single strict encoder pass. The supervisor submits the returned bytes
    unchanged (issue #1001), so an encoder rejection keeps the heartbeat away
    from the native bridge. *)
let encode_heartbeat ~path heartbeat =
  Result.map_error
    (fun error -> protocol_error ~path error)
    (Encoded_heartbeat.encode heartbeat)

(** The same single encoder pass for an asynchronous handle's heartbeat,
    whose document also carries the attempt's heartbeat timeout (#1025). *)
let encode_async_heartbeat ~path heartbeat =
  Result.map_error
    (fun error -> protocol_error ~path error)
    (Encoded_async_heartbeat.encode heartbeat)

(** Converts the activity state an async heartbeat reported (#1025) into the
    handle's outcome, as the official SDKs raise a typed error from an async
    heartbeat in these states. [Heartbeat_recorded] succeeds. The other
    states are an [Activity_state] error in the [`Cancelled] category, shaped
    like the errors a running activity sees: a cancellation request is the
    non-retryable error of [Activity_context.cancelled_error], and a pause
    or reset is a retryable error of type [ActivityPause] or [ActivityReset],
    the failure the worker reports for a running attempt stopped that way
    (so passing it to [Async_handle.fail] reports the same failure). The
    heartbeat itself was recorded and the handle stays live: the activity
    must still be finished with a terminal operation. *)
let heartbeat_response_result = function
  | Protocol.Heartbeat_recorded -> Ok ()
  | Protocol.Heartbeat_cancel_requested ->
      Error
        (Async_activity.Activity_state
           (Base_error.make ~non_retryable:true ~category:`Cancelled
              ~message:"activity cancellation requested: cancelled" ()))
  | Protocol.Heartbeat_paused ->
      Error
        (Async_activity.Activity_state
           (Base_error.make ~error_type:"ActivityPause" ~category:`Cancelled
              ~message:"activity cancellation requested: paused" ()))
  | Protocol.Heartbeat_reset ->
      Error
        (Async_activity.Activity_state
           (Base_error.make ~error_type:"ActivityReset" ~category:`Cancelled
              ~message:"activity cancellation requested: reset" ()))

(** Converts a supervisor completion exception to a stable diagnostic without
    revealing the opaque token or native exception object. *)
let completion_exception_error ?(retryable = false) exception_ =
  make_error ~path:"$.completion" ~retryable "completion_failed"
    (Printf.sprintf "supervisor completion raised: %s"
       (exception_error exception_).message)

(** The synchronous attempt whose callback is currently executing. It exists
    so that a Core cancellation task taken while the callback runs can be
    routed to that attempt's context instead of becoming a second completion:
    Core's cancellation is an update to the original start token, and the
    start's one completion remains owned by [process_start]. [observed] keeps
    the exact Core task so the completion can report its reason and details.
    [running_correlation] lets the cancellation-delivery record name the
    attempt it reached. *)
type running_attempt = {
  running_token : bytes;
  running_correlation : correlation;
  signal : Activity_context.cancellation_signal;
  mutable observed : Protocol.activity_cancel option;
}

(** Work taken from the supervisor by a heartbeat delivery sweep that does not
    belong to the running attempt. Each item is handled later, in order, by
    [poll] (or [drain] at shutdown), exactly as if it had been polled then. A
    poll error is kept rather than dropped because the supervisor has already
    reported it once and the lane must still observe it. *)
type deferred_work =
  | Deferred_task of Protocol.task
  | Deferred_poll_error of error_view

(** Bounds one delivery sweep. Core's activity slots bound the tasks that can
    be ready, so the cap is only a guard against a misbehaving source. *)
let max_delivery_sweep = 64

(** Builds the cancellation reported for a deferred local-activity start at
    shutdown (#1209), as the bridge reports every local token it retires
    ([bridge_retirement_result], #1029). Core's local-activity manager retries
    a retryable local failure in-process after a backoff of 1 s and more, so
    the retryable [undispatched_failure] would keep shutdown waiting for that
    retry; a cancellation is not retried. The workflow task owning the local
    activity is failed by the same drain, so the next worker re-executes it. *)
let undispatched_local_cancellation : Protocol.failure =
  Protocol.
    {
      message = "local activity was not dispatched before the worker shut down";
      source = "ocaml-temporal";
      stack_trace = "";
      encoded_attributes = None;
      cause = None;
      info = Canceled { details = []; identity = "ocaml-temporal" };
    }

(** Builds the retryable failure for a start task that a delivery sweep took
    but the worker shut down before dispatching. The callback never ran, so
    Temporal may retry the attempt; this mirrors the bridge's own failure of
    tasks it could not deliver at shutdown. *)
let undispatched_failure : Protocol.failure =
  Protocol.
    {
      message = "activity task was not dispatched before the worker shut down";
      source = "ocaml-temporal";
      stack_trace = "";
      encoded_attributes = None;
      cause = None;
      info =
        Application
          {
            type_name = "ocaml_temporal_worker_shutdown";
            non_retryable = false;
            details = [];
            category = Application_category_unspecified;
            next_retry_delay = None;
          };
    }

(** Returns the failure that must complete an attempt whose Core
    cancellation came from an operator pause or reset, or [None] when the
    attempt is completed as cancelled.

    The server accepts [RespondActivityTaskCanceled] only after a
    cancellation request; it rejects one for a paused or reset attempt, and
    the attempt then stays open until it times out (#1022). Pinned Core
    reports [Paused] or [Reset] as the primary reason only when no
    cancellation was requested (it ranks a request above a reset, and a reset
    above a pause), so the primary reason alone decides. Like the Python SDK,
    such an attempt is failed retryably with type ["ActivityPause"] or
    ["ActivityReset"]: the server holds a paused activity's next attempt until
    it is unpaused and schedules a reset activity afresh. The other reasons
    keep the cancelled completion: Core suppresses it for [Not_found] and
    [Timed_out], and reports it as a failure itself for [Worker_shutdown].
    [details] are the acknowledging error's validated detail payloads. *)
let interruption_failure ?(details = []) (cancel : Protocol.activity_cancel) :
    Protocol.failure option =
  let failure type_name message =
    Some
      Protocol.
        {
          message;
          source = "ocaml-temporal";
          stack_trace = "";
          encoded_attributes = None;
          cause = None;
          info =
            Application
              {
                type_name;
                non_retryable = false;
                details;
                category = Application_category_unspecified;
                next_retry_delay = None;
              };
        }
  in
  match cancel.reason with
  | Protocol.Cancellation_paused ->
      failure "ActivityPause" "activity attempt stopped because it was paused"
  | Cancellation_reset ->
      failure "ActivityReset" "activity attempt stopped because it was reset"
  | Cancellation_requested | Cancellation_not_found | Cancellation_timed_out
  | Cancellation_worker_shutdown ->
      None

module Make (Supervisor : SUPERVISOR) = struct
  (** State owned by one activity adapter. Definitions never change after
      construction; [leases] contains only copied completions whose opaque
      task-token acknowledgements are still uncertain. Every mutable field
      except [async_leases], [running], [deferred] and [callback_active] is
      accessed while [mutex] is held; [async_leases] is guarded by
      [async_mutex], [running] and [deferred] by [delivery_mutex] (see
      below), and [callback_active] is atomic. Supervisor calls are made
      under [mutex], except the heartbeat and delivery-sweep calls that a
      running callback makes, which rely on the supervisor mailbox being
      safe to call from any Domain. *)
  type adapter_state = {
    (* The owner-confined native supervisor handle. It is borrowed for each
       serialized operation and never retained by a user activity. *)
    supervisor : Supervisor.t;
    (* Immutable existential definitions keyed by Temporal activity type. *)
    definitions : registered_definition Name_map.t;
    (* Owned completion leases keyed by copied binary task token. Entries remain
       until the exact completion is acknowledged successfully. *)
    mutable leases : lease Token_map.t;
    (* Accepted deferred handles are not worker leases. They remain here until
       a namespace-bound client operation is accepted or terminal cleanup has
       proved that the native runtime is force-retired. *)
    mutable async_leases : async_lease Token_map.t;
    (* Serializes registry updates, completion retries, and source operations. *)
    mutex : Mutex.t;
    (* Separate lock for handle callbacks. It is never held while the adapter
       mutex is acquired, preventing a handle callback from deadlocking the
       serialized poll path. *)
    async_mutex : Mutex.t;
    (* Guards [running] and [deferred]. A delivery sweep runs inside a
       heartbeat, i.e. inside user code that the executor calls while holding
       [mutex], and user code may heartbeat from another Domain; the sweep
       therefore cannot take [mutex]. Lock order: an activity context's own
       mutex, then [delivery_mutex]; [mutex] may be held around either. While
       holding [delivery_mutex] the adapter takes no other adapter or context
       lock; the sweep's supervisor calls take only the mailbox's own
       internal locks. *)
    delivery_mutex : Mutex.t;
    (* The synchronous attempt now running, set by [process_start] before the
       callback and cleared after its context is invalidated, so no sweep can
       route a cancellation to an attempt that has already finished. *)
    mutable running : running_attempt option;
    (* FIFO of swept work that [poll] handles before polling the supervisor. *)
    deferred : deferred_work Queue.t;
    (* Whether an application callback, synchronous or asynchronous, is
       executing now. Written only by the activity lane around the callback
       and read by bounded shutdown from any thread, so it is an atomic
       rather than state behind a lock that a sweep may hold (#1023). *)
    callback_active : bool Atomic.t;
    (* Non-blocking, Domain-safe probe of the owning worker's stop flag,
       exposed to activity contexts. *)
    worker_shutting_down : unit -> bool;
    (* Monotonic seconds used only to expire async leases (#1043); tests
       inject a controllable clock. *)
    now : unit -> float;
    (* SDK-side metrics recorder (#500), [Metrics.disabled] until
       [attach_metrics]. Atomic so a lane on another Domain reads a fully
       published value; it is written once, before polling starts. *)
    metrics : Metrics.t Atomic.t;
  }

  (** The public worker handle is the mutex-confined state above. *)
  type t = adapter_state

  (** A malformed test double or future supervisor must not be able to turn a
      diagnostic-classifier exception into an unbounded retry loop. Treat a
      classifier defect as permanent; only an explicit [true] classification
      can authorize retained-completion retry. *)
  let source_error_is_retryable source_error =
    try Supervisor.error_is_retryable source_error with _ -> false

  (** Async-operation classification distinguishes an uncertain RPC from a
      rejected request that leaves its activity live, for heartbeats and
      terminal operations alike. A faulty source classifier still fails closed
      instead of hiding a defect. *)
  let async_operation_error_disposition source_error =
    try Supervisor.async_operation_error_disposition source_error
    with _ -> Worker_policy.Retired

  (** Exception classification is equally conservative: arbitrary exceptions
      are owner-domain defects unless the supervisor explicitly marks one as a
      transient completion transport failure. *)
  let completion_exception_is_retryable exception_ =
    try Supervisor.exception_is_retryable exception_ with _ -> false

  (** Runs [f] with [delivery_mutex] held. [f] must not take an adapter or
      context lock (see the lock order on [adapter_state]). *)
  let with_delivery adapter f =
    Mutex.lock adapter.delivery_mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock adapter.delivery_mutex) f

  (** Routes one task taken by a delivery sweep; [delivery_mutex] is held. A
      cancellation for the running attempt's token is published to its
      context exactly once; everything else is deferred unchanged. *)
  let route_swept_task adapter (task : Protocol.task) =
    match (adapter.running, task.variant) with
    | Some running, Cancel cancel
      when Bytes.equal running.running_token task.task_token -> (
        match running.observed with
        | Some _ ->
            (* Core sends one cancellation per attempt; a repeated update
               carries no new obligation and the first reason stays stable. *)
            ()
        | None ->
            running.observed <- Some cancel;
            ignore
              (Activity_context.signal_cancellation running.signal
                 (context_cancellation cancel));
            report Logs.Debug ~operation:"activity_cancellation_delivered"
              ~correlation:running.running_correlation ())
    | _ -> Queue.push (Deferred_task task) adapter.deferred

  (** Delivers pending Core activity tasks while a callback is running.

      The executor holds [mutex] for the whole callback, so [poll] cannot take
      a cancellation for the running attempt until the callback returns. A
      heartbeat is the point where the callback hands control back to the
      SDK, and it is also how a server-side cancellation reaches Core (in the
      heartbeat response), so the heartbeat callback ends with this
      non-blocking sweep. It takes every ready task through the same
      supervisor mailbox as [poll], routes the running attempt's cancellation
      to its context, and defers anything else for [poll]. It never runs user
      code and never submits a completion. *)
  let deliver_pending adapter =
    with_delivery adapter (fun () ->
        let rec loop remaining =
          if remaining > 0 then
            match
              try Ok (Supervisor.try_poll_activity adapter.supervisor)
              with exception_ ->
                Error (exception_error ~path:"$.poll" exception_)
            with
            | Ok (Ok None) -> ()
            | Ok (Ok (Some task)) ->
                route_swept_task adapter task;
                loop (remaining - 1)
            | Ok (Error source_error) ->
                Queue.push
                  (Deferred_poll_error
                     (supervisor_error ~path:"$.poll"
                        ~retryable:(source_error_is_retryable source_error)
                        ~error_code:Supervisor.error_code
                        ~error_message:Supervisor.error_message source_error))
                  adapter.deferred
            | Error error -> Queue.push (Deferred_poll_error error) adapter.deferred
        in
        loop max_delivery_sweep)

  (** Builds the context passed to one activity attempt. Heartbeats go back
      through the same typed supervisor mailbox as polling and completion; the
      callback never captures a raw Rust pointer and the token is copied for
      each request. Contexts are invalidated by [process_start] before it
      returns, so retaining one in user code cannot submit progress for a later
      attempt. [signal] is the attempt's cancellation cell, written by
      [deliver_pending]. *)
  let activity_context adapter ~token ~signal ~info ~details ~heartbeat_timeout =
    (* The callback remains valid only for this lease. It copies the token and
       every detail before crossing to the supervisor, so a caller cannot
       mutate a heartbeat after submission. *)
    let record payloads =
      (* Convert public payloads with indexed paths while preserving their
         order; conversion errors never reach the native callback. *)
      let rec convert index reversed = function
        | [] -> Ok (List.rev reversed)
        | payload :: rest ->
            let path = Printf.sprintf "$.heartbeat.details[%d]" index in
            (match protocol_payload path payload with
            | Error error ->
                Error
                  (Base_error.make ~category:`Codec
                     ~message:(
                       Printf.sprintf
                         "activity heartbeat payload rejected at %s: %s"
                         error.path error.message)
                     ())
            | Ok payload -> convert (index + 1) (payload :: reversed) rest)
      in
      match convert 0 [] payloads with
      | Error _ as error -> error
      | Ok details -> (
          let heartbeat = Protocol.{ task_token = Bytes.copy token; details } in
          (* The heartbeat's only encoder pass; a rejection is a codec error
             in user-supplied details and never reaches the supervisor. *)
          match encode_heartbeat ~path:"$.heartbeat" heartbeat with
          | Error error ->
              Error
                (Base_error.make ~category:`Codec
                   ~message:(
                     Printf.sprintf "activity heartbeat rejected at %s: %s"
                       error.path error.message)
                   ())
          | Ok encoded -> (
          try
            match
              Supervisor.record_activity_heartbeat adapter.supervisor encoded
            with
            | Ok () -> Ok ()
            | Error source_error ->
                let source =
                  supervisor_error ~path:"$.heartbeat"
                    ~retryable:(source_error_is_retryable source_error)
                    ~error_code:Supervisor.error_code
                    ~error_message:Supervisor.error_message source_error
                in
                Error
                  (Base_error.make ~category:`Bridge
                     ~message:(
                       Printf.sprintf "activity heartbeat failed (%s): %s"
                         source.code source.message)
                     ())
          with exception_ ->
            let source = exception_error ~path:"$.heartbeat" exception_ in
            Error
              (Base_error.make ~category:`Bridge
                 ~message:(
                   Printf.sprintf "activity heartbeat failed (%s): %s"
                     source.code source.message)
                 ())))
    in
    (* Record first, then deliver: Core can only learn of a server-side
       cancellation from a heartbeat response, and the sweep runs even when
       the record failed so a cancellation already queued is not missed. *)
    let heartbeat payloads =
      let recorded = record payloads in
      deliver_pending adapter;
      recorded
    in
    Activity_context.create_for_task ~cancellation:signal
      ~worker_shutting_down:adapter.worker_shutting_down ~info ~heartbeat
      ~details ~heartbeat_timeout

  (** Creates the registry without contacting native Core or invoking user
      code. [now] is the clock that expires async leases. *)
  let create_with_clock ~now ~supervisor ~activities ~worker_shutting_down =
    match build_definitions activities with
    | Error error -> Error error
    | Ok definitions ->
        Ok
          {
            supervisor;
            definitions;
            leases = Token_map.empty;
            async_leases = Token_map.empty;
            mutex = Mutex.create ();
            async_mutex = Mutex.create ();
            delivery_mutex = Mutex.create ();
            running = None;
            deferred = Queue.create ();
            callback_active = Atomic.make false;
            worker_shutting_down;
            now;
            metrics = Atomic.make Metrics.disabled;
          }

  (** Creates the registry with the process's monotonic clock, in seconds.
      Lease expiry only compares elapsed times, so wall-clock adjustments
      must not affect it. *)
  let create ~supervisor ~activities ~worker_shutting_down =
    let now () =
      Int64.to_float (Temporal_core_bridge.Native_bridge.monotonic_now_ns ())
      /. 1e9
    in
    create_with_clock ~now ~supervisor ~activities ~worker_shutting_down

  (** Converts an adapter diagnostic into the base error type expected by an
      asynchronous handle callback. The callback is a private boundary, so a
      bounded message is sufficient and no task token is ever included. *)
  let base_operation_error operation (error : error_view) =
    Base_error.make ~category:`Bridge ~non_retryable:(not error.retryable)
      ~message:(
        Printf.sprintf "%s failed (%s): %s" operation error.code error.message)
      ()

  (** Owned, fully validated requests ready to cross the supervisor boundary.
      Each is carried as its single encoder pass's canonical bytes, which the
      supervisor submits unchanged (issues #995 and #1001). Preparing these
      values never changes either lease ledger. *)
  type async_request =
    | Async_completion of Encoded_completion.t
    | Async_heartbeat of Encoded_async_heartbeat.t

  (** Copies and validates an entire late operation before any native call.
      Payload conversion alone cannot detect duplicate metadata or wire limits;
      the strict encoder must also accept the complete request. *)
  let prepare_async_request ~token ~heartbeat_timeout operation =
    let task_token = Bytes.copy token in
    (* Converts ordered detail payloads without retaining caller-owned bytes. *)
    let details path payloads =
      let rec loop reversed = function
        | [] -> Ok (List.rev reversed)
        | payload :: rest ->
            let* payload = protocol_payload path payload in
            loop (payload :: reversed) rest
      in
      loop [] payloads
    in
    (* Build the typed request first; it is then encoded exactly once below
       and only its bytes enter [async_request]. *)
    let* request =
      match operation with
      | Async_activity.Complete payload ->
          let* payload = protocol_payload "$.async_completion.result" payload in
          Ok (`Completion Protocol.{ task_token; result = Completed payload })
      | Async_activity.Fail failure ->
          let diagnostic = application_error ~path:"$.async_failure" failure in
          let* failure = failure_of_application_error diagnostic failure in
          Ok (`Completion Protocol.{ task_token; result = Failed failure })
      | Async_activity.Cancel payloads ->
          let* details = details "$.async_cancellation.details" payloads in
          let failure = cancellation_failure ~details Protocol.Cancellation_requested in
          Ok (`Completion Protocol.{ task_token; result = Cancelled failure })
      | Async_activity.Heartbeat payloads ->
          let* details = details "$.async_heartbeat.details" payloads in
          Ok (`Heartbeat Protocol.{ task_token; details })
    in
    match request with
    | `Completion completion ->
        (* The completion's only encoder pass; its bytes are what the
           supervisor submits. *)
        let* encoded = encode_completion completion in
        Ok (Async_completion encoded)
    | `Heartbeat heartbeat ->
        (* Likewise the heartbeat's only encoder pass. The attempt's
           heartbeat timeout travels with it for the bridge's throttle. *)
        let* encoded =
          encode_async_heartbeat ~path:"$.async_heartbeat"
            { Protocol.heartbeat; heartbeat_timeout }
        in
        Ok (Async_heartbeat encoded)

  (** Maps an adapter-local failure that never received a native answer (a
      missing lease or a supervisor exception) to a lifecycle decision. Such
      failures are defects or teardown races, so they retire the handle unless
      explicitly retryable. Native typed errors use
      [async_native_submission_error] instead. *)
  let async_submission_error operation (error : error_view) =
    let diagnostic = base_operation_error operation error in
    if error.retryable then Async_activity.Retryable_submission diagnostic
    else Async_activity.Terminal_submission diagnostic

  (** Maps a typed native failure of any namespace-bound async request to the
      base handle's submission outcome. The same disposition applies to
      heartbeats and complete/fail/cancel because none of them consumes a Core
      worker lease: an uncertain RPC keeps the lease ([Retryable_submission]),
      a definitive rejection keeps the live handle ([Rejected_submission]), and
      only token loss or an unusable graph retires it. The base handle then
      decides whether an uncertain request must be retried exactly.

      A call the bridge refused to admit because its client-call registry was
      full ([Not_admitted], #1104) never reached the server, so it maps to
      [Not_submitted] with a retryable diagnostic: the handle and lease stay
      live, an earlier uncertain terminal submission is still owed, and the
      caller may retry the same operation once capacity frees. *)
  let async_native_submission_error adapter ~operation ~name ~path
      source_error =
    let disposition = async_operation_error_disposition source_error in
    (match disposition with
    | Worker_policy.Not_admitted ->
        Metrics.async_activity_not_admitted (Atomic.get adapter.metrics)
          operation
    | Worker_policy.Retry_exact | Worker_policy.Rejected_live
    | Worker_policy.Retired ->
        ());
    let retryable =
      match disposition with
      | Worker_policy.Retry_exact | Worker_policy.Not_admitted -> true
      | Worker_policy.Rejected_live | Worker_policy.Retired -> false
    in
    let diagnostic =
      base_operation_error name
        (supervisor_error ~path ~retryable ~error_code:Supervisor.error_code
           ~error_message:Supervisor.error_message source_error)
    in
    match disposition with
    | Worker_policy.Retry_exact -> Async_activity.Retryable_submission diagnostic
    | Worker_policy.Rejected_live -> Async_activity.Rejected_submission diagnostic
    | Worker_policy.Not_admitted -> Async_activity.Not_submitted diagnostic
    | Worker_policy.Retired -> Async_activity.Terminal_submission diagnostic

  (** Closes and drops every admitted async lease whose attempt the server
      must already have timed out (#1043); [async_mutex] must be held. A
      lease with an operation in flight is kept until that operation's
      outcome is recorded: the RPC runs without [async_mutex] (#1025), and a
      heartbeat that reaches the server defers the expiry. A later call on a
      closed handle returns a non-retryable error, which is what the server
      would answer with [NotFound]. Each expiry is reported once at
      [Warning] with the attempt's identity, since it means application code
      never finished the handle. *)
  let retire_expired_async_leases_locked adapter =
    let now = adapter.now () in
    let expired, live =
      Token_map.partition
        (fun _ (Async_lease lease_fields as lease) ->
          (not lease_fields.in_flight) && async_lease_expired ~now lease)
        adapter.async_leases
    in
    adapter.async_leases <- live;
    Token_map.iter
      (fun _ (Async_lease lease) ->
        ignore (Async_activity.close lease.handle);
        Metrics.async_activity_lease_expired (Atomic.get adapter.metrics);
        report Logs.Warning ~operation:"activity_async_lease_expired"
          ?correlation:lease.correlation ())
      expired

  (** Calls the namespace-bound client only after local validation succeeds.

      The operation is three steps. Under [async_mutex] the lease is looked
      up, the request prepared, and the lease marked [in_flight], which
      refuses a concurrent operation on the same handle. The native call
      then runs {e without} any adapter lock (#1025): it is a submitted
      client call that waits for the server on this caller's thread, so a
      slow server delays only this handle, never another handle's
      operation, the admission of a new async lease by the activity lane, or
      a shutdown probe. Finally, under [async_mutex] again, the outcome is
      recorded and [in_flight] cleared. Only accepted terminal operations or
      terminal native errors retire the lease.

      Terminal [discard] may run while the call is in flight. It closes the
      handle and empties the registry, so the last step then finds no lease
      to update; the caller still receives the call's own outcome. That is
      safe because [discard] runs only after the native release, when no
      operation on the handle can be submitted again. *)
  let submit_async_operation adapter ~token operation
      : (unit, Async_activity.submission_error) result =
    (* Step 1: claim the lease and prepare the request. *)
    let claimed =
      Mutex.lock adapter.async_mutex;
      Fun.protect
        ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
        (fun () ->
          match Token_map.find_opt token adapter.async_leases with
          | None ->
              Error (async_submission_error "async activity handle" (make_error
                ~path:"$.async_handle" "closed" "asynchronous activity handle is no longer active"))
          | Some (Async_lease lease) when lease.in_flight ->
              Error (Async_activity.Not_submitted
                (base_operation_error "async activity handle" (make_error
                  ~path:"$.async_handle" "busy" "asynchronous activity operation is already in flight")))
          | Some (Async_lease lease as entry) -> (
              (* The handle's own request identity; computed only while an
                 uncertain completion is retained. *)
              let key = lazy (Async_activity.operation_key operation) in
              let prepared =
                match lease.uncertain_completion with
                | Some (retained_key, encoded)
                  when String.equal retained_key (Lazy.force key) ->
                    (* A retry of an uncertain terminal submission that the
                       handle admitted as the same request: resend the bytes
                       already validated and encoded for it. *)
                    Ok (Async_completion encoded)
                | Some _ | None -> (
                    try
                      prepare_async_request ~token
                        ~heartbeat_timeout:lease.heartbeat_timeout operation
                    with exception_ ->
                      Error (exception_error ~path:"$.async_payload" exception_))
              in
              match prepared with
              | Error error ->
                  Error (Async_activity.Not_submitted
                    (base_operation_error "async activity payload" error))
              | Ok request ->
                  lease.in_flight <- true;
                  Ok (entry, request, key)))
    in
    match claimed with
    | Error _ as error -> error
    | Ok ((Async_lease lease as entry), request, key) ->
        let terminal, name, path =
          match request with
          | Async_completion _ ->
              (true, "async activity completion", "$.async_completion")
          | Async_heartbeat _ ->
              (false, "async activity heartbeat", "$.async_heartbeat")
        in
        (* Step 2: the native call, with no adapter lock held. Every
           exception, including one from the supervisor, is an uncertain
           outcome classified below, so step 3 always runs. *)
        let result =
          try
            let submitted =
              match request with
              | Async_completion encoded ->
                  Result.map Result.ok
                    (Supervisor.complete_async_activity adapter.supervisor encoded)
              | Async_heartbeat heartbeat ->
                  Result.map heartbeat_response_result
                    (Supervisor.record_async_activity_heartbeat
                       adapter.supervisor heartbeat)
            in
            match submitted with
            | Ok (Ok ()) -> Ok ()
            | Ok (Error state_error) -> Error state_error
            | Error source_error ->
                Error
                  (async_native_submission_error adapter
                     ~operation:
                       (if terminal then Metrics.Completion
                        else Metrics.Heartbeat)
                     ~name ~path source_error)
          with exception_ ->
            Error (async_submission_error name (exception_error ~path exception_))
        in
        (* Step 3: record the outcome on the lease, if it is still the one
           registered for [token] (terminal [discard] may have run). *)
        Mutex.lock adapter.async_mutex;
        Fun.protect
          ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
          (fun () ->
            lease.in_flight <- false;
            let registered =
              match Token_map.find_opt token adapter.async_leases with
              | Some current -> current == entry
              | None -> false
            in
            if registered then begin
              (* A heartbeat that may have reached the server restarts its
                 heartbeat-timeout window there, so it also defers this
                 lease's expiry (#1043). Treating an uncertain outcome as
                 progress errs towards keeping the lease. *)
              (match (request, result) with
              | Async_heartbeat _,
                ( Ok ()
                | Error
                    ( Async_activity.Retryable_submission _
                    | Async_activity.Activity_state _ ) ) ->
                  lease.last_progress <- adapter.now ()
              | Async_heartbeat _, Error _ | Async_completion _, _ -> ());
              (match result with
              | Ok () when terminal ->
                  adapter.async_leases <- Token_map.remove token adapter.async_leases
              | Error (Async_activity.Terminal_submission _) ->
                  adapter.async_leases <- Token_map.remove token adapter.async_leases
              | Ok () | Error (Async_activity.Not_submitted _
                  | Async_activity.Rejected_submission _
                  | Async_activity.Retryable_submission _
                  | Async_activity.Activity_state _) -> ());
              (* Only a terminal request with an unknown native outcome
                 keeps its bytes for the exact retry the handle allows; a
                 heartbeat never touches the retained completion. A call
                 the bridge did not admit ([Not_submitted] is only produced
                 here for a full client-call registry, #1104) leaves any
                 retained bytes in place: the base handle still owes that
                 earlier uncertain request, and its retry must resend them. *)
              match (request, result) with
              | Async_completion encoded,
                Error (Async_activity.Retryable_submission _) ->
                  lease.uncertain_completion <- Some (Lazy.force key, encoded)
              | Async_completion _, Error (Async_activity.Not_submitted _) -> ()
              | Async_completion _, _ -> lease.uncertain_completion <- None
              | Async_heartbeat _, _ -> ()
            end;
            result)

  (** Submits an already encoded, immutable completion and preserves lease
      uncertainty when a typed native error or an exception is returned. Local
      validation (the single encoder pass) happens before admission to the
      pending-completion map. *)
  let attempt_completion supervisor encoded =
    try
      match Supervisor.complete_activity supervisor encoded with
      | Ok () -> Accepted
      | Error source_error ->
          let source =
            supervisor_error ~path:"$.completion"
              ~retryable:(source_error_is_retryable source_error)
              ~error_code:Supervisor.error_code
              ~error_message:Supervisor.error_message source_error
          in
          Rejected_by_supervisor
            (make_error ~path:"$.completion" ~retryable:source.retryable
               "completion_failed"
               (Printf.sprintf "supervisor rejected completion (%s): %s"
                  source.code source.message))
    with exception_ -> Raised_by_supervisor exception_

  (** Inserts a copied completion lease. A duplicate token is a native protocol
      violation; refusing to overwrite the existing lease preserves the original
      completion and avoids acknowledging the wrong obligation. *)
  let add_lease adapter lease =
    if Token_map.mem lease.token adapter.leases then
      Error
        (make_error ~path:"$.task_token" "duplicate_task_token"
           "native supervisor delivered an already leased activity token")
    else (
      adapter.leases <- Token_map.add lease.token lease adapter.leases;
      Ok ())

  (** Publishes an accepted async handle only after the corresponding worker
      completion has returned [Ok]. The handle was reserved before that
      completion, so no caller can submit through it while this publication is
      in progress. The native completion lease is removed only after both the
      registry insertion and lifecycle activation succeed; an unexpected
      activation failure therefore leaves the accepted completion visible to
      drain diagnostics and terminal [discard] (which closes its handle)
      instead of orphaning it; the lease is never resubmitted. Admission
      first retires expired leases, so a worker whose application code
      leaks handles does not accumulate them for its whole life (#1043). *)
  let admit_async_lease adapter ~token ~correlation ~timeouts handle =
    Mutex.lock adapter.async_mutex;
    let admission =
      Fun.protect
        ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
        (fun () ->
          retire_expired_async_leases_locked adapter;
          if Token_map.mem token adapter.async_leases then
            Error
              (make_error ~path:"$.async_handle" "duplicate_async_lease"
                 "native accepted an asynchronous activity token twice")
          else (
            let admitted_at = adapter.now () in
            adapter.async_leases <-
              Token_map.add token
                (Async_lease
                   {
                     token = Bytes.copy token;
                     handle;
                     correlation;
                     attempt_deadline =
                       async_attempt_deadline ~admitted_at timeouts;
                     heartbeat_s = timeouts.heartbeat_s;
                     heartbeat_timeout = timeouts.heartbeat_timeout;
                     last_progress = admitted_at;
                     in_flight = false;
                     uncertain_completion = None;
                   })
                adapter.async_leases;
            Ok ()))
    in
    match admission with
    | Error _ as error -> error
    | Ok () -> (
        match Async_activity.activate handle with
        | Ok () -> Ok ()
        | Error activation_error ->
            Mutex.lock adapter.async_mutex;
            Fun.protect
              ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
              (fun () ->
                adapter.async_leases <-
                  Token_map.remove token adapter.async_leases);
            Error
              (application_error ~path:"$.async_handle.activate"
                 activation_error))

  (** Submits one pending lease exactly once and removes it only after native
      Core accepts the exact copied token. Every failure leaves it in the map;
      a failure that is not explicitly retryable also sets [retry_refusal].
      Callers must use [finish_lease], which honours that refusal. *)
  let submit_lease adapter lease : (outcome, error_view) result =
    (* Records a fail-closed refusal before reporting the failure. *)
    let refuse_unless_retryable (error : error_view) =
      if not error.retryable then lease.retry_refusal <- Some error;
      Error error
    in
    match attempt_completion adapter.supervisor lease.encoded with
    | Rejected_by_supervisor error -> refuse_unless_retryable error
    | Raised_by_supervisor exception_ ->
        let retryable = completion_exception_is_retryable exception_ in
        refuse_unless_retryable
          (completion_exception_error ~retryable exception_)
    | Accepted ->
        begin match lease.accepted_result with
        | Completed_result { kind; cancellation_details } ->
            adapter.leases <- Token_map.remove lease.token adapter.leases;
            report Logs.Debug ~operation:"activity_task_completed"
              ?correlation:lease.correlation ();
            Ok
              (Completed
                 {
                   activity_type = lease_activity_type lease;
                   kind;
                   cancellation_details;
                 })
        | Rejected_result { error; origin } ->
            adapter.leases <- Token_map.remove lease.token adapter.leases;
            (* This is the only record for an acknowledged failure: the worker
               loop deliberately does not log [Rejected] again (issue #1024).
               An application [Error] is expected, retried Temporal behaviour,
               so it is reported at [Info] under its own operation; [Warning]
               is reserved for SDK-side rejections an operator must fix. *)
            (match origin with
            | Application_failure ->
                report Logs.Info ~operation:"activity_task_failed"
                  ~error_kind:error.code ?correlation:lease.correlation ()
            | Sdk_rejection ->
                report Logs.Warning ~operation:"activity_task_rejected"
                  ~error_kind:error.code ?correlation:lease.correlation ());
            Ok
              (Rejected
                 {
                   activity_type = lease_activity_type lease;
                   error;
                   lease_retired = true;
                 })
        | Async_handoff { handle; timeouts } ->
            (match
               admit_async_lease adapter ~token:lease.token
                 ~correlation:lease.correlation ~timeouts handle
             with
            | Error error ->
                (* Core has already accepted [WillCompleteAsync], so the lease
                   is retained only as a diagnostic owner of the handle until
                   terminal [discard]; it must never be submitted again. *)
                refuse_unless_retryable { error with retryable = false }
            | Ok () ->
                adapter.leases <- Token_map.remove lease.token adapter.leases;
                report Logs.Debug ~operation:"activity_async_handoff_accepted"
                  ?correlation:lease.correlation ();
                Ok
                  (Completed
                     {
                       activity_type = lease_activity_type lease;
                       kind = Deferred;
                       cancellation_details = None;
                     }))
        end

  (** Submits a pending lease unless an earlier failure refused its retry.
      Once [retry_refusal] is set this returns the recorded error without
      calling the supervisor, so neither a later poll, a second worker run, nor
      a shutdown drain can submit the completion again (issue #843). *)
  let finish_lease adapter lease : (outcome, error_view) result =
    match lease.retry_refusal with
    | Some error -> Error error
    | None -> submit_lease adapter lease

  (** Validates and records one completion without submitting it. Invalid
      application data becomes a bounded failure before anything is retained
      for transport retry. The completion is encoded exactly once here; the
      lease retains only those immutable bytes, which every submission and
      retry sends unchanged (issue #995). [enqueue_and_finish] submits the
      recorded lease at once; [drain] records several and then submits them in
      token order. *)
  let admit_completion adapter ~token ~correlation ~completion
      ~accepted_result =
    let (completion : Protocol.completion) = completion in
    let token = Bytes.copy token in
    (* Encoding snapshots the token and payload bytes, so no defensive copy of
       the typed completion is needed before it is retained. *)
    let* encoded, accepted_result =
      match encode_completion completion with
      | Ok encoded -> Ok (encoded, accepted_result)
      | Error error ->
          (* No native submission has occurred. Do not retain an invalid value
             that every subsequent poll would reject, and do not admit an async
             handle when its handoff was replaced with a task failure. *)
          (match accepted_result with
          | Async_handoff { handle; _ } -> ignore (Async_activity.close handle)
          | Completed_result _ | Rejected_result _ -> ());
          let fallback =
            Protocol.{ task_token = Bytes.copy token;
              result = Failed (failure_of_error error) }
          in
          (* A malformed supervisor token cannot be repaired by dropping
             application details. Encode (and so validate) the fallback too,
             before admission. *)
          let* encoded = encode_completion fallback in
          Metrics.activity_task_refused (Atomic.get adapter.metrics)
            Metrics.Invalid;
          Ok (encoded, Rejected_result { error; origin = Sdk_rejection })
    in
    let lease =
      {
        token;
        correlation;
        encoded;
        accepted_result;
        retry_refusal = None;
      }
    in
    match add_lease adapter lease with
    | Error error ->
        (* A duplicate token means this completion was never admitted. Close a
           reserved async handle so a callback cannot retain a capability for
           a task that the adapter rejected before contacting Core. *)
        (match accepted_result with
        | Async_handoff { handle; _ } -> ignore (Async_activity.close handle)
        | Completed_result _ | Rejected_result _ -> ());
        Error error
    | Ok () -> Ok lease

  (** Validates, records, and submits one completion. Recording precedes the
      native call so uncertain submissions retain their exact completion; only
      a [Retryable] source classification may authorize resubmission, while
      generic transport failures remain fail-closed. *)
  let enqueue_and_finish adapter ~token ~correlation ~completion
      ~accepted_result =
    let* lease =
      admit_completion adapter ~token ~correlation ~completion
        ~accepted_result
    in
    finish_lease adapter lease

  (** Acknowledges the task with [failure] while preserving the original task
      token. [origin] defaults to [Sdk_rejection]; application [Error] results
      pass [Application_failure] so they are logged as ordinary failures. An
      SDK rejection is counted once here, under [refusal] (default
      [Invalid]), before submission, so a retried completion is not counted
      again. *)
  let reject_task_with_failure ?(origin = Sdk_rejection)
      ?(refusal = Metrics.Invalid) adapter ~token ~correlation ~failure error =
    (match origin with
    | Sdk_rejection ->
        Metrics.activity_task_refused (Atomic.get adapter.metrics) refusal
    | Application_failure -> ());
    let completion =
      Protocol.{ task_token = Bytes.copy token; result = Failed failure }
    in
    enqueue_and_finish adapter ~token ~correlation ~completion
      ~accepted_result:(Rejected_result { error; origin })

  (** Builds the standard non-retryable adapter failure for errors that
      another attempt would repeat on any worker: an output codec or
      completion-validation error, an unrepresentable heartbeat context, or a
      protocol defect. An unknown type and an undecodable input use
      [reject_unrunnable_task] instead. *)
  let reject_task adapter ~token ~correlation error =
    reject_task_with_failure adapter ~token ~correlation
      ~failure:(failure_of_error error) error

  (** Rejects a task that this worker cannot run but another worker polling
      the same queue might: an unregistered activity type
      ([type_name = "NotFoundError"], as in the Go, Python, TypeScript and
      .NET SDKs) or an input this build cannot decode. For a remote activity
      the failure is retryable, so the activity's retry policy decides and a
      retry can reach an updated worker during a rolling deploy or on a queue
      shared by workers with different registrations (#1031). A local
      activity always runs on the scheduling worker, where a retry would fail
      the same way, so it stays non-retryable. *)
  let reject_unrunnable_task ?type_name adapter ~token ~correlation
      ~(start : Protocol.activity_start) error =
    reject_task_with_failure ~refusal:Metrics.Unrunnable adapter ~token
      ~correlation ~failure:
        (failure_of_error ?type_name ~non_retryable:start.is_local error)
      error

  (** Routes a start-task conversion failure: an undecodable input through
      [reject_unrunnable_task], an unrepresentable heartbeat context through
      [reject_task]. *)
  let reject_decode_failure adapter ~token ~correlation ~start = function
    | Input_rejected error ->
        reject_unrunnable_task adapter ~token ~correlation ~start error
    | Context_rejected error -> reject_task adapter ~token ~correlation error

  (** Retires a task whose application callback raised. Without [backtrace]
      it must be called directly from the exception handler so the captured
      backtrace is the callback's; a caller that does other work first passes
      the backtrace it captured in the handler. *)
  let reject_callback_exception ?backtrace adapter ~token ~correlation
      exception_ =
    let backtrace =
      match backtrace with
      | Some backtrace -> backtrace
      | None -> Printexc.get_raw_backtrace ()
    in
    let backtrace =
      try Printexc.raw_backtrace_to_string backtrace with _ -> ""
    in
    let diagnostic =
      exception_error ~path:"$.implementation" exception_
    in
    reject_task_with_failure ~refusal:Metrics.Callback_raised adapter ~token
      ~correlation ~failure:(callback_exception_failure diagnostic backtrace)
      diagnostic

  (** Records that an application callback is about to run (admission and
      occupancy, #500) and returns the function that records its end
      (occupancy and duration). Both synchronous and asynchronous dispatch
      call it immediately around the callback. With metrics off it reads no
      clock and returns [ignore]; the returned function never raises, so it
      is safe in a [finally]. *)
  let begin_callback_metrics adapter =
    let metrics = Atomic.get adapter.metrics in
    if not (Metrics.enabled metrics) then ignore
    else begin
      let started_ns = Temporal_core_bridge.Native_bridge.monotonic_now_ns () in
      Metrics.activity_callback_started metrics;
      fun () ->
        Metrics.activity_callback_finished metrics
          ~elapsed_ns:
            (Int64.sub
               (Temporal_core_bridge.Native_bridge.monotonic_now_ns ())
               started_ns)
    end

  (** Runs one application callback with [callback_active] set, clearing it
      however the callback ends. Its exception, and the backtrace an
      immediate handler then reads, pass through unchanged. The callback's
      metrics are recorded around it ({!begin_callback_metrics}). *)
  let with_callback_active adapter callback =
    let finished = begin_callback_metrics adapter in
    Atomic.set adapter.callback_active true;
    match callback () with
    | result ->
        Atomic.set adapter.callback_active false;
        finished ();
        result
    | exception exception_ ->
        let backtrace = Printexc.get_raw_backtrace () in
        Atomic.set adapter.callback_active false;
        finished ();
        Printexc.raise_with_backtrace exception_ backtrace

  (** Reports whether an activity error claims to acknowledge cancellation. *)
  let is_cancelled_error error =
    match (Base_error.view error).category with
    | `Cancelled -> true
    | `Activity | `Bridge | `Child_workflow | `Codec | `Defect | `Nexus
    | `Terminated | `Timeout | `Update | `Workflow ->
        false

  (** Reads the cancellation delivered to [running], under [delivery_mutex]
      because a sweep on a callback-spawned Domain may still be writing it. *)
  let observed_cancel adapter running =
    with_delivery adapter (fun () -> running.observed)

  (** Completes a running attempt after its callback returned a [`Cancelled]
      error for a delivered Core cancellation. The error's detail payloads
      become the details of the canceled failure or, for a pause or reset,
      of the retryable application failure that [interruption_failure]
      requires instead (#1022); Core's reason and flags stay private outcome
      metadata, as for an unstarted cancellation. *)
  let complete_acknowledged_cancellation adapter ~token ~correlation
      (cancel : Protocol.activity_cancel) error =
    let rec details_loop reversed = function
      | [] -> Ok (List.rev reversed)
      | payload :: rest ->
          let* payload =
            protocol_payload "$.completion.result.info.details" payload
          in
          details_loop (payload :: reversed) rest
    in
    match details_loop [] (Base_error.view error).details with
    | Error error -> reject_task adapter ~token ~correlation error
    | Ok details ->
        let result, kind =
          match interruption_failure ~details cancel with
          | Some failure -> (Protocol.Failed failure, Failed)
          | None ->
              ( Protocol.Cancelled (cancellation_failure ~details cancel.reason),
                Cancelled )
        in
        let completion = Protocol.{ task_token = Bytes.copy token; result } in
        enqueue_and_finish adapter ~token ~correlation ~completion
          ~accepted_result:
            (Completed_result { kind; cancellation_details = cancel.details })

  (** Executes an asynchronous activity callback. The handle is dormant while
      the callback runs; only an accepted remote [Will_complete_async]
      completion causes [finish_lease] to publish and activate it; any other
      outcome closes it. Core cannot
      accept this handoff for a local activity, so that outcome is converted to
      an ordinary failure before a completion is queued. *)
  let process_async_start adapter token definition
      (start : Protocol.activity_start) =
    let correlation = Some (correlation_of_start start) in
    let process () =
      (* [decode_payloads], not [decode_start]: the heartbeat interval is
         only needed by synchronous contexts. *)
      match decode_payloads definition start with
      | Error failure ->
          reject_decode_failure adapter ~token ~correlation ~start failure
      | Ok (input, _details) ->
          (match Definition.implementation definition with
          | None ->
              reject_task adapter ~token ~correlation
                (make_error ~path:"$.variant.activity_type" "not_executable"
                   "registered asynchronous activity has no local implementation")
          | Some implementation ->
              let encode_output output =
                match Codec.encode (Definition.output definition) output with
                | Error error -> Error error
                | Ok payload -> Ok payload
              in
              let handle =
                Async_activity.create
                  ~worker_shutting_down:adapter.worker_shutting_down
                  ~submit:(submit_async_operation adapter ~token)
                  ~encode_output ()
              in
              let context =
                Async_activity.context ~info:(task_info start) handle
              in
              let context_handle = Async_activity.handle context in
              (* External code may retain this handle and retry the retryable
                 "not active yet" error while the handoff is outstanding
                 (#766). Every outcome that does not reserve it for the
                 handoff (a synchronous result, a callback defect, a local
                 activity, or a handle from another attempt) closes it here,
                 so such a retry loop ends with a non-retryable error. A
                 reserved handle is left to [finish_lease] or [discard]. *)
              Fun.protect
                ~finally:(fun () -> Async_activity.close_if_dormant context_handle)
              @@ fun () ->
              (try
                 match
                   with_callback_active adapter (fun () ->
                       implementation context input)
                 with
                 | exception exception_ ->
                     reject_callback_exception adapter ~token ~correlation
                       exception_
                 | Async_activity.Completed output ->
                     (match encode_output output with
                     | Error error ->
                         reject_task adapter ~token ~correlation
                           (application_error
                              ~path:"$.implementation.output" error)
                     | Ok payload ->
                         (match protocol_payload "$.completion.result" payload with
                         | Error error ->
                             reject_task adapter ~token ~correlation error
                         | Ok payload ->
                             let completion =
                               Protocol.
                                 {
                                   task_token = Bytes.copy token;
                                   result = Completed payload;
                                 }
                             in
                             enqueue_and_finish adapter ~token ~correlation
                               ~completion
                               ~accepted_result:
                               (Completed_result
                                  {
                                    kind = Succeeded;
                                    cancellation_details = None;
                                  })))
                 | Async_activity.Failed implementation_error ->
                     let diagnostic =
                       application_error ~path:"$.implementation"
                         implementation_error
                     in
                     (match
                        failure_of_application_error diagnostic
                          implementation_error
                      with
                     | Error error -> reject_task adapter ~token ~correlation error
                     | Ok failure ->
                         reject_task_with_failure ~origin:Application_failure
                           adapter ~token ~correlation ~failure diagnostic)
                 | Async_activity.Will_complete_async handle ->
                     if start.is_local then begin
                       (* The callback may retain the dormant handle. Close its
                          current-attempt capability before acknowledging a
                          failure, so no later operation can target this token.
                          The failure lease remains retryable if native
                          submission is genuinely uncertain. *)
                       ignore (Async_activity.close context_handle);
                       reject_task adapter ~token ~correlation
                         (make_error ~path:"$.implementation.async_handle"
                            "local_async_unsupported"
                            "local activities cannot be completed asynchronously")
                     end
                     else
                       (match
                          Async_activity.prepare_handoff ~expected:context_handle
                            handle
                        with
                       | Error error ->
                           reject_task adapter ~token ~correlation
                             (application_error ~path:"$.implementation.async_handle"
                                error)
                       | Ok () ->
                           let completion =
                             Protocol.
                               {
                                 task_token = Bytes.copy token;
                                 result = Will_complete_async;
                               }
                           in
                           enqueue_and_finish adapter ~token ~correlation
                             ~completion
                             ~accepted_result:
                               (Async_handoff
                                  {
                                    handle;
                                    timeouts = async_timeouts_of_start start;
                                  }))
               with exception_ ->
                 reject_task adapter ~token ~correlation
                   (exception_error ~path:"$.implementation" exception_)))
    in
    try process ()
    with exception_ ->
      reject_task adapter ~token ~correlation
        (exception_error ~path:"$.implementation" exception_)

  (** Executes one start-task implementation under a final exception guard.
      The [activity_task_started] record is emitted before dispatch, so every
      application log written by the callback falls between it and the
      attempt's terminal lease record, both tagged with the same identity. *)
  let process_start adapter token (start : Protocol.activity_start) =
    let identity = correlation_of_start start in
    let correlation = Some identity in
    report Logs.Debug ~operation:"activity_task_started" ~correlation:identity
      ();
    (* Keep all local dispatch and codec failures on the completion path. The
       outer guard catches defects so even an exception retires this lease. *)
    let process () =
      match find_definition adapter.definitions start.activity_type with
      | Error error ->
          reject_unrunnable_task ~type_name:"NotFoundError" adapter ~token
            ~correlation ~start error
      | Ok (Registered_async_definition definition) ->
          process_async_start adapter token definition start
      | Ok (Registered_definition definition) ->
          (match Definition.implementation definition with
          | None ->
              reject_task adapter ~token ~correlation
                (make_error ~path:"$.variant.activity_type" "not_executable"
                   "registered activity has no local implementation")
          | Some implementation ->
              (match decode_start definition start with
              | Error failure ->
                  reject_decode_failure adapter ~token ~correlation ~start
                    failure
              | Ok (input, details, heartbeat_timeout) ->
                  let signal = Activity_context.cancellation_signal () in
                  let running =
                    {
                      running_token = Bytes.copy token;
                      running_correlation = identity;
                      signal;
                      observed = None;
                    }
                  in
                  with_delivery adapter (fun () ->
                      adapter.running <- Some running);
                  Atomic.set adapter.callback_active true;
                  let context =
                    activity_context adapter ~token ~signal
                      ~info:(task_info start) ~details ~heartbeat_timeout
                  in
                  (* Capture the callback's outcome, then end the attempt
                     before classifying it. Invalidation waits for a heartbeat
                     in flight (possibly on a Domain the callback spawned), so
                     once it returns no sweep can still be routing to this
                     attempt; only then is [running] cleared and the
                     cancellation cell read. Classifying earlier would race a
                     sweep that delivers the cancellation after the callback
                     returned its [`Cancelled] error. The exception backtrace
                     is captured in the handler, before any other work. *)
                  let finish_metrics = begin_callback_metrics adapter in
                  let returned =
                    Fun.protect
                      ~finally:(fun () ->
                        Activity_context.invalidate context;
                        with_delivery adapter (fun () ->
                            adapter.running <- None);
                        Atomic.set adapter.callback_active false;
                        finish_metrics ())
                      (fun () ->
                        match implementation context input with
                        | result -> Ok result
                        | exception exception_ ->
                            Error (exception_, Printexc.get_raw_backtrace ()))
                  in
                  (* No writer remains, so this is the final value. *)
                  let delivered = observed_cancel adapter running in
                  (match returned with
                      | Error (exception_, backtrace) ->
                          reject_callback_exception ~backtrace adapter ~token
                            ~correlation exception_
                      | Ok (Error implementation_error)
                        when is_cancelled_error implementation_error
                             && Option.is_some delivered ->
                          (* The callback acknowledged a cancellation that
                             Core actually delivered: report it as one, or
                             as the failure a pause or reset requires. *)
                          complete_acknowledged_cancellation adapter ~token
                            ~correlation (Option.get delivered)
                            implementation_error
                      | Ok (Error implementation_error) ->
                          let diagnostic =
                            application_error ~path:"$.implementation"
                              implementation_error
                          in
                          begin
                            match
                              failure_of_application_error diagnostic
                                implementation_error
                            with
                            | Error error ->
                                reject_task adapter ~token ~correlation error
                            | Ok failure ->
                                reject_task_with_failure
                                  ~origin:Application_failure adapter ~token
                                  ~correlation ~failure diagnostic
                          end
                      | Ok (Ok output) ->
                          (match
                             Codec.encode (Definition.output definition) output
                           with
                          | Error error ->
                              reject_task adapter ~token ~correlation
                                (application_error
                                   ~path:"$.implementation.output" error)
                          | Ok payload ->
                              (match
                                 protocol_payload "$.completion.result" payload
                               with
                              | Error error ->
                                  reject_task adapter ~token ~correlation error
                              | Ok payload ->
                                  let completion =
                                    Protocol.
                                      {
                                        task_token = Bytes.copy token;
                                        result = Completed payload;
                                      }
                                  in
                                  enqueue_and_finish adapter ~token
                                    ~correlation ~completion
                                    ~accepted_result:
                                    (Completed_result
                                       {
                                         kind = Succeeded;
                                         cancellation_details = None;
                                       }))))))
    in
    try process ()
    with exception_ ->
      reject_task adapter ~token ~correlation
        (exception_error ~path:"$.implementation" exception_)

  (** Builds the canceled completion and accepted result for a cancellation
      whose callback never ran, or the retryable failure that a pause or
      reset requires instead (#1022). Its independent Core details are
      retained on the private outcome for instrumentation. They remain
      metadata rather than being copied into the Temporal failure. *)
  let cancelled_completion token (cancel : Protocol.activity_cancel) =
    let result, kind =
      match interruption_failure cancel with
      | Some failure -> (Protocol.Failed failure, Failed)
      | None -> (Protocol.Cancelled (cancellation_failure cancel.reason), Cancelled)
    in
    ( Protocol.{ task_token = Bytes.copy token; result },
      Completed_result { kind; cancellation_details = cancel.details } )

  (** Submits the cancelled completion for a task whose callback never ran:
      a bare cancellation, or a deferred start whose cancellation was also
      deferred. [correlation] is known only in the second case. *)
  let process_cancel ?correlation adapter token
      (cancel : Protocol.activity_cancel) =
    let completion, accepted_result = cancelled_completion token cancel in
    enqueue_and_finish adapter ~token ~correlation ~completion
      ~accepted_result

  (** Removes every deferred cancellation for [token] and returns the first.
      Core issues at most one per attempt; any repeat would only fabricate a
      second completion for the same token, so it is dropped with the first. *)
  let take_deferred_cancel adapter token =
    with_delivery adapter (fun () ->
        let found = ref None in
        let kept = Queue.create () in
        Queue.iter
          (fun work ->
            match work with
            | Deferred_task { task_token; variant = Cancel cancel }
              when Bytes.equal task_token token ->
                if Option.is_none !found then found := Some cancel
            | Deferred_task _ | Deferred_poll_error _ -> Queue.push work kept)
          adapter.deferred;
        Queue.clear adapter.deferred;
        Queue.transfer kept adapter.deferred;
        !found)

  (** Processes one decoded task after copying its token. The extra empty-token
      check protects the adapter if a test or future supervisor bypasses the
      strict JSON decoder. A start whose cancellation a sweep already took is
      completed as cancelled without running its callback: the cancellation
      arrived before the attempt was admitted to the executor. *)
  let process_task adapter (task : Protocol.task) =
    let token = Bytes.copy task.task_token in
    if Bytes.length token = 0 then
      Error
        (make_error ~path:"$.task_token" "invalid_message"
           "activity task token must not be empty")
    else
      match task.variant with
      | Protocol.Start start -> (
          match take_deferred_cancel adapter token with
          | Some cancel ->
              process_cancel ~correlation:(correlation_of_start start) adapter
                token cancel
          | None -> process_start adapter token start)
      | Cancel cancel -> process_cancel adapter token cancel

  (** Converts work deferred by delivery sweeps into recorded completions at
      shutdown, without running user code. A deferred start is cancelled when
      its cancellation was deferred too. Otherwise a remote start is failed
      retryably so Temporal can run it on another worker, and a local start
      is reported as cancelled ({!undispatched_local_cancellation}, #1209). A bare deferred cancellation owns
      no completion and a deferred poll error no longer matters once the lanes
      have stopped, so both are dropped. Returns the first admission error. *)
  let retire_deferred adapter =
    let work =
      with_delivery adapter (fun () ->
          let work = List.of_seq (Queue.to_seq adapter.deferred) in
          Queue.clear adapter.deferred;
          work)
    in
    let cancel_for token =
      List.find_map
        (function
          | Deferred_task { task_token; variant = Cancel cancel }
            when Bytes.equal task_token token ->
              Some cancel
          | Deferred_task _ | Deferred_poll_error _ -> None)
        work
    in
    List.fold_left
      (fun result work ->
        match (result, work) with
        | Error _, _ -> result
        | Ok (), Deferred_task { task_token; variant = Start start } ->
            let token = Bytes.copy task_token in
            let completion, accepted_result =
              match cancel_for token with
              | Some cancel -> cancelled_completion token cancel
              | None when start.is_local ->
                  ( Protocol.
                      {
                        task_token = Bytes.copy token;
                        result = Cancelled undispatched_local_cancellation;
                      },
                    Completed_result
                      { kind = Cancelled; cancellation_details = None } )
              | None ->
                  ( Protocol.
                      {
                        task_token = Bytes.copy token;
                        result = Failed undispatched_failure;
                      },
                    Completed_result
                      { kind = Failed; cancellation_details = None } )
            in
            admit_completion adapter ~token
              ~correlation:(Some (correlation_of_start start)) ~completion
              ~accepted_result
            |> Result.map ignore
        | Ok (), (Deferred_task { variant = Cancel _; _ } | Deferred_poll_error _)
          ->
            result)
      (Ok ()) work

  (** Retries retained activity completions while the adapter mutex is held.
      Native worker shutdown calls this before closing Rust so an explicitly
      retryable completion transport failure cannot become an outstanding
      task-token lease. A lease whose earlier failure was not retryable is not
      resubmitted; [finish_lease] returns its recorded non-retryable error, so
      shutdown takes the terminal force-release path. Work a delivery sweep
      deferred is first recorded as completions by [retire_deferred], so a
      start the executor never dispatched does not outlive the worker. *)
  let drain_locked adapter : (unit, error_view) result =
    let* () = retire_deferred adapter in
    (* Retry the smallest token first for deterministic shutdown behavior;
       stop at the first failure and retain that lease. A fail-closed lease
       stops the loop without any native call. *)
    let rec loop () =
      match Token_map.min_binding_opt adapter.leases with
      | None -> Ok ()
      | Some (_, lease) -> (
          match finish_lease adapter lease with
          | Ok _ -> loop ()
          | Error error -> Error error)
    in
    match loop () with
    | Error error -> Error error
    | Ok () ->
        Mutex.lock adapter.async_mutex;
        Fun.protect
          ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
          (fun () ->
            (* A lease whose attempt the server has already timed out can
               never be finished; it must not hold shutdown (#1043). *)
            retire_expired_async_leases_locked adapter;
            if Token_map.is_empty adapter.async_leases then Ok ()
            else
              let error =
                make_error ~path:"$.async_leases" ~retryable:true
                  "outstanding_async_leases"
                  "asynchronous activity completions remain admitted"
              in
              Error error)

  (** Takes the adapter mutex for [drain_locked]. *)
  let drain adapter : (unit, error_view) result =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () -> drain_locked adapter)

  (** Drops copied activity completions after terminal native cleanup. The Rust
      runtime has already force-retired its leases, so retaining or retrying
      these tokens could duplicate a completion. Async handles, whether
      reserved for an unaccepted handoff or already admitted, are closed so
      retained copies stop reporting retryable errors. The mutex keeps discard
      ordered with any final adapter operation. *)
  let discard_locked adapter =
    (* Deferred tasks were force-retired with the rest of the native
       graph; dispatching or completing them now would duplicate that. *)
    with_delivery adapter (fun () -> Queue.clear adapter.deferred);
    (* A handoff that was never accepted leaves its handle reserved in
       [Handoff_pending]. Close it so external code retrying the
       retryable "not active yet" error observes a terminal error instead
       of waiting for an activation that can no longer happen. *)
    Token_map.iter
      (fun _ lease ->
        match lease.accepted_result with
        | Async_handoff { handle; _ } -> ignore (Async_activity.close handle)
        | Completed_result _ | Rejected_result _ -> ())
      adapter.leases;
    adapter.leases <- Token_map.empty;
    Mutex.lock adapter.async_mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
      (fun () ->
        Token_map.iter
          (fun _ (Async_lease lease) ->
            ignore (Async_activity.close lease.handle))
          adapter.async_leases;
        adapter.async_leases <- Token_map.empty)

  (** Takes the adapter mutex for [discard_locked]. *)
  let discard adapter =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () -> discard_locked adapter)

  (** Runs [f] with the adapter mutex only if it is free right now. A running
      callback holds that mutex for its whole duration, so [None] is the
      non-blocking signal that the activity lane is still occupied. *)
  let if_idle adapter f =
    if Mutex.try_lock adapter.mutex then
      Some (Fun.protect ~finally:(fun () -> Mutex.unlock adapter.mutex) f)
    else None

  (** Non-blocking [drain] for bounded shutdown (#495). *)
  let try_drain adapter = if_idle adapter (fun () -> drain_locked adapter)

  (** Non-blocking [discard] for bounded shutdown (#495). *)
  let try_discard adapter =
    Option.is_some (if_idle adapter (fun () -> discard_locked adapter))

  (** Reads the atomic callback flag. It takes no lock: a delivery sweep
      inside a heartbeat holds the delivery lock across supervisor calls, so
      reading [running] under it could make the shutdown thread wait for an
      abandoned callback (#1023). *)
  let callback_running adapter = Atomic.get adapter.callback_active

  let attach_metrics adapter metrics = Atomic.set adapter.metrics metrics

  (** Counts admitted async leases under [async_mutex] only, which a running
      callback never holds. The lock is held only for short registry updates
      (never across an async RPC since #1025), but the probe must never wait,
      so it is only tried. Expired leases are retired first, so they are not
      counted (#1043). *)
  let outstanding_async_leases adapter =
    if Mutex.try_lock adapter.async_mutex then
      Some
        (Fun.protect
           ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
           (fun () ->
             retire_expired_async_leases_locked adapter;
             Token_map.cardinal adapter.async_leases))
    else None

  (** Serializes pending-completion retry, native polling, implementation
      execution, and completion submission. A retained lease blocks new tasks
      and is resubmitted only after an explicitly retryable failure. The mutex
      covers the complete transaction, including the user implementation, so
      the map cannot race with another poll and no token can be dispatched
      twice. Work deferred by a delivery sweep is handled, in the order it was
      taken, before the supervisor is polled again. *)
  let poll adapter =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () ->
        match Token_map.min_binding_opt adapter.leases with
        | Some (_, lease) -> finish_lease adapter lease
        | None -> (
            match
              with_delivery adapter (fun () -> Queue.take_opt adapter.deferred)
            with
            | Some (Deferred_task task) -> process_task adapter task
            | Some (Deferred_poll_error error) -> Error error
            | None -> (
                let polled =
                  try Ok (Supervisor.try_poll_activity adapter.supervisor)
                  with exception_ ->
                    Error (exception_error ~path:"$.poll" exception_)
                in
                let* polled = polled in
                match polled with
                | Error source_error ->
                    Error
                      (supervisor_error ~path:"$.poll"
                         ~retryable:(source_error_is_retryable source_error)
                         ~error_code:Supervisor.error_code
                         ~error_message:Supervisor.error_message source_error)
                | Ok None ->
                    report Logs.Debug ~operation:"activity_poll_not_ready" ();
                    Ok Not_ready
                | Ok (Some task) -> process_task adapter task)))
end

(** Hides the existential constructor from callers while retaining the shared
    [Definition.t] representation used by public activity definitions. *)
let register definition = Activity definition

(** Packs an asynchronous definition while keeping its output type paired with
    the callback and codec. *)
let register_async definition = Async_activity definition

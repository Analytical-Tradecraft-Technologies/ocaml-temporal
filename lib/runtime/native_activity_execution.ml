(** Private typed execution of one native Temporal activity-task lease.

    Rust/Core owns polling, protobuf conversion, and network concurrency. This
    module owns only the OCaml half: a deterministic activity registry, codec
    conversion, implementation dispatch, and the small pending-completion map
    needed when the native completion call is temporarily unavailable. The map
    is deliberately protected by one mutex; no task can be executed twice merely
    because its completion transport failed. *)

module Protocol = Temporal_protocol.Activity_protocol
module Definition = Temporal_base.Definition
module Codec = Temporal_base.Codec
module Base_error = Temporal_base.Error
module Observability = Temporal_base.Observability
module Activity_context = Temporal_base.Activity_context
module Async_activity = Temporal_base.Async_activity

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
  val complete_activity : t -> Protocol.completion -> (unit, error) result
  (* Submits a completion for an activity that has already returned
     [WillCompleteAsync]. This path uses Core's namespace-bound client and
     never consults the worker task-token ledger. *)
  val complete_async_activity :
    t -> Protocol.completion -> (unit, error) result
  val record_activity_heartbeat : t -> Protocol.heartbeat -> (unit, error) result
  (* Records heartbeat details through the namespace-bound async handle. *)
  val record_async_activity_heartbeat :
    t -> Protocol.heartbeat -> (unit, error) result
  val error_code : error -> string
  val error_message : error -> string
  val error_is_retryable : error -> bool
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

(** What the caller should observe after a lease is acknowledged. A rejected
    task carries the original adapter diagnostic so the caller can identify a
    bad registration or payload without inspecting the completion bytes. *)
type accepted_result =
  | Completed_result of {
      kind : completion_kind;
      cancellation_details : cancellation_details option;
    }
  | Rejected_result of error_view
  | Async_handoff :
      'output Temporal_base.Async_activity.handle -> accepted_result

type lease = {
  token : bytes;
  activity_type : string option;
  completion : Protocol.completion;
  accepted_result : accepted_result;
}
(** One completion that has not yet been proven accepted by native Core. The
    [token] is always an owned copy and [completion] contains another owned copy
    of that same token. *)

(** An async lease is the capability retained after Core accepts
    [WillCompleteAsync]. It is deliberately kept in a separate map from worker
    task leases because later operations use the namespace-bound Temporal
    client and must never be sent through the worker completion ledger. *)
type async_lease =
  | Async_lease :
      {
        token : bytes;
        handle : 'output Temporal_base.Async_activity.handle;
        mutable in_flight : bool;
      }
      -> async_lease

(** Bounds diagnostics before they are sent to Logs or embedded in a
    non-retryable Temporal failure. Invalid UTF-8 is replaced because the
    protocol's string fields are strict UTF-8. *)
let bounded_text ~fallback value =
  let maximum = 1_024 in
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

(** Builds a non-retryable application failure used when a task cannot be
    dispatched locally. Empty details keep the failure independent of mutable
    application payloads and make it safe to retry completion submission. *)
let failure_of_error (error : error_view) : Protocol.failure =
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
            type_name = "ocaml_temporal_native_activity";
            non_retryable = true;
            details = [];
          };
    }

(** Uses a stable, closed set of cancellation labels so cancellation failures
    remain valid protocol strings even when the source task is malformed. *)
let cancellation_reason = function
  | Protocol.Cancellation_not_found -> "not_found"
  | Cancellation_requested -> "cancelled"
  | Cancellation_timed_out -> "timed_out"
  | Cancellation_worker_shutdown -> "worker_shutdown"
  | Cancellation_paused -> "paused"
  | Cancellation_reset -> "reset"

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
    replacement decoding. Body and metadata bytes are copied so a task can be
    retained safely after the supervisor releases its source buffer. *)
let runtime_payload path (payload : Protocol.payload) =
  (* Accumulate backwards for linear construction, then reverse once so the
     caller observes metadata in the same order as the protocol task. *)
  let rec metadata_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | (key, bytes) :: rest ->
        if String.length key = 0 || String.contains key '\000' then
          Error
            (make_error ~path:(path ^ ".metadata") "invalid_message"
               "metadata key must be non-empty and must not contain NUL")
        else if not (Codec.valid_utf_8 key) then
          Error
            (make_error ~path:(path ^ ".metadata.key") "invalid_message"
               "metadata key must be valid UTF-8")
        else
          let value = Bytes.to_string bytes in
          if not (Codec.valid_utf_8 value) then
            Error
              (make_error
                 ~path:(path ^ ".metadata." ^ key)
                 "unsupported"
                 "binary metadata cannot be represented by the runtime")
          else metadata_loop ((key, value) :: reversed) rest
  in
  let* metadata = metadata_loop [] payload.metadata in
  Ok { Temporal_base.Payload.metadata; data = Bytes.copy payload.data }

(** Converts a runtime payload into the binary-safe protocol representation,
    validating metadata before copying it across the ownership boundary. *)
let protocol_payload path (payload : Temporal_base.Codec.payload) =
  (* Use the same order-preserving construction as [runtime_payload], but turn
     validated strings back into freshly owned byte buffers. *)
  let rec metadata_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | (key, value) :: rest ->
        if String.length key = 0 || String.contains key '\000' then
          Error
            (make_error ~path:(path ^ ".metadata") "invalid_message"
               "metadata key must be non-empty and must not contain NUL")
        else if not (Codec.valid_utf_8 key) then
          Error
            (make_error ~path:(path ^ ".metadata.key") "invalid_message"
               "metadata key must be valid UTF-8")
        else if not (Codec.valid_utf_8 value) then
          Error
            (make_error
               ~path:(path ^ ".metadata." ^ key)
               "invalid_message" "metadata value must be valid UTF-8")
        else metadata_loop ((key, Bytes.of_string value) :: reversed) rest
  in
  let* metadata = metadata_loop [] payload.metadata in
  Ok Protocol.{ metadata; data = Bytes.copy payload.data }

(** Converts a Core duration into the runtime's millisecond-only value without
    silently rounding. Core can represent sub-millisecond durations, but the
    public OCaml duration intentionally cannot; rejecting those values keeps a
    heartbeat timeout exact instead of changing its meaning at the boundary. *)
let runtime_duration path (duration : Protocol.duration) =
  if Int64.compare duration.seconds 0L < 0 then
    Error
      (make_error ~path:(path ^ ".seconds") "invalid_message"
         "duration must not be negative")
  else if duration.nanoseconds < 0 || duration.nanoseconds >= 1_000_000_000 then
    Error
      (make_error ~path:(path ^ ".nanoseconds") "invalid_message"
         "nanoseconds are outside protobuf range")
  else if duration.nanoseconds mod 1_000_000 <> 0 then
    Error
      (make_error ~path:(path ^ ".nanoseconds") "unsupported"
         "sub-millisecond durations are not representable by the runtime")
  else
    (* Compute the largest representable whole-millisecond value before
       multiplying seconds, avoiding an overflowing intermediate Int64. *)
    let milliseconds_per_second = 1_000L in
    let milliseconds = Int64.of_int (duration.nanoseconds / 1_000_000) in
    let maximum_seconds = Int64.div Int64.max_int milliseconds_per_second in
    let maximum_remainder =
      Int64.rem Int64.max_int milliseconds_per_second
    in
    if Int64.compare duration.seconds maximum_seconds > 0
       || (Int64.equal duration.seconds maximum_seconds
          && Int64.compare milliseconds maximum_remainder > 0)
    then
      Error
        (make_error ~path "unsupported"
           "duration exceeds the runtime millisecond range")
    else
      Ok
        (Temporal_base.Duration.of_ms
           (Int64.add
              (Int64.mul duration.seconds milliseconds_per_second)
              milliseconds))

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
    retryability classification and every application-supplied detail payload.
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
              type_name = Base_error.kind error;
              non_retryable = view.non_retryable;
              details;
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

(** Finds an executable definition by the Temporal activity type. *)
let find_definition definitions activity_type =
  match Name_map.find_opt activity_type definitions with
  | Some definition -> Ok definition
  | None ->
      Error
        (make_error ~path:"$.variant.activity_type" "unknown_activity_type"
           ("no executable activity is registered for type " ^ activity_type))

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
    affect lease ownership or activity execution. *)
let report level ~operation ?error_kind () =
  try
    let tags = Observability.tags ~operation ?error_kind () in
    Observability.report ~src:Observability.Source.lifecycle level ~tags
      "native activity worker adapter event"
  with _ -> ()

(** One of the two ways a native completion call may finish. Keeping raised
    exceptions distinct lets [poll] retain the pending lease in both cases. *)
type completion_attempt =
  | Accepted
  | Rejected_by_supervisor of error_view
  | Raised_by_supervisor of exn

(** Validates the typed completion using the same strict JSON semantic encoder
    that the Rust-facing supervisor uses. No JSON string is retained; the
    round-trip is a validation gate only. *)
let validate_completion completion =
  match Protocol.encode_completion completion with
  | Ok _ -> Ok ()
  | Error error -> Error (protocol_error ~path:"$.completion" error)

(** Converts a supervisor completion exception to a stable diagnostic without
    revealing the opaque token or native exception object. *)
let completion_exception_error ?(retryable = false) exception_ =
  make_error ~path:"$.completion" ~retryable "completion_failed"
    (Printf.sprintf "supervisor completion raised: %s"
       (exception_error exception_).message)

module Make (Supervisor : SUPERVISOR) = struct
  (** State owned by one activity adapter. Definitions never change after
      construction; [leases] contains only copied completions whose opaque
      task-token acknowledgements are still uncertain. Every field is accessed
      while [mutex] is held, including calls into the supervisor. *)
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
  }

  (** The public worker handle is the mutex-confined state above. *)
  type t = adapter_state

  (** A malformed test double or future supervisor must not be able to turn a
      diagnostic-classifier exception into an unbounded retry loop. Treat a
      classifier defect as permanent; only an explicit [true] classification
      can authorize retained-completion retry. *)
  let source_error_is_retryable source_error =
    try Supervisor.error_is_retryable source_error with _ -> false

  (** Exception classification is equally conservative: arbitrary exceptions
      are owner-domain defects unless the supervisor explicitly marks one as a
      transient completion transport failure. *)
  let completion_exception_is_retryable exception_ =
    try Supervisor.exception_is_retryable exception_ with _ -> false

  (** Builds the context passed to one activity attempt. Heartbeats go back
      through the same typed supervisor mailbox as polling and completion; the
      callback never captures a raw Rust pointer and the token is copied for
      each request. Contexts are invalidated by [process_start] before it
      returns, so retaining one in user code cannot submit progress for a later
      attempt. *)
  let activity_context adapter ~token ~details ~heartbeat_timeout =
    (* The callback remains valid only for this lease. It copies the token and
       every detail before crossing to the supervisor, so a caller cannot
       mutate a heartbeat after submission. *)
    let heartbeat payloads =
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
      | Ok details ->
          let heartbeat = Protocol.{ task_token = Bytes.copy token; details } in
          try
            match
              Supervisor.record_activity_heartbeat adapter.supervisor heartbeat
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
                 ())
    in
    Activity_context.create ~heartbeat ~details ~heartbeat_timeout

  (** Creates the registry without contacting native Core or invoking user code. *)
  let create ~supervisor ~activities =
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
          }

  (** Converts an adapter diagnostic into the base error type expected by an
      asynchronous handle callback. The callback is a private boundary, so a
      bounded message is sufficient and no task token is ever included. *)
  let base_operation_error operation (error : error_view) =
    Base_error.make ~category:`Bridge ~non_retryable:(not error.retryable)
      ~message:(
        Printf.sprintf "%s failed (%s): %s" operation error.code error.message)
      ()

  (** Calls the namespace-bound async client for one retained handle operation.
      The adapter mutex is held for the complete reservation, native call, and
      state transition. This intentionally serializes retained async
      operations: shutdown must not be able to discard the registry between a
      native call returning and its lease state being committed. Rust/Core
      owns network concurrency; this OCaml lock protects only the small
      cross-language ownership ledger. *)
  let submit_async_operation adapter ~token operation : Async_activity.submit_result =
    Mutex.lock adapter.async_mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
      (fun () ->
        match Token_map.find_opt token adapter.async_leases with
        | None -> Error (base_operation_error "async activity handle" (make_error
            ~path:"$.async_handle" "closed" "asynchronous activity handle is no longer active"))
        | Some (Async_lease lease) when lease.in_flight ->
            Error (base_operation_error "async activity handle" (make_error
              ~path:"$.async_handle" "busy" "asynchronous activity operation is already in flight"))
        | Some (Async_lease lease) ->
            lease.in_flight <- true;
        let terminal =
          match operation with
          | Async_activity.Complete _
          | Async_activity.Fail _
          | Async_activity.Cancel _ -> true
          | Async_activity.Heartbeat _ -> false
        in
        let operation_result =
          try
            let protocol_error_to_base error =
              base_operation_error "async activity payload" error
            in
            match operation with
          | Async_activity.Complete payload ->
              (match protocol_payload "$.async_completion.result" payload with
              | Error error -> Error (protocol_error_to_base error)
              | Ok payload ->
                  let completion =
                    Protocol.
                      {
                        task_token = Bytes.copy token;
                        result = Completed (Some payload);
                      }
                  in
                  (try
                     match
                       Supervisor.complete_async_activity adapter.supervisor
                         completion
                     with
                     | Ok () -> Ok ()
                     | Error source_error ->
                         Error
                           (base_operation_error "async activity completion"
                              (supervisor_error ~path:"$.async_completion"
                                 ~retryable:(source_error_is_retryable source_error)
                                 ~error_code:Supervisor.error_code
                                 ~error_message:Supervisor.error_message
                                 source_error))
                   with exception_ ->
                     Error
                       (base_operation_error "async activity completion"
                          (exception_error ~path:"$.async_completion" exception_))))
          | Async_activity.Fail failure ->
              let diagnostic = application_error ~path:"$.async_failure" failure in
              (match failure_of_application_error diagnostic failure with
              | Error error -> Error (base_operation_error "async activity failure" error)
              | Ok failure ->
                  let completion =
                    Protocol.{ task_token = Bytes.copy token; result = Failed failure }
                  in
                  (try
                     match
                       Supervisor.complete_async_activity adapter.supervisor
                         completion
                     with
                     | Ok () -> Ok ()
                     | Error source_error ->
                         Error
                           (base_operation_error "async activity failure"
                              (supervisor_error ~path:"$.async_failure"
                                 ~retryable:(source_error_is_retryable source_error)
                                 ~error_code:Supervisor.error_code
                                 ~error_message:Supervisor.error_message
                                 source_error))
                   with exception_ ->
                     Error
                       (base_operation_error "async activity failure"
                          (exception_error ~path:"$.async_failure" exception_))))
          | Async_activity.Cancel details ->
              let rec convert reversed = function
                | [] -> Ok (List.rev reversed)
                | payload :: rest ->
                    let* payload =
                      protocol_payload "$.async_cancellation.details" payload
                    in
                    convert (payload :: reversed) rest
              in
              (match convert [] details with
              | Error error -> Error (protocol_error_to_base error)
              | Ok details ->
                  let completion =
                    Protocol.
                      {
                        task_token = Bytes.copy token;
                        result =
                          Cancelled
                            (cancellation_failure
                               ~details Protocol.Cancellation_requested);
                      }
                  in
                  (try
                     match
                       Supervisor.complete_async_activity adapter.supervisor
                         completion
                     with
                     | Ok () -> Ok ()
                     | Error source_error ->
                         Error
                           (base_operation_error "async activity cancellation"
                              (supervisor_error ~path:"$.async_cancellation"
                                 ~retryable:(source_error_is_retryable source_error)
                                 ~error_code:Supervisor.error_code
                                 ~error_message:Supervisor.error_message
                                 source_error))
                   with exception_ ->
                     Error
                       (base_operation_error "async activity cancellation"
                          (exception_error ~path:"$.async_cancellation" exception_))))
          | Async_activity.Heartbeat details ->
              let rec convert reversed = function
                | [] -> Ok (List.rev reversed)
                | payload :: rest ->
                    let* payload =
                      protocol_payload "$.async_heartbeat.details" payload
                    in
                    convert (payload :: reversed) rest
              in
              (match convert [] details with
              | Error error -> Error (protocol_error_to_base error)
              | Ok details ->
                  let heartbeat = Protocol.{ task_token = Bytes.copy token; details } in
                  (try
                     match
                       Supervisor.record_async_activity_heartbeat
                         adapter.supervisor heartbeat
                     with
                     | Ok () -> Ok ()
                     | Error source_error ->
                         Error
                           (base_operation_error "async activity heartbeat"
                              (supervisor_error ~path:"$.async_heartbeat"
                                 ~retryable:(source_error_is_retryable source_error)
                                 ~error_code:Supervisor.error_code
                                 ~error_message:Supervisor.error_message
                                 source_error))
                   with exception_ ->
                     Error
                       (base_operation_error "async activity heartbeat"
                          (exception_error ~path:"$.async_heartbeat" exception_))))
          with exception_ ->
            lease.in_flight <- false;
            raise exception_
        in
        lease.in_flight <- false;
        (match operation_result with
        | Ok () when terminal ->
            adapter.async_leases <- Token_map.remove token adapter.async_leases;
            Ok ()
        | Error error ->
            let view = Base_error.view error in
            if view.category = `Bridge && view.non_retryable then begin
              (* A non-retryable native result is terminal for this retained
                 capability. Remove the adapter lease before returning the
                 error; the base state machine closes its corresponding handle
                 from the same classification, so neither side can retain a
                 stale task token after Temporal has rejected it permanently. *)
              adapter.async_leases <- Token_map.remove token adapter.async_leases
            end;
            Error error
        | _ -> operation_result))

  (** Submits an already validated, owned completion and preserves lease
      uncertainty when a typed native error or an exception is returned. Local
      validation happens before admission to the pending-completion map. *)
  let attempt_completion supervisor completion =
    try
      match Supervisor.complete_activity supervisor completion with
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
      activation failure therefore leaves the accepted completion visible for
      recovery instead of orphaning its handle. *)
  let admit_async_lease adapter ~token handle =
    Mutex.lock adapter.async_mutex;
    let admission =
      Fun.protect
        ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
        (fun () ->
          if Token_map.mem token adapter.async_leases then
            Error
              (make_error ~path:"$.async_handle" "duplicate_async_lease"
                 "native accepted an asynchronous activity token twice")
          else (
            adapter.async_leases <-
              Token_map.add token
                (Async_lease { token = Bytes.copy token; handle; in_flight = false })
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

  (** Submits one pending lease and removes it only after native Core accepts
      the exact copied token. Rejections leave it in the map for the next poll.
  *)
  let finish_lease adapter lease : (outcome, error_view) result =
    match attempt_completion adapter.supervisor lease.completion with
    | Rejected_by_supervisor error -> Error error
    | Raised_by_supervisor exception_ ->
        let retryable =
          completion_exception_is_retryable exception_
        in
        Error (completion_exception_error ~retryable exception_)
    | Accepted ->
        begin match lease.accepted_result with
        | Completed_result { kind; cancellation_details } ->
            adapter.leases <- Token_map.remove lease.token adapter.leases;
            report Logs.Debug ~operation:"activity_task_completed" ();
            Ok
              (Completed
                 {
                   activity_type = lease.activity_type;
                   kind;
                   cancellation_details;
                 })
        | Rejected_result error ->
            adapter.leases <- Token_map.remove lease.token adapter.leases;
            report Logs.Warning ~operation:"activity_task_rejected"
              ~error_kind:error.code ();
            Ok
              (Rejected
                 {
                   activity_type = lease.activity_type;
                   error;
                   lease_retired = true;
                 })
        | Async_handoff handle ->
            (match admit_async_lease adapter ~token:lease.token handle with
            | Error error -> Error error
            | Ok () ->
                adapter.leases <- Token_map.remove lease.token adapter.leases;
                report Logs.Debug ~operation:"activity_async_handoff_accepted" ();
                Ok
                  (Completed
                     {
                       activity_type = lease.activity_type;
                       kind = Deferred;
                       cancellation_details = None;
                     }))
        end

  (** Validates, records, and submits one completion. Invalid application data
      becomes a bounded failure before anything is retained for transport retry.
      Recording still precedes the native call so uncertain submissions retain
      their exact completion; only a [Retryable] source classification may
      authorize resubmission, while generic transport failures remain
      fail-closed. *)
  let enqueue_and_finish adapter ~token ~activity_type ~completion
      ~accepted_result =
    let (completion : Protocol.completion) = completion in
    let token = Bytes.copy token in
    let completion =
      Protocol.{ completion with task_token = Bytes.copy completion.task_token }
    in
    let* completion, accepted_result =
      match validate_completion completion with
      | Ok () -> Ok (completion, accepted_result)
      | Error error ->
          (* No native submission has occurred. Do not retain an invalid value
             that every subsequent poll would reject, and do not admit an async
             handle when its handoff was replaced with a task failure. *)
          (match accepted_result with
          | Async_handoff handle -> ignore (Async_activity.close handle)
          | Completed_result _ | Rejected_result _ -> ());
          let fallback =
            Protocol.{ task_token = Bytes.copy token;
              result = Failed (failure_of_error error) }
          in
          (* A malformed supervisor token cannot be repaired by dropping
             application details. Validate the fallback too, before admission. *)
          let* () = validate_completion fallback in
          Ok (fallback, Rejected_result error)
    in
    let lease = { token; activity_type; completion; accepted_result } in
    match add_lease adapter lease with
    | Error error ->
        (* A duplicate token means this completion was never admitted. Close a
           reserved async handle so a callback cannot retain a capability for
           a task that the adapter rejected before contacting Core. *)
        (match accepted_result with
        | Async_handoff handle -> ignore (Async_activity.close handle)
        | Completed_result _ | Rejected_result _ -> ());
        Error error
    | Ok () -> finish_lease adapter lease

  (** Turns an adapter diagnostic into a non-retryable failure completion while
      preserving the original task token. *)
  let reject_task_with_failure adapter ~token ~activity_type ~failure error =
    let completion =
      Protocol.{ task_token = Bytes.copy token; result = Failed failure }
    in
    enqueue_and_finish adapter ~token ~activity_type ~completion
      ~accepted_result:(Rejected_result error)

  (** Builds the standard adapter failure for registry, codec, and protocol
      errors, all of which are non-retryable because retrying cannot repair the
      worker configuration or malformed task. *)
  let reject_task adapter ~token ~activity_type error =
    reject_task_with_failure adapter ~token ~activity_type
      ~failure:(failure_of_error error) error

  (** Executes an asynchronous activity callback. The handle is dormant while
      the callback runs; only an accepted [Will_complete_async] completion
      causes [finish_lease] to publish and activate it. *)
  let process_async_start adapter token definition
      (start : Protocol.activity_start) =
    let activity_type = Some start.activity_type in
    let process () =
      match decode_input definition start.input with
      | Error error -> reject_task adapter ~token ~activity_type error
      | Ok input ->
          let* _details =
            runtime_payloads "$.variant.heartbeat_details"
              start.heartbeat_details
          in
          let* _heartbeat_timeout =
            match start.heartbeat_timeout with
            | None -> Ok None
            | Some timeout ->
                Result.map (fun timeout -> Some timeout)
                  (runtime_duration "$.variant.heartbeat_timeout" timeout)
          in
          (match Definition.implementation definition with
          | None ->
              reject_task adapter ~token ~activity_type
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
                  ~submit:(submit_async_operation adapter ~token)
                  ~encode_output
              in
              let context = Async_activity.context handle in
              let context_handle = Async_activity.handle context in
              (try
                 match implementation context input with
                 | Async_activity.Completed output ->
                     (match encode_output output with
                     | Error error ->
                         reject_task adapter ~token ~activity_type
                           (application_error
                              ~path:"$.implementation.output" error)
                     | Ok payload ->
                         (match protocol_payload "$.completion.result" payload with
                         | Error error ->
                             reject_task adapter ~token ~activity_type error
                         | Ok payload ->
                             let completion =
                               Protocol.
                                 {
                                   task_token = Bytes.copy token;
                                   result = Completed (Some payload);
                                 }
                             in
                             enqueue_and_finish adapter ~token ~activity_type
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
                     | Error error -> reject_task adapter ~token ~activity_type error
                     | Ok failure ->
                         reject_task_with_failure adapter ~token ~activity_type
                           ~failure diagnostic)
                 | Async_activity.Will_complete_async handle ->
                     (match
                        Async_activity.prepare_handoff ~expected:context_handle
                          handle
                      with
                     | Error error ->
                         reject_task adapter ~token ~activity_type
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
                         enqueue_and_finish adapter ~token ~activity_type
                           ~completion ~accepted_result:(Async_handoff handle))
               with exception_ ->
                 reject_task adapter ~token ~activity_type
                   (exception_error ~path:"$.implementation" exception_)))
    in
    try process ()
    with exception_ ->
      reject_task adapter ~token ~activity_type
        (exception_error ~path:"$.implementation" exception_)

  (** Executes one start-task implementation under a final exception guard. *)
  let process_start adapter token (start : Protocol.activity_start) =
    let activity_type = Some start.activity_type in
    (* Keep all local dispatch and codec failures on the completion path. The
       outer guard catches defects so even an exception retires this lease. *)
    let process () =
      match find_definition adapter.definitions start.activity_type with
      | Error error -> reject_task adapter ~token ~activity_type error
      | Ok (Registered_async_definition definition) ->
          process_async_start adapter token definition start
      | Ok (Registered_definition definition) ->
          (match Definition.implementation definition with
          | None ->
              reject_task adapter ~token ~activity_type
                (make_error ~path:"$.variant.activity_type" "not_executable"
                   "registered activity has no local implementation")
          | Some implementation ->
              (match decode_input definition start.input with
              | Error error -> reject_task adapter ~token ~activity_type error
              | Ok input ->
                  let* details =
                    runtime_payloads "$.variant.heartbeat_details"
                      start.heartbeat_details
                  in
                  let* heartbeat_timeout =
                    match start.heartbeat_timeout with
                    | None -> Ok None
                    | Some timeout ->
                        Result.map (fun timeout -> Some timeout)
                          (runtime_duration "$.variant.heartbeat_timeout" timeout)
                  in
                  let context =
                    activity_context adapter ~token ~details ~heartbeat_timeout
                  in
                  Fun.protect
                    ~finally:(fun () -> Activity_context.invalidate context)
                    (fun () ->
                      match implementation context input with
                      | Error implementation_error ->
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
                                reject_task adapter ~token ~activity_type error
                            | Ok failure ->
                                reject_task_with_failure adapter ~token
                                  ~activity_type ~failure diagnostic
                          end
                      | Ok output ->
                          (match
                             Codec.encode (Definition.output definition) output
                           with
                          | Error error ->
                              reject_task adapter ~token ~activity_type
                                (application_error
                                   ~path:"$.implementation.output" error)
                          | Ok payload ->
                              (match
                                 protocol_payload "$.completion.result" payload
                               with
                              | Error error ->
                                  reject_task adapter ~token ~activity_type error
                              | Ok payload ->
                                  let completion =
                                    Protocol.
                                      {
                                        task_token = Bytes.copy token;
                                        result = Completed (Some payload);
                                      }
                                  in
                                  enqueue_and_finish adapter ~token
                                    ~activity_type ~completion
                                    ~accepted_result:
                                    (Completed_result
                                       {
                                         kind = Succeeded;
                                         cancellation_details = None;
                                       }))))))
    in
    try process ()
    with exception_ ->
      reject_task adapter ~token ~activity_type
        (exception_error ~path:"$.implementation" exception_)

  (** Converts a cancellation task into a canceled completion. Cancellation has
      no activity type in the native task shape, but its independent Core
      details are retained on the private outcome for instrumentation. They
      remain metadata rather than being copied into the Temporal failure. *)
  let process_cancel adapter token (cancel : Protocol.activity_cancel) =
    let completion =
      Protocol.
        {
          task_token = Bytes.copy token;
          result = Cancelled (cancellation_failure cancel.reason);
        }
    in
    enqueue_and_finish adapter ~token ~activity_type:None ~completion
      ~accepted_result:
        (Completed_result
           { kind = Cancelled; cancellation_details = cancel.details })

  (** Processes one decoded task after copying its token. The extra empty-token
      check protects the adapter if a test or future supervisor bypasses the
      strict JSON decoder. *)
  let process_task adapter (task : Protocol.task) =
    let token = Bytes.copy task.task_token in
    if Bytes.length token = 0 then
      Error
        (make_error ~path:"$.task_token" "invalid_message"
           "activity task token must not be empty")
    else
      match task.variant with
      | Protocol.Start start -> process_start adapter token start
      | Cancel cancel -> process_cancel adapter token cancel

  (** Retries every retained activity completion while the adapter mutex is
      held. Native worker shutdown calls this before closing Rust so a
      temporary completion transport failure cannot become an outstanding
      task-token lease. *)
  let drain adapter : (unit, error_view) result =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () ->
        (* Retry the smallest token first for deterministic shutdown behavior;
           stop at the first failure and retain that lease for the next drain. *)
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
                if Token_map.is_empty adapter.async_leases then Ok ()
                else
                  let error =
                    make_error ~path:"$.async_leases" ~retryable:true
                      "outstanding_async_leases"
                      "asynchronous activity completions remain admitted"
                  in
                  Error error))

  (** Drops copied activity completions after terminal native cleanup. The Rust
      runtime has already force-retired its leases, so retaining or retrying
      these tokens could duplicate a completion. The mutex keeps discard
      ordered with any final adapter operation. *)
  let discard adapter =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () ->
        adapter.leases <- Token_map.empty;
        Mutex.lock adapter.async_mutex;
        Fun.protect
          ~finally:(fun () -> Mutex.unlock adapter.async_mutex)
          (fun () ->
            Token_map.iter
              (fun _ (Async_lease lease) ->
                ignore (Async_activity.close lease.handle))
              adapter.async_leases;
            adapter.async_leases <- Token_map.empty))

  (** Serializes pending-completion retry, native polling, implementation
      execution, and completion submission. The mutex covers the complete
      transaction, including the user implementation, so the map cannot race
      with another poll and no token can be dispatched twice. *)
  let poll adapter =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () ->
        match Token_map.min_binding_opt adapter.leases with
        | Some (_, lease) -> finish_lease adapter lease
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
            | Ok (Some task) -> process_task adapter task))
end

(** Hides the existential constructor from callers while retaining the shared
    [Definition.t] representation used by public activity definitions. *)
let register definition = Activity definition

(** Packs an asynchronous definition while keeping its output type paired with
    the callback and codec. *)
let register_async definition = Async_activity definition

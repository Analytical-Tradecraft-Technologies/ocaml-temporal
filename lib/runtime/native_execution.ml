(** The checked, pure-OCaml half of the workflow activation boundary.

    Rust owns Core's protobuf and network machinery. This module intentionally
    stays below the supervisor: it validates a semantic activation, converts its
    payloads into the deterministic runtime representation, runs an existing
    [Execution.t], and converts only exactly representable commands back to the
    semantic protocol. In particular, it never invents activity timeouts, task
    queues, or child-workflow options that are absent from the semantic
    protocol merely to make a command fit an older protocol shape. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Failure_diagnostic = Temporal_protocol.Failure_diagnostic

(** Result-bind notation keeps every boundary conversion on the typed error
    path; no protocol or runtime input is handled with an exception. *)
let ( let* ) = Result.bind

type error = { code : string; path : string; message : string }
(** Translation errors are immutable and contain no payload bytes. The private
    representation lets us change internal constructors without changing the
    diagnostics consumed by a worker loop. *)

(** Publicly safe projection of [error]. Keeping this record separate from the
    private error value prevents callers from depending on internal
    construction details while allowing stable logging and assertions. *)
type error_view = { code : string; path : string; message : string }

type initialization = {
  workflow_id : string;
  workflow_type : string;
  arguments : Protocol.payload list;
  randomness_seed : string;
  attempt : int;
  context : Protocol.initialize_context option;
}
(** Initialization data retained while the runtime uses a smaller start marker.
*)

type cache_removal = { message : string; reason : Protocol.eviction_reason }
(** Cache-removal facts retained next to the runtime eviction marker. *)

type translated_activation = {
  run_id : string;
  timestamp : Protocol.timestamp option;
  is_replaying : bool;
  history_length : int64;
  metadata : Protocol.activation_metadata option;
  initialization : initialization option;
  cancellation_reason : string option;
  cache_removal : cache_removal option;
  jobs : Activation.job list;
}
(** A protocol activation after its jobs have been converted to runtime jobs. *)

(** Copies a translation error into its stable public diagnostic shape. *)
let error_view (error : error) : error_view =
  { code = error.code; path = error.path; message = error.message }

(** Creates one bounded diagnostic. Callers pass only constant or already
    validated descriptions; source payload bytes never enter the message. *)
let make_error code path message : error = { code; path; message }

(** Errors caused by a malformed value constructed by the caller or decoded by a
    peer. The protocol validator supplies the same code for JSON failures. *)
let invalid path message = make_error "invalid_message" path message

(** Errors caused by a value that is valid in one layer but has no exact
    representation in the other layer. *)
let unsupported path message = make_error "unsupported" path message

(** Converts the protocol module's privacy-safe diagnostic without exposing any
    of its internal error representation. *)
let protocol_error error =
  let view = Protocol.error_view error in
  make_error view.code view.path view.message

(** Keeps sequence conversion explicit. Core uses an unsigned 32-bit sequence
    number while the OCaml runtime uses [int64] to avoid platform narrowing. *)
let validate_sequence path sequence : (unit, error) result =
  if Int64.compare sequence 0L < 0 then
    Error (invalid path "sequence number must not be negative")
  else if Int64.compare sequence 4_294_967_295L > 0 then
    Error (invalid path "sequence number is outside unsigned 32-bit range")
  else Ok ()

(** Identifiers in runtime commands are checked here as well as by the protocol
    encoder, because callers may invoke [command_to_protocol] directly without
    first constructing a completion. *)
let validate_identifier path value : (unit, error) result =
  if String.length value = 0 then
    Error (invalid path "identifier must not be empty")
  else if String.contains value '\000' then
    Error (invalid path "identifier must not contain NUL")
  else if String.length value > 65_536 then
    Error (invalid path "identifier exceeds 65536 bytes")
  else if not (Temporal_base.Codec.valid_utf_8 value) then
    Error (invalid path "identifier must be valid UTF-8")
  else Ok ()

(** Validates a bounded text field whose empty value is meaningful.  Signal
    identities are supplied by Core as text but are not Temporal identifiers;
    keeping this check local to the runtime prevents malformed bytes assembled
    by an OCaml caller from being retained in a runtime job if the protocol
    encoder is ever bypassed or extended. *)
let validate_bounded_text path value : (unit, error) result =
  if String.length value > 65_536 then
    Error (invalid path "text exceeds 65536 bytes")
  else if String.contains value '\000' then
    Error (invalid path "text must not contain NUL")
  else if not (Temporal_base.Codec.valid_utf_8 value) then
    Error (invalid path "text must be valid UTF-8")
  else Ok ()

(** Applies the stricter text contract used for cancellation reasons.  Reasons
    are stored in Temporal history, so accepting malformed or empty text here
    would make replay and the Rust boundary disagree about one command. *)
let validate_cancellation_reason path value : (unit, error) result =
  if String.length value = 0 then
    Error (invalid path "reason must not be empty")
  else if String.contains value '\000' then
    Error (invalid path "reason must not contain NUL")
  else if String.length value > 65_536 then
    Error (invalid path "reason exceeds 65536 bytes")
  else if not (Temporal_base.Codec.valid_utf_8 value) then
    Error (invalid path "reason must be valid UTF-8")
  else Ok ()

(** Bounds human-readable failure diagnostics before they are copied into
    protocol string fields. Runtime errors can include application text or
    server-supplied identifiers that were valid by themselves; this helper keeps
    the combined message within the protocol encoder's 65,536-byte string limit
    without splitting a UTF-8 code point. *)
let bounded_protocol_message value =
  let maximum = 65_536 in
  let fallback = "invalid workflow failure diagnostic" in
  if not (Temporal_base.Codec.valid_utf_8 value) then fallback
  else if String.length value <= maximum then value
  else
    let rec prefix length =
      if length <= 0 then fallback
      else
        let candidate = String.sub value 0 length in
        if Temporal_base.Codec.valid_utf_8 candidate then candidate ^ "..."
        else prefix (length - 1)
    in
    prefix (maximum - 3)

(** Copies a protocol payload before retaining it in translated activation
    metadata. Protocol payload bodies and metadata values are mutable [bytes],
    so retaining the caller's record directly would let a later mutation alter
    replay inputs after validation. *)
let copy_protocol_payload (value : Protocol.payload) : Protocol.payload =
  Protocol.
    {
      metadata =
        List.map (fun (key, bytes) -> (key, Bytes.copy bytes)) value.metadata;
      data = Bytes.copy value.data;
    }

(** Copies payload lists while retaining their order. *)
let copy_payloads values = List.map copy_protocol_payload values

(** Deep-copies a protocol failure so continuation metadata cannot retain
    caller-owned payload bytes through encoded attributes, details, or causes. *)
let rec copy_failure (value : Protocol.failure) : Protocol.failure =
  let info =
    match value.info with
    | Protocol.Application ({ details; _ } as info) ->
        Protocol.Application { info with details = copy_payloads details }
    | Protocol.Canceled ({ details; _ } as info) ->
        Protocol.Canceled { info with details = copy_payloads details }
    | Protocol.Timeout_failure ({ last_heartbeat_details; _ } as info) ->
        Protocol.Timeout_failure
          { info with last_heartbeat_details = copy_payloads last_heartbeat_details }
    | Protocol.Activity _ as info -> info
    | Protocol.Child_workflow _ as info -> info
  in
  Protocol.
    {
      value with
      encoded_attributes = Option.map copy_protocol_payload value.encoded_attributes;
      cause = Option.map copy_failure value.cause;
      info;
    }

(** Copies continuation metadata, including optional failure and completion
    payloads, before it is retained by the runtime execution. *)
let copy_continuation (value : Protocol.continuation) : Protocol.continuation =
  Protocol.
    {
      value with
      continued_failure = Option.map copy_failure value.continued_failure;
      last_completion_result = Option.map copy_payloads value.last_completion_result;
    }

(** Copies every payload-bearing part of initialization context. The remaining
    context fields, including the immutable retry policy, are immutable
    strings, options, integers, and small records, so a record copy is
    sufficient for them. *)
let copy_initialize_context (value : Protocol.initialize_context) =
  Protocol.
    {
      value with
      headers =
        List.map
          (fun (key, payload) -> (key, copy_protocol_payload payload))
          value.headers;
      memo = Option.map (List.map (fun (key, payload) ->
        (key, copy_protocol_payload payload))) value.memo;
      search_attributes = Option.map (List.map (fun (key, payload) ->
        (key, copy_protocol_payload payload))) value.search_attributes;
      continuation = Option.map copy_continuation value.continuation;
    }

(** Converts one protocol payload into the runtime payload type. Runtime codecs
    intentionally expose metadata values as strings; a binary metadata value
    therefore cannot be represented losslessly and is reported as an explicit
    unsupported boundary instead of being silently decoded with a replacement
    character. *)
let runtime_payload path (value : Protocol.payload) =
  let rec metadata_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | (key, bytes) :: rest ->
        let* () = validate_identifier (path ^ ".metadata.key") key in
        let value = Bytes.to_string bytes in
        if not (Temporal_base.Codec.valid_utf_8 value) then
          Error
            (unsupported
               (path ^ ".metadata." ^ key)
               "binary metadata is not representable by the runtime string \
                metadata type")
        else metadata_loop ((key, value) :: reversed) rest
  in
  let* metadata = metadata_loop [] value.metadata in
  Ok { Temporal_base.Payload.metadata; data = Bytes.copy value.data }

(** Converts a runtime payload back to the protocol's binary-safe payload.
    Copying both metadata bytes and the body makes ownership independent of the
    mutable [bytes] values held by workflow code. *)
let protocol_payload path (value : Temporal_base.Codec.payload) =
  let rec metadata_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | (key, text) :: rest ->
        let* () = validate_identifier (path ^ ".metadata.key") key in
        if not (Temporal_base.Codec.valid_utf_8 text) then
          Error
            (invalid (path ^ ".metadata." ^ key) "metadata must be valid UTF-8")
        else metadata_loop ((key, Bytes.of_string text) :: reversed) rest
  in
  let* metadata = metadata_loop [] value.metadata in
  Ok { Protocol.metadata; data = Bytes.copy value.data }

(** Canonical payload used when Core reports a completed operation without a
    result. It is the same representation produced by [Codec.unit] and keeps the
    optional protocol field's [None] semantics deterministic. *)
let null_runtime_payload : Temporal_base.Codec.payload =
  {
    Temporal_base.Payload.metadata = [ ("encoding", "binary/null") ];
    data = Bytes.empty;
  }

(** Recognizes the same canonical null marker after conversion to protocol
    payload bytes. *)
let is_null_protocol_payload (value : Protocol.payload) =
  Bytes.length value.data = 0
  &&
  match value.metadata with
  | [ ("encoding", bytes) ] -> Bytes.equal bytes (Bytes.of_string "binary/null")
  | _ -> false

(** Collects application and cancellation details through an activity wrapper.
    Temporal commonly reports an activity failure as an outer [Activity] record
    whose [cause] contains the application failure and its payloads. The bounded
    walk preserves details from every such layer without allowing a malformed
    recursive value to consume unbounded stack or memory. *)
let failure_details (failure : Protocol.failure) =
  let rec loop depth reversed (value : Protocol.failure) =
    let reversed =
      match value.info with
      | Protocol.Application { details; _ } | Protocol.Canceled { details; _ }
        ->
          List.rev_append details reversed
      | Protocol.Timeout_failure { last_heartbeat_details; _ } ->
          List.rev_append last_heartbeat_details reversed
      | Protocol.Activity _ | Protocol.Child_workflow _ -> reversed
    in
    match value.cause with
    | Some cause when depth < 128 -> loop (depth + 1) reversed cause
    | None | Some _ -> List.rev reversed
  in
  loop 0 [] failure

(** Converts one protocol failure to the broad typed error used by workflow
    futures. The error view retains the primary category and details, while the
    protocol's richer recursive fields are included in its diagnostic. *)
let runtime_error_of_failure path ~category (failure : Protocol.failure) =
  let rec details_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | payload :: rest ->
        let* payload = runtime_payload (path ^ ".details") payload in
        details_loop (payload :: reversed) rest
  in
  let details = failure_details failure in
  let* details = details_loop [] details in
  let non_retryable = Protocol.failure_non_retryable failure in
  Ok
    (Temporal_base.Error.make ~non_retryable ~details ~category
       ~message:(Failure_diagnostic.failure_diagnostic failure)
       ())

(** Converts a protocol activity result while preserving the distinction between
    a successful empty result and a failure. *)
let runtime_activity_result path result =
  match result with
  | Protocol.Completed None -> Ok (Ok null_runtime_payload)
  | Protocol.Completed (Some payload) ->
      let* payload = runtime_payload (path ^ ".payload") payload in
      Ok (Ok payload)
  | Protocol.Failed failure ->
      let* error =
        runtime_error_of_failure (path ^ ".failure") ~category:`Activity failure
      in
      Ok (Error error)
  | Protocol.Cancelled failure ->
      let* error =
        runtime_error_of_failure (path ^ ".failure") ~category:`Cancelled
          failure
      in
      Ok (Error error)
  | Protocol.Backoff _ ->
      Error (invalid path "local activity backoff must be handled as a retry job")

(** Converts a Core retry delay to the runtime's millisecond timer unit. Core
    currently reports retry delays at millisecond precision, but rounding any
    future sub-millisecond value upward avoids firing the retry before Core's
    requested deadline. *)
let milliseconds_of_duration path (duration : Protocol.duration) =
  if Int64.compare duration.seconds 0L < 0 then
    Error (invalid path "duration must not be negative")
  else if duration.nanoseconds < 0 || duration.nanoseconds >= 1_000_000_000 then
    Error (invalid path "duration nanoseconds are outside protobuf range")
  else if Int64.compare duration.seconds (Int64.div Int64.max_int 1_000L) > 0 then
    Error (invalid path "duration exceeds runtime timer range")
  else
    let seconds_ms = Int64.mul duration.seconds 1_000L in
    let fractional_ms =
      Int64.of_int ((duration.nanoseconds + 999_999) / 1_000_000)
    in
    if Int64.compare seconds_ms (Int64.sub Int64.max_int fractional_ms) > 0 then
      Error (invalid path "duration exceeds runtime timer range")
    else Ok (Int64.add seconds_ms fractional_ms)

(** Converts the child-start acknowledgment. A successful run ID advances the
    per-sequence lifecycle without completing the child future; start failures
    become typed child/cancellation errors so no child remains indefinitely
    pending. *)
let runtime_child_workflow_start_result path result =
  match result with
  | Protocol.Child_start_succeeded run_id ->
      let* () = validate_identifier (path ^ ".run_id") run_id in
      Ok (Ok run_id)
  | Protocol.Child_start_failed { workflow_id; workflow_type; cause } ->
      let cause =
        match cause with
        | Protocol.Child_start_unspecified -> "unspecified"
        | Child_start_workflow_already_exists -> "workflow_already_exists"
      in
      Ok
        (Error
           (Temporal_base.Error.make ~non_retryable:true
              ~category:`Child_workflow
              ~message:
                (Printf.sprintf
                   "child workflow start failed: id=%s type=%s cause=%s"
                   workflow_id workflow_type cause)
              ()))
  | Protocol.Child_start_cancelled failure ->
      let* error =
        runtime_error_of_failure (path ^ ".failure") ~category:`Cancelled
          failure
      in
      Ok (Error error)

(** Converts the terminal child result while retaining the same null-payload
    convention as activity completion. *)
let runtime_child_workflow_result path result =
  match result with
  | Protocol.Child_completed None -> Ok (Ok null_runtime_payload)
  | Protocol.Child_completed (Some payload) ->
      let* payload = runtime_payload (path ^ ".payload") payload in
      Ok (Ok payload)
  | Protocol.Child_failed failure ->
      let* error =
        runtime_error_of_failure (path ^ ".failure") ~category:`Child_workflow
          failure
      in
      Ok (Error error)
  | Protocol.Child_cancelled failure ->
      let* error =
        runtime_error_of_failure (path ^ ".failure") ~category:`Cancelled
          failure
      in
      Ok (Error error)

(** Converts an external workflow acknowledgement. Core's absent failure is a
    successful delivery; a populated failure is returned as a typed workflow
    error so user code can decide whether to retry or compensate. *)
let runtime_external_workflow_result path result =
  match result with
  | Ok () -> Ok (Ok ())
  | Error failure ->
      let* error =
        runtime_error_of_failure (path ^ ".failure") ~category:`Workflow failure
      in
      Ok (Error error)

(** Operation families that may consume a Core sequence number. A child
    workflow intentionally has two entries—start acknowledgement and terminal
    result—while all other families must appear at most once per activation. *)
type sequence_kind =
  | Activity
  | Child_start
  | Child_result
  | External_signal
  | External_cancel
  | Timer

(** Converts an activation job and checks its sequence number before any
    mutable execution state is touched. The optional tuple fields retain
    initialization, cancellation, and eviction facts that the compact runtime
    job algebra cannot carry directly. *)
let runtime_job path = function
  | Protocol.Initialize_workflow
      {
        workflow_id;
        workflow_type;
        arguments;
        randomness_seed;
        attempt;
        context;
      } ->
      let* () = validate_identifier (path ^ ".workflow_id") workflow_id in
      let* () = validate_identifier (path ^ ".workflow_type") workflow_type in
      if attempt < 1 then
        Error (invalid (path ^ ".attempt") "attempt must be positive")
      else
        Ok
          ( Activation.Start_workflow,
            Some
              {
                workflow_id;
                workflow_type;
                arguments = List.map copy_protocol_payload arguments;
                randomness_seed;
                attempt;
                context = Option.map copy_initialize_context context;
              },
            None,
            None,
            None )
  | Protocol.Resolve_activity { seq; result } ->
      let* () = validate_sequence (path ^ ".seq") seq in
      begin match result with
      | Protocol.Backoff { attempt; backoff_duration; original_schedule_time } ->
          if Int64.compare attempt 0L <= 0 then
            Error (invalid (path ^ ".result.attempt") "local activity backoff attempt must be positive")
          else
            let* backoff_milliseconds =
              milliseconds_of_duration (path ^ ".result.backoff_duration") backoff_duration
            in
            Ok
              ( Activation.Resolve_local_activity_backoff
                  { seq; attempt; backoff_milliseconds; original_schedule_time },
                None, None, None, Some (Activity, seq) )
      | (Protocol.Completed _ | Protocol.Failed _ | Protocol.Cancelled _) as result ->
          let* result = runtime_activity_result (path ^ ".result") result in
          Ok
            ( Activation.Resolve_activity { seq; result }, None, None, None,
              Some (Activity, seq) )
      end
  | Protocol.Resolve_child_workflow_start { seq; result } ->
      let* () = validate_sequence (path ^ ".seq") seq in
      let* result =
        runtime_child_workflow_start_result (path ^ ".result") result
      in
      Ok
        ( Activation.Resolve_child_workflow_start { seq; result }, None, None,
          None, Some (Child_start, seq) )
  | Protocol.Resolve_child_workflow { seq; result } ->
      let* () = validate_sequence (path ^ ".seq") seq in
      let* result = runtime_child_workflow_result (path ^ ".result") result in
      Ok
        ( Activation.Resolve_child_workflow { seq; result }, None, None, None,
          Some (Child_result, seq) )
  | Protocol.Resolve_signal_external_workflow { seq; result } ->
      let* () = validate_sequence (path ^ ".seq") seq in
      let* result = runtime_external_workflow_result (path ^ ".result") result in
      Ok
        ( Activation.Resolve_signal_external_workflow { seq; result }, None, None,
          None, Some (External_signal, seq) )
  | Protocol.Resolve_request_cancel_external_workflow { seq; result } ->
      let* () = validate_sequence (path ^ ".seq") seq in
      let* result = runtime_external_workflow_result (path ^ ".result") result in
      Ok
        ( Activation.Resolve_request_cancel_external_workflow { seq; result }, None,
          None, None, Some (External_cancel, seq) )
  | Protocol.Query_workflow { query_id; query_type; arguments; headers } ->
      (* Query payloads are copied into runtime-owned values, retaining every
         repeated argument and sorted header. The public adapter may reject
         arguments later, but this layer never drops them silently. *)
      let* () = validate_identifier (path ^ ".query_id") query_id in
      let* () = validate_identifier (path ^ ".query_type") query_type in
      let rec payloads_loop index reversed = function
        | [] -> Ok (List.rev reversed)
        | payload :: rest ->
            let* payload =
              runtime_payload
                (Printf.sprintf "%s.arguments[%d]" path index)
                payload
            in
            payloads_loop (index + 1) (payload :: reversed) rest
      in
      let rec headers_loop reversed = function
        | [] -> Ok (List.rev reversed)
        | (key, payload) :: rest ->
            let* () = validate_identifier (path ^ ".headers.key") key in
            let* payload = runtime_payload (path ^ ".headers." ^ key) payload in
            headers_loop ((key, payload) :: reversed) rest
      in
      let* arguments = payloads_loop 0 [] arguments in
      let* headers = headers_loop [] headers in
      Ok (Activation.Query_workflow { query_id; query_type; arguments; headers }, None,
          None, None, None)
  | Protocol.Do_update
      {
        id;
        protocol_instance_id;
        name;
        input;
        headers;
        meta;
        run_validator;
      } ->
      let* () = validate_identifier (path ^ ".id") id in
      let* () =
        validate_identifier (path ^ ".protocol_instance_id") protocol_instance_id
      in
      let* () = validate_identifier (path ^ ".name") name in
      let* () = validate_identifier (path ^ ".meta.update_id") meta.update_id in
      let* () = validate_bounded_text (path ^ ".meta.identity") meta.identity in
      if not (String.equal id meta.update_id) then
        Error (invalid (path ^ ".meta.update_id") "must match update id")
      else
        let rec payloads_loop index reversed = function
          | [] -> Ok (List.rev reversed)
          | payload :: rest ->
              let* payload =
                runtime_payload
                  (Printf.sprintf "%s.input[%d]" path index)
                  payload
              in
              payloads_loop (index + 1) (payload :: reversed) rest
        in
        let rec headers_loop reversed = function
          | [] -> Ok (List.rev reversed)
          | (key, payload) :: rest ->
              let* () = validate_identifier (path ^ ".headers.key") key in
              let* payload = runtime_payload (path ^ ".headers." ^ key) payload in
              headers_loop ((key, payload) :: reversed) rest
        in
        let* input = payloads_loop 0 [] input in
        let* headers = headers_loop [] headers in
        Ok
          ( Activation.Do_update
              {
                id;
                protocol_instance_id;
                name;
                input;
                headers;
                identity = meta.identity;
                update_id = meta.update_id;
                run_validator;
              },
            None,
            None,
            None,
            None )
  | Protocol.Signal_workflow { signal_name; input; identity; headers } ->
      let* () = validate_identifier (path ^ ".signal_name") signal_name in
      let* () = validate_bounded_text (path ^ ".identity") identity in
      let rec payloads_loop index reversed = function
        | [] -> Ok (List.rev reversed)
        | payload :: rest ->
            let* payload =
              runtime_payload
                (Printf.sprintf "%s.input[%d]" path index)
                payload
            in
            payloads_loop (index + 1) (payload :: reversed) rest
      in
      let rec headers_loop reversed = function
        | [] -> Ok (List.rev reversed)
        | (key, payload) :: rest ->
            let* () = validate_identifier (path ^ ".headers.key") key in
            let* payload = runtime_payload (path ^ ".headers." ^ key) payload in
            headers_loop ((key, payload) :: reversed) rest
      in
      let* input = payloads_loop 0 [] input in
      let* headers = headers_loop [] headers in
      Ok
        ( Activation.Signal_workflow { signal_name; input; identity; headers },
          None,
          None,
          None,
          None )
  | Protocol.Notify_has_patch { patch_id } ->
      let* () = validate_identifier (path ^ ".patch_id") patch_id in
      Ok
        ( Activation.Notify_has_patch { patch_id },
          None,
          None,
          None,
          None )
  | Protocol.Update_random_seed { randomness_seed } ->
      Ok
        ( Activation.Update_random_seed { randomness_seed },
          None,
          None,
          None,
          None )
  | Protocol.Fire_timer { seq } ->
      let* () = validate_sequence (path ^ ".seq") seq in
      Ok
        (Activation.Fire_timer { seq }, None, None, None, Some (Timer, seq))
  | Protocol.Cancel_workflow { reason } ->
      Ok (Activation.Cancel_workflow, None, Some reason, None, None)
  | Protocol.Remove_from_cache { message; reason } ->
      Ok
        ( Activation.Remove_from_cache,
          None,
          None,
          Some { message; reason },
          None )

(** Validates one activation through the canonical semantic codec. This keeps
    direct OCaml construction subject to the same closed-object, payload-size,
    timestamp, and ordering rules as Rust input. *)
let validate_activation value =
  match Protocol.encode_activation value with
  | Ok _ -> Ok ()
  | Error error -> Error (protocol_error error)

(** Translates the entire activation while preserving source order and all
    protocol metadata that the runtime's smaller algebra cannot carry. *)
let translate_activation (value : Protocol.activation) =
  let* () = validate_activation value in
  (* Core normally allocates one sequence namespace for all commands.  Child
     resolution is the deliberate exception: the same sequence appears once
     for the start acknowledgment and once for the terminal result.  Keep the
     accepted kinds per sequence so every other cross-kind collision remains a
     protocol error. *)
  let seen_sequences = Hashtbl.create (List.length value.jobs) in
  let initialization = ref None in
  let cancellation_reason = ref None in
  let cache_removal = ref None in
  let rec loop index reversed = function
    | [] ->
        Ok
          {
            run_id = value.run_id;
            timestamp = value.timestamp;
            is_replaying = value.is_replaying;
            history_length = value.history_length;
            metadata = value.metadata;
            initialization = !initialization;
            cancellation_reason = !cancellation_reason;
            cache_removal = !cache_removal;
            jobs = List.rev reversed;
          }
    | job :: rest ->
        let path = Printf.sprintf "$.jobs[%d]" index in
        let* runtime, init, cancel, eviction, sequence = runtime_job path job in
        let* () =
          match sequence with
          | None -> Ok ()
          | Some (kind, sequence) ->
              let previous =
                match Hashtbl.find_opt seen_sequences sequence with
                | None -> []
                | Some kinds -> kinds
              in
              let child_pair_allowed =
                match (kind, previous) with
                | Child_start, [ Child_result ]
                | Child_result, [ Child_start ] -> true
                | _ -> false
              in
              if previous <> [] && not child_pair_allowed then
                Error
                  (invalid (path ^ ".seq")
                     "duplicate activation sequence for a different operation")
              else if List.mem kind previous then
                Error
                  (invalid (path ^ ".seq")
                     "duplicate activation sequence for the same operation kind")
              else (
                Hashtbl.replace seen_sequences sequence (kind :: previous);
                Ok ())
        in
        begin match init with
        | None -> ()
        | Some value -> initialization := Some value
        end;
        begin match cancel with
        | None -> ()
        | Some value -> cancellation_reason := Some value
        end;
        begin match eviction with
        | None -> ()
        | Some value -> cache_removal := Some value
        end;
        loop (index + 1) (runtime :: reversed) rest
  in
  loop 0 [] value.jobs

(** Projects the checked translation to only its runtime jobs. *)
let activation_jobs value =
  Result.map (fun translated -> translated.jobs) (translate_activation value)

(** Converts milliseconds into the normalized nonnegative protocol duration
    without floating-point rounding. *)
let duration_of_milliseconds path milliseconds =
  if Int64.compare milliseconds 0L < 0 then
    Error (invalid path "timer duration must not be negative")
  else
    let seconds = Int64.div milliseconds 1_000L in
    let remainder = Int64.rem milliseconds 1_000L in
    let nanoseconds = Int64.to_int (Int64.mul remainder 1_000_000L) in
    Ok Protocol.{ seconds; nanoseconds }

(** Converts an optional runtime timeout without losing the distinction between
    an omitted policy and an exact zero duration. *)
let optional_duration_of_milliseconds path = function
  | None -> Ok None
  | Some milliseconds ->
      let* duration = duration_of_milliseconds path milliseconds in
      Ok (Some duration)

(** Maps the runtime cancellation policy to the checked semantic protocol. *)
let protocol_cancellation_type = function
  | Activation.Try_cancel -> Protocol.Try_cancel
  | Activation.Wait_cancellation_completed -> Protocol.Wait_cancellation_completed
  | Activation.Abandon -> Protocol.Abandon

(** Maps the runtime child policy to the semantic protocol without allowing the
    activity-only policy type to leak across the module boundary. *)
let protocol_child_cancellation_type = function
  | Activation.Child_try_cancel -> Protocol.Child_try_cancel
  | Activation.Child_wait_cancellation_completed ->
      Protocol.Child_wait_cancellation_completed
  | Activation.Child_abandon -> Protocol.Child_abandon
  | Activation.Child_wait_cancellation_requested ->
      Protocol.Child_wait_cancellation_requested

(** Converts the broad runtime error into the protocol's structured failure
    shape. Category and retryability remain explicit in the application-info
    variant, while details are copied as binary-safe protocol payloads. *)
let protocol_failure path (error : Temporal_base.Error.t) =
  let view = Temporal_base.Error.view error in
  let rec details_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | payload :: rest ->
        let* payload = protocol_payload (path ^ ".details") payload in
        details_loop (payload :: reversed) rest
  in
  let* details = details_loop [] view.details in
  let failure_info =
    match view.category with
    | `Cancelled -> Protocol.Canceled { details; identity = "ocaml" }
    | _ ->
        Protocol.Application
          {
            type_name = Temporal_base.Error.kind error;
            non_retryable = view.non_retryable;
            details;
          }
  in
  Ok
    Protocol.
      {
        message = bounded_protocol_message view.message;
        source = "ocaml";
        stack_trace = "";
        encoded_attributes = None;
        cause = None;
        info = failure_info;
      }

(** Converts one command without fabricating fields. Activity commands already
    carry all Core-required identifiers, arguments, timeout policies, and
    cancellation options; the final semantic encoder below performs a second
    closed-object and range check before the command reaches Rust. *)
let command_to_protocol command =
  match command with
  | Activation.Schedule_activity
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
      } ->
      let* () = validate_sequence "$.command.seq" seq in
      let* () = validate_identifier "$.command.activity_id" activity_id in
      let* () = validate_identifier "$.command.activity_type" activity_type in
      let* () = validate_identifier "$.command.task_queue" task_queue in
      let rec arguments_loop reversed index = function
        | [] -> Ok (List.rev reversed)
        | payload :: rest ->
            let* payload =
              protocol_payload
                (Printf.sprintf "$.command.arguments[%d]" index)
                payload
            in
            arguments_loop (payload :: reversed) (index + 1) rest
      in
      let* arguments = arguments_loop [] 0 arguments in
      let* schedule_to_close_timeout =
        optional_duration_of_milliseconds
          "$.command.schedule_to_close_timeout" schedule_to_close_timeout
      in
      let* schedule_to_start_timeout =
        optional_duration_of_milliseconds
          "$.command.schedule_to_start_timeout" schedule_to_start_timeout
      in
      let* start_to_close_timeout =
        optional_duration_of_milliseconds
          "$.command.start_to_close_timeout" start_to_close_timeout
      in
      let* heartbeat_timeout =
        optional_duration_of_milliseconds "$.command.heartbeat_timeout"
          heartbeat_timeout
      in
      let* retry_policy =
        match retry_policy with
        | None -> Ok None
        | Some value ->
            let* initial_interval =
              duration_of_milliseconds
                "$.command.retry_policy.initial_interval"
                value.initial_interval
            in
            let* maximum_interval =
              duration_of_milliseconds
                "$.command.retry_policy.maximum_interval"
                value.maximum_interval
            in
            let policy =
              Protocol.
                {
                  initial_interval;
                  backoff_coefficient_bits = value.backoff_coefficient_bits;
                  maximum_interval;
                  maximum_attempts = value.maximum_attempts;
                  non_retryable_error_types =
                    List.map Fun.id value.non_retryable_error_types;
                }
            in
            let* () =
              Protocol.validate_retry_policy policy
              |> Result.map_error protocol_error
            in
            Ok (Some policy)
      in
      let priority =
        Option.map
          (fun value ->
            Protocol.
              {
                priority_key = Int32.to_int (Temporal_base.Priority.priority_key value);
                fairness_key = Temporal_base.Priority.fairness_key value;
                fairness_weight_bits =
                  Int64.of_int32
                    (Temporal_base.Priority.fairness_weight_bits value);
              })
          priority
      in
      if Option.is_none schedule_to_close_timeout
         && Option.is_none start_to_close_timeout
      then
        Error
          (invalid "$.command"
             "activity requires schedule-to-close or start-to-close timeout")
      else
        Ok
          (Protocol.Schedule_activity
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
               cancellation_type = protocol_cancellation_type cancellation_type;
               do_not_eagerly_execute;
             })
  | Activation.Schedule_local_activity
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
      } ->
      let* () = validate_sequence "$.command.seq" seq in
      let* () = validate_identifier "$.command.activity_id" activity_id in
      let* () = validate_identifier "$.command.activity_type" activity_type in
      let* arguments =
        let rec loop index reversed = function
          | [] -> Ok (List.rev reversed)
          | payload :: rest ->
              let* payload =
                protocol_payload
                  (Printf.sprintf "$.command.arguments[%d]" index)
                  payload
              in
              loop (index + 1) (payload :: reversed) rest
        in
        loop 0 [] arguments
      in
      let* schedule_to_close_timeout =
        optional_duration_of_milliseconds
          "$.command.schedule_to_close_timeout" schedule_to_close_timeout
      in
      let* schedule_to_start_timeout =
        optional_duration_of_milliseconds
          "$.command.schedule_to_start_timeout" schedule_to_start_timeout
      in
      let* start_to_close_timeout =
        optional_duration_of_milliseconds
          "$.command.start_to_close_timeout" start_to_close_timeout
      in
      let* local_retry_threshold =
        optional_duration_of_milliseconds
          "$.command.local_retry_threshold" local_retry_threshold
      in
      let* retry_policy =
        match retry_policy with
        | None -> Ok None
        | Some value ->
            let* initial_interval =
              duration_of_milliseconds
                "$.command.retry_policy.initial_interval" value.initial_interval
            in
            let* maximum_interval =
              duration_of_milliseconds
                "$.command.retry_policy.maximum_interval" value.maximum_interval
            in
            let policy =
              Protocol.
                {
                  initial_interval;
                  backoff_coefficient_bits = value.backoff_coefficient_bits;
                  maximum_interval;
                  maximum_attempts = value.maximum_attempts;
                  non_retryable_error_types = List.map Fun.id value.non_retryable_error_types;
                }
            in
            let* () = Protocol.validate_retry_policy policy |> Result.map_error protocol_error in
            Ok (Some policy)
      in
      let original_schedule_time =
        match original_schedule_time with
        | None -> None
        | Some value ->
            Some
              ({ Protocol.seconds = value.seconds; nanoseconds = value.nanoseconds }
                : Protocol.timestamp)
      in
      if Int64.compare attempt 0L <= 0 then
        Error (invalid "$.command.attempt" "local activity attempt must be positive")
      else if Option.is_none schedule_to_close_timeout
              && Option.is_none start_to_close_timeout
      then
        Error
          (invalid "$.command"
             "local activity requires schedule-to-close or start-to-close timeout")
      else
        Ok
          (Protocol.Schedule_local_activity
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
               cancellation_type = protocol_cancellation_type cancellation_type;
             })
  | Activation.Start_child_workflow
      { seq; id; name; input; retry_policy; cancellation_type } ->
      let* () = validate_sequence "$.command.seq" seq in
      let* () = validate_identifier "$.command.id" id in
      let* () = validate_identifier "$.command.name" name in
      let* input = protocol_payload "$.command.input" input in
      let* retry_policy =
        match retry_policy with
        | None -> Ok None
        | Some value ->
            let* initial_interval =
              duration_of_milliseconds
                "$.command.retry_policy.initial_interval"
                value.initial_interval
            in
            let* maximum_interval =
              duration_of_milliseconds
                "$.command.retry_policy.maximum_interval"
                value.maximum_interval
            in
            let policy =
              Protocol.
                {
                  initial_interval;
                  backoff_coefficient_bits = value.backoff_coefficient_bits;
                  maximum_interval;
                  maximum_attempts = value.maximum_attempts;
                  non_retryable_error_types =
                    List.map Fun.id value.non_retryable_error_types;
                }
            in
            let* () =
              Protocol.validate_retry_policy policy
              |> Result.map_error protocol_error
            in
            Ok (Some policy)
      in
      Ok
        (Protocol.Start_child_workflow
           {
             seq;
             workflow_id = id;
             workflow_type = name;
             input = [ input ];
             retry_policy;
             cancellation_type = protocol_child_cancellation_type cancellation_type;
           })
  | Activation.Cancel_child_workflow { seq; reason } ->
      let* () = validate_sequence "$.command.seq" seq in
      let* () = validate_cancellation_reason "$.command.reason" reason in
      Ok (Protocol.Cancel_child_workflow { seq; reason })
  | Activation.Signal_external_workflow
      { seq; workflow_id; run_id; signal_name; input; child_workflow_only; headers }
    ->
      let* () = validate_sequence "$.command.seq" seq in
      let* () = validate_identifier "$.command.workflow_id" workflow_id in
      let* () = validate_bounded_text "$.command.run_id" run_id in
      let* () = validate_identifier "$.command.signal_name" signal_name in
      let rec payloads_loop index reversed = function
        | [] -> Ok (List.rev reversed)
        | payload :: rest ->
            let* payload = protocol_payload
              (Printf.sprintf "$.command.input[%d]" index) payload
            in
            payloads_loop (index + 1) (payload :: reversed) rest
      in
      let rec headers_loop reversed = function
        | [] -> Ok (List.rev reversed)
        | (key, payload) :: rest ->
            let* () = validate_identifier "$.command.headers.key" key in
            let* payload = protocol_payload ("$.command.headers." ^ key) payload in
            headers_loop ((key, payload) :: reversed) rest
      in
      let* input = payloads_loop 0 [] input in
      let* headers = headers_loop [] headers in
      Ok
        (Protocol.Signal_external_workflow
           { seq; workflow_id; run_id; signal_name; input; child_workflow_only; headers })
  | Activation.Request_cancel_external_workflow
      { seq; workflow_id; run_id; reason } ->
      let* () = validate_sequence "$.command.seq" seq in
      let* () = validate_identifier "$.command.workflow_id" workflow_id in
      let* () = validate_bounded_text "$.command.run_id" run_id in
      let* () = validate_cancellation_reason "$.command.reason" reason in
      Ok
        (Protocol.Request_cancel_external_workflow
           { seq; workflow_id; run_id; reason })
  | Activation.Request_cancel_activity { seq } ->
      let* () = validate_sequence "$.command.seq" seq in
      Ok (Protocol.Request_cancel_activity { seq })
  | Activation.Request_cancel_local_activity { seq } ->
      let* () = validate_sequence "$.command.seq" seq in
      Ok (Protocol.Request_cancel_local_activity { seq })
  | Activation.Start_timer { seq; milliseconds } ->
      let* () = validate_sequence "$.command.seq" seq in
      let* start_to_fire_timeout =
        duration_of_milliseconds "$.command.milliseconds" milliseconds
      in
      Ok (Protocol.Start_timer { seq; start_to_fire_timeout })
  | Activation.Cancel_timer { seq } ->
      let* () = validate_sequence "$.command.seq" seq in
      Ok (Protocol.Cancel_timer { seq })
  | Activation.Query_result { query_id; result } ->
      let* () = validate_identifier "$.command.query_id" query_id in
      let* result =
        match result with
        | Ok payload ->
            let* payload = protocol_payload "$.command.result.payload" payload in
            Ok (Protocol.Query_succeeded payload)
        | Error error ->
            let* failure = protocol_failure "$.command.result.failure" error in
            Ok (Protocol.Query_failed failure)
      in
      Ok (Protocol.Query_result { query_id; result })
  | Activation.Update_response { protocol_instance_id; response } ->
      let* () =
        validate_identifier "$.command.protocol_instance_id" protocol_instance_id
      in
      let* response =
        match response with
        | `Accepted -> Ok Protocol.Update_accepted
        | `Rejected error ->
            let* failure = protocol_failure "$.command.response.failure" error in
            Ok (Protocol.Update_rejected failure)
        | `Completed payload ->
            let* payload = protocol_payload "$.command.response.payload" payload in
            Ok (Protocol.Update_completed payload)
      in
      Ok (Protocol.Update_response { protocol_instance_id; response })
  | Activation.Set_patch_marker { patch_id; deprecated } ->
      let* () = validate_identifier "$.command.patch_id" patch_id in
      Ok (Protocol.Set_patch_marker { patch_id; deprecated })
  | Activation.Upsert_search_attributes { search_attributes } ->
      let rec validate_keys seen = function
        | [] -> Ok ()
        | (key, payload) :: rest ->
            let* () = validate_identifier "$.command.search_attributes" key in
            if List.mem key seen then
              Error
                (invalid "$.command.search_attributes"
                   "search-attribute keys must be unique")
            else
              let* _ = protocol_payload "$.command.search_attributes" payload in
              validate_keys (key :: seen) rest
      in
      let* () = validate_keys [] search_attributes in
      let rec convert reversed = function
        | [] -> Ok (List.rev reversed)
        | (key, payload) :: rest ->
            let* payload = protocol_payload "$.command.search_attributes" payload in
            convert ((key, payload) :: reversed) rest
      in
      let* search_attributes = convert [] search_attributes in
      Ok (Protocol.Upsert_search_attributes { search_attributes })
  | Activation.Complete_workflow payload ->
      let* payload = protocol_payload "$.command.result" payload in
      let result =
        if is_null_protocol_payload payload then None else Some payload
      in
      Ok (Protocol.Complete_workflow { result })
  | Activation.Fail_workflow error ->
      let* failure = protocol_failure "$.command.failure" error in
      Ok (Protocol.Fail_workflow { failure })
  | Activation.Continue_as_new { workflow_type; input } ->
      let* () = validate_identifier "$.command.workflow_type" workflow_type in
      let* input = protocol_payload "$.command.input" input in
      Ok (Protocol.Continue_as_new { workflow_type; input = [ input ] })
  | Activation.Cancel_workflow_execution ->
      Ok Protocol.Cancel_workflow_execution

(** Converts an ordered command list and lets the canonical protocol encoder
    re-check terminal ordering, run-id bounds, payload limits, and all nested
    fields before returning it to the bridge. *)
let completion_of_commands ~run_id commands =
  let rec loop reversed = function
    | [] -> Ok (List.rev reversed)
    | command :: rest ->
        let* command = command_to_protocol command in
        loop (command :: reversed) rest
  in
  let* commands = loop [] commands in
  let completion = Protocol.{ run_id; task_failure = None; commands } in
  match Protocol.encode_completion completion with
  | Ok _ -> Ok completion
  | Error error -> Error (protocol_error error)

(** Checks that query answers belong to the activation being acknowledged.

    Rust repeats this check before handing a completion to Temporal Core, but
    the OCaml boundary performs it first so a malformed in-process completion
    cannot cross the supervisor boundary. Query activations must return one
    answer for every query ID and no ordinary workflow command; ordinary
    activations must not contain a query answer at all. Sorting copies only
    bounded identifiers and gives duplicate/missing/extra IDs one deterministic
    comparison without retaining payload bytes. *)
let validate_completion_for_activation activation completion =
  let query_ids =
    List.filter_map
      (function
        | Protocol.Query_workflow { query_id; _ } -> Some query_id
        | _ -> None)
      activation.Protocol.jobs
  in
  let result_ids =
    List.filter_map
      (function
        | Protocol.Query_result { query_id; _ } -> Some query_id
        | _ -> None)
      completion.Protocol.commands
  in
  let rec has_duplicate seen = function
    | [] -> false
    | id :: rest -> List.mem id seen || has_duplicate (id :: seen) rest
  in
  if query_ids = [] then
    if result_ids = [] then Ok ()
    else
      Error
        (invalid "$.commands"
           "query result command requires a query activation")
  else if List.length query_ids <> List.length activation.Protocol.jobs then
    Error
      (invalid "$.jobs"
         "query activation cannot contain ordinary workflow jobs")
  else if has_duplicate [] query_ids then
    Error (invalid "$.jobs" "query activation contains a duplicate query ID")
  else if has_duplicate [] result_ids then
    Error
      (invalid "$.commands"
         "query completion contains a duplicate query ID")
  else if
    List.length result_ids <> List.length completion.Protocol.commands
    || List.sort String.compare query_ids <> List.sort String.compare result_ids
  then
    Error
      (invalid "$.commands"
         "query completion identifiers must exactly match the activation")
  else Ok ()

(** Applies one protocol activation to a pre-created deterministic execution.
    Initialization data remains available from [translate_activation] for the
    caller that constructs the typed [Execution.t]; this function only feeds the
    runtime jobs and translates its resulting command batch. *)
let activate execution activation =
  let* translated = translate_activation activation in
  let* () =
    match translated.initialization with
    | None -> Ok ()
    | Some initialization ->
        let* metadata = match initialization.context with
          | None -> Ok None
          | Some context ->
              let payloads path = function
                | None -> Ok None
                | Some values ->
                    let rec loop reversed = function
                      | [] -> Ok (Some (List.rev reversed))
                      | (key, value) :: rest ->
                          let* value = runtime_payload (path ^ "." ^ key) value in
                          loop ((key, value) :: reversed) rest
                    in loop [] values
              in
              let* memo = payloads "$.jobs.context.memo" context.memo in
              let* search_attributes = payloads "$.jobs.context.search_attributes" context.search_attributes in
              Ok (Some Workflow_context_store.{ memo; search_attributes;
                execution_expiration_time = context.workflow_execution_expiration_time })
        in
        Execution.set_start_metadata execution metadata;
        Ok ()
  in
  (* Install the activation's deterministic clock before entering user code.
     The execution context is reused across tasks, so synthetic activations
     explicitly clear the previous value rather than leaving stale time
     visible to [Temporal.Workflow.now]. *)
  Execution.set_activation_timestamp execution translated.timestamp;
  let deployment_version =
    Option.map
      (fun (version : Protocol.worker_deployment_version) ->
        (version.deployment_name, version.build_id))
      (Option.bind translated.metadata (fun metadata ->
           metadata.deployment_version_for_current_task))
  in
  Execution.set_activation_deployment_version execution deployment_version;
  Execution.set_activation_is_replaying execution translated.is_replaying;
  let commands = Execution.activate execution translated.jobs in
  let* completion =
    match Execution.task_failure execution with
    | None -> completion_of_commands ~run_id:translated.run_id commands
    | Some error ->
        let* failure = protocol_failure "$.task_failure" error in
        let completion = Protocol.{
          run_id = translated.run_id; commands = []; task_failure = Some failure
        } in
        let* _ = Protocol.encode_completion completion |> Result.map_error protocol_error in
        Ok completion
  in
  let* () = validate_completion_for_activation activation completion in
  match translated.cache_removal with
  | Some _ when completion.commands <> [] ->
      Error
        (invalid "$.commands"
           "cache eviction must acknowledge the activation with no workflow \
            commands")
  | _ -> Ok completion

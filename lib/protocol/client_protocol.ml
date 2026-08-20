(** Closed client-side JSON protocol implementation.

    The protocol deliberately reuses the workflow module's payload and failure
    codecs.  That keeps binary payload ownership, base64 canonicalization, and
    recursive failure validation identical on both client and worker paths. *)

module Control = Control_protocol
module Workflow = Workflow_protocol

type payload = Workflow.payload
type failure = Workflow.failure
type execution = { namespace : string; workflow_id : string; run_id : string }

(** One named payload attached to a workflow start.  Arrays are used on the
    wire instead of JSON objects so duplicate keys can be rejected before a
    map is constructed and encoding remains deterministic after sorting. *)
type metadata_field = { key : string; value : payload }

type start_request = {
  request_id : string;
  namespace : string;
  workflow_id : string;
  workflow_type : string;
  task_queue : string;
  input : payload list;
  memo : metadata_field list;
  search_attributes : metadata_field list;
}

type start_response = { execution : execution }
type start_ticket = { request : start_request; ticket : string }
type wait_request = execution

type cancel_request = {
  execution : execution;
  request_id : string;
  reason : string;
}

type cancel_response = { acknowledged : bool }

type reset_request = {
  execution : execution;
  request_id : string;
  reason : string;
  workflow_task_finish_event_id : int64;
}

type reset_response = { execution : execution }

(** Exact-run termination request. Temporal termination is an immediate
    control-plane operation and therefore carries operator reason text rather
    than a cancellation request ID. *)
type terminate_request = { execution : execution; reason : string }

type terminate_response = { acknowledged : bool }

type signal_request = {
  execution : execution;
  signal_name : string;
  request_id : string;
  input : payload list;
}

type signal_response = { acknowledged : bool }

type query_request = {
  execution : execution;
  query_type : string;
  input : payload list;
}

type query_response = { result : payload list }

type visibility_request = {
  namespace : string;
  query : string;
  page_size : int;
  next_page_token : string option;
}

type visibility_execution = {
  workflow_id : string;
  run_id : string;
  workflow_type : string;
  task_queue : string;
  status : string;
}

type visibility_page = {
  executions : visibility_execution list;
  next_page_token : string option;
}

type update_request = {
  execution : execution;
  update_id : string;
  update_name : string;
  input : payload list;
}

type poll_update_request = { execution : execution; update_id : string }

type update_outcome =
  | Update_completed of { result : payload list }
  | Update_failed of { failure : failure }

type update_response = {
  update_id : string;
  execution : execution;
  outcome : update_outcome option;
}

type poll_update_response = { outcome : update_outcome option }
type outcome =
  | Completed of { result : payload list; successor : execution option }
  | Failed of { failure : failure; successor : execution option }
  | Cancelled of { details : payload list }
  | Terminated of { details : payload list }
  | Timed_out of { successor : execution option }
  | Continued_as_new of { successor : execution }

type wait_response = { execution : execution; outcome : outcome }

type client_error =
  | Already_started of { workflow_id : string; existing_run_id : string option }
  | Rpc of { code : string }
  | Protocol of { code : string }

type start_outcome =
  | Accepted of start_response
  | Rejected of client_error
  | Unknown of { request_id : string; workflow_id : string }

type error = { code : string; path : string; message : string }
type error_view = { code : string; path : string; message : string }

let error_view (error : error) : error_view =
  { code = error.code; path = error.path; message = error.message }

let ( let* ) = Result.bind

let of_workflow_error path (error : Workflow.error) : error =
  let view = Workflow.error_view error in
  { code = view.code; path; message = view.message }

(** Converts a strict-JSON failure while retaining its safe nested path. *)
let of_control_error (error : Control.error) : error =
  let view = Control.error_view error in
  { code = view.code; path = view.path; message = view.message }

let invalid ?(path = "$") message : error =
  { code = "invalid_message"; path; message }

let exact_object path fields json =
  match json with
  | `Assoc _ -> (
      match Workflow.Internal.exact_object path fields json with
      | Ok entries -> Ok entries
      | Error error -> Error (of_workflow_error path error))
  | _ -> Error (invalid ~path "expected JSON object")

let field path name entries =
  match Workflow.Internal.field path name entries with
  | Ok value -> Ok value
  | Error error -> Error (of_workflow_error path error)

let string path json =
  match Workflow.Internal.string path json with
  | Ok value -> Ok value
  | Error error -> Error (of_workflow_error path error)

let bool path json =
  match Workflow.Internal.bool path json with
  | Ok value -> Ok value
  | Error error -> Error (of_workflow_error path error)

let identifier path json =
  match Workflow.Internal.identifier path json with
  | Ok value when String.contains value '\000' ->
      Error (invalid ~path "identifier contains a NUL byte")
  | Ok value -> Ok value
  | Error error -> Error (of_workflow_error path error)

(** Validates an identifier before it is serialized. The Rust bridge applies
    the same non-empty, bounded, NUL-free rule; enforcing it here keeps an
    invalid request from crossing the FFI boundary at all. *)
let validate_identifier path value =
  if String.length value = 0 then Error (invalid ~path "identifier is empty")
  else if String.length value > 65_536 then
    Error (invalid ~path "identifier exceeds the protocol string safety limit")
  else if String.contains value '\000' then
    Error (invalid ~path "identifier contains a NUL byte")
  else Ok ()

let nullable _path (decode : Yojson.Safe.t -> ('a, error) result)
    (json : Yojson.Safe.t) : ('a option, error) result =
  match json with
  | `Null -> Ok None
  | value -> Result.map (fun value -> Some value) (decode value)

let payload path json =
  match Workflow.Internal.payload path json with
  | Ok value -> Ok value
  | Error error -> Error (of_workflow_error path error)

let payloads path json =
  match json with
  | `List values ->
      let rec loop index reversed = function
        | [] -> Ok (List.rev reversed)
        | value :: rest ->
            let* value = payload (Printf.sprintf "%s[%d]" path index) value in
            loop (index + 1) (value :: reversed) rest
      in
      loop 0 [] values
  | _ -> Error (invalid ~path "expected JSON array")

let payload_json value =
  Workflow.Internal.payload_json value
  |> Result.map_error (of_workflow_error "$.payload")

let payloads_json values =
  let rec loop reversed = function
    | [] -> Ok (`List (List.rev reversed))
    | value :: rest ->
        let* encoded = payload_json value in
        loop (encoded :: reversed) rest
  in
  loop [] values

let json_string value = `String value

let decode_execution path json =
  let* entries = exact_object path [ "namespace"; "workflow_id"; "run_id" ] json in
  let* namespace_json = field path "namespace" entries in
  let* namespace = identifier (path ^ ".namespace") namespace_json in
  let* workflow_id_json = field path "workflow_id" entries in
  let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
  let* run_id_json = field path "run_id" entries in
  let* run_id = identifier (path ^ ".run_id") run_id_json in
  Ok { namespace; workflow_id; run_id }

(** Serializes one execution identity after applying the same identifier
    limits used by the decoder. This is used only for closed outcome encoding;
    Temporal's protobuf representation remains entirely Rust-owned. *)
let encode_execution path (value : execution) =
  let* () = validate_identifier (path ^ ".namespace") value.namespace in
  let* () = validate_identifier (path ^ ".workflow_id") value.workflow_id in
  let* () = validate_identifier (path ^ ".run_id") value.run_id in
  Ok
    (`Assoc
      [
        ("namespace", json_string value.namespace);
        ("workflow_id", json_string value.workflow_id);
        ("run_id", json_string value.run_id);
      ])

let encode_object json =
  match Control.encode_payload_object json with
  | Ok value -> Ok value
  | Error error -> Error (of_control_error error)

let decode_object input =
  match Control.decode_payload_object input with
  | Ok value -> Ok value
  | Error error -> Error (of_control_error error)

let encode_start_request (value : start_request) =
  let* () = validate_identifier "$.request_id" value.request_id in
  let* () = validate_identifier "$.namespace" value.namespace in
  let* () = validate_identifier "$.workflow_id" value.workflow_id in
  let* () = validate_identifier "$.workflow_type" value.workflow_type in
  let* () = validate_identifier "$.task_queue" value.task_queue in
  let* input = payloads_json value.input in
  let encode_metadata path fields =
    let sorted = List.sort (fun left right -> String.compare left.key right.key) fields in
    let rec loop seen reversed = function
      | [] -> Ok (`List (List.rev reversed))
      | field :: rest ->
          let* () = validate_identifier (path ^ "[key]") field.key in
          if List.mem field.key seen then
            Error (invalid ~path ("duplicate metadata key " ^ field.key))
          else
            let* payload = payload_json field.value in
            loop (field.key :: seen)
              (`Assoc [ ("key", json_string field.key); ("value", payload) ] :: reversed)
              rest
    in
    loop [] [] sorted
  in
  let* memo = encode_metadata "$.memo" value.memo in
  let* search_attributes = encode_metadata "$.search_attributes" value.search_attributes in
  encode_object
      (`Assoc
      [
        ("request_id", json_string value.request_id);
        ("namespace", json_string value.namespace);
        ("workflow_id", json_string value.workflow_id);
        ("workflow_type", json_string value.workflow_type);
        ("task_queue", json_string value.task_queue);
        ("input", input);
        ("memo", memo);
        ("search_attributes", search_attributes);
      ])

(** Serializes the opaque native capability used by asynchronous start polls.
    The request is retained in the OCaml value but is deliberately omitted
    from the wire document: Rust only needs the generated ticket, while the
    OCaml supervisor uses the retained request to correlate terminal output. *)
let encode_start_ticket (value : start_ticket) =
  let* () = validate_identifier "$.ticket" value.ticket in
  encode_object (`Assoc [ ("ticket", json_string value.ticket) ])

(** Decodes a native ticket and binds it to the exact request that admitted it.
    Keeping this association private makes it impossible for a caller to pass
    a valid ticket alongside a different request and accidentally accept a
    response for the wrong workflow. *)
let decode_start_ticket ~(request : start_request) input =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "ticket" ] json in
  let* ticket_json = field "$" "ticket" entries in
  let* ticket = identifier "$.ticket" ticket_json in
  Ok { request; ticket }

(** Returns the request retained by an opaque ticket. The native ticket value
    remains inaccessible; this accessor exists only so the supervisor can
    correlate terminal output with the request before decoding it. *)
let start_ticket_request (ticket : start_ticket) = ticket.request

(** Verifies that a decoded response names the request's workflow identity.
    Start responses only require the server-assigned run to be non-empty;
    exact-run waits additionally pass [run_id] and require an exact match. *)
let validate_execution_matches path ~namespace ~workflow_id ?run_id
    (actual : execution) =
  if not (String.equal actual.namespace namespace) then
    Error
      (invalid ~path:(path ^ ".namespace")
         "response namespace does not match the requested execution")
  else if not (String.equal actual.workflow_id workflow_id) then
    Error
      (invalid ~path:(path ^ ".workflow_id")
         "response workflow ID does not match the requested execution")
  else if String.length actual.run_id = 0 then
    Error (invalid ~path:(path ^ ".run_id") "response run ID is empty")
  else
    match run_id with
    | Some expected when not (String.equal actual.run_id expected) ->
        Error
          (invalid ~path:(path ^ ".run_id")
             "response run ID does not match the requested execution")
    | Some _ | None -> Ok ()

(** Parses a successful start document and correlates its execution with the
    request that produced it before exposing the server-assigned run. *)
let decode_start_response ~(request : start_request) input =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "execution" ] json in
  let* execution_json = field "$" "execution" entries in
  let* execution = decode_execution "$.execution" execution_json in
  let* () =
    validate_execution_matches "$.execution" ~namespace:request.namespace
      ~workflow_id:request.workflow_id execution
  in
  Ok ({ execution } : start_response)

let encode_wait_request (value : wait_request) =
  let* () = validate_identifier "$.namespace" value.namespace in
  let* () = validate_identifier "$.workflow_id" value.workflow_id in
  let* () = validate_identifier "$.run_id" value.run_id in
  encode_object
    (`Assoc
      [
        ("namespace", json_string value.namespace);
        ("workflow_id", json_string value.workflow_id);
        ("run_id", json_string value.run_id);
      ])

(** Serializes an exact-run cancellation request. [request_id] is the
    Temporal idempotency key for this control operation; [reason] is copied as
    opaque UTF-8 text and may be empty because Temporal treats it as optional
    operator context. *)
let encode_cancel_request (value : cancel_request) =
  let* () = validate_identifier "$.namespace" value.execution.namespace in
  let* () = validate_identifier "$.workflow_id" value.execution.workflow_id in
  let* () = validate_identifier "$.run_id" value.execution.run_id in
  let* () = validate_identifier "$.request_id" value.request_id in
  if String.length value.reason > 65_536 then
    Error (invalid ~path:"$.reason" "reason exceeds the protocol string safety limit")
  else if String.contains value.reason '\000' then
    Error (invalid ~path:"$.reason" "reason contains a NUL byte")
  else
    encode_object
      (`Assoc
        [
          ("namespace", json_string value.execution.namespace);
          ("workflow_id", json_string value.execution.workflow_id);
          ("run_id", json_string value.execution.run_id);
          ("request_id", json_string value.request_id);
          ("reason", json_string value.reason);
        ])

(** Decodes the positive acknowledgement returned by Rust after Temporal has
    accepted the cancellation RPC. A false acknowledgement is rejected rather
    than being exposed as success because the public operation has no separate
    pending state. *)
let decode_cancel_response input : (cancel_response, error) result =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "acknowledged" ] json in
  let* acknowledged_json = field "$" "acknowledged" entries in
  let* acknowledged = bool "$.acknowledged" acknowledged_json in
  if acknowledged then Ok ({ acknowledged } : cancel_response)
  else Error (invalid ~path:"$.acknowledged" "cancellation was not acknowledged")

(** Serializes a reset request for one exact run. Temporal requires the reset
    point to identify a completed or started workflow task event; keeping the
    event ID as an exact signed 64-bit value avoids float or JSON rounding. *)
let encode_reset_request (value : reset_request) =
  let* () = validate_identifier "$.namespace" value.execution.namespace in
  let* () = validate_identifier "$.workflow_id" value.execution.workflow_id in
  let* () = validate_identifier "$.run_id" value.execution.run_id in
  let* () = validate_identifier "$.request_id" value.request_id in
  if value.workflow_task_finish_event_id < 0L then
    Error (invalid ~path:"$.workflow_task_finish_event_id" "event ID must be non-negative")
  else if String.length value.reason > 65_536 then
    Error (invalid ~path:"$.reason" "reason exceeds the protocol string safety limit")
  else if String.contains value.reason '\000' then
    Error (invalid ~path:"$.reason" "reason contains a NUL byte")
  else
    encode_object
      (`Assoc
        [
          ("namespace", json_string value.execution.namespace);
          ("workflow_id", json_string value.execution.workflow_id);
          ("run_id", json_string value.execution.run_id);
          ("request_id", json_string value.request_id);
          ( "workflow_task_finish_event_id",
            `Intlit (Int64.to_string value.workflow_task_finish_event_id) );
          ("reason", json_string value.reason);
        ])

(** Decodes the new exact execution returned by Temporal after a reset. The
    server may choose a different run ID, so only namespace and workflow ID
    must match the request; the returned run ID must still be non-empty. *)
let decode_reset_response ~(request : reset_request) input : (reset_response, error) result =
  let* json = decode_object input in
  let* outer = exact_object "$" [ "execution" ] json in
  let* execution_json = field "$" "execution" outer in
  let* entries = exact_object "$.execution" [ "namespace"; "workflow_id"; "run_id" ] execution_json in
  let* namespace_json = field "$.execution" "namespace" entries in
  let* namespace = identifier "$.execution.namespace" namespace_json in
  let* workflow_id_json = field "$.execution" "workflow_id" entries in
  let* workflow_id = identifier "$.execution.workflow_id" workflow_id_json in
  let* run_id_json = field "$.execution" "run_id" entries in
  let* run_id = identifier "$.execution.run_id" run_id_json in
  if not (String.equal namespace request.execution.namespace) then
    Error (invalid ~path:"$.execution.namespace" "reset response namespace does not match request")
  else if not (String.equal workflow_id request.execution.workflow_id) then
    Error (invalid ~path:"$.execution.workflow_id" "reset response workflow ID does not match request")
  else Ok { execution = { namespace; workflow_id; run_id } }

(** Serializes termination using the same closed identity/reason shape as
    cancellation while keeping the operation-specific type explicit. *)
let encode_terminate_request (value : terminate_request) =
  let* () = validate_identifier "$.namespace" value.execution.namespace in
  let* () = validate_identifier "$.workflow_id" value.execution.workflow_id in
  let* () = validate_identifier "$.run_id" value.execution.run_id in
  if String.length value.reason > 65_536 then
    Error (invalid ~path:"$.reason" "reason exceeds the protocol string safety limit")
  else if String.contains value.reason '\000' then
    Error (invalid ~path:"$.reason" "reason contains a NUL byte")
  else
    encode_object
      (`Assoc
        [ ("namespace", json_string value.execution.namespace);
          ("workflow_id", json_string value.execution.workflow_id);
          ("run_id", json_string value.execution.run_id);
          ("reason", json_string value.reason) ])

(** Decodes the positive acknowledgement returned after Temporal accepts a
    termination request. *)
let decode_terminate_response input : (terminate_response, error) result =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "acknowledged" ] json in
  let* acknowledged_json = field "$" "acknowledged" entries in
  let* acknowledged = bool "$.acknowledged" acknowledged_json in
  if acknowledged then Ok ({ acknowledged } : terminate_response)
  else Error (invalid ~path:"$.acknowledged" "termination was not acknowledged")

(** Serializes one exact-run signal request. Signal input remains an ordered
    payload list so codecs that produce multiple Temporal payloads retain the
    same order through OCaml, JSON, Rust, and the official protobuf service. *)
let encode_signal_request (value : signal_request) =
  let* () = validate_identifier "$.namespace" value.execution.namespace in
  let* () = validate_identifier "$.workflow_id" value.execution.workflow_id in
  let* () = validate_identifier "$.run_id" value.execution.run_id in
  let* () = validate_identifier "$.signal_name" value.signal_name in
  let* () = validate_identifier "$.request_id" value.request_id in
  let* input = payloads_json value.input in
  encode_object
    (`Assoc
      [
        ("namespace", json_string value.execution.namespace);
        ("workflow_id", json_string value.execution.workflow_id);
        ("run_id", json_string value.execution.run_id);
        ("signal_name", json_string value.signal_name);
        ("request_id", json_string value.request_id);
        ("input", input);
      ])

(** Decodes the positive acknowledgement returned by Rust after Temporal has
    accepted a signal RPC. A false value is rejected so callers never observe
    [Ok ()] for a request that the bridge did not positively acknowledge. *)
let decode_signal_response input : (signal_response, error) result =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "acknowledged" ] json in
  let* acknowledged_json = field "$" "acknowledged" entries in
  let* acknowledged = bool "$.acknowledged" acknowledged_json in
  if acknowledged then Ok ({ acknowledged } : signal_response)
  else Error (invalid ~path:"$.acknowledged" "signal was not acknowledged")

(** Serializes one output-only query request. The [input] member remains an
    ordered payload list even though this first public API sends an empty list;
    retaining the list keeps the private contract ready for typed query
    arguments without changing the execution identity or query name fields. *)
let encode_query_request (value : query_request) =
  let* () = validate_identifier "$.namespace" value.execution.namespace in
  let* () = validate_identifier "$.workflow_id" value.execution.workflow_id in
  let* () = validate_identifier "$.run_id" value.execution.run_id in
  let* () = validate_identifier "$.query_type" value.query_type in
  let* input = payloads_json value.input in
  encode_object
    (`Assoc
      [
        ("namespace", json_string value.execution.namespace);
        ("workflow_id", json_string value.execution.workflow_id);
        ("run_id", json_string value.execution.run_id);
        ("query_type", json_string value.query_type);
        ("input", input);
      ])

(** Decodes one successful output-only query response. The server may return
    zero payloads for a unit-like query; the public codec layer decides whether
    that cardinality is valid for the caller's result type. *)
let decode_query_response input : (query_response, error) result =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "result" ] json in
  let* result_json = field "$" "result" entries in
  let* result = payloads "$.result" result_json in
  Ok { result }

let encode_visibility_request (value : visibility_request) =
  let* () = validate_identifier "$.namespace" value.namespace in
  if String.length value.query > 65_536 || String.contains value.query '\000' then
    Error (invalid ~path:"$.query" "query exceeds the protocol string safety limit")
  else if value.page_size < 1 || value.page_size > 1_000 then
    Error (invalid ~path:"$.page_size" "page_size must be between 1 and 1000")
  else
    let token = match value.next_page_token with None -> `Null | Some v -> `String v in
    encode_object
      (`Assoc
        [
          ("namespace", json_string value.namespace);
          ("query", json_string value.query);
          ("page_size", `Int value.page_size);
          ("next_page_token", token);
        ])

let decode_visibility_response input : (visibility_page, error) result =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "executions"; "next_page_token" ] json in
  let* executions_json = field "$" "executions" entries in
  let* executions =
    match executions_json with
    | `List values ->
        let rec loop index acc = function
          | [] -> Ok (List.rev acc)
          | value :: rest ->
              let path = Printf.sprintf "$.executions[%d]" index in
              let* row = exact_object path
                  [ "workflow_id"; "run_id"; "workflow_type"; "task_queue"; "status" ] value in
              let* workflow_id_json = field path "workflow_id" row in
              let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
              let* run_id_json = field path "run_id" row in
              let* run_id = identifier (path ^ ".run_id") run_id_json in
              let* workflow_type_json = field path "workflow_type" row in
              let* workflow_type = identifier (path ^ ".workflow_type") workflow_type_json in
              let* task_queue_json = field path "task_queue" row in
              let* task_queue = identifier (path ^ ".task_queue") task_queue_json in
              let* status_json = field path "status" row in
              let* status = identifier (path ^ ".status") status_json in
              let* status =
                match status with
                | "running" | "completed" | "failed" | "canceled"
                | "terminated" | "continued_as_new" | "timed_out"
                | "paused" | "unspecified" -> Ok status
                | _ -> Error (invalid ~path:(path ^ ".status") "unknown visibility status")
              in
              loop (index + 1) ({ workflow_id; run_id; workflow_type; task_queue; status } :: acc) rest
        in
        loop 0 [] values
    | _ -> Error (invalid ~path:"$.executions" "expected JSON array")
  in
  let* token_json = field "$" "next_page_token" entries in
  let* next_page_token = nullable "$.next_page_token" (identifier "$.next_page_token") token_json in
  Ok { executions; next_page_token }

let decode_update_outcome path json =
  let* kind_json =
    match json with
    | `Assoc entries -> field path "kind" entries
    | _ -> Error (invalid ~path "expected JSON object")
  in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "completed" ->
      let* entries = exact_object path [ "kind"; "result" ] json in
      let* result_json = field path "result" entries in
      let* result = payloads (path ^ ".result") result_json in
      Ok (Update_completed { result })
  | "failed" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* failure =
        match Workflow.Internal.failure (path ^ ".failure") failure_json with
        | Ok failure -> Ok failure
        | Error error -> Error (of_workflow_error (path ^ ".failure") error)
      in
      Ok (Update_failed { failure })
  | _ -> Error (invalid ~path:(path ^ ".kind") "unknown workflow update outcome kind")

let encode_update_request (value : update_request) =
  let* _execution = encode_execution "$.execution" value.execution in
  let* () = validate_identifier "$.update_id" value.update_id in
  let* () = validate_identifier "$.update_name" value.update_name in
  let* input = payloads_json value.input in
  encode_object
    (`Assoc
      [
        ("namespace", json_string value.execution.namespace);
        ("workflow_id", json_string value.execution.workflow_id);
        ("run_id", json_string value.execution.run_id);
        ("update_id", json_string value.update_id);
        ("update_name", json_string value.update_name);
        ("input", input);
      ])

let encode_poll_update_request (value : poll_update_request) =
  let* () = validate_identifier "$.namespace" value.execution.namespace in
  let* () = validate_identifier "$.workflow_id" value.execution.workflow_id in
  let* () = validate_identifier "$.run_id" value.execution.run_id in
  let* () = validate_identifier "$.update_id" value.update_id in
  encode_object
    (`Assoc
      [
        ("namespace", json_string value.execution.namespace);
        ("workflow_id", json_string value.execution.workflow_id);
        ("run_id", json_string value.execution.run_id);
        ("update_id", json_string value.update_id);
      ])

let decode_optional_update_outcome path entries =
  let* outcome_json = field path "outcome" entries in
  nullable (path ^ ".outcome") (decode_update_outcome (path ^ ".outcome")) outcome_json

let decode_update_response ~(request : update_request) input =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "update_id"; "execution"; "outcome" ] json in
  let* update_id_json = field "$" "update_id" entries in
  let* update_id = identifier "$.update_id" update_id_json in
  if not (String.equal update_id request.update_id) then
    Error (invalid ~path:"$.update_id" "update response update ID does not match request")
  else
    let* execution_json = field "$" "execution" entries in
    let* execution = decode_execution "$.execution" execution_json in
    let* () =
      validate_execution_matches "$.execution"
        ~namespace:request.execution.namespace
        ~workflow_id:request.execution.workflow_id
        ~run_id:request.execution.run_id execution
    in
    let* outcome = decode_optional_update_outcome "$" entries in
    Ok { update_id; execution; outcome }

let decode_poll_update_response input =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "outcome" ] json in
  let* outcome = decode_optional_update_outcome "$" entries in
  Ok { outcome }

let decode_successor path entries =
  let* successor_json = field path "successor" entries in
  nullable (path ^ ".successor") (decode_execution (path ^ ".successor"))
    successor_json

let decode_outcome json =
  let path = "$.outcome" in
  let* kind_json =
    match json with
    | `Assoc entries -> field path "kind" entries
    | _ -> Error (invalid ~path "expected JSON object")
  in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "completed" ->
      let* entries = exact_object path [ "kind"; "result"; "successor" ] json in
      let* result_json = field path "result" entries in
      let* result = payloads (path ^ ".result") result_json in
      let* successor = decode_successor path entries in
      Ok (Completed { result; successor })
  | "failed" ->
      let* entries = exact_object path [ "kind"; "failure"; "successor" ] json in
      let* failure_json = field path "failure" entries in
      let* failure =
        match Workflow.Internal.failure (path ^ ".failure") failure_json with
        | Ok value -> Ok value
        | Error error -> Error (of_workflow_error path error)
      in
      let* successor = decode_successor path entries in
      Ok (Failed { failure; successor })
  | "cancelled" ->
      let* entries = exact_object path [ "kind"; "details" ] json in
      let* details_json = field path "details" entries in
      let* details = payloads (path ^ ".details") details_json in
      Ok (Cancelled { details })
  | "terminated" ->
      let* entries = exact_object path [ "kind"; "details" ] json in
      let* details_json = field path "details" entries in
      let* details = payloads (path ^ ".details") details_json in
      Ok (Terminated { details })
  | "timed_out" ->
      let* entries = exact_object path [ "kind"; "successor" ] json in
      let* successor = decode_successor path entries in
      Ok (Timed_out { successor })
  | "continued_as_new" ->
      let* entries = exact_object path [ "kind"; "successor" ] json in
      let* successor_json = field path "successor" entries in
      let* successor = decode_execution (path ^ ".successor") successor_json in
      Ok (Continued_as_new { successor })
  | _ ->
      Error
        (invalid ~path:(path ^ ".kind") "unknown workflow outcome kind")

(** Checks that a successor stays in the same workflow chain and names a new
    run. This mirrors Rust's server-response validation before the result is
    allowed into public OCaml code. *)
let validate_successor_for_execution (execution : execution) path
    (successor : execution) =
  let* () = validate_identifier (path ^ ".namespace") successor.namespace in
  let* () = validate_identifier (path ^ ".workflow_id") successor.workflow_id in
  let* () = validate_identifier (path ^ ".run_id") successor.run_id in
  if not (String.equal successor.namespace execution.namespace) then
    Error
      (invalid ~path:(path ^ ".namespace")
         "successor namespace does not match the waited execution")
  else if not (String.equal successor.workflow_id execution.workflow_id) then
    Error
      (invalid ~path:(path ^ ".workflow_id")
         "successor workflow ID does not match the waited execution")
  else if String.equal successor.run_id execution.run_id then
    Error
      (invalid ~path:(path ^ ".run_id")
         "successor run ID must differ from the waited run")
  else Ok ()

(** Applies successor-chain validation to every outcome variant that carries
    optional or required successor metadata. *)
let validate_wait_successor execution outcome =
  let validate path successor = validate_successor_for_execution execution path successor in
  match outcome with
  | Completed { successor = Some successor; _ }
  | Failed { successor = Some successor; _ }
  | Timed_out { successor = Some successor } ->
      validate "$.outcome.successor" successor
  | Continued_as_new { successor } ->
      validate "$.outcome.successor" successor
  | Completed { successor = None; _ }
  | Failed { successor = None; _ }
  | Cancelled _
  | Terminated _
  | Timed_out { successor = None } -> Ok ()

(** Accepts exactly the stable status-code vocabulary emitted by the Rust
    client bridge. Unknown codes fail closed rather than being mistaken for a
    known operational category. *)
let client_error_code kind path value =
  let rpc_codes =
    [
      "ok";
      "cancelled";
      "unknown";
      "invalid_argument";
      "deadline_exceeded";
      "termination_outcome_uncertain";
      "not_found";
      "already_exists";
      "permission_denied";
      "resource_exhausted";
      "failed_precondition";
      "aborted";
      "out_of_range";
      "unimplemented";
      "internal";
      "unavailable";
      "data_loss";
      "unauthenticated";
    ]
  in
  let protocol_codes = [ "core_unsupported"; "core_invalid" ] in
  let allowed = if String.equal kind "rpc" then rpc_codes else protocol_codes in
  if List.mem value allowed then Ok value
  else Error (invalid ~path "unknown client error code")

(** Builds one closed client-error object for tests and for callers that need
    to persist a terminal asynchronous outcome. The native bridge normally
    emits this document, but validating the OCaml encoder too keeps both sides
    of the private protocol symmetric. *)
let encode_client_error_json path = function
  | Already_started { workflow_id; existing_run_id } ->
      let* () = validate_identifier (path ^ ".workflow_id") workflow_id in
      let* () =
        match existing_run_id with
        | None -> Ok ()
        | Some run_id -> validate_identifier (path ^ ".existing_run_id") run_id
      in
      Ok
        (`Assoc
          [
            ("kind", json_string "already_started");
            ("workflow_id", json_string workflow_id);
            ( "existing_run_id",
              match existing_run_id with
              | None -> `Null
              | Some run_id -> json_string run_id );
          ])
  | Rpc { code } ->
      let* code = client_error_code "rpc" (path ^ ".code") code in
      Ok (`Assoc [ ("kind", json_string "rpc"); ("code", json_string code) ])
  | Protocol { code } ->
      let* code = client_error_code "protocol" (path ^ ".code") code in
      Ok
        (`Assoc
          [ ("kind", json_string "protocol"); ("code", json_string code) ])

(** Parses one terminal exact-run response and verifies that the response
    execution is the requested run before validating its outcome chain. *)
let decode_wait_response ~(request : wait_request) input =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "execution"; "outcome" ] json in
  let* execution_json = field "$" "execution" entries in
  let* execution = decode_execution "$.execution" execution_json in
  let* () =
    validate_execution_matches "$.execution" ~namespace:request.namespace
      ~workflow_id:request.workflow_id ~run_id:request.run_id execution
  in
  let* outcome_json = field "$" "outcome" entries in
  let* outcome = decode_outcome outcome_json in
  let* () = validate_wait_successor execution outcome in
  Ok { execution; outcome }

(** Parses the closed structured error body emitted by Rust. Operation-specific
    wrappers below add request identity and allowed-category checks. *)
let decode_client_error input =
  let* json = decode_object input in
  let path = "$" in
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string "$.kind" kind_json in
  match kind with
  | "already_started" ->
      let* entries = exact_object path [ "kind"; "workflow_id"; "existing_run_id" ] json in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier "$.workflow_id" workflow_id_json in
      let* existing_json = field path "existing_run_id" entries in
      let* existing_run_id =
        nullable "$.existing_run_id"
          (fun value -> identifier "$.existing_run_id" value)
          existing_json
      in
      Ok (Already_started { workflow_id; existing_run_id })
  | "rpc" | "protocol" ->
      let* entries = exact_object path [ "kind"; "code" ] json in
      let* code_json = field path "code" entries in
      let* code = identifier "$.code" code_json in
      let* code = client_error_code kind "$.code" code in
      if String.equal kind "rpc" then Ok (Rpc { code })
      else Ok (Protocol { code })
  | _ -> Error (invalid ~path:"$.kind" "unknown client error kind")

(** Checks that an error body is valid for a workflow-start operation. The
    [already_started] identity is correlated with the request so a malformed
    native response cannot attribute another workflow's conflict to the
    caller. *)
let validate_start_error (request : start_request) = function
  | Already_started { workflow_id; _ } as error ->
      if String.equal workflow_id request.workflow_id then Ok error
      else
        Error
          (invalid ~path:"$.workflow_id"
             "already-started error names a different workflow ID")
  | (Rpc _ | Protocol _) as error -> Ok error

(** Serializes one terminal asynchronous-start outcome. [Unknown] is kept as a
    first-class value instead of being encoded as a transport error, because a
    transport failure can occur after Temporal accepted the request. *)
let encode_start_outcome = function
  | Accepted { execution } ->
      let* execution = encode_execution "$.execution" execution in
      encode_object
        (`Assoc
          [ ("kind", json_string "accepted"); ("execution", execution) ])
  | Rejected error ->
      let* error = encode_client_error_json "$.error" error in
      encode_object
        (`Assoc [ ("kind", json_string "rejected"); ("error", error) ])
  | Unknown { request_id; workflow_id } ->
      let* () = validate_identifier "$.request_id" request_id in
      let* () = validate_identifier "$.workflow_id" workflow_id in
      encode_object
        (`Assoc
          [
            ("kind", json_string "unknown");
            ("request_id", json_string request_id);
            ("workflow_id", json_string workflow_id);
          ])

(** Parses a terminal asynchronous-start outcome and checks both sides of its
    identity. Accepted and already-started responses must name the requested
    workflow, while unknown responses must repeat the stable logical request
    ID and workflow ID so they cannot be attributed to another ticket. *)
let decode_start_outcome ~(request : start_request) input =
  let* json = decode_object input in
  let path = "$" in
  let* kind_json =
    match json with
    | `Assoc entries -> field path "kind" entries
    | _ -> Error (invalid ~path "expected JSON object")
  in
  let* kind = string "$.kind" kind_json in
  match kind with
  | "accepted" ->
      let* entries = exact_object path [ "kind"; "execution" ] json in
      let* execution_json = field path "execution" entries in
      let* execution = decode_execution "$.execution" execution_json in
      let* () =
        validate_execution_matches "$.execution" ~namespace:request.namespace
          ~workflow_id:request.workflow_id execution
      in
      Ok (Accepted { execution })
  | "rejected" ->
      let* entries = exact_object path [ "kind"; "error" ] json in
      let* error_json = field path "error" entries in
      let* error_text =
        try Ok (Yojson.Safe.to_string error_json)
        with _ -> Error (invalid "invalid rejected client error")
      in
      let* error = decode_client_error error_text in
      let* error = validate_start_error request error in
      Ok (Rejected error)
  | "unknown" ->
      let* entries =
        exact_object path [ "kind"; "request_id"; "workflow_id" ] json
      in
      let* request_id_json = field path "request_id" entries in
      let* request_id = identifier "$.request_id" request_id_json in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier "$.workflow_id" workflow_id_json in
      if not (String.equal request_id request.request_id) then
        Error
          (invalid ~path:"$.request_id"
             "unknown outcome request ID does not match the start request")
      else if not (String.equal workflow_id request.workflow_id) then
        Error
          (invalid ~path:"$.workflow_id"
             "unknown outcome workflow ID does not match the start request")
      else Ok (Unknown { request_id; workflow_id })
  | _ -> Error (invalid ~path:"$.kind" "unknown asynchronous start outcome kind")

(** Rejects error categories that cannot be returned by an exact-run wait.
    Keeping this check beside the decoder makes the operation-specific closed
    vocabulary explicit rather than relying only on Rust status numbers. *)
let validate_wait_error (_request : wait_request) = function
  | Already_started _ ->
      Error
        (invalid ~path:"$.kind"
           "already_started is not a valid exact-run wait error")
  | (Rpc _ | Protocol _) as error -> Ok error

(** Decodes a start failure and correlates any existing-run identity with the
    requested workflow ID. *)
let decode_start_error ~(request : start_request) input =
  let* error = decode_client_error input in
  validate_start_error request error

(** Decodes a wait failure while rejecting the start-only conflict category. *)
let decode_wait_error ~(request : wait_request) input =
  let* error = decode_client_error input in
  validate_wait_error request error

(** Decodes a cancellation failure while rejecting the workflow-start-only
    conflict category. Temporal cancellation can fail at the RPC or protocol
    layer, but it cannot report that a workflow ID was already started. *)
let decode_cancel_error input =
  let* error = decode_client_error input in
  match error with
  | Already_started _ ->
      Error
        (invalid ~path:"$.kind"
           "already_started is not a valid cancellation error")
  | (Rpc _ | Protocol _) -> Ok error

(** Reset shares the cancellation RPC error vocabulary but has its own public
    adapter entry point so operation-specific validation remains explicit. *)
let decode_reset_error input = decode_cancel_error input

(** Decodes a signal failure while rejecting [Already_started], which is a
    start-only category and cannot describe a signal RPC. *)
let decode_signal_error input =
  let* error = decode_client_error input in
  match error with
  | Already_started _ ->
      Error
        (invalid ~path:"$.kind"
           "already_started is not a valid signal error")
  | (Rpc _ | Protocol _) -> Ok error

(** Decodes a query failure while rejecting the start-only conflict category.
    Query rejection is reported as a stable RPC/protocol error; it cannot
    carry an [Already_started] workflow identity. *)
let decode_query_error input =
  let* error = decode_client_error input in
  match error with
  | Already_started _ ->
      Error
        (invalid ~path:"$.kind"
           "already_started is not a valid query error")
  | (Rpc _ | Protocol _) -> Ok error

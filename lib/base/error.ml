(** Lists the broad error categories exposed by the public SDK. Keep it in sync
    with the public interface and with conversions from Temporal failures. *)
type category =
  [ `Activity
  | `Bridge
  | `Cancelled
  | `Child_workflow
  | `Codec
  | `Defect
  | `Nexus
  | `Terminated
  | `Timeout
  | `Update
  | `Workflow ]

type view = {
  category : category;
  message : string;
  non_retryable : bool;
  details : Payload.t list;
  error_type : string option;
}

(** The internal representation currently matches the fields visible through
    [view]. Public callers still use accessors, so this may change later. *)
type t = view

(** Copies one detail payload so [make] never aliases a caller's mutable
    [bytes] buffer. *)
let copy_detail (payload : Payload.t) : Payload.t =
  {
    Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Matches the Rust bridge's [MAX_STRING_BYTES]; see [error.mli]. *)
let max_error_type_bytes = 65_536

(** Counts the characters [value] occupies once JSON-escaped by the bridge
    transport, whose string limit is measured on that encoded form: a double
    quote or a backslash takes two characters; every other byte of an accepted
    type (control characters are rejected first) takes one. *)
let encoded_error_type_length value =
  String.fold_left
    (fun length character ->
      match character with '"' | '\\' -> length + 2 | _ -> length + 1)
    0 value

(** Normalizes an optional application error type. Empty means "no type" so
    that callers forwarding a possibly-empty wire value need no special case.
    Invalid text is rejected eagerly: deferring the check to the transport
    boundary would turn a construction mistake into an unrelated completion
    failure far from its source. Control characters are rejected because a
    type is an identifier and because each would expand to a six-character
    JSON escape, so a type within the byte limit could still exceed the
    transport's encoded string limit. *)
let normalize_error_type = function
  | None | Some "" -> None
  | Some value ->
      if not (String.is_valid_utf_8 value) then
        invalid_arg "Error.make: error_type is not valid UTF-8"
      else if String.exists (fun c -> Char.code c < 0x20 || Char.code c = 0x7f) value
      then invalid_arg "Error.make: error_type contains a control character"
      else if encoded_error_type_length value > max_error_type_bytes then
        invalid_arg "Error.make: error_type exceeds 65536 encoded characters"
      else Some value

(** Creates an error with the common defaults: retryable, untyped and without
    details. Detail payloads are deep-copied so later mutation of a caller's
    [bytes] cannot change an error already retained by the SDK. *)
let make ?(non_retryable = false) ?error_type ?(details = []) ~category
    ~message () =
  {
    category;
    message;
    non_retryable;
    details = List.map copy_detail details;
    error_type = normalize_error_type error_type;
  }

(** Returns a detached public view. Detail bytes stay mutable for application
    decoders, so copy them before crossing the abstract error boundary rather
    than allowing one inspection to mutate the retained error. *)
let view error = { error with details = List.map copy_detail error.details }

(** Converts a category to the lowercase name used in logs and metrics. *)
let kind error =
  match error.category with
  | `Activity -> "activity"
  | `Bridge -> "bridge"
  | `Cancelled -> "cancelled"
  | `Child_workflow -> "child_workflow"
  | `Codec -> "codec"
  | `Defect -> "defect"
  | `Nexus -> "nexus"
  | `Terminated -> "terminated"
  | `Timeout -> "timeout"
  | `Update -> "update"
  | `Workflow -> "workflow"

(** Provides the common field access and constructors used throughout the SDK. *)
let message error = error.message
let error_type error = error.error_type

(** Falls back to the category label so untyped errors keep the wire type they
    had before explicit types existed. *)
let application_failure_type error =
  match error.error_type with Some value -> value | None -> kind error

let codec ~message = make ~category:`Codec ~message ()
let defect ~message = make ~non_retryable:true ~category:`Defect ~message ()

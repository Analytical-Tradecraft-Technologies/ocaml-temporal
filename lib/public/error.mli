(** Identifies the part of the SDK or Temporal operation that failed. These
    broad categories are intended for pattern matching and metrics; [message]
    provides the more specific diagnostic. *)
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

(** The information application code can inspect about an SDK error.
    [non_retryable] records whether Temporal should avoid retrying the failure.
    [details] contains any additional raw Temporal payloads supplied with it.
    [error_type] is the application-defined failure type, Temporal's
    [ApplicationFailureInfo.type]; see {!make} and {!error_type}. *)
type view = {
  category : category;
  message : string;
  non_retryable : bool;
  details : Payload.t list;
  error_type : string option;
}

(** A failure returned by SDK operations through [result]. Expected failures,
    such as an activity error, cancellation, timeout, or invalid payload, are
    represented by this type rather than exceptions. *)
type t

(** Constructs a structured error at an application-facing boundary. Most
    callers should prefer the more specific [codec] or [defect] helpers, but
    this constructor is useful to adapters and custom activity implementations
    that need to preserve a Temporal category.

    [error_type] names the business failure, for example ["InvalidInput"]. It
    is sent as Temporal's [ApplicationFailureInfo.type] when an activity or
    workflow fails with this error, so it is what a retry policy's
    [non_retryable_error_types] list is matched against and what callers
    written in other Temporal SDKs see. When omitted, the lowercase category
    name ({!kind}) is sent instead. An empty string is treated as omitted.
    Raises [Invalid_argument] if [error_type] is not valid UTF-8, contains an
    ASCII control character, or is longer than 65536 characters once
    JSON-escaped (a double quote or backslash counts twice); such a value
    cannot be transmitted and indicates a programming error. *)
val make :
  ?non_retryable:bool ->
  ?error_type:string ->
  ?details:Payload.t list ->
  category:category ->
  message:string ->
  unit ->
  t

(** Returns all publicly inspectable fields of an error. Detail payloads are
    copied, so mutating bytes in the returned view cannot change the error or a
    later view. *)
val view : t -> view

(** Returns the lowercase category name, such as ["activity"] or ["codec"]. *)
val kind : t -> string

(** Returns the human-readable explanation of the failure. *)
val message : t -> string

(** Returns the application failure type, if any. For an error received from
    Temporal (an activity, child workflow, or workflow result) this is the type
    of the first application failure found walking the failure chain from the
    outermost layer inward (so an application failure nested under an
    activity or child-workflow wrapper is found, while an application failure
    wrapping another application failure reports its own type), as set by the
    code that raised it in any SDK; an OCaml error created without
    [~error_type] arrives with its category name. [None] means the failure had
    no application layer or its type was empty. *)
val error_type : t -> string option

(** Creates an error for a value that a custom codec could not encode or
    decode. *)
val codec : message:string -> t

(** Creates an error for an SDK bug or a violation of an API requirement.
    Its diagnostic [non_retryable] field is true. When propagated from workflow
    code, this fails the workflow task and leaves the execution open. Use
    [make ~category:`Workflow] for a deliberate terminal business failure. *)
val defect : message:string -> t

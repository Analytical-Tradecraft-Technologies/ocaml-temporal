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
    [details] contains any additional raw Temporal payloads supplied with it. *)
type view = {
  category : category;
  message : string;
  non_retryable : bool;
  details : Payload.t list;
}

(** A failure returned by SDK operations through [result]. Expected failures,
    such as an activity error, cancellation, timeout, or invalid payload, are
    represented by this type rather than exceptions. *)
type t

(** Constructs a structured error at an application-facing boundary. Most
    callers should prefer the more specific [codec] or [defect] helpers, but
    this constructor is useful to adapters and custom activity implementations
    that need to preserve a Temporal category. *)
val make :
  ?non_retryable:bool ->
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

(** Creates an error for a value that a custom codec could not encode or
    decode. *)
val codec : message:string -> t

(** Creates a non-retryable error for an SDK bug or a violation of an API
    requirement. *)
val defect : message:string -> t

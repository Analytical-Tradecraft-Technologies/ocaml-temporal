(** Broad categories describing what failed. They are less specific than
    individual Temporal failure messages, so the SDK can add diagnostic detail
    without breaking existing OCaml pattern matches. *)
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

(** Fields application code may inspect. [details] keeps any additional raw
    Temporal payloads for application-specific decoding. [non_retryable]
    records whether Temporal should avoid retrying the failure. [error_type] is
    the application-defined Temporal failure type ([ApplicationFailureInfo.type]
    on the wire), or [None] when the error carries no explicit type. *)
type view = {
  category : category;
  message : string;
  non_retryable : bool;
  details : Payload.t list;
  error_type : string option;
}

(** An abstract structured error. Expected Temporal failures travel as this
    type inside [result], never as control-flow exceptions. *)
type t

(** Constructs an error at a subsystem boundary. Details default to none and
    failures remain retryable unless the caller explicitly says otherwise.
    [error_type] defaults to absent; an empty string is normalized to absent.
    Raises [Invalid_argument] when [error_type] is not valid UTF-8 or exceeds
    {!max_error_type_bytes}, because such a value could never cross the Core
    bridge and indicates a programming error. *)
val make :
  ?non_retryable:bool ->
  ?error_type:string ->
  ?details:Payload.t list ->
  category:category ->
  message:string ->
  unit ->
  t

(** Returns all fields that application code may inspect. Detail payloads are
    copied, so mutating bytes in the returned view cannot change the error or a
    later view. *)
val view : t -> view

(** Returns a lowercase wire/log label for the error category. *)
val kind : t -> string

(** Returns the explicit application error type, if any. *)
val error_type : t -> string option

(** Returns the Temporal [ApplicationFailureInfo.type] emitted for this error:
    the explicit [error_type] when present, otherwise {!kind}. Keeping the
    category fallback preserves the wire type of errors that never set a type. *)
val application_failure_type : t -> string

(** Largest accepted [error_type] in bytes. It equals the bridge's protocol
    string limit, so an accepted type always fits in a Core completion. *)
val max_error_type_bytes : int

(** Returns the human-readable diagnostic without discarding structure. *)
val message : t -> string

(** Constructs a retryable serialization or payload-validation failure. *)
val codec : message:string -> t

(** Creates a non-retryable error for an SDK bug or violated API requirement. *)
val defect : message:string -> t

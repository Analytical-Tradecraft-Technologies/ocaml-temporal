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
    records whether Temporal should avoid retrying the failure. *)
type view = {
  category : category;
  message : string;
  non_retryable : bool;
  details : Payload.t list;
}

(** An abstract structured error. Expected Temporal failures travel as this
    type inside [result], never as control-flow exceptions. *)
type t

(** Constructs an error at a subsystem boundary. Details default to none and
    failures remain retryable unless the caller explicitly says otherwise. *)
val make :
  ?non_retryable:bool ->
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

(** Returns the human-readable diagnostic without discarding structure. *)
val message : t -> string

(** Constructs a retryable serialization or payload-validation failure. *)
val codec : message:string -> t

(** Creates a non-retryable error for an SDK bug or violated API requirement. *)
val defect : message:string -> t

(** Owns the structured errors returned by the public API. The representation is
    intentionally hidden by [error.mli]; private adapters copy this view into
    the native/base representation at the transport boundary. *)
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

type timeout_type =
  [ `Start_to_close | `Schedule_to_start | `Schedule_to_close | `Heartbeat | `Unspecified ]

type origin =
  [ `Application | `Timeout of timeout_type | `Server | `Cancelled | `Terminated | `Other ]

(** The inspectable [view] plus the received-failure facts of #1143, which are
    never transmitted to Temporal. *)
type t = {
  fields : view;
  origin : origin option;
  last_heartbeat_details : Payload.t list;
}

let copy_detail (payload : Payload.t) : Payload.t =
  {
    Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Uses the base error's normalization so a type accepted here can always be
    copied into the base representation by [Error_private.to_base] without
    raising there. *)
let normalize_error_type = Temporal_base.Error.normalize_error_type

(** Creates an error with the common defaults: retryable, untyped and without
    details. Detail payloads are deep-copied so later mutation of a caller's
    [bytes] cannot change an error already retained by the SDK. *)
let make ?(non_retryable = false) ?error_type ?(details = []) ?origin
    ?(last_heartbeat_details = []) ~category ~message () =
  {
    fields =
      {
        category;
        message;
        non_retryable;
        details = List.map copy_detail details;
        error_type = normalize_error_type error_type;
      };
    origin;
    last_heartbeat_details = List.map copy_detail last_heartbeat_details;
  }

(** Returns a detached public view. Detail bytes stay mutable for application
    decoders, so copy them before crossing the abstract error boundary rather
    than allowing one inspection to mutate the retained error. *)
let view { fields; _ } = { fields with details = List.map copy_detail fields.details }

let kind error =
  match error.fields.category with
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

let message error = error.fields.message
let error_type error = error.fields.error_type
let origin error = error.origin

let timeout_type error =
  match error.origin with Some (`Timeout timeout) -> Some timeout | Some _ | None -> None

(** Copies the payloads as [view] does, for the same reason. *)
let last_heartbeat_details error = List.map copy_detail error.last_heartbeat_details
let codec ~message = make ~category:`Codec ~message ()
let defect ~message = make ~non_retryable:true ~category:`Defect ~message ()

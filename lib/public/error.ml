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
}

type t = view

let copy_detail (payload : Payload.t) : Payload.t =
  {
    Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Creates an error with the common defaults: retryable and without details.
    Detail payloads are deep-copied so later mutation of a caller's [bytes]
    cannot change an error already retained by the SDK. *)
let make ?(non_retryable = false) ?(details = []) ~category ~message () =
  { category; message; non_retryable; details = List.map copy_detail details }

(** Returns a detached public view. Detail bytes stay mutable for application
    decoders, so copy them before crossing the abstract error boundary rather
    than allowing one inspection to mutate the retained error. *)
let view error = { error with details = List.map copy_detail error.details }

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

let message error = error.message
let codec ~message = make ~category:`Codec ~message ()
let defect ~message = make ~non_retryable:true ~category:`Defect ~message ()

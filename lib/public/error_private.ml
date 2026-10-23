(** Converts structured errors at the boundary between the public API and the
    package-private base/runtime libraries. Details are copied through the
    payload adapter so no internal record or mutable byte buffer leaks. *)

(* Copies a base error into the public view returned to application code. *)
(** Copies a base error into an owned public value so no private record escapes
    through a result returned by the public API. *)
let of_base (error : Temporal_base.Error.t) : Error.t =
  let view = Temporal_base.Error.view error in
  Error.make ~non_retryable:view.non_retryable ?error_type:view.error_type
    ~details:(List.map Payload_private.of_base view.details)
    ~category:view.category ~message:view.message ()

(* Copies a public error into the base representation required by Core. *)
(** Rebuilds a base error for Rust/Core adapters without sharing its private
    representation with application code. *)
let to_base (error : Error.t) : Temporal_base.Error.t =
  let view = Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ?error_type:view.error_type ~details:(List.map Payload_private.to_base view.details)
    ~category:view.category ~message:view.message ()

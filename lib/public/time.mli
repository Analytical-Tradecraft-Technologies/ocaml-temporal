(** Deterministic instants for workflow code.

    Workflow code must not read the host wall clock; it obtains the current
    time from {!Workflow.now}, which returns this replay-stable type. *)

(** A deterministic instant supplied by Temporal during workflow replay. *)
type t

(** Builds an instant from a Unix timestamp without floating-point rounding.
    [nanoseconds] must satisfy [0 <= nanoseconds < 1_000_000_000]. Invalid
    components return a non-retryable defect. *)
val of_unix : seconds:int64 -> nanoseconds:int -> (t, Error.t) result

(** Returns the whole Unix seconds component. *)
val seconds : t -> int64

(** Returns the normalized nanosecond fraction. *)
val nanoseconds : t -> int

(** Compares two instants using seconds first and nanoseconds second. *)
val compare : t -> t -> int

(** Tests exact equality of two instants. *)
val equal : t -> t -> bool

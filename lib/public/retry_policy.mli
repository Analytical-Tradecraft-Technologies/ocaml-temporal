(** A deterministic Temporal retry policy, shared by activities
    ([Activity.start]), child workflows ([Child_workflow.start]), and
    client-started workflows ([Client.start]).

    [initial_interval] and [maximum_interval] are positive whole-millisecond
    durations, with the maximum at least as large as the initial delay.
    [backoff_coefficient] must be finite and at least [1.0].  A
    [maximum_attempts] value of [0] means that Temporal imposes no attempt
    count limit; positive values include the initial attempt.
    [non_retryable_error_types] lists application error types that stop
    retries; Temporal matches them against the failure type set by
    [Error.make ~error_type] (or the category name, {!Error.kind}, for an
    untyped error).  The constructor returns a typed defect instead of
    raising so callers can validate policy configuration while assembling a
    workflow definition. *)

(** Opaque immutable retry policy validated before command construction. *)
type t

(** Validates and constructs an immutable retry policy. *)
val make :
  initial_interval:Duration.t ->
  backoff_coefficient:float ->
  maximum_interval:Duration.t ->
  maximum_attempts:int ->
  ?non_retryable_error_types:string list ->
  unit ->
  (t, Error.t) result

(** Returns the exact initial retry delay. *)
val initial_interval : t -> Duration.t

(** Returns the finite multiplier applied between retry attempts. *)
val backoff_coefficient : t -> float

(** Returns the cap applied to the retry delay. *)
val maximum_interval : t -> Duration.t

(** Returns the maximum number of attempts; [0] means unlimited. *)
val maximum_attempts : t -> int

(** Returns the immutable list of Temporal error type names that must not be
    retried. *)
val non_retryable_error_types : t -> string list

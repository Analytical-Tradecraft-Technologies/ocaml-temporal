(** A deterministic Temporal retry policy, shared by activities
    ([Activity.start]), child workflows ([Child_workflow.start]), and
    client-started workflows ([Client.start]).

    Every field is optional. An omitted field is sent to Temporal as unset
    and the server (or Temporal Core, for a local activity) applies its
    default: an initial interval of 1 second, a backoff coefficient of 2.0,
    a maximum interval of 100 times the initial interval, and no attempt
    limit. Server operators can change these defaults per cluster, which is
    why the SDK does not substitute them itself. So
    [Retry_policy.make ~maximum_attempts:3 ()] keeps Temporal's backoff and
    only caps the attempt count. A workflow can read the effective policy
    that the server recorded for its own run, with the defaults filled in,
    through [Workflow.Info.retry_policy].

    Supplied fields are validated:
    - [initial_interval] and [maximum_interval] are positive whole-millisecond
      durations. When both are given the maximum must be at least the initial
      interval. When only [maximum_interval] is given it must be at least
      1 second, Temporal's default initial interval, because the server would
      otherwise reject every command carrying the policy (set
      [initial_interval] explicitly to use a shorter maximum).
    - [backoff_coefficient] must be finite and at least [1.0].
    - [maximum_attempts] must be between [0] and [Int32.max_int]
      (2,147,483,647), the range of Temporal's signed 32-bit field. Positive
      values include the initial attempt. [0] is identical to omitting it:
      the wire format cannot tell them apart, and Temporal reads both as
      "use the default", which is unlimited unless the operator configured a
      limit.
    - [non_retryable_error_types] lists application error types that stop
      retries; Temporal matches them against the failure type set by
      [Error.make ~error_type] (or the category name, {!Error.kind}, for an
      untyped error). Each name must be non-empty, valid UTF-8, free of NUL,
      and at most 65,536 bytes.

    The constructor returns a typed defect instead of raising so callers can
    validate policy configuration while assembling a workflow definition. *)

(** Opaque immutable retry policy validated before command construction. *)
type t

(** Validates and constructs an immutable retry policy. Omitted fields take
    Temporal's server-side defaults (see the module description). *)
val make :
  ?initial_interval:Duration.t ->
  ?backoff_coefficient:float ->
  ?maximum_interval:Duration.t ->
  ?maximum_attempts:int ->
  ?non_retryable_error_types:string list ->
  unit ->
  (t, Error.t) result

(** Returns the exact initial retry delay, or [None] when the server default
    applies. *)
val initial_interval : t -> Duration.t option

(** Returns the finite multiplier applied between retry attempts, or [None]
    when the server default applies. *)
val backoff_coefficient : t -> float option

(** Returns the cap applied to the retry delay, or [None] when the server
    default applies. *)
val maximum_interval : t -> Duration.t option

(** Returns the maximum number of attempts. [0] means that no limit was
    chosen (the server default, unlimited unless configured otherwise). *)
val maximum_attempts : t -> int

(** Returns the immutable list of Temporal error type names that must not be
    retried. *)
val non_retryable_error_types : t -> string list

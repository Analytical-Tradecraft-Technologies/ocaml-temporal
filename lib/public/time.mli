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

(** [add instant duration] returns the instant [duration] later, exactly
    (milliseconds are carried into the nanosecond fraction). A result past
    the last representable instant ([Int64.max_int] seconds plus 999,999,999
    nanoseconds, about 292 billion years from 1970) saturates there instead
    of wrapping or raising. Temporal timestamps end at year 9999, so
    saturation never changes a result that Temporal could use. Like
    {!Duration.add}, the operation is total and deterministic, which keeps
    "start time plus 24 hours" free of [result] plumbing in workflow code:

    {[
      let deadline start = Temporal.Time.add start (Temporal.Duration.of_hours 24L)
    ]} *)
val add : t -> Duration.t -> t

(** [diff later earlier] returns the elapsed time from [earlier] to [later],
    truncated to whole milliseconds. It is {!Duration.t}, which cannot be
    negative, so the result is zero when [later] is not after [earlier]
    (compare the instants with {!compare} when the order matters), and it
    saturates at {!Duration.maximum} for gaps beyond 10,000 years. For
    example, a workflow can test whether seven days have passed since
    [started]:

    {[
      Temporal.Duration.compare
        (Temporal.Time.diff now started)
        (Temporal.Duration.of_days 7L)
      >= 0
    ]} *)
val diff : t -> t -> Duration.t

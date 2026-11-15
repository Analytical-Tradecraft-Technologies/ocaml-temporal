(** Non-negative, millisecond-precision durations for workflow timers and
    timeouts. *)

(** A non-negative length of time represented in whole milliseconds. Workflow
    timers use this type so their requested duration is recorded exactly and
    can be reproduced during replay. *)
type t

(** Creates a duration from milliseconds. A negative value, or one above
    315,576,000,000,999 ms (the protobuf [Duration] maximum of 10,000 years),
    raises [Invalid_argument] because it is a programming error; Temporal
    would otherwise reject the resulting command on every workflow task. *)
val of_ms : int64 -> t

(** Creates a duration from whole seconds, with the same bounds and
    [Invalid_argument] contract as {!of_ms}. The range check runs before
    scaling, so no input can overflow into an accepted value. *)
val of_seconds : int64 -> t

(** Creates a duration from whole minutes; see {!of_seconds}. *)
val of_minutes : int64 -> t

(** Creates a duration from whole hours; see {!of_seconds}. *)
val of_hours : int64 -> t

(** Creates a duration from whole 24-hour days; see {!of_seconds}. A day is
    exactly 86,400,000 ms here: durations are elapsed time and never follow
    calendar or daylight-saving rules. *)
val of_days : int64 -> t

(** The largest representable duration, 315,576,000,000,999 ms (the protobuf
    [Duration] maximum of 10,000 years). Arithmetic saturates here. *)
val maximum : t

(** Adds two durations, saturating at {!maximum} instead of overflowing or
    raising. Saturation never changes a result that is representable: any
    sum above {!maximum} would be rejected by Temporal anyway, and 10,000 years
    is indistinguishable from "never" for a timer or timeout. Arithmetic on
    already validated values is total so workflow code can use it without
    [result] plumbing or exceptions. *)
val add : t -> t -> t

(** Orders durations by length; the result follows [Int64.compare]. *)
val compare : t -> t -> int

(** Returns the exact number of milliseconds supplied to [of_ms]. *)
val to_ms : t -> int64

(** Returns the number of whole seconds, truncating any millisecond
    remainder, so [to_seconds (of_ms 1_999L) = 1L]. *)
val to_seconds : t -> int64

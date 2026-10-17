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

(** Returns the exact number of milliseconds supplied to [of_ms]. *)
val to_ms : t -> int64

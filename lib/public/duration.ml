(** Stores whole milliseconds in the public API without exposing the private
    base library's type path. The value is validated at construction and is
    converted to an integer only when a command is assembled. *)
type t = int64

(** protobuf [Duration] accepts at most 315,576,000,000 seconds (10,000
    years) plus up to 999,999,999 nanoseconds, so the largest whole-millisecond
    value is 315,576,000,000,999 ms. Every SDK duration eventually becomes a
    protobuf duration in a Temporal command, and the server rejects larger
    values on every workflow task retry, so the bound is enforced at
    construction. *)
let max_ms = 315_576_000_000_999L

let of_ms milliseconds =
  if Int64.compare milliseconds 0L < 0 then
    invalid_arg "Temporal duration cannot be negative";
  if Int64.compare milliseconds max_ms > 0 then
    invalid_arg "Temporal duration exceeds the protobuf maximum of 10,000 years";
  milliseconds

(** Scales a whole-unit count to milliseconds. The bound is compared against
    [max_ms / unit_ms] before multiplying, so a large count is rejected
    rather than wrapping around [Int64] into a plausible duration. *)
let of_unit ~unit_ms count =
  if Int64.compare count 0L < 0 then
    invalid_arg "Temporal duration cannot be negative";
  if Int64.compare count (Int64.div max_ms unit_ms) > 0 then
    invalid_arg "Temporal duration exceeds the protobuf maximum of 10,000 years";
  Int64.mul count unit_ms

let of_seconds = of_unit ~unit_ms:1_000L
let of_minutes = of_unit ~unit_ms:60_000L
let of_hours = of_unit ~unit_ms:3_600_000L

let to_ms duration = duration

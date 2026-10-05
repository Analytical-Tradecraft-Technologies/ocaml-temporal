(** Stores whole milliseconds in the public API without exposing the private
    base library's type path. The value is validated at construction and is
    converted to an integer only when a command is assembled. *)
type t = int64

(** protobuf [Duration] accepts at most 315,576,000,000 seconds (10,000
    years). Every SDK duration eventually becomes a protobuf duration in a
    Temporal command, and the server rejects larger values on every workflow
    task retry, so the bound is enforced at construction. *)
let max_ms = 315_576_000_000_000L

let of_ms milliseconds =
  if Int64.compare milliseconds 0L < 0 then
    invalid_arg "Temporal duration cannot be negative";
  if Int64.compare milliseconds max_ms > 0 then
    invalid_arg "Temporal duration exceeds the protobuf maximum of 10,000 years";
  milliseconds

let to_ms duration = duration

(** Stores the exact timestamp representation used by Temporal's activation
    protocol. Keeping this representation integer-only makes workflow replay
    independent of local timezone and floating-point behavior. *)
type t = {
  seconds : int64;
  nanoseconds : int;
}

(** Rejects a fractional component that cannot be represented by the protocol.
    Temporal normalizes fractions to a single non-negative second, so negative
    values and one-billion nanoseconds are both invalid. *)
let valid_nanoseconds nanoseconds =
  nanoseconds >= 0 && nanoseconds < 1_000_000_000

(** Constructs a validated timestamp for workflow code. The error is typed so
    callers can handle malformed application-provided values without relying
    on exceptions for normal control flow. *)
let of_unix ~seconds ~nanoseconds =
  if valid_nanoseconds nanoseconds then Ok { seconds; nanoseconds }
  else
    Error
      (Error.defect
         ~message:
           "Temporal.Time nanoseconds must be between 0 and 999999999")

(** Returns the signed whole-second component. *)
let seconds instant = instant.seconds

(** Returns the normalized fractional component. *)
let nanoseconds instant = instant.nanoseconds

(** Compares timestamps lexicographically without converting to a lossy float. *)
let compare left right =
  let seconds_order = Int64.compare left.seconds right.seconds in
  if seconds_order <> 0 then seconds_order
  else Int.compare left.nanoseconds right.nanoseconds

(** Tests exact equality using both timestamp components. *)
let equal left right =
  Int64.equal left.seconds right.seconds
  && Int.equal left.nanoseconds right.nanoseconds

(** The saturation point of {!add}: the last instant the representation can
    hold. *)
let latest = { seconds = Int64.max_int; nanoseconds = 999_999_999 }

(** Splits the duration into whole seconds and a nanosecond fraction, adds
    the fractions first so a carry is known, and only then adds the seconds.
    The overflow test [seconds > max_int - (whole + carry)] cannot itself
    overflow because [whole + carry] is at most about [3.2e11]. *)
let add instant duration =
  let milliseconds = Duration.to_ms duration in
  let whole = Int64.div milliseconds 1_000L in
  let fraction_ns = Int64.to_int (Int64.rem milliseconds 1_000L) * 1_000_000 in
  let nanoseconds = instant.nanoseconds + fraction_ns in
  let carry, nanoseconds =
    if nanoseconds >= 1_000_000_000 then (1L, nanoseconds - 1_000_000_000)
    else (0L, nanoseconds)
  in
  let increment = Int64.add whole carry in
  if Int64.compare instant.seconds (Int64.sub Int64.max_int increment) > 0 then
    latest
  else { seconds = Int64.add instant.seconds increment; nanoseconds }

(** Computes [later - earlier] without overflow: both seconds fields can be
    any [Int64], so their difference is only taken after checking that it
    fits, and anything past the duration bound saturates. Truncation to
    milliseconds matches the precision of {!Duration.t}. *)
let diff later earlier =
  if compare later earlier <= 0 then Duration.of_ms 0L
  else
    let maximum_seconds = Int64.div (Duration.to_ms Duration.maximum) 1_000L in
    (* [later > earlier] implies [later.seconds >= earlier.seconds]. The
       subtraction can only wrap when [earlier.seconds] is negative and the
       true difference exceeds [Int64.max_int]. *)
    let wraps =
      Int64.compare earlier.seconds 0L < 0
      && Int64.compare later.seconds (Int64.add Int64.max_int earlier.seconds)
         > 0
    in
    if wraps then Duration.maximum
    else
      let seconds = Int64.sub later.seconds earlier.seconds in
      let seconds, nanoseconds =
        let fraction = later.nanoseconds - earlier.nanoseconds in
        if fraction < 0 then (Int64.pred seconds, fraction + 1_000_000_000)
        else (seconds, fraction)
      in
      if Int64.compare seconds maximum_seconds > 0 then Duration.maximum
      else
        let milliseconds =
          Int64.add (Int64.mul seconds 1_000L)
            (Int64.of_int (nanoseconds / 1_000_000))
        in
        Duration.of_ms (Int64.min milliseconds (Duration.to_ms Duration.maximum))

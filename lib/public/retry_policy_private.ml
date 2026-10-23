(** Converts the public retry-policy value into the compact runtime record
    consumed by command construction.  This module is private to the SDK
    library: callers can configure a policy without depending on the bridge's
    wire representation or on the Rust-facing runtime types. *)

(** Formats an arbitrary IEEE-754 bit pattern as the unsigned decimal string
    required by the semantic JSON protocol.  The signed OCaml [Int64] printer
    cannot represent values whose high bit is set, so this uses two unsigned
    base-[2^32] limbs and long division by ten. *)
let unsigned_int64_decimal bits =
  if Int64.compare bits 0L >= 0 then Int64.to_string bits
  else
    let base = 4_294_967_296L in
    let high = Int64.logand (Int64.shift_right_logical bits 32) 0xffff_ffffL in
    let low = Int64.logand bits 0xffff_ffffL in
    let rec digits high low acc =
      if Int64.equal high 0L && Int64.equal low 0L then acc
      else
        let high_quotient = Int64.div high 10L in
        let high_remainder = Int64.rem high 10L in
        let combined = Int64.add (Int64.mul high_remainder base) low in
        let low_quotient = Int64.div combined 10L in
        let digit = Int64.to_int (Int64.rem combined 10L) in
        digits high_quotient low_quotient
          (Char.chr (Char.code '0' + digit) :: acc)
    in
    digits high low []
    |> List.map (String.make 1)
    |> String.concat ""

(** Copies a validated policy at the last public-to-runtime boundary.  The
    accessors preserve the exact float chosen by the caller; converting its
    bits again therefore produces the same replay-stable representation as
    activity commands without exposing the private record field. *)
let to_runtime (policy : Retry_policy.t) =
  Temporal_sdk_kernel.Activation
  .{
    initial_interval =
      Duration.to_ms (Retry_policy.initial_interval policy);
    backoff_coefficient_bits =
      unsigned_int64_decimal
        (Int64.bits_of_float
           (Retry_policy.backoff_coefficient policy));
    maximum_interval =
      Duration.to_ms (Retry_policy.maximum_interval policy);
    maximum_attempts = Retry_policy.maximum_attempts policy;
    non_retryable_error_types =
      Retry_policy.non_retryable_error_types policy;
  }

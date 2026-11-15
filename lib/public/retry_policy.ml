(* Public retry policy shared by activity, child-workflow, and client starts.
    The record is hidden by [retry_policy.mli]; the private
    [Retry_policy_private] module converts it to the runtime command record. *)
(** A validated policy. [None] marks a field the caller left unset, which
    reaches Temporal as the protocol's zero value so the server applies its
    own default. The coefficient is kept as the caller's exact float;
    {!Retry_policy_private.to_runtime} derives its IEEE-754 bits for the JSON
    bridge, so replay sees precisely the value selected by the caller. *)
type t = {
  (* Delay before the first retry attempt, represented exactly in ms. *)
  initial_interval : Duration.t option;
  (* Finite multiplier, at least 1.0, applied to each retry delay. *)
  backoff_coefficient : float option;
  (* Upper bound applied to computed retry delays. *)
  maximum_interval : Duration.t option;
  (* Maximum attempt count; zero means unset (Temporal's default limit). *)
  maximum_attempts : int;
  (* Error type names that should fail without another attempt. *)
  non_retryable_error_types : string list;
}

(** Temporal's default initial interval in milliseconds. It is used only to
    reject a lone [maximum_interval] that the server would refuse once it
    fills in the default initial interval; the SDK never sends it. *)
let server_default_initial_interval_ms = 1_000L

(** Validates names embedded in the retry policy.  Keeping this validation at
    construction means a policy cannot later fail merely because a worker
    tries to serialize it. *)
let validate_non_retryable_error_type index value =
  let field = "non_retryable_error_types[" ^ string_of_int index ^ "]" in
  if String.equal value "" then
    Error (Error.defect ~message:(field ^ " must not be empty"))
  else if String.contains value '\000' then
    Error (Error.defect ~message:(field ^ " must not contain NUL"))
  else if String.length value > 65_536 then
    Error (Error.defect ~message:(field ^ " exceeds 65536 bytes"))
  else if not (Temporal_base.Codec.valid_utf_8 value) then
    Error (Error.defect ~message:(field ^ " must be valid UTF-8"))
  else Ok ()

(** Checks the optional intervals. Zero is reserved on the wire for "unset",
    so a supplied interval must be positive; the ordering rule mirrors the
    check Temporal applies after it has filled in its defaults. *)
let validate_intervals initial_interval maximum_interval =
  let invalid message = Error (Error.defect ~message) in
  let initial_ms = Option.map Duration.to_ms initial_interval in
  let maximum_ms = Option.map Duration.to_ms maximum_interval in
  match (initial_ms, maximum_ms) with
  | Some initial, _ when Int64.compare initial 0L <= 0 ->
      invalid "retry policy initial_interval must be positive"
  | _, Some maximum when Int64.compare maximum 0L <= 0 ->
      invalid "retry policy maximum_interval must be positive"
  | Some initial, Some maximum when Int64.compare maximum initial < 0 ->
      invalid "retry policy maximum_interval must be at least initial_interval"
  | None, Some maximum
    when Int64.compare maximum server_default_initial_interval_ms < 0 ->
      invalid
        "retry policy maximum_interval below 1s requires an explicit \
         initial_interval"
  | _ -> Ok ()

(** Checks a supplied coefficient. Zero is the wire's "unset" value and is
    rejected here together with every other value below 1.0. *)
let validate_coefficient = function
  | None -> Ok ()
  | Some coefficient -> (
      let invalid message = Error (Error.defect ~message) in
      match classify_float coefficient with
      | FP_nan | FP_infinite ->
          invalid "retry policy backoff_coefficient must be finite"
      | FP_zero | FP_subnormal | FP_normal when coefficient < 1.0 ->
          invalid "retry policy backoff_coefficient must be at least 1.0"
      | FP_zero | FP_subnormal | FP_normal -> Ok ())

(** Validates and constructs a policy without allowing invalid values to
    cross into workflow history.  The checks mirror Temporal Core's retry
    policy constraints and are deliberately expressed as [result] values so
    routine configuration failures never use exceptions as control flow. *)
let make ?initial_interval ?backoff_coefficient ?maximum_interval
    ?(maximum_attempts = 0) ?(non_retryable_error_types = []) () =
  let ( let* ) = Result.bind in
  let* () = validate_intervals initial_interval maximum_interval in
  let* () = validate_coefficient backoff_coefficient in
  let maximum_int32 = Int32.to_int Int32.max_int in
  let* () =
    if maximum_attempts < 0 || maximum_attempts > maximum_int32 then
      Error
        (Error.defect
           ~message:
             "retry policy maximum_attempts must be between 0 and \
              Int32.max_int")
    else Ok ()
  in
  let rec validate_types index = function
    | [] -> Ok ()
    | value :: rest ->
        let* () = validate_non_retryable_error_type index value in
        validate_types (index + 1) rest
  in
  let* () = validate_types 0 non_retryable_error_types in
  Ok
    {
      initial_interval;
      backoff_coefficient;
      maximum_interval;
      maximum_attempts;
      non_retryable_error_types = List.map Fun.id non_retryable_error_types;
    }

(** Returns the exact delay before the first retry, if one was chosen. *)
let initial_interval policy = policy.initial_interval

(** Returns the multiplier applied to the previous retry delay, if chosen. *)
let backoff_coefficient policy = policy.backoff_coefficient

(** Returns the upper bound for a retry delay, if one was chosen. *)
let maximum_interval policy = policy.maximum_interval

(** Returns the attempt cap, where zero means no limit was chosen. *)
let maximum_attempts policy = policy.maximum_attempts

(** A fresh list protects the policy's immutable representation from callers
    that retain and mutate a list value they received from an accessor. *)
let non_retryable_error_types policy =
  List.map Fun.id policy.non_retryable_error_types

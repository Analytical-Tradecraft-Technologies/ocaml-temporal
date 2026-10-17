(* Public retry policy shared by activity, child-workflow, and client starts.
    The record is hidden by [retry_policy.mli]; the private
    [Retry_policy_private] module converts it to the runtime command record. *)
(** A validated policy. The coefficient is kept as the caller's exact float;
    {!Retry_policy_private.to_runtime} derives its IEEE-754 bits for the JSON
    bridge, so replay sees precisely the value selected by the caller. *)
type t = {
  (* Delay before the first retry attempt, represented exactly in ms. *)
  initial_interval : Duration.t;
  (* Finite multiplier applied to each retry delay. *)
  backoff_coefficient : float;
  (* Upper bound applied to computed retry delays. *)
  maximum_interval : Duration.t;
  (* Maximum attempt count; zero means that Temporal imposes no limit. *)
  maximum_attempts : int;
  (* Error type names that should fail without another attempt. *)
  non_retryable_error_types : string list;
}

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

(** Validates and constructs a policy without allowing invalid values to
    cross into workflow history.  The checks mirror Temporal Core's retry
    policy constraints and are deliberately expressed as [result] values so
    routine configuration failures never use exceptions as control flow. *)
let make ~initial_interval ~backoff_coefficient ~maximum_interval
    ~maximum_attempts ?(non_retryable_error_types = []) () =
  let initial_ms = Duration.to_ms initial_interval in
  let maximum_ms = Duration.to_ms maximum_interval in
  let invalid message = Error (Error.defect ~message) in
  if Int64.compare initial_ms 0L <= 0 then
    invalid "retry policy initial_interval must be positive"
  else if Int64.compare maximum_ms initial_ms < 0 then
    invalid
      "retry policy maximum_interval must be at least initial_interval"
  else
    match classify_float backoff_coefficient with
    | FP_nan | FP_infinite ->
        invalid "retry policy backoff_coefficient must be finite"
    | FP_zero | FP_subnormal | FP_normal when backoff_coefficient < 1.0 ->
        invalid "retry policy backoff_coefficient must be at least 1.0"
    | FP_zero | FP_subnormal | FP_normal ->
        let maximum_int32 = Int32.to_int Int32.max_int in
        if maximum_attempts < 0 || maximum_attempts > maximum_int32 then
          invalid
            "retry policy maximum_attempts must be between 0 and Int32.max_int"
        else
          let rec validate_types index = function
            | [] -> Ok ()
            | value :: rest -> (
                match validate_non_retryable_error_type index value with
                | Error _ as error -> error
                | Ok () -> validate_types (index + 1) rest)
          in
          (match validate_types 0 non_retryable_error_types with
          | Error _ as error -> error
          | Ok () ->
              Ok
                {
                  initial_interval;
                  backoff_coefficient;
                  maximum_interval;
                  maximum_attempts;
                  non_retryable_error_types =
                    List.map Fun.id non_retryable_error_types;
                })

(** Returns the exact delay before the first retry. *)
let initial_interval policy = policy.initial_interval

(** Returns the multiplier applied to the previous retry delay. *)
let backoff_coefficient policy = policy.backoff_coefficient

(** Returns the upper bound for a retry delay. *)
let maximum_interval policy = policy.maximum_interval

(** Returns the attempt cap, where zero means unlimited attempts. *)
let maximum_attempts policy = policy.maximum_attempts

(** A fresh list protects the policy's immutable representation from callers
    that retain and mutate a list value they received from an accessor. *)
let non_retryable_error_types policy =
  List.map Fun.id policy.non_retryable_error_types

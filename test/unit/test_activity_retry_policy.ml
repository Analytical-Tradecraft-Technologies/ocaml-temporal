module Retry_policy = Temporal.Retry_policy

(** Builds a valid policy shared by the accessor and boundary tests. *)
let valid_policy () =
  match
    Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1_000L)
      ~backoff_coefficient:1.5
      ~maximum_interval:(Temporal.Duration.of_ms 60_000L)
      ~maximum_attempts:3
      ~non_retryable_error_types:[ "InvalidInput" ] ()
  with
  | Ok policy -> policy
  | Error error ->
      failwith
        ("valid retry policy was rejected: " ^ Temporal.Error.message error)

(** Builds the smallest policy that exercises the zero-attempts and
    sub-second-duration boundaries.  Temporal interprets zero attempts as
    unlimited retries, while the one-millisecond values ensure the bridge does
    not silently round an interval to whole seconds. *)
let boundary_policy () =
  match
    Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
      ~backoff_coefficient:1.0
      ~maximum_interval:(Temporal.Duration.of_ms 1_001L) ~maximum_attempts:0 ()
  with
  | Ok policy -> policy
  | Error error ->
      failwith
        ("boundary retry policy was rejected: " ^ Temporal.Error.message error)

(** Requires a constructor failure to be a typed defect rather than an
    exception, preserving the public API's result-based error convention. *)
let expect_invalid label result =
  match result with
  | Error error when Temporal.Error.kind error = "defect" -> ()
  | Error _ -> failwith (label ^ " returned the wrong error category")
  | Ok _ -> failwith (label ^ " accepted an invalid retry policy")

let () =
  let policy = valid_policy () in
  assert (
    Temporal.Duration.to_ms (Retry_policy.initial_interval policy) = 1_000L);
  assert (Retry_policy.backoff_coefficient policy = 1.5);
  assert (
    Temporal.Duration.to_ms (Retry_policy.maximum_interval policy) = 60_000L);
  assert (Retry_policy.maximum_attempts policy = 3);
  assert (Retry_policy.non_retryable_error_types policy = [ "InvalidInput" ]);
  let boundary = boundary_policy () in
  assert (
    Temporal.Duration.to_ms (Retry_policy.initial_interval boundary) = 1L);
  assert (
    Temporal.Duration.to_ms (Retry_policy.maximum_interval boundary) = 1_001L);
  assert (Retry_policy.backoff_coefficient boundary = 1.0);
  (* Zero is a meaningful policy value, not a failed constructor default: it
     delegates the attempt limit to Temporal. *)
  assert (Retry_policy.maximum_attempts boundary = 0);
  let maximum_attempts = Int32.to_int Int32.max_int in
  let maximum_policy =
    match
      Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
        ~backoff_coefficient:1.0
        ~maximum_interval:(Temporal.Duration.of_ms 1L) ~maximum_attempts ()
    with
    | Ok policy -> policy
    | Error error ->
        failwith
          ("maximum-attempt retry policy was rejected: "
          ^ Temporal.Error.message error)
  in
  assert (Retry_policy.maximum_attempts maximum_policy = maximum_attempts);
  expect_invalid "zero initial interval"
    (Retry_policy.make
       ~initial_interval:(Temporal.Duration.of_ms 0L)
       ~backoff_coefficient:1.0
       ~maximum_interval:(Temporal.Duration.of_ms 1L) ~maximum_attempts:0 ());
  expect_invalid "sub-unit coefficient"
    (Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
       ~backoff_coefficient:0.99
       ~maximum_interval:(Temporal.Duration.of_ms 1L) ~maximum_attempts:0 ());
  expect_invalid "infinite coefficient"
    (Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
       ~backoff_coefficient:infinity
       ~maximum_interval:(Temporal.Duration.of_ms 1L) ~maximum_attempts:0 ());
  expect_invalid "NaN coefficient"
    (Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
       ~backoff_coefficient:nan
       ~maximum_interval:(Temporal.Duration.of_ms 1L) ~maximum_attempts:0 ());
  expect_invalid "maximum interval below initial"
    (Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 2L)
       ~backoff_coefficient:1.0
       ~maximum_interval:(Temporal.Duration.of_ms 1L) ~maximum_attempts:0 ());
  expect_invalid "negative attempts"
    (Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
       ~backoff_coefficient:1.0
       ~maximum_interval:(Temporal.Duration.of_ms 1L) ~maximum_attempts:(-1) ());
  expect_invalid "attempts above signed 32-bit range"
    (Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
       ~backoff_coefficient:1.0
       ~maximum_interval:(Temporal.Duration.of_ms 1L)
       ~maximum_attempts:(Int32.to_int Int32.max_int + 1) ());
  expect_invalid "empty non-retryable error type"
    (Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
       ~backoff_coefficient:1.0
       ~maximum_interval:(Temporal.Duration.of_ms 1L) ~maximum_attempts:0
       ~non_retryable_error_types:[ "" ] ());
  expect_invalid "NUL non-retryable error type"
    (Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
       ~backoff_coefficient:1.0
       ~maximum_interval:(Temporal.Duration.of_ms 1L) ~maximum_attempts:0
       ~non_retryable_error_types:[ "bad\000type" ] ())

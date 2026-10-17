(* Unit tests for the whole-unit [Temporal.Duration] constructors. They pin
   exact millisecond scaling, the shared negative and protobuf-maximum bounds,
   and that a count large enough to overflow [Int64] when scaled is rejected
   instead of wrapping into an accepted duration. *)

module Duration = Temporal.Duration

(** Asserts that [f ()] raises [Invalid_argument]; [label] names the case in
    the failure message. *)
let expect_invalid label f =
  match f () with
  | _ -> failwith (label ^ " was accepted")
  | exception Invalid_argument _ -> ()

(** Each constructor scales exactly to milliseconds, including zero. *)
let test_scaling () =
  assert (Duration.to_ms (Duration.of_seconds 0L) = 0L);
  assert (Duration.to_ms (Duration.of_seconds 30L) = 30_000L);
  assert (Duration.to_ms (Duration.of_minutes 2L) = 120_000L);
  assert (Duration.to_ms (Duration.of_hours 3L) = 10_800_000L)

(** The largest accepted count of each unit is the protobuf maximum rounded
    down to that unit; one more is rejected, as is any negative count. *)
let test_bounds () =
  assert (
    Duration.to_ms (Duration.of_seconds 315_576_000_000L)
    = 315_576_000_000_000L);
  expect_invalid "seconds above maximum" (fun () ->
      Duration.of_seconds 315_576_000_001L);
  assert (
    Duration.to_ms (Duration.of_minutes 5_259_600_000L) = 315_576_000_000_000L);
  expect_invalid "minutes above maximum" (fun () ->
      Duration.of_minutes 5_259_600_001L);
  assert (
    Duration.to_ms (Duration.of_hours 87_660_000L) = 315_576_000_000_000L);
  expect_invalid "hours above maximum" (fun () -> Duration.of_hours 87_660_001L);
  expect_invalid "negative seconds" (fun () -> Duration.of_seconds (-1L));
  expect_invalid "negative minutes" (fun () -> Duration.of_minutes (-1L));
  expect_invalid "negative hours" (fun () -> Duration.of_hours (-1L))

(** A count whose product with the unit wraps [Int64] to a small positive
    value must still be rejected. *)
let test_overflow_rejected () =
  expect_invalid "max_int seconds" (fun () -> Duration.of_seconds Int64.max_int);
  (* 2^63 / 1000 rounded up wraps to a small value when multiplied by 1000. *)
  expect_invalid "wrapping seconds" (fun () ->
      Duration.of_seconds 9_223_372_036_854_776L);
  expect_invalid "max_int hours" (fun () -> Duration.of_hours Int64.max_int)

let () =
  test_scaling ();
  test_bounds ();
  test_overflow_rejected ();
  print_endline "duration tests passed"

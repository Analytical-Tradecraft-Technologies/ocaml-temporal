(* Unit tests for [Temporal.Duration] and the [Temporal.Time] arithmetic
   built on it. They pin exact millisecond scaling, the shared negative and
   protobuf-maximum bounds, that a count large enough to overflow [Int64] when
   scaled is rejected instead of wrapping into an accepted duration, and the
   saturating overflow contract of [Duration.add], [Time.add] and
   [Time.diff] (#1145). *)

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

module Time = Temporal.Time

(** The protobuf maximum in milliseconds, spelled out independently of the
    module under test. *)
let maximum_ms = 315_576_000_000_999L

(** Days scale exactly, with the same bounds and overflow rejection as the
    other whole-unit constructors. *)
let test_days () =
  assert (Duration.to_ms (Duration.of_days 0L) = 0L);
  assert (Duration.to_ms (Duration.of_days 7L) = 604_800_000L);
  assert (
    Duration.to_ms (Duration.of_days 3_652_500L) = 315_576_000_000_000L);
  expect_invalid "days above maximum" (fun () -> Duration.of_days 3_652_501L);
  expect_invalid "negative days" (fun () -> Duration.of_days (-1L));
  expect_invalid "max_int days" (fun () -> Duration.of_days Int64.max_int)

(** [add] is exact below the bound and saturates at [maximum] above it;
    [compare] and [to_seconds] follow the millisecond value. *)
let test_add_compare_seconds () =
  assert (Duration.to_ms Duration.maximum = maximum_ms);
  assert (
    Duration.to_ms (Duration.add (Duration.of_ms 1_500L) (Duration.of_seconds 2L))
    = 3_500L);
  assert (Duration.to_ms (Duration.add (Duration.of_ms 0L) Duration.maximum) = maximum_ms);
  assert (
    Duration.to_ms (Duration.add Duration.maximum (Duration.of_ms 1L)) = maximum_ms);
  assert (
    Duration.to_ms (Duration.add Duration.maximum Duration.maximum) = maximum_ms);
  assert (
    Duration.to_ms
      (Duration.add (Duration.of_ms (Int64.sub maximum_ms 1L)) (Duration.of_ms 1L))
    = maximum_ms);
  assert (Duration.compare (Duration.of_ms 1L) (Duration.of_ms 2L) < 0);
  assert (Duration.compare (Duration.of_ms 2L) (Duration.of_ms 1L) > 0);
  assert (Duration.compare (Duration.of_minutes 1L) (Duration.of_seconds 60L) = 0);
  assert (Duration.to_seconds (Duration.of_ms 0L) = 0L);
  assert (Duration.to_seconds (Duration.of_ms 999L) = 0L);
  assert (Duration.to_seconds (Duration.of_ms 1_999L) = 1L);
  assert (Duration.to_seconds (Duration.of_days 1L) = 86_400L);
  assert (Duration.to_seconds Duration.maximum = 315_576_000_000L)

(** Builds an instant or fails the test. *)
let instant seconds nanoseconds =
  match Time.of_unix ~seconds ~nanoseconds with
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Asserts both components of an instant. *)
let expect_instant label value seconds nanoseconds =
  if not (Time.seconds value = seconds && Time.nanoseconds value = nanoseconds)
  then
    failwith
      (Printf.sprintf "%s: got %Ld.%09d, expected %Ld.%09d" label
         (Time.seconds value) (Time.nanoseconds value) seconds nanoseconds)

(** [Time.add] carries milliseconds into the fraction exactly, works for
    pre-epoch instants, and saturates at the last representable instant. *)
let test_time_add () =
  expect_instant "zero" (Time.add (instant 10L 5) (Duration.of_ms 0L)) 10L 5;
  expect_instant "fraction"
    (Time.add (instant 10L 100) (Duration.of_ms 1_250L))
    11L 250_000_100;
  expect_instant "carry"
    (Time.add (instant 10L 900_000_000) (Duration.of_ms 200L))
    11L 100_000_000;
  expect_instant "pre-epoch"
    (Time.add (instant (-5L) 999_999_999) (Duration.of_ms 1L))
    (-4L) 999_999;
  expect_instant "one day"
    (Time.add (instant 0L 0) (Duration.of_days 1L))
    86_400L 0;
  expect_instant "exactly representable"
    (Time.add (instant (Int64.sub Int64.max_int 1L) 0) (Duration.of_ms 1_000L))
    Int64.max_int 0;
  expect_instant "saturates on seconds"
    (Time.add (instant Int64.max_int 0) (Duration.of_ms 1_000L))
    Int64.max_int 999_999_999;
  expect_instant "saturates on carry"
    (Time.add (instant Int64.max_int 999_000_000) (Duration.of_ms 1L))
    Int64.max_int 999_999_999;
  expect_instant "saturates from a large duration"
    (Time.add (instant (Int64.sub Int64.max_int 10L) 0) Duration.maximum)
    Int64.max_int 999_999_999

(** [Time.diff] truncates to milliseconds, clamps a non-positive interval to
    zero, and saturates at [Duration.maximum] without wrapping [Int64]. *)
let test_time_diff () =
  let diff later earlier = Duration.to_ms (Time.diff later earlier) in
  assert (diff (instant 11L 250_000_100) (instant 10L 100) = 1_250L);
  assert (diff (instant 10L 1_999_999) (instant 10L 0) = 1L);
  assert (diff (instant 11L 0) (instant 10L 999_999_999) = 0L);
  assert (diff (instant 11L 0) (instant 10L 999_000_000) = 1L);
  assert (diff (instant 1L 0) (instant (-1L) 500_000_000) = 1_500L);
  assert (diff (instant 10L 0) (instant 10L 0) = 0L);
  assert (diff (instant 10L 0) (instant 11L 0) = 0L);
  assert (diff (instant 315_576_000_000L 999_999_999) (instant 0L 0) = maximum_ms);
  assert (diff (instant 315_576_000_001L 0) (instant 0L 0) = maximum_ms);
  assert (diff (instant Int64.max_int 0) (instant Int64.min_int 0) = maximum_ms);
  assert (diff (instant Int64.max_int 0) (instant (-1L) 0) = maximum_ms);
  (* [add] and [diff] are inverse while the result stays in range. *)
  let start = instant 1_700_000_000L 123_456_789 in
  let week = Duration.of_days 7L in
  assert (Duration.compare (Time.diff (Time.add start week) start) week = 0)

let () =
  test_scaling ();
  test_bounds ();
  test_overflow_rejected ();
  test_days ();
  test_add_compare_seconds ();
  test_time_add ();
  test_time_diff ();
  print_endline "duration tests passed"

(** Tests for the deterministic float, bytes and UUID draws (#1155).

    Every draw comes from the run's seeded stream, so the fixtures compare
    two fresh contexts with the same seed, check that a reset seed restarts
    the stream exactly as a fresh context would, and pin a golden stream so
    an accidental change of the generator, which would make recorded
    histories diverge on replay, fails here. *)

module Context = Temporal_runtime.Workflow_context_store
module Scheduler = Temporal_runtime.Scheduler

(** Fails with the scenario label when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Unwraps a public draw. *)
let get = function Ok value -> value | Error error -> failwith (Temporal.Error.message error)

(** Reports whether [needle] occurs in [haystack]. *)
let contains haystack needle =
  let n = String.length needle in
  let rec scan index =
    index + n <= String.length haystack
    && (String.sub haystack index n = needle || scan (index + 1))
  in
  scan 0

(** Requires a typed defect whose message contains [fragment]. *)
let expect_defect label fragment = function
  | Error error
    when Temporal.Error.kind error = "defect" && contains (Temporal.Error.message error) fragment
    ->
      ()
  | Error error -> failwith (label ^ ": wrong error " ^ Temporal.Error.message error)
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")

(** Runs [action] inside a fresh workflow context seeded with [seed]. *)
let with_seed seed action =
  let context = Context.create ~randomness_seed:seed (Scheduler.create ()) in
  Fun.protect ~finally:(fun () -> Context.shutdown context) (fun () ->
      Context.with_context context (fun () -> action context))

(** One interleaved draw of every kind through the public API. *)
let draws () =
  let uuid = get (Temporal.Workflow.uuid4 ()) in
  let float = get (Temporal.Workflow.random_float ()) in
  let bytes = get (Temporal.Workflow.random_bytes ~length:11) in
  let int = get (Temporal.Workflow.random_int ~bound:1_000) in
  (uuid, float, bytes, int)

(** The same seed yields the same interleaved stream; another seed yields
    a different one; and a reset seed restarts the stream exactly as a
    fresh context with that seed does, which is what replay after a reset
    relies on. *)
let test_stream_is_deterministic () =
  let first = with_seed "42" (fun _ -> (draws (), draws ())) in
  expect "same seed" first (with_seed "42" (fun _ -> (draws (), draws ())));
  if first = with_seed "43" (fun _ -> (draws (), draws ())) then
    failwith "different seeds produced the same stream";
  let reset =
    with_seed "42" (fun context ->
        ignore (draws ());
        Context.update_random_seed context ~randomness_seed:"7";
        draws ())
  in
  expect "reset restarts the stream" (with_seed "7" (fun _ -> draws ())) reset

(** Pins the stream for seed 42 so the generator, its SplitMix64 output
    mixing, or the draw layout cannot change silently. *)
let test_golden_stream () =
  let uuid, float, bytes, int = with_seed "42" (fun _ -> draws ()) in
  let hex =
    String.concat ""
      (List.map (fun c -> Printf.sprintf "%02x" (Char.code c)) (List.of_seq (String.to_seq bytes)))
  in
  expect "golden uuid" "7102e8bc-b290-467c-af6e-4db9aeb01cd3" uuid;
  expect "golden float" "0.74263600004166519" (Printf.sprintf "%.17g" float);
  expect "golden bytes" "c0ff98087e4f507dfef74c" hex;
  (* The pre-#1155 [random_int] stream is unchanged by the new draws' mixing. *)
  expect "golden int" 619 int

(** A UUID has the version-4 layout: 36 lowercase hex characters with
    hyphens at 8, 13, 18 and 23, version nibble 4, and variant 8, 9, a or b. *)
let test_uuid_layout () =
  with_seed "123456789" (fun _ ->
      for _ = 1 to 200 do
        let uuid = get (Temporal.Workflow.uuid4 ()) in
        expect "uuid length" 36 (String.length uuid);
        String.iteri
          (fun index character ->
            match index with
            | 8 | 13 | 18 | 23 -> expect "uuid hyphen" '-' character
            | 14 -> expect "uuid version" '4' character
            | 19 ->
                if not (String.contains "89ab" character) then failwith "uuid variant"
            | _ ->
                if not (String.contains "0123456789abcdef" character) then
                  failwith "uuid hex digit")
          uuid
      done)

(** Floats stay in [0, 1); byte draws have exactly the requested length,
    including 0 and the 65,536 maximum, and reject lengths outside it. *)
let test_float_and_bytes_bounds () =
  with_seed "99" (fun _ ->
      for _ = 1 to 1_000 do
        let value = get (Temporal.Workflow.random_float ()) in
        if value < 0. || value >= 1. then failwith "random_float out of range"
      done;
      List.iter
        (fun length ->
          expect "bytes length" length
            (String.length (get (Temporal.Workflow.random_bytes ~length))))
        [ 0; 1; 7; 8; 9; 65_536 ];
      List.iter
        (fun length ->
          expect_defect "bytes bound" "between 0 and 65536"
            (Temporal.Workflow.random_bytes ~length))
        [ -1; 65_537 ])

(** Draws are typed defects in a read-only update validator and outside a
    workflow, as for [random_int]. *)
let test_lifecycle_errors () =
  with_seed "5" (fun context ->
      Context.with_randomness_disabled context (fun () ->
          expect_defect "validator uuid" "read-only" (Temporal.Workflow.uuid4 ());
          expect_defect "validator float" "read-only" (Temporal.Workflow.random_float ());
          expect_defect "validator bytes" "read-only"
            (Temporal.Workflow.random_bytes ~length:4)));
  expect_defect "outside uuid" "Temporal.Workflow.uuid4" (Temporal.Workflow.uuid4 ());
  expect_defect "outside float" "Temporal.Workflow.random_float"
    (Temporal.Workflow.random_float ());
  expect_defect "outside bytes" "Temporal.Workflow.random_bytes"
    (Temporal.Workflow.random_bytes ~length:1)

(** Runs the deterministic randomness contract as one dune test. *)
let () =
  test_stream_is_deterministic ();
  test_golden_stream ();
  test_uuid_layout ();
  test_float_and_bytes_bounds ();
  test_lifecycle_errors ()

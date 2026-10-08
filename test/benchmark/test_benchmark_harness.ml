(** Unit checks for benchmark harness invariants that the report smoke runs
    cannot observe: that recovery snapshots see a released workload, and that
    the fan-out fixture byte bound rejects oversized configurations. *)

module Harness = Benchmark_harness
module Worker = Benchmark_worker

(** Fails the test even if OCaml assertions are disabled. *)
let expect condition message = if not condition then failwith message

(** Bytes held by the dummy workload; large enough that leaking it would
    dwarf any incidental allocation by the harness itself. *)
let workload_bytes = 32 * 1024 * 1024

(** Runs one memory-instrumented repetition over a workload whose sample,
    close and observation closures all capture a [workload_bytes] buffer, and
    requires [retained_live_bytes] to exclude it. Before the recovery
    snapshots moved out of the workload's scope, the [finally] closure kept the
    buffer reachable and this reported at least [workload_bytes]. *)
let test_released_workload_not_retained () =
  let make_workload _config =
    let buffer = Bytes.make workload_bytes 'x' in
    {
      Harness.workload =
        {
          Harness.sample = (fun _seed -> ignore (Bytes.get buffer 0));
          close = (fun () -> ignore (Bytes.length buffer));
        };
      observations =
        (fun () -> [ ("buffer_bytes", `Int (Bytes.length buffer)) ]);
    }
  in
  let config =
    { Harness.warmup = 1; samples = 2; repetitions = 1; seed = "1" }
  in
  let total_errors = ref 0 in
  let fields =
    Harness.run_one_repetition ~config ~rates:[] ~memory:true ~total_errors
      ~make_workload 0
  in
  expect (!total_errors = 0) "dummy workload reported errors";
  let open Yojson.Basic.Util in
  let memory = List.assoc "memory" fields in
  let load =
    memory |> member "recovery" |> member "load_live_bytes_above_baseline"
    |> to_int
  in
  let retained =
    memory |> member "recovery" |> member "retained_live_bytes" |> to_int
  in
  expect (load >= workload_bytes) "load snapshot did not observe the workload";
  expect
    (retained < workload_bytes / 4)
    (Printf.sprintf "released workload still counted: %d retained live bytes"
       retained)

(** Requires the fan-out fixture bound to accept sizes at the limit, reject
    one byte over it, and reject the reviewer's 20,000 x 10 x 1 MiB example
    without overflowing on 32- or 64-bit [int]. *)
let test_fixture_byte_bound () =
  let limit = 512 * 1024 * 1024 in
  let ok ~items ~bytes_per_item =
    Result.is_ok
      (Worker.check_fixture_bytes ~what:"test" ~items ~bytes_per_item ~limit)
  in
  expect (ok ~items:0 ~bytes_per_item:max_int) "empty fixture rejected";
  expect (ok ~items:1024 ~bytes_per_item:(limit / 1024)) "exact limit rejected";
  expect
    (not (ok ~items:1024 ~bytes_per_item:((limit / 1024) + 1)))
    "limit plus one accepted";
  expect
    (not
       (ok ~items:(20_000 * 10)
          ~bytes_per_item:(Worker.encoded_payload_estimate 1_048_576)))
    "20000 x 10 x 1 MiB fan-out accepted";
  expect
    (not (ok ~items:max_int ~bytes_per_item:max_int))
    "overflowing product accepted";
  expect
    (ok ~items:1_000 ~bytes_per_item:(Worker.encoded_payload_estimate 256))
    "default fan-out rejected";
  (* Base64 expands each started 3-byte group to 4 bytes. *)
  expect
    (Worker.encoded_payload_estimate 3 - Worker.encoded_payload_estimate 0 = 4
    && Worker.encoded_payload_estimate 4 - Worker.encoded_payload_estimate 0
       = 8)
    "unexpected base64 expansion estimate"

(** Runs every check; an uncaught failure fails the Dune test. *)
let () =
  test_released_workload_not_retained ();
  test_fixture_byte_bound ()

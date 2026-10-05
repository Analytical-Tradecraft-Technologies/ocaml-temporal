(** Validates the tiny Dune smoke reports' schema and bounded sample counts. *)

module Json = Yojson.Basic.Util

(** Fails the smoke run even if OCaml assertions are disabled. *)
let expect condition message = if not condition then failwith message

(** Returns a required integer field, failing the test on a schema mismatch. *)
let integer json key = Json.(json |> member key |> to_int)

(** Returns a required object field, failing the test on a schema mismatch. *)
let object_field json key = Json.(json |> member key |> to_assoc)

(** Returns a required list field, failing the test on a schema mismatch. *)
let list json key = Json.(json |> member key |> to_list)

(** Returns a required nonnegative JSON number. *)
let nonnegative_number json key =
  let value = Json.member key json in
  let number =
    match value with
    | `Int value -> float_of_int value
    | `Float value -> value
    | _ -> failwith (key ^ " must be numeric")
  in
  if Float.is_nan number || number < 0. then
    failwith (key ^ " must be nonnegative")

(** Checks an expected sample count and zero errors for one phase. *)
let check_phase ~attempts ~latencies phase =
  expect (integer phase "attempts" = attempts) "unexpected phase attempts";
  expect (integer phase "errors" = 0) "phase errors";
  expect
    (List.length (list phase "latency_us") = latencies)
    "unexpected latency count";
  expect (list phase "error_examples" = []) "unexpected error examples";
  nonnegative_number phase "elapsed_seconds";
  nonnegative_number phase "throughput_successes_per_second"

(** Checks a report emitted by one of the actual activation workloads. *)
let () =
  if Array.length Sys.argv <> 3 then
    failwith "check_report expects suite and measured sample count";
  let expected_suite = Sys.argv.(1) in
  let samples = int_of_string Sys.argv.(2) in
  let report = Yojson.Basic.from_channel stdin in
  expect (integer report "schema_version" = 1) "unexpected report schema";
  expect
    (Json.(report |> member "suite" |> to_string) = expected_suite)
    "unexpected benchmark suite";
  ignore (object_field report "machine");
  let config = Json.member "config" report in
  ignore (object_field report "config");
  if expected_suite <> "local-minimal-activation" then (
    expect
      (integer config "admitted_concurrency" = 1)
      "unexpected admitted concurrency";
    expect
      (Json.(config |> member "admission_model" |> to_string) = "closed_loop")
      "unexpected admission model";
    expect
      (integer config "pending_attempt_backlog_peak" = 0)
      "unexpected closed-loop backlog";
    expect
      (Json.(config |> member "saturation_observation" |> to_string)
      = "not_exercised")
      "unexpected saturation observation");
  let provenance = Json.member "provenance" report in
  expect
    (Json.(provenance |> member "dune_profile" |> to_string) = "dev")
    "unexpected Dune profile";
  expect
    (Json.(provenance |> member "development_image_id" |> to_string)
    = "unavailable")
    "unexpected test image identity";
  let totals = Json.member "totals" report in
  expect (integer totals "warmup_attempts" = 1) "unexpected warmup count";
  expect
    (integer totals "measurement_attempts" = samples)
    "unexpected measurement count";
  expect (integer totals "errors" = 0) "unexpected total errors";
  match list report "repetitions" with
  | [ repetition ] ->
      check_phase ~attempts:1 ~latencies:0 (Json.member "warmup" repetition);
      let measured = Json.member "measurement" repetition in
      check_phase ~attempts:samples ~latencies:samples measured;
      List.iter (nonnegative_number measured) [ "p50_us"; "p95_us"; "p99_us" ]
  | _ -> failwith "expected one benchmark repetition"

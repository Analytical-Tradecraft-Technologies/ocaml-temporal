(** Shared warmup, measurement, and versioned-report machinery for SDK benchmark
    suites. Each suite supplies its workload and states its measured boundary;
    this module makes no claim that local timing qualifies a release. *)

type config = { warmup : int; samples : int; repetitions : int; seed : string }
(** Bounded command-line inputs for one reproducible benchmark invocation. *)

type workload = { sample : string -> unit; close : unit -> unit }
(** One repetition's independently owned workload. [sample] runs inside the
    timed phase; [close] releases its state after both phases, even on error. *)

type phase = {
  elapsed_seconds : float;
  latencies_us : float list;
  errors : int;
  error_examples : string list;
}
(** One phase's elapsed wall time and error observations. Measurement phases
    retain per-attempt latency so later analysis can inspect the distribution.
*)

(** Reads a required provenance field set by the Makefile benchmark command. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> value
  | _ -> failwith ("benchmark provenance is missing " ^ name)

(** Reads an optional machine label supplied by the benchmark operator. *)
let optional_env name =
  match Sys.getenv_opt name with Some value -> value | None -> "unspecified"

(** Parses a bounded positive count, rejecting accidental unbounded soak runs
    through this small in-memory report format. *)
let bounded_count ~name ~limit text =
  match int_of_string_opt text with
  | Some value when value > 0 && value <= limit -> value
  | _ -> raise (Arg.Bad (Printf.sprintf "%s must be in 1..%d" name limit))

(** Parses benchmark configuration without changing the global application
    argument parser. A fixed seed is recorded even though this first workload
    does not consume randomness; later suites can use the same field. *)
let parse_config () =
  let warmup = ref 100 in
  let samples = ref 1_000 in
  let repetitions = ref 3 in
  let seed = ref "1" in
  let usage =
    "benchmark [--warmup N] [--samples N] [--repetitions N] [--seed VALUE]"
  in
  Arg.parse
    [
      ( "--warmup",
        Arg.String
          (fun text ->
            warmup := bounded_count ~name:"warmup" ~limit:100_000 text),
        "Warmup operations per repetition" );
      ( "--samples",
        Arg.String
          (fun text ->
            samples := bounded_count ~name:"samples" ~limit:100_000 text),
        "Measured operations per repetition" );
      ( "--repetitions",
        Arg.String
          (fun text ->
            repetitions := bounded_count ~name:"repetitions" ~limit:100 text),
        "Independent warmup/measurement repetitions" );
      ( "--seed",
        Arg.String (fun value -> seed := value),
        "Recorded workflow random seed" );
    ]
    (fun value -> raise (Arg.Bad ("unexpected argument: " ^ value)))
    usage;
  if String.length !seed = 0 || String.length !seed > 128 then
    raise (Arg.Bad "seed must contain 1..128 bytes");
  if !samples * !repetitions > 1_000_000 || !warmup * !repetitions > 1_000_000
  then raise (Arg.Bad "each phase is limited to one million total attempts");
  {
    warmup = !warmup;
    samples = !samples;
    repetitions = !repetitions;
    seed = !seed;
  }

(** Measures a bounded phase and preserves a few distinct error messages. A
    failed operation counts as an attempted sample and makes the process fail
    after it has written the JSON report. *)
let run_phase ~workload ~record_latencies ~count ~seed =
  let latencies = ref [] in
  let errors = ref 0 in
  let examples = ref [] in
  let started = Mtime_clock.counter () in
  for _ = 1 to count do
    let before = Mtime_clock.counter () in
    let error = ref None in
    (try workload seed with exn -> error := Some (Printexc.to_string exn));
    let elapsed_us =
      Mtime.Span.to_float_ns (Mtime_clock.count before) /. 1_000.
    in
    (match !error with
    | Some message ->
        incr errors;
        if List.length !examples < 3 then examples := message :: !examples
    | None -> ());
    if record_latencies then latencies := elapsed_us :: !latencies
  done;
  let elapsed_seconds =
    Mtime.Span.to_float_ns (Mtime_clock.count started) /. 1_000_000_000.
  in
  {
    elapsed_seconds;
    latencies_us = List.rev !latencies;
    errors = !errors;
    error_examples = List.rev !examples;
  }

(** Returns the nearest-rank percentile of per-attempt latency in microseconds.
*)
let percentile sorted quantile =
  let count = Array.length sorted in
  sorted.(max 0
            (min (count - 1)
               (int_of_float (ceil (quantile *. float_of_int count)) - 1)))

(** Encodes one phase as stable JSON with explicit units and sample counts. *)
let phase_json ~count phase =
  let sorted = Array.of_list phase.latencies_us in
  Array.sort Float.compare sorted;
  let statistics =
    if Array.length sorted = 0 then []
    else
      [
        ("p50_us", `Float (percentile sorted 0.50));
        ("p95_us", `Float (percentile sorted 0.95));
        ("p99_us", `Float (percentile sorted 0.99));
      ]
  in
  `Assoc
    ([
       ("attempts", `Int count);
       ("errors", `Int phase.errors);
       ( "error_examples",
         `List (List.map (fun value -> `String value) phase.error_examples) );
       ("elapsed_seconds", `Float phase.elapsed_seconds);
       ( "throughput_successes_per_second",
         `Float
           (if phase.elapsed_seconds = 0. then 0.
            else float_of_int (count - phase.errors) /. phase.elapsed_seconds)
       );
       ( "latency_us",
         `List (List.map (fun value -> `Float value) phase.latencies_us) );
     ]
    @ statistics)

(** Encodes UTC run time without depending on a machine's local timezone. *)
let timestamp_utc () =
  let now = Unix.gmtime (Unix.gettimeofday ()) in
  Printf.sprintf "%04d-%02d-%02dT%02d:%02d:%02dZ" (now.Unix.tm_year + 1900)
    (now.Unix.tm_mon + 1) now.Unix.tm_mday now.Unix.tm_hour now.Unix.tm_min
    now.Unix.tm_sec

(** Returns the runtime OS and architecture as seen by the benchmark process. A
    missing [uname] only affects descriptive metadata, not measurements. *)
let runtime_uname () =
  try
    let process = Unix.open_process_in "uname -srm" in
    let value = input_line process in
    ignore (Unix.close_process_in process);
    value
  with _ -> "unavailable"

(** Runs independently prepared workloads across repetitions and prints one
    versioned report. Preparation and cleanup are outside each timed phase; each
    sample, including its validation, is inside. Extra configuration describes
    suite-specific dimensions and must not shadow shared keys. The process exits
    nonzero after writing the report if any sample failed. *)
let run_repetitions ~suite ~boundary ~server_version ~workload_config
    ~make_workload () =
  let config = parse_config () in
  let source_commit = required_env "BENCH_SOURCE_COMMIT" in
  let source_dirty = required_env "BENCH_SOURCE_DIRTY" in
  if source_dirty <> "true" && source_dirty <> "false" then
    failwith "BENCH_SOURCE_DIRTY must be true or false";
  let sdk_version = required_env "BENCH_SDK_VERSION" in
  let core_revision = required_env "BENCH_CORE_REVISION" in
  let dune_profile = required_env "BENCH_DUNE_PROFILE" in
  let base_image_reference = required_env "BENCH_BASE_IMAGE_REFERENCE" in
  let development_image_id = required_env "BENCH_DEVELOPMENT_IMAGE_ID" in
  let total_errors = ref 0 in
  let repetitions =
    List.init config.repetitions (fun index ->
        let workload = make_workload config in
        Fun.protect ~finally:workload.close (fun () ->
            let warmup =
              run_phase ~workload:workload.sample ~record_latencies:false
                ~count:config.warmup ~seed:config.seed
            in
            let measured =
              run_phase ~workload:workload.sample ~record_latencies:true
                ~count:config.samples ~seed:config.seed
            in
            total_errors := !total_errors + warmup.errors + measured.errors;
            `Assoc
              [
                ("index", `Int (index + 1));
                ("warmup", phase_json ~count:config.warmup warmup);
                ("measurement", phase_json ~count:config.samples measured);
              ]))
  in
  let report =
    `Assoc
      [
        ("schema_version", `Int 1);
        ("suite", `String suite);
        ("generated_at_utc", `String (timestamp_utc ()));
        ("boundary", `String boundary);
        ( "provenance",
          `Assoc
            [
              ("source_commit", `String source_commit);
              ("source_dirty", `Bool (source_dirty = "true"));
              ("sdk_version", `String sdk_version);
              ("ocaml_version", `String Sys.ocaml_version);
              ("dune_profile", `String dune_profile);
              ("core_revision", `String core_revision);
              ("server_version", `String server_version);
              ("base_image_reference", `String base_image_reference);
              ("development_image_id", `String development_image_id);
            ] );
        ( "machine",
          `Assoc
            [
              ("hostname", `String (Unix.gethostname ()));
              ("runtime_uname", `String (runtime_uname ()));
              ("os_type", `String Sys.os_type);
              ("word_size_bits", `Int Sys.word_size);
              ("recommended_domains", `Int (Domain.recommended_domain_count ()));
              ("host_label", `String (optional_env "BENCH_HOST_LABEL"));
            ] );
        ( "config",
          `Assoc
            ([
               ("warmup_per_repetition", `Int config.warmup);
               ("samples_per_repetition", `Int config.samples);
               ("repetitions", `Int config.repetitions);
               ("seed", `String config.seed);
             ]
            @ workload_config) );
        ( "totals",
          `Assoc
            [
              ("warmup_attempts", `Int (config.warmup * config.repetitions));
              ( "measurement_attempts",
                `Int (config.samples * config.repetitions) );
              ("errors", `Int !total_errors);
            ] );
        ("repetitions", `List repetitions);
      ]
  in
  Yojson.Basic.pretty_to_channel stdout report;
  output_char stdout '\n';
  if !total_errors <> 0 then exit 1

(** Keeps the original stateless-workload entry point for suites that create and
    release all of their state within each timed sample. *)
let run ~suite ~boundary ~server_version ~workload_config ~workload () =
  run_repetitions ~suite ~boundary ~server_version ~workload_config
    ~make_workload:(fun _ -> { sample = workload; close = (fun () -> ()) })
    ()

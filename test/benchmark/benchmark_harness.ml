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

type instrumented = {
  workload : workload;
  observations : unit -> (string * Yojson.Basic.t) list;
}
(** A workload with suite-specific, untimed observations. [observations] runs
    once per repetition after the measurement phase and its memory snapshot,
    but before [workload.close], so it may inspect retained state or run a
    separately instrumented attribution pass without perturbing sample
    latency. *)

(** OCaml heap, GC, and process-resident memory observations.

    Allocation counters are [Gc.quick_stat] deltas. Live and heap sizes come
    from [Gc.stat], which in OCaml 5 forces a full major collection, so "live"
    means reachable data rather than garbage awaiting collection. Snapshots
    are taken only at phase boundaries, never inside a timed sample. Counters
    describe the calling Domain; the instrumented suites run on one Domain.

    Process memory uses OS counters: [/proc/self/status] on Linux (current
    [VmRSS] and peak [VmHWM]), otherwise the current RSS reported by [ps].
    The standard [Unix] library does not expose [getrusage], so the peak is
    [null] off Linux. RSS includes every native component (the OCaml runtime,
    code pages, C or Rust allocations, and allocator capacity not yet returned
    to the OS); each suite names the components it cannot attribute. *)
module Memory = struct
  type snapshot = {
    heap_words : int;
    top_heap_words : int;
    live_words : int;
    rss_bytes : int option;
    peak_rss_bytes : int option;
    rss_source : string;
  }
  (** One boundary observation. [rss_source] names the OS counter used. *)

  (** Converts OCaml heap words to bytes for the host word size. *)
  let bytes_of_words words = words * (Sys.word_size / 8)

  (** Parses a ["Name:  1234 kB"] procfs line with [prefix] into bytes. *)
  let proc_status_bytes line prefix =
    let prefix_length = String.length prefix in
    if
      String.length line > prefix_length
      && String.sub line 0 prefix_length = prefix
    then
      let rest =
        String.trim
          (String.sub line prefix_length (String.length line - prefix_length))
      in
      match String.split_on_char ' ' rest with
      | value :: _ -> Option.map (fun kib -> kib * 1024) (int_of_string_opt value)
      | [] -> None
    else None

  (** Reads current and peak RSS from Linux procfs. [None] when the file is
      absent, which is expected on macOS and Windows. *)
  let linux_resident () =
    match open_in "/proc/self/status" with
    | exception Sys_error _ -> None
    | channel ->
        Fun.protect
          ~finally:(fun () -> close_in_noerr channel)
          (fun () ->
            let rss = ref None and peak = ref None in
            (try
               while true do
                 let line = input_line channel in
                 Option.iter
                   (fun value -> rss := Some value)
                   (proc_status_bytes line "VmRSS:");
                 Option.iter
                   (fun value -> peak := Some value)
                   (proc_status_bytes line "VmHWM:")
               done
             with End_of_file -> ());
            Option.map
              (fun rss -> (Some rss, !peak, "linux_proc_self_status"))
              !rss)

  (** Reads the current RSS through [ps] where procfs is unavailable. A
      missing or failing [ps] only removes this metadata. *)
  let ps_resident () =
    try
      let command =
        Printf.sprintf "ps -o rss= -p %d 2>/dev/null" (Unix.getpid ())
      in
      let process = Unix.open_process_in command in
      let line =
        Fun.protect
          ~finally:(fun () -> ignore (Unix.close_process_in process))
          (fun () -> input_line process)
      in
      match int_of_string_opt (String.trim line) with
      | Some kib -> (Some (kib * 1024), None, "ps_rss")
      | None -> (None, None, "unavailable")
    with _ -> (None, None, "unavailable")

  (** Returns current RSS, peak RSS, and the counter source. *)
  let resident () =
    match linux_resident () with Some value -> value | None -> ps_resident ()

  (** Takes one boundary snapshot. [compact] first runs [Gc.compact], which
      releases free major-heap pools to the OS; it is used for the pre-load
      baseline and the final allocator-capacity observation. *)
  let snapshot ~compact () =
    if compact then Gc.compact ();
    let stat = Gc.stat () in
    let rss_bytes, peak_rss_bytes, rss_source = resident () in
    {
      heap_words = stat.heap_words;
      top_heap_words = stat.top_heap_words;
      live_words = stat.live_words;
      rss_bytes;
      peak_rss_bytes;
      rss_source;
    }

  (** Encodes an optional byte count, using JSON [null] when unavailable. *)
  let optional_int = function Some value -> `Int value | None -> `Null

  (** Encodes one snapshot with explicit byte units. *)
  let snapshot_json snapshot =
    `Assoc
      [
        ("ocaml_heap_bytes", `Int (bytes_of_words snapshot.heap_words));
        ("ocaml_top_heap_bytes", `Int (bytes_of_words snapshot.top_heap_words));
        ("ocaml_live_bytes", `Int (bytes_of_words snapshot.live_words));
        ("process_rss_bytes", optional_int snapshot.rss_bytes);
        ("process_peak_rss_bytes", optional_int snapshot.peak_rss_bytes);
        ("rss_source", `String snapshot.rss_source);
      ]

  (** Encodes GC counter deltas for one phase. Allocated words are minor plus
      direct major allocation minus promotion, which would otherwise be
      counted twice. *)
  let allocation_json ~attempts ~(before : Gc.stat) ~(after : Gc.stat) =
    let word_bytes = float_of_int (Sys.word_size / 8) in
    let minor = after.minor_words -. before.minor_words in
    let promoted = after.promoted_words -. before.promoted_words in
    let major = after.major_words -. before.major_words in
    let allocated = word_bytes *. (minor +. major -. promoted) in
    `Assoc
      [
        ("allocated_bytes", `Float allocated);
        ( "allocated_bytes_per_attempt",
          `Float
            (if attempts = 0 then 0. else allocated /. float_of_int attempts) );
        ("minor_words", `Float minor);
        ("promoted_words", `Float promoted);
        ("major_words", `Float major);
        ( "minor_collections",
          `Int (after.minor_collections - before.minor_collections) );
        ( "major_collections",
          `Int (after.major_collections - before.major_collections) );
        ("compactions", `Int (after.compactions - before.compactions));
      ]

  (** Subtracts optional RSS values, yielding [null] if either is unknown. *)
  let rss_delta left right =
    match (left.rss_bytes, right.rss_bytes) with
    | Some left, Some right -> `Int (left - right)
    | _ -> `Null

  (** Encodes one repetition's lifecycle snapshots and recovery deltas.

      [retained_live_bytes] is reachable OCaml data that survived closing the
      workload, relative to the pre-load baseline; growth of that value across
      repetitions is the suspected-retention signal. RSS that falls between
      [after_close] and [after_compact] was free OCaml heap capacity. RSS still
      above baseline after compaction while live bytes have recovered is
      capacity held by the runtime or system allocator, not reachable data.
      No pass/fail threshold is applied. *)
  let repetition_json ~before_load ~after_warmup ~after_measurement
      ~after_close ~after_compact =
    `Assoc
      [
        ("before_load", snapshot_json before_load);
        ("after_warmup", snapshot_json after_warmup);
        ("after_measurement", snapshot_json after_measurement);
        ("after_close", snapshot_json after_close);
        ("after_compact", snapshot_json after_compact);
        ( "recovery",
          `Assoc
            [
              ( "load_live_bytes_above_baseline",
                `Int
                  (bytes_of_words
                     (max after_warmup.live_words after_measurement.live_words
                     - before_load.live_words)) );
              ( "retained_live_bytes",
                `Int
                  (bytes_of_words
                     (after_close.live_words - before_load.live_words)) );
              ( "heap_bytes_after_compact_minus_baseline",
                `Int
                  (bytes_of_words
                     (after_compact.heap_words - before_load.heap_words)) );
              ( "rss_bytes_released_by_compaction",
                rss_delta after_close after_compact );
              ( "rss_bytes_after_compact_minus_baseline",
                rss_delta after_compact before_load );
            ] );
      ]

  (** Summarizes retained live bytes across repetitions. Each repetition owns a
      fresh workload, so a sequence that keeps rising is reproducible evidence
      of retention outside the workload, whereas a flat sequence attributes
      any RSS difference to allocator capacity. *)
  let trend_json repetitions =
    let retained =
      List.filter_map
        (fun repetition ->
          match
            Yojson.Basic.Util.(
              repetition |> member "memory" |> member "recovery"
              |> member "retained_live_bytes")
          with
          | `Int value -> Some value
          | _ -> None)
        repetitions
    in
    let rec increasing = function
      | first :: (second :: _ as rest) -> first < second && increasing rest
      | _ -> true
    in
    `Assoc
      [
        ( "retained_live_bytes_by_repetition",
          `List (List.map (fun value -> `Int value) retained) );
        ( "strictly_increasing",
          if List.length retained < 3 then `Null
          else `Bool (increasing retained) );
      ]

  (** Describes what the memory figures do and do not cover. *)
  let scope_json unmeasured =
    `Assoc
      [
        ( "ocaml_heap",
          `String
            "Gc.stat after a forced full major collection at phase \
             boundaries; allocation from Gc.quick_stat deltas on the benchmark \
             Domain" );
        ( "process_resident",
          `String
            "OS resident set size, including the OCaml runtime, code, any \
             native allocation, and allocator capacity not yet returned to \
             the OS" );
        ( "unmeasured_native_components",
          `List (List.map (fun value -> `String value) unmeasured) );
      ]
end

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

(** Runs one repetition's warmup and measurement phases and returns its report
    fields. With [memory], it also records {!Memory} lifecycle snapshots,
    per-phase allocation deltas, and the workload's untimed observations. The
    workload is always closed exactly once, including on error; when measuring
    memory it is closed before the recovery snapshots. *)
let run_one_repetition ~config ~memory ~total_errors ~make_workload index =
  (* The baseline precedes workload construction so the first warmup snapshot
     includes everything the workload retains. *)
  let before_load =
    if memory then Some (Memory.snapshot ~compact:true ()) else None
  in
  let instrumented = make_workload config in
  let workload = instrumented.workload in
  let closed = ref false in
  let close () =
    if not !closed then (
      closed := true;
      workload.close ())
  in
  Fun.protect ~finally:close (fun () ->
      let warmup_start = Gc.quick_stat () in
      let warmup =
        run_phase ~workload:workload.sample ~record_latencies:false
          ~count:config.warmup ~seed:config.seed
      in
      let warmup_end = Gc.quick_stat () in
      let after_warmup =
        if memory then Some (Memory.snapshot ~compact:false ()) else None
      in
      let measurement_start = Gc.quick_stat () in
      let measured =
        run_phase ~workload:workload.sample ~record_latencies:true
          ~count:config.samples ~seed:config.seed
      in
      let measurement_end = Gc.quick_stat () in
      total_errors := !total_errors + warmup.errors + measured.errors;
      let base =
        [
          ("index", `Int (index + 1));
          ("warmup", phase_json ~count:config.warmup warmup);
          ("measurement", phase_json ~count:config.samples measured);
        ]
      in
      match (before_load, after_warmup) with
      | Some before_load, Some after_warmup ->
          let after_measurement = Memory.snapshot ~compact:false () in
          let observations = instrumented.observations () in
          close ();
          let after_close = Memory.snapshot ~compact:false () in
          let after_compact = Memory.snapshot ~compact:true () in
          base
          @ [
              ( "allocation",
                `Assoc
                  [
                    ( "warmup",
                      Memory.allocation_json ~attempts:config.warmup
                        ~before:warmup_start ~after:warmup_end );
                    ( "measurement",
                      Memory.allocation_json ~attempts:config.samples
                        ~before:measurement_start ~after:measurement_end );
                  ] );
              ( "memory",
                Memory.repetition_json ~before_load ~after_warmup
                  ~after_measurement ~after_close ~after_compact );
              ("observations", `Assoc observations);
            ]
      | _ -> base)

(** Runs independently prepared workloads across repetitions and prints one
    versioned report. Preparation and cleanup are outside each timed phase; each
    sample, including its validation, is inside. Extra configuration describes
    suite-specific dimensions and must not shadow shared keys. [memory] carries
    the suite's unmeasured native components and enables instrumentation;
    [None] keeps the original report shape byte-compatible. The process exits
    nonzero after writing the report if any sample failed. *)
let run_report ~suite ~boundary ~server_version ~workload_config ~memory
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
        `Assoc
          (run_one_repetition ~config ~memory:(Option.is_some memory)
             ~total_errors ~make_workload index))
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
  let report =
    match (memory, report) with
    | Some unmeasured, `Assoc fields ->
        `Assoc
          (fields
          @ [
              ("memory_scope", Memory.scope_json unmeasured);
              ("memory_trend", Memory.trend_json repetitions);
            ])
    | _ -> report
  in
  Yojson.Basic.pretty_to_channel stdout report;
  output_char stdout '\n';
  if !total_errors <> 0 then exit 1

(** Runs uninstrumented repetitions with the original report shape. *)
let run_repetitions ~suite ~boundary ~server_version ~workload_config
    ~make_workload () =
  run_report ~suite ~boundary ~server_version ~workload_config ~memory:None
    ~make_workload:(fun config ->
      { workload = make_workload config; observations = (fun () -> []) })
    ()

(** Keeps the original stateless-workload entry point for suites that create and
    release all of their state within each timed sample. *)
let run ~suite ~boundary ~server_version ~workload_config ~workload () =
  run_repetitions ~suite ~boundary ~server_version ~workload_config
    ~make_workload:(fun _ -> { sample = workload; close = (fun () -> ()) })
    ()

(** Runs memory-instrumented repetitions for the allocation, history, cache,
    and fan-out suites (#527, #528). [unmeasured] names every native or
    out-of-process component whose memory the suite cannot attribute, so a
    reader never mistakes OCaml heap figures for total SDK memory. *)
let run_instrumented ~suite ~boundary ~server_version ~workload_config
    ~unmeasured ~make_workload () =
  run_report ~suite ~boundary ~server_version ~workload_config
    ~memory:(Some unmeasured) ~make_workload ()

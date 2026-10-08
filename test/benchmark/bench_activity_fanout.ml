(** High fan-out workflows and the share of OCaml-side JSON bridge work (#528).

    One sample runs [BENCH_FANOUT_CONCURRENCY] workflow runs to completion
    through the production OCaml worker adapter. Each run schedules
    [BENCH_FANOUT_WIDTH] remote activities in its first activation and awaits
    them all. Results arrive [BENCH_FANOUT_BATCH] per activation, interleaved
    round-robin across the concurrent runs as Core would deliver completions
    that land while other runs are active; the final batch completes the run,
    and Core's [Workflow_execution_ending] eviction follows. Activity arguments
    and results are JSON strings of [BENCH_FANOUT_PAYLOAD_BYTES] encoded
    bytes.

    Sample latency is end-to-end fan-out completion for all concurrent runs.
    An untimed attribution pass splits adapter time into strict activation
    decoding, completion encoding and the completion byte copy (the OCaml half
    of the JSON bridge) and everything else (translation, deterministic
    workflow code, futures and command validation). Rust/serde, Core, FFI and
    the server are excluded; see {!Benchmark_worker}. *)

module W = Benchmark_worker
module Protocol = W.Protocol
module Codec = Temporal_base.Codec

(** Activities scheduled by each run. *)
let width = W.env_count ~name:"BENCH_FANOUT_WIDTH" ~default:1_000 ~limit:20_000

(** Workflow runs advanced together, interleaved by activation. *)
let concurrency =
  W.env_count ~name:"BENCH_FANOUT_CONCURRENCY" ~default:1 ~limit:256

(** Activity results delivered per activation. *)
let batch =
  min width (W.env_count ~name:"BENCH_FANOUT_BATCH" ~default:10 ~limit:20_000)

(** Encoded bytes of every activity argument and result. *)
let payload_bytes =
  W.env_count ~name:"BENCH_FANOUT_PAYLOAD_BYTES" ~default:256 ~limit:1_048_576

(** Upper bound, in bytes, on the pre-encoded activation fixtures. The
    fixtures are resident for the whole process, and running the workflows
    holds roughly as many bytes again in decoded results and scheduled
    arguments, so this keeps a misconfigured run from exhausting memory. *)
let fixture_byte_limit = 512 * 1024 * 1024

(** Bounds the activities processed by one sample and, before any fixture is
    built, the bytes their encoded results would occupy. Both products are
    checked without overflow: the first factors are at most 20,000 and 256,
    and the byte bound is checked by division in
    {!Benchmark_worker.check_fixture_bytes}. *)
let () =
  if width * concurrency > 200_000 then
    failwith "BENCH_FANOUT_WIDTH * BENCH_FANOUT_CONCURRENCY must be <= 200000";
  match
    W.check_fixture_bytes
      ~what:
        "BENCH_FANOUT_WIDTH * BENCH_FANOUT_CONCURRENCY * \
         encoded(BENCH_FANOUT_PAYLOAD_BYTES)"
      ~items:(width * concurrency)
      ~bytes_per_item:(W.encoded_payload_estimate payload_bytes)
      ~limit:fixture_byte_limit
  with
  | Ok () -> ()
  | Error message -> failwith message

(** Registered workflow type name. *)
let workflow_type = "benchmark_activity_fanout"

(** Schedules [width] activities at once and completes with their count. *)
let workflow =
  Temporal_base.Definition.make ~name:workflow_type ~input:Codec.unit
    ~output:Codec.string
    ~implementation:
      (Some
         (fun () ->
           match W.current_context () with
           | Error error -> Error error
           | Ok context -> (
               let futures =
                 List.init width (fun _ ->
                     W.schedule_activity context ~payload_bytes)
               in
               let ownership_error () =
                 Temporal_base.Error.defect
                   ~message:"fan-out future crossed workflow schedulers"
               in
               match W.Future.await (W.Future.all ~ownership_error futures) with
               | Ok results -> Ok (string_of_int (List.length results))
               | Error error -> Error error)))

(** Result batches per run. *)
let batches = (width + batch - 1) / batch

type documents = { start : bytes; results : bytes array; ending : bytes }
(** Pre-encoded activations for one concurrent run. *)

(** Encodes one run's start, result batches and terminal eviction. *)
let documents_for index =
  let run_id = Printf.sprintf "fanout-run-%03d" index in
  let result = W.protocol_payload (W.string_payload payload_bytes) in
  let encode ~history_length jobs =
    W.encode
      (W.activation ~run_id ~replaying:false
         ~history_length:(Int64.of_int history_length) jobs)
  in
  {
    start = encode ~history_length:3 [ W.initialize ~run_id ~workflow_type ];
    results =
      Array.init batches (fun b ->
          let first = (b * batch) + 1 in
          let last = min width ((b + 1) * batch) in
          encode
            ~history_length:(4 + (2 * width) + (3 * last))
            (List.init (last - first + 1) (fun i ->
                 W.resolve_activity ~payload:result (Int64.of_int (first + i)))));
    ending =
      encode ~history_length:(5 + (5 * width))
        [ W.remove_from_cache Protocol.Workflow_execution_ending ];
  }

(** All runs' documents, built before the baseline snapshot. *)
let documents = Array.init concurrency documents_for

(** Encoded activation bytes delivered per sample. *)
let activation_bytes_per_sample =
  Array.fold_left
    (fun total doc ->
      total + Bytes.length doc.start + Bytes.length doc.ending
      + Array.fold_left (fun t b -> t + Bytes.length b) 0 doc.results)
    0 documents

(** Requires the first completion to schedule exactly activities 1..width. *)
let expect_fanout commands =
  let rec check expected = function
    | [] -> if expected <> width + 1 then failwith "fan-out scheduled too few"
    | Protocol.Schedule_activity { seq; _ } :: rest
      when seq = Int64.of_int expected ->
        check (expected + 1) rest
    | _ -> failwith "fan-out schedule commands were not sequential"
  in
  check 1 commands

(** Runs every concurrent fan-out to completion and eviction. *)
let fan_out worker =
  Array.iter (fun doc -> expect_fanout (W.deliver worker doc.start)) documents;
  for b = 0 to batches - 1 do
    Array.iter
      (fun doc ->
        let commands = W.deliver worker doc.results.(b) in
        if b = batches - 1 then W.expect_complete commands
        else W.expect_empty commands)
      documents
  done;
  Array.iter (fun doc -> W.expect_empty (W.deliver worker doc.ending)) documents

(** Makes one registry per repetition. The attribution pass runs three
    untimed samples so short fan-outs are not dominated by timer resolution. *)
let make_workload _config =
  let worker = W.create [ W.Worker.register workflow ] in
  {
    Benchmark_harness.workload =
      {
        Benchmark_harness.sample = (fun _seed -> fan_out worker);
        close = (fun () -> W.discard worker);
      };
    observations =
      (fun () ->
        [
          ( "json_bridge_attribution",
            `Assoc (W.attribute worker ~units:3 (fun () -> fan_out worker)) );
        ]);
  }

(** Emits the instrumented report for one fan-out configuration. A sample
    completes [concurrency] fan-outs, so [throughput_successes_per_second]
    counts samples; the derived [fanouts_per_second] and
    [activities_per_second] fields scale it by the work in each sample. *)
let () =
  Benchmark_harness.run_instrumented ~suite:"activity-fanout"
    ~rates:[ ("fanouts", concurrency); ("activities", width * concurrency) ]
    ~boundary:
      "Concurrent fan-out workflow runs from start activation through every \
       activity result batch, terminal completion and eviction, via strict \
       activation JSON decode, the production OCaml worker adapter and \
       completion encode; no Core, FFI, supervisor, polling, server or \
       network"
    ~server_version:"none"
    ~workload_config:
      [
        ("admitted_concurrency", `Int concurrency);
        ("admission_model", `String "closed_loop_interleaved_runs");
        ("pending_attempt_backlog_peak", `Int 0);
        ("saturation_observation", `String "not_exercised");
        ("fanout_width", `Int width);
        ("concurrent_runs", `Int concurrency);
        ("results_per_activation", `Int batch);
        ("activities_per_sample", `Int (width * concurrency));
        ("activations_per_sample", `Int (concurrency * (batches + 2)));
        ("payload_bytes", `Int payload_bytes);
        ("activation_json_bytes_per_sample", `Int activation_bytes_per_sample);
      ]
    ~unmeasured:
      [
        "Rust serde JSON encoding of activations and decoding of completions \
         (not exercised)";
        "Temporal Core activity state machines and command buffers (Rust \
         heap; not exercised)";
        "C stub byte copies between Rust and OCaml (not exercised)";
        "Activity execution, polling and server round trips (not exercised)";
      ]
    ~make_workload ()

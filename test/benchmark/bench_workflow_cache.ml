(** Workflow cache memory under steady residency and eviction churn (#527).

    [BENCH_CACHE_RUNS] workflow runs are advanced round-robin, one per sample,
    through the production OCaml worker adapter. Each run is a loop of
    [BENCH_CACHE_DEPTH] one-millisecond timers that then completes and is
    evicted with [Workflow_execution_ending]; its next generation starts
    fresh under the same run ID. At most [BENCH_CACHE_CAPACITY] runs are
    resident. When a sample touches a non-resident run, the oldest resident
    run is evicted with [Cache_full] first, and the touched run is reloaded
    by replaying its initialization and every timer it had already fired,
    exactly as Core does after a cache miss. Oldest-first eviction equals LRU
    under round-robin access, so a capacity below the run count makes every
    sample a miss ("churn"), and a capacity equal to it never evicts for
    space ("steady").

    Every activation document is encoded before the baseline snapshot, so the
    fixture is not counted as load. Core's own workflow cache, history
    retention and sticky-queue state are excluded; see {!Benchmark_worker}
    for the exact boundary. *)

module W = Benchmark_worker
module Protocol = W.Protocol
module Codec = Temporal_base.Codec

(** Working set of distinct run IDs. *)
let runs = W.env_count ~name:"BENCH_CACHE_RUNS" ~default:1_000 ~limit:20_000

(** Maximum resident runs before a [Cache_full] eviction. *)
let capacity =
  min runs (W.env_count ~name:"BENCH_CACHE_CAPACITY" ~default:runs ~limit:20_000)

(** Timers each run generation fires before completing. *)
let depth = W.env_count ~name:"BENCH_CACHE_DEPTH" ~default:8 ~limit:64

(** Registered workflow type name. *)
let workflow_type = "benchmark_timer_steps"

(** Waits on [depth] sequential timers, then completes. *)
let workflow =
  Temporal_base.Definition.make ~name:workflow_type ~input:Codec.unit
    ~output:Codec.unit
    ~implementation:
      (Some
         (fun () ->
           match W.current_context () with
           | Error error -> Error error
           | Ok context ->
               let rec loop fired =
                 if fired = depth then Ok ()
                 else
                   match W.Future.await (W.Context.start_timer context 1L) with
                   | Ok () -> loop (fired + 1)
                   | Error error -> Error error
               in
               loop 0))

type documents = {
  start : bytes;
  reload : bytes;
  fire : bytes array;
  replay_fire : bytes array;
  cache_full : bytes;
  ending : bytes;
}
(** Pre-encoded activations for one run ID. [fire.(i)] and [replay_fire.(i)]
    fire timer [i + 1] live and during a reload respectively. *)

(** Encodes every activation one run ID can receive. *)
let documents_for index =
  let run_id = Printf.sprintf "cache-run-%05d" index in
  let encode ~replaying ~history_length jobs =
    W.encode
      (W.activation ~run_id ~replaying ~history_length:(Int64.of_int history_length)
         jobs)
  in
  let fire_job seq = Protocol.Fire_timer { seq = Int64.of_int seq } in
  {
    start =
      encode ~replaying:false ~history_length:3
        [ W.initialize ~run_id ~workflow_type ];
    reload =
      encode ~replaying:true ~history_length:3
        [ W.initialize ~run_id ~workflow_type ];
    fire =
      Array.init depth (fun i ->
          encode ~replaying:false ~history_length:(3 + (5 * (i + 1)))
            [ fire_job (i + 1) ]);
    replay_fire =
      Array.init depth (fun i ->
          encode ~replaying:true ~history_length:(3 + (5 * (i + 1)))
            [ fire_job (i + 1) ]);
    cache_full =
      encode ~replaying:false ~history_length:3
        [ W.remove_from_cache Protocol.Cache_full ];
    ending =
      encode ~replaying:false ~history_length:3
        [ W.remove_from_cache Protocol.Workflow_execution_ending ];
  }

(** All runs' documents, built at module initialization (before baselines). *)
let documents = Array.init runs documents_for

(** Total pre-encoded fixture bytes, reported so readers can relate it to the
    baseline live size. *)
let fixture_bytes =
  Array.fold_left
    (fun total doc ->
      let sum = Array.fold_left (fun t b -> t + Bytes.length b) 0 in
      total + Bytes.length doc.start + Bytes.length doc.reload + sum doc.fire
      + sum doc.replay_fire + Bytes.length doc.cache_full
      + Bytes.length doc.ending)
    0 documents

type state = {
  worker : W.t;
  progress : int array;  (** Timers fired by each run's current generation. *)
  resident : bool array;
  started : bool array;  (** Whether the current generation was ever loaded. *)
  order : int Queue.t;  (** Resident runs, oldest first, possibly stale. *)
  mutable resident_count : int;
  mutable next_run : int;
  mutable evictions : int;
  mutable reloads : int;
  mutable replayed_activations : int;
  mutable completions : int;
}
(** One repetition's model of Core's cache for the synthetic worker. Stale
    queue entries (runs already evicted on completion) are skipped. *)

(** Creates an empty cache model and registry. *)
let create_state () =
  {
    worker = W.create [ W.Worker.register workflow ];
    progress = Array.make runs 0;
    resident = Array.make runs false;
    started = Array.make runs false;
    order = Queue.create ();
    resident_count = 0;
    next_run = 0;
    evictions = 0;
    reloads = 0;
    replayed_activations = 0;
    completions = 0;
  }

(** Evicts the oldest resident run for space. Its progress is kept so the
    next touch replays it. *)
let rec evict_oldest state =
  match Queue.take_opt state.order with
  | None -> failwith "cache model has no resident run to evict"
  | Some run when not state.resident.(run) -> evict_oldest state
  | Some run ->
      W.expect_empty (W.deliver state.worker documents.(run).cache_full);
      state.resident.(run) <- false;
      state.resident_count <- state.resident_count - 1;
      state.evictions <- state.evictions + 1

(** Loads [run]: a fresh start for a new generation, or a replay of its
    initialization and already-fired timers after an eviction. *)
let load state run =
  if state.resident_count >= capacity then evict_oldest state;
  let docs = documents.(run) in
  let progress = state.progress.(run) in
  if state.started.(run) then (
    state.reloads <- state.reloads + 1;
    W.expect_timer 1L (W.deliver state.worker docs.reload);
    for i = 0 to progress - 1 do
      W.expect_timer (Int64.of_int (i + 2)) (W.deliver state.worker docs.replay_fire.(i))
    done;
    state.replayed_activations <- state.replayed_activations + progress + 1)
  else (
    W.expect_timer 1L (W.deliver state.worker docs.start);
    state.started.(run) <- true);
  state.resident.(run) <- true;
  state.resident_count <- state.resident_count + 1;
  Queue.push run state.order

(** One sample: touch the next run, loading it if needed, and fire its next
    timer. A generation that completes is evicted and restarts fresh. *)
let advance state =
  let run = state.next_run in
  state.next_run <- (run + 1) mod runs;
  if not state.resident.(run) then load state run;
  let docs = documents.(run) in
  let fired = state.progress.(run) + 1 in
  let commands = W.deliver state.worker docs.fire.(fired - 1) in
  if fired = depth then (
    W.expect_complete commands;
    W.expect_empty (W.deliver state.worker docs.ending);
    state.resident.(run) <- false;
    state.resident_count <- state.resident_count - 1;
    state.started.(run) <- false;
    state.progress.(run) <- 0;
    state.completions <- state.completions + 1)
  else (
    W.expect_timer (Int64.of_int (fired + 1)) commands;
    state.progress.(run) <- fired)

(** Evicts every resident run through ordinary eviction activations, as Core
    does on worker shutdown. *)
let evict_all state =
  Array.iteri
    (fun run resident ->
      if resident then (
        W.expect_empty (W.deliver state.worker documents.(run).cache_full);
        state.resident.(run) <- false))
    state.resident;
  state.resident_count <- 0

(** Evicts any remaining runs, then releases the registry. *)
let close state () =
  Fun.protect
    ~finally:(fun () -> W.discard state.worker)
    (fun () -> evict_all state)

(** Untimed per-repetition counts, plus the reachable OCaml data released by
    evicting the resident runs, which is the cache's own footprint. This
    evicts the cache, so it runs after the measurement snapshot. *)
let observations state () =
  let resident = state.resident_count in
  let with_cache = W.live_bytes () in
  evict_all state;
  let without_cache = W.live_bytes () in
  [
    ("resident_runs_at_end", `Int resident);
    ("cache_full_evictions", `Int state.evictions);
    ("replay_reloads", `Int state.reloads);
    ("replayed_activations", `Int state.replayed_activations);
    ("completed_generations", `Int state.completions);
    ("resident_cache_live_bytes", `Int (with_cache - without_cache));
    ( "live_bytes_per_resident_run",
      `Float
        (if resident = 0 then 0.
         else float_of_int (with_cache - without_cache) /. float_of_int resident)
    );
  ]

(** Makes one fresh cache model per repetition. *)
let make_workload _config =
  let state = create_state () in
  {
    Benchmark_harness.workload =
      { Benchmark_harness.sample = (fun _seed -> advance state); close = close state };
    observations = observations state;
  }

(** Emits the instrumented report for one cache configuration. *)
let () =
  Benchmark_harness.run_instrumented ~suite:"workflow-cache-memory"
    ~boundary:
      "One timer activation for the next round-robin run through strict \
       activation JSON decode, the production OCaml worker adapter and \
       completion encode, including any Cache_full eviction, replayed reload \
       and completion eviction it causes; no Core, FFI, supervisor, polling, \
       server or network"
    ~server_version:"none"
    ~workload_config:
      [
        ("admitted_concurrency", `Int 1);
        ("admission_model", `String "closed_loop");
        ("pending_attempt_backlog_peak", `Int 0);
        ("saturation_observation", `String "not_exercised");
        ("cache_runs", `Int runs);
        ("cache_capacity", `Int capacity);
        ("cache_mode", `String (if capacity < runs then "churn" else "steady"));
        ("timers_per_generation", `Int depth);
        ("eviction_policy", `String "oldest_first_equals_lru_round_robin");
        ("fixture_document_bytes", `Int fixture_bytes);
      ]
    ~unmeasured:
      [
        "Temporal Core workflow cache, retained histories and sticky-queue \
         state (Rust heap; not exercised)";
        "Rust serde JSON encoding of activations and decoding of completions \
         (not exercised)";
        "C stub byte copies between Rust and OCaml (not exercised)";
      ]
    ~make_workload ()

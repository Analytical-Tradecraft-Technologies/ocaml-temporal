(** Allocation and memory for replaying one long history through the OCaml
    worker adapter (#527).

    A synthetic history of sequential activities is replayed per sample: one
    replaying [Initialize_workflow] activation, one replaying
    [Resolve_activity] activation per step, then Core's
    [Workflow_execution_ending] eviction. Each step stands for six history
    events (workflow task completed, activity scheduled/started/completed,
    workflow task scheduled/started) plus five fixed start/end events, so
    [BENCH_HISTORY_EVENTS] selects the equivalent history length. Documents
    are encoded before the baseline snapshot, so the fixture is not counted as
    load. Temporal Core's own history processing is excluded; see
    {!Benchmark_worker} for the exact boundary. *)

module W = Benchmark_worker
module Protocol = W.Protocol
module Codec = Temporal_base.Codec

(** History events represented by one activity step. *)
let events_per_step = 6

(** Start and completion events outside the per-step pattern. *)
let fixed_events = 5

(** Requested equivalent history length, bounded to keep fixtures in memory. *)
let history_events =
  W.env_count ~name:"BENCH_HISTORY_EVENTS" ~default:10_000 ~limit:200_000

(** Encoded bytes of every activity argument and result. *)
let payload_bytes =
  W.env_count ~name:"BENCH_HISTORY_PAYLOAD_BYTES" ~default:128 ~limit:65_536

(** Activity steps whose equivalent history does not exceed the request. *)
let steps = max 1 ((history_events - fixed_events) / events_per_step)

(** The equivalent history length actually replayed. *)
let equivalent_events = fixed_events + (steps * events_per_step)

(** Registered workflow type name. *)
let workflow_type = "benchmark_sequential_activities"

(** Runs [steps] activities one after another, so every replayed resolution
    resumes workflow code and emits exactly the next schedule command. *)
let workflow =
  Temporal_base.Definition.make ~name:workflow_type ~input:Codec.unit
    ~output:Codec.string
    ~implementation:
      (Some
         (fun () ->
           match W.current_context () with
           | Error error -> Error error
           | Ok context ->
               let rec loop completed =
                 if completed = steps then Ok (string_of_int completed)
                 else
                   match
                     W.Future.await (W.schedule_activity context ~payload_bytes)
                   with
                   | Ok _ -> loop (completed + 1)
                   | Error error -> Error error
               in
               loop 0))

(** The single run ID; each sample evicts it before the next replay. *)
let run_id = "history-replay-run"

(** The replay documents: index 0 initializes, index [i] resolves step [i]. *)
let history =
  let result = W.protocol_payload (W.string_payload payload_bytes) in
  Array.init (steps + 1) (fun index ->
      let jobs =
        if index = 0 then [ W.initialize ~run_id ~workflow_type ]
        else [ W.resolve_activity ~payload:result (Int64.of_int index) ]
      in
      W.encode
        (W.activation ~run_id ~replaying:true
           ~history_length:(Int64.of_int (3 + (index * events_per_step)))
           jobs))

(** Core's eviction after the replayed run reaches its terminal command. *)
let eviction =
  W.encode
    (W.activation ~run_id ~replaying:false
       ~history_length:(Int64.of_int equivalent_events)
       [ W.remove_from_cache Protocol.Workflow_execution_ending ])

(** Total encoded activation bytes delivered per replay. *)
let history_document_bytes =
  Array.fold_left (fun total wire -> total + Bytes.length wire) 0 history
  + Bytes.length eviction

(** Replays steps [0..last] and checks each completion's single command. *)
let replay_prefix worker ~last =
  for index = 0 to last do
    let commands = W.deliver worker history.(index) in
    if index = steps then W.expect_complete commands
    else W.expect_schedule (Int64.of_int (index + 1)) commands
  done

(** One complete replay of the history followed by its eviction. *)
let replay_once worker =
  replay_prefix worker ~last:steps;
  W.expect_empty (W.deliver worker eviction)

(** Measures, outside timing, the live OCaml data held by the run just before
    its terminal step and after its eviction, then runs one attribution pass
    splitting boundary JSON work from workflow and adapter work. *)
let observations worker () =
  let before = W.live_bytes () in
  replay_prefix worker ~last:(steps - 1);
  let resident = W.live_bytes () in
  W.expect_complete (W.deliver worker history.(steps));
  W.expect_empty (W.deliver worker eviction);
  let after = W.live_bytes () in
  [
    ("resident_run_live_bytes_before_terminal_step", `Int (resident - before));
    ("live_bytes_after_eviction_minus_before_replay", `Int (after - before));
    ( "json_bridge_attribution",
      `Assoc (W.attribute worker ~units:1 (fun () -> replay_once worker)) );
  ]

(** Makes one registry per repetition; the run is evicted by every sample. *)
let make_workload _config =
  let worker = W.create [ W.Worker.register workflow ] in
  {
    Benchmark_harness.workload =
      {
        Benchmark_harness.sample = (fun _seed -> replay_once worker);
        close = (fun () -> W.discard worker);
      };
    observations = observations worker;
  }

(** Emits the instrumented report for one history length. *)
let () =
  Benchmark_harness.run_instrumented ~suite:"history-replay-memory"
    ~boundary:
      "One full replay of a synthetic sequential-activity history through \
       strict activation JSON decode, the production OCaml worker adapter, \
       deterministic execution, completion encode, and eviction; no Core, \
       FFI, supervisor, polling, server or network"
    ~server_version:"none"
    ~workload_config:
      [
        ("admitted_concurrency", `Int 1);
        ("admission_model", `String "closed_loop");
        ("pending_attempt_backlog_peak", `Int 0);
        ("saturation_observation", `String "not_exercised");
        ("history_events_requested", `Int history_events);
        ("history_events_equivalent", `Int equivalent_events);
        ("activity_steps", `Int steps);
        ("activations_per_replay", `Int (steps + 2));
        ("payload_bytes", `Int payload_bytes);
        ("history_document_bytes", `Int history_document_bytes);
      ]
    ~unmeasured:
      [
        "Temporal Core history buffers and replay state machines (Rust heap; \
         not exercised)";
        "Rust serde JSON encoding of activations and decoding of completions \
         (not exercised)";
        "C stub byte copies between Rust and OCaml (not exercised)";
      ]
    ~make_workload ()

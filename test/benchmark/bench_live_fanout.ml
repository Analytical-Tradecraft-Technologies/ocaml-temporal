(** Live high fan-out against a Temporal Server, with JSON bridge attribution
    (#528).

    One sample starts [BENCH_FANOUT_CONCURRENCY] workflow executions through
    the public client and waits for all of them. Each execution starts
    [BENCH_FANOUT_WIDTH] remote activities in its first activation, awaits
    them all with [Future.all] (the fan-in) and returns their count. Every
    activity echoes its argument, a JSON string of [BENCH_FANOUT_PAYLOAD_BYTES]
    encoded bytes. A single worker in this process runs both the workflow and
    the activities on one task queue, so sample latency covers client start,
    Temporal Server and PostgreSQL, Core, the Rust bridge, the C stubs, the
    supervisor, the OCaml worker adapter, workflow code and activity
    execution, through to the terminal result each client wait observes.

    After the timed phases of each repetition, an untimed attribution pass
    runs [BENCH_LIVE_ATTRIBUTION_SAMPLES] further samples with the SDK's
    bridge and workflow log sources at [Debug], and aggregates the records
    they emit:
    - every bridge call's native duration ([temporal.sdk.bridge] "bridge
      operation completed"), split by whether the call returned work,
      [Not_ready] or an error;
    - the OCaml protocol codec passes timed by
      [Observability.debug_timed] (activation and activity-task decoding,
      completion encoding and the activity completion validation pass);
    - each workflow activation's local processing time ([activate]).

    Those records cost a clock read and a reporter call each, so the pass
    is slower than the timed phases; its per-sample wall time is reported
    beside the measured latency so the overhead is visible. Native durations
    of polls that returned work bound the Rust half of the activation path
    (Core handoff, conversion, serde encoding and the C copy); completion
    calls also include Core and, for workflow tasks, the server round trip,
    so the Rust decode share of a completion is measured separately by the
    Rust [json_bridge] benchmark. *)

open Temporal
module Observability = Temporal_base.Observability

(** Reads a bounded positive integer from the environment, so suite-specific
    sizes travel through the shared Makefile command. *)
let env_count ~name ~default ~limit =
  match Sys.getenv_opt name with
  | None | Some "" -> default
  | Some text -> (
      match int_of_string_opt text with
      | Some value when value > 0 && value <= limit -> value
      | _ -> failwith (Printf.sprintf "%s must be in 1..%d" name limit))

(** Reads a non-empty string from the environment or returns [default]. *)
let env_string ~name ~default =
  match Sys.getenv_opt name with
  | None | Some "" -> default
  | Some value -> value

(** Activities started by each execution. Bounded so one execution's history
    (about six events per activity) stays well below Temporal's default
    50,000-event limit. *)
let width = env_count ~name:"BENCH_FANOUT_WIDTH" ~default:1_000 ~limit:5_000

(** Executions started together in each sample. The native client admits at
    most 64 starts in flight; this suite starts them one after another. *)
let concurrency =
  env_count ~name:"BENCH_FANOUT_CONCURRENCY" ~default:1 ~limit:64

(** Encoded bytes of each activity argument and result: a JSON string whose
    quotes are included, so at least two. *)
let payload_bytes =
  env_count ~name:"BENCH_FANOUT_PAYLOAD_BYTES" ~default:256 ~limit:262_144

(** Untimed samples in each repetition's attribution pass. *)
let attribution_samples =
  env_count ~name:"BENCH_LIVE_ATTRIBUTION_SAMPLES" ~default:1 ~limit:100

(** Temporal frontend, for example [http://temporal:7233] inside Compose. *)
let target_url =
  match Sys.getenv_opt "BENCH_LIVE_TARGET_URL" with
  | Some value when value <> "" -> value
  | _ -> failwith "BENCH_LIVE_TARGET_URL must name the Temporal frontend"

(** Namespace registered by the Compose stack's health check. *)
let namespace =
  env_string ~name:"BENCH_LIVE_NAMESPACE" ~default:"temporal-sdk-test"

(** Server image recorded in the report provenance. *)
let server_version =
  env_string ~name:"BENCH_SERVER_VERSION" ~default:"unspecified"

(** Rejects configurations whose histories would approach Temporal's default
    history size limits: arguments and results of one execution must stay
    within 32 MiB. The factors are bounded above, so the product cannot
    overflow. *)
let () =
  if payload_bytes < 2 then
    failwith "BENCH_FANOUT_PAYLOAD_BYTES must be at least 2 (a JSON string)";
  if width * payload_bytes * 2 > 32 * 1024 * 1024 then
    failwith
      "BENCH_FANOUT_WIDTH * BENCH_FANOUT_PAYLOAD_BYTES * 2 must be <= 32 MiB"

type plan = { fanout : int; payload_chars : int }
(** Workflow input. Carrying the shape in the input keeps the workflow
    deterministic on replay, independent of the worker's environment. *)

(** Encodes a plan as one interoperable JSON object. *)
let plan_codec =
  Codec.json_conv
    ~to_json:(fun plan ->
      `Assoc
        [ ("width", `Int plan.fanout); ("payload_chars", `Int plan.payload_chars) ])
    ~of_json:(function
      | `Assoc [ ("width", `Int fanout); ("payload_chars", `Int payload_chars) ]
        ->
          Ok { fanout; payload_chars }
      | _ -> Error (Error.codec ~message:"malformed fan-out plan"))

(** Returns its argument: the activity is pure overhead, so its time is SDK,
    Core and server cost rather than application work. *)
let echo =
  Activity.define ~name:"bench-live-fanout-echo" ~input:Codec.string
    ~output:Codec.string (fun payload -> Ok payload)

(** Starts every activity before awaiting any, then fans the results back in
    and returns how many arrived. *)
let workflow =
  Workflow.define ~name:"bench-live-fanout" ~input:plan_codec
    ~output:Codec.int (fun plan ->
      let payload = String.make plan.payload_chars 'x' in
      let futures =
        List.init plan.fanout (fun _ ->
            Activity.start
              ~start_to_close_timeout:(Duration.of_ms 60_000L)
              echo payload)
      in
      Result.map List.length (Future.await (Future.all futures)))

(** The plan every execution runs. *)
let plan = { fanout = width; payload_chars = payload_bytes - 2 }

(** Converts a public result into a sample failure the harness counts. *)
let get label = function
  | Ok value -> value
  | Error error -> failwith (label ^ ": " ^ Error.message error)

(** Aggregates the SDK log records of the attribution pass. Records arrive on
    whichever thread made the SDK call, so the tables are guarded by one
    mutex. A bridge call emits its "completed" record and, only when the
    result is unsuccessful, one status record immediately afterwards on the
    same thread; the completed record is therefore held per thread until the
    next record from that thread shows whether a status followed. *)
module Attribution = struct
  type bucket = {
    mutable count : int;
    mutable total_ms : float;
    mutable max_ms : float;
  }
  (** Count, summed and largest duration of one (source, operation, outcome)
      key. *)

  (** Guards [buckets] and [pending]. *)
  let lock = Mutex.create ()

  (** Aggregates keyed by source name, operation and outcome. *)
  let buckets : (string * string * string, bucket) Hashtbl.t =
    Hashtbl.create 64

  (** The last unclassified bridge completion of each thread. *)
  let pending : (int, string * float) Hashtbl.t = Hashtbl.create 8

  (** Adds one duration to a bucket; called with [lock] held. *)
  let add key duration_ms =
    let bucket =
      match Hashtbl.find_opt buckets key with
      | Some bucket -> bucket
      | None ->
          let bucket = { count = 0; total_ms = 0.; max_ms = 0. } in
          Hashtbl.replace buckets key bucket;
          bucket
    in
    bucket.count <- bucket.count + 1;
    bucket.total_ms <- bucket.total_ms +. duration_ms;
    bucket.max_ms <- Float.max bucket.max_ms duration_ms

  (** Classifies a held completion as successful; called with [lock] held. *)
  let flush_thread thread =
    match Hashtbl.find_opt pending thread with
    | None -> ()
    | Some (operation, duration_ms) ->
        Hashtbl.remove pending thread;
        add ("bridge", operation, "ok") duration_ms

  (** Routes one rendered SDK record into the aggregates. *)
  let observe ~source ~message tags =
    let operation = Logs.Tag.find Observability.Tag.operation tags in
    let duration = Logs.Tag.find Observability.Tag.duration_ms tags in
    let status = Logs.Tag.find Observability.Tag.bridge_status tags in
    let thread = Thread.id (Thread.self ()) in
    Mutex.protect lock (fun () ->
        match (source, message, operation, duration, status) with
        | "temporal.sdk.bridge", "bridge operation completed", Some op, Some ms, _
          ->
            flush_thread thread;
            Hashtbl.replace pending thread (op, ms)
        | "temporal.sdk.bridge", _, Some op, _, Some status -> (
            match Hashtbl.find_opt pending thread with
            | Some (held, ms) when held = op ->
                Hashtbl.remove pending thread;
                add ("bridge", op, status) ms
            | _ -> flush_thread thread)
        | "temporal.sdk.bridge", "codec operation completed", Some op, Some ms, _
          ->
            flush_thread thread;
            add ("codec", op, "ok") ms
        | "temporal.sdk.workflow", _, Some ("activate" as op), Some ms, _ ->
            add ("workflow", op, "ok") ms
        | _ -> ())

  (** Renders each record's constant message and forwards it to [observe].
      Records from other sources are dropped. *)
  let reporter =
    let report src _level ~over continuation messagef =
      messagef (fun ?header:_ ?(tags = Logs.Tag.empty) format ->
          Format.kasprintf
            (fun message ->
              observe ~source:(Logs.Src.name src) ~message tags;
              over ();
              continuation ())
            format)
    in
    { Logs.report }

  (** Runs [action] with this reporter installed and the bridge and workflow
      sources at [Debug], then restores the previous reporter and levels and
      classifies any completion still held. *)
  let capture action =
    let sources =
      [ Observability.Source.bridge; Observability.Source.workflow ]
    in
    let levels = List.map Logs.Src.level sources in
    let previous = Logs.reporter () in
    Mutex.protect lock (fun () ->
        Hashtbl.reset buckets;
        Hashtbl.reset pending);
    Logs.set_reporter reporter;
    List.iter (fun src -> Logs.Src.set_level src (Some Logs.Debug)) sources;
    Fun.protect
      ~finally:(fun () ->
        List.iter2 Logs.Src.set_level sources levels;
        Logs.set_reporter previous;
        Mutex.protect lock (fun () ->
            List.iter flush_thread
              (Hashtbl.fold (fun thread _ acc -> thread :: acc) pending [])))
      action

  (** Sums the buckets whose key satisfies [select]. *)
  let total select =
    Mutex.protect lock (fun () ->
        Hashtbl.fold
          (fun key bucket (count, ms) ->
            if select key then (count + bucket.count, ms +. bucket.total_ms)
            else (count, ms))
          buckets (0, 0.))

  (** Every bucket, sorted by key, as report rows. *)
  let rows ~units =
    let per_unit value = value /. float_of_int units in
    Mutex.protect lock (fun () ->
        Hashtbl.fold (fun key bucket acc -> (key, bucket) :: acc) buckets [])
    |> List.sort (fun (a, _) (b, _) -> compare a b)
    |> List.map (fun ((kind, operation, outcome), bucket) ->
           `Assoc
             [
               ("kind", `String kind);
               ("operation", `String operation);
               ("outcome", `String outcome);
               ("count_per_unit", `Float (per_unit (float_of_int bucket.count)));
               ("total_ms_per_unit", `Float (per_unit bucket.total_ms));
               ( "mean_us",
                 `Float (bucket.total_ms *. 1_000. /. float_of_int bucket.count)
               );
               ("max_ms", `Float bucket.max_ms);
             ])
end

(** Terminates every execution in [handles] and waits for its terminal
    result, so a failed sample cannot leave executions holding the worker's
    single activity slot while later samples run. A termination the server
    did not acknowledge (including one for an already closed execution) is
    not waited on, because that wait might never end; the sample still fails
    with its original error. *)
let retire handles =
  List.iter
    (fun handle ->
      match Client.terminate ~reason:"benchmark sample failed" handle with
      | Ok () -> ignore (Client.wait handle)
      | Error _ -> ())
    handles

(** Requires one execution to complete with every activity result. *)
let expect_fan_in handle =
  match get "wait" (Client.wait handle) with
  | Client.Completed { output; _ } when output = width -> ()
  | Client.Completed { output; _ } ->
      failwith (Printf.sprintf "fan-in returned %d results" output)
  | _ -> failwith "fan-out execution did not complete"

(** Starts every execution of one sample, then waits for each terminal
    result. Waiting in start order is sufficient: the executions progress on
    the server concurrently, and the sample ends when the last one is
    observed. If a start or a check fails, every execution already admitted
    and not yet observed is retired before the failure propagates, so the
    next sample starts with an idle worker. *)
let run_sample ~client ~task_queue ~next_id () =
  let rec start admitted index =
    if index = concurrency then List.rev admitted
    else begin
      incr next_id;
      match
        Client.start client ~workflow ~task_queue
          ~id:(Printf.sprintf "%s-%d-%d" task_queue !next_id index)
          ~input:plan ()
      with
      | Ok handle -> start (handle :: admitted) (index + 1)
      | Error error ->
          retire admitted;
          failwith ("start: " ^ Error.message error)
    end
  in
  let rec await = function
    | [] -> ()
    | handle :: rest -> (
        match expect_fan_in handle with
        | () -> await rest
        | exception failure ->
            retire rest;
            raise failure)
  in
  await (start [] 0)

(** Runs the attribution pass and derives the summary. Durations are
    reported per unit (one sample of [concurrency] executions). *)
let attribute ~sample =
  let started = Unix.gettimeofday () in
  Attribution.capture (fun () ->
      for _ = 1 to attribution_samples do
        sample ()
      done);
  let wall_ms = (Unix.gettimeofday () -. started) *. 1_000. in
  let units = attribution_samples in
  let per_unit value = value /. float_of_int units in
  let sum select = snd (Attribution.total select) in
  let count select = fst (Attribution.total select) in
  let worker_op prefixes (kind, operation, _) =
    kind = "bridge"
    && List.exists (fun prefix -> String.starts_with ~prefix operation) prefixes
  in
  let ready_polls = function
    | ("bridge", ("worker_try_poll_workflow" | "worker_try_poll_activity"), "ok")
      ->
        true
    | _ -> false
  in
  let empty_polls = function
    | ( "bridge",
        ("worker_try_poll_workflow" | "worker_try_poll_activity"),
        "not_ready" ) ->
        true
    | _ -> false
  in
  let completions = function
    | ( "bridge",
        ("worker_complete_workflow_json" | "worker_complete_activity_json"),
        _ ) ->
        true
    | _ -> false
  in
  let codec (kind, _, _) = kind = "codec" in
  let workflow_code (kind, _, _) = kind = "workflow" in
  let waits = worker_op [ "worker_wait_" ] in
  let client = worker_op [ "client_" ] in
  let codec_ms = sum codec in
  let ready_ms = sum ready_polls in
  let completion_ms = sum completions in
  let workflow_ms = sum workflow_code in
  let activations = count (fun (_, op, _) -> op = "workflow_activation_decode") in
  let activity_tasks = count (fun (_, op, _) -> op = "activity_task_decode") in
  let share value = if wall_ms <= 0. then 0. else value /. wall_ms in
  let per_second value =
    if wall_ms <= 0. then 0. else float_of_int value /. (wall_ms /. 1_000.)
  in
  `Assoc
    [
      ("units", `Int units);
      ("instrumented_wall_ms_per_unit", `Float (per_unit wall_ms));
      ("workflow_activations_per_unit", `Float (per_unit (float_of_int activations)));
      ( "activity_tasks_per_unit",
        `Float (per_unit (float_of_int activity_tasks)) );
      ("workflow_activations_per_second", `Float (per_second activations));
      ("activity_tasks_per_second", `Float (per_second activity_tasks));
      ("ocaml_json_codec_ms_per_unit", `Float (per_unit codec_ms));
      ("native_ready_poll_ms_per_unit", `Float (per_unit ready_ms));
      ("native_completion_ms_per_unit", `Float (per_unit completion_ms));
      ("native_empty_poll_ms_per_unit", `Float (per_unit (sum empty_polls)));
      ("native_wait_ms_per_unit", `Float (per_unit (sum waits)));
      ("client_calls_ms_per_unit", `Float (per_unit (sum client)));
      ("workflow_activation_ms_per_unit", `Float (per_unit workflow_ms));
      ("ocaml_json_codec_share_of_wall", `Float (share codec_ms));
      ("native_ready_poll_share_of_wall", `Float (share ready_ms));
      ("native_completion_share_of_wall", `Float (share completion_ms));
      ("workflow_activation_share_of_wall", `Float (share workflow_ms));
      ("operations", `List (Attribution.rows ~units));
    ]

(** Creates one repetition's worker, on its own Domain, and client, on a
    fresh task queue. [close] stops the worker's run loop, joins its Domain
    and shuts both down; a shutdown failure is reported rather than hidden. *)
let make_workload _config =
  let task_queue =
    Printf.sprintf "bench-live-fanout-%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  (* The client is created first: it owns no Domain, so a worker creation
     failure needs only the client shut down, and the worker's run Domain is
     spawned once both handles exist. Nothing after the spawn can fail before
     the harness receives [close]. *)
  let client = get "client create" (Client.create ~target_url ~namespace ()) in
  let worker =
    match
      Worker.create ~target_url ~namespace ~task_queue
        ~workflows:[ Worker.workflow workflow ]
        ~activities:[ Worker.activity echo ] ()
    with
    | Ok worker -> worker
    | Error error ->
        ignore (Client.shutdown client);
        failwith ("worker create: " ^ Error.message error)
  in
  let running = Domain.spawn (fun () -> Worker.run worker) in
  let next_id = ref 0 in
  let sample () = run_sample ~client ~task_queue ~next_id () in
  let close () =
    Worker.request_shutdown worker;
    let run_result = Domain.join running in
    let worker_result = Worker.shutdown worker in
    let client_result = Client.shutdown client in
    get "worker run" run_result;
    get "worker shutdown" worker_result;
    get "client shutdown" client_result
  in
  {
    Benchmark_harness.workload =
      { Benchmark_harness.sample = (fun _seed -> sample ()); close };
    observations =
      (fun () -> [ ("live_bridge_attribution", attribute ~sample) ]);
  }

(** Emits the instrumented report for one live fan-out configuration. *)
let () =
  Benchmark_harness.run_instrumented ~suite:"live-activity-fanout"
    ~rates:[ ("fanouts", concurrency); ("activities", width * concurrency) ]
    ~boundary:
      "Concurrent fan-out workflow executions from client start through \
       Temporal Server and PostgreSQL, Temporal Core, the Rust bridge, the C \
       stubs, the supervisor, the OCaml worker adapter, workflow code and \
       activity execution, to every terminal result observed by client wait; \
       one in-process worker on one task queue"
    ~server_version
    ~workload_config:
      [
        ("admitted_concurrency", `Int concurrency);
        ("admission_model", `String "closed_loop_concurrent_executions");
        ("pending_attempt_backlog_peak", `Int 0);
        ("saturation_observation", `String "not_exercised");
        ("fanout_width", `Int width);
        ("concurrent_executions", `Int concurrency);
        ("activities_per_sample", `Int (width * concurrency));
        ("payload_bytes", `Int payload_bytes);
        ("attribution_samples", `Int attribution_samples);
        ("namespace", `String namespace);
        ("worker_topology", `String "one_process_one_task_queue");
        ("remote_activity_slots", `Int 1);
      ]
    ~unmeasured:
      [
        "Temporal Server, PostgreSQL and the Docker VM (other processes; \
         not in this RSS)";
        "Allocation counters are Gc.quick_stat deltas taken on the benchmark \
         Domain while the worker, supervisor and activity Domains also run; \
         they are not attributed to a Domain, so compare them only between \
         runs on the same compiler";
        "Temporal Core, Tokio and Rust/serde heap (included in RSS, not \
         separable)";
      ]
    ~make_workload ()

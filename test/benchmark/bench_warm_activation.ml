(** Closed-loop synthetic activation throughput for a retained OCaml workflow.
    Each repetition keeps one execution and its suspended timer fiber in the
    cache; measured samples resolve one timer and validate the next command.
    There is no Core worker, native bridge, Temporal Server, or network. *)

module Activation = Temporal_runtime.Activation
module Execution = Temporal_runtime.Execution
module Context = Temporal_runtime.Workflow_context_store
module Future = Temporal_runtime.Future_store
module Definition = Temporal_base.Definition
module Codec = Temporal_base.Codec

(** Suspends on a durable timer after each firing so every measured activation
    resumes workflow code and emits the next timer command. *)
let rec timer_loop () =
  match Context.current () with
  | None ->
      Error
        (Temporal_base.Error.defect
           ~message:"warm benchmark ran outside a workflow execution")
  | Some context -> (
      match Future.await (Context.start_timer context 1L) with
      | Ok () -> timer_loop ()
      | Error error -> Error error)

(** The output is never reached during a bounded benchmark invocation. *)
let workflow =
  Definition.make ~name:"benchmark_warm_timer" ~input:Codec.unit
    ~output:Codec.unit ~implementation:(Some timer_loop)

(** Checks that an activation left exactly one pending one-millisecond timer.
    Its sequence is the only valid input for the next synthetic firing. *)
let pending_timer = function
  | [ Activation.Start_timer { seq; milliseconds = 1L } ] -> seq
  | _ -> failwith "warm activation did not emit one next timer"

(** Makes one independently seeded cached execution per repetition. The first
    warmup sample starts it; all measured samples run only the retained
    execution's timer-firing activation. Shutdown always releases its fiber. *)
let make_workload _config =
  let active = ref None in
  let sample seed =
    let execution, seq =
      match !active with
      | Some state -> state
      | None ->
          let execution = Execution.start ~randomness_seed:seed workflow () in
          active := Some (execution, 0L);
          let seq =
            pending_timer
              (Execution.activate execution [ Activation.Start_workflow ])
          in
          active := Some (execution, seq);
          (execution, seq)
    in
    let next =
      pending_timer
        (Execution.activate execution [ Activation.Fire_timer { seq } ])
    in
    if next <= seq then failwith "warm timer sequence did not advance";
    active := Some (execution, next)
  in
  let close () =
    Option.iter (fun (execution, _) -> Execution.shutdown execution) !active;
    active := None
  in
  { Benchmark_harness.sample; close }

(** Emits the shared versioned report with its precise local timing boundary.
    Closed-loop admission has no queued samples, so no saturation inference is
    made from this single-concurrency baseline. *)
let () =
  Benchmark_harness.run_repetitions ~suite:"ocaml-warm-cache-activation"
    ~boundary:
      "One Fire_timer activation on a retained OCaml execution through the \
       next Start_timer command and validation; no Core, FFI, polling, server \
       or network"
    ~server_version:"none"
    ~workload_config:
      [
        ("admitted_concurrency", `Int 1);
        ("admission_model", `String "closed_loop");
        ("pending_attempt_backlog_peak", `Int 0);
        ("saturation_observation", `String "not_exercised");
        ("timer_milliseconds", `Int 1);
        ("history_events", `Int 0);
      ]
    ~make_workload ()

(** Cold, no-server replay through Temporal Core, the native supervisor, and the
    OCaml worker adapter. Each sample owns a fresh native graph and drains one
    checked-in history to natural finalization before it can succeed. *)

module Native = Sdk_supervisor.Native
module Bridge = Temporal_core_bridge.Native_bridge
module Protocol = Temporal_protocol.Workflow_protocol
module Worker = Temporal_runtime.Native_worker_execution
module Execution = Temporal_runtime.Execution
module Codec = Temporal_base.Codec

(** Reads the fixed protobuf-history envelope before benchmark phases begin. *)
let read_history path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in channel)
    (fun () ->
      Bytes.of_string (really_input_string channel (in_channel_length channel)))

(** Turns an owner-Domain failure into a safe sample error without exposing a
    native resource or an unbounded workflow payload. *)
let require_native = function
  | Ok value -> value
  | Error (Native.Backend { message; _ }) -> failwith message
  | Error Native.Closed -> failwith "replay supervisor closed early"
  | Error (Native.Supervisor_failed exn | Native.Owner_unavailable exn) ->
      raise exn

(** Preserves the original sample failure when best-effort cleanup also fails. A
    cleanup failure following a successful sample remains a sample failure. *)
let protect_preserving_failure ~finally body =
  match body () with
  | value ->
      finally ();
      value
  | exception primary ->
      (try finally () with _ -> ());
      raise primary

(** Supplies the production worker adapter with replay operations and records
    evidence only after Core acknowledges each completion lease. *)
module Source = struct
  type t = {
    native : Native.t;
    mutable initial_jobs : int;
    mutable results : bytes list;
  }
  (** Per-sample observations, owned by the benchmark controller. *)

  type error = Native.error
  (** The native supervisor's typed operational error. *)

  (** Requires the historical start and two signals, and rejects a failed Core
      state machine rather than counting disposal as replay success. *)
  let try_poll_workflow source =
    Result.map
      (fun activation ->
        Option.iter
          (fun (value : Protocol.activation) ->
            (match value.jobs with
            | [
             Protocol.Initialize_workflow _;
             Protocol.Signal_workflow _;
             Protocol.Signal_workflow _;
            ] ->
                if not value.is_replaying then
                  failwith "historical activation was not marked replaying";
                source.initial_jobs <- source.initial_jobs + 1
            | _ -> ());
            List.iter
              (function
                | Protocol.Remove_from_cache
                    {
                      reason =
                        Nondeterminism | Fatal | Lang_fail | Unhandled_command;
                      _;
                    } ->
                    failwith "Core rejected the initial-signal history"
                | _ -> ())
              value.jobs)
          activation;
        activation)
      (Native.perform source.native Native.Try_poll_replay_workflow)

  (** Rejects workflow/task failures and retains the terminal result only after
      Core accepted the completion for its exact leased run. *)
  let complete_workflow source (completion : Protocol.completion) =
    if completion.task_failure <> None then
      failwith "cold replay failed its workflow task";
    let result =
      Native.perform source.native (Native.Complete_replay_workflow completion)
    in
    Result.iter
      (fun () ->
        List.iter
          (function
            | Protocol.Complete_workflow { result = Some payload } ->
                source.results <- source.results @ [ Bytes.copy payload.data ]
            | Protocol.Fail_workflow _ ->
                failwith "cold replay failed its workflow execution"
            | _ -> ())
          completion.commands)
      result;
    result

  (** Classifies a failed native operation in adapter diagnostics. *)
  let error_code _ = "replay_failed"

  (** Uses only bounded bridge-owned diagnostic text in adapter errors. *)
  let error_message = function
    | Native.Backend { message; _ } -> message
    | Native.Closed -> "replay supervisor closed"
    | Native.Supervisor_failed _ -> "replay supervisor failed"
    | Native.Owner_unavailable _ -> "replay supervisor could not start"
end

module Replay = Worker.Make (Source)
(** Executes the same private registration and completion logic used by live
    workflow workers, with the replay source in place of a server poller. *)

(** Builds a fresh deterministic signal-owning workflow for one replay sample.
    The closure state is never shared across histories or concurrent samples. *)
let registered_workflow observed =
  let handler =
    Execution.make_signal_handler ~name:"record" ~dispatch:(fun event ->
        match event.input with
        | [ payload ] -> (
            match Codec.decode Codec.string payload with
            | Ok value ->
                observed := !observed @ [ value ];
                Ok ()
            | Error error -> Error error)
        | _ ->
            Error
              (Temporal_base.Error.defect
                 ~message:"unexpected replay signal arity"))
  in
  let definition =
    Temporal_base.Definition.make ~name:"initial-signals" ~input:Codec.unit
      ~output:Codec.string
      ~implementation:(Some (fun () -> Ok (String.concat "," !observed)))
  in
  Worker.register ~signal_handlers:[ handler ] definition

(** Drains the replay lane until every activation is acknowledged and Core
    naturally finalizes. A bounded deadline turns a stalled lane into a failed
    sample; explicit shutdown in the caller is cleanup, never success. *)
let drain ~native ~registry =
  let deadline = Unix.gettimeofday () +. 30. in
  let rec loop () =
    if Unix.gettimeofday () > deadline then
      failwith "cold replay did not drain within 30 seconds";
    match Replay.poll registry with
    | Error error -> failwith error.message
    | Ok (Worker.Rejected _) ->
        failwith "OCaml rejected a cold replay activation"
    | Ok (Worker.Completed _) -> loop ()
    | Ok Worker.Not_ready -> (
        match Native.perform native Native.Finalize_replay with
        | Ok () -> ()
        | Error (Native.Backend { status = Bridge.Outstanding_tasks; _ }) ->
            (match Native.perform native Native.Wait_replay_workflow with
            | Ok () | Error (Native.Backend { status = Bridge.Not_ready; _ }) ->
                ()
            | Error error -> require_native (Error error));
            loop ()
        | Error error -> require_native (Error error))
  in
  loop ()

(** Runs a complete, isolated replay from native graph construction through
    natural finalization and graph shutdown, then verifies its observed jobs,
    signal order, and terminal command bytes. No Temporal client is created. *)
let replay_once ~history ~config () =
  let observed = ref [] in
  let native = require_native (Native.create ~capacity:8 ()) in
  let source = { Source.native; initial_jobs = 0; results = [] } in
  let registry_owner = ref None in
  protect_preserving_failure
    ~finally:(fun () ->
      let shutdown = Native.shutdown native in
      Option.iter Replay.discard !registry_owner;
      require_native shutdown)
    (fun () ->
      let registry =
        match
          Replay.create ~supervisor:source ~task_queue:"initial-signals"
            ~workflows:[ registered_workflow observed ]
            ()
        with
        | Ok registry -> registry
        | Error error -> failwith error.message
      in
      registry_owner := Some registry;
      require_native (Native.perform native (Native.Start_replay_worker config));
      require_native
        (Native.perform native (Native.Feed_replay_history history));
      require_native (Native.perform native Native.Finish_replay_input);
      drain ~native ~registry;
      if source.initial_jobs <> 1 then
        failwith "Core did not deliver one historical initial activation";
      if !observed <> [ "first"; "second" ] then
        failwith "replayed signal order changed";
      if source.results <> [ Bytes.of_string "\"first,second\"" ] then
        failwith "replayed terminal result changed")

(** Runs the shared bounded harness; after one failed attempt, later attempts
    fail immediately so a broken replay lane cannot consume the full sample
    count times the 30-second per-sample deadline. *)
let () =
  let history_path = Benchmark_harness.required_env "BENCH_REPLAY_HISTORY" in
  let history = read_history history_path in
  let config =
    match
      Native.worker_config ~namespace:"default" ~task_queue:"initial-signals"
        ~build_id:"cold-replay-benchmark" ~max_cached_workflows:10
        ~max_outstanding_workflow_tasks:10 ~max_concurrent_workflow_task_polls:2
        ~graceful_shutdown_timeout_ms:1_000L ()
    with
    | Ok config -> config
    | Error error -> failwith error.message
  in
  let first_failure = ref None in
  let workload _seed =
    match !first_failure with
    | Some error -> raise error
    | None -> (
        try replay_once ~history ~config ()
        with exn ->
          first_failure := Some exn;
          raise exn)
  in
  Benchmark_harness.run ~suite:"core-cold-replay"
    ~boundary:
      "One fresh Core replay worker, strict native bridge and supervisor, \
       OCaml activation/command processing, natural finalization and shutdown \
       per attempt; no Temporal client, server or network"
    ~server_version:"none"
    ~workload_config:
      [
        ("concurrency", `Int 1);
        ("admitted_concurrency", `Int 1);
        ("admission_model", `String "closed_loop");
        ("pending_attempt_backlog_peak", `Int 0);
        ("saturation_observation", `String "not_exercised");
        ("history_document_bytes", `Int (Bytes.length history));
        ("history_digest_md5", `String (Digest.to_hex (Digest.bytes history)));
        ("fixture", `String history_path);
      ]
    ~workload ()

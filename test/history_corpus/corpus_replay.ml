(** Private, server-free replay of one corpus history.

    This is the same path as [test/benchmark/bench_cold_replay.ml] and
    [test/integration/temporal/initial_signals]: a fresh supervisor instance
    owns a workflow-only Temporal Core replay worker, and the production
    [Native_worker_execution] adapter runs the registered OCaml definitions
    against Core's activations. No client, network connection or Temporal
    Server is involved. When the public [Temporal.Replay] API (issue #515)
    lands, issue #524 may switch the corpus runner to it; the manifest and
    histories are independent of which runner consumes them.

    Public [Temporal.Workflow] definitions and handlers are converted to the
    private registrations here, mirroring the package-private
    [Workflow_private], [Codec_private] and [Native_worker] conversions,
    because those modules are not visible to tests. The conversions only copy
    payloads and errors; they add no behavior. *)

module Native = Sdk_supervisor.Native
module Bridge = Temporal_core_bridge.Native_bridge
module Protocol = Temporal_protocol.Workflow_protocol
module Worker = Temporal_runtime.Native_worker_execution

(** {1 Public-to-private registration} *)

(** Copies a public payload into the base representation. *)
let payload_to_base (payload : Temporal.Payload.t) : Temporal_base.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Copies a base payload into the public representation. *)
let payload_of_base (payload : Temporal_base.Payload.t) : Temporal.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Rebuilds a public error as a base error, preserving every field Core
    encodes into a failure. *)
let error_to_base error =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ?error_type:view.error_type
    ~details:(List.map payload_to_base view.details)
    ~category:view.category ~message:view.message ()

(** Wraps a public codec in the base codec record. *)
let codec_to_base codec =
  Temporal_base.Codec.of_payload
    ~encode:(fun value ->
      Temporal.Codec.encode codec value
      |> Result.map payload_to_base |> Result.map_error error_to_base)
    ~decode:(fun payload ->
      Temporal.Codec.decode codec (payload_of_base payload)
      |> Result.map_error error_to_base)

(** Converts one frozen corpus workflow and its handlers into a private
    registration, exactly as the public worker does for live execution. *)
let register (Corpus_definitions.Workflow { definition; signals; queries; updates })
    =
  let implementation =
    Option.map
      (fun implementation input ->
        Result.map_error error_to_base (implementation input))
      (Temporal.Workflow.implementation definition)
  in
  let base =
    Temporal_base.Definition.make
      ~name:(Temporal.Workflow.name definition)
      ~input:(codec_to_base (Temporal.Workflow.input definition))
      ~output:(codec_to_base (Temporal.Workflow.output definition))
      ~implementation
  in
  let signal_handlers =
    List.map
      (fun handler ->
        Worker.make_signal_handler ~name:(Temporal.Signal.Handler.name handler)
          ~dispatch:(fun signal ->
            Worker.signal_input signal
            |> List.map payload_of_base
            |> Temporal.Signal.Handler.dispatch_payloads handler
            |> Result.map_error error_to_base))
      signals
  in
  let query_handlers =
    List.map
      (fun handler ->
        Worker.make_query_handler ~name:(Temporal.Query.Handler.name handler)
          ~dispatch:(fun query ->
            Worker.query_arguments query
            |> List.map payload_of_base
            |> Temporal.Query.Handler.dispatch_payloads handler
            |> Result.map payload_to_base |> Result.map_error error_to_base))
      queries
  in
  let update_handlers =
    List.map
      (fun handler ->
        Worker.make_update_handler ~name:(Temporal.Update.Handler.name handler)
          ~dispatch:(fun ~run_validator ~on_validated update ->
            Worker.update_input update
            |> List.map payload_of_base
            |> Temporal.Update.Handler.dispatch_payloads ~run_validator
                 ~on_validated handler
            |> Result.map payload_to_base |> Result.map_error error_to_base))
      updates
  in
  Worker.register ~signal_handlers ~query_handlers ~update_handlers base

(** {1 Observation} *)

(** What Core and the adapter did during one replay. Written only by the
    controller thread through the source callbacks below. *)
type observation = {
  native : Native.t;
  mutable initialized : (string * string) list;
      (** Run ID and workflow type of each [InitializeWorkflow] job. *)
  mutable evictions : (Protocol.eviction_reason * string) list;
  mutable task_failures : string list;
  mutable terminals : string list;
      (** Terminal commands that Core acknowledged, in order. *)
  mutable completion_errors : string list;
}

(** Safe text for a supervisor error. *)
let native_message = function
  | Native.Backend { message; _ } -> message
  | Native.Closed -> "replay supervisor closed"
  | Native.Supervisor_failed _ -> "replay supervisor failed"
  | Native.Owner_unavailable _ -> "replay supervisor could not start"

(** The replay source records every activation and acknowledged completion,
    then forwards it unchanged to the supervisor. It never decides the
    verdict; [replay] does that after natural finalization. *)
module Source = struct
  type t = observation
  type error = Native.error

  (** Records initialization and eviction jobs before the adapter sees them. *)
  let try_poll_workflow observation =
    let result =
      Native.perform observation.native Native.Try_poll_replay_workflow
    in
    Result.iter
      (Option.iter (fun (activation : Protocol.activation) ->
           List.iter
             (function
               | Protocol.Initialize_workflow { workflow_type; _ } ->
                   observation.initialized <-
                     observation.initialized
                     @ [ (activation.run_id, workflow_type) ]
               | Protocol.Remove_from_cache { reason; message } ->
                   observation.evictions <-
                     observation.evictions @ [ (reason, message) ]
               | _ -> ())
             activation.jobs))
      result;
    result

  (** Submits the adapter's canonical completion bytes and records task
      failures and terminal commands only after Core accepted them. *)
  let complete_workflow observation ~(completion : Protocol.completion) encoded
      =
    let result =
      Native.perform observation.native (Native.Complete_replay_workflow encoded)
    in
    (match result with
    | Ok () ->
        Option.iter
          (fun (failure : Protocol.failure) ->
            observation.task_failures <-
              observation.task_failures @ [ failure.message ])
          completion.task_failure;
        List.iter
          (function
            | Protocol.Complete_workflow _ ->
                observation.terminals <- observation.terminals @ [ "complete" ]
            | Protocol.Fail_workflow _ ->
                observation.terminals <- observation.terminals @ [ "fail" ]
            | Protocol.Continue_as_new _ ->
                observation.terminals <-
                  observation.terminals @ [ "continue_as_new" ]
            | Protocol.Cancel_workflow_execution ->
                observation.terminals <- observation.terminals @ [ "cancel" ]
            | _ -> ())
          completion.commands
    | Error error ->
        observation.completion_errors <-
          observation.completion_errors @ [ native_message error ]);
    result

  (** Classifies a failed native operation in adapter diagnostics. *)
  let error_code _ = "replay_failed"

  (** Uses only bounded bridge-owned diagnostic text. *)
  let error_message = native_message

  (** A replay completion is never resubmitted: the source cannot prove that
      the lease is still outstanding after a failure. *)
  let error_is_retryable _ = false

  (** A raised completion is equally fail-closed. *)
  let exception_is_retryable _ = false
end

module Replay = Worker.Make (Source)
(** The production adapter driven by the observing replay source. *)

(** {1 Verdicts} *)

(** The outcome of one replay, derived only after the replay worker has
    either finalized naturally or failed. *)
type verdict =
  | Replayed of { run_id : string; workflow_type : string; terminal : string }
      (** Every activation was accepted, the run's terminal command matched
          history, and the replay worker finalized naturally. *)
  | Nondeterministic of string
      (** Core evicted the run with a nondeterminism reason. *)
  | Failed of string
      (** Anything else: invalid history, task failure, rejection, stall, or
          incomplete replay. The text is a bounded diagnostic. *)

(** Describes a verdict for test output. *)
let describe = function
  | Replayed { terminal; _ } -> "replays_ok (terminal " ^ terminal ^ ")"
  | Nondeterministic message -> "nondeterminism: " ^ message
  | Failed message -> "failed: " ^ message

(** Builds the private replay-history document accepted by the bridge. The
    protobuf bytes are wrapped unchanged; the bridge and Core perform all
    history validation. *)
let replay_document ~workflow_id protobuf =
  match
    Temporal_protocol.Control_protocol.payload_json (Bytes.of_string protobuf)
  with
  | Error _ -> Error "history exceeds the bridge payload limit"
  | Ok history ->
      Ok
        (Yojson.Safe.to_string
           (`Assoc [ ("workflow_id", `String workflow_id); ("history", history) ]))

(** Shared bounded worker settings. Replay feeds one history directly to Core,
    so the namespace and queue are placeholders and never reach a server. *)
let worker_config () =
  match
    Native.worker_config ~namespace:"default" ~task_queue:"history-corpus-replay"
      ~build_id:"history-corpus-replay" ~max_cached_workflows:10
      ~max_outstanding_workflow_tasks:10 ~max_concurrent_workflow_task_polls:2
      ~graceful_shutdown_timeout_ms:1_000L ()
  with
  | Ok config -> config
  | Error (error : Bridge.error) -> failwith error.message

(** Upper bound for one replay. Corpus histories are small; a stall is a
    failure verdict rather than a hung CI job. *)
let deadline_seconds = 30.

(** Replays [protobuf] against [workflows] and returns its verdict. Every
    native resource is released before returning, on success and failure. *)
let replay ~workflow_id ~workflows protobuf =
  match Native.create ~capacity:8 () with
  | Error error -> Failed ("supervisor create: " ^ native_message error)
  | Ok native -> (
      let observation =
        {
          native;
          initialized = [];
          evictions = [];
          task_failures = [];
          terminals = [];
          completion_errors = [];
        }
      in
      let registry =
        Replay.create ~supervisor:observation
          ~task_queue:"history-corpus-replay"
          ~workflows:(List.map register workflows) ()
      in
      let finish verdict =
        (* Shutdown disposes any undrained replay worker and joins the owner
           Domain before OCaml executions are discarded. *)
        let shutdown = Native.shutdown native in
        Result.iter Replay.discard registry;
        match shutdown with
        | Ok () -> verdict
        | Error error -> Failed ("supervisor shutdown: " ^ native_message error)
      in
      match registry with
      | Error error -> finish (Failed ("registry: " ^ error.message))
      | Ok registry -> (
          let perform label operation =
            Native.perform native operation
            |> Result.map_error (fun error ->
                   label ^ ": " ^ native_message error)
          in
          let started =
            Result.bind (replay_document ~workflow_id protobuf) (fun document ->
            Result.bind
              (perform "start" (Native.Start_replay_worker (worker_config ())))
              (fun () ->
                Result.bind
                  (perform "feed"
                     (Native.Feed_replay_history (Bytes.of_string document)))
                  (fun () -> perform "finish input" Native.Finish_replay_input)))
          in
          let deadline = Unix.gettimeofday () +. deadline_seconds in
          (* Polls until Core has delivered every activation and the replay
             worker finalizes naturally; [Outstanding_tasks] means more
             activations or shutdown observation remain. *)
          let rec drain () =
            if Unix.gettimeofday () > deadline then Error "replay stalled"
            else
              match Replay.poll registry with
              | Error error -> Error ("adapter: " ^ error.message)
              | Ok (Worker.Rejected { error; _ }) ->
                  Error ("adapter rejected an activation: " ^ error.message)
              | Ok (Worker.Completed _) -> drain ()
              | Ok Worker.Not_ready -> (
                  match Native.perform native Native.Finalize_replay with
                  | Ok () -> Ok ()
                  | Error (Native.Backend { status = Bridge.Outstanding_tasks; _ })
                    -> (
                      match Native.perform native Native.Wait_replay_workflow with
                      | Ok ()
                      | Error (Native.Backend { status = Bridge.Not_ready; _ }) ->
                          drain ()
                      | Error error -> Error ("wait: " ^ native_message error))
                  | Error error -> Error ("finalize: " ^ native_message error))
          in
          let drained = Result.bind started drain in
          let nondeterminism =
            List.find_map
              (function
                | Protocol.Nondeterminism, message -> Some message | _ -> None)
              observation.evictions
          in
          let unexpected_eviction =
            List.find_map
              (function
                | ( (Protocol.Fatal | Protocol.Lang_fail | Protocol.Unhandled_command
                    | Protocol.Pagination_or_history_fetch | Protocol.Task_not_found
                    | Protocol.Eviction_unspecified | Protocol.Cache_full
                    | Protocol.Cache_miss),
                    message ) ->
                    Some message
                | _ -> None)
              observation.evictions
          in
          (* Nondeterminism is classified first: Core reports it through an
             eviction even when the adapter's completion was accepted. *)
          match (nondeterminism, drained) with
          | Some message, _ -> finish (Nondeterministic message)
          | None, Error message -> finish (Failed message)
          | None, Ok () -> (
              match
                ( observation.initialized,
                  observation.task_failures,
                  observation.completion_errors,
                  unexpected_eviction,
                  observation.terminals )
              with
              | [ (run_id, workflow_type) ], [], [], None, [ terminal ] ->
                  finish (Replayed { run_id; workflow_type; terminal })
              | _, failure :: _, _, _, _ -> finish (Failed ("task failure: " ^ failure))
              | _, _, error :: _, _, _ -> finish (Failed ("completion: " ^ error))
              | _, _, _, Some message, _ -> finish (Failed ("eviction: " ^ message))
              | initialized, _, _, _, terminals ->
                  finish
                    (Failed
                       (Printf.sprintf
                          "incomplete replay: %d initializations, terminals [%s]"
                          (List.length initialized)
                          (String.concat "; " terminals))))))

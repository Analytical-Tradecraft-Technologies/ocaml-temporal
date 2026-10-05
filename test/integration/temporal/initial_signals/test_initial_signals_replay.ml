(** Qualifies first-activation signal ordering through the actual pinned Core,
    native supervisor, worker adapter, and public signal dispatcher. *)
module Native = Sdk_supervisor.Native
module Bridge = Temporal_core_bridge.Native_bridge
module Protocol = Temporal_protocol.Workflow_protocol
module Worker = Temporal_runtime.Native_worker_execution
module Execution = Temporal_runtime.Execution

(** Reads the frozen synthetic history without retaining a file descriptor. *)
let read_file path =
  let channel = open_in_bin path in
  Fun.protect ~finally:(fun () -> close_in channel)
    (fun () -> really_input_string channel (in_channel_length channel))

(** Preserves a failed supervisor operation's safe diagnostic. *)
let require_native = function
  | Ok value -> value
  | Error (Native.Backend { message; _ }) -> failwith message
  | Error Native.Closed -> failwith "replay supervisor closed early"
  | Error (Native.Supervisor_failed exn | Native.Owner_unavailable exn) -> raise exn

(** Observes actual Core jobs and acknowledged OCaml completions outside the
    workflow, using the production supervisor's replay operations. *)
module Source = struct
  (** Test observations belong to the controller; native resources belong to
      the supervisor's owner Domain. *)
  type t = { native : Native.t; mutable initial_jobs : int;
             mutable results : bytes list }

  (** The adapter propagates the supervisor's typed operational errors. *)
  type error = Native.error

  (** Requires Core to produce the initialization-plus-signals trigger and
      rejects a failed state machine instead of accepting cleanup as success. *)
  let try_poll_workflow source =
    Result.map (fun activation ->
      Option.iter (fun (value : Protocol.activation) ->
        (match value.jobs with
         | [ Protocol.Initialize_workflow _;
             Protocol.Signal_workflow _; Protocol.Signal_workflow _ ] ->
             if not value.is_replaying then failwith "expected historical activation";
             source.initial_jobs <- source.initial_jobs + 1
         | _ -> ());
        List.iter (function
          | Protocol.Remove_from_cache
              { reason = (Nondeterminism | Fatal | Lang_fail | Unhandled_command); _ } ->
              failwith "Core rejected the initial signal history"
          | _ -> ()) value.jobs) activation;
      activation)
      (Native.perform source.native Native.Try_poll_replay_workflow)

  (** Records results only after Core has acknowledged the completion lease. *)
  let complete_workflow source (completion : Protocol.completion) =
    if completion.task_failure <> None then failwith "initial signals failed their task";
    let result = Native.perform source.native (Native.Complete_replay_workflow completion) in
    Result.iter (fun () ->
      List.iter (function
        | Protocol.Complete_workflow { result = Some payload } ->
            source.results <- source.results @ [ Bytes.copy payload.data ]
        | Protocol.Fail_workflow _ -> failwith "initial signals failed the workflow"
        | _ -> ()) completion.commands) result;
    result

  (** Supplies a stable classification for adapter diagnostics. *)
  let error_code _ = "replay_failed"

  (** Exposes only the supervisor's safe diagnostic message. *)
  let error_message = function
    | Native.Backend { message; _ } -> message
    | Native.Closed -> "replay supervisor closed"
    | Native.Supervisor_failed _ -> "replay supervisor failed"
    | Native.Owner_unavailable _ -> "replay supervisor could not start"
end

(** Uses the ordinary worker registry and deterministic execution machinery. *)
module Replay = Worker.Make (Source)

(** Runs a complete history without any client, network connection, or server.
    The root deliberately returns immediately, so losing a signal cannot be
    hidden by a timer or other artificial suspension. *)
let () =
  let observed = ref [] in
  let signal = Temporal.Signal.define ~name:"record" ~input:Temporal.Codec.string in
  let public_handler = Temporal.Signal.Handler.make signal
      (fun value -> observed := !observed @ [ value ]; Ok ()) in
  let handler = Execution.make_signal_handler ~name:"record"
      ~dispatch:(fun (event : Execution.signal) ->
        match event.input with
        | [ payload ] ->
            let payload : Temporal.Payload.t =
              { metadata = payload.metadata; data = Bytes.copy payload.data } in
            Temporal.Signal.Handler.dispatch public_handler payload
            |> Result.map_error (fun error ->
              Temporal_base.Error.make ~category:`Defect
                ~message:(Temporal.Error.message error) ())
        | _ -> failwith "unexpected replay signal arity") in
  let definition = Temporal_base.Definition.make ~name:"initial-signals"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.string
      ~implementation:(Some (fun () -> Ok (String.concat "," !observed))) in
  let native = require_native (Native.create ~capacity:8 ()) in
  let source = { Source.native; initial_jobs = 0; results = [] } in
  let registry = Result.get_ok (Replay.create ~supervisor:source
      ~task_queue:"initial-signals"
      ~workflows:[ Worker.register ~signal_handlers:[ handler ] definition ] ()) in
  Fun.protect
    ~finally:(fun () ->
      let shutdown = Native.shutdown native in
      Replay.discard registry;
      require_native shutdown)
    (fun () ->
      let config = Result.get_ok (Native.worker_config ~namespace:"default"
        ~task_queue:"initial-signals" ~build_id:"initial-signals-test"
        ~max_cached_workflows:10 ~max_outstanding_workflow_tasks:10
        ~max_concurrent_workflow_task_polls:2 ~graceful_shutdown_timeout_ms:1_000L ()) in
      require_native (Native.perform native (Native.Start_replay_worker config));
      require_native (Native.perform native (Native.Feed_replay_history
        (Bytes.of_string (read_file "history.replay.json"))));
      require_native (Native.perform native Native.Finish_replay_input);
      let deadline = Unix.gettimeofday () +. 30. in
      let rec drain () =
        if Unix.gettimeofday () > deadline then failwith "initial signal replay did not drain";
        match Replay.poll registry with
        | Error error -> failwith error.message
        | Ok (Worker.Rejected _) -> failwith "OCaml rejected an initial signal activation"
        | Ok (Worker.Completed _) -> drain ()
        | Ok Worker.Not_ready ->
            (match Native.perform native Native.Finalize_replay with
             | Ok () -> ()
             | Error (Native.Backend { status = Bridge.Outstanding_tasks; _ }) ->
                 (match Native.perform native Native.Wait_replay_workflow with
                  | Ok () | Error (Native.Backend { status = Bridge.Not_ready; _ }) -> ()
                  | Error error -> require_native (Error error));
                 drain ()
             | Error error -> require_native (Error error)) in
      drain ();
      if source.initial_jobs <> 1 then failwith "Core did not deliver initial signals";
      if !observed <> [ "first"; "second" ] then failwith "initial signal order changed";
      if source.results <> [ Bytes.of_string "\"first,second\"" ] then
        failwith "root completed without observing both initial signals";
      Printf.printf "PASS initial signals: both observed before root, naturally finalized\n%!")

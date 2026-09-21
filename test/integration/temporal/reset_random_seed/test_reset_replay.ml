(** Offline replay of a real server reset through Core, the strict bridge, and
    the OCaml worker adapter. Natural finalization is required for success;
    disposal in cleanup cannot turn an incomplete replay into a passing test. *)
module Native = Sdk_supervisor.Native
module Bridge = Temporal_core_bridge.Native_bridge
module Protocol = Temporal_protocol.Workflow_protocol
module Worker = Temporal_runtime.Native_worker_execution

(** Reads one checked-in fixture with deterministic channel cleanup. *)
let read_file path =
  let channel = open_in_bin path in
  Fun.protect ~finally:(fun () -> close_in channel)
    (fun () -> really_input_string channel (in_channel_length channel))

(** Requires a successful owner-Domain operation, retaining its safe diagnostic. *)
let require_native = function
  | Ok value -> value
  | Error (Native.Backend { message; _ }) -> failwith message
  | Error Native.Closed -> failwith "replay supervisor closed early"
  | Error (Native.Supervisor_failed exn) -> raise exn

(** Connects the production worker adapter to the private replay operations.
    Observations belong to the test controller, outside workflow execution. *)
module Source = struct
  (** One native owner and the reset/completion evidence it has acknowledged. *)
  type t = { native : Native.t; mutable seeds : string list;
             mutable results : string list }
  type error = Native.error

  (** Rejects machine failures and records the actual reset seed supplied by Core. *)
  let try_poll_workflow source =
    Result.map (fun activation ->
      Option.iter (fun (value : Protocol.activation) ->
        List.iter (function
          | Protocol.Update_random_seed { randomness_seed } ->
              source.seeds <- source.seeds @ [ randomness_seed ]
          | Protocol.Remove_from_cache
              { reason = (Nondeterminism | Fatal | Lang_fail | Unhandled_command); _ } ->
              failwith "Core rejected the recorded reset history"
          | _ -> ()) value.jobs) activation;
      activation)
      (Native.perform source.native Native.Try_poll_replay_workflow)

  (** Asserts the exact workflow output as well as Core's command comparison. *)
  let complete_workflow source (completion : Protocol.completion) =
    if completion.task_failure <> None then failwith "replay failed its workflow task";
    let result = Native.perform source.native (Native.Complete_replay_workflow completion) in
    Result.iter (fun () ->
      List.iter (function
        | Protocol.Complete_workflow { result = Some payload } ->
            let value = Yojson.Safe.from_string (Bytes.to_string payload.data) in
            (match value with
             | `String result -> source.results <- source.results @ [ result ]
             | _ -> failwith "replay result is not a string")
        | Protocol.Fail_workflow _ -> failwith "replay failed its workflow execution"
        | _ -> ()) completion.commands) result;
    result

  (** The adapter includes this bounded classification in any test failure. *)
  let error_code _ = "replay_failed"

  (** Uses only bridge-owned safe error text in adapter diagnostics. *)
  let error_message = function
    | Native.Backend { message; _ } -> message
    | Native.Closed -> "replay supervisor closed"
    | Native.Supervisor_failed _ -> "replay supervisor failed"
end

(** Uses the same registration, activation translation, and scheduler as live work. *)
module Replay = Worker.Make (Source)

(** Runs the frozen history without any Temporal client or network connection. *)
let () =
  let native = require_native (Native.create ~capacity:8 ()) in
  let source = { Source.native; seeds = []; results = [] } in
  let definition = Temporal_base.Definition.make ~name:"reset-random-seed"
      ~input:Temporal_base.Codec.string ~output:Temporal_base.Codec.string
      ~implementation:(Some (fun input ->
        Result.map_error (fun error -> Temporal_base.Error.make ~category:`Defect
          ~message:(Temporal.Error.message error) ()) (Reset_definition.run input))) in
  let registry = Result.get_ok (Replay.create ~supervisor:source
      ~task_queue:"reset-random-seed" ~workflows:[ Worker.register definition ] ()) in
  Fun.protect
    ~finally:(fun () ->
      let shutdown = Native.shutdown native in
      Replay.discard registry;
      require_native shutdown)
    (fun () ->
      let config = Result.get_ok (Native.worker_config ~namespace:"default"
        ~task_queue:"reset-random-seed" ~build_id:"reset-replay-test"
        ~max_cached_workflows:10 ~max_outstanding_workflow_tasks:10
        ~max_concurrent_workflow_task_polls:2 ~graceful_shutdown_timeout_ms:1_000L ()) in
      require_native (Native.perform native (Native.Start_replay_worker config));
      require_native (Native.perform native (Native.Feed_replay_history
        (Bytes.of_string (read_file "reset-history.replay.json"))));
      require_native (Native.perform native Native.Finish_replay_input);
      let deadline = Unix.gettimeofday () +. 30. in
      let rec drain () =
        if Unix.gettimeofday () > deadline then failwith "reset replay did not drain";
        match Replay.poll registry with
        | Error error -> failwith error.message
        | Ok (Worker.Rejected _) -> failwith "OCaml rejected a reset activation"
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
      if List.length source.seeds <> 1 then failwith "replay did not deliver one reset seed";
      let expected = String.trim (read_file "expected-result.txt") in
      if source.results <> [ expected ] then failwith "reset random result changed during replay";
      Printf.printf "PASS reset history: seed=%s result=%s, naturally finalized\n%!"
        (List.hd source.seeds) expected)

(** A no-server synthetic activation workload for the shared benchmark harness.
    Each attempt creates, activates, validates, and shuts down one OCaml
    execution; it does not traverse Temporal Core or the network. *)

module Activation = Temporal_runtime.Activation
module Execution = Temporal_runtime.Execution
module Definition = Temporal_base.Definition
module Codec = Temporal_base.Codec

(** An immediately completing workflow with no payload or external effects. *)
let workflow =
  Definition.make ~name:"benchmark_minimal" ~input:Codec.unit ~output:Codec.unit
    ~implementation:(Some (fun () -> Ok ()))

(** Runs one independently seeded execution and validates its terminal command.
*)
let run_workload seed =
  let execution = Execution.start ~randomness_seed:seed workflow () in
  Fun.protect
    ~finally:(fun () -> Execution.shutdown execution)
    (fun () ->
      match Execution.activate execution [ Activation.Start_workflow ] with
      | [ Activation.Complete_workflow payload ] -> (
          match Codec.decode Codec.unit payload with
          | Ok () -> ()
          | Error error ->
              failwith
                ("invalid completion payload: "
                ^ Temporal_base.Error.message error))
      | _ -> failwith "minimal workflow did not emit one terminal completion")

(** Runs the bounded benchmark and emits a versioned JSON report on stdout. *)
let () =
  Benchmark_harness.run ~suite:"local-minimal-activation"
    ~boundary:
      "OCaml execution creation, one synthetic Start_workflow activation, and \
       shutdown; no Core, FFI, polling, server, or network"
    ~server_version:"none"
    ~workload_config:
      [
        ("concurrency", `Int 1);
        ("payload_bytes", `Int 0);
        ("history_events", `Int 0);
      ]
    ~workload:run_workload ()

(** The defective source generation. This binary does not register the missing
    workflow and cannot execute the corrected implementation by a runtime flag. *)

(** Buffer a command after an earlier committed timer, then raise unexpectedly. *)
let body = Temporal.Workflow.define ~name:(Failure_support.workflow_type "body")
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      let open Temporal.Result_syntax in
      let* () = Failure_support.history_boundary () in
      ignore (Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 60_000L));
      failwith "intentional fixture body defect")

(** Encoding fails after the workflow has replayable history and returns. *)
let encoder = Temporal.Codec.make ~encoding:"binary/plain"
    ~encode:(fun _ -> failwith "intentional fixture encoder defect")
    ~decode:(fun bytes -> Ok (Bytes.to_string bytes))

(** The successful body exposes the separate output-encoder boundary. *)
let encoded = Temporal.Workflow.define ~name:(Failure_support.workflow_type "encoder")
    ~input:Temporal.Codec.unit ~output:encoder (fun () ->
      let open Temporal.Result_syntax in
      let* () = Failure_support.history_boundary () in
      Ok "recovered")

(** Starts only this generation's deliberately defective registrations. *)
let () = Failure_support.run_worker "broken"
    [ Temporal.Worker.workflow body; Temporal.Worker.workflow encoded ]

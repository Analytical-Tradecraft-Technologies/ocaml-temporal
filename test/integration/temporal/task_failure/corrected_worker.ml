(** Compatible corrected workflow sources, linked independently from the
    defective worker. Both timer histories must replay in this fresh process. *)

(** The committed prefix is unchanged; the repaired task returns a value. *)
let repaired name = Temporal.Workflow.define ~name:(Failure_support.workflow_type name)
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      let open Temporal.Result_syntax in
      let* () = Failure_support.history_boundary () in
      Ok "recovered")

(** A previously missing definition now handles the same outstanding run. *)
let missing = Temporal.Workflow.define ~name:(Failure_support.workflow_type "missing")
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () -> Ok "recovered")

(** Starts the corrected source generation on the original task queue. *)
let () = Failure_support.run_worker "corrected"
    (List.map Temporal.Worker.workflow [ repaired "body"; repaired "encoder"; missing ])

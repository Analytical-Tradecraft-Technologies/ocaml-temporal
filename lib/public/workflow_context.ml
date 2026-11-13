(** Reports whether this OCaml Domain is currently running workflow code. This
    is a diagnostic API. If workflow logic branches on the result, replay must
    take the same branch. *)
let is_active () =
  Option.is_some (Temporal_sdk_kernel.Workflow_context_store.current ())

module Local = struct
  (** A public key paired with the private runtime key that stores one value
      independently in every workflow execution. *)
  type 'a t = {
    key : 'a Temporal_sdk_kernel.Workflow_context_store.local;
  }

  (** Allocates an execution-local key without requiring an active workflow.
      Definitions create keys at module initialization and handlers use them
      later when their owning execution context is installed. *)
  let create () =
    { key = Temporal_sdk_kernel.Workflow_context_store.create_local () }

  (** Reads a value from the current workflow execution and fails closed when
      called from an ordinary client, activity, or infrastructure callback. *)
  let get local =
    match Temporal_sdk_kernel.Workflow_context_store.current () with
    | None ->
        Error
          (Error.defect
             ~message:"Temporal.Workflow_context.Local.get used outside a workflow execution")
    | Some context ->
        Ok (Temporal_sdk_kernel.Workflow_context_store.get_local context local.key)

  (** Stores a value in the current workflow execution. The owner scheduler
      serializes workflow and signal-handler callbacks, so a replacement is a
      deterministic history-derived state transition. A read-only callback
      (an update validator or a query handler) receives a typed defect and
      the value is unchanged, because replay would not repeat the write. *)
  let set local value =
    match Temporal_sdk_kernel.Workflow_context_store.current () with
    | None ->
        Error
          (Error.defect
             ~message:"Temporal.Workflow_context.Local.set used outside a workflow execution")
    | Some context ->
        Result.map
          (fun () ->
            Temporal_sdk_kernel.Workflow_context_store.set_local context local.key value)
          (Error_private.check_writable ~what:"local value")
end

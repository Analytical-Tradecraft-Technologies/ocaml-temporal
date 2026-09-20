(** An ordinary installed-package worker must ignore acceptance-only settings.
    Without a live address, the native configuration path is exercised before
    allocation. The optional live mode also constructs and shuts down Core. *)
let () =
  let live_address = Sys.getenv_opt "TEMPORAL_TEST_INSTALLED_WORKER_ADDRESS" in
  let target_url = Option.value live_address ~default:"invalid://ordinary-worker" in
  let workflow =
    Temporal.Workflow.define ~name:"installed-worker-environment"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  match
    Temporal.Worker.create ~target_url ~namespace:"temporal-sdk-test"
      ~task_queue:"installed-worker-environment"
      ~workflows:[ Temporal.Worker.workflow workflow ] ~activities:[] ()
  with
  | Ok worker ->
      (match Temporal.Worker.shutdown worker with
      | Ok () when Option.is_some live_address -> ()
      | Ok () -> failwith "invalid native address was accepted"
      | Error error -> failwith (Temporal.Error.message error))
  | Error error ->
      if Option.is_some live_address
         || Temporal.Error.kind error <> "bridge"
         || Temporal.Error.message error <>
            "client configuration failed (configuration): target_url must be an absolute http or https URL"
      then failwith ("fixture settings changed ordinary worker: " ^ Temporal.Error.message error)

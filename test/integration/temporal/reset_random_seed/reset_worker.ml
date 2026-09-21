(** Opt-in live worker for capturing the timer-reset history used by the offline
    regression. Its controller owns the server and sends SIGTERM after capture. *)
let () =
  if Sys.getenv_opt "TEMPORAL_RUN_LIVE" <> Some "1" then
    failwith "reset worker requires TEMPORAL_RUN_LIVE=1";
  let worker = Result.get_ok (Temporal.Worker.create
      ~identity:"reset-random-seed-fixture"
      ~target_url:(Sys.getenv "TEMPORAL_ADDRESS") ~namespace:"default"
      ~task_queue:"reset-random-seed"
      ~workflows:[ Temporal.Worker.workflow Reset_definition.workflow ]
      ~activities:[] ()) in
  let stop = Atomic.make false in
  Sys.set_signal Sys.sigterm (Sys.Signal_handle (fun _ -> Atomic.set stop true));
  Sys.set_signal Sys.sigint (Sys.Signal_handle (fun _ -> Atomic.set stop true));
  let running = Domain.spawn (fun () -> Temporal.Worker.run worker) in
  Fun.protect
    ~finally:(fun () ->
      ignore (Result.get_ok (Temporal.Worker.shutdown worker));
      ignore (Result.get_ok (Domain.join running)))
    (fun () -> while not (Atomic.get stop) do Unix.sleepf 0.1 done)

(** An installed SDK consumer cannot name the private injection seam. *)
module Private = Temporal_runtime.Native_worker_observer
let () = ignore (Private.current ())

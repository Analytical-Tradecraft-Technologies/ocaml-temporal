(** Dedicated worker for the live sticky-cache eviction acceptance test.

    The ordinary smoke worker registers many workflow types and is useful for
    broad coverage, but it is the wrong fixture for proving a one-entry Core
    cache transition.  This process registers only the cache workflow and
    uses the same public [Temporal.Worker] API as an application worker. *)

module Worker = Temporal.Worker
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Reads a required setting without raising during worker startup. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Publishes a complete readiness marker in one rename, so Compose never
    observes a partially written marker while checking worker health. *)
let publish_marker path contents =
  let temporary = Filename.temp_file ~temp_dir:(Filename.dirname path)
      (Filename.basename path ^ ".tmp.") "" in
  try
    let channel = open_out_bin temporary in
    Fun.protect
      ~finally:(fun () -> close_out_noerr channel)
      (fun () -> output_string channel contents; flush channel);
    Sys.rename temporary path
  with exception_ ->
    (try Sys.remove temporary with _ -> ());
    raise exception_

(** Removes an old readiness marker before construction. A stale marker must
    never make a failed [Worker.create] look healthy to Compose. *)
let clear_marker path =
  try
    if Sys.file_exists path then Sys.remove path;
    if Sys.file_exists path then
      Error (Error.defect ~message:("cannot remove stale marker " ^ path))
    else Ok ()
  with exception_ ->
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot remove marker %s: %s" path
              (Printexc.to_string exception_)))

(** Runs the isolated cache worker.  The marker is written only after native
    worker construction succeeds; the public worker loop then owns the process
    until Compose stops it. *)
let run () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" ->
      let open Temporal.Result_syntax in
      let* ready_file = required_env "SMOKE_CACHE_EVICTION_READY_FILE" in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* () = clear_marker ready_file in
      let* worker =
        Acceptance_observer.with_worker (fun () ->
          Worker.create ~target_url ~namespace
            ~identity:"ocaml-temporal-cache-eviction-worker"
            ~max_cached_workflows:1 ~task_queue:Definitions.task_queue
            ~workflows:[ Worker.workflow Definitions.cache_eviction ]
            ~activities:[] ())
      in
      publish_marker ready_file "worker-ready\n";
      let run_result = Worker.run worker in
      let* () = run_result in
      Worker.shutdown worker
  | _ ->
      Error
        (Error.defect
           ~message:
             "cache eviction acceptance is not enabled; set \
              TEMPORAL_TWO_BINARY_LIVE=1")

let () =
  match run () with
  | Ok () -> print_endline "cache eviction worker stopped"
  | Error error ->
      Printf.eprintf "cache eviction worker failed (%s): %s\n%!"
        (Error.kind error) (Error.message error);
      exit 1

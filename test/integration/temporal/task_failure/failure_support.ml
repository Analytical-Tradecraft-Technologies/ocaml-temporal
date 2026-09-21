(** Public-SDK-only support for exact-run workflow-task failure recovery. No
    workflow can read the controller's files or select a source generation. *)

(** Isolated queue shared by the two worker executables and client. *)
let task_queue = "ocaml-task-failure-recovery"

(** Fixture names also identify immutable artifact files. *)
let cases = [ "body"; "encoder"; "missing"; "business-retryable"; "business-permanent" ]

(** Raises a useful process error at fixture boundaries, outside workflow code. *)
let require = function
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.kind error ^ ": " ^ Temporal.Error.message error)

(** Builds one stable workflow type for both separately compiled generations. *)
let workflow_type name = "task-failure." ^ name

(** Declares a client-only reference with no implementation. *)
let reference name = Temporal.Workflow.remote ~name:(workflow_type name)
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string

(** Publishes a complete marker by rename; no partial file can satisfy the
    controller's readiness or exact-run identity assertions. *)
let publish name contents =
  let path = Filename.concat (Sys.getenv "TASK_FAILURE_ARTIFACT_DIR") name in
  let temporary = path ^ ".tmp" in
  let channel = open_out temporary in
  Fun.protect ~finally:(fun () -> close_out channel)
    (fun () -> output_string channel contents);
  Sys.rename temporary path

(** Requires an explicit live-fixture opt-in before allocating native graphs. *)
let require_live () =
  if Sys.getenv_opt "TEMPORAL_TASK_FAILURE_LIVE" <> Some "1" then
    failwith "set TEMPORAL_TASK_FAILURE_LIVE=1 only for the isolated live gate"

(** Defines deliberate typed application errors with both retryability flags.
    Client.start supplies no workflow retry policy, so either run must close. *)
let business name non_retryable =
  Temporal.Workflow.define ~name:(workflow_type name)
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      Error (Temporal.Error.make ~category:`Workflow ~non_retryable
          ~message:"intentional business failure" ()))

(** Registrations common to both code generations retain deliberate failure
    behavior while the recoverable workflows change source code. *)
let business_registrations = [
  Temporal.Worker.workflow (business "business-retryable" false);
  Temporal.Worker.workflow (business "business-permanent" true);
]

(** A committed timer forces the corrected process to replay earlier history.
    The second, speculative timer in broken code must never reach the server. *)
let history_boundary () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 100L)

(** Runs a worker until the controller replaces its process. The signal handler
    only sets a flag; shutdown runs on an ordinary Domain and is joined before
    the stopped marker is published. Every successfully created graph is shut
    down even if startup, run, or marker publication raises. *)
let run_worker generation workflows =
  require_live ();
  let worker = require (Temporal.Worker.create
      ~identity:("task-failure-" ^ generation)
      ~target_url:(Sys.getenv "TEMPORAL_ADDRESS")
      ~namespace:(Sys.getenv "TEMPORAL_NAMESPACE") ~task_queue
      ~workflows:(workflows @ business_registrations) ~activities:[] ()) in
  let stop = Atomic.make false and finished = Atomic.make false in
  let signal _ = Atomic.set stop true in
  let previous = Sys.signal Sys.sigterm (Sys.Signal_handle signal) in
  let watcher = Domain.spawn (fun () ->
      while not (Atomic.get finished) && not (Atomic.get stop) do Unix.sleepf 0.05 done;
      if Atomic.get stop then require (Temporal.Worker.shutdown worker)) in
  Fun.protect ~finally:(fun () ->
      Atomic.set finished true;
      Domain.join watcher;
      Sys.set_signal Sys.sigterm previous;
      require (Temporal.Worker.shutdown worker)) (fun () ->
      publish (generation ^ ".ready") "ready\n";
      require (Temporal.Worker.run worker));
  publish (generation ^ ".stopped") "stopped\n"

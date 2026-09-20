(** Bounded live metadata acceptance executable. The worker and client modes run
    as separate processes against the Compose server; the controller replaces
    the worker between [start] and [finish] to force reconstruction from history. *)
module T = Temporal
module C = T.Client

(** Isolates metadata workflows from all other acceptance scenarios. *)
let queue = "ocaml-start-metadata"

(** A signal releases each parked workflow only after the worker is replaced. *)
let release = T.Signal.define ~name:"release" ~input:T.Codec.unit

(** Run-local signal state is reconstructed on replay without external I/O. *)
let released = T.Workflow_context.Local.create ()

(** Signal delivery updates only its owning workflow run. *)
let release_handler = T.Signal.Handler.make release (fun () ->
  T.Workflow_context.Local.set released true)

(** Read-only readiness query reports the historical metadata from workflow code. *)
let status = T.Query.define ~name:"metadata" ~output:T.Codec.string

(** Decodes only the fields asserted by this fixture while preserving all other
    metadata in the SDK. Unknown server-added search attributes are harmless. *)
let metadata_summary () =
  let open T.Result_syntax in
  let* metadata = T.Workflow.start_metadata () in
  let field key = function
    | None -> Ok "-"
    | Some fields -> match List.assoc_opt key fields with
        | None -> Ok "-"
        | Some value -> T.Codec.decode T.Codec.string value
  in
  let* memo = field "note" metadata.memo in
  let* search = field "MetadataKeyword" metadata.search_attributes in
  let expiration = if Option.is_some metadata.execution_expiration_time then "deadline" else "none" in
  Ok (memo ^ ":" ^ search ^ ":" ^ expiration)

(** The query can be repeated after cache eviction and worker replacement. *)
let status_handler = T.Query.Handler.make status metadata_summary

(** Continue-as-new uses the same workflow type and codecs in its successor. *)
let target = T.Workflow.remote ~name:"metadata.roundtrip"
    ~input:T.Codec.string ~output:T.Codec.string

(** Reads metadata on initialization, across a durable timer, and after a
    signal. Every observation must match the recorded expectation in the input. *)
let workflow = T.Workflow.define ~name:"metadata.roundtrip"
    ~input:T.Codec.string ~output:T.Codec.string (fun input ->
  let open T.Result_syntax in
  let continue = String.starts_with ~prefix:"continue|" input in
  let expected = if continue then String.sub input 9 (String.length input - 9) else input in
  let* initial = metadata_summary () in
  if initial <> expected then Error (T.Error.defect ~message:("initial metadata mismatch: " ^ initial))
  else if continue then T.Workflow.continue_as_new target expected
  else
    let* () = T.Workflow.sleep (T.Duration.of_ms 1L) in
    let* () = T.Condition.wait_until (fun () ->
      match T.Workflow_context.Local.get released with Ok (Some true) -> true | _ -> false) in
    let* resumed = metadata_summary () in
    if resumed = expected then Ok resumed
    else Error (T.Error.defect ~message:"metadata changed after replay"))

(** Reads an explicit harness setting; callers opt into the real service. *)
let env name = match Sys.getenv_opt name with
  | Some value when value <> "" -> value
  | _ -> failwith (name ^ " must be set")

(** Converts an expected operation failure into a short process-level diagnostic. *)
let ok = function
  | Ok value -> value
  | Error error -> failwith (T.Error.kind error ^ ": " ^ T.Error.message error)

(** Connects a client with identity distinct from either worker generation. *)
let client () = ok (C.create ~target_url:(env "TEMPORAL_ADDRESS")
    ~namespace:(env "TEMPORAL_NAMESPACE") ~identity:"metadata-client" ())

(** Records exact run identities for later inspection and post-restart signals. *)
let write_runs path runs =
  let channel = open_out path in
  Fun.protect ~finally:(fun () -> close_out_noerr channel) (fun () ->
    List.iter (fun (id, run_id, expected) ->
      Printf.fprintf channel "%s\t%s\t%s\n" id run_id expected) runs)

(** Restores only complete run records written by the successful start phase. *)
let read_runs path =
  let channel = open_in path in
  Fun.protect ~finally:(fun () -> close_in_noerr channel) (fun () ->
    let rec loop acc = match input_line channel with
      | line -> (match String.split_on_char '\t' line with
          | [id; run_id; expected] -> loop ((id, run_id, expected)::acc)
          | _ -> failwith "malformed metadata run record")
      | exception End_of_file -> List.rev acc
    in loop [])

(** Reconstructs a public exact-run handle without starting more durable work. *)
let handle client id run_id = ok (C.follow client ~workflow
  { C.namespace = env "TEMPORAL_NAMESPACE"; workflow_id = id; run_id })

(** Waits for the workflow to report its expected snapshot. The process-level
    controller also supplies a timeout, so an RPC cannot hang acceptance. *)
let ready handle expected =
  let observed = ok (C.query handle ~query:status) in
  if observed <> expected then failwith ("unexpected query snapshot: " ^ observed)

(** Starts each advertised metadata option separately, their combination, and
    the same combination inherited through a continued run. *)
let start () =
  let client = client () in
  Fun.protect ~finally:(fun () -> ignore (C.shutdown client)) (fun () ->
    let payload = ok (T.Codec.encode T.Codec.string "value") in
    let cases = ["memo", true, false, false; "search", false, true, false;
                 "both", true, true, false; "continue", true, true, true] in
    let roots = ref [] in
    let runs = List.map (fun (name, memo, search, continue) ->
      let id = env "METADATA_PREFIX" ^ "-" ^ name in
      let expected = (if memo then "value" else "-") ^ ":" ^
        (if search then "value" else "-") ^ ":none" in
      let root = ok (C.start client ~workflow ~task_queue:queue ~id
        ~memo:(if memo then ["note", payload] else [])
        ~search_attributes:(if search then ["MetadataKeyword", payload] else [])
        ~input:((if continue then "continue|" else "") ^ expected) ()) in
      let root_id = C.run_id root in
      roots := (id, root_id, expected) :: !roots;
      let handle = if not continue then root else match ok (C.wait root) with
        | C.Continued_as_new execution -> ok (C.follow client ~workflow execution)
        | _ -> failwith "metadata workflow did not continue as new" in
      ready handle expected;
      Printf.printf "started %s root=%s active=%s\n%!" id root_id (C.run_id handle);
      id, C.run_id handle, expected) cases in
    write_runs (env "METADATA_RUNS") runs;
    write_runs (env "METADATA_RUNS" ^ ".roots") (List.rev !roots))

(** Resumes exact histories after worker replacement and checks durable output. *)
let finish () =
  let client = client () in
  Fun.protect ~finally:(fun () -> ignore (C.shutdown client)) (fun () ->
    List.iter (fun (id, run_id, expected) ->
      let handle = handle client id run_id in
      ready handle expected;
      ok (C.signal handle ~signal:release ~input:());
      match ok (C.wait handle) with
      | C.Completed value when value = expected -> Printf.printf "completed %s %s\n%!" id run_id
      | _ -> failwith ("unexpected terminal metadata outcome for " ^ id))
      (read_runs (env "METADATA_RUNS")))

(** Runs the public worker loop; each controller generation has a distinct
    identity, allowing raw histories to prove that replacement served the task. *)
let worker () =
  let worker = ok (T.Worker.create ~target_url:(env "TEMPORAL_ADDRESS")
    ~namespace:(env "TEMPORAL_NAMESPACE") ~identity:(env "METADATA_IDENTITY")
    ~task_queue:queue
    ~workflows:[T.Worker.workflow ~signals:[release_handler] ~queries:[status_handler] workflow]
    ~activities:[] ()) in
  Printf.printf "ready %s\n%!" (env "METADATA_IDENTITY");
  ok (T.Worker.run worker);
  ok (T.Worker.shutdown worker)

(** Dispatches only explicit live modes and returns a failing status on errors. *)
let () =
  try
    if env "TEMPORAL_METADATA_LIVE" <> "1" then failwith "live metadata acceptance is disabled";
    match Array.to_list Sys.argv with
    | [_; "worker"] -> worker ()
    | [_; "start"] -> start ()
    | [_; "finish"] -> finish ()
    | _ -> failwith "expected worker, start, or finish mode"
  with exception_ -> Printf.eprintf "%s\n%!" (Printexc.to_string exception_); exit 1

(** Bounded live metadata acceptance executable. The worker and client modes run
    as separate processes against the Compose server; the controller replaces
    the worker between [start] and [finish] to force reconstruction from history.

    A second, independent scenario ([cache-worker], [cache-start] and
    [cache-finish]) runs the same workflow on a worker whose sticky cache holds
    one run. The client interleaves three runs so that Temporal Core must evict
    each one with [RemoveFromCache] reason cache-full and later rebuild it from
    history (#512). The cache worker records payload-free activation and
    completion facts so the controller can prove that both happened. *)
module T = Temporal
module C = T.Client
module Observer = Temporal_runtime.Native_worker_observer
module Execution = Temporal_runtime.Native_worker_execution

(** Isolates metadata workflows from all other acceptance scenarios. *)
let queue = "ocaml-start-metadata"

(** Separate queue for the one-slot cache scenario. The default-cache worker of
    the replacement scenario must never poll these tasks: a second poller would
    let a run avoid the cache-full eviction this scenario has to prove. *)
let cache_queue = "ocaml-start-metadata-cache"

(** A signal releases each parked workflow only after the worker is replaced. *)
let release = T.Signal.define ~name:"release" ~input:T.Codec.unit

(** Run-local signal state is reconstructed on replay without external I/O. *)
let released = T.Workflow_context.Local.create ()

(** Set once the run has passed its durable timer and parked on the signal.
    Replay sets it again at the same point, so the phase query is
    deterministic. *)
let parked = T.Workflow_context.Local.create ()

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

(** Formats the exact expiration timestamp, keeping all nine nanosecond digits,
    so the controller can compare it before eviction and after replay. *)
let expiration_text = function
  | None -> "none"
  | Some time -> Printf.sprintf "%Ld.%09d" (T.Time.seconds time) (T.Time.nanoseconds time)

(** Reports the summary together with the exact expiration timestamp. The
    cache scenario asks for it while a run is still cached and again after the
    run has been evicted and replayed; both answers must be identical. *)
let exact = T.Query.define ~name:"metadata.exact" ~output:T.Codec.string

(** Reads the snapshot afresh on every call; it never caches a prior answer. *)
let exact_handler = T.Query.Handler.make exact (fun () ->
  let open T.Result_syntax in
  let* summary = metadata_summary () in
  let* metadata = T.Workflow.start_metadata () in
  Ok (summary ^ "|" ^ expiration_text metadata.execution_expiration_time))

(** Reports whether the run has finished its timer task and is waiting for the
    release signal. The cache scenario starts the next run only after this
    answers ["parked"], which fixes the order in which runs enter the cache. *)
let phase = T.Query.define ~name:"metadata.phase" ~output:T.Codec.string

(** Read-only: it reads run-local state and emits no command. *)
let phase_handler = T.Query.Handler.make phase (fun () ->
  match T.Workflow_context.Local.get parked with
  | Ok (Some true) -> Ok "parked"
  | Ok _ -> Ok "pending"
  | Error error -> Error error)

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
    let* () = T.Workflow_context.Local.set parked true in
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
      let root_id = Option.get (C.run_id root) in
      roots := (id, root_id, expected) :: !roots;
      let handle = if not continue then root else match ok (C.wait root) with
        | C.Continued_as_new execution -> ok (C.follow client ~workflow execution)
        | _ -> failwith "metadata workflow did not continue as new" in
      ready handle expected;
      Printf.printf "started %s root=%s active=%s\n%!" id root_id (Option.get (C.run_id handle));
      id, Option.get (C.run_id handle), expected) cases in
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
      | C.Completed { output = value; _ } when value = expected -> Printf.printf "completed %s %s\n%!" id run_id
      | _ -> failwith ("unexpected terminal metadata outcome for " ^ id))
      (read_runs (env "METADATA_RUNS")))

(** The single workflow registration shared by both worker modes. *)
let registration () =
  T.Worker.workflow ~signals:[release_handler]
    ~queries:[status_handler; exact_handler; phase_handler] workflow

(** Runs the public worker loop; each controller generation has a distinct
    identity, allowing raw histories to prove that replacement served the task. *)
let worker () =
  let worker = ok (T.Worker.create ~target_url:(env "TEMPORAL_ADDRESS")
    ~namespace:(env "TEMPORAL_NAMESPACE") ~identity:(env "METADATA_IDENTITY")
    ~task_queue:queue ~workflows:[registration ()] ~activities:[] ()) in
  Printf.printf "ready %s\n%!" (env "METADATA_IDENTITY");
  ok (T.Worker.run worker);
  ok (T.Worker.shutdown worker)

(** The three interleaved cache runs: memo, search attribute and an execution
    deadline in different combinations, so every snapshot field is checked
    after a cache-full replay. Each tuple is name, memo, search, deadline. *)
let cache_cases = ["cache-a", true, true, true; "cache-b", true, false, false;
                   "cache-c", false, true, true]

(** Expected [metadata] query answer for one start option combination. *)
let expected_summary ~memo ~search ~deadline =
  (if memo then "value" else "-") ^ ":" ^ (if search then "value" else "-") ^ ":"
  ^ if deadline then "deadline" else "none"

(** Polls the phase query until the run has parked. Each attempt is a short
    read-only query; an error before the first workflow task completes is
    retried. The bound (300 attempts, about 60 s) fails the scenario instead of
    waiting for the container's kill timeout. *)
let wait_parked handle =
  let rec loop attempt =
    match C.query handle ~query:phase with
    | Ok "parked" -> ()
    | (Ok _ | Error _) when attempt < 300 -> Unix.sleepf 0.2; loop (attempt + 1)
    | Ok observed -> failwith ("cache run did not park: " ^ observed)
    | Error error -> failwith ("cache run did not park: " ^ T.Error.message error)
  in loop 0

(** Writes one tab-separated [id, value] record per run. *)
let write_pairs path pairs =
  let channel = open_out path in
  Fun.protect ~finally:(fun () -> close_out_noerr channel) (fun () ->
    List.iter (fun (id, value) -> Printf.fprintf channel "%s\t%s\n" id value) pairs)

(** Reads the records written by [write_pairs]. *)
let read_pairs path =
  let channel = open_in path in
  Fun.protect ~finally:(fun () -> close_in_noerr channel) (fun () ->
    let rec loop acc = match input_line channel with
      | line -> (match String.split_on_char '\t' line with
          | [id; value] -> loop ((id, value) :: acc)
          | _ -> failwith "malformed cache snapshot record")
      | exception End_of_file -> List.rev acc
    in loop [])

(** Starts the cache runs one at a time and waits for each to park before the
    next starts. With one cache slot, run B's first task makes Core evict A
    (cache-full) and C's first task evicts B; C stays cached. Each run's exact
    snapshot is read while it is still cached and saved for [cache_finish]. *)
let cache_start () =
  let client = client () in
  Fun.protect ~finally:(fun () -> ignore (C.shutdown client)) (fun () ->
    let payload = ok (T.Codec.encode T.Codec.string "value") in
    let started = List.map (fun (name, memo, search, deadline) ->
      let id = env "METADATA_PREFIX" ^ "-" ^ name in
      let expected = expected_summary ~memo ~search ~deadline in
      (* The same timeouts as the official CLI case, which the history
         verifier checks for every [deadline] run. *)
      let execution_timeout, run_timeout, task_timeout =
        if deadline then Some (T.Duration.of_ms 3_600_000L),
          Some (T.Duration.of_ms 1_800_000L), Some (T.Duration.of_ms 10_000L)
        else None, None, None in
      let handle = ok (C.start client ~workflow ~task_queue:cache_queue ~id
        ~memo:(if memo then ["note", payload] else [])
        ~search_attributes:(if search then ["MetadataKeyword", payload] else [])
        ?execution_timeout ?run_timeout ?task_timeout ~input:expected ()) in
      wait_parked handle;
      let observed = ok (C.query handle ~query:exact) in
      let summary = match String.split_on_char '|' observed with
        | [summary; _] -> summary
        | _ -> failwith ("malformed exact snapshot: " ^ observed) in
      if summary <> expected then failwith ("unexpected cached snapshot: " ^ observed);
      let run_id = Option.get (C.run_id handle) in
      Printf.printf "cache started %s run=%s snapshot=%s\n%!" id run_id observed;
      (id, run_id, expected), (id, observed)) cache_cases in
    write_runs (env "METADATA_CACHE_RUNS") (List.map fst started);
    write_pairs (env "METADATA_CACHE_SNAPSHOTS") (List.map snd started))

(** Visits the runs in start order. The first query for A needs A's history
    replayed, which evicts the cached C (cache-full); B and C are likewise
    rebuilt from history when queried. Each replayed snapshot must equal the
    one read while the run was cached, including the exact expiration, and
    each run must complete with its exact expected result. *)
let cache_finish () =
  let client = client () in
  Fun.protect ~finally:(fun () -> ignore (C.shutdown client)) (fun () ->
    let snapshots = read_pairs (env "METADATA_CACHE_SNAPSHOTS") in
    List.iter (fun (id, run_id, expected) ->
      let handle = handle client id run_id in
      let observed = ok (C.query handle ~query:exact) in
      if Some observed <> List.assoc_opt id snapshots then
        failwith ("snapshot changed after cache-full replay for " ^ id ^ ": " ^ observed);
      ready handle expected;
      ok (C.signal handle ~signal:release ~input:());
      match ok (C.wait handle) with
      | C.Completed { output = value; _ } when value = expected ->
          Printf.printf "cache completed %s %s snapshot=%s\n%!" id run_id observed
      | _ -> failwith ("unexpected terminal cache outcome for " ^ id))
      (read_runs (env "METADATA_CACHE_RUNS")))

(** Appends one payload-free record per activation or acknowledged completion:
    kind, run ID, workflow ID (Core may omit it), replay flag, history length
    and cache-removal reason. Each line is flushed at once so the controller
    still has the records if the worker is killed. Callbacks run serialized on
    the worker's polling Domain, so lines never interleave. A failed write is
    reported on stderr and never raised, because an exception from the
    activation callback would fail the workflow task under test; a missing
    record then fails the controller's verification instead. *)
let cache_event channel kind (info : Execution.activation_info) =
  try
    Printf.fprintf channel "%s\t%s\t%s\t%b\t%Ld\t%s\n%!" kind info.run_id
      (Option.value info.workflow_id ~default:"-") info.is_replaying
      info.history_length (Option.value info.cache_removal_reason ~default:"-")
  with exception_ ->
    Printf.eprintf "cache event write failed: %s\n%!" (Printexc.to_string exception_)

(** Runs a worker with one sticky-cache slot, configured through
    [Worker.Options], on the cache queue. The private observer seam is scoped
    to this one construction and the worker owns the callbacks afterwards; the
    event channel stays open until the process exits. *)
let cache_worker () =
  let options = ok (T.Worker.Options.make ~max_cached_workflows:1 ()) in
  let channel = open_out_gen [Open_wronly; Open_creat; Open_trunc; Open_text] 0o644
      (env "METADATA_CACHE_EVENTS") in
  let callbacks = { Observer.on_activation = Some (cache_event channel "activation");
                    on_completion = Some (cache_event channel "completion") } in
  let worker = ok (Observer.with_callbacks callbacks (fun () ->
    T.Worker.create ~target_url:(env "TEMPORAL_ADDRESS")
      ~namespace:(env "TEMPORAL_NAMESPACE") ~identity:(env "METADATA_IDENTITY")
      ~options ~task_queue:cache_queue ~workflows:[registration ()] ~activities:[] ())) in
  if T.Worker.Options.max_cached_workflows (T.Worker.options worker) <> Some 1 then
    failwith "cache worker did not keep its one-slot cache bound";
  Printf.printf "ready %s max_cached_workflows=1\n%!" (env "METADATA_IDENTITY");
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
    | [_; "cache-worker"] -> cache_worker ()
    | [_; "cache-start"] -> cache_start ()
    | [_; "cache-finish"] -> cache_finish ()
    | _ -> failwith "expected worker, start, finish, cache-worker, cache-start, or cache-finish mode"
  with exception_ -> Printf.eprintf "%s\n%!" (Printexc.to_string exception_); exit 1

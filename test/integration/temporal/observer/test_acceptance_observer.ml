(** Exercises the actual fixture observers at the private adapter boundary.
    No process-global environment, server, or native handle is needed. *)

module Observer = Temporal_runtime.Native_worker_observer
module Adapter = Temporal_runtime.Native_worker_execution
module Protocol = Temporal_protocol.Workflow_protocol

(** Isolates every artifact and removes both files and empty failure-injection
    directories, including temporary files left behind by a broken observer. *)
let with_directory body =
  let directory = Filename.temp_file "temporal-observer-" "" in
  Sys.remove directory;
  Unix.mkdir directory 0o700;
  Fun.protect
    ~finally:(fun () ->
      Array.iter
        (fun name ->
          let path = Filename.concat directory name in
          if Sys.is_directory path then Unix.rmdir path else Sys.remove path)
        (Sys.readdir directory);
      Unix.rmdir directory)
    (fun () -> body directory)

(** Loads a fixture's closed JSON marker after its atomic publication. *)
let read_json path =
  let input = open_in_bin path in
  Fun.protect ~finally:(fun () -> close_in_noerr input) (fun () ->
      Yojson.Safe.from_channel input)

(** Reads the byte-exact ready marker, including its portable LF terminator. *)
let read_bytes path =
  let input = open_in_bin path in
  Fun.protect ~finally:(fun () -> close_in_noerr input) (fun () ->
      really_input_string input (in_channel_length input))

(** Creates fresh observers using a pure environment map. *)
let callbacks settings =
  match Acceptance_observer.create ~getenv:(fun key -> List.assoc_opt key settings) () with
  | Ok value -> value
  | Error error -> failwith (Temporal_base.Error.message error)

(** Supplies the single-run controller's existing exact identity contract. *)
let single_settings directory =
  [
    ("SMOKE_WORKER_REPLAY_DIAGNOSTICS_FILE", Filename.concat directory "replay.json");
    ("SMOKE_WORKER_GENERATION", "1");
    ("SMOKE_REPLAY_WORKFLOW_ID", "workflow-a");
  ]

(** Asserts that callbacks cannot leak from a completed construction scope. *)
let expect_empty () =
  let selected = Observer.current () in
  if Option.is_some selected.on_activation || Option.is_some selected.on_completion then
    failwith "observer construction scope leaked"

(** Nested construction, typed errors, exceptions, and other Domains must not
    share observer selection. Captured callbacks remain owned by the worker. *)
let test_scope_ownership () =
  expect_empty ();
  let seen = ref 0 in
  let outer : Observer.t =
    { on_activation = Some (fun _ -> incr seen); on_completion = None }
  in
  let empty = Observer.current () in
  let captured =
    Observer.with_callbacks outer (fun () ->
        Domain.join (Domain.spawn expect_empty);
        let thread_error = ref None in
        Thread.join
          (Thread.create
             (fun () -> try expect_empty () with error -> thread_error := Some error) ());
        Option.iter raise !thread_error;
        let failed : (unit, string) result =
          Observer.with_callbacks empty (fun () ->
              expect_empty ();
              Error "creation failed")
        in
        if failed <> Error "creation failed" then failwith "typed result changed";
        (try
           Observer.with_callbacks empty (fun () -> raise Exit)
         with Exit -> ());
        if Observer.current () != outer then failwith "nested scope was not restored";
        Observer.current ())
  in
  expect_empty ();
  Option.iter
    (fun callback ->
      callback
        { Adapter.run_id = "run-a"; workflow_id = Some "workflow-a";
          is_replaying = false; history_length = 1L; cache_removal_reason = None })
    captured.on_activation;
  if !seen <> 1 then failwith "worker lost its captured callback"

(** A deterministic lease source makes Core acknowledgement ordering visible. *)
type source = {
  queue : Protocol.activation Queue.t;
  leases : (string, unit) Hashtbl.t;
  mutable completions : Protocol.completion list;
  mutable reject_next : bool;
}

(** Allocates a fresh source ledger for one worker. *)
let source () =
  { queue = Queue.create (); leases = Hashtbl.create 2; completions = []; reject_next = false }

(** Implements just the adapter's two typed native operations. *)
module Source = struct
  type t = source
  type error = string

  (** Polling transfers exactly one lease into the ledger. *)
  let try_poll_workflow source =
    if Queue.is_empty source.queue then Ok None
    else
      let activation = Queue.take source.queue in
      Hashtbl.add source.leases activation.run_id ();
      Ok (Some activation)

  (** A rejected completion leaves its lease available for the retained retry. *)
  let complete_workflow source (completion : Protocol.completion) =
    if source.reject_next then begin
      source.reject_next <- false;
      Error "retry"
    end
    else if not (Hashtbl.mem source.leases completion.run_id) then
      Error "stale lease"
    else begin
      Hashtbl.remove source.leases completion.run_id;
      source.completions <- completion :: source.completions;
      Ok ()
    end

  (** Keeps injected failures bounded and free of native details. *)
  let error_code _ = "test_source"

  (** Returns the fixture's fixed diagnostic. *)
  let error_message error = error
end

module Worker = Adapter.Make (Source)
(** The production adapter owns callback execution and exception containment. *)

(** Builds a complete unit activation with an exact workflow/run identity. *)
let initial ?(workflow_id = "workflow-a") ?(run_id = "run-a") () : Protocol.activation =
  {
    run_id;
    timestamp = Some { seconds = 1L; nanoseconds = 0 };
    is_replaying = false;
    history_length = 1L;
    metadata = None;
    jobs =
      [ Protocol.Initialize_workflow
          { workflow_id; workflow_type = "observer-test"; arguments = [];
            randomness_seed = "1"; attempt = 1; context = None } ];
  }

(** Constructs the adapter in exactly the scoped capture pattern used by the
    private production wiring, while keeping all native calls deterministic. *)
let worker source selected =
  Observer.with_callbacks selected (fun () ->
      let { Observer.on_activation; on_completion } = Observer.current () in
      let definition =
        Temporal_base.Definition.make ~name:"observer-test"
          ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
          ~implementation:(Some (fun () -> Ok ()))
      in
      Result.get_ok
        (Worker.create ?on_activation ?on_completion ~supervisor:source
           ~workflows:[ Adapter.register definition ] ()))

(** Requires one accepted completion with no outstanding lease. *)
let expect_completed source worker =
  (match Worker.poll worker with
  | Ok (Adapter.Completed _) -> ()
  | _ -> failwith "expected an acknowledged activation");
  if Hashtbl.length source.leases <> 0 then failwith "activation lease leaked"

(** Cache barriers and cache-full evidence must remain absent until the exact
    retained completion is acknowledged. Replay records retain their existing
    pre-execution metadata semantics; they are not completion proof. *)
let test_cache_acknowledgement () =
  with_directory (fun directory ->
      let ready = Filename.concat directory "ready" in
      let evicted = Filename.concat directory "evicted.json" in
      let selected =
        callbacks
          (("SMOKE_WORKER_CACHE_EVICTION_READY_FILE", ready)
          :: ("SMOKE_WORKER_CACHE_EVICTION_FILE", evicted)
          :: single_settings directory)
      in
      let source = source () in
      let worker = worker source selected in
      Queue.add (initial ()) source.queue;
      source.reject_next <- true;
      (match Worker.poll worker with Error _ -> () | Ok _ -> failwith "retry expected");
      if Sys.file_exists ready || Sys.file_exists evicted then
        failwith "marker preceded Core acknowledgement";
      expect_completed source worker;
      if read_bytes ready <> "initial-completion\n" then failwith "ready marker changed";
      Queue.add
        { (initial ()) with timestamp = None;
          jobs = [ Protocol.Remove_from_cache { message = "pressure"; reason = Protocol.Cache_full } ] }
        source.queue;
      source.reject_next <- true;
      (match Worker.poll worker with Error _ -> () | Ok _ -> failwith "eviction retry expected");
      if Sys.file_exists evicted then failwith "eviction marker preceded Core acknowledgement";
      expect_completed source worker;
      if read_json evicted <>
         `Assoc [ ("workflow_id", `String "workflow-a"); ("run_id", `String "run-a");
                  ("reason", `String "cache_full") ] then
        failwith "eviction marker lost its exact identity";
      if List.length source.completions <> 2 then failwith "lease completed twice")

(** Atomic replacement failures remove their temporary files. Activation
    observer exceptions retire the lease through the typed failure path;
    completion observer exceptions cannot undo an already retired lease. *)
let test_write_failure_cleanup ~completion =
  with_directory (fun directory ->
      let blocked = Filename.concat directory (if completion then "ready" else "replay.json") in
      Unix.mkdir blocked 0o700;
      let settings = single_settings directory in
      let settings =
        if completion then ("SMOKE_WORKER_CACHE_EVICTION_READY_FILE", blocked) :: settings
        else settings
      in
      let source = source () in
      let worker = worker source (callbacks settings) in
      Queue.add (initial ()) source.queue;
      (match Worker.poll worker with
      | Ok (Adapter.Completed _) when completion -> ()
      | Ok (Adapter.Rejected { lease_retired = true; error; _ })
        when not completion && error.path = "$.activation.replay_metadata" -> ()
      | _ -> failwith "observer write failure changed lease error handling");
      if Hashtbl.length source.leases <> 0 || List.length source.completions <> 1 then
        failwith "observer failure leaked or completed a lease twice";
      Array.iter
        (fun name ->
          if String.starts_with ~prefix:(Filename.basename blocked ^ ".tmp.") name then
            failwith "failed atomic write left a temporary artifact")
        (Sys.readdir directory);
      expect_empty ())

(** A parent/child checkpoint is published only after both exact role identities
    have been seen, and the replacement continues the same two runs. *)
let test_parent_child_generations () =
  with_directory (fun directory ->
      let path = Filename.concat directory "parent-child.json" in
      let settings generation =
        [ ("SMOKE_PARENT_CHILD_REPLAY_DIAGNOSTICS_FILE", path);
          ("SMOKE_PARENT_CHILD_REPLAY_GENERATION", generation);
          ("SMOKE_PARENT_CHILD_REPLAY_PARENT_WORKFLOW_ID", "parent");
          ("SMOKE_PARENT_CHILD_REPLAY_CHILD_WORKFLOW_ID", "child") ]
      in
      let observe selected workflow_id run_id is_replaying history_length =
        (Option.get selected.Observer.on_activation)
          { Adapter.run_id; workflow_id = Some workflow_id; is_replaying;
            history_length; cache_removal_reason = None }
      in
      let first = callbacks (settings "1") in
      observe first "parent" "parent-run" false 1L;
      if Sys.file_exists path then failwith "published a partial role checkpoint";
      observe first "child" "child-run" false 1L;
      let previous = read_json path in
      let second =
        callbacks
          (("SMOKE_PARENT_CHILD_REPLAY_PARENT_RUN_ID", "parent-run")
          :: ("SMOKE_PARENT_CHILD_REPLAY_CHILD_RUN_ID", "child-run")
          :: settings "2")
      in
      (match observe second "parent" "other-run" true 3L with
      | () -> failwith "mismatched parent run was accepted"
      | exception Failure _ -> ());
      if read_json path <> previous then failwith "unrelated run changed checkpoint";
      observe second "parent" "parent-run" true 3L;
      if read_json path <> previous then failwith "published partial replay checkpoint";
      observe second "child" "child-run" true 3L;
      match read_json path with
      | `Assoc fields ->
          (match List.assoc "records" fields with
          | `List records when List.length records = 4 -> ()
          | _ -> failwith "replacement lost role replay records")
      | _ -> failwith "parent-child checkpoint format changed")

(** Incomplete fixture configuration fails before callback construction, while
    an empty configuration leaves the ordinary private adapter uninstrumented. *)
let test_configuration () =
  let empty = callbacks [] in
  if Option.is_some empty.on_activation || Option.is_some empty.on_completion then
    failwith "empty configuration enabled observers";
  List.iter
    (fun settings ->
      match Acceptance_observer.create ~getenv:(fun key -> List.assoc_opt key settings) () with
      | Error _ -> ()
      | Ok _ -> failwith "invalid observer configuration was accepted")
    [ [ ("SMOKE_WORKER_REPLAY_DIAGNOSTICS_FILE", "relative") ];
      [ ("SMOKE_PARENT_CHILD_REPLAY_GENERATION", "1") ];
      [ ("SMOKE_WORKER_GENERATION", "1"); ("SMOKE_PARENT_CHILD_REPLAY_GENERATION", "1") ] ]

(** All fixture-boundary regressions run in the ordinary offline Dune suite. *)
let () =
  test_scope_ownership ();
  test_configuration ();
  test_cache_acknowledgement ();
  test_write_failure_cleanup ~completion:false;
  test_write_failure_cleanup ~completion:true;
  test_parent_child_generations ();
  print_endline "acceptance observer boundary: ok"

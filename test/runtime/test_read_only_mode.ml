(** Regression tests for the read-only mode shared by query handlers and
    update validators (#513).

    Validators run only for live update requests and are skipped on replay,
    and a query activation records no commands, so neither callback may
    change replay-sensitive SDK state. These tests drive one workflow through
    the real execution runtime several times:
    - live, with queries and rejected, raising, and accepted updates whose
      query handler and validators try every mutating SDK operation;
    - live, with only the accepted update that history records;
    - after an eviction, replaying the recorded history and then continuing
      live with further queries and a rejected update;
    - as a full replay with validation skipped.
    Every run must emit the same durable commands, which covers the command
    buffer, sequence numbers, the random stream (it sets a timer duration),
    workflow-local values (they set the same duration), and patch decisions.
    Further cases check that the mode is restored after exceptions and
    nesting, and that an accepted update's implementation can still mutate
    state and suspend once its validator has returned. *)

module T = Temporal
module Execution = Temporal_runtime.Execution
module Activation = Temporal_runtime.Activation
module Context = Temporal_runtime.Workflow_context_store
module Scheduler = Temporal_runtime.Scheduler
module Future_store = Temporal_runtime.Future_store

(** Fails with [label] when [condition] is false. *)
let check label condition = if not condition then failwith label

(** Reports whether [needle] occurs in [haystack]. *)
let contains haystack needle =
  let length = String.length needle in
  let rec scan index =
    index + length <= String.length haystack
    && (String.sub haystack index length = needle || scan (index + 1))
  in
  scan 0

(** Copies a public payload into the private representation used by the
    execution runtime. *)
let base_payload (payload : T.Payload.t) : Temporal_base.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Copies a private payload back into the public representation. *)
let public_payload (payload : Temporal_base.Payload.t) : T.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Keeps the category and message of a public error at the private
    boundary. *)
let base_error error =
  let view = T.Error.view error in
  Temporal_base.Error.make ~category:view.category ~message:view.message ()

(** Encodes one value with a public codec, failing the test on error. *)
let encode codec value =
  match T.Codec.encode codec value with
  | Ok payload -> payload
  | Error error -> failwith (T.Error.message error)

(** The canonical unit payload in the private representation. *)
let unit_payload = base_payload (encode T.Codec.unit ())

(** Milliseconds as a public duration. *)
let ms value = T.Duration.of_ms value

(** The remote activity, child workflow, signal, and successor the probes try
    to start; none is ever started successfully. *)
let activity = T.Activity.remote ~name:"read_only_activity" ~input:T.Codec.string ~output:T.Codec.unit
let child = T.Workflow.remote ~name:"read_only_child" ~input:T.Codec.unit ~output:T.Codec.unit
let signal = T.Signal.define ~name:"read_only_signal" ~input:T.Codec.unit
let successor = T.Workflow.remote ~name:"read_only_successor" ~input:T.Codec.unit ~output:T.Codec.unit

(** The workflow-local counter the root and the accepted update change, and
    the probes try to overwrite. *)
let counter : int T.Workflow_context.Local.t = T.Workflow_context.Local.create ()

(** Handles a root run publishes for the probes: a pending activity, a live
    scope, the pending timer the root is awaiting, and a ready future owned by
    the workflow scheduler. They are test-owned references, not SDK state;
    each run gets fresh ones. *)
type stash = {
  mutable handle : unit T.Activity.handle option;
  mutable scope : T.Scope.t option;
  mutable pending : (unit, T.Error.t) T.Future.t option;
  mutable ready : (unit, T.Error.t) T.Future.t option;
}

(** Creates an empty stash for one run. *)
let empty_stash () = { handle = None; scope = None; pending = None; ready = None }

(** Unwraps a stashed handle published by the root before any probe runs. *)
let get label = function Some value -> value | None -> failwith (label ^ " was not stashed")

(** A probe result: the operation's label and whether it was rejected as the
    read-only contract requires. *)
type probe = string * bool

(** A typed result must be an error whose message contains [fragment]. *)
let typed ?(fragment = "") label = function
  | Ok _ -> (label, false)
  | Error error -> (label, contains (T.Error.message error) fragment)

(** A returned future must already hold a typed read-only defect, so the
    operation recorded nothing and cannot suspend. *)
let ready label future =
  match T.Future.peek future with
  | Some (Error error) -> (label, contains (T.Error.message error) "read-only")
  | Some (Ok _) | None -> (label, false)

(** The operation must raise [Invalid_argument] naming the read-only
    callback. *)
let raises label action =
  match action () with
  | () -> (label, false)
  | exception Invalid_argument message -> (label, contains message "read-only")

(** Attempts every SDK operation that could change replay-sensitive state,
    emit a command, or suspend, and returns whether each was rejected. Reads
    are checked separately by {!reads}. *)
let probe stash : probe list =
  let handle = get "activity handle" stash.handle in
  let scope = get "scope" stash.scope in
  let pending = get "pending timer" stash.pending in
  let ready_future = get "ready future" stash.ready in
  let value = encode T.Codec.string "v" in
  [ typed ~fragment:"read-only" "Local.set" (T.Workflow_context.Local.set counter 99);
    typed ~fragment:"read-only" "random_int" (T.Workflow.random_int ~bound:1000);
    typed ~fragment:"read-only" "uuid4" (T.Workflow.uuid4 ());
    ready "start_sleep" (T.Workflow.start_sleep (ms 1_000L));
    ready "start_sleep zero" (T.Workflow.start_sleep (ms 0L));
    ready "scoped start_sleep" (T.Workflow.start_sleep ~scope (ms 1_000L));
    typed ~fragment:"read-only" "sleep" (T.Workflow.sleep (ms 1_000L));
    ready "activity start" (T.Activity.start activity "x");
    ready "scoped activity start" (T.Activity.start ~scope activity "x");
    ready "local activity start" (T.Activity.start_local activity "x");
    ready "child start" (T.Child_workflow.start ~id:"read-only-child" child ());
    ready "child start with metadata"
      (T.Child_workflow.start ~memo:[ ("key", value) ] ~search_attributes:[ ("Key", value) ]
         ~id:"read-only-child" child ());
    (* The guard precedes metadata validation, so even a bad key reports the
       read-only rejection rather than a key defect. *)
    ready "child start with invalid metadata"
      (T.Child_workflow.start ~memo:[ ("", value) ] ~id:"read-only-child" child ());
    ready "external signal"
      (T.Workflow.signal_external_workflow ~workflow_id:"other" ~signal ~input:() ());
    ready "external cancellation"
      (T.Workflow.cancel_external_workflow ~workflow_id:"other" ~reason:"probe" ());
    typed ~fragment:"read-only" "activity cancel" (T.Activity.cancel handle);
    raises "patched" (fun () -> ignore (T.Workflow.patched ~id:"probe-only"));
    raises "deprecate_patch" (fun () -> T.Workflow.deprecate_patch ~id:"root-patch");
    raises "upsert_search_attributes" (fun () ->
        T.Workflow.upsert_search_attributes [ ("Key", value) ]);
    raises "upsert_memo" (fun () -> T.Workflow.upsert_memo [ ("key", value) ]);
    raises "continue_as_new" (fun () -> T.Workflow.continue_as_new successor ());
    raises "continue_as_new with options" (fun () ->
        T.Workflow.continue_as_new ~task_queue:"other" ~memo:[ ("key", value) ]
          ~search_attributes:[ ("Key", value) ] successor ());
    raises "continue_as_new with invalid option" (fun () ->
        T.Workflow.continue_as_new ~task_queue:"" successor ());
    raises "upsert_typed_search_attributes" (fun () ->
        T.Workflow.upsert_typed_search_attributes
          [ T.Search_attribute.set (T.Search_attribute.keyword "Status") "probe" ]);
    typed ~fragment:"read-only" "Scope.create" (T.Scope.create ());
    typed "Scope.cancel" (T.Scope.cancel scope);
    typed "Condition.wait_until" (T.Condition.wait_until (fun () -> true));
    typed ~fragment:"read-only" "Condition.wait_until with timeout"
      (T.Condition.wait_until ~timeout:(ms 100L) (fun () -> false));
    typed "Future.await pending" (T.Future.await pending);
    raises "Future.map pending" (fun () -> ignore (T.Future.map Fun.id pending));
    raises "Future.map ready" (fun () -> ignore (T.Future.map Fun.id ready_future));
    raises "Future.both" (fun () -> ignore (T.Future.both ready_future pending)) ]

(** The reads a read-only callback keeps: the workflow-local value (which
    must equal [expected]), its own scope's status, a ready future, and a
    pending future's readiness. *)
let reads stash ~expected : probe list =
  let scope = get "scope" stash.scope in
  [ ("Local.get", T.Workflow_context.Local.get counter = Ok (Some expected));
    ("Scope.check", T.Scope.check scope = Ok ());
    ("Scope.is_cancelled", T.Scope.is_cancelled scope = Ok false);
    ("Future.await ready",
     match T.Future.await (get "ready future" stash.ready) with
     | Error error -> contains (T.Error.message error) "activity id"
     | Ok () -> false);
    ("Future.peek pending", T.Future.peek (get "pending timer" stash.pending) = None) ]

(** Fails unless every probe result holds, naming the callback and the first
    operation that did not. *)
let expect_all callback results =
  List.iter
    (fun (label, held) -> if not held then failwith (callback ^ ": " ^ label))
    results

(** Builds one execution of the probed workflow.

    The root sets the counter to 1, starts an activity it never awaits,
    creates a scope, and waits on a 100 ms timer. Then it draws a random
    number, consults the patch ["root-patch"], reads the counter, and waits
    on a second timer whose duration combines the three, so any interference
    with the random stream, the counter, or the patch decision changes a
    durable command. The ["probe"] query and every update validator run
    {!probe} and {!reads}, recording their results in [probes]. The update's
    input selects its validator outcome: ["reject"] returns an error,
    ["raise"] raises, and ["ok"] accepts; the accepted implementation adds 10
    to the counter. *)
let execution ~probes =
  let stash = empty_stash () in
  let expected = ref 1 in
  let root () =
    let ( let* ) = Result.bind in
    let* () = T.Workflow_context.Local.set counter 1 in
    stash.handle <- Some (T.Activity.start_handle activity "work");
    let* scope = T.Scope.create () in
    stash.scope <- Some scope;
    stash.ready <- Some (T.Activity.start ~activity_id:"" activity "invalid");
    let timer = T.Workflow.start_sleep (ms 100L) in
    stash.pending <- Some timer;
    let* () = T.Future.await timer in
    let* draw = T.Workflow.random_int ~bound:1000 in
    let patched = T.Workflow.patched ~id:"root-patch" in
    let* value = T.Workflow_context.Local.get counter in
    let value = Option.value value ~default:0 in
    let duration = (draw * 100) + value + if patched then 1 else 0 in
    let timer = T.Workflow.start_sleep (ms (Int64.of_int duration)) in
    stash.pending <- Some timer;
    T.Future.await timer
  in
  let record callback =
    probes :=
      (callback, probe stash @ reads stash ~expected:!expected) :: !probes
  in
  let definition =
    Temporal_base.Definition.make ~name:"read_only_mode"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Result.map_error base_error (root ())))
  in
  let query =
    Execution.make_query_handler ~name:"probe" ~dispatch:(fun _ ->
        record "query";
        Ok unit_payload)
  in
  let update = T.Update.define ~name:"bump" ~input:T.Codec.string ~output:T.Codec.unit in
  let public_handler =
    T.Update.Handler.make
      ~validator:(fun mode ->
        record ("validator " ^ mode);
        match mode with
        | "reject" -> Error (T.Error.make ~category:`Workflow ~message:"rejected by validator" ())
        | "raise" -> failwith "validator raised"
        | _ -> Ok ())
      update
      (fun _ ->
        let ( let* ) = Result.bind in
        let* value = T.Workflow_context.Local.get counter in
        let value = Option.value value ~default:0 + 10 in
        let* () = T.Workflow_context.Local.set counter value in
        expected := value;
        Ok ())
  in
  let update_handler =
    Execution.make_update_handler ~name:"bump"
      ~dispatch:(fun ~run_validator ~on_validated (request : Execution.update) ->
        match request.input with
        | [ payload ] ->
            T.Update.Handler.dispatch ~run_validator ~on_validated public_handler
              (public_payload payload)
            |> Result.map base_payload |> Result.map_error base_error
        | _ -> failwith "invalid update fixture input")
  in
  Execution.start ~randomness_seed:"513" ~query_handlers:[ query ]
    ~update_handlers:[ update_handler ] definition ()

(** A query job for the ["probe"] handler. *)
let query_job id =
  Activation.Query_workflow { query_id = id; query_type = "probe"; arguments = []; headers = [] }

(** An update job whose input selects the validator outcome. *)
let update_job ?(run_validator = true) mode =
  Activation.Do_update
    { id = "update-" ^ mode; protocol_instance_id = "protocol-" ^ mode; name = "bump";
      input = [ base_payload (encode T.Codec.string mode) ]; headers = [];
      identity = "test"; update_id = "update-" ^ mode; run_validator }

(** Keeps the commands that reach history, dropping query results and update
    responses, which are protocol messages rather than workflow commands. *)
let durable commands =
  List.filter
    (function
      | Activation.Query_result _ | Update_response _ -> false
      | _ -> true)
    commands

(** Applies [steps] in order, each with its replay flag, and returns every
    durable command in emission order. Interaction steps are checked as they
    run: a query must answer, an update must be rejected or accepted and
    completed as its input requires, and no step may fail the workflow
    task. *)
let run execution steps =
  List.concat_map
    (fun (replaying, jobs) ->
      Execution.set_activation_is_replaying execution replaying;
      let commands = Execution.activate execution jobs in
      (match Execution.task_failure execution with
      | Some error -> failwith ("workflow task failed: " ^ Temporal_base.Error.message error)
      | None -> ());
      List.iter
        (function
          | Activation.Do_update { protocol_instance_id; input = [ payload ]; _ } -> (
              let mode =
                match T.Codec.decode T.Codec.string (public_payload payload) with
                | Ok mode -> mode
                | Error error -> failwith (T.Error.message error)
              in
              let responses =
                List.filter_map
                  (function
                    | Activation.Update_response { protocol_instance_id = id; response }
                      when String.equal id protocol_instance_id ->
                        Some response
                    | _ -> None)
                  commands
              in
              match (mode, responses) with
              | ("reject" | "raise"), [ `Rejected _ ] -> ()
              | "ok", [ `Accepted; `Completed _ ] -> ()
              | _ -> failwith ("unexpected responses for update " ^ mode))
          | Activation.Query_workflow { query_id; _ } ->
              check ("query " ^ query_id ^ " was not answered")
                (List.exists
                   (function
                     | Activation.Query_result { query_id = id; result = Ok _ } -> String.equal id query_id
                     | _ -> false)
                   commands)
          | _ -> ())
        jobs;
      durable commands)
    steps

(** The workflow's recorded history as activation steps: start, the accepted
    update, the first timer (with the patch marker notification when
    replaying), and the second timer. *)
let history ~replaying =
  [ (replaying, [ Activation.Start_workflow ]);
    (replaying, [ update_job ~run_validator:(not replaying) "ok" ]);
    (replaying,
     (if replaying then [ Activation.Notify_has_patch { patch_id = "root-patch" } ] else [])
     @ [ Activation.Fire_timer { seq = 2L } ]);
    (replaying, [ Activation.Fire_timer { seq = 3L } ]) ]

(** Verifies queries and rejected or raising validators leave every durable
    command unchanged, live and across eviction and replay, while each of
    their mutating SDK operations is rejected and their reads succeed. *)
let test_interference_leaves_history_unchanged () =
  let probes = ref [] in
  (* Live run with interference at every point between the recorded steps. *)
  let live_execution = execution ~probes in
  let interfered =
    run live_execution
      [ (false, [ Activation.Start_workflow ]);
        (false, [ query_job "q1" ]);
        (false, [ update_job "reject" ]);
        (false, [ update_job "raise" ]);
        (false, [ query_job "q2"; query_job "q3" ]);
        (false, [ update_job "ok" ]);
        (false, [ query_job "q4" ]);
        (false, [ Activation.Fire_timer { seq = 2L } ]);
        (false, [ query_job "q5" ]);
        (false, [ update_job "reject" ]);
        (false, [ Activation.Fire_timer { seq = 3L } ]) ]
  in
  (* Live run with only what history records. *)
  let clean = run (execution ~probes:(ref [])) (history ~replaying:false) in
  (* After eviction: a fresh execution replays the start and the accepted
     update, then continues live with more interference. *)
  let after_eviction_execution = execution ~probes in
  let after_eviction =
    run after_eviction_execution
      [ (true, [ Activation.Start_workflow ]);
        (true, [ update_job ~run_validator:false "ok" ]);
        (false, [ query_job "q6" ]);
        (false, [ update_job "raise" ]);
        (false, [ Activation.Fire_timer { seq = 2L } ]);
        (false, [ query_job "q7" ]);
        (false, [ Activation.Fire_timer { seq = 3L } ]) ]
  in
  (* A full replay with validation skipped. *)
  let replayed = run (execution ~probes:(ref [])) (history ~replaying:true) in
  (match clean with
  | [ Activation.Schedule_activity { seq = 1L; _ };
      Activation.Start_timer { seq = 2L; milliseconds = 100L };
      Activation.Set_patch_marker { patch_id = "root-patch"; deprecated = false };
      Activation.Start_timer { seq = 3L; milliseconds };
      Activation.Complete_workflow _ ] ->
      (* Counter 1 + 10 from the accepted update, plus 1 for the patch. *)
      check "second timer does not include the accepted update"
        (Int64.rem milliseconds 100L = 12L)
  | _ -> failwith "clean run emitted an unexpected command sequence");
  check "queries and rejected updates changed the live commands" (interfered = clean);
  check "replay after eviction diverged" (after_eviction = clean);
  check "replay with validation skipped diverged" (replayed = clean);
  (* The live run has five queries and four validators (two rejecting, one
     raising, one accepting); after the eviction two queries and one raising
     validator run, while the replayed accepted update skips its
     validator. *)
  check "not every read-only callback ran" (List.length !probes = 12);
  List.iter (fun (callback, results) -> expect_all callback results) !probes

(** Verifies the mode is restored after an exception and after nesting, that
    it clears the scheduler owner only for its extent, and that it leaves no
    thread-local binding behind. *)
let test_restore_after_exception_and_nesting () =
  let scheduler = Scheduler.create () in
  let context = Context.create scheduler in
  let writable label =
    check (label ^ ": context still read-only") (not (Context.is_read_only context));
    check (label ^ ": thread still marked read-only") (not (Future_store.in_read_only ()));
    check (label ^ ": Local.set rejected") (T.Workflow_context.Local.set counter 1 = Ok ())
  in
  let read_only label =
    check (label ^ ": context not read-only") (Context.is_read_only context);
    check (label ^ ": thread not marked read-only") (Future_store.in_read_only ());
    check (label ^ ": Local.set accepted")
      (Result.is_error (T.Workflow_context.Local.set counter 2))
  in
  Context.with_context context (fun () ->
      Future_store.with_current_owner_id (Some (Scheduler.id scheduler)) (fun () ->
          writable "before";
          Context.with_read_only context (fun () ->
              read_only "outer";
              check "owner id not cleared"
                (not (Future_store.current_owner_matches (Scheduler.id scheduler)));
              (match
                 Context.with_read_only context (fun () ->
                     read_only "inner";
                     failwith "inner callback raised")
               with
              | () -> failwith "inner exception was swallowed"
              | exception Failure _ -> ());
              read_only "after inner exception";
              Context.with_read_only context (fun () -> read_only "second inner");
              read_only "after nested return");
          writable "after outer";
          check "owner id not restored"
            (Future_store.current_owner_matches (Scheduler.id scheduler));
          (match Context.with_read_only context (fun () -> raise Exit) with
          | () -> failwith "outer exception was swallowed"
          | exception Exit -> ());
          writable "after outer exception";
          (match Context.with_read_only_query context (fun () -> raise Exit) with
          | () -> failwith "query exception was swallowed"
          | exception Exit -> ());
          writable "after query exception"));
  check "a read-only binding leaked" (Context.bound_thread_count () = 0);
  check "an owner binding leaked" (Future_store.bound_owner_count () = 0);
  (* The rejected write left the value the writable calls stored. *)
  Context.with_context context (fun () ->
      check "a rejected write changed the value"
        (T.Workflow_context.Local.get counter = Ok (Some 1)));
  Context.shutdown context

(** Verifies an accepted update's implementation runs outside the mode: once
    its validator returned it can mutate workflow-local state, emit a
    command, and suspend on a timer, and it completes in a later
    activation. *)
let test_accepted_update_mutates_and_suspends () =
  let validator_rejections = ref [] in
  let definition =
    Temporal_base.Definition.make ~name:"read_only_then_suspend"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:
        (Some
           (fun () ->
             Result.map_error base_error (T.Workflow.sleep (ms 1_000L))))
  in
  let update = T.Update.define ~name:"suspend" ~input:T.Codec.unit ~output:T.Codec.unit in
  let public_handler =
    T.Update.Handler.make
      ~validator:(fun () ->
        validator_rejections :=
          [ typed ~fragment:"read-only" "Local.set" (T.Workflow_context.Local.set counter 5);
            typed ~fragment:"read-only" "sleep" (T.Workflow.sleep (ms 10L)) ];
        Ok ())
      update
      (fun () ->
        let ( let* ) = Result.bind in
        let* () = T.Workflow_context.Local.set counter 7 in
        let* () = T.Workflow.sleep (ms 10L) in
        match T.Workflow_context.Local.get counter with
        | Ok (Some 7) -> Ok ()
        | _ -> Error (T.Error.defect ~message:"update lost its write"))
  in
  let handler =
    Execution.make_update_handler ~name:"suspend"
      ~dispatch:(fun ~run_validator ~on_validated _ ->
        T.Update.Handler.dispatch ~run_validator ~on_validated public_handler
          (public_payload unit_payload)
        |> Result.map base_payload |> Result.map_error base_error)
  in
  let execution = Execution.start ~update_handlers:[ handler ] definition () in
  check "root timer"
    (Execution.activate execution [ Activation.Start_workflow ]
     = [ Activation.Start_timer { seq = 1L; milliseconds = 1_000L } ]);
  check "update did not suspend after acceptance"
    (Execution.activate execution
       [ Activation.Do_update
           { id = "u"; protocol_instance_id = "p"; name = "suspend"; input = [ unit_payload ];
             headers = []; identity = "test"; update_id = "u"; run_validator = true } ]
     = [ Activation.Update_response { protocol_instance_id = "p"; response = `Accepted };
         Activation.Start_timer { seq = 2L; milliseconds = 10L } ]);
  expect_all "validator" !validator_rejections;
  check "validator did not run" (List.length !validator_rejections = 2);
  check "update did not complete after its timer"
    (Execution.activate execution [ Activation.Fire_timer { seq = 2L } ]
     = [ Activation.Update_response
           { protocol_instance_id = "p"; response = `Completed unit_payload } ])

(** Runs every #513 regression; any failure raises and fails the alias. *)
let () =
  test_interference_leaves_history_unchanged ();
  test_restore_after_exception_and_nesting ();
  test_accepted_update_mutates_and_suspends ()

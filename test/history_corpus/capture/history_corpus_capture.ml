(** Live capture program for the replay history corpus.

    One invocation runs one worker generation (a corpus definition set) and a
    client in the same process, starts that generation's corpus scenarios with
    fixed workflow IDs, waits for every run to close, and writes the exact
    workflow/run identities to a JSON file. The surrounding script
    ([test/history_corpus/scripts/capture-history-corpus.sh]) then exports each
    recorded run's history. The program never edits the committed corpus.

    Usage: [history_corpus_capture.exe GENERATION OUTPUT_JSON], with
    [TEMPORAL_ADDRESS] and [TEMPORAL_NAMESPACE] in the environment. Expected
    scenario failures exit non-zero; a capture is never partially accepted. *)

module Defs = Corpus_definitions

(** Converts a typed SDK error into a fixture failure. This program is a test
    controller, so failing loudly is the intended response. *)
let require label = function
  | Ok value -> value
  | Error error ->
      failwith
        (Printf.sprintf "%s: %s: %s" label (Temporal.Error.kind error)
           (Temporal.Error.message error))

(** Reads a required, non-empty environment variable. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> value
  | _ -> failwith (name ^ " must be set")

(** One captured run, written to the output file for the export script. *)
type record = {
  case : string;
  workflow_id : string;
  run_id : string option;
      (** [None] for a child run, whose run ID is assigned by the server and
          resolved by the export script from the child's latest run. *)
  workflow_type : string;
}

(** Client-side references with the same names and codecs as the frozen
    definitions. A client only needs the type name and codecs. *)
let remote name input output = Temporal.Workflow.remote ~name ~input ~output

(** Waits for a run and requires the expected terminal shape. *)
let expect_completed label handle =
  match require label (Temporal.Client.wait handle) with
  | Temporal.Client.Completed { output = value; _ } -> value
  | _ -> failwith (label ^ ": run did not complete")

(** Starts one workflow with a fixed ID; the export script relies on these IDs
    and on the recorded run ID, never on visibility search. *)
let start client ~workflow ~id ~input =
  require ("start " ^ id)
    (Temporal.Client.start client ~workflow ~task_queue:Defs.task_queue ~id
       ~input ())

(** Records a closed run. *)
let record ~case ~workflow_type handle =
  {
    case;
    workflow_id = Temporal.Client.workflow_id handle;
    run_id = Temporal.Client.run_id handle;
    workflow_type;
  }

(** Polls the interaction query until the workflow's first task has run. A
    query against a run whose first task has not completed may fail
    transiently, so failures are retried within a bounded deadline. *)
let wait_for_query handle expected =
  let deadline = Unix.gettimeofday () +. 30. in
  let rec loop () =
    match Temporal.Client.query handle ~query:Defs.interaction_query with
    | Ok value when value = expected -> ()
    | _ when Unix.gettimeofday () < deadline ->
        Unix.sleepf 0.2;
        loop ()
    | Ok value -> failwith ("interaction query returned " ^ value)
    | Error error -> require "interaction query" (Error error)
  in
  loop ()

(** Scenarios captured by the [corpus-v1] generation. *)
let corpus_v1 client =
  let string_workflow name =
    remote name Temporal.Codec.string Temporal.Codec.string
  in
  let simple ~case ~workflow_type ~input =
    let handle =
      start client ~workflow:(string_workflow workflow_type)
        ~id:("history-corpus-" ^ case) ~input
    in
    ignore (expect_completed case handle : string);
    record ~case ~workflow_type handle
  in
  let activity =
    simple ~case:"activity" ~workflow_type:Defs.activity_workflow_type
      ~input:"corpus"
  in
  let timer =
    let workflow_type = Defs.timer_workflow_type in
    let handle =
      start client
        ~workflow:(remote workflow_type Temporal.Codec.unit Temporal.Codec.string)
        ~id:"history-corpus-timer" ~input:()
    in
    ignore (expect_completed "timer" handle : string);
    record ~case:"timer" ~workflow_type handle
  in
  let activity_retry =
    simple ~case:"activity-retry"
      ~workflow_type:Defs.activity_retry_workflow_type ~input:"corpus"
  in
  let interaction =
    let workflow_type = Defs.interaction_workflow_type in
    let handle =
      start client ~workflow:(string_workflow workflow_type)
        ~id:"history-corpus-interaction" ~input:"corpus"
    in
    wait_for_query handle "signal=0 update=0";
    require "signal"
      (Temporal.Client.signal handle ~signal:Defs.interaction_signal
         ~input:"signalled");
    let update =
      require "start update"
        (Temporal.Client.start_update ~update_id:"history-corpus-update" handle
           ~update:Defs.interaction_update ~input:"updated" ())
    in
    let update_result = require "wait update" (Temporal.Client.wait_update update) in
    if update_result <> "UPDATE:updated" then
      failwith ("unexpected update result " ^ update_result);
    let result = expect_completed "interaction" handle in
    if result <> "corpus:signalled:updated" then
      failwith ("unexpected interaction result " ^ result);
    record ~case:"interaction" ~workflow_type handle
  in
  let parent =
    simple ~case:"parent" ~workflow_type:Defs.parent_workflow_type
      ~input:"corpus"
  in
  (* The parent derives its child's ID from its input; the child run ID is
     only known to the server, so the export script resolves it. *)
  let child =
    {
      case = "child";
      workflow_id = "history-corpus-child-corpus";
      run_id = None;
      workflow_type = Defs.child_workflow_type;
    }
  in
  let continue_as_new =
    let workflow_type = Defs.continue_as_new_workflow_type in
    let workflow = string_workflow workflow_type in
    let first =
      start client ~workflow ~id:"history-corpus-continue-as-new" ~input:"first"
    in
    let successor =
      match require "continue-as-new wait" (Temporal.Client.wait first) with
      | Temporal.Client.Continued_as_new execution -> execution
      | _ -> failwith "continue-as-new first run did not continue"
    in
    let second =
      require "follow successor"
        (Temporal.Client.follow client ~workflow successor)
    in
    ignore (expect_completed "continue-as-new successor" second : string);
    [
      record ~case:"continue-as-new-first" ~workflow_type first;
      record ~case:"continue-as-new-second" ~workflow_type second;
    ]
  in
  [ activity; timer; activity_retry; interaction; parent; child ]
  @ continue_as_new

(** Scenarios captured by the [corpus-v2-cancellation] generation (#1134,
    #1135). Every run but the bounded-condition release completes on its
    own; that one is signalled only after its query reports it waiting, so
    its timer is always in history before the signal. The options child's
    run ID is resolved by the export script from its derived workflow ID. *)
let corpus_v2_cancellation client =
  let unit_workflow name = remote name Temporal.Codec.unit Temporal.Codec.string in
  let simple ~case ~workflow_type =
    let handle =
      start client ~workflow:(unit_workflow workflow_type)
        ~id:("history-corpus-" ^ case) ~input:()
    in
    ignore (expect_completed case handle : string);
    record ~case ~workflow_type handle
  in
  let before =
    simple ~case:"timer-cancel-before-fire"
      ~workflow_type:Defs.timer_cancel_before_fire_workflow_type
  in
  let after =
    simple ~case:"timer-cancel-after-fire"
      ~workflow_type:Defs.timer_cancel_after_fire_workflow_type
  in
  let timeout =
    simple ~case:"condition-timeout" ~workflow_type:Defs.condition_timeout_workflow_type
  in
  let satisfied =
    let workflow_type = Defs.condition_satisfied_workflow_type in
    let handle =
      start client ~workflow:(unit_workflow workflow_type)
        ~id:"history-corpus-condition-satisfied" ~input:()
    in
    let deadline = Unix.gettimeofday () +. 30. in
    let rec await_waiting () =
      match Temporal.Client.query handle ~query:Defs.condition_state with
      | Ok "waiting" -> ()
      | _ when Unix.gettimeofday () < deadline ->
          Unix.sleepf 0.2;
          await_waiting ()
      | Ok state -> failwith ("condition state query returned " ^ state)
      | Error error -> require "condition state query" (Error error)
    in
    await_waiting ();
    require "condition release"
      (Temporal.Client.signal handle ~signal:Defs.condition_release ~input:());
    ignore (expect_completed "condition-satisfied" handle : string);
    record ~case:"condition-satisfied" ~workflow_type handle
  in
  let parent =
    simple ~case:"options-parent" ~workflow_type:Defs.options_parent_workflow_type
  in
  let child =
    {
      case = "options-child";
      workflow_id = "history-corpus-options-parent-child";
      run_id = None;
      workflow_type = Defs.options_child_workflow_type;
    }
  in
  [ before; after; timeout; satisfied; parent; child ]

(** Captures one [corpus.patch] run under the current generation. *)
let patch client ~case =
  let workflow_type = Defs.patch_workflow_type in
  let handle =
    start client
      ~workflow:(remote workflow_type Temporal.Codec.unit Temporal.Codec.string)
      ~id:("history-corpus-" ^ case) ~input:()
  in
  ignore (expect_completed case handle : string);
  record ~case ~workflow_type handle

(** Workflow ID of the handover run. The legacy generation starts it and the
    patched generation, a separate process, finds it again by this ID. *)
let handover_workflow_id = "history-corpus-lifecycle-handover"

(** Client reference for every [corpus.lifecycle.*] workflow type, which all
    take and return a string. *)
let lifecycle_remote workflow_type =
  remote workflow_type Temporal.Codec.string Temporal.Codec.string

(** Captures the three matrix workflows of one lifecycle stage. [stage] is
    the short stage name used in case IDs and as the workflow input, so each
    stage's runs (and the multi workflow's child IDs) are distinct. *)
let lifecycle_matrix client ~stage =
  let run ~kind ~workflow_type =
    let case = Printf.sprintf "lifecycle-%s-%s" kind stage in
    let handle =
      start client ~workflow:(lifecycle_remote workflow_type)
        ~id:("history-corpus-" ^ case) ~input:stage
    in
    (handle, case)
  in
  let simple ~kind ~workflow_type =
    let handle, case = run ~kind ~workflow_type in
    ignore (expect_completed case handle : string);
    record ~case ~workflow_type handle
  in
  let multi =
    simple ~kind:"multi" ~workflow_type:Defs.lifecycle_multi_workflow_type
  in
  let loop = simple ~kind:"loop" ~workflow_type:Defs.lifecycle_loop_workflow_type in
  let handlers =
    let workflow_type = Defs.lifecycle_handlers_workflow_type in
    let handle, case = run ~kind:"handlers" ~workflow_type in
    require "lifecycle signal"
      (Temporal.Client.signal handle ~signal:Defs.lifecycle_signal
         ~input:("signal-" ^ stage));
    let update =
      require "lifecycle start update"
        (Temporal.Client.start_update
           ~update_id:("history-corpus-lifecycle-update-" ^ stage)
           handle ~update:Defs.lifecycle_update ~input:("update-" ^ stage) ())
    in
    ignore (require "lifecycle wait update" (Temporal.Client.wait_update update)
            : string);
    ignore (expect_completed case handle : string);
    record ~case ~workflow_type handle
  in
  [ multi; loop; handlers ]

(** Polls the handover status query until it returns [expected], proving the
    run's first workflow task completed. Transient query failures before that
    task are retried within a bounded deadline. *)
let wait_for_handover_status handle expected =
  let deadline = Unix.gettimeofday () +. 30. in
  let rec loop () =
    match Temporal.Client.query handle ~query:Defs.lifecycle_handover_status with
    | Ok value when value = expected -> ()
    | _ when Unix.gettimeofday () < deadline ->
        Unix.sleepf 0.2;
        loop ()
    | Ok value -> failwith ("handover query returned " ^ value)
    | Error error -> require "handover query" (Error error)
  in
  loop ()

(** Legacy half of the handover: starts the run and leaves it parked before
    its patch point once the legacy worker has completed its first task. The
    run stays open when this process exits; nothing is recorded yet. *)
let start_handover client =
  let handle =
    start client
      ~workflow:(lifecycle_remote Defs.lifecycle_handover_workflow_type)
      ~id:handover_workflow_id ~input:"handover"
  in
  wait_for_handover_status handle "parked"

(** Patched half of the handover: a fresh worker process replays the legacy
    prefix, is released by a signal, and reaches the patch point for the
    first time. The run ID is left to the export script, which reads the
    only run of this workflow ID. *)
let finish_handover client =
  let workflow_type = Defs.lifecycle_handover_workflow_type in
  let handle =
    require "handover handle"
      (Temporal.Client.get_handle client
         ~workflow:(lifecycle_remote workflow_type)
         ~id:handover_workflow_id ())
  in
  require "handover release"
    (Temporal.Client.signal handle ~signal:Defs.lifecycle_handover_release
       ~input:());
  let result = expect_completed "lifecycle-handover" handle in
  if result <> "NEW:handover" then
    failwith ("handover did not take the new branch: " ^ result);
  record ~case:"lifecycle-handover" ~workflow_type handle

(** Selects the scenarios a generation captures. Each patch generation owns
    one [corpus.patch] history; [corpus-v1] also captures the active marker.
    The lifecycle generations each capture their matrix workflows; the
    legacy generation must run before the patched one because it leaves the
    handover run open for the patched generation to finish. *)
let scenarios generation client =
  match generation with
  | "corpus-v1" -> corpus_v1 client @ [ patch client ~case:"patch-active" ]
  | "corpus-v1-patch-legacy" -> [ patch client ~case:"patch-marker-free" ]
  | "corpus-v1-patch-deprecated" -> [ patch client ~case:"patch-deprecated" ]
  | "corpus-lifecycle-legacy" ->
      let records = lifecycle_matrix client ~stage:"legacy" in
      start_handover client;
      records
  | "corpus-lifecycle-patched" ->
      let handover = finish_handover client in
      lifecycle_matrix client ~stage:"patched" @ [ handover ]
  | "corpus-lifecycle-deprecated" -> lifecycle_matrix client ~stage:"deprecated"
  | "corpus-lifecycle-removed" -> lifecycle_matrix client ~stage:"removed"
  | "corpus-v2-cancellation" -> corpus_v2_cancellation client
  | other -> failwith ("generation has no capture scenarios: " ^ other)

(** Converts a definition-set entry to a live worker registration. *)
let registration (Defs.Workflow { definition; signals; queries; updates }) =
  Temporal.Worker.workflow ~signals ~queries ~updates definition

(** Writes the records atomically so a failed run leaves no partial file. *)
let write_records path ~generation records =
  let json =
    `Assoc
      [
        ("generation", `String generation);
        (* Producer toolchain facts recorded as capture provenance. *)
        ("ocaml_version", `String Sys.ocaml_version);
        ("os_type", `String Sys.os_type);
        ( "executions",
          `List
            (List.map
               (fun r ->
                 `Assoc
                   [
                     ("case", `String r.case);
                     ("workflow_id", `String r.workflow_id);
                     ( "run_id",
                       match r.run_id with Some id -> `String id | None -> `Null );
                     ("workflow_type", `String r.workflow_type);
                   ])
               records) );
      ]
  in
  let temporary = path ^ ".tmp" in
  Yojson.Safe.to_file temporary json;
  Sys.rename temporary path

(** Runs the worker on its own Domain, executes the scenarios, then shuts the
    worker and client down before writing any output. *)
let () =
  let generation, output =
    match Sys.argv with
    | [| _; generation; output |] -> (generation, output)
    | _ -> failwith "usage: history_corpus_capture.exe GENERATION OUTPUT_JSON"
  in
  let workflows =
    match Defs.definition_set generation with
    | Some workflows -> List.map registration workflows
    | None -> failwith ("unknown definition set " ^ generation)
  in
  let target_url = required_env "TEMPORAL_ADDRESS" in
  let namespace = required_env "TEMPORAL_NAMESPACE" in
  let worker =
    require "worker create"
      (Temporal.Worker.create ~identity:("history-corpus-" ^ generation)
         ~target_url ~namespace ~task_queue:Defs.task_queue ~workflows
         ~activities:Defs.activities ())
  in
  let runner = Domain.spawn (fun () -> Temporal.Worker.run worker) in
  let client =
    (* Fixed identities keep host names out of the recorded histories. *)
    require "client create"
      (Temporal.Client.create ~identity:"history-corpus-client" ~target_url
         ~namespace ())
  in
  let records =
    Fun.protect
      ~finally:(fun () ->
        Temporal.Worker.request_shutdown worker;
        let run_result = Domain.join runner in
        require "worker shutdown" (Temporal.Worker.shutdown worker);
        require "client shutdown" (Temporal.Client.shutdown client);
        require "worker run" run_result)
      (fun () -> scenarios generation client)
  in
  write_records output ~generation records;
  Printf.printf "history corpus capture generation=%s executions=%d\n%!"
    generation (List.length records)

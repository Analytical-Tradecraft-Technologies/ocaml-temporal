(** Tests for child workflow timeouts and ID reuse policy (#1135) at the public
    [Temporal.Child_workflow] boundary.

    The fixtures install a private workflow context on a deterministic
    scheduler and inspect the buffered runtime command, so they prove that
    valid options reach the [Start_child_workflow] command as exact
    milliseconds, that omitted options leave the command as before, and that
    every combination [Client.start] rejects is a typed defect returned
    before any command or sequence number exists. The protocol and Rust
    bridge re-checks are covered by the bridge tests. *)

module Activation = Temporal_runtime.Activation
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Fails with the scenario label when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** A command-only child reference; no child code runs in these tests. *)
let child =
  Temporal.Workflow.remote ~name:"child" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Shorthand for a whole-second public duration. *)
let seconds value = Temporal.Duration.of_seconds value

(** Starts one child with [start] inside a fresh workflow context, returning
    the ready result (if any) and every buffered command. *)
let with_start start =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let peeked = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          peeked := Temporal.Future.peek (start ())));
  ignore (Scheduler.run_label scheduler);
  let commands = Workflow_context_store.take_commands context in
  Workflow_context_store.shutdown context;
  (!peeked, commands)

(** Valid timeouts and every reuse policy are copied into the command as
    exact milliseconds and the matching runtime variant. *)
let test_options_reach_command () =
  List.iter
    (fun (policy, expected_policy) ->
      let _, commands =
        with_start (fun () ->
            Temporal.Child_workflow.start ~id:"child-1"
              ~execution_timeout:(seconds 3_600L) ~run_timeout:(seconds 600L)
              ~task_timeout:(Temporal.Duration.of_ms 10_500L)
              ~id_reuse_policy:policy child "input")
      in
      match commands with
      | [ Activation.Start_child_workflow
            { execution_timeout; run_timeout; task_timeout; id_reuse_policy; _ } ]
        ->
          expect "execution timeout" (Some 3_600_000L) execution_timeout;
          expect "run timeout" (Some 600_000L) run_timeout;
          expect "task timeout" (Some 10_500L) task_timeout;
          expect "reuse policy" (Some expected_policy) id_reuse_policy
      | _ -> failwith "expected exactly one child start command")
    [
      (`Allow_duplicate, Activation.Reuse_allow_duplicate);
      (`Allow_duplicate_failed_only, Activation.Reuse_allow_duplicate_failed_only);
      (`Reject_duplicate, Activation.Reuse_reject_duplicate);
    ]

(** Omitted options leave every new field unset, so the command (and the
    history it records) is unchanged from earlier releases. *)
let test_omitted_options_unchanged () =
  let _, commands =
    with_start (fun () -> Temporal.Child_workflow.start ~id:"child-1" child "input")
  in
  match commands with
  | [ Activation.Start_child_workflow
        { execution_timeout = None; run_timeout = None; task_timeout = None;
          id_reuse_policy = None; _ } ] -> ()
  | _ -> failwith "omitted child options changed the start command"

(** Every timeout combination [Client.start] rejects is a ready typed defect
    with the same message, and no command is emitted. The input codec is
    never reached, so a rejected start cannot run application code. *)
let test_invalid_timeouts_rejected () =
  List.iter
    (fun (label, start, message) ->
      let peeked, commands = with_start start in
      (match peeked with
      | Some (Error error) ->
          expect (label ^ " kind") "defect" (Temporal.Error.kind error);
          expect (label ^ " message") message (Temporal.Error.message error)
      | Some (Ok _) | None -> failwith (label ^ " was not a ready defect"));
      expect (label ^ " emitted nothing") [] commands)
    [
      ( "zero execution",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~execution_timeout:(Temporal.Duration.of_ms 0L) child "input"),
        "execution_timeout must be positive" );
      ( "task above 120 s",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~task_timeout:(seconds 121L) child "input"),
        "task_timeout (at most 120 seconds) exceeds its maximum" );
      ( "run above execution",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~execution_timeout:(seconds 60L) ~run_timeout:(seconds 61L) child
            "input"),
        "run_timeout exceeds execution_timeout" );
      ( "task above run",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~run_timeout:(seconds 30L) ~task_timeout:(seconds 31L) child "input"),
        "task_timeout exceeds the run (or execution) timeout" );
      ( "task above execution without run",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~execution_timeout:(seconds 30L) ~task_timeout:(seconds 31L) child
            "input"),
        "task_timeout exceeds the run (or execution) timeout" );
    ]

(** Encodes a string as a public payload for memo values. *)
let string_payload value =
  match Temporal.Codec.encode Temporal.Codec.string value with
  | Ok payload -> payload
  | Error error -> failwith ("encode failed: " ^ Temporal.Error.message error)

(** The keyword search attribute every metadata scenario sets. *)
let keyword = Temporal.Search_attribute.keyword "CustomKeywordField"

(** Typed search attributes encoded as the raw list the child start takes. *)
let search_attributes value =
  match Temporal.Search_attribute.encode [ Temporal.Search_attribute.set keyword value ] with
  | Ok values -> values
  | Error error -> failwith ("encode failed: " ^ Temporal.Error.message error)

(** A child memo and search attributes (#1135) reach the runtime command
    with every key and payload byte, and the native conversion carries them
    into the protocol command the bridge encodes. An empty map is the same
    as an omitted one, so the command stays the one earlier releases
    emitted. *)
let test_metadata_reaches_command () =
  let memo = [ ("note", string_payload "hello") ] in
  let attributes = search_attributes "child" in
  let _, commands =
    with_start (fun () ->
        Temporal.Child_workflow.start ~id:"child-1" ~memo
          ~search_attributes:attributes child "input")
  in
  (match commands with
  | [ (Activation.Start_child_workflow
         { memo = Some [ ("note", note) ]; search_attributes = Some [ (key, value) ]; _ }
       as command) ] -> (
      expect "memo payload" (string_payload "hello").data note.data;
      expect "search-attribute key" "CustomKeywordField" key;
      expect "search-attribute payload" (snd (List.hd attributes)).data value.data;
      expect "search-attribute metadata" (snd (List.hd attributes)).metadata
        value.metadata;
      match Temporal_runtime.Native_execution.command_to_protocol command with
      | Ok
          (Temporal_protocol.Workflow_protocol.Start_child_workflow
            { memo = Some [ ("note", _) ];
              search_attributes = Some [ ("CustomKeywordField", _) ]; _ }) -> ()
      | Ok _ -> failwith "native conversion dropped the child start metadata"
      | Error _ -> failwith "native conversion rejected the child start metadata")
  | _ -> failwith "expected one child start command with memo and search attributes");
  List.iter
    (fun (label, start) ->
      match with_start start with
      | _, [ Activation.Start_child_workflow { memo = None; search_attributes = None; _ } ] -> ()
      | _ -> failwith (label ^ " changed the start command"))
    [
      ("omitted metadata", fun () -> Temporal.Child_workflow.start ~id:"child-1" child "input");
      ( "empty metadata",
        fun () ->
          Temporal.Child_workflow.start ~id:"child-1" ~memo:[] ~search_attributes:[]
            child "input" );
    ]

(** Every malformed memo or search-attribute key is a ready typed defect
    with the message [Workflow.upsert_memo] and continue-as-new report, and
    no command is emitted. *)
let test_invalid_metadata_rejected () =
  let value = string_payload "v" in
  List.iter
    (fun (label, start, message) ->
      let peeked, commands = with_start start in
      (match peeked with
      | Some (Error error) ->
          expect (label ^ " kind") "defect" (Temporal.Error.kind error);
          expect (label ^ " message") message (Temporal.Error.message error)
      | Some (Ok _) | None -> failwith (label ^ " was not a ready defect"));
      expect (label ^ " emitted nothing") [] commands)
    [
      ( "empty memo key",
        (fun () -> Temporal.Child_workflow.start ~id:"child-1" ~memo:[ ("", value) ] child "input"),
        "Temporal memo key is empty" );
      ( "NUL memo key",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1" ~memo:[ ("a\000b", value) ] child "input"),
        "Temporal memo key contains a NUL byte" );
      ( "duplicate memo key",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~memo:[ ("k", value); ("k", value) ] child "input"),
        "Temporal memo keys must be unique" );
      ( "invalid UTF-8 search-attribute key",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~search_attributes:[ ("\xff", value) ] child "input"),
        "Temporal search-attribute key must be valid UTF-8" );
      ( "oversized search-attribute key",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~search_attributes:[ (String.make 65_537 'k', value) ] child "input"),
        "Temporal search-attribute key exceeds 65536 bytes" );
    ]

(** [Temporal.Testing] carries a child's memo and search attributes into the
    child's start metadata, as a server does: the child reads them through
    [Workflow.start_metadata] and returns what it saw to the parent. *)
let test_testing_carries_metadata () =
  let module T = Temporal in
  let ok label = function
    | Ok value -> value
    | Error error -> failwith (label ^ " failed: " ^ T.Error.message error)
  in
  let observed =
    T.Workflow.define ~name:"child_metadata_child" ~input:T.Codec.unit
      ~output:T.Codec.string (fun () ->
        let metadata = ok "start metadata" (T.Workflow.start_metadata ()) in
        let memo =
          match metadata.memo with
          | Some [ ("note", payload) ] -> ok "memo" (T.Codec.decode T.Codec.string payload)
          | Some _ | None -> "-"
        in
        let attribute =
          match
            T.Search_attribute.decode keyword
              (Option.value metadata.search_attributes ~default:[])
          with
          | Ok (Some value) -> value
          | Ok None | Error _ -> "-"
        in
        Ok (memo ^ "/" ^ attribute))
  in
  let parent =
    T.Workflow.define ~name:"child_metadata_parent" ~input:T.Codec.unit
      ~output:T.Codec.string (fun () ->
        let with_metadata =
          T.Child_workflow.execute ~id:"with-metadata"
            ~memo:[ ("note", string_payload "hello") ]
            ~search_attributes:(search_attributes "child") observed ()
        in
        let without = T.Child_workflow.execute ~id:"without-metadata" observed () in
        match (with_metadata, without) with
        | Ok first, Ok second -> Ok (first ^ ";" ^ second)
        | Error error, _ | _, Error error -> Error error)
  in
  let environment =
    ok "create"
      (T.Testing.create
         ~workflows:[ T.Testing.workflow parent; T.Testing.workflow observed ]
         ~activities:[] ())
  in
  Fun.protect
    ~finally:(fun () -> T.Testing.shutdown environment)
    (fun () ->
      let result = ok "parent" (T.Testing.execute environment parent ()) in
      expect ("child observations " ^ result) "hello/child;-/-" result)

(** Runs the child start option contract as one dune test. *)
let () =
  test_options_reach_command ();
  test_omitted_options_unchanged ();
  test_invalid_timeouts_rejected ();
  test_metadata_reaches_command ();
  test_invalid_metadata_rejected ();
  test_testing_carries_metadata ()

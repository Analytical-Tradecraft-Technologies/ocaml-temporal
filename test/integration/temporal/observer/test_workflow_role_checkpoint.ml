(** Focused tests for the private parent/child replay diagnostic state machine.

    These tests deliberately avoid files, JSON, Docker, and Temporal Server.
    They prove the pure transition rules which the public native-worker hook
    later combines with strict JSON decoding and atomic publication. *)

module Checkpoint = Workflow_role_checkpoint
(** Short alias that keeps the acceptance-specific test names readable. *)

(** Fails with contextual text when a simple value comparison differs. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Requires the stable typed error expected from one rejected transition. *)
let expect_error label expected_code = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error ({ Checkpoint.code; _ } : Checkpoint.error) ->
      expect (label ^ " code") expected_code code

(** The two deterministic workflow names used throughout this pure fixture. *)
let parent_workflow_id = "parent-workflow"

let child_workflow_id = "child-workflow"

(** The exact run IDs the controller learns from generation one before it starts
    the replacement worker. *)
let parent_run_id = "parent-run"

let child_run_id = "child-run"

(** Builds one role configuration whose run ID is intentionally unknown to the
    first worker generation. *)
let generation_one_configuration workflow_id =
  ({ Checkpoint.workflow_id; run_id = None } : Checkpoint.role_configuration)

(** Builds one role configuration for generation two, when the controller has
    already validated and supplied the exact run ID learned in generation one.
*)
let generation_two_configuration workflow_id run_id =
  ({ Checkpoint.workflow_id; run_id = Some run_id }
    : Checkpoint.role_configuration)

(** Extracts a successful pure state while keeping individual test bodies
    focused on the transition they are proving. *)
let require_state label = function
  | Ok state -> state
  | Error error ->
      failwith
        (label ^ " state creation failed: " ^ error.Checkpoint.code ^ " "
       ^ error.message)

(** Extracts the only complete document a two-role checkpoint transition may
    return. An [Accepted] result is intentionally not a publication point. *)
let require_checkpoint label = function
  | Ok (Checkpoint.Checkpoint { state; document }) -> (state, document)
  | Ok Checkpoint.Ignored -> failwith (label ^ " ignored the target activation")
  | Ok (Checkpoint.Accepted _) ->
      failwith (label ^ " returned an incomplete checkpoint")
  | Ok Checkpoint.Duplicate -> failwith (label ^ " unexpectedly duplicated")
  | Error error ->
      failwith
        (label ^ " checkpoint failed: " ^ error.Checkpoint.code ^ " "
       ^ error.message)

(** Builds metadata exactly as the native worker supplies it after protocol
    translation. No test fixture contains payloads or native handles. *)
let activation ?workflow_id ~run_id ~is_replaying ~history_length () =
  ({ Checkpoint.workflow_id; run_id; is_replaying; history_length }
    : Checkpoint.activation)

(** Creates the clean generation-one state used by multiple tests. *)
let generation_one_state () =
  Checkpoint.create ~generation:1
    ~parent:(generation_one_configuration parent_workflow_id)
    ~child:(generation_one_configuration child_workflow_id)
    ~previous:None
  |> require_state "generation one"

(** Advances a clean first generation through both causally ordered initial
    roles and returns its publishable document. *)
let complete_generation_one () =
  let initial = generation_one_state () in
  let after_parent =
    match
      Checkpoint.observe initial
        (activation ~workflow_id:parent_workflow_id ~run_id:parent_run_id
           ~is_replaying:false ~history_length:5L ())
    with
    | Ok (Checkpoint.Accepted state) -> state
    | Ok _ -> failwith "parent initial did not retain private partial state"
    | Error error -> failwith ("parent initial failed: " ^ error.Checkpoint.code)
  in
  Checkpoint.observe after_parent
    (activation ~workflow_id:child_workflow_id ~run_id:child_run_id
       ~is_replaying:false ~history_length:7L ())
  |> require_checkpoint "child initial"

(** Verifies that generation one learns exact runs from initial activations but
    exposes one complete atomic checkpoint only after both roles are present. *)
let test_generation_one_atomic_checkpoint () =
  let state, document = complete_generation_one () in
  expect "published parent workflow ID" parent_workflow_id
    document.Checkpoint.parent.workflow_id;
  expect "published parent run ID" parent_run_id document.parent.run_id;
  expect "published child workflow ID" child_workflow_id
    document.child.workflow_id;
  expect "published child run ID" child_run_id document.child.run_id;
  expect "generation-one canonical records"
    [
      {
        Checkpoint.role = Checkpoint.Parent;
        phase = Checkpoint.Initial;
        generation = 1;
        is_replaying = false;
        history_length = 5L;
      };
      {
        Checkpoint.role = Checkpoint.Child;
        phase = Checkpoint.Initial;
        generation = 1;
        is_replaying = false;
        history_length = 7L;
      };
    ]
    document.records;
  match
    Checkpoint.observe state
      (activation ~workflow_id:parent_workflow_id ~run_id:parent_run_id
         ~is_replaying:false ~history_length:9L ())
  with
  | Ok Checkpoint.Duplicate -> ()
  | Ok _ -> failwith "a repeated initial activation rewrote the checkpoint"
  | Error error -> failwith ("repeated initial activation failed: " ^ error.code)

(** A child cannot begin until the parent initial workflow task has issued the
    child-start command, so the state machine rejects an impossible ordering
    rather than accepting ambiguous role evidence. *)
let test_generation_one_rejects_child_before_parent () =
  Checkpoint.observe (generation_one_state ())
    (activation ~workflow_id:child_workflow_id ~run_id:child_run_id
       ~is_replaying:false ~history_length:7L ())
  |> expect_error "child before parent" "child_before_parent"

(** Verifies that a replacement generation validates the complete initial
    document, tolerates independent replay arrival order, and still publishes
    its records in canonical parent/child order. *)
let test_generation_two_replay_checkpoint () =
  let _generation_one_state, previous = complete_generation_one () in
  let generation_two =
    Checkpoint.create ~generation:2
      ~parent:(generation_two_configuration parent_workflow_id parent_run_id)
      ~child:(generation_two_configuration child_workflow_id child_run_id)
      ~previous:(Some previous)
    |> require_state "generation two"
  in
  let after_child =
    match
      Checkpoint.observe generation_two
        (activation ~workflow_id:child_workflow_id ~run_id:child_run_id
           ~is_replaying:true ~history_length:11L ())
    with
    | Ok (Checkpoint.Accepted state) -> state
    | Ok _ -> failwith "first replay should remain an unpublished partial state"
    | Error error -> failwith ("child replay failed: " ^ error.code)
  in
  let state, document =
    Checkpoint.observe after_child
      (activation ~workflow_id:parent_workflow_id ~run_id:parent_run_id
         ~is_replaying:true ~history_length:13L ())
    |> require_checkpoint "parent replay"
  in
  expect "generation-two canonical records"
    [
      {
        Checkpoint.role = Checkpoint.Parent;
        phase = Checkpoint.Initial;
        generation = 1;
        is_replaying = false;
        history_length = 5L;
      };
      {
        Checkpoint.role = Checkpoint.Child;
        phase = Checkpoint.Initial;
        generation = 1;
        is_replaying = false;
        history_length = 7L;
      };
      {
        Checkpoint.role = Checkpoint.Parent;
        phase = Checkpoint.Replay;
        generation = 2;
        is_replaying = true;
        history_length = 13L;
      };
      {
        Checkpoint.role = Checkpoint.Child;
        phase = Checkpoint.Replay;
        generation = 2;
        is_replaying = true;
        history_length = 11L;
      };
    ]
    document.records;
  match
    Checkpoint.observe state
      (activation ~run_id:parent_run_id ~is_replaying:true ~history_length:15L
         ())
  with
  | Ok Checkpoint.Duplicate -> ()
  | Ok _ ->
      failwith "a replay without initialization metadata was not deduplicated"
  | Error error -> failwith ("repeated replay failed: " ^ error.code)

(** Once a generation-two role has supplied its replay checkpoint, later live
    activations for that exact execution are outside this diagnostic's remit,
    both before and after the other role completes the atomic checkpoint. In
    particular, a durable timer may fire with [is_replaying = false]; treating
    that as a duplicate keeps the observer from failing an otherwise valid
    workflow task. Identity classification remains fail-closed throughout. *)
let test_generation_two_ignores_post_checkpoint_live_activations () =
  let _generation_one_state, previous = complete_generation_one () in
  let generation_two =
    Checkpoint.create ~generation:2
      ~parent:(generation_two_configuration parent_workflow_id parent_run_id)
      ~child:(generation_two_configuration child_workflow_id child_run_id)
      ~previous:(Some previous)
    |> require_state "generation two post-checkpoint activation"
  in
  let after_child_replay =
    match
      Checkpoint.observe generation_two
        (activation ~workflow_id:child_workflow_id ~run_id:child_run_id
           ~is_replaying:true ~history_length:11L ())
    with
    | Ok (Checkpoint.Accepted state) -> state
    | Ok _ -> failwith "child replay did not retain its partial checkpoint"
    | Error error -> failwith ("child replay failed: " ^ error.code)
  in
  (match
     Checkpoint.observe after_child_replay
       (activation ~workflow_id:child_workflow_id ~run_id:child_run_id
          ~is_replaying:false ~history_length:12L ())
   with
  | Ok Checkpoint.Duplicate -> ()
  | Ok _ -> failwith "partial post-checkpoint live activation was not deduplicated"
  | Error error ->
      failwith ("partial post-checkpoint live activation failed: " ^ error.code));
  let complete_state, document =
    Checkpoint.observe after_child_replay
      (activation ~workflow_id:parent_workflow_id ~run_id:parent_run_id
         ~is_replaying:true ~history_length:13L ())
    |> require_checkpoint "parent replay before live activation"
  in
  expect "interleaved generation-two canonical records"
    [
      {
        Checkpoint.role = Checkpoint.Parent;
        phase = Checkpoint.Initial;
        generation = 1;
        is_replaying = false;
        history_length = 5L;
      };
      {
        Checkpoint.role = Checkpoint.Child;
        phase = Checkpoint.Initial;
        generation = 1;
        is_replaying = false;
        history_length = 7L;
      };
      {
        Checkpoint.role = Checkpoint.Parent;
        phase = Checkpoint.Replay;
        generation = 2;
        is_replaying = true;
        history_length = 13L;
      };
      {
        Checkpoint.role = Checkpoint.Child;
        phase = Checkpoint.Replay;
        generation = 2;
        is_replaying = true;
        history_length = 11L;
      };
    ]
    document.records;
  (match
     Checkpoint.observe complete_state
       (activation ~workflow_id:child_workflow_id ~run_id:child_run_id
          ~is_replaying:false ~history_length:15L ())
   with
  | Ok Checkpoint.Duplicate -> ()
  | Ok _ -> failwith "complete post-checkpoint live activation was not deduplicated"
  | Error error ->
      failwith ("complete post-checkpoint live activation failed: " ^ error.code));
  Checkpoint.observe complete_state
    (activation ~workflow_id:"wrong-child-workflow" ~run_id:child_run_id
       ~is_replaying:false ~history_length:15L ())
  |> expect_error "post-checkpoint workflow mismatch"
       "activation_identity_mismatch"

(** Tests the fail-closed identity and phase checks that prevent one wrong run
    or non-replay task from being mistaken for parent/child recovery evidence.
*)
let test_generation_two_rejects_mixed_identity_and_phase () =
  let _generation_one_state, previous = complete_generation_one () in
  let state =
    Checkpoint.create ~generation:2
      ~parent:(generation_two_configuration parent_workflow_id parent_run_id)
      ~child:(generation_two_configuration child_workflow_id child_run_id)
      ~previous:(Some previous)
    |> require_state "generation two identity checks"
  in
  Checkpoint.observe state
    (activation ~workflow_id:parent_workflow_id ~run_id:"wrong-parent-run"
       ~is_replaying:true ~history_length:9L ())
  |> expect_error "wrong parent run" "activation_identity_mismatch";
  Checkpoint.observe state
    (activation ~workflow_id:"other-workflow" ~run_id:parent_run_id
       ~is_replaying:true ~history_length:9L ())
  |> expect_error "wrong parent workflow" "activation_identity_mismatch";
  Checkpoint.observe state
    (activation ~workflow_id:parent_workflow_id ~run_id:parent_run_id
       ~is_replaying:false ~history_length:9L ())
  |> expect_error "generation two non-replay" "unexpected_replay_state"

(** No real workflow activation can have an empty history, so zero or negative
    values cannot serve as evidence in either worker generation. *)
let test_rejects_non_positive_activation_history () =
  let state = generation_one_state () in
  Checkpoint.observe state
    (activation ~workflow_id:parent_workflow_id ~run_id:parent_run_id
       ~is_replaying:false ~history_length:0L ())
  |> expect_error "zero initial history" "invalid_history_length";
  Checkpoint.observe state
    (activation ~workflow_id:parent_workflow_id ~run_id:parent_run_id
       ~is_replaying:false ~history_length:(-1L) ())
  |> expect_error "negative initial history" "invalid_history_length"

(** Ensures unsupported generations, known generation-one run IDs, and partial
    prior documents fail before any activation is observed. *)
let test_configuration_and_prior_document_rejections () =
  Checkpoint.create ~generation:3
    ~parent:(generation_one_configuration parent_workflow_id)
    ~child:(generation_one_configuration child_workflow_id)
    ~previous:None
  |> expect_error "unsupported generation" "invalid_generation";
  Checkpoint.create ~generation:1
    ~parent:(generation_two_configuration parent_workflow_id parent_run_id)
    ~child:(generation_one_configuration child_workflow_id)
    ~previous:None
  |> expect_error "generation one run ID" "invalid_generation_one_configuration";
  let partial_document : Checkpoint.document =
    {
      parent = { workflow_id = parent_workflow_id; run_id = parent_run_id };
      child = { workflow_id = child_workflow_id; run_id = child_run_id };
      records =
        [
          {
            Checkpoint.role = Checkpoint.Parent;
            phase = Checkpoint.Initial;
            generation = 1;
            is_replaying = false;
            history_length = 5L;
          };
        ];
    }
  in
  Checkpoint.create ~generation:2
    ~parent:(generation_two_configuration parent_workflow_id parent_run_id)
    ~child:(generation_two_configuration child_workflow_id child_run_id)
    ~previous:(Some partial_document)
  |> expect_error "partial prior document" "invalid_prior_document"

(** The persisted JSON decoder accepts exactly one decimal spelling for each
    history length. OCaml's integer parser accepts several convenient source
    syntaxes which must not leak into this cross-process protocol. *)
let test_canonical_history_length_parser () =
  expect "zero history length" (Ok 0L) (Checkpoint.history_length_of_string "0");
  expect "maximum history length" (Ok Int64.max_int)
    (Checkpoint.history_length_of_string "9223372036854775807");
  List.iter
    (fun value ->
      Checkpoint.history_length_of_string value
      |> expect_error
           ("non-canonical history length " ^ value)
           "invalid_history_length")
    [ ""; "-0"; "+1"; "01"; "0x10"; "1_0"; "9223372036854775808" ]

(** Identifier validation is stricter than Temporal's general identifier limit
    because four identities must fit one bounded, atomically persisted
    checkpoint even after JSON escaping. *)
let test_identifier_encoding_and_size_bounds () =
  let invalid_utf_8 = String.make 1 (Char.chr 0xff) in
  Checkpoint.create ~generation:1
    ~parent:(generation_one_configuration invalid_utf_8)
    ~child:(generation_one_configuration child_workflow_id)
    ~previous:None
  |> expect_error "invalid UTF-8 workflow ID" "invalid_identifier";
  Checkpoint.create ~generation:1
    ~parent:(generation_one_configuration (String.make 4_097 'p'))
    ~child:(generation_one_configuration child_workflow_id)
    ~previous:None
  |> expect_error "oversized workflow ID" "invalid_identifier";
  ignore
    (Checkpoint.create ~generation:1
       ~parent:(generation_one_configuration (String.make 4_096 '\\'))
       ~child:(generation_one_configuration child_workflow_id)
       ~previous:None
    |> require_state "maximum escaped workflow ID")

(** Executes every pure parent/child diagnostic transition scenario. *)
let () =
  test_generation_one_atomic_checkpoint ();
  test_generation_one_rejects_child_before_parent ();
  test_generation_two_replay_checkpoint ();
  test_generation_two_ignores_post_checkpoint_live_activations ();
  test_generation_two_rejects_mixed_identity_and_phase ();
  test_rejects_non_positive_activation_history ();
  test_configuration_and_prior_document_rejections ();
  test_canonical_history_length_parser ();
  test_identifier_encoding_and_size_bounds ()

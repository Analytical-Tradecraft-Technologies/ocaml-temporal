(** Implements the pure parent/child restart-replay diagnostic state machine.

    The surrounding native-worker hook owns environment parsing, JSON I/O, and
    atomic publication. This module intentionally contains none of those
    effects: a caller can calculate a candidate transition, publish its entire
    document, and commit the candidate only after that publication succeeds. *)

(** The fixed roles observed by this acceptance-only state machine. *)
type role = Parent | Child

(** The two generation-specific observation phases. *)
type phase = Initial | Replay

type identity = { workflow_id : string; run_id : string }
(** Exact, payload-free identity retained after a role has been observed. *)

type activation = {
  workflow_id : string option;
  run_id : string;
  is_replaying : bool;
  history_length : int64;
}
(** Metadata copied from a strictly translated native activation. *)

type record = {
  role : role;
  phase : phase;
  generation : int;
  is_replaying : bool;
  history_length : int64;
}
(** One canonical role checkpoint in the file-backed diagnostic document. *)

type document = { parent : identity; child : identity; records : record list }
(** The complete, payload-free document handed to the native-worker JSON adapter
    for atomic publication. *)

type role_configuration = { workflow_id : string; run_id : string option }
(** Configuration known before one worker generation begins. The run ID is
    necessarily absent for generation one because Temporal allocates it only
    after that worker begins processing the parent/child executions. *)

type error = { code : string; message : string }
(** Stable failure detail returned for invalid state-machine inputs. It does not
    include an identifier, payload, task token, or arbitrary activation content,
    so callers can safely surface its bounded message in diagnostics. *)

type role_progress = {
  workflow_id : string;
  run_id : string option;
  initial_history_length : int64 option;
  replay_history_length : int64 option;
}
(** One role's locally retained progress. [run_id] becomes [Some] only after
    generation one observes the exact role. Generation two starts with the run
    IDs and initial history lengths validated from the persisted document. *)

type t = { generation : int; parent : role_progress; child : role_progress }
(** Immutable state for the one fixed two-role diagnostic. Immutability is
    important at the publication boundary: a failed write leaves the caller's
    previous value valid for a later retry. *)

(** The outcome of considering one activation without side effects. *)
type observation =
  | Ignored
  | Accepted of t
  | Duplicate
  | Checkpoint of { state : t; document : document }

(** Result-bind notation keeps invalid diagnostics on the typed error path. *)
let ( let* ) = Result.bind

(** Builds a stable error without exposing input values in the message. *)
let error code message = Error { code; message }

(** Returns the closed spelling stored in a JSON role record. *)
let role_name = function Parent -> "parent" | Child -> "child"

(** Returns the closed spelling stored in a JSON phase record. *)
let phase_name = function Initial -> "initial" | Replay -> "replay"

(** Parses one closed role spelling from a persisted document. *)
let role_of_string = function
  | "parent" -> Ok Parent
  | "child" -> Ok Child
  | _ -> error "invalid_role" "parent/child diagnostic role is invalid"

(** Parses one closed phase spelling from a persisted document. *)
let phase_of_string = function
  | "initial" -> Ok Initial
  | "replay" -> Ok Replay
  | _ -> error "invalid_phase" "parent/child diagnostic phase is invalid"

(** Checks that an identifier is present, bounded, and safe for the closed JSON
    diagnostic protocol. Temporal identifiers may contain ordinary printable
    Unicode, but control characters make fixture logs and files hard to reason
    about, so the test-only contract rejects them. *)
let validate_identifier kind value =
  let rec contains_control index =
    if index = String.length value then false
    else
      let code = Char.code value.[index] in
      if code < 0x20 || code = 0x7f then true else contains_control (index + 1)
  in
  if
    value = ""
    || String.length value > 4_096
    || contains_control 0
    || not (Temporal_base.Codec.valid_utf_8 value)
  then
    error "invalid_identifier"
      ("parent/child diagnostic " ^ kind
     ^ " is empty, exceeds 4096 bytes, contains control bytes, or is not valid \
        UTF-8")
  else Ok ()

(** Parses the canonical decimal representation used for persisted history
    lengths. Requiring the parsed value to print back identically rejects the
    alternate syntaxes accepted by [Int64.of_string], including signs, numeric
    separators, and hexadecimal notation. *)
let history_length_of_string value =
  try
    let parsed = Int64.of_string value in
    if
      Int64.compare parsed 0L < 0
      || not (String.equal value (Int64.to_string parsed))
    then
      error "invalid_history_length"
        "parent/child diagnostic history length is not canonical non-negative \
         decimal"
    else Ok parsed
  with _ ->
    error "invalid_history_length"
      "parent/child diagnostic history length is outside signed 64-bit range"

(** Validates one role configuration before it can become retained state. *)
let validate_configuration (configuration : role_configuration) =
  let* () = validate_identifier "workflow ID" configuration.workflow_id in
  match configuration.run_id with
  | None -> Ok ()
  | Some run_id -> validate_identifier "run ID" run_id

(** Rejects role aliases. Two roles with one workflow or one run identity would
    let one activation satisfy both checkpoints and would make the diagnostic
    evidence ambiguous. *)
let validate_distinct_roles (parent : role_configuration)
    (child : role_configuration) =
  if String.equal parent.workflow_id child.workflow_id then
    error "ambiguous_roles"
      "parent and child diagnostic workflow IDs must be distinct"
  else
    match (parent.run_id, child.run_id) with
    | Some parent_run, Some child_run when String.equal parent_run child_run ->
        error "ambiguous_roles"
          "parent and child diagnostic run IDs must be distinct"
    | _ -> Ok ()

(** Builds the parent or child progress used before an initial observation. *)
let empty_progress (configuration : role_configuration) =
  {
    workflow_id = configuration.workflow_id;
    run_id = configuration.run_id;
    initial_history_length = None;
    replay_history_length = None;
  }

(** Builds a record only after the phase-specific replay marker was checked. *)
let record role phase generation history_length : record =
  { role; phase; generation; is_replaying = phase = Replay; history_length }

(** Converts both complete role progresses into a canonical document. The
    document is deliberately absent while a generation has only one role:
    readers either see no checkpoint or an atomic checkpoint containing every
    role required for that generation. *)
let document_if_complete (state : t) =
  match
    ( state.parent.run_id,
      state.child.run_id,
      state.parent.initial_history_length,
      state.child.initial_history_length,
      state.parent.replay_history_length,
      state.child.replay_history_length )
  with
  | ( Some parent_run,
      Some child_run,
      Some parent_initial,
      Some child_initial,
      _,
      _ )
    when state.generation = 1 ->
      Some
        {
          parent =
            { workflow_id = state.parent.workflow_id; run_id = parent_run };
          child = { workflow_id = state.child.workflow_id; run_id = child_run };
          records =
            [
              record Parent Initial 1 parent_initial;
              record Child Initial 1 child_initial;
            ];
        }
  | ( Some parent_run,
      Some child_run,
      Some parent_initial,
      Some child_initial,
      Some parent_replay,
      Some child_replay )
    when state.generation = 2 ->
      Some
        {
          parent =
            { workflow_id = state.parent.workflow_id; run_id = parent_run };
          child = { workflow_id = state.child.workflow_id; run_id = child_run };
          records =
            [
              record Parent Initial 1 parent_initial;
              record Child Initial 1 child_initial;
              record Parent Replay 2 parent_replay;
              record Child Replay 2 child_replay;
            ];
        }
  | _ -> None

(** Requires one complete canonical generation-one document and returns its two
    initial history lengths. Generation two must never resume from a partial,
    older, mixed-role, or already-replayed document. *)
let validate_generation_one_document ~(parent : role_configuration)
    ~(child : role_configuration) (document : document) =
  let* () =
    validate_identifier "parent workflow ID" document.parent.workflow_id
  in
  let* () = validate_identifier "parent run ID" document.parent.run_id in
  let* () =
    validate_identifier "child workflow ID" document.child.workflow_id
  in
  let* () = validate_identifier "child run ID" document.child.run_id in
  match (parent.run_id, child.run_id) with
  | Some expected_parent_run, Some expected_child_run -> (
      if
        not
          (String.equal document.parent.workflow_id parent.workflow_id
          && String.equal document.parent.run_id expected_parent_run
          && String.equal document.child.workflow_id child.workflow_id
          && String.equal document.child.run_id expected_child_run)
      then
        error "prior_identity_mismatch"
          "parent/child diagnostic document identities do not match generation \
           two configuration"
      else
        match document.records with
        | [ parent_record; child_record ]
          when parent_record.role = Parent
               && parent_record.phase = Initial
               && parent_record.generation = 1
               && (not parent_record.is_replaying)
               && parent_record.history_length > 0L
               && child_record.role = Child
               && child_record.phase = Initial
               && child_record.generation = 1
               && (not child_record.is_replaying)
               && child_record.history_length > 0L ->
            Ok (parent_record.history_length, child_record.history_length)
        | _ ->
            error "invalid_prior_document"
              "parent/child generation two requires the complete canonical \
               generation-one document")
  | _ ->
      error "invalid_generation_two_configuration"
        "generation two requires configured parent and child run IDs"

(** Creates the immutable state for one exact worker generation. Generation one
    learns run IDs only from initial activations; generation two validates
    supplied IDs against the document atomically published by generation one. *)
let create ~generation ~(parent : role_configuration)
    ~(child : role_configuration) ~previous =
  let* () = validate_configuration parent in
  let* () = validate_configuration child in
  let* () = validate_distinct_roles parent child in
  match generation with
  | 1 -> (
      match (parent.run_id, child.run_id, previous) with
      | None, None, None ->
          Ok
            {
              generation;
              parent = empty_progress parent;
              child = empty_progress child;
            }
      | _ ->
          error "invalid_generation_one_configuration"
            "generation one requires no run IDs and no prior parent/child \
             document")
  | 2 -> (
      match (parent.run_id, child.run_id, previous) with
      | Some _, Some _, Some document ->
          let* parent_initial, child_initial =
            validate_generation_one_document ~parent ~child document
          in
          Ok
            {
              generation;
              parent =
                {
                  workflow_id = parent.workflow_id;
                  run_id = parent.run_id;
                  initial_history_length = Some parent_initial;
                  replay_history_length = None;
                };
              child =
                {
                  workflow_id = child.workflow_id;
                  run_id = child.run_id;
                  initial_history_length = Some child_initial;
                  replay_history_length = None;
                };
            }
      | _ ->
          error "invalid_generation_two_configuration"
            "generation two requires exact run IDs and one complete \
             parent/child document")
  | _ ->
      error "invalid_generation"
        "parent/child diagnostic generation must be exactly one or two"

(** Identifies how a translated activation relates to one fixed role. A known
    workflow ID with a mismatched run ID, or a known run ID paired with a
    different workflow ID, is a fail-closed configuration/history error rather
    than an unrelated activation. *)
type role_match = Unrelated | Matches of role

(** Returns whether [run_id] is already bound to the role other than [role].
    This protects generation one while it is learning its second run ID. *)
let run_belongs_to_other (state : t) role run_id =
  let other = match role with Parent -> state.child | Child -> state.parent in
  match other.run_id with
  | Some expected -> String.equal expected run_id
  | None -> false

(** Classifies an activation by exact configured workflow ID and, once known,
    exact run ID. A missing workflow ID is acceptable only for an already bound
    run, which is needed for later replay/cache-removal activations that omit
    InitializeWorkflow metadata. *)
let classify (state : t) (activation : activation) =
  let progress_for = function Parent -> state.parent | Child -> state.child in
  let validate_match role =
    let progress = progress_for role in
    match progress.run_id with
    | Some expected when not (String.equal expected activation.run_id) ->
        error "activation_identity_mismatch"
          "parent/child diagnostic activation run ID changed"
    | Some _ -> Ok (Matches role)
    | None when run_belongs_to_other state role activation.run_id ->
        error "activation_identity_mismatch"
          "one activation cannot satisfy both parent and child roles"
    | None -> Ok (Matches role)
  in
  match activation.workflow_id with
  | Some workflow_id when String.equal workflow_id state.parent.workflow_id ->
      validate_match Parent
  | Some workflow_id when String.equal workflow_id state.child.workflow_id ->
      validate_match Child
  | Some _ ->
      let run_matches progress =
        match progress.run_id with
        | Some expected -> String.equal expected activation.run_id
        | None -> false
      in
      if run_matches state.parent || run_matches state.child then
        error "activation_identity_mismatch"
          "parent/child diagnostic activation workflow ID changed"
      else Ok Unrelated
  | None -> (
      match (state.parent.run_id, state.child.run_id) with
      | Some parent_run, _ when String.equal parent_run activation.run_id ->
          Ok (Matches Parent)
      | _, Some child_run when String.equal child_run activation.run_id ->
          Ok (Matches Child)
      | _ -> Ok Unrelated)

(** Validates the replay bit and history length for a role's first observation
    in the current generation. A positive replay history is required because a
    replay marker with no history would not prove that the replacement worker
    replayed work. Later activations for an observed exact run are duplicates
    and deliberately bypass this phase check. *)
let validate_activation_phase (state : t) (activation : activation) =
  if activation.history_length <= 0L then
    error "invalid_history_length"
      "parent/child diagnostic activation history length must be positive"
  else
    let expected_replay = state.generation = 2 in
    if activation.is_replaying <> expected_replay then
      error "unexpected_replay_state"
        "parent/child diagnostic activation replay state does not match its \
         generation"
    else Ok ()

(** Returns the progress currently associated with one fixed role. *)
let progress_for (state : t) = function
  | Parent -> state.parent
  | Child -> state.child

(** Installs the first phase-specific observation into a role progress. The
    caller has already validated that either the configured run matches or the
    generation-one run is still unknown. *)
let observe_progress (state : t) (progress : role_progress)
    (activation : activation) =
  if state.generation = 1 then
    {
      progress with
      run_id = Some activation.run_id;
      initial_history_length = Some activation.history_length;
    }
  else { progress with replay_history_length = Some activation.history_length }

(** Returns whether the current generation already has a checkpoint for one
    role. Repeated delivery is normal around worker replacement, and live
    activations resume after replay finishes. Once the exact role has supplied
    its evidence, both cases are no-ops rather than opportunities to revalidate
    or rewrite the published checkpoint. *)
let already_observed (state : t) (progress : role_progress) =
  if state.generation = 1 then Option.is_some progress.initial_history_length
  else Option.is_some progress.replay_history_length

(** Rebuilds an immutable state after one role's first observation. *)
let replace_progress (state : t) role (progress : role_progress) =
  match role with
  | Parent -> { state with parent = progress }
  | Child -> { state with child = progress }

(** Validates and records one activation. Initial child observation is ordered
    after parent observation because the child cannot exist until the parent
    initial workflow task emits its start command. Generation-two replays are
    intentionally unordered: independent server workflow tasks may reach the
    replacement worker in either order, while [document_if_complete] still
    publishes their canonical parent-then-child representation. *)
let observe (state : t) (activation : activation) =
  let* classification = classify state activation in
  match classification with
  | Unrelated -> Ok Ignored
  | Matches role -> (
      let progress = progress_for state role in
      if already_observed state progress then Ok Duplicate
      else
        let* () = validate_activation_phase state activation in
        if
          state.generation = 1 && role = Child
          && Option.is_none state.parent.initial_history_length
        then
          error "child_before_parent"
            "parent initial checkpoint must be observed before the child \
             checkpoint"
        else
          let next =
            replace_progress state role
              (observe_progress state progress activation)
          in
          match document_if_complete next with
          | Some document -> Ok (Checkpoint { state = next; document })
          | None -> Ok (Accepted next))

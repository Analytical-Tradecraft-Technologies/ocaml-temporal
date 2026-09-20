(** Acceptance-only replay, cache-eviction and parent/child checkpoint observers.

    This library is neither installed nor linked by ordinary SDK consumers.
    Each fixture creates fresh observer state and explicitly scopes injection
    around one public worker construction. Callback execution and native lease
    ownership remain with the serialized workflow adapter. *)

module Base_error = Temporal_base.Error
module Workflow_adapter = Temporal_runtime.Native_worker_execution
module Role_checkpoint = Workflow_role_checkpoint
module Observer = Temporal_runtime.Native_worker_observer

(** Keep expected fixture-configuration failures on the typed result path. *)
let ( let* ) = Result.bind

type replay_record = {
  phase : string;
  generation : int;
  is_replaying : bool;
  history_length : int64;
}
(** A replay diagnostic record is deliberately smaller than a workflow
    activation: it contains only the identity and Core metadata needed by the
    restart acceptance test. The record never includes payload bytes,
    timestamps, task tokens, or user workflow values. *)

type replay_diagnostics = {
  path : string;
  cache_eviction_path : string option;
  cache_eviction_ready_path : string option;
  (* Optional marker for the second cache fixture run's first acknowledged
     activation. It is intentionally separate from the A-run barrier so the
     client can prove B completed before waiting for A's eviction marker. *)
  cache_eviction_second_ready_path : string option;
  generation : int;
  target_workflow_id : string option;
  (* Optional exact workflow ID used by the second cache fixture barrier. *)
  second_target_workflow_id : string option;
  mutable workflow_id : string option;
  mutable run_id : string option;
  mutable records : replay_record list;
}
(** Mutable state for the optional file-backed replay observer. The state is
    reached only from the serialized workflow adapter callback, while the file
    itself is replaced atomically after every new record. *)

(** Returns one required JSON object field while rejecting duplicate or missing
    values. Diagnostics are a private test protocol, but strict decoding here
    prevents a stale or hand-edited file from being mistaken for replay proof.
*)
let replay_field name fields =
  match List.filter (fun (key, _) -> String.equal key name) fields with
  | [ (_, value) ] -> Ok value
  | [] ->
      Error (Base_error.defect ~message:("replay diagnostics missing " ^ name))
  | _ ->
      Error
        (Base_error.defect ~message:("replay diagnostics duplicate " ^ name))

(** Requires an exact set of object keys before reading a replay document. *)
let replay_object expected fields =
  let actual = List.map fst fields |> List.sort String.compare in
  let expected = List.sort String.compare expected in
  if actual = expected then Ok fields
  else
    Error
      (Base_error.defect
         ~message:"replay diagnostics contain unexpected or missing fields")

(** Reads one bounded JSON document from the diagnostic path. The size limit
    protects worker startup from accidentally ingesting a large arbitrary file
    mounted at the test path. *)
let read_replay_json path =
  try
    let channel = open_in_bin path in
    Fun.protect
      ~finally:(fun () -> close_in_noerr channel)
      (fun () ->
        let length = in_channel_length channel in
        if length < 0 || length > 65_536 then
          Error
            (Base_error.defect
               ~message:"replay diagnostics file is missing or too large")
        else
          let contents = really_input_string channel length in
          try Ok (Yojson.Safe.from_string contents)
          with Yojson.Json_error message ->
            Error
              (Base_error.defect
                 ~message:("replay diagnostics JSON is invalid: " ^ message)))
  with exception_ ->
    Error
      (Base_error.defect
         ~message:
           (Printf.sprintf "cannot read replay diagnostics: %s"
              (Printexc.to_string exception_)))

(** Decodes a decimal JSON string as a signed 64-bit history length. History
    lengths stay strings on disk so JSON number implementations cannot round a
    large Temporal value through a floating-point representation. *)
let replay_history_length = function
  | `String value ->
      Role_checkpoint.history_length_of_string value
      |> Result.map_error (fun error ->
          Base_error.defect ~message:error.Role_checkpoint.message)
  | _ ->
      Error
        (Base_error.defect
           ~message:"replay diagnostics history length must be a string")

(** Decodes one strict replay record from a previously published document. *)
let decode_replay_record = function
  | `Assoc fields ->
      let* fields =
        replay_object
          [ "phase"; "generation"; "is_replaying"; "history_length" ]
          fields
      in
      let* phase = replay_field "phase" fields in
      let* phase =
        match phase with
        | `String ("initial" as phase) | `String ("replay" as phase) -> Ok phase
        | _ ->
            Error (Base_error.defect ~message:"invalid replay diagnostic phase")
      in
      let* generation = replay_field "generation" fields in
      let* generation =
        match generation with
        | `Int value when value >= 1 -> Ok value
        | `Intlit value -> (
            try
              let parsed = int_of_string value in
              if parsed >= 1 then Ok parsed
              else
                Error (Base_error.defect ~message:"invalid replay generation")
            with _ ->
              Error (Base_error.defect ~message:"invalid replay generation"))
        | _ -> Error (Base_error.defect ~message:"invalid replay generation")
      in
      let* is_replaying = replay_field "is_replaying" fields in
      let* is_replaying =
        match is_replaying with
        | `Bool value -> Ok value
        | _ -> Error (Base_error.defect ~message:"invalid replay marker")
      in
      let* history_length = replay_field "history_length" fields in
      let* history_length = replay_history_length history_length in
      Ok { phase; generation; is_replaying; history_length }
  | _ ->
      Error
        (Base_error.defect ~message:"replay diagnostic record is not an object")

(** Loads the prior generation's diagnostic document and checks that its records
    already prove the initial activation. Generation one starts from a clean
    path; later generations must not silently create a new root. *)
let load_replay_diagnostics path generation target_workflow_id =
  if generation = 1 then
    Ok
      {
        path;
        cache_eviction_path = None;
        cache_eviction_ready_path = None;
        cache_eviction_second_ready_path = None;
        generation;
        target_workflow_id;
        second_target_workflow_id = None;
        workflow_id = None;
        run_id = None;
        records = [];
      }
  else
    let* json = read_replay_json path in
    match json with
    | `Assoc fields ->
        let* fields =
          replay_object [ "workflow_id"; "run_id"; "records" ] fields
        in
        let* workflow_id = replay_field "workflow_id" fields in
        let* workflow_id =
          match workflow_id with
          | `String value when value <> "" -> Ok value
          | _ -> Error (Base_error.defect ~message:"invalid replay workflow ID")
        in
        let* run_id = replay_field "run_id" fields in
        let* run_id =
          match run_id with
          | `String value when value <> "" -> Ok value
          | _ -> Error (Base_error.defect ~message:"invalid replay run ID")
        in
        let* records = replay_field "records" fields in
        let* records =
          match records with
          | `List values ->
              let rec loop reversed = function
                | [] -> Ok (List.rev reversed)
                | value :: rest ->
                    let* record = decode_replay_record value in
                    loop (record :: reversed) rest
              in
              loop [] values
          | _ ->
              Error
                (Base_error.defect ~message:"replay records must be an array")
        in
        let* () =
          match records with
          | [ { phase = "initial"; generation = 1; is_replaying = false; _ } ]
            ->
              Ok ()
          | _ ->
              Error
                (Base_error.defect
                   ~message:
                     "replay diagnostics must contain exactly one \
                      generation-one initial record")
        in
        let* () =
          if
            match target_workflow_id with
            | Some expected -> not (String.equal expected workflow_id)
            | None -> false
          then
            Error
              (Base_error.defect
                 ~message:
                   "replay diagnostics workflow ID does not match configuration")
          else Ok ()
        in
        Ok
          {
            path;
            cache_eviction_path = None;
            cache_eviction_ready_path = None;
            cache_eviction_second_ready_path = None;
            generation;
            target_workflow_id;
            second_target_workflow_id = None;
            workflow_id = Some workflow_id;
            run_id = Some run_id;
            records;
          }
    | _ ->
        Error
          (Base_error.defect ~message:"replay diagnostics root is not an object")

(** Writes a diagnostic document through a same-directory temporary file and
    rename. The worker callback is serialized, so a generation cannot interleave
    two writes; the rename additionally ensures readers never see partial JSON.
*)
let write_replay_diagnostics state =
  let record_json record =
    `Assoc
      [
        ("phase", `String record.phase);
        ("generation", `Int record.generation);
        ("is_replaying", `Bool record.is_replaying);
        ("history_length", `String (Int64.to_string record.history_length));
      ]
  in
  match (state.workflow_id, state.run_id) with
  | Some workflow_id, Some run_id -> (
      let json =
        `Assoc
          [
            ("workflow_id", `String workflow_id);
            ("run_id", `String run_id);
            ("records", `List (List.map record_json state.records));
          ]
      in
      let temporary = ref None in
      try
        let generated =
          Filename.temp_file
            ~temp_dir:(Filename.dirname state.path)
            (Filename.basename state.path ^ ".tmp.")
            ""
        in
        temporary := Some generated;
        let channel = open_out_bin generated in
        Fun.protect
          ~finally:(fun () -> close_out_noerr channel)
          (fun () ->
            Yojson.Safe.to_channel channel json;
            output_char channel '\n';
            flush channel);
        Sys.rename generated state.path;
        temporary := None
      with exception_ ->
        Option.iter
          (fun generated -> try Sys.remove generated with _ -> ())
          !temporary;
        raise exception_)
  | _ -> failwith "replay diagnostics state has no workflow/run identity"

(** Writes one payload-free eviction marker after Core has acknowledged an
    explicit cache-removal activation. The marker is separate from replay
    history because an eviction can occur between two replay records and must
    not change the restart document's two-record contract. *)
let write_cache_eviction_marker state ~reason =
  match (state.cache_eviction_path, state.workflow_id, state.run_id) with
  | Some path, Some workflow_id, Some run_id -> (
      let json =
        `Assoc
          [
            ("workflow_id", `String workflow_id);
            ("run_id", `String run_id);
            ("reason", `String reason);
          ]
      in
      let temporary = ref None in
      try
        let generated =
          Filename.temp_file ~temp_dir:(Filename.dirname path)
            (Filename.basename path ^ ".tmp.")
            ""
        in
        temporary := Some generated;
        let channel = open_out_bin generated in
        Fun.protect
          ~finally:(fun () -> close_out_noerr channel)
          (fun () ->
            Yojson.Safe.to_channel channel json;
            output_char channel '\n';
            flush channel);
        Sys.rename generated path;
        temporary := None
      with exception_ ->
        Option.iter
          (fun generated -> try Sys.remove generated with _ -> ())
          !temporary;
        raise exception_)
  | None, _, _ -> ()
  | _ -> failwith "cache eviction state has no workflow/run identity"

(** Publishes a private completion barrier after the first cache fixture run's
    normal activation completion has been acknowledged by Core. Atomic replace
    keeps the client-only driver from observing a partial marker. *)
let write_cache_eviction_ready_marker path =
  let temporary = ref None in
  try
    let generated =
      Filename.temp_file ~temp_dir:(Filename.dirname path)
        (Filename.basename path ^ ".tmp.")
        ""
    in
    temporary := Some generated;
    let channel = open_out_bin generated in
    Fun.protect
      ~finally:(fun () -> close_out_noerr channel)
      (fun () ->
        output_string channel "initial-completion\n";
        flush channel);
    Sys.rename generated path;
    temporary := None
  with exception_ ->
    Option.iter
      (fun generated -> try Sys.remove generated with _ -> ())
      !temporary;
    raise exception_

(** Validates an optional payload-free cache-eviction marker path. *)
let optional_marker_path name = function
  | None -> Ok None
  | Some "" -> Ok None
  | Some path
    when path <> ""
         && (not (String.contains path '\000'))
         && not (Filename.is_relative path) ->
      Ok (Some path)
  | Some _ ->
      Error
        (Base_error.defect
           ~message:(name ^ " must be a non-empty absolute path without NUL"))

(** Creates the optional replay observer from test-only environment settings.
    Only fixture executables call this factory; the installed worker never
    reads these settings or links this module. The observer records only the first initial and first
    replay activation for the configured workflow/run. *)
let replay_diagnostic_hook ~getenv () =
  match getenv "SMOKE_WORKER_REPLAY_DIAGNOSTICS_FILE" with
  | None -> Ok (None, None)
  | Some path
    when path <> ""
         && (not (String.contains path '\000'))
         && not (Filename.is_relative path) ->
      let* generation =
        match getenv "SMOKE_WORKER_GENERATION" with
        | Some value -> (
            try
              let parsed = int_of_string value in
              if parsed >= 1 then Ok parsed
              else
                Error
                  (Base_error.defect
                     ~message:"SMOKE_WORKER_GENERATION must be positive")
            with _ ->
              Error
                (Base_error.defect
                   ~message:"SMOKE_WORKER_GENERATION must be an integer"))
        | None ->
            Error
              (Base_error.defect ~message:"SMOKE_WORKER_GENERATION must be set")
      in
      let target_workflow_id =
        match getenv "SMOKE_REPLAY_WORKFLOW_ID" with
        | Some value when value <> "" -> Some value
        | _ -> None
      in
      let* cache_eviction_path =
        optional_marker_path "SMOKE_WORKER_CACHE_EVICTION_FILE"
          (getenv "SMOKE_WORKER_CACHE_EVICTION_FILE")
      in
      let* cache_eviction_ready_path =
        optional_marker_path "SMOKE_WORKER_CACHE_EVICTION_READY_FILE"
          (getenv "SMOKE_WORKER_CACHE_EVICTION_READY_FILE")
      in
      let* cache_eviction_second_ready_path =
        optional_marker_path "SMOKE_WORKER_CACHE_EVICTION_SECOND_READY_FILE"
          (getenv "SMOKE_WORKER_CACHE_EVICTION_SECOND_READY_FILE")
      in
      let second_target_workflow_id =
        match getenv "SMOKE_CACHE_EVICTION_SECOND_WORKFLOW_ID" with
        | Some value when value <> "" -> Some value
        | _ -> None
      in
      let* state = load_replay_diagnostics path generation target_workflow_id in
      let state =
        {
          state with
          cache_eviction_path;
          cache_eviction_ready_path;
          cache_eviction_second_ready_path;
          second_target_workflow_id;
          (* Core may omit workflow identity metadata on the initial and
             cache-removal activations.  The cache acceptance fixture supplies
             the exact workflow ID out of band, so retain it as the diagnostic
             identity while still learning the run ID from the activation.
             This keeps a later [cache_full] marker attributable even when the
             activation itself contains no workflow ID. *)
          workflow_id =
            (match (cache_eviction_path, target_workflow_id) with
            | Some _, Some workflow_id -> Some workflow_id
            | _ -> state.workflow_id);
        }
      in
      let matches_target (info : Workflow_adapter.activation_info) =
        match
          (state.target_workflow_id, info.workflow_id, state.workflow_id)
        with
        | Some target, Some workflow_id, _ -> String.equal target workflow_id
        | Some target, None, Some workflow_id -> String.equal target workflow_id
        | Some _, None, None -> Option.is_some state.cache_eviction_path
        | None, Some workflow_id, _ -> (
            match state.workflow_id with
            | None -> true
            | Some expected -> String.equal expected workflow_id)
        | None, None, _ -> true
      in
      (* Selects only the second cache fixture run for its independent initial
         completion barrier. A missing target or workflow ID fails closed and
         cannot publish a misleading marker. *)
      let matches_second_target (info : Workflow_adapter.activation_info) =
        match (state.second_target_workflow_id, info.workflow_id) with
        | Some expected, Some actual -> String.equal expected actual
        | _ -> false
      in
      (* Binds the exact target identity before a later RemoveFromCache
         activation omits InitializeWorkflow metadata. A replayed activation
         can be the first delivery this worker observes after a workflow-task
         timeout, so the cache-fixture replay exemption must retain identity
         even though it intentionally skips the restart diagnostic record. *)
      let remember_target_identity (info : Workflow_adapter.activation_info) =
        (match (state.workflow_id, info.workflow_id) with
        | None, Some workflow_id -> state.workflow_id <- Some workflow_id
        | Some expected, Some actual when not (String.equal expected actual) ->
            failwith "replay activation workflow ID changed"
        | _ -> ());
        match state.run_id with
        | None -> state.run_id <- Some info.run_id
        | Some expected when not (String.equal expected info.run_id) ->
            failwith "replay activation run ID changed"
        | Some _ -> ()
      in
      let callback (info : Workflow_adapter.activation_info) =
        if matches_target info then
          begin match (state.run_id, info.run_id) with
          | Some expected, actual when not (String.equal expected actual) -> ()
          | _ ->
              if Option.is_none info.cache_removal_reason then
                (* The one-slot cache fixture can legitimately receive a
                   replaying normal activation both after CacheFull eviction
                   and before cache pressure when Temporal redelivers an
                   unacknowledged workflow task. Restart diagnostics use
                   generation one to reject unexpected replay, but the cache
                   fixture is identified by its otherwise absent marker path
                   and must not turn either valid delivery into workflow
                   failure. Its dedicated post-acknowledgement markers, not
                   this restart record, provide the acceptance evidence. *)
                let cache_fixture_replay =
                  state.generation = 1 && info.is_replaying
                  && Option.is_some state.cache_eviction_path
                in
                if cache_fixture_replay then begin
                  if info.history_length < 0L then
                    failwith "replay diagnostics history length was negative";
                  remember_target_identity info
                end
                else
                  let phase =
                    if info.is_replaying then "replay" else "initial"
                  in
                  let already_recorded =
                    List.exists
                      (fun record -> String.equal record.phase phase)
                      state.records
                  in
                  if not already_recorded then begin
                    if info.history_length < 0L then
                      failwith "replay diagnostics history length was negative";
                    if state.generation = 1 && info.is_replaying then
                      failwith "generation one unexpectedly reported replay";
                    if state.generation > 1 && not info.is_replaying then
                      failwith "replacement worker did not report replay";
                    remember_target_identity info;
                    state.records <-
                      state.records
                      @ [
                          {
                            phase;
                            generation = state.generation;
                            is_replaying = info.is_replaying;
                            history_length = info.history_length;
                          };
                        ];
                    write_replay_diagnostics state
                  end
          end
      in
      let completion_callback (info : Workflow_adapter.activation_info) =
        (* Keep the isolated cache fixture diagnosable without exposing this
           test-only observer through the public API.  In particular, Core may
           deliver the eviction activation without workflow metadata, so the
           run ID and reason are the useful facts when the marker is absent. *)
        if Option.is_some state.cache_eviction_path then
          Printf.eprintf
            "cache observer completion run_id=%s workflow_id=%s reason=%s\n%!"
            info.run_id
            (Option.value info.workflow_id ~default:"<none>")
            (Option.value info.cache_removal_reason ~default:"<none>");
        if matches_target info then
          match info.cache_removal_reason with
          | Some "cache_full" ->
              (* Publish only the cache-pressure event under test. Core can
                 later remove the same run because its execution ended; that
                 lifecycle event must not overwrite the acknowledged
                 CacheFull evidence before the post-driver validator reads it. *)
              write_cache_eviction_marker state ~reason:"cache_full"
          | Some _ -> ()
          | None -> (
              (* This barrier proves only that Core acknowledged a normal
                 activation completion. A pre-pressure task redelivery is
                 replaying but is still a valid cached execution, so excluding
                 it can deadlock the client-only driver before run B starts. *)
              match state.cache_eviction_ready_path with
              | Some path -> write_cache_eviction_ready_marker path
              | None -> ())
        else if matches_second_target info then
          match info.cache_removal_reason with
          | None -> (
              match state.cache_eviction_second_ready_path with
              | Some path -> write_cache_eviction_ready_marker path
              | None -> ())
          | Some _ -> ()
      in
      Ok (Some callback, Some completion_callback)
  | Some _ ->
      Error
        (Base_error.defect
           ~message:
             "SMOKE_WORKER_REPLAY_DIAGNOSTICS_FILE must be a non-empty \
              absolute path without NUL")

(** The closed test-only configuration names for the parent/child replay
    observer. Generation one intentionally omits run IDs because Temporal has
    not created them before the worker starts processing the parent and child.
    Generation two requires both learned exact run IDs. *)
let parent_child_replay_environment_names =
  [
    "SMOKE_PARENT_CHILD_REPLAY_DIAGNOSTICS_FILE";
    "SMOKE_PARENT_CHILD_REPLAY_GENERATION";
    "SMOKE_PARENT_CHILD_REPLAY_PARENT_WORKFLOW_ID";
    "SMOKE_PARENT_CHILD_REPLAY_PARENT_RUN_ID";
    "SMOKE_PARENT_CHILD_REPLAY_CHILD_WORKFLOW_ID";
    "SMOKE_PARENT_CHILD_REPLAY_CHILD_RUN_ID";
  ]

(** The existing one-workflow observer has a different document shape. Mixing it
    with the fixed parent/child observer could let two callbacks overwrite or
    interpret one path differently, so the new mode rejects every related legacy
    setting before a native worker is created. *)
let parent_child_replay_legacy_environment_names =
  [
    "SMOKE_WORKER_REPLAY_DIAGNOSTICS_FILE";
    "SMOKE_WORKER_GENERATION";
    "SMOKE_REPLAY_WORKFLOW_ID";
    "SMOKE_WORKER_CACHE_EVICTION_FILE";
    "SMOKE_WORKER_CACHE_EVICTION_READY_FILE";
    "SMOKE_WORKER_CACHE_EVICTION_SECOND_READY_FILE";
    "SMOKE_CACHE_EVICTION_SECOND_WORKFLOW_ID";
  ]

(** Returns whether a named test-only setting was explicitly supplied, even if
    its value is empty. Empty values are invalid configuration rather than an
    invitation to silently fall back to another diagnostic mode. *)
let environment_is_set ~getenv name = Option.is_some (getenv name)

(** Reads one required parent/child setting. Its value is deliberately not
    included in the error because identifiers and paths should not leak into a
    public worker startup diagnostic. *)
let required_parent_child_replay_setting ~getenv name =
  match getenv name with
  | Some value -> Ok value
  | None ->
      Error
        (Base_error.defect
           ~message:("missing required parent/child replay setting " ^ name))

(** Validates the one file path used by the private parent/child observer. It
    must be a direct absolute filesystem path so its atomic same-directory
    replacement cannot accidentally target a relative working directory. *)
let parent_child_replay_path value =
  if
    value <> ""
    && (not (String.contains value '\000'))
    && not (Filename.is_relative value)
  then Ok value
  else
    Error
      (Base_error.defect
         ~message:
           "SMOKE_PARENT_CHILD_REPLAY_DIAGNOSTICS_FILE must be a non-empty \
            absolute path without NUL")

(** Parses the deliberately closed generation setting. Accepting only the two
    canonical decimal spellings makes a stale, hand-edited, or future mode fail
    before the worker starts rather than changing the diagnostic contract at
    runtime. *)
let parent_child_replay_generation = function
  | "1" -> Ok 1
  | "2" -> Ok 2
  | _ ->
      Error
        (Base_error.defect
           ~message:
             "SMOKE_PARENT_CHILD_REPLAY_GENERATION must be exactly 1 or 2")

(** Requires that a generation-one-only setting is absent rather than merely
    empty. A known run ID in generation one would be evidence from a different
    lifecycle than the worker about to create the parent and child executions.
*)
let require_parent_child_replay_absent ~getenv name =
  match getenv name with
  | None -> Ok ()
  | Some _ ->
      Error
        (Base_error.defect
           ~message:
             (name ^ " must be absent for parent/child replay generation one"))

(** Converts a strict JSON identity object to the pure state-machine input. The
    helper accepts no aliases or additional fields, so a persisted document
    cannot smuggle a role identity through an unvalidated key. *)
let decode_parent_child_replay_identity role_name = function
  | `Assoc fields ->
      let* fields = replay_object [ "workflow_id"; "run_id" ] fields in
      let* workflow_id = replay_field "workflow_id" fields in
      let* workflow_id =
        match workflow_id with
        | `String value -> Ok value
        | _ ->
            Error
              (Base_error.defect
                 ~message:
                   ("parent/child replay " ^ role_name
                  ^ " workflow ID must be a string"))
      in
      let* run_id = replay_field "run_id" fields in
      let* run_id =
        match run_id with
        | `String value -> Ok value
        | _ ->
            Error
              (Base_error.defect
                 ~message:
                   ("parent/child replay " ^ role_name
                  ^ " run ID must be a string"))
      in
      Ok ({ Role_checkpoint.workflow_id; run_id } : Role_checkpoint.identity)
  | _ ->
      Error
        (Base_error.defect
           ~message:
             ("parent/child replay " ^ role_name ^ " identity is not an object"))

(** Decodes one closed parent/child replay record. The pure state machine later
    validates the exact generation-one record sequence, while this layer rejects
    malformed JSON types and unknown role/phase spellings. *)
let decode_parent_child_replay_record = function
  | `Assoc fields ->
      let* fields =
        replay_object
          [ "role"; "phase"; "generation"; "is_replaying"; "history_length" ]
          fields
      in
      let* role = replay_field "role" fields in
      let* role =
        match role with
        | `String value ->
            Role_checkpoint.role_of_string value
            |> Result.map_error (fun error ->
                Base_error.defect ~message:error.Role_checkpoint.message)
        | _ ->
            Error
              (Base_error.defect
                 ~message:"parent/child replay record role must be a string")
      in
      let* phase = replay_field "phase" fields in
      let* phase =
        match phase with
        | `String value ->
            Role_checkpoint.phase_of_string value
            |> Result.map_error (fun error ->
                Base_error.defect ~message:error.Role_checkpoint.message)
        | _ ->
            Error
              (Base_error.defect
                 ~message:"parent/child replay record phase must be a string")
      in
      let* generation = replay_field "generation" fields in
      let* generation =
        match generation with
        | `Int ((1 | 2) as value) -> Ok value
        | _ ->
            Error
              (Base_error.defect
                 ~message:
                   "parent/child replay record generation must be exactly 1 or \
                    2")
      in
      let* is_replaying = replay_field "is_replaying" fields in
      let* is_replaying =
        match is_replaying with
        | `Bool value -> Ok value
        | _ ->
            Error
              (Base_error.defect
                 ~message:
                   "parent/child replay record is_replaying must be a boolean")
      in
      let* history_length = replay_field "history_length" fields in
      let* history_length = replay_history_length history_length in
      Ok
        ({
           Role_checkpoint.role;
           phase;
           generation;
           is_replaying;
           history_length;
         }
          : Role_checkpoint.record)
  | _ ->
      Error
        (Base_error.defect
           ~message:"parent/child replay record is not an object")

(** Loads one bounded parent/child document from the prior generation. The
    parser rejects a partial, legacy, or mixed-shape document before passing its
    typed values to the pure generation-two validation. *)
let load_parent_child_replay_document path =
  let* json = read_replay_json path in
  match json with
  | `Assoc fields ->
      let* fields = replay_object [ "parent"; "child"; "records" ] fields in
      let* parent = replay_field "parent" fields in
      let* parent = decode_parent_child_replay_identity "parent" parent in
      let* child = replay_field "child" fields in
      let* child = decode_parent_child_replay_identity "child" child in
      let* records = replay_field "records" fields in
      let* records =
        match records with
        | `List values when List.length values <= 4 ->
            let rec decode reversed = function
              | [] -> Ok (List.rev reversed)
              | value :: rest ->
                  let* record = decode_parent_child_replay_record value in
                  decode (record :: reversed) rest
            in
            decode [] values
        | `List _ ->
            Error
              (Base_error.defect
                 ~message:
                   "parent/child replay records exceed the fixed checkpoint \
                    bound")
        | _ ->
            Error
              (Base_error.defect
                 ~message:"parent/child replay records must be an array")
      in
      Ok ({ Role_checkpoint.parent; child; records } : Role_checkpoint.document)
  | _ ->
      Error
        (Base_error.defect
           ~message:"parent/child replay diagnostic root is not an object")

(** Converts one closed state-machine record to the payload-free JSON contract.
    History lengths remain decimal strings so no JSON consumer can round a
    Temporal 64-bit value through a floating-point number. *)
let parent_child_replay_record_json (record : Role_checkpoint.record) =
  `Assoc
    [
      ("role", `String (Role_checkpoint.role_name record.role));
      ("phase", `String (Role_checkpoint.phase_name record.phase));
      ("generation", `Int record.generation);
      ("is_replaying", `Bool record.is_replaying);
      ("history_length", `String (Int64.to_string record.history_length));
    ]

(** Converts one exact role identity to the closed JSON object used by the
    parent/child diagnostic. *)
let parent_child_replay_identity_json (identity : Role_checkpoint.identity) =
  `Assoc
    [
      ("workflow_id", `String identity.workflow_id);
      ("run_id", `String identity.run_id);
    ]

(** Builds the complete canonical JSON document. Callers receive this only after
    the pure state machine has observed every required role checkpoint, so no
    file can expose a one-role or mixed-generation document. *)
let parent_child_replay_document_json (document : Role_checkpoint.document) =
  `Assoc
    [
      ("parent", parent_child_replay_identity_json document.parent);
      ("child", parent_child_replay_identity_json document.child);
      ( "records",
        `List (List.map parent_child_replay_record_json document.records) );
    ]

(** Atomically publishes one complete parent/child checkpoint by flushing a
    same-directory temporary file and replacing the destination. Readers see
    either the old complete document or the new complete document, never a
    partially encoded transition. Like the existing single-run diagnostic, this
    is an atomic-visibility guarantee rather than an fsync durability guarantee.
*)
let write_parent_child_replay_document_atomically path document =
  let temporary = ref None in
  try
    let encoded =
      Yojson.Safe.to_string (parent_child_replay_document_json document) ^ "\n"
    in
    (* Generation two reads this same private artifact through a 64 KiB bound.
       Enforce that bound before creating a temporary file so generation one
       can never publish a checkpoint its replacement cannot read. *)
    if String.length encoded > 65_536 then
      failwith
        "parent/child replay diagnostics exceed the 65536-byte file bound";
    let generated =
      Filename.temp_file ~temp_dir:(Filename.dirname path)
        (Filename.basename path ^ ".tmp.")
        ""
    in
    temporary := Some generated;
    let channel = open_out_bin generated in
    Fun.protect
      ~finally:(fun () -> close_out_noerr channel)
      (fun () ->
        output_string channel encoded;
        flush channel);
    Sys.rename generated path;
    temporary := None
  with exception_ ->
    Option.iter
      (fun generated -> try Sys.remove generated with _ -> ())
      !temporary;
    raise exception_

(** Turns a pure state-machine error into the private adapter failure that the
    existing [on_activation] callback converts to its normal typed worker
    result. The bounded message intentionally omits every configured identity.
*)
let raise_parent_child_replay_error (error : Role_checkpoint.error) =
  failwith
    ("parent/child replay diagnostics rejected activation ("
   ^ error.Role_checkpoint.code ^ "): " ^ error.Role_checkpoint.message)

(** Creates the optional fixed two-role observer. It is enabled only by its
    complete, generation-specific environment contract and is deliberately
    independent of the existing single-run observer/schema. The returned
    callback runs inside the serialized adapter poll transaction, not on a
    Rust/Tokio thread. *)
let parent_child_replay_diagnostic_hook ~getenv () =
  if not (List.exists (environment_is_set ~getenv) parent_child_replay_environment_names)
  then Ok None
  else if
    List.exists (environment_is_set ~getenv) parent_child_replay_legacy_environment_names
  then
    Error
      (Base_error.defect
         ~message:
           "parent/child replay diagnostics cannot be mixed with legacy \
            single-run replay settings")
  else
    let* configured_path =
      required_parent_child_replay_setting ~getenv
        "SMOKE_PARENT_CHILD_REPLAY_DIAGNOSTICS_FILE"
    in
    let* path = parent_child_replay_path configured_path in
    let* configured_generation =
      required_parent_child_replay_setting ~getenv
        "SMOKE_PARENT_CHILD_REPLAY_GENERATION"
    in
    let* generation = parent_child_replay_generation configured_generation in
    let* parent_workflow_id =
      required_parent_child_replay_setting ~getenv
        "SMOKE_PARENT_CHILD_REPLAY_PARENT_WORKFLOW_ID"
    in
    let* child_workflow_id =
      required_parent_child_replay_setting ~getenv
        "SMOKE_PARENT_CHILD_REPLAY_CHILD_WORKFLOW_ID"
    in
    let* parent_run_id, child_run_id, previous =
      match generation with
      | 1 ->
          let* () =
            require_parent_child_replay_absent ~getenv
              "SMOKE_PARENT_CHILD_REPLAY_PARENT_RUN_ID"
          in
          let* () =
            require_parent_child_replay_absent ~getenv
              "SMOKE_PARENT_CHILD_REPLAY_CHILD_RUN_ID"
          in
          Ok (None, None, None)
      | 2 ->
          let* parent_run_id =
            required_parent_child_replay_setting ~getenv
              "SMOKE_PARENT_CHILD_REPLAY_PARENT_RUN_ID"
          in
          let* child_run_id =
            required_parent_child_replay_setting ~getenv
              "SMOKE_PARENT_CHILD_REPLAY_CHILD_RUN_ID"
          in
          let* previous = load_parent_child_replay_document path in
          Ok (Some parent_run_id, Some child_run_id, Some previous)
      | _ ->
          Error
            (Base_error.defect
               ~message:
                 "parent/child replay diagnostics selected an unsupported \
                  generation")
    in
    let parent : Role_checkpoint.role_configuration =
      { workflow_id = parent_workflow_id; run_id = parent_run_id }
    in
    let child : Role_checkpoint.role_configuration =
      { workflow_id = child_workflow_id; run_id = child_run_id }
    in
    let* initial_state =
      Role_checkpoint.create ~generation ~parent ~child ~previous
      |> Result.map_error (fun error ->
          Base_error.defect
            ~message:
              ("invalid parent/child replay diagnostics configuration ("
             ^ error.Role_checkpoint.code ^ "): "
             ^ error.Role_checkpoint.message))
    in
    let state = ref initial_state in
    let callback (info : Workflow_adapter.activation_info) =
      (* Cache-removal jobs are not workflow replay checkpoints. They may omit
         initialization metadata and have a distinct empty-completion contract,
         so the parent/child recovery observer leaves them to the dedicated
         cache-eviction diagnostic rather than treating them as a role phase. *)
      if Option.is_none info.cache_removal_reason then
        let activation : Role_checkpoint.activation =
          {
            workflow_id = info.workflow_id;
            run_id = info.run_id;
            is_replaying = info.is_replaying;
            history_length = info.history_length;
          }
        in
        match Role_checkpoint.observe !state activation with
        | Error error -> raise_parent_child_replay_error error
        | Ok Role_checkpoint.Ignored | Ok Role_checkpoint.Duplicate -> ()
        | Ok (Role_checkpoint.Accepted next) ->
            (* No document is publishable until both roles have been observed.
               This private update cannot leak a partial checkpoint. *)
            state := next
        | Ok (Role_checkpoint.Checkpoint { state = next; document }) ->
            (* Build/publish before commit: a failed write leaves [state] at
               the preceding valid transition, so a later redelivery cannot
               claim an unpersisted role checkpoint. *)
            write_parent_child_replay_document_atomically path document;
            state := next
    in
    Ok (Some callback)

(** Combines two private activation observers without changing the workflow
    adapter API. The existing single-run hook retains its historical behavior;
    the parent/child mode rejects mixed configuration before this helper is
    reached. *)
let combine_activation_hooks first second =
  match (first, second) with
  | None, None -> None
  | Some callback, None | None, Some callback -> Some callback
  | Some first, Some second ->
      Some
        (fun info ->
          first info;
          second info)

(** Allocates one fixture's observer state without starting a worker. Supplying
    [getenv] lets unit tests exercise the complete environment contract without
    changing process-global environment state. *)
let create ?(getenv = Sys.getenv_opt) () =
  let* single_run_on_activation, on_completion = replay_diagnostic_hook ~getenv () in
  let* parent_child_on_activation = parent_child_replay_diagnostic_hook ~getenv () in
  let on_activation =
    combine_activation_hooks single_run_on_activation parent_child_on_activation
  in
  Ok { Observer.on_activation; on_completion }

(** Installs fresh callbacks only while this Domain constructs one worker.
    Configuration failures precede native allocation; typed failures and raised
    exceptions both restore the prior injection scope. After successful creation
    the adapter owns the callbacks for the worker's lifetime. *)
let with_worker create_worker =
  let* callbacks =
    create ()
    |> Result.map_error (fun error ->
        Temporal.Error.defect ~message:(Base_error.message error))
  in
  Observer.with_callbacks callbacks create_worker

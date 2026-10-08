(** Replays corpus entries through the public [Temporal.Replay] API and
    renders the per-case table and machine-readable report (issue #524).

    The runner deliberately uses the same entry point applications use for
    their own pre-deployment replay checks, so an SDK or Temporal Core upgrade
    is judged by the behavior users observe rather than by a private path.
    Every entry is replayed in its own isolated native graph (that is what
    [Temporal.Replay.replay] guarantees), so one failing or hanging case
    cannot influence another. Nothing here reads a clock for verdicts, writes
    to the corpus, or regenerates a history: the corpus is read-only
    evidence. *)

(** {1 Outcomes} *)

(** What one replay produced, in the vocabulary of the manifest's [expected]
    member plus the public API's failure kinds. *)
type outcome =
  | Replays_ok  (** [Temporal.Replay.replay] returned [Ok ()]. *)
  | Nondeterminism  (** Core reported a command mismatch. *)
  | Workflow_task_failed  (** Workflow code could not complete a task. *)
  | Invalid_history  (** Core or the bridge rejected the history itself. *)
  | Unsupported_history  (** Valid history using an unsupported feature. *)
  | Replay_error  (** The replay could not run; the verdict is unknown. *)
  | Not_run
      (** The runner could not build the replay input (an unknown definition
          set, an unregistered workflow type, or an unreadable history), so no
          replay was attempted. *)

(** Stable lowercase label used in the table and in the JSON report. The two
    labels shared with the manifest are spelled exactly as its [expected]
    values. *)
let outcome_label = function
  | Replays_ok -> "replays_ok"
  | Nondeterminism -> "nondeterminism"
  | Workflow_task_failed -> "workflow_task_failed"
  | Invalid_history -> "invalid_history"
  | Unsupported_history -> "unsupported_history"
  | Replay_error -> "replay_error"
  | Not_run -> "not_run"

(** Converts a manifest expectation into the matching outcome. *)
let expected_outcome = function
  | Corpus_manifest.Replays_ok -> Replays_ok
  | Corpus_manifest.Nondeterminism -> Nondeterminism

(** The result of one corpus entry. *)
type case = {
  entry : Corpus_manifest.entry;
  capture : Corpus_manifest.capture option;
      (** Provenance of the history; [None] only for a manifest that failed
          validation, which never reaches replay. *)
  actual : outcome;
  passed : bool;
      (** [actual] equals the expectation and every identity check held. *)
  message : string option;
      (** Bounded diagnostic: the public failure message, or why a
          matching outcome was still rejected. [None] for a clean pass. *)
}

(** {1 Candidate identity} *)

(** Identifies the SDK and Core build being qualified. *)
type candidate = {
  sdk_commit : string;
      (** Commit of the checked-out SDK, supplied by the caller because the
          runner may execute without Git (in Docker or a Dune sandbox);
          ["unknown"] when not supplied. *)
  core_revision : string;
      (** Temporal Core revision read from [rust/Cargo.lock], or
          ["unknown"]. *)
  core_source : string;  (** Where [core_revision] came from. *)
  ocaml_version : string;
  bridge_abi : string;
      (** Native bridge ABI version reported by the linked library, or the
          error text when it is not the version this package expects. *)
}

(** Extracts the Temporal Core revision from a Cargo lockfile: the commit after
    [#] in the [source] of the [temporalio-sdk-core] package. Core is pinned by
    an immutable Git revision (see docs/dependencies.md), so this is the exact
    Core under test. Returns [None] when the package or its Git source is
    absent. *)
let core_revision_of_cargo_lock text =
  let lines = String.split_on_char '\n' text |> List.map String.trim in
  let rec find_package = function
    | [] -> None
    | "name = \"temporalio-sdk-core\"" :: rest -> find_source rest
    | _ :: rest -> find_package rest
  (* The package's [source] follows its [version] within the same
     [[package]] table; a blank line ends the table. *)
  and find_source = function
    | [] | "" :: _ -> None
    | line :: rest -> (
        match String.index_opt line '#' with
        | Some hash when String.starts_with ~prefix:"source = \"git+" line ->
            let revision =
              String.sub line (hash + 1) (String.length line - hash - 1)
            in
            Some
              (if String.ends_with ~suffix:"\"" revision then
                 String.sub revision 0 (String.length revision - 1)
               else revision)
        | _ -> find_source rest)
  in
  find_package lines

(** Builds the candidate identity. [cargo_lock] is a path to the lockfile;
    an unreadable or unrecognized lockfile yields ["unknown"] rather than an
    exception so the report is still written. *)
let candidate ~sdk_commit ~cargo_lock =
  let core_revision, core_source =
    match cargo_lock with
    | None -> ("unknown", "not supplied")
    | Some path -> (
        match In_channel.with_open_bin path In_channel.input_all with
        | exception Sys_error message -> ("unknown", message)
        | text -> (
            match core_revision_of_cargo_lock text with
            | Some revision -> (revision, "rust/Cargo.lock temporalio-sdk-core")
            | None -> ("unknown", path ^ " has no temporalio-sdk-core Git source")))
  in
  let bridge_abi =
    match Temporal.Runtime_info.native_bridge_abi_version () with
    | Ok version -> Int32.to_string version
    | Error error -> "error: " ^ Temporal.Error.message error
  in
  {
    sdk_commit = Option.value sdk_commit ~default:"unknown";
    core_revision;
    core_source;
    ocaml_version = Sys.ocaml_version;
    bridge_abi;
  }

(** {1 Replay} *)

(** Public registrations for one entry: only the workflow of the entry's
    recorded type, taken from its frozen definition set. Registering that one
    type means a history recorded for any other type cannot replay
    successfully; together with the manifest's JSON identity check this keeps
    a mislabelled entry from passing. *)
let registrations (entry : Corpus_manifest.entry) =
  match Corpus_definitions.definition_set entry.replay_definitions with
  | None -> Error ("unknown replay_definitions " ^ entry.replay_definitions)
  | Some workflows -> (
      match
        List.filter_map
          (fun (Corpus_definitions.Workflow { definition; signals; queries; updates })
             ->
            if Temporal.Workflow.name definition = entry.workflow_type then
              Some (Temporal.Replay.workflow ~signals ~queries ~updates definition)
            else None)
          workflows
      with
      | [] ->
          Error
            (Printf.sprintf "definition set %s does not register %s"
               entry.replay_definitions entry.workflow_type)
      | registrations -> Ok registrations)

(** Classifies a public replay failure. *)
let failure_outcome = function
  | Temporal.Replay.Nondeterminism _ -> Nondeterminism
  | Temporal.Replay.Workflow_task_failed _ -> Workflow_task_failed
  | Temporal.Replay.Invalid_history _ -> Invalid_history
  | Temporal.Replay.Unsupported_history _ -> Unsupported_history
  | Temporal.Replay.Replay_error _ -> Replay_error

(** Replays one entry from [root], the corpus directory, and compares the
    outcome with the manifest. A nondeterminism verdict must also name the
    entry's run ID, so a negative control cannot pass on the wrong run. *)
let run_entry ~root ~captures (entry : Corpus_manifest.entry) =
  let capture =
    List.find_opt
      (fun (capture : Corpus_manifest.capture) ->
        capture.capture_id = entry.capture)
      captures
  in
  let expected = expected_outcome entry.expected in
  let finish actual message =
    let passed = actual = expected && Option.is_none message in
    let message =
      match (passed, message) with
      | false, None ->
          Some
            (Printf.sprintf "expected %s, got %s" (outcome_label expected)
               (outcome_label actual))
      | _ -> message
    in
    { entry; capture; actual; passed; message }
  in
  let history =
    match
      Corpus_manifest.read_file (Filename.concat root entry.history.protobuf)
    with
    | exception Sys_error message -> Error message
    | bytes ->
        Temporal.Replay.History.of_protobuf ~workflow_id:entry.workflow_id bytes
        |> Result.map_error Temporal.Error.message
  in
  match (history, registrations entry) with
  | Error message, _ | _, Error message -> finish Not_run (Some message)
  | Ok history, Ok workflows -> (
      match Temporal.Replay.replay ~workflows history with
      | Ok () -> finish Replays_ok None
      | Error (Temporal.Replay.Nondeterminism { run_id; _ } as failure)
        when run_id <> entry.run_id && expected = Nondeterminism ->
          finish Nondeterminism
            (Some
               (Printf.sprintf "nondeterminism reported for run %s, manifest says \
                                %s: %s"
                  run_id entry.run_id
                  (Temporal.Replay.failure_message failure)))
      | Error failure ->
          let actual = failure_outcome failure in
          let message = Temporal.Replay.failure_message failure in
          (* A matching negative control keeps Core's mismatch text for the
             report but is still a pass. *)
          if actual = expected then
            { entry; capture; actual; passed = true; message = Some message }
          else
            finish actual
              (Some
                 (Printf.sprintf "expected %s, got %s" (outcome_label expected)
                    message)))

(** Replays every entry in manifest order. *)
let run ~root (manifest : Corpus_manifest.t) =
  List.map (run_entry ~root ~captures:manifest.captures) manifest.entries

(** IDs of the cases whose outcome did not match, in manifest order. *)
let failing_ids cases =
  List.filter_map (fun case -> if case.passed then None else Some case.entry.id) cases

(** {1 Output} *)

(** First twelve characters of a revision, enough to identify it in a table. *)
let short revision =
  if String.length revision > 12 then String.sub revision 0 12 else revision

(** Prints the per-case table to standard output. [CORE] is the Core revision
    that produced each history, so a cross-version replay is visible next to
    the candidate's revision in the heading. Diagnostics for failing cases
    follow the table so the rows stay aligned. *)
let print_table candidate cases =
  Printf.printf "history corpus: %d case(s); candidate SDK %s, Core %s, OCaml %s\n"
    (List.length cases) (short candidate.sdk_commit)
    (short candidate.core_revision) candidate.ocaml_version;
  let width =
    List.fold_left
      (fun width case -> max width (String.length case.entry.id))
      4 cases
  in
  let row id produced expected actual result =
    Printf.printf "  %-*s  %-12s  %-14s  %-20s  %s\n" width id produced expected
      actual result
  in
  (* A manifest that failed validation replays nothing; its problems are
     reported by the caller, so an empty table is omitted. *)
  if cases <> [] then row "CASE" "CORE" "EXPECTED" "ACTUAL" "RESULT";
  List.iter
    (fun case ->
      row case.entry.id
        (match case.capture with
        | Some capture -> short capture.core_revision
        | None -> "?")
        (outcome_label (expected_outcome case.entry.expected))
        (outcome_label case.actual)
        (if case.passed then "pass" else "FAIL"))
    cases;
  List.iter
    (fun case ->
      if not case.passed then
        Printf.printf "  %s: %s\n" case.entry.id
          (Option.value case.message ~default:"mismatch"))
    cases;
  flush stdout

(** Schema identifier of the JSON report. A future incompatible report shape
    must use a new identifier. *)
let report_schema = "ocaml-temporal/history-corpus-report/v1"

(** What a passing run does and does not establish. Printed by the runner and
    embedded in the report so an upgrade PR that cites either carries the
    limitation with it: the candidate replaying histories written by older
    builds is forward compatibility only. Rolling back needs the previous
    build to replay histories the candidate wrote, which this corpus does not
    test (production rollback qualification is issue #508). *)
let scope_note =
  "forward compatibility only: the candidate replays histories recorded by \
   older SDK/Core builds; this does not show that the previous release can \
   replay histories written by the candidate, so it is not rollback evidence"

(** Encodes an optional string as JSON [null] or a string. *)
let json_option = function None -> `Null | Some value -> `String value

(** Builds the machine-readable report. [problems] are manifest or coverage
    failures not tied to a single replayed case; any problem or failing case
    makes the status ["fail"]. *)
let report ~manifest_path ~candidate ~problems cases =
  let failing = failing_ids cases in
  let passed = List.length cases - List.length failing in
  `Assoc
    [
      ("schema", `String report_schema);
      ("scope", `String scope_note);
      ( "status",
        `String (if failing = [] && problems = [] then "pass" else "fail") );
      ("manifest", `String manifest_path);
      ( "candidate",
        `Assoc
          [
            ("sdk_commit", `String candidate.sdk_commit);
            ("core_revision", `String candidate.core_revision);
            ("core_source", `String candidate.core_source);
            ("ocaml_version", `String candidate.ocaml_version);
            ("native_bridge_abi", `String candidate.bridge_abi);
          ] );
      ( "summary",
        `Assoc
          [
            ("cases", `Int (List.length cases));
            ("passed", `Int passed);
            ("failed", `Int (List.length failing));
            ("problems", `Int (List.length problems));
          ] );
      ("failing_cases", `List (List.map (fun id -> `String id) failing));
      ("problems", `List (List.map (fun problem -> `String problem) problems));
      ( "cases",
        `List
          (List.map
             (fun case ->
               let entry = case.entry in
               `Assoc
                 [
                   ("id", `String entry.id);
                   ("workflow_type", `String entry.workflow_type);
                   ("workflow_id", `String entry.workflow_id);
                   ("run_id", `String entry.run_id);
                   ("history", `String entry.history.protobuf);
                   ("replay_definitions", `String entry.replay_definitions);
                   ( "expected",
                     `String (outcome_label (expected_outcome entry.expected)) );
                   ("actual", `String (outcome_label case.actual));
                   ("result", `String (if case.passed then "pass" else "fail"));
                   ("message", json_option case.message);
                   ( "produced_by",
                     match case.capture with
                     | None -> `Null
                     | Some capture ->
                         `Assoc
                           [
                             ("capture", `String capture.capture_id);
                             ("kind", `String capture.kind);
                             ("sdk_commit", `String capture.sdk_commit);
                             ("core_revision", `String capture.core_revision);
                           ] );
                 ])
             cases) );
    ]

(** Writes [json] to [path], creating missing parent directories, through a
    temporary file renamed into place so a reader never sees a partial
    report. *)
let write_report path json =
  let rec make_directory directory =
    if directory <> "" && directory <> "." && not (Sys.file_exists directory)
    then (
      make_directory (Filename.dirname directory);
      Sys.mkdir directory 0o755)
  in
  make_directory (Filename.dirname path);
  let temporary = path ^ ".tmp" in
  Out_channel.with_open_text temporary (fun channel ->
      Yojson.Safe.pretty_to_channel channel json;
      Out_channel.output_char channel '\n');
  Sys.rename temporary path

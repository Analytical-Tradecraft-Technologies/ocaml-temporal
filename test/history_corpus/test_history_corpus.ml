(** Docker-free gate for the replay history corpus (issue #518).

    1. Validates [manifest.json] against the v1 schema, every checksum, every
       cross reference, and the absence of orphaned history files.
    2. Requires the corpus to cover the feature set promised in
       [docs/reference/history-corpus.md], including at least one negative
       control.
    3. Replays every entry through the private Core replay path against its
       named frozen definition set and requires the manifest's expected
       verdict. A [replays_ok] entry must also report the recorded run ID and
       workflow type, so a mislabelled history cannot pass.

    The corpus is read-only here: nothing is regenerated or rewritten. Every
    failure names the entry ID so a broken compatibility case is identifiable
    from CI output alone. *)

(** Feature tags that must each have at least one [replays_ok] entry. Removing
    a tag from this list is a deliberate corpus-scope change and must be
    reflected in docs/reference/history-corpus.md. *)
let required_features =
  [
    "activity"; "timer"; "activity-retry"; "signal"; "update"; "query";
    "child-workflow"; "continue-as-new"; "patch-marker-free"; "patch-active";
    "patch-deprecated"; "workflow-failure"; "workflow-task-failure-recovery";
  ]

(** Checks the coverage contract and returns its failures. *)
let coverage_failures (manifest : Corpus_manifest.t) =
  let ok_features =
    List.concat_map
      (fun (entry : Corpus_manifest.entry) ->
        if entry.expected = Replays_ok then entry.features else [])
      manifest.entries
  in
  let missing =
    List.filter (fun feature -> not (List.mem feature ok_features)) required_features
    |> List.map (fun feature -> "no replays_ok entry covers feature " ^ feature)
  in
  if
    List.exists
      (fun (entry : Corpus_manifest.entry) -> entry.expected = Nondeterminism)
      manifest.entries
  then missing
  else missing @ [ "the corpus has no nondeterminism negative control" ]

(** Replays one entry and returns [None] on the expected verdict or a
    diagnostic naming the entry. *)
let check_entry ~root (entry : Corpus_manifest.entry) =
  match Corpus_definitions.definition_set entry.replay_definitions with
  | None -> Some ("unknown replay_definitions " ^ entry.replay_definitions)
  | Some workflows -> (
      let protobuf =
        Corpus_manifest.read_file (Filename.concat root entry.history.protobuf)
      in
      let verdict =
        Corpus_replay.replay ~workflow_id:entry.workflow_id ~workflows protobuf
      in
      Printf.printf "  %-48s %s\n%!" entry.id (Corpus_replay.describe verdict);
      match (entry.expected, verdict) with
      | Replays_ok, Replayed { run_id; workflow_type; _ } ->
          if run_id <> entry.run_id then
            Some (Printf.sprintf "replayed run %s, manifest says %s" run_id entry.run_id)
          else if workflow_type <> entry.workflow_type then
            Some
              (Printf.sprintf "replayed type %s, manifest says %s" workflow_type
                 entry.workflow_type)
          else None
      | Nondeterminism, Nondeterministic _ -> None
      | Replays_ok, _ -> Some ("expected replays_ok, got " ^ Corpus_replay.describe verdict)
      | Nondeterminism, _ ->
          Some ("expected nondeterminism, got " ^ Corpus_replay.describe verdict))

(** Entry point. Exits non-zero after reporting every failure. *)
let () =
  let manifest_path =
    match Sys.argv with
    | [| _; path |] -> path
    | _ -> failwith "usage: test_history_corpus.exe MANIFEST"
  in
  Corpus_sha256.self_test ();
  let manifest =
    match Corpus_manifest.load manifest_path with
    | Ok manifest -> manifest
    | Error errors ->
        List.iter (prerr_endline) errors;
        Printf.eprintf "FAIL history corpus manifest: %d problem(s)\n"
          (List.length errors);
        exit 1
  in
  let failures = coverage_failures manifest in
  let root = Filename.dirname manifest_path in
  Printf.printf "history corpus: replaying %d entries\n%!"
    (List.length manifest.entries);
  let failures =
    failures
    @ List.filter_map
        (fun (entry : Corpus_manifest.entry) ->
          Option.map
            (fun message -> entry.id ^ ": " ^ message)
            (check_entry ~root entry))
        manifest.entries
  in
  match failures with
  | [] ->
      Printf.printf "PASS history corpus: %d entries, %d captures\n%!"
        (List.length manifest.entries)
        (List.length manifest.captures)
  | failures ->
      List.iter prerr_endline failures;
      Printf.eprintf "FAIL history corpus: %d failure(s)\n" (List.length failures);
      exit 1

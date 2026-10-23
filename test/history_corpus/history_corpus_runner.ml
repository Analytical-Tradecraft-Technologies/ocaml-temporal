(** Replay history corpus gate and SDK/Core upgrade runner (issues #518 and
    #524).

    {v
    history_corpus_runner.exe MANIFEST [--report FILE] [--cargo-lock FILE]
                                       [--sdk-commit SHA] [--coverage-doc FILE]
    v}

    1. Validates [MANIFEST] against the v1 schema, every checksum, every cross
       reference, each JSON history's recorded workflow type and run ID, and
       the absence of orphaned history files.
    2. Checks the feature coverage table ({!Corpus_coverage}): every
       supported feature has a corpus case of the required verdict (a
       replayed history, a nondeterminism control or a malformed-history
       control) or an explicit not-applicable or not-yet-captured reason,
       every manifest feature tag is classified, and, with [--coverage-doc],
       the table in [docs/reference/history-corpus.md] matches.
    3. Replays every entry through the public [Temporal.Replay] API against
       its named frozen definition set and requires the manifest's expected
       outcome.
    4. Prints a per-case table, optionally writes the JSON report described in
       the reference document, and exits 1 listing every failing case ID
       (2 for a usage error).

    The same executable is the Dune test (so [dune runtest], [make test] and
    the native jobs run it) and the explicit upgrade command
    [make test-history-corpus-upgrade], which adds the report and candidate
    identifiers. The corpus is read-only here: nothing is regenerated or
    rewritten. *)

(** Command-line options. *)
type options = {
  manifest_path : string;
  report_path : string option;
  cargo_lock : string option;
  sdk_commit : string option;
  coverage_doc : string option;
      (** The corpus reference document whose feature coverage table must
          match {!Corpus_coverage.table}. The Dune test passes it; the
          upgrade command does not need it. *)
}

(** Prints usage and exits 2. *)
let usage () =
  prerr_endline
    "usage: history_corpus_runner.exe MANIFEST [--report FILE] [--cargo-lock \
     FILE] [--sdk-commit SHA] [--coverage-doc FILE]";
  exit 2

(** Parses [Sys.argv]. An empty [--sdk-commit] (for example from a Make
    variable outside a Git checkout) is treated as absent. *)
let parse_options () =
  let rec parse options manifest = function
    | [] -> (
        match manifest with
        | Some manifest_path -> { options with manifest_path }
        | None -> usage ())
    | "--report" :: path :: rest ->
        parse { options with report_path = Some path } manifest rest
    | "--cargo-lock" :: path :: rest ->
        parse { options with cargo_lock = Some path } manifest rest
    | "--sdk-commit" :: commit :: rest ->
        let sdk_commit = if commit = "" then None else Some commit in
        parse { options with sdk_commit } manifest rest
    | "--coverage-doc" :: path :: rest ->
        parse { options with coverage_doc = Some path } manifest rest
    | argument :: rest
      when manifest = None && not (String.starts_with ~prefix:"--" argument) ->
        parse options (Some argument) rest
    | _ -> usage ()
  in
  parse
    {
      manifest_path = "";
      report_path = None;
      cargo_lock = None;
      sdk_commit = None;
      coverage_doc = None;
    }
    None
    (List.tl (Array.to_list Sys.argv))

(** Entry point. Every failure is reported before exiting non-zero. *)
let () =
  let options = parse_options () in
  Corpus_sha256.self_test ();
  let candidate =
    Corpus_runner.candidate ~sdk_commit:options.sdk_commit
      ~cargo_lock:options.cargo_lock
  in
  let document_problems =
    match options.coverage_doc with
    | None -> []
    | Some path -> Corpus_coverage.check_document path
  in
  let problems, entries, cases =
    match Corpus_manifest.load options.manifest_path with
    | Error errors -> (errors @ document_problems, [], [])
    | Ok manifest ->
        let root = Filename.dirname options.manifest_path in
        ( Corpus_coverage.check manifest.entries @ document_problems,
          manifest.entries,
          Corpus_runner.run ~root manifest )
  in
  Corpus_runner.print_table candidate cases;
  Printf.printf "history corpus: %s\n%!" Corpus_runner.scope_note;
  Option.iter
    (fun path ->
      Corpus_runner.write_report path
        (Corpus_runner.report ~manifest_path:options.manifest_path ~candidate
           ~problems
           ~feature_coverage:(Corpus_coverage.report_json entries)
           cases);
      Printf.printf "history corpus: report written to %s\n%!" path)
    options.report_path;
  match (problems, Corpus_runner.failing_ids cases) with
  | [], [] -> Printf.printf "PASS history corpus: %d case(s)\n%!" (List.length cases)
  | problems, failing ->
      List.iter prerr_endline problems;
      if failing <> [] then
        Printf.eprintf "FAIL history corpus: %d mismatched case(s): %s\n"
          (List.length failing) (String.concat ", " failing);
      if problems <> [] then
        Printf.eprintf "FAIL history corpus: %d manifest or coverage problem(s)\n"
          (List.length problems);
      exit 1

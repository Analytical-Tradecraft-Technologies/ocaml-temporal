(** Negative paths of the history corpus runner (issue #524).

    Each scenario copies the committed corpus into a temporary directory,
    deliberately breaks the copy, runs the real [history_corpus_runner.exe]
    against it, and checks the exit status, output and JSON report. The
    committed corpus is never modified.

    {b Mismatched outcomes.} Two expectations are broken, covering both
    directions of an upgrade regression:
    - [compat-patch-active-on-deprecated] is pointed at the pre-patch
      definitions, so a history expected to replay is now nondeterministic
      (what an incompatible SDK/Core change looks like);
    - [negative-timer-removed] is pointed at the compatible definitions, so a
      negative control expected to fail now replays cleanly (what a gate that
      silently stopped detecting nondeterminism looks like).
    The runner must exit 1, list exactly those case IDs on standard error and
    in the report's [failing_cases], and still pass every untouched case.

    {b Swapped protobuf.} [live-2026-10-08-activity]'s protobuf is replaced by
    another valid history (the timer run), its checksum is updated, and its
    JSON copy is dropped. Because the public replay API reports no identity on
    success, the manifest must reject the entry from the protobuf bytes alone:
    the runner must exit 1 with the entry's workflow type and run ID
    mismatches as report problems, and replay nothing. *)

(** The two mutated entries, in manifest order, with their new definition
    sets. *)
let mutations =
  [
    ("compat-patch-active-on-deprecated", "corpus-v1-patch-legacy");
    ("negative-timer-removed", "corpus-v1");
  ]

(** A syntactically valid commit passed as the candidate SDK identifier. *)
let sdk_commit = "0123456789abcdef0123456789abcdef01234567"

(** Fails the test with a message. *)
let fail format = Printf.ksprintf failwith format

(** Copies one file byte for byte. *)
let copy_file source destination =
  let contents = In_channel.with_open_bin source In_channel.input_all in
  Out_channel.with_open_bin destination (fun channel ->
      Out_channel.output_string channel contents)

(** Removes a directory tree created by this test. *)
let rec remove_tree path =
  if Sys.is_directory path then (
    Array.iter (fun name -> remove_tree (Filename.concat path name)) (Sys.readdir path);
    Sys.rmdir path)
  else Sys.remove path

(** Applies [edit] to the members of each entry named in [ids], failing if an
    entry is missing so a scenario cannot silently stop exercising its
    negative path. *)
let edit_entries ids edit manifest =
  let applied = ref [] in
  let entry = function
    | `Assoc members as json -> (
        match List.assoc_opt "id" members with
        | Some (`String id) when List.mem id ids ->
            applied := id :: !applied;
            `Assoc (edit id members)
        | _ -> json)
    | json -> json
  in
  let edited =
    match manifest with
    | `Assoc members ->
        `Assoc
          (List.map
             (function
               | "entries", `List entries -> ("entries", `List (List.map entry entries))
               | member -> member)
             members)
    | _ -> fail "manifest is not an object"
  in
  List.iter
    (fun id -> if not (List.mem id !applied) then fail "manifest has no entry %s" id)
    ids;
  edited

(** Points each {!mutations} entry at its new definition set. *)
let mutate_outcomes manifest =
  edit_entries (List.map fst mutations)
    (fun id members ->
      List.map
        (function
          | "replay_definitions", _ ->
              ("replay_definitions", `String (List.assoc id mutations))
          | member -> member)
        members)
    manifest

(** Entry whose protobuf the swap scenario replaces. *)
let swapped_entry = "live-2026-10-08-activity"

(** Entry whose valid history replaces {!swapped_entry}'s protobuf. *)
let donor_entry = "live-2026-10-08-timer"

(** Returns a string member of the [history] object of entry [id]. *)
let history_member manifest id name =
  let open Yojson.Safe.Util in
  manifest |> member "entries" |> to_list
  |> List.find (fun entry -> entry |> member "id" |> to_string = id)
  |> member "history" |> member name |> to_string

(** Swap scenario: overwrites {!swapped_entry}'s protobuf in the copy at
    [directory] with {!donor_entry}'s bytes, updates its checksum so only the
    identity differs, and removes its JSON copy and reference so no JSON check
    can catch the swap. *)
let swap_protobuf ~directory manifest =
  let path name = Filename.concat directory (history_member manifest name "protobuf") in
  copy_file (path donor_entry) (path swapped_entry);
  Sys.remove (Filename.concat directory (history_member manifest swapped_entry "json"));
  let donor_digest = history_member manifest donor_entry "protobuf_sha256" in
  edit_entries [ swapped_entry ]
    (fun _ members ->
      List.map
        (function
          | "history", `Assoc history ->
              ( "history",
                `Assoc
                  (List.filter_map
                     (function
                       | ("json" | "json_sha256"), _ -> None
                       | "protobuf_sha256", _ ->
                           Some ("protobuf_sha256", `String donor_digest)
                       | member -> Some member)
                     history) )
          | member -> member)
        members)
    manifest

(** Runs [program] with [arguments], redirecting standard output and error to
    files in [directory], and returns the exit code with both outputs. *)
let run_capturing ~directory program arguments =
  let stdout_path = Filename.concat directory "runner.stdout" in
  let stderr_path = Filename.concat directory "runner.stderr" in
  let open_output path =
    Unix.openfile path [ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC ] 0o644
  in
  let out = open_output stdout_path in
  let err = open_output stderr_path in
  let pid =
    Fun.protect
      ~finally:(fun () ->
        Unix.close out;
        Unix.close err)
      (fun () ->
        Unix.create_process program
          (Array.of_list (program :: arguments))
          Unix.stdin out err)
  in
  let status =
    match snd (Unix.waitpid [] pid) with
    | Unix.WEXITED code -> code
    | Unix.WSIGNALED signal | Unix.WSTOPPED signal ->
        fail "runner stopped by signal %d" signal
  in
  let read path = In_channel.with_open_bin path In_channel.input_all in
  (status, read stdout_path, read stderr_path)

(** Returns whether [text] contains [fragment]. *)
let contains text fragment =
  let length = String.length fragment in
  let rec scan index =
    index + length <= String.length text
    && (String.sub text index length = fragment || scan (index + 1))
  in
  scan 0

(** Reads a string at a member [path] of [json]. *)
let string_at names json =
  let open Yojson.Safe.Util in
  List.fold_left (fun json name -> member name json) json names |> to_string

(** Checks the runner's exit code, output and report for the mismatched
    outcomes scenario. *)
let check_outcomes ~status ~stdout ~stderr report =
  let open Yojson.Safe.Util in
  let ids = List.map fst mutations in
  if status <> 1 then fail "runner exited %d, expected 1\n%s\n%s" status stdout stderr;
  let summary =
    Printf.sprintf "FAIL history corpus: 2 mismatched case(s): %s"
      (String.concat ", " ids)
  in
  if not (contains stderr summary) then
    fail "stderr does not list the failing IDs:\n%s" stderr;
  List.iter
    (fun id ->
      if not (contains stdout (id ^ ": expected")) then
        fail "stdout has no diagnostic for %s:\n%s" id stdout)
    ids;
  if string_at [ "status" ] report <> "fail" then fail "report status is not fail";
  if
    List.map to_string (report |> member "failing_cases" |> to_list) <> ids
  then fail "report failing_cases differ from %s" (String.concat ", " ids);
  if report |> member "problems" |> to_list <> [] then
    fail "the mutation must not introduce manifest problems";
  if string_at [ "candidate"; "sdk_commit" ] report <> sdk_commit then
    fail "report does not carry the supplied SDK commit";
  let core = string_at [ "candidate"; "core_revision" ] report in
  if String.length core <> 40 then
    fail "report core_revision %S was not read from Cargo.lock" core;
  let cases = report |> member "cases" |> to_list in
  List.iter
    (fun case ->
      let id = string_at [ "id" ] case in
      let result = string_at [ "result" ] case in
      let actual = string_at [ "actual" ] case in
      match id with
      | "compat-patch-active-on-deprecated" ->
          if result <> "fail" || actual <> "nondeterminism" then
            fail "%s: result %s actual %s" id result actual
      | "negative-timer-removed" ->
          if result <> "fail" || actual <> "replays_ok" then
            fail "%s: result %s actual %s" id result actual
      | _ ->
          if result <> "pass" then fail "untouched case %s did not pass" id;
          if string_at [ "produced_by"; "core_revision" ] case |> String.length <> 40
          then fail "case %s has no producing Core revision" id)
    cases

(** Checks the swap scenario: validation fails on the swapped entry's
    workflow type and run ID, read from its protobuf, and nothing replays. *)
let check_swap ~status ~stdout ~stderr ~expected_problems report =
  let open Yojson.Safe.Util in
  if status <> 1 then fail "runner exited %d, expected 1\n%s\n%s" status stdout stderr;
  List.iter
    (fun problem ->
      if not (contains stderr problem) then
        fail "stderr does not report %S:\n%s" problem stderr)
    expected_problems;
  if string_at [ "status" ] report <> "fail" then fail "report status is not fail";
  if List.map to_string (report |> member "problems" |> to_list) <> expected_problems
  then fail "report problems are not exactly the swapped identity:\n%s" stderr;
  if report |> member "cases" |> to_list <> [] then
    fail "an invalid manifest must not replay any case"

(** Copies the corpus next to [manifest_path] into a fresh temporary
    directory, lets [prepare] edit the copy and return the manifest to write,
    runs [runner] on it, and passes the outcome and report to [check]. The
    directory is removed on every path. *)
let run_scenario ~runner ~manifest_path ~cargo_lock ~prepare check =
  let directory = Filename.temp_dir "history-corpus-mismatch" "" in
  Fun.protect
    ~finally:(fun () -> remove_tree directory)
    (fun () ->
      let source_histories =
        Filename.concat (Filename.dirname manifest_path) "histories"
      in
      let histories = Filename.concat directory "histories" in
      Sys.mkdir histories 0o755;
      Array.iter
        (fun name ->
          copy_file
            (Filename.concat source_histories name)
            (Filename.concat histories name))
        (Sys.readdir source_histories);
      let manifest = Filename.concat directory "manifest.json" in
      Yojson.Safe.to_file manifest
        (prepare ~directory (Yojson.Safe.from_file manifest_path));
      let report_path = Filename.concat directory "report/report.json" in
      let status, stdout, stderr =
        run_capturing ~directory runner
          [
            manifest; "--report"; report_path; "--cargo-lock"; cargo_lock;
            "--sdk-commit"; sdk_commit;
          ]
      in
      check ~status ~stdout ~stderr (Yojson.Safe.from_file report_path))

(** Entry point: [test_history_corpus_mismatch.exe RUNNER MANIFEST CARGO_LOCK]. *)
let () =
  let runner, manifest_path, cargo_lock =
    match Sys.argv with
    | [| _; runner; manifest; cargo_lock |] -> (runner, manifest, cargo_lock)
    | _ -> fail "usage: test_history_corpus_mismatch.exe RUNNER MANIFEST CARGO_LOCK"
  in
  (* Dune passes paths relative to the test's directory; the runner is
     started by path, so make it explicit. *)
  let runner =
    if Filename.is_relative runner then Filename.concat (Sys.getcwd ()) runner
    else runner
  in
  run_scenario ~runner ~manifest_path ~cargo_lock
    ~prepare:(fun ~directory:_ manifest -> mutate_outcomes manifest)
    check_outcomes;
  print_endline "PASS history corpus runner reports mismatched case IDs and exits 1";
  let original = Yojson.Safe.from_file manifest_path in
  let entry_field id name =
    let open Yojson.Safe.Util in
    original |> member "entries" |> to_list
    |> List.find (fun entry -> entry |> member "id" |> to_string = id)
    |> member name |> to_string
  in
  let expected_problems =
    [
      Printf.sprintf
        "entry %s: protobuf history workflow type is %s, manifest says %s"
        swapped_entry (entry_field donor_entry "workflow_type")
        (entry_field swapped_entry "workflow_type");
      Printf.sprintf "entry %s: protobuf history run ID is %s, manifest says %s"
        swapped_entry (entry_field donor_entry "run_id")
        (entry_field swapped_entry "run_id");
    ]
  in
  run_scenario ~runner ~manifest_path ~cargo_lock
    ~prepare:(fun ~directory manifest -> swap_protobuf ~directory manifest)
    (check_swap ~expected_problems);
  print_endline
    "PASS history corpus runner rejects a swapped protobuf without a JSON copy"

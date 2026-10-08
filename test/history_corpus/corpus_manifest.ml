(** Strict reader and validator for the replay history corpus manifest.

    The manifest format is documented by
    [docs/schemas/history-corpus/manifest.schema.json] and
    [docs/reference/history-corpus.md]. This module is the executable form of
    that schema for the Docker-free test: every object is closed (unknown
    members are rejected), every required field is checked, and file-level
    invariants that JSON Schema cannot express (checksums, references between
    entries and captures, orphaned files) are checked against the corpus
    directory. All failures are collected and reported together so one run
    shows every problem in a broken corpus change. *)

(** The only manifest format this test understands. A future incompatible
    format must use a new identifier rather than reinterpret this one. *)
let schema_id = "ocaml-temporal/history-corpus/v1"

(** Replay verdicts a corpus entry may require. *)
type expected =
  | Replays_ok
      (** Core accepted every command and the run's terminal command, and the
          replay worker finalized naturally. *)
  | Nondeterminism
      (** Core evicted the run with a nondeterminism reason: the registered
          definitions are incompatible with the history. *)

(** Paths and checksums of one history. [protobuf] is the replay input;
    [json] is the optional human-readable source it was encoded from. *)
type history = {
  protobuf : string;
  protobuf_sha256 : string;
  json : (string * string) option;  (** Path and SHA-256. *)
}

(** One replay case: a history, the definition set to replay it against, and
    the verdict that set must produce. *)
type entry = {
  id : string;
  purpose : string;
  features : string list;
  workflow_type : string;
  workflow_id : string;
  run_id : string;
  capture : string;
  history : history;
  replay_definitions : string;
  expected : expected;
  negative_control_of : string option;
      (** For a [Nondeterminism] entry, the [Replays_ok] entry whose history
          it reuses. *)
}

(** Validated manifest. Capture provenance is validated but only its IDs are
    needed after loading. *)
type t = { captures : string list; entries : entry list }

(** Accumulates validation failures with a stable location prefix. *)
type errors = string list ref

(** Records one validation failure. *)
let error (errors : errors) location message =
  errors := Printf.sprintf "%s: %s" location message :: !errors

(** Lowercase 40-hex Git object ID. *)
let is_commit value =
  String.length value = 40
  && String.for_all (function '0' .. '9' | 'a' .. 'f' -> true | _ -> false) value

(** Lowercase 64-hex SHA-256 digest. *)
let is_sha256 value =
  String.length value = 64
  && String.for_all (function '0' .. '9' | 'a' .. 'f' -> true | _ -> false) value

(** Stable identifier syntax for entries, captures and feature tags. *)
let is_identifier value =
  value <> ""
  && String.for_all
       (function 'a' .. 'z' | '0' .. '9' | '-' -> true | _ -> false)
       value
  && value.[0] <> '-'

(** ISO calendar date [YYYY-MM-DD]. *)
let is_date value =
  String.length value = 10
  && String.for_all
       (fun c -> c = '-' || (c >= '0' && c <= '9'))
       value
  && value.[4] = '-' && value.[7] = '-'

(** A history path is relative, inside [histories/], and cannot escape it. *)
let is_history_path value =
  String.starts_with ~prefix:"histories/" value
  && (not (String.contains value '\\'))
  && (not (String.contains value '\000'))
  && not
       (List.exists
          (fun part -> part = ".." || part = "")
          (String.split_on_char '/' value))

(** Rejects members outside a closed object's documented set. *)
let check_members errors location allowed members =
  List.iter
    (fun (name, _) ->
      if not (List.mem name allowed) then
        error errors location (Printf.sprintf "unknown member %S" name))
    members

(** Reads a required non-empty string member. *)
let string_member errors location members name =
  match List.assoc_opt name members with
  | Some (`String value) when value <> "" -> value
  | Some _ ->
      error errors location (name ^ " must be a non-empty string");
      ""
  | None ->
      error errors location ("missing " ^ name);
      ""

(** Reads a required non-empty list of strings. *)
let string_list_member errors location members name =
  match List.assoc_opt name members with
  | Some (`List (_ :: _ as values)) ->
      List.filter_map
        (function
          | `String value when value <> "" -> Some value
          | _ ->
              error errors location (name ^ " must contain non-empty strings");
              None)
        values
  | _ ->
      error errors location (name ^ " must be a non-empty string list");
      []

(** Validates one capture provenance record. Live captures must name the
    Temporal Server image; synthetic histories record ["none"]. *)
let check_capture errors id json =
  let location = "captures." ^ id in
  if not (is_identifier id) then error errors location "invalid capture ID";
  match json with
  | `Assoc members ->
      check_members errors location
        [
          "kind"; "captured_on"; "sdk_commit"; "sdk_tree_dirty"; "core_revision";
          "temporal_server_image"; "temporal_cli_image"; "worker_toolchains";
          "capture_command"; "protobuf_export"; "source"; "notes";
        ]
        members;
      let kind = string_member errors location members "kind" in
      if not (List.mem kind [ "live"; "synthetic" ]) then
        error errors location "kind must be live or synthetic";
      if not (is_date (string_member errors location members "captured_on"))
      then error errors location "captured_on must be YYYY-MM-DD";
      if not (is_commit (string_member errors location members "sdk_commit"))
      then error errors location "sdk_commit must be a 40-hex commit";
      if not (is_commit (string_member errors location members "core_revision"))
      then error errors location "core_revision must be a 40-hex commit";
      let server = string_member errors location members "temporal_server_image" in
      if kind = "live" && not (String.contains server '@') then
        error errors location "a live capture must pin the server image by digest";
      ignore (string_member errors location members "capture_command" : string);
      (match List.assoc_opt "sdk_tree_dirty" members with
      | None | Some (`Bool _) -> ()
      | Some _ -> error errors location "sdk_tree_dirty must be a boolean");
      (match List.assoc_opt "protobuf_export" members with
      | None | Some (`Assoc _) -> ()
      | Some _ -> error errors location "protobuf_export must be an object");
      List.iter
        (fun name ->
          match List.assoc_opt name members with
          | None | Some (`String _) -> ()
          | Some _ -> error errors location (name ^ " must be a string"))
        [ "temporal_cli_image"; "source"; "notes" ]
  | _ -> error errors location "capture must be an object"

(** Validates and decodes one history reference. *)
let check_history errors location json =
  let location = location ^ ".history" in
  match json with
  | Some (`Assoc members) ->
      check_members errors location
        [ "protobuf"; "protobuf_sha256"; "json"; "json_sha256" ]
        members;
      let path name =
        let value = string_member errors location members name in
        if not (is_history_path value) then
          error errors location (name ^ " must be a path under histories/");
        value
      in
      let digest name =
        let value = string_member errors location members name in
        if not (is_sha256 value) then
          error errors location (name ^ " must be a lowercase SHA-256");
        value
      in
      let protobuf = path "protobuf" in
      let protobuf_sha256 = digest "protobuf_sha256" in
      let json =
        match (List.mem_assoc "json" members, List.mem_assoc "json_sha256" members) with
        | false, false -> None
        | true, true -> Some (path "json", digest "json_sha256")
        | _ ->
            error errors location "json and json_sha256 must appear together";
            None
      in
      { protobuf; protobuf_sha256; json }
  | _ ->
      error errors location "history must be an object";
      { protobuf = ""; protobuf_sha256 = ""; json = None }

(** Validates and decodes one entry. Cross-entry checks happen later. *)
let check_entry errors index json =
  let location = Printf.sprintf "entries[%d]" index in
  match json with
  | `Assoc members ->
      check_members errors location
        [
          "id"; "purpose"; "features"; "workflow_type"; "workflow_id"; "run_id";
          "capture"; "generation"; "history"; "replay_definitions"; "expected";
          "negative_control_of"; "notes";
        ]
        members;
      let id = string_member errors location members "id" in
      let location = if id = "" then location else location ^ " (" ^ id ^ ")" in
      if not (is_identifier id) then error errors location "invalid entry ID";
      let features = string_list_member errors location members "features" in
      List.iter
        (fun feature ->
          if not (is_identifier feature) then
            error errors location ("invalid feature tag " ^ feature))
        features;
      let expected =
        match string_member errors location members "expected" with
        | "replays_ok" -> Replays_ok
        | "nondeterminism" -> Nondeterminism
        | other ->
            error errors location ("unknown expected outcome " ^ other);
            Replays_ok
      in
      (* A negative control states which entry's history it reuses, so a
         reviewer can see that the expected failure is deliberate. *)
      (match (expected, List.assoc_opt "negative_control_of" members) with
      | Nondeterminism, Some (`String _) | Replays_ok, None -> ()
      | Nondeterminism, _ ->
          error errors location "a nondeterminism entry needs negative_control_of"
      | Replays_ok, Some _ ->
          error errors location "only a negative control has negative_control_of");
      List.iter
        (fun name ->
          match List.assoc_opt name members with
          | None | Some (`String _) -> ()
          | Some _ -> error errors location (name ^ " must be a string"))
        [ "generation"; "notes"; "negative_control_of" ];
      Some
        {
          id;
          purpose = string_member errors location members "purpose";
          features;
          workflow_type = string_member errors location members "workflow_type";
          workflow_id = string_member errors location members "workflow_id";
          run_id = string_member errors location members "run_id";
          capture = string_member errors location members "capture";
          history = check_history errors location (List.assoc_opt "history" members);
          replay_definitions =
            string_member errors location members "replay_definitions";
          expected;
          negative_control_of =
            (match List.assoc_opt "negative_control_of" members with
            | Some (`String value) -> Some value
            | _ -> None);
        }
  | _ ->
      error errors location "entry must be an object";
      None

(** Reads a whole file as a string. *)
let read_file path =
  In_channel.with_open_bin path In_channel.input_all

(** Checks checksums, cross references and orphaned files against [root], the
    corpus directory containing [manifest.json] and [histories/]. *)
let check_files errors ~root ~captures entries =
  let referenced = Hashtbl.create 32 in
  let digests = Hashtbl.create 32 in
  let check_file location path expected_digest =
    Hashtbl.replace referenced path ();
    (* Two entries naming one file (a negative control and its source) must
       agree on its checksum. *)
    (match Hashtbl.find_opt digests path with
    | Some digest when digest <> expected_digest ->
        error errors location (path ^ " has conflicting checksums")
    | _ -> Hashtbl.replace digests path expected_digest);
    let file = Filename.concat root path in
    if not (Sys.file_exists file) then error errors location ("missing " ^ path)
    else
      let actual = Corpus_sha256.digest_hex (read_file file) in
      if actual <> expected_digest then
        error errors location
          (Printf.sprintf "%s SHA-256 is %s, manifest says %s" path actual
             expected_digest)
  in
  let ids = Hashtbl.create 32 in
  List.iter
    (fun entry ->
      let location = "entry " ^ entry.id in
      if Hashtbl.mem ids entry.id then error errors location "duplicate entry ID";
      Hashtbl.replace ids entry.id ();
      if not (List.mem entry.capture captures) then
        error errors location ("unknown capture " ^ entry.capture);
      check_file location entry.history.protobuf entry.history.protobuf_sha256;
      Option.iter
        (fun (path, digest) -> check_file location path digest)
        entry.history.json)
    entries;
  List.iter
    (fun capture ->
      if not (List.exists (fun entry -> entry.capture = capture) entries) then
        error errors ("captures." ^ capture) "capture is not used by any entry")
    captures;
  let directory = Filename.concat root "histories" in
  Array.iter
    (fun name ->
      let path = "histories/" ^ name in
      if not (Hashtbl.mem referenced path) then
        error errors path "orphaned file is not referenced by the manifest")
    (Sys.readdir directory)

(** Loads and validates [manifest_path]. Returns every failure on error. *)
let load manifest_path =
  let errors = ref [] in
  let root = Filename.dirname manifest_path in
  let manifest =
    match Yojson.Safe.from_file manifest_path with
    | json -> Some json
    | exception Yojson.Json_error message ->
        error errors manifest_path ("invalid JSON: " ^ message);
        None
  in
  let result =
    match manifest with
    | Some (`Assoc members) ->
        check_members errors "manifest" [ "schema"; "captures"; "entries" ] members;
        if string_member errors "manifest" members "schema" <> schema_id then
          error errors "manifest" ("schema must be " ^ schema_id);
        let captures =
          match List.assoc_opt "captures" members with
          | Some (`Assoc captures) ->
              List.iter (fun (id, json) -> check_capture errors id json) captures;
              List.map fst captures
          | _ ->
              error errors "manifest" "captures must be an object";
              []
        in
        let entries =
          match List.assoc_opt "entries" members with
          | Some (`List (_ :: _ as entries)) ->
              List.filter_map Fun.id (List.mapi (check_entry errors) entries)
          | _ ->
              error errors "manifest" "entries must be a non-empty list";
              []
        in
        (* Negative controls must point at an existing replays_ok entry with
           the same history, so the expected failure is attributable to the
           definitions rather than to a different or corrupt history. *)
        List.iter
          (fun entry ->
            match entry.negative_control_of with
            | None -> ()
            | Some source -> (
                match List.find_opt (fun other -> other.id = source) entries with
                | Some other
                  when other.expected = Replays_ok
                       && other.history.protobuf = entry.history.protobuf ->
                    ()
                | _ ->
                    error errors ("entry " ^ entry.id)
                      ("negative_control_of must name a replays_ok entry with \
                        the same history: " ^ source)))
          entries;
        check_files errors ~root ~captures entries;
        { captures; entries }
    | Some _ ->
        error errors "manifest" "manifest must be an object";
        { captures = []; entries = [] }
    | None -> { captures = []; entries = [] }
  in
  match !errors with [] -> Ok result | errors -> Error (List.rev errors)

(* The interface documents the contract; comments here explain the file
   layout and the steps that make a posting atomic. A ledger directory
   holds:
   - [record-<hex key>]: one committed record, published by a hard link and
     never rewritten;
   - [partial-<pid>-<sequence>]: a record being written; it becomes visible
     under its record name only through the link, and is removed afterwards;
   - [journal]: one line per request, appended after the request is
     answered. *)

type t = { directory : string }

type posting = { account : string; amount_cents : int }

type correlation = {
  workflow_id : string;
  run_id : string;
  activity_id : string;
  attempt : int;
  worker : string;
}

type record = { key : string; posting : posting; committed_by : correlation }

type outcome =
  | Committed of record
  | Duplicate of record
  | Conflict of record

type outcome_kind = Committed_kind | Duplicate_kind | Conflict_kind

type journal_entry = {
  journal_key : string;
  requested_by : correlation;
  answer : outcome_kind;
}

(** Longest accepted key, in bytes. Record file names spell the key in
    hexadecimal, so this keeps them well inside the common 255-byte file
    name limit. *)
let max_key_bytes = 120

(** Name of the journal file inside the ledger directory. *)
let journal_name = "journal"

(** Prefix of every published record file. *)
let record_prefix = "record-"

(** Converts a file-system exception into the [Error] the interface
    promises. Any other exception is a defect and propagates. *)
let protect description f =
  match f () with
  | value -> Ok value
  | exception Unix.Unix_error (error, call, argument) ->
      Error
        (Printf.sprintf "%s: %s(%s): %s" description call argument
           (Unix.error_message error))
  | exception Sys_error message -> Error (description ^ ": " ^ message)

let open_ledger directory =
  protect ("opening ledger " ^ directory) (fun () ->
      (try Unix.mkdir directory 0o755
       with Unix.Unix_error (Unix.EEXIST, _, _) -> ());
      { directory })

let directory ledger = ledger.directory

(** Spells [key] in lower-case hexadecimal, so any key is a safe and
    distinct file name. *)
let hex key =
  String.concat ""
    (List.map
       (fun character -> Printf.sprintf "%02x" (Char.code character))
       (List.of_seq (String.to_seq key)))

(** Path of the published record for [key]. *)
let record_path ledger key =
  Filename.concat ledger.directory (record_prefix ^ hex key)

(** Distinguishes the temporary files of concurrent requests made by one
    process on several Domains; the process ID distinguishes processes. *)
let partial_sequence = Atomic.make 0

(** A temporary record path that no other live request uses. *)
let partial_path ledger =
  Filename.concat ledger.directory
    (Printf.sprintf "partial-%d-%d" (Unix.getpid ())
       (Atomic.fetch_and_add partial_sequence 1))

(** Serializes a record as one field per line, with OCaml string-literal
    quoting so any key or label survives unchanged. *)
let string_of_record record =
  let { workflow_id; run_id; activity_id; attempt; worker } =
    record.committed_by
  in
  Printf.sprintf
    "key %S\naccount %S\namount_cents %d\nworkflow_id %S\nrun_id %S\nactivity_id %S\nattempt %d\nworker %S\n"
    record.key record.posting.account record.posting.amount_cents workflow_id
    run_id activity_id attempt worker

(** Parses {!string_of_record}'s format. A malformed file is reported as an
    [Error] naming [path]; it cannot be produced by {!post}, which publishes
    only completely written files. *)
let record_of_string path contents =
  match
    Scanf.sscanf contents
      " key %S account %S amount_cents %d workflow_id %S run_id %S activity_id \
       %S attempt %d worker %S %!"
      (fun key account amount_cents workflow_id run_id activity_id attempt
           worker ->
        {
          key;
          posting = { account; amount_cents };
          committed_by = { workflow_id; run_id; activity_id; attempt; worker };
        })
  with
  | record -> Ok record
  | exception (Scanf.Scan_failure _ | Failure _ | End_of_file) ->
      Error ("malformed ledger record " ^ path)

(** Reads and parses the record file at [path]. *)
let read_record path =
  Result.bind
    (protect ("reading " ^ path) (fun () ->
         In_channel.with_open_bin path In_channel.input_all))
    (record_of_string path)

(** Writes all of [contents] to [descriptor], looping over short writes. *)
let write_all descriptor contents =
  let bytes = Bytes.of_string contents in
  let rec loop offset =
    if offset < Bytes.length bytes then
      loop
        (offset
        + Unix.write descriptor bytes offset (Bytes.length bytes - offset))
  in
  loop 0

(** Creates [path], which must not exist, writes [contents] and syncs it, so
    the file is complete and durable before anything links to it. *)
let write_new_synced path contents =
  let descriptor =
    Unix.openfile path [ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_EXCL; Unix.O_CLOEXEC ] 0o644
  in
  Fun.protect
    ~finally:(fun () -> Unix.close descriptor)
    (fun () ->
      write_all descriptor contents;
      Unix.fsync descriptor)

(** Syncs the ledger directory so a new link survives a machine crash, not
    only a process crash. Some platforms (Windows, some network file
    systems) cannot open or sync a directory; the posting is then still
    atomic and visible to every process, only less durable, so the failure
    is ignored. *)
let sync_directory ledger =
  match Unix.openfile ledger.directory [ Unix.O_RDONLY; Unix.O_CLOEXEC ] 0 with
  | exception Unix.Unix_error _ -> ()
  | descriptor ->
      Fun.protect
        ~finally:(fun () -> Unix.close descriptor)
        (fun () -> try Unix.fsync descriptor with Unix.Unix_error _ -> ())

(** Removes a temporary file, ignoring a file that is already gone. *)
let remove_quietly path =
  try Unix.unlink path with Unix.Unix_error (Unix.ENOENT, _, _) -> ()

let string_of_outcome_kind = function
  | Committed_kind -> "committed"
  | Duplicate_kind -> "duplicate"
  | Conflict_kind -> "conflict"

(** Inverse of {!string_of_outcome_kind}. *)
let outcome_kind_of_string = function
  | "committed" -> Some Committed_kind
  | "duplicate" -> Some Duplicate_kind
  | "conflict" -> Some Conflict_kind
  | _ -> None

let outcome_record = function
  | Committed record | Duplicate record | Conflict record -> record

let outcome_kind = function
  | Committed _ -> Committed_kind
  | Duplicate _ -> Duplicate_kind
  | Conflict _ -> Conflict_kind

(** Appends one journal line in a single [write] and syncs it. Each line is
    written whole by one call on a file opened for appending, so lines from
    concurrent requests do not interleave on a local file system. *)
let append_journal ledger ~key correlation kind =
  let { workflow_id; run_id; activity_id; attempt; worker } = correlation in
  let line =
    Printf.sprintf "%S %S %S %S %d %S %s\n" key workflow_id run_id activity_id
      attempt worker
      (string_of_outcome_kind kind)
  in
  let descriptor =
    Unix.openfile
      (Filename.concat ledger.directory journal_name)
      [ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_APPEND; Unix.O_CLOEXEC ]
      0o644
  in
  Fun.protect
    ~finally:(fun () -> Unix.close descriptor)
    (fun () ->
      write_all descriptor line;
      Unix.fsync descriptor)

(** Validates a key before any file is touched. *)
let check_key key =
  if String.equal key "" then Error "an idempotency key must not be empty"
  else if String.length key > max_key_bytes then
    Error
      (Printf.sprintf "an idempotency key must be at most %d bytes"
         max_key_bytes)
  else Ok ()

let post ledger ~key posting correlation =
  let ( let* ) = Result.bind in
  let* () = check_key key in
  let candidate = { key; posting; committed_by = correlation } in
  let final = record_path ledger key in
  let partial = partial_path ledger in
  let* outcome =
    protect ("posting " ^ key) (fun () ->
        Fun.protect
          ~finally:(fun () -> remove_quietly partial)
          (fun () ->
            write_new_synced partial (string_of_record candidate);
            (* The atomic step: the link either publishes this complete
               record under the key's name or fails because a record for
               the key already exists. *)
            match Unix.link partial final with
            | () ->
                sync_directory ledger;
                `Won
            | exception Unix.Unix_error (Unix.EEXIST, _, _) -> `Lost))
  in
  let* outcome =
    match outcome with
    | `Won -> Ok (Committed candidate)
    | `Lost ->
        (* A published record is complete, because it was synced before it
           was linked, so reading it here never sees a partial write. *)
        let* existing = read_record final in
        if existing.posting = posting then Ok (Duplicate existing)
        else Ok (Conflict existing)
  in
  let* () =
    protect "appending to the ledger journal" (fun () ->
        append_journal ledger ~key correlation (outcome_kind outcome))
  in
  Ok outcome

let find ledger ~key =
  let ( let* ) = Result.bind in
  let* () = check_key key in
  let path = record_path ledger key in
  if Sys.file_exists path then Result.map Option.some (read_record path)
  else Ok None

let records ledger =
  let ( let* ) = Result.bind in
  let* names =
    protect ("listing " ^ ledger.directory) (fun () ->
        Sys.readdir ledger.directory)
  in
  let* records =
    Array.fold_left
      (fun accumulated name ->
        let* records = accumulated in
        if String.starts_with ~prefix:record_prefix name then
          let* record = read_record (Filename.concat ledger.directory name) in
          Ok (record :: records)
        else Ok records)
      (Ok []) names
  in
  Ok (List.sort (fun left right -> String.compare left.key right.key) records)

(** Parses one journal line written by {!append_journal}. *)
let journal_entry_of_line line =
  match
    Scanf.sscanf line " %S %S %S %S %d %S %s %!"
      (fun journal_key workflow_id run_id activity_id attempt worker answer ->
        ( journal_key,
          { workflow_id; run_id; activity_id; attempt; worker },
          answer ))
  with
  | journal_key, requested_by, answer -> (
      match outcome_kind_of_string answer with
      | Some answer -> Ok { journal_key; requested_by; answer }
      | None -> Error ("malformed ledger journal line: " ^ line))
  | exception (Scanf.Scan_failure _ | Failure _ | End_of_file) ->
      Error ("malformed ledger journal line: " ^ line)

let journal ledger =
  let ( let* ) = Result.bind in
  let path = Filename.concat ledger.directory journal_name in
  if not (Sys.file_exists path) then Ok []
  else
    let* contents =
      protect ("reading " ^ path) (fun () ->
          In_channel.with_open_bin path In_channel.input_all)
    in
    List.fold_right
      (fun line accumulated ->
        let* entries = accumulated in
        if String.equal (String.trim line) "" then Ok entries
        else
          let* entry = journal_entry_of_line line in
          Ok (entry :: entries))
      (String.split_on_char '\n' contents)
      (Ok [])

let describe_correlation { workflow_id; run_id; activity_id; attempt; worker }
    =
  Printf.sprintf "workflow=%s run=%s activity=%s attempt=%d worker=%s"
    workflow_id run_id activity_id attempt worker

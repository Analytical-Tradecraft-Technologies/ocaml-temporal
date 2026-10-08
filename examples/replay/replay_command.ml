(** A small, application-linked command line around [Temporal.Replay].

    Replay must run the application's own workflow code and codecs, so a
    replay checker cannot be a generic tool that loads arbitrary OCaml
    binaries: each application builds its own tiny executable that passes its
    workflow registrations to {!main}. This module is the part such an
    executable can copy unchanged. It parses the command line, reads each
    binary [temporal.api.history.v1.History] file, replays it offline,
    prints one result line per history, and returns a stable exit status.

    No Temporal Server, network connection, or credential is used. Every
    history replays in its own isolated native graph through
    {!Temporal.Replay.replay}, which blocks the calling thread, so this module
    must be called from ordinary program code, never from a workflow or
    activity. *)

(** Process exit statuses. Each history that does not replay cleanly maps to
    one of the non-zero values; the command exits with the status of the
    first such history in command-line order, after replaying and reporting
    every history. The values are part of the command's documented contract
    so CI scripts can branch on them. *)
module Exit_status = struct
  (** Every history replayed cleanly. *)
  let ok = 0

  (** The workflow code produced commands that differ from the recorded
      history: deploying it would break the recorded execution. *)
  let nondeterminism = 1

  (** Workflow code could not complete a replayed task: it raised, returned a
      defect, could not decode its input, or its type is not registered. *)
  let workflow_task_failed = 2

  (** The file is not a valid Temporal [History] protobuf, or it is empty or
      larger than {!Temporal.Replay.History.max_bytes}. *)
  let invalid_history = 3

  (** The history is valid but uses a feature this SDK cannot replay yet. *)
  let unsupported_history = 4

  (** The replay could not run (invalid registrations or options, or an SDK
      runtime failure); the history's verdict is unknown. *)
  let replay_error = 5

  (** The command line is malformed. Matches [EX_USAGE] from [sysexits.h]. *)
  let usage = 64

  (** A history file could not be opened or read. Matches [EX_NOINPUT]. *)
  let unreadable_input = 66
end

(** One history to replay: the file to read and the workflow ID it belongs
    to. The History protobuf does not carry the workflow ID, so the caller
    must supply it. *)
type input = { path : string; workflow_id : string }

(** A fully parsed command line. [namespace] and [task_queue] are only
    reported to workflow code through [Temporal.Workflow.info]; they never
    select a server. [inputs] keeps command-line order. *)
type options = { namespace : string; task_queue : string; inputs : input list }

(** The usage text printed for [--help]. *)
let usage program =
  String.concat "\n"
    [
      "usage: " ^ program
      ^ " [--namespace NAME] [--task-queue NAME] --workflow-id ID FILE...";
      "         [--workflow-id ID FILE...]";
      "";
      "Replays each binary Temporal History protobuf FILE offline against the";
      "workflows linked into this program. Each FILE uses the most recent";
      "--workflow-id before it.";
      "";
      "exit status: 0 every history replayed, 1 nondeterminism,";
      "2 workflow task failed, 3 invalid history, 4 unsupported history,";
      "5 replay could not run, 64 usage error, 66 unreadable history file";
      "";
    ]

(** The outcome of parsing a command line. [Help] is a request for the usage
    text rather than an error. *)
type parsed = Run of options | Help | Usage_error of string

(** Parses [arguments] (without the program name). Options accept both
    [--name VALUE] and [--name=VALUE]; [--] ends option parsing so a history
    path may begin with [-]. [--workflow-id] may be repeated, and each history
    path uses the most recent one, which lets one invocation check histories
    from several executions. *)
let parse arguments =
  (* Splits [--name=value] so both spellings share one code path. *)
  let split argument =
    match String.index_opt argument '=' with
    | Some index when String.starts_with ~prefix:"--" argument ->
        ( String.sub argument 0 index,
          Some
            (String.sub argument (index + 1)
               (String.length argument - index - 1)) )
    | _ -> (argument, None)
  in
  let rec loop ~namespace ~task_queue ~workflow_id ~options_done inputs =
    function
    | [] -> (
        match List.rev inputs with
        | [] -> Usage_error "at least one history FILE is required"
        | inputs -> Run { namespace; task_queue; inputs })
    | path :: rest
      when options_done
           || (not (String.starts_with ~prefix:"-" path))
           || String.equal path "-" -> (
        match workflow_id with
        | None ->
            Usage_error
              (Printf.sprintf "%s: no --workflow-id was given before it" path)
        | Some workflow_id ->
            loop ~namespace ~task_queue ~workflow_id:(Some workflow_id)
              ~options_done
              ({ path; workflow_id } :: inputs)
              rest)
    | "--" :: rest ->
        loop ~namespace ~task_queue ~workflow_id ~options_done:true inputs rest
    | ("-h" | "--help") :: _ -> Help
    | argument :: rest -> (
        let name, inline_value = split argument in
        (* Takes the option's value from [--name=value] or the next word. *)
        let value k =
          match (inline_value, rest) with
          | Some value, rest -> k value rest
          | None, value :: rest -> k value rest
          | None, [] -> Usage_error (name ^ " requires a value")
        in
        let non_empty value k =
          if String.equal value "" then
            Usage_error (name ^ " must not be empty")
          else k value
        in
        match name with
        | "--workflow-id" ->
            value (fun id rest ->
                non_empty id (fun id ->
                    loop ~namespace ~task_queue ~workflow_id:(Some id)
                      ~options_done inputs rest))
        | "--namespace" ->
            value (fun namespace rest ->
                non_empty namespace (fun namespace ->
                    loop ~namespace ~task_queue ~workflow_id ~options_done
                      inputs rest))
        | "--task-queue" ->
            value (fun task_queue rest ->
                non_empty task_queue (fun task_queue ->
                    loop ~namespace ~task_queue ~workflow_id ~options_done
                      inputs rest))
        | _ -> Usage_error ("unknown option " ^ name))
  in
  (* The defaults match [Temporal.Replay.replay]'s own defaults. *)
  loop ~namespace:"default" ~task_queue:"temporal-replay" ~workflow_id:None
    ~options_done:false [] arguments

(** Reads one history file. The size is checked before reading so an
    unexpectedly large file is rejected without loading it into memory.
    Returns [`Unreadable] for I/O failures and [`Invalid] for content that
    cannot be a replayable history. *)
let read_history path =
  match
    In_channel.with_open_bin path (fun channel ->
        let length = In_channel.length channel in
        if Int64.compare length (Int64.of_int Temporal.Replay.History.max_bytes)
           > 0
        then Error `Too_large
        else Ok (In_channel.input_all channel))
  with
  | Ok bytes -> Ok bytes
  | Error `Too_large ->
      Error
        ( `Invalid,
          Printf.sprintf "invalid history: larger than %d bytes"
            Temporal.Replay.History.max_bytes )
  | exception Sys_error message ->
      Error (`Unreadable, "cannot read history file: " ^ message)

(** Maps a replay verdict to its exit status. *)
let failure_status = function
  | Temporal.Replay.Nondeterminism _ -> Exit_status.nondeterminism
  | Workflow_task_failed _ -> Exit_status.workflow_task_failed
  | Invalid_history _ -> Exit_status.invalid_history
  | Unsupported_history _ -> Exit_status.unsupported_history
  | Replay_error _ -> Exit_status.replay_error

(** Replays one input and returns its exit status together with the
    diagnostic to print, or [None] when it replayed cleanly. *)
let check ~namespace ~task_queue ~workflows { path; workflow_id } =
  match read_history path with
  | Error (`Unreadable, message) ->
      (Exit_status.unreadable_input, Some message)
  | Error (`Invalid, message) -> (Exit_status.invalid_history, Some message)
  | Ok bytes -> (
      match Temporal.Replay.History.of_protobuf ~workflow_id bytes with
      | Error error ->
          ( Exit_status.invalid_history,
            Some ("invalid history: " ^ Temporal.Error.message error) )
      | Ok history -> (
          match
            Temporal.Replay.replay ~namespace ~task_queue ~workflows history
          with
          | Ok () -> (Exit_status.ok, None)
          | Error failure ->
              ( failure_status failure,
                Some (Temporal.Replay.failure_message failure) )))

(** Makes [value] safe to embed in one output line. File paths, workflow IDs
    and diagnostics come from the command line, the history, or workflow code
    and may contain line breaks; printed raw, they would split a record across
    lines or forge an extra [PASS]/[FAIL] record. A backslash becomes [\\],
    newline, carriage return and tab become [\n], [\r] and [\t], and every
    other ASCII control byte (including DEL) becomes [\xHH]. All other bytes,
    including UTF-8 text, are kept as they are so ordinary output stays
    readable. *)
let escape value =
  let needs_escape = function
    | '\\' | '\000' .. '\031' | '\127' -> true
    | _ -> false
  in
  if not (String.exists needs_escape value) then value
  else
    let buffer = Buffer.create (String.length value + 16) in
    String.iter
      (function
        | '\\' -> Buffer.add_string buffer "\\\\"
        | '\n' -> Buffer.add_string buffer "\\n"
        | '\r' -> Buffer.add_string buffer "\\r"
        | '\t' -> Buffer.add_string buffer "\\t"
        | ('\000' .. '\031' | '\127') as control ->
            Buffer.add_string buffer
              (Printf.sprintf "\\x%02x" (Char.code control))
        | character -> Buffer.add_char buffer character)
      value;
    Buffer.contents buffer

(** Replays every input in [options] in order, prints one line per history on
    standard output, and returns the process exit status. Lines have the
    stable form [PASS FILE workflow_id=ID] or
    [FAIL FILE workflow_id=ID: DIAGNOSTIC], where [DIAGNOSTIC] starts with the
    failure kind printed by {!Temporal.Replay.failure_message}. Every dynamic
    field is passed through {!escape}, so each record is exactly one line. A
    final summary line counts the results. Diagnostics never contain payload
    bytes, but they can name workflow, activity, and timer identifiers
    recorded in the history. *)
let run ~workflows { namespace; task_queue; inputs } =
  let results =
    List.map
      (fun input ->
        let status, diagnostic = check ~namespace ~task_queue ~workflows input in
        (match diagnostic with
        | None ->
            Printf.printf "PASS %s workflow_id=%s\n%!" (escape input.path)
              (escape input.workflow_id)
        | Some diagnostic ->
            Printf.printf "FAIL %s workflow_id=%s: %s\n%!" (escape input.path)
              (escape input.workflow_id) (escape diagnostic));
        status)
      inputs
  in
  let failed = List.length (List.filter (fun status -> status <> 0) results) in
  let total = List.length results in
  Printf.printf "replayed %d %s: %d passed, %d failed\n%!" total
    (if total = 1 then "history" else "histories")
    (total - failed) failed;
  (* The first failure in command-line order decides the exit status. *)
  Option.value ~default:Exit_status.ok
    (List.find_opt (fun status -> status <> Exit_status.ok) results)

(** Parses [arguments] (without the program name), runs the replay, and
    returns the exit status without exiting, so tests and larger programs can
    embed the command. Usage errors go to standard error. *)
let run_command ~program ~workflows arguments =
  match parse arguments with
  | Help ->
      print_string (usage program);
      Exit_status.ok
  | Usage_error message ->
      Printf.eprintf "%s: %s\nRun %s --help for usage.\n%!" (escape program)
        (escape message) (escape program);
      Exit_status.usage
  | Run options -> run ~workflows options

(** Entry point for an application's replay executable: runs the command over
    [Sys.argv] with [workflows] and exits with its status. [workflows] must be
    the same registrations, with the same signal, query, and update handlers,
    that the application's production worker uses. *)
let main ~workflows =
  let arguments = List.tl (Array.to_list Sys.argv) in
  let program = Filename.basename Sys.executable_name in
  exit (run_command ~program ~workflows arguments)

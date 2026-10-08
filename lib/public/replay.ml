(** Public offline replay over the private Core replay worker.

    This module adds no replay engine of its own. Temporal Core's replay
    worker turns a history into activations and checks the produced commands;
    the private supervisor owns that native graph on its own Domain; and the
    same workflow adapter used by [Temporal.Worker] executes the registered
    OCaml code. This module only selects the replay operations for the
    adapter's task source, observes the outcome, and guarantees that every
    native graph is released before a call returns. *)

module Supervisor = Temporal_sdk_kernel.Supervisor
module Bridge = Temporal_sdk_kernel.Bridge
module Protocol = Temporal_sdk_kernel.Workflow_protocol
module Adapter = Temporal_sdk_kernel.Native_worker_execution
module Control = Temporal_sdk_kernel.Control_protocol

(** Heterogeneous workflow registration, mirroring [Worker.registered_workflow]
    so the codecs stay paired with their implementation. *)
type registered_workflow =
  | Workflow :
      ('input, 'output) Workflow.t
      * Signal.Handler.t list
      * Query.Handler.t list
      * Update.Handler.t list
      -> registered_workflow

(** Packs a workflow definition and its handlers for {!replay}. *)
let workflow ?(signals = []) ?(queries = []) ?(updates = []) definition =
  Workflow (definition, signals, queries, updates)

(** Upper bound, in bytes, of a diagnostic copied into a public [failure].
    Core's mismatch descriptions are normally a few hundred bytes; the bound
    keeps an unexpectedly verbose message from flooding CI logs. *)
let max_message_bytes = 4_096

(** Truncates [value] to [max_message_bytes] on a UTF-8 character boundary so
    the shortened text remains valid UTF-8. *)
let bounded value =
  if String.length value <= max_message_bytes then value
  else
    let limit = max_message_bytes - 3 in
    (* Step back over UTF-8 continuation bytes (0b10xxxxxx) so the cut never
       splits a multi-byte character. *)
    let rec boundary index =
      if index > 0 && Char.code value.[index] land 0xC0 = 0x80 then
        boundary (index - 1)
      else index
    in
    String.sub value 0 (boundary limit) ^ "..."

module History = struct
  type t = { workflow_id : string; document : bytes }
  (** [document] is the private replay-history JSON envelope built once at
      construction; it never aliases caller-owned memory. *)

  let max_bytes = Control.max_payload_bytes

  (** Matches the native bridge's workflow-ID bound so an over-long ID fails
      here with a precise message instead of as an invalid history later. *)
  let max_workflow_id_bytes = 65_536

  (** Validates the schema-free bounds and builds the strict envelope that the
      private bridge accepts: the workflow ID beside the canonical base64
      wrapper of the protobuf bytes. *)
  let of_protobuf ~workflow_id protobuf =
    let invalid message = Error (Error.defect ~message) in
    if String.equal workflow_id "" then
      invalid "replay workflow_id must not be empty"
    else if String.length workflow_id > max_workflow_id_bytes then
      invalid "replay workflow_id exceeds 65536 bytes"
    else if String.contains workflow_id '\000' then
      invalid "replay workflow_id must not contain NUL"
    else if not (String.is_valid_utf_8 workflow_id) then
      invalid "replay workflow_id must be valid UTF-8"
    else if String.equal protobuf "" then
      invalid "replay history must not be empty"
    else if String.length protobuf > max_bytes then
      invalid "replay history exceeds Temporal.Replay.History.max_bytes"
    else
      match Control.payload_json (Bytes.of_string protobuf) with
      | Error _ ->
          invalid "replay history exceeds Temporal.Replay.History.max_bytes"
      | Ok history ->
          let document =
            `Assoc [ ("workflow_id", `String workflow_id); ("history", history) ]
          in
          Ok
            {
              workflow_id;
              document = Bytes.unsafe_of_string (Yojson.Safe.to_string document);
            }

  (** Returns the workflow ID attached at construction. *)
  let workflow_id history = history.workflow_id
end

type mismatch = {
  workflow_id : string;
  workflow_type : string option;
  event_id : int64 option;
  event_type : string option;
  command : string option;
  reason : string;
}

type failure =
  | Nondeterminism of {
      run_id : string;
      message : string;
      mismatch : mismatch;
    }
  | Workflow_task_failed of { run_id : string option; message : string }
  | Invalid_history of { message : string }
  | Unsupported_history of { message : string }
  | Replay_error of Error.t

(** Best-effort reading of Temporal Core's nondeterminism text.

    Core reports a replay mismatch as an eviction message whose shape is not a
    protocol: at the pinned revision it is the Rust [Debug] rendering of the
    workflow-task failure, [Workflow activation completion failed: Failure {
    failure: Some(Failure { message: "[TMPRL1100] Nondeterminism error: <M>
    machine does not handle this event: HistoryEvent(id: <N>, <Type>)", ...
    }), force_cause: NonDeterministicError }]. Every extraction below is
    therefore optional and falls back to "not reported" rather than guessing,
    so a future Core wording change degrades the structured fields to [None]
    while the complete message is still delivered unchanged. The input is
    already bounded by [bounded], so every scan covers at most a few
    kilobytes. *)
module Core_text = struct
  (** Returns the first index where [needle] starts in [text], or [None]. *)
  let find text needle =
    let text_length = String.length text in
    let needle_length = String.length needle in
    let rec loop index =
      if index + needle_length > text_length then None
      else if String.sub text index needle_length = needle then Some index
      else loop (index + 1)
    in
    loop 0

  (** Decodes the Rust [Debug] string literal whose contents start at
      [start], stopping at its closing quote or at the end of [text] when a
      bounded message cut the literal short. The simple escapes Rust emits
      are decoded; line breaks become spaces so the result stays on one line;
      any other escape (such as [\u{...}]) is kept as written. *)
  let debug_string text start =
    let length = String.length text in
    let buffer = Buffer.create 160 in
    let rec loop index =
      if index < length then
        match text.[index] with
        | '"' -> ()
        | '\\' when index + 1 < length -> (
            match text.[index + 1] with
            | ('"' | '\\' | '\'') as character ->
                Buffer.add_char buffer character;
                loop (index + 2)
            | 'n' | 'r' | 't' ->
                Buffer.add_char buffer ' ';
                loop (index + 2)
            | _ ->
                Buffer.add_char buffer '\\';
                loop (index + 1))
        | character ->
            Buffer.add_char buffer character;
            loop (index + 1)
    in
    loop start;
    Buffer.contents buffer

  (** Core's own mismatch sentence: the [message] field of the first failure
      in the [Debug] envelope, or the whole text when there is no envelope. *)
  let reason message =
    let field = "message: \"" in
    match find message field with
    | None -> message
    | Some index -> (
        match debug_string message (index + String.length field) with
        | "" -> message
        | reason -> reason)

  (** Returns the longest prefix of [text] starting at [start] whose
      characters satisfy [accept], as [(value, next_index)]. *)
  let span text start accept =
    let length = String.length text in
    let rec stop index =
      if index < length && accept text.[index] then stop (index + 1) else index
    in
    let next = stop start in
    (String.sub text start (next - start), next)

  (** Parses Core's [HistoryEvent(id: <N>, <Type>)] display of the event it
      could not match. The type is kept only when it is a plain identifier
      followed by the closing parenthesis. *)
  let event reason =
    let marker = "HistoryEvent(id: " in
    match find reason marker with
    | None -> (None, None)
    | Some index -> (
        let digits, next =
          span reason (index + String.length marker) (function
            | '0' .. '9' -> true
            | _ -> false)
        in
        match Int64.of_string_opt digits with
        | None -> (None, None)
        | Some id ->
            let event_type =
              if
                next + 2 <= String.length reason
                && String.sub reason next 2 = ", "
              then
                let name, after =
                  span reason (next + 2) (function
                    | 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' -> true
                    | _ -> false)
                in
                if
                  name <> "" && after < String.length reason
                  && reason.[after] = ')'
                then Some name
                else None
              else None
            in
            (Some id, event_type))

  (** Parses the state machine Core names in ["<M> machine does not handle
      this event"] or ["<M> machine cannot handle this event"], the two
      wordings Core's command state machines use. The name must directly
      follow Core's ["Nondeterminism error: "] prefix and consist of words,
      so unrelated sentences that merely contain the word "machine" yield
      [None]. *)
  let command reason =
    let prefix = "Nondeterminism error: " in
    match find reason prefix with
    | None -> None
    | Some index ->
        let start = index + String.length prefix in
        let name, next =
          span reason start (function
            | 'A' .. 'Z' | 'a' .. 'z' | ' ' -> true
            | _ -> false)
        in
        let rest = String.sub reason next (String.length reason - next) in
        let ends_with_machine suffix =
          String.length name > String.length suffix
          && String.ends_with ~suffix name
        in
        (* [span] stops at the colon after "this event", so [name] is
           "<M> machine does not handle this event" when the shape matches. *)
        let strip suffix =
          String.sub name 0 (String.length name - String.length suffix)
        in
        if not (String.starts_with ~prefix:":" rest) then None
        else if ends_with_machine " machine does not handle this event" then
          Some (strip " machine does not handle this event")
        else if ends_with_machine " machine cannot handle this event" then
          Some (strip " machine cannot handle this event")
        else None
end

(** Builds the structured view of one bounded Core nondeterminism message. *)
let mismatch_of_message ~workflow_id ~workflow_type message =
  let reason = Core_text.reason message in
  let event_id, event_type = Core_text.event reason in
  {
    workflow_id;
    workflow_type;
    event_id;
    event_type;
    command = Core_text.command reason;
    reason;
  }

(** Renders the actionable part of a nondeterminism line: which workflow,
    where it diverged when Core said so, Core's own sentence, and the fix for
    an intentional change. Absent fields are omitted, never invented. *)
let describe_mismatch mismatch =
  let workflow =
    match mismatch.workflow_type with
    | Some workflow_type ->
        Printf.sprintf "workflow %s (ID %s)" workflow_type mismatch.workflow_id
    | None -> Printf.sprintf "workflow ID %s" mismatch.workflow_id
  in
  let event =
    match (mismatch.event_id, mismatch.event_type) with
    | Some id, Some event_type ->
        Some (Printf.sprintf "recorded event %Ld (%s)" id event_type)
    | Some id, None -> Some (Printf.sprintf "recorded event %Ld" id)
    | None, _ -> None
  in
  let location =
    match (event, mismatch.command) with
    | Some event, Some command ->
        Printf.sprintf ": %s does not match the current code's %s command"
          event command
    | Some event, None -> Printf.sprintf ": first mismatch at %s" event
    | None, Some command ->
        Printf.sprintf ": mismatch in the current code's %s command" command
    | None, None -> ""
  in
  Printf.sprintf
    "%s%s; Core: %s; guard intentional command changes with \
     Temporal.Workflow.patched"
    workflow location mismatch.reason

(** Prefixes each diagnostic with a stable kind so CI output can be grepped. *)
let failure_message = function
  | Nondeterminism { run_id; mismatch; message = _ } ->
      Printf.sprintf "nondeterminism (run %s): %s" run_id
        (describe_mismatch mismatch)
  | Workflow_task_failed { run_id = Some run_id; message } ->
      Printf.sprintf "workflow task failed (run %s): %s" run_id message
  | Workflow_task_failed { run_id = None; message } ->
      "workflow task failed: " ^ message
  | Invalid_history { message } -> "invalid history: " ^ message
  | Unsupported_history { message } -> "unsupported history: " ^ message
  | Replay_error error -> "replay error: " ^ Error.message error

(** Wraps an SDK-side failure that leaves the history's verdict unknown. *)
let replay_error message =
  Replay_error (Error.make ~category:`Bridge ~message:(bounded message) ())

(** Stable lowercase label and bounded text for a supervisor failure. Bridge
    messages are constant categories chosen by Rust; they never echo history
    bytes. *)
let native_error_view (error : Supervisor.error) =
  match error with
  | Supervisor.Backend ({ Bridge.status; message } : Bridge.error) ->
      let code =
        match status with
        | Bridge.Protocol -> "protocol"
        | Bridge.Invalid_state -> "invalid_state"
        | Bridge.Invalid_argument -> "invalid_argument"
        | Bridge.Configuration -> "configuration"
        | Bridge.Worker -> "worker"
        | Bridge.Outstanding_tasks -> "outstanding_tasks"
        | Bridge.Not_ready -> "not_ready"
        | Bridge.Panic -> "panic"
        | _ -> "bridge"
      in
      (code, bounded message)
  | Supervisor.Closed -> ("closed", "replay supervisor is shut down")
  | Supervisor.Owner_unavailable _ ->
      ("owner_unavailable", "replay supervisor Domain could not start")
  | Supervisor.Supervisor_failed _ ->
      ("supervisor_failed", "replay supervisor failed")

(** Converts a supervisor failure into a [Replay_error] naming the step. *)
let native_failure operation error =
  let code, message = native_error_view error in
  replay_error (Printf.sprintf "%s failed (%s): %s" operation code message)

(** Eviction reasons that mean Core refused the replayed run. Other reasons
    (cache pressure, a requested eviction, or the end of the history) are
    normal replay housekeeping. [workflow_id] and [workflow_type] are the
    context the SDK adds to a nondeterminism diagnostic. *)
let classify_eviction ~workflow_id ~workflow_type ~run_id ~message = function
  | Protocol.Nondeterminism ->
      Some
        (Nondeterminism
           {
             run_id;
             message;
             mismatch = mismatch_of_message ~workflow_id ~workflow_type message;
           })
  | Protocol.Fatal -> Some (Invalid_history { message })
  | Protocol.Lang_fail | Protocol.Unhandled_command ->
      Some (Workflow_task_failed { run_id = Some run_id; message })
  | Protocol.Eviction_unspecified | Protocol.Cache_full | Protocol.Cache_miss
  | Protocol.Lang_requested | Protocol.Task_not_found
  | Protocol.Pagination_or_history_fetch | Protocol.Workflow_execution_ending
    ->
      None

(** Replay-mode task source for the shared workflow adapter. It forwards to
    the supervisor's replay operations and records the first refusal; it
    never alters an activation or completion. *)
module Source = struct
  type t = {
    native : Supervisor.t;
    workflow_id : string;
    mutable workflow_type : string option;
    mutable verdict : failure option;
  }
  (** One replay's supervisor, the replayed history's workflow ID, the
      workflow type Core delivered in the run's start job (once seen), and
      the first observed refusal. Owned by the calling thread; the adapter's
      mutex serializes access. *)

  type error = Supervisor.error

  (** Keeps the earliest refusal: later evictions are consequences of it. *)
  let record source failure =
    if Option.is_none source.verdict then source.verdict <- Some failure

  (** Takes one replay activation, remembers the recorded workflow type from
      its start job, and records a refusing eviction before the adapter
      acknowledges it. Jobs are visited in Core's order, so a start job is
      seen before any eviction of the same run. *)
  let try_poll_workflow source =
    let result = Supervisor.perform source.native Supervisor.Try_poll_replay_workflow in
    (match result with
    | Ok (Some (activation : Protocol.activation)) ->
        List.iter
          (function
            | Protocol.Initialize_workflow { workflow_type; _ } ->
                if Option.is_none source.workflow_type then
                  source.workflow_type <- Some (bounded workflow_type)
            | Protocol.Remove_from_cache { message; reason } ->
                Option.iter (record source)
                  (classify_eviction ~workflow_id:source.workflow_id
                     ~workflow_type:source.workflow_type
                     ~run_id:activation.run_id ~message:(bounded message)
                     reason)
            | _ -> ())
          activation.jobs
    | Ok None | Error _ -> ());
    result

  (** Records a workflow-task failure produced by OCaml code, then submits
      the adapter's canonical bytes unchanged. *)
  let complete_workflow source ~(completion : Protocol.completion) encoded =
    Option.iter
      (fun (failure : Protocol.failure) ->
        record source
          (Workflow_task_failed
             { run_id = Some completion.run_id; message = bounded failure.message }))
      completion.task_failure;
    Supervisor.perform source.native
      (Supervisor.Complete_replay_workflow encoded)

  (** Stable classification for adapter diagnostics. *)
  let error_code error = fst (native_error_view error)

  (** Bounded diagnostic for adapter diagnostics. *)
  let error_message error = snd (native_error_view error)

  (** A replay completion is never resubmitted: no status proves that its
      lease is still outstanding. *)
  let error_is_retryable (_ : error) = false

  (** A raised completion is an uncertain acknowledgement; fail closed. *)
  let exception_is_retryable (_ : exn) = false
end

module Runner = Adapter.Make (Source)
(** The production workflow adapter instantiated with the replay source. *)

(** Validates the public registration list exactly as [Worker.create] does and
    converts it to the private adapter's registrations. No native resource
    exists yet, so an error here needs no cleanup. *)
let registrations workflows =
  let module Names = Set.Make (String) in
  let rec collect names acc = function
    | [] -> Ok (List.rev acc)
    | Workflow (definition, signals, queries, updates) :: rest -> (
        let name = Workflow.name definition in
        match Workflow.implementation definition with
        | None ->
            Error ("workflow " ^ name ^ " has no local implementation")
        | Some _ when Names.mem name names ->
            Error ("duplicate workflow registration: " ^ name)
        | Some _ -> (
            match Interaction.create ~signals ~queries ~updates () with
            | Error error -> Error (Error.message error)
            | Ok _ ->
                let registration =
                  Native_worker.register_workflow ~signals ~queries ~updates
                    (Workflow_private.to_base definition)
                in
                collect (Names.add name names) (registration :: acc) rest))
  in
  collect Names.empty [] workflows
  |> Result.map_error (fun message ->
         Replay_error (Error.defect ~message))

(** Consecutive idle readiness waits tolerated before a replay is abandoned.
    Each native wait is bounded to about 100 ms, so this is roughly 30 seconds
    without any activation; a replay that keeps producing activations is
    bounded by its history length instead. *)
let max_idle_waits = 300

(** Drives the adapter until Core has consumed the history, every activation
    has been acknowledged, and the replay worker finalized naturally. The
    native graph is still owned by the caller on every return. *)
let drain (source : Source.t) runner =
  let rec loop idle =
    match Runner.poll runner with
    | Error ({ code = "protocol"; message; _ } : Adapter.error_view) ->
        (* The supervisor already retired a lease whose activation the OCaml
           protocol could not represent; replay must not skip that history. *)
        Error (Unsupported_history { message = bounded message })
    | Error { code; path; message } ->
        Error
          (replay_error
             (Printf.sprintf "replay activation failed at %s (%s): %s" path
                code message))
    | Ok (Adapter.Rejected { run_id; error; lease_retired = true }) ->
        Source.record source
          (Workflow_task_failed { run_id; message = bounded error.message });
        loop 0
    | Ok (Adapter.Rejected { error; lease_retired = false; _ }) ->
        Error
          (replay_error
             (Printf.sprintf "replay completion failed (%s): %s" error.code
                error.message))
    | Ok (Adapter.Completed _) -> loop 0
    | Ok Adapter.Not_ready -> (
        match Supervisor.perform source.native Supervisor.Finalize_replay with
        | Ok () -> Ok ()
        | Error (Supervisor.Backend { status = Bridge.Outstanding_tasks; _ })
          ->
            if idle >= max_idle_waits then
              Error (replay_error "replay made no progress for 30 seconds")
            else (
              match
                Supervisor.perform source.native Supervisor.Wait_replay_workflow
              with
              | Ok () | Error (Supervisor.Backend { status = Bridge.Not_ready; _ })
                ->
                  loop (idle + 1)
              | Error error -> Error (native_failure "replay wait" error))
        | Error error -> Error (native_failure "replay finalization" error))
  in
  loop 0

(** Native settings for the workflow-only replay worker. The cache must hold
    the one replayed run; Core requires two pollers whenever caching is on. *)
let worker_config ~namespace ~task_queue =
  Supervisor.worker_config ~namespace ~task_queue
    ~build_id:"ocaml-temporal-replay" ~max_cached_workflows:2
    ~max_outstanding_workflow_tasks:2 ~max_concurrent_workflow_task_polls:2
    ~graceful_shutdown_timeout_ms:1_000L ()

(** Feeds the one history and closes input. A protocol status means the
    bridge or Core rejected the history before admitting it. *)
let feed native (history : History.t) =
  match
    Supervisor.perform native (Supervisor.Feed_replay_history history.document)
  with
  | Error (Supervisor.Backend { status = Bridge.Protocol; message }) ->
      Error (Invalid_history { message = bounded message })
  | Error error -> Error (native_failure "replay history admission" error)
  | Ok () -> (
      match Supervisor.perform native Supervisor.Finish_replay_input with
      | Ok () -> Ok ()
      | Error error -> Error (native_failure "replay input close" error))

(** Runs one replay inside an already created supervisor. *)
let run_replay ~native ~config ~runner_slot ~namespace ~task_queue ~workflows
    history =
  let source =
    {
      Source.native;
      workflow_id = History.workflow_id history;
      workflow_type = None;
      verdict = None;
    }
  in
  match
    Runner.create ~task_queue ~namespace ~supervisor:source ~workflows ()
  with
  | Error { code; path; message } ->
      Error
        (Replay_error
           (Error.defect
              ~message:
                (Printf.sprintf "replay registration failed at %s (%s): %s"
                   path code message)))
  | Ok runner -> (
      runner_slot := Some runner;
      match Supervisor.perform native (Supervisor.Start_replay_worker config) with
      | Error error -> Error (native_failure "replay worker start" error)
      | Ok () -> (
          match feed native history with
          | Error _ as error -> error
          | Ok () -> (
              let drained = drain source runner in
              (* The first recorded refusal is the history's verdict even when
                 draining later also failed: the later failure is cleanup. *)
              match (source.verdict, drained) with
              | Some failure, _ -> Error failure
              | None, result -> result)))

let replay ?(namespace = "default") ?(task_queue = "temporal-replay")
    ~workflows history =
  match registrations workflows with
  | Error _ as error -> error
  | Ok workflows -> (
      match worker_config ~namespace ~task_queue with
      | Error { Bridge.message; _ } ->
          Error
            (Replay_error
               (Error.defect
                  ~message:("invalid replay options: " ^ bounded message)))
      | Ok config -> (
          match Supervisor.create ~capacity:8 () with
          | Error error -> Error (native_failure "replay runtime start" error)
          | Ok native ->
              let runner_slot = ref None in
              (* Shutdown disposes any undrained replay worker, joins the
                 owner Domain, and frees the runtime. Only then are the OCaml
                 executions released, because Core no longer holds leases
                 that could reference them. *)
              let release () =
                let shutdown = Supervisor.shutdown native in
                Option.iter Runner.discard !runner_slot;
                shutdown
              in
              let outcome =
                match
                  run_replay ~native ~config ~runner_slot ~namespace
                    ~task_queue ~workflows history
                with
                | outcome -> outcome
                | exception exception_ ->
                    (* An exception here is an SDK defect or an interrupt
                       such as [Sys.Break], not a replay verdict: release the
                       native graph, then re-raise it unchanged with its
                       backtrace so it is never masked as [Replay_error]. *)
                    let backtrace = Printexc.get_raw_backtrace () in
                    (try ignore (release ()) with _ -> ());
                    Printexc.raise_with_backtrace exception_ backtrace
              in
              let shutdown = release () in
              match (outcome, shutdown) with
              | Error _, _ -> outcome
              | Ok (), Ok () -> Ok ()
              | Ok (), Error error ->
                  Error (native_failure "replay shutdown" error)))

let replay_all ?namespace ?task_queue ~workflows histories =
  List.map
    (fun history ->
      (history, replay ?namespace ?task_queue ~workflows history))
    histories

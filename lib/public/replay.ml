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

type failure =
  | Nondeterminism of { run_id : string; message : string }
  | Workflow_task_failed of { run_id : string option; message : string }
  | Invalid_history of { message : string }
  | Unsupported_history of { message : string }
  | Replay_error of Error.t

(** Prefixes each diagnostic with a stable kind so CI output can be grepped. *)
let failure_message = function
  | Nondeterminism { run_id; message } ->
      Printf.sprintf "nondeterminism (run %s): %s" run_id message
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
    normal replay housekeeping. *)
let classify_eviction ~run_id ~message = function
  | Protocol.Nondeterminism -> Some (Nondeterminism { run_id; message })
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
  type t = { native : Supervisor.t; mutable verdict : failure option }
  (** One replay's supervisor and its first observed refusal. Owned by the
      calling thread; the adapter's mutex serializes access. *)

  type error = Supervisor.error

  (** Keeps the earliest refusal: later evictions are consequences of it. *)
  let record source failure =
    if Option.is_none source.verdict then source.verdict <- Some failure

  (** Takes one replay activation and records a refusing eviction before the
      adapter acknowledges it. *)
  let try_poll_workflow source =
    let result = Supervisor.perform source.native Supervisor.Try_poll_replay_workflow in
    (match result with
    | Ok (Some (activation : Protocol.activation)) ->
        List.iter
          (function
            | Protocol.Remove_from_cache { message; reason } ->
                Option.iter (record source)
                  (classify_eviction ~run_id:activation.run_id
                     ~message:(bounded message) reason)
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
  let source = { Source.native; verdict = None } in
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
              let outcome =
                try
                  run_replay ~native ~config ~runner_slot ~namespace
                    ~task_queue ~workflows history
                with exception_ ->
                  (* A raise here is an SDK defect; keep the result typed so
                     the native graph below is still released. *)
                  Error
                    (replay_error
                       ("replay raised " ^ Printexc.to_string exception_))
              in
              (* Shutdown disposes any undrained replay worker, joins the
                 owner Domain, and frees the runtime. Only then are the OCaml
                 executions released, because Core no longer holds leases
                 that could reference them. *)
              let shutdown = Supervisor.shutdown native in
              Option.iter Runner.discard !runner_slot;
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

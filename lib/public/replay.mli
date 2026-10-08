(** Offline replay of recorded workflow histories against application code.

    Replay checks that the workflow definitions an application is about to
    deploy still produce the commands recorded in an existing execution's
    history. It runs entirely in-process: no Temporal Server, network
    connection, or credentials are needed, so it is suitable for unit tests
    and pre-deployment CI gates.

    Each {!replay} call feeds one history to Temporal Core's replay worker,
    which turns the history into workflow activations and compares the
    commands produced by the registered OCaml workflow with the recorded
    events. The workflow code runs through the same registration, codec, and
    completion path as {!Worker}; only the task source differs.

    {[
      let check_history ~workflow_id path =
        let bytes = In_channel.with_open_bin path In_channel.input_all in
        match Temporal.Replay.History.of_protobuf ~workflow_id bytes with
        | Error error -> Error (Temporal.Error.message error)
        | Ok history -> (
            match
              Temporal.Replay.replay
                ~workflows:[ Temporal.Replay.workflow order_workflow ]
                history
            with
            | Ok () -> Ok ()
            | Error failure -> Error (Temporal.Replay.failure_message failure))
    ]}

    Replay proves compatibility only for the code paths a recorded history
    actually exercised. It does not compare payload values: a changed result,
    activity argument, or signal payload with the same command shape replays
    successfully. Activities and child workflows are not executed; their
    recorded results are delivered from history.

    Threading: [replay] blocks the calling system thread until the replay
    finishes. Call it from ordinary application or test code, never from a
    workflow or activity callback. Every call owns an isolated native replay
    graph and a dedicated owner Domain, which are released before the call
    returns on every path, including failures. *)

(** A workflow registration for {!replay}. *)
type registered_workflow

(** Registers a workflow with optional signal, query, and update handlers,
    exactly as for [Temporal.Worker.workflow]. Handlers must be registered
    when the recorded history delivers the corresponding signals or updates;
    otherwise the replayed workflow observes a different sequence of events
    than the original execution did. *)
val workflow :
  ?signals:Signal.Handler.t list ->
  ?queries:Query.Handler.t list ->
  ?updates:Update.Handler.t list ->
  ('input, 'output) Workflow.t ->
  registered_workflow

(** Recorded histories accepted by {!replay}. *)
module History : sig
  (** One validated history together with the workflow ID it belongs to. The
      value owns a private copy of its input bytes. *)
  type t

  (** Maximum accepted size, in bytes, of a serialized history. It matches
      the native bridge's payload safety limit. *)
  val max_bytes : int

  (** Accepts the binary protobuf encoding of one Temporal
      [temporal.api.history.v1.History] message, as returned by the
      [GetWorkflowExecutionHistory] API, together with the execution's
      workflow ID, which the History message does not carry.

      This checks only the bounds that do not need the protobuf schema: the
      workflow ID must be non-empty valid UTF-8 without NUL and at most 65,536
      bytes, and the history must be non-empty and at most {!max_bytes}. The
      protobuf itself and Core's history invariants are checked by {!replay},
      which reports a malformed history as {!Invalid_history}.

      The Temporal CLI's [temporal workflow show --output json] export is
      protobuf JSON, not this binary encoding; convert it with any protobuf
      library that has the Temporal API descriptors (for example Python's
      [google.protobuf.json_format.Parse] followed by [SerializeToString])
      before calling this function. *)
  val of_protobuf : workflow_id:string -> string -> (t, Error.t) result

  (** Returns the workflow ID supplied to {!of_protobuf}. *)
  val workflow_id : t -> string
end

(** Where and how a replay first diverged from its recorded history, as far
    as Temporal Core's diagnostic says. The SDK adds the context it knows
    (the workflow ID and type) and extracts the event and command that Core's
    text names. A field that Core's text does not supply is [None]: the SDK
    never infers an event, command, or source location that Core did not
    report, and Core does not report OCaml source locations at all. *)
type mismatch = {
  workflow_id : string;
      (** The workflow ID supplied to {!History.of_protobuf}. *)
  workflow_type : string option;
      (** The workflow type recorded in the history's start event, as
          delivered to the replayed workflow. [None] only when Core refused
          the run before delivering its start. *)
  event_id : int64 option;
      (** The ID of the first recorded history event that Core could not
          match, for example [5]. These are the event IDs shown by the
          Temporal UI and [temporal workflow show]. *)
  event_type : string option;
      (** Core's name for that event's type, for example ["TimerStarted"] or
          ["ActivityTaskScheduled"]. *)
  command : string option;
      (** Core's name for the command state machine the event was matched
          against, normally the command the current workflow code produced
          at that point, for example ["Timer"], ["Activity"], or
          ["Complete workflow"]. [None] when Core's text names none, such as
          ["No command scheduled for event ..."], which means the current
          code produced no command where the history recorded one. *)
  reason : string;
      (** Core's mismatch sentence, unwrapped from the workflow-task failure
          envelope it travels in, for example
          ["[TMPRL1100] Nondeterminism error: Complete workflow machine does
          not handle this event: HistoryEvent(id: 5, TimerStarted)"]. It is
          the whole [message] when Core sent no envelope. *)
}

(** Why a history did not replay cleanly. Each diagnostic is bounded to a few
    kilobytes. Core's mismatch text describes recorded events and workflow
    commands; it can name workflow types, activity types, timer or activity
    IDs, and similar identifiers from the history, but the SDK never copies
    payload bytes into it. *)
type failure =
  | Nondeterminism of {
      run_id : string;
      message : string;
      mismatch : mismatch;
    }
      (** The workflow produced commands that do not match the recorded
          history, for example a removed, added, or reordered timer or
          activity. [message] is Temporal Core's complete description of the
          mismatch, [run_id] identifies the replayed run, and [mismatch]
          structures the parts of it needed to locate the change. This is
          distinct from [Workflow_task_failed] (the code itself failed) and
          from [Invalid_history] (the input is malformed). *)
  | Workflow_task_failed of { run_id : string option; message : string }
      (** Workflow code could not complete an activation: it raised, returned
          a defect, could not decode its input with the registered codec, or
          the history's workflow type has no registration. This is a failure
          of the replayed {i task}; a workflow that deliberately returns
          [Error] and whose history records that failure replays
          successfully. *)
  | Invalid_history of { message : string }
      (** The input is not a valid Temporal history: the bytes are not a
          [History] protobuf, or Core rejected the event sequence (for
          example a missing start event or malformed workflow-task
          boundaries). *)
  | Unsupported_history of { message : string }
      (** The history is valid but uses a Temporal feature this SDK cannot
          replay yet. *)
  | Replay_error of Error.t
      (** The replay could not be carried out for a reason unrelated to the
          history's compatibility: an invalid registration list or option,
          a native runtime that could not start, or an internal SDK failure.
          The history's verdict is unknown. *)

(** Returns a one-line, human-readable description of [failure], prefixed with
    its kind, suitable for logs and CI output. The prefixes are stable:
    ["nondeterminism (run RUN_ID): "], ["workflow task failed"],
    ["invalid history: "], ["unsupported history: "], and
    ["replay error: "].

    A nondeterminism line continues with the workflow type and ID, the
    recorded event and the command it was matched against when Core named
    them, Core's [reason], and a reminder that an intentional
    change must be guarded with [Temporal.Workflow.patched]. For example
    (wrapped here; the real output is one line):

    {v
nondeterminism (run 01a1...): workflow corpus.timer (ID history-corpus-timer):
recorded event 5 (TimerStarted) does not match the current code's Complete
workflow command; Core: [TMPRL1100] Nondeterminism error: Complete workflow
machine does not handle this event: HistoryEvent(id: 5, TimerStarted); guard
intentional command changes with Temporal.Workflow.patched
    v}

    The wording after the prefix is for people and may be refined; match on
    the {!failure} constructor and {!mismatch} fields in code. *)
val failure_message : failure -> string

(** Replays [history] against [workflows] and returns [Ok ()] when Core
    accepted every recorded workflow task. The run must be registered under
    the workflow type recorded in the history.

    [namespace] (default ["default"]) and [task_queue] (default
    ["temporal-replay"]) are reported to workflow code by
    [Temporal.Workflow.info]; pass the original worker's values when the
    workflow branches on them. Registration is validated exactly as for
    [Temporal.Worker.create]; duplicate names, remote-only definitions, and
    invalid option strings return [Replay_error] before any native resource
    is created.

    Each call creates and fully releases its own native replay graph, so
    repeated and concurrent calls from different system threads are
    independent. A replay that stops making progress for about 30 seconds is
    abandoned and reported as [Replay_error]. An exception that escapes the
    SDK itself (an internal defect, or an interrupt such as [Sys.Break]) is
    not turned into a result: the native graph is released and the exception
    is re-raised unchanged. *)
val replay :
  ?namespace:string ->
  ?task_queue:string ->
  workflows:registered_workflow list ->
  History.t ->
  (unit, failure) result

(** Replays every history in order with {!replay}, each in its own isolated
    native graph, and pairs it with its result. A failure in one history does
    not stop the others. *)
val replay_all :
  ?namespace:string ->
  ?task_queue:string ->
  workflows:registered_workflow list ->
  History.t list ->
  (History.t * (unit, failure) result) list

(** Offline replay through the installed [temporal-sdk] package (#497).

    [public_api.ml] only type-checks [Temporal.Replay]; this executable runs
    it. It replays one checked-in history, the history corpus's
    [live-2026-10-08-timer.pb] (workflow type [corpus.timer], workflow ID
    [history-corpus-timer]), against two application definitions and asserts
    both verdicts:

    - the recorded code (one durable timer, then a constant result) must
      replay cleanly;
    - the same workflow type with the timer removed must be reported as
      [Nondeterminism] at the recorded [TimerStarted] event.

    The definitions mirror [timer_workflow] and [timer_removed_workflow] in
    [test/history_corpus/corpus_definitions.ml], which own that history's
    expected verdicts in the corpus manifest. They are restated here because
    the consumer may depend on nothing but the installed package.

    The single command-line argument is the history file's path; the install
    scripts copy the corpus file next to the consumer. No Temporal Server,
    network, or credentials are involved. *)

module T = Temporal

(** The workflow type and ID recorded in the history's start event. Replay
    requires the supplied ID to equal the recorded one. *)
let workflow_type = "corpus.timer"

let workflow_id = "history-corpus-timer"

(** The timer duration the recorded execution used. Core does not compare
    timer durations during replay, but keeping the recorded value makes the
    compatible definition a faithful copy of the code that produced the
    history. *)
let recorded_timer = T.Duration.of_ms 100L

(** The code that produced the history: one durable timer, then a constant
    result. *)
let compatible_workflow =
  T.Workflow.define ~name:workflow_type ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      match T.Workflow.sleep recorded_timer with
      | Error error -> Error error
      | Ok () -> Ok "TIMER:FIRED")

(** An incompatible deployment of the same workflow type: the timer has been
    removed, so the workflow completes where the history records
    [TimerStarted]. *)
let timer_removed_workflow =
  T.Workflow.define ~name:workflow_type ~input:T.Codec.unit
    ~output:T.Codec.string (fun () -> Ok "TIMER:FIRED")

(** Reads the history named on the command line and wraps it for replay. A
    construction error means the installed package rejected a valid corpus
    history, which is a test failure. *)
let history =
  let path =
    match Sys.argv with
    | [| _; path |] -> path
    | _ -> failwith "usage: installed_replay HISTORY.pb"
  in
  let bytes = In_channel.with_open_bin path In_channel.input_all in
  match T.Replay.History.of_protobuf ~workflow_id bytes with
  | Ok history -> history
  | Error error -> failwith ("history rejected: " ^ T.Error.message error)

(** Replays [history] against [definition] alone. *)
let replay definition =
  T.Replay.replay ~workflows:[ T.Replay.workflow definition ] history

let () =
  if not (String.equal (T.Replay.History.workflow_id history) workflow_id) then
    failwith "history did not retain its workflow ID";
  (match replay compatible_workflow with
  | Ok () -> ()
  | Error failure ->
      failwith
        ("recorded code did not replay: " ^ T.Replay.failure_message failure));
  (match replay timer_removed_workflow with
  | Error
      (T.Replay.Nondeterminism
        { mismatch = { workflow_id = id; event_type; _ }; _ })
    when String.equal id workflow_id && event_type = Some "TimerStarted" ->
      ()
  | Error failure ->
      failwith
        ("changed code produced the wrong verdict: "
        ^ T.Replay.failure_message failure)
  | Ok () -> failwith "changed code replayed cleanly");
  print_endline
    "Installed consumer replayed the timer history: recorded code passed, \
     changed code was nondeterministic."

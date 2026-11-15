(** Offline replay through the installed [temporal-sdk] package (#497).

    [public_api.ml] only type-checks [Temporal.Replay]; this executable runs
    it. It replays two checked-in history-corpus histories, one per closed
    outcome, against the code that produced each and against one incompatible
    change, and asserts all four verdicts:

    - [live-2026-10-08-timer.pb] (workflow type [corpus.timer], workflow ID
      [history-corpus-timer]) records a successful run. The recorded code
      (one durable timer, then a constant result) must replay cleanly, and
      the same workflow type with the timer removed must be reported as
      [Nondeterminism] at the recorded [TimerStarted] event.
    - [task-failure-business-permanent.pb] (workflow type
      [task-failure.business-permanent], workflow ID
      [task-failure-business-permanent]) records a run that
      failed with a non-retryable application error. The recorded code, which
      returns that typed error, must replay cleanly; code changed to succeed
      instead must be reported as [Nondeterminism] at the recorded
      [WorkflowExecutionFailed] event, because Core matches the new
      completion command against the recorded failure.

    The definitions mirror [timer_workflow] and [timer_removed_workflow] in
    [test/history_corpus/corpus_definitions.ml] and [business] in
    [test/integration/temporal/task_failure/failure_support.ml], which own
    those histories' expected verdicts in the corpus manifest. They are
    restated here because the consumer may depend on nothing but the
    installed package.

    The two command-line arguments are the successful and failed histories'
    paths, in that order; the install scripts copy both corpus files next to
    the consumer. No Temporal Server, network, or credentials are involved. *)

module T = Temporal

(** {1 Successful history} *)

(** The workflow type and ID recorded in the timer history's start event.
    Replay requires the supplied ID to equal the recorded one. *)
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

(** {1 Failed history} *)

(** The workflow type and ID recorded in the failed history's start event. *)
let failed_workflow_type = "task-failure.business-permanent"

let failed_workflow_id = "task-failure-business-permanent"

(** The code that produced the failed history: it returns a non-retryable
    application error, which the worker recorded as
    [WorkflowExecutionFailed]. Returning a typed [Error] is the supported way
    for workflow code to fail its execution. *)
let failing_workflow =
  T.Workflow.define ~name:failed_workflow_type ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      Error
        (T.Error.make ~category:`Workflow ~non_retryable:true
           ~message:"intentional business failure" ()))

(** An incompatible deployment of the same workflow type: the business failure
    has been "fixed", so the code completes where the history records the
    execution's failure. *)
let now_succeeds_workflow =
  T.Workflow.define ~name:failed_workflow_type ~input:T.Codec.unit
    ~output:T.Codec.string (fun () -> Ok "recovered")

(** {1 Replay} *)

(** The two history paths named on the command line, successful first. *)
let timer_path, failed_path =
  match Sys.argv with
  | [| _; timer_path; failed_path |] -> (timer_path, failed_path)
  | _ -> failwith "usage: installed_replay TIMER_HISTORY.pb FAILED_HISTORY.pb"

(** Reads the history at [path] and wraps it for replay under [workflow_id].
    A construction error, or a history that loses its workflow ID, means the
    installed package rejected a valid corpus history, which is a test
    failure. *)
let load ~workflow_id path =
  let bytes = In_channel.with_open_bin path In_channel.input_all in
  match T.Replay.History.of_protobuf ~workflow_id bytes with
  | Error error ->
      failwith (path ^ ": history rejected: " ^ T.Error.message error)
  | Ok history ->
      if not (String.equal (T.Replay.History.workflow_id history) workflow_id)
      then failwith (path ^ ": history did not retain its workflow ID");
      history

(** Replays [history] against [definition] alone. *)
let replay history definition =
  T.Replay.replay ~workflows:[ T.Replay.workflow definition ] history

(** Requires the recorded code to replay [history] cleanly; [label] names the
    case in the failure message. *)
let expect_ok label history definition =
  match replay history definition with
  | Ok () -> ()
  | Error failure ->
      failwith (label ^ ": recorded code did not replay: "
               ^ T.Replay.failure_message failure)

(** Requires changed code to be reported as [Nondeterminism] for [workflow_id]
    at the recorded event of type [event_type]. *)
let expect_nondeterminism label ~workflow_id ~event_type history definition =
  match replay history definition with
  | Error
      (T.Replay.Nondeterminism
        { mismatch = { workflow_id = id; event_type = recorded; _ }; _ })
    when String.equal id workflow_id && recorded = Some event_type ->
      ()
  | Error failure ->
      failwith
        (label ^ ": changed code produced the wrong verdict: "
        ^ T.Replay.failure_message failure)
  | Ok () -> failwith (label ^ ": changed code replayed cleanly")

let () =
  let timer_history = load ~workflow_id timer_path in
  expect_ok "timer" timer_history compatible_workflow;
  expect_nondeterminism "timer" ~workflow_id ~event_type:"TimerStarted"
    timer_history timer_removed_workflow;
  let failed_history = load ~workflow_id:failed_workflow_id failed_path in
  expect_ok "business failure" failed_history failing_workflow;
  expect_nondeterminism "business failure" ~workflow_id:failed_workflow_id
    ~event_type:"WorkflowExecutionFailed" failed_history now_succeeds_workflow;
  print_endline
    "Installed consumer replayed the timer and business-failure histories: \
     recorded code passed, changed code was nondeterministic."

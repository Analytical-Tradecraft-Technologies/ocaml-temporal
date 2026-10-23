(** Developer-facing nondeterminism diagnostics of [Temporal.Replay] (#529).

    Replays the two negative controls of the versioned history corpus
    ([test/fixtures/history-corpus]) through the public API against the
    corpus's frozen incompatible definitions, and checks the structured
    [mismatch] and the [failure_message] line a developer reads: the workflow
    context, the recorded event and the command Core matched it against when
    Core names them, [None] when Core does not, and no payload bytes. No
    Temporal Server is involved. *)

module R = Temporal.Replay
module Corpus = Corpus_definitions

(** Directory of the corpus histories, declared as a Dune dependency. *)
let histories_dir = "../fixtures/history-corpus/histories"

(** Builds the public history for a corpus protobuf [file] recorded under
    [workflow_id]. *)
let corpus_history ~workflow_id file =
  let bytes =
    In_channel.with_open_bin (Filename.concat histories_dir file)
      In_channel.input_all
  in
  match R.History.of_protobuf ~workflow_id bytes with
  | Ok history -> history
  | Error error -> failwith (file ^ ": " ^ Temporal.Error.message error)

(** Converts one frozen corpus definition set to public replay
    registrations, keeping each definition's handlers. *)
let registrations set =
  match Corpus.definition_set set with
  | None -> failwith ("unknown corpus definition set " ^ set)
  | Some workflows ->
      List.map
        (fun (Corpus.Workflow { definition; signals; queries; updates }) ->
          R.workflow ~signals ~queries ~updates definition)
        workflows

(** [contains text needle] is true when [needle] occurs in [text]. *)
let contains text needle =
  let text_length = String.length text in
  let needle_length = String.length needle in
  let rec loop index =
    index + needle_length <= text_length
    && (String.sub text index needle_length = needle || loop (index + 1))
  in
  loop 0

(** Renders an optional field for assertion messages. *)
let show_option show = function None -> "None" | Some value -> show value

(** Fails [label] unless [actual] equals [expected]. *)
let check_field label name show expected actual =
  if expected <> actual then
    failwith
      (Printf.sprintf "%s: %s expected %s, got %s" label name
         (show_option show expected) (show_option show actual))

(** Replays [history] against the corpus definition [set] and returns the
    [Nondeterminism] fields, failing the test for any other result. *)
let nondeterminism label ~set history =
  match R.replay ~workflows:(registrations set) history with
  | Error (R.Nondeterminism { run_id; message; mismatch } as failure) ->
      (run_id, message, mismatch, R.failure_message failure)
  | Ok () -> failwith (label ^ ": expected nondeterminism, got Ok")
  | Error failure ->
      failwith
        (label ^ ": expected nondeterminism, got " ^ R.failure_message failure)

(** Properties every rendered nondeterminism line must have: the stable
    prefix with the run ID, one line, the workflow ID, Core's reason, the
    patching hint, and no recorded payload text. *)
let check_line label ~run_id ~(mismatch : R.mismatch) ~forbidden line =
  let prefix = "nondeterminism (run " ^ run_id ^ "): " in
  if not (String.starts_with ~prefix line) then
    failwith (label ^ ": line lacks the stable prefix: " ^ line);
  if String.contains line '\n' || String.contains line '\r' then
    failwith (label ^ ": line is not a single line");
  List.iter
    (fun needle ->
      if not (contains line needle) then
        failwith (label ^ ": line lacks " ^ needle ^ ": " ^ line))
    [
      "ID " ^ mismatch.workflow_id;
      mismatch.reason;
      "Temporal.Workflow.patched";
    ];
  List.iter
    (fun needle ->
      if contains line needle then
        failwith (label ^ ": line leaks payload text " ^ needle))
    forbidden

(** Corpus [negative-timer-removed]: the [corpus.timer] history replayed
    against the definition with its timer removed. Core names the recorded
    [TimerStarted] event and the [Complete workflow] command the changed code
    produced in its place. *)
let test_timer_removed () =
  let label = "negative-timer-removed" in
  let workflow_id = "history-corpus-timer" in
  let history =
    corpus_history ~workflow_id "live-2026-10-08-timer.pb"
  in
  let run_id, message, mismatch, line =
    nondeterminism label ~set:"negative-timer-removed" history
  in
  if run_id <> "01a11a23-88ed-7d4a-bd17-721a837b08e5" then
    failwith (label ^ ": unexpected run ID " ^ run_id);
  if mismatch.workflow_id <> workflow_id then
    failwith (label ^ ": wrong workflow ID " ^ mismatch.workflow_id);
  check_field label "workflow_type" Fun.id (Some Corpus.timer_workflow_type)
    mismatch.workflow_type;
  check_field label "event_id" Int64.to_string (Some 5L) mismatch.event_id;
  check_field label "event_type" Fun.id (Some "TimerStarted")
    mismatch.event_type;
  check_field label "command" Fun.id (Some "Complete workflow")
    mismatch.command;
  let expected_reason =
    "[TMPRL1100] Nondeterminism error: Complete workflow machine does not \
     handle this event: HistoryEvent(id: 5, TimerStarted)"
  in
  if mismatch.reason <> expected_reason then
    failwith (label ^ ": unexpected reason " ^ mismatch.reason);
  (* [message] keeps Core's complete text, of which [reason] is a part. *)
  if not (contains message mismatch.reason) then
    failwith (label ^ ": message does not contain the reason");
  check_line label ~run_id ~mismatch ~forbidden:[ "TIMER:FIRED" ] line;
  List.iter
    (fun needle ->
      if not (contains line needle) then
        failwith (label ^ ": line lacks " ^ needle ^ ": " ^ line))
    [
      "workflow corpus.timer (ID history-corpus-timer)";
      "recorded event 5 (TimerStarted) does not match the current code's \
       Complete workflow command";
    ]

(** Corpus [negative-patch-active-on-legacy]: a history that took the patched
    branch replayed against the pre-patch generation. Core reports the
    recorded patch marker that no [patched] call claimed, naming the patch ID
    but no history event ID or command state machine, so those fields stay
    [None] instead of being guessed. *)
let test_patch_active_on_legacy () =
  let label = "negative-patch-active-on-legacy" in
  let workflow_id = "history-corpus-patch-active" in
  let history =
    corpus_history ~workflow_id "live-2026-10-08-patch-active.pb"
  in
  let run_id, _message, mismatch, line =
    nondeterminism label ~set:"corpus-v1-patch-legacy" history
  in
  check_field label "workflow_type" Fun.id (Some Corpus.patch_workflow_type)
    mismatch.workflow_type;
  check_field label "event_id" Int64.to_string None mismatch.event_id;
  check_field label "event_type" Fun.id None mismatch.event_type;
  check_field label "command" Fun.id None mismatch.command;
  List.iter
    (fun needle ->
      if not (contains mismatch.reason needle) then
        failwith (label ^ ": reason lacks " ^ needle ^ ": " ^ mismatch.reason))
    [ "patch marker"; Corpus.patch_id; "no corresponding change command" ];
  check_line label ~run_id ~mismatch ~forbidden:[] line;
  if contains line "recorded event" then
    failwith (label ^ ": line names an event Core did not report: " ^ line)

let () =
  test_timer_removed ();
  test_patch_active_on_legacy ()

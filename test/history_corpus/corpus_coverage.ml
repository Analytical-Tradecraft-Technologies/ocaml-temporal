(** The replay corpus feature coverage table and its checks (issue #503).

    Every feature the SDK supports or exports for evaluation that could
    affect what a workflow history records, or how it replays, has exactly
    one row here, derived from [docs/reference/v1-support-policy.md] and
    [docs/reference/feature-coverage.md]. A row either names a corpus verdict
    that some entry tagged with the feature must produce, or gives an
    explicit reason why no such entry exists: replay is not applicable, or
    the history has not been captured yet. Known gaps are listed as gaps,
    never satisfied by a relabelled case.

    The checks keep the table and the manifest honest in both directions:
    - an entry's feature tag must have a row, so a new feature cannot enter
      the corpus (or be added to the SDK and tagged) without being
      classified;
    - a [Replayed] or [Rejected] row must have a matching entry, so a
      supported feature cannot silently lose its case;
    - a [Not_yet_captured] row must have no [replays_ok] entry, so a gap is
      promoted to [Replayed] as soon as it is captured, and a
      [Not_applicable] row must have no entry at all;
    - the human-readable table in [docs/reference/history-corpus.md] must list
      exactly the same features with the same status.

    Feature tags are manifest identifiers ([a-z0-9-]). The table order is the
    order of the documentation table. *)

(** The negative-control verdicts a feature can be proved by. *)
type control =
  | Nondeterminism_control  (** Entries expected to report nondeterminism. *)
  | Invalid_history_control  (** Entries expected to be invalid histories. *)

(** The manifest verdict a control kind requires. *)
let control_verdict = function
  | Nondeterminism_control -> Corpus_manifest.Nondeterminism
  | Invalid_history_control -> Corpus_manifest.Invalid_history

(** How one feature is represented in the corpus. *)
type status =
  | Replayed
      (** At least one [replays_ok] entry carries the tag: a history recorded
          with the feature replays against its frozen definitions. *)
  | Rejected of control
      (** At least one entry with this negative-control verdict carries the
          tag. *)
  | Not_applicable of string
      (** Replay cannot exercise the feature; the reason says why. No entry
          may carry the tag. *)
  | Not_yet_captured of string
      (** A supported feature whose history is not in the corpus yet. The
          reason names what a capture must record; the gap is tracked by
          issue #503 and needs a live Temporal Server capture. No
          [replays_ok] entry may carry the tag. *)

(** One coverage row. *)
type row = { feature : string; status : status }

(** Shorthand constructors keep the table readable. *)
let replayed feature = { feature; status = Replayed }

(** A feature proved by a nondeterminism negative control. *)
let nondeterminism_control feature =
  { feature; status = Rejected Nondeterminism_control }

(** A feature proved by a malformed-history negative control. *)
let invalid_history_control feature =
  { feature; status = Rejected Invalid_history_control }

(** A feature replay cannot exercise, with the reason. *)
let not_applicable feature reason = { feature; status = Not_applicable reason }

(** A known corpus gap, with what its capture must record. *)
let not_yet_captured feature reason = { feature; status = Not_yet_captured reason }

(** The coverage table. Removing a row, or demoting a [Replayed] row, is a
    deliberate corpus-scope change and must be reflected in
    [docs/reference/history-corpus.md] (the documentation check enforces
    that the two agree). Queries are [Not_applicable]: they record no history
    events, so replaying a history never runs a query handler. *)
let table =
  [
    (* Workflow authoring: timers, conditions, child workflows and
       continue-as-new are core MVP candidates. *)
    replayed "timer";
    replayed "condition";
    replayed "child-workflow";
    replayed "child-side";
    replayed "continue-as-new";
    replayed "continue-as-new-successor";
    (* Timer cancellation, bounded conditions and child start options
       (#1134, #1135), captured by corpus-v2-cancellation. *)
    replayed "timer-cancellation";
    replayed "condition-timeout";
    replayed "child-start-options";
    (* Deterministic uuid4, random_bytes and random_float draws (#1155),
       captured by corpus-v3-random. *)
    replayed "deterministic-randomness";
    (* Remote activities. *)
    replayed "activity";
    replayed "activity-retry";
    (* Signals and updates (experimental). *)
    replayed "signal";
    replayed "signal-before-first-task";
    replayed "update";
    (* Typed workflow results and workflow-task failures. *)
    replayed "workflow-failure";
    replayed "non-retryable-failure";
    replayed "workflow-task-failure-recovery";
    replayed "missing-registration";
    (* Patching, including the #531 lifecycle boundaries. *)
    replayed "patch";
    replayed "patch-marker-free";
    replayed "patch-active";
    replayed "patch-deprecated";
    replayed "patch-removed";
    replayed "patch-multiple";
    replayed "patch-loop";
    replayed "patch-signal-handler";
    replayed "patch-update-handler";
    replayed "patch-mid-run-upgrade";
    replayed "patch-upgrade";
    replayed "patch-rollback";
    (* Negative controls: the gate must still detect incompatible code and
       refuse histories it cannot truly check. *)
    nondeterminism_control "negative-control";
    invalid_history_control "malformed-history";
    invalid_history_control "invalid-history-truncated-protobuf";
    invalid_history_control "invalid-history-event-id-gap";
    invalid_history_control "invalid-history-missing-task-queue";
    invalid_history_control "invalid-history-first-task-rejected";
    invalid_history_control "invalid-history-workflow-id-mismatch";
    (* Supported, but replay cannot exercise them. *)
    not_applicable "query"
      "Queries record no history events, so replay never runs a query \
       handler; the live completed-query regression covers them.";
    not_applicable "asynchronous-activity-completion"
      "An activity completed through its task token records the same \
       ActivityTaskCompleted event as a synchronous one, which the activity \
       cases replay.";
    not_applicable "payload-codecs"
      "Replay compares command shapes, not payload values; every case \
       decodes its recorded json/plain input and results, but codec \
       compatibility is checked by the codec and interoperability tests.";
    not_applicable "client-operations"
      "Client calls, start options and RPC deadlines run outside workflow \
       code; what they record in a history is listed under the \
       workflow-side features.";
    not_applicable "worker-configuration"
      "Worker resource, polling and shutdown options, metrics and logging \
       configure the worker process and record nothing in a history.";
    not_applicable "in-process-testing"
      "Temporal.Testing runs workflows against a simulator and produces no \
       Temporal Server history.";
    (* Supported or exported for evaluation, but no history captured yet.
       Each needs a live capture against Temporal Server (#503). *)
    not_yet_captured "workflow-cancellation"
      "A cancellation request delivered to a running workflow and a run \
       that completes as cancelled.";
    not_yet_captured "activity-cancellation"
      "Activity cancellation by handle or scope \
       (ActivityTaskCancelRequested and ActivityTaskCanceled).";
    not_yet_captured "child-workflow-cancellation"
      "Child workflow cancellation by handle or scope.";
    not_yet_captured "activity-timeout"
      "An activity attempt ending in a start-to-close, schedule-to-close or \
       heartbeat timeout (ActivityTaskTimedOut).";
    not_yet_captured "workflow-timeout"
      "A run ended by its execution or run timeout.";
    not_yet_captured "workflow-retry"
      "A workflow retry run, whose recorded retry policy, attempt and \
       predecessor workflow code reads through Workflow.Info.";
    not_yet_captured "local-activity"
      "A local activity and its marker, including a Core-directed backoff \
       timer.";
    not_yet_captured "external-signal"
      "Signalling another workflow from workflow code \
       (SignalExternalWorkflowExecutionInitiated).";
    not_yet_captured "external-cancellation"
      "Requesting cancellation of another workflow from workflow code \
       (RequestCancelExternalWorkflowExecutionInitiated).";
    not_yet_captured "start-metadata"
      "A start memo and search attributes read through \
       Workflow.start_metadata.";
    not_yet_captured "search-attribute-upsert"
      "Workflow.upsert_search_attributes \
       (UpsertWorkflowSearchAttributes).";
    not_yet_captured "deterministic-time-random"
      "Commands that depend on Workflow.now or Workflow.random_int.";
    not_yet_captured "deployment-version"
      "A run on a deployment-versioned worker whose commands branch on \
       Workflow.current_deployment_version.";
  ]

(** The status word used in the documentation table. *)
let status_label = function
  | Replayed -> "replayed"
  | Rejected Nondeterminism_control -> "nondeterminism control"
  | Rejected Invalid_history_control -> "invalid-history control"
  | Not_applicable _ -> "not applicable"
  | Not_yet_captured _ -> "not yet captured"

(** The manifest spelling of a verdict, for diagnostics. *)
let verdict_label = function
  | Corpus_manifest.Replays_ok -> "replays_ok"
  | Corpus_manifest.Nondeterminism -> "nondeterminism"
  | Corpus_manifest.Invalid_history -> "invalid_history"

(** IDs of the entries carrying [feature] whose verdict satisfies
    [verdict], in manifest order. *)
let entries_with ~verdict feature (entries : Corpus_manifest.entry list) =
  List.filter_map
    (fun (entry : Corpus_manifest.entry) ->
      if verdict entry.expected && List.mem feature entry.features then
        Some entry.id
      else None)
    entries

(** The cases that satisfy a row, for the report: the matching entries for
    [Replayed] and [Rejected] rows, and none for the others. *)
let cases row entries =
  match row.status with
  | Replayed ->
      entries_with ~verdict:(( = ) Corpus_manifest.Replays_ok) row.feature entries
  | Rejected control ->
      entries_with ~verdict:(( = ) (control_verdict control)) row.feature entries
  | Not_applicable _ | Not_yet_captured _ -> []

(** Checks [table] against the manifest entries and returns every failure,
    each naming the feature (and entry, where one is involved). *)
let check ?(table = table) (entries : Corpus_manifest.entry list) =
  let problems = ref [] in
  let problem format = Printf.ksprintf (fun text -> problems := text :: !problems) format in
  let seen = Hashtbl.create 64 in
  List.iter
    (fun row ->
      if Hashtbl.mem seen row.feature then
        problem "feature coverage table lists %s twice" row.feature;
      Hashtbl.replace seen row.feature ())
    table;
  (* Every tag in the manifest must be classified. *)
  let unclassified = Hashtbl.create 8 in
  List.iter
    (fun (entry : Corpus_manifest.entry) ->
      List.iter
        (fun feature ->
          if (not (Hashtbl.mem seen feature)) && not (Hashtbl.mem unclassified feature)
          then (
            Hashtbl.replace unclassified feature ();
            problem
              "entry %s: feature %s has no row in the corpus feature coverage \
               table"
              entry.id feature))
        entry.features)
    entries;
  List.iter
    (fun row ->
      let any = entries_with ~verdict:(fun _ -> true) row.feature entries in
      let ok =
        entries_with ~verdict:(( = ) Corpus_manifest.Replays_ok) row.feature entries
      in
      match row.status with
      | Replayed ->
          if ok = [] then problem "no replays_ok entry covers feature %s" row.feature
      | Rejected control ->
          if cases row entries = [] then
            problem "no %s negative control covers feature %s"
              (verdict_label (control_verdict control))
              row.feature
      | Not_yet_captured _ -> (
          match ok with
          | [] -> ()
          | id :: _ ->
              problem
                "feature %s is listed as not yet captured, but entry %s \
                 replays it; mark it replayed"
                row.feature id)
      | Not_applicable _ -> (
          match any with
          | [] -> ()
          | id :: _ ->
              problem "feature %s is marked not applicable, but entry %s is \
                       tagged with it"
                row.feature id))
    table;
  List.rev !problems

(** Splits a Markdown table row into trimmed cells, without the outer
    pipes. Returns [None] for a line that is not a table row. *)
let cells line =
  let line = String.trim line in
  let length = String.length line in
  if length < 2 || line.[0] <> '|' || line.[length - 1] <> '|' then None
  else
    Some
      (String.sub line 1 (length - 2)
      |> String.split_on_char '|' |> List.map String.trim)

(** Reads the [(feature, status label)] rows of the "Feature coverage" table
    in the corpus reference document: the Markdown rows, after the
    [## Feature coverage] heading and before the next [## ] heading, whose
    first cell is one backquoted feature tag. *)
let documented_rows text =
  let lines = String.split_on_char '\n' text in
  let rec skip_to_section = function
    | [] -> []
    | line :: rest ->
        if String.trim line = "## Feature coverage" then rest else skip_to_section rest
  in
  let rec rows acc = function
    | [] -> List.rev acc
    | line :: _ when String.starts_with ~prefix:"## " line -> List.rev acc
    | line :: rest -> (
        match cells line with
        | Some (first :: status :: _)
          when String.length first > 2
               && first.[0] = '`'
               && first.[String.length first - 1] = '`' ->
            rows ((String.sub first 1 (String.length first - 2), status) :: acc) rest
        | _ -> rows acc rest)
  in
  rows [] (skip_to_section lines)

(** Checks that the document at [path] lists exactly the features of
    [table], in the same order, with the same status labels. Returns every
    difference. *)
let check_document ?(table = table) path =
  match In_channel.with_open_bin path In_channel.input_all with
  | exception Sys_error message -> [ "feature coverage document: " ^ message ]
  | text ->
      let documented = documented_rows text in
      let expected = List.map (fun row -> (row.feature, status_label row.status)) table in
      if documented = [] then
        [ path ^ ": no \"## Feature coverage\" table with feature rows" ]
      else
        let missing =
          List.filter_map
            (fun (feature, status) ->
              match List.assoc_opt feature documented with
              | None -> Some (Printf.sprintf "%s: feature %s is not documented" path feature)
              | Some documented_status when documented_status <> status ->
                  Some
                    (Printf.sprintf "%s: feature %s is documented as %S, the runner \
                                     says %S"
                       path feature documented_status status)
              | Some _ -> None)
            expected
        in
        let extra =
          List.filter_map
            (fun (feature, _) ->
              if List.mem_assoc feature expected then None
              else
                Some
                  (Printf.sprintf "%s: documented feature %s is not in the runner's \
                                   coverage table"
                     path feature))
            documented
        in
        let order =
          if missing = [] && extra = [] && List.map fst documented <> List.map fst expected
          then [ path ^ ": feature coverage rows are not in the runner's table order" ]
          else []
        in
        missing @ extra @ order

(** The report's [feature_coverage] member: one object per row with its
    status label, the satisfying case IDs, and the reason for a row without
    cases. *)
let report_json entries =
  `List
    (List.map
       (fun row ->
         `Assoc
           [
             ("feature", `String row.feature);
             ("status", `String (status_label row.status));
             ("cases", `List (List.map (fun id -> `String id) (cases row entries)));
             ( "reason",
               match row.status with
               | Not_applicable reason | Not_yet_captured reason -> `String reason
               | Replayed | Rejected _ -> `Null );
           ])
       table)

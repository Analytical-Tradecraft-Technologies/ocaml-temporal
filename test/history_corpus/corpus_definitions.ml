(** Frozen definitions behind the replay history corpus. See the interface for
    the immutability rule: a definition used by a corpus entry must never
    change, because the corpus proves that a candidate SDK still replays
    histories recorded by this exact application code. *)

type workflow =
  | Workflow : {
      definition : ('input, 'output) Temporal.Workflow.t;
      signals : Temporal.Signal.Handler.t list;
      queries : Temporal.Query.Handler.t list;
      updates : Temporal.Update.Handler.t list;
    }
      -> workflow

(** Corpus-only task queue. *)
let task_queue = "ocaml-temporal-history-corpus"

(** Recorded workflow type names and patch ID; see the interface. *)
let activity_workflow_type = "corpus.activity"
let timer_workflow_type = "corpus.timer"
let activity_retry_workflow_type = "corpus.activity_retry"
let interaction_workflow_type = "corpus.interaction"
let parent_workflow_type = "corpus.parent"
let child_workflow_type = "corpus.child"
let continue_as_new_workflow_type = "corpus.continue_as_new"
let patch_workflow_type = "corpus.patch"
let patch_id = "corpus.patch.activity.v1"

(** Packs a workflow without handlers. *)
let plain definition =
  Workflow { definition; signals = []; queries = []; updates = [] }

(** Short durable timer shared by every timer-bearing corpus workflow. It is a
    Temporal command, so its duration is recorded in history; 100 ms keeps a
    live capture fast without racing the first workflow task. *)
let corpus_timer = Temporal.Duration.of_ms 100L

(** {1 Activities} *)

(** Uppercases its input. Deterministic, so the recorded result is stable
    across captures. *)
let echo_activity =
  Temporal.Activity.define ~name:"corpus.echo" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string (fun input ->
      Ok (String.uppercase_ascii input))

(** Fails its first attempt with a retryable error and succeeds on any later
    attempt. The attempt number comes from the server-delivered task metadata,
    not from process state, so the failure is reproducible on every capture. *)
let flaky_activity =
  Temporal.Activity.define_with_context ~name:"corpus.flaky"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string
    (fun context input ->
      match Temporal.Activity.Context.info context with
      | Error error -> Error error
      | Ok info ->
          let attempt = Temporal.Activity.Info.attempt info in
          if attempt < 2 then
            Error
              (Temporal.Error.make ~category:`Activity
                 ~message:"intentional first-attempt failure" ())
          else Ok (Printf.sprintf "%s:ATTEMPT:%d" input attempt))

(** The activity scheduled by the pre-patch [corpus.patch] generation. *)
let patch_legacy_activity =
  Temporal.Activity.define ~name:"corpus.patch.legacy_activity"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      Ok "PATCH:LEGACY")

(** The activity scheduled by the patched and deprecated generations. *)
let patch_new_activity =
  Temporal.Activity.define ~name:"corpus.patch.new_activity"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      Ok "PATCH:NEW")

(** Every corpus activity, so any capture generation can serve them. *)
let activities =
  Temporal.Worker.
    [
      activity echo_activity;
      activity flaky_activity;
      activity patch_legacy_activity;
      activity patch_new_activity;
    ]

(** {1 Workflows} *)

(** [corpus.activity]: one activity with an explicit start-to-close timeout. *)
let activity_workflow =
  Temporal.Workflow.define ~name:activity_workflow_type
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Temporal.Activity.execute
        ~start_to_close_timeout:(Temporal.Duration.of_ms 10_000L)
        echo_activity input)

(** [corpus.timer]: one durable timer, then a constant result. *)
let timer_workflow =
  Temporal.Workflow.define ~name:timer_workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      match Temporal.Workflow.sleep corpus_timer with
      | Error error -> Error error
      | Ok () -> Ok "TIMER:FIRED")

(** The negative control: the same workflow type as [timer_workflow], with the
    timer removed. A recorded [corpus.timer] history contains [TimerStarted],
    so Core must report this definition as nondeterministic. This value is
    never registered by a capture worker. *)
let timer_removed_workflow =
  Temporal.Workflow.define ~name:timer_workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () -> Ok "TIMER:FIRED")

(** Bounded retry policy for [activity_retry_workflow]: a 100 ms constant
    backoff and three attempts. The activity needs exactly two. A constructor
    error is returned from the workflow as a typed defect. *)
let retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 100L) ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 100L) ~maximum_attempts:3 ()

(** [corpus.activity_retry]: [flaky_activity] under [retry_policy]. *)
let activity_retry_workflow =
  Temporal.Workflow.define ~name:activity_retry_workflow_type
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      match retry_policy with
      | Error error -> Error error
      | Ok retry_policy ->
          Temporal.Activity.execute ~retry_policy
            ~start_to_close_timeout:(Temporal.Duration.of_ms 10_000L)
            flaky_activity input)

(** Signal definition shared with the capture client. *)
let interaction_signal =
  Temporal.Signal.define ~name:"corpus.interaction.signal"
    ~input:Temporal.Codec.string

(** Update definition shared with the capture client. *)
let interaction_update =
  Temporal.Update.define ~name:"corpus.interaction.update"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string

(** Output-only query definition shared with the capture client. *)
let interaction_query =
  Temporal.Query.define ~name:"corpus.interaction.status"
    ~output:Temporal.Codec.string

(** Per-run state written by the signal handler. *)
let signal_value = Temporal.Workflow_context.Local.create ()

(** Per-run state written by the update handler. *)
let update_value = Temporal.Workflow_context.Local.create ()

(** Reads one optional per-run value, mapping a context error to a typed
    workflow error. *)
let read_local key = Temporal.Workflow_context.Local.get key

(** [corpus.interaction]: parks until both the signal and the update have
    been handled, then joins the seed and both values. *)
let interaction_workflow =
  Temporal.Workflow.define ~name:interaction_workflow_type
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      let open Temporal.Result_syntax in
      let* () =
        Temporal.Condition.wait_until_result (fun () ->
            let* signal = read_local signal_value in
            let* update = read_local update_value in
            Ok (Option.is_some signal && Option.is_some update))
      in
      let* signal = read_local signal_value in
      let* update = read_local update_value in
      match (signal, update) with
      | Some signal, Some update ->
          Ok (String.concat ":" [ seed; signal; update ])
      | _ ->
          Error
            (Temporal.Error.defect
               ~message:"interaction condition resumed without both values"))

(** Records the signal value; deterministic and I/O-free. *)
let interaction_signal_handler =
  Temporal.Signal.Handler.make interaction_signal (fun value ->
      Temporal.Workflow_context.Local.set signal_value value)

(** Records the update value and acknowledges it with a distinct result. *)
let interaction_update_handler =
  Temporal.Update.Handler.make interaction_update (fun value ->
      match Temporal.Workflow_context.Local.set update_value value with
      | Error error -> Error error
      | Ok () -> Ok ("UPDATE:" ^ value))

(** Reports which interactions the parked workflow has observed. Queries do
    not produce history events; the handler is registered so a capture can
    prove the query path ran against the same execution. *)
let interaction_query_handler =
  Temporal.Query.Handler.make interaction_query (fun () ->
      let open Temporal.Result_syntax in
      let* signal = read_local signal_value in
      let* update = read_local update_value in
      let flag = function Some _ -> "1" | None -> "0" in
      Ok (Printf.sprintf "signal=%s update=%s" (flag signal) (flag update)))

(** [corpus.child]: returns its uppercased input without commands. *)
let child_workflow =
  Temporal.Workflow.define ~name:child_workflow_type
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Ok ("CHILD:" ^ String.uppercase_ascii input))

(** The child ID is derived only from the parent's input, so the start-child
    command is identical on every replay. *)
let parent_workflow =
  Temporal.Workflow.define ~name:parent_workflow_type
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Temporal.Child_workflow.execute ~id:("history-corpus-child-" ^ input)
        child_workflow input)

(** Command-only reference used to name the continue-as-new successor without
    a recursive value. *)
let continue_as_new_target =
  Temporal.Workflow.remote ~name:continue_as_new_workflow_type
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string

(** [corpus.continue_as_new]: ["first"] continues as new with ["second"],
    which completes; any other input is a defect. *)
let continue_as_new_workflow =
  Temporal.Workflow.define ~name:continue_as_new_workflow_type
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (function
    | "first" -> Temporal.Workflow.continue_as_new continue_as_new_target "second"
    | "second" -> Ok "CONTINUED:SECOND"
    | input ->
        Error
          (Temporal.Error.defect
             ~message:(Printf.sprintf "unexpected continue-as-new input %S" input)))

(** Runs one patch-generation activity with the corpus activity timeout. *)
let run_patch_activity activity =
  Temporal.Activity.execute
    ~start_to_close_timeout:(Temporal.Duration.of_ms 10_000L)
    activity ()

(** Generation 1: before the patch existed. Its histories carry no marker. *)
let patch_legacy_workflow =
  Temporal.Workflow.define ~name:patch_workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      match Temporal.Workflow.sleep corpus_timer with
      | Error error -> Error error
      | Ok () -> run_patch_activity patch_legacy_activity)

(** Generation 2: the active patch. The decision precedes the timer, so a
    marker-free legacy history still replays (the patch reports [false]),
    while a new execution records an active marker and takes the new branch. *)
let patch_patched_workflow =
  Temporal.Workflow.define ~name:patch_workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let use_new = Temporal.Workflow.patched ~id:patch_id in
      match Temporal.Workflow.sleep corpus_timer with
      | Error error -> Error error
      | Ok () ->
          run_patch_activity
            (if use_new then patch_new_activity else patch_legacy_activity))

(** Generation 3: the deprecated patch. The new branch is unconditional; a
    new execution records a deprecated marker, and histories with an active
    marker still replay. *)
let patch_deprecated_workflow =
  Temporal.Workflow.define ~name:patch_workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      Temporal.Workflow.deprecate_patch ~id:patch_id;
      match Temporal.Workflow.sleep corpus_timer with
      | Error error -> Error error
      | Ok () -> run_patch_activity patch_new_activity)

(** Workflows that do not vary between corpus-v1 generations. *)
let stable_workflows =
  [
    plain activity_workflow;
    plain timer_workflow;
    plain activity_retry_workflow;
    Workflow
      {
        definition = interaction_workflow;
        signals = [ interaction_signal_handler ];
        queries = [ interaction_query_handler ];
        updates = [ interaction_update_handler ];
      };
    plain parent_workflow;
    plain child_workflow;
    plain continue_as_new_workflow;
  ]

(** {1 Reused fixture definitions} *)

(** The corrected generation of [test/integration/temporal/task_failure]. The
    business workflows come from that fixture's shared library unchanged; the
    repaired and missing workflows are restated from [corrected_worker.ml],
    which is an executable and cannot be linked. They must stay identical to
    that file for the retained histories to remain meaningful. *)
let task_failure_corrected =
  let repaired name =
    Temporal.Workflow.define ~name:(Failure_support.workflow_type name)
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
        let open Temporal.Result_syntax in
        let* () = Failure_support.history_boundary () in
        Ok "recovered")
  in
  let missing =
    Temporal.Workflow.define ~name:(Failure_support.workflow_type "missing")
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
        Ok "recovered")
  in
  [
    plain (repaired "body");
    plain (repaired "encoder");
    plain missing;
    plain (Failure_support.business "business-retryable" false);
    plain (Failure_support.business "business-permanent" true);
  ]

(** Values observed by the [initial-signals] workflow's [record] handler. *)
let recorded_signals = Temporal.Workflow_context.Local.create ()

(** Public-API restatement of the synthetic
    [test/integration/temporal/initial_signals] workflow: it returns the
    comma-joined values of every [record] signal seen before the root ran,
    without suspending. *)
let initial_signals =
  let record =
    Temporal.Signal.define ~name:"record" ~input:Temporal.Codec.string
  in
  let handler =
    Temporal.Signal.Handler.make record (fun value ->
        match Temporal.Workflow_context.Local.get recorded_signals with
        | Error error -> Error error
        | Ok previous ->
            Temporal.Workflow_context.Local.set recorded_signals
              (Option.value ~default:[] previous @ [ value ]))
  in
  let definition =
    Temporal.Workflow.define ~name:"initial-signals" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.string (fun () ->
        match Temporal.Workflow_context.Local.get recorded_signals with
        | Error error -> Error error
        | Ok values ->
            Ok (String.concat "," (Option.value ~default:[] values)))
  in
  [ Workflow { definition; signals = [ handler ]; queries = []; updates = [] } ]

(** Named definition sets; see the interface for each set's purpose. *)
let definition_sets =
  [
    ("corpus-v1", stable_workflows @ [ plain patch_patched_workflow ]);
    ("corpus-v1-patch-legacy", [ plain patch_legacy_workflow ]);
    ("corpus-v1-patch-deprecated", [ plain patch_deprecated_workflow ]);
    ("negative-timer-removed", [ plain timer_removed_workflow ]);
    ("task-failure-corrected", task_failure_corrected);
    ("initial-signals", initial_signals);
  ]

(** Set names in declaration order. *)
let definition_set_names = List.map fst definition_sets

(** Looks up one definition set by name. *)
let definition_set name = List.assoc_opt name definition_sets

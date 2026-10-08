(** Frozen workflow and activity definitions for the replay history corpus.

    Every history in [test/fixtures/history-corpus] was produced by one of the
    definition sets below, and the corpus test replays it against the same (or
    a deliberately incompatible) set. The definitions are therefore part of the
    corpus evidence: once a corpus entry names a workflow type, its body must
    never change. A behavior change is added as a new workflow type or a new
    definition set, never as an edit, so a replay failure always identifies an
    SDK or Core regression rather than a fixture edit.

    The workflows use only the public [Temporal] API, contain no I/O, clock,
    randomness, or process-global state, and are safe for Temporal replay.
    Activity callbacks are only invoked by the live capture worker; replay
    resolves activity results from history. *)

(** One workflow registration, independent of the worker kind that consumes
    it. The live capture worker turns it into a [Temporal.Worker] registration;
    the Docker-free corpus test turns it into a private replay registration.
    Keeping the handlers beside the definition guarantees that capture and
    replay attach the same signal, query, and update callbacks. *)
type workflow =
  | Workflow : {
      definition : ('input, 'output) Temporal.Workflow.t;
      signals : Temporal.Signal.Handler.t list;
      queries : Temporal.Query.Handler.t list;
      updates : Temporal.Update.Handler.t list;
    }
      -> workflow

(** Task queue used only by the corpus capture worker. It never matches a
    production queue, so a reused namespace cannot dispatch corpus work to an
    application worker. *)
val task_queue : string

(** {1 Workflow types}

    The Temporal workflow type names recorded in corpus histories. *)

(** [corpus.activity]: schedules one activity and returns its result. *)
val activity_workflow_type : string

(** [corpus.timer]: waits on one durable timer and returns a constant. *)
val timer_workflow_type : string

(** [corpus.activity_retry]: one activity that fails its first attempt and
    succeeds on the retry permitted by an explicit retry policy. *)
val activity_retry_workflow_type : string

(** [corpus.interaction]: parks on a condition satisfied by one signal and one
    update; a query reads the parked state. *)
val interaction_workflow_type : string

(** [corpus.parent] and [corpus.child]: a parent that executes one child
    workflow and returns the child's result. *)
val parent_workflow_type : string

(** See {!parent_workflow_type}. *)
val child_workflow_type : string

(** [corpus.continue_as_new]: continues once, then completes in the successor
    run. *)
val continue_as_new_workflow_type : string

(** [corpus.patch]: the workflow type shared by the three patch generations.
    The patch identifier is {!patch_id}. *)
val patch_workflow_type : string

(** The patch identifier used by [corpus.patch]'s patched and deprecated
    generations. *)
val patch_id : string

(** {1 Interaction handler names}

    Used by the capture driver to send the interactions recorded in history. *)

(** Signal setting the interaction workflow's first value. *)
val interaction_signal : string Temporal.Signal.t

(** Update setting the interaction workflow's second value. *)
val interaction_update : (string, string) Temporal.Update.definition

(** Query reading the interaction workflow's parked state. *)
val interaction_query : string Temporal.Query.definition

(** {1 Definition sets}

    A definition set is the complete set of workflow registrations that one
    worker generation (and the matching replay) uses. Set names are recorded
    in the corpus manifest's [replay_definitions] and capture [generation]
    fields, so they are stable identifiers. *)

(** Names of every known definition set, in a stable order. *)
val definition_set_names : string list

(** Returns the workflows of one definition set, or [None] for an unknown set
    name. Sets:
    - ["corpus-v1"]: every current corpus workflow, with the patched
      [corpus.patch] generation that calls [Temporal.Workflow.patched].
    - ["corpus-v1-patch-legacy"]: [corpus.patch] before the patch existed.
    - ["corpus-v1-patch-deprecated"]: [corpus.patch] after the patch was
      deprecated with [Temporal.Workflow.deprecate_patch].
    - ["negative-timer-removed"]: an intentionally incompatible [corpus.timer]
      that returns without starting its timer. Replaying a [corpus.timer]
      history against it must report nondeterminism.
    - ["task-failure-corrected"]: the corrected generation of the
      [test/integration/temporal/task_failure] fixture, reused for its
      retained live histories.
    - ["initial-signals"]: the [initial-signals] workflow of the synthetic
      [test/integration/temporal/initial_signals] fixture. *)
val definition_set : string -> workflow list option

(** Activities registered by the live capture worker. Every activity used by a
    corpus workflow is listed so any capture generation can serve them. *)
val activities : Temporal.Worker.registered_activity list

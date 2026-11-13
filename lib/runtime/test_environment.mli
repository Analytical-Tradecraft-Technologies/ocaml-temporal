(** Deterministic in-process workflow test engine.

    This module drives the same {!Execution} runtime that the native worker
    uses, but replaces Temporal Server and Core with a small single-threaded
    simulator. It interprets the commands an execution emits (activities,
    timers, child workflows, external signals and cancellations, queries,
    updates, continue-as-new, and terminal commands) and feeds the matching
    activation jobs back into the owning execution. Time is virtual: it only
    moves when a caller asks for a workflow result or update that cannot make
    progress otherwise, or when the caller skips time explicitly, so a
    workflow that sleeps for a day completes immediately.

    The engine is the private implementation behind [Temporal.Testing]. It is
    deliberately an approximation of the server, not a replay of real history:
    an activity attempt takes the virtual time its registration declares,
    workflow code is replayed only to retry a failed workflow task in
    {!Retry_task} mode, and every identifier is a deterministic counter so
    two runs of the same test observe the same values. Where it does
    simulate a behavior it follows the native worker and server: jobs reach
    an execution in Core's activation order, activity and child failures
    carry the native error shape, activity timeouts and workflow execution
    and run timeouts expire in virtual time, retry policies, workflow ID
    reuse and conflict policies are applied, asynchronously completed
    activities wait for their handles, and commands that a server would
    never dispatch because the same activation closed the run are
    dropped.

    Threading: an environment, its handles, and every workflow and activity
    callback it invokes run on the caller's system thread. An environment must
    not be shared between threads or Domains, and workflow or activity code
    must not call back into the environment that is running it. *)

(** A workflow registration. The existential package keeps the definition's
    codecs paired with its implementation and its private interaction
    handlers. *)
type workflow

(** Packs a local workflow definition for {!create}. [override] marks a
    replacement registration: it replaces the ordinary registration with the
    same name, wherever either appears in the list, instead of being
    reported as a duplicate, which lets a test swap a child workflow for a
    stub while reusing an application's registration list. Two ordinary
    registrations, or two replacements, with one name are a defect. *)
val workflow :
  ?override:bool ->
  ?signal_handlers:Execution.signal_handler list ->
  ?query_handlers:Execution.query_handler list ->
  ?update_handlers:Execution.update_handler list ->
  ( 'input,
    'output,
    'input -> ('output, Temporal_base.Error.t) result )
  Temporal_base.Definition.t ->
  workflow

(** An activity registration: executable code, or a named placeholder that
    fails every attempt with a fixed, non-retryable diagnostic. *)
type activity

(** Packs a synchronous activity whose callback receives an attempt context.
    [override] has the same replacement meaning as on {!val-workflow}.
    [durations_ms] gives the virtual milliseconds each attempt takes, in
    attempt order, with the last entry repeating; each must be
    non-negative. Without it every attempt takes zero virtual time, so no
    activity timeout can expire. *)
val activity :
  ?override:bool ->
  ?durations_ms:int64 list ->
  ( 'input,
    'output,
    Temporal_base.Activity_context.t ->
    'input ->
    ('output, Temporal_base.Error.t) result )
  Temporal_base.Definition.t ->
  activity

(** Packs an asynchronous activity (#1160). Each attempt runs the callback
    with a fresh handle whose operations the engine applies as Temporal
    Server would: an attempt whose callback returns [Completed] or [Failed]
    takes its [durations_ms] entry like {!val-activity}, and one that returns
    [Will_complete_async] stays pending until a test completes, fails, or
    cancels the handle, or one of its timeouts expires in virtual time
    (each heartbeat restarts the heartbeat timeout). The handle's
    operations may be called only from the environment's thread, between
    environment operations; their effect reaches the workflow when the
    environment next runs. A local activity cannot be completed
    asynchronously, so that outcome fails the attempt non-retryably, as on
    the native worker. *)
val async_activity :
  ?override:bool ->
  ?durations_ms:int64 list ->
  ( 'input,
    'output,
    'output Temporal_base.Async_activity.context ->
    'input ->
    'output Temporal_base.Async_activity.async_result )
  Temporal_base.Definition.t ->
  activity

(** Registers a name whose attempts always fail with [reason]. The public
    adapter uses it for definitions the engine cannot run, such as remote
    references, so the failure names the cause instead of reporting an
    unregistered activity. *)
val unsupported_activity : ?override:bool -> name:string -> reason:string -> unit -> activity

(** What the environment does with a workflow task failure (#1160): an
    exception, a [Defect], [Codec], or [Bridge] error from workflow code, or
    an unhandled signal. [Fail_run] closes the run with that error, so the
    test sees it at once. [Retry_task] keeps the run open with the task
    failing, as a server does, until {!retry_workflow_tasks} rebuilds its
    execution by replaying its completed activations and delivers the failed
    task's jobs again, letting a test fix the state the workflow reads in
    between. *)
type task_failure_mode = Fail_run | Retry_task

(** What {!start} does when its workflow ID has an open chain: return
    Temporal's already-started error, attach to the open chain, or
    terminate it and start a new one. *)
type id_conflict_policy =
  | Conflict_fail
  | Conflict_use_existing
  | Conflict_terminate_existing

(** One simulated namespace, task queue, virtual clock, and set of
    workflow executions. *)
type t

(** A workflow execution chain started by {!start}. It follows
    continue-as-new successors, so {!result} reports the outcome of the last
    run in the chain. *)
type handle

(** Creates an environment. [start_time_ms] is the initial virtual time in
    milliseconds since the Unix epoch; it defaults to
    2024-01-01T00:00:00Z and must be non-negative. A retry policy with
    unlimited attempts (including the server default used when a workflow
    supplies none) is capped at [max_activity_attempts] attempts, default
    [10], so a permanently failing activity cannot spin the virtual clock
    forever; the same cap bounds the runs of a child workflow whose retry
    policy is unlimited. Duplicate registration names (see
    {!val-workflow}), an invalid
    namespace or task queue, and a non-positive attempt cap return defects.
    [task_failure_mode] defaults to {!Fail_run}. *)
val create :
  ?namespace:string ->
  ?task_queue:string ->
  ?start_time_ms:int64 ->
  ?max_activity_attempts:int ->
  ?task_failure_mode:task_failure_mode ->
  workflows:workflow list ->
  activities:activity list ->
  unit ->
  (t, Temporal_base.Error.t) result

(** Starts a registered workflow and runs every execution until none can make
    progress without virtual time passing. [workflow_id] defaults to a
    deterministic ["workflow-<n>"]. An unknown workflow type or an input the
    registered codec rejects returns an error and starts nothing.

    [memo] and [search_attributes] are the first run's start maps. The
    timeouts are in milliseconds: the execution and run timeouts are
    enforced in virtual time, and the task timeout is only reported by
    [Workflow.info]. [retry_policy] retries a failed or timed-out run as the
    server does. When [workflow_id] has an open chain, [id_conflict_policy]
    (default {!Conflict_fail}) decides; when its latest chain is closed,
    [id_reuse_policy] (default: allow) decides. A refused start returns
    [already_started] applied to the ID and the conflicting run, which the
    public adapter builds as the client's typed already-started error. *)
val start :
  ?workflow_id:string ->
  ?memo:(string * Temporal_base.Codec.payload) list ->
  ?search_attributes:(string * Temporal_base.Codec.payload) list ->
  ?id_reuse_policy:Activation.child_workflow_id_reuse_policy ->
  ?id_conflict_policy:id_conflict_policy ->
  ?execution_timeout_ms:int64 ->
  ?run_timeout_ms:int64 ->
  ?task_timeout_ms:int64 ->
  ?retry_policy:Activation.retry_policy ->
  already_started:(workflow_id:string -> run_id:string -> Temporal_base.Error.t) ->
  t ->
  workflow_type:string ->
  input:Temporal_base.Codec.payload ->
  (handle, Temporal_base.Error.t) result

(** Returns [false] when {!start} attached to an open chain under
    {!Conflict_use_existing}, and [true] when it started one. *)
val started : handle -> bool

(** Returns the failure of the chain's current workflow task when it is
    failing in {!Retry_task} mode, and [None] otherwise. *)
val workflow_task_failure : handle -> Temporal_base.Error.t option

(** Retries every failing workflow task in the environment, in run creation
    order: each run's execution is rebuilt by replaying its completed
    activations, the failed task's jobs are delivered again together with
    every job that arrived meanwhile, and the environment runs until no
    execution can make progress without virtual time passing. A task that
    fails again, during the replay or after it, leaves its run failing with
    the new error. Returns a defect when no workflow task is failing. *)
val retry_workflow_tasks : t -> (unit, Temporal_base.Error.t) result

(** Returns the workflow ID shared by every run of the chain. *)
val workflow_id : handle -> string

(** Returns the ID of the chain's current (latest) run. *)
val run_id : handle -> string

(** Returns the current run's memo (#1154): the memo it started with, after
    its [Workflow.upsert_memo] merges, sorted by key, with copied payloads.
    A continue-as-new successor starts with its predecessor's merged memo
    once that run upserted one, and with the predecessor's start memo
    otherwise, as Core does. *)
val memo : handle -> (string * Temporal_base.Codec.payload) list

(** Returns the current run's search attributes after its upserts, sorted by
    key, with copied payloads. A continue-as-new successor inherits them. *)
val search_attributes : handle -> (string * Temporal_base.Codec.payload) list

(** Runs the environment, skipping virtual time to the next pending timer or
    activity retry whenever every execution is blocked, until the chain
    closes or, in {!Retry_task} mode, its current workflow task is failing.
    Returns the encoded output, or the workflow's failure, cancellation,
    termination, timeout, or task-failure error; a failing task leaves the
    chain open. Returns a defect when
    the chain is blocked with nothing scheduled (for example, waiting for a
    signal nobody sends) and a [Timeout] error when the next scheduled event
    lies more than [timeout_ms] of virtual time after the call started. A
    deadline past the last representable instant ([Int64.max_int]
    milliseconds) saturates there; once the clock has reached that instant
    and another event is due at it, time can no longer advance, so the call
    returns [Timeout] instead of running forever. *)
val result :
  ?timeout_ms:int64 -> handle -> (Temporal_base.Codec.payload, Temporal_base.Error.t) result

(** Delivers one signal to the chain's current run and runs every execution
    until none can make progress without virtual time passing. Signalling a
    closed chain returns an error. *)
val signal :
  handle ->
  name:string ->
  input:Temporal_base.Codec.payload list ->
  (unit, Temporal_base.Error.t) result

(** Runs pending work, then answers one query from the chain's current run in
    a query-only activation, which does not count toward the run's history
    length. Closed runs remain queryable. A run whose workflow task is
    failing is first rebuilt from its completed activations, so the query
    sees the state before the failing task. *)
val query :
  handle ->
  name:string ->
  arguments:Temporal_base.Codec.payload list ->
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result

(** Delivers one update with validation enabled and runs the environment,
    skipping virtual time like {!result}, until the update completes or is
    rejected. Returns the encoded result or the rejection; a chain that closes
    first, or that blocks with nothing scheduled, returns an error, and a
    failing workflow task returns its failure while the update stays queued
    for the retried task. *)
val update :
  ?timeout_ms:int64 ->
  handle ->
  name:string ->
  input:Temporal_base.Codec.payload list ->
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result

(** Requests cancellation of the chain's current run and runs the environment
    until no execution can make progress without virtual time passing. *)
val cancel : handle -> (unit, Temporal_base.Error.t) result

(** Advances virtual time by [milliseconds], firing every timer and activity
    retry that falls due in time order and running the executions they wake.
    The clock saturates at [Int64.max_int] milliseconds rather than
    wrapping. A negative value returns a defect and changes nothing. *)
val skip : t -> milliseconds:int64 -> (unit, Temporal_base.Error.t) result

(** Returns the current virtual time in milliseconds since the Unix epoch. *)
val now_ms : t -> int64

(** Returns the namespace runs report through [Workflow.info]. *)
val namespace : t -> string

(** Releases every execution's paused fibers and pending operation tables.
    Idempotent; later operations on the environment or its handles return a
    defect. *)
val shutdown : t -> unit

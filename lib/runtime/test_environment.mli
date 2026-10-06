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
    activities complete in zero virtual time, activity timeouts are not
    enforced, workflow task retries are not simulated (a task failure ends the
    run with that error), and every identifier is a deterministic counter so
    two runs of the same test observe the same values.

    Threading: an environment, its handles, and every workflow and activity
    callback it invokes run on the caller's system thread. An environment must
    not be shared between threads or Domains, and workflow or activity code
    must not call back into the environment that is running it. *)

(** A workflow registration. The existential package keeps the definition's
    codecs paired with its implementation and its private interaction
    handlers. *)
type workflow

(** Packs a local workflow definition for {!create}. [override] marks a
    replacement registration: it replaces an earlier registration with the
    same name instead of being reported as a duplicate, which lets a test swap
    a child workflow for a stub while reusing an application's registration
    list. *)
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
    [override] has the same replacement meaning as on {!val-workflow}. *)
val activity :
  ?override:bool ->
  ( 'input,
    'output,
    Temporal_base.Activity_context.t ->
    'input ->
    ('output, Temporal_base.Error.t) result )
  Temporal_base.Definition.t ->
  activity

(** Registers a name whose attempts always fail with [reason]. The public
    adapter uses it for definitions the engine cannot run, such as remote
    references and asynchronous activities, so the failure names the cause
    instead of reporting an unregistered activity. *)
val unsupported_activity : ?override:bool -> name:string -> reason:string -> unit -> activity

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
    forever. Duplicate registration names without [override], an invalid
    namespace or task queue, and a non-positive attempt cap return defects. *)
val create :
  ?namespace:string ->
  ?task_queue:string ->
  ?start_time_ms:int64 ->
  ?max_activity_attempts:int ->
  workflows:workflow list ->
  activities:activity list ->
  unit ->
  (t, Temporal_base.Error.t) result

(** Starts a registered workflow and runs every execution until none can make
    progress without virtual time passing. [workflow_id] defaults to a
    deterministic ["workflow-<n>"]. An unknown workflow type, an input the
    registered codec rejects, or a workflow ID that is already running
    returns an error and starts nothing. *)
val start :
  ?workflow_id:string ->
  t ->
  workflow_type:string ->
  input:Temporal_base.Codec.payload ->
  (handle, Temporal_base.Error.t) result

(** Returns the workflow ID shared by every run of the chain. *)
val workflow_id : handle -> string

(** Returns the ID of the chain's current (latest) run. *)
val run_id : handle -> string

(** Runs the environment, skipping virtual time to the next pending timer or
    activity retry whenever every execution is blocked, until the chain
    closes. Returns the encoded output, or the workflow's failure,
    cancellation, termination, or task-failure error. Returns a defect when
    the chain is blocked with nothing scheduled (for example, waiting for a
    signal nobody sends) and a [Timeout] error when the next scheduled event
    lies more than [timeout_ms] of virtual time after the call started. *)
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
    a query-only activation. Closed runs remain queryable. *)
val query :
  handle ->
  name:string ->
  arguments:Temporal_base.Codec.payload list ->
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result

(** Delivers one update with validation enabled and runs the environment,
    skipping virtual time like {!result}, until the update completes or is
    rejected. Returns the encoded result or the rejection; a chain that closes
    first, or that blocks with nothing scheduled, returns an error. *)
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
    A negative value returns a defect and changes nothing. *)
val skip : t -> milliseconds:int64 -> (unit, Temporal_base.Error.t) result

(** Returns the current virtual time in milliseconds since the Unix epoch. *)
val now_ms : t -> int64

(** Releases every execution's paused fibers and pending operation tables.
    Idempotent; later operations on the environment or its handles return a
    defect. *)
val shutdown : t -> unit

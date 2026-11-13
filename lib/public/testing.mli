(** In-process, time-skipping workflow test environment.

    [Testing] runs registered workflows and activities deterministically in
    the calling process, without a Temporal Server, Temporal Core, or the
    native bridge. Workflow code executes on the same private scheduler and
    command runtime as a native worker, so [Activity.execute],
    [Workflow.sleep], [Child_workflow.execute], [Scope], [Condition],
    signals, queries, updates, and [Workflow.continue_as_new] behave as they
    do against a server. A small simulator stands in for the server: it runs
    activities when they are scheduled, starts child workflows (retrying a
    failed child run under its retry policy), routes signals and
    cancellations, and fires timers. It delivers each activation's jobs in
    Temporal Core's order, reports activity and child failures with the
    native worker's error categories, types, retryability and
    {!Error.val-origin} (an unregistered type is a ["NotFoundError"], retryable
    for a remote activity; a cancellation has origin [`Cancelled]), gives a
    local activity no heartbeat hand-off between attempts, as Core does, and,
    like a
    server, never dispatches an activity, child start, or external signal
    or cancellation emitted in the same activation that closes the run.

    Time is virtual. It starts at a fixed instant (2024-01-01T00:00:00Z by
    default) and moves only when {!result} or {!update} waits for a workflow
    that is blocked on a timer or an activity retry, or when {!skip} is
    called. A workflow that sleeps for a week therefore finishes immediately,
    and [Workflow.now] observes the skipped time.

    {[
      let test_greeting () =
        let open Temporal.Result_syntax in
        let* env =
          Temporal.Testing.create
            ~workflows:[ Temporal.Testing.workflow greeting_workflow ]
            ~activities:
              [ Temporal.Testing.mock_activity compose_greeting (fun name ->
                    Ok ("Hello, " ^ name)) ]
            ()
        in
        Fun.protect
          ~finally:(fun () -> Temporal.Testing.shutdown env)
          (fun () -> Temporal.Testing.execute env greeting_workflow "OCaml")
    ]}

    It also simulates, in virtual time, what a test usually needs from the
    server around those operations (#1160): activity start-to-close,
    schedule-to-close and heartbeat timeouts with the native error shape;
    workflow execution and run timeouts of a top-level or child workflow;
    workflow ID reuse and conflict policies with the client's
    already-started error; a run's start memo and search attributes and
    their upserts; asynchronous activities completed through their real
    {!Activity.Async_handle}; and, on request, retrying a failed workflow
    task (see {!type-workflow_task_failure}).

    Differences from a server, which a test should not rely on:
    - An activity attempt takes the virtual time its registration declares
      ([durations] on {!activity} and {!mock_activity}), zero by default.
      Start-to-close, schedule-to-close and heartbeat timeouts expire in
      virtual time against that duration and fail with the typed error a
      server produces ({!Error.val-timeout_type}); see {!mock_activity}. An
      activity callback still runs to completion at the attempt's start, so
      its heartbeats all happen then and a timed-out attempt's side effects
      remain. Schedule-to-start timeouts never expire, because a worker is
      always polling. Activity and workflow retry policies are honored, with
      backoff delays in virtual time; an unlimited policy is capped (see
      {!create}).
    - An asynchronous activity's handle (see {!mock_async_activity}) must be
      used from the environment's thread between environment calls, and its
      outcome reaches the workflow when the environment next runs.
      Heartbeats are recorded at once rather than throttled, and an
      operation after the attempt ended returns a non-retryable error
      without contacting anything.
    - Workflow code is replayed only to retry a failed workflow task in
      [`Retry] mode, and that replay does not compare commands, so
      nondeterministic workflow code is not detected; use [Temporal.Replay]
      against recorded histories for that.
    - By default a workflow task failure (an exception, a [Defect], [Codec],
      or [Bridge] error, or an unhandled signal) ends the run with that
      error instead of being retried. In [`Retry] mode it is retried only
      when the test calls {!retry_workflow_tasks}, with no server backoff
      between attempts.
    - Workflow cancellation and child cancellation follow the runtime's
      immediate cancellation path; child cancellation types are not
      distinguished.
    - Nexus operations and visibility queries ([Client.list_visibility],
      [Client.count]) are not simulated: memo and search attributes are
      stored per run ({!memo}, {!search_attributes}) without being indexed,
      validated against a registered schema, or annotated with a [type]
      metadata entry.
    - A workflow task timeout ([task_timeout]) is only recorded and
      reported, because a simulated workflow task never waits for a worker.
    - Identifiers (workflow IDs, run IDs, update IDs) are deterministic
      counters, so repeated runs of a test observe identical values.
    - [Workflow.Info.execution_timeout], [run_timeout], [task_timeout] and
      [retry_policy] report only what the start, child command, or a
      continue-as-new set; a native worker normally reports the 10 s task
      timeout Temporal records by default.
    - Cron schedules and start delays are not simulated (#1158): {!start}
      takes neither, every run's first task runs at once, and
      [Workflow.Info.cron_schedule] and
      [Workflow.Info.first_workflow_task_backoff] are always [None].
    - An activity's {!Activity.Info.scheduled_time} is the virtual time at
      which it was scheduled, and its {!Activity.Info.started_time} and
      {!Activity.Info.current_attempt_scheduled_time} are the virtual time at
      which the attempt ran, not server timestamps.

    Threading: an environment and its handles must be used from one system
    thread, and workflow or activity code must not call back into the
    environment running it. The environment is independent of
    [Temporal.Client] and [Temporal.Worker]; the [mock://] target remains a
    plumbing-only backend that never runs workflow code. *)

(** A workflow registration for {!create}. *)
type registered_workflow

(** Registers a workflow with optional signal, query, and update handlers,
    exactly as for [Temporal.Worker.workflow]. A [Workflow.remote] reference
    registered here fails every run with a defect asking for
    {!mock_workflow}. *)
val workflow :
  ?signals:Signal.Handler.t list ->
  ?queries:Query.Handler.t list ->
  ?updates:Update.Handler.t list ->
  ('input, 'output) Workflow.t ->
  registered_workflow

(** Registers [implementation] under the name and codecs of [workflow],
    replacing any other registration with that name wherever it appears in
    the list. Use it to stub a child
    workflow, including a [Workflow.remote] reference implemented by another
    worker. The stub runs as a real workflow, so it may itself use workflow
    operations. *)
val mock_workflow :
  ?signals:Signal.Handler.t list ->
  ?queries:Query.Handler.t list ->
  ?updates:Update.Handler.t list ->
  ('input, 'output) Workflow.t ->
  ('input, 'output) Workflow.implementation ->
  registered_workflow

(** An activity registration for {!create}. *)
type registered_activity

(** Registers an activity's own implementation. A plain, context-aware, or
    asynchronous callback runs as written; an asynchronous one behaves as
    described on {!mock_async_activity}. A remote reference cannot run
    in-process; every attempt then fails with a non-retryable error naming
    the reason, so replace it with {!mock_activity}. [durations] has the
    meaning described on {!mock_activity}. *)
val activity :
  ?durations:Duration.t list -> ('input, 'output) Activity.t -> registered_activity

(** Registers [implementation] under the name and codecs of [activity],
    replacing any other registration with that name wherever it appears in
    the list. This is the usual way
    to isolate workflow logic from external I/O: the workflow still encodes
    the input, and the stub's output is still decoded through the
    activity's codecs.

    [durations] is the virtual time each attempt takes, in attempt order;
    its last entry applies to every later attempt, and the default is zero.
    The workflow observes the attempt's outcome that much later. When a
    timeout's deadline falls strictly before the attempt would finish, the
    attempt fails at the deadline with an [Activity] error whose
    {!Error.val-timeout_type} names the timeout and whose
    {!Error.last_heartbeat_details} are the last heartbeat's: start-to-close
    and heartbeat deadlines are measured from the attempt's start and
    schedule-to-close from the first attempt's scheduling. Start-to-close
    and heartbeat timeouts are retried under the retry policy; an expired
    schedule-to-close timeout, including one reached while waiting for a
    retry, ends the activity. So [~durations:[ Duration.of_minutes 5L;
    Duration.of_ms 0L ]] with a one-minute start-to-close timeout makes the
    first attempt time out after a virtual minute and the retry succeed. *)
val mock_activity :
  ?durations:Duration.t list ->
  ('input, 'output) Activity.t ->
  ('input, 'output) Activity.implementation ->
  registered_activity

(** Registers an asynchronous [implementation] under the name and codecs of
    [activity], replacing any other registration with that name, as
    {!mock_activity} does (#1160). Each attempt calls it with a fresh
    context. An attempt that returns [Completed] or [Failed] behaves like a
    synchronous one, including its [durations] entry. An attempt that
    returns [Will_complete_async] stays pending, holding no virtual time,
    until the test finishes the handle it obtained from the context
    ({!Activity.Async_context.handle}), typically by keeping it in a
    reference:

    {[
      let pending = ref None in
      let charge_stub =
        Temporal.Testing.mock_async_activity charge (fun context _order ->
            let handle = Temporal.Activity.Async_context.handle context in
            pending := Some handle;
            Temporal.Activity.Will_complete_async handle)
      in
      (* ... start the workflow, then complete the charge: *)
      Temporal.Activity.Async_handle.complete (Option.get !pending) receipt
    ]}

    The handle's operations then act as against a server.
    [Async_handle.complete] resolves the activity, [Async_handle.fail]
    fails the attempt, which is retried under its retry policy (the retry
    calls [implementation] again with a new handle), and
    [Async_handle.heartbeat] records details and restarts the heartbeat
    timeout. Start-to-close, schedule-to-close and heartbeat timeouts expire
    in virtual time while the attempt is pending. After the workflow
    requested cancellation, a heartbeat returns the non-retryable
    [`Cancelled] error a server's response produces and
    [Async_handle.cancel] reports the cancellation; [Async_handle.cancel]
    without a request is rejected and leaves the handle live. Once the
    attempt has ended (it completed, timed out, or its run closed), every
    operation returns a non-retryable error and retires the handle. A local
    activity cannot be completed asynchronously, so [Will_complete_async]
    fails such an attempt non-retryably, as on a native worker. *)
val mock_async_activity :
  ?durations:Duration.t list ->
  ('input, 'output) Activity.t ->
  ('input, 'output) Activity.async_implementation ->
  registered_activity

(** What the environment does with a workflow task failure: an exception, a
    [Defect], [Codec], or [Bridge] error returned by workflow code, or an
    unhandled signal.

    - [`Fail_run] (the default) ends the run with that error, which
      {!result} returns, so a defect surfaces at once.
    - [`Retry] keeps the run open with its workflow task failing, as a
      server does, so a test can assert the stuck run and fix whatever the
      workflow reads before retrying (#1160). {!result} and {!update} then
      return the failure while the run stays open,
      {!val-workflow_task_failure} reports it, signals and cancellations are
      queued for the retried task, timers and activities keep running in
      virtual time, and a query is answered from the state before the
      failing task. {!retry_workflow_tasks} retries it by replaying the
      run's completed workflow tasks against a fresh execution, as a worker
      does after a failure, then delivering the failed task again. *)
type workflow_task_failure = [ `Fail_run | `Retry ]

(** One simulated namespace and task queue with its own virtual clock. *)
type t

(** Creates an environment. Registering two definitions with the same name
    is a defect unless exactly one comes from {!mock_workflow} or
    {!mock_activity}, which replaces the other regardless of order. [namespace] and [task_queue] default
    to ["default"] and ["temporal-testing"] and are reported by
    [Workflow.info]. [start_time] is the initial virtual time.
    [max_activity_attempts] (default [10]) caps the attempts of an activity
    whose retry policy is unlimited, which includes the server default used
    when a workflow supplies no policy, and the runs of a child workflow
    whose retry policy is unlimited. [workflow_task_failure] (default
    [`Fail_run]) is described at {!type-workflow_task_failure}. Invalid
    settings return a defect. *)
val create :
  ?namespace:string ->
  ?task_queue:string ->
  ?start_time:Time.t ->
  ?max_activity_attempts:int ->
  ?workflow_task_failure:workflow_task_failure ->
  workflows:registered_workflow list ->
  activities:registered_activity list ->
  unit ->
  (t, Error.t) result

(** Releases every workflow execution's suspended fibers. Idempotent; later
    operations on the environment or its handles return a defect. *)
val shutdown : t -> unit

(** Returns the current virtual time. *)
val now : t -> Time.t

(** Advances virtual time by [duration], firing every timer and activity
    retry that falls due, in time order, and running the workflows they
    wake. Use it to observe a workflow part-way through a long wait. *)
val skip : t -> Duration.t -> (unit, Error.t) result

(** A started workflow execution. It follows continue-as-new, so
    operations address the latest run for the workflow ID. *)
type ('input, 'output) handle

(** Starts [workflow], which must be registered, and runs it until it is
    blocked without moving virtual time. [id] defaults to a deterministic
    ["workflow-<n>"]. An unregistered workflow type or an input that does
    not encode or decode returns an error. A workflow failure is reported
    by {!result}, not here.

    The options mean what they mean for {!Client.start} (#1160):

    - [memo] and [search_attributes] are the first run's start maps, read
      back by {!memo} and {!search_attributes} and by the workflow through
      [Workflow.start_metadata]; build search attributes with
      {!Search_attribute.encode}.
    - [id_conflict_policy] (default [`Fail]) decides what happens when [id]
      has an open run: [`Fail] returns the already-started error that
      {!Client.already_started} recognizes, [`Use_existing] returns a handle
      for the open run ({!started} is then [false]), and
      [`Terminate_existing] terminates it (a parent waiting for it observes
      a termination) and starts a new run.
    - [id_reuse_policy] (default [`Allow_duplicate]) decides whether a
      closed run's ID may be reused; a refused start returns the same
      already-started error, naming the closed run. Child workflow starts
      apply the child's own [id_reuse_policy] the same way.
    - [execution_timeout] and [run_timeout] expire in virtual time: the
      run ends and {!result} returns a [Timeout] error whose
      {!Error.val-timeout_type} is [`Start_to_close], unless a run timeout
      is retried under [retry_policy]. Child workflow timeouts behave the
      same, and the parent observes a [Child_workflow] failure with that
      origin. [task_timeout] is only reported by [Workflow.info].
    - [retry_policy] retries a failed or timed-out run as a server does.

    The timeouts are validated as {!Client.start} validates them. *)
val start :
  ?id:string ->
  ?memo:(string * Payload.t) list ->
  ?search_attributes:(string * Payload.t) list ->
  ?id_conflict_policy:Client.id_conflict_policy ->
  ?id_reuse_policy:Client.id_reuse_policy ->
  ?execution_timeout:Duration.t ->
  ?run_timeout:Duration.t ->
  ?task_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  t ->
  ('input, 'output) Workflow.t ->
  'input ->
  (('input, 'output) handle, Error.t) result

(** Waits for the workflow to close, skipping virtual time whenever every
    execution is blocked on a timer or activity retry, and decodes its
    output. Returns the workflow's own failure, a [Cancelled],
    [Terminated], or workflow [Timeout] error, or the task-failure error;
    in [`Retry] mode a failing workflow task returns its failure at once
    and leaves the run open. A workflow blocked with
    nothing scheduled, such as one waiting for a signal no one sends,
    returns a defect instead of hanging; [timeout] bounds the virtual time
    the call may skip and returns a [Timeout] error when exceeded. Supply
    [timeout] for a workflow that may keep scheduling timers forever, which
    would otherwise never return. *)
val result :
  ?timeout:Duration.t -> ('input, 'output) handle -> ('output, Error.t) result

(** [start] followed by [result]. *)
val execute :
  ?id:string ->
  ?memo:(string * Payload.t) list ->
  ?search_attributes:(string * Payload.t) list ->
  ?id_conflict_policy:Client.id_conflict_policy ->
  ?id_reuse_policy:Client.id_reuse_policy ->
  ?execution_timeout:Duration.t ->
  ?run_timeout:Duration.t ->
  ?task_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
  ?timeout:Duration.t ->
  t ->
  ('input, 'output) Workflow.t ->
  'input ->
  ('output, Error.t) result

(** Sends a signal and runs the workflow until it is blocked again, without
    moving virtual time. Signalling a closed workflow returns an error. *)
val signal :
  ('input, 'output) handle -> 'signal Signal.t -> 'signal -> (unit, Error.t) result

(** Runs pending work, then answers an output-only query. A closed workflow
    remains queryable, and a run whose workflow task is failing answers from
    its state before that task. *)
val query : ('input, 'output) handle -> 'query Query.t -> ('query, Error.t) result

(** Answers a query that takes one typed input. *)
val query_with_input :
  ('input, 'output) handle ->
  ('query_input, 'query) Query.typed ->
  'query_input ->
  ('query, Error.t) result

(** Sends an update with validation enabled and waits for its result,
    skipping virtual time like {!result}. A validator rejection or handler
    failure is returned as the error; a workflow that closes first returns
    an [Update] error. A failing workflow task returns its failure, and the
    update is delivered again with the retried task. *)
val update :
  ?timeout:Duration.t ->
  ('input, 'output) handle ->
  ('update_input, 'update_output) Update.t ->
  'update_input ->
  ('update_output, Error.t) result

(** Requests cancellation and runs the workflow until it is blocked again.
    {!result} then reports the [Cancelled] outcome. *)
val cancel : ('input, 'output) handle -> (unit, Error.t) result

(** Returns the workflow ID. *)
val workflow_id : ('input, 'output) handle -> string

(** Returns the ID of the latest run, which changes after continue-as-new. *)
val run_id : ('input, 'output) handle -> string

(** Returns [false] when {!start} attached to an open run under
    [`Use_existing], and [true] when it started a run, as
    {!Client.started} does. *)
val started : ('input, 'output) handle -> bool

(** Returns the failure of the latest run's workflow task while it is
    failing in [`Retry] mode, and [None] otherwise, including after the run
    closed. *)
val workflow_task_failure : ('input, 'output) handle -> Error.t option

(** Retries every failing workflow task in the environment, in the order
    the runs were created, and runs the workflows until they are blocked
    without moving virtual time. Each run's execution is rebuilt by
    replaying its completed workflow tasks with the current code and state,
    then the failed task is delivered again together with the signals,
    timers, and other events that arrived meanwhile. A task that fails
    again, during the replay or after it, stays failing with the new error.
    Returns a defect when no workflow task is failing. *)
val retry_workflow_tasks : t -> (unit, Error.t) result

(** Returns the latest run's memo (#1154), sorted by key: the memo the run
    started with, merged with its {!Workflow.upsert_memo} calls as the server
    merges them, so a [binary/null] value removes its key. A run started by
    {!start} has the memo passed as [memo]. A continue-as-new successor starts with its
    predecessor's merged memo once the predecessor upserted one, and with the
    predecessor's start memo otherwise, as a native worker does; a retry run
    starts with the start memo. *)
val memo : ('input, 'output) handle -> (string * Payload.t) list

(** Returns the latest run's search attributes, sorted by key, after its
    {!Workflow.upsert_search_attributes} and
    {!Workflow.upsert_typed_search_attributes} calls; read typed values with
    {!Search_attribute.decode}. A run started by {!start} starts with its
    [search_attributes], and a continue-as-new successor inherits its
    predecessor's current values.
    Only the stored values are simulated: there is no visibility store to
    query, and names need not be registered. *)
val search_attributes : ('input, 'output) handle -> (string * Payload.t) list

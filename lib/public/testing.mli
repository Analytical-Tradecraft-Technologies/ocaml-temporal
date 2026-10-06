(** In-process, time-skipping workflow test environment.

    [Testing] runs registered workflows and activities deterministically in
    the calling process, without a Temporal Server, Temporal Core, or the
    native bridge. Workflow code executes on the same private scheduler and
    command runtime as a native worker, so [Activity.execute],
    [Workflow.sleep], [Child_workflow.execute], [Scope], [Condition],
    signals, queries, updates, and [Workflow.continue_as_new] behave as they
    do against a server. A small simulator stands in for the server: it runs
    activities when they are scheduled, starts child workflows, routes
    signals and cancellations, and fires timers.

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

    Differences from a server, which a test should not rely on:
    - Activities complete in zero virtual time; schedule-to-start,
      start-to-close, schedule-to-close, and heartbeat timeouts are not
      enforced. Activity retry policies are honored, with backoff delays in
      virtual time; an unlimited policy is capped (see {!create}).
    - A workflow task failure (an exception, a [Defect], [Codec], or [Bridge]
      error, or an unhandled signal) ends the run with that error instead of
      being retried until the code is fixed.
    - Workflow cancellation and child cancellation follow the runtime's
      immediate cancellation path; child cancellation types are not
      distinguished.
    - Asynchronous activities, Nexus operations, search-attribute visibility,
      memos, and workflow ID reuse policies are not simulated.
    - Identifiers (workflow IDs, run IDs, update IDs) are deterministic
      counters, so repeated runs of a test observe identical values.

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
    replacing any other registration with that name. Use it to stub a child
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

(** Registers an activity's own implementation. A plain or context-aware
    callback runs as written. A remote reference or an asynchronous
    activity cannot run in-process; every attempt then fails with a
    non-retryable error naming the reason, so replace it with
    {!mock_activity}. *)
val activity : ('input, 'output) Activity.t -> registered_activity

(** Registers [implementation] under the name and codecs of [activity],
    replacing any other registration with that name. This is the usual way
    to isolate workflow logic from external I/O: the workflow still encodes
    the input, and the stub's output is still decoded through the
    activity's codecs. *)
val mock_activity :
  ('input, 'output) Activity.t ->
  ('input, 'output) Activity.implementation ->
  registered_activity

(** One simulated namespace and task queue with its own virtual clock. *)
type t

(** Creates an environment. Registering two definitions with the same name
    is a defect unless the later one comes from {!mock_workflow} or
    {!mock_activity}, which replace it. [namespace] and [task_queue] default
    to ["default"] and ["temporal-testing"] and are reported by
    [Workflow.info]. [start_time] is the initial virtual time.
    [max_activity_attempts] (default [10]) caps the attempts of an activity
    whose retry policy is unlimited, which includes the server default used
    when a workflow supplies no policy. Invalid settings return a defect. *)
val create :
  ?namespace:string ->
  ?task_queue:string ->
  ?start_time:Time.t ->
  ?max_activity_attempts:int ->
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
    ["workflow-<n>"]. An unregistered workflow type, an input that does not
    encode or decode, or an ID that is already running returns an error.
    A workflow failure is reported by {!result}, not here. *)
val start :
  ?id:string ->
  t ->
  ('input, 'output) Workflow.t ->
  'input ->
  (('input, 'output) handle, Error.t) result

(** Waits for the workflow to close, skipping virtual time whenever every
    execution is blocked on a timer or activity retry, and decodes its
    output. Returns the workflow's own failure, a [Cancelled] or
    [Terminated] error, or the task-failure error. A workflow blocked with
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
    remains queryable. *)
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
    an [Update] error. *)
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

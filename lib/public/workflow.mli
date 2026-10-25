(** Typed workflow definitions and the deterministic operations available
    inside workflow code.

    A workflow is a direct-style OCaml function that Temporal can replay from
    its history. Workflow code must not perform I/O, read the wall clock, use
    unseeded randomness, or mutate process-global state; it uses the
    replay-safe helpers here, such as {!sleep}, {!now}, and {!random_int},
    together with {!Activity} and {!Child_workflow}. *)

(** The type of an OCaml function that implements a workflow. It receives a
    decoded input and returns either the workflow output or a structured error.
    The function must obey Temporal's workflow determinism rules. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** The deployment identity Core selected for the workflow task currently
    running this workflow. It is task-local because a versioned worker may
    route later tasks of the same run to another build. *)
type deployment_version = { deployment_name : string; build_id : string }

(** A description of a workflow and the OCaml types it accepts and returns. A
    definition stores the Temporal workflow type name, its codecs, and
    optionally the OCaml function that implements it. *)
type ('input, 'output) t

(** Creates a workflow definition implemented by this OCaml worker. [name] is
    validated immediately; it must be non-empty, valid UTF-8, NUL-free, and no
    more than 65,536 bytes because it crosses the native protocol into
    Temporal history. Violations raise [Invalid_argument] as construction
    defects. The implementation must remain deterministic and return expected
    failures as [Error.t] values. Returning a [Workflow]-category error is
    an intentional terminal failure. Unexpected exceptions and propagated
    [Defect], [Bridge], or [Codec] errors fail only the workflow task so
    corrected code can replay the same open execution. Result-encoder errors
    also fail only the task. *)
val define :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) implementation ->
  ('input, 'output) t

(** Creates a typed reference to a workflow implemented by another worker. [name]
    has the same non-empty, valid UTF-8, NUL-free, 65,536-byte contract as
    [define]. Use the reference with [Child_workflow.start] or
    [Child_workflow.execute] when invoking the workflow as a child. It has no
    local implementation and cannot be registered as executable worker code. *)
val remote :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) t

(** Returns the workflow type name used for registration and commands. *)
val name : ('input, 'output) t -> string

(** Returns the codec used to decode workflow inputs. This accessor is useful
    to worker adapters and generic workflow tooling; the definition itself
    remains opaque and cannot be constructed by record syntax. *)
val input : ('input, 'output) t -> 'input Codec.t

(** Returns the codec used to encode successful workflow outputs. *)
val output : ('input, 'output) t -> 'output Codec.t

(** Returns executable code for a local definition, or [None] for a remote
    reference. The callback still returns typed [Error.t] values rather than
    raising expected workflow failures. *)
val implementation :
  ('input, 'output) t -> ('input, 'output) implementation option

(** Starts a durable Temporal timer and returns immediately. Starting several
    timers before awaiting one emits them in call order. A zero duration returns
    a ready future without recording a timer. Outside workflow execution the
    returned future is ready with a typed defect rather than touching global
    time or creating an unowned timer.

    With [scope], cancelling the scope while the timer is pending records
    [TimerCanceled] in history (one [Cancel_timer] command, however often the
    scope is cancelled) and settles the future with a typed [`Cancelled]
    error. A timer that already fired keeps its [Ok ()] and emits nothing. If
    the scope is already cancelled, no timer is started and the future is
    ready with the scope's [`Cancelled] error; a scoped start from a query or
    outside a workflow scheduler turn is a typed defect. Wrapping a race in
    {!Scope.with_scope} therefore cancels the losing timer, for example
    [Scope.with_scope (fun scope -> ... Future.race work
    (start_sleep ~scope timeout) ...)]. *)
val start_sleep : ?scope:Scope.t -> Duration.t -> (unit, Error.t) Future.t

(** Starts a durable Temporal timer and waits until it fires. Without [scope]
    this is equivalent to [Future.await (start_sleep duration)]; with [scope]
    the wait is [Scope.await scope (start_sleep ~scope duration)], so a scope
    cancelled by another fiber cancels the timer and returns [`Cancelled]. A
    zero duration returns immediately without recording a timer; outside
    workflow execution it returns a typed defect. *)
val sleep : ?scope:Scope.t -> Duration.t -> (unit, Error.t) result

(** Sends a typed signal to another workflow execution. The operation is
    represented as a future because Temporal acknowledges delivery in a later
    activation. Omitting [run_id] lets the server resolve the current run for
    [workflow_id]; an explicit [run_id] targets that run and, like
    [workflow_id], must be non-empty, valid UTF-8, NUL-free, and at most
    65,536 bytes, or the future resolves with a typed defect. *)
val signal_external_workflow :
  workflow_id:string ->
  ?run_id:string ->
  signal:'input Signal.t ->
  input:'input ->
  unit ->
  (unit, Error.t) Future.t

(** Requests cancellation of another workflow execution and returns a future
    resolved by Temporal's acknowledgement or structured failure. [run_id]
    follows {!signal_external_workflow}: omitted selects the current run, and
    an explicit value must be a valid non-empty run ID. *)
val cancel_external_workflow :
  workflow_id:string ->
  ?run_id:string ->
  reason:string ->
  unit ->
  (unit, Error.t) Future.t

(** Returns the exact timestamp attached to the activation currently executing
    this workflow. Temporal supplies the value for both live execution and
    replay, so the result is deterministic. Calling this outside workflow
    execution, or while processing an activation without a timestamp, returns
    a typed defect rather than reading the host wall clock. *)
val now : unit -> (Time.t, Error.t) result

(** Returns a deterministic pseudo-random integer [n] with [0 <= n < bound].
    The stream is seeded by Temporal for the workflow run and replayed from
    the same initialization and reset metadata, so the result is stable for an
    identical call sequence. [bound] must be positive; invalid bounds and calls
    outside a workflow return a typed defect. *)
val random_int : bound:int -> (int, Error.t) result

(** Returns a deterministic pseudo-random float [x] with [0. <= x < 1.],
    from the same per-run stream as {!random_int} (#1155), so it is stable
    across replay and follows a reset's new seed. Calls outside a workflow,
    or in a read-only update validator, return a typed defect. *)
val random_float : unit -> (float, Error.t) result

(** Returns [length] deterministic pseudo-random bytes from the same stream
    as {!random_int} (#1155). [length] must be between 0 and 65,536;
    otherwise, and outside a workflow, the result is a typed defect. *)
val random_bytes : length:int -> (string, Error.t) result

(** Returns a deterministic version-4 UUID, such as
    ["1b4e28ba-2fa1-4d2a-883f-0016d3cca427"], from the same stream as
    {!random_int} (#1155): 36 lowercase hexadecimal characters with
    hyphens, with the RFC 9562 version and variant bits set. Use it for
    child workflow IDs and idempotency keys that must differ between runs
    yet be identical on replay. It is not a security token: the stream is
    recorded in history and predictable from the run's seed. Calls outside
    a workflow return a typed defect.

    Generic side effects that read the outside world have no
    deterministic equivalent here; run them in a local activity, whose
    result is recorded in history. *)
val uuid4 : unit -> (string, Error.t) result

(** Returns the deployment/build identity attached to the current workflow
    task. [None] means that the worker is unversioned, the activation is
    synthetic, or the call is outside workflow execution. The value is
    Temporal metadata and never reads process or wall-clock state. *)
val current_deployment_version : unit -> deployment_version option

(** A snapshot of metadata recorded when this workflow run started. [None]
    means Core omitted that protobuf field; [Some []] preserves an explicitly
    empty map. Search attributes are their initial values, before any upsert.
    Expiration is a server-enforced deadline across the execution chain, not
    a timer the workflow runtime should schedule. *)
type start_metadata = {
  memo : (string * Payload.t) list option;
  search_attributes : (string * Payload.t) list option;
  execution_expiration_time : Time.t option;
}

(** Returns this run's deterministic start snapshot with independently owned
    payload bytes on every call. Replay reconstructs it from the same history;
    a continued run receives its own inherited metadata. Calling outside a
    workflow, or in a synthetic execution lacking metadata, returns a defect. *)
val start_metadata : unit -> (start_metadata, Error.t) result

(** Read-only metadata about the workflow run executing the caller. Values
    come only from Temporal's activations, never from the host clock or
    process state, so a replay observes the same identity. The type is
    abstract so later releases can add fields compatibly.

    Identity fields are fixed for the run. History fields and {!is_replaying}
    describe the activation that was current when {!info} was called; call
    {!info} again after a suspension to observe newer values. *)
module Info : sig
  (** A snapshot of run identity and current-activation history facts. *)
  type t

  (** Identity of the workflow that started this run as a child. *)
  type parent = { namespace : string; workflow_id : string; run_id : string }

  (** Why the server suggested continuing as new.
      - [`History_size_too_large]: the history byte size passed the
        namespace's suggestion threshold.
      - [`Too_many_history_events]: the history event count passed that
        threshold.
      - [`Too_many_updates]: the run accepted many workflow updates, which
        count toward a per-run limit. *)
  type continue_as_new_reason =
    [ `History_size_too_large | `Too_many_history_events | `Too_many_updates ]

  (** Returns the workflow ID shared by every run of this execution chain. *)
  val workflow_id : t -> string

  (** Returns the ID of this run. *)
  val run_id : t -> string

  (** Returns the run ID of the first run in this continue-as-new, retry, or
      cron chain, or [None] when Temporal did not report it. *)
  val first_execution_run_id : t -> string option

  (** Returns the registered workflow type name. *)
  val workflow_type : t -> string

  (** Returns the Temporal namespace of this run, which is the namespace of
      the worker executing it. *)
  val namespace : t -> string

  (** Returns the worker task queue that delivers this run's workflow tasks. *)
  val task_queue : t -> string

  (** Returns the 1-based workflow retry attempt of this run. *)
  val attempt : t -> int

  (** How Temporal created this run from an earlier run of the same
      workflow ID.
      - [`Continue_as_new]: the previous run called {!continue_as_new}.
      - [`Retry]: the previous run failed or timed out and the workflow
        retry policy started this run.
      - [`Cron_schedule]: a cron schedule fired. OCaml workers currently
        reject cron-initiated runs before workflow code starts, so this
        constructor is reserved and is not observed today. *)
  type continuation_initiator = [ `Continue_as_new | `Retry | `Cron_schedule ]

  (** Returns the run ID this run was continued or retried from, or [None]
      for the first run of a chain, including a run started by a Temporal
      Schedule. *)
  val continued_from_run_id : t -> string option

  (** Returns why Temporal started this run from {!continued_from_run_id},
      or [None] for a first run or when Temporal did not report a reason. *)
  val continuation_initiator : t -> continuation_initiator option

  (** Returns the execution timeout recorded when the chain started
      ([?execution_timeout] on [Client.start]). It bounds the whole chain,
      across retries and continue-as-new. [None] means Temporal recorded no
      limit. Durations are rounded up to whole milliseconds. These timeout
      accessors are recorded metadata: the server enforces them, and reading
      one schedules nothing. *)
  val execution_timeout : t -> Duration.t option

  (** Returns this run's run timeout ([?run_timeout] on [Client.start]), or
      [None] when Temporal recorded no limit. *)
  val run_timeout : t -> Duration.t option

  (** Returns the workflow task timeout ([?task_timeout] on [Client.start]).
      Temporal normally records its 10 second default when the starter
      supplied none, so this is usually [Some _]. *)
  val task_timeout : t -> Duration.t option

  (** Returns the workflow retry policy ([?retry_policy] on [Client.start]),
      or [None] when the run is not retried. Temporal fills omitted policy
      fields with its defaults before recording the start event, so the
      result is the effective policy rather than the literal request.
      Intervals are rounded up to whole milliseconds. Temporal does not
      validate a policy whose maximum attempts is 1, so another SDK can
      record one that {!Retry_policy.make} would reject (for
      example a backoff coefficient below 1.0); such a policy is also
      reported as [None]. *)
  val retry_policy : t -> Retry_policy.t option

  (** Returns the parent workflow, or [None] for a top-level workflow. *)
  val parent : t -> parent option

  (** Returns when the server started this run, if Temporal reported it. *)
  val start_time : t -> Time.t option

  (** Returns whether Temporal was replaying history for the activation
      current when this snapshot was taken. See {!val-is_replaying}. *)
  val is_replaying : t -> bool

  (** Returns the number of history events Temporal reported with the
      activation current when this snapshot was taken. *)
  val history_length : t -> int

  (** Returns the history size in bytes reported with that activation, or
      [None] when the activation carried no size. *)
  val history_size_bytes : t -> int option

  (** Returns whether the server suggested continuing as new with that
      activation, typically because history is growing large. Workflows that
      run indefinitely should check it at a safe point and call
      {!continue_as_new}. *)
  val continue_as_new_suggested : t -> bool

  (** Returns the reasons the server gave with that activation's
      continue-as-new suggestion, in the order Temporal reported them. The
      list is empty whenever {!val-continue_as_new_suggested} is [false], and
      may also be empty when it is [true] because older servers suggest
      continue-as-new without naming a reason. Like the suggestion itself,
      the reasons come only from activation metadata and are replayed
      deterministically. *)
  val continue_as_new_reasons : t -> continue_as_new_reason list
end

(** Returns metadata for the current workflow run. Calling it outside workflow
    execution, or in a synthetic execution that never received Temporal's
    initialization activation, returns a typed defect. *)
val info : unit -> (Info.t, Error.t) result

(** Returns [true] while the current activation replays existing history and
    [false] for new progress or outside workflow execution. Use it only for
    side effects that are not part of workflow state, such as suppressing
    duplicate log lines; branching workflow commands on it breaks
    determinism. *)
val is_replaying : unit -> bool

(** Returns whether workflow code should take the new branch identified by
    [id]. On a new execution the first call returns [true] and records a patch
    marker; replay returns [true] only when Core reports that marker, otherwise
    it returns [false] and records nothing. The first answer for [id] is
    retained for the workflow run, so later calls and later history
    notifications cannot change it. Calls whose answer is [true] emit Core's
    idempotent marker command.

    Patch IDs must be non-empty, valid UTF-8, NUL-free, and at most 65,536
    bytes. Invalid IDs or calls outside workflow execution raise
    [Invalid_argument] as programmer misuse. IDs are durable history keys:
    never reuse an ID for a different behavioral change. *)
val patched : id:string -> bool

(** Records that the behavioral change identified by [id] is being phased out.
    A transition release replaces its [patched] call with [deprecate_patch] at
    the same logical point; it must not call both operations for one ID during a
    workflow execution. This function returns [unit] because deprecation is a
    durable lifecycle marker, not a branch decision.

    Patch IDs have the same validation and immutable-history requirements as
    [patched]. Invalid IDs, calls outside workflow execution, calls after that
    execution ends, or mixed patch modes raise [Invalid_argument] as programmer
    misuse. Replacing [patched] is safe only after marker-free executions that
    could take the old branch can no longer replay across that point. Removing
    the deprecation call is a later gate, safe only after incompatible
    non-deprecated-marker histories have drained or been otherwise accounted
    for. *)
val deprecate_patch : id:string -> unit

(** Merges encoded values into Temporal's indexed search attributes. The
    update is deterministic and becomes visible only after the workflow task
    is accepted. Duplicate, empty, malformed, or oversized keys raise
    [Invalid_argument] as programmer misuse; payload ownership is copied. An
    empty list is a no-op and emits no command, because Temporal rejects an
    empty upsert.

    Temporal Server decodes search-attribute values itself rather than
    through the worker's codecs. Each value must therefore be a [json/plain]
    encoding of the attribute's registered type, such as the output of
    {!Codec.string}, {!Codec.int}, or {!Codec.bool}; a [binary/null] payload
    (for example {!Codec.option} encoding [None]) removes the key. A custom
    codec built with [~encoding:"json/plain"] that emits valid JSON of the
    registered type is accepted like a built-in one. Other encodings, such
    as {!Codec.bytes} ([binary/plain]) or a custom encoding name, are
    rejected by the server, which fails the workflow task on every retry
    until the code is corrected. *)
val upsert_search_attributes : (string * Payload.t) list -> unit

(** Ends the current run and starts a new run of [definition] with [input].
    This operation never returns to the calling workflow fiber. It is
    deterministic: the input is encoded through the definition's codec before
    the successor command is emitted. A codec failure fails only the current
    workflow task, like any propagated [Codec] error: no continue-as-new
    command is emitted and the run stays open so a corrected worker can replay
    it. Calling it outside workflow execution is programmer misuse and raises
    [Invalid_argument].

    The fiber ends through a private exception that cleanup code
    ([Fun.protect ~finally]) observes but must let through. Do not wrap the
    call in a catch-all ([try ... with _ ->]): the run still continues as
    new, but whatever the handler returns is ignored with a
    [workflow_result_after_abort_ignored] warning (#1225). *)
val continue_as_new : ('input, 'output) t -> 'input -> 'value

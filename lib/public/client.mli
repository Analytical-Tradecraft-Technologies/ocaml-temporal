(** Client operations for starting workflows and addressing their runs.

    The client is deliberately smaller than a worker: it owns a connection
    backend and typed handles, but never registers or executes workflow code.

    A handle addresses either one exact run (from {!start}, {!follow}, or
    {!get_handle} with [~run_id]) or the workflow's current run (from
    {!get_handle} without [~run_id]). Every operation accepts both kinds.

    {2:rpc_deadlines RPC deadlines and workflow timeouts}

    Two kinds of time bound are deliberately separate:

    - A workflow timeout ([?execution_timeout], [?run_timeout],
      [?task_timeout] on {!start}) is a server-side execution policy recorded
      in the workflow's history. When it expires Temporal ends the workflow,
      and {!wait} reports [Timed_out].
    - An RPC deadline ([?rpc_timeout] on {!start}, {!signal}, {!query},
      {!query_with_input}, {!start_update}, {!cancel}, {!terminate},
      {!reset}, and {!list_visibility}) bounds one client call, including
      the transport retries the SDK performs inside it, and never affects the
      workflow. It must be between 1 ms and 60 seconds; other values are
      typed defects returned before any request is sent. When omitted, each
      operation keeps its built-in budget: 10 seconds for {!start} and
      {!list_visibility}, 3 seconds for {!signal}, {!cancel}, {!terminate},
      and {!reset}, and 30 seconds for queries and for {!start_update}'s
      acceptance wait. {!wait} and {!wait_update} have no RPC deadline: they
      wait for the workflow or update, with bounded internal polls.

    An expired RPC deadline does not prove that Temporal rejected the
    request: the server may have applied it before the reply was lost. It is
    reported as an error with {!val-rpc_status} [Some `Deadline_exceeded] (or
    [`Unavailable] when the last transport attempt failed first), except
    that an expired {!start} is recognized by {!is_start_outcome_uncertain}
    and an expired {!terminate} reports [`Termination_outcome_uncertain].
    Reconcile by retrying with the same request ID (start, signal, cancel,
    reset) or update ID, or by observing the run with {!wait}. *)

(** An opaque client connection owned by the caller. *)
type t

(** A typed address for a workflow execution: one exact run, or the current
    run of a workflow ID (see {!get_handle}). The input parameter documents
    the value used at start; the output parameter controls decoding of the
    terminal payload. *)
type ('input, 'output) handle

(** Opaque client-side handle for one accepted workflow update. It records the
    typed definition and exact target execution internally; callers can only
    await it or inspect its update ID. *)
type ('input, 'output) update_handle

(** An exact workflow/run identity: a successor returned after a completed,
    failed, timed-out, or continued-as-new run, the open run reported by
    {!already_started}, or the new run returned by {!reset}.
    It contains no codec or client ownership; use [follow] with the original
    client and typed workflow definition to construct a handle for this run.
    The namespace is retained so a successor cannot accidentally be used
    with a client connected to a different Temporal namespace. *)
type execution = {
  namespace : string;  (** Namespace that owns the successor execution. *)
  workflow_id : string;
      (** Durable workflow identity shared by the original and successor runs. *)
  run_id : string;  (** Server-issued identity of the successor run. *)
}

(** Terminal outcomes are values so workflow failures do not become control
    flow exceptions. The outer [result] of [wait] is reserved for bridge or
    payload transport errors. *)
type 'output terminal_result =
  | Completed of { output : 'output; successor : execution option }
      (** The terminal payload decoded using the workflow definition's output
          codec. [successor] is the run a cron schedule or retry policy started
          when this run completed, or [None]; following it is the caller's
          choice. *)
  | Failed of { error : Error.t; successor : execution option }
      (** Failure and optional retry successor. Following it is the caller's
          choice. *)
  | Cancelled of Error.t  (** The exact run reached the cancellation state. *)
  | Terminated of Error.t
      (** The exact run was terminated by an operator or another client. *)
  | Timed_out of { error : Error.t; successor : execution option }
      (** Timeout and optional successor. Following it is the caller's choice. *)
  | Continued_as_new of execution
      (** The run continued as new; the caller decides whether to wait on the
          returned successor identity. *)

(** What [start] does when its workflow ID already has an open (running) run.
    The policy never affects a closed run: Temporal's default reuse policy
    lets a new start reuse the ID of a completed, failed, or terminated run.

    - [`Fail]: the start returns the typed already-started error described
      at {!val-already_started} and leaves the running workflow untouched.
    - [`Use_existing]: the start returns a handle for the running workflow
      instead of creating one; {!val-started} is [false] and {!val-run_id} is the
      existing run's ID. Suited to idempotent "start or attach" orchestration.
    - [`Terminate_existing]: Temporal terminates the running workflow and
      starts a new run; the returned handle names the new run. *)
type id_conflict_policy = [ `Fail | `Use_existing | `Terminate_existing ]

(** What [start] does when its workflow ID's latest run has already closed
    and Temporal still retains it. The policy never affects an open run; see
    {!type-id_conflict_policy} for that case.

    - [`Allow_duplicate] (Temporal's default): start a new run whatever the
      closed run's outcome.
    - [`Allow_duplicate_failed_only]: start a new run only when the closed run
      failed, was cancelled or terminated, or timed out; a run that completed
      successfully keeps its ID.
    - [`Reject_duplicate]: never start another run with this ID.

    A refused start returns the same already-started error as a [`Fail]
    conflict; {!val-already_started} then names the closed run. Every
    combination with {!type-id_conflict_policy} is valid; for example
    [`Reject_duplicate] with [`Use_existing] attaches to an open run and
    refuses a closed one. Temporal's deprecated [TERMINATE_IF_RUNNING] value
    is not offered; use [`Terminate_existing] instead. *)
type id_reuse_policy =
  [ `Allow_duplicate | `Allow_duplicate_failed_only | `Reject_duplicate ]

(** One execution row returned by the Temporal visibility service. *)
type visibility_execution = {
  workflow_id : string;
  run_id : string;
  workflow_type : string;
  task_queue : string;
  status : string;
}

(** A bounded visibility page and its opaque continuation token. *)
type visibility_page = {
  executions : visibility_execution list;
  next_page_token : string option;
}

(** Connects to the configured Temporal endpoint. [target_url] is copied into
    the private backend graph and [namespace] is required for every operation.
    The namespace and optional identity must be non-empty, valid UTF-8,
    NUL-free, and no more than 65,536 bytes; invalid configuration is returned
    as a typed defect. An explicit [identity] is used unchanged. When omitted,
    the identity defaults to [<pid>@<hostname>], matching the official
    Temporal SDKs, computed once when the client is created; host names are
    sanitized to printable ASCII and bounded so the default is always valid.

    [io_threads] is an upper bound on the background threads this client
    uses for network I/O and server communication; the SDK may use fewer.
    When omitted it is the host's available parallelism capped at 4. Each
    client and each worker has its own threads, so the bound applies per
    instance. It must be between 1 and 256; any other value returns a typed
    defect before anything is allocated, for every target including
    [mock://].

    A [mock://] target selects an in-memory ledger for testing client plumbing
    only. It runs no workflow code: [wait] echoes the encoded start input back
    as the output, completing only when the workflow's output codec can
    decode it and returning a codec error otherwise, and queries and updates return
    typed errors. Use an [http://] or [https://] target against a Temporal
    Server to observe real workflow results. *)
val create :
  ?identity:string ->
  ?io_threads:int ->
  target_url:string ->
  namespace:string ->
  unit ->
  (t, Error.t) result

(** Starts a typed workflow execution and returns the exact server run handle.
    Encoding happens before the backend receives the request, so codec errors
    cannot create a partial workflow history entry.

    [task_queue], [id], the workflow type name retained by [workflow], and an
    explicitly supplied [request_id] must be non-empty, valid UTF-8, NUL-free,
    and no more than 65,536 bytes. Invalid fields return a typed defect before
    transport selection, so the deterministic mock and the native JSON bridge
    enforce the same request boundary.

    [request_id] is an optional caller-owned Temporal idempotency key. When a
    start result is uncertain, retry the same logical start with the same
    [request_id] so Temporal can deduplicate an already accepted request. If
    omitted, the SDK allocates a fresh request ID for this call. Do not reuse
    one ID for unrelated workflow starts.

    [id_conflict_policy] (default [`Fail]) chooses what happens when [id]
    already has an open run; see {!type-id_conflict_policy}. The SDK always sends
    the policy explicitly rather than relying on the server default. Request
    ID deduplication takes precedence: a retry with the same [request_id] as
    the start that created the open run returns that run as newly started
    under every policy, so a retried [`Terminate_existing] start never
    terminates the run it created. Retrying a still-pending request ID with
    a different policy is rejected rather than treated as the same start.

    [id_reuse_policy] (default [`Allow_duplicate]) chooses what happens when
    [id]'s latest run has closed; see {!type-id_reuse_policy}. It is also
    always sent explicitly.

    [execution_timeout] bounds the whole workflow execution, across retries
    and continue-as-new runs; [run_timeout] bounds one run; [task_timeout]
    bounds one workflow task (Temporal's default is 10 seconds). Omitted
    timeouts keep Temporal's defaults: no execution or run limit. Each
    supplied timeout must be positive, [run_timeout] no larger than
    [execution_timeout], and [task_timeout] at most 120 seconds and no larger
    than [run_timeout] (or [execution_timeout] when there is no
    [run_timeout]): Temporal would silently lower a larger value instead of
    honouring it. Violations are typed defects returned before anything is
    sent. An expired workflow timeout ends the run, which {!wait} reports as
    [Timed_out].

    [retry_policy] asks Temporal to retry a failed or timed-out run as a new
    run of the same workflow ID. Omitted, a workflow is not retried. The
    policy is the same validated {!Activity.Retry_policy.t} used for
    activities. An exact-run {!wait} on the closed run links the retry run as
    its successor; the pinned Temporal server reports that link to this
    client as [Continued_as_new] rather than as [Failed] with a successor, so
    accept either. A current-run handle from {!get_handle} follows the retry
    chain to its last run.

    [rpc_timeout] bounds this start call only (default 10 seconds; see
    {{!section-rpc_deadlines} RPC deadlines}). When it expires, or the client
    shuts down while the start is in flight, the error is recognized by
    {!is_start_outcome_uncertain}: Temporal may have accepted the start.
    Retry with the same [request_id] to obtain the run if it was created, or
    to create it exactly once if it was not; the RPC deadline is not part of
    the request, so the retry may use another one.

    [memo] attaches named payloads visible when describing the execution.
    [search_attributes] attaches named indexed payloads used by visibility
    queries. Keys in both collections are non-empty, valid UTF-8, NUL-free, at
    most 65,536 bytes, and unique within their respective collection. Payload
    metadata keys obey the same constraints within each payload. Payload
    metadata values may contain arbitrary bytes. Values are encoded before the
    native bridge is called. Search attributes must be registered in the
    namespace with matching server types. Workers
    can read the recorded values through [Workflow.start_metadata].

    This client does not expose cron schedules or delayed starts. OCaml
    workers reject cron and nonzero root start delay; do not start those
    policies from another SDK on a queue served by this worker.
    Server-applied workflow/retry continuation backoff, timeouts, and
    execution expiration metadata are preserved.

    The native client keeps at most 64 starts in flight at once. A start
    beyond that bound is rejected before anything is sent to Temporal, with a
    retryable error recognized by [is_at_capacity]; the client stays usable
    and the start may be retried once another one finishes. Retrying a start
    that is still in flight with the same [request_id] does not use another
    slot. *)
val start :
  t ->
  ?request_id:string ->
  ?memo:(string * Payload.t) list ->
  ?search_attributes:(string * Payload.t) list ->
  ?id_conflict_policy:id_conflict_policy ->
  ?id_reuse_policy:id_reuse_policy ->
  ?execution_timeout:Duration.t ->
  ?run_timeout:Duration.t ->
  ?task_timeout:Duration.t ->
  ?retry_policy:Activity.Retry_policy.t ->
  ?rpc_timeout:Duration.t ->
  workflow:('input, 'output) Workflow.t ->
  task_queue:string ->
  id:string ->
  input:'input ->
  unit ->
  (('input, 'output) handle, Error.t) result

(** Rebuilds a typed exact-run handle for an {!type-execution}, such as a
    successor returned by [wait]. [follow client ~workflow e] is
    [get_handle client ~workflow ~id:e.workflow_id ~run_id:e.run_id ()] with
    an additional namespace check.
    This does not start a workflow or follow a run implicitly: it only combines
    the caller's existing client, the supplied workflow definition's codecs,
    and the successor identity. The successor namespace must equal the
    namespace used to create [client]. All identity fields must be non-empty,
    valid UTF-8, NUL-free, and no more than 65,536 bytes; malformed or
    cross-namespace values are returned as typed defects before any backend
    operation. *)
val follow :
  t ->
  workflow:('input, 'output) Workflow.t ->
  execution ->
  (('input, 'output) handle, Error.t) result

(** Builds a handle for a workflow the caller did not necessarily start, from
    its workflow ID: for example a web service signalling a long-running
    entity workflow by its business ID. No request is sent and the workflow's
    existence is not checked; an unknown workflow ID or run surfaces from the
    first operation as an error with {!val-rpc_status} [Some `Not_found]. [id] and
    an explicit [run_id] must be non-empty, valid UTF-8, NUL-free, and no more
    than 65,536 bytes; violations are typed defects. The workflow definition
    supplies the codecs, exactly as for {!follow}; nothing checks that it
    matches the workflow type that was started.

    With [~run_id] the handle addresses that exact run, like {!follow}.

    Without it, the handle addresses the workflow's current run: the latest
    run of the workflow ID, open or closed. Each operation sends an empty run
    ID and Temporal resolves the current run when it handles that request, so
    a handle kept across a continue-as-new reaches the new run, which is what
    an entity workflow's signal senders need. On such a handle:
    - {!signal}, {!query}, {!query_with_input}, {!cancel}, {!terminate}, and
      {!reset} act on the run that is current when Temporal handles the call.
    - {!start_update} targets the current run, and the returned update handle
      keeps the exact run that accepted the update, so {!wait_update} polls
      that run even if the workflow later continues as new.
    - {!wait} follows the run chain, as the official SDKs' result methods do
      for a handle obtained by workflow ID: it waits for the current run and,
      whenever a run closes with a successor (continued-as-new, a cron run, or
      a retry), waits for that successor, returning the outcome of the first
      run that closes without one. It therefore never returns
      [Continued_as_new], and its successor fields are always [None]. A
      workflow with a cron schedule never ends its chain, so such a wait does
      not return until the schedule stops. Use an exact-run handle to observe
      one run at a time.
    - {!val-run_id} returns [None] and {!started} returns [false].

    Without [~run_id], the default cancellation request ID is derived from
    the workflow ID alone; see {!cancel}. *)
val get_handle :
  t ->
  ?run_id:string ->
  workflow:('input, 'output) Workflow.t ->
  id:string ->
  unit ->
  (('input, 'output) handle, Error.t) result

(** Waits for the run [handle] addresses to close. On an exact-run handle the
    wait observes only that run: completed, failed, timed-out, and
    continued-as-new outcomes may carry a typed successor, and the wait never
    follows one implicitly. On a current-run handle from {!get_handle} the
    wait follows the run chain to its last run, as described there.

    The native client retains at most 64 distinct runs being waited on.
    Concurrent waits on the same run share one slot. Waiting on another run
    at capacity returns a retryable error recognized by [is_at_capacity]; the
    client stays usable. Terminal results and errors free their slots, and
    client shutdown interrupts pending waits. *)
val wait :
  ('input, 'output) handle ->
  ('output terminal_result, Error.t) result

(** Requests cancellation of the run [handle] addresses. A successful
    call acknowledges Temporal's cancellation RPC; it does not wait for the
    workflow to stop. Call [wait handle] to observe [Cancelled]. [request_id]
    is the idempotency key for this logical control operation. When omitted,
    the client derives a stable key from the handle's workflow ID and run ID
    (the workflow ID alone for a current-run handle),
    so every defaulted call for the same run, including a retry after an
    uncertain transport error, is the same logical request. Temporal
    deduplicates the key per run, so it does not prevent cancelling a later
    run of the same workflow. Supply an explicit
    value only when separate cancellation requests for the same run must be
    distinguished. An explicit [request_id] must be non-empty and valid UTF-8.
    Both [request_id] and [reason] are limited to 65,536 bytes and may not
    contain NUL; [reason] may be empty. [rpc_timeout] bounds this call
    (default 3 seconds); see {{!section-rpc_deadlines} RPC deadlines}. *)
val cancel :
  ?request_id:string ->
  ?reason:string ->
  ?rpc_timeout:Duration.t ->
  ('input, 'output) handle ->
  (unit, Error.t) result

(** Terminates the run [handle] addresses immediately. Success means
    Temporal acknowledged the termination RPC; call [wait handle] to observe
    the immutable [Terminated] terminal result. [reason] is bounded operator
    context and may be empty. The request is re-sent only after the server
    rejects it as [resource_exhausted]. If the transport deadline expires or
    the server is unavailable, the returned non-retryable bridge error has
    [rpc_status] [Some `Termination_outcome_uncertain]: the server may have
    accepted the command, and this RPC has no idempotency key for a blind
    retry. Reconcile that result with [wait handle] or visibility.
    [rpc_timeout] bounds this call (default 3 seconds); see
    {{!section-rpc_deadlines} RPC deadlines}. *)
val terminate :
  ?reason:string ->
  ?rpc_timeout:Duration.t ->
  ('input, 'output) handle ->
  (unit, Error.t) result

(** Resets the run [handle] addresses at a workflow-task event boundary and
    returns the new run identity. A still-running old run is terminated, while an already
    closed run keeps its terminal result. Callers must explicitly use [follow]
    with the returned execution to wait for the new run. When [request_id]
    is omitted, each call uses a fresh ID, so calling [reset] again at the same
    event creates another new run. Pass the same explicit [request_id] to
    retry one logical reset idempotently: Temporal then returns the run created
    by the first accepted request. An explicitly supplied [request_id] must be
    non-empty, valid UTF-8, NUL-free, and no more than 65,536 bytes.
    [workflow_task_finish_event_id] must be greater than 1 and identify a
    workflow-task finish event accepted by Temporal.
    [rpc_timeout] bounds this call (default 3 seconds); see
    {{!section-rpc_deadlines} RPC deadlines}. *)
val reset :
  ?request_id:string ->
  ?reason:string ->
  ?rpc_timeout:Duration.t ->
  workflow_task_finish_event_id:int64 ->
  ('input, 'output) handle ->
  (execution, Error.t) result

(** Sends one typed signal to the run [handle] addresses. A successful
    call acknowledges Temporal's signal RPC; it does not wait for workflow code
    to process the message. [request_id] is optional: when omitted, the SDK
    allocates a fresh random ID across client handles and processes. Supply the
    same ID when retrying an uncertain transport result. An explicitly
    supplied ID must be non-empty, valid UTF-8, NUL-free, and no more than
    65,536 bytes. Signal names are validated when their definitions are
    created and input is encoded before transport.
    [rpc_timeout] bounds this call (default 3 seconds); see
    {{!section-rpc_deadlines} RPC deadlines}. *)
val signal :
  ?request_id:string ->
  ?rpc_timeout:Duration.t ->
  ('workflow_input, 'workflow_output) handle ->
  signal:'signal Signal.t ->
  input:'signal ->
  (unit, Error.t) result

(** Executes an output-only query against the run [handle] addresses.
    A successful result is decoded with [query]'s output codec; routine
    Temporal query failures and codec failures are returned as typed [Error.t]
    values. When the workflow's query handler fails, or the worker has no
    handler registered under the query's name, the error is recognized by
    [is_query_failed] and carries the handler's message. The SDK asks Temporal
    not to reject queries by workflow status, so a run that has already
    completed can still be queried, as long as a worker can replay it.
    [rpc_status] is [Some `Failed_precondition] only when Temporal itself
    reports the query as rejected. Use [query_with_input] when the query
    accepts one typed argument.
    [rpc_timeout] bounds this call (default 30 seconds); see
    {{!section-rpc_deadlines} RPC deadlines}. *)
val query :
  ?rpc_timeout:Duration.t ->
  ('workflow_input, 'workflow_output) handle ->
  query:'query Query.t ->
  ('query, Error.t) result

(** Lists one bounded page of workflow executions using Temporal's visibility
    query language. [page_token] is opaque and may be passed unchanged to a
    later call; when supplied, it must be non-empty, valid UTF-8, NUL-free, and
    no more than 65,536 bytes. Invalid query metadata is returned as a typed
    defect. [rpc_timeout] bounds this call (default 10 seconds); see
    {{!section-rpc_deadlines} RPC deadlines}. *)
val list_visibility :
  ?page_size:int ->
  ?page_token:string ->
  ?rpc_timeout:Duration.t ->
  t ->
  query:string ->
  unit ->
  (visibility_page, Error.t) result

(** Executes a typed one-input query against the run [handle] addresses. The input is encoded with [query]'s codec before transport and
    the result is decoded with its output codec. Query handlers remain
    synchronous and read-only; routine Temporal failures are returned as
    typed errors. [rpc_timeout] bounds this call (default 30 seconds); see
    {{!section-rpc_deadlines} RPC deadlines}. *)
val query_with_input :
  ?rpc_timeout:Duration.t ->
  ('workflow_input, 'workflow_output) handle ->
  query:('query_input, 'query_output) Query.typed ->
  input:'query_input ->
  ('query_output, Error.t) result

(** Starts a typed workflow update and waits until a Temporal worker accepts it.
    [update_id] is optional but should be supplied by callers that may retry
    after an uncertain transport result. When supplied, it must be non-empty,
    valid UTF-8, NUL-free, and no more than 65,536 bytes. The returned handle
    can be polled independently of other workflow handles. Failures already
    returned at admission, including validator rejection, are returned directly
    as [Error.t]. A completed successful outcome is retained in the handle.
    An update Temporal has only admitted (not yet accepted) never yields a
    handle: the request is re-issued with the same update ID until a worker
    accepts or rejects it, for at most 30 seconds, after which a retryable
    [deadline_exceeded] RPC error is returned. [rpc_timeout] replaces that
    30-second acceptance budget; see
    {{!section-rpc_deadlines} RPC deadlines}. *)
val start_update :
  ?update_id:string ->
  ?rpc_timeout:Duration.t ->
  ('workflow_input, 'workflow_output) handle ->
  update:('input, 'output) Update.t ->
  input:'input ->
  unit ->
  (('input, 'output) update_handle, Error.t) result

(** Waits for an accepted update to complete and decodes its typed result.
    Uses the admission outcome when present, otherwise polls Temporal.
    Application-level update failures and transport defects are returned as
    [Error.t] values; no expected update failure is raised as an exception. *)
val wait_update :
  ('input, 'output) update_handle -> ('output, Error.t) result

(** Returns the workflow-scoped update ID retained by a handle. *)
val update_id : ('input, 'output) update_handle -> string

(** Returns the durable workflow ID supplied to [start]. *)
val workflow_id : ('input, 'output) handle -> string

(** Returns the exact run ID [handle] addresses: the server-issued run ID
    returned by [start], or the run supplied to [follow] or [get_handle].
    Returns [None] for a current-run handle from {!get_handle}, which never
    pins a run: the run it reaches can change with every operation. *)
val run_id : ('input, 'output) handle -> string option

(** Returns [true] when the [start] call that produced [handle] created its
    run, including a request-ID deduplicated retry of that same start.
    Returns [false] when a [`Use_existing] start attached to a run created by
    another start, and for every handle built by [follow] or [get_handle]. *)
val started : ('input, 'output) handle -> bool

(** Returns the run that made a start fail with Temporal's already-started
    error, or [None] for any other error. That run is open when a [`Fail]
    conflict policy refused the start, and closed when the
    {!type-id_reuse_policy} refused it.

    That start error has category [`Workflow], is non-retryable, and has
    [Error.error_type] [Some "WorkflowExecutionAlreadyStarted"], Temporal's
    name for this failure; test the type to recognize the conflict even in
    the rare case where Temporal did not report the existing run ID, in which
    case this function also returns [None]. The returned execution can be
    passed to [follow] with the same client to wait on, signal, or query that
    workflow run. The identity travels as one JSON detail payload of the
    error, so it survives if the error is forwarded unchanged. *)
val already_started : Error.t -> execution option

(** Returns [true] when [error] means Temporal did not prove whether a
    [start] was accepted: its RPC deadline expired, the transport failed after
    the request may have reached the server, or the client shut down while
    the start was in flight. The workflow may or may not exist. Such an error
    has category [`Bridge], is non-retryable as a blind repeat, has
    [Error.error_type] [Some "StartOutcomeUncertain"], and its message names
    the workflow ID and request ID. Reconcile it by retrying the same start
    with the same [request_id] (Temporal returns the run if the first attempt
    created it) or by observing the workflow with {!get_handle} and {!wait}. *)
val is_start_outcome_uncertain : Error.t -> bool

(** Returns [true] when [error] means the native client refused a [start] or
    [wait] because its bounded set of in-flight operations was full (64 starts
    or 64 distinct waited runs). Nothing was sent to Temporal, the client
    remains connected, and the same call may be retried after another
    operation on this client finishes, for example with a backoff. Such an
    error has category [`Bridge], is not marked non-retryable, and has
    [Error.error_type] [Some "resource_exhausted"]. A closed client or any
    other failure returns [false]. *)
val is_at_capacity : Error.t -> bool

(** Classification of a client operation that failed with a Temporal RPC
    error. Every value except [`Termination_outcome_uncertain] is the gRPC
    status code the server (or the transport) reported;
    [`Termination_outcome_uncertain] means [terminate]'s transport deadline
    expired and the server may have accepted it.

    The same error carries the classification in its public fields, so code
    that only inspects {!Error.val-view} can use it too: the category is
    [`Bridge], [Error.error_type] is the code's canonical gRPC name in
    PascalCase (["Cancelled"], ["Unknown"], ["InvalidArgument"],
    ["DeadlineExceeded"], ["NotFound"], ["AlreadyExists"],
    ["PermissionDenied"], ["ResourceExhausted"], ["FailedPrecondition"],
    ["Aborted"], ["OutOfRange"], ["Unimplemented"], ["Internal"],
    ["Unavailable"], ["DataLoss"], ["Unauthenticated"], or
    ["TerminationOutcomeUncertain"]), and [non_retryable] is [true] exactly
    for the permanent conditions [`Invalid_argument], [`Not_found] (for
    example a signal to an unknown or closed run), [`Already_exists],
    [`Failed_precondition], [`Permission_denied], [`Unauthenticated],
    [`Unimplemented], and [`Termination_outcome_uncertain] (reconcile with
    [wait] instead of repeating it). The other statuses are transient and the
    same call may succeed when retried with backoff. The message names the
    code but never includes server text, which may contain user data. *)
type rpc_status =
  [ `Cancelled
  | `Unknown
  | `Invalid_argument
  | `Deadline_exceeded
  | `Not_found
  | `Already_exists
  | `Permission_denied
  | `Resource_exhausted
  | `Failed_precondition
  | `Aborted
  | `Out_of_range
  | `Unimplemented
  | `Internal
  | `Unavailable
  | `Data_loss
  | `Unauthenticated
  | `Termination_outcome_uncertain ]

(** Returns the RPC classification of a client operation error, or [None]
    when [error] did not come from a Temporal RPC failure. The result is
    derived from the error's category and [Error.error_type], not its message.
    The deterministic in-memory client reports an unknown workflow or a
    mismatched run as [`Not_found], as a server would. A server
    [ResourceExhausted] is [Some `Resource_exhausted] and is distinct from the
    local refusal recognized by [is_at_capacity], whose error type is the
    lowercase ["resource_exhausted"]. *)
val rpc_status : Error.t -> rpc_status option

(** Returns [true] when [error] means the workflow's query handler failed, or
    the worker had no handler for the query's name. Such an error has
    category [`Workflow], is non-retryable (the same query against the same
    workflow state fails the same way), and has [Error.error_type]
    [Some "QueryFailed"]. Its [Error.message] is the handler's own message,
    truncated to at most 4,096 bytes, or ["workflow query handler failed"]
    when the handler gave none. A Temporal Server too old to attach the query
    failure detail reports the same condition as [rpc_status]
    [Some `Invalid_argument] instead. *)
val is_query_failed : Error.t -> bool

(** Shuts down the client graph. Repeated calls are idempotent and return the
    same cached result, including a terminal teardown error, after the first
    shutdown request has consumed or invalidated the backend resources.

    Shutdown does not fail because another Domain or thread is still inside
    [start]. A native start whose request was already handed to the transport
    is aborted; that [start] call returns the error recognized by
    {!is_start_outcome_uncertain}, with the workflow and request IDs needed
    to reconcile it. A start that had not
    yet been admitted returns the ordinary shut-down error. *)
val shutdown : t -> (unit, Error.t) result

(** Client operations for starting workflows and awaiting an exact run.

    The client is deliberately smaller than a worker: it owns a connection
    backend and typed handles, but never registers or executes workflow code. *)

(** An opaque client connection owned by the caller. *)
type t

(** A typed identity for one started workflow execution. The input parameter
    documents the value used at start; the output parameter controls decoding
    of the terminal payload. *)
type ('input, 'output) handle

(** Opaque client-side handle for one accepted workflow update. It records the
    typed definition and exact target execution internally; callers can only
    await it or inspect its update ID. *)
type ('input, 'output) update_handle

(** An exact successor workflow/run identity returned after a failed, timed-out,
    or continued-as-new run.
    It contains no codec or client ownership; use [follow] with the original
    client and typed workflow definition to construct a handle for this run.
    The namespace is retained so a successor cannot accidentally be used
    with a client connected to a different Temporal namespace. *)
type execution = {
  (* Namespace that owns the successor execution. *)
  namespace : string;
  (* Durable workflow identity shared by the original and successor runs. *)
  workflow_id : string;
  (* Server-issued identity of the successor run. *)
  run_id : string;
}

(** Terminal outcomes are values so workflow failures do not become control
    flow exceptions. The outer [result] of [wait] is reserved for bridge or
    payload transport errors. *)
type 'output terminal_result =
  (* The terminal payload decoded using the workflow definition's output codec. *)
  | Completed of 'output
  (* Failure and optional retry successor. Following it is the caller's choice. *)
  | Failed of { error : Error.t; successor : execution option }
  (* The exact run reached the cancellation state. *)
  | Cancelled of Error.t
  (* The exact run was terminated by an operator or another client. *)
  | Terminated of Error.t
  (* Timeout and optional successor. Following it is the caller's choice. *)
  | Timed_out of { error : Error.t; successor : execution option }
  (* The run continued as new; the caller decides whether to wait on the
     returned successor identity. *)
  | Continued_as_new of execution

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

    [memo] attaches named payloads visible when describing the execution.
    [search_attributes] attaches named indexed payloads used by visibility
    queries. Keys in both collections are non-empty, valid UTF-8, NUL-free, at
    most 65,536 bytes, and unique within their respective collection. Payload
    metadata keys obey the same constraints within each payload. Payload
    metadata values may contain arbitrary bytes. Values are encoded before the
    native bridge is called. Search attributes must be registered in the
    namespace with matching server types. Workers
    can read the recorded values through [Workflow.start_metadata].

    This client does not expose cron schedules, delayed starts, or workflow
    execution/run/task timeout options yet. OCaml workers reject cron and
    nonzero root start delay; do not start those policies from another SDK on
    a queue served by this worker. Server-applied workflow/retry continuation
    backoff, timeouts, and execution expiration metadata are preserved.

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
  workflow:('input, 'output) Workflow.t ->
  task_queue:string ->
  id:string ->
  input:'input ->
  unit ->
  (('input, 'output) handle, Error.t) result

(** Rebuilds a typed exact-run handle for a successor returned by [wait].
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

(** Waits for the exact workflow ID and run ID returned by [start]. Failed,
    timed-out, and continued-as-new outcomes may carry a typed successor;
    the wait never follows one implicitly.

    The native client retains at most 64 distinct runs being waited on.
    Concurrent waits on the same run share one slot. Waiting on another run
    at capacity returns a retryable error recognized by [is_at_capacity]; the
    client stays usable. Terminal results and errors free their slots, and
    client shutdown interrupts pending waits. *)
val wait :
  ('input, 'output) handle ->
  ('output terminal_result, Error.t) result

(** Requests cancellation of the exact run retained by [handle]. A successful
    call acknowledges Temporal's cancellation RPC; it does not wait for the
    workflow to stop. Call [wait handle] to observe [Cancelled]. [request_id]
    is the idempotency key for this logical control operation. When omitted,
    the client derives a stable key from the handle's workflow ID and run ID,
    so every defaulted call for the same run, including a retry after an
    uncertain transport error, is the same logical request. Supply an explicit
    value only when separate cancellation requests for the same run must be
    distinguished. An explicit [request_id] must be non-empty and valid UTF-8.
    Both [request_id] and [reason] are limited to 65,536 bytes and may not
    contain NUL; [reason] may be empty. *)
val cancel :
  ?request_id:string ->
  ?reason:string ->
  ('input, 'output) handle ->
  (unit, Error.t) result

(** Terminates the exact run retained by [handle] immediately. Success means
    Temporal acknowledged the termination RPC; call [wait handle] to observe
    the immutable [Terminated] terminal result. [reason] is bounded operator
    context and may be empty. The request is re-sent only after the server
    rejects it as [resource_exhausted]. If the transport deadline expires or
    the server is unavailable, the returned non-retryable bridge error has
    [rpc_status] [Some `Termination_outcome_uncertain]: the server may have
    accepted the command, and this RPC has no idempotency key for a blind
    retry. Reconcile that result with [wait handle] or visibility. *)
val terminate :
  ?reason:string ->
  ('input, 'output) handle ->
  (unit, Error.t) result

(** Resets the exact run at a workflow-task event boundary and returns the new
    run identity. A still-running old run is terminated, while an already
    closed run keeps its terminal result. Callers must explicitly use [follow]
    with the returned execution to wait for the new run. When [request_id]
    is omitted, each call uses a fresh ID, so calling [reset] again at the same
    event creates another new run. Pass the same explicit [request_id] to
    retry one logical reset idempotently: Temporal then returns the run created
    by the first accepted request. An explicitly supplied [request_id] must be
    non-empty, valid UTF-8, NUL-free, and no more than 65,536 bytes.
    [workflow_task_finish_event_id] must be greater than 1 and identify a
    workflow-task finish event accepted by Temporal. *)
val reset :
  ?request_id:string ->
  ?reason:string ->
  workflow_task_finish_event_id:int64 ->
  ('input, 'output) handle ->
  (execution, Error.t) result

(** Sends one typed signal to the exact run retained by [handle]. A successful
    call acknowledges Temporal's signal RPC; it does not wait for workflow code
    to process the message. [request_id] is optional: when omitted, the SDK
    allocates a fresh random ID across client handles and processes. Supply the
    same ID when retrying an uncertain transport result. An explicitly
    supplied ID must be non-empty, valid UTF-8, NUL-free, and no more than
    65,536 bytes. Signal names are validated when their definitions are
    created and input is encoded before transport. *)
val signal :
  ?request_id:string ->
  ('workflow_input, 'workflow_output) handle ->
  signal:'signal Signal.t ->
  input:'signal ->
  (unit, Error.t) result

(** Executes an output-only query against the exact run retained by [handle].
    A successful result is decoded with [query]'s output codec; routine
    Temporal query failures and codec failures are returned as typed [Error.t]
    values. When the workflow's query handler fails, or the worker has no
    handler registered under the query's name, the error is recognized by
    [is_query_failed] and carries the handler's message. A query against a
    closed run is rejected with [rpc_status] [Some `Failed_precondition]. Use
    [query_with_input] when the query accepts one typed argument. *)
val query :
  ('workflow_input, 'workflow_output) handle ->
  query:'query Query.t ->
  ('query, Error.t) result

(** Lists one bounded page of workflow executions using Temporal's visibility
    query language. [page_token] is opaque and may be passed unchanged to a
    later call; when supplied, it must be non-empty, valid UTF-8, NUL-free, and
    no more than 65,536 bytes. Invalid query metadata is returned as a typed
    defect. *)
val list_visibility :
  ?page_size:int ->
  ?page_token:string ->
  t ->
  query:string ->
  unit ->
  (visibility_page, Error.t) result

(** Executes a typed one-input query against the exact run retained by
    [handle]. The input is encoded with [query]'s codec before transport and
    the result is decoded with its output codec. Query handlers remain
    synchronous and read-only; routine Temporal failures are returned as
    typed errors. *)
val query_with_input :
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
    [deadline_exceeded] RPC error is returned. *)
val start_update :
  ?update_id:string ->
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

(** Returns the server-issued run ID supplied to [start]. *)
val run_id : ('input, 'output) handle -> string

(** Returns [true] when the [start] call that produced [handle] created its
    run, including a request-ID deduplicated retry of that same start.
    Returns [false] when a [`Use_existing] start attached to a run created by
    another start, and for every handle built by [follow]. *)
val started : ('input, 'output) handle -> bool

(** Returns the open run that made a [`Fail] start fail, or [None] for any
    other error.

    That start error has category [`Workflow], is non-retryable, and has
    [Error.error_type] [Some "WorkflowExecutionAlreadyStarted"], Temporal's
    name for this failure; test the type to recognize the conflict even in
    the rare case where Temporal did not report the existing run ID, in which
    case this function also returns [None]. The returned execution can be
    passed to [follow] with the same client to wait on, signal, or query the
    running workflow. The identity travels as one JSON detail payload of the
    error, so it survives if the error is forwarded unchanged. *)
val already_started : Error.t -> execution option

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
    is aborted; that [start] call returns a non-retryable [`Bridge] error
    stating that Temporal did not prove whether the start was accepted, with
    the workflow and request IDs needed to reconcile it. A start that had not
    yet been admitted returns the ordinary shut-down error. *)
val shutdown : t -> (unit, Error.t) result

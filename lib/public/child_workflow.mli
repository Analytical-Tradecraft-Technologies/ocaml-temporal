(** Starting, awaiting, and cancelling child workflows from workflow code.

    A child workflow is a separate Temporal execution whose start, completion,
    and cancellation are recorded in the parent's history, so these operations
    are replay-safe. Use {!execute} for the common start-and-wait case and
    {!start_handle} when the parent needs the child's future or cancellation. *)

(** Controls how Core reports a parent operation after cancellation has been
    requested. [Abandon] reports immediately without asking the child worker;
    [Try_cancel] requests cancellation and reports immediately;
    [Wait_cancellation_completed] waits for child cancellation to complete; and
    [Wait_cancellation_requested] waits until Core confirms the request. *)
type cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon
  | Wait_cancellation_requested

(** The server action when this child's parent closes. This differs from the
    cancellation type, which controls an explicit child cancellation request. *)
module Parent_close_policy : sig
  (** [Abandon] preserves a child after parent closure; the other policies ask
      the server to terminate or request cancellation of it. *)
  type t = Terminate | Abandon | Request_cancel
end

(** What Temporal does when the child's [id] belongs to a closed run; the
    same choices as {!Client.type-id_reuse_policy}. [`Allow_duplicate]
    starts a new run, [`Allow_duplicate_failed_only] starts one only if the
    last run failed, was cancelled, terminated, or timed out, and
    [`Reject_duplicate] refuses the start, which the child's {!started} and
    {!future} report as a start failure. *)
type id_reuse_policy =
  [ `Allow_duplicate | `Allow_duplicate_failed_only | `Reject_duplicate ]

(** An opaque typed child operation. The handle owns the result future and can
    emit one deterministic cancellation command for that exact child. *)
type 'output handle

(** Starts a child workflow and retains a handle that can cancel it. The
    command is emitted immediately; {!started} resolves when Temporal starts
    the child and {!future} when it finishes or fails to start. The default [Try_cancel] policy asks Core to
    cancel the child when [cancel] is called; choose [Abandon] explicitly when
    the child should keep running. [retry_policy], when supplied, is handed to
    Temporal Core so retryable child failures are retried by the durable state
    machine rather than by replay-sensitive OCaml code. Invalid input and
    detached calls return ready failed handles, so no command or sequence
    number is created. When [scope] is supplied, cancelling it emits the
    child's Core cancellation command with the stable default reason
    ["cancelled by workflow"]. [task_queue] routes the child to another worker;
    omission inherits the parent's queue. A supplied queue must be a non-empty
    UTF-8 identifier of at most 65,536 bytes without NUL. [parent_close_policy]
    controls the server action after parent closure; omission keeps the server
    default. Choose [Parent_close_policy.Abandon] for work that must outlive
    the parent, and await {!started} before the parent returns: Temporal Server
    does not start a child whose parent has already closed, whatever the
    policy, so a child scheduled in the parent's last workflow task is
    silently never started.

    [execution_timeout] bounds the whole child execution (across its retries
    and continue-as-new runs), [run_timeout] one child run, and
    [task_timeout] one child workflow task (Temporal's default is 10
    seconds). They follow the same rules as {!Client.start}: each supplied
    timeout must be positive, [run_timeout] no larger than
    [execution_timeout], and [task_timeout] at most 120 seconds and no larger
    than [run_timeout] (or [execution_timeout] without a [run_timeout]).
    Violations return a ready failed handle with a typed defect before any
    command is emitted. An expired child timeout ends the child, which the
    parent observes as a failed {!future}; the child reads the values through
    {!Workflow.Info.execution_timeout}, {!Workflow.Info.run_timeout} and
    {!Workflow.Info.task_timeout}. [id_reuse_policy] is sent only when
    supplied; omitted, Temporal's default ([`Allow_duplicate]) applies. *)
val start_handle :
  ?scope:Scope.t ->
  ?cancellation_type:cancellation_type ->
  ?retry_policy:Retry_policy.t ->
  ?task_queue:string ->
  ?parent_close_policy:Parent_close_policy.t ->
  ?execution_timeout:Duration.t ->
  ?run_timeout:Duration.t ->
  ?task_timeout:Duration.t ->
  ?id_reuse_policy:id_reuse_policy ->
  id:string ->
  ('input, 'output) Workflow.t ->
  'input ->
  'output handle

(** Returns the typed future owned by a child operation handle. *)
val future : 'output handle -> ('output, Error.t) Future.t

(** [started handle] resolves with the child's run ID once Temporal has
    started the child, without waiting for it to finish. If the child never
    starts (for example its workflow ID is already in use, or it was cancelled
    first) it resolves with the same error as {!future}. Await it before the
    parent completes when the child should keep running after the parent
    closes, as the official Temporal SDKs do:

    {[
      let child =
        Child_workflow.start_handle
          ~parent_close_policy:Child_workflow.Parent_close_policy.Abandon
          ~id:("bill-" ^ order) bill order
      in
      match Future.await (Child_workflow.started child) with
      | Ok _run_id -> Ok ()
      | Error error -> Error error
    ]} *)
val started : 'output handle -> (string, Error.t) Future.t

(** Requests cancellation of the exact child represented by [handle]. The
    default reason is stable and replay-safe. Repeated calls are idempotent;
    this includes a valid call that arrives after natural child completion or a
    start failure has retired the pending entry. Core determines when the
    future receives the typed cancellation result. *)
val cancel :
  ?reason:string ->
  'output handle ->
  (unit, Error.t) result

(** Starts [definition] as a durable child workflow and returns immediately.
    [id] is the child execution's application-owned durable identity. It must
    be non-empty, valid UTF-8, and no more than 65,536 UTF-8 bytes so it can
    cross the strict bridge boundary. Invalid identity or input encoding returns
    a ready failed future without emitting a command or consuming a sequence.
    Starting several operations before awaiting them lets Temporal run them
    concurrently. The default cancellation policy is [Try_cancel]. A supplied
    [retry_policy] controls retries in Temporal Core. A supplied [scope]
    attaches server-side child cancellation to the scope. The timeouts and
    [id_reuse_policy] behave as for {!start_handle}. The returned future
    resolves only when the child finishes; use {!start_handle} and {!started}
    to wait for the child to be started instead. *)
val start :
  ?scope:Scope.t ->
  ?cancellation_type:cancellation_type ->
  ?retry_policy:Retry_policy.t ->
  ?task_queue:string ->
  ?parent_close_policy:Parent_close_policy.t ->
  ?execution_timeout:Duration.t ->
  ?run_timeout:Duration.t ->
  ?task_timeout:Duration.t ->
  ?id_reuse_policy:id_reuse_policy ->
  id:string ->
  ('input, 'output) Workflow.t ->
  'input ->
  ('output, Error.t) Future.t

(** Starts a child workflow and waits for its result. This is equivalent to
    [Future.await (start ~id definition input)]. Child, codec, cancellation,
    retry, and bridge failures are returned as structured values. The default
    cancellation policy is [Try_cancel]. A supplied [scope] also requests
    server-side cancellation of the child. *)
val execute :
  ?scope:Scope.t ->
  ?cancellation_type:cancellation_type ->
  ?retry_policy:Retry_policy.t ->
  ?task_queue:string ->
  ?parent_close_policy:Parent_close_policy.t ->
  ?execution_timeout:Duration.t ->
  ?run_timeout:Duration.t ->
  ?task_timeout:Duration.t ->
  ?id_reuse_policy:id_reuse_policy ->
  id:string ->
  ('input, 'output) Workflow.t ->
  'input ->
  ('output, Error.t) result

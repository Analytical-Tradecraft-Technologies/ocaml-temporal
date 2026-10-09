(** Private typed execution of one native Temporal activity-task lease.

    The native supervisor supplies already decoded semantic tasks and accepts
    semantic completions. This adapter owns the OCaml activity registry, decodes
    one activity input with the definition's codec, invokes the local
    implementation, and keeps an unacknowledged completion until the supervisor
    confirms the exact opaque task token. It is intentionally hidden from the
    public worker API: [Temporal.Worker] supplies the supervisor operations and
    owns lifecycle while this module remains responsible only for typed
    dispatch and lease-safe completion. *)

(** Native operations needed by the adapter. A supervisor implementation owns
    the Rust/Core handle graph and must serialize these calls on its owner
    domain. The adapter never retains the supervisor's error value or native
    handle. *)
module type SUPERVISOR = sig
  type t
  (** Opaque owner-confined supervisor state. *)

  type error
  (** Expected native failure. Accessors below must return bounded, privacy-safe
      diagnostics and must not expose a task token. *)

  val try_poll_activity :
    t -> (Temporal_protocol.Activity_protocol.task option, error) result
  (** Polls at most one activity task. [None] is ordinary nonblocking scheduler
      state; [Some task] transfers one opaque task-token lease to this adapter.
  *)

  val complete_activity :
    t -> Temporal_protocol.Activity_protocol.completion -> (unit, error) result
  (** Submits a semantic completion. [Ok ()] is the only proof that the native
      side accepted and retired the task-token lease. *)

  val complete_async_activity :
    t -> Temporal_protocol.Activity_protocol.completion -> (unit, error) result
  (** Submits a completion for a task already handed off with
      [WillCompleteAsync] through the namespace-bound client path. *)

  val record_activity_heartbeat :
    t -> Temporal_protocol.Activity_protocol.heartbeat -> (unit, error) result
  (** Records progress for the currently leased task without retiring its
      lease. The supervisor must serialize this call with completion and
      shutdown operations. *)

  val record_async_activity_heartbeat :
    t -> Temporal_protocol.Activity_protocol.heartbeat -> (unit, error) result
  (** Records heartbeat details for an admitted asynchronous activity. *)

  val error_code : error -> string
  (** Stable classification for a native failure. *)

  val error_message : error -> string
  (** Stable diagnostic for a native failure. Implementations must not include
      payload bytes, credentials, opaque task tokens, or unbounded text. *)

  val error_is_retryable : error -> bool
  (** Classifies a native failure without inspecting free-form text. [true]
      means the completion lease is still owned by this adapter and the caller
      may retry the same completion after a bounded wait. Permanent, protocol,
      configuration, and lifecycle failures must return [false]. *)

  val async_operation_error_disposition :
    error -> Native_worker_policy.async_operation_disposition
  (** Classifies a failed namespace-bound async heartbeat, complete, fail, or
      cancel separately from a Core worker completion. An uncertain RPC keeps
      the live handle and lease (a terminal request must then be retried
      exactly, a heartbeat is dropped); a definitive rejection releases only
      that request; confirmed token loss retires both. *)

  val exception_is_retryable : exn -> bool
  (** Classifies an exception raised by the completion call. Production
      supervisors should return [false] unless they define a private,
      explicitly transient exception category; unexpected owner-domain defects
      must remain fatal. *)
end

type error_view = {
  code : string;
  path : string;
  message : string;
  retryable : bool;
}
(** Privacy-safe diagnostic returned by the private adapter and suitable for
    logging. Opaque task-token bytes never appear in this record. [retryable]
    is a source classification, not a request to rerun user code: it is only
    used for a retained completion or another operation that has no leased
    token yet. *)

type registered_activity
(** One heterogeneous, executable activity registration. The existential
    representation keeps the input/output codec relationship intact while
    allowing one registry to contain activities with different OCaml types. *)

(** Kind of a completion accepted by the native supervisor. *)
type completion_kind = Succeeded | Failed | Cancelled | Deferred

(** Independent cancellation facts copied from Core with a [Cancelled]
    completion. They are retained as private outcome metadata and never
    fabricated from an acknowledgement-only heartbeat call. *)
type cancellation_details = Temporal_protocol.Activity_protocol.cancellation_details

(** Result of one serialized poll/execute/complete transaction.

    [Completed] reports only non-sensitive summary information plus the
    independent cancellation facts that Core supplied with a [Cancel] task;
    callers that need the opaque token for diagnostics should use the native
    supervisor's own correlation logging rather than copying it into OCaml
    state. [Rejected] means the task was acknowledged with a failure completion.
    A supervisor rejection remains a [result] error because lease retirement is
    unproven. *)
type outcome =
  | Not_ready
  | Completed of {
      activity_type : string option;
      kind : completion_kind;
      cancellation_details : cancellation_details option;
    }
  | Rejected of {
      activity_type : string option;
      error : error_view;
      lease_retired : bool;
    }

module Make (Supervisor : SUPERVISOR) : sig
  type t
  (** One owner-confined activity registry. Calls to [poll] are serialized by an
      internal mutex, so independent Domains may ask the adapter to poll; user
      activity implementations still run synchronously in that caller and must
      not call back into this adapter recursively. *)

  val create :
    supervisor:Supervisor.t ->
    activities:registered_activity list ->
    worker_shutting_down:(unit -> bool) ->
    (t, error_view) result
  (** Builds a registry after checking that every definition has a local
      implementation and that Temporal activity names are unique. No native call
      or user implementation runs during creation. [worker_shutting_down] is
      exposed to every synchronous activity context as its worker-shutdown
      signal; it must be non-blocking and safe to call from any Domain, which
      reading the owner's lifecycle atomics is. *)

  val poll : t -> (outcome, error_view) result
  (** Polls at most one task. A pending completion blocks new tasks, so an
      activity is never executed twice merely because its completion transport
      was unavailable. It is resubmitted only when its earlier failure was
      explicitly retryable; otherwise its recorded error is returned again
      without a native call (issue #843). [Ok Not_ready] means the native poll
      had no ready task. A retryable [Error] retains the exact completion and
      can be retried by the worker loop; a non-retryable [Error] is fatal for
      this worker instance.

      Cancellation delivery (issue #494): while a synchronous callback runs,
      each heartbeat it sends ends with a non-blocking sweep of the
      supervisor's ready activity tasks. A Core cancellation for the running
      attempt is published to its context, which the callback reads with
      [Activity_context.cancellation] and which makes later heartbeats return
      a [`Cancelled] error. Any other swept task (for example a local-activity
      start) is deferred and handled by later polls in the order it was
      taken; a deferred start whose cancellation was deferred too completes as
      cancelled without running its callback.

      The callback's result is classified only after its context has been
      invalidated (which waits for a heartbeat in flight, including one from a
      Domain the callback spawned) and the running attempt cleared, so the
      cancellation it reads is final. Outcome precedence for a running
      attempt: [Ok] completes it; an
      [Error] in the [`Cancelled] category completes it as cancelled only when
      a cancellation was delivered to its context, and is an ordinary failure
      otherwise; any other [Error] fails it; an exception fails it as a
      defect. A cancellation that arrives after the callback returned is an
      update to a start whose completion is already owned, so it never creates
      a second completion. *)

  (** Retries retained completions whose earlier failure was explicitly
      retryable while the adapter mutex is held; a fail-closed completion is
      never resubmitted and its recorded error is returned instead. Work that
      a cancellation-delivery sweep deferred is first recorded without running
      user code: a deferred start is completed as cancelled when its
      cancellation was also deferred, and otherwise failed retryably so
      Temporal can run it elsewhere. [Ok ()]
      proves that no opaque activity lease remains in this adapter. [Error _]
      leaves the exact completion retained. The caller must either retry it
      after an explicitly safe transient classification or force-retire the
      native graph and then call [discard] on a terminal path; it must never
      silently drop the token while Rust still owns it. *)
  val drain : t -> (unit, error_view) result

  (** Discards copied completion leases after terminal native cleanup. This is
      irreversible and must be called only after the supervisor has
      force-retired its native task-token leases; it never attempts a retry. *)
  val discard : t -> unit

  (** [drain] without waiting (#495): [None] when the adapter lock is held,
      which means a callback (or a supervisor call made for it) is still
      running on the activity lane. Bounded shutdown uses it so an abandoned
      callback can never block the drain. *)
  val try_drain : t -> (unit, error_view) result option

  (** [discard] without waiting: [false] when the adapter lock is held, in
      which case nothing was discarded and the caller must retry later or
      leave the copies to the garbage collector. *)
  val try_discard : t -> bool

  (** Whether a synchronous activity callback is executing now. It takes
      only the short delivery lock, never the adapter lock that the callback
      holds, so it is safe from any thread while the callback runs. *)
  val callback_running : t -> bool

  (** The number of admitted asynchronous activity leases ([WillCompleteAsync]
      handoffs Core accepted that external code has not yet completed), read
      without the adapter lock that a running callback holds (#495). These
      leases belong to no callback, so bounded shutdown must not attribute
      them to an abandoned one. [None] means the separate async lock is held
      right now, i.e. a request on an admitted lease is in flight, so at
      least one lease is outstanding; the probe never waits for it. *)
  val outstanding_async_leases : t -> int option
end

val register :
  ( 'input,
    'output,
    Temporal_base.Activity_context.t ->
    'input -> ('output, Temporal_base.Error.t) result )
  Temporal_base.Definition.t ->
  registered_activity
(** Wraps a public or internal activity definition without exposing the
    existential constructor used by [Make]. Definitions made with
    [Temporal.Activity.remote] are accepted here only so [create] can return a
    precise configuration error instead of silently dropping them. *)

val register_async :
  ( 'input,
    'output,
    'output Temporal_base.Async_activity.context ->
    'input -> 'output Temporal_base.Async_activity.async_result )
  Temporal_base.Definition.t ->
  registered_activity
(** Wraps an asynchronous activity definition for deferred completion. *)

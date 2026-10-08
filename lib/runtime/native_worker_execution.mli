(** Private worker-loop state for one OCaml-owned Temporal workflow worker.

    The functor below deliberately accepts already-decoded semantic protocol
    values. A native supervisor is responsible for decoding JSON, rejecting a
    malformed leased activation, and retiring that lease before returning its
    typed error. Keeping that responsibility below this module means this
    layer never guesses at a child/activity wire shape or silently drops a
    leased task. *)

module type SUPERVISOR = sig
  (** The opaque supervisor instance. Its implementation owns the native
      runtime/client/worker graph and serializes every operation on one owner
      Domain. *)
  type t

  (** Expected native-supervisor failure. The adapter copies only the stable
      code and message supplied by [error_code] and [error_message]; it never
      retains an exception or native pointer. *)
  type error

  (** The typed workflow poll operation. [None] means that the nonblocking
      poll observed no ready task; it is ordinary scheduler state. A returned
      [Error] must already have retired any leased activation that failed
      semantic decoding. *)
  val try_poll_workflow :
    t ->
    (Temporal_protocol.Workflow_protocol.activation option, error) result

  (** Submits one complete activation result as the canonical bytes from its
      single encoder pass, and retires the exact native lease named by the
      completion's run ID. The adapter has already validated the completion
      while encoding it, so the supervisor passes these bytes on without
      encoding them again (issue #846). A retried submission is given the same
      value, byte for byte.

      [completion] is the typed value those bytes were encoded from, supplied
      read-only so a test or benchmark source can inspect the submitted
      commands without parsing the JSON a second time. The encoded value is
      authoritative: a source must submit only its bytes and must never
      re-encode or mutate [completion]. [completion] may alias payload buffers
      owned by workflow code, so unlike the bytes it is not a snapshot. The
      production supervisor ignores it.

      [abandon_activation] calls this from the watchdog Domain while the
      workflow lane is still inside workflow code, so an implementation must
      accept a call from a Domain other than the one that polled. The two
      calls never overlap for one adapter. *)
  val complete_workflow :
    t ->
    completion:Temporal_protocol.Workflow_protocol.completion ->
    Temporal_protocol.Encoded_workflow_completion.t ->
    (unit, error) result

  (** Stable, bounded classification for a supervisor error. *)
  val error_code : error -> string

  (** Stable diagnostic for a supervisor error. Implementations must omit
      payload bytes, credentials, task tokens, and unbounded remote text. *)
  val error_message : error -> string

  (** Classifies a failed [complete_workflow] without inspecting free-form
      text. [true] asserts that the exact native lease is still outstanding,
      so resubmitting the same retained completion cannot duplicate it. Every
      other failure must return [false]; the adapter then never submits that
      completion again (issue #843). *)
  val error_is_retryable : error -> bool

  (** Classifies an exception raised by [complete_workflow]. An exception is
      an uncertain acknowledgement, so production supervisors return [false]
      unless they define a private, explicitly transient exception category. *)
  val exception_is_retryable : exn -> bool
end

(** Stable diagnostic exposed by this private worker loop. It is safe to log
    and intentionally excludes payload bytes and native handles. *)
type error_view = { code : string; path : string; message : string }

type activation_info = {
  run_id : string;
  workflow_id : string option;
  is_replaying : bool;
  history_length : int64;
  cache_removal_reason : string option;
}
(** Metadata observed after one activation has passed strict protocol
    translation. The callback receives no payloads, continuations, or native
    handles. It runs on the worker's serialized OCaml owner Domain, before
    workflow code is entered, so an activation diagnostic sink can prove
    replay or an explicit Core cache eviction without introducing an
    asynchronous cross-language callback. The same value may be delivered to
    the completion observer after Core has acknowledged the activation
    completion. *)

type abandonment =
  [ `Task_failed | `Queries_failed | `Eviction_acknowledged | `Not_acknowledged ]
(** What the watchdog submitted in place of the lane's completion and whether
    the supervisor acknowledged it. [`Task_failed]: an ordinary activation's
    workflow task was failed. [`Queries_failed]: a query-only activation's
    queries were answered with failures; the workflow task was not failed.
    [`Eviction_acknowledged]: an eviction-only activation received its empty
    acknowledgement. [`Not_acknowledged]: encoding or submission failed, so
    the task, query, or eviction is left to time out. *)

type stuck_activation = {
  run_id : string;
  workflow_id : string option;
  workflow_type : string option;
  is_replaying : bool;
  elapsed_ms : int;
  abandoned : abandonment;
}
(** Bounded identity of an activation abandoned by the non-yielding-code
    watchdog (#493). [workflow_id] and [workflow_type] are [None] only when the
    activation neither initialized the run nor matched a cached run.
    [elapsed_ms] is the watchdog's lower bound on the time the activation had
    spent in workflow code. [abandoned] describes the watchdog's replacement
    completion and its acknowledgement. No payload,
    task token, or failure text is retained. *)

(** One workflow definition registered with the worker. The existential
    wrapper preserves the input/output codec relationship while allowing one
    registry to contain heterogeneous workflow functions. *)
type registered_workflow

(** The validated signal event and private handler types used by a registered
    workflow. They are aliases to the execution runtime and expose no
    continuation or native handle. *)
type signal = Execution.signal
type signal_handler = Execution.signal_handler
type query = Execution.query
type query_handler = Execution.query_handler
type update = Execution.update
type update_handler = Execution.update_handler

(** Builds a private handler that is invoked only on its workflow scheduler. *)
val make_signal_handler :
  name:string ->
  dispatch:(signal -> (unit, Temporal_base.Error.t) result) ->
  signal_handler

(** Returns the one payload sequence delivered with a signal. The native public
    adapter uses this accessor to apply its exact-one-payload policy. *)
val signal_input : signal -> Temporal_base.Codec.payload list

(** Returns the validated sender identity retained with a signal. *)
val signal_identity : signal -> string

(** Returns the validated signal headers in their source order. *)
val signal_headers :
  signal -> (string * Temporal_base.Codec.payload) list

(** Returns a handler's stable Temporal name for registration validation. *)
val signal_handler_name : signal_handler -> string

(** Builds a synchronous query handler invoked inline on the owner Domain. *)
val make_query_handler :
  name:string ->
  dispatch:(query -> (Temporal_base.Codec.payload, Temporal_base.Error.t) result) ->
  query_handler

(** Returns query arguments retained at the protocol boundary. *)
val query_arguments : query -> Temporal_base.Codec.payload list

(** Returns query headers retained at the protocol boundary. *)
val query_headers : query -> (string * Temporal_base.Codec.payload) list

(** Returns a query handler's stable registration name. *)
val query_handler_name : query_handler -> string

(** Builds an update handler that runs on the execution owner Domain. *)
val make_update_handler :
  name:string ->
  dispatch:
    (run_validator:bool -> on_validated:(unit -> unit) -> update ->
     (Temporal_base.Codec.payload, Temporal_base.Error.t) result) ->
  update_handler

(** Returns a handler's stable update registration name. *)
val update_handler_name : update_handler -> string

(** Returns all payloads carried by an update activation. *)
val update_input : update -> Temporal_base.Codec.payload list

(** One worker-loop outcome. [Rejected] means a valid lease was completed with
    a non-retryable bridge failure, so the caller can log the typed rejection
    and continue polling. A supervisor error remains a [result] error because
    the lease could not be proven retired. *)
type outcome =
  | Not_ready
  | Completed of {
      run_id : string;
      command_count : int;
      terminal : bool;
    }
  | Rejected of {
      run_id : string option;
      error : error_view;
      lease_retired : bool;
    }

module Make (Supervisor : SUPERVISOR) : sig
  (** A private owner-confined registry of running workflow executions. Calls
      to [poll] are serialized by an internal mutex so callers may safely
      invoke it from multiple ordinary Domains; workflow fibers must not call
      it directly because native supervisor operations are blocking
      producer-Domain calls. *)
  type t

  (** Creates a registry after validating every executable definition and
      rejecting duplicate Temporal workflow type names. [task_queue] is checked
      before the registry is published; empty, NUL-containing, oversized, or
      non-UTF-8 values return a typed configuration error instead of failing
      the first workflow activation. A valid queue is copied into every
      execution context so an activity without an explicit queue is sent back
      to the same queue as its workflow worker. [namespace] is the worker's
      Temporal namespace; it is validated with the same rules (reported at
      path [$.namespace]) and copied into every execution context so
      [Temporal.Workflow.info] can report it, because Core activations do not
      carry it. Both default to ["default"]. No native operation is
      performed and no workflow function is called during creation. *)
  val create :
    ?on_activation:(activation_info -> unit) ->
    ?on_completion:(activation_info -> unit) ->
    ?task_queue:string ->
    ?namespace:string ->
    supervisor:Supervisor.t ->
    workflows:registered_workflow list ->
    unit ->
    (t, error_view) result

  (** Polls at most one activation, applies it to the deterministic execution
      selected by its run ID, and submits exactly one completion. Empty native
      lanes return [Ok Not_ready]. Unknown run IDs and invalid initialization
      inputs are converted to typed non-retryable workflow failures and
      reported as [Ok (Rejected _)] after their lease is retired. Child-start
      commands and two-stage child start/result resolutions are translated to
      Core without allowing a completed child to remain leased. *)
  val poll : t -> (outcome, error_view) result

  (** Retries completions whose native acknowledgement previously failed with
      an explicitly retryable classification. The adapter mutex remains held
      while this operation runs, so no new activation can overtake an older
      lease. [Ok ()] proves that the pending map is empty. [Error _] leaves the
      exact completion in place. A completion whose earlier failure was not
      classified retryable is never resubmitted: [drain] (and [poll]) return
      its recorded error without a native call. The caller must either retry
      after an explicitly safe transient classification or force-retire the
      native graph and then call [discard] on a terminal path; it must never
      silently drop this completion while Rust still owns it. *)
  val drain : t -> (unit, error_view) result

  (** Returns the epoch of the activation currently executing workflow code
      (codecs, observers, handlers, or workflow fibers), or [None] while the
      lane polls, waits, or submits a completion. Each leased activation gets
      a fresh epoch. This is a lock-free read intended for the watchdog
      Domain; it never blocks on the lane. *)
  val running_epoch : t -> int option

  (** [abandon_activation t ~epoch ~elapsed_ms] releases the lease of
      activation [epoch] if it is still executing workflow code. The watchdog
      and the lane race on one atomic claim, so exactly one of them completes
      the native lease: on winning, this submits a failure completion through
      the supervisor (a task failure, an empty eviction acknowledgement, or
      failed query answers, exactly as an adapter-level rejection would),
      records the sticky {!stuck} report with the matching {!abandonment},
      logs one bounded diagnostic, and returns it. When the lane later returns, its completion is dropped
      without a native call and, unless the activation only answered
      queries, its run is removed (a task failure makes Core evict it); the poll reports [Rejected] with code
      [activation_deadline_exceeded], or [Error] when the watchdog's
      submission was not acknowledged. Returns [None] when [epoch] is no
      longer running or the lane claimed the lease first.

      This is the only operation that calls [Supervisor.complete_workflow]
      without the adapter mutex, from a Domain other than the lane. It never
      interrupts, resumes, or inspects workflow code, and never runs while
      the lane is itself inside a supervisor call for this adapter. *)
  val abandon_activation :
    t -> epoch:int -> elapsed_ms:int -> stuck_activation option

  (** The first activation abandoned by the watchdog, if any. The report is
      sticky for the lifetime of the adapter: workflow code that failed to
      yield may have corrupted process state, so recovery is a process
      restart. *)
  val stuck : t -> stuck_activation option

  (** Discards all retained completion bytes and shuts down every OCaml-owned
      execution after terminal native cleanup. This is irreversible and must
      be called only after the supervisor has force-retired its native leases;
      it never attempts another completion. *)
  val discard : t -> unit

end

(** Wraps a public workflow definition in the private existential registration
    used by [Make]. Remote definitions are accepted by this constructor so the
    registry can report a typed configuration error at [create] rather than
    silently pretending they are executable. *)
val register :
  ?signal_handlers:signal_handler list ->
  ?query_handlers:query_handler list ->
  ?update_handlers:update_handler list ->
  ('input, 'output,
   'input -> ('output, Temporal_base.Error.t) result)
  Temporal_base.Definition.t ->
  registered_workflow

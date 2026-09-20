(** Immutable metadata recorded on this run's start event. Payload bytes are
    copied on installation and observation; later search-attribute upserts do
    not change this historical snapshot. [None] and [Some []] remain distinct. *)
type start_metadata = {
  memo : (string * Temporal_base.Payload.t) list option;
  search_attributes : (string * Temporal_base.Payload.t) list option;
  execution_expiration_time : Temporal_protocol.Workflow_protocol.timestamp option;
}

(** The activities, timers, and commands belonging to one workflow execution.
    The runtime temporarily makes this context current while running that
    workflow's OCaml code. *)
type t

(** A typed key for one value stored separately in every workflow execution. *)
type 'a local

(** Creates an empty context whose futures use [scheduler]. [task_queue] is the
    deterministic default used by activities that do not override their queue;
    a worker supplies its own queue when it creates an execution. *)
val create :
  ?task_queue:string ->
  ?randomness_seed:string ->
  Scheduler.t ->
  t

(** Checks a worker's implicit activity queue without allocating a context.
    [Ok ()] means that the byte string is non-empty, contains no NUL byte, is
    at most 65,536 bytes, and is valid UTF-8. The worker adapter uses this
    result to reject invalid configuration before accepting a worker; direct
    [create] callers still receive [Invalid_argument] for the same defect. *)
val validate_task_queue : string -> (unit, string) result

(** Returns the context installed on the current OCaml Domain, if any. *)
val current : unit -> t option

(** Allocates a key for execution-local workflow state. The key itself may be
    retained by a workflow definition and its registered interaction handlers;
    values written through it never cross execution-context boundaries. *)
val create_local : unit -> 'a local

(** Reads a key from one execution context, returning [None] until that run has
    assigned a value. *)
val get_local : t -> 'a local -> 'a option

(** Writes a key in one execution context. *)
val set_local : t -> 'a local -> 'a -> unit

(** Installs this run's start snapshot before workflow code executes. *)
val set_start_metadata : t -> start_metadata option -> unit

(** Returns a copy of the historical start snapshot, or [None] when this is a
    synthetic execution without initialization metadata. *)
val start_metadata : t -> start_metadata option

(** Records the timestamp attached to the activation that is about to run user
    workflow code. [None] is used for synthetic runtime activations, such as
    cache eviction, where no workflow clock is available. *)
val set_activation_timestamp :
  t -> Temporal_protocol.Workflow_protocol.timestamp option -> unit

(** Returns the timestamp captured for the current activation, if one exists.
    The public [Temporal.Workflow.now] wrapper converts it to its abstract
    deterministic time type. *)
val activation_timestamp :
  t -> Temporal_protocol.Workflow_protocol.timestamp option

(** Records the deployment/build identity attached to the current activation;
    [None] clears the value so metadata cannot leak between tasks. *)
val set_activation_deployment_version : t -> (string * string) option -> unit

(** Returns the deployment/build identity for the current activation, if Core
    supplied one. *)
val activation_deployment_version : t -> (string * string) option

(** Records whether Core is replaying history for the activation about to run.
    This must be installed before patch notifications and workflow dispatch. *)
val set_activation_is_replaying : t -> bool -> unit

(** Records authoritative history evidence that [patch_id] is present for this
    execution. The ID has already passed the closed protocol validator and is
    copied before this execution retains it. *)
val notify_has_patch : t -> patch_id:string -> unit

(** Returns this execution's deterministic decision for [patch_id] and emits a
    non-deprecated marker command on every call. The first decision is true in
    live execution and false during replay unless Core previously notified the
    marker. The ID is copied before retention and emission. Calling this after
    shutdown raises [Invalid_argument]. *)
val patched : t -> patch_id:string -> bool

(** Emits a deprecated marker command for [patch_id] and retains the same
    execution-local decision state used by [patched]. Repeated deprecation calls
    are allowed. Mixing [patched] and [deprecate_patch] for one ID in the same
    execution raises [Invalid_argument] before a conflicting command is emitted;
    Core would otherwise keep only the first mode. The ID is copied before
    retention and emission, and calls after shutdown raise [Invalid_argument]. *)
val deprecate_patch : t -> patch_id:string -> unit

(** Draws one deterministic integer in [0, bound).  The stream is seeded from
    Temporal's initialization metadata and advances only in this execution's
    owner Domain.  Invalid bounds and lifecycle misuse are typed defects. *)
val random_int : t -> bound:int -> (int, Temporal_base.Error.t) result

(** Runs a callback in the read-only mode used by live update validators.
    Deterministic workflow helpers that would mutate execution state, such as
    [random_int], return a typed defect while this callback is active. *)
val with_randomness_disabled : t -> (unit -> 'value) -> 'value

(** Emits one durable command that merges encoded values into the workflow's
    indexed search attributes. Keys and payloads are validated by the strict
    protocol encoder before Core receives the completion. *)
val upsert_search_attributes :
   t -> (string * Temporal_base.Codec.payload) list -> unit

(** Runs [action] with [t] dynamically installed and restores the previous
    context even if [action] raises. Nested calls are supported. *)
val with_context : t -> (unit -> 'value) -> 'value

(** Runs [action] with no workflow context installed and restores the previous
    context afterward. Infrastructure callbacks that must not re-enter
    deterministic workflow state use this boundary. *)
val without_context : (unit -> 'value) -> 'value

(** Creates an already-resolved future owned by the context scheduler. *)
val resolved :
  t ->
  ('value, Temporal_base.Error.t) result ->
  ('value, Temporal_base.Error.t) Future_store.t

(** Creates a pending, scheduler-owned notification future and its resolver.
    This is an internal coordination primitive for deterministic workflow
    helpers such as cancellation scopes; it emits no Temporal command and is
    closed automatically with the workflow context. *)
val create_signal :
  t ->
  (unit, Temporal_base.Error.t) Future_store.t
  * (unit, Temporal_base.Error.t) Future_store.resolver

(** Evaluates a condition predicate immediately and, if false, suspends the
    current workflow fiber on the execution's private condition store. *)
val wait_until :
  t ->
  predicate:(unit -> (bool, Temporal_base.Error.t) result) ->
  (unit, Temporal_base.Error.t) result

(** Re-evaluates all false condition predicates in deterministic registration
    order and reports whether any waiter was settled. *)
val notify_conditions : t -> bool

(** Creates a detached failed future for an operation attempted without an
    active workflow context. *)
val detached_error :
  message:string -> ('value, Temporal_base.Error.t) Future_store.t

(** Assigns a sequence number, records how to decode the eventual result,
    produces a complete schedule-activity command, and returns the decoded
    output future together with an owner-checked cancellation operation.
    [activity_id] and [task_queue] default deterministically; when all timeout
    options are absent, a 60-second start-to-close timeout is used because
    Temporal requires at least one activity timeout. The cancellation operation
    emits at most one activity-cancellation command for its sequence and
    remains a valid no-op after a terminal result or activity-start failure.
    Calls made without this context current are typed lifecycle defects. *)
val schedule_activity :
  t ->
  name:string ->
  input:Temporal_base.Codec.payload ->
  ?activity_id:string ->
  ?task_queue:string ->
  ?schedule_to_close_timeout:int64 ->
  ?schedule_to_start_timeout:int64 ->
  ?start_to_close_timeout:int64 ->
  ?heartbeat_timeout:int64 ->
  ?retry_policy:Activation.retry_policy ->
  ?priority:Activation.priority ->
  ?cancellation_type:Activation.activity_cancellation_type ->
  ?do_not_eagerly_execute:bool ->
  ?local:bool ->
  decode:(Temporal_base.Codec.payload -> ('output, Temporal_base.Error.t) result) ->
  unit ->
  ( ('output, Temporal_base.Error.t) Future_store.t
  * (unit -> (unit, Temporal_base.Error.t) result) )

(** Schedules one local activity through Temporal Core's local-activity lane.
    Unlike [schedule_activity], this command has no remote task queue,
    heartbeat timeout, priority, or eager-execution setting; Core retries it
    locally and records the result in workflow history for replay. *)
val schedule_local_activity :
  t ->
  name:string ->
  input:Temporal_base.Codec.payload ->
  ?activity_id:string ->
  ?schedule_to_close_timeout:int64 ->
  ?schedule_to_start_timeout:int64 ->
  ?start_to_close_timeout:int64 ->
  ?retry_policy:Activation.retry_policy ->
  ?cancellation_type:Activation.activity_cancellation_type ->
  decode:(Temporal_base.Codec.payload -> ('output, Temporal_base.Error.t) result) ->
  unit ->
  ( ('output, Temporal_base.Error.t) Future_store.t
  * (unit -> (unit, Temporal_base.Error.t) result) )

(** Assigns a private correlation sequence, records how to decode the child
    result, emits a command containing the application-supplied durable [id]
    and optional Core-owned retry policy, and returns the child result future
    together with a cancellation operation.
    The default policy is [Child_try_cancel], so cancellation requests the
    child unless a caller explicitly chooses [Child_abandon]. The operation is
    valid only while this context is current; it emits at most one cancellation
    command and leaves Core to resolve the future. A repeated valid call is a
    no-op, including after Core has delivered a terminal result or start
    failure; a call after context shutdown remains a typed lifecycle defect. *)
val start_child_workflow :
  t ->
  id:string ->
  name:string ->
  input:Temporal_base.Codec.payload ->
  ?retry_policy:Activation.retry_policy ->
  ?cancellation_type:Activation.child_workflow_cancellation_type ->
  decode:(Temporal_base.Codec.payload -> ('output, Temporal_base.Error.t) result) ->
  unit ->
  ( ('output, Temporal_base.Error.t) Future_store.t
  * (reason:string -> (unit, Temporal_base.Error.t) result) )

(** Sends a signal to an external workflow and returns a future resolved by
    Core's delivery acknowledgement or structured failure. *)
val signal_external_workflow :
  t ->
  workflow_id:string ->
  run_id:string ->
  signal_name:string ->
  input:Temporal_base.Codec.payload list ->
  ?child_workflow_only:bool ->
  ?headers:(string * Temporal_base.Codec.payload) list ->
  unit ->
  (unit, Temporal_base.Error.t) Future_store.t

(** Requests cancellation of an external workflow and returns a future for
    Core's asynchronous resolution. *)
val cancel_external_workflow :
  t ->
  workflow_id:string ->
  run_id:string ->
  reason:string ->
  unit ->
  (unit, Temporal_base.Error.t) Future_store.t

(** Emits a durable timer command and returns a future resolved by [fire_timer].
    The duration is an exact non-negative millisecond count. *)
val start_timer : t -> int64 -> (unit, Temporal_base.Error.t) Future_store.t

(** Supplies an activity result to the pending future with this sequence number
    and then removes it. An unknown or repeated number returns a non-retryable
    bridge error because Core and the OCaml runtime disagree about state. *)
val resolve_activity :
  t ->
  seq:int64 ->
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result ->
  (unit, Temporal_base.Error.t) result

(** Retains a pending local activity while scheduling the workflow timer Core
    requested for a long retry backoff. The next timer activation re-emits the
    same activity sequence with the supplied attempt and original schedule
    timestamp; terminal resolution remains responsible for completing the
    activity future. *)
val resolve_local_activity_backoff :
  t ->
  seq:int64 ->
  attempt:int64 ->
  backoff_milliseconds:int64 ->
  original_schedule_time:Temporal_protocol.Workflow_protocol.timestamp option ->
  (unit, Temporal_base.Error.t) result

(** Completes and removes the pending child workflow with this private
    correlation sequence. Unknown and repeated numbers are bridge defects. *)
val resolve_child_workflow :
  t ->
  seq:int64 ->
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result ->
  (unit, Temporal_base.Error.t) result

(** Completes one external workflow signal/cancellation operation. *)
val resolve_external_workflow :
  t ->
  operation:[ `Cancel | `Signal ] ->
  seq:int64 ->
  (unit, Temporal_base.Error.t) result ->
  (unit, Temporal_base.Error.t) result

(** Records a child start acknowledgment. [Ok run_id] advances the pending
    child without resolving its result future; [Error] retires the child and
    resolves that future with the typed start failure. *)
val resolve_child_workflow_start :
  t ->
  seq:int64 ->
  (string, Temporal_base.Error.t) result ->
  (unit, Temporal_base.Error.t) result

(** Completes and removes the pending timer with this sequence number. *)
val fire_timer : t -> seq:int64 -> (unit, Temporal_base.Error.t) result

(** Appends a command to the current activation output buffer. *)
val emit : t -> Activation.command -> unit

(** Buffers a terminal command and stops the current workflow fiber. This is a
    package-private control boundary used by terminal workflow operations. *)
val terminate : t -> Activation.command -> 'value

(** Buffers a continue-as-new command and stops the current workflow fiber.
    The successor input is encoded before it reaches this private operation. *)
val continue_as_new :
  t -> workflow_type:string -> input:Temporal_base.Codec.payload -> 'value

(** True when the pending command buffer already holds a terminal workflow
    command such as complete, fail, cancel, or continue-as-new. *)
val has_buffered_terminal : t -> bool

(** Returns buffered commands in emission order and atomically clears them. *)
val take_commands : t -> Activation.command list

(** Closes the scheduler and removes all saved activity, child workflow, and
    timer callbacks. Calling it more than once is safe. *)
val shutdown : t -> unit

(** Reason a call to the linked Rust library failed. [Unknown code] retains a
    numeric status introduced by a newer Rust bridge instead of losing it. *)
type status =
  | Invalid_argument
  | Abi_mismatch
  | Panic
  | Internal
  | Invalid_state
  | Configuration
  | Connection
  | Worker
  | Outstanding_tasks
  | Not_ready
  | Protocol
  | Already_started
  | Retryable
  | Async_heartbeat_rejected
  | Resource_exhausted
  | Unknown of int

(** Error copied into the OCaml heap. Once returned, it contains no pointer to
    Rust memory. *)
type error = {
  status : status;
  message : string;
}

(** Private owner of the Temporal Core runtime and Tokio executor for one SDK
    instance. It is intentionally abstract and never enters the public API. *)
type runtime

(** Private owner of one Core runtime (Tokio executor) that several [runtime]
    graphs can share (#832). Each graph created by {!runtime_attach} holds
    its own native reference to that Core, so closing this value never frees
    Core under a live graph; Core is destroyed by whichever releases last. *)
type shared_runtime

(** Validated settings for one official Temporal client connection. The JSON
    representation and concrete fields remain private so callers cannot bypass
    sender-side transport validation. *)
type client_config

(** Validated settings for one workflow-only Core worker. Construction does
    not perform network access; the supervisor sends it to Rust at start. *)
type worker_config

(** The default workflow versioning behavior for deployment-based workers. *)
type default_versioning_behavior =
  | Auto_upgrade
  | Pinned

(** Selects how Temporal routes workflow tasks to this worker. [No_versioning]
    preserves the existing unversioned behavior while retaining the build ID as
    worker metadata. [Legacy_build_id] enables Temporal's whole-worker legacy
    build-ID versioning. [Deployment_based] opts into Temporal's modern
    deployment versioning with an explicit deployment name and build ID. *)
type worker_versioning =
  | No_versioning
  | Legacy_build_id of string
  | Deployment_based of {
      deployment_name : string;
      build_id : string;
      use_worker_versioning : bool;
      default_versioning_behavior : default_versioning_behavior option;
    }

(** Version of the C-compatible interface expected by this OCaml code. *)
val abi_version : int32

(** Checks whether the linked Rust library implements [requested_version]. *)
val check_abi_version : int32 -> (unit, error) result

(** Sends bytes through the Rust allocation boundary and copies them back. This
    exists to test memory ownership; workflow code does not use it. *)
val echo : bytes -> (bytes, error) result

(** Returns a monotonic clock reading in nanoseconds with an arbitrary
    origin. Readings are only meaningful relative to each other in one
    process; client RPC deadlines (#499) use them so the time a request spends
    queued in the supervisor counts against its budget and wall-clock changes
    cannot move a deadline. Raises [Failure] only if the platform has no
    monotonic clock, which is a host defect. *)
val monotonic_now_ns : unit -> int64

(** Waits in Rust for at most 1,000 milliseconds while allowing other OCaml
    Domains to run. This tests the blocking-call design used by future worker
    polling. Values outside 0 through 1,000 return an error. *)
val conformance_wait_ms : int -> (unit, error) result

(** Validates connection settings without opening a connection. The bridge
    applies only transport-safety checks; Core and Temporal Server retain
    authority over namespace-configurable semantic limits. *)
val client_config :
  target_url:string -> identity:string -> (client_config, error) result

(** Autoscaling bounds for Core's workflow-task pollers: at least [minimum]
    and at most [maximum] concurrent polls, starting from [initial]. *)
type poller_autoscaling = { minimum : int; maximum : int; initial : int }

(** Optional Core worker settings added by #498. Each [None] keeps Core's
    default. Durations are milliseconds and rates are tasks per second.

    - [workflow_task_poller_autoscaling] replaces the fixed workflow poller
      count with Core's server-driven autoscaling; its [maximum] must equal
      [max_concurrent_workflow_task_polls]. Core applies these bounds to the
      sticky and normal poll buffers separately, whereas it splits a fixed
      count between them, so the two-poller cache rule applies only to a
      fixed count.
    - [sticky_queue_schedule_to_start_timeout_ms] (Core default 10 s) bounds
      how long a task waits on this worker's sticky queue before the server
      moves it to the normal queue.
    - [max_heartbeat_throttle_interval_ms] (default 60 s) and
      [default_heartbeat_throttle_interval_ms] (default 30 s) bound how often
      activity heartbeats are flushed to the server.
    - [max_worker_activities_per_second] limits this worker's remote activity
      starts; [max_task_queue_activities_per_second] asks the server to limit
      the whole task queue, and the most recent poller's value wins.

    An all-[None] value is not serialized, so a default worker sends exactly
    the document that bridges built before #498 accept. *)
type worker_tuning = {
  workflow_task_poller_autoscaling : poller_autoscaling option;
  sticky_queue_schedule_to_start_timeout_ms : int64 option;
  max_heartbeat_throttle_interval_ms : int64 option;
  default_heartbeat_throttle_interval_ms : int64 option;
  max_worker_activities_per_second : float option;
  max_task_queue_activities_per_second : float option;
}

(** Every tuning field set to [None]. *)
val default_worker_tuning : worker_tuning

(** Validates worker settings without constructing a worker.
    Counts are explicit so resource policy is visible to the application.

    [workflow_tasks] and [activity_tasks] (both default [true]) select which
    task kinds Core polls from the server. A worker must pass [false] for a
    kind it has no registered implementation for, or it would take and fail
    tasks that a sibling worker on the same task queue could execute. Local
    activities follow [workflow_tasks] because Core dispatches them in-process.
    At least one kind must be enabled; otherwise a [Configuration] error is
    returned. Replay workers ignore both: Core forces workflow-only replay.

    [tuning] (default {!default_worker_tuning}) is validated here and again
    by Rust: tuning durations must be between 1 ms and one day, rates must be
    positive finite numbers, the worker rate must be at least one per day, an explicit default heartbeat throttle interval
    must not exceed an explicit maximum, and autoscaling bounds must satisfy
    [1 <= minimum <= initial <= maximum]. *)
val worker_config :
  namespace:string ->
  task_queue:string ->
  build_id:string ->
  ?versioning:worker_versioning ->
  max_cached_workflows:int ->
  max_outstanding_workflow_tasks:int ->
  max_concurrent_workflow_task_polls:int ->
  graceful_shutdown_timeout_ms:int64 ->
  ?tuning:worker_tuning ->
  ?workflow_tasks:bool ->
  ?activity_tasks:bool ->
  unit ->
  (worker_config, error) result

(** The exact private JSON document that worker startup sends to Rust. It
    contains only routing and resource settings, never credentials, so it can
    be logged or compared in tests to inspect the effective configuration. *)
val worker_config_document : worker_config -> string

(** Largest explicit Tokio worker-thread count accepted by [runtime_create]. *)
val max_runtime_worker_threads : int

(** Returns [Invalid_argument] unless an explicit count is between [1] and
    [max_runtime_worker_threads]; [None] is always valid. *)
val validate_runtime_worker_threads : int option -> (unit, error) result

(** Creates a native runtime after checking that the statically linked bridge
    implements the compatibility contract expected by this OCaml build.
    [worker_threads] bounds the runtime's Tokio worker pool (#832); when
    omitted the bridge uses the host's available parallelism capped at 4. An
    out-of-range count returns [Invalid_argument] before anything is
    allocated. *)
val runtime_create : ?worker_threads:int -> unit -> (runtime, error) result

(** Connects the official Core-based Temporal client. The network wait occurs
    in Rust while the C stub has released the OCaml runtime lock. *)
val client_connect : runtime -> client_config -> (unit, error) result

(** Starts one dynamically named workflow through the connected Rust client.
    The returned bytes are a strictly validated client-start response; a
    duplicate workflow ID is returned as [Already_started] with its closed
    structured error document. *)
val client_start_workflow_json : runtime -> bytes -> (bytes, error) result

(** Requests cancellation of one exact workflow run. A successful response is
    only the server acknowledgement; callers must use the exact-run wait
    operation to observe the eventual [Cancelled] terminal outcome. *)
val client_cancel_workflow_json : runtime -> bytes -> (bytes, error) result

(** Resets one exact workflow run and returns the newly assigned run identity. *)
val client_reset_workflow_json : runtime -> bytes -> (bytes, error) result

(** Requests immediate termination of one exact workflow run. *)
val client_terminate_workflow_json : runtime -> bytes -> (bytes, error) result

(** Sends one typed signal to one exact workflow run. A successful response is
    only the server acknowledgement; it does not wait for workflow code to
    process the signal. *)
val client_signal_workflow_json : runtime -> bytes -> (bytes, error) result

(** Lists one bounded visibility page through Rust's official client. *)
val client_list_visibility_json : runtime -> bytes -> (bytes, error) result

(** Executes one output-only query against one exact workflow run. *)
val client_query_workflow_json : runtime -> bytes -> (bytes, error) result

(** Starts one workflow update and waits until it is admitted. *)
val client_update_workflow_json : runtime -> bytes -> (bytes, error) result

(** Polls one admitted workflow update for a bounded interval. *)
val client_poll_update_workflow_json : runtime -> bytes -> (bytes, error) result

(** Admits one asynchronous workflow start and returns a strict opaque ticket
    document. Rust owns the pending task and its request metadata until a
    later poll or bounded wait reaches a terminal outcome. *)
val client_begin_start_workflow_json : runtime -> bytes -> (bytes, error) result

(** Polls one asynchronous start ticket without waiting. [Not_ready] means the
    request remains in flight; a successful response is a terminal
    accepted/rejected/unknown outcome document and retires the ticket. *)
val client_poll_start_workflow_json : runtime -> bytes -> (bytes, error) result

(** Waits for one bounded interval for an asynchronous start ticket. The C
    binding releases the OCaml runtime lock around the native wait, and
    [Not_ready] asks the supervisor to service its mailbox and retry. *)
val client_wait_start_workflow_json : runtime -> bytes -> (bytes, error) result

(** Waits for one exact workflow run. Each call polls the retained Rust history
    future for at most 100 ms while the C stub releases the OCaml runtime lock.
    [Not_ready] preserves the request and pagination state for the next call;
    continued-as-new is returned as a terminal response and is never followed
    implicitly. *)
val client_wait_workflow_json : runtime -> bytes -> (bytes, error) result

(** Completes an activity already handed off with [WillCompleteAsync] through
    the namespace-bound Temporal client. This does not touch the worker's
    outstanding-task ledger. *)
val client_complete_async_activity_json : runtime -> bytes -> (unit, error) result

(** Records a heartbeat for an admitted asynchronous activity through the
    namespace-bound client. *)
val client_record_async_activity_heartbeat_json :
  runtime -> bytes -> (unit, error) result

(** Constructs a workflow-only worker and completes Core namespace validation
    before publishing it into the owned graph. *)
val worker_start : runtime -> worker_config -> (unit, error) result

(** Constructs the private workflow-only replay worker. It does not require a
    client connection because histories arrive through the bounded feeder. *)
val replay_worker_start : runtime -> worker_config -> (unit, error) result

(** Validates and feeds one strict replay-history JSON document. The native
    OCaml side checks the closed envelope and canonical payload first; the
    native feeder repeats those checks, accepts one queued history, and applies
    backpressure to later calls. *)
val replay_worker_feed_history : runtime -> bytes -> (unit, error) result

(** Closes replay input. Already queued histories remain available to drain. *)
val replay_worker_finish_input : runtime -> (unit, error) result

(** Takes one ready replay activation without waiting. *)
val replay_worker_try_poll_workflow : runtime -> (bytes, error) result

(** Waits for replay readiness without consuming a task. *)
val replay_worker_wait_workflow : runtime -> (unit, error) result

(** Validates and completes one previously leased replay activation. *)
val replay_worker_complete_workflow_json :
  runtime -> bytes -> (unit, error) result

(** Retires one replay activation that OCaml could not decode. *)
val replay_worker_reject_workflow_json :
  runtime -> bytes -> (unit, error) result

(** Finalizes a naturally drained replay. A failure retains the native graph. *)
val replay_worker_finalize : runtime -> (unit, error) result

(** Explicitly abandons replay and force-completes native debts. *)
val replay_worker_dispose : runtime -> (unit, error) result

(** Takes one ready workflow activation without waiting. [Not_ready] is an
    expected empty-lane result. Successful bytes are a closed semantic JSON
    document copied into the OCaml heap. *)
val worker_try_poll_workflow : runtime -> (bytes, error) result

(** Waits for workflow readiness without consuming a task. The native wait is
    bounded and releases the OCaml runtime lock; [Not_ready] requests a retry
    so the supervisor mailbox can process lifecycle messages. *)
val worker_wait_workflow : runtime -> (unit, error) result

(** Validates and completes one previously leased workflow activation. The
    completion JSON must identify the exact run returned by the poll operation.
    Rust retains no input bytes after the call returns. *)
val worker_complete_workflow_json :
  runtime -> bytes -> (unit, error) result

(** Returns the exact Rust-produced activation after an OCaml semantic decode
    failure. Rust reparses and matches the complete retained activation before
    failing Core and retiring its one-shot lease. *)
val worker_reject_workflow_json : runtime -> bytes -> (unit, error) result

(** Takes one ready remote activity task without waiting. Successful bytes are
    a closed semantic activity-task JSON document. *)
val worker_try_poll_activity : runtime -> (bytes, error) result

(** Waits for remote-activity readiness without consuming a task. It has the
    same bounded lock-release semantics as [worker_wait_workflow]. *)
val worker_wait_activity : runtime -> (unit, error) result

(** Waits for readiness on either worker lane without consuming a task. It has
    the same bounded lock-release semantics as [worker_wait_workflow], but a
    queued task on either lane ends it, so the worker loop's single idle wait
    cannot sleep through work on the lane it did not expect. *)
val worker_wait_any : runtime -> (unit, error) result

(** Applies the fixed native delay used only after an explicit retryable
    activity-completion transport outcome. *)
val worker_wait_activity_completion_retry_backoff :
  runtime -> (unit, error) result

(** Validates and completes one previously leased remote activity task. The
    opaque task token in the JSON must match the poll result exactly. *)
val worker_complete_activity_json :
  runtime -> bytes -> (unit, error) result

(** Validates and submits progress for a currently leased remote activity. The
    task remains outstanding for its later terminal completion. The result is
    acknowledgement-only; pinned Temporal Core delivers cancellation, pause,
    and reset flags asynchronously in a later Cancel task. *)
val worker_record_activity_heartbeat_json :
  runtime -> bytes -> (unit, error) result

(** Returns the exact Rust-produced task after an OCaml semantic decode
    failure. Rust matches the complete retained task, extracts its canonical
    opaque token, and retires that native obligation exactly once. *)
val worker_reject_activity_json : runtime -> bytes -> (unit, error) result

(** Gracefully finalizes the worker. Absence is treated as already shut down,
    making sequential repeated calls safe. *)
val worker_shutdown : runtime -> (unit, error) result

(** Drops the connected client after its worker is absent. Absence is treated
    as already disconnected. *)
val client_disconnect : runtime -> (unit, error) result

(** Destroys the complete native graph in worker-client-runtime order.
    Repeating this call on the same value is safe; explicit child operations
    remain useful for deterministic diagnostics but are not required for
    leak-free defensive cleanup. *)
val runtime_close : runtime -> (unit, error) result

(** Creates a Core runtime that several graphs can share, after the same ABI
    check and [worker_threads] validation as {!runtime_create}. The value
    holds no client or worker. *)
val shared_runtime_create :
  ?worker_threads:int -> unit -> (shared_runtime, error) result

(** Creates one graph (used and closed exactly like a {!runtime_create}
    graph) that runs on [shared]'s Core instead of building its own. A closed
    [shared] returns [Invalid_argument]. Callers must not close [shared]
    concurrently with this call; the C borrow gate makes such a race return
    an error rather than a use-after-free. *)
val runtime_attach : shared_runtime -> (runtime, error) result

(** Releases [shared]'s Core reference, waiting with the OCaml runtime lock
    released. Core is destroyed before this returns only if no attached graph
    remains; otherwise the last graph's {!runtime_close} destroys it.
    Repeating the call is safe. *)
val shared_runtime_close : shared_runtime -> (unit, error) result

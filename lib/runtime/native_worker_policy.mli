(** Classification rules shared by the native worker and its focused tests.

    Keeping these predicates in the runtime library gives the retry and
    shutdown decisions a small, pure surface. They never inspect diagnostic
    text and they do not own a native handle. *)

type drain_failure =
  | Workflow_drain
  | Activity_drain of bool
(** Source of a failed adapter drain. The boolean records the adapter's
    explicit transient classification, not a guess based on the message. *)

val activity_completion_retryable :
  Temporal_core_bridge.Native_bridge.status -> bool
(** Returns [true] only for the bilateral retryable-completion status. Generic
    connection, readiness, worker, protocol, and closed states are false. *)

type async_heartbeat_disposition = Retry_exact | Rejected_live | Retired
(** Whether a failed heartbeat must retry the identical request, can replace a
    definitively rejected request while retaining its live activity lease, or
    must retire the handle after token loss or unusable native state. *)

val async_heartbeat_disposition :
  Temporal_core_bridge.Native_bridge.status -> async_heartbeat_disposition
(** Classifies typed bridge statuses without reading diagnostic text.
    [Connection] is uncertain; [Async_heartbeat_rejected] and local preflight
    failures leave the handle live; [Invalid_state] closes a lost token. *)

val shutdown_retryable : drain_failure -> bool
(** Returns [true] only when an activity adapter retained a completion after an
    explicitly transient native failure. Workflow drains and all permanent
    errors must leave a worker terminal rather than reopening it. *)

val needs_native_cleanup : drain_failure -> bool
(** Returns [true] when a failed drain must be followed by native graph
    disposal. Only the explicitly retryable activity case may leave the graph
    open for another completion attempt. *)

type closed_flag_action =
  | Leave_unchanged
  | Write of bool
(** How a [shutdown] admission decision must treat the shared [closed] stop
    flag. [Leave_unchanged] performs no write; [Write] overwrites it. *)

val reentrant_same_domain_shutdown : closed_flag_action * bool
(** The flag effect of rejecting a re-entrant same-Domain [shutdown]. The
    [closed_flag_action] is always [Leave_unchanged]: a concurrent [shutdown] on
    another Domain may already have set [closed] to stop the run loop, so this
    branch must not write it (a write would race that caller and could strand
    the loop). The [bool] is the [shutdown_retryable] value to raise so the
    admission failure can be retried from another Domain after the loop exits. *)

(** Runs a terminal cleanup while preserving the original adapter error. The
    boolean result is [true] when the cleanup callback returned either [Ok] or
    [Error]; both outcomes mean the native implementation reached its
    documented terminal-release path. It is [false] only when the callback
    raised before returning, in which case callers must retain adapter state
    and arrange a last-resort retry or finalizer path. A cleanup diagnostic is
    sent to [on_cleanup_error] rather than replacing the error that explains
    why the worker became terminal. [on_cleanup_exception] handles an
    unexpected cleanup exception for the same reason. *)
val retain_original_error :
  cleanup:(unit -> ('ok, 'cleanup_error) result) ->
  on_cleanup_error:('cleanup_error -> unit) ->
  on_cleanup_exception:(exn -> unit) ->
  'original -> bool * 'original

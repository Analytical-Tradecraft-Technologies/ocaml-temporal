(** Classification rules shared by the native worker and its focused tests.

    Keeping these predicates in the runtime library gives the retry and
    shutdown decisions a small, pure surface. They never inspect diagnostic
    text and they do not own a native handle. *)

val activity_completion_retryable :
  Temporal_core_bridge.Native_bridge.status -> bool
(** Returns [true] only for the bilateral retryable-completion status. Generic
    connection, readiness, worker, protocol, and closed states are false. *)

type async_operation_disposition = Retry_exact | Rejected_live | Retired
(** Ownership outcome of a failed namespace-bound async activity request.
    [Retry_exact]: the outcome is uncertain and the live lease is retained; a
    terminal operation may only be retried byte-for-byte, while a heartbeat is
    dropped. [Rejected_live]: the request was definitively not applied and the
    handle stays live for a corrected or different operation. [Retired]: the
    token is gone or the native graph is unusable. *)

val async_operation_disposition :
  Temporal_core_bridge.Native_bridge.status -> async_operation_disposition
(** Classifies typed bridge statuses for async heartbeats and async
    complete/fail/cancel alike, without reading diagnostic text.
    [Connection] is uncertain; [Async_heartbeat_rejected] (which the bridge
    emits for every definitively rejected async request) and local preflight
    failures leave the handle live; [Invalid_state] closes a lost token. *)

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


(** Pure lifecycle predicates for the private native worker.

    The pinned Temporal Core revision currently consumes activity completions
    and logs network failures internally. Consequently the only status that
    can authorize a future completion retry is the dedicated bilateral
    [Retryable] category; this module deliberately fails closed for every
    existing generic bridge status. Namespace-bound async activity requests
    do not use a Core worker lease and are classified separately by
    [async_operation_disposition]. *)

type drain_failure =
  | Workflow_drain
  | Activity_drain of bool

(** Returns whether a native activity-completion error proves that the lease
    remains available for another submission. The current Core bridge can only
    make that claim through the dedicated bilateral status. *)
let activity_completion_retryable = function
  | Temporal_core_bridge.Native_bridge.Retryable -> true
  | _ -> false

(** The three ownership outcomes of a failed namespace-bound async activity
    request (heartbeat, complete, fail, or cancel). These RPCs address the
    server by task token and never consume a Core worker lease, so unlike
    worker completions they cannot fail closed on an uncertain transport
    result: doing so would drop a still-live activity. *)
type async_operation_disposition = Retry_exact | Rejected_live | Retired

(** Classifies a typed async-operation result without inspecting server
    diagnostic text. [Connection] (and the bilateral [Retryable]) means the
    outcome is unknown: the lease stays registered and the base handle decides
    whether the exact request must be retried (terminal operations) or may be
    dropped (heartbeats). The explicit rejected status means the request was
    not applied but does not prove that the activity token is gone; local
    argument, protocol, and configuration failures likewise have not crossed
    the RPC boundary. Every other status, including [Invalid_state] for a
    server [NotFound], retires the handle. *)
let async_operation_disposition = function
  | Temporal_core_bridge.Native_bridge.Connection
  | Temporal_core_bridge.Native_bridge.Retryable -> Retry_exact
  | Temporal_core_bridge.Native_bridge.Async_heartbeat_rejected
  | Temporal_core_bridge.Native_bridge.Invalid_argument
  | Temporal_core_bridge.Native_bridge.Protocol
  | Temporal_core_bridge.Native_bridge.Configuration -> Rejected_live
  | _ -> Retired

(** Returns whether a failed adapter drain can safely reopen worker admission.
    A same-Domain shutdown admission defect is handled before a drain and is
    intentionally separate from this predicate. *)
let shutdown_retryable = function
  | Activity_drain true -> true
  | Workflow_drain | Activity_drain false -> false

(** Permanent drain failures cannot be retried safely, so the native graph must
    be disposed even though the adapter error remains the public result. *)
let needs_native_cleanup failure = not (shutdown_retryable failure)

type closed_flag_action =
  | Leave_unchanged
  | Write of bool

(** Flag effect of a re-entrant same-Domain [shutdown] admission failure.

    The [closed] stop flag is shared by [run] (its sole reader) and every
    concurrent [shutdown] caller. A [shutdown] issued from the run loop's own
    Domain must be rejected before it can wait on the run mutex (that would
    self-deadlock the calling loop), but it must not write [closed]: another
    Domain's [shutdown] may already have set it to request the loop stop, and
    any write here -- even echoing the observed value back after a racing
    read -- can undo that request and strand the loop. The action is therefore
    [Leave_unchanged]. Only [shutdown_retryable] is raised (the [bool]), so the
    public wrapper reopens admission and a later call from another Domain can
    perform the real drain-then-native-shutdown once the loop exits. *)
let reentrant_same_domain_shutdown : closed_flag_action * bool =
  (Leave_unchanged, true)

(** Keeps a terminal adapter failure authoritative while making native cleanup
    observable to the caller through callbacks. The cleanup callback is
    deliberately injected so the policy can be tested without constructing a
    live Temporal worker or depending on a server. *)
let retain_original_error ~cleanup ~on_cleanup_error ~on_cleanup_exception
    original =
  match cleanup () with
  | Ok _ -> (true, original)
  | Error error ->
      on_cleanup_error error;
      (true, original)
  | exception exception_ ->
      on_cleanup_exception exception_;
      (false, original)

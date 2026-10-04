(** Pure lifecycle predicates for the private native worker.

    The pinned Temporal Core revision currently consumes activity completions
    and logs network failures internally. Consequently the only status that
    can authorize a future completion retry is the dedicated bilateral
    [Retryable] category; this module deliberately fails closed for every
    existing generic bridge status. *)

type drain_failure =
  | Workflow_drain
  | Activity_drain of bool

(** Returns whether a native activity-completion error proves that the lease
    remains available for another submission. The current Core bridge can only
    make that claim through the dedicated bilateral status. *)
let activity_completion_retryable = function
  | Temporal_core_bridge.Native_bridge.Retryable -> true
  | _ -> false

(** Returns whether an async-client heartbeat failure leaves its nonterminal
    capability available. Unlike a Core worker completion, a heartbeat cannot
    consume the completion lease: an uncertain RPC result may be retried with
    the same details. [Invalid_state] is the bridge's confirmed NotFound result
    and remains terminal. *)
let async_heartbeat_retryable = function
  | Temporal_core_bridge.Native_bridge.Connection
  | Temporal_core_bridge.Native_bridge.Retryable -> true
  | _ -> false

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

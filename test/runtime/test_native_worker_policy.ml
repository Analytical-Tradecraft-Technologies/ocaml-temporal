(** Focused tests for native worker retry and shutdown classification.

    The predicates under test are deliberately pure. They encode the safety
    boundary between an adapter's retained completion and the pinned Temporal
    Core implementation, whose generic completion failures do not prove that
    a lease is still available. *)

module Bridge = Temporal_core_bridge.Native_bridge
module Policy = Temporal_runtime.Native_worker_policy

(** Fails with a stable message when a boolean safety decision differs from
    the expected policy. *)
let expect_bool label expected actual =
  if expected <> actual then
    failwith
      (Printf.sprintf "%s expected %b but received %b" label expected actual)

(** Generic bridge failures are not completion-safe: the pinned Core call may
    already have consumed the lease before reporting them. Only the explicit
    bilateral status is authorized for a future Core-aware retry path. *)
let test_activity_completion_policy () =
  expect_bool "explicit retryable completion" true
    (Policy.activity_completion_retryable Bridge.Retryable);
  List.iter
    (fun (label, status) ->
      expect_bool label false (Policy.activity_completion_retryable status))
    [
      ("connection", Bridge.Connection);
      ("not-ready", Bridge.Not_ready);
      ("worker", Bridge.Worker);
      ("protocol", Bridge.Protocol);
      ("closed-equivalent invalid state", Bridge.Invalid_state);
      ("unknown status", Bridge.Unknown 13);
    ]

(** Async client heartbeats and complete/fail/cancel do not consume a Core
    completion lease. A connection result is uncertain and retains the live
    handle (#821), a definitive rejection keeps it live for a different
    request, and the bridge's NotFound/invalid-state response closes it. This
    must not weaken the ordinary Core completion policy tested above, which
    still fails closed on [Connection]. *)
let test_async_operation_policy () =
  let expect_disposition label expected actual =
    if actual <> expected then failwith label
  in
  if Policy.activity_completion_retryable Bridge.Connection then
    failwith "worker completion policy no longer fails closed";
  expect_disposition "uncertain async connection" Policy.Retry_exact
    (Policy.async_operation_disposition Bridge.Connection);
  expect_disposition "explicit retryable heartbeat" Policy.Retry_exact
    (Policy.async_operation_disposition Bridge.Retryable);
  List.iter
    (fun (label, status) ->
      expect_disposition label Policy.Rejected_live
        (Policy.async_operation_disposition status))
    [
      ("definitive RPC rejection", Bridge.Async_heartbeat_rejected);
      ("local invalid argument", Bridge.Invalid_argument);
      ("local protocol rejection", Bridge.Protocol);
      ("local configuration rejection", Bridge.Configuration);
    ];
  List.iter
    (fun (label, status) ->
      expect_disposition label Policy.Retired
        (Policy.async_operation_disposition status))
    [
      ("not-found heartbeat", Bridge.Invalid_state);
      ("not-ready heartbeat", Bridge.Not_ready);
      ("worker heartbeat", Bridge.Worker);
      ("unknown heartbeat status", Bridge.Unknown 13);
    ]

(** Proves a re-entrant same-Domain [shutdown] never clears the shared [closed]
    stop flag, reproducing the documented multi-caller deadlock interleaving:

    1. Domain E calls [shutdown]: it wins the stop-flag gate, setting [closed]
       to [true], and then blocks on [run_mutex] waiting for the run loop.
    2. A systhread on the run loop's own Domain calls [shutdown]: it takes the
       re-entrant same-Domain branch and applies this policy's flag action.

    If that branch writes [closed] (the previous defect wrote [false]), it
    undoes E's stop request; the loop keeps observing [closed = false], never
    exits, holds [run_mutex] forever, and E deadlocks. The policy must therefore
    leave [closed] untouched while still marking the admission failure retryable
    so a later call from another Domain can drive the real drain-then-shutdown
    once the loop exits. This test drives the exact production decision against a
    real [closed] atomic, so a regression to any [closed] write fails here. *)
let test_reentrant_same_domain_shutdown_preserves_closed () =
  let closed_action, retryable = Policy.reentrant_same_domain_shutdown in
  expect_bool "reentrant same-Domain shutdown retryable" true retryable;
  (* A concurrent [shutdown] on another Domain wins the stop-flag gate. *)
  let closed = Atomic.make false in
  if not (Atomic.compare_and_set closed false true) then
    failwith "concurrent shutdown failed to set the stop flag";
  (* The re-entrant same-Domain branch applies its flag action next. *)
  (match closed_action with
  | Policy.Leave_unchanged -> ()
  | Policy.Write value -> Atomic.set closed value);
  if not (Atomic.get closed) then
    failwith
      "re-entrant same-Domain shutdown cleared the stop flag set by a \
       concurrent shutdown; the run loop would never exit and deadlock the \
       waiting caller"

(** Runs all pure policy regressions. *)
let () =
  test_activity_completion_policy ();
  test_async_operation_policy ();
  test_reentrant_same_domain_shutdown_preserves_closed ()

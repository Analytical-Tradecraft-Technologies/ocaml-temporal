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

(** Async client heartbeats do not consume a completion lease. A connection
    result is uncertain and retains the handle for an identical retry, while
    the bridge's NotFound/invalid-state response closes it. This must not
    weaken the ordinary Core completion policy tested above. *)
let test_async_heartbeat_policy () =
  expect_bool "uncertain heartbeat connection" true
    (Policy.async_heartbeat_retryable Bridge.Connection);
  expect_bool "explicit retryable heartbeat" true
    (Policy.async_heartbeat_retryable Bridge.Retryable);
  List.iter
    (fun (label, status) ->
      expect_bool label false (Policy.async_heartbeat_retryable status))
    [
      ("not-found heartbeat", Bridge.Invalid_state);
      ("not-ready heartbeat", Bridge.Not_ready);
      ("worker heartbeat", Bridge.Worker);
      ("protocol heartbeat", Bridge.Protocol);
      ("configuration heartbeat", Bridge.Configuration);
      ("unknown heartbeat status", Bridge.Unknown 13);
    ]

(** Shutdown can reopen admission only when an activity drain retained a
    completion after an explicitly transient native failure. Workflow drains
    and permanent activity failures must remain terminal. *)
let test_shutdown_policy () =
  expect_bool "retryable activity drain" true
    (Policy.shutdown_retryable (Policy.Activity_drain true));
  expect_bool "permanent activity drain" false
    (Policy.shutdown_retryable (Policy.Activity_drain false));
  expect_bool "workflow drain" false
    (Policy.shutdown_retryable Policy.Workflow_drain);
  expect_bool "retryable activity native cleanup" false
    (Policy.needs_native_cleanup (Policy.Activity_drain true));
  expect_bool "permanent activity native cleanup" true
    (Policy.needs_native_cleanup (Policy.Activity_drain false));
  expect_bool "workflow native cleanup" true
    (Policy.needs_native_cleanup Policy.Workflow_drain)

(** Proves that terminal cleanup is injected and its diagnostic cannot replace
    the adapter failure which explains why admission became terminal. The same
    helper also contains the exception guard used for a defensive cleanup
    callback, so an unexpected callback defect cannot change the public result. *)
let test_terminal_cleanup_preserves_original_error () =
  let original = "completion could not be retired" in
  (* A cleanup callback that returns [Ok] also proves the native runtime was
     released, exactly like an [Error] result -- both mean adapter state may
     be discarded. This is the exact decision the native worker's successful
     shutdown path relies on to know it may call [Workflow.discard] and
     [Activity.discard] after [Native.shutdown] returns [Ok ()]. *)
  let ok_cleanup_calls = ref 0 in
  let ok_cleanup_returned, ok_result =
    Policy.retain_original_error
      ~cleanup:(fun () ->
        incr ok_cleanup_calls;
        Ok ())
      ~on_cleanup_error:(fun _ -> failwith "unexpected cleanup error")
      ~on_cleanup_exception:(fun _ -> failwith "unexpected cleanup exception")
      original
  in
  if not ok_cleanup_returned then
    failwith "successful native cleanup was not recognized as release-complete";
  if ok_result <> original then
    failwith "successful native cleanup replaced the original value";
  if !ok_cleanup_calls <> 1 then failwith "successful native cleanup was not requested";
  let cleanup_calls = ref 0 in
  let reported_error = ref None in
  let reported_exception = ref false in
  let cleanup_returned, result =
    Policy.retain_original_error
      ~cleanup:(fun () ->
        incr cleanup_calls;
        Error "native graph already closed")
      ~on_cleanup_error:(fun error -> reported_error := Some error)
      ~on_cleanup_exception:(fun _ -> reported_exception := true)
      original
  in
  if not cleanup_returned then
    failwith "returned native cleanup was not recognized as release-complete";
  if result <> original then
    failwith "native cleanup replaced the original adapter error";
  if !cleanup_calls <> 1 then failwith "native cleanup was not requested";
  if !reported_error <> Some "native graph already closed" then
    failwith "native cleanup diagnostic was not reported";
  if !reported_exception then
    failwith "cleanup error was incorrectly reported as an exception";
  let exception_returned, exception_result =
    Policy.retain_original_error
      ~cleanup:(fun () -> raise Exit)
      ~on_cleanup_error:(fun _ -> failwith "unexpected cleanup error")
      ~on_cleanup_exception:(fun exception_ ->
        match exception_ with
        | Exit -> ()
        | _ -> failwith "unexpected cleanup exception")
      original
  in
  if exception_returned then
    failwith "cleanup exception was incorrectly treated as release-complete";
  (* This mirrors the production gate: adapter discard is reachable only after
     the cleanup callback returned a result, never from the exception branch. *)
  let exception_discarded = ref false in
  if exception_returned then exception_discarded := true;
  if !exception_discarded then
    failwith "cleanup exception discarded adapter state";
  if exception_result <> original then
    failwith "cleanup exception replaced the original adapter error"

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
  test_async_heartbeat_policy ();
  test_shutdown_policy ();
  test_terminal_cleanup_preserves_original_error ();
  test_reentrant_same_domain_shutdown_preserves_closed ()

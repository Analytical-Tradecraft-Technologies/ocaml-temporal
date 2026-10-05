(** Regression for #796: a full native start or wait registry must surface as a
    retryable capacity error that callers can recognize structurally, never as
    the invalid-state error used for a closed client. The supervisor failure
    values are constructed directly, so no live Temporal server or 64 blocked
    calls are needed to exercise the public classification. *)

module Backend = Temporal__Backend
module Bridge = Temporal_sdk_kernel.Bridge
module Supervisor = Temporal_sdk_kernel.Supervisor
module Client = Temporal.Client
module Error = Temporal.Error

(** Converts one native bridge status into the public error a client caller
    would receive from [Client.start] or [Client.wait]. *)
let public_error status message =
  Backend.native_supervisor_error (Supervisor.Backend { Bridge.status; message })

(** Returns whether [needle] occurs anywhere in [haystack]; the standard
    library offers no substring search on every supported compiler. *)
let contains ~needle haystack =
  let n = String.length haystack and m = String.length needle in
  let rec loop i =
    i + m <= n && (String.sub haystack i m = needle || loop (i + 1))
  in
  loop 0

(** The capacity rejection keeps the bridge diagnostic, is not marked
    non-retryable, and carries the documented stable error type. *)
let test_capacity_error_is_retryable_and_recognized () =
  let error =
    public_error Bridge.Resource_exhausted
      "too many Temporal workflow waits are pending (limit 64)"
  in
  let view = Error.view error in
  assert (view.category = `Bridge);
  assert (not view.non_retryable);
  assert (view.error_type = Some "resource_exhausted");
  assert (Error.error_type error = Some Backend.client_at_capacity_error_type);
  assert (Client.is_at_capacity error);
  let message = Error.message error in
  if not (contains ~needle:"limit 64" message) then
    failwith ("capacity diagnostic was lost: " ^ message)

(** Closed-client, lifecycle, and unrelated failures must not be mistaken for
    a transient capacity condition, including a forged error that only copies
    the error type onto a non-retryable or non-bridge failure. *)
let test_other_failures_are_not_capacity () =
  List.iter
    (fun (label, error) ->
      if Client.is_at_capacity error then
        failwith (label ^ " was classified as at capacity"))
    [
      ( "closed client",
        public_error Bridge.Invalid_state "Temporal client is not connected" );
      ("connection", public_error Bridge.Connection "transport unavailable");
      ("unknown status", public_error (Bridge.Unknown 99) "newer bridge");
      ("closed supervisor", Backend.native_supervisor_error Supervisor.Closed);
      ( "non-retryable forgery",
        Error.make ~non_retryable:true ~error_type:"resource_exhausted"
          ~category:`Bridge ~message:"forged" () );
      ( "workflow forgery",
        Error.make ~error_type:"resource_exhausted" ~category:`Workflow
          ~message:"forged" () );
    ]

(** Runs the capacity classification regressions. *)
let () =
  test_capacity_error_is_retryable_and_recognized ();
  test_other_failures_are_not_capacity ()

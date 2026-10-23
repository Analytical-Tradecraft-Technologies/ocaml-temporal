(** Regression for #823: client RPC failures must be classifiable from their
    structured fields. Permanent statuses are non-retryable, transient ones
    are retryable, every status has a stable [Error.error_type], and a failed
    query handler is a distinct non-retryable [`Workflow] error carrying the
    handler's message. The protocol values are constructed directly, so no
    live Temporal server is needed to exercise the public classification. *)

module Backend = Temporal__Backend
module Protocol = Temporal_sdk_kernel.Client_protocol
module Client = Temporal.Client
module Error = Temporal.Error

(** Converts one closed protocol error into the public error a client caller
    would receive. *)
let public_error error = Backend.native_client_error ~namespace:"default" error

(** Every closed RPC code with its expected public status, error type, and
    retryability. Kept independent of the backend's table so a change there
    must be made deliberately in both places. *)
let expected =
  [
    ("cancelled", `Cancelled, "Cancelled", false);
    ("unknown", `Unknown, "Unknown", false);
    ("invalid_argument", `Invalid_argument, "InvalidArgument", true);
    ("deadline_exceeded", `Deadline_exceeded, "DeadlineExceeded", false);
    ("not_found", `Not_found, "NotFound", true);
    ("already_exists", `Already_exists, "AlreadyExists", true);
    ("permission_denied", `Permission_denied, "PermissionDenied", true);
    ("resource_exhausted", `Resource_exhausted, "ResourceExhausted", false);
    ("failed_precondition", `Failed_precondition, "FailedPrecondition", true);
    ("aborted", `Aborted, "Aborted", false);
    ("out_of_range", `Out_of_range, "OutOfRange", false);
    ("unimplemented", `Unimplemented, "Unimplemented", true);
    ("internal", `Internal, "Internal", false);
    ("unavailable", `Unavailable, "Unavailable", false);
    ("data_loss", `Data_loss, "DataLoss", false);
    ("unauthenticated", `Unauthenticated, "Unauthenticated", true);
    ( "termination_outcome_uncertain",
      `Termination_outcome_uncertain,
      "TerminationOutcomeUncertain",
      true );
  ]

(** Each RPC code becomes a [`Bridge] error with the documented type and
    retryability, round-trips through [Client.rpc_status], keeps the
    code-only message, and is never mistaken for a query failure or a local
    capacity refusal. *)
let test_rpc_codes_are_classified () =
  List.iter
    (fun (code, status, error_type, non_retryable) ->
      let error = public_error (Protocol.Rpc { code }) in
      let view = Error.view error in
      if view.category <> `Bridge then failwith (code ^ ": category changed");
      if view.error_type <> Some error_type then
        failwith (code ^ ": unexpected error type");
      if view.non_retryable <> non_retryable then
        failwith (code ^ ": unexpected retryability");
      if Client.rpc_status error <> Some status then
        failwith (code ^ ": rpc_status did not round-trip");
      if view.message <> "Temporal client RPC failed: " ^ code then
        failwith (code ^ ": message changed");
      if Client.is_query_failed error then
        failwith (code ^ ": classified as a query failure");
      if Client.is_at_capacity error then
        failwith (code ^ ": classified as at capacity"))
    expected

(** The near-unreachable ["ok"] code falls back to the transient [Unknown]
    class instead of raising or being reported as permanent. *)
let test_ok_code_falls_back_to_unknown () =
  let error = public_error (Protocol.Rpc { code = "ok" }) in
  assert (Client.rpc_status error = Some `Unknown);
  assert (not (Error.view error).non_retryable)

(** A failed query handler is a non-retryable [`Workflow] error whose message
    is the handler's own; an empty handler message gets a fixed diagnostic. *)
let test_query_failure_is_typed () =
  let error =
    public_error (Protocol.Query_failed { message = "no handler for state" })
  in
  let view = Error.view error in
  assert (view.category = `Workflow);
  assert view.non_retryable;
  assert (view.error_type = Some "QueryFailed");
  assert (view.message = "no handler for state");
  assert (view.details = []);
  assert (Client.is_query_failed error);
  assert (Client.rpc_status error = None);
  let empty = public_error (Protocol.Query_failed { message = "" }) in
  assert (Client.is_query_failed empty);
  assert (Error.message empty = "workflow query handler failed")

(** Errors that merely resemble an RPC or query failure are not classified:
    the category, type, and (for queries) retryability must all match. *)
let test_lookalikes_are_not_classified () =
  List.iter
    (fun (label, error) ->
      if Client.rpc_status error <> None then
        failwith (label ^ " was classified as an RPC failure");
      if Client.is_query_failed error then
        failwith (label ^ " was classified as a query failure"))
    [
      ( "workflow NotFound",
        Error.make ~error_type:"NotFound" ~category:`Workflow ~message:"x" () );
      ( "lowercase code",
        Error.make ~error_type:"not_found" ~category:`Bridge ~message:"x" () );
      ( "capacity refusal",
        Error.make ~error_type:Backend.client_at_capacity_error_type
          ~category:`Bridge ~message:"x" () );
      ("untyped bridge", Error.make ~category:`Bridge ~message:"x" ());
      ( "retryable query failure",
        Error.make ~error_type:"QueryFailed" ~category:`Workflow ~message:"x" ()
      );
      ( "bridge query failure",
        Error.make ~non_retryable:true ~error_type:"QueryFailed"
          ~category:`Bridge ~message:"x" () );
    ]

(** Runs the client RPC error classification regressions. *)
let () =
  test_rpc_codes_are_classified ();
  test_ok_code_falls_back_to_unknown ();
  test_query_failure_is_typed ();
  test_lookalikes_are_not_classified ()

(** Produces the defect used when a child workflow operation is attempted
    outside deterministic workflow execution. *)
let outside_error () =
  Error.defect ~message:"child workflow operation used outside a workflow"

(** Controls when Core reports a parent future after cancellation has been
    requested. The policy travels with the start command so Core can handle a
    cancel-before-start race without language-layer guessing. Handles default
    to [Try_cancel], so calling [cancel] requests cancellation of the child;
    [Abandon] remains available when deliberately detaching the child. *)
type cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon
  | Wait_cancellation_requested

(** Couples a typed child result future with the only cancellation operation
    that may target its private sequence. The record is hidden by the public
    interface so callers cannot forge a sequence number. *)
type 'output handle = {
  (* Future resolved by Core for the child start and terminal outcome. *)
  future : ('output, Error.t) Future.t;
  (* Owner-checked operation that emits at most one cancellation command for
     this child and preserves the caller's reason for replay diagnostics. *)
  cancel : reason:string -> (unit, Error.t) result;
}

(** Creates an already-completed future without emitting a command. Inside a
    workflow the current scheduler owns it; outside a workflow an inert store
    retains the diagnostic for [peek] and [await]. *)
let resolved result =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | Some context ->
      Future_private.of_internal
        (Temporal_sdk_kernel.Workflow_context_store.resolved context
           (Result.map_error Error_private.to_base result))
  | None -> Future_private.resolved ~outside_error result

(** Maximum UTF-8 byte length accepted by the strict bridge JSON protocol. The
    server's configurable identifier policy can be narrower, but no command may
    exceed this compiled cross-language safety boundary. *)
let max_id_utf_8_bytes = 65_536

(** Validates every property required before an ID can enter command history.
    OCaml strings are byte sequences, so both an explicit UTF-8 check and a byte
    limit are necessary before JSON encoding. *)
let validate_id id =
  if String.equal id "" then
    Error (Error.defect ~message:"child workflow id must not be empty")
  else if String.contains id '\000' then
    Error (Error.defect ~message:"child workflow id must not contain NUL")
  else if String.length id > max_id_utf_8_bytes then
    Error
      (Error.defect
         ~message:"child workflow id exceeds 65536 UTF-8 bytes")
  else if not (Temporal_base.Codec.valid_utf_8 id) then
    Error (Error.defect ~message:"child workflow id must be valid UTF-8")
  else Ok ()

(** Validates the cancellation reason before it can enter durable command
    history. Empty reasons are rejected so every cancellation has a useful
    diagnostic in Core and in replay logs. *)
let validate_reason reason =
  if String.equal reason "" then
    Error (Error.defect ~message:"child cancellation reason must not be empty")
  else if String.contains reason '\000' then
    Error (Error.defect ~message:"child cancellation reason must not contain NUL")
  else if String.length reason > max_id_utf_8_bytes then
    Error
      (Error.defect
         ~message:"child cancellation reason exceeds 65536 bytes")
  else if not (Temporal_base.Codec.valid_utf_8 reason) then
    Error (Error.defect ~message:"child cancellation reason must be valid UTF-8")
  else Ok ()

(** Converts the public policy to the package-private runtime variant at the
    final boundary before command emission. *)
let runtime_cancellation_type = function
  | Try_cancel -> Temporal_sdk_kernel.Activation.Child_try_cancel
  | Wait_cancellation_completed ->
      Temporal_sdk_kernel.Activation.Child_wait_cancellation_completed
  | Abandon -> Temporal_sdk_kernel.Activation.Child_abandon
  | Wait_cancellation_requested ->
      Temporal_sdk_kernel.Activation.Child_wait_cancellation_requested

(** Builds a handle for a request that failed before a child command could be
    emitted. The future is ready and cancellation returns the same typed
    defect, so invalid lifecycle requests cannot leave hidden state. *)
let failed_handle error =
  { future = resolved (Error error); cancel = (fun ~reason:_ -> Error error) }

(** Verifies an optional structured-cancellation scope before validating or
    encoding a child request. This keeps a cancelled scope from emitting a
    new start command or invoking application codecs. *)
let check_scope = function
  | None -> Ok ()
  | Some scope -> Scope.check scope

(** Validates durable identity and encodes input before allocating a private
    sequence number. Consequently invalid requests cannot change command order
    or appear in replay history. *)
let start_handle ?scope ?(cancellation_type = Try_cancel) ?retry_policy ~id
    definition input =
  match check_scope scope with
  | Error error -> failed_handle error
  | Ok () ->
      match validate_id id with
      | Error error -> failed_handle error
      | Ok () ->
          match Codec_private.encode_base (Workflow.input definition) input with
          | Error error -> failed_handle (Error_private.of_base error)
          | Ok input ->
              match Temporal_sdk_kernel.Workflow_context_store.current () with
              | None -> failed_handle (outside_error ())
              | Some context ->
                  let future, cancel =
                    Temporal_sdk_kernel.Workflow_context_store.start_child_workflow
                      context ~id ~name:(Workflow.name definition) ~input
                      ?retry_policy:
                        (Option.map Retry_policy_private.to_runtime retry_policy)
                      ~cancellation_type:
                        (runtime_cancellation_type cancellation_type)
                      ~decode:
                        (Codec_private.decode_base (Workflow.output definition))
                      ()
                  in
                  let request_cancel ~reason =
                    match validate_reason reason with
                    | Error error -> Error error
                    | Ok () ->
                        Result.map_error Error_private.of_base (cancel ~reason)
                  in
                  let () =
                    Option.iter
                      (fun scope ->
                        (* Scope ownership was checked before scheduling, so
                           this registration cannot race another Domain. *)
                        ignore
                          (Scope.on_cancel ~until:(Future_private.of_internal future) scope (fun () ->
                               request_cancel ~reason:"cancelled by workflow")))
                      scope
                  in
                  {
                    future = Future_private.of_internal future;
                    cancel = request_cancel;
                  }

(** Returns the typed future associated with an operation handle. *)
let future handle = handle.future

(** Requests cancellation of one child. Repeating the request is idempotent
    for this handle, including a valid late call after natural completion or a
    start failure; Core still owns the single terminal resolution that settles
    the future. *)
let cancel ?(reason = "cancelled by workflow") handle = handle.cancel ~reason

(** Starts a child workflow and returns only its future. Callers that need to
    cancel explicitly should retain the handle returned by [start_handle]. *)
let start ?scope ?cancellation_type ?retry_policy ~id definition input =
  future
    (start_handle ?scope ?cancellation_type ?retry_policy ~id definition input)

(** Implements the direct-style child call as start followed by an effect-backed
    wait. Expected child and codec failures remain explicit [result] values. *)
let execute ?scope ?cancellation_type ?retry_policy ~id definition input =
  Future.await
    (start ?scope ?cancellation_type ?retry_policy ~id definition input)

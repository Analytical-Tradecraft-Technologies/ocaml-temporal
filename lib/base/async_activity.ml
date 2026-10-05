(** Correctness-first state machine for a retained asynchronous activity
    completion capability.

    The handle starts dormant because the callback may return a handle before
    the worker has accepted the [WillCompleteAsync] handoff. Every operation
    reserves a request key under [mutex], releases the lock while entering the
    supervisor, and commits the result under the lock. Consequently a second
    Domain cannot submit a conflicting terminal operation, while a transport
    failure leaves the exact operation available for a later retry. *)

(** An operation that may cross the native supervisor boundary. Payloads are
    copied before they enter this type so retrying a transport failure can
    resubmit the exact request without retaining caller-owned mutable bytes. *)
type operation =
  | Complete of Payload.t
  | Fail of Error.t
  | Cancel of Payload.t list
  | Heartbeat of Payload.t list

(** The result exposed by handle methods and lifecycle transitions. *)
type submit_result = (unit, Error.t) result

(** The adapter explicitly distinguishes preflight rejection from native
    outcomes. [Not_submitted] releases a new operation key while preserving
    the handle; a definitive native heartbeat rejection has the same key effect
    without claiming that no call occurred. Unresolved earlier submissions
    retain the exact key, and terminal native outcomes close the handle. *)
type submission_error =
  | Not_submitted of Error.t
  | Rejected_submission of Error.t
  | Retryable_submission of Error.t
  | Terminal_submission of Error.t

(** The handle lifecycle is protected by [handle.mutex]. [Handoff_pending]
    closes the gap between a callback returning [Will_complete_async] and the
    worker accepting that handoff; [Terminal] prevents duplicate completion,
    while [Closed] permanently rejects operations after teardown. *)
type lifecycle = Dormant | Handoff_pending | Active | Terminal | Closed

(** The one operation currently reserved by this handle. [in_flight] is set
    while the supervisor callback runs, allowing a transport error to retain
    the request for an explicit retry without allowing concurrent duplicates.
    [retry_pending] preserves an earlier uncertain submission even if a later
    retry is rejected before reaching the supervisor. *)
type pending = {
  key : string;
  mutable in_flight : bool;
  mutable retry_pending : bool;
}

(** Mutable state shared by every Domain that retains one completion handle.
    The submit callback is the only route to native code; this module owns the
    lock and lifecycle but not the native task token captured by that callback. *)
type 'output handle = {
  mutex : Mutex.t;
  mutable lifecycle : lifecycle;
  mutable pending : pending option;
  submit : operation -> (unit, submission_error) result;
  encode_output : 'output -> (Payload.t, Error.t) result;
}

(** The short-lived callback context from which an activity obtains its
    attempt-scoped completion capability. *)
type 'output context = { handle : 'output handle }

(** The callback's immediate outcome. [Will_complete_async] transfers the
    handle to external code, so the callback must not later use that same
    attempt through a different completion path. *)
type 'output async_result =
  | Completed of 'output
  | Failed of Error.t
  | Will_complete_async of 'output handle

(** The implementation type is repeated here so the private adapter can store
    typed callbacks without exposing the handle representation. *)
type ('input, 'output) implementation =
  'output context -> 'input -> 'output async_result

(** Builds the non-retryable error used when a lifecycle transition rejects a
    request. Operational transport errors are created by [submit] instead and
    may keep the pending request retryable. *)
let lifecycle_error message =
  Error (Error.make ~non_retryable:true ~category:`Activity ~message ())

(** Executes one state transition while preserving the mutex invariant even if
    the transition raises. No supervisor callback may run while this lock is
    held, because callback code can block or re-enter this state machine. *)
let with_mutex mutex operation =
  Mutex.lock mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock mutex) operation

(** Creates a handle before the worker has accepted its asynchronous handoff.
    Keeping it dormant prevents a callback from completing an activity through
    a task token that the native worker has not yet made durable. *)
let create ~submit ~encode_output =
  {
    mutex = Mutex.create ();
    lifecycle = Dormant;
    pending = None;
    submit;
    encode_output;
  }

(** Builds the callback context for a newly created attempt-scoped handle. *)
let context handle = { handle }

(** Wraps a handle for the activity callback without copying the capability. *)
let handle context = context.handle

(** Marks a handed-off capability usable by external completion code. The
    transition is safe against duplicate activation because every already
    active or terminal lifecycle is rejected. *)
let activate handle =
  with_mutex handle.mutex (fun () ->
      match handle.lifecycle with
      | Dormant | Handoff_pending ->
          handle.lifecycle <- Active;
          Ok ()
      | Active | Terminal | Closed ->
          lifecycle_error "asynchronous activity handle is already activated")

(** Moves the expected callback handle into the handoff-reserved state. The
    identity check prevents a completion callback from one activity attempt
    being accidentally attached to another attempt's native task token. *)
let prepare_handoff ~expected handle =
  if not (expected == handle) then
    lifecycle_error
      "asynchronous activity handle belongs to another activity attempt"
  else
    with_mutex handle.mutex (fun () ->
        match handle.lifecycle with
        | Dormant ->
            handle.lifecycle <- Handoff_pending;
            Ok ()
        | Handoff_pending ->
            lifecycle_error
              "asynchronous activity handle is already reserved for a handoff"
        | Active | Terminal | Closed ->
            lifecycle_error
              "asynchronous activity handle cannot be reserved for a handoff")

(** Reserves one operation key under the lock. The key is installed before the
    supervisor call so another Domain cannot submit a conflicting operation or
    duplicate the same request while the first submission is in flight. *)
let begin_operation handle ~key operation =
  with_mutex handle.mutex (fun () ->
      match handle.lifecycle with
      | Dormant ->
          Error
            (Error.make ~non_retryable:true ~category:`Activity
               ~message:
                 "asynchronous activity handle is not active; return Will_complete_async first"
               ())
      | Closed -> lifecycle_error "asynchronous activity handle is closed"
      | Handoff_pending ->
          Error
            (Error.make ~non_retryable:true ~category:`Activity
               ~message:
                 "asynchronous activity handle is waiting for the worker handoff"
               ())
      | Terminal -> lifecycle_error "asynchronous activity handle is terminal"
      | Active -> (
          match handle.pending with
          | Some pending when not (String.equal pending.key key) ->
              lifecycle_error
                "a different asynchronous activity operation is already pending"
          | Some pending when pending.in_flight ->
              lifecycle_error
                "the same asynchronous activity operation is already in flight"
          | Some pending ->
              pending.in_flight <- true;
              Ok (pending, operation)
          | None ->
              let pending = { key; in_flight = true; retry_pending = false } in
              handle.pending <- Some pending;
              Ok (pending, operation)))

(** Appends one length-prefixed field to the internal operation key. Length
    prefixes avoid ambiguities such as ["ab", "c"] versus ["a", "bc"]. *)
let add_field buffer value =
  Buffer.add_string buffer (string_of_int (String.length value));
  Buffer.add_char buffer ':';
  Buffer.add_string buffer value

(** Adds a payload's metadata and bytes to the operation key in wire order.
    This is an equality key, not a digest or authentication mechanism. *)
let add_payload buffer ({ Payload.metadata; data } : Payload.t) =
  add_field buffer "payload";
  add_field buffer (string_of_int (List.length metadata));
  List.iter
    (fun (key, value) ->
      add_field buffer "metadata";
      add_field buffer key;
      add_field buffer value)
    metadata;
  add_field buffer "data";
  add_field buffer (string_of_int (Bytes.length data));
  add_field buffer (Bytes.to_string data)

(** Adds an ordered payload list to the operation key, including its length so
    an empty list and a list with empty payloads remain distinct. *)
let add_payloads buffer payloads =
  add_field buffer "payloads";
  add_field buffer (string_of_int (List.length payloads));
  List.iter (add_payload buffer) payloads

(** Derives the stable equality key used to permit only byte-identical retries
    after an uncertain supervisor submission. Error category, application
    failure type, retryability, and detail payloads are included because they
    affect the completion request. *)
let operation_key operation =
  let buffer = Buffer.create 64 in
  (match operation with
  | Complete payload ->
      add_field buffer "complete";
      add_payload buffer payload
  | Fail error ->
      add_field buffer "fail";
      let ({ Error.message; non_retryable; details; _ } : Error.view) =
        Error.view error
      in
      add_field buffer (Error.kind error);
      add_field buffer (Error.application_failure_type error);
      add_field buffer message;
      add_field buffer (if non_retryable then "1" else "0");
      add_payloads buffer details
  | Cancel payloads ->
      add_field buffer "cancel";
      add_payloads buffer payloads
  | Heartbeat payloads ->
      add_field buffer "heartbeat";
      add_payloads buffer payloads);
  Buffer.contents buffer

(** Executes one supervisor submission outside the mutex, then commits its
    result under the mutex. A successful terminal operation clears the pending
    key and closes the handle; a retryable failure clears only [in_flight], so a
    later call can repeat the exact operation without rerunning user code.
    Local or definitive native rejection releases a fresh key so a corrected
    operation can run. Neither can erase an earlier uncertain submission. *)
let submit_operation handle ~terminal operation =
  let key = operation_key operation in
  match begin_operation handle ~key operation with
  | Error _ as error -> error
  | Ok (pending, operation) ->
      let result =
        try handle.submit operation with exception_ ->
          Error
            (Retryable_submission (Error.make ~non_retryable:false ~category:`Bridge
               ~message:
                 (Printf.sprintf
                    "asynchronous activity operation raised: %s"
                    (Printexc.to_string exception_))
               ()))
      in
      with_mutex handle.mutex (fun () ->
          pending.in_flight <- false;
          match result with
          | Error (Not_submitted error | Rejected_submission error) ->
              if not pending.retry_pending then handle.pending <- None;
              Error error
          | Error (Retryable_submission error) ->
              (* Native acceptance is unresolved. Only the byte-identical
                 request may retry; local validation cannot erase that debt. *)
              pending.retry_pending <- true;
              Error error
          | Error (Terminal_submission error) ->
              handle.lifecycle <- Closed;
              handle.pending <- None;
              Error error
          | Ok () ->
              handle.pending <- None;
              if terminal then handle.lifecycle <- Terminal;
              Ok ())

(** Encodes a typed output before reserving the terminal operation. Encoding
    failures therefore leave the handle active and do not create a retry slot. *)
let complete handle output =
  match handle.encode_output output with
  | Error error -> Error error
  | Ok payload -> submit_operation handle ~terminal:true (Complete payload)

(** Records a terminal activity failure through the retained capability. *)
let fail handle error = submit_operation handle ~terminal:true (Fail error)

(** Records terminal cancellation details through the retained capability. *)
let cancel handle details =
  submit_operation handle ~terminal:true (Cancel details)

(** Sends non-terminal progress details. A successful heartbeat clears its
    pending key but leaves the handle active for a later terminal operation. *)
let heartbeat handle details =
  submit_operation handle ~terminal:false (Heartbeat details)

(** Invalidates the capability during worker teardown. An in-flight supervisor
    call is allowed to finish first; otherwise closing would hide whether its
    remote operation was accepted and make safe retry impossible. *)
let close handle =
  with_mutex handle.mutex (fun () ->
      match handle.pending with
      | Some { in_flight = true; _ } ->
          Error
            (Error.make ~non_retryable:true ~category:`Activity
               ~message:
                 "cannot close asynchronous activity handle while an operation is in flight"
               ())
      | _ ->
          handle.lifecycle <- Closed;
          handle.pending <- None;
          Ok ())

(** State machine for an activity completion that was handed off to external
    code.

    This module is deliberately below the public API. It owns no native
    pointer and performs no I/O; the adapter supplies a callback that enters
    the single SDK supervisor. Keeping lifecycle state here makes handle
    methods safe when they are called from several OCaml Domains. *)

(** An encoded operation submitted to the private native adapter. The public
    boundary copies payloads; the adapter must validate the entire wire request
    before submitting it to the supervisor. *)
type operation =
  | Complete of Payload.t
  | Fail of Error.t
  | Cancel of Payload.t list
  | Heartbeat of Payload.t list

(** The result exposed by handle methods and lifecycle transitions. *)
type submit_result = (unit, Error.t) result

(** Private submission disposition, independent of the public diagnostic's
    error category. [Not_submitted] proves local validation prevented any
    native call, permitting a corrected or different operation unless an
    earlier submission is still unresolved. [Rejected_submission] is a
    definitive native heartbeat rejection that also permits a corrected
    request while retaining the live handle and lease. An explicitly retryable
    submission retains the exact request key; a terminal native rejection or
    closed capability retires the handle. *)
type submission_error =
  | Not_submitted of Error.t
  | Rejected_submission of Error.t
  | Retryable_submission of Error.t
  | Terminal_submission of Error.t

(** An opaque handle paired with the output type of its activity definition. *)
type 'output handle

(** An attempt-scoped context from which the callback can obtain its handle. *)
type 'output context

(** The outcome returned by an asynchronous activity implementation.

    [Completed] and [Failed] finish the activity while the worker callback is
    still running. [Will_complete_async] transfers the completion capability to
    the returned handle; the callback must not perform another completion after
    returning that value. *)
type 'output async_result =
  | Completed of 'output
  | Failed of Error.t
  | Will_complete_async of 'output handle

(** The callback type for an activity that may finish after its worker task
    has been acknowledged. The context is attempt-scoped and exists solely to
    obtain the opaque completion handle. *)
type ('input, 'output) implementation =
  'output context -> 'input -> 'output async_result

(** Creates a dormant handle. It cannot submit an operation until [activate]
    succeeds after the worker accepts [WillCompleteAsync]. [encode_output] is
    retained by the handle so callers can complete it with the activity's
    typed output rather than constructing a wire payload themselves. *)
val create :
  submit:(operation -> (unit, submission_error) result) ->
  encode_output:('output -> (Payload.t, Error.t) result) ->
  'output handle

(** Builds the callback context associated with a handle. *)
val context : 'output handle -> 'output context

(** Returns the handle retained by a callback context. *)
val handle : 'output context -> 'output handle

(** Linearizes the worker-to-client handoff. Calling this more than once is a
    typed lifecycle error. *)
val activate : 'output handle -> submit_result

(** Reserves the current attempt's dormant handle for the worker-side
    [WillCompleteAsync] acknowledgement. The [expected] identity is checked
    before changing lifecycle state, so a callback cannot return a handle
    retained from an earlier attempt whose submit callback still captures the
    earlier task token. Only the owning adapter calls this function. *)
val prepare_handoff : expected:'output handle -> 'output handle -> submit_result

(** Encodes and submits one complete operation. The state machine derives a
    canonical key from the encoded payload; if the transport fails, only the
    same byte-identical request may retry. *)
val complete : 'output handle -> 'output -> submit_result

(** Submits one failed operation attempt. *)
val fail : 'output handle -> Error.t -> submit_result

(** Submits one cancellation operation attempt with optional detail payloads. *)
val cancel : 'output handle -> Payload.t list -> submit_result

(** Sends one heartbeat without changing the terminal lifecycle. *)
val heartbeat : 'output handle -> Payload.t list -> submit_result

(** Closes a handle after the owning SDK has stopped. Closing while an
    operation is in flight returns an outstanding-operation error instead of
    silently invalidating that request. *)
val close : 'output handle -> submit_result

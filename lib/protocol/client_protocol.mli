(** Strict JSON values for the native client start and exact-run wait bridge.

    The Rust side owns Temporal's protobuf and network client.  This module
    owns the corresponding OCaml representation of the small JSON documents
    that cross that private boundary.  Every decoder rejects unknown or missing
    members and every encoder validates its own output before returning it. *)

type payload = Workflow_protocol.payload
(** Binary-safe Temporal payload shared with the workflow semantic protocol. *)

type failure = Workflow_protocol.failure
(** Structured Temporal failure shared with workflow activations. *)

type execution = { namespace : string; workflow_id : string; run_id : string }
(** The exact Temporal execution identified by namespace, workflow ID, and run. *)

type metadata_field = { key : string; value : payload }
(** One named payload attached to a workflow start memo or search attribute. *)

type id_conflict_policy = Fail | Use_existing | Terminate_existing
(** Temporal's [WorkflowIdConflictPolicy] for a start whose workflow ID has
    an open run, without its [UNSPECIFIED] value: the encoder always sends an
    explicit policy. The wire names are ["fail"], ["use_existing"], and
    ["terminate_existing"]. *)

type start_request = {
  request_id : string;
  namespace : string;
  workflow_id : string;
  workflow_type : string;
  task_queue : string;
  input : payload list;
  memo : metadata_field list;
  search_attributes : metadata_field list;
  id_conflict_policy : id_conflict_policy;
}
(** Dynamic workflow-start request sent to the Rust client adapter. [request_id]
    is stable across retries and is passed unchanged to Temporal, so a caller
    can reconcile an uncertain asynchronous start without issuing a second
    logical operation. *)

type start_response = { execution : execution; started : bool }
(** Execution returned by a successful start. [started] is [false] only when
    [Use_existing] returned a run created by another start request; the
    execution then names that existing run. *)

type start_ticket
(** Opaque capability for one admitted asynchronous start. The ticket retains
    the originating request privately so terminal outcomes can be correlated
    with the caller's requested workflow identity before they are exposed. *)

type wait_request = execution
(** Exact run selected by a wait; continued-as-new successors are not followed. *)

type cancel_request = {
  execution : execution;
  request_id : string;
  reason : string;
}
(** Exact run and idempotency metadata for a client cancellation request. *)

type cancel_response = { acknowledged : bool }
(** Positive acknowledgement returned after Temporal accepts the cancellation RPC. *)

type reset_request = {
  execution : execution;
  request_id : string;
  reason : string;
  workflow_task_finish_event_id : int64;
}
(** Exact run and workflow-task event used as the reset point. *)

type reset_response = { execution : execution }
(** New run identity returned by Temporal after a successful reset. *)

type terminate_request = { execution : execution; reason : string }
(** Exact-run termination request. [reason] is bounded operator context. *)

type terminate_response = { acknowledged : bool }
(** Positive acknowledgement returned after Temporal accepts termination. *)

type signal_request = {
  execution : execution;
  signal_name : string;
  request_id : string;
  input : payload list;
}
(** Exact workflow/run identity and typed payloads for one signal delivery.

    [request_id] is the Temporal idempotency key for this logical control
    operation. The signal name and payload list are encoded in the same closed
    JSON document on both sides of the native bridge. *)

type signal_response = { acknowledged : bool }
(** Positive acknowledgement returned after Temporal accepts a signal RPC. *)

type query_request = {
  execution : execution;
  query_type : string;
  input : payload list;
}
(** Exact execution identity and output-only query name sent to Temporal. The
    input list is currently required to be empty by the public client API but
    remains explicit in the closed protocol for future typed query arguments. *)

type query_response = { result : payload list }
(** Ordered payloads returned by a successful workflow query. *)

type visibility_request = {
  namespace : string;
  query : string;
  page_size : int;
  next_page_token : string option;
}
(** One explicitly bounded visibility page request. The token is opaque base64. *)

type visibility_execution = {
  workflow_id : string;
  run_id : string;
  workflow_type : string;
  task_queue : string;
  status : string;
}
(** Stable subset of Temporal visibility metadata exposed by this SDK. *)

type visibility_page = {
  executions : visibility_execution list;
  next_page_token : string option;
}
(** One visibility page and its optional opaque continuation token. *)

type update_request = {
  execution : execution;
  update_id : string;
  update_name : string;
  input : payload list;
}
(** Request to admit one named workflow update. *)

type poll_update_request = { execution : execution; update_id : string }
(** Exact update handle used by completion polls. *)

type update_outcome =
  | Update_completed of { result : payload list }
  | Update_failed of { failure : failure }
(** Terminal success or application failure returned by an update handler. *)

type update_response = {
  update_id : string;
  execution : execution;
  outcome : update_outcome option;
}
(** Admission response; [outcome = None] means the update is still pending. *)

type poll_update_response = { outcome : update_outcome option }
(** Bounded completion poll; [None] is an expected pending result. *)

type outcome =
  | Completed of { result : payload list; successor : execution option }
  | Failed of { failure : failure; successor : execution option }
  | Cancelled of { details : payload list }
  | Terminated of { details : payload list }
  | Timed_out of { successor : execution option }
  | Continued_as_new of { successor : execution }
(** Terminal outcome returned by Temporal for one exact run. *)

type wait_response = { execution : execution; outcome : outcome }
(** Exact execution and its terminal result returned by the native adapter. *)

type client_error =
  | Already_started of { workflow_id : string; existing_run_id : string option }
  | Rpc of { code : string }
  | Protocol of { code : string }
(** Closed error body returned by a native client operation. *)

type start_outcome =
  | Accepted of start_response
  | Rejected of client_error
  | Unknown of { request_id : string; workflow_id : string }
(** Terminal result of an asynchronous start. [Unknown] means the bridge
    cannot prove whether Temporal accepted the request; callers must reconcile
    using [request_id] rather than automatically retrying. *)

type error
(** Privacy-safe client-protocol validation failure. *)

type error_view = { code : string; path : string; message : string }
(** Stable diagnostic view that never contains payload bytes. *)

val error_view : error -> error_view
(** Copies the safe fields of a protocol error. *)

val encode_start_request : start_request -> (string, error) result
(** Validates and serializes one start request. *)

val encode_start_ticket : start_ticket -> (string, error) result
(** Serializes one opaque ticket for the private poll/wait bridge calls. *)

val decode_start_ticket :
  request:start_request -> string -> (start_ticket, error) result
(** Strictly decodes a native ticket and binds it to the request that admitted
    it. Binding the request here prevents a ticket result from being accepted
    for another workflow identity. *)

val start_ticket_request : start_ticket -> start_request
(** Returns the request retained by an opaque ticket for supervisor-side
    correlation. The native ticket string itself remains inaccessible. *)

val decode_start_outcome :
  request:start_request -> string -> (start_outcome, error) result
(** Strictly decodes a terminal asynchronous-start outcome and correlates every
    execution, rejection identity, and unknown request identity with [request]. *)

val encode_start_outcome : start_outcome -> (string, error) result
(** Validates and serializes one terminal asynchronous-start outcome. *)

val decode_start_response : request:start_request -> string -> (start_response, error) result
(** Strictly decodes one successful start response and verifies that the
    returned namespace and workflow ID belong to the requested start. *)

val encode_wait_request : wait_request -> (string, error) result
(** Validates and serializes one exact-run wait request. *)

val encode_cancel_request : cancel_request -> (string, error) result
(** Validates and serializes one exact-run cancellation request. *)

val decode_cancel_response : string -> (cancel_response, error) result
(** Strictly decodes the positive native cancellation acknowledgement. *)

val encode_reset_request : reset_request -> (string, error) result
(** Validates and serializes one exact-run reset request whose workflow-task
    finish event ID is greater than 1. *)

val decode_reset_response : request:reset_request -> string -> (reset_response, error) result
(** Strictly decodes and correlates the new run returned after reset. *)

val encode_terminate_request : terminate_request -> (string, error) result
(** Validates and serializes one exact-run termination request. *)

val decode_terminate_response : string -> (terminate_response, error) result
(** Strictly decodes the positive native termination acknowledgement. *)

val encode_signal_request : signal_request -> (string, error) result
(** Validates and serializes one exact-run signal request. *)

val decode_signal_response : string -> (signal_response, error) result
(** Strictly decodes the positive native signal acknowledgement. *)

val encode_query_request : query_request -> (string, error) result
(** Validates and serializes one exact-run, output-only query request. *)

val decode_query_response : string -> (query_response, error) result
(** Strictly decodes one successful query result payload list. *)

val encode_visibility_request : visibility_request -> (string, error) result
(** Validates and serializes one bounded visibility request. *)

val decode_visibility_response : string -> (visibility_page, error) result
(** Strictly decodes one visibility page returned by Rust. *)

val encode_update_request : update_request -> (string, error) result
(** Validates and serializes one update admission request. *)

val encode_poll_update_request : poll_update_request -> (string, error) result
(** Validates and serializes one update completion poll request. *)

val decode_update_response : request:update_request -> string -> (update_response, error) result
(** Decodes and correlates one update admission response. *)

val decode_poll_update_response : string -> (poll_update_response, error) result
(** Strictly decodes one bounded update poll response. *)

val decode_wait_response : request:wait_request -> string -> (wait_response, error) result
(** Strictly decodes one terminal exact-run response and verifies that the
    returned execution is exactly the requested run. *)

val decode_client_error : string -> (client_error, error) result
(** Strictly decodes the structured error body returned by the native ABI. *)

val decode_start_error :
  request:start_request -> string -> (client_error, error) result
(** Decodes a start error and correlates an [already_started] body with the
    workflow ID supplied by the start request. *)

val decode_wait_error :
  request:wait_request -> string -> (client_error, error) result
(** Decodes an exact-run wait error and rejects the start-only
    [already_started] category. *)

val decode_cancel_error : string -> (client_error, error) result
(** Decodes a cancellation error and rejects the start-only
    [already_started] category. *)

val decode_reset_error : string -> (client_error, error) result
(** Decodes a reset error and rejects the start-only [already_started]
    category. *)

val decode_signal_error : string -> (client_error, error) result
(** Decodes a signal error and rejects the start-only [already_started]
    category. *)

val decode_query_error : string -> (client_error, error) result
(** Decodes a query error and rejects the start-only [already_started]
    category. *)

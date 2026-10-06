(** The private transport boundary used by the public client and worker.

    [mock://] targets use the deterministic in-memory records below for unit
    tests of client and worker plumbing. The mock is not a workflow test
    environment: a client wait echoes the start input as the completed output
    without running a workflow, and worker tasks carry an empty [binary/null]
    input unrelated to any client start. A worker task still invokes a
    registered implementation whose input codec accepts that payload (for
    example [Codec.unit]), outside any workflow scheduler context, so callback
    side effects can occur during mock runs. HTTP(S) targets are
    routed through the private supervisor and its Rust/Core protocol; that
    path uses separate activation/completion semantic values and an explicit
    native lifecycle. Keeping those representations
    private lets the installed [Temporal] API avoid Rust handles, JSON bytes,
    and transport-specific ownership rules. *)

(** A validated connection configuration shared by client and worker graphs. *)
type config = {
  target_url : string;
  namespace : string;
  identity : string;
  task_queue : string option;
}

(** The request sent when a typed client starts one workflow execution. *)
type start_request = {
  (* Optional caller-owned idempotency key. [None] asks the native adapter to
     allocate a fresh request ID for this start call. *)
  request_id : string option;
  workflow_name : string;
  workflow_id : string;
  task_queue : string;
  input : Payload.t;
  memo : (string * Payload.t) list;
  search_attributes : (string * Payload.t) list;
  (* What to do when [workflow_id] already has an open run; see
     [Client.id_conflict_policy]. Part of the mock's request-ID fingerprint. *)
  id_conflict_policy : [ `Fail | `Use_existing | `Terminate_existing ];
}

(** The server-issued identity returned by a successful start. [started] is
    [false] only when a [`Use_existing] start returned a run that another
    request created; [run_id] then names that existing run. *)
type start_response = {
  workflow_id : string;
  run_id : string;
  started : bool;
}

(** The exact execution selected by a client wait. *)
type wait_request = {
  workflow_id : string;
  run_id : string;
}

(** Exact workflow/run pair and operator metadata for a cancellation request.
    [request_id] is stable across retries of the same logical control
    operation; [reason] is optional operator context and may be empty. *)
type cancel_request = {
  workflow_id : string;
  run_id : string;
  request_id : string;
  reason : string;
}

(** Exact workflow/run pair and operator metadata for immediate termination. *)
type terminate_request = {
  workflow_id : string;
  run_id : string;
  reason : string;
}

(** Exact workflow/run pair and event boundary for a reset request. *)
type reset_request = {
  workflow_id : string;
  run_id : string;
  request_id : string;
  reason : string;
  workflow_task_finish_event_id : int64;
}

(** New run identity returned by a successful reset. *)
type reset_response = { workflow_id : string; run_id : string }

(** Exact workflow/run pair and typed payload for one signal request. The
    connected client supplies the namespace to the native protocol so callers
    cannot redirect a handle to another namespace. *)
type signal_request = {
  workflow_id : string;
  run_id : string;
  signal_name : string;
  request_id : string;
  input : Payload.t;
}

(** Exact workflow/run identity and encoded query arguments. Output-only
    queries use an empty list; typed-input queries use one payload. The
    connected client supplies the namespace when adapting this request to the
    closed native protocol. *)
type query_request = {
  workflow_id : string;
  run_id : string;
  query_name : string;
  input : Payload.t list;
}

(** One bounded visibility query. The continuation token is opaque to callers
    and must be supplied exactly as returned by the previous page. *)
type visibility_request = {
  query : string;
  page_size : int;
  next_page_token : string option;
}

(** Stable visibility metadata returned for one execution. *)
type visibility_execution = {
  workflow_id : string;
  run_id : string;
  workflow_type : string;
  task_queue : string;
  status : string;
}

(** One visibility page and its optional opaque continuation token. *)
type visibility_page = {
  executions : visibility_execution list;
  next_page_token : string option;
}

(** Request to admit one named workflow update on an exact run. *)
type update_request = {
  workflow_id : string;
  run_id : string;
  update_id : string;
  update_name : string;
  input : Payload.t;
}

(** Terminal update outcome returned by the native protocol. *)
type update_outcome =
  | Update_completed of Payload.t list
  | Update_failed of Error.t

(** Admission response retained by the typed client update handle. *)
type update_response = {
  update_id : string;
  workflow_id : string;
  run_id : string;
  outcome : update_outcome option;
}

(** Bounded completion poll response; [None] means still pending. *)
type poll_update_response = { outcome : update_outcome option }

(** Successor identity from an exact-run close event. The protocol already
    checks its namespace against the waited execution. *)
type successor = { workflow_id : string; run_id : string }

(** Terminal outcomes are kept separate from bridge transport errors so a
    completed Temporal failure remains an ordinary typed value. Failure and
    timeout outcomes retain any server-supplied successor for explicit follow. *)
type terminal_result =
  | Completed of Payload.t
  | Failed of { error : Error.t; successor : successor option }
  | Cancelled of Error.t
  | Terminated of Error.t
  | Timed_out of { error : Error.t; successor : successor option }
  | Continued_as_new of successor

(** A synthetic workflow task used only by the deterministic unit-test seam.
    Native Core activations carry replay metadata, jobs, and history context;
    the native worker adapter translates those through separate private
    semantic types rather than widening this mock record. *)
type workflow_task = {
  task_token : string;
  workflow_name : string;
  input : Payload.t;
}

(** A synthetic activity task used only by the deterministic unit-test seam.
    Native activity tasks have cancellation and asynchronous-completion
    variants that are intentionally absent from this mock record. *)
type activity_task = {
  task_token : string;
  activity_name : string;
  input : Payload.t;
}

(** A single poll result. [Shutdown] is terminal for that poll stream and
    [Idle] permits adapters with a non-blocking readiness API. *)
type 'task poll_result =
  | Task of 'task
  | Idle
  | Shutdown

(** Synthetic workflow completion used only by the unit-test seam. Native Core
    completion is a semantic command set, not an output/failure payload. *)
type workflow_completion =
  | Workflow_completed of {
      task_token : string;
      output : Payload.t;
    }
  | Workflow_failed of {
      task_token : string;
      error : Error.t;
    }

(** Synthetic activity completion used only by the unit-test seam. Native Core
    completion also models cancellation and asynchronous completion. *)
type activity_completion =
  | Activity_completed of {
      task_token : string;
      output : Payload.t;
    }
  | Activity_failed of {
      task_token : string;
      error : Error.t;
    }

(** An opaque client backend instance owned by one public [Client.t]. *)
type client

(** An opaque worker backend instance owned by one public [Worker.t]. *)
type worker

(** Returns a defect unless an explicit [io_threads] bound is between [1]
    and the bridge maximum; [None] is always valid. *)
val validate_io_threads : int option -> (unit, Error.t) result

(** Creates a client transport after validating its configuration. The
    deterministic [mock://] ledger is test-only; HTTP(S) creates and connects
    one private supervisor graph before publishing the client value.
    [io_threads] is the public network-thread bound; the native backend
    maps it to the private runtime's Tokio worker pool. It is validated for
    every target. *)
val client_create :
  ?io_threads:int -> config -> (client, Error.t) result

(** Starts one workflow after validating the request in the backend boundary. *)
val client_start : client -> start_request -> (start_response, Error.t) result

(** Waits for the exact workflow/run pair and returns its terminal outcome. *)
val client_wait : client -> wait_request -> (terminal_result, Error.t) result

(** The [Error.error_type] of a retryable [`Bridge] error returned when the
    native client's bounded pending-start or pending-wait registry is full.
    [Client.is_at_capacity] recognizes it. *)
val client_at_capacity_error_type : string

(** The [Error.error_type] (["WorkflowExecutionAlreadyStarted"]) of the
    non-retryable [`Workflow] error returned when a [`Fail] start finds an open
    run with the same workflow ID. *)
val already_started_error_type : string

(** Returns the [(namespace, workflow_id, run_id)] of the open run attached to
    an already-started error by either transport, or [None] for any other
    error or when Temporal did not report the existing run. *)
val already_started_execution : Error.t -> (string * string * string) option

(** Converts a private supervisor failure into the public error vocabulary.
    Exposed in this private interface so bridge tests can verify that a full
    native registry ([Resource_exhausted]) stays distinct from a closed client
    ([Invalid_state]) without saturating a live Temporal connection. *)
val native_supervisor_error : Temporal_sdk_kernel.Supervisor.error -> Error.t

(** Scripts a failed or timed-out exact run in the deterministic mock for the
    public [Client.wait] to [Client.follow] regression. This private test seam
    rejects native clients and does not create successor runs. *)
val mock_set_wait_outcome_for_test :
  client -> wait_request -> terminal_result -> (unit, Error.t) result

(** Converts a protocol-validated native wait response to the private semantic
    terminal result. Kept in this private interface so bridge tests can verify
    that close-event successor identities survive the conversion. *)
val native_terminal_result :
  Temporal_sdk_kernel.Client_protocol.wait_response ->
  (terminal_result, Error.t) result

(** Requests cancellation of one exact workflow run. Success acknowledges the
    server RPC; a later [client_wait] observes the terminal cancellation. *)
val client_cancel : client -> cancel_request -> (unit, Error.t) result

(** Resets one exact workflow run and returns its new run identity. *)
val client_reset : client -> reset_request -> (reset_response, Error.t) result

(** Terminates one exact workflow run immediately. *)
val client_terminate : client -> terminate_request -> (unit, Error.t) result

(** Sends one signal to one exact workflow run. Success acknowledges the
    server RPC; it does not wait for workflow code to process the message. *)
val client_signal : client -> signal_request -> (unit, Error.t) result

(** Executes one output-only query against an exact workflow run and returns
    its encoded result payload. Query handler failures are typed errors; the
    deterministic mock reports that it does not execute handlers. *)
val client_query : client -> query_request -> (Payload.t, Error.t) result

(** Lists one bounded visibility page through Temporal's visibility service. *)
val client_list_visibility :
  client -> visibility_request -> (visibility_page, Error.t) result

(** Admits one workflow update and returns its durable update handle data. *)
val client_update : client -> update_request -> (update_response, Error.t) result

(** Polls one admitted workflow update for completion. *)
val client_poll_update :
  client -> update_request -> (poll_update_response, Error.t) result

(** Closes a client backend. Native shutdown is serialized and terminal even
    when it returns an error; the public client caches that exact result so a
    repeated call cannot hide a failed teardown. *)
val client_shutdown : client -> (unit, Error.t) result

(** Creates a deterministic worker test seam and records the task queue and
    names registered by the OCaml registry. The queue and names are retained
    here so tests exercise the same admission inputs that the native adapter
    will validate, even though its activation protocol is different. *)
val worker_create :
  config ->
  workflow_names:string list ->
  activity_names:string list ->
  (worker, Error.t) result

(** Polls one workflow activation. At most one call may be in flight per
    worker; the supervisor implementation will enforce that invariant. *)
val worker_poll_workflow :
  worker -> (workflow_task poll_result, Error.t) result

(** Polls one activity task. At most one call may be in flight per worker. *)
val worker_poll_activity :
  worker -> (activity_task poll_result, Error.t) result

(** Completes exactly one previously polled workflow activation. *)
val worker_complete_workflow :
  worker -> workflow_completion -> (unit, Error.t) result

(** Completes exactly one previously polled activity task. *)
val worker_complete_activity :
  worker -> activity_completion -> (unit, Error.t) result

(** Closes a worker backend after pollers have drained. Repeated calls are
    idempotent so application shutdown paths can be safely retried. *)
val worker_shutdown : worker -> (unit, Error.t) result

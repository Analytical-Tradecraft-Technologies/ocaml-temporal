(** Implements the private transport selected by the public client and worker.

    [mock://] remains a deterministic in-memory seam for unit tests. HTTP(S)
    targets use the private supervisor, which owns the Rust runtime and
    serializes every native call on one owner Domain. Keeping this routing in a
    private module lets the public API stay independent of JSON, Rust handles,
    and lifecycle implementation details. Every payload is copied when it is
    converted between the public and protocol representations. *)

module Bridge = Temporal_sdk_kernel.Bridge
module Native = Temporal_sdk_kernel.Supervisor
module Shared_runtime = Temporal_sdk_kernel.Shared_runtime
module Client_protocol = Temporal_sdk_kernel.Client_protocol
module Workflow_protocol = Temporal_sdk_kernel.Workflow_protocol
module Failure_diagnostic = Temporal_sdk_kernel.Failure_diagnostic

(** Connection settings copied into each backend graph. *)
type config = {
  target_url : string;
  namespace : string;
  identity : string;
  task_queue : string option;
}

(** Client start input after public codec encoding. *)
type start_request = {
  (* Optional caller-owned Temporal request ID. [None] asks the native
     adapter to allocate one fresh ID for this logical start call; [Some id]
     lets a caller retry an uncertain result with the same idempotency key. *)
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

(** Exact-run wait selector. *)
type wait_request = {
  workflow_id : string;
  run_id : string;
}

(** Exact workflow/run pair and stable control-operation metadata. *)
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

(** Exact workflow/run pair and typed payload for one signal operation. *)
type signal_request = {
  workflow_id : string;
  run_id : string;
  signal_name : string;
  request_id : string;
  input : Payload.t;
}

(** Exact workflow/run identity and encoded arguments for one read-only
    control-plane query. The list is empty for legacy output-only queries and
    contains exactly one payload for the typed-input API. *)
type query_request = {
  workflow_id : string;
  run_id : string;
  query_name : string;
  input : Payload.t list;
}

(** One bounded visibility query. The continuation token is opaque to callers
    and remains encoded only at the native protocol boundary. *)
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

(** Exact-run workflow update request retained by the private backend. *)
type update_request = {
  workflow_id : string;
  run_id : string;
  update_id : string;
  update_name : string;
  input : Payload.t;
}

(** Update completion preserves multiple payloads until the public codec
    validates the result cardinality. *)
type update_outcome =
  | Update_completed of Payload.t list
  | Update_failed of Error.t

type update_response = {
  update_id : string;
  workflow_id : string;
  run_id : string;
  outcome : update_outcome option;
}

type poll_update_response = { outcome : update_outcome option }

(** A successor returned by a close event. Its namespace was validated against
    the waited execution by the client protocol and is supplied by the owning
    public client when constructing a typed execution. *)
type successor = { workflow_id : string; run_id : string }

(** Terminal workflow outcome represented independently from transport errors. *)
type terminal_result =
  | Completed of Payload.t
  | Failed of { error : Error.t; successor : successor option }
  | Cancelled of Error.t
  | Terminated of Error.t
  | Timed_out of { error : Error.t; successor : successor option }
  | Continued_as_new of successor

(** New run identity returned by a successful reset. *)
type reset_response = { workflow_id : string; run_id : string }

(** Synthetic workflow task delivered by the deterministic unit-test seam. *)
type workflow_task = {
  task_token : string;
  workflow_name : string;
  input : Payload.t;
}

(** Synthetic activity task delivered by the deterministic unit-test seam. *)
type activity_task = {
  task_token : string;
  activity_name : string;
  input : Payload.t;
}

(** Poll state shared by workflow and activity streams. *)
type 'task poll_result =
  | Task of 'task
  | Idle
  | Shutdown

(** Synthetic workflow completion used by the deterministic unit-test seam. *)
type workflow_completion =
  | Workflow_completed of {
      task_token : string;
      output : Payload.t;
    }
  | Workflow_failed of {
      task_token : string;
      error : Error.t;
    }

(** Synthetic activity completion used by the deterministic unit-test seam. *)
type activity_completion =
  | Activity_completed of {
      task_token : string;
      output : Payload.t;
    }
  | Activity_failed of {
      task_token : string;
      error : Error.t;
    }

(** Terminal state for one mock execution. The state is monotone: once a wait
    observes completion, a later cancellation cannot rewrite that terminal
    result, which mirrors Temporal's immutable execution history. *)
type mock_terminal =
  | Mock_pending
  | Mock_completed
  | Mock_cancelled
  | Mock_terminated
  | Mock_failed of { error : Error.t; successor : successor option }
  | Mock_timed_out of { error : Error.t; successor : successor option }

(** A mock signal delivery records the request identity and payload. Retaining
    this small value lets the deterministic transport model Temporal's
    idempotency rule: repeating the same request is harmless, while reusing an
    ID for different signal data is rejected. *)
type mock_signal = {
  signal_name : string;
  input : Payload.t;
}

(** A mock execution is retained so repeated exact waits return the same
    terminal result and cancellation can only affect work still pending. The
    execution also owns its accepted signal IDs so separate client handles
    connected to the same mock endpoint observe one service ledger. *)
type mock_execution = {
  run_id : string;
  workflow_type : string;
  task_queue : string;
  input : Payload.t;
  mutable terminal : mock_terminal;
  signal_requests : (string, mock_signal) Hashtbl.t;
}

(** One accepted start keyed by workflow ID and explicit request ID. Its
    copied request distinguishes an idempotent retry from accidental key reuse. *)
type mock_start = {
  request : start_request;
  response : start_response;
}

(** One accepted reset request and the successor identity it created. Keeping
    the original request fields lets the mock enforce the same idempotency
    contract as Temporal when a caller retries an uncertain reset response. *)
type mock_reset = {
  request : reset_request;
  response : reset_response;
}

(** A deterministic mock endpoint is a process-local service ledger. Multiple
    public [Client.t] values for the same target URL and namespace share this
    ledger, which mirrors separate handles talking to one Temporal service and
    lets tests exercise exact-run operations across clients. *)
type mock_service = {
  key : string * string;
  mutable clients : int;
  mutable next_run : int;
  mutex : Mutex.t;
  executions : (string, mock_execution) Hashtbl.t;
  (** Every run, including runs retired by reset, remains addressable by its
      exact workflow/run pair until the service is released. *)
  history : ((string * string), mock_execution) Hashtbl.t;
  (** Explicit start IDs are scoped to a workflow and retain the first run. *)
  start_requests : ((string * string), mock_start) Hashtbl.t;
  (** Reset request IDs are scoped to a workflow, matching server-side
      deduplication. Each entry retains its input fingerprint so retries
      return the original successor instead of creating another run. *)
  reset_requests : ((string * string), mock_reset) Hashtbl.t;
}

(** A client graph contributes one lifecycle bit to a shared mock service.
    The service mutex protects the shared ledger; the client bit is read and
    written under that same mutex so a shutdown cannot race an operation. *)
type mock_client = {
  service : mock_service;
  mutable closed : bool;
  (** Attachment to a shared runtime (#832). The mock uses no native
      resources, but it holds a lease exactly like a native client so the
      [Runtime.shutdown] ordering rule is the same for every target. It is
      released once, when [closed] first becomes true. *)
  lease : Shared_runtime.lease option;
}

(** Serializes creation and retirement of process-local mock services. A
    service is removed when its last client shuts down so unit-test endpoints
    do not retain execution payloads for the lifetime of the process. *)
let mock_services_mutex = Mutex.create ()

(** Maps one endpoint identity to its shared deterministic ledger. The pair
    key avoids ambiguities that a separator-based concatenated string could
    introduce if a caller used that separator in a URL or namespace. *)
let mock_services : ((string * string), mock_service) Hashtbl.t =
  Hashtbl.create 8

(** Native client state retained by the private backend.

    [supervisor] is the sole owner of the Rust runtime, connected client, and
    any asynchronous start tickets. Request identities are allocated separately
    for each logical call and do not share mutable client state. *)
type native_client = {
  namespace : string;
  supervisor : Native.t;
  closed : bool Atomic.t;
}

(** The private client representation selects the deterministic mock or the
    real Rust/Core adapter from the endpoint scheme. *)
type client = Mock_client of mock_client | Native_client of native_client

(** Poll streams have independent queues because Core forbids overlapping polls
    of the same kind but permits workflow and activity streams together. *)
type mock_worker = {
  _namespace : string;
  _task_queue : string;
  mutable closed : bool;
  (** Protects queues, outstanding tables, and idle counters. Unit tests may
      exercise the mock worker from more than one Domain. *)
  mutex : Mutex.t;
  workflow_tasks : workflow_task Queue.t;
  activity_tasks : activity_task Queue.t;
  outstanding_workflows : (string, unit) Hashtbl.t;
  outstanding_activities : (string, unit) Hashtbl.t;
  mutable idle_workflow_polls : int;
  mutable idle_activity_polls : int;
  (** Shared-runtime attachment, released once on the first shutdown; see
      [mock_client.lease]. *)
  lease : Shared_runtime.lease option;
}

(** The private worker representation leaves room for the supervisor adapter. *)
type worker = Mock_worker of mock_worker

(** Copies payload bytes before retaining them in a backend-owned ledger. *)
let copy_payload (payload : Payload.t) : Payload.t =
  { payload with data = Bytes.copy payload.data }

(** Copies all mutable payload bytes in an accepted start before retaining its
    idempotency fingerprint in the process-local mock ledger. *)
let copy_start_request (request : start_request) : start_request =
  let copy_fields fields =
    List.map (fun (key, payload) -> (key, copy_payload payload)) fields
  in
  {
    request with
    input = copy_payload request.input;
    memo = copy_fields request.memo;
    search_attributes = copy_fields request.search_attributes;
  }

(** Compares every caller-controlled start field, including payload bytes, so
    reusing one explicit ID with changed data cannot return the wrong run. *)
let equal_start_request (left : start_request) (right : start_request) =
  let equal_payload (left : Payload.t) (right : Payload.t) =
    left.metadata = right.metadata && Bytes.equal left.data right.data
  in
  let equal_field (left_key, left_payload) (right_key, right_payload) =
    String.equal left_key right_key && equal_payload left_payload right_payload
  in
  left.request_id = right.request_id
  && left.id_conflict_policy = right.id_conflict_policy
  && String.equal left.workflow_name right.workflow_name
  && String.equal left.workflow_id right.workflow_id
  && String.equal left.task_queue right.task_queue
  && equal_payload left.input right.input
  && List.equal equal_field left.memo right.memo
  && List.equal equal_field left.search_attributes right.search_attributes

(** Creates a structured bridge error without exposing a backend exception. *)
let bridge_error message = Error.make ~category:`Bridge ~message ()

(** Creates a structured defect for malformed configuration or impossible local
    state transitions. *)
let defect message = Error.defect ~message

(** Converts a native status to a stable diagnostic label without depending on
    the private logging implementation in [Native_bridge]. Unknown statuses
    remain visible, which makes an ABI mismatch diagnosable without exposing a
    Rust-owned error buffer. *)
let native_status_name = function
  | Bridge.Invalid_argument -> "invalid_argument"
  | Bridge.Abi_mismatch -> "abi_mismatch"
  | Bridge.Panic -> "panic"
  | Bridge.Internal -> "internal"
  | Bridge.Invalid_state -> "invalid_state"
  | Bridge.Configuration -> "configuration"
  | Bridge.Connection -> "connection"
  | Bridge.Worker -> "worker"
  | Bridge.Outstanding_tasks -> "outstanding_tasks"
  | Bridge.Not_ready -> "not_ready"
  | Bridge.Protocol -> "protocol"
  | Bridge.Already_started -> "already_started"
  | Bridge.Retryable -> "retryable"
  | Bridge.Async_heartbeat_rejected -> "async_heartbeat_rejected"
  | Bridge.Resource_exhausted -> "resource_exhausted"
  | Bridge.Unknown code -> Printf.sprintf "unknown(%d)" code

(** The [Error.error_type] carried by a client operation that the native
    bridge refused because its bounded pending-start or pending-wait registry
    was full. The value mirrors gRPC's [RESOURCE_EXHAUSTED] code and is part
    of the documented [Client.is_at_capacity] contract. *)
let client_at_capacity_error_type = "resource_exhausted"

(** Converts a supervisor failure to the public bridge/defect vocabulary.
    Backend errors are expected operational failures; a supervisor exception
    is an internal invariant failure and is therefore deliberately marked
    non-retryable. Worker statuses are normalized here as a final defense
    against Core/gRPC diagnostic prose escaping through a client callsite. *)
let native_supervisor_error = function
  | Native.Backend { Bridge.status = Bridge.Resource_exhausted; message } ->
      (* A full bridge registry rejected admission before any RPC, while the
         client stays connected. The explicit retryable flag and stable
         [error_type] let callers tell this apart from a closed client
         without parsing [message]. *)
      Error.make ~non_retryable:false ~error_type:client_at_capacity_error_type
        ~category:`Bridge
        ~message:("native client bridge resource_exhausted: " ^ message)
        ()
  | Native.Backend { Bridge.status; message } ->
      let message =
        match status with
        | Bridge.Worker -> "native worker operation failed"
        | Bridge.Outstanding_tasks -> "native worker has outstanding tasks"
        | _ -> message
      in
      bridge_error
        (Printf.sprintf "native client bridge %s: %s" (native_status_name status)
           message)
  | Native.Closed -> bridge_error "native client supervisor is shut down"
  | Native.Owner_unavailable exception_ ->
      (* Resource exhaustion, not a defect: retrying may succeed once another
         client or worker releases its owner Domain. *)
      bridge_error
        (Printf.sprintf "native client supervisor could not start: %s"
           (Printexc.to_string exception_))
  | Native.Supervisor_failed exception_ ->
      Error.defect
        ~message:
          (Printf.sprintf "native client supervisor failed: %s"
             (Printexc.to_string exception_))

(** The [Error.error_type] of the error returned when a [`Fail] start finds an
    open run with the same workflow ID. It is Temporal's own failure name
    ([WorkflowExecutionAlreadyStartedFailure] without the suffix, as the
    official SDKs report it) and is part of the documented
    [Client.already_started] contract. *)
let already_started_error_type = "WorkflowExecutionAlreadyStarted"

(** Metadata marking the single detail payload that carries the conflicting
    execution's identity. The [encoding] entry follows Temporal's JSON payload
    convention so other SDKs can read the detail if the error is forwarded as
    a failure; the second entry distinguishes it from arbitrary JSON details
    an application might attach to its own [`Workflow] errors. *)
let already_started_detail_metadata =
  [ ("encoding", "json/plain"); ("ocaml-temporal-detail", "already_started") ]

(** Builds the typed already-started error. When Temporal reported the
    existing run, its exact identity is attached as one JSON detail payload
    ([{"namespace","workflow_id","run_id"}]) that [already_started_execution]
    decodes, so callers never parse [message] (issue #837). The run ID is still
    repeated in the message for logs. *)
let already_started_error ~namespace ~workflow_id ~existing_run_id =
  let suffix, details =
    match existing_run_id with
    | None -> ("", [])
    | Some run_id ->
        let json =
          `Assoc
            [
              ("namespace", `String namespace);
              ("workflow_id", `String workflow_id);
              ("run_id", `String run_id);
            ]
        in
        ( "; existing_run_id=" ^ run_id,
          [
            {
              Payload.metadata = already_started_detail_metadata;
              data = Bytes.of_string (Yojson.Safe.to_string json);
            };
          ] )
  in
  Error.make ~non_retryable:true ~error_type:already_started_error_type
    ~details ~category:`Workflow
    ~message:(Printf.sprintf "workflow %S is already started%s" workflow_id suffix)
    ()

(** Recovers the [(namespace, workflow_id, run_id)] attached by
    [already_started_error]. Any other error, including an already-started
    error whose run Temporal did not report, returns [None]. The structural
    checks keep an unrelated [`Workflow] error with a JSON detail from being
    misread; identity validation is left to [Client.follow]. *)
let already_started_execution error =
  let view = Error.view error in
  match (view.category, view.error_type, view.details) with
  | `Workflow, Some error_type, [ detail ]
    when String.equal error_type already_started_error_type
         && detail.metadata = already_started_detail_metadata -> (
      match Yojson.Safe.from_string (Bytes.to_string detail.data) with
      | `Assoc
          [
            ("namespace", `String namespace);
            ("workflow_id", `String workflow_id);
            ("run_id", `String run_id);
          ] ->
          Some (namespace, workflow_id, run_id)
      | _ -> None
      | exception Yojson.Json_error _ -> None)
  | _ -> None

(** The typed classification of a Temporal client RPC failure, mirrored by
    [Client.rpc_status]. Each value except [`Termination_outcome_uncertain]
    is one gRPC status code; that one is the bridge's own verdict for a
    terminate RPC whose transport deadline expired. *)
type rpc_status =
  [ `Cancelled
  | `Unknown
  | `Invalid_argument
  | `Deadline_exceeded
  | `Not_found
  | `Already_exists
  | `Permission_denied
  | `Resource_exhausted
  | `Failed_precondition
  | `Aborted
  | `Out_of_range
  | `Unimplemented
  | `Internal
  | `Unavailable
  | `Data_loss
  | `Unauthenticated
  | `Termination_outcome_uncertain ]

(** The single classification table for RPC failures: the closed protocol
    code, the public status, the stable [Error.error_type] (the canonical
    gRPC code name in PascalCase, the same naming style as Temporal's
    ["WorkflowExecutionAlreadyStarted"]), and whether retrying the same call
    is pointless.

    A failure is permanent when the same request cannot succeed without a
    change by the caller or an operator: a malformed request, a missing or
    closed run, an existing entity, a failed precondition, missing
    credentials or permission, or an unimplemented RPC. Termination with an
    uncertain outcome is also non-retryable, because the RPC has no
    idempotency key and must be reconciled with [Client.wait] instead. Every
    other status follows Temporal Core's retryable set ([Unavailable],
    [Resource_exhausted], [Aborted], [Internal], [Unknown], [Out_of_range],
    [Data_loss]) plus [Deadline_exceeded] and [Cancelled], which describe
    the attempt rather than the request. The error types are deliberately
    distinct from [client_at_capacity_error_type] (["resource_exhausted"]):
    a server [ResourceExhausted] must not look like a local admission
    refusal. *)
let rpc_statuses : (string * rpc_status * string * bool) list =
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

(** Builds the typed [`Bridge] error for one closed RPC code. The message
    keeps the historical ["Temporal client RPC failed: <code>"] form for logs
    and never includes server text. The protocol decoder admits only known
    codes; ["ok"], which tonic cannot report as a failure, and any code
    missing from the table fall back to the retryable [Unknown] class rather
    than raising. *)
let rpc_error code =
  let error_type, non_retryable =
    match
      List.find_opt (fun (wire, _, _, _) -> String.equal wire code) rpc_statuses
    with
    | Some (_, _, error_type, non_retryable) -> (error_type, non_retryable)
    | None -> ("Unknown", false)
  in
  Error.make ~category:`Bridge ~non_retryable ~error_type
    ~message:("Temporal client RPC failed: " ^ code)
    ()

(** Recognizes an error built by [rpc_error] (or the mock's equivalent) from
    its structural fields, never its message. *)
let rpc_status error =
  let view = Error.view error in
  match (view.category, view.error_type) with
  | `Bridge, Some error_type ->
      List.find_map
        (fun (_, status, candidate, _) ->
          if String.equal candidate error_type then Some status else None)
        rpc_statuses
  | _ -> None

(** The [Error.error_type] of a failed workflow query handler: Temporal's
    [QueryFailedFailure] without the suffix, as for
    [already_started_error_type]. Part of the [Client.is_query_failed]
    contract. *)
let query_failed_error_type = "QueryFailed"

(** Builds the non-retryable [`Workflow] error for a failed query handler.
    The workflow answered, so this is not a transport failure, and the same
    query against the same state fails the same way. [message] is the
    handler's own bounded message, which the bridge deliberately keeps (unlike
    server text) because it is the application's answer to its caller. *)
let query_failed_error message =
  let message =
    if String.equal message "" then "workflow query handler failed" else message
  in
  Error.make ~category:`Workflow ~non_retryable:true
    ~error_type:query_failed_error_type ~message ()

(** Recognizes [query_failed_error] structurally. *)
let is_query_failed error =
  let view = Error.view error in
  view.category = `Workflow
  && view.non_retryable
  && view.error_type = Some query_failed_error_type

(** Builds the mock transport's counterpart of Temporal's [NotFound] for an
    unknown workflow or a mismatched run, so code tested against the mock
    sees the same typed classification as against a server while keeping the
    mock's more specific diagnostic. *)
let mock_not_found message =
  Error.make ~category:`Bridge ~non_retryable:true ~error_type:"NotFound"
    ~message ()

(** Converts a structured native client operation failure while preserving its
    semantic distinction from a transport/lifecycle error. Temporal's
    duplicate-workflow response is a workflow failure carrying the existing
    run in [namespace]; RPC and protocol failures remain bridge failures
    because no terminal workflow result exists. *)
let native_client_error ~namespace = function
  | Client_protocol.Already_started { workflow_id; existing_run_id } ->
      already_started_error ~namespace ~workflow_id ~existing_run_id
  | Client_protocol.Rpc { code } -> rpc_error code
  | Client_protocol.Query_failed { message } -> query_failed_error message
  | Client_protocol.Protocol { code } ->
      bridge_error ("Temporal client protocol rejected the response: " ^ code)

(** Copies one public payload into the binary-safe protocol representation.
    Metadata values are strings in the public codec API but bytes in the closed
    JSON protocol; [Bytes.of_string] preserves every byte without assuming
    UTF-8. *)
let protocol_payload (payload : Payload.t) : Client_protocol.payload =
  {
    Workflow_protocol.metadata =
      List.map
        (fun (key, value) -> (key, Bytes.of_string value))
        payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Converts a client operation's one encoded input into Temporal's repeated
    input payloads by the shared rule [Temporal_base.Payload.input_arguments]:
    the canonical [Codec.unit] payload is sent as zero payloads, as the CLI and
    other SDKs do for a no-argument call, so a zero-parameter foreign handler
    is not given a surplus [null] argument. The base record built here only
    shares the caller's fields for inspection; [protocol_payload] copies. *)
let protocol_input (payload : Payload.t) =
  if
    Temporal_base.Payload.is_unit_null
      { Temporal_base.Payload.metadata = payload.metadata; data = payload.data }
  then []
  else [ protocol_payload payload ]

(** Copies one protocol payload into the public representation. OCaml strings
    are byte strings, so converting metadata with [Bytes.to_string] is
    lossless even for a future binary metadata value. *)
let public_payload (payload : Client_protocol.payload) : Payload.t =
  {
    Payload.metadata =
      List.map
        (fun (key, value) -> (key, Bytes.to_string value))
        payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Collects all application/cancellation detail payloads through a bounded
    failure cause chain. Protocol decoding already applies a depth limit; this
    second guard also protects callers that construct protocol values directly. *)
let failure_details failure =
  let rec loop depth reversed (value : Workflow_protocol.failure) =
    let reversed =
      match value.info with
      | Workflow_protocol.Application { details; _ }
      | Workflow_protocol.Canceled { details; _ } ->
          List.rev_append details reversed
      | Workflow_protocol.Activity _ | Workflow_protocol.Child_workflow _
      | Workflow_protocol.Terminated _ | Workflow_protocol.Server _
      | Workflow_protocol.Nexus_operation _ | Workflow_protocol.Nexus_handler _
      | Workflow_protocol.Absent ->
          reversed
      | Workflow_protocol.Timeout_failure { last_heartbeat_details; _ }
      | Workflow_protocol.Reset_workflow { last_heartbeat_details } ->
          List.rev_append last_heartbeat_details reversed
    in
    match value.cause with
    | Some cause when depth < 128 -> loop (depth + 1) reversed cause
    | None | Some _ -> List.rev reversed
  in
  loop 0 [] failure |> List.map public_payload

(** Converts a native workflow failure into the broad public error type while
    retaining retryability, structured details, the innermost application
    failure type, and a bounded diagnostic. A
    terminal client failure remains a workflow failure even when its Core
    diagnostic contains an activity or child-workflow wrapper; those more
    specific categories are reserved for errors observed inside a running
    workflow through an activity or child future. *)
let workflow_failure_error ?(category = `Workflow)
    (failure : Workflow_protocol.failure) =
  let non_retryable = Workflow_protocol.failure_non_retryable failure in
  let error_type = Failure_diagnostic.application_failure_type failure in
  Error.make ~non_retryable ?error_type ~category
    ~details:(failure_details failure)
    ~message:(Failure_diagnostic.failure_diagnostic failure) ()

(** Builds a typed cancellation/termination error from terminal detail
    payloads. Details remain binary-safe and are not interpolated into logs. *)
let terminal_details_error ~category ~message details =
  Error.make ~category
    ~details:(List.map public_payload details) ~message ()

(** Canonical [Codec.unit] / [binary/null] payload. The worker maps unit
    completions to zero Core payloads; the client reconstructs this marker so
    unit-output workflows decode instead of becoming a spurious codec error. *)
let unit_null_payload : Payload.t =
  { Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }

(** Converts one native wait response into the backend terminal-result algebra.
    A single payload is the normal case; zero payloads are the unit completion
    marker; multiple payloads remain a codec error. A close event's validated
    successor identity is retained for the public client's explicit choice. *)
let native_terminal_result (response : Client_protocol.wait_response) =
  (* The protocol has already checked the successor against the exact run;
     retain its identity until the public client can attach its namespace. *)
  let successor_ref (value : Client_protocol.execution option) : successor option =
    Option.map
      (fun (value : Client_protocol.execution) ->
        ({ workflow_id = value.workflow_id; run_id = value.run_id } : successor))
      value
  in
  match response.outcome with
  | Client_protocol.Completed { result; successor = _ } -> (
      match result with
      | [ payload ] -> Ok (Completed (public_payload payload))
      | [] -> Ok (Completed unit_null_payload)
      | _ ->
          Error
            (Error.make ~category:`Codec
               ~message:
                 "Temporal completed with multiple output payloads; the public client expects one"
               ()))
  | Client_protocol.Failed { failure; successor } ->
      Ok
        (Failed
           {
             error = workflow_failure_error failure;
             successor = successor_ref successor;
           })
  | Client_protocol.Cancelled { details } ->
      Ok
        (Cancelled
           (terminal_details_error ~category:`Cancelled
              ~message:"workflow execution was cancelled" details))
  | Client_protocol.Terminated { details } ->
      Ok
        (Terminated
           (Error.make ~non_retryable:true ~category:`Terminated
              ~details:(List.map public_payload details)
              ~message:"workflow execution was terminated" ()))
  | Client_protocol.Timed_out { successor } ->
      Ok
        (Timed_out
           {
             error =
               Error.make ~category:`Timeout
                 ~message:"workflow execution timed out" ();
             successor = successor_ref successor;
           })
  | Client_protocol.Continued_as_new { successor } ->
      Ok
        (Continued_as_new
           {
             workflow_id = successor.workflow_id;
             run_id = successor.run_id;
           })

(** Recognizes only URLs accepted by this boundary. The mock scheme remains
    private to unit tests; normal callers must use HTTP or HTTPS. *)
let valid_target_url url =
  String.starts_with ~prefix:"http://" url
  || String.starts_with ~prefix:"https://" url
  || String.starts_with ~prefix:"mock://" url

(** Rejects empty names before they enter backend state. *)
let valid_nonempty field value =
  if String.equal value "" then
    Error (defect (field ^ " must not be empty"))
  else if String.contains value '\000' then
    Error (defect (field ^ " must not contain NUL"))
  else Ok ()

(** Validates shared connection settings once at the private boundary. *)
let validate_config { target_url; namespace; identity; task_queue } =
  if not (valid_target_url target_url) then
    Error (defect "target_url must use http, https, or the private mock scheme")
  else
    match valid_nonempty "namespace" namespace with
    | Error _ as error -> error
    | Ok () -> (
        match valid_nonempty "identity" identity with
        | Error _ as error -> error
        | Ok () -> (
            match task_queue with
            | None -> Ok ()
            | Some task_queue -> valid_nonempty "task queue" task_queue))

(** Validates the optional public [io_threads] bound (#832) before any
    backend is selected, so [mock://] and native targets accept exactly the
    same values and an invalid count never allocates a supervisor. *)
let validate_io_threads io_threads =
  match Bridge.validate_runtime_worker_threads io_threads with
  | Ok () -> Ok ()
  | Error _ ->
      Error
        (defect
           (Printf.sprintf "io_threads must be between 1 and %d"
              Bridge.max_runtime_worker_threads))

(** Rejects [?runtime] together with [?io_threads] (#832): a shared runtime
    already fixes its thread bound, so accepting both would silently ignore
    one. Called before any allocation. *)
let validate_runtime_source ~io_threads ~runtime =
  match (io_threads, runtime) with
  | Some _, Some _ ->
      Error
        (defect
           "~runtime and ~io_threads are mutually exclusive; set io_threads \
            on Runtime.create instead")
  | _ -> Ok ()

(** Reserves one attachment on an optional shared runtime. The caller owns
    the returned lease and must hand it to exactly one owner (a mock value
    or [Native.create]) or release it. A shut-down runtime is a defect. *)
let acquire_runtime_lease = function
  | None -> Ok None
  | Some runtime -> (
      match Shared_runtime.acquire runtime with
      | Some lease -> Ok (Some lease)
      | None ->
          Error
            (defect
               "the runtime passed as ~runtime has already been shut down"))

(** Acquires the shared deterministic ledger for one mock endpoint. The
    registry lock protects the service reference count; operations on the
    returned service use its own mutex so unrelated endpoints can progress
    independently. *)
let acquire_mock_service ~target_url ~namespace =
  Mutex.lock mock_services_mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock mock_services_mutex)
    (fun () ->
      let key = (target_url, namespace) in
      match Hashtbl.find_opt mock_services key with
      | Some service ->
          service.clients <- service.clients + 1;
          service
      | None ->
          let service : mock_service =
            {
              key;
              clients = 1;
              next_run = 0;
              mutex = Mutex.create ();
              executions = Hashtbl.create 16;
              history = Hashtbl.create 32;
              start_requests = Hashtbl.create 16;
              reset_requests = Hashtbl.create 16;
            }
          in
          Hashtbl.add mock_services key service;
          service)

(** Releases one client reference to a mock service. The final reference
    removes the service from the registry, allowing its execution payloads and
    signal history to be collected after the last client shuts down. *)
let release_mock_service (service : mock_service) =
  Mutex.lock mock_services_mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock mock_services_mutex)
    (fun () ->
      if service.clients > 0 then service.clients <- service.clients - 1;
      if service.clients = 0 then
        match Hashtbl.find_opt mock_services service.key with
        | Some registered when registered == service ->
            Hashtbl.remove mock_services service.key
        | Some _ | None -> ())

(** Constructs either the deterministic mock ledger or the real native client.
    Native creation first validates the endpoint, then creates the complete
    supervisor graph, connects the official Rust client, and cleans up the
    graph if any step fails. No partially connected value is published. *)
let client_create ?io_threads ?runtime config =
  match validate_config config with
  | Error error -> Error error
  | Ok () ->
      match validate_io_threads io_threads with
      | Error error -> Error error
      | Ok () ->
      match validate_runtime_source ~io_threads ~runtime with
      | Error error -> Error error
      | Ok () ->
      if String.starts_with ~prefix:"mock://" config.target_url then
        Result.map
          (fun lease ->
            Mock_client
              {
                service =
                  acquire_mock_service ~target_url:config.target_url
                    ~namespace:config.namespace;
                closed = false;
                lease;
              })
          (acquire_runtime_lease runtime)
      else
        match
          Native.client_config ~target_url:config.target_url
            ~identity:config.identity
        with
        | Error error -> Error (bridge_error (Printf.sprintf "native client configuration failed: %s" error.message))
        | Ok native_config -> (
            match acquire_runtime_lease runtime with
            | Error error -> Error error
            | Ok lease ->
            (* [Native.create] owns [lease] from here: it releases it on
               failure, or after closing the graph during shutdown. *)
            match
              Native.create ?runtime_threads:io_threads ?runtime:lease
                ~capacity:32 ()
            with
            | Error error -> Error (native_supervisor_error error)
            | Ok supervisor -> (
                match Native.perform supervisor (Native.Connect_client native_config) with
                | Ok () ->
                    Ok
                      (Native_client
                         {
                           namespace = config.namespace;
                           supervisor;
                           closed = Atomic.make false;
                         })
                | Error error ->
                    (* The supervisor contract consumes the complete graph on
                       every shutdown result, including an error. *)
                    ignore (Native.shutdown supervisor);
                    Error (native_supervisor_error error)))

(** Converts one public start request to the closed native protocol value. *)
let native_start_request client (request : start_request) : Client_protocol.start_request =
  let request_id =
    match request.request_id with
    | Some request_id -> request_id
    | None -> Temporal_base.Client_request_id.create ()
  in
  let metadata fields =
    List.map
      (fun (key, value) ->
        { Client_protocol.key = key; value = protocol_payload value })
      fields
  in
  {
    request_id;
    namespace = client.namespace;
    workflow_id = request.workflow_id;
    workflow_type = request.workflow_name;
    task_queue = request.task_queue;
    input = protocol_input request.input;
    memo = metadata request.memo;
    search_attributes = metadata request.search_attributes;
    id_conflict_policy =
      (match request.id_conflict_policy with
      | `Fail -> Client_protocol.Fail
      | `Use_existing -> Client_protocol.Use_existing
      | `Terminate_existing -> Client_protocol.Terminate_existing);
  }

(** Builds the non-retryable bridge error for a start whose acceptance was
    not observed. The request and workflow IDs identify the logical operation
    so the caller can reconcile it by describing the workflow or by retrying
    with the same explicit request ID. [reason] explains why the outcome was
    lost when the bridge itself did not report an [Unknown] outcome. *)
let uncertain_start_error ?reason ~request_id ~workflow_id () =
  let suffix = match reason with None -> "" | Some reason -> ": " ^ reason in
  Error.make ~non_retryable:true ~category:`Bridge
    ~message:
      (Printf.sprintf
         "Temporal did not prove whether workflow start %S was accepted (request_id=%S)%s"
         workflow_id request_id suffix)
    ()

(** Starts one native workflow through the asynchronous ticket path. Each
    bounded wait releases the OCaml runtime lock inside the C bridge; retrying
    [None] keeps the supervisor mailbox able to accept shutdown and other
    lifecycle messages.

    Once a ticket has been issued the request belongs to a Rust task that may
    already have reached Temporal. Client shutdown aborts and retires that
    ticket, so any later lifecycle failure (closed client, closed supervisor,
    or a bridge error such as a retired ticket) is reported as an uncertain
    start rather than a plain shutdown error that would imply nothing was
    sent. Before admission the start is definitely not sent and the ordinary
    shutdown error remains accurate. *)
let native_client_start (client : native_client) (request : start_request) :
    (start_response, Error.t) result =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    let request = native_start_request client request in
    match Native.perform client.supervisor (Native.Client_begin_start_workflow request) with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error ~namespace:client.namespace error)
    | Ok (Ok ticket) ->
        let uncertain ?reason () =
          Error
            (uncertain_start_error ?reason ~request_id:request.request_id
               ~workflow_id:request.workflow_id ())
        in
        let rec await_outcome () : (start_response, Error.t) result =
          if Atomic.get client.closed then
            uncertain ~reason:"client shut down before the start outcome was observed" ()
          else
            match
              Native.perform client.supervisor
                (Native.Client_wait_start_workflow ticket)
            with
            | Error Native.Closed ->
                uncertain ~reason:"client shut down before the start outcome was observed" ()
            | Error (Native.Backend _ as error) ->
                (* The ticket may have been retired by a concurrent disconnect
                   after the RPC was issued; the diagnostic is preserved but
                   the classification stays uncertain. *)
                uncertain ~reason:(Error.message (native_supervisor_error error)) ()
            | Error error -> Error (native_supervisor_error error)
            | Ok None ->
                (* The native wait is intentionally bounded. Yielding here is
                   only on the ordinary caller Domain, never on the supervisor
                   owner or a workflow scheduler fiber. *)
                Thread.yield ();
                await_outcome ()
          | Ok (Some (Client_protocol.Accepted { execution; started })) ->
              Ok
                {
                  workflow_id = execution.workflow_id;
                  run_id = execution.run_id;
                  started;
                }
          | Ok (Some (Client_protocol.Rejected error)) ->
              Error (native_client_error ~namespace:client.namespace error)
          | Ok
              (Some
                 (Client_protocol.Unknown { request_id; workflow_id })) ->
              Error (uncertain_start_error ~request_id ~workflow_id ())
        in
        await_outcome ()

(** Allocates a new pending mock run as the current execution for the
    request's workflow ID and records an explicit request ID for idempotent
    retries. The caller holds [service.mutex] and has already resolved any
    conflict with an open run. *)
let mock_start_new_run service (request : start_request) =
  service.next_run <- service.next_run + 1;
  let run_id = Printf.sprintf "mock-run-%d" service.next_run in
  let execution =
    {
      run_id;
      workflow_type = request.workflow_name;
      task_queue = request.task_queue;
      input = copy_payload request.input;
      terminal = Mock_pending;
      signal_requests = Hashtbl.create 8;
    }
  in
  Hashtbl.replace service.executions request.workflow_id execution;
  Hashtbl.add service.history (request.workflow_id, run_id) execution;
  let response : start_response =
    { workflow_id = request.workflow_id; run_id; started = true }
  in
  (match request.request_id with
  | None -> ()
  | Some request_id ->
      Hashtbl.add service.start_requests
        (request.workflow_id, request_id)
        { request = copy_start_request request; response });
  Ok response

(** Starts a mock execution or returns the first run for an identical retry of
    an accepted explicit request ID. Retry lookup precedes workflow-ID checks,
    matching Temporal's request-ID deduplication. A new request facing a
    pending current run follows its conflict policy: [`Fail] returns the typed
    already-started error, [`Use_existing] returns the pending run with
    [started = false] (without recording the request ID, since it created
    nothing), and [`Terminate_existing] marks the pending run terminated before
    starting a new one. A closed current run is always replaced. Exact old runs
    remain in [history] after the current slot is replaced. *)
let mock_client_start (client : mock_client) (request : start_request) =
  let service = client.service in
  Mutex.lock service.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock service.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else
        let previous =
          match request.request_id with
          | None -> None
          | Some request_id ->
              Hashtbl.find_opt service.start_requests
                (request.workflow_id, request_id)
        in
        match previous with
        | Some previous when equal_start_request previous.request request ->
            Ok previous.response
        | Some _ ->
            Error
              (Error.make ~non_retryable:true ~category:`Workflow
                 ~message:"start request ID was already used for different start data"
                 ())
        | None -> (
            let open_run =
              match Hashtbl.find_opt service.executions request.workflow_id with
              | Some ({ terminal = Mock_pending; _ } as execution) -> Some execution
              | Some _ | None -> None
            in
            match (open_run, request.id_conflict_policy) with
            | Some existing, `Fail ->
                let namespace = snd service.key in
                Error
                  (already_started_error ~namespace
                     ~workflow_id:request.workflow_id
                     ~existing_run_id:(Some existing.run_id))
            | Some existing, `Use_existing ->
                Ok
                  {
                    workflow_id = request.workflow_id;
                    run_id = existing.run_id;
                    started = false;
                  }
            | Some existing, `Terminate_existing ->
                existing.terminal <- Mock_terminated;
                mock_start_new_run service request
            | None, (`Fail | `Use_existing | `Terminate_existing) ->
                mock_start_new_run service request))


(** Starts a workflow on the selected private transport. *)
let client_start client request =
  match client with
  | Mock_client client -> mock_client_start client request
  | Native_client client -> native_client_start client request

(** Waits for one exact native run. [Not_ready] resumes the same retained
    history request on the next supervisor turn, preserving pagination and
    exact-run identity while allowing shutdown to linearize between turns. *)
let native_client_wait (client : native_client) (request : wait_request) =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    let request : Client_protocol.wait_request =
      {
        namespace = client.namespace;
        workflow_id = request.workflow_id;
        run_id = request.run_id;
      }
    in
    let rec await_terminal () =
      if Atomic.get client.closed then Error (bridge_error "client is shut down")
      else
        match Native.perform client.supervisor (Native.Client_wait_workflow request) with
        | Error (Native.Backend { Bridge.status = Bridge.Not_ready; _ }) ->
            Thread.yield ();
            await_terminal ()
        | Error error -> Error (native_supervisor_error error)
        | Ok (Error error) -> Error (native_client_error ~namespace:client.namespace error)
        | Ok (Ok response) -> native_terminal_result response
    in
    await_terminal ()

(** Waits for a mock execution and echoes its input as the completed output.
    Echoing is sufficient to test typed output decoding without coupling this
    private transport to any application workflow implementation. *)
let mock_client_wait (client : mock_client) (request : wait_request) =
  let service = client.service in
  Mutex.lock service.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock service.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else
        let execution =
          match Hashtbl.find_opt service.executions request.workflow_id with
          | None -> Hashtbl.find_opt service.history (request.workflow_id, request.run_id)
          | Some current when String.equal current.run_id request.run_id -> Some current
          | Some _ -> Hashtbl.find_opt service.history (request.workflow_id, request.run_id)
        in
        match execution with
        | None -> Error (mock_not_found "workflow run id does not match the started run")
        | Some execution -> (
            match execution.terminal with
            | Mock_pending ->
                (* The first wait linearizes the synthetic execution into its
                   completed terminal state before returning the copied
                   payload. *)
                execution.terminal <- Mock_completed;
                Ok (Completed (copy_payload execution.input))
            | Mock_completed -> Ok (Completed (copy_payload execution.input))
            | Mock_cancelled ->
                Ok
                  (Cancelled
                     (Error.make ~category:`Cancelled
                        ~message:"workflow execution was cancelled" ()))
            | Mock_terminated ->
                Ok
                  (Terminated
                     (Error.make ~non_retryable:true ~category:`Terminated
                        ~message:"workflow execution was terminated" ()))
            | Mock_failed { error; successor } ->
                Ok (Failed { error; successor })
            | Mock_timed_out { error; successor } ->
                Ok (Timed_out { error; successor })))

(** Waits for a terminal result on the selected private transport. *)
let client_wait client (request : wait_request) =
  match client with
  | Mock_client client -> mock_client_wait client request
  | Native_client client -> native_client_wait client request

(** Scripts one exact mock close event for the public wait/follow regression.
    This private test seam cannot alter a native client or allocate a run. *)
let mock_set_wait_outcome_for_test client (request : wait_request) outcome =
  match client with
  | Native_client _ -> Error (defect "mock wait outcomes require a mock client")
  | Mock_client client ->
      let service = client.service in
      Mutex.lock service.mutex;
      Fun.protect
        ~finally:(fun () -> Mutex.unlock service.mutex)
        (fun () ->
          if client.closed then Error (bridge_error "client is shut down")
          else
            match
              Hashtbl.find_opt service.history
                (request.workflow_id, request.run_id)
            with
            | None -> Error (mock_not_found "workflow run id does not match the started run")
            | Some execution -> (
                match outcome with
                | Failed { error; successor } ->
                    execution.terminal <- Mock_failed { error; successor };
                    Ok ()
                | Timed_out { error; successor } ->
                    execution.terminal <- Mock_timed_out { error; successor };
                    Ok ()
                | Completed _ | Cancelled _ | Terminated _ | Continued_as_new _ ->
                    Error (defect "mock wait outcome must be failed or timed out")))

(** Converts one public cancellation request to the closed native protocol
    representation. The namespace is supplied by the connected client rather
    than copied from a caller-controlled handle. *)
let native_cancel_request client (request : cancel_request) :
    Client_protocol.cancel_request =
  {
    execution =
      {
        namespace = client.namespace;
        workflow_id = request.workflow_id;
        run_id = request.run_id;
      };
    request_id = request.request_id;
    reason = request.reason;
  }

(** Requests cancellation through the serialized supervisor operation. The
    returned acknowledgement only means Temporal accepted the RPC; the exact
    wait path remains responsible for observing the eventual terminal state. *)
let native_client_cancel (client : native_client) (request : cancel_request) :
    (unit, Error.t) result =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    match
      Native.perform client.supervisor
        (Native.Client_cancel_workflow (native_cancel_request client request))
    with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error ~namespace:client.namespace error)
    | Ok (Ok ()) -> Ok ()

(** Sends one exact-run termination through the serialized native supervisor. *)
let native_client_terminate (client : native_client)
    (request : terminate_request) : (unit, Error.t) result =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    let request : Client_protocol.terminate_request =
      {
        execution =
          {
            namespace = client.namespace;
            workflow_id = request.workflow_id;
            run_id = request.run_id;
          };
        reason = request.reason;
      }
    in
    match Native.perform client.supervisor (Native.Client_terminate_workflow request) with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error ~namespace:client.namespace error)
    | Ok (Ok ()) -> Ok ()

(** Converts a reset request to the namespace-bound protocol representation. *)
let native_reset_request client (request : reset_request) :
    Client_protocol.reset_request =
  {
    execution =
      {
        namespace = client.namespace;
        workflow_id = request.workflow_id;
        run_id = request.run_id;
      };
    request_id = request.request_id;
    reason = request.reason;
    workflow_task_finish_event_id = request.workflow_task_finish_event_id;
  }

(** Resets one exact run through the serialized supervisor operation. The
    returned run identity is retained as a value; callers can explicitly
    rebuild a typed handle and observe the new history. *)
let native_client_reset (client : native_client) (request : reset_request) :
    (reset_response, Error.t) result =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    match
      Native.perform client.supervisor
        (Native.Client_reset_workflow (native_reset_request client request))
    with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error ~namespace:client.namespace error)
    | Ok (Ok response) ->
        Ok { workflow_id = response.execution.workflow_id; run_id = response.execution.run_id }

(** Converts a public signal request to the namespace-bound protocol value.
    The exact run identity comes from the typed handle, while the connected
    client's namespace is the only namespace allowed to cross the bridge. *)
let native_signal_request client (request : signal_request) :
    Client_protocol.signal_request =
  {
    execution =
      {
        namespace = client.namespace;
        workflow_id = request.workflow_id;
        run_id = request.run_id;
      };
    signal_name = request.signal_name;
    request_id = request.request_id;
    input = protocol_input request.input;
  }

(** Sends one signal through the serialized supervisor operation. The result is
    only an RPC acknowledgement; workflow code may process the signal later. *)
let native_client_signal (client : native_client) (request : signal_request) :
    (unit, Error.t) result =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    match
      Native.perform client.supervisor
        (Native.Client_signal_workflow (native_signal_request client request))
    with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error ~namespace:client.namespace error)
    | Ok (Ok ()) -> Ok ()

(** Converts a public query request to the namespace-bound protocol document.
    The connected client's namespace is authoritative; callers cannot route a
    handle through another namespace by modifying an untyped request. *)
let native_query_request client (request : query_request) :
    Client_protocol.query_request =
  {
    execution =
      {
        namespace = client.namespace;
        workflow_id = request.workflow_id;
        run_id = request.run_id;
    };
    query_type = request.query_name;
    input = List.map protocol_payload request.input;
  }

(** Sends one output-only query through the serialized supervisor operation.
    The returned payload is decoded by the public [Client.query] function so
    the backend never needs to know the caller's result type. *)
let native_client_query (client : native_client) (request : query_request) :
    (Payload.t, Error.t) result =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    match
      Native.perform client.supervisor
        (Native.Client_query_workflow (native_query_request client request))
    with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error ~namespace:client.namespace error)
    | Ok (Ok response) -> (
        match response with
        | [ payload ] -> Ok (public_payload payload)
        | [] ->
            Error
              (Error.make ~category:`Bridge
                 ~message:"query response contained no result payload" ())
        | _ ->
            Error
              (Error.make ~category:`Bridge
                 ~message:"query response contained multiple result payloads" ()))

(** Marks one exact mock execution cancelled. Repeated cancellation requests
    are idempotent, while a mismatched workflow/run pair is rejected so the
    deterministic seam exercises the same identity contract as native Core.
    A completed execution is deliberately left unchanged: terminal history is
    immutable even when a caller sends a late cancellation request. *)
let mock_client_cancel (client : mock_client) (request : cancel_request) =
  let service = client.service in
  Mutex.lock service.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock service.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else
        match Hashtbl.find_opt service.executions request.workflow_id with
        | None -> Error (mock_not_found "workflow execution was not started")
        | Some execution
          when not (String.equal execution.run_id request.run_id) ->
            Error (mock_not_found "workflow run id does not match the started run")
        | Some execution ->
            (match execution.terminal with
            | Mock_pending -> execution.terminal <- Mock_cancelled
            | Mock_completed | Mock_cancelled | Mock_terminated
            | Mock_failed _ | Mock_timed_out _ ->
                ());
            Ok ())

(** Requests cancellation on the selected private transport. *)
let client_cancel client request =
  match client with
  | Mock_client client -> mock_client_cancel client request
  | Native_client client -> native_client_cancel client request

(** Compares all caller-visible fields that make a reset request idempotent.
    Reusing a request ID for a different event boundary or run is rejected
    rather than silently returning an unrelated successor. *)
let equal_reset_request (left : reset_request) (right : reset_request) =
  String.equal left.workflow_id right.workflow_id
  && String.equal left.run_id right.run_id
  && String.equal left.request_id right.request_id
  && String.equal left.reason right.reason
  && Int64.equal left.workflow_task_finish_event_id
       right.workflow_task_finish_event_id

(** Resets the exact source run retained in history, including a run retired
    by an earlier reset. The mock cannot replay history, so it preserves the
    source input and terminates any current pending run before installing the
    new successor. Closed runs keep their terminal result, and the
    workflow-scoped request fingerprint preserves exact reset retries. *)
let mock_client_reset (client : mock_client) (request : reset_request) =
  let service = client.service in
  Mutex.lock service.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock service.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else if request.workflow_task_finish_event_id <= 1L then
        Error
          (Error.defect
             ~message:
               "reset workflow-task finish event ID must be greater than 1")
      else
        match
          Hashtbl.find_opt service.reset_requests
            (request.workflow_id, request.request_id)
        with
        | Some previous when equal_reset_request previous.request request ->
            Ok previous.response
        | Some _ ->
            Error
              (Error.make ~category:`Workflow
                 ~message:
                   "reset request ID was already used for different reset data"
                 ())
        | None ->
          match
            Hashtbl.find_opt service.history
              (request.workflow_id, request.run_id)
          with
        | None when Hashtbl.mem service.executions request.workflow_id ->
            Error (mock_not_found "workflow run id does not match the started run")
        | None -> Error (mock_not_found "workflow execution was not started")
        | Some execution ->
            (match Hashtbl.find_opt service.executions request.workflow_id with
            | Some current when current.terminal = Mock_pending ->
                current.terminal <- Mock_terminated
            | Some _ | None -> ());
            service.next_run <- service.next_run + 1;
            let run_id = Printf.sprintf "mock-run-%d" service.next_run in
            let successor =
              {
                run_id;
                workflow_type = execution.workflow_type;
                task_queue = execution.task_queue;
                input = copy_payload execution.input;
                terminal = Mock_pending;
                signal_requests = Hashtbl.create 8;
              }
            in
            Hashtbl.replace service.executions request.workflow_id successor;
            Hashtbl.replace service.history (request.workflow_id, run_id) successor;
            let response = { workflow_id = request.workflow_id; run_id } in
            Hashtbl.add service.reset_requests
              (request.workflow_id, request.request_id)
              { request; response };
            Ok response)

(** Resets one workflow using the selected private transport. *)
let client_reset client request =
  match client with
  | Mock_client client -> mock_client_reset client request
  | Native_client client -> native_client_reset client request

(** Marks one pending mock execution as terminated; completed terminal history
    remains immutable and repeated termination is idempotent. *)
let mock_client_terminate (client : mock_client) (request : terminate_request) =
  let service = client.service in
  Mutex.lock service.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock service.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else
        match Hashtbl.find_opt service.executions request.workflow_id with
        | None -> Error (mock_not_found "workflow execution was not started")
        | Some execution
          when not (String.equal execution.run_id request.run_id) ->
            Error (mock_not_found "workflow run id does not match the started run")
        | Some execution ->
            (match execution.terminal with
            | Mock_pending -> execution.terminal <- Mock_terminated
            | Mock_completed | Mock_cancelled | Mock_terminated
            | Mock_failed _ | Mock_timed_out _ ->
                ());
            Ok ())

(** Requests immediate termination on the selected private transport. *)
let client_terminate client request =
  match client with
  | Mock_client client -> mock_client_terminate client request
  | Native_client client -> native_client_terminate client request

(** Accepts a signal for a pending mock run. The deterministic mock does not
    execute workflow code, but it records each request ID and its signal data
    so retries remain idempotent and accidental ID reuse is visible in tests.
    The shared service ledger means a handle rebuilt through another public
    [Client.t] still addresses the same synthetic execution. *)
let mock_client_signal (client : mock_client) (request : signal_request) =
  let service = client.service in
  Mutex.lock service.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock service.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else
        match Hashtbl.find_opt service.executions request.workflow_id with
        | None -> Error (mock_not_found "workflow execution was not started")
        | Some execution
          when not (String.equal execution.run_id request.run_id) ->
            Error (mock_not_found "workflow run id does not match the started run")
        | Some ({ terminal = Mock_pending; _ } as execution) -> (
            let delivery =
              { signal_name = request.signal_name; input = copy_payload request.input }
            in
            match Hashtbl.find_opt execution.signal_requests request.request_id with
            | None ->
                Hashtbl.add execution.signal_requests request.request_id delivery;
                Ok ()
            | Some previous
              when String.equal previous.signal_name delivery.signal_name
                   && previous.input.metadata = delivery.input.metadata
                   && Bytes.equal previous.input.data delivery.input.data ->
                Ok ()
            | Some _ ->
                Error
                  (Error.make ~category:`Workflow
                     ~message:
                       "signal request ID was already used for different signal data"
                     ()))
        | Some _ ->
            (* Temporal answers a signal to a closed run with NotFound. *)
            Error (mock_not_found "workflow execution is not running"))

(** Sends one signal on the selected private transport. *)
let client_signal client request =
  match client with
  | Mock_client client -> mock_client_signal client request
  | Native_client client -> native_client_signal client request

(** The mock endpoint does not execute workflow code, so it cannot produce a
    query handler result. It still validates the exact run identity before
    returning a typed error, which keeps lifecycle behavior deterministic in
    unit tests and avoids pretending an empty result is a real query answer. *)
let mock_client_query (client : mock_client) (request : query_request) =
  let service = client.service in
  Mutex.lock service.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock service.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else
        match Hashtbl.find_opt service.executions request.workflow_id with
        | None -> Error (mock_not_found "workflow execution was not started")
        | Some execution
          when not (String.equal execution.run_id request.run_id) ->
            Error (mock_not_found "workflow run id does not match the started run")
        | Some _ ->
            Error
              (Error.make ~category:`Workflow
                 ~message:
                   "the deterministic mock does not execute workflow queries"
                 ()))

(** Executes one query using the selected private transport. *)
let client_query client request =
  match client with
  | Mock_client client -> mock_client_query client request
  | Native_client client -> native_client_query client request

(** Converts one public visibility request to the namespace-bound protocol
    representation. The server namespace is never caller-controlled. *)
let native_visibility_request client (request : visibility_request) :
    Client_protocol.visibility_request =
  {
    namespace = client.namespace;
    query = request.query;
    page_size = request.page_size;
    next_page_token = request.next_page_token;
  }

(** Lists one visibility page through the serialized supervisor operation.
    Rust validates the request again and owns the opaque protobuf pagination
    token; OCaml only maps the stable row fields into the private backend type. *)
let native_client_list_visibility (client : native_client)
    (request : visibility_request) : (visibility_page, Error.t) result =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    match
      Native.perform client.supervisor
        (Native.Client_list_visibility_workflows
           (native_visibility_request client request))
    with
    | Error error -> Error (native_supervisor_error error)
    | Ok page ->
        Ok
          {
            executions =
              List.map
                (fun (execution : Client_protocol.visibility_execution) ->
                  {
                    workflow_id = execution.workflow_id;
                    run_id = execution.run_id;
                    workflow_type = execution.workflow_type;
                    task_queue = execution.task_queue;
                    status = execution.status;
                  })
                page.executions;
            next_page_token = page.next_page_token;
          }

(** Lists every retained mock run in workflow-ID and run-ID order. A page token
    resumes after the last exact run returned, even when reset created another
    run with the same workflow ID. The mock accepts only an empty query;
    native clients use Temporal's query language. *)
let mock_client_list_visibility (client : mock_client)
    (request : visibility_request) : (visibility_page, Error.t) result =
  if request.page_size < 1 || request.page_size > 1_000 then
    Error (defect "visibility page_size must be between 1 and 1000")
  else if String.length request.query > 65_536 then
    Error (defect "visibility query exceeds the protocol safety limit")
  else if String.contains request.query '\000' then
    Error (defect "visibility query must not contain NUL")
  else if not (String.equal request.query "") then
    Error (defect "the deterministic mock only supports an empty visibility query")
  else
    let token_prefix = "mock-visibility-v2:" in
    let cursor =
      match request.next_page_token with
      | None -> Ok None
      | Some token ->
          if String.starts_with ~prefix:token_prefix token
             && String.length token > String.length token_prefix
          then
            let start = String.length token_prefix in
            (match String.index_from_opt token start ':' with
            | None -> Error (defect "invalid mock visibility page token")
            | Some delimiter ->
                let length =
                  String.sub token start (delimiter - start) |> int_of_string_opt
                in
                let remaining = String.length token - delimiter - 1 in
                (match length with
                | Some length when length > 0 && length < remaining ->
                    Ok
                      (Some
                         ( String.sub token (delimiter + 1) length,
                           String.sub token (delimiter + 1 + length)
                             (remaining - length) ))
                | _ -> Error (defect "invalid mock visibility page token")))
          else Error (defect "invalid mock visibility page token")
    in
    Result.bind cursor (fun cursor ->
      let service = client.service in
      Mutex.lock service.mutex;
      Fun.protect
        ~finally:(fun () -> Mutex.unlock service.mutex)
        (fun () ->
          if client.closed then Error (bridge_error "client is shut down")
          else if
            (match cursor with
            | Some key -> not (Hashtbl.mem service.history key)
            | None -> false)
          then Error (defect "invalid mock visibility page token")
          else
            let executions : visibility_execution list =
              Hashtbl.to_seq service.history
              |> Seq.map (fun ((workflow_id, _run_id), (execution : mock_execution)) ->
                     let status =
                       match execution.terminal with
                       | Mock_pending -> "running"
                       | Mock_completed -> "completed"
                       | Mock_cancelled -> "canceled"
                       | Mock_terminated -> "terminated"
                       | Mock_failed _ -> "failed"
                       | Mock_timed_out _ -> "timed_out"
                     in
                     ({
                        workflow_id;
                        run_id = execution.run_id;
                        workflow_type = execution.workflow_type;
                        task_queue = execution.task_queue;
                        status;
                      }
                       : visibility_execution))
              |> List.of_seq
              |> List.sort (fun (left : visibility_execution)
                                (right : visibility_execution) ->
                      let workflow_order =
                        String.compare left.workflow_id right.workflow_id
                      in
                      if workflow_order <> 0 then workflow_order
                      else String.compare left.run_id right.run_id)
            in
            let remaining =
              match cursor with
              | None -> executions
              | Some (workflow_id, run_id) ->
                  List.filter
                    (fun (execution : visibility_execution) ->
                      let workflow_order =
                        String.compare execution.workflow_id workflow_id
                      in
                      workflow_order > 0
                      || (workflow_order = 0
                         && String.compare execution.run_id run_id > 0))
                    executions
            in
            (* A token is present exactly when another row follows this page. *)
            let rec take_page count last acc = function
              | [] ->
                  ({ executions = List.rev acc; next_page_token = None }
                    : visibility_page)
              | _ :: _ when count = 0 ->
                  {
                    executions = List.rev acc;
                    next_page_token =
                      Option.map
                        (fun (execution : visibility_execution) ->
                          token_prefix
                          ^ string_of_int (String.length execution.workflow_id)
                          ^ ":" ^ execution.workflow_id ^ execution.run_id)
                        last;
                  }
              | execution :: rest ->
                  take_page (count - 1) (Some execution) (execution :: acc) rest
            in
            Ok (take_page request.page_size None [] remaining)))

(** Executes visibility through the selected private transport. *)
let client_list_visibility client request =
  match client with
  | Mock_client client -> mock_client_list_visibility client request
  | Native_client client -> native_client_list_visibility client request

(** Converts a public update request to the namespace-bound JSON protocol. *)
let native_update_request client (request : update_request) :
    Client_protocol.update_request =
  {
    execution =
      {
        namespace = client.namespace;
        workflow_id = request.workflow_id;
        run_id = request.run_id;
      };
    update_id = request.update_id;
    update_name = request.update_name;
    input = protocol_input request.input;
  }

(** Converts one protocol update outcome into public payloads or a typed
    update failure. The protocol decoder has already validated every payload
    and failure tree before this function runs. *)
let public_update_outcome = function
  | Client_protocol.Update_completed { result } ->
      Update_completed (List.map public_payload result)
  | Client_protocol.Update_failed { failure } ->
      Update_failed (workflow_failure_error ~category:`Update failure)

(** Starts one native update and returns its admitted handle data. *)
let native_client_update (client : native_client) (request : update_request) =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    match
      Native.perform client.supervisor
        (Native.Client_update_workflow (native_update_request client request))
    with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error ~namespace:client.namespace error)
    | Ok (Ok response) ->
        Ok
          {
            update_id = response.update_id;
            workflow_id = response.execution.workflow_id;
            run_id = response.execution.run_id;
            outcome = Option.map public_update_outcome response.outcome;
          }

(** Polls one admitted native update. The request input is ignored after
    admission but retained in the private value to keep exact handle identity
    and codec ownership in one record. *)
let native_client_poll_update (client : native_client) (request : update_request) =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    let protocol_request : Client_protocol.poll_update_request =
      {
        execution =
          {
            namespace = client.namespace;
            workflow_id = request.workflow_id;
            run_id = request.run_id;
          };
        update_id = request.update_id;
      }
    in
    match
      Native.perform client.supervisor
        (Native.Client_poll_update_workflow protocol_request)
    with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error ~namespace:client.namespace error)
    | Ok (Ok response) ->
        Ok { outcome = Option.map public_update_outcome response.outcome }

(** The mock endpoint intentionally does not execute workflow handlers, so it
    cannot claim an update was admitted. *)
let mock_client_update (client : mock_client) (_request : update_request) =
  if client.closed then Error (bridge_error "client is shut down")
  else
    Error
      (Error.make ~category:`Update
         ~message:"the deterministic mock does not execute workflow updates" ())

let mock_client_poll_update (client : mock_client) (_request : update_request) =
  if client.closed then Error (bridge_error "client is shut down")
  else
    Error
      (Error.make ~category:`Update
         ~message:"the deterministic mock does not execute workflow updates" ())

(** Starts one update through the selected private transport. *)
let client_update client request =
  match client with
  | Mock_client client -> mock_client_update client request
  | Native_client client -> native_client_update client request

(** Polls one update through the selected private transport. *)
let client_poll_update client request =
  match client with
  | Mock_client client -> mock_client_poll_update client request
  | Native_client client -> native_client_poll_update client request

(** Marks a client closed while preserving idempotent cleanup. Native shutdown
    runs the supervisor's reverse-order worker/client/runtime release path;
    mock shutdown only flips its local lifecycle bit. The supervisor caches the
    terminal shutdown result and its backend contract guarantees that an error
    still consumes or invalidates the complete native graph, so recording the
    public closed bit before returning an error cannot strand a live handle. *)
let client_shutdown = function
  | Mock_client client ->
      let service = client.service in
      let should_release =
        Mutex.lock service.mutex;
        Fun.protect
          ~finally:(fun () -> Mutex.unlock service.mutex)
          (fun () ->
            if client.closed then false
            else (
              client.closed <- true;
              true))
      in
      if should_release then (
        release_mock_service service;
        Option.iter Shared_runtime.release client.lease);
      Ok ()
  | Native_client client ->
      if Atomic.exchange client.closed true then Ok ()
      else
        match Native.shutdown client.supervisor with
        | Ok () -> Ok ()
        | Error error -> Error (native_supervisor_error error)

(** The canonical empty payload used by the deterministic mock worker tasks. *)
let unit_payload =
  { Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }

(** Registers one mock task per local definition. The input is [unit] so tests
    can observe dispatch without adding a test-only payload protocol. *)
let worker_create ?runtime config ~workflow_names ~activity_names =
  match validate_config config with
  | Error error -> Error error
  | Ok () ->
      if config.task_queue = None then
        Error (defect "worker task queue is required")
      else if not (String.starts_with ~prefix:"mock://" config.target_url) then
        Error
          (bridge_error
             "native worker adapter is not connected in this build")
      else
        let workflow_tasks = Queue.create () in
        let activity_tasks = Queue.create () in
        List.iter
          (fun name ->
            Queue.add
              {
                task_token = "mock-workflow-" ^ name;
                workflow_name = name;
                input = copy_payload unit_payload;
              }
              workflow_tasks)
          workflow_names;
        List.iter
          (fun name ->
            Queue.add
              {
                task_token = "mock-activity-" ^ name;
                activity_name = name;
                input = copy_payload unit_payload;
              }
              activity_tasks)
          activity_names;
        Result.map
          (fun lease ->
            Mock_worker
             {
               _namespace = config.namespace;
               _task_queue = Option.get config.task_queue;
               closed = false;
               mutex = Mutex.create ();
               workflow_tasks;
               activity_tasks;
               outstanding_workflows = Hashtbl.create 16;
               outstanding_activities = Hashtbl.create 16;
               idle_workflow_polls = 0;
               idle_activity_polls = 0;
               lease;
             })
          (acquire_runtime_lease runtime)

(** Polls the workflow queue and records ownership before exposing a task. *)
let worker_poll_workflow (Mock_worker worker) =
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      if worker.closed then Ok Shutdown
      else if Queue.is_empty worker.workflow_tasks then (
        worker.idle_workflow_polls <- worker.idle_workflow_polls + 1;
        Ok Shutdown)
      else
        let task = Queue.take worker.workflow_tasks in
        Hashtbl.replace worker.outstanding_workflows task.task_token ();
        Ok (Task task))

(** Polls the activity queue and records ownership before exposing a task. *)
let worker_poll_activity (Mock_worker worker) =
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      if worker.closed then Ok Shutdown
      else if Queue.is_empty worker.activity_tasks then (
        worker.idle_activity_polls <- worker.idle_activity_polls + 1;
        Ok Shutdown)
      else
        let task = Queue.take worker.activity_tasks in
        Hashtbl.replace worker.outstanding_activities task.task_token ();
        Ok (Task task))

(** Removes an outstanding workflow task exactly once. *)
let require_workflow_token worker token =
  if Hashtbl.mem worker.outstanding_workflows token then (
    Hashtbl.remove worker.outstanding_workflows token;
    Ok ())
  else Error (bridge_error "workflow completion token is unknown or reused")

(** Removes an outstanding activity task exactly once. *)
let require_activity_token worker token =
  if Hashtbl.mem worker.outstanding_activities token then (
    Hashtbl.remove worker.outstanding_activities token;
    Ok ())
  else Error (bridge_error "activity completion token is unknown or reused")

(** Completes a workflow task after checking its one-shot token lease. *)
let worker_complete_workflow (Mock_worker worker) completion =
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      if worker.closed then Error (bridge_error "worker is shut down")
      else
        let token =
          match completion with
          | Workflow_completed { task_token; _ }
          | Workflow_failed { task_token; _ } -> task_token
        in
        require_workflow_token worker token)

(** Completes an activity task after checking its one-shot token lease. *)
let worker_complete_activity (Mock_worker worker) completion =
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      if worker.closed then Error (bridge_error "worker is shut down")
      else
        let token =
          match completion with
          | Activity_completed { task_token; _ }
          | Activity_failed { task_token; _ } -> task_token
        in
        require_activity_token worker token)

(** Closes worker admission; Core-backed implementations will first drain
    pollers and outstanding leases before returning from this function. *)
let worker_shutdown (Mock_worker worker) =
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      if not worker.closed then Option.iter Shared_runtime.release worker.lease;
      worker.closed <- true;
      Ok ())

//! Regression coverage for issue #1151: the submitted `SIGNAL_WITH_START` and
//! `UPDATE_WITH_START` client calls behind `Client.signal_with_start` and
//! `Client.update_with_start`.
//!
//! A minimal plaintext HTTP/2 gRPC double stands in for the Temporal server,
//! so the tests are deterministic and run in every Rust test job. It answers
//! `SignalWithStartWorkflowExecution` according to the requested workflow ID
//! (see [`signal_with_start_answer`]) and `ExecuteMultiOperation` according to
//! the workflow ID and how many times that workflow was asked before (see
//! [`update_with_start_answer`]). Every other RPC succeeds with an empty
//! message.

use std::io::Read as _;
use std::net::SocketAddr;
use std::ptr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_NOT_READY, STATUS_OK, STATUS_PROTOCOL, Status,
    ocaml_temporal_core_v4_client_await_call, ocaml_temporal_core_v4_client_connect_json,
    ocaml_temporal_core_v4_client_disconnect, ocaml_temporal_core_v4_client_submit_json,
    ocaml_temporal_core_v4_result_free, ocaml_temporal_core_v4_runtime_free,
    ocaml_temporal_core_v4_runtime_new,
};
use prost::Message as _;
use prost::bytes::Bytes;
use temporalio_common::protos::google::rpc::Status as RpcStatus;
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowExecution,
    enums::v1::{UpdateWorkflowExecutionLifecycleStage, WorkflowIdConflictPolicy},
    errordetails::v1::{
        MultiOperationExecutionFailure, WorkflowExecutionAlreadyStartedFailure,
        multi_operation_execution_failure::OperationStatus,
    },
    failure::v1::Failure,
    update::v1::{Outcome, UpdateRef, outcome::Value},
    workflowservice::v1::{
        ExecuteMultiOperationRequest, ExecuteMultiOperationResponse,
        SignalWithStartWorkflowExecutionRequest, SignalWithStartWorkflowExecutionResponse,
        StartWorkflowExecutionResponse, UpdateWorkflowExecutionResponse,
        execute_multi_operation_request, execute_multi_operation_response,
    },
};

/// Selector of a submitted start, as in the C header.
const CALL_START: u32 = 1;
/// Selector of a submitted signal-with-start, as in the C header.
const CALL_SIGNAL_WITH_START: u32 = 16;
/// Selector of a submitted update-with-start, as in the C header.
const CALL_UPDATE_WITH_START: u32 = 17;

/// The run the double reports for a newly started workflow.
const NEW_RUN: &str = "new-run";
/// The run the double reports for a workflow that was already running.
const EXISTING_RUN: &str = "existing-run";

/// Upper bound for one bridge call under test; a regression fails instead of
/// hanging CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// Requests the double received, in arrival order.
#[derive(Default)]
struct Seen {
    signal_with_starts: Vec<SignalWithStartWorkflowExecutionRequest>,
    multi_operations: Vec<ExecuteMultiOperationRequest>,
}

/// Shared record of what the double received.
type Observed = Arc<Mutex<Seen>>;

/// A gRPC answer: an encoded response message, or a status code with
/// optional encoded `google.rpc.Status` details.
type Answer = Result<Vec<u8>, (&'static str, Option<Vec<u8>>)>;

/// Returns the protobuf message of one gRPC request body, decompressing it
/// when its length-prefix flag says Core sent it gzip-compressed.
fn grpc_message(buffer: &[u8]) -> Vec<u8> {
    let compressed = buffer.first() == Some(&1);
    let message = buffer.get(5..).unwrap_or_default();
    if compressed {
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(message)
            .read_to_end(&mut decoded)
            .expect("Core sends valid gzip request messages");
        decoded
    } else {
        message.to_vec()
    }
}

/// Answers one signal-with-start by its workflow ID: `fresh` starts a new
/// run, `existing` signals a running one, `invalid` is `INVALID_ARGUMENT`,
/// and `down` is `UNAVAILABLE`.
fn signal_with_start_answer(request: &SignalWithStartWorkflowExecutionRequest) -> Answer {
    let (run_id, started) = match request.workflow_id.as_str() {
        "fresh" => (NEW_RUN, true),
        "existing" => (EXISTING_RUN, false),
        "invalid" => return Err(("3", None)),
        _ => return Err(("14", None)),
    };
    Ok(SignalWithStartWorkflowExecutionResponse {
        run_id: run_id.to_owned(),
        started,
        ..Default::default()
    }
    .encode_to_vec())
}

/// Builds a multi-operation answer whose start reports [`NEW_RUN`] and whose
/// update has `stage` and `outcome`.
fn multi_operation_answer(
    request: &ExecuteMultiOperationRequest,
    stage: UpdateWorkflowExecutionLifecycleStage,
    outcome: Option<Outcome>,
) -> Vec<u8> {
    use execute_multi_operation_response::{Response, response::Response as Kind};
    let update_id = match request
        .operations
        .get(1)
        .and_then(|op| op.operation.as_ref())
    {
        Some(execute_multi_operation_request::operation::Operation::UpdateWorkflow(update)) => {
            update
                .request
                .as_ref()
                .and_then(|request| request.meta.as_ref())
                .map(|meta| meta.update_id.clone())
                .unwrap_or_default()
        }
        _ => String::new(),
    };
    ExecuteMultiOperationResponse {
        responses: vec![
            Response {
                response: Some(Kind::StartWorkflow(StartWorkflowExecutionResponse {
                    run_id: NEW_RUN.to_owned(),
                    started: true,
                    ..Default::default()
                })),
            },
            Response {
                response: Some(Kind::UpdateWorkflow(UpdateWorkflowExecutionResponse {
                    update_ref: Some(UpdateRef {
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id: request.resource_id.clone(),
                            run_id: NEW_RUN.to_owned(),
                        }),
                        update_id,
                    }),
                    outcome,
                    stage: stage as i32,
                    ..Default::default()
                })),
            },
        ],
    }
    .encode_to_vec()
}

/// The encoded `google.rpc.Status` details of a multi-operation whose start
/// failed because `existing` is already running and whose update was
/// aborted.
fn start_conflict_details() -> Vec<u8> {
    let any = |type_url: &str, value: Vec<u8>| {
        let mut status = RpcStatus {
            code: 6,
            message: String::new(),
            details: vec![Default::default()],
        };
        status.details[0].type_url = type_url.to_owned();
        status.details[0].value = value;
        status.details
    };
    let failure = MultiOperationExecutionFailure {
        statuses: vec![
            OperationStatus {
                code: 6,
                message: String::new(),
                details: any(
                    "type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure",
                    WorkflowExecutionAlreadyStartedFailure {
                        start_request_id: "other".to_owned(),
                        run_id: EXISTING_RUN.to_owned(),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                ),
            },
            OperationStatus {
                code: 10,
                message: String::new(),
                details: any(
                    "type.googleapis.com/temporal.api.failure.v1.MultiOperationExecutionAborted",
                    Vec::new(),
                ),
            },
        ],
    };
    RpcStatus {
        code: 6,
        message: String::new(),
        details: any(
            "type.googleapis.com/temporal.api.errordetails.v1.MultiOperationExecutionFailure",
            failure.encode_to_vec(),
        ),
    }
    .encode_to_vec()
}

/// Answers one update-with-start by its workflow ID; `asked` counts the
/// earlier requests for the same workflow:
///
/// - `accept`: the update is accepted at once;
/// - `admit-once`: admitted on the first request, accepted on the next;
/// - `admit-forever`: always only admitted;
/// - `reject`: a validator rejected the update (a failed outcome);
/// - `conflict`: the start failed as already started, the update aborted;
/// - `down`: `UNAVAILABLE`.
fn update_with_start_answer(request: &ExecuteMultiOperationRequest, asked: usize) -> Answer {
    use UpdateWorkflowExecutionLifecycleStage::{Accepted, Admitted, Completed};
    match request.resource_id.as_str() {
        "accept" => Ok(multi_operation_answer(request, Accepted, None)),
        "admit-once" if asked == 0 => Ok(multi_operation_answer(request, Admitted, None)),
        "admit-once" => Ok(multi_operation_answer(request, Accepted, None)),
        "admit-forever" => Ok(multi_operation_answer(request, Admitted, None)),
        "reject" => Ok(multi_operation_answer(
            request,
            Completed,
            Some(Outcome {
                value: Some(Value::Failure(Failure {
                    message: "rejected by validator".to_owned(),
                    ..Default::default()
                })),
            }),
        )),
        "conflict" => Err(("6", Some(start_conflict_details()))),
        _ => Err(("14", None)),
    }
}

/// Starts the gRPC double on a detached OS thread with its own
/// current-thread Tokio runtime and returns its loopback address. The thread
/// ends with the test process.
fn spawn_server(observed: Observed) -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-with-start-double".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test server runtime");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind loopback test server");
                address_sender
                    .send(listener.local_addr().expect("test server address"))
                    .expect("report test server address");
                loop {
                    let Ok((socket, _)) = listener.accept().await else {
                        continue;
                    };
                    tokio::spawn(serve_connection(socket, Arc::clone(&observed)));
                }
            });
        })
        .expect("spawn test server thread");
    address_receiver
        .recv_timeout(CALL_DEADLINE)
        .expect("test server started")
}

/// Serves one HTTP/2 connection until the client closes it, answering each
/// request on its own task.
async fn serve_connection(socket: tokio::net::TcpStream, observed: Observed) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((request, respond))) = connection.accept().await {
        tokio::spawn(answer(request, respond, Arc::clone(&observed)));
    }
}

/// Reads one request body and answers it as described in the module docs.
async fn answer(
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    observed: Observed,
) {
    let path = request.uri().path().to_owned();
    let mut body = request.into_body();
    let mut received = Vec::new();
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
        received.extend_from_slice(&chunk);
    }
    let received = grpc_message(&received);
    // The default instance of every other response message encodes to zero
    // bytes, so an empty message is a valid success.
    let outcome = if path.ends_with("/SignalWithStartWorkflowExecution") {
        let request = SignalWithStartWorkflowExecutionRequest::decode(received.as_slice())
            .expect("Core sends a valid signal-with-start request");
        let outcome = signal_with_start_answer(&request);
        observed
            .lock()
            .expect("observed lock")
            .signal_with_starts
            .push(request);
        outcome
    } else if path.ends_with("/ExecuteMultiOperation") {
        let request = ExecuteMultiOperationRequest::decode(received.as_slice())
            .expect("Core sends a valid multi-operation request");
        let mut seen = observed.lock().expect("observed lock");
        let asked = seen
            .multi_operations
            .iter()
            .filter(|earlier| earlier.resource_id == request.resource_id)
            .count();
        let outcome = update_with_start_answer(&request, asked);
        seen.multi_operations.push(request);
        outcome
    } else {
        Ok(Vec::new())
    };
    let response = http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(())
        .expect("static gRPC response");
    let Ok(mut stream) = respond.send_response(response, false) else {
        return;
    };
    let mut trailers = http::HeaderMap::new();
    let grpc_status = match outcome {
        Ok(message) => {
            let length = u32::try_from(message.len()).expect("small test message");
            let mut frame = vec![0];
            frame.extend_from_slice(&length.to_be_bytes());
            frame.extend_from_slice(&message);
            if stream.send_data(Bytes::from(frame), false).is_err() {
                return;
            }
            "0"
        }
        Err((code, details)) => {
            if let Some(details) = details {
                let encoded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(details);
                trailers.insert(
                    "grpc-status-details-bin",
                    http::HeaderValue::from_str(&encoded).expect("base64 header"),
                );
            }
            code
        }
    };
    trailers.insert(
        "grpc-status",
        http::HeaderValue::from_str(grpc_status).expect("static status"),
    );
    let _ = stream.send_trailers(trailers);
}

/// Copies one live ABI buffer without taking ownership from its result.
fn bytes(buffer: &Buffer) -> Vec<u8> {
    if buffer.ptr.is_null() {
        assert_eq!(buffer.len, 0);
        Vec::new()
    } else {
        // SAFETY: The bridge owns this readable allocation until the
        // containing result is released by `release`.
        unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len).to_vec() }
    }
}

/// Frees a live result and returns its status, value, and diagnostic text.
fn release(mut result: AbiResult) -> (Status, Vec<u8>, String) {
    let status = result.status;
    let value = bytes(&result.value);
    let message = String::from_utf8(bytes(&result.error)).expect("diagnostic is UTF-8");
    // SAFETY: This helper has exclusive ownership of the initialized result.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    (status, value, message)
}

/// Creates a runtime connected to the double and returns it, exclusively
/// owned by the caller.
fn start_runtime(address: SocketAddr) -> *mut Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);
    let client = format!(r#"{{"target_url":"http://{address}","identity":"with-start-test"}}"#);
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned; the input remains
    // readable for the full blocking call.
    unsafe {
        ocaml_temporal_core_v4_client_connect_json(
            runtime,
            client.as_ptr(),
            client.len(),
            &mut result,
        );
    }
    let (status, _, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
    runtime
}

/// Submits one call of `kind` and returns its handle, or the submission's
/// failure.
fn submit(
    runtime: *mut Runtime,
    kind: u32,
    document: &str,
) -> Result<(u64, u64), (Status, String)> {
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread; the
    // document stays readable for the call.
    unsafe {
        ocaml_temporal_core_v4_client_submit_json(
            runtime,
            kind,
            document.as_ptr(),
            document.len(),
            &mut result,
        );
    }
    let (status, value, message) = release(result);
    if status != STATUS_OK {
        return Err((status, message));
    }
    let handle = String::from_utf8(value).expect("ASCII call handle");
    let (owner, call) = handle.split_once('.').expect("<owner>.<call> handle");
    Ok((
        owner.parse().expect("decimal owner"),
        call.parse().expect("decimal call"),
    ))
}

/// Awaits one submitted call from this thread and returns its terminal
/// status, value, and diagnostic.
fn await_call((owner, call): (u64, u64)) -> (Status, Vec<u8>, String) {
    let deadline = Instant::now() + CALL_DEADLINE;
    loop {
        let mut result = AbiResult::default();
        // SAFETY: The output location is writable and exclusively owned; the
        // await borrows no runtime.
        unsafe { ocaml_temporal_core_v4_client_await_call(owner, call, 1_000, &mut result) };
        let outcome = release(result);
        if outcome.0 != STATUS_NOT_READY {
            return outcome;
        }
        assert!(Instant::now() < deadline, "submitted call never completed");
    }
}

/// Submits a call that must complete successfully and returns its parsed
/// outcome document.
fn outcome(runtime: *mut Runtime, kind: u32, document: &str) -> serde_json::Value {
    let (status, value, message) = await_call(submit(runtime, kind, document).expect(document));
    assert_eq!(status, STATUS_OK, "{message}");
    serde_json::from_slice(&value).expect("outcome is JSON")
}

/// Disconnects the client and frees the runtime.
fn close_runtime(mut runtime: *mut Runtime) {
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread.
    unsafe { ocaml_temporal_core_v4_client_disconnect(runtime, &mut result) };
    let (status, _, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
    // SAFETY: The client is closed and the slot is exclusively owned.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

/// The canonical JSON payload object for `data` with `json/plain` encoding.
fn payload_json(data: &str) -> serde_json::Value {
    let base64 = |bytes: &[u8]| {
        serde_json::json!({
            "encoding": "base64",
            "data": base64::engine::general_purpose::STANDARD.encode(bytes),
        })
    };
    serde_json::json!({
        "metadata": {"encoding": base64(b"json/plain")},
        "data": base64(data.as_bytes()),
    })
}

/// A start document for `workflow_id` with `conflict` policy and an
/// optional RPC timeout.
fn start_document(
    workflow_id: &str,
    conflict: &str,
    rpc_timeout_ms: Option<u64>,
) -> serde_json::Value {
    let mut start = serde_json::json!({
        "request_id": format!("request-{workflow_id}"),
        "namespace": "default",
        "workflow_id": workflow_id,
        "workflow_type": "entity",
        "task_queue": "queue",
        "input": [payload_json("1")],
        "memo": [{"key": "m", "value": payload_json("2")}],
        "search_attributes": [],
        "id_conflict_policy": conflict,
        "id_reuse_policy": "allow_duplicate",
        "execution_timeout_ms": 60_000,
    });
    if let Some(timeout) = rpc_timeout_ms {
        start["rpc_timeout_ms"] = serde_json::json!(timeout);
    }
    start
}

/// A signal-with-start document for `workflow_id`.
fn signal_with_start(workflow_id: &str, conflict: &str, rpc_timeout_ms: Option<u64>) -> String {
    serde_json::json!({
        "start": start_document(workflow_id, conflict, rpc_timeout_ms),
        "signal_name": "deliver",
        "signal_input": [payload_json("\"hello\"")],
    })
    .to_string()
}

/// An update-with-start document for `workflow_id`.
fn update_with_start(workflow_id: &str, rpc_timeout_ms: Option<u64>) -> String {
    serde_json::json!({
        "start": start_document(workflow_id, "use_existing", rpc_timeout_ms),
        "update_id": format!("update-{workflow_id}"),
        "update_name": "apply",
        "update_input": [payload_json("3")],
    })
    .to_string()
}

/// The expected accepted start of `workflow_id` on `run`.
fn execution(workflow_id: &str, run: &str) -> serde_json::Value {
    serde_json::json!({"namespace": "default", "workflow_id": workflow_id, "run_id": run})
}

/// Signal-with-start sends every start option and the signal in one
/// `SignalWithStartWorkflowExecution` RPC and reports whether it started the
/// run, as a start outcome.
#[test]
fn signal_with_start_starts_or_signals() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let value = outcome(
        runtime,
        CALL_SIGNAL_WITH_START,
        &signal_with_start("fresh", "use_existing", Some(5_000)),
    );
    assert_eq!(
        value,
        serde_json::json!({"kind": "accepted", "execution": execution("fresh", NEW_RUN), "started": true})
    );
    let value = outcome(
        runtime,
        CALL_SIGNAL_WITH_START,
        &signal_with_start("existing", "use_existing", None),
    );
    assert_eq!(
        value,
        serde_json::json!({"kind": "accepted", "execution": execution("existing", EXISTING_RUN), "started": false})
    );
    let seen = observed.lock().expect("observed lock");
    let request = &seen.signal_with_starts[0];
    assert_eq!(request.namespace, "default");
    assert_eq!(request.request_id, "request-fresh");
    assert_eq!(request.signal_name, "deliver");
    assert_eq!(
        request
            .signal_input
            .as_ref()
            .map(|payloads| payloads.payloads[0].data.clone()),
        Some(b"\"hello\"".to_vec())
    );
    assert_eq!(
        request.input.as_ref().map(|input| input.payloads.len()),
        Some(1)
    );
    assert_eq!(
        request.workflow_type.as_ref().map(|t| t.name.as_str()),
        Some("entity")
    );
    assert_eq!(
        request.task_queue.as_ref().map(|q| q.name.as_str()),
        Some("queue")
    );
    assert_eq!(
        request.workflow_id_conflict_policy,
        WorkflowIdConflictPolicy::UseExisting as i32
    );
    assert!(
        request
            .memo
            .as_ref()
            .is_some_and(|memo| memo.fields.contains_key("m"))
    );
    assert_eq!(
        request
            .workflow_execution_timeout
            .as_ref()
            .map(|timeout| timeout.seconds),
        Some(60)
    );
    assert_eq!(request.identity, "with-start-test");
    drop(seen);
    close_runtime(runtime);
}

/// A rejected signal-with-start is a start rejection, and a transport
/// failure is an uncertain start.
#[test]
fn signal_with_start_failures_are_start_outcomes() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let value = outcome(
        runtime,
        CALL_SIGNAL_WITH_START,
        &signal_with_start("invalid", "terminate_existing", None),
    );
    assert_eq!(
        value,
        serde_json::json!({"kind": "rejected", "error": {"kind": "rpc", "code": "invalid_argument"}})
    );
    let value = outcome(
        runtime,
        CALL_SIGNAL_WITH_START,
        &signal_with_start("down", "use_existing", Some(300)),
    );
    assert_eq!(
        value,
        serde_json::json!({"kind": "unknown", "request_id": "request-down", "workflow_id": "down"})
    );
    let seen = observed.lock().expect("observed lock");
    assert_eq!(
        seen.signal_with_starts[0].workflow_id_conflict_policy,
        WorkflowIdConflictPolicy::TerminateExisting as i32
    );
    drop(seen);
    close_runtime(runtime);
}

/// Update-with-start sends `[StartWorkflow, UpdateWorkflow]` routed by the
/// workflow ID, with no run on the update, and reports the start beside the
/// accepted update.
#[test]
fn update_with_start_reports_start_and_update() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let value = outcome(
        runtime,
        CALL_UPDATE_WITH_START,
        &update_with_start("accept", Some(5_000)),
    );
    assert_eq!(
        value,
        serde_json::json!({
            "kind": "accepted",
            "execution": execution("accept", NEW_RUN),
            "started": true,
            "update": {
                "kind": "response",
                "update_id": "update-accept",
                "execution": execution("accept", NEW_RUN),
                "outcome": null,
            },
        })
    );
    let seen = observed.lock().expect("observed lock");
    let request = &seen.multi_operations[0];
    assert_eq!(request.namespace, "default");
    assert_eq!(request.resource_id, "accept");
    use execute_multi_operation_request::operation::Operation;
    match (
        request.operations[0].operation.as_ref(),
        request.operations[1].operation.as_ref(),
    ) {
        (Some(Operation::StartWorkflow(start)), Some(Operation::UpdateWorkflow(update))) => {
            assert_eq!(start.request_id, "request-accept");
            assert_eq!(
                start.workflow_id_conflict_policy,
                WorkflowIdConflictPolicy::UseExisting as i32
            );
            let target = update.workflow_execution.as_ref().expect("update target");
            assert_eq!(
                (target.workflow_id.as_str(), target.run_id.as_str()),
                ("accept", "")
            );
            let meta = update
                .request
                .as_ref()
                .and_then(|request| request.meta.as_ref())
                .expect("update meta");
            assert_eq!(meta.update_id, "update-accept");
            assert_eq!(
                update
                    .wait_policy
                    .as_ref()
                    .map(|policy| policy.lifecycle_stage),
                Some(UpdateWorkflowExecutionLifecycleStage::Accepted as i32)
            );
        }
        other => panic!("unexpected operations {other:?}"),
    }
    assert_eq!(request.operations.len(), 2);
    drop(seen);
    close_runtime(runtime);
}

/// An update that is only admitted is re-issued, identically, until it is
/// accepted; one that stays admitted ends with `deadline_exceeded` beside
/// the start that was applied.
#[test]
fn update_with_start_readmits_until_accepted() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let value = outcome(
        runtime,
        CALL_UPDATE_WITH_START,
        &update_with_start("admit-once", None),
    );
    assert_eq!(value["update"]["kind"], "response");
    assert_eq!(value["started"], true);
    let value = outcome(
        runtime,
        CALL_UPDATE_WITH_START,
        &update_with_start("admit-forever", Some(400)),
    );
    assert_eq!(
        value,
        serde_json::json!({
            "kind": "accepted",
            "execution": execution("admit-forever", NEW_RUN),
            "started": true,
            "update": {"kind": "error", "error": {"kind": "rpc", "code": "deadline_exceeded"}},
        })
    );
    let seen = observed.lock().expect("observed lock");
    let once: Vec<_> = seen
        .multi_operations
        .iter()
        .filter(|request| request.resource_id == "admit-once")
        .collect();
    assert_eq!(once.len(), 2);
    assert_eq!(once[0], once[1], "a re-issued attempt is identical");
    assert!(
        seen.multi_operations
            .iter()
            .filter(|request| request.resource_id == "admit-forever")
            .count()
            >= 2
    );
    drop(seen);
    close_runtime(runtime);
}

/// A validator rejection reports both the applied start and the rejection;
/// a start conflict is the typed already-started rejection naming the
/// existing run; a transport failure on the first attempt is uncertain.
#[test]
fn update_with_start_reports_rejections_and_conflicts() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let value = outcome(
        runtime,
        CALL_UPDATE_WITH_START,
        &update_with_start("reject", None),
    );
    assert_eq!(value["kind"], "accepted");
    assert_eq!(value["execution"], execution("reject", NEW_RUN));
    assert_eq!(value["update"]["kind"], "response");
    assert_eq!(value["update"]["outcome"]["kind"], "failed");
    assert_eq!(
        value["update"]["outcome"]["failure"]["message"],
        "rejected by validator"
    );
    let value = outcome(
        runtime,
        CALL_UPDATE_WITH_START,
        &update_with_start("conflict", None),
    );
    assert_eq!(
        value,
        serde_json::json!({
            "kind": "rejected",
            "error": {"kind": "already_started", "workflow_id": "conflict", "existing_run_id": EXISTING_RUN},
        })
    );
    let value = outcome(
        runtime,
        CALL_UPDATE_WITH_START,
        &update_with_start("down", Some(300)),
    );
    assert_eq!(
        value,
        serde_json::json!({"kind": "unknown", "request_id": "request-down", "workflow_id": "down"})
    );
    close_runtime(runtime);
}

/// A pending request ID may be shared only by the same logical operation of
/// the same kind: a plain start reusing a pending update-with-start's
/// request ID is refused at submission, while an identical retry is
/// admitted.
#[test]
fn pending_request_ids_are_not_shared_across_kinds() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let document = update_with_start("admit-forever", Some(2_000));
    let first = submit(runtime, CALL_UPDATE_WITH_START, &document).expect("first submission");
    let start = start_document("admit-forever", "use_existing", None).to_string();
    let refused = submit(runtime, CALL_START, &start).expect_err("start reusing the request ID");
    assert_eq!(refused.0, STATUS_PROTOCOL, "{}", refused.1);
    let retry = submit(runtime, CALL_UPDATE_WITH_START, &document).expect("identical retry");
    for call in [first, retry] {
        let (status, value, message) = await_call(call);
        assert_eq!(status, STATUS_OK, "{message}");
        let value: serde_json::Value = serde_json::from_slice(&value).expect("JSON outcome");
        assert_eq!(value["update"]["error"]["code"], "deadline_exceeded");
    }
    close_runtime(runtime);
}

/// Both documents are closed and validated, and signal-with-start refuses
/// the `fail` conflict policy: anything else fails its submission
/// synchronously, before any task or RPC exists.
#[test]
fn with_start_documents_validate_before_the_rpc() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let mut signal_documents = vec![signal_with_start("w", "fail", None)];
    let mut update_documents = Vec::new();
    for (field, value) in [
        ("signal_name", serde_json::json!("")),
        ("signal_input", serde_json::json!([{"bad": true}])),
        ("extra", serde_json::json!(1)),
    ] {
        let mut document: serde_json::Value =
            serde_json::from_str(&signal_with_start("w", "use_existing", None)).expect("JSON");
        document[field] = value;
        signal_documents.push(document.to_string());
    }
    for (field, value) in [
        ("update_id", serde_json::json!("")),
        ("update_name", serde_json::json!("a\u{0}")),
        ("update_input", serde_json::json!("not a list")),
        ("extra", serde_json::json!(1)),
    ] {
        let mut document: serde_json::Value =
            serde_json::from_str(&update_with_start("w", None)).expect("JSON");
        document[field] = value;
        update_documents.push(document.to_string());
    }
    let mut no_signal: serde_json::Value =
        serde_json::from_str(&signal_with_start("w", "use_existing", None)).expect("JSON");
    no_signal
        .as_object_mut()
        .expect("object")
        .remove("signal_name");
    signal_documents.push(no_signal.to_string());
    let mut bad_start: serde_json::Value =
        serde_json::from_str(&update_with_start("w", None)).expect("JSON");
    bad_start["start"]["rpc_timeout_ms"] = serde_json::json!(60_001);
    update_documents.push(bad_start.to_string());
    for document in &signal_documents {
        let refused = submit(runtime, CALL_SIGNAL_WITH_START, document).expect_err(document);
        assert_eq!(refused.0, STATUS_PROTOCOL, "{document}: {}", refused.1);
    }
    for document in &update_documents {
        let refused = submit(runtime, CALL_UPDATE_WITH_START, document).expect_err(document);
        assert_eq!(refused.0, STATUS_PROTOCOL, "{document}: {}", refused.1);
    }
    let seen = observed.lock().expect("observed lock");
    assert!(seen.signal_with_starts.is_empty() && seen.multi_operations.is_empty());
    drop(seen);
    close_runtime(runtime);
}

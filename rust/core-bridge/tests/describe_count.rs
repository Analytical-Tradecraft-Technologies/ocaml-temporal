//! Regression coverage for issue #1152: the submitted `DESCRIBE` and `COUNT`
//! client calls behind `Client.describe` and `Client.count`.
//!
//! A minimal plaintext HTTP/2 gRPC double stands in for the Temporal server,
//! so the tests are deterministic and run in every Rust test job. It answers
//! `DescribeWorkflowExecution` according to the requested workflow ID (see
//! [`describe_answer`]) and `CountWorkflowExecutions` according to the query
//! (see [`count_answer`]). Every other RPC succeeds with an empty message.

use std::collections::HashMap;
use std::io::Read as _;
use std::net::SocketAddr;
use std::ptr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_CONNECTION, STATUS_NOT_READY, STATUS_OK,
    STATUS_PROTOCOL, Status, ocaml_temporal_core_v4_client_await_call,
    ocaml_temporal_core_v4_client_connect_json, ocaml_temporal_core_v4_client_disconnect,
    ocaml_temporal_core_v4_client_submit_json, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new,
};
use prost::Message as _;
use prost::bytes::Bytes;
use temporalio_common::protos::temporal::api::{
    common::v1::{ActivityType, Memo, Payload, SearchAttributes, WorkflowExecution, WorkflowType},
    workflow::v1::{PendingActivityInfo, PendingChildExecutionInfo, WorkflowExecutionInfo},
    workflowservice::v1::{
        CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
        DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
    },
};

/// Selector of a submitted describe, as in the C header.
const CALL_DESCRIBE: u32 = 14;
/// Selector of a submitted count, as in the C header.
const CALL_COUNT: u32 = 15;

/// The closed failure of a Core conversion; its text never crosses the ABI.
const CORE_INVALID: &str = r#"{"kind":"protocol","code":"core_invalid"}"#;

/// The run the double resolves an empty run ID to.
const CURRENT_RUN: &str = "current-run";

/// Upper bound for one bridge call under test; a regression fails instead of
/// hanging CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// Requests the double received, in arrival order.
#[derive(Default)]
struct Seen {
    describes: Vec<DescribeWorkflowExecutionRequest>,
    counts: Vec<CountWorkflowExecutionsRequest>,
}

/// Shared record of what the double received.
type Observed = Arc<Mutex<Seen>>;

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

/// A payload with `data` and one `encoding` metadata entry.
fn payload(data: &str) -> Payload {
    Payload {
        metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
        data: data.as_bytes().to_vec(),
        ..Default::default()
    }
}

/// Answers one describe request by its workflow ID:
///
/// - `open`: a running execution with memo, a search attribute carrying the
///   server's `type` metadata, two pending activities (one in a state the
///   bridge does not name) and two pending children (one not yet started);
/// - `closed`: a completed execution with a close time and nothing pending;
/// - `missing`: `NOT_FOUND`;
/// - `other-run`: an execution whose run differs from the requested one;
/// - `unspecified`: an execution with status 0;
/// - anything else: a response with no execution record.
///
/// An empty requested run resolves to [`CURRENT_RUN`].
fn describe_answer(request: &DescribeWorkflowExecutionRequest) -> Result<Vec<u8>, &'static str> {
    let execution = request.execution.clone().unwrap_or_default();
    let run_id = if execution.run_id.is_empty() {
        CURRENT_RUN.to_owned()
    } else {
        execution.run_id.clone()
    };
    let info = |status: i32| WorkflowExecutionInfo {
        execution: Some(WorkflowExecution {
            workflow_id: execution.workflow_id.clone(),
            run_id: run_id.clone(),
        }),
        r#type: Some(WorkflowType {
            name: "fixture".to_owned(),
        }),
        start_time: Some(prost_wkt_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 5,
        }),
        status,
        history_length: 7,
        task_queue: "queue".to_owned(),
        ..Default::default()
    };
    let response = match execution.workflow_id.as_str() {
        "missing" => return Err("5"),
        "open" => {
            let mut search = payload("\"value\"");
            search
                .metadata
                .insert("type".to_owned(), b"Keyword".to_vec());
            DescribeWorkflowExecutionResponse {
                workflow_execution_info: Some(WorkflowExecutionInfo {
                    memo: Some(Memo {
                        fields: HashMap::from([
                            ("b".to_owned(), payload("2")),
                            ("a".to_owned(), payload("1")),
                        ]),
                    }),
                    search_attributes: Some(SearchAttributes {
                        indexed_fields: HashMap::from([("Key".to_owned(), search)]),
                    }),
                    ..info(1)
                }),
                pending_activities: vec![
                    PendingActivityInfo {
                        activity_id: "1".to_owned(),
                        activity_type: Some(ActivityType {
                            name: "charge".to_owned(),
                        }),
                        state: 2,
                        attempt: 3,
                        ..Default::default()
                    },
                    PendingActivityInfo {
                        activity_id: "2".to_owned(),
                        activity_type: Some(ActivityType {
                            name: "ship".to_owned(),
                        }),
                        state: 9,
                        attempt: 1,
                        ..Default::default()
                    },
                ],
                pending_children: vec![
                    PendingChildExecutionInfo {
                        workflow_id: "child-started".to_owned(),
                        run_id: "child-run".to_owned(),
                        workflow_type_name: "child".to_owned(),
                        ..Default::default()
                    },
                    PendingChildExecutionInfo {
                        workflow_id: "child-initiated".to_owned(),
                        run_id: String::new(),
                        workflow_type_name: "child".to_owned(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        }
        "closed" => DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(WorkflowExecutionInfo {
                close_time: Some(prost_wkt_types::Timestamp {
                    seconds: 1_700_000_060,
                    nanos: 999_999_999,
                }),
                ..info(2)
            }),
            ..Default::default()
        },
        "other-run" => DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(WorkflowExecutionInfo {
                execution: Some(WorkflowExecution {
                    workflow_id: execution.workflow_id.clone(),
                    run_id: "different".to_owned(),
                }),
                ..info(1)
            }),
            ..Default::default()
        },
        "unspecified" => DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(info(0)),
            ..Default::default()
        },
        _ => DescribeWorkflowExecutionResponse::default(),
    };
    Ok(response.encode_to_vec())
}

/// Answers one count request by its query: `invalid` is `INVALID_ARGUMENT`,
/// `negative` reports -1, and any other query reports 42.
fn count_answer(request: &CountWorkflowExecutionsRequest) -> Result<Vec<u8>, &'static str> {
    let count = match request.query.as_str() {
        "invalid" => return Err("3"),
        "negative" => -1,
        _ => 42,
    };
    Ok(CountWorkflowExecutionsResponse {
        count,
        ..Default::default()
    }
    .encode_to_vec())
}

/// Starts the gRPC double on a detached OS thread with its own
/// current-thread Tokio runtime and returns its loopback address. The thread
/// ends with the test process.
fn spawn_server(observed: Observed) -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-describe-double".to_owned())
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
    let outcome = if path.ends_with("/DescribeWorkflowExecution") {
        let request = DescribeWorkflowExecutionRequest::decode(received.as_slice())
            .expect("Core sends a valid describe request");
        let outcome = describe_answer(&request);
        observed
            .lock()
            .expect("observed lock")
            .describes
            .push(request);
        outcome
    } else if path.ends_with("/CountWorkflowExecutions") {
        let request = CountWorkflowExecutionsRequest::decode(received.as_slice())
            .expect("Core sends a valid count request");
        let outcome = count_answer(&request);
        observed.lock().expect("observed lock").counts.push(request);
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
        Err(code) => code,
    };
    let mut trailers = http::HeaderMap::new();
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
    let client = format!(r#"{{"target_url":"http://{address}","identity":"describe-test"}}"#);
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

/// Submits one call, awaits it from this thread, and returns its terminal
/// status, value, and diagnostic.
fn call(runtime: *mut Runtime, kind: u32, document: &str) -> (Status, Vec<u8>, String) {
    let (owner, call) = submit(runtime, kind, document).expect("submission");
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

/// Submits a describe that must succeed and returns its parsed document.
fn describe(runtime: *mut Runtime, document: &str) -> serde_json::Value {
    let (status, value, message) = call(runtime, CALL_DESCRIBE, document);
    assert_eq!(status, STATUS_OK, "{message}");
    serde_json::from_slice(&value).expect("describe answers JSON")
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

/// The canonical JSON payload object for `data` with the `encoding`
/// metadata of [`payload`] and the optional server `type` entry.
fn payload_json(data: &str, search_type: Option<&str>) -> serde_json::Value {
    let base64 = |bytes: &[u8]| {
        serde_json::json!({
            "encoding": "base64",
            "data": base64::engine::general_purpose::STANDARD.encode(bytes),
        })
    };
    let mut metadata = serde_json::Map::new();
    metadata.insert("encoding".to_owned(), base64(b"json/plain"));
    if let Some(search_type) = search_type {
        metadata.insert("type".to_owned(), base64(search_type.as_bytes()));
    }
    serde_json::json!({ "metadata": metadata, "data": base64(data.as_bytes()) })
}

/// A current-run describe of an open run reports the resolved run, the
/// routing and status fields, exact times and history length, memo and
/// search attributes sorted by key with the server's metadata intact, and
/// every pending activity and child, including a state the bridge does not
/// name and a child whose start is still being initiated.
#[test]
fn describe_reduces_an_open_run() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let value = describe(
        runtime,
        r#"{"namespace":"default","workflow_id":"open","run_id":"","rpc_timeout_ms":5000}"#,
    );
    let expected = serde_json::json!({
        "execution": {"namespace": "default", "workflow_id": "open", "run_id": CURRENT_RUN},
        "workflow_type": "fixture",
        "task_queue": "queue",
        "status": "running",
        "start_time": {"seconds": 1_700_000_000_i64, "nanoseconds": 5},
        "close_time": null,
        "history_length": 7,
        "memo": [
            {"key": "a", "value": payload_json("1", None)},
            {"key": "b", "value": payload_json("2", None)},
        ],
        "search_attributes": [
            {"key": "Key", "value": payload_json("\"value\"", Some("Keyword"))},
        ],
        "pending_activities": [
            {"activity_id": "1", "activity_type": "charge", "state": "started", "attempt": 3},
            {"activity_id": "2", "activity_type": "ship", "state": "unknown_9", "attempt": 1},
        ],
        "pending_children": [
            {"workflow_id": "child-started", "run_id": "child-run", "workflow_type": "child"},
            {"workflow_id": "child-initiated", "run_id": null, "workflow_type": "child"},
        ],
    });
    assert_eq!(value, expected);
    let seen = observed.lock().expect("observed lock");
    let request = &seen.describes[0];
    assert_eq!(request.namespace, "default");
    let execution = request.execution.as_ref().expect("execution");
    assert_eq!(
        (execution.workflow_id.as_str(), execution.run_id.as_str()),
        ("open", "")
    );
    drop(seen);
    close_runtime(runtime);
}

/// An exact-run describe of a closed run reports its close time and empty
/// collections.
#[test]
fn describe_reports_a_closed_run() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let value = describe(
        runtime,
        r#"{"namespace":"default","workflow_id":"closed","run_id":"exact"}"#,
    );
    assert_eq!(value["execution"]["run_id"], "exact");
    assert_eq!(value["status"], "completed");
    assert_eq!(
        value["close_time"],
        serde_json::json!({"seconds": 1_700_000_060_i64, "nanoseconds": 999_999_999})
    );
    for empty in [
        "memo",
        "search_attributes",
        "pending_activities",
        "pending_children",
    ] {
        assert_eq!(value[empty], serde_json::json!([]), "{empty}");
    }
    close_runtime(runtime);
}

/// An unknown run is the closed `rpc` failure document with the connection
/// status, and an answer naming another run, with an unspecified status, or
/// without an execution record is a protocol error.
#[test]
fn describe_reports_missing_and_malformed_runs() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let (status, _, message) = call(
        runtime,
        CALL_DESCRIBE,
        r#"{"namespace":"default","workflow_id":"missing","run_id":""}"#,
    );
    assert_eq!(status, STATUS_CONNECTION, "{message}");
    assert_eq!(message, r#"{"kind":"rpc","code":"not_found"}"#);
    for workflow_id in ["other-run", "unspecified", "empty"] {
        let document =
            format!(r#"{{"namespace":"default","workflow_id":"{workflow_id}","run_id":"asked"}}"#);
        let (status, _, message) = call(runtime, CALL_DESCRIBE, &document);
        assert_eq!(
            (status, message.as_str()),
            (STATUS_PROTOCOL, CORE_INVALID),
            "{workflow_id}"
        );
    }
    close_runtime(runtime);
}

/// A count sends the namespace and query unchanged and returns the server's
/// total; a server failure is the closed `rpc` document and a negative count
/// is a protocol error.
#[test]
fn count_returns_the_server_total() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let (status, value, message) = call(
        runtime,
        CALL_COUNT,
        r#"{"namespace":"default","query":"WorkflowType = 'fixture'","rpc_timeout_ms":5000}"#,
    );
    assert_eq!(status, STATUS_OK, "{message}");
    assert_eq!(value, br#"{"count":42}"#);
    let (status, value, message) =
        call(runtime, CALL_COUNT, r#"{"namespace":"default","query":""}"#);
    assert_eq!(status, STATUS_OK, "{message}");
    assert_eq!(value, br#"{"count":42}"#);
    let (status, _, message) = call(
        runtime,
        CALL_COUNT,
        r#"{"namespace":"default","query":"invalid"}"#,
    );
    assert_eq!(status, STATUS_CONNECTION, "{message}");
    assert_eq!(message, r#"{"kind":"rpc","code":"invalid_argument"}"#);
    let (status, _, message) = call(
        runtime,
        CALL_COUNT,
        r#"{"namespace":"default","query":"negative"}"#,
    );
    assert_eq!((status, message.as_str()), (STATUS_PROTOCOL, CORE_INVALID));
    let seen = observed.lock().expect("observed lock");
    let queries: Vec<_> = seen
        .counts
        .iter()
        .map(|request| (request.namespace.as_str(), request.query.as_str()))
        .collect();
    assert_eq!(
        queries,
        [
            ("default", "WorkflowType = 'fixture'"),
            ("default", ""),
            ("default", "invalid"),
            ("default", "negative"),
        ]
    );
    drop(seen);
    close_runtime(runtime);
}

/// Describe and count documents are closed and validated: anything else
/// fails its submission synchronously, before any task or RPC exists.
#[test]
fn describe_and_count_validate_before_the_rpc() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    for document in [
        r#"{"namespace":"default","workflow_id":"","run_id":""}"#,
        r#"{"namespace":"","workflow_id":"w","run_id":""}"#,
        "{\"namespace\":\"default\",\"workflow_id\":\"w\\u0000\",\"run_id\":\"\"}",
        r#"{"namespace":"default","workflow_id":"w","run_id":"","rpc_timeout_ms":0}"#,
        r#"{"namespace":"default","workflow_id":"w","run_id":"","rpc_timeout_ms":60001}"#,
        r#"{"namespace":"default","workflow_id":"w"}"#,
        r#"{"namespace":"default","workflow_id":"w","run_id":"","page_size":1}"#,
    ] {
        let refused = submit(runtime, CALL_DESCRIBE, document).expect_err(document);
        assert_eq!(refused.0, STATUS_PROTOCOL, "{document}: {}", refused.1);
    }
    for document in [
        r#"{"namespace":"","query":""}"#,
        r#"{"namespace":"default"}"#,
        "{\"namespace\":\"default\",\"query\":\"a\\u0000\"}",
        r#"{"namespace":"default","query":"","rpc_timeout_ms":0}"#,
        r#"{"namespace":"default","query":"","page_size":1}"#,
    ] {
        let refused = submit(runtime, CALL_COUNT, document).expect_err(document);
        assert_eq!(refused.0, STATUS_PROTOCOL, "{document}: {}", refused.1);
    }
    let seen = observed.lock().expect("observed lock");
    assert!(seen.describes.is_empty() && seen.counts.is_empty());
    drop(seen);
    close_runtime(runtime);
}

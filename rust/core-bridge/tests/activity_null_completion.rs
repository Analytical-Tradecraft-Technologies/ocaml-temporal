//! Regression coverage for a `null` successful activity payload (issue #954).
//!
//! The semantic activity protocol used to accept
//! `{"kind":"completed","result":null}` and convert it to Core's
//! `Success { result: None }`. Core's `validate_activity_completion` rejects
//! that as malformed, so the bridge returned `STATUS_WORKER` for a document it
//! had accepted. The decoder now refuses the document with `STATUS_PROTOCOL`
//! before any Core call, so the lease stays held and either a corrected
//! completion or the existing reject path retires it exactly once.
//!
//! The tests use the minimal plaintext HTTP/2 gRPC double pattern from
//! `worker_shutdown_outstanding.rs`. It hands out one activity task, counts
//! every activity completion and failure RPC, decodes the completion request,
//! accepts other RPCs with an empty success response, and holds further polls
//! open like an idle server long poll.

use std::io::Read;
use std::net::SocketAddr;
use std::ptr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_NOT_READY, STATUS_OK, STATUS_PROTOCOL, Status,
    ocaml_temporal_core_v4_client_connect_json, ocaml_temporal_core_v4_client_disconnect,
    ocaml_temporal_core_v4_result_free, ocaml_temporal_core_v4_runtime_free,
    ocaml_temporal_core_v4_runtime_new, ocaml_temporal_core_v4_worker_complete_activity_json,
    ocaml_temporal_core_v4_worker_reject_activity_json, ocaml_temporal_core_v4_worker_shutdown,
    ocaml_temporal_core_v4_worker_start_json, ocaml_temporal_core_v4_worker_try_poll_activity,
    ocaml_temporal_core_v4_worker_wait_activity,
};
use prost::Message;
use prost::bytes::{Bytes, BytesMut};
use temporalio_common::protos::temporal::api::{
    common::v1::{ActivityType, WorkflowExecution, WorkflowType},
    workflowservice::v1::{PollActivityTaskQueueResponse, RespondActivityTaskCompletedRequest},
};

/// Upper bound for one bridge call under test, so a regression fails instead
/// of hanging CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// Opaque token of the single activity task the double hands out.
const TASK_TOKEN: &[u8] = b"issue-954-activity-task";

/// Canonical base64 of [`TASK_TOKEN`], as it appears in completion documents.
const TASK_TOKEN_BASE64: &str = "aXNzdWUtOTU0LWFjdGl2aXR5LXRhc2s=";

/// Requests observed by the gRPC double, shared with the test thread.
#[derive(Default)]
struct Observed {
    /// Number of `PollActivityTaskQueue` requests received, including held ones.
    activity_polls: Mutex<usize>,
    /// Decoded `RespondActivityTaskCompleted` requests, in arrival order.
    completions: Mutex<Vec<RespondActivityTaskCompletedRequest>>,
    /// Number of `RespondActivityTaskFailed` requests received.
    failures: Mutex<usize>,
}

impl Observed {
    /// Returns a snapshot of the completion requests received so far.
    fn completions(&self) -> Vec<RespondActivityTaskCompletedRequest> {
        self.completions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Returns the number of failure requests received so far.
    fn failures(&self) -> usize {
        *self
            .failures
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

/// Starts a background gRPC double and returns its loopback address.
///
/// The server runs on its own detached OS thread with a private
/// current-thread Tokio runtime, so it never shares an executor with the
/// bridge under test and simply ends with the test process.
fn spawn_single_activity_server(observed: Arc<Observed>) -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-null-completion-double".to_owned())
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
                    .expect("publish test server address");
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
        .expect("test server publishes its address")
}

/// Encodes the activity task returned by the first activity poll.
///
/// No timeouts are set, so Core starts no local timeout timer that could
/// complete or cancel the task independently of the calls under test.
fn activity_task_message() -> Vec<u8> {
    PollActivityTaskQueueResponse {
        task_token: TASK_TOKEN.to_vec(),
        workflow_namespace: "null-completion-test".to_owned(),
        workflow_type: Some(WorkflowType {
            name: "null-completion-workflow".to_owned(),
        }),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: "null-completion-workflow-id".to_owned(),
            run_id: "null-completion-run-id".to_owned(),
        }),
        activity_type: Some(ActivityType {
            name: "null-completion-activity".to_owned(),
        }),
        activity_id: "null-completion-activity-id".to_owned(),
        attempt: 1,
        ..Default::default()
    }
    .encode_to_vec()
}

/// Reads one complete unary gRPC request body and returns the protobuf
/// message after the five-byte length prefix, decompressing a gzip message.
async fn read_unary_message(mut body: h2::RecvStream) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
        buffer.extend_from_slice(&chunk);
    }
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

/// Serves one HTTP/2 connection until the client closes it.
///
/// Each request is handled on its own task because reading a request body
/// needs the connection future to keep being polled by this loop.
async fn serve_connection(socket: tokio::net::TcpStream, observed: Arc<Observed>) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((request, respond))) = connection.accept().await {
        tokio::spawn(serve_request(request, respond, Arc::clone(&observed)));
    }
}

/// Answers one gRPC request, recording activity completions and failures.
///
/// A held poll never completes, like an idle server long poll; Core cancels
/// that stream when it stops polling.
async fn serve_request(
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    observed: Arc<Observed>,
) {
    let path = request.uri().path().to_owned();
    let message = if path.ends_with("/PollActivityTaskQueue") {
        let first = {
            let mut polls = observed
                .activity_polls
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *polls += 1;
            *polls == 1
        };
        if !first {
            std::future::pending::<()>().await;
            return;
        }
        activity_task_message()
    } else if path.ends_with("/PollWorkflowTaskQueue") {
        std::future::pending::<()>().await;
        return;
    } else {
        if path.ends_with("/RespondActivityTaskCompleted") {
            let bytes = read_unary_message(request.into_body()).await;
            let completed = RespondActivityTaskCompletedRequest::decode(bytes.as_slice())
                .expect("Core sends a valid RespondActivityTaskCompleted request");
            observed
                .completions
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(completed);
        } else if path.ends_with("/RespondActivityTaskFailed") {
            *observed
                .failures
                .lock()
                .unwrap_or_else(|error| error.into_inner()) += 1;
        }
        // The default instance of every response message encodes to zero
        // bytes, so one empty frame is a valid success for any method.
        Vec::new()
    };
    let response = http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(())
        .expect("static gRPC response");
    let Ok(mut stream) = respond.send_response(response, false) else {
        return;
    };
    // gRPC length-prefixed message: uncompressed flag, big-endian length.
    let length = u32::try_from(message.len()).expect("small test message");
    let mut frame = Vec::with_capacity(5 + message.len());
    frame.push(0);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&message);
    if stream.send_data(Bytes::from(frame), false).is_err() {
        return;
    }
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
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

/// Outcome of one ABI call after its result has been released.
struct Outcome {
    /// Status returned by the call.
    status: Status,
    /// Copy of the success value buffer.
    value: Vec<u8>,
    /// Diagnostic text from the error buffer.
    message: String,
}

/// Frees a live result and returns copies of its status and buffers.
fn release(mut result: AbiResult) -> Outcome {
    let outcome = Outcome {
        status: result.status,
        value: bytes(&result.value),
        message: String::from_utf8(bytes(&result.error)).expect("diagnostic is UTF-8"),
    };
    // SAFETY: This helper has exclusive ownership of the initialized result.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    outcome
}

/// Raw runtime pointer that may move to the bounded call thread.
///
/// The test thread never touches the runtime while the helper thread owns the
/// call, matching the ABI rule that one caller owns a runtime at a time.
struct RuntimePtr(*mut Runtime);

// SAFETY: Ownership of the runtime pointer is transferred to exactly one
// thread at a time and returned through a channel before reuse.
unsafe impl Send for RuntimePtr {}

/// Signature shared by the runtime-only worker ABI entry points under test.
type RuntimeCall = unsafe extern "C" fn(*mut Runtime, *mut AbiResult) -> Status;

/// Signature shared by the worker ABI entry points that take one document.
type DocumentCall = unsafe extern "C" fn(*mut Runtime, *const u8, usize, *mut AbiResult) -> Status;

/// One ABI call to run against the runtime.
enum Call {
    /// An entry point that takes only the runtime.
    Runtime(RuntimeCall),
    /// An entry point that also takes one owned input document.
    Document(DocumentCall, Vec<u8>),
}

/// Runs one ABI call on a helper thread and fails the test if it does not
/// return within [`CALL_DEADLINE`]. Returns the runtime with the call's
/// outcome.
///
/// On timeout the helper thread is intentionally leaked with the runtime: the
/// bridge is wedged inside Core, so neither releasing nor joining it is safe.
fn call_bounded(runtime: *mut Runtime, call: Call, what: &str) -> (*mut Runtime, Outcome) {
    let (sender, receiver) = mpsc::channel();
    let owned = RuntimePtr(runtime);
    thread::spawn(move || {
        let owned = owned;
        let mut result = AbiResult::default();
        // SAFETY: This thread exclusively owns the live runtime for the call,
        // the document stays readable for the whole call, and the output
        // location is unique.
        unsafe {
            match &call {
                Call::Runtime(call) => call(owned.0, &mut result),
                Call::Document(call, input) => {
                    call(owned.0, input.as_ptr(), input.len(), &mut result)
                }
            };
        }
        let _ = sender.send((owned, release(result)));
    });
    let (owned, outcome) = receiver
        .recv_timeout(CALL_DEADLINE)
        .unwrap_or_else(|_| panic!("{what} must return in bounded time"));
    (owned.0, outcome)
}

/// Activity-only worker document. Disabling workflows keeps the scenario to
/// the single activity lane.
const ACTIVITY_WORKER: &[u8] = br#"{
    "namespace":"null-completion-test",
    "task_queue":"null-completion-test",
    "build_id":"null-completion-build",
    "versioning":{"kind":"none"},
    "max_cached_workflows":10,
    "max_outstanding_workflow_tasks":10,
    "max_concurrent_workflow_task_polls":2,
    "graceful_shutdown_timeout_ms":1000,
    "task_types":{"workflows":false,"activities":true}
}"#;

/// Creates a runtime, connects it to the double, starts the activity worker,
/// waits for the task, and leases it. Returns the exclusively owned runtime
/// and the leased task document the reject path expects back unchanged.
fn start_worker_with_lease(address: SocketAddr) -> (*mut Runtime, Vec<u8>) {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).status, STATUS_OK);

    let client =
        format!(r#"{{"target_url":"http://{address}","identity":"null-completion-test"}}"#);
    let (runtime, outcome) = call_bounded(
        runtime,
        Call::Document(
            ocaml_temporal_core_v4_client_connect_json,
            client.into_bytes(),
        ),
        "client connect",
    );
    assert_eq!(outcome.status, STATUS_OK, "{}", outcome.message);
    let (mut runtime, outcome) = call_bounded(
        runtime,
        Call::Document(
            ocaml_temporal_core_v4_worker_start_json,
            ACTIVITY_WORKER.to_vec(),
        ),
        "worker start",
    );
    assert_eq!(outcome.status, STATUS_OK, "{}", outcome.message);

    let deadline = Instant::now() + CALL_DEADLINE;
    loop {
        let (returned, outcome) = call_bounded(
            runtime,
            Call::Runtime(ocaml_temporal_core_v4_worker_wait_activity),
            "activity readiness wait",
        );
        runtime = returned;
        match outcome.status {
            STATUS_OK => break,
            STATUS_NOT_READY if Instant::now() < deadline => {}
            status => panic!(
                "activity task never became ready: {status} {}",
                outcome.message
            ),
        }
    }
    let (runtime, outcome) = call_bounded(
        runtime,
        Call::Runtime(ocaml_temporal_core_v4_worker_try_poll_activity),
        "activity lease",
    );
    assert_eq!(outcome.status, STATUS_OK, "{}", outcome.message);
    assert!(
        !outcome.value.is_empty(),
        "the lease carries a task document"
    );
    (runtime, outcome.value)
}

/// Submits the issue's `null` success payload and asserts it is refused as a
/// protocol error before any completion reaches the server.
fn submit_null_completion(runtime: *mut Runtime, observed: &Observed) -> *mut Runtime {
    let document = format!(
        r#"{{"task_token":"{TASK_TOKEN_BASE64}","result":{{"kind":"completed","result":null}}}}"#
    );
    let (runtime, outcome) = call_bounded(
        runtime,
        Call::Document(
            ocaml_temporal_core_v4_worker_complete_activity_json,
            document.into_bytes(),
        ),
        "null activity completion",
    );
    assert_eq!(outcome.status, STATUS_PROTOCOL, "{}", outcome.message);
    assert!(observed.completions().is_empty());
    assert_eq!(observed.failures(), 0);
    runtime
}

/// Shuts the worker down, asserting that no lease is outstanding, then
/// disconnects the client and frees the runtime.
fn shutdown_without_outstanding_lease(runtime: *mut Runtime) {
    let (mut runtime, outcome) = call_bounded(
        runtime,
        Call::Runtime(ocaml_temporal_core_v4_worker_shutdown),
        "worker shutdown",
    );
    assert_eq!(outcome.status, STATUS_OK, "{}", outcome.message);

    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread.
    unsafe { ocaml_temporal_core_v4_client_disconnect(runtime, &mut result) };
    let outcome = release(result);
    assert_eq!(outcome.status, STATUS_OK, "{}", outcome.message);
    // SAFETY: Children are closed and the slot is exclusively owned.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

/// A refused `null` payload leaves the lease held, so a corrected completion
/// with an empty void payload retires it with exactly one server completion
/// that carries a result.
#[test]
fn null_payload_is_refused_and_corrected_completion_retires_lease() {
    let observed = Arc::new(Observed::default());
    let address = spawn_single_activity_server(Arc::clone(&observed));
    let (runtime, _task) = start_worker_with_lease(address);
    let runtime = submit_null_completion(runtime, &observed);

    let document = format!(
        r#"{{"task_token":"{TASK_TOKEN_BASE64}","result":{{"kind":"completed","result":{{"metadata":{{}},"data":{{"encoding":"base64","data":""}}}}}}}}"#
    );
    let (runtime, outcome) = call_bounded(
        runtime,
        Call::Document(
            ocaml_temporal_core_v4_worker_complete_activity_json,
            document.into_bytes(),
        ),
        "void activity completion",
    );
    assert_eq!(outcome.status, STATUS_OK, "{}", outcome.message);
    let completions = observed.completions();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].task_token, TASK_TOKEN);
    let result = completions[0]
        .result
        .as_ref()
        .expect("the server receives a result collection");
    assert_eq!(result.payloads.len(), 1);
    assert!(result.payloads[0].data.is_empty());
    assert_eq!(observed.failures(), 0);

    shutdown_without_outstanding_lease(runtime);
}

/// A refused `null` payload leaves the lease in a state the existing reject
/// path retires with exactly one failure and no completion.
#[test]
fn null_payload_is_refused_and_reject_path_retires_lease() {
    let observed = Arc::new(Observed::default());
    let address = spawn_single_activity_server(Arc::clone(&observed));
    let (runtime, task) = start_worker_with_lease(address);
    let runtime = submit_null_completion(runtime, &observed);

    let (runtime, outcome) = call_bounded(
        runtime,
        Call::Document(ocaml_temporal_core_v4_worker_reject_activity_json, task),
        "activity rejection",
    );
    assert_eq!(outcome.status, STATUS_OK, "{}", outcome.message);
    assert!(observed.completions().is_empty());
    assert_eq!(observed.failures(), 1);

    shutdown_without_outstanding_lease(runtime);
}

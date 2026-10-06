//! Regression coverage for one unrepresentable activity task (issue #801).
//!
//! A remote activity task the bridge cannot represent to OCaml used to be
//! failed back to Core and then reported as a fatal protocol status, which
//! ended `Worker.run` for every workflow and activity on the task queue. The
//! failure was also retryable, so the server redelivered the same task and
//! stopped the worker again after every restart. The bridge must instead fail
//! only that task, non-retryably and with a bounded diagnostic, release its
//! activity slot, and keep polling.
//!
//! The tests use a minimal plaintext HTTP/2 gRPC double instead of a Temporal
//! server, following `worker_shutdown_outstanding.rs`. The double hands out a
//! fixed sequence of activity tasks, one per poll, records every
//! `RespondActivityTaskFailed` request, accepts other RPCs with an empty
//! success response, and holds further polls open like an idle long poll.

use std::collections::HashMap;
use std::io::Read;
use std::net::SocketAddr;
use std::ptr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_NOT_READY, STATUS_OK, STATUS_OUTSTANDING_TASKS,
    Status, ocaml_temporal_core_v3_client_connect_json, ocaml_temporal_core_v3_client_disconnect,
    ocaml_temporal_core_v3_result_free, ocaml_temporal_core_v3_runtime_free,
    ocaml_temporal_core_v3_runtime_new, ocaml_temporal_core_v3_worker_shutdown,
    ocaml_temporal_core_v3_worker_start_json, ocaml_temporal_core_v3_worker_try_poll_activity,
    ocaml_temporal_core_v3_worker_wait_activity,
    worker_bridge::{
        ActivityAdmission, Admission, AdmitError, TaskLedger,
        UNREPRESENTABLE_ACTIVITY_TASK_FAILURE_TYPE, is_ignorable_activity_cancellation,
        unrepresentable_activity_failure,
    },
};
use prost::Message;
use prost::bytes::{Bytes, BytesMut};
use temporalio_common::protos::temporal::api::{
    common::v1::{ActivityType, Header, Payload, WorkflowExecution, WorkflowType},
    failure::v1::failure::FailureInfo,
    workflowservice::v1::{PollActivityTaskQueueResponse, RespondActivityTaskFailedRequest},
};

/// Upper bound for one bridge call under test, so a regression fails instead
/// of hanging CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// Opaque token of the unrepresentable task. The failure must never echo it.
const REJECTED_TOKEN: &[u8] = b"issue-801-private-rejected-token";

/// Opaque token of the ordinary task that follows the rejected one.
const VALID_TOKEN: &[u8] = b"issue-801-valid-token";

/// Requests observed by the gRPC double, shared with the test thread.
#[derive(Default)]
struct Observed {
    /// Number of `PollActivityTaskQueue` requests received, including held ones.
    activity_polls: Mutex<usize>,
    /// Decoded `RespondActivityTaskFailed` requests, in arrival order.
    failures: Mutex<Vec<RespondActivityTaskFailedRequest>>,
}

impl Observed {
    /// Returns a snapshot of the failure requests received so far.
    fn failures(&self) -> Vec<RespondActivityTaskFailedRequest> {
        self.failures
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

/// Starts a background gRPC double that answers the `n`th activity poll with
/// `tasks[n]` and holds later polls open. Returns its loopback address.
///
/// The server runs on its own detached OS thread with a private
/// current-thread Tokio runtime, so it never shares an executor with the
/// bridge under test and ends with the test process.
fn spawn_activity_server(
    tasks: Vec<PollActivityTaskQueueResponse>,
    observed: Arc<Observed>,
) -> SocketAddr {
    let tasks = Arc::new(tasks);
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-activity-rejection-double".to_owned())
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
                    tokio::spawn(serve_connection(
                        socket,
                        Arc::clone(&tasks),
                        Arc::clone(&observed),
                    ));
                }
            });
        })
        .expect("spawn test server thread");
    address_receiver
        .recv_timeout(CALL_DEADLINE)
        .expect("test server publishes its address")
}

/// Reads one complete unary gRPC request body and returns the protobuf
/// message after the five-byte length prefix.
///
/// Core's client gzip-compresses requests by default, which the prefix's
/// first byte reports; such a message is decompressed here.
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
async fn serve_connection(
    socket: tokio::net::TcpStream,
    tasks: Arc<Vec<PollActivityTaskQueueResponse>>,
    observed: Arc<Observed>,
) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((request, respond))) = connection.accept().await {
        tokio::spawn(serve_request(
            request,
            respond,
            Arc::clone(&tasks),
            Arc::clone(&observed),
        ));
    }
}

/// Answers one gRPC request.
///
/// A held poll keeps its response handle alive by never completing, like an
/// idle server long poll; Core cancels that stream when it stops polling.
async fn serve_request(
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    tasks: Arc<Vec<PollActivityTaskQueueResponse>>,
    observed: Arc<Observed>,
) {
    let path = request.uri().path().to_owned();
    let message = if path.ends_with("/PollActivityTaskQueue") {
        let index = {
            let mut polls = observed
                .activity_polls
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *polls += 1;
            *polls - 1
        };
        match tasks.get(index) {
            Some(task) => task.encode_to_vec(),
            None => {
                std::future::pending::<()>().await;
                return;
            }
        }
    } else if path.ends_with("/PollWorkflowTaskQueue") {
        std::future::pending::<()>().await;
        return;
    } else {
        if path.ends_with("/RespondActivityTaskFailed") {
            let bytes = read_unary_message(request.into_body()).await;
            let failed = RespondActivityTaskFailedRequest::decode(bytes.as_slice())
                .expect("Core sends a valid RespondActivityTaskFailed request");
            observed
                .failures
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(failed);
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

/// An ordinary workflow-scheduled activity task the bridge can represent.
///
/// No timeouts are set, so Core starts no local timeout timer that could
/// complete or cancel the task independently of the scenario under test.
fn valid_task(task_token: &[u8]) -> PollActivityTaskQueueResponse {
    PollActivityTaskQueueResponse {
        task_token: task_token.to_vec(),
        workflow_namespace: "rejection-test".to_owned(),
        workflow_type: Some(WorkflowType {
            name: "rejection-test-workflow".to_owned(),
        }),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: "rejection-test-workflow-id".to_owned(),
            run_id: "rejection-test-run-id".to_owned(),
        }),
        activity_type: Some(ActivityType {
            name: "rejection-test-activity".to_owned(),
        }),
        activity_id: "rejection-test-activity-id".to_owned(),
        attempt: 1,
        ..Default::default()
    }
}

/// A standalone activity task: Core fills the absent workflow namespace,
/// type, and execution with empty strings, which the semantic protocol cannot
/// represent.
fn standalone_task() -> PollActivityTaskQueueResponse {
    PollActivityTaskQueueResponse {
        task_token: REJECTED_TOKEN.to_vec(),
        activity_type: Some(ActivityType {
            name: "standalone-activity".to_owned(),
        }),
        activity_id: "standalone-activity-id".to_owned(),
        activity_run_id: "standalone-activity-run-id".to_owned(),
        attempt: 1,
        ..Default::default()
    }
}

/// A workflow-scheduled task carrying an empty header key, which another SDK
/// may allow but the semantic protocol rejects.
fn empty_header_key_task() -> PollActivityTaskQueueResponse {
    PollActivityTaskQueueResponse {
        header: Some(Header {
            fields: HashMap::from([(String::new(), Payload::default())]),
        }),
        ..valid_task(REJECTED_TOKEN)
    }
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

/// Frees a live result and returns its status and diagnostic text.
fn release(mut result: AbiResult) -> (Status, String) {
    let status = result.status;
    let message = String::from_utf8(bytes(&result.error)).expect("diagnostic is UTF-8");
    // SAFETY: This helper has exclusive ownership of the initialized result.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_result_free(&mut result) },
        STATUS_OK
    );
    (status, message)
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

/// Runs one runtime-only ABI call on a helper thread and fails the test if it
/// does not return within [`CALL_DEADLINE`]. Returns the runtime with the
/// call's status and diagnostic.
///
/// On timeout the helper thread is intentionally leaked with the runtime: the
/// bridge is wedged inside Core, so neither releasing nor joining it is safe.
fn call_bounded(
    runtime: *mut Runtime,
    call: RuntimeCall,
    what: &str,
) -> (*mut Runtime, Status, String) {
    let (sender, receiver) = mpsc::channel();
    let owned = RuntimePtr(runtime);
    thread::spawn(move || {
        let owned = owned;
        let mut result = AbiResult::default();
        // SAFETY: This thread exclusively owns the live runtime for the call
        // and the output location is unique.
        unsafe { call(owned.0, &mut result) };
        let (status, message) = release(result);
        let _ = sender.send((owned, status, message));
    });
    let (owned, status, message) = receiver
        .recv_timeout(CALL_DEADLINE)
        .unwrap_or_else(|_| panic!("{what} must return in bounded time"));
    (owned.0, status, message)
}

/// Activity-only worker document. The bridge enables exactly one activity
/// slot, so Core polls for the second task only after the rejected task's
/// completion released the first slot.
const ACTIVITY_WORKER: &[u8] = br#"{
    "namespace":"rejection-test",
    "task_queue":"activity-rejection-test",
    "build_id":"activity-rejection-build",
    "versioning":{"kind":"none"},
    "max_cached_workflows":10,
    "max_outstanding_workflow_tasks":10,
    "max_concurrent_workflow_task_polls":2,
    "graceful_shutdown_timeout_ms":1000,
    "task_types":{"workflows":false,"activities":true}
}"#;

/// Creates a runtime, connects it to the double, and starts the activity
/// worker. Returns the exclusively owned runtime.
fn start_worker(address: SocketAddr) -> *mut Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v3_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);

    let client = format!(r#"{{"target_url":"http://{address}","identity":"rejection-test"}}"#);
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned; the input remains
    // readable for the full blocking call.
    unsafe {
        ocaml_temporal_core_v3_client_connect_json(
            runtime,
            client.as_ptr(),
            client.len(),
            &mut result,
        );
    }
    let (status, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");

    let mut result = AbiResult::default();
    // SAFETY: Same exclusive runtime ownership; the static document stays
    // readable for the call.
    unsafe {
        ocaml_temporal_core_v3_worker_start_json(
            runtime,
            ACTIVITY_WORKER.as_ptr(),
            ACTIVITY_WORKER.len(),
            &mut result,
        );
    }
    let (status, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
    runtime
}

/// Waits until an activity task is queued in the bridge's ready handoff,
/// without taking it. Each readiness wait is itself bounded by the bridge.
fn wait_for_ready_activity(mut runtime: *mut Runtime) -> *mut Runtime {
    let deadline = Instant::now() + CALL_DEADLINE;
    loop {
        let (returned, status, message) = call_bounded(
            runtime,
            ocaml_temporal_core_v3_worker_wait_activity,
            "activity readiness wait",
        );
        runtime = returned;
        match status {
            STATUS_OK => return runtime,
            STATUS_NOT_READY if Instant::now() < deadline => {}
            _ => panic!("activity task never became ready: {status} {message}"),
        }
    }
}

/// Disconnects the client and frees the runtime, asserting both succeed.
fn close_runtime(mut runtime: *mut Runtime) {
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread.
    unsafe { ocaml_temporal_core_v3_client_disconnect(runtime, &mut result) };
    let (status, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
    // SAFETY: Children are closed and the slot is exclusively owned.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

/// Drives the issue #801 scenario for one unrepresentable first task.
///
/// The poll that takes the unrepresentable task must report an empty lane,
/// not a fatal status, after exactly one non-retryable, typed, bounded
/// failure reached the server for that token. The worker must then receive
/// and deliver the next, ordinary task, which proves both that polling
/// continued and that the rejected task's only activity slot was released.
/// Shutdown afterwards force-completes the abandoned second lease and still
/// finalizes the worker.
fn rejected_task_keeps_worker_polling(rejected: PollActivityTaskQueueResponse) {
    let observed = Arc::new(Observed::default());
    let address = spawn_activity_server(
        vec![rejected, valid_task(VALID_TOKEN)],
        Arc::clone(&observed),
    );
    let runtime = wait_for_ready_activity(start_worker(address));

    let (runtime, status, message) = call_bounded(
        runtime,
        ocaml_temporal_core_v3_worker_try_poll_activity,
        "poll of an unrepresentable activity task",
    );
    assert_eq!(status, STATUS_NOT_READY, "{message}");

    let failures = observed.failures();
    assert_eq!(failures.len(), 1, "exactly one generated task failure");
    let failed = &failures[0];
    assert_eq!(failed.task_token, REJECTED_TOKEN);
    let failure = failed.failure.as_ref().expect("failure is reported");
    assert!(
        failure
            .message
            .starts_with("OCaml bridge could not represent the activity task: "),
        "{}",
        failure.message
    );
    assert!(failure.message.len() < 256, "{}", failure.message);
    assert!(
        !failure.message.contains("issue-801"),
        "{}",
        failure.message
    );
    match &failure.failure_info {
        Some(FailureInfo::ApplicationFailureInfo(info)) => {
            assert!(info.non_retryable, "the rejection must not be redelivered");
            assert_eq!(info.r#type, "UnrepresentableActivityTask");
        }
        other => panic!("expected an application failure, got {other:?}"),
    }

    let runtime = wait_for_ready_activity(runtime);
    let (runtime, status, message) = call_bounded(
        runtime,
        ocaml_temporal_core_v3_worker_try_poll_activity,
        "poll of the next ordinary activity task",
    );
    assert_eq!(status, STATUS_OK, "{message}");

    let (runtime, status, message) = call_bounded(
        runtime,
        ocaml_temporal_core_v3_worker_shutdown,
        "worker shutdown after a rejected task",
    );
    assert_eq!(status, STATUS_OUTSTANDING_TASKS, "{message}");
    let failures = observed.failures();
    assert_eq!(
        failures.len(),
        2,
        "shutdown failed only the abandoned lease"
    );
    assert_eq!(failures[1].task_token, VALID_TOKEN);
    close_runtime(runtime);
}

/// A standalone activity (no workflow execution) is failed alone and the
/// worker keeps serving the queue.
#[test]
fn standalone_activity_task_is_failed_without_stopping_the_worker() {
    rejected_task_keeps_worker_polling(standalone_task());
}

/// A task with an empty header key from another SDK is failed alone and the
/// worker keeps serving the queue.
#[test]
fn empty_header_key_task_is_failed_without_stopping_the_worker() {
    rejected_task_keeps_worker_polling(empty_header_key_task());
}

/// The orphaned-cancellation race from issue #801: a Cancel whose Start
/// completed between Core's poll returning and the lane's admission is
/// unknown to the ledger. That Cancel, like a repeated or retired one, owns
/// no completion debt, so the poll lane drops it silently instead of
/// publishing a lane error that would end `Worker.run`. Only a Cancel that
/// updates a live Start is delivered, and no Start outcome is ever ignored.
#[test]
fn orphaned_or_repeated_cancellation_is_dropped_without_a_lane_error() {
    let mut ledger = TaskLedger::new();
    let token = b"issue-801-cancel-token";

    let orphaned = ledger.admit_polled_activity(token, ActivityAdmission::Cancel);
    assert_eq!(orphaned, Err(AdmitError::UnknownActivityCancellation));
    assert!(is_ignorable_activity_cancellation(
        ActivityAdmission::Cancel,
        &orphaned
    ));

    let start = ledger.admit_polled_activity(token, ActivityAdmission::Start);
    assert_eq!(start, Ok(Admission::New));
    assert!(!is_ignorable_activity_cancellation(
        ActivityAdmission::Start,
        &start
    ));

    let first_cancel = ledger.admit_polled_activity(token, ActivityAdmission::Cancel);
    assert_eq!(first_cancel, Ok(Admission::ExistingCancellation));
    assert!(!is_ignorable_activity_cancellation(
        ActivityAdmission::Cancel,
        &first_cancel
    ));

    let repeated_cancel = ledger.admit_polled_activity(token, ActivityAdmission::Cancel);
    assert_eq!(repeated_cancel, Ok(Admission::Duplicate));
    assert!(is_ignorable_activity_cancellation(
        ActivityAdmission::Cancel,
        &repeated_cancel
    ));

    for error in [
        AdmitError::Draining,
        AdmitError::InvalidIdentity,
        AdmitError::Retired,
    ] {
        assert!(is_ignorable_activity_cancellation(
            ActivityAdmission::Cancel,
            &Err(error)
        ));
        assert!(!is_ignorable_activity_cancellation(
            ActivityAdmission::Start,
            &Err(error)
        ));
    }
    assert!(!is_ignorable_activity_cancellation(
        ActivityAdmission::Start,
        &Ok(Admission::Duplicate)
    ));
    assert_eq!(ledger.outstanding_activities(), 1);
}

/// The generated rejection failure is a typed, non-retryable application
/// failure whose message is built only from the static reason.
#[test]
fn rejection_failure_is_typed_non_retryable_and_bounded() {
    let failure = unrepresentable_activity_failure("activity task header key is empty");
    assert_eq!(
        failure.message,
        "OCaml bridge could not represent the activity task: \
         activity task header key is empty"
    );
    assert!(failure.cause.is_none());
    match failure.failure_info {
        Some(FailureInfo::ApplicationFailureInfo(info)) => {
            assert!(info.non_retryable);
            assert_eq!(info.r#type, UNREPRESENTABLE_ACTIVITY_TASK_FAILURE_TYPE);
            assert!(info.details.is_none());
        }
        other => panic!("expected an application failure, got {other:?}"),
    }
}

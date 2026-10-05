//! Regression coverage for live worker shutdown with outstanding Core tasks
//! (issue #769).
//!
//! Core's poll APIs report `ShutDown` only after every task they produced has
//! been completed. Worker shutdown used to join both poll lanes without
//! completing anything, so a task queued in the bridge's ready handoff or
//! leased to OCaml and never completed made `Worker.shutdown` hang forever.
//! The bridge must retire those debts itself, join the lanes, finalize Core,
//! and return in bounded time.
//!
//! The tests use a minimal plaintext HTTP/2 gRPC double instead of a Temporal
//! server. It hands out exactly one activity task, accepts every other RPC
//! with an empty success response, and holds further polls open the way a
//! server long poll does, so the tests are deterministic and run in every
//! Rust test job.

use std::net::SocketAddr;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_NOT_READY, STATUS_OK, STATUS_OUTSTANDING_TASKS,
    Status, ocaml_temporal_core_v2_client_connect_json, ocaml_temporal_core_v2_client_disconnect,
    ocaml_temporal_core_v2_result_free, ocaml_temporal_core_v2_runtime_free,
    ocaml_temporal_core_v2_runtime_new, ocaml_temporal_core_v2_worker_shutdown,
    ocaml_temporal_core_v2_worker_start_json, ocaml_temporal_core_v2_worker_try_poll_activity,
    ocaml_temporal_core_v2_worker_wait_activity,
};
use prost::Message;
use prost::bytes::Bytes;
use temporalio_common::protos::temporal::api::{
    common::v1::{ActivityType, WorkflowExecution, WorkflowType},
    workflowservice::v1::PollActivityTaskQueueResponse,
};

/// Upper bound for one bridge call under test. The fixed shutdown takes well
/// under a second against the double; the bound only exists so a regression
/// fails instead of hanging CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// Opaque task token of the single activity task the double hands out.
const TASK_TOKEN: &[u8] = b"issue-769-activity-task";

/// Requests observed by the gRPC double, shared with the test thread.
#[derive(Default)]
struct Observed {
    /// `PollActivityTaskQueue` requests received, including held ones.
    activity_polls: AtomicUsize,
    /// `RespondActivityTaskFailed` requests: each is one forced completion.
    activity_failures: AtomicUsize,
}

/// Starts a background gRPC double and returns its loopback address.
///
/// The first `PollActivityTaskQueue` request receives one activity task;
/// later activity polls and every workflow poll are held open without a
/// response, like an idle server long poll, until Core cancels them. Every
/// other method, including connection probes, namespace validation,
/// `ShutdownWorker`, and activity completions, succeeds with an empty
/// message. The server runs on its own detached OS thread with a private
/// current-thread Tokio runtime, so it never shares an executor with the
/// bridge under test and simply ends with the test process.
fn spawn_single_activity_server(observed: Arc<Observed>) -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-single-activity-double".to_owned())
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
/// Only the fields the bridge's semantic conversion requires are populated.
/// No timeouts are set, so Core starts no local timeout timer that could
/// complete or cancel the task independently of the shutdown under test.
fn activity_task_message() -> Vec<u8> {
    PollActivityTaskQueueResponse {
        task_token: TASK_TOKEN.to_vec(),
        workflow_namespace: "shutdown-test".to_owned(),
        workflow_type: Some(WorkflowType {
            name: "shutdown-test-workflow".to_owned(),
        }),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: "shutdown-test-workflow-id".to_owned(),
            run_id: "shutdown-test-run-id".to_owned(),
        }),
        activity_type: Some(ActivityType {
            name: "shutdown-test-activity".to_owned(),
        }),
        activity_id: "shutdown-test-activity-id".to_owned(),
        attempt: 1,
        ..Default::default()
    }
    .encode_to_vec()
}

/// Serves one HTTP/2 connection until the client closes it.
///
/// Held polls keep their response handle alive in `held` for the lifetime of
/// the connection; Core cancels those streams when it shuts down its pollers.
async fn serve_connection(socket: tokio::net::TcpStream, observed: Arc<Observed>) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    let mut held = Vec::new();
    while let Some(Ok((request, mut respond))) = connection.accept().await {
        let path = request.uri().path().to_owned();
        let message = if path.ends_with("/PollActivityTaskQueue") {
            if observed.activity_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                activity_task_message()
            } else {
                held.push(respond);
                continue;
            }
        } else if path.ends_with("/PollWorkflowTaskQueue") {
            held.push(respond);
            continue;
        } else {
            if path.ends_with("/RespondActivityTaskFailed") {
                observed.activity_failures.fetch_add(1, Ordering::SeqCst);
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
            continue;
        };
        // gRPC length-prefixed message: uncompressed flag, big-endian length.
        let length = u32::try_from(message.len()).expect("small test message");
        let mut frame = Vec::with_capacity(5 + message.len());
        frame.push(0);
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&message);
        if stream.send_data(Bytes::from(frame), false).is_err() {
            continue;
        }
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        let _ = stream.send_trailers(trailers);
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
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
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
        .unwrap_or_else(|_| panic!("{what} must return in bounded time (issue #769)"));
    (owned.0, status, message)
}

/// Activity-only worker document. Disabling workflows keeps the scenario to
/// the single activity lane; the bridge enables exactly one activity slot.
const ACTIVITY_WORKER: &[u8] = br#"{
    "namespace":"shutdown-test",
    "task_queue":"shutdown-outstanding-test",
    "build_id":"shutdown-outstanding-build",
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
    unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);

    let client = format!(r#"{{"target_url":"http://{address}","identity":"shutdown-test"}}"#);
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned; the input remains
    // readable for the full blocking call.
    unsafe {
        ocaml_temporal_core_v2_client_connect_json(
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
        ocaml_temporal_core_v2_worker_start_json(
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

/// Waits until the activity task is queued in the bridge's ready handoff,
/// without taking it. Each readiness wait is itself bounded by the bridge.
fn wait_for_ready_activity(mut runtime: *mut Runtime) -> *mut Runtime {
    let deadline = Instant::now() + CALL_DEADLINE;
    loop {
        let (returned, status, message) = call_bounded(
            runtime,
            ocaml_temporal_core_v2_worker_wait_activity,
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
    unsafe { ocaml_temporal_core_v2_client_disconnect(runtime, &mut result) };
    let (status, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
    // SAFETY: Children are closed and the slot is exclusively owned.
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

/// A task Core handed to the bridge but OCaml never took (the issue's
/// "graceful shutdown under load" case) no longer blocks shutdown. OCaml
/// never owned it, so shutdown fails it back to Core and succeeds.
#[test]
fn shutdown_retires_undelivered_task_without_hanging() {
    let observed = Arc::new(Observed::default());
    let address = spawn_single_activity_server(Arc::clone(&observed));
    let runtime = wait_for_ready_activity(start_worker(address));

    let (runtime, status, message) = call_bounded(
        runtime,
        ocaml_temporal_core_v2_worker_shutdown,
        "worker shutdown with an undelivered task",
    );
    assert_eq!(status, STATUS_OK, "{message}");
    assert_eq!(observed.activity_failures.load(Ordering::SeqCst), 1);

    // The worker was finalized and released, so a repeated shutdown is the
    // idempotent absent case.
    let (runtime, status, message) = call_bounded(
        runtime,
        ocaml_temporal_core_v2_worker_shutdown,
        "repeated worker shutdown",
    );
    assert_eq!(status, STATUS_OK, "{message}");
    close_runtime(runtime);
}

/// A task leased to OCaml but never completed (an abandoned activity
/// callback) is force-completed exactly once. The worker is still finalized
/// and released, and shutdown reports the abandoned work with the typed
/// outstanding-task status.
#[test]
fn shutdown_force_completes_abandoned_lease_and_reports_it() {
    let observed = Arc::new(Observed::default());
    let address = spawn_single_activity_server(Arc::clone(&observed));
    let runtime = wait_for_ready_activity(start_worker(address));

    let (runtime, status, message) = call_bounded(
        runtime,
        ocaml_temporal_core_v2_worker_try_poll_activity,
        "activity lease",
    );
    assert_eq!(status, STATUS_OK, "{message}");

    let (runtime, status, message) = call_bounded(
        runtime,
        ocaml_temporal_core_v2_worker_shutdown,
        "worker shutdown with an abandoned lease",
    );
    assert_eq!(status, STATUS_OUTSTANDING_TASKS, "{message}");
    assert_eq!(
        message,
        "Temporal worker shutdown force-completed tasks that were never completed by the worker"
    );
    assert_eq!(observed.activity_failures.load(Ordering::SeqCst), 1);

    let (runtime, status, message) = call_bounded(
        runtime,
        ocaml_temporal_core_v2_worker_shutdown,
        "repeated worker shutdown",
    );
    assert_eq!(status, STATUS_OK, "{message}");
    close_runtime(runtime);
}

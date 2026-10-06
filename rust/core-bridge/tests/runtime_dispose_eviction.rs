//! Regression coverage for runtime disposal with a leased workflow activation
//! (issue #775).
//!
//! Runtime close force-fails every workflow activation OCaml still holds and
//! tombstones its run ID, so a late poll of the same identity cannot be
//! completed twice. Core answers that failure with a cache-eviction
//! activation for the same run, and reports `ShutDown` from the workflow poll
//! only after the eviction is acknowledged. The poll lane used to drop the
//! eviction as a retired duplicate, so the lane join in runtime close waited
//! for the full drain bound and then released the worker without finalizing
//! it. Close must instead acknowledge the eviction and return promptly.
//!
//! The test uses a minimal plaintext HTTP/2 gRPC double instead of a Temporal
//! server. It hands out exactly one workflow task, accepts every other RPC
//! with an empty success response, and holds further polls open the way a
//! server long poll does, so the test is deterministic and runs in every Rust
//! test job.

use std::net::SocketAddr;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_NOT_READY, STATUS_OK, Status,
    ocaml_temporal_core_v3_client_connect_json, ocaml_temporal_core_v3_result_free,
    ocaml_temporal_core_v3_runtime_free, ocaml_temporal_core_v3_runtime_new,
    ocaml_temporal_core_v3_worker_start_json, ocaml_temporal_core_v3_worker_try_poll_workflow,
    ocaml_temporal_core_v3_worker_wait_workflow,
};
use prost::Message;
use prost::bytes::Bytes;
use prost_wkt_types::{Duration as ProtoDuration, Timestamp};
use temporalio_common::protos::temporal::api::{
    common::v1::{WorkflowExecution, WorkflowType},
    enums::v1::EventType,
    history::v1::{
        History, HistoryEvent, WorkflowExecutionStartedEventAttributes,
        WorkflowTaskScheduledEventAttributes, WorkflowTaskStartedEventAttributes, history_event,
    },
    taskqueue::v1::TaskQueue,
    workflowservice::v1::PollWorkflowTaskQueueResponse,
};

/// Upper bound for one bridge call under test. The fixed close takes well
/// under a second against the double. The bound is far below the bridge's
/// 90-second worker drain bound, so the regression fails here instead of
/// waiting out that bound or hanging CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// Task queue served by the double and polled by the worker under test.
const TASK_QUEUE: &str = "dispose-eviction-test";

/// Run ID of the single workflow task the double hands out.
const RUN_ID: &str = "issue-775-run-id";

/// Requests observed by the gRPC double, shared with the test thread.
#[derive(Default)]
struct Observed {
    /// `PollWorkflowTaskQueue` requests received, including held ones.
    workflow_polls: AtomicUsize,
    /// `RespondWorkflowTaskFailed` requests: each is one forced failure.
    workflow_failures: AtomicUsize,
}

/// Starts a background gRPC double and returns its loopback address.
///
/// The first `PollWorkflowTaskQueue` request receives one workflow task;
/// later workflow polls (normal or sticky) are held open without a response,
/// like an idle server long poll, until Core cancels them. Every other
/// method, including connection probes, namespace validation,
/// `ShutdownWorker`, and workflow task failures, succeeds with an empty
/// message. The server runs on its own detached OS thread with a private
/// current-thread Tokio runtime, so it never shares an executor with the
/// bridge under test and simply ends with the test process.
fn spawn_single_workflow_server(observed: Arc<Observed>) -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-single-workflow-double".to_owned())
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

/// Gives each synthetic history event a deterministic, increasing timestamp.
fn event_time(event_id: i64) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: event_id,
        nanos: 0,
    })
}

/// Ten-second workflow task timeout metadata Core expects on a normal task.
fn task_timeout() -> Option<ProtoDuration> {
    Some(ProtoDuration {
        seconds: 10,
        nanos: 0,
    })
}

/// Builds the history of a workflow whose first workflow task has just
/// started, so Core produces one non-eviction activation for it.
fn first_task_history() -> History {
    let task_queue = || {
        Some(TaskQueue {
            name: TASK_QUEUE.to_owned(),
            ..Default::default()
        })
    };
    let started = HistoryEvent {
        event_id: 1,
        event_time: event_time(1),
        event_type: EventType::WorkflowExecutionStarted as i32,
        attributes: Some(
            history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                WorkflowExecutionStartedEventAttributes {
                    workflow_type: Some(WorkflowType {
                        name: "dispose-eviction-workflow".to_owned(),
                    }),
                    task_queue: task_queue(),
                    workflow_task_timeout: task_timeout(),
                    original_execution_run_id: RUN_ID.to_owned(),
                    first_execution_run_id: RUN_ID.to_owned(),
                    attempt: 1,
                    first_workflow_task_backoff: Some(ProtoDuration::default()),
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    let scheduled = HistoryEvent {
        event_id: 2,
        event_time: event_time(2),
        event_type: EventType::WorkflowTaskScheduled as i32,
        attributes: Some(
            history_event::Attributes::WorkflowTaskScheduledEventAttributes(
                WorkflowTaskScheduledEventAttributes {
                    task_queue: task_queue(),
                    start_to_close_timeout: task_timeout(),
                    attempt: 1,
                },
            ),
        ),
        ..Default::default()
    };
    let task_started = HistoryEvent {
        event_id: 3,
        event_time: event_time(3),
        event_type: EventType::WorkflowTaskStarted as i32,
        attributes: Some(
            history_event::Attributes::WorkflowTaskStartedEventAttributes(
                WorkflowTaskStartedEventAttributes {
                    identity: "dispose-eviction-test".to_owned(),
                    scheduled_event_id: 2,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    History {
        events: vec![started, scheduled, task_started],
    }
}

/// Encodes the workflow task returned by the first workflow poll. The full
/// history is inline, so Core never pages it from the double.
fn workflow_task_message() -> Vec<u8> {
    PollWorkflowTaskQueueResponse {
        task_token: b"issue-775-workflow-task".to_vec(),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: "issue-775-workflow-id".to_owned(),
            run_id: RUN_ID.to_owned(),
        }),
        workflow_type: Some(WorkflowType {
            name: "dispose-eviction-workflow".to_owned(),
        }),
        started_event_id: 3,
        attempt: 1,
        history: Some(first_task_history()),
        workflow_execution_task_queue: Some(TaskQueue {
            name: TASK_QUEUE.to_owned(),
            ..Default::default()
        }),
        scheduled_time: event_time(2),
        started_time: event_time(3),
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
        let message = if path.ends_with("/PollWorkflowTaskQueue") {
            if observed.workflow_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                workflow_task_message()
            } else {
                held.push(respond);
                continue;
            }
        } else if path.ends_with("/PollActivityTaskQueue") {
            held.push(respond);
            continue;
        } else {
            if path.ends_with("/RespondWorkflowTaskFailed") {
                observed.workflow_failures.fetch_add(1, Ordering::SeqCst);
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

/// Runs `call` with exclusive ownership of the runtime on a helper thread and
/// fails the test if it does not return within [`CALL_DEADLINE`]. Returns
/// the runtime pointer (null once freed) with the call's result.
///
/// On timeout the helper thread is intentionally leaked with the runtime: the
/// bridge is wedged inside Core, so neither releasing nor joining it is safe.
fn call_bounded<T: Send + 'static>(
    runtime: *mut Runtime,
    what: &str,
    call: impl FnOnce(&mut *mut Runtime) -> T + Send + 'static,
) -> (*mut Runtime, T) {
    let (sender, receiver) = mpsc::channel();
    let owned = RuntimePtr(runtime);
    thread::spawn(move || {
        let mut owned = owned;
        let value = call(&mut owned.0);
        let _ = sender.send((owned, value));
    });
    let (owned, value) = receiver
        .recv_timeout(CALL_DEADLINE)
        .unwrap_or_else(|_| panic!("{what} must return in bounded time (issue #775)"));
    (owned.0, value)
}

/// Runs one runtime-only ABI call under [`call_bounded`] and returns its
/// status and diagnostic.
fn call_result(
    runtime: *mut Runtime,
    what: &str,
    call: unsafe extern "C" fn(*mut Runtime, *mut AbiResult) -> Status,
) -> (*mut Runtime, Status, String) {
    let (runtime, (status, message)) = call_bounded(runtime, what, move |runtime| {
        let mut result = AbiResult::default();
        // SAFETY: This thread exclusively owns the live runtime for the call
        // and the output location is unique.
        unsafe { call(*runtime, &mut result) };
        release(result)
    });
    (runtime, status, message)
}

/// Workflow-only worker document. Disabling activities keeps the scenario to
/// the single workflow lane.
const WORKFLOW_WORKER: &[u8] = br#"{
    "namespace":"dispose-eviction-test",
    "task_queue":"dispose-eviction-test",
    "build_id":"dispose-eviction-build",
    "versioning":{"kind":"none"},
    "max_cached_workflows":10,
    "max_outstanding_workflow_tasks":10,
    "max_concurrent_workflow_task_polls":2,
    "graceful_shutdown_timeout_ms":1000,
    "task_types":{"workflows":true,"activities":false}
}"#;

/// Creates a runtime, connects it to the double, and starts the workflow
/// worker. Returns the exclusively owned runtime.
fn start_worker(address: SocketAddr) -> *mut Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v3_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);

    let client = format!(r#"{{"target_url":"http://{address}","identity":"dispose-eviction"}}"#);
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
            WORKFLOW_WORKER.as_ptr(),
            WORKFLOW_WORKER.len(),
            &mut result,
        );
    }
    let (status, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
    runtime
}

/// Waits until the workflow activation is queued in the bridge's ready
/// handoff, without taking it. Each readiness wait is itself bounded by the
/// bridge.
fn wait_for_ready_workflow(mut runtime: *mut Runtime) -> *mut Runtime {
    let deadline = Instant::now() + CALL_DEADLINE;
    loop {
        let (returned, status, message) = call_result(
            runtime,
            "workflow readiness wait",
            ocaml_temporal_core_v3_worker_wait_workflow,
        );
        runtime = returned;
        match status {
            STATUS_OK => return runtime,
            STATUS_NOT_READY if Instant::now() < deadline => {}
            _ => panic!("workflow activation never became ready: {status} {message}"),
        }
    }
}

/// Closing a runtime whose worker still has a workflow activation leased to
/// OCaml force-fails that activation, acknowledges Core's follow-up eviction
/// for the same run, and returns well within the call deadline instead of
/// waiting out the worker drain bound.
#[test]
fn runtime_close_acknowledges_eviction_after_retiring_leased_activation() {
    let observed = Arc::new(Observed::default());
    let address = spawn_single_workflow_server(Arc::clone(&observed));
    let runtime = wait_for_ready_workflow(start_worker(address));

    // Lease the activation to "OCaml" and abandon it, as a supervisor that
    // stops without completing its workflow task would.
    let (runtime, status, message) = call_result(
        runtime,
        "workflow lease",
        ocaml_temporal_core_v3_worker_try_poll_workflow,
    );
    assert_eq!(status, STATUS_OK, "{message}");

    // A dropped eviction only ends by waiting out the 90-second worker drain
    // bound, so returning within `CALL_DEADLINE` proves it was acknowledged.
    let (runtime, status) = call_bounded(runtime, "runtime close", |runtime| {
        // SAFETY: This thread exclusively owns the live runtime slot. Close
        // is allowed with a live worker: it disposes the whole graph.
        unsafe { ocaml_temporal_core_v3_runtime_free(runtime) }
    });
    assert_eq!(status, STATUS_OK);
    assert!(runtime.is_null());
    assert_eq!(observed.workflow_failures.load(Ordering::SeqCst), 1);
}

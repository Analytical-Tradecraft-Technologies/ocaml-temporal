//! Regression coverage for head-of-line blocking on workflow completion.
//!
//! The live worker used to complete a workflow activation by awaiting Core's
//! `complete_workflow_activation` on the runtime's sole owner thread. Core
//! reports the workflow task to the server inside that call, so while one
//! run's `RespondWorkflowTaskCompleted` RPC was slow, the owner could neither
//! hand out another run's activation, accept its completion, nor service any
//! other operation. The bridge now admits a completion, returns a ticket,
//! and lets a Tokio task await Core.
//!
//! The tests use a plaintext HTTP/2 gRPC double instead of a Temporal server.
//! It hands out one workflow task for each of two runs and holds run A's
//! `RespondWorkflowTaskCompleted` behind a latch the test controls, while
//! every other RPC succeeds at once. With the latch closed, the same runtime
//! must admit A's completion, deliver B, admit and settle B's completion,
//! and serve another operation; only releasing the latch settles A. The old
//! synchronous path fails the first step: A's completion never returns while
//! the latch is closed, so the bounded call below panics.

use std::io::Read;
use std::net::SocketAddr;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_NOT_READY, STATUS_OK, Status,
    ocaml_temporal_core_v4_client_connect_json, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new,
    ocaml_temporal_core_v4_worker_shutdown, ocaml_temporal_core_v4_worker_start_json,
    ocaml_temporal_core_v4_worker_submit_workflow_completion_json,
    ocaml_temporal_core_v4_worker_take_workflow_completions,
    ocaml_temporal_core_v4_worker_try_poll_activity,
    ocaml_temporal_core_v4_worker_try_poll_workflow, ocaml_temporal_core_v4_worker_wait_workflow,
    ocaml_temporal_core_v4_worker_wait_workflow_completion,
};
use prost::Message;
use prost::bytes::{Bytes, BytesMut};
use prost_wkt_types::{Duration as ProtoDuration, Timestamp};
use temporalio_common::protos::temporal::api::{
    common::v1::{WorkflowExecution, WorkflowType},
    enums::v1::EventType,
    history::v1::{
        History, HistoryEvent, WorkflowExecutionStartedEventAttributes,
        WorkflowTaskScheduledEventAttributes, WorkflowTaskStartedEventAttributes, history_event,
    },
    taskqueue::v1::TaskQueue,
    workflowservice::v1::{PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedRequest},
};

/// Watchdog for one bridge call or wait. Every step under test returns in
/// milliseconds; the bound only turns a regression into a failure.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// Task queue served by the double and polled by the worker under test.
const TASK_QUEUE: &str = "completion-overlap-test";

/// Run whose completion RPC the double holds.
const RUN_A: &str = "completion-overlap-run-a";
/// Run whose completion RPC the double answers at once.
const RUN_B: &str = "completion-overlap-run-b";

/// Requests observed by the double and the latch holding run A's RPC.
struct Observed {
    /// `PollWorkflowTaskQueue` requests received, including held ones.
    workflow_polls: AtomicUsize,
    /// Ordered record of completion RPCs and test steps.
    events: Mutex<Vec<String>>,
    /// Opens the latch on run A's `RespondWorkflowTaskCompleted`.
    release_a: tokio::sync::watch::Sender<bool>,
    /// Signals the test thread that run A's RPC reached the double.
    a_received: Mutex<Option<mpsc::Sender<()>>>,
}

impl Observed {
    /// Appends one entry to the ordered event record.
    fn record(&self, event: impl Into<String>) {
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(event.into());
    }

    /// A copy of the ordered event record.
    fn events(&self) -> Vec<String> {
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

/// Opens run A's latch when dropped, so a failing assertion cannot leave the
/// double's request (and with it the bridge's completion task) held forever.
struct ReleaseOnDrop(Arc<Observed>);

impl ReleaseOnDrop {
    /// Opens the latch now and records the step.
    fn release(&self) {
        self.0.record("latch released");
        let _ = self.0.release_a.send(true);
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let _ = self.0.release_a.send(true);
    }
}

/// Starts the gRPC double on its own thread and runtime and returns its
/// loopback address.
fn spawn_server(observed: Arc<Observed>) -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-completion-overlap-double".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
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

/// Deterministic, increasing event timestamps.
fn event_time(event_id: i64) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: event_id,
        nanos: 0,
    })
}

/// Ten-second workflow task timeout Core expects on a normal task.
fn task_timeout() -> Option<ProtoDuration> {
    Some(ProtoDuration {
        seconds: 10,
        nanos: 0,
    })
}

/// History of a workflow whose first workflow task has just started.
fn first_task_history(run_id: &str) -> History {
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
                        name: "completion-overlap-workflow".to_owned(),
                    }),
                    task_queue: task_queue(),
                    workflow_task_timeout: task_timeout(),
                    original_execution_run_id: run_id.to_owned(),
                    first_execution_run_id: run_id.to_owned(),
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
                    identity: "completion-overlap-test".to_owned(),
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

/// The task token the double gives `run_id`'s workflow task.
fn task_token(run_id: &str) -> Vec<u8> {
    format!("token-{run_id}").into_bytes()
}

/// The workflow task returned for `run_id`, with its full history inline.
fn workflow_task_message(run_id: &str) -> Vec<u8> {
    PollWorkflowTaskQueueResponse {
        task_token: task_token(run_id),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: format!("{run_id}-workflow"),
            run_id: run_id.to_owned(),
        }),
        workflow_type: Some(WorkflowType {
            name: "completion-overlap-workflow".to_owned(),
        }),
        started_event_id: 3,
        attempt: 1,
        history: Some(first_task_history(run_id)),
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

/// Reads one unary gRPC request message, decompressing a gzip message.
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

/// Serves one HTTP/2 connection, one task per request so a held request
/// does not block the connection.
async fn serve_connection(socket: tokio::net::TcpStream, observed: Arc<Observed>) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((request, respond))) = connection.accept().await {
        tokio::spawn(serve_request(request, respond, Arc::clone(&observed)));
    }
}

/// Answers one request. The first two workflow polls receive run A's and
/// run B's tasks; later polls are held like idle long polls. Run A's
/// `RespondWorkflowTaskCompleted` waits for the latch; everything else
/// succeeds at once with an empty message.
async fn serve_request(
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    observed: Arc<Observed>,
) {
    let path = request.uri().path().to_owned();
    let message = if path.ends_with("/PollWorkflowTaskQueue") {
        match observed.workflow_polls.fetch_add(1, Ordering::SeqCst) {
            0 => workflow_task_message(RUN_A),
            1 => workflow_task_message(RUN_B),
            _ => {
                std::future::pending::<()>().await;
                return;
            }
        }
    } else if path.ends_with("/PollActivityTaskQueue") {
        std::future::pending::<()>().await;
        return;
    } else if path.ends_with("/RespondWorkflowTaskCompleted") {
        let bytes = read_unary_message(request.into_body()).await;
        let completed = RespondWorkflowTaskCompletedRequest::decode(bytes.as_slice())
            .expect("Core sends a valid RespondWorkflowTaskCompleted request");
        if completed.task_token == task_token(RUN_A) {
            observed.record("rpc A received");
            if let Some(sender) = observed
                .a_received
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                let _ = sender.send(());
            }
            let mut latch = observed.release_a.subscribe();
            let _ = latch.wait_for(|released| *released).await;
            observed.record("rpc A answered");
        } else {
            observed.record("rpc B answered");
        }
        Vec::new()
    } else {
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

/// Frees a live result and returns its status, value, and diagnostic.
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

/// Raw runtime pointer that may move to the bounded call thread.
///
/// Ownership passes to exactly one thread at a time, matching the ABI rule
/// that one caller owns a runtime at a time.
struct RuntimePtr(*mut Runtime);

// SAFETY: Ownership of the runtime pointer is transferred to exactly one
// thread at a time and returned through a channel before reuse.
unsafe impl Send for RuntimePtr {}

/// Runs `call` with exclusive ownership of the runtime on a helper thread
/// and fails the test if it does not return within `deadline`. On timeout
/// the helper thread is leaked with the runtime, because it is still inside
/// the bridge.
fn call_within<T: Send + 'static>(
    runtime: *mut Runtime,
    what: &str,
    deadline: Duration,
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
        .recv_timeout(deadline)
        .unwrap_or_else(|_| panic!("{what} did not return within {deadline:?}"));
    (owned.0, value)
}

/// Runs one runtime-only ABI operation under [`CALL_DEADLINE`].
fn call_op(
    runtime: *mut Runtime,
    what: &str,
    operation: unsafe extern "C" fn(*mut Runtime, *mut AbiResult) -> Status,
) -> (*mut Runtime, (Status, Vec<u8>, String)) {
    call_within(runtime, what, CALL_DEADLINE, move |runtime| {
        let mut result = AbiResult::default();
        // SAFETY: This thread exclusively owns the live runtime for the
        // call and the output location is unique.
        unsafe { operation(*runtime, &mut result) };
        release(result)
    })
}

/// Admits the empty completion of `run_id` and returns its ticket. The call
/// must return promptly whatever Core and the server are doing.
fn submit_empty_completion(runtime: *mut Runtime, run_id: &str) -> (*mut Runtime, u64) {
    let document = format!(r#"{{"run_id":"{run_id}","commands":[]}}"#);
    let what = format!("admitting run {run_id}'s completion");
    let (runtime, (status, value, message)) =
        call_within(runtime, &what, Duration::from_secs(5), move |runtime| {
            let mut result = AbiResult::default();
            // SAFETY: Exclusive runtime ownership; the input stays readable
            // for the whole call.
            unsafe {
                ocaml_temporal_core_v4_worker_submit_workflow_completion_json(
                    *runtime,
                    document.as_ptr(),
                    document.len(),
                    &mut result,
                );
            }
            release(result)
        });
    assert_eq!(status, STATUS_OK, "{message}");
    let ticket = String::from_utf8(value)
        .expect("ticket is ASCII")
        .parse::<u64>()
        .expect("ticket is decimal");
    assert!(ticket > 0);
    (runtime, ticket)
}

/// Waits for and takes outcomes until `ticket`'s appears; returns the
/// runtime, that outcome, and every outcome seen.
fn await_outcome(
    mut runtime: *mut Runtime,
    ticket: u64,
) -> (*mut Runtime, serde_json::Value, Vec<serde_json::Value>) {
    let deadline = Instant::now() + CALL_DEADLINE;
    let mut seen = Vec::new();
    loop {
        let (returned, (status, value, message)) = call_op(
            runtime,
            "taking completion outcomes",
            ocaml_temporal_core_v4_worker_take_workflow_completions,
        );
        runtime = returned;
        assert_eq!(status, STATUS_OK, "{message}");
        let document: serde_json::Value =
            serde_json::from_slice(&value).expect("outcome document is JSON");
        for outcome in document["outcomes"].as_array().expect("outcome array") {
            seen.push(outcome.clone());
        }
        if let Some(found) = seen.iter().find(|outcome| outcome["ticket"] == ticket) {
            return (runtime, found.clone(), seen);
        }
        assert!(Instant::now() < deadline, "ticket {ticket} never settled");
        let (returned, (status, _, message)) = call_op(
            runtime,
            "waiting for a completion outcome",
            ocaml_temporal_core_v4_worker_wait_workflow_completion,
        );
        runtime = returned;
        assert!(
            status == STATUS_OK || status == STATUS_NOT_READY,
            "{status} {message}"
        );
    }
}

/// Polls until `count` activations have been handed over, returning their
/// run IDs in delivery order.
fn lease_activations(mut runtime: *mut Runtime, count: usize) -> (*mut Runtime, Vec<String>) {
    let deadline = Instant::now() + CALL_DEADLINE;
    let mut runs = Vec::new();
    while runs.len() < count {
        let (returned, (status, value, message)) = call_op(
            runtime,
            "workflow poll",
            ocaml_temporal_core_v4_worker_try_poll_workflow,
        );
        runtime = returned;
        match status {
            STATUS_OK => {
                let activation: serde_json::Value =
                    serde_json::from_slice(&value).expect("activation is JSON");
                runs.push(
                    activation["run_id"]
                        .as_str()
                        .expect("activation run ID")
                        .to_owned(),
                );
            }
            STATUS_NOT_READY => {
                assert!(Instant::now() < deadline, "activations never arrived");
                let (returned, _) = call_op(
                    runtime,
                    "workflow readiness wait",
                    ocaml_temporal_core_v4_worker_wait_workflow,
                );
                runtime = returned;
            }
            other => panic!("workflow poll failed: {other} {message}"),
        }
    }
    (runtime, runs)
}

/// Requires one activation for each run. Core's two workflow pollers may
/// hand them over in either order.
fn assert_both_runs(mut runs: Vec<String>) {
    runs.sort();
    assert_eq!(runs, [RUN_A, RUN_B]);
}

/// Workflow-only worker document with room for both runs.
const WORKFLOW_WORKER: &[u8] = br#"{
    "namespace":"completion-overlap-test",
    "task_queue":"completion-overlap-test",
    "build_id":"completion-overlap-build",
    "versioning":{"kind":"none"},
    "max_cached_workflows":10,
    "max_outstanding_workflow_tasks":10,
    "max_concurrent_workflow_task_polls":2,
    "graceful_shutdown_timeout_ms":1000,
    "task_types":{"workflows":true,"activities":false}
}"#;

/// Creates a runtime, connects it to the double, and starts the worker.
fn start_worker(address: SocketAddr) -> *mut Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);

    let client = format!(r#"{{"target_url":"http://{address}","identity":"completion-overlap"}}"#);
    let mut result = AbiResult::default();
    // SAFETY: Live, exclusively owned runtime; the input stays readable.
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

    let mut result = AbiResult::default();
    // SAFETY: Same exclusive ownership; the static document stays readable.
    unsafe {
        ocaml_temporal_core_v4_worker_start_json(
            runtime,
            WORKFLOW_WORKER.as_ptr(),
            WORKFLOW_WORKER.len(),
            &mut result,
        );
    }
    let (status, _, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
    runtime
}

/// While run A's completion RPC is held, the same runtime admits A's
/// completion at once, delivers run B, admits and settles B's completion,
/// and serves an unrelated operation; A settles only after the latch opens,
/// and shutdown then drains normally.
#[test]
fn a_held_completion_rpc_does_not_block_another_run() {
    let (release_a, _) = tokio::sync::watch::channel(false);
    let (a_received_sender, a_received) = mpsc::channel();
    let observed = Arc::new(Observed {
        workflow_polls: AtomicUsize::new(0),
        events: Mutex::new(Vec::new()),
        release_a,
        a_received: Mutex::new(Some(a_received_sender)),
    });
    let latch = ReleaseOnDrop(Arc::clone(&observed));
    let address = spawn_server(Arc::clone(&observed));
    let runtime = start_worker(address);

    let (runtime, runs) = lease_activations(runtime, 2);
    assert_both_runs(runs);

    // A's completion is admitted although its RPC will be held.
    let (runtime, ticket_a) = submit_empty_completion(runtime, RUN_A);
    a_received
        .recv_timeout(CALL_DEADLINE)
        .expect("run A's completion RPC reaches the server");

    // With A's RPC held, B's completion is admitted and settles.
    let (runtime, ticket_b) = submit_empty_completion(runtime, RUN_B);
    assert_ne!(ticket_a, ticket_b);
    let (runtime, outcome_b, seen) = await_outcome(runtime, ticket_b);
    observed.record("outcome B taken");
    assert_eq!(outcome_b["result"], "completed");
    assert_eq!(outcome_b["run_id"], RUN_B);
    assert!(
        seen.iter().all(|outcome| outcome["ticket"] != ticket_a),
        "A cannot settle while its RPC is held: {seen:?}"
    );

    // The runtime still serves unrelated operations promptly.
    let (runtime, (status, _, message)) = call_within(
        runtime,
        "an unrelated poll while A is held",
        Duration::from_secs(5),
        |runtime| {
            let mut result = AbiResult::default();
            // SAFETY: Exclusive runtime ownership for this call.
            unsafe { ocaml_temporal_core_v4_worker_try_poll_activity(*runtime, &mut result) };
            release(result)
        },
    );
    assert_eq!(status, STATUS_NOT_READY, "{message}");

    latch.release();
    let (runtime, outcome_a, _) = await_outcome(runtime, ticket_a);
    observed.record("outcome A taken");
    assert_eq!(outcome_a["result"], "completed");
    assert_eq!(outcome_a["run_id"], RUN_A);

    assert_eq!(
        observed.events(),
        [
            "rpc A received",
            "rpc B answered",
            "outcome B taken",
            "latch released",
            "rpc A answered",
            "outcome A taken",
        ]
    );

    // Both outcomes were collected, so shutdown has nothing to force.
    let (runtime, (status, _, message)) = call_op(
        runtime,
        "worker shutdown",
        ocaml_temporal_core_v4_worker_shutdown,
    );
    assert_eq!(status, STATUS_OK, "{message}");
    let (_runtime, status) = call_within(runtime, "runtime close", CALL_DEADLINE, |runtime| {
        // SAFETY: This thread exclusively owns the live runtime slot.
        unsafe { ocaml_temporal_core_v4_runtime_free(runtime) }
    });
    assert_eq!(status, STATUS_OK);
}

/// Worker shutdown joins a completion that is still awaiting Core before it
/// finalizes the worker: it does not return while A's RPC is held, and
/// returns cleanly once the latch opens.
#[test]
fn shutdown_joins_an_in_flight_completion_before_finalizing() {
    let (release_a, _) = tokio::sync::watch::channel(false);
    let (a_received_sender, a_received) = mpsc::channel();
    let observed = Arc::new(Observed {
        workflow_polls: AtomicUsize::new(0),
        events: Mutex::new(Vec::new()),
        release_a,
        a_received: Mutex::new(Some(a_received_sender)),
    });
    let latch = ReleaseOnDrop(Arc::clone(&observed));
    let address = spawn_server(Arc::clone(&observed));
    let runtime = start_worker(address);
    let (runtime, runs) = lease_activations(runtime, 2);
    assert_both_runs(runs);
    let (runtime, _ticket_a) = submit_empty_completion(runtime, RUN_A);
    let (runtime, ticket_b) = submit_empty_completion(runtime, RUN_B);
    a_received
        .recv_timeout(CALL_DEADLINE)
        .expect("run A's completion RPC reaches the server");
    let (runtime, _, _) = await_outcome(runtime, ticket_b);

    let (sender, receiver) = mpsc::channel();
    let owned = RuntimePtr(runtime);
    thread::spawn(move || {
        let owned = owned;
        let mut result = AbiResult::default();
        // SAFETY: This thread exclusively owns the live runtime for the call.
        unsafe { ocaml_temporal_core_v4_worker_shutdown(owned.0, &mut result) };
        let _ = sender.send((owned, release(result)));
    });
    // Negative check: shutdown must still be joining A's task. The timeout
    // is a watchdog for the opposite outcome, not what drives the test.
    assert!(
        receiver.recv_timeout(Duration::from_millis(300)).is_err(),
        "shutdown returned while an admitted completion was awaiting Core"
    );
    latch.release();
    let (owned, (status, _, message)) = receiver
        .recv_timeout(CALL_DEADLINE)
        .expect("shutdown returns once the completion settles");
    assert_eq!(status, STATUS_OK, "{message}");
    let (_runtime, status) = call_within(owned.0, "runtime close", CALL_DEADLINE, |runtime| {
        // SAFETY: This thread exclusively owns the live runtime slot.
        unsafe { ocaml_temporal_core_v4_runtime_free(runtime) }
    });
    assert_eq!(status, STATUS_OK);
}

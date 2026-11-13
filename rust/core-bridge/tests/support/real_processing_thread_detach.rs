//! Lifecycle stress case for #1149: Core's real workflow processing thread
//! stays alive through worker shutdown and runtime release.
//!
//! Included as a child module of `src/abi.rs`, so it can shorten the
//! worker's shutdown bounds, while the worker itself is created, polled,
//! shut down, and released through the real C ABI entry points.
//!
//! The scenario needs no fault injection inside the bridge or Core. A gRPC
//! double hands the workflow worker one workflow task and then fails every
//! later workflow poll with gRPC's "message larger than max" status, which
//! Core's client forwards at once instead of retrying. Core passes that
//! error to the bridge's workflow lane, which publishes it and exits, and
//! starts shutting the worker down. Core's workflow processing thread,
//! however, still holds the workflow run: its activation, or the eviction
//! Core issues once shutdown has failed that activation, is never
//! acknowledged, because no lane polls any more and Core's default
//! `ignore_evicts_on_shutdown` is false. The thread therefore never reaches
//! its shutdown condition and stays alive for the rest of the worker's life.
//!
//! This is as far as Core's real thread can be kept alive, and it shows
//! where the bounds actually apply. Core reports `ShutDown` from a workflow
//! poll only once the processing thread's stream has ended, and the bridge's
//! local-activity lane, which every workflow worker has, only after that. A
//! drain that succeeds therefore implies the thread is already exiting, so
//! `finalize_bounded` never runs Core's finalizer while the thread lives.
//! Instead the drain times out: `worker_shutdown` returns at its drain bound,
//! and runtime disposal refuses finalization (lost poll lease) and drops the
//! worker. Dropping Core does not wait for the thread, which is a plain OS
//! thread rather than blocking-pool work, so both releases return
//! `STATUS_OK` far within their production bounds. The blocking-pool hazard
//! that does make those releases return `STATUS_DETACHED` is reproduced by
//! `runtime_release_bound.rs`.

use super::{
    Result as AbiResult, Runtime, STATUS_NOT_READY, STATUS_OK, STATUS_WORKER, SharedRuntime,
    Status, ocaml_temporal_core_v4_client_connect_json, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new,
    ocaml_temporal_core_v4_runtime_new_attached, ocaml_temporal_core_v4_shared_runtime_free,
    ocaml_temporal_core_v4_shared_runtime_new, ocaml_temporal_core_v4_worker_shutdown,
    ocaml_temporal_core_v4_worker_start_json, ocaml_temporal_core_v4_worker_try_poll_workflow,
};
use prost::Message;
use prost_wkt_types::{Duration as ProtoDuration, Timestamp};
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
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

use super::grpc_double::{self, Reply};

/// Drain bound installed in place of the 90-second production bound. The
/// drain cannot succeed while the processing thread lives, so worker
/// shutdown and runtime disposal each wait this long.
const DRAIN_BOUND: Duration = Duration::from_millis(300);

/// Finalize bound installed in place of the 30-second production bound. A
/// timed-out drain never reaches Core's finalizer, so this only caps a
/// regression.
const FINALIZE_BOUND: Duration = Duration::from_secs(5);

/// Upper bound for one release of a runtime or shared runtime. Each waits
/// only for the shortened drain and the drop of Core, far below the 150 s
/// and 30 s production bounds, and must not wait for the processing thread.
const RELEASE_DEADLINE: Duration = Duration::from_secs(10);

/// Upper bound for any one step of the test; a step that outlives it is
/// reported as a hang.
const STEP_DEADLINE: Duration = Duration::from_secs(30);

/// The closed diagnostic `shutdown_worker` reports for a timed-out drain.
const DRAIN_TIMEOUT_MESSAGE: &str =
    "Temporal worker poll lanes did not stop within the shutdown bound";

/// Task queue served by the double.
const TASK_QUEUE: &str = "processing-thread-detach-test";

/// Workflow worker without remote activities. Its activity lane serves only
/// local activities, so it ends only after the workflow poll has ended.
const WORKFLOW_WORKER: &[u8] = br#"{
    "namespace":"processing-thread-detach-test",
    "task_queue":"processing-thread-detach-test",
    "build_id":"processing-thread-detach-build",
    "versioning":{"kind":"none"},
    "max_cached_workflows":10,
    "max_outstanding_workflow_tasks":10,
    "max_concurrent_workflow_task_polls":2,
    "graceful_shutdown_timeout_ms":1000,
    "task_types":{"workflows":true,"activities":false}
}"#;

/// gRPC `RESOURCE_EXHAUSTED`.
const GRPC_RESOURCE_EXHAUSTED: u32 = 8;

/// A message prefix that Core's retrying client treats as a non-retryable
/// oversized message and forwards to the poller on the first attempt.
const MESSAGE_TOO_LARGE: &str = "grpc: received message larger than max (8388608 vs. 4194304)";

/// Gives each synthetic history event a deterministic, increasing time.
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

/// The double's task queue in the shape history events carry it.
fn task_queue() -> Option<TaskQueue> {
    Some(TaskQueue {
        name: TASK_QUEUE.to_owned(),
        ..Default::default()
    })
}

/// History of a workflow whose first workflow task has just started, so Core
/// creates the run and issues one activation for it.
fn first_task_history(run_id: &str) -> History {
    let started = HistoryEvent {
        event_id: 1,
        event_time: event_time(1),
        event_type: EventType::WorkflowExecutionStarted as i32,
        attributes: Some(
            history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                WorkflowExecutionStartedEventAttributes {
                    workflow_type: Some(WorkflowType {
                        name: "processing-thread-detach".to_owned(),
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
                    identity: "processing-thread-detach".to_owned(),
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

/// Encodes the single workflow task the double hands out, with its whole
/// history inline so Core never pages it.
fn workflow_task_message(run_id: &str) -> Vec<u8> {
    PollWorkflowTaskQueueResponse {
        task_token: format!("{run_id}-task").into_bytes(),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: format!("{run_id}-workflow"),
            run_id: run_id.to_owned(),
        }),
        workflow_type: Some(WorkflowType {
            name: "processing-thread-detach".to_owned(),
        }),
        started_event_id: 3,
        attempt: 1,
        history: Some(first_task_history(run_id)),
        workflow_execution_task_queue: task_queue(),
        scheduled_time: event_time(2),
        started_time: event_time(3),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Starts the double: the first workflow poll (normal or sticky) gets the
/// workflow task, every later one the oversized-message error, and every
/// other method an empty success.
fn spawn_double(run_id: &'static str) -> grpc_double::Double {
    grpc_double::spawn(Arc::new(move |method: &str, earlier| match method {
        "PollWorkflowTaskQueue" if earlier == 0 => Reply::Message(workflow_task_message(run_id)),
        "PollWorkflowTaskQueue" => Reply::Error {
            code: GRPC_RESOURCE_EXHAUSTED,
            message: MESSAGE_TOO_LARGE,
        },
        method if method.starts_with("Poll") => Reply::Hold,
        _ => Reply::Message(Vec::new()),
    }))
}

/// Frees a live result and returns its status and diagnostic text.
fn release(mut result: AbiResult) -> (Status, String) {
    let status = result.status;
    let message = if result.error.ptr.is_null() {
        String::new()
    } else {
        // SAFETY: The bridge owns this readable allocation until the result
        // is freed below.
        let bytes =
            unsafe { std::slice::from_raw_parts(result.error.ptr, result.error.len) }.to_vec();
        String::from_utf8(bytes).expect("diagnostic is UTF-8")
    };
    // SAFETY: This helper has exclusive ownership of the initialized result.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    (status, message)
}

/// Exclusive ownership of one live runtime graph published by the ABI. It is
/// never null, it moves whole into each ABI call, and [`free_runtime`]
/// consumes it, so the test cannot touch the graph while a call holds it or
/// after `runtime_free` has released it.
struct OwnedRuntime(NonNull<Runtime>);

// SAFETY: The handle is owned by exactly one thread at a time: it moves to
// the bounded helper thread and comes back through a channel before reuse,
// which is the ABI's single-owner rule.
unsafe impl Send for OwnedRuntime {}

impl OwnedRuntime {
    /// Takes ownership of the runtime an ABI constructor wrote to `slot`,
    /// failing the test if it published none.
    fn published(slot: *mut Runtime) -> Self {
        Self(NonNull::new(slot).expect("a successful constructor publishes a runtime"))
    }

    /// Lends the graph to `inspect` for the duration of that closure only, so
    /// the reference can neither outlive this owner nor alias a pointer
    /// handed to an ABI call.
    fn with_graph<T>(&mut self, inspect: impl FnOnce(&mut Runtime) -> T) -> T {
        // SAFETY: `self` is the sole owner of a live runtime that has not
        // been freed (`free_runtime` consumes it), and no ABI call is in
        // progress because every call takes the owner by value. The
        // `&mut self` borrow keeps the reference unique and confined to
        // `inspect`.
        inspect(unsafe { self.0.as_mut() })
    }
}

/// Exclusive ownership of one live shared-runtime handle; the counterpart of
/// [`OwnedRuntime`], consumed by its release.
struct OwnedShared(NonNull<SharedRuntime>);

// SAFETY: As for `OwnedRuntime`: exactly one thread owns the handle at a
// time.
unsafe impl Send for OwnedShared {}

impl OwnedShared {
    /// Lends the handle to `inspect` for the duration of that closure only.
    fn with_handle<T>(&self, inspect: impl FnOnce(&SharedRuntime) -> T) -> T {
        // SAFETY: `self` owns a live handle that has not been released (the
        // release consumes it), and no ABI call is using it meanwhile.
        inspect(unsafe { self.0.as_ref() })
    }
}

/// Runs `call` on a helper thread and fails the test if it does not return
/// within [`STEP_DEADLINE`]. Returns the value and how long the call took.
/// On timeout the helper and whatever it owns are leaked on purpose, since
/// the bridge is wedged and neither releasing nor joining them is safe.
fn bounded<T: Send + 'static>(
    what: &str,
    call: impl FnOnce() -> T + Send + 'static,
) -> (T, Duration) {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let started = Instant::now();
        let value = call();
        let _ = sender.send((value, started.elapsed()));
    });
    receiver
        .recv_timeout(STEP_DEADLINE)
        .unwrap_or_else(|_| panic!("{what} must return in bounded time (#1149)"))
}

/// Runs one runtime-only ABI entry point under [`bounded`] and returns the
/// runtime with the call's status, diagnostic, and duration.
fn call(
    runtime: OwnedRuntime,
    what: &str,
    entry: unsafe extern "C" fn(*mut Runtime, *mut AbiResult) -> Status,
) -> (OwnedRuntime, Status, String, Duration) {
    let ((runtime, (status, message)), elapsed) = bounded(what, move || {
        let mut result = AbiResult::default();
        // SAFETY: The helper thread exclusively owns the live runtime for the
        // call and the output location is unique.
        unsafe { entry(runtime.0.as_ptr(), &mut result) };
        (runtime, release(result))
    });
    (runtime, status, message, elapsed)
}

/// Connects `runtime` to the double and starts the workflow worker through
/// the ABI, then shortens the worker's shutdown bounds.
fn start_worker(runtime: &mut OwnedRuntime, double: &grpc_double::Double) {
    let client = format!(
        r#"{{"target_url":"http://{}","identity":"processing-thread-detach"}}"#,
        double.address
    );
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned; the input stays
    // readable for the whole call.
    unsafe {
        ocaml_temporal_core_v4_client_connect_json(
            runtime.0.as_ptr(),
            client.as_ptr(),
            client.len(),
            &mut result,
        );
    }
    let (status, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");

    let mut result = AbiResult::default();
    // SAFETY: Same exclusive ownership; the static document stays readable.
    unsafe {
        ocaml_temporal_core_v4_worker_start_json(
            runtime.0.as_ptr(),
            WORKFLOW_WORKER.as_ptr(),
            WORKFLOW_WORKER.len(),
            &mut result,
        );
    }
    let (status, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");

    runtime.with_graph(|graph| {
        graph
            .worker
            .as_mut()
            .expect("the worker was started")
            .set_shutdown_bounds_for_test(DRAIN_BOUND, FINALIZE_BOUND);
    });
}

/// Polls the worker through the ABI until its workflow lane reports the
/// forwarded poll error. An activation delivered first is leased and never
/// completed, as by a supervisor whose run loop stopped on the lane error;
/// whether it arrives before the error depends on Core's poll timing, and
/// either way the run stays with the processing thread.
fn wait_for_lane_failure(mut runtime: OwnedRuntime) -> OwnedRuntime {
    let deadline = Instant::now() + STEP_DEADLINE;
    loop {
        let (returned, status, message, _) = call(
            runtime,
            "workflow poll",
            ocaml_temporal_core_v4_worker_try_poll_workflow,
        );
        runtime = returned;
        match status {
            STATUS_OK => {}
            STATUS_NOT_READY if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            STATUS_WORKER => return runtime,
            _ => panic!("the workflow lane never failed: {status} {message}"),
        }
    }
}

/// Shuts the worker down through the ABI and checks that the drain, which
/// cannot finish while the processing thread lives, times out at its bound
/// with the closed diagnostic and keeps the worker in the graph, its ledger
/// unfinalizable, for runtime disposal.
fn shutdown_times_out(runtime: OwnedRuntime) -> OwnedRuntime {
    let (mut runtime, status, message, elapsed) = call(
        runtime,
        "worker shutdown",
        ocaml_temporal_core_v4_worker_shutdown,
    );
    assert_eq!(status, STATUS_WORKER, "{message}");
    assert_eq!(message, DRAIN_TIMEOUT_MESSAGE);
    assert!(
        elapsed >= DRAIN_BOUND,
        "the drain returned before its bound: {elapsed:?}"
    );
    assert!(
        elapsed < DRAIN_BOUND + FINALIZE_BOUND,
        "worker shutdown outlived its bounds: {elapsed:?}"
    );
    runtime.with_graph(|graph| {
        let worker = graph
            .worker
            .as_ref()
            .expect("a timed-out drain keeps the worker in the graph");
        assert!(
            !worker.can_finalize(),
            "a timed-out drain must leave the worker unfinalizable"
        );
    });
    runtime
}

/// Releases `runtime` through `runtime_free`, which disposes any worker
/// still in the graph, and checks that it reports success within
/// [`RELEASE_DEADLINE`] and clears the slot. Consumes the owner.
fn free_runtime(runtime: OwnedRuntime) {
    let ((status, cleared), elapsed) = bounded("runtime free", move || {
        // Move the whole owner into the helper; a closure capturing only
        // its field would capture a non-`Send` pointer.
        let runtime = runtime;
        let mut slot = runtime.0.as_ptr();
        // SAFETY: The helper exclusively owns the live runtime, and the owner
        // is consumed here, so nothing uses the pointer afterwards. Freeing
        // with a live worker is allowed: it disposes the whole graph.
        let status = unsafe { ocaml_temporal_core_v4_runtime_free(&mut slot) };
        (status, slot.is_null())
    });
    assert_eq!(status, STATUS_OK);
    assert!(cleared, "runtime_free must clear the caller's slot");
    assert!(
        elapsed < RELEASE_DEADLINE,
        "runtime free waited for the processing thread: {elapsed:?}"
    );
}

/// A worker with its own runtime, released as `Worker.shutdown_with_report`
/// releases it: `worker_shutdown` returns at the drain bound, and
/// `runtime_free`, which disposes the worker and performs Core's final drop,
/// returns `STATUS_OK` without waiting for the live processing thread, having
/// released Core.
#[test]
fn own_runtime_release_stays_bounded_while_processing_thread_lives() {
    let double = spawn_double("issue-1149-own-runtime");
    let mut slot = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v4_runtime_new(&mut slot, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);
    let mut runtime = OwnedRuntime::published(slot);
    start_worker(&mut runtime, &double);
    let runtime = wait_for_lane_failure(runtime);
    let mut runtime = shutdown_times_out(runtime);
    // The double handed out the workflow task and failed at least one later
    // poll, so Core holds a run whether or not its activation reached the
    // bridge before the lane error.
    assert!(double.received("PollWorkflowTaskQueue") >= 2);

    // The weak reference only observes whether the graph released Core.
    let core = runtime
        .with_graph(|graph| Arc::downgrade(graph.core.as_ref().expect("a live runtime owns Core")));
    free_runtime(runtime);
    assert_eq!(
        core.strong_count(),
        0,
        "runtime free reported completion before releasing Core"
    );
}

/// A worker attached to a shared runtime, released as `Runtime.shutdown`
/// releases it: after the same timed-out `worker_shutdown`, the graph's
/// `runtime_free` disposes the worker and drops only its own reference, and
/// `shared_runtime_free`, which performs Core's final drop, returns
/// `STATUS_OK` within its bound rather than `STATUS_DETACHED`.
#[test]
fn shared_runtime_release_stays_bounded_while_processing_thread_lives() {
    let double = spawn_double("issue-1149-shared-runtime");
    let mut shared_slot: *mut SharedRuntime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v4_shared_runtime_new(2, &mut shared_slot, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);
    let shared = OwnedShared(
        NonNull::new(shared_slot).expect("a successful constructor publishes a shared runtime"),
    );
    let mut slot = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: The shared handle is live; both outputs are exclusively owned.
    unsafe {
        ocaml_temporal_core_v4_runtime_new_attached(shared.0.as_ptr(), &mut slot, &mut result)
    };
    assert_eq!(release(result).0, STATUS_OK);
    let mut runtime = OwnedRuntime::published(slot);
    start_worker(&mut runtime, &double);
    let runtime = wait_for_lane_failure(runtime);
    let runtime = shutdown_times_out(runtime);
    // The double handed out the workflow task and failed at least one later
    // poll, so Core holds a run whether or not its activation reached the
    // bridge before the lane error.
    assert!(double.received("PollWorkflowTaskQueue") >= 2);
    free_runtime(runtime);

    // The weak reference only observes whether the release dropped Core.
    let core = shared.with_handle(|handle| Arc::downgrade(&handle.core));
    let ((status, cleared), elapsed) = bounded("shared runtime free", move || {
        let shared = shared;
        let mut slot = shared.0.as_ptr();
        // SAFETY: The helper exclusively owns the live shared handle, no
        // graph is attached to it any more, and the owner is consumed here.
        let status = unsafe { ocaml_temporal_core_v4_shared_runtime_free(&mut slot) };
        (status, slot.is_null())
    });
    assert_eq!(status, STATUS_OK);
    assert!(cleared, "shared_runtime_free must clear the caller's slot");
    assert!(
        elapsed < RELEASE_DEADLINE,
        "the shared release waited for the processing thread: {elapsed:?}"
    );
    assert_eq!(core.strong_count(), 0, "the shared release kept Core alive");
}

//! End-to-end C ABI coverage of a live worker shutdown whose drain times out
//! (#1229).
//!
//! Included as a child module of `src/abi.rs`, so the test can shorten the
//! worker's shutdown bounds and replace its workflow poll lane with one that
//! never stops, while every lifecycle step still goes through the real C ABI
//! entry points: `runtime_new`, `client_connect_json`, `worker_start_json`,
//! `worker_shutdown`, and `runtime_free`. The lane-level timeout and the
//! result mapping are pinned separately (`replay_bridge.rs`,
//! `worker_shutdown_outcomes.rs`); this test checks that they compose into
//! the documented ABI behaviour.

use super::{
    Result as AbiResult, Runtime, STATUS_OK, STATUS_WORKER, Status, live_worker_tokio_handle,
    ocaml_temporal_core_v4_client_connect_json, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new,
    ocaml_temporal_core_v4_worker_shutdown, ocaml_temporal_core_v4_worker_start_json,
};
use std::ptr;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

#[path = "grpc_double.rs"]
mod grpc_double;

use grpc_double::Reply;

/// Drain bound installed on the worker in place of the 90-second production
/// bound.
const DRAIN_BOUND: Duration = Duration::from_millis(200);

/// Finalize bound installed in place of the 30-second production bound. A
/// timed-out drain never reaches finalization in `shutdown_worker`, and
/// runtime disposal refuses it at once, so this only caps a regression.
const FINALIZE_BOUND: Duration = Duration::from_secs(5);

/// Upper bound for one ABI call under test. Each call here is bounded by
/// [`DRAIN_BOUND`] plus Core's own shutdown signalling, so a call that
/// outlives this is reported as a hang instead of stalling CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// The closed diagnostic `shutdown_worker` reports for a timed-out drain.
const DRAIN_TIMEOUT_MESSAGE: &str =
    "Temporal worker poll lanes did not stop within the shutdown bound";

/// Workflow-only worker document. The double never answers a poll, so the
/// worker has no task and the drain is blocked only by the stuck lane.
const WORKFLOW_WORKER: &[u8] = br#"{
    "namespace":"drain-timeout-test",
    "task_queue":"drain-timeout-test",
    "build_id":"drain-timeout-build",
    "versioning":{"kind":"none"},
    "max_cached_workflows":10,
    "max_outstanding_workflow_tasks":10,
    "max_concurrent_workflow_task_polls":2,
    "graceful_shutdown_timeout_ms":1000,
    "task_types":{"workflows":true,"activities":false}
}"#;

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

/// Raw runtime slot that may move to the bounded call thread. The test
/// thread never touches the runtime while the helper owns the call, which
/// matches the ABI's single-owner rule.
struct RuntimePtr(*mut Runtime);

// SAFETY: Ownership of the runtime pointer moves to exactly one thread at a
// time and comes back through a channel before reuse.
unsafe impl Send for RuntimePtr {}

/// Runs `call` with exclusive ownership of the runtime slot on a helper
/// thread and fails the test if it does not return within [`CALL_DEADLINE`].
/// Returns the slot (null once freed), the call's value, and how long it
/// took. On timeout the helper and its runtime are leaked on purpose: the
/// bridge is wedged, so neither releasing nor joining them is safe.
fn call_bounded<T: Send + 'static>(
    runtime: *mut Runtime,
    what: &str,
    call: impl FnOnce(&mut *mut Runtime) -> T + Send + 'static,
) -> (*mut Runtime, T, Duration) {
    let (sender, receiver) = mpsc::channel();
    let owned = RuntimePtr(runtime);
    let started = Instant::now();
    thread::spawn(move || {
        let mut owned = owned;
        let value = call(&mut owned.0);
        let _ = sender.send((owned, value, started.elapsed()));
    });
    let (owned, value, elapsed) = receiver
        .recv_timeout(CALL_DEADLINE)
        .unwrap_or_else(|_| panic!("{what} must return in bounded time (#1229)"));
    (owned.0, value, elapsed)
}

/// Calls the `worker_shutdown` ABI entry point under [`call_bounded`].
fn shutdown(runtime: *mut Runtime, what: &str) -> (*mut Runtime, Status, String, Duration) {
    let (runtime, (status, message), elapsed) = call_bounded(runtime, what, |runtime| {
        let mut result = AbiResult::default();
        // SAFETY: This thread exclusively owns the live runtime for the call
        // and the output location is unique.
        unsafe { ocaml_temporal_core_v4_worker_shutdown(*runtime, &mut result) };
        release(result)
    });
    (runtime, status, message, elapsed)
}

/// Creates a runtime through the ABI, connects it to `double`, and starts
/// the workflow worker. Returns the exclusively owned runtime.
fn start_worker(double: &grpc_double::Double) -> *mut Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);
    // A successful `runtime_new` writes a live graph through the out-pointer;
    // checking it here keeps every later dereference of `runtime` justified.
    assert!(
        !runtime.is_null(),
        "runtime_new succeeded without a runtime"
    );

    let client = format!(
        r#"{{"target_url":"http://{}","identity":"drain-timeout-test"}}"#,
        double.address
    );
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned; the input stays
    // readable for the whole call.
    unsafe {
        ocaml_temporal_core_v4_client_connect_json(
            runtime,
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

/// A live worker whose workflow lane never stops makes `worker_shutdown`
/// time out at the worker's drain bound. The ABI reports `STATUS_WORKER`
/// with the closed drain-timeout diagnostic, keeps the worker in the graph
/// with its ledger marked as having lost a poll lease, and reports the same
/// failure again on a repeated call instead of the idempotent success of an
/// absent worker. `runtime_free` then still disposes the graph through the
/// last-resort path (bounded drain, refused finalization, drop) and returns
/// `STATUS_OK`, having released Core.
#[test]
fn drain_timeout_keeps_worker_and_runtime_free_still_disposes_it() {
    let double = grpc_double::spawn(Arc::new(|method: &str, _earlier| {
        if method.starts_with("Poll") {
            Reply::Hold
        } else {
            Reply::Message(Vec::new())
        }
    }));
    let runtime = start_worker(&double);

    // Wait until Core's real workflow poller reached the double, so the
    // worker is live before its lane is replaced.
    let deadline = Instant::now() + CALL_DEADLINE;
    while double.received("PollWorkflowTaskQueue") == 0 {
        assert!(
            Instant::now() < deadline,
            "the worker never polled for workflow tasks"
        );
        thread::sleep(Duration::from_millis(10));
    }

    // SAFETY: `start_worker` checked that `runtime` is non-null, and the test
    // thread exclusively owns the live runtime here; no ABI call is in
    // progress. `as_mut` repeats the null check so the borrow never comes
    // from an unchecked pointer.
    let graph = unsafe { runtime.as_mut() }.expect("start_worker returned a live runtime");
    let handle = live_worker_tokio_handle(&graph.core);
    let core = Arc::downgrade(graph.core.as_ref().expect("a live runtime owns Core"));
    let worker = graph.worker.as_mut().expect("the worker was started");
    handle.block_on(worker.install_stuck_workflow_lane_for_test());
    worker.set_shutdown_bounds_for_test(DRAIN_BOUND, FINALIZE_BOUND);

    let (runtime, status, message, elapsed) = shutdown(runtime, "worker shutdown");
    assert_eq!(status, STATUS_WORKER, "{message}");
    assert_eq!(message, DRAIN_TIMEOUT_MESSAGE);
    assert!(
        elapsed >= DRAIN_BOUND,
        "the drain returned before its bound: {elapsed:?}"
    );

    // The worker stays owned by the graph. Its ledger is draining with no
    // outstanding task, so the only thing that can block finalization is the
    // lost poll lease the timed-out drain recorded.
    {
        // SAFETY: Exclusive ownership came back with the call's result, and
        // `worker_shutdown` never frees or replaces the runtime pointer.
        let graph = unsafe { runtime.as_ref() }.expect("worker_shutdown kept the runtime");
        let worker = graph
            .worker
            .as_ref()
            .expect("a timed-out drain keeps the worker in the graph");
        assert!(!worker.has_outstanding_tasks());
        assert!(
            !worker.can_finalize(),
            "a timed-out drain must leave the worker unfinalizable"
        );
    }

    let (runtime, status, message, _) = shutdown(runtime, "repeated worker shutdown");
    assert_eq!(status, STATUS_WORKER, "{message}");
    assert_eq!(message, DRAIN_TIMEOUT_MESSAGE);

    let (runtime, status, _) = call_bounded(runtime, "runtime free", |runtime| {
        // SAFETY: This thread exclusively owns the live runtime slot. Freeing
        // with a live worker is allowed: it disposes the whole graph.
        unsafe { ocaml_temporal_core_v4_runtime_free(runtime) }
    });
    assert_eq!(status, STATUS_OK);
    assert!(runtime.is_null());
    assert_eq!(
        core.strong_count(),
        0,
        "runtime_free reported completion before releasing Core"
    );
}

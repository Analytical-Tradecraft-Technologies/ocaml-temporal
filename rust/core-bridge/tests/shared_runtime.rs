//! Several runtime graphs can share one Core runtime (#832).
//!
//! A shared handle owns one reference to Core and every graph attached to it
//! owns another, so these tests prove that attached graphs really share one
//! Tokio pool, that each graph still runs its own worker independently, and
//! that releasing the shared handle first never invalidates a live graph.
//! Core-destruction counters live in `shared_runtime_cleanup.rs`, a separate
//! test process, so these parallel tests cannot disturb them.

use std::ptr;

use ocaml_temporal_core_bridge::{
    MAX_RUNTIME_WORKER_THREADS, Result as AbiResult, Runtime, STATUS_INVALID_ARGUMENT,
    STATUS_NOT_READY, STATUS_OK, SharedRuntime,
    ocaml_temporal_core_v4_replay_worker_feed_history_json,
    ocaml_temporal_core_v4_replay_worker_start_json,
    ocaml_temporal_core_v4_replay_worker_try_poll_workflow,
    ocaml_temporal_core_v4_replay_worker_wait_workflow, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new_attached,
    ocaml_temporal_core_v4_shared_runtime_free, ocaml_temporal_core_v4_shared_runtime_new,
    test_runtime_worker_threads, test_runtimes_share_core, test_shared_runtime_references,
};

#[path = "support/replay_fixture.rs"]
#[allow(dead_code)] // These tests use only the completed-history fixture.
mod replay_fixture;

/// Releases one bridge result and asserts the release itself succeeded.
fn free_result(result: &mut AbiResult) {
    // SAFETY: The result was initialized by the bridge and is freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(result) },
        STATUS_OK
    );
}

/// Creates a shared handle with `worker_threads`, panicking on failure.
fn new_shared(worker_threads: u32) -> *mut SharedRuntime {
    let mut shared: *mut SharedRuntime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    let status = unsafe {
        ocaml_temporal_core_v4_shared_runtime_new(worker_threads, &mut shared, &mut result)
    };
    free_result(&mut result);
    assert_eq!(status, STATUS_OK);
    assert!(!shared.is_null());
    shared
}

/// Attaches one graph to `shared`, returning its status and (possibly null)
/// handle.
fn attach(shared: *const SharedRuntime) -> (i32, *mut Runtime) {
    let mut runtime: *mut Runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: `shared` is null or live; both outputs are writable.
    let status =
        unsafe { ocaml_temporal_core_v4_runtime_new_attached(shared, &mut runtime, &mut result) };
    free_result(&mut result);
    (status, runtime)
}

/// Releases a graph through its owning slot and asserts the slot is cleared.
fn free_runtime(runtime: &mut *mut Runtime) {
    // SAFETY: The slot holds a live handle or null and is used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

/// Releases a shared handle through its owning slot.
fn free_shared(shared: &mut *mut SharedRuntime) {
    // SAFETY: The slot holds a live handle or null and is used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(shared) },
        STATUS_OK
    );
    assert!(shared.is_null());
}

/// Starts a replay worker on `runtime`, feeds one completed history, and
/// waits until Core leases its first activation. This drives real Core
/// worker work through the graph's (possibly shared) Tokio pool without a
/// Temporal server.
fn lease_replay_activation(runtime: *mut Runtime, workflow_id: &str) {
    let mut result = AbiResult::default();
    let config = br#"{"namespace":"default","task_queue":"replay","build_id":"shared-runtime-test","versioning":{"kind":"none"},"max_cached_workflows":0,"max_outstanding_workflow_tasks":1,"max_concurrent_workflow_task_polls":1,"graceful_shutdown_timeout_ms":1000}"#;
    // SAFETY: `runtime` is a live handle used only by this thread.
    let status = unsafe {
        ocaml_temporal_core_v4_replay_worker_start_json(
            runtime,
            config.as_ptr(),
            config.len(),
            &mut result,
        )
    };
    free_result(&mut result);
    assert_eq!(status, STATUS_OK);

    let history = replay_fixture::complete_history_document(workflow_id);
    // SAFETY: As above; the history bytes outlive the call.
    let status = unsafe {
        ocaml_temporal_core_v4_replay_worker_feed_history_json(
            runtime,
            history.as_ptr(),
            history.len(),
            &mut result,
        )
    };
    free_result(&mut result);
    assert_eq!(status, STATUS_OK);

    for _ in 0..20 {
        // SAFETY: As above.
        let wait =
            unsafe { ocaml_temporal_core_v4_replay_worker_wait_workflow(runtime, &mut result) };
        free_result(&mut result);
        assert!(wait == STATUS_OK || wait == STATUS_NOT_READY);
        // SAFETY: As above.
        let poll =
            unsafe { ocaml_temporal_core_v4_replay_worker_try_poll_workflow(runtime, &mut result) };
        let leased = poll == STATUS_OK && !result.value.ptr.is_null();
        free_result(&mut result);
        assert!(poll == STATUS_OK || poll == STATUS_NOT_READY);
        if leased {
            return;
        }
    }
    panic!("replay activation for {workflow_id} was not leased");
}

/// Two graphs attached to one handle run on the same Core and Tokio pool,
/// and each drives its own worker independently.
#[test]
fn attached_graphs_share_one_core_and_run_independent_workers() {
    let mut shared = new_shared(2);
    let (status, mut first) = attach(shared);
    assert_eq!(status, STATUS_OK);
    let (status, mut second) = attach(shared);
    assert_eq!(status, STATUS_OK);

    // SAFETY: All handles are live and used only by this thread.
    unsafe {
        assert_eq!(test_runtimes_share_core(first, second), Some(true));
        assert_eq!(test_shared_runtime_references(shared), Some(3));
        assert_eq!(test_runtime_worker_threads(first), Some(2));
        assert_eq!(test_runtime_worker_threads(second), Some(2));
    }

    lease_replay_activation(first, "shared-first");
    lease_replay_activation(second, "shared-second");

    // Releasing one graph (which defensively disposes its leased replay
    // worker) leaves the other graph and the shared Core untouched.
    free_runtime(&mut first);
    // SAFETY: `shared` and `second` remain live.
    unsafe {
        assert_eq!(test_shared_runtime_references(shared), Some(2));
        assert_eq!(test_runtime_worker_threads(second), Some(2));
    }
    free_runtime(&mut second);
    // SAFETY: `shared` remains live.
    assert_eq!(unsafe { test_shared_runtime_references(shared) }, Some(1));
    free_shared(&mut shared);
}

/// Releasing the shared handle before its graphs cannot free Core under
/// them: the graphs keep their own references and remain fully usable.
#[test]
fn releasing_the_shared_handle_first_keeps_attached_graphs_alive() {
    let mut shared = new_shared(1);
    let (status, mut runtime) = attach(shared);
    assert_eq!(status, STATUS_OK);
    free_shared(&mut shared);

    // SAFETY: `runtime` is live and used only by this thread.
    assert_eq!(unsafe { test_runtime_worker_threads(runtime) }, Some(1));
    lease_replay_activation(runtime, "shared-released-first");
    free_runtime(&mut runtime);
}

/// Separately created graphs never share Core by accident.
#[test]
fn graphs_on_different_shared_handles_do_not_share_core() {
    let mut left_shared = new_shared(1);
    let mut right_shared = new_shared(1);
    let (_, mut left) = attach(left_shared);
    let (_, mut right) = attach(right_shared);
    // SAFETY: All handles are live and used only by this thread.
    assert_eq!(
        unsafe { test_runtimes_share_core(left, right) },
        Some(false)
    );
    free_runtime(&mut left);
    free_runtime(&mut right);
    free_shared(&mut left_shared);
    free_shared(&mut right_shared);
}

/// Invalid pointers and counts fail with typed statuses and leave every
/// output slot null; release is idempotent on a cleared slot.
#[test]
fn invalid_shared_runtime_arguments_are_rejected() {
    let mut shared: *mut SharedRuntime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    let status = unsafe {
        ocaml_temporal_core_v4_shared_runtime_new(
            MAX_RUNTIME_WORKER_THREADS + 1,
            &mut shared,
            &mut result,
        )
    };
    free_result(&mut result);
    assert_eq!(status, STATUS_INVALID_ARGUMENT);
    assert!(shared.is_null());

    // SAFETY: A null slot pointer is rejected before any write.
    let status =
        unsafe { ocaml_temporal_core_v4_shared_runtime_new(0, ptr::null_mut(), &mut result) };
    free_result(&mut result);
    assert_eq!(status, STATUS_INVALID_ARGUMENT);

    let (status, runtime) = attach(ptr::null());
    assert_eq!(status, STATUS_INVALID_ARGUMENT);
    assert!(runtime.is_null());

    let mut shared = new_shared(1);
    free_shared(&mut shared);
    // The slot is now null, so a second release is a no-op and an attach
    // through it is a null-handle error rather than a use-after-free.
    free_shared(&mut shared);
    let (status, runtime) = attach(shared);
    assert_eq!(status, STATUS_INVALID_ARGUMENT);
    assert!(runtime.is_null());
    // SAFETY: A null slot pointer is rejected without dereferencing it.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
}

use std::ptr;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Result as AbiResult, STATUS_NOT_READY, STATUS_OK,
    ocaml_temporal_core_v3_replay_worker_feed_history_json,
    ocaml_temporal_core_v3_replay_worker_start_json,
    ocaml_temporal_core_v3_replay_worker_try_poll_workflow,
    ocaml_temporal_core_v3_replay_worker_wait_workflow, ocaml_temporal_core_v3_result_free,
    ocaml_temporal_core_v3_runtime_dispose, ocaml_temporal_core_v3_runtime_new,
    test_runtime_cleanup_counts,
};

#[path = "support/replay_fixture.rs"]
#[allow(dead_code)] // This test uses only the completed-history fixture.
mod replay_fixture;

/// Waits for the process-local asynchronous runtime destructor to finish.
/// This test file has one test so another test cannot satisfy its counters.
fn await_cleanup(created_before: u64, cleaned_before: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (created, cleaned) = test_runtime_cleanup_counts();
        if created == created_before + 1 && cleaned == cleaned_before + 1 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "runtime cleanup did not complete: created {created_before}->{created}, cleaned {cleaned_before}->{cleaned}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Proves the nonblocking finalizer path eventually runs Core's destructor.
///
/// This lives in its own integration-test process so no parallel runtime test
/// can advance the process-local counters and accidentally satisfy the check.
#[test]
fn asynchronous_disposal_completes_without_leaking_core() {
    let (created_before, cleaned_before) = test_runtime_cleanup_counts();
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();

    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_dispose(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());

    await_cleanup(created_before, cleaned_before);

    // Repeat with a leased replay activation. Unlike an empty runtime, this
    // forces the fallback cleanup thread to retire Core's completion debt
    // before it can release the parent Tokio runtime.
    let (created_before, cleaned_before) = test_runtime_cleanup_counts();
    let mut replay_runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_new(&mut replay_runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_result_free(&mut result) },
        STATUS_OK
    );
    let config = br#"{"namespace":"default","task_queue":"replay","build_id":"cleanup-test","versioning":{"kind":"none"},"max_cached_workflows":0,"max_outstanding_workflow_tasks":1,"max_concurrent_workflow_task_polls":1,"graceful_shutdown_timeout_ms":1000}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v3_replay_worker_start_json(
                replay_runtime,
                config.as_ptr(),
                config.len(),
                &mut result,
            )
        },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_result_free(&mut result) },
        STATUS_OK
    );
    let history = replay_fixture::complete_history_document("cleanup-replay");
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v3_replay_worker_feed_history_json(
                replay_runtime,
                history.as_ptr(),
                history.len(),
                &mut result,
            )
        },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_result_free(&mut result) },
        STATUS_OK
    );
    let mut leased = false;
    for _ in 0..20 {
        let wait = unsafe {
            ocaml_temporal_core_v3_replay_worker_wait_workflow(replay_runtime, &mut result)
        };
        assert!(wait == STATUS_OK || wait == STATUS_NOT_READY);
        assert_eq!(
            unsafe { ocaml_temporal_core_v3_result_free(&mut result) },
            STATUS_OK
        );
        let poll = unsafe {
            ocaml_temporal_core_v3_replay_worker_try_poll_workflow(replay_runtime, &mut result)
        };
        assert!(poll == STATUS_OK || poll == STATUS_NOT_READY);
        if poll == STATUS_OK {
            assert!(!result.value.ptr.is_null());
            leased = true;
        }
        assert_eq!(
            unsafe { ocaml_temporal_core_v3_result_free(&mut result) },
            STATUS_OK
        );
        if leased {
            break;
        }
    }
    assert!(leased, "replay activation was not leased before disposal");
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_dispose(&mut replay_runtime) },
        STATUS_OK
    );
    assert!(replay_runtime.is_null());
    await_cleanup(created_before, cleaned_before);
}

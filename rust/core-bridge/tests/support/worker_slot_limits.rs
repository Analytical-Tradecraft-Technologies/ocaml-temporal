//! Regression for cache-limited workflow permits and Core poller balancing.

use super::{STATUS_CONFIGURATION, WorkerConfigInput, WorkerVersioningInput};

/// Constructs the same one-slot cache/default task capacity used by the live
/// eviction worker, while allowing boundary cases without a Temporal server.
fn config(cache: u32, tasks: u32) -> WorkerConfigInput {
    WorkerConfigInput {
        namespace: "slot-limit-test".to_owned(),
        task_queue: "slot-limit-test".to_owned(),
        build_id: "slot-limit-test".to_owned(),
        versioning: WorkerVersioningInput::None,
        max_cached_workflows: cache,
        max_outstanding_workflow_tasks: tasks,
        max_concurrent_workflow_task_polls: 2,
        graceful_shutdown_timeout_ms: 1_000,
    }
}

/// Returns the workflow-task capacity Core will see. The bridge passes slot
/// limits through a tuner (so remote and local activities can share one pool),
/// which leaves `max_outstanding_workflow_tasks` unset.
fn workflow_slots(core: &temporalio_sdk_core::WorkerConfig) -> Option<usize> {
    assert_eq!(core.max_outstanding_workflow_tasks, None);
    core.tuner
        .as_ref()
        .expect("bridge workers configure a slot tuner")
        .workflow_task_slot_supplier()
        .available_slots()
}

/// Core independently caps cache-enabled permit acquisition. Its poller
/// balancer must see that same capacity, or idle sticky polls can reserve the
/// permits required to poll a new workflow from the normal queue.
#[test]
fn small_cache_advertises_its_effective_task_capacity() {
    for (cache, expected) in [(1, 2), (2, 2), (3, 3), (100, 100)] {
        let core = config(cache, 1_000).into_core().expect("valid worker");
        assert_eq!(core.max_cached_workflows, cache as usize);
        assert_eq!(workflow_slots(&core), Some(expected));
    }
}

/// A caller's stricter task limit remains authoritative; disabling caching
/// must not introduce an unrelated concurrency limit.
#[test]
fn explicit_smaller_and_uncached_limits_are_preserved() {
    for (cache, tasks) in [(100, 2), (0, 1), (0, 1_000), (1_000, 1_000)] {
        let core = config(cache, tasks).into_core().expect("valid worker");
        assert_eq!(workflow_slots(&core), Some(tasks as usize));
    }
}

/// Normalization must not hide invalid input by increasing it to two slots.
#[test]
fn cached_single_task_capacity_still_fails_validation() {
    let error = config(1, 1)
        .into_core()
        .err()
        .expect("cached workers require two slots");
    assert_eq!(error.status, STATUS_CONFIGURATION);
}

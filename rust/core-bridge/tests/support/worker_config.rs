//! Worker-config validation and its conversion into Core worker and
//! versioning options.
//!
//! Mounted from `abi.rs` with `#[path]` rather than built as an
//! integration test because [`super::WorkerConfigInput`] and its Core
//! conversion are private to the ABI module.

use super::{
    MIN_CACHED_WORKFLOW_POLLS, STATUS_CONFIGURATION, VersioningBehavior, WorkerConfigInput,
};

/// Builds a complete worker document while varying only workflow poller
/// concurrency, so the Core cache invariant is isolated from other fields.
fn config(pollers: u32) -> WorkerConfigInput {
    WorkerConfigInput {
        namespace: "temporal-sdk-test".to_owned(),
        task_queue: "worker-config-test".to_owned(),
        build_id: "worker-config-test".to_owned(),
        versioning: super::WorkerVersioningInput::None,
        max_cached_workflows: 100,
        max_outstanding_workflow_tasks: 100,
        max_concurrent_workflow_task_polls: pollers,
        graceful_shutdown_timeout_ms: 1_000,
        task_types: Default::default(),
        tuning: Default::default(),
    }
}

/// Returns the configuration failure of `worker`, panicking with `why`
/// when the configuration is unexpectedly accepted.
fn rejected(worker: WorkerConfigInput, why: &str) -> super::Failure {
    match worker.into_core() {
        Err(failure) => failure,
        Ok(_) => panic!("{why}"),
    }
}

/// A deployment-based worker named `agents` on the top-level build ID,
/// with the given worker-versioning switch and default behavior.
fn deployment(use_worker_versioning: bool, behavior: Option<&str>) -> WorkerConfigInput {
    let mut worker = config(MIN_CACHED_WORKFLOW_POLLS);
    worker.versioning = super::WorkerVersioningInput::DeploymentBased {
        deployment_name: "agents".to_owned(),
        build_id: worker.build_id.clone(),
        use_worker_versioning,
        default_versioning_behavior: behavior.map(str::to_owned),
    };
    worker
}

/// Rejects the configuration that Core would otherwise report only after
/// a live worker startup, keeping the public boundary deterministic.
#[test]
fn cached_workflows_require_two_workflow_pollers() {
    let failure = rejected(
        config(MIN_CACHED_WORKFLOW_POLLS - 1),
        "cached workflows must require two workflow pollers",
    );
    assert_eq!(failure.status, STATUS_CONFIGURATION);
    assert!(
        failure
            .message
            .contains("max_concurrent_workflow_task_polls must be at least 2")
    );
}

/// Core must never be given more activity slots than the serial OCaml
/// executor can run (#777); surplus slots let the server start tasks
/// whose timeouts then expire while they wait in the bridge queue.
#[test]
fn activity_slots_match_serial_executor() {
    let core = config(MIN_CACHED_WORKFLOW_POLLS)
        .into_core()
        .expect("default worker configuration should be valid");
    assert_eq!(core.max_outstanding_activities, Some(1));
    assert_eq!(core.max_outstanding_local_activities, Some(1));
}

/// Accepts the public default poller count before any runtime or network
/// resource is allocated.
#[test]
fn cached_workflows_accept_two_workflow_pollers() {
    config(MIN_CACHED_WORKFLOW_POLLS)
        .into_core()
        .expect("two workflow pollers should satisfy Core's cache invariant");
}

/// Rejects a NUL before Core construction so the namespace retained for
/// child-failure metadata can never become invalid after worker startup.
#[test]
fn worker_identifiers_reject_nul() {
    let mut worker = config(MIN_CACHED_WORKFLOW_POLLS);
    worker.namespace.push('\0');
    let failure = rejected(worker, "worker identifiers must reject NUL");
    assert_eq!(failure.status, STATUS_CONFIGURATION);
    assert!(failure.message.contains("namespace must not contain NUL"));
}

/// Maps the explicit legacy mode to Core's build-ID strategy instead of
/// merely retaining the build ID as metadata.
#[test]
fn legacy_build_id_selects_core_versioning_strategy() {
    let mut worker = config(MIN_CACHED_WORKFLOW_POLLS);
    worker.versioning = super::WorkerVersioningInput::LegacyBuildId {
        build_id: worker.build_id.clone(),
    };
    let core = worker
        .into_core()
        .expect("matching legacy build ID should be accepted");
    assert!(matches!(
        core.versioning_strategy,
        temporalio_sdk_core::WorkerVersioningStrategy::LegacyBuildIdBased { .. }
    ));
}

/// Rejects a mismatched repeated build ID before Core construction so the
/// server cannot receive contradictory worker routing metadata.
#[test]
fn legacy_build_id_must_match_top_level_build_id() {
    let mut worker = config(MIN_CACHED_WORKFLOW_POLLS);
    worker.versioning = super::WorkerVersioningInput::LegacyBuildId {
        build_id: "different-build".to_owned(),
    };
    let failure = rejected(worker, "mismatched legacy build IDs must fail closed");
    assert_eq!(failure.status, STATUS_CONFIGURATION);
    assert_eq!(failure.message, "versioning.build_id must match build_id");
}

/// Maps deployment-based routing to Core's modern worker deployment
/// strategy and preserves the pinned default behavior used by workflows
/// that do not specify an explicit versioning option.
#[test]
fn deployment_based_selects_core_versioning_strategy() {
    let worker = deployment(true, Some("pinned"));
    let core = worker
        .into_core()
        .expect("valid deployment routing should be accepted");
    match core.versioning_strategy {
        temporalio_sdk_core::WorkerVersioningStrategy::WorkerDeploymentBased(options) => {
            assert_eq!(options.version.deployment_name, "agents");
            assert_eq!(options.version.build_id, "worker-config-test");
            assert!(options.use_worker_versioning);
            assert_eq!(
                options.default_versioning_behavior,
                Some(VersioningBehavior::Pinned)
            );
        }
        other => panic!("unexpected Core versioning strategy: {other:?}"),
    }
}

/// Rejects a deployment behavior when worker versioning is disabled; this
/// avoids silently carrying a policy that Core would ignore.
#[test]
fn deployment_behavior_requires_worker_versioning() {
    let worker = deployment(false, Some("auto_upgrade"));
    let failure = rejected(
        worker,
        "disabled deployment versioning must reject a default behavior",
    );
    assert_eq!(failure.status, STATUS_CONFIGURATION);
    assert!(
        failure
            .message
            .contains("default_versioning_behavior requires use_worker_versioning")
    );
}

/// Rejects an enabled versioned worker without a default behavior. The
/// bridge never sends a per-workflow behavior, so Core would otherwise
/// stamp UNSPECIFIED on every workflow-task completion of a versioned
/// deployment (issue #817).
#[test]
fn worker_versioning_requires_default_behavior() {
    let worker = deployment(true, None);
    let failure = rejected(
        worker,
        "versioned workers must not complete with UNSPECIFIED behavior",
    );
    assert_eq!(failure.status, STATUS_CONFIGURATION);
    assert_eq!(
        failure.message,
        "versioning.use_worker_versioning requires default_versioning_behavior"
    );
}

/// Keeps the opted-out deployment form valid: without worker versioning
/// Core does not route by deployment, so UNSPECIFIED is the correct
/// completion behavior and no default may be supplied.
#[test]
fn deployment_without_worker_versioning_accepts_no_behavior() {
    let worker = deployment(false, None);
    let core = worker
        .into_core()
        .expect("unversioned deployment metadata should be accepted");
    match core.versioning_strategy {
        temporalio_sdk_core::WorkerVersioningStrategy::WorkerDeploymentBased(options) => {
            assert!(!options.use_worker_versioning);
            assert_eq!(options.default_versioning_behavior, None);
        }
        other => panic!("unexpected Core versioning strategy: {other:?}"),
    }
}

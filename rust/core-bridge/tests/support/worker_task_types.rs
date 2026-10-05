//! Worker-config regressions for registration-derived Core task types (#805).
//!
//! A worker that cannot execute a task kind must not poll it from a shared
//! task queue. These tests pin the private JSON field and its mapping onto the
//! Core configuration that decides which pollers exist.

use super::{STATUS_CONFIGURATION, WorkerConfigInput, decode_config};

/// Builds a complete worker document whose only variable part is the
/// `task_types` member, rendered verbatim so omission can be tested too.
fn document(task_types: Option<&str>) -> String {
    let task_types = task_types
        .map(|value| format!(",\"task_types\":{value}"))
        .unwrap_or_default();
    format!(
        "{{\"namespace\":\"task-types-test\",\"task_queue\":\"task-types-test\",\
         \"build_id\":\"task-types-test\",\"versioning\":{{\"kind\":\"none\"}},\
         \"max_cached_workflows\":100,\"max_outstanding_workflow_tasks\":100,\
         \"max_concurrent_workflow_task_polls\":2,\
         \"graceful_shutdown_timeout_ms\":1000{task_types}}}"
    )
}

/// Decodes one document through the same strict decoder as the C ABI.
fn decode(task_types: Option<&str>) -> Result<WorkerConfigInput, super::Failure> {
    let text = document(task_types);
    // SAFETY: `text` owns `text.len()` initialized bytes for the whole call.
    unsafe { decode_config::<WorkerConfigInput>(text.as_ptr(), text.len()) }
}

/// Decodes and converts one document into the Core configuration.
fn core_task_types(
    task_types: Option<&str>,
) -> Result<temporalio_common::worker::WorkerTaskTypes, super::Failure> {
    decode(task_types)?
        .into_core()
        .map(|config| config.task_types)
}

/// A document written before the field existed keeps polling every kind the
/// bridge supports, so older private callers do not silently lose a lane.
#[test]
fn omitted_task_types_poll_workflows_and_activities() {
    let task_types = core_task_types(None).expect("legacy document is valid");
    assert!(task_types.enable_workflows);
    assert!(task_types.enable_local_activities);
    assert!(task_types.enable_remote_activities);
    assert!(!task_types.enable_nexus);
}

/// A workflow-only registration must never take remote activity tasks that a
/// sibling activity worker on the same queue could execute. Local activities
/// stay enabled because Core dispatches them in-process from this worker's
/// own workflows rather than polling the server for them.
#[test]
fn workflow_only_registration_disables_remote_activity_polling() {
    let task_types = core_task_types(Some(r#"{"workflows":true,"activities":false}"#))
        .expect("workflow-only document is valid");
    assert!(task_types.enable_workflows);
    assert!(task_types.enable_local_activities);
    assert!(!task_types.enable_remote_activities);
    assert!(!task_types.enable_nexus);
}

/// An activity-only registration must not take workflow tasks, and Core
/// forbids local activities without workflows.
#[test]
fn activity_only_registration_disables_workflow_polling() {
    let task_types = core_task_types(Some(r#"{"workflows":false,"activities":true}"#))
        .expect("activity-only document is valid");
    assert!(!task_types.enable_workflows);
    assert!(!task_types.enable_local_activities);
    assert!(task_types.enable_remote_activities);
    assert!(!task_types.enable_nexus);
}

/// A worker with nothing to execute is rejected with a bridge-owned
/// configuration error before Core or the network is involved.
#[test]
fn empty_registration_is_rejected() {
    let failure = match core_task_types(Some(r#"{"workflows":false,"activities":false}"#)) {
        Err(failure) => failure,
        Ok(_) => panic!("a worker without task types must be rejected"),
    };
    assert_eq!(failure.status, STATUS_CONFIGURATION);
    assert_eq!(
        failure.message,
        "task_types must enable workflows or activities"
    );
}

/// The member is as strict as the rest of the document: unknown or missing
/// keys fail decoding instead of defaulting to a poll-everything worker.
#[test]
fn task_types_member_is_strict() {
    for value in [
        r#"{"workflows":true,"activities":true,"nexus":false}"#,
        r#"{"workflows":true}"#,
        r#"{"activities":true}"#,
        r#"{"workflows":"yes","activities":true}"#,
        "null",
    ] {
        assert!(
            decode(Some(value)).is_err(),
            "task_types value {value} must be rejected"
        );
    }
}

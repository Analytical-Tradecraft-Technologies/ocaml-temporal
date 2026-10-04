use super::*;
use crate::activity_protocol::{
    ActivityCancel, ActivityCancelReason, ActivityStart, ActivityTask, ActivityTaskVariant,
};
use crate::workflow_protocol::{Activation, Timestamp, WorkflowExecution};
use temporalio_common::protos::coresdk::activity_task as core_activity_task;
use temporalio_common::protos::coresdk::activity_task::ActivityTask as CoreActivityTask;

/// Builds the smallest ordinary activation needed to test retained identity
/// without involving Temporal Core or a network worker.
fn activation(run_id: &str, history_length: u32) -> Activation {
    Activation {
        run_id: run_id.to_owned(),
        timestamp: Some(Timestamp {
            seconds: 1,
            nanoseconds: 0,
        }),
        is_replaying: false,
        history_length,
        jobs: Vec::new(),
        metadata: None,
    }
}

/// Builds a valid start task that shares its opaque token with cancellation
/// updates in the rejection-ledger regression below.
fn start_activity_task() -> ActivityTask {
    ActivityTask {
        task_token: "AAEC".to_owned(),
        variant: ActivityTaskVariant::Start(Box::new(ActivityStart {
            workflow_namespace: "default".to_owned(),
            workflow_type: "example.workflow".to_owned(),
            workflow_execution: WorkflowExecution {
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
            },
            activity_id: "activity-1".to_owned(),
            activity_type: "example.activity".to_owned(),
            is_local: false,
            header_fields: std::collections::BTreeMap::new(),
            input: Vec::new(),
            heartbeat_details: Vec::new(),
            scheduled_time: None,
            current_attempt_scheduled_time: None,
            started_time: None,
            attempt: 1,
            schedule_to_close_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            retry_policy: None,
            priority: None,
            standalone_run_id: String::new(),
        })),
    }
}

/// Exact workflow JSON proves both correlation identity and immutable semantic
/// content; altering either must leave the pending value intact.
#[test]
fn workflow_rejection_requires_the_complete_retained_document() {
    let retained = activation("run-1", 1);
    let mut pending = HashMap::new();
    retain_workflow_activation(&mut pending, retained.clone()).expect("first activation");

    let changed_id = workflow_protocol::encode_activation(&activation("run-2", 1))
        .expect("changed identifier encodes");
    assert_eq!(
        workflow_rejection_run_id(&pending, changed_id.as_bytes())
            .expect_err("changed run ID must not correlate")
            .status,
        STATUS_PROTOCOL
    );
    let changed_content = workflow_protocol::encode_activation(&activation("run-1", 2))
        .expect("changed semantic content encodes");
    assert_eq!(
        workflow_rejection_run_id(&pending, changed_content.as_bytes())
            .expect_err("changed activation must not correlate")
            .status,
        STATUS_PROTOCOL
    );
    assert_eq!(pending.get("run-1"), Some(&retained));

    let exact = workflow_protocol::encode_activation(&retained).expect("retained value encodes");
    assert_eq!(
        workflow_rejection_run_id(&pending, exact.as_bytes()).expect("exact value correlates"),
        "run-1"
    );
}

/// A duplicate poll reports an invariant failure but cannot overwrite the
/// activation document against which completion or rejection is checked.
#[test]
fn duplicate_workflow_poll_preserves_the_original_activation() {
    let original = activation("run-1", 1);
    let mut pending = HashMap::new();
    retain_workflow_activation(&mut pending, original.clone()).expect("first activation");

    assert_eq!(
        retain_workflow_activation(&mut pending, activation("run-1", 2))
            .expect_err("duplicate must fail")
            .status,
        STATUS_INTERNAL
    );
    assert_eq!(pending.get("run-1"), Some(&original));
}

/// Activity rejection requires the full retained document, so neither a
/// changed token nor changed cancellation content can reach the ledger.
#[test]
fn activity_rejection_requires_the_complete_retained_document() {
    let task = ActivityTask {
        task_token: "AAEC".to_owned(),
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::WorkerShutdown,
            details: None,
        }),
    };
    let mut pending = HashMap::new();
    retain_activity_task(&mut pending, vec![0, 1, 2], task.clone());
    let encoded = activity_protocol::encode_task(&task).expect("activity task encodes");

    assert_eq!(
        activity_rejection_token(&pending, encoded.as_bytes()).expect("exact task correlates"),
        vec![0, 1, 2]
    );

    let changed_token = ActivityTask {
        task_token: "AwQF".to_owned(),
        ..task.clone()
    };
    let changed_token =
        activity_protocol::encode_task(&changed_token).expect("changed token encodes");
    assert_eq!(
        activity_rejection_token(&pending, changed_token.as_bytes())
            .expect_err("changed token must not correlate")
            .status,
        STATUS_PROTOCOL
    );

    let changed_content = ActivityTask {
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::Cancelled,
            details: None,
        }),
        ..task
    };
    let changed_content =
        activity_protocol::encode_task(&changed_content).expect("changed task encodes");
    assert_eq!(
        activity_rejection_token(&pending, changed_content.as_bytes())
            .expect_err("changed task content must not correlate")
            .status,
        STATUS_PROTOCOL
    );
    assert_eq!(pending.get(&vec![0, 1, 2]).map(Vec::len), Some(1));

    let cancellation_update = ActivityTask {
        task_token: "AAEC".to_owned(),
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::Cancelled,
            details: None,
        }),
    };
    retain_activity_task(&mut pending, vec![0, 1, 2], cancellation_update.clone());
    let cancellation_update =
        activity_protocol::encode_task(&cancellation_update).expect("cancellation update encodes");
    assert_eq!(
        activity_rejection_token(&pending, cancellation_update.as_bytes())
            .expect("retained update correlates"),
        vec![0, 1, 2]
    );
    assert_eq!(pending.get(&vec![0, 1, 2]).map(Vec::len), Some(2));

    // Cancellation updates share one Temporal completion debt with the start
    // task, so retirement must clear every semantic document for that token.
    retire_activity_semantics(&mut pending, &[0, 1, 2]);
    assert!(pending.is_empty());
}

/// Rejecting a cancellation update removes only that document and leaves the
/// shared Start document available for its eventual completion.
#[test]
fn cancellation_rejection_preserves_shared_start_document() {
    let start = start_activity_task();
    let cancellation = ActivityTask {
        task_token: "AAEC".to_owned(),
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::WorkerShutdown,
            details: None,
        }),
    };
    let mut pending = HashMap::new();
    retain_activity_task(&mut pending, vec![0, 1, 2], start.clone());
    retain_activity_task(&mut pending, vec![0, 1, 2], cancellation.clone());

    let encoded =
        activity_protocol::encode_task(&cancellation).expect("cancellation update encodes");
    let rejection = activity_rejection_task(&pending, encoded.as_bytes())
        .expect("retained cancellation update correlates");
    assert!(matches!(
        &rejection.task.variant,
        ActivityTaskVariant::Cancel(_)
    ));
    retire_activity_semantic(&mut pending, &rejection.task_token, &rejection.task);

    let retained = pending
        .get(&vec![0, 1, 2])
        .expect("the shared start remains leased");
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0], start);
}

/// A retained cancellation update is the ABI handoff that carries heartbeat
/// response state to OCaml.  Matching the complete document here proves that
/// cancel, pause, and reset facts remain independently observable while the
/// update shares the Start task's single completion debt.
#[test]
fn cancellation_rejection_retains_all_heartbeat_flags() {
    let cancellation = ActivityTask {
        task_token: "AAEC".to_owned(),
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::Cancelled,
            details: Some(crate::activity_protocol::ActivityCancellationDetails {
                is_not_found: false,
                is_cancelled: true,
                is_paused: true,
                is_timed_out: false,
                is_worker_shutdown: false,
                is_reset: true,
            }),
        }),
    };
    let mut pending = HashMap::new();
    retain_activity_task(&mut pending, vec![0, 1, 2], cancellation.clone());

    let encoded =
        activity_protocol::encode_task(&cancellation).expect("flagged cancellation update encodes");
    let rejection = activity_rejection_task(&pending, encoded.as_bytes())
        .expect("flagged cancellation update correlates");
    assert_eq!(rejection.task, cancellation);
    let ActivityTaskVariant::Cancel(cancel) = rejection.task.variant else {
        panic!("retained cancellation must remain a Cancel variant");
    };
    let details = cancel.details.expect("Core flags must remain present");
    assert!(details.is_cancelled);
    assert!(details.is_paused);
    assert!(details.is_reset);
    assert!(!details.is_not_found);
    assert!(!details.is_timed_out);
    assert!(!details.is_worker_shutdown);
}

/// Conversion failures from the native poll path classify Cancel as a
/// non-owning update, while an unrepresentable Start still owns the lease that
/// must be failed before the worker can shut down.
#[test]
fn conversion_failure_classifies_cancel_without_start_completion_debt() {
    let malformed_cancel = CoreActivityTask {
        task_token: vec![0, 1, 2],
        variant: Some(core_activity_task::activity_task::Variant::Cancel(
            core_activity_task::Cancel {
                reason: i32::MAX,
                details: None,
            },
        )),
    };
    assert!(
        activity_protocol::task_from_core(&malformed_cancel).is_err(),
        "unknown cancellation reasons must fail semantic conversion"
    );
    assert!(!activity_task_owns_completion_debt(&malformed_cancel));

    let unrepresentable_start = CoreActivityTask {
        task_token: vec![0, 1, 2],
        variant: Some(core_activity_task::activity_task::Variant::Start(
            core_activity_task::Start {
                workflow_namespace: "default".to_owned(),
                workflow_type: "example.workflow".to_owned(),
                workflow_execution: Some(
                    temporalio_common::protos::temporal::api::common::v1::WorkflowExecution {
                        workflow_id: "workflow-1".to_owned(),
                        run_id: "run-1".to_owned(),
                    },
                ),
                activity_id: "activity-1".to_owned(),
                activity_type: "example.activity".to_owned(),
                is_local: true,
                ..Default::default()
            },
        )),
    };
    assert!(
        activity_protocol::task_from_core(&unrepresentable_start).is_ok(),
        "local activity tasks must be admitted by semantic conversion"
    );
    assert!(activity_task_owns_completion_debt(&unrepresentable_start));

    let missing_variant = CoreActivityTask {
        task_token: vec![0, 1, 2],
        variant: None,
    };
    assert!(activity_task_owns_completion_debt(&missing_variant));
}

//! Unit regressions for the signal-with-start and update-with-start codec
//! (#1151). They exercise private helpers of
//! `src/client_protocol/with_start.rs`: how a multi-operation gRPC failure
//! maps to the typed error of the operation that actually failed, how plain
//! statuses keep their own code, and which error kinds the update-with-start
//! outcome document rejects. The module is compiled into the crate through a
//! `#[path]` declaration so production code and tests stay in separate files.

use super::*;
use temporalio_common::protos::temporal::api::errordetails::v1::{
    WorkflowExecutionAlreadyStartedFailure, multi_operation_execution_failure::OperationStatus,
};

/// A `google.rpc.Status` whose one detail is `value` under `type_url`.
/// The `Any` type is left to inference so the test does not depend on
/// which well-known-types crate Core maps it to.
fn with_detail(type_url: &str, value: Vec<u8>) -> RpcStatus {
    let mut status = RpcStatus {
        code: Code::InvalidArgument as i32,
        message: String::new(),
        details: vec![Default::default()],
    };
    status.details[0].type_url = type_url.to_owned();
    status.details[0].value = value;
    status
}

/// Wraps `message` as the one detail of an encoded `google.rpc.Status`.
fn status_details(type_url: &str, message: Vec<u8>) -> Vec<u8> {
    with_detail(type_url, message).encode_to_vec()
}

/// The status details marking an operation aborted by another one's
/// failure.
fn aborted() -> RpcStatus {
    with_detail(
        &format!("type.googleapis.com/{MULTI_OPERATION_ABORTED_SUFFIX}"),
        Vec::new(),
    )
}

/// A start conflict inside a multi-operation failure is the typed
/// already-started error with the existing run, and an update failure
/// keeps its own code, whichever operation Temporal marked aborted.
#[test]
fn multi_operation_failures_name_the_failed_operation() {
    let conflict = with_detail(
        "type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure",
        WorkflowExecutionAlreadyStartedFailure {
            start_request_id: "r".to_owned(),
            run_id: "existing".to_owned(),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let start_failed = MultiOperationExecutionFailure {
        statuses: vec![
            OperationStatus {
                code: Code::AlreadyExists as i32,
                message: String::new(),
                details: conflict.details,
            },
            OperationStatus {
                code: Code::Aborted as i32,
                message: String::new(),
                details: aborted().details,
            },
        ],
    };
    let status = Status::with_details(
        Code::AlreadyExists,
        "",
        status_details(
            MULTI_OPERATION_FAILURE_TYPE_URL,
            start_failed.encode_to_vec(),
        )
        .into(),
    );
    assert_eq!(
        map_multi_operation_status("w", status),
        ClientOperationError::AlreadyStarted {
            workflow_id: "w".to_owned(),
            existing_run_id: Some("existing".to_owned()),
        }
    );
    let update_failed = MultiOperationExecutionFailure {
        statuses: vec![
            OperationStatus {
                code: Code::Aborted as i32,
                message: String::new(),
                details: aborted().details,
            },
            OperationStatus {
                code: Code::InvalidArgument as i32,
                message: String::new(),
                details: Vec::new(),
            },
        ],
    };
    let status = Status::with_details(
        Code::InvalidArgument,
        "",
        status_details(
            MULTI_OPERATION_FAILURE_TYPE_URL,
            update_failed.encode_to_vec(),
        )
        .into(),
    );
    assert_eq!(
        map_multi_operation_status("w", status),
        ClientOperationError::Rpc {
            code: "invalid_argument".to_owned()
        }
    );
}

/// A status without the multi-operation detail, or with a detail of
/// another type, keeps its own code.
#[test]
fn plain_statuses_map_by_code() {
    assert_eq!(
        map_multi_operation_status("w", Status::unavailable("")),
        ClientOperationError::Rpc {
            code: "unavailable".to_owned()
        }
    );
    let foreign = Status::with_details(
        Code::InvalidArgument,
        "",
        status_details("type.googleapis.com/google.rpc.BadRequest", vec![1, 2]).into(),
    );
    assert_eq!(
        map_multi_operation_status("w", foreign),
        ClientOperationError::Rpc {
            code: "invalid_argument".to_owned()
        }
    );
}

/// The outcome document rejects error kinds that cannot occur: a query
/// failure anywhere and an already-started failure for the update half.
#[test]
fn outcome_documents_reject_impossible_error_kinds() {
    let start = StartWorkflowResponse {
        execution: ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "w".to_owned(),
            run_id: "r".to_owned(),
        },
        started: true,
    };
    let already_started = ClientOperationError::AlreadyStarted {
        workflow_id: "w".to_owned(),
        existing_run_id: None,
    };
    assert!(
        encode_update_with_start_outcome(&UpdateWithStartOutcome::Accepted {
            start: start.clone(),
            update: Err(already_started.clone()),
        })
        .is_err()
    );
    assert!(
        encode_update_with_start_outcome(&UpdateWithStartOutcome::Rejected(already_started))
            .is_ok()
    );
    let query_failed = ClientOperationError::QueryFailed {
        message: String::new(),
    };
    assert!(
        encode_update_with_start_outcome(&UpdateWithStartOutcome::Rejected(query_failed.clone()))
            .is_err()
    );
    assert!(
        encode_update_with_start_outcome(&UpdateWithStartOutcome::Accepted {
            start,
            update: Err(query_failed),
        })
        .is_err()
    );
}

//! ABI status classification of asynchronous activity completion and
//! heartbeat RPC failures.
//!
//! Mounted from `abi.rs` with `#[path]` rather than built as an
//! integration test because [`super::async_activity_failure`] is private
//! to the ABI module.

use super::{
    AsyncActivityOperation, STATUS_ASYNC_HEARTBEAT_REJECTED, STATUS_CONNECTION,
    STATUS_INVALID_STATE, async_activity_failure,
};
use temporalio_client::{
    errors::AsyncActivityError,
    tonic::{Code, Status},
};

/// A server-side not-found response means the retained task token is
/// terminal, not that the OCaml supervisor should retry the same request.
#[test]
fn not_found_is_terminal_invalid_state() {
    let failure = async_activity_failure(
        AsyncActivityOperation::Completion,
        AsyncActivityError::NotFound(Status::not_found("ignored")),
    );

    assert_eq!(failure.status, STATUS_INVALID_STATE);
    assert_eq!(
        failure.message,
        "Temporal asynchronous activity completion is no longer active"
    );
}

/// A generic RPC failure stays Connection: the request may or may not have
/// been applied, so OCaml keeps the live handle and lease.
#[test]
fn heartbeat_rpc_failure_is_connection() {
    let failure = async_activity_failure(
        AsyncActivityOperation::Heartbeat,
        AsyncActivityError::Rpc(Status::unavailable("ignored")),
    );

    assert_eq!(failure.status, STATUS_CONNECTION);
    assert_eq!(
        failure.message,
        "Temporal asynchronous activity heartbeat failed"
    );
}

/// A confirmed missing heartbeat token is terminal just like a missing
/// completion token, even though a connection failure is uncertain.
#[test]
fn heartbeat_not_found_is_terminal_invalid_state() {
    let failure = async_activity_failure(
        AsyncActivityOperation::Heartbeat,
        AsyncActivityError::NotFound(Status::not_found("ignored")),
    );

    assert_eq!(failure.status, STATUS_INVALID_STATE);
    assert_eq!(
        failure.message,
        "Temporal asynchronous activity heartbeat is no longer active"
    );
}

/// Codes that prove the server answered without applying the request.
const DEFINITIVE_REJECTIONS: [Code; 6] = [
    Code::InvalidArgument,
    Code::PermissionDenied,
    Code::FailedPrecondition,
    Code::OutOfRange,
    Code::Unimplemented,
    Code::Unauthenticated,
];

/// Codes for which the server may or may not have applied the request.
const AMBIGUOUS_FAILURES: [Code; 7] = [
    Code::Unavailable,
    Code::DeadlineExceeded,
    Code::ResourceExhausted,
    Code::Cancelled,
    Code::Aborted,
    Code::Unknown,
    Code::Internal,
];

/// A definitive heartbeat RPC rejection does not consume the live activity
/// capability; the OCaml caller may correct this request or complete it.
#[test]
fn definitive_heartbeat_rpc_rejection_keeps_live_capability() {
    for code in DEFINITIVE_REJECTIONS {
        let failure = async_activity_failure(
            AsyncActivityOperation::Heartbeat,
            AsyncActivityError::Rpc(Status::new(code, "discard this remote text")),
        );
        assert_eq!(failure.status, STATUS_ASYNC_HEARTBEAT_REJECTED, "{code:?}");
        assert_eq!(
            failure.message,
            "Temporal asynchronous activity heartbeat request was rejected"
        );
    }
}

/// A definitive rejection of complete/fail/cancel (for example cancel
/// without a cancel request, or an oversized result) leaves the activity
/// live so the caller can submit a different terminal operation (#821).
#[test]
fn definitive_completion_rpc_rejection_keeps_live_capability() {
    for code in DEFINITIVE_REJECTIONS {
        let failure = async_activity_failure(
            AsyncActivityOperation::Completion,
            AsyncActivityError::Rpc(Status::new(code, "discard this remote text")),
        );
        assert_eq!(failure.status, STATUS_ASYNC_HEARTBEAT_REJECTED, "{code:?}");
        assert_eq!(
            failure.message,
            "Temporal asynchronous activity completion request was rejected"
        );
    }
}

/// Deadline, transport, and other ambiguous RPC outcomes may occur after
/// the server applied the request. They stay Connection for both
/// operations, so OCaml retains the lease instead of closing the handle.
#[test]
fn ambiguous_rpc_failure_stays_connection() {
    for operation in [
        AsyncActivityOperation::Heartbeat,
        AsyncActivityOperation::Completion,
    ] {
        for code in AMBIGUOUS_FAILURES {
            let failure = async_activity_failure(
                operation,
                AsyncActivityError::Rpc(Status::new(code, "discard this remote text")),
            );
            assert_eq!(failure.status, STATUS_CONNECTION, "{code:?}");
            assert!(!failure.message.contains("discard"));
        }
    }
}

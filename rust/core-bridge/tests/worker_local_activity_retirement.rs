//! Regression coverage for issue #1029: shutdown and disposal must not
//! complete a local activity with a retryable failure.
//!
//! Core retries a retryable local-activity failure in-process, with a new
//! token after each backoff, until the backoff exceeds the local-retry
//! threshold (about a minute by default). While a worker drains, every retry
//! was admitted and retired again, so teardown stalled and detached. The
//! bridge now classifies a token by the same predicate Core uses to route a
//! completion to its local-activity manager and reports a retired local
//! activity as cancelled, which Core resolves without retrying; a remote
//! activity keeps the retryable failure the server dispatches again.

use ocaml_temporal_core_bridge::worker_bridge::{
    bridge_retirement_result, is_local_activity_token,
};
use temporalio_common::protos::TaskToken;
use temporalio_common::protos::coresdk::activity_result::activity_execution_result::Status;

/// The diagnostic the bridge would attach to a retired remote activity.
fn message() -> String {
    "OCaml bridge retired the task during worker shutdown: test".to_owned()
}

/// A token Core minted for a local activity is classified as local, and a
/// server-issued remote token is not.
#[test]
fn local_activity_tokens_are_recognized_as_core_routes_them() {
    let local = TaskToken::new_local_activity_token(b"run-1-seq-7".iter().copied()).into_inner();
    assert!(is_local_activity_token(&local));
    assert!(!is_local_activity_token(b"server-issued-remote-token"));
    assert!(!is_local_activity_token(b""));
}

/// A retired local activity is cancelled, never failed, so Core's
/// local-activity manager reports it without an in-process retry.
#[test]
fn retired_local_activity_is_cancelled_not_failed() {
    let local = TaskToken::new_local_activity_token(b"run-1-seq-7".iter().copied()).into_inner();
    let result = bridge_retirement_result(&local, message());
    assert!(
        matches!(result.status, Some(Status::Cancelled(_))),
        "a retired local activity must not be a retryable failure: {:?}",
        result.status
    );
}

/// A retired remote activity keeps the retryable application failure, so the
/// server dispatches the next attempt, possibly to another worker.
#[test]
fn retired_remote_activity_is_failed_retryably() {
    let result = bridge_retirement_result(b"server-issued-remote-token", message());
    match result.status {
        Some(Status::Failed(failure)) => {
            let failure = failure.failure.expect("failure details");
            assert_eq!(failure.message, message());
        }
        other => panic!("a retired remote activity must fail retryably: {other:?}"),
    }
}

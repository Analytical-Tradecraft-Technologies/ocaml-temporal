//! Result mapping of `shutdown_worker`'s error branches (#1229).
//!
//! Included as a child module of `src/abi.rs` so it can reach the private
//! mapping helpers. The drain timeout and the lost-lease refusal themselves
//! are exercised on real poll lanes in the replay bridge tests; this file
//! pins what each outcome becomes at the C ABI, including the Panicked and
//! Detached finalizer outcomes, which Core cannot be made to produce
//! deterministically in a test.

use super::{
    ReleasedFinalize, STATUS_OUTSTANDING_TASKS, STATUS_WORKER, shutdown_drain_failure,
    shutdown_release_result,
};
use crate::worker_bridge::{PollLaneError, ShutdownDrainError};

/// A clean finalization succeeds with an empty result, and one that had to
/// force-complete leases OCaml never completed reports
/// `STATUS_OUTSTANDING_TASKS` although the worker was still released.
#[test]
fn finalized_worker_succeeds_unless_leases_were_abandoned() {
    let finalized = shutdown_release_result(ReleasedFinalize::Finalized, 0)
        .unwrap_or_else(|failure| panic!("a clean finalization failed: {}", failure.message));
    assert!(finalized.is_empty());
    let failure = shutdown_release_result(ReleasedFinalize::Finalized, 2)
        .expect_err("abandoned leases must be reported");
    assert_eq!(failure.status, STATUS_OUTSTANDING_TASKS);
}

/// A panicked and a detached finalizer are both closed worker failures with
/// distinct, payload-free diagnostics, whatever the lease count.
#[test]
fn panicked_and_detached_finalizers_are_worker_failures() {
    for abandoned in [0, 3] {
        let panicked = shutdown_release_result(ReleasedFinalize::Panicked, abandoned)
            .expect_err("a panicked finalizer is a failure");
        assert_eq!(panicked.status, STATUS_WORKER);
        assert_eq!(panicked.message, "Temporal worker finalization failed");

        let detached = shutdown_release_result(ReleasedFinalize::Detached, abandoned)
            .expect_err("a detached finalizer is a failure");
        assert_eq!(detached.status, STATUS_WORKER);
        assert_eq!(
            detached.message,
            "Temporal worker finalization did not finish within the shutdown bound"
        );
    }
}

/// A drain that timed out, and one whose lane failed, are both
/// `STATUS_WORKER`: `shutdown_worker` keeps the worker in the graph for
/// runtime close to dispose of.
#[test]
fn drain_failures_are_worker_failures() {
    let timed_out = shutdown_drain_failure(ShutdownDrainError::TimedOut);
    assert_eq!(timed_out.status, STATUS_WORKER);
    assert_eq!(
        timed_out.message,
        "Temporal worker poll lanes did not stop within the shutdown bound"
    );
    let lane = shutdown_drain_failure(ShutdownDrainError::Lane(PollLaneError::Core(
        "private Core detail".to_owned(),
    )));
    assert_eq!(lane.status, STATUS_WORKER);
    assert!(
        !lane.message.contains("private Core detail"),
        "a lane failure must not expose Core's text: {}",
        lane.message
    );
}

//! Bounded replay completion await and replay-lane join.
//!
//! Mounted from `worker_bridge.rs` with `#[path]` rather than built as an
//! integration test because [`super::await_reply_unless_stranded`] and
//! [`super::join_replay_lane_within`] are private to the worker bridge.

use super::{
    GuardedCompletion, REPLAY_STRANDED_COMPLETION_GRACE, Readiness, ReplayLaneJoin, TaskLedger,
    await_reply_unless_stranded, join_replay_lane_within,
};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use temporalio_common::protos::coresdk::workflow_activation::WorkflowActivation;
use tokio::sync::mpsc;

/// Builds the current-thread runtime with timers that the helper needs.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime should start")
}

/// A reply that never arrives after Core's stream ended is classified as
/// stranded instead of waiting for the much longer overall bound.
#[test]
fn pending_reply_after_core_shutdown_is_stranded() {
    let signal = Readiness::new();
    signal.close_after_core_shutdown();
    let outcome = runtime().block_on(await_reply_unless_stranded(
        std::future::pending(),
        &signal,
        Duration::from_secs(60),
    ));
    assert!(
        matches!(outcome, GuardedCompletion::Stranded),
        "{outcome:?}"
    );
}

/// A bridge-initiated close does not prove Core's stream has ended, so a
/// pending reply is only released by the overall bound.
#[test]
fn bridge_close_alone_does_not_strand_a_reply() {
    let signal = Readiness::new();
    signal.close();
    let outcome = runtime().block_on(await_reply_unless_stranded(
        std::future::pending(),
        &signal,
        REPLAY_STRANDED_COMPLETION_GRACE * 2,
    ));
    assert!(
        matches!(outcome, GuardedCompletion::TimedOut),
        "{outcome:?}"
    );
}

/// A reply that is already available wins even when Core's stream has
/// ended, so a processed completion is never misreported as stranded.
#[test]
fn ready_reply_is_returned_after_core_shutdown() {
    let signal = Readiness::new();
    signal.close_after_core_shutdown();
    let outcome = runtime().block_on(await_reply_unless_stranded(
        std::future::ready(Ok(())),
        &signal,
        Duration::from_secs(60),
    ));
    assert!(
        matches!(outcome, GuardedCompletion::Finished(Ok(()))),
        "{outcome:?}"
    );
}

/// A reply that arrives within the grace period after Core's stream ended
/// is still returned rather than abandoned.
#[test]
fn late_reply_within_grace_is_returned() {
    let signal = Readiness::new();
    signal.close_after_core_shutdown();
    let outcome = runtime().block_on(await_reply_unless_stranded(
        async {
            tokio::time::sleep(REPLAY_STRANDED_COMPLETION_GRACE / 5).await;
            Ok(())
        },
        &signal,
        Duration::from_secs(60),
    ));
    assert!(
        matches!(outcome, GuardedCompletion::Finished(Ok(()))),
        "{outcome:?}"
    );
}

/// The replay lane join bound covers acknowledgement waits too (PR #966
/// review). A lane that never exits publishes one activation whose
/// acknowledgement never completes; the join must still return
/// `TimedOut` close to its injected 200 ms bound, with the lane aborted.
#[test]
fn lane_join_bound_includes_a_stuck_acknowledgement() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime should start");
    runtime.block_on(async {
        let signal = Readiness::new();
        let (sender, mut ready) = mpsc::unbounded_channel();
        let ledger = Mutex::new(TaskLedger::new());
        let activation = WorkflowActivation {
            run_id: "run-stuck-ack".to_owned(),
            ..Default::default()
        };
        assert!(signal.enqueue(&sender, Ok(activation)));
        let lane = tokio::spawn(std::future::pending::<()>());
        let lane_probe = lane.abort_handle();
        let started = Instant::now();
        let outcome = join_replay_lane_within(
            lane,
            Duration::from_millis(200),
            &signal,
            &mut ready,
            &ledger,
            |_run_id| std::future::pending::<()>(),
        )
        .await;
        assert!(matches!(outcome, ReplayLaneJoin::TimedOut), "{outcome:?}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the join overran its bound: {:?}",
            started.elapsed()
        );
        assert!(
            lane_probe.is_finished(),
            "the timed-out lane must be aborted"
        );
    });
}

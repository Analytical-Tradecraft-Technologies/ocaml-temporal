//! Poll-lane readiness signalling: wakeups, bounded waits, terminal lane
//! errors, shutdown wakes, and the combined any-lane wait.
//!
//! Mounted from `worker_bridge.rs` with `#[path]` rather than built as an
//! integration test because [`super::Readiness`] and
//! [`super::wait_any_lane`] are private to the worker bridge.

use super::{
    ACTIVITY_COMPLETION_RETRY_BACKOFF, AnyLaneWake, PollLaneError, Readiness, ReadinessWait,
    ReadyTask, WORKFLOW_DELIVERY_REJECTION_BACKOFF, wait_any_lane,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;

/// Keeps the completion retry delay positive and bounded so a future
/// change cannot accidentally reintroduce a tight loop or make shutdown
/// wait for an unbounded interval.
#[test]
fn completion_retry_backoff_is_positive_and_bounded() {
    assert!(ACTIVITY_COMPLETION_RETRY_BACKOFF > Duration::ZERO);
    assert!(ACTIVITY_COMPLETION_RETRY_BACKOFF <= Duration::from_secs(1));
    assert!(WORKFLOW_DELIVERY_REJECTION_BACKOFF > Duration::ZERO);
    assert!(WORKFLOW_DELIVERY_REJECTION_BACKOFF <= Duration::from_secs(1));
}

/// Confirms a notification committed before waiting is observed immediately.
#[test]
fn notification_before_wait_returns_ready() {
    let signal = Readiness::new();
    let (sender, mut receiver) = mpsc::unbounded_channel::<ReadyTask<()>>();

    assert!(signal.enqueue(&sender, Ok(())));
    assert_eq!(signal.wait(), ReadinessWait::Ready);
    assert!(signal.take(&mut receiver).is_some());
}

/// Confirms queued work cannot be received before its pending count exists.
///
/// The producer runs on another OS thread to exercise the same ordering as
/// a Tokio poll lane racing an owner-domain drain.
#[test]
fn queued_work_has_a_linearizable_send_and_receive_order() {
    let signal = Arc::new(Readiness::new());
    let (sender, mut receiver) = mpsc::unbounded_channel::<ReadyTask<usize>>();
    let producer_signal = Arc::clone(&signal);
    let producer = thread::spawn(move || {
        for value in 0..128 {
            assert!(producer_signal.enqueue(&sender, Ok(value)));
        }
    });

    let mut received = Vec::new();
    while received.len() < 128 {
        assert_eq!(signal.wait(), ReadinessWait::Ready);
        while let Some(Ok(value)) = signal.take(&mut receiver) {
            received.push(value);
        }
    }
    producer.join().expect("producer must not panic");
    received.sort_unstable();
    assert_eq!(received, (0..128).collect::<Vec<_>>());
}

/// Confirms closing a quiet lane wakes a waiter instead of leaving it
/// blocked until the bounded timeout expires.
#[test]
fn shutdown_wakes_a_waiting_owner() {
    let signal = Arc::new(Readiness::new());
    let waiter_signal = Arc::clone(&signal);
    let waiter = thread::spawn(move || waiter_signal.wait());
    assert_close_wakes(waiter, || signal.close());
}

/// Runs `close` once `waiter` has had time to block, then asserts that
/// the waiter returned `Shutdown` rather than waiting out its timeout.
fn assert_close_wakes(waiter: thread::JoinHandle<ReadinessWait>, close: impl FnOnce()) {
    thread::sleep(Duration::from_millis(10));
    close();

    assert_eq!(
        waiter.join().expect("waiter must not panic"),
        ReadinessWait::Shutdown
    );
}

/// Confirms a quiet lane returns control to its supervisor after the
/// documented bound instead of waiting forever for an event.
#[test]
fn quiet_lane_wait_is_bounded() {
    let signal = Readiness::new();

    assert_eq!(signal.wait(), ReadinessWait::TimedOut);
}

/// Confirms a fatal lane error remains observable after its wakeup.
#[test]
fn lane_error_wakes_and_remains_terminal() {
    let signal = Readiness::new();
    let error = PollLaneError::Core("poll failed".to_owned());
    signal.fail(error.clone());

    assert_eq!(signal.wait(), ReadinessWait::Error(error.clone()));
    assert_eq!(signal.wait(), ReadinessWait::Error(error));
}

/// A lane that yields locally instead of waiting on its native condition
/// still sees the fatal error after all previously queued work is drained.
#[test]
fn nonblocking_poll_sees_error_after_queued_work() {
    let signal = Readiness::new();
    let (sender, mut receiver) = mpsc::unbounded_channel::<ReadyTask<usize>>();
    let error = PollLaneError::Core("poll failed".to_owned());

    assert!(signal.enqueue(&sender, Ok(7)));
    signal.fail(error.clone());
    assert_eq!(signal.take_or_failure(&mut receiver), Some(Ok(7)));
    assert_eq!(signal.take_or_failure(&mut receiver), Some(Err(error)));
}

/// Builds the workflow/activity signal pair exactly as a live worker does:
/// two lane signals sharing one combined wake. The completion-window
/// readiness tests reuse it, so it is visible to sibling test modules.
pub(super) fn lane_pair() -> (Arc<Readiness>, Arc<Readiness>) {
    let any = Arc::new(AnyLaneWake::default());
    (
        Arc::new(Readiness::sharing(&any)),
        Arc::new(Readiness::sharing(&any)),
    )
}

/// Regression for #806: work committed on the lane the owner was not
/// expecting must end a combined wait. A lane-specific wait on the quiet
/// lane would keep timing out however often it was repeated. The test
/// asserts the outcome rather than elapsed time, so scheduler delays on a
/// loaded runner cannot make it fail: a wait that happens to time out
/// before the producer runs is simply repeated, as the worker loop does.
#[test]
fn combined_wait_wakes_for_either_lane() {
    for activity_side in [true, false] {
        let (workflow, activity) = lane_pair();
        let (sender, mut receiver) = mpsc::unbounded_channel::<ReadyTask<()>>();
        let producer_signal = Arc::clone(if activity_side { &activity } else { &workflow });
        let waiter_workflow = Arc::clone(&workflow);
        let waiter_activity = Arc::clone(&activity);
        let waiter = thread::spawn(move || {
            for _ in 0..100 {
                match wait_any_lane(&waiter_workflow, &waiter_activity, None) {
                    ReadinessWait::TimedOut => continue,
                    other => return other,
                }
            }
            ReadinessWait::TimedOut
        });
        assert!(producer_signal.enqueue(&sender, Ok(())));
        let result = waiter.join().expect("waiter must not panic");

        assert_eq!(result, ReadinessWait::Ready);
        // The wait is only a wake signal; the task is still drainable.
        assert!(producer_signal.take(&mut receiver).is_some());
    }
}

/// Queued work on one lane wins over a fatal error or closure on the
/// other, and the combined wait never consumes that work.
#[test]
fn combined_wait_prefers_work_then_error_then_shutdown() {
    let (workflow, activity) = lane_pair();
    let (sender, mut receiver) = mpsc::unbounded_channel::<ReadyTask<()>>();
    let error = PollLaneError::Core("poll failed".to_owned());

    assert!(activity.enqueue(&sender, Ok(())));
    workflow.fail(error.clone());
    assert_eq!(
        wait_any_lane(&workflow, &activity, None),
        ReadinessWait::Ready
    );
    assert_eq!(
        wait_any_lane(&workflow, &activity, None),
        ReadinessWait::Ready
    );
    assert!(activity.take(&mut receiver).is_some());
    assert_eq!(
        wait_any_lane(&workflow, &activity, None),
        ReadinessWait::Error(error)
    );

    let (workflow, activity) = lane_pair();
    activity.close();
    assert_eq!(
        wait_any_lane(&workflow, &activity, None),
        ReadinessWait::Shutdown
    );
}

/// Closing either lane wakes a combined waiter instead of leaving it
/// blocked until the bounded timeout.
#[test]
fn combined_wait_wakes_on_shutdown() {
    let (workflow, activity) = lane_pair();
    let waiter_workflow = Arc::clone(&workflow);
    let waiter_activity = Arc::clone(&activity);
    let waiter = thread::spawn(move || wait_any_lane(&waiter_workflow, &waiter_activity, None));
    assert_close_wakes(waiter, || workflow.close());
}

/// Two quiet lanes still return control to the supervisor within the
/// documented bound, preserving shutdown responsiveness.
#[test]
fn quiet_combined_wait_is_bounded() {
    let (workflow, activity) = lane_pair();

    assert_eq!(
        wait_any_lane(&workflow, &activity, None),
        ReadinessWait::TimedOut
    );
}

//! Combined-readiness behavior with a workflow-completion window.
//!
//! [`super::wait_any_lane`] is private to the worker bridge, so this test
//! lives in a child module of it rather than under `tests/`.

use super::{
    AnyLaneWake, OutcomeGuard, READINESS_WAIT_TIMEOUT, Readiness, ReadinessWait, ReadyTask,
    WorkflowCompletions, wait_any_lane,
};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Two lane signals sharing one combined wake, as one worker's lanes do.
fn lane_pair() -> (Arc<Readiness>, Arc<Readiness>) {
    let any = Arc::new(AnyLaneWake::default());
    (
        Arc::new(Readiness::sharing(&any)),
        Arc::new(Readiness::sharing(&any)),
    )
}

/// With the completion window full, a queued activation is not work the
/// owner can take, so the combined wait does not report it (which would
/// spin the workflow lane); a published completion outcome is work and
/// ends the wait, through the same combined wake, from another thread.
#[test]
fn combined_wait_respects_the_completion_window() {
    let (workflow, activity) = lane_pair();
    let any = Arc::clone(&workflow.any);
    let completions = Arc::new(WorkflowCompletions::new(
        Some(1),
        Arc::new(move || any.notify()),
    ));
    let (sender, _receiver) = mpsc::unbounded_channel::<ReadyTask<()>>();
    assert!(workflow.enqueue(&sender, Ok(())));
    assert_eq!(
        wait_any_lane(&workflow, &activity, Some(&completions)),
        ReadinessWait::Ready,
        "a queued activation with a free credit is work"
    );

    completions.deliver("run-a");
    let ticket = completions.admit("run-a");
    let waiter_workflow = Arc::clone(&workflow);
    let waiter_activity = Arc::clone(&activity);
    let waiter_completions = Arc::clone(&completions);
    let waiter = thread::spawn(move || {
        let started = Instant::now();
        let result = wait_any_lane(
            &waiter_workflow,
            &waiter_activity,
            Some(&waiter_completions),
        );
        (result, started.elapsed())
    });
    thread::sleep(Duration::from_millis(20));
    OutcomeGuard::new(Arc::clone(&completions), ticket, "run-a".to_owned()).finish(Ok(()));
    let (result, elapsed) = waiter.join().expect("waiter must not panic");
    assert_eq!(result, ReadinessWait::Ready);
    assert!(
        elapsed >= Duration::from_millis(15),
        "a full window must not report the queued activation as ready"
    );
    assert!(
        elapsed < READINESS_WAIT_TIMEOUT,
        "the outcome must end the wait before its timeout"
    );
}

//! Unit coverage for the bounded workflow-completion window that lets the
//! live worker admit a completion without waiting for Temporal Core.
//!
//! These tests exercise [`WorkflowCompletions`] and [`OutcomeGuard`] directly,
//! without Core: credit accounting from delivery through collection, ticket
//! generations that keep a late outcome from matching a newer activation of
//! the same run, exactly-once publication when a completion task panics or
//! is aborted, the bounded outcome wait, and the wake hook the worker's
//! combined readiness wait relies on.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ocaml_temporal_core_bridge::workflow_completions::{
    CompletionFailure, CompletionWait, OutcomeGuard, WorkflowCompletions, encode_outcomes,
};

/// Builds a window of `capacity` whose wake hook counts its calls.
fn window(capacity: Option<usize>) -> (Arc<WorkflowCompletions>, Arc<AtomicUsize>) {
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&wakes);
    let completions = Arc::new(WorkflowCompletions::new(
        capacity,
        Arc::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }),
    ));
    (completions, wakes)
}

/// A credit is held from delivery until the outcome is collected, wherever
/// it is in between, so a full window refuses another delivery until an
/// outcome has been taken, and taking it frees exactly one credit.
#[test]
fn credit_is_held_from_delivery_until_the_outcome_is_collected() {
    let (completions, wakes) = window(Some(2));
    assert!(completions.has_credit());
    completions.deliver("run-a");
    completions.deliver("run-b");
    assert!(
        !completions.has_credit(),
        "two deliveries fill a window of two"
    );
    assert_eq!(completions.window_full_total(), 1);

    let ticket_a = completions.admit("run-a");
    assert!(
        !completions.has_credit(),
        "an in-flight completion keeps its credit"
    );
    OutcomeGuard::new(Arc::clone(&completions), ticket_a, "run-a".to_owned()).finish(Ok(()));
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "publication wakes readiness"
    );
    assert!(completions.has_ready());
    assert!(
        !completions.credit_available(),
        "an uncollected outcome keeps its credit, bounding the ready queue"
    );

    let outcomes = completions.take_ready();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].ticket, ticket_a);
    assert_eq!(outcomes[0].run_id, "run-a");
    assert_eq!(outcomes[0].result, Ok(()));
    assert!(
        completions.credit_available(),
        "collection frees the credit"
    );
    assert_eq!(completions.held(), 1, "run-b is still delivered");

    // A rejection or synchronous completion of a delivered lease frees it.
    completions.retire_delivered("run-b");
    assert_eq!(completions.held(), 0);
    assert_eq!(completions.outstanding(), 0);
}

/// Every delivery of the same run gets a fresh generation, so the ticket of
/// an earlier completion can never equal the ticket of the run's next one.
#[test]
fn tickets_are_fresh_lease_generations() {
    let (completions, _) = window(Some(8));
    let first_generation = completions.deliver("run-a");
    let first_ticket = completions.admit("run-a");
    assert_eq!(first_ticket, first_generation);
    let second_generation = completions.deliver("run-a");
    assert_ne!(second_generation, first_ticket);
    assert_eq!(completions.admit("run-a"), second_generation);
    assert_eq!(completions.admitted_total(), 2);
}

/// A task dropped without publishing (aborted at disposal) still publishes
/// one outcome, marked as aborted, so the in-flight count and any waiter are
/// never stranded.
#[test]
fn an_aborted_task_publishes_exactly_one_unknown_outcome() {
    let (completions, wakes) = window(Some(1));
    completions.deliver("run-a");
    let ticket = completions.admit("run-a");
    drop(OutcomeGuard::new(
        Arc::clone(&completions),
        ticket,
        "run-a".to_owned(),
    ));
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    let outcomes = completions.take_ready();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].result, Err(CompletionFailure::Aborted));
    assert_eq!(completions.outstanding(), 0);
}

/// A task that panics while it owns a completion publishes a panicked
/// outcome while unwinding instead of losing it.
#[test]
fn a_panicking_task_publishes_a_panicked_outcome() {
    let (completions, _) = window(Some(1));
    completions.deliver("run-a");
    let ticket = completions.admit("run-a");
    let guard = OutcomeGuard::new(Arc::clone(&completions), ticket, "run-a".to_owned());
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = guard;
        panic!("completion task defect");
    }));
    assert!(unwound.is_err());
    let outcomes = completions.take_ready();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].result, Err(CompletionFailure::Panicked));
}

/// The bounded wait reports idle at once with nothing in flight, times out
/// while a completion is still awaiting Core, and returns as soon as another
/// thread publishes an outcome.
#[test]
fn the_outcome_wait_is_bounded_and_woken_by_publication() {
    let (completions, _) = window(Some(4));
    assert_eq!(
        completions.wait_ready(Duration::from_secs(5)),
        CompletionWait::Idle
    );
    completions.deliver("run-a");
    let ticket = completions.admit("run-a");
    assert_eq!(
        completions.wait_ready(Duration::from_millis(1)),
        CompletionWait::TimedOut
    );
    let publisher = {
        let completions = Arc::clone(&completions);
        std::thread::spawn(move || {
            OutcomeGuard::new(completions, ticket, "run-a".to_owned()).finish(Ok(()));
        })
    };
    // The 30-second bound is only a watchdog; publication ends the wait.
    assert_eq!(
        completions.wait_ready(Duration::from_secs(30)),
        CompletionWait::Ready
    );
    publisher.join().expect("publisher thread");
}

/// A replay worker has no window: deliveries never run out of credit.
#[test]
fn a_disabled_window_never_refuses_delivery() {
    let (completions, _) = window(None);
    for index in 0..1_000 {
        completions.deliver(&format!("run-{index}"));
    }
    assert!(completions.has_credit());
    assert_eq!(completions.capacity(), None);
}

/// Shutdown forgets delivered leases and uncollected outcomes, because it
/// retires those leases itself.
#[test]
fn shutdown_clears_delivered_leases_and_uncollected_outcomes() {
    let (completions, _) = window(Some(2));
    completions.deliver("run-a");
    completions.deliver("run-b");
    let ticket = completions.admit("run-b");
    OutcomeGuard::new(Arc::clone(&completions), ticket, "run-b".to_owned()).finish(Ok(()));
    completions.clear_for_shutdown();
    assert_eq!(completions.held(), 0);
    assert!(completions.take_ready().is_empty());
}

/// The outcome document is the closed shape OCaml decodes, and Core's
/// rejection text never crosses into it.
#[test]
fn outcome_documents_are_closed_and_omit_core_text() {
    let (completions, _) = window(Some(2));
    completions.deliver("run-a");
    let ticket = completions.admit("run-a");
    OutcomeGuard::new(Arc::clone(&completions), ticket, "run-a".to_owned()).finish(Err(
        CompletionFailure::CoreRejected("server detail that must stay private".to_owned()),
    ));
    let document = String::from_utf8(encode_outcomes(&completions.take_ready()))
        .expect("outcome document is UTF-8");
    assert!(!document.contains("server detail"));
    let value: serde_json::Value = serde_json::from_str(&document).expect("valid JSON");
    let outcome = &value["outcomes"][0];
    assert_eq!(outcome["ticket"], ticket);
    assert_eq!(outcome["run_id"], "run-a");
    assert_eq!(outcome["result"], "rejected");
    assert!(outcome["core_await_us"].is_u64());
    assert!(outcome["queued_us"].is_u64());
    assert_eq!(value.as_object().map(|object| object.len()), Some(1));
}

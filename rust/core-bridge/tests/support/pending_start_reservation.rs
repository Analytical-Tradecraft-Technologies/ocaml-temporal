//! Capacity reservation in the pending-start registry before a start
//! task is admitted.
//!
//! Mounted from `abi.rs` with `#[path]` rather than built as an
//! integration test because [`super::reserve_pending_start_slots`] and
//! [`super::PendingStart`] are private to the ABI module.

use super::{HashMap, PendingStart, STATUS_INTERNAL, reserve_pending_start_slots};

/// A normal admission reservation creates room for the one entry that is
/// inserted after the Tokio task is spawned.
#[test]
fn reserves_capacity_for_one_pending_start() {
    let mut pending: HashMap<String, PendingStart> = HashMap::new();
    reserve_pending_start_slots(&mut pending, 1)
        .expect("one pending-start slot should be reservable");
    assert!(pending.capacity() > pending.len());
}

/// Capacity overflow is converted into an internal operation error while
/// the registry remains untouched, so task admission has not begun.
#[test]
fn rejects_unrepresentable_reservation_before_task_admission() {
    let mut pending: HashMap<String, PendingStart> = HashMap::new();
    let failure = reserve_pending_start_slots(&mut pending, usize::MAX)
        .expect_err("an impossible reservation must fail before spawning");
    assert_eq!(failure.status, STATUS_INTERNAL);
    assert!(pending.is_empty());
}

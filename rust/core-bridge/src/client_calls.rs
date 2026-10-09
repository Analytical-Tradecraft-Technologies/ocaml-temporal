//! Completion cells for submit-and-complete client RPCs (#807).
//!
//! A client RPC is submitted by the runtime's sole owner (the OCaml
//! supervisor Domain), which validates the request, spawns one Tokio task, and
//! returns a numeric call identifier at once. The task deposits exactly one
//! encoded outcome into that call's [`CallSlot`]; the OCaml caller that
//! submitted the call then blocks on the slot from its own Domain, with the
//! OCaml runtime lock released, until the outcome is ready. The owner Domain
//! therefore never waits for the network, and one long poll cannot delay any
//! other operation on the same client.
//!
//! # Ownership
//!
//! * The process-wide registry owns one `Arc<CallSlot>` per live call
//!   identifier. A call is removed by exactly one of two paths: the caller's
//!   terminal read in [`await_call`], or [`release_owner`] when the runtime
//!   that created it disconnects or closes.
//! * The Tokio task owns a second reference through a [`CompletionGuard`]. It
//!   never touches the runtime graph, the registry, or OCaml; it only writes
//!   its slot and wakes the waiter. Its join handle stays in the runtime's own
//!   registry so shutdown can abort and join it.
//! * Awaiting a call needs no runtime pointer. The registry is reached by
//!   identifier, so a caller racing shutdown can never dereference a released
//!   graph: it simply observes that its call was closed.
//!
//! # Access control
//!
//! A call is addressed by the pair `(owner, call)`, never by the call
//! identifier alone. The owner is the identity of the runtime graph that
//! submitted the call; [`register`] binds it into the registry entry and the
//! submission returns both values to that graph's client only. [`await_call`]
//! answers a pair whose owner does not match exactly like an identifier that
//! was never issued (the closed/unknown failure), so a caller can neither
//! read nor consume another graph's outcome, nor learn whether such a call
//! exists. Owner identities are drawn at random from a 62-bit space rather
//! than a counter, so they are not guessable from one's own identity.
//!
//! Lock order: the registry mutex is never held while a slot mutex is taken.

use crate::abi::{
    Failure, Operation, STATUS_INTERNAL, STATUS_INVALID_STATE, STATUS_NOT_READY, STATUS_PANIC,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// Largest call identifier handed to OCaml. OCaml represents it as a native
/// `int`, which has at least 63 bits on every supported platform; staying
/// below 2^62 keeps the value positive there with a wide margin. Reaching the
/// bound would need billions of calls per second for centuries, so it is a
/// defensive check rather than a reachable condition.
const MAX_CALL_ID: u64 = 1 << 62;

/// Terminal and pending states of one call. A slot moves from `Pending` to
/// either `Ready` or `Closed` exactly once; the terminal read consumes it.
enum SlotState {
    /// The task has not produced an outcome yet.
    Pending,
    /// The task's encoded outcome, waiting for the caller's terminal read.
    Ready(Operation),
    /// The owning runtime shut down (or the outcome was already consumed);
    /// no outcome will ever arrive.
    Closed,
}

/// One per-call result cell shared by the Tokio task and the OCaml caller.
pub(crate) struct CallSlot {
    state: Mutex<SlotState>,
    changed: Condvar,
}

/// One registered call: its slot and the runtime that may release it.
struct RegisteredCall {
    owner: u64,
    slot: Arc<CallSlot>,
}

/// Process-wide map from call identifier to slot. It is shared by every
/// runtime so that a caller can await its call without borrowing the runtime
/// graph, which only its owner Domain may touch.
static CALLS: LazyLock<Mutex<HashMap<u64, RegisteredCall>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Next call identifier. Identifiers are never reused within a process.
static NEXT_CALL: AtomicU64 = AtomicU64::new(1);

/// Locks a mutex whose protected data stays consistent even if a holder
/// panicked: every critical section here is a single assignment or map edit.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The failure an awaiting caller receives when its call can no longer
/// produce an outcome: the runtime disconnected or closed, or the identifier
/// was already consumed or never issued. The OCaml supervisor maps exactly
/// this status from an await to its typed `Closed` error; no submitted
/// operation produces it from inside its task.
fn closed_failure() -> Failure {
    Failure {
        status: STATUS_INVALID_STATE,
        message: "Temporal client call ended because the client shut down".to_owned(),
    }
}

impl CallSlot {
    /// Creates an empty pending slot.
    fn new() -> Self {
        Self {
            state: Mutex::new(SlotState::Pending),
            changed: Condvar::new(),
        }
    }

    /// Publishes the task's outcome if the call is still pending and wakes
    /// its waiter. An outcome that arrives after shutdown closed the slot is
    /// dropped, since nobody can read it any more.
    fn complete(&self, outcome: Operation) {
        let mut state = lock(&self.state);
        if matches!(*state, SlotState::Pending) {
            *state = SlotState::Ready(outcome);
            self.changed.notify_all();
        }
    }

    /// Marks a pending call closed and wakes its waiter. A ready outcome is
    /// left in place so a caller already holding the slot still receives it.
    fn close(&self) {
        let mut state = lock(&self.state);
        if matches!(*state, SlotState::Pending) {
            *state = SlotState::Closed;
            self.changed.notify_all();
        }
    }
}

/// The task-side capability to complete one slot exactly once.
///
/// If the task ends without completing it, the guard still settles the slot
/// when it is dropped: with `STATUS_PANIC` while a panic is unwinding the
/// task, otherwise (the task was aborted or its executor shut down) with the
/// closed failure. A waiter can therefore never be stranded on a call whose
/// task is gone, even when shutdown has not closed the slot yet.
pub(crate) struct CompletionGuard {
    slot: Option<Arc<CallSlot>>,
}

impl CompletionGuard {
    /// Publishes `outcome` and disarms the drop fallback.
    pub(crate) fn complete(mut self, outcome: Operation) {
        if let Some(slot) = self.slot.take() {
            slot.complete(outcome);
        }
    }
}

impl Drop for CompletionGuard {
    /// Settles a slot whose task ended without an outcome.
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            let failure = if std::thread::panicking() {
                Failure {
                    status: STATUS_PANIC,
                    message: "Rust panic contained in a Temporal client call".to_owned(),
                }
            } else {
                closed_failure()
            };
            slot.complete(Err(failure));
        }
    }
}

/// Allocates the identity a runtime graph uses to own, await, and release
/// its calls. It is random, nonzero, and below [`MAX_CALL_ID`] so OCaml can
/// carry it as a positive native `int`; a collision between two live graphs
/// would need two equal 62-bit random draws and would still only let each
/// graph address calls whose identifiers it was handed.
pub(crate) fn new_owner_id() -> u64 {
    loop {
        let (high, _) = uuid::Uuid::new_v4().as_u64_pair();
        let owner = high & (MAX_CALL_ID - 1);
        if owner != 0 {
            return owner;
        }
    }
}

/// Registers a new pending call for `owner` and returns its identifier and
/// the guard its task must complete. Registration happens before the task is
/// spawned, so the outcome always has a slot to land in.
pub(crate) fn register(owner: u64) -> std::result::Result<(u64, CompletionGuard), Failure> {
    let call = NEXT_CALL.fetch_add(1, Ordering::Relaxed);
    if call >= MAX_CALL_ID {
        return Err(Failure {
            status: STATUS_INTERNAL,
            message: "Temporal client call identifiers are exhausted".to_owned(),
        });
    }
    let slot = Arc::new(CallSlot::new());
    let mut calls = lock(&CALLS);
    calls.try_reserve(1).map_err(|_| Failure {
        status: STATUS_INTERNAL,
        message: "could not reserve a Temporal client call slot".to_owned(),
    })?;
    calls.insert(
        call,
        RegisteredCall {
            owner,
            slot: Arc::clone(&slot),
        },
    );
    Ok((call, CompletionGuard { slot: Some(slot) }))
}

/// Unregisters every call created by `owner` and closes the ones still
/// pending, waking their waiters. Called by the owner Domain when the
/// runtime disconnects or closes, before it aborts the calls' tasks, so an
/// aborted task's guard finds its slot already closed.
pub(crate) fn release_owner(owner: u64) {
    let released = {
        let mut calls = lock(&CALLS);
        let ids = calls
            .iter()
            .filter(|(_, call)| call.owner == owner)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| calls.remove(&id))
            .collect::<Vec<_>>()
    };
    // Close outside the registry lock to respect the lock order.
    for call in released {
        call.slot.close();
    }
}

/// Blocks the calling thread for at most `timeout` until the call named by
/// `(owner, call)` has an outcome, then retires the call and returns that
/// outcome unchanged.
///
/// Returns `STATUS_NOT_READY` when the interval elapses first; the call stays
/// registered and may be awaited again. Returns the closed failure
/// (`STATUS_INVALID_STATE`) when the runtime released the call, the
/// identifier is unknown, or the call belongs to a different owner; the
/// three cases are indistinguishable, and a mismatched owner neither waits on,
/// settles, nor retires the call. Any thread may call this; it never touches a
/// runtime graph. The C stub releases the OCaml runtime lock around it.
pub(crate) fn await_call(owner: u64, call: u64, timeout: Duration) -> Operation {
    let slot = match lock(&CALLS).get(&call) {
        Some(registered) if registered.owner == owner => Arc::clone(&registered.slot),
        Some(_) | None => return Err(closed_failure()),
    };
    let state = lock(&slot.state);
    let (mut state, _) = slot
        .changed
        .wait_timeout_while(state, timeout, |state| matches!(state, SlotState::Pending))
        .unwrap_or_else(PoisonError::into_inner);
    let terminal = match std::mem::replace(&mut *state, SlotState::Closed) {
        SlotState::Pending => {
            *state = SlotState::Pending;
            return Err(Failure {
                status: STATUS_NOT_READY,
                message: "Temporal client call has not completed; retry".to_owned(),
            });
        }
        SlotState::Ready(outcome) => outcome,
        SlotState::Closed => Err(closed_failure()),
    };
    drop(state);
    // Retire the identifier after releasing the slot lock (lock order). A
    // concurrent release may already have removed it, which is harmless. The
    // owner was verified above and identifiers are never reused.
    lock(&CALLS).remove(&call);
    terminal
}

/// Reports how many calls `owner` still has registered. Test-only
/// observation of the release paths.
#[cfg(test)]
pub(crate) fn registered_calls(owner: u64) -> usize {
    lock(&CALLS)
        .values()
        .filter(|call| call.owner == owner)
        .count()
}

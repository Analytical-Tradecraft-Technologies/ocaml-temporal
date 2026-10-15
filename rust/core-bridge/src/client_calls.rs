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
//!   identifier. A call is removed by exactly one of three paths: the
//!   caller's terminal read in [`await_call`], the caller abandoning it with
//!   [`release_call`] (the OCaml await loop does this when it is interrupted
//!   before the terminal read), or [`release_owner`] when the runtime that
//!   created it disconnects or closes.
//! * A call counts against its runtime's capacity from registration until
//!   that removal, whether or not its task has finished. A completed but
//!   unread outcome therefore keeps its slot, so abandoned calls cannot grow
//!   the registry past the runtime's ceiling ([`registered_calls`]).
//! * The Tokio task owns a second reference through a [`CompletionGuard`]. It
//!   never touches the runtime graph, the registry, or OCaml; it only writes
//!   its slot and wakes the waiter. Its join handle stays in the runtime's own
//!   registry so shutdown can abort and join it.
//! * The registry entry also holds the task's [`AbortHandle`], attached by
//!   the owner right after spawning ([`attach_task`]). Releasing a call
//!   aborts its task through that handle (#1019), so an abandoned long poll
//!   ends at once instead of running until its run closes or the client
//!   shuts down. Aborting needs no runtime pointer and is safe from any
//!   thread; the join handle still lets disconnect and close join the task.
//!   The owner also stops counting a call against any limit as soon as the
//!   call leaves the registry ([`unregistered_calls`]), even if the aborted
//!   task has not finished unwinding yet.
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
    Failure, Operation, STATUS_CONFIGURATION, STATUS_INTERNAL, STATUS_INVALID_STATE,
    STATUS_NOT_READY, STATUS_PANIC,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::task::AbortHandle;

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

/// One registered call: its slot, the runtime that may release it, and the
/// handle that aborts its task once the owner has attached it.
struct RegisteredCall {
    owner: u64,
    slot: Arc<CallSlot>,
    /// `None` only between [`register`] and [`attach_task`], which the owner
    /// runs back to back before the call identifier leaves the owner Domain,
    /// so no caller can release the call in that window.
    task: Option<AbortHandle>,
}

/// Registered calls plus a per-owner count of them, kept in step under one
/// mutex so admission can check a runtime's outstanding calls in O(1).
#[derive(Default)]
struct Registry {
    calls: HashMap<u64, RegisteredCall>,
    per_owner: HashMap<u64, usize>,
}

impl Registry {
    /// Removes `call` if `owner` (when given) matches, keeping the per-owner
    /// count exact; the only way an entry leaves the registry.
    fn remove(&mut self, call: u64, owner: Option<u64>) -> Option<RegisteredCall> {
        match self.calls.get(&call) {
            Some(registered) if owner.is_none_or(|owner| owner == registered.owner) => {}
            Some(_) | None => return None,
        }
        let removed = self.calls.remove(&call)?;
        if let Some(count) = self.per_owner.get_mut(&removed.owner) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.per_owner.remove(&removed.owner);
            }
        }
        Some(removed)
    }
}

/// Process-wide map from call identifier to slot. It is shared by every
/// runtime so that a caller can await its call without borrowing the runtime
/// graph, which only its owner Domain may touch.
static CALLS: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(Registry::default()));

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
    let mut registry = lock(&CALLS);
    let reserved = registry
        .calls
        .try_reserve(1)
        .and_then(|()| registry.per_owner.try_reserve(1));
    reserved.map_err(|_| Failure {
        status: STATUS_INTERNAL,
        message: "could not reserve a Temporal client call slot".to_owned(),
    })?;
    *registry.per_owner.entry(owner).or_insert(0) += 1;
    registry.calls.insert(
        call,
        RegisteredCall {
            owner,
            slot: Arc::clone(&slot),
            task: None,
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
        let mut registry = lock(&CALLS);
        let ids = registry
            .calls
            .iter()
            .filter(|(_, call)| call.owner == owner)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| registry.remove(id, Some(owner)))
            .collect::<Vec<_>>()
    };
    // Close outside the registry lock to respect the lock order.
    for call in released {
        call.slot.close();
    }
}

/// Records the abort handle of the task the owner just spawned for `call`,
/// so a later [`release_call`] can end that task (#1019). The owner calls
/// this before the call identifier is returned to OCaml, so the call is
/// always still registered; if it somehow is not, the task is aborted at
/// once rather than left running unowned.
pub(crate) fn attach_task(owner: u64, call: u64, task: AbortHandle) {
    let orphan = {
        let mut registry = lock(&CALLS);
        match registry.calls.get_mut(&call) {
            Some(registered) if registered.owner == owner => {
                registered.task = Some(task);
                None
            }
            Some(_) | None => Some(task),
        }
    };
    if let Some(task) = orphan {
        task.abort();
    }
}

/// Abandons the call named by `(owner, call)` without reading it: the call
/// is unregistered, which frees its capacity slot and drops any outcome
/// already published, and its task is aborted (#1019). An abandoned long
/// poll, such as an interrupted `Client.wait` on a run that stays open, would
/// otherwise keep its server request and its owner's wait and in-flight
/// slots until the run closed or the client shut down. Aborting through the
/// [`AbortHandle`] touches neither the runtime graph nor its task registry;
/// the owner still holds the join handle, stops counting the call at its
/// next submission, and joins the task at disconnect or close. An aborted
/// task's completion guard finds its cell already closed. A mismatched owner,
/// a retired call, or an unknown identifier is a no-op, so callers may
/// release unconditionally and learn nothing about other owners' calls. Any
/// thread may call this.
pub(crate) fn release_call(owner: u64, call: u64) {
    let released = lock(&CALLS).remove(call, Some(owner));
    if let Some(released) = released {
        // Close before aborting, and outside the registry lock (lock order):
        // a task dropped by the abort settles its guard against the slot.
        released.slot.close();
        if let Some(task) = released.task {
            task.abort();
        }
    }
}

/// Returns those of `calls` that are no longer registered under `owner`:
/// read, released, or closed. The owner uses this at submission to stop
/// counting such calls against its limits while their tasks finish (#1019).
pub(crate) fn unregistered_calls(owner: u64, calls: impl Iterator<Item = u64>) -> Vec<u64> {
    let registry = lock(&CALLS);
    calls
        .filter(|call| {
            !registry
                .calls
                .get(call)
                .is_some_and(|registered| registered.owner == owner)
        })
        .collect()
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
    let slot = match lock(&CALLS).calls.get(&call) {
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
    lock(&CALLS).remove(call, Some(owner));
    terminal
}

/// Reports how many calls `owner` has registered: submitted and not yet
/// read, released, or closed, whether or not their tasks have finished. The
/// runtime's admission ceiling is checked against this count.
pub(crate) fn registered_calls(owner: u64) -> usize {
    lock(&CALLS).per_owner.get(&owner).copied().unwrap_or(0)
}

/// Largest accepted `max_pending_waits` (#796). Each distinct waited run holds
/// one Tokio task and one open history long poll, so the bound keeps a typo
/// from turning one client into thousands of server long polls.
pub(crate) const MAX_CONFIGURABLE_PENDING_WAITS: u32 = 4_096;
/// Largest accepted `max_pending_starts` (#796), for the same reason as
/// [`MAX_CONFIGURABLE_PENDING_WAITS`]: each start holds one task and request.
pub(crate) const MAX_CONFIGURABLE_PENDING_STARTS: u32 = 4_096;
/// Largest accepted `max_in_flight_calls` (#796). The ceiling bounds the
/// process-wide completion registry and task set of one runtime; much beyond
/// it, a leak of abandoned calls would no longer be caught in practice.
pub(crate) const MAX_CONFIGURABLE_IN_FLIGHT_CALLS: u32 = 65_536;

/// Admission limits of one connected client (#796).
///
/// The runtime owner reads these when it admits a start, an exact-run wait,
/// or any submitted call; admission beyond a limit returns
/// `STATUS_RESOURCE_EXHAUSTED` before any task or RPC exists. The values are
/// fixed for the lifetime of one connection: [`ClientLimits::from_config`]
/// validates the optional fields of the client connect document, disconnect
/// resets them, and a runtime without a connection reports the defaults. All three are counts and
/// every value is at least one. The in-flight ceiling applies on top of the
/// start and wait limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ClientLimits {
    /// Distinct exact runs being waited on; repeated waits share one slot.
    pub(crate) pending_waits: usize,
    /// Distinct start request IDs in flight; a retry shares its slot.
    pub(crate) pending_starts: usize,
    /// Submitted calls of every kind outstanding until their cells retire.
    pub(crate) in_flight_calls: usize,
}

impl Default for ClientLimits {
    /// The limits used before they became configurable: 64 waits, 64
    /// starts, and 4,096 calls. A connect document without limit fields
    /// therefore behaves exactly as it did before #796.
    fn default() -> Self {
        Self {
            pending_waits: crate::abi::MAX_PENDING_WAITS,
            pending_starts: crate::abi::MAX_PENDING_STARTS,
            in_flight_calls: crate::abi::MAX_PENDING_CALLS,
        }
    }
}

impl ClientLimits {
    /// Validates the optional connect-document limits and fills omitted ones
    /// with the defaults. A value outside `1..=maximum` is a
    /// `STATUS_CONFIGURATION` failure naming the field; OCaml validates the
    /// same bounds first, so this is the defense-in-depth check.
    pub(crate) fn from_config(
        pending_waits: Option<u32>,
        pending_starts: Option<u32>,
        in_flight_calls: Option<u32>,
    ) -> std::result::Result<Self, Failure> {
        let defaults = Self::default();
        Ok(Self {
            pending_waits: limit(
                "max_pending_waits",
                pending_waits,
                MAX_CONFIGURABLE_PENDING_WAITS,
                defaults.pending_waits,
            )?,
            pending_starts: limit(
                "max_pending_starts",
                pending_starts,
                MAX_CONFIGURABLE_PENDING_STARTS,
                defaults.pending_starts,
            )?,
            in_flight_calls: limit(
                "max_in_flight_calls",
                in_flight_calls,
                MAX_CONFIGURABLE_IN_FLIGHT_CALLS,
                defaults.in_flight_calls,
            )?,
        })
    }
}

/// Checks one optional limit against `1..=maximum`, returning `default` when
/// it is absent.
fn limit(
    name: &str,
    value: Option<u32>,
    maximum: u32,
    default: usize,
) -> std::result::Result<usize, Failure> {
    match value {
        None => Ok(default),
        Some(value) if (1..=maximum).contains(&value) => {
            usize::try_from(value).map_err(|_| limit_failure(name, maximum))
        }
        Some(_) => Err(limit_failure(name, maximum)),
    }
}

/// The configuration failure for an out-of-range limit. The message carries
/// only the field name and the accepted range, never the rejected value.
fn limit_failure(name: &str, maximum: u32) -> Failure {
    Failure {
        status: STATUS_CONFIGURATION,
        message: format!("client {name} must be between 1 and {maximum}"),
    }
}

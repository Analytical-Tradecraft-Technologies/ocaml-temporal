//! Per-token throttling of heartbeats for activities handed off with
//! `WillCompleteAsync` (#1025).
//!
//! A running activity's heartbeats go through Core's heartbeat manager,
//! which throttles them per task token and sends them in the background. An
//! async handle's heartbeats are namespace-bound client RPCs that Core never
//! sees, so without this module every `Async_handle.heartbeat` was a separate
//! server round trip. [`HeartbeatThrottle`] applies Core's rules
//! (`activity_heartbeat_manager.rs` at the pinned revision) to those RPCs:
//!
//! - The first heartbeat for a token is sent at once, and its caller receives
//!   that send's outcome.
//! - Until the throttle interval has passed since that send started, later
//!   heartbeats only replace the token's pending details; their callers
//!   return at once.
//! - When the interval expires, a trailing send carries the latest pending
//!   details and opens the next interval. When nothing is pending, the
//!   token's state is forgotten, so the next heartbeat is again sent at once.
//!
//! The interval is 80% of the attempt's heartbeat timeout, or the worker's
//! default interval when no timeout is set, capped by the worker's maximum
//! ([`HeartbeatIntervals::interval`], Core's `record_heartbeat`). Consecutive
//! sends are therefore never further apart than the interval plus one RPC, so
//! a caller that keeps heartbeating more often than the heartbeat timeout
//! keeps the server's heartbeat timer satisfied.
//!
//! Unlike Core, which only logs a failed background send, any failed send
//! here ends the token's interval. The next heartbeat is then sent at once
//! and its caller observes the server's answer itself, so a lost token, a
//! rejected request, or an unreachable server is never hidden behind a
//! coalesced success for longer than one interval.
//!
//! # Ownership
//!
//! The runtime owner ([`crate::abi::Runtime`]) holds the throttle in an
//! `Arc`; submitted heartbeat calls and drivers hold clones. Each token with
//! state has exactly one driver task, spawned on Core's Tokio runtime, which
//! owns a client (and so a connection clone) and performs every send for
//! that token. A driver's `JoinHandle` is retained in the throttle until it
//! is reaped, so disconnect and close can abort and join every driver before
//! the connection is dropped, as they do for submitted calls. A token's state
//! is removed by its driver (quiet interval or failed send), by
//! [`HeartbeatThrottle::evict`] before a terminal request for the token, or
//! by [`HeartbeatThrottle::shutdown`]. State is created only while fewer
//! than the connection's in-flight call limit of tokens are tracked; beyond
//! that, heartbeats are sent unthrottled, as before #1025, so memory stays
//! bounded however many tokens application code heartbeats.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use temporalio_client::Client;
use temporalio_common::protos::temporal::api::common::v1::Payloads;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::abi::{Failure, STATUS_CONNECTION};

/// Server-reported state of an activity, returned by every accepted
/// `RecordActivityTaskHeartbeat` RPC.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HeartbeatFlags {
    /// The workflow requested cancellation of the activity.
    pub(crate) cancel_requested: bool,
    /// The activity has been paused.
    pub(crate) paused: bool,
    /// The activity has been reset.
    pub(crate) reset: bool,
}

impl HeartbeatFlags {
    /// The closed submitted-call outcome body for these flags. Cancellation
    /// takes precedence over reset and reset over pause, the order Core uses
    /// to choose a single cancel reason for a running activity; an empty body
    /// means none is set.
    pub(crate) fn outcome_body(self) -> &'static [u8] {
        if self.cancel_requested {
            b"cancel_requested"
        } else if self.reset {
            b"reset"
        } else if self.paused {
            b"paused"
        } else {
            b""
        }
    }
}

/// Outcome of one heartbeat RPC: the server's activity flags, or a failure
/// classified as for any async activity request (`STATUS_INVALID_STATE` for
/// a token the server no longer knows).
pub(crate) type SendOutcome = Result<HeartbeatFlags, Failure>;

/// The worker's heartbeat throttle settings, validated with the worker
/// configuration and handed to Core unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HeartbeatIntervals {
    /// Upper bound on every throttle interval
    /// (`max_heartbeat_throttle_interval`).
    pub(crate) max: Duration,
    /// Interval for an attempt without a heartbeat timeout
    /// (`default_heartbeat_throttle_interval`).
    pub(crate) default: Duration,
}

impl HeartbeatIntervals {
    /// Throttle interval for an attempt with `heartbeat_timeout`, computed
    /// exactly as Core's `ActivitiesManager::record_heartbeat` does: a
    /// timeout below one millisecond counts as unset (the server reports an
    /// unset timeout as zero), an unset timeout uses the default interval,
    /// and a set one uses 80% of it; the result is capped by the maximum.
    pub(crate) fn interval(self, heartbeat_timeout: Option<Duration>) -> Duration {
        let interval = match heartbeat_timeout {
            Some(timeout) if timeout.as_millis() != 0 => timeout.mul_f64(0.8),
            _ => self.default,
        };
        interval.min(self.max)
    }
}

/// How [`HeartbeatThrottle::record`] admitted one heartbeat.
pub(crate) enum Admission {
    /// The heartbeat opened a new interval and is being sent at once; the
    /// receiver yields that send's outcome.
    Sent(oneshot::Receiver<SendOutcome>),
    /// The heartbeat replaced the token's pending details, which the trailing
    /// send will carry. The flags are those of the latest completed send.
    Coalesced(HeartbeatFlags),
    /// The throttle tracks the maximum number of tokens, or has shut down, so
    /// the caller sends these details itself without throttling.
    Unthrottled(Option<Payloads>),
}

/// Throttle state of one token.
struct TokenState {
    /// Identifies this state and its driver, so a driver never updates state
    /// created for the same token after its own was removed.
    id: u64,
    /// Latest details recorded since the last send started, if any. The
    /// outer `Option` is whether a trailing send is due.
    pending: Option<Option<Payloads>>,
    /// Flags returned by the latest completed send.
    flags: HeartbeatFlags,
    /// Whether the driver is waiting for an RPC rather than sleeping. Only a
    /// sleeping driver is aborted by [`HeartbeatThrottle::evict`].
    sending: bool,
}

/// Mutable throttle state, guarded by [`HeartbeatThrottle::state`].
#[derive(Default)]
struct ThrottleState {
    /// State of every token whose driver is running.
    tokens: HashMap<Vec<u8>, TokenState>,
    /// Driver tasks by state id. Finished drivers are reaped whenever a new
    /// one is spawned, so the map holds at most one live driver per tracked
    /// token plus drivers that finished since the last spawn.
    drivers: HashMap<u64, JoinHandle<()>>,
    /// Source of [`TokenState::id`].
    next_id: u64,
    /// Set by [`HeartbeatThrottle::shutdown`]; no driver is spawned after it.
    closed: bool,
}

/// Per-token heartbeat throttle for one runtime's async activity client
/// calls. See the module documentation for the protocol and ownership.
#[derive(Default)]
pub(crate) struct HeartbeatThrottle {
    state: Mutex<ThrottleState>,
}

impl HeartbeatThrottle {
    /// Creates an empty throttle.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Locks the state. Every critical section is a short map edit that
    /// leaves the state consistent if it panics, so a poisoned lock is used
    /// as is.
    fn lock(&self) -> MutexGuard<'_, ThrottleState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Admits one heartbeat for `task_token`.
    ///
    /// A token without state gets new state and a driver spawned on
    /// `runtime`, which sends `details` at once through `client` and then
    /// throttles the token with `interval`. A token with state only records
    /// `details` for the trailing send. At most `max_tokens` tokens have
    /// state; a heartbeat for another token is returned as
    /// [`Admission::Unthrottled`], as is every heartbeat after
    /// [`Self::shutdown`].
    pub(crate) fn record(
        self: &Arc<Self>,
        runtime: &tokio::runtime::Handle,
        client: Client,
        task_token: Vec<u8>,
        details: Option<Payloads>,
        interval: Duration,
        max_tokens: usize,
    ) -> Admission {
        let mut state = self.lock();
        if let Some(token) = state.tokens.get_mut(&task_token) {
            token.pending = Some(details);
            return Admission::Coalesced(token.flags);
        }
        if state.closed || state.tokens.len() >= max_tokens {
            return Admission::Unthrottled(details);
        }
        state.drivers.retain(|_, driver| !driver.is_finished());
        let id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        state.tokens.insert(
            task_token.clone(),
            TokenState {
                id,
                pending: None,
                flags: HeartbeatFlags::default(),
                sending: true,
            },
        );
        let (reply, outcome) = oneshot::channel();
        // Spawning under the lock orders it before any eviction or shutdown
        // of this token, so neither can miss the driver's handle.
        let driver = runtime.spawn(drive(
            Arc::clone(self),
            client,
            task_token,
            id,
            details,
            interval,
            reply,
        ));
        state.drivers.insert(id, driver);
        Admission::Sent(outcome)
    }

    /// Forgets `task_token` before a terminal request for it, as Core evicts
    /// a completed activity from its heartbeat manager, and returns the
    /// details recorded since the last send, if any. A sleeping driver is
    /// aborted; a driver whose send is in flight finishes that RPC and then
    /// finds its state gone.
    ///
    /// The aborted driver's handle stays in the throttle until it is reaped,
    /// so shutdown can still join it if its cancellation has not run yet.
    pub(crate) fn evict(&self, task_token: &[u8]) -> Option<Option<Payloads>> {
        let (token, sleeping_driver) = {
            let mut state = self.lock();
            let token = state.tokens.remove(task_token)?;
            let driver = if token.sending {
                None
            } else {
                state.drivers.get(&token.id).map(JoinHandle::abort_handle)
            };
            (token, driver)
        };
        // Aborted outside the lock: cancelling the driver drops its guard,
        // which takes the lock.
        if let Some(driver) = sleeping_driver {
            driver.abort();
        }
        token.pending
    }

    /// Forgets every token, refuses new drivers, and aborts every driver,
    /// returning their handles for the caller to join before the connection
    /// they hold clones of is dropped.
    pub(crate) fn shutdown(&self) -> Vec<JoinHandle<()>> {
        let drivers = {
            let mut state = self.lock();
            state.closed = true;
            state.tokens.clear();
            state
                .drivers
                .drain()
                .map(|(_, driver)| driver)
                .collect::<Vec<_>>()
        };
        // Aborted outside the lock, as in `evict`.
        for driver in &drivers {
            driver.abort();
        }
        drivers
    }

    /// Tracked tokens and retained driver handles (after reaping finished
    /// drivers), for the bounded-memory tests.
    pub(crate) fn counts(&self) -> (usize, usize) {
        let mut state = self.lock();
        state.drivers.retain(|_, driver| !driver.is_finished());
        (state.tokens.len(), state.drivers.len())
    }

    /// Records the outcome of the driver `id`'s send. Returns whether the
    /// driver should keep throttling: its state still exists and the send
    /// succeeded. A failed send removes the state, so the next heartbeat is
    /// sent at once and reports its own outcome.
    fn finish_send(&self, task_token: &[u8], id: u64, outcome: &SendOutcome) -> bool {
        let mut state = self.lock();
        let Some(token) = state
            .tokens
            .get_mut(task_token)
            .filter(|token| token.id == id)
        else {
            return false;
        };
        token.sending = false;
        match outcome {
            Ok(flags) => {
                token.flags = *flags;
                true
            }
            Err(_) => {
                state.tokens.remove(task_token);
                false
            }
        }
    }

    /// Takes the pending details for the driver `id`'s trailing send, or
    /// removes the token's state when nothing is pending. Both happen under
    /// one lock, so a heartbeat recorded concurrently either becomes the
    /// trailing send or finds no state and opens a new interval.
    fn take_pending(&self, task_token: &[u8], id: u64) -> Option<Option<Payloads>> {
        let mut state = self.lock();
        let token = state
            .tokens
            .get_mut(task_token)
            .filter(|token| token.id == id)?;
        match token.pending.take() {
            Some(details) => {
                token.sending = true;
                Some(details)
            }
            None => {
                state.tokens.remove(task_token);
                None
            }
        }
    }

    /// Removes the driver `id`'s state if it still exists; used only when a
    /// driver unwinds, so a panic cannot leave a token throttled forever.
    fn remove_if_current(&self, task_token: &[u8], id: u64) {
        let mut state = self.lock();
        if state
            .tokens
            .get(task_token)
            .is_some_and(|token| token.id == id)
        {
            state.tokens.remove(task_token);
        }
    }
}

/// Removes a driver's state when the driver ends without doing so itself.
struct DriverGuard {
    throttle: Arc<HeartbeatThrottle>,
    task_token: Vec<u8>,
    id: u64,
}

impl Drop for DriverGuard {
    /// Normal exits have already removed the state, so this matters only
    /// for a panic; abort happens only after the state was removed.
    fn drop(&mut self) {
        self.throttle.remove_if_current(&self.task_token, self.id);
    }
}

/// Performs every send for one token: the immediate first send, whose
/// outcome goes to `reply`, then one trailing send per elapsed interval while
/// details are pending. Each interval starts when its send starts, as in
/// Core, so consecutive sends start at most `interval` plus one RPC apart.
async fn drive(
    throttle: Arc<HeartbeatThrottle>,
    client: Client,
    task_token: Vec<u8>,
    id: u64,
    first: Option<Payloads>,
    interval: Duration,
    reply: oneshot::Sender<SendOutcome>,
) {
    let _guard = DriverGuard {
        throttle: Arc::clone(&throttle),
        task_token: task_token.clone(),
        id,
    };
    let mut details = first;
    let mut reply = Some(reply);
    loop {
        let started = tokio::time::Instant::now();
        let outcome =
            crate::abi::send_async_heartbeat(client.clone(), task_token.clone(), details).await;
        let keep = throttle.finish_send(&task_token, id, &outcome);
        if let Some(reply) = reply.take() {
            // The caller may have been released; its outcome is then unused.
            let _ = reply.send(outcome);
        }
        if !keep {
            return;
        }
        tokio::time::sleep_until(started + interval).await;
        match throttle.take_pending(&task_token, id) {
            Some(next) => details = next,
            None => return,
        }
    }
}

/// The failure of a heartbeat whose driver ended before reporting the first
/// send's outcome: the runtime shut down or the driver panicked, so whether
/// the server recorded it is unknown.
pub(crate) fn unknown_outcome() -> Failure {
    Failure {
        status: STATUS_CONNECTION,
        message: "Temporal asynchronous activity heartbeat outcome is unknown".to_owned(),
    }
}

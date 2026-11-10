//! Bounded, asynchronous workflow-completion admission for one live worker.
//!
//! Temporal Core's `complete_workflow_activation` may wait for the network:
//! it reports the workflow task to the server and can fetch more history
//! before it returns, and Core documents that completions should run
//! concurrently for that reason. The OCaml supervisor is the sole owner of
//! the native graph, so awaiting that future on the owner Domain stalled
//! every other workflow run, the activity lane, and lifecycle messages until
//! the RPC finished. This module splits a live completion into two steps:
//!
//! 1. *Admission* (owner Domain, never waits for Core or the network): the
//!    caller has already validated and converted the completion and retired
//!    its ledger lease; [`WorkflowCompletions::admit`] moves the run's
//!    delivery credit to a numbered ticket, and the caller spawns one Tokio
//!    task that owns the Core completion and an [`OutcomeGuard`].
//! 2. *Outcome delivery* (any Tokio thread): the task publishes exactly one
//!    [`WorkflowCompletionOutcome`] into a bounded queue and wakes the
//!    worker's combined readiness wait. The owner drains the queue without
//!    blocking through [`WorkflowCompletions::take_ready`].
//!
//! # Credits
//!
//! One credit covers one workflow activation from the moment it is handed to
//! OCaml until OCaml has collected the outcome of its completion. A credit is
//! held in exactly one of three places: the `delivered` map (activation
//! leased to OCaml), the in-flight count (Tokio task awaiting Core), or the
//! `ready` queue (outcome not yet collected). The workflow poll checks
//! [`WorkflowCompletions::has_credit`] before it dequeues an activation, so
//! OCaml can always submit a completion for every lease it already holds and
//! the ready queue can never exceed the capacity. When no credit is free the
//! workflow lane stops delivering activations (Core keeps polling, and its
//! own slots bound what it hands the bridge) and the combined readiness wait
//! wakes on outcome readiness instead of on queued activations, so a full
//! window neither spins nor stalls the activity lane.
//!
//! # Ownership
//!
//! The struct is shared through an `Arc` between the owner Domain (admission,
//! drain, waits) and completion tasks (publication only). Tasks never touch
//! the runtime graph, the OCaml runtime, or the readiness channels; they hold
//! the Core worker, the task ledger, and this queue. [`crate::worker_bridge::
//! PollLanes`] owns the tasks' join handles and joins (or, during disposal,
//! aborts and joins) them before the Core worker is finalized.
//!
//! Lock order: the state mutex is a leaf. It is never held while another
//! bridge mutex is taken, and a publisher releases it before waking the
//! combined readiness condition.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// Largest completion window derived from a worker configuration. Core
/// already bounds outstanding workflow tasks; this cap only keeps a
/// misconfigured worker from reserving an absurd queue.
pub const MAX_WORKFLOW_COMPLETION_CAPACITY: usize = 65_536;

/// Why an admitted completion has no Core result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompletionFailure {
    /// Core rejected the completion. The bridge restored the run's ledger
    /// lease, because a rejected request never landed with Core. The text is
    /// Core's internal diagnostic and must not cross the ABI.
    CoreRejected(String),
    /// The completion task panicked. Core may or may not have consumed the
    /// completion, so its outcome is unknown and the lease stays retired.
    Panicked,
    /// The completion task was aborted (worker disposal) or its executor shut
    /// down before Core replied. The outcome is unknown.
    Aborted,
}

/// The one result published by an admitted workflow completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowCompletionOutcome {
    /// The ticket returned by admission. Tickets are lease generations: every
    /// activation handed to OCaml gets a fresh one, so a late outcome can
    /// never be confused with a newer activation of the same run.
    pub ticket: u64,
    /// The completion's run identifier, for correlation and stale checks.
    pub run_id: String,
    /// `Ok` once Core's `complete_workflow_activation` returned successfully.
    /// That proves Core accepted the completion, not that the server recorded
    /// a history event.
    pub result: Result<(), CompletionFailure>,
    /// Monotonic time spent awaiting Core, including any RPC it made.
    pub core_await: Duration,
    /// Monotonic time the outcome waited in the queue before the owner
    /// collected it. Zero until [`WorkflowCompletions::take_ready`] sets it.
    pub queued: Duration,
}

/// Result of [`WorkflowCompletions::wait_ready`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionWait {
    /// At least one outcome is ready to be collected.
    Ready,
    /// No outcome became ready within the bound; tasks are still in flight.
    TimedOut,
    /// No completion is in flight and none is ready, so waiting cannot help.
    Idle,
}

/// Outcome stored with the instant it became ready, so queue dwell can be
/// measured when it is collected.
struct ReadyOutcome {
    outcome: WorkflowCompletionOutcome,
    ready_at: Instant,
}

/// Mutable state protected by [`WorkflowCompletions::state`].
struct CompletionState {
    /// Maximum credits; `None` for a replay worker, whose completions stay on
    /// their synchronous, bounded contract and whose deliveries are not
    /// windowed.
    capacity: Option<usize>,
    /// Activations leased to OCaml, keyed by run, with their lease generation.
    /// The task ledger allows at most one lease per run at a time.
    delivered: HashMap<String, u64>,
    /// Admitted completions whose task has not published an outcome yet.
    in_flight: usize,
    /// Published outcomes awaiting collection, in publication order.
    ready: VecDeque<ReadyOutcome>,
    /// Next lease generation. Starts at 1 so a ticket is never zero.
    next_generation: u64,
    /// Lifetime counters exposed for tests and diagnostics.
    admitted_total: u64,
    /// Number of times a poll found the window full.
    window_full_total: u64,
}

impl CompletionState {
    /// Credits currently held anywhere in the pipeline.
    fn held(&self) -> usize {
        self.delivered.len() + self.in_flight + self.ready.len()
    }
}

/// Wake hook shared with the worker's combined readiness wait. The hook
/// is called after every publication with no lock of this module held.
pub type WakeHook = Arc<dyn Fn() + Send + Sync>;

/// Worker-wide completion window and outcome queue; see the module docs.
pub struct WorkflowCompletions {
    state: Mutex<CompletionState>,
    /// Wakes [`Self::wait_ready`] after a publication.
    changed: Condvar,
    /// Wakes the worker's combined readiness wait after a publication.
    wake: WakeHook,
}

/// Locks a mutex whose state stays consistent if a holder panicked: every
/// critical section is a short sequence of counter and map updates.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl WorkflowCompletions {
    /// Creates an empty window. `capacity` of `None` disables the window (a
    /// replay worker); `Some(n)` is clamped to `1..=MAX_WORKFLOW_COMPLETION_CAPACITY`.
    pub fn new(capacity: Option<usize>, wake: WakeHook) -> Self {
        Self {
            state: Mutex::new(CompletionState {
                capacity: capacity.map(|n| n.clamp(1, MAX_WORKFLOW_COMPLETION_CAPACITY)),
                delivered: HashMap::new(),
                in_flight: 0,
                ready: VecDeque::new(),
                next_generation: 1,
                admitted_total: 0,
                window_full_total: 0,
            }),
            changed: Condvar::new(),
            wake,
        }
    }

    /// The configured window, or `None` when it is disabled.
    pub fn capacity(&self) -> Option<usize> {
        lock(&self.state).capacity
    }

    /// Whether another activation may be handed to OCaml now. A `false`
    /// result is counted, so tests can prove the window actually filled.
    pub fn has_credit(&self) -> bool {
        let mut state = lock(&self.state);
        let free = state
            .capacity
            .is_none_or(|capacity| state.held() < capacity);
        if !free {
            state.window_full_total = state.window_full_total.saturating_add(1);
        }
        free
    }

    /// Whether a credit is free, without counting a full observation. Used
    /// by readiness predicates, which may evaluate it many times per wait.
    pub fn credit_available(&self) -> bool {
        let state = lock(&self.state);
        state
            .capacity
            .is_none_or(|capacity| state.held() < capacity)
    }

    /// Records that the activation for `run_id` was handed to OCaml and
    /// returns its lease generation. Called only after the ledger lease
    /// succeeded, so a run is present at most once; a stale entry (which the
    /// ledger makes unreachable) is replaced rather than double-counted.
    pub fn deliver(&self, run_id: &str) -> u64 {
        let mut state = lock(&self.state);
        let generation = state.next_generation;
        state.next_generation = state.next_generation.wrapping_add(1).max(1);
        state.delivered.insert(run_id.to_owned(), generation);
        generation
    }

    /// Releases the credit of an activation that was retired without an
    /// admitted completion: a rejection after hand-off, a synchronous
    /// completion Core accepted, or a replay completion. Unknown runs are a
    /// no-op, so every retirement path may call it unconditionally.
    pub fn retire_delivered(&self, run_id: &str) {
        lock(&self.state).delivered.remove(run_id);
    }

    /// Moves the credit of `run_id` from its delivered activation to an
    /// in-flight completion and returns the ticket (its lease generation).
    /// A run without a recorded delivery (not reachable through the ABI,
    /// whose ledger check runs first) receives a fresh generation, so the
    /// in-flight count is never left without a ticket.
    pub fn admit(&self, run_id: &str) -> u64 {
        let mut state = lock(&self.state);
        let ticket = match state.delivered.remove(run_id) {
            Some(generation) => generation,
            None => {
                let generation = state.next_generation;
                state.next_generation = state.next_generation.wrapping_add(1).max(1);
                generation
            }
        };
        state.in_flight += 1;
        state.admitted_total = state.admitted_total.saturating_add(1);
        ticket
    }

    /// Publishes the outcome of one admitted completion and wakes waiters.
    /// Only [`OutcomeGuard`] calls this, exactly once per admission.
    fn publish(&self, outcome: WorkflowCompletionOutcome) {
        {
            let mut state = lock(&self.state);
            debug_assert!(state.in_flight > 0);
            state.in_flight = state.in_flight.saturating_sub(1);
            state.ready.push_back(ReadyOutcome {
                outcome,
                ready_at: Instant::now(),
            });
            self.changed.notify_all();
        }
        // Released before the combined wake (lock order).
        (self.wake)();
    }

    /// Removes and returns every ready outcome, oldest first, with its queue
    /// dwell measured now. Collecting an outcome releases its credit.
    pub fn take_ready(&self) -> Vec<WorkflowCompletionOutcome> {
        let now = Instant::now();
        let mut state = lock(&self.state);
        state
            .ready
            .drain(..)
            .map(|ready| {
                let mut outcome = ready.outcome;
                outcome.queued = now.saturating_duration_since(ready.ready_at);
                outcome
            })
            .collect()
    }

    /// Whether an outcome is waiting to be collected.
    pub fn has_ready(&self) -> bool {
        !lock(&self.state).ready.is_empty()
    }

    /// Admitted completions not yet collected (in flight plus ready).
    pub fn outstanding(&self) -> usize {
        let state = lock(&self.state);
        state.in_flight + state.ready.len()
    }

    /// Credits held anywhere in the pipeline, including delivered leases.
    pub fn held(&self) -> usize {
        lock(&self.state).held()
    }

    /// Lifetime number of admitted completions.
    pub fn admitted_total(&self) -> u64 {
        lock(&self.state).admitted_total
    }

    /// Lifetime number of polls that found the window full.
    pub fn window_full_total(&self) -> u64 {
        lock(&self.state).window_full_total
    }

    /// Blocks for at most `timeout` until an outcome is ready. Returns
    /// [`CompletionWait::Idle`] at once when nothing is in flight. The C stub
    /// releases the OCaml runtime lock around this wait.
    pub fn wait_ready(&self, timeout: Duration) -> CompletionWait {
        let deadline = Instant::now() + timeout;
        let mut state = lock(&self.state);
        loop {
            if !state.ready.is_empty() {
                return CompletionWait::Ready;
            }
            if state.in_flight == 0 {
                return CompletionWait::Idle;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return CompletionWait::TimedOut;
            }
            let (next, _) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner);
            state = next;
        }
    }

    /// Forgets every delivered activation and every uncollected outcome.
    /// Called when shutdown or disposal has retired all leases on OCaml's
    /// behalf; in-flight tasks still publish and are joined separately.
    pub fn clear_for_shutdown(&self) {
        let mut state = lock(&self.state);
        state.delivered.clear();
        state.ready.clear();
    }
}

/// Task-side capability that publishes one admitted completion's outcome
/// exactly once.
///
/// If the task ends without calling [`Self::finish`], the drop still
/// publishes an outcome: [`CompletionFailure::Panicked`] while a panic is
/// unwinding, otherwise [`CompletionFailure::Aborted`]. The in-flight count
/// and any waiter can therefore never be stranded by a task that is gone.
pub struct OutcomeGuard {
    completions: Option<Arc<WorkflowCompletions>>,
    ticket: u64,
    run_id: String,
    started: Instant,
}

impl OutcomeGuard {
    /// Arms a guard for the completion admitted as `ticket`.
    pub fn new(completions: Arc<WorkflowCompletions>, ticket: u64, run_id: String) -> Self {
        Self {
            completions: Some(completions),
            ticket,
            run_id,
            started: Instant::now(),
        }
    }

    /// Publishes Core's result and disarms the drop fallback.
    pub fn finish(mut self, result: Result<(), CompletionFailure>) {
        self.publish(result);
    }

    /// Shared body of [`Self::finish`] and the drop fallback.
    fn publish(&mut self, result: Result<(), CompletionFailure>) {
        if let Some(completions) = self.completions.take() {
            completions.publish(WorkflowCompletionOutcome {
                ticket: self.ticket,
                run_id: std::mem::take(&mut self.run_id),
                result,
                core_await: self.started.elapsed(),
                queued: Duration::ZERO,
            });
        }
    }
}

impl Drop for OutcomeGuard {
    /// Publishes an unknown outcome for a task that ended without one.
    fn drop(&mut self) {
        let failure = if std::thread::panicking() {
            CompletionFailure::Panicked
        } else {
            CompletionFailure::Aborted
        };
        self.publish(Err(failure));
    }
}

/// Stable name of an outcome's result in the JSON document read by OCaml.
/// Core's rejection text stays inside Rust: it may carry server data.
fn outcome_kind(result: &Result<(), CompletionFailure>) -> &'static str {
    match result {
        Ok(()) => "completed",
        Err(CompletionFailure::CoreRejected(_)) => "rejected",
        Err(CompletionFailure::Panicked) => "panicked",
        Err(CompletionFailure::Aborted) => "aborted",
    }
}

/// Encodes collected outcomes as the closed JSON document OCaml decodes:
/// `{"outcomes":[{"ticket":n,"run_id":s,"result":k,"core_await_us":n,"queued_us":n}]}`,
/// where `k` is `completed`, `rejected`, `panicked`, or `aborted`. Durations
/// are whole microseconds, saturated to `u64`. The array is wrapped in an
/// object so OCaml reads it with the same strict object decoder as every
/// other bridge document.
pub fn encode_outcomes(outcomes: &[WorkflowCompletionOutcome]) -> Vec<u8> {
    let micros = |duration: Duration| u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
    let documents = outcomes
        .iter()
        .map(|outcome| {
            serde_json::json!({
                "ticket": outcome.ticket,
                "run_id": outcome.run_id,
                "result": outcome_kind(&outcome.result),
                "core_await_us": micros(outcome.core_await),
                "queued_us": micros(outcome.queued),
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({ "outcomes": documents })
        .to_string()
        .into_bytes()
}

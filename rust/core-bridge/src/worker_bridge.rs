//! Task ownership and shutdown admission for the private Core worker bridge.
//!
//! This module contains no OCaml-facing types. It centralizes the invariants
//! shared by the Rust-owned workflow and activity poll lanes so that task
//! identity, completion, and worker finalization cannot race through separate
//! ad-hoc state machines.

use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use temporalio_common::protos::coresdk::{
    ActivityHeartbeat, ActivityTaskCompletion,
    activity_result::ActivityExecutionResult,
    activity_task::{ActivityTask, activity_task},
    workflow_activation::WorkflowActivation,
    workflow_completion::WorkflowActivationCompletion,
};
use temporalio_common::protos::temporal::api::enums::v1::WorkflowTaskFailedCause;
use temporalio_common::protos::temporal::api::failure::v1::{
    ApplicationFailureInfo, Failure as TemporalFailure, failure::FailureInfo,
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk_core::{PollError, Worker};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;

/// Maximum UTF-8 byte length accepted for a workflow run identifier.
const MAX_RUN_ID_BYTES: usize = 64 * 1024;
/// Maximum opaque task-token length admitted into the private bridge.
const MAX_TASK_TOKEN_BYTES: usize = 128 * 1024 * 1024;
/// Minimum delay between attempts to submit a completion whose transport
/// outcome is explicitly marked retryable.
///
/// This timer is deliberately owned by the Rust supervisor Domain rather than
/// by an OCaml workflow scheduler. It is short enough to bound shutdown
/// admission latency while still preventing a ready activity lane from
/// turning a transient completion failure into a tight loop.
pub const ACTIVITY_COMPLETION_RETRY_BACKOFF: Duration = Duration::from_millis(10);
/// Minimum delay after a successfully rejected workflow delivery. Core may
/// immediately redeliver an activation containing unsupported future fields;
/// this bound keeps one such run from monopolizing the supervisor poll loop.
pub const WORKFLOW_DELIVERY_REJECTION_BACKOFF: Duration = Duration::from_millis(100);

/// Builds the bounded failure text sent to Core when a workflow activation
/// cannot cross the private semantic boundary.
///
/// The caller can provide only a process-static string. This keeps payloads,
/// headers, run identifiers, and other workflow-controlled data out of the
/// server-visible diagnostic while still identifying the conversion category.
pub fn workflow_rejection_message(reason: &'static str) -> String {
    format!("OCaml bridge could not represent the workflow activation: {reason}")
}

/// Application failure type reported for an activity task the bridge cannot
/// represent to OCaml (issue #801).
///
/// The type lets workflow code and operators distinguish this bridge-level
/// rejection from a failure raised by an activity implementation.
pub const UNREPRESENTABLE_ACTIVITY_TASK_FAILURE_TYPE: &str = "UnrepresentableActivityTask";

/// Builds the bounded failure text sent to Core when a leased activity task
/// cannot cross the private semantic boundary.
///
/// As with [`workflow_rejection_message`], only a process-static category is
/// accepted, so payloads, headers, and identifiers never reach the
/// server-visible failure.
pub fn activity_rejection_message(reason: &'static str) -> String {
    format!("OCaml bridge could not represent the activity task: {reason}")
}

/// Builds the Temporal failure for an activity task the bridge cannot
/// represent to OCaml.
///
/// The failure is a non-retryable application failure. Representability is a
/// deterministic property of the task document and this worker build: every
/// redelivery of the same task to this worker would be rejected again, and a
/// retryable failure would make the server redeliver it under the activity's
/// retry policy indefinitely. Failing it once lets the scheduling workflow
/// observe a typed activity failure instead.
pub fn unrepresentable_activity_failure(reason: &'static str) -> TemporalFailure {
    TemporalFailure {
        message: activity_rejection_message(reason),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: UNREPRESENTABLE_ACTIVITY_TASK_FAILURE_TYPE.to_owned(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

/// Builds the bounded failure text sent to Core when a polled workflow
/// activation cannot be admitted into the bridge ledger for delivery to OCaml.
fn workflow_admission_rejection_message(reason: &'static str) -> String {
    format!("OCaml bridge could not admit the workflow activation: {reason}")
}

/// Builds the bounded failure text sent to Core when a polled activity task
/// cannot be admitted into the bridge ledger for delivery to OCaml.
fn activity_admission_rejection_message(reason: &'static str) -> String {
    format!("OCaml bridge could not admit the activity task: {reason}")
}

/// Fails one Core workflow activation that the bridge will never hand to OCaml.
///
/// Core already transferred ownership of the activation by returning it from
/// `poll_workflow_activation`. Dropping it without a completion leaves an
/// outstanding workflow task that blocks further polling and graceful
/// finalization. Admission failures therefore complete through Core here
/// before the poll lane surfaces a diagnostic error.
async fn force_fail_undeliverable_workflow(worker: &Worker, run_id: &str, reason: &'static str) {
    let completion = WorkflowActivationCompletion::fail(
        run_id,
        workflow_admission_rejection_message(reason).into(),
        Some(WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure),
    );
    // Best-effort: a Core rejection is already a fatal worker condition. The
    // lane still reports the original admission failure to the owner Domain.
    let _ = worker.complete_workflow_activation(completion).await;
}

/// Completes one workflow activation that runtime disposal retires on OCaml's
/// behalf.
///
/// A pure cache eviction owns no workflow task, so Core accepts only an empty
/// acknowledgement for it; failing it would leave the eviction outstanding and
/// keep the workflow poll from ever reporting `ShutDown` (issue #775). Every
/// other activation is failed, exactly as an undeliverable admission is. Core
/// errors are ignored because disposal cannot retry them.
async fn complete_workflow_for_dispose(
    worker: &Worker,
    run_id: &str,
    eviction_only: bool,
    reason: &'static str,
) {
    if eviction_only {
        let _ = worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id))
            .await;
    } else {
        force_fail_undeliverable_workflow(worker, run_id, reason).await;
    }
}

/// Fails one Core activity task that the bridge will never hand to OCaml.
///
/// Used only for undeliverable *start* (or malformed) tasks that would
/// otherwise leave a Core completion debt. Pure cancel notifications that do
/// not create a new ledger obligation are not completed here.
async fn force_fail_undeliverable_activity(
    worker: &Worker,
    task_token: &[u8],
    reason: &'static str,
) {
    let completion = ActivityTaskCompletion {
        task_token: task_token.to_vec(),
        result: Some(ActivityExecutionResult::fail(
            activity_admission_rejection_message(reason).into(),
        )),
    };
    let _ = worker.complete_activity_task(completion).await;
}

/// Returns whether a failed cancellation handoff is a stale update.
///
/// Core represents cancellation as an update on the original activity token,
/// not as a second activity execution. A cancellation is deliverable while its
/// start token remains in the ledger, even when the start has already been
/// leased to OCaml. Only a token that has already completed is stale; trying to
/// complete that update would fabricate a second completion for the same Core
/// task. Start-shaped lease failures remain real delivery errors and continue
/// through the force-failure path below.
fn is_stale_activity_cancellation(task: &ActivityTask, error: CompleteError) -> bool {
    matches!(
        task.variant.as_ref(),
        Some(activity_task::Variant::Cancel(_))
    ) && matches!(error, CompleteError::UnknownActivity)
}

/// Maps the task kinds an OCaml worker registration can execute onto the
/// exact Core task surface the bridge enables.
///
/// `workflows` and `activities` report whether the OCaml worker registered at
/// least one workflow or activity implementation. Core polls the server only
/// for the kinds enabled here, so a worker that cannot execute a task kind
/// never takes such a task from a shared task queue (#805). Polling a kind
/// without an implementation would let this worker win tasks that a sibling
/// worker on the same queue could execute, and then fail them.
///
/// Local activities are enabled exactly when workflows are. Local activities
/// use the same Core activity-task handoff as remote activities, but they are
/// never polled from the server: Core dispatches them in-process from this
/// worker's own workflow commands and records the result in workflow history,
/// so they cannot steal another worker's tasks. Keeping them enabled for a
/// worker without registered activities makes a local activity scheduled by
/// one of its workflows fail promptly as an unregistered type instead of
/// waiting forever on a disabled Core manager. Core itself rejects local
/// activities without workflows. Nexus is never enabled because the bridge has
/// no Nexus task handoff.
///
/// Returns an error when neither kind is selected: such a worker could never
/// make progress, and Core would reject it with a less specific diagnostic.
pub fn bridge_task_types(
    workflows: bool,
    activities: bool,
) -> Result<WorkerTaskTypes, &'static str> {
    if !workflows && !activities {
        return Err("task_types must enable workflows or activities");
    }
    Ok(WorkerTaskTypes {
        enable_workflows: workflows,
        enable_local_activities: workflows,
        enable_remote_activities: activities,
        enable_nexus: false,
    })
}

/// Reports which guarded poll lanes a Core worker with `task_types` needs, as
/// `(workflow_lane, activity_lane)`.
///
/// The workflow lane must not run when workflows are disabled: Core's
/// `poll_workflow_activation` then returns `ShutDown` immediately *and*
/// initiates shutdown of the whole worker, which would stop an activity-only
/// worker before it executes anything. The activity lane is needed whenever
/// Core can produce either a remote or a local activity task.
pub fn poll_lanes_for(task_types: &WorkerTaskTypes) -> (bool, bool) {
    (
        task_types.enable_workflows,
        task_types.enable_remote_activities || task_types.enable_local_activities,
    )
}

/// Upper bound for each phase of releasing a worker whose validation failed.
///
/// A worker that never entered the graph has no OCaml-owned tasks, so both
/// the poll drain and Core's finalizer normally finish in milliseconds. The
/// bound exists only so a Core regression returns the typed validation error
/// to `Worker.create` instead of wedging the supervisor Domain forever.
pub const UNVALIDATED_WORKER_RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// Delay before re-polling after Core reports a non-shutdown poll error while
/// an unvalidated worker drains, preventing a tight error loop.
const UNVALIDATED_WORKER_POLL_ERROR_BACKOFF: Duration = Duration::from_millis(10);

/// Releases a Core worker whose namespace validation failed before it was
/// published into the runtime graph.
///
/// The caller transfers sole ownership of the never-published worker; this
/// function is its only release path. Core's `finalize_shutdown` does not
/// complete until both `poll_workflow_activation` and `poll_activity_task`
/// have returned `ShutDown`: the activity manager waits for its poll stream to
/// observe shutdown, and nobody else will ever poll this worker. Awaiting the
/// finalizer directly therefore hung `Worker.create` forever (issue #770).
///
/// The release runs as one task on the worker's Tokio runtime. That task owns
/// the worker until `finalize_shutdown` completes: it initiates shutdown,
/// drives both poll APIs until each reports `ShutDown`, force-fails any task
/// Core unexpectedly hands out (so no completion debt blocks finalization), and
/// then finalizes. The worker is never dropped mid-release, because only
/// `finalize_shutdown` performs Core's `finalize_unregister`; dropping it
/// instead would leave the worker registrator in the client's registry, which
/// keeps the client alive through an `Arc` cycle.
///
/// The caller waits at most [`UNVALIDATED_WORKER_RELEASE_TIMEOUT`] so it can
/// still return its typed error if Core's `ShutdownWorker` RPC is slow. After
/// that bound the task keeps ownership and completes the release in the
/// background; it ends only when finalization finishes or the runtime itself
/// is shut down. Must run inside the worker's Tokio runtime.
pub async fn release_unvalidated_worker(worker: Worker) {
    let release = tokio::spawn(async move {
        worker.initiate_shutdown();
        tokio::join!(
            drain_unvalidated_workflow_polls(&worker),
            drain_unvalidated_activity_polls(&worker)
        );
        worker.finalize_shutdown().await;
    });
    // Elapsing only stops waiting: the spawned task keeps the worker and still
    // completes `finalize_shutdown`, including Core's unregister step.
    let _ = tokio::time::timeout(UNVALIDATED_WORKER_RELEASE_TIMEOUT, release).await;
}

/// Polls workflow activations on a shut-down, unvalidated worker until Core
/// reports `ShutDown`. Any activation is force-failed because no OCaml owner
/// exists to complete it.
async fn drain_unvalidated_workflow_polls(worker: &Worker) {
    loop {
        match worker.poll_workflow_activation().await {
            Err(PollError::ShutDown) => return,
            Ok(activation) => {
                force_fail_undeliverable_workflow(
                    worker,
                    &activation.run_id,
                    "worker validation failed before delivery",
                )
                .await;
            }
            Err(_) => tokio::time::sleep(UNVALIDATED_WORKER_POLL_ERROR_BACKOFF).await,
        }
    }
}

/// Polls activity tasks on a shut-down, unvalidated worker until Core reports
/// `ShutDown`. Start-shaped tasks are force-failed; cancellation updates carry
/// no separate completion debt and are dropped.
async fn drain_unvalidated_activity_polls(worker: &Worker) {
    loop {
        match worker.poll_activity_task().await {
            Err(PollError::ShutDown) => return,
            Ok(task) => {
                if !matches!(task.variant, Some(activity_task::Variant::Cancel(_))) {
                    force_fail_undeliverable_activity(
                        worker,
                        &task.task_token,
                        "worker validation failed before delivery",
                    )
                    .await;
                }
            }
            Err(_) => tokio::time::sleep(UNVALIDATED_WORKER_POLL_ERROR_BACKOFF).await,
        }
    }
}

/// Upper bound for the drain-and-join phase of a live worker shutdown.
///
/// Core's poll APIs report `ShutDown` only after every task they produced has
/// been completed and, when the server supports graceful poll shutdown, after
/// the in-flight long poll returns. Shutdown retires every completion debt
/// itself (see [`PollLanes::drain_and_join_for_shutdown`]), so the remaining
/// wait is normally Core's `ShutdownWorker` RPC and at most one server long
/// poll. The bound exceeds the server's default 60-second long-poll interval
/// plus Core's client-side margin. It is reached only when Core or the server
/// misbehaves, and exists so `Worker.shutdown` cannot wedge the supervisor
/// Domain forever (issue #769).
pub const WORKER_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(90);

/// Upper bound the supervisor waits for Core's terminal finalizer after both
/// poll lanes have joined.
///
/// With both polls already at `ShutDown`, `finalize_shutdown` only awaits the
/// `ShutdownWorker` RPC and Core's internal manager teardown. After the bound
/// the finalizer keeps running in a Tokio task that owns the worker; see
/// [`PollLanes::finalize_bounded`].
pub const WORKER_FINALIZE_TIMEOUT: Duration = Duration::from_secs(30);

/// Completion debts that worker shutdown had to retire on OCaml's behalf.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShutdownRetirement {
    /// Tasks leased to OCaml that were never completed. Each was completed by
    /// the bridge (failed, or acknowledged if it was a pure cache eviction),
    /// so a non-zero count means language-side work was lost.
    pub abandoned_leases: usize,
    /// Tasks Core produced that never crossed into OCaml: queued handoffs and
    /// results of polls already in flight when shutdown began. OCaml never
    /// owned them, so retiring them is ordinary shutdown behavior.
    pub undelivered: usize,
}

/// Reason [`PollLanes::drain_and_join_for_shutdown`] could not establish that
/// both poll lanes stopped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShutdownDrainError {
    /// A poll lane task panicked or was cancelled. Both lanes were still
    /// joined before this was reported.
    Lane(PollLaneError),
    /// Core did not report `ShutDown` on every lane within the bound. The
    /// unjoined lane handles remain owned by [`PollLanes`].
    TimedOut,
}

/// Outcome of [`PollLanes::finalize_bounded`].
pub enum BoundedFinalize {
    /// Core's finalizer completed, including its worker unregistration.
    Finalized,
    /// The bridge refused to consume the worker; ownership is returned so the
    /// caller can keep it in the runtime graph for disposal.
    Refused(Box<PollLanes>, WorkerBridgeError),
    /// Core's finalizer panicked. The worker was released while unwinding.
    Panicked,
    /// The bound elapsed. The finalizer task still owns the worker and
    /// completes in the background, or is cancelled when the Tokio runtime
    /// itself shuts down.
    Detached,
}

/// Awaits one poll lane handle, or never completes when the lane is absent or
/// already joined. Used as a `select!` branch so an absent lane cannot win.
async fn join_lane(lane: &mut Option<JoinHandle<()>>) -> Result<(), tokio::task::JoinError> {
    match lane {
        Some(handle) => handle.await,
        None => std::future::pending().await,
    }
}

/// Builds the bounded failure text sent to Core when worker shutdown retires a
/// task that OCaml will never complete. Only process-static reasons are
/// accepted, for the same reason as [`workflow_rejection_message`].
fn shutdown_retirement_message(reason: &'static str) -> String {
    format!("OCaml bridge retired the task during worker shutdown: {reason}")
}

/// Completes one workflow activation that shutdown retires on OCaml's behalf.
///
/// A pure cache eviction owns no workflow task, so the only completion Core
/// accepts for it is an empty acknowledgement; failing it would leave the
/// eviction outstanding and keep the workflow poll from reporting `ShutDown`.
/// Every other activation belongs to a real workflow task and is failed, never
/// completed empty: an empty completion would record a workflow task that
/// silently dropped the activation's jobs, which a later replay could not
/// reproduce. Core errors are ignored because shutdown cannot retry them; the
/// bounded join reports a completion Core never accepted.
async fn complete_workflow_for_shutdown(
    worker: &Worker,
    run_id: &str,
    eviction_only: bool,
    reason: &'static str,
) {
    let completion = if eviction_only {
        WorkflowActivationCompletion::empty(run_id)
    } else {
        WorkflowActivationCompletion::fail(
            run_id,
            shutdown_retirement_message(reason).into(),
            Some(WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure),
        )
    };
    let _ = worker.complete_workflow_activation(completion).await;
}

/// Fails one activity start that shutdown retires on OCaml's behalf. The
/// failure is retryable under the activity's retry policy, so the server can
/// dispatch the attempt to another worker.
async fn fail_activity_for_shutdown(worker: &Worker, task_token: &[u8], reason: &'static str) {
    let completion = ActivityTaskCompletion {
        task_token: task_token.to_vec(),
        result: Some(ActivityExecutionResult::fail(
            shutdown_retirement_message(reason).into(),
        )),
    };
    let _ = worker.complete_activity_task(completion).await;
}

/// Fatal reason a guarded poll lane stopped before ordinary Core shutdown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PollLaneError {
    /// Core reported a non-shutdown polling error.
    Core(String),
    /// Core emitted a task identity that violated bridge ownership rules.
    Admission(AdmitError),
    /// Core emitted a second outstanding task with the same identity.
    DuplicateIdentity,
    /// Core emitted an activity without a known start or cancel variant.
    InvalidActivityVariant,
}

/// Failure while completing or consuming a draining Core worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerBridgeError {
    /// The completion does not refer to a task leased to OCaml.
    Completion(CompleteError),
    /// Core rejected a workflow completion.
    CoreWorkflow(String),
    /// Core rejected an activity completion.
    CoreActivity(String),
    /// A future Core/client completion API explicitly reported a transient
    /// transport failure before consuming the activity lease.
    ///
    /// The pinned Core revision currently hides those network outcomes and
    /// therefore does not construct this variant. Keeping the category
    /// explicit prevents the OCaml side from guessing retryability from a
    /// generic worker or connection status.
    RetryableActivityCompletion,
    /// Finalization was attempted while tasks remain outstanding.
    OutstandingTasks(usize),
    /// A poll result was dropped without a safe, identity-specific Core
    /// completion, so consuming the worker would make shutdown unsound.
    LostPollLease,
    /// A poll task still retained the worker after both joins completed.
    WorkerStillShared,
}

/// Converts one internal worker failure into the bounded diagnostic category
/// that the ABI may expose to OCaml.
///
/// The `CoreWorkflow` and `CoreActivity` variants retain their detailed Core
/// error text so the Rust state machine can classify the failure internally.
/// That text may contain server-provided data, however, so it must never be
/// formatted into a C result or an OCaml exception.  Keeping this mapping as a
/// closed match makes adding a new failure variant a compiler-audited change:
/// the new variant cannot accidentally inherit a debug representation at the
/// public boundary.
pub fn public_worker_error_message(error: &WorkerBridgeError) -> &'static str {
    match error {
        WorkerBridgeError::Completion(CompleteError::UnknownWorkflow) => {
            "Temporal worker completion referred to an unknown workflow"
        }
        WorkerBridgeError::Completion(CompleteError::UnknownActivity) => {
            "Temporal worker completion referred to an unknown activity"
        }
        WorkerBridgeError::Completion(CompleteError::NotLeased) => {
            "Temporal worker completion referred to an unleased task"
        }
        WorkerBridgeError::Completion(CompleteError::AlreadyLeased) => {
            "Temporal worker completion referred to an already leased task"
        }
        WorkerBridgeError::CoreWorkflow(_) => "Temporal workflow completion was rejected by Core",
        WorkerBridgeError::CoreActivity(_) => "Temporal activity completion was rejected by Core",
        WorkerBridgeError::RetryableActivityCompletion => {
            "Temporal activity completion transport is temporarily unavailable"
        }
        WorkerBridgeError::OutstandingTasks(_) => "Temporal worker has outstanding tasks",
        WorkerBridgeError::LostPollLease => "Temporal worker has an uncompleted poll lease",
        WorkerBridgeError::WorkerStillShared => "Temporal worker remains shared after shutdown",
    }
}

/// One task or terminal lane error waiting for the OCaml supervisor.
pub type ReadyTask<T> = Result<T, PollLaneError>;

/// Result of waiting for one poll lane's next owner-domain action.
///
/// `Ready` means that at least one message is queued and the caller should use
/// the corresponding non-blocking drain operation. `Shutdown` is returned only
/// after the lane is closed and its queued messages have been drained. `Error`
/// preserves a fatal lane error when no earlier queued message remains. The
/// wait is deliberately synchronous because its C caller releases the OCaml
/// runtime lock and invokes it only from the dedicated supervisor owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadinessWait {
    /// At least one task or queued lane error is ready to be drained.
    Ready,
    /// The lane closed normally and has no queued messages left.
    Shutdown,
    /// The lane stopped with a fatal bridge error after its queued messages.
    Error(PollLaneError),
    /// No event arrived before the bounded supervisor-mailbox wait elapsed.
    ///
    /// A timeout is intentional: the supervisor must regain its mailbox loop
    /// periodically so a queued shutdown command can run even while no Core
    /// task is available to wake this lane.
    TimedOut,
}

/// Maximum time one owner-domain readiness call may hold the supervisor loop.
///
/// This bound is a liveness guard rather than a workflow timer. It leaves the
/// supervisor mailbox responsive to lifecycle messages while still avoiding a
/// polling spin when Core is quiet.
pub const READINESS_WAIT_TIMEOUT: Duration = Duration::from_millis(100);

/// Shared wake state for one Rust-owned poll queue.
///
/// The state mutex is held across the unbounded-channel send and pending-count
/// update. The owner-domain drain holds the same mutex across `try_recv` and
/// decrement. This ordering makes the count and queue linearizable: a waiter
/// can never observe a queued message with a zero count, and a drain can never
/// decrement a count before the producer increments it. The condition variable
/// is only a wake mechanism; every waiter rechecks the state predicate while
/// holding the mutex, so notifications cannot be lost before a wait begins.
///
/// Each signal also notifies a worker-wide [`AnyLaneWake`] after every state
/// change, so one owner-domain wait can observe either lane (#806).
struct Readiness {
    state: Mutex<ReadinessState>,
    wake: Condvar,
    any: Arc<AnyLaneWake>,
}

/// Worker-wide wake shared by the workflow and activity [`Readiness`] signals.
///
/// A lane-specific wait leaves the sole supervisor owner blind to the other
/// lane for up to [`READINESS_WAIT_TIMEOUT`], which made every step of a
/// sequential activity workflow pay one dead bounded wait (#806).
/// [`wait_any_lane`] waits on this condition instead and rechecks both lane
/// predicates.
///
/// Lock order is `AnyLaneWake::generation` before a lane's `state`, and the
/// workflow lane's `state` before the activity lane's when both are held. A
/// producer therefore releases its lane mutex before calling
/// [`Self::notify`]. That is still lossless: the waiter holds `generation`
/// from its predicate check until `wait_timeout` atomically releases it, so a
/// producer whose lane update the check missed cannot notify until the waiter
/// is already blocked. The counter only makes each notification a state
/// change; waiters never trust a wake without rechecking the lane predicates.
#[derive(Debug, Default)]
struct AnyLaneWake {
    generation: Mutex<u64>,
    wake: Condvar,
}

impl AnyLaneWake {
    /// Wakes every combined waiter after a lane's predicate changed. Callers
    /// must not hold any lane `state` mutex (see the lock order above).
    fn notify(&self) {
        let mut generation = self
            .generation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *generation = generation.wrapping_add(1);
        self.wake.notify_all();
    }
}

/// Snapshot of one lane's wait predicate taken under its state mutex.
struct LaneObservation {
    pending: bool,
    error: Option<PollLaneError>,
    closed: bool,
}

/// Mutable predicate protected by [`Readiness::state`].
#[derive(Debug, Default)]
struct ReadinessState {
    /// Number of channel messages that have been committed by a producer but
    /// not yet consumed by the owner Domain.
    pending: usize,
    /// A fatal poll-lane error. It remains visible after its error message is
    /// drained so later waits cannot block after the lane has failed.
    error: Option<PollLaneError>,
    /// No new Core poll result is expected after this flag is set. In-flight
    /// polls may still enqueue messages while the flag is true; pending work
    /// always takes precedence over this terminal state.
    closed: bool,
}

impl Readiness {
    /// Creates an open signal with no queued work and a private combined
    /// wake. Only isolated tests use a signal that no sibling lane shares.
    #[cfg(test)]
    fn new() -> Self {
        Self::sharing(&Arc::new(AnyLaneWake::default()))
    }

    /// Creates an open signal that also notifies `any` on every state change.
    /// Both lanes of one [`PollLanes`] share the same `any` wake.
    fn sharing(any: &Arc<AnyLaneWake>) -> Self {
        Self {
            state: Mutex::new(ReadinessState::default()),
            wake: Condvar::new(),
            any: Arc::clone(any),
        }
    }

    /// Atomically publishes one queue message and its pending-count update.
    ///
    /// The producer sends while holding the state mutex. `UnboundedSender::send`
    /// is non-blocking, so this short critical section cannot stall Tokio or
    /// the OCaml owner; it only establishes the queue/count ordering required
    /// by the wait predicate.
    fn enqueue<T>(
        &self,
        sender: &mpsc::UnboundedSender<ReadyTask<T>>,
        message: ReadyTask<T>,
    ) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let sent = if sender.send(message).is_err() {
            // The owner has gone away. No future caller can drain this lane,
            // but marking it closed also prevents a defensive waiter from
            // sleeping forever if it is still holding the runtime graph.
            state.closed = true;
            false
        } else {
            // Core's outstanding-task permits keep this count far below
            // `usize::MAX` in normal operation. Saturation is still safer than
            // panicking in a background lane if a future producer violates
            // that assumption.
            state.pending = state.pending.saturating_add(1);
            true
        };
        self.wake.notify_all();
        // Release the lane mutex before the combined wake (lock order).
        drop(state);
        self.any.notify();
        sent
    }

    /// Consumes one queue message while atomically retiring its pending count.
    ///
    /// Only the owner Domain calls this method, but the producer uses the same
    /// mutex while publishing, which prevents a send/receive reordering race.
    fn take<T>(
        &self,
        receiver: &mut mpsc::UnboundedReceiver<ReadyTask<T>>,
    ) -> Option<ReadyTask<T>> {
        self.take_inner(receiver, false)
    }

    /// Checks the queue and terminal error under one lock. A separate empty
    /// receive followed by an error lookup would let a producer enqueue a
    /// task between those checks, incorrectly reporting the error first.
    fn take_or_failure<T>(
        &self,
        receiver: &mut mpsc::UnboundedReceiver<ReadyTask<T>>,
    ) -> Option<ReadyTask<T>> {
        self.take_inner(receiver, true)
    }

    fn take_inner<T>(
        &self,
        receiver: &mut mpsc::UnboundedReceiver<ReadyTask<T>>,
        report_failure: bool,
    ) -> Option<ReadyTask<T>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match receiver.try_recv() {
            Ok(message) => {
                // A successful receive is paired with exactly one successful
                // enqueue while this mutex was held. Keep release builds
                // defensive in case a future channel implementation changes
                // that invariant.
                debug_assert!(state.pending > 0);
                state.pending = state.pending.saturating_sub(1);
                Some(message)
            }
            Err(TryRecvError::Empty) => {
                if report_failure {
                    state.error.clone().map(Err)
                } else {
                    None
                }
            }
            Err(TryRecvError::Disconnected) => {
                state.closed = true;
                self.wake.notify_all();
                let result = if report_failure {
                    state.error.clone().map(Err)
                } else {
                    None
                };
                drop(state);
                self.any.notify();
                result
            }
        }
    }

    /// Awaits one queue message while preserving the same pending-count
    /// invariant as [`Self::take`].  Replay disposal uses this owner-side
    /// variant while a poll lane is being joined: the lane may publish an
    /// eviction only after the preceding empty completion has been accepted,
    /// so a synchronous drain would race the producer and leave the join
    /// waiting forever.
    async fn take_async<T>(
        &self,
        receiver: &mut mpsc::UnboundedReceiver<ReadyTask<T>>,
    ) -> Option<ReadyTask<T>> {
        let message = receiver.recv().await?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        debug_assert!(state.pending > 0);
        state.pending = state.pending.saturating_sub(1);
        Some(message)
    }

    /// Records a fatal poll-lane error and wakes the owner immediately.
    fn fail(&self, error: PollLaneError) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.error = Some(error);
        self.wake.notify_all();
        drop(state);
        self.any.notify();
    }

    /// Marks the lane as normally closed while retaining queued work for drain.
    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.closed = true;
        self.wake.notify_all();
        drop(state);
        self.any.notify();
    }

    /// Blocks until work, a fatal error, or terminal closure is observable.
    fn wait(&self) -> ReadinessWait {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let deadline = Instant::now() + READINESS_WAIT_TIMEOUT;
        loop {
            // Queued messages always win over terminal flags: the supervisor
            // must drain all messages before it reports shutdown or failure.
            if state.pending > 0 {
                return ReadinessWait::Ready;
            }
            if let Some(error) = state.error.clone() {
                return ReadinessWait::Error(error);
            }
            if state.closed {
                return ReadinessWait::Shutdown;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return ReadinessWait::TimedOut;
            }
            let (next_state, timeout) = self
                .wake
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
            if timeout.timed_out() {
                return ReadinessWait::TimedOut;
            }
        }
    }
}

/// Reads both lane predicates as one snapshot by holding both lane mutexes at
/// once (workflow first, then activity). Reading them one after another would
/// let a producer publish to the first lane between the two reads, so a
/// terminal flag on the second lane could win over work that is already
/// pending. Producers release their lane mutex before taking the combined
/// wake mutex, and no producer holds two lane mutexes, so this cannot
/// deadlock with them.
fn observe_both(workflow: &Readiness, activity: &Readiness) -> [LaneObservation; 2] {
    let workflow_state = workflow
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let activity_state = activity
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    [&*workflow_state, &*activity_state].map(|state| LaneObservation {
        pending: state.pending > 0,
        error: state.error.clone(),
        closed: state.closed,
    })
}

/// Blocks until either lane has work, a fatal error, or terminal closure, or
/// until [`READINESS_WAIT_TIMEOUT`] elapses.
///
/// Queued work in *either* lane wins over every terminal state, so the owner
/// drains everything before it reports a failure or shutdown. Without work, a
/// fatal error in either lane is reported before closure, and closure of
/// either lane is reported as shutdown, matching what a lane-specific wait on
/// that lane would return. Both signals must share one [`AnyLaneWake`]. The
/// wait never consumes a task, so the matching non-blocking drain still owns
/// delivery.
fn wait_any_lane(workflow: &Readiness, activity: &Readiness) -> ReadinessWait {
    debug_assert!(Arc::ptr_eq(&workflow.any, &activity.any));
    let any = &workflow.any;
    let mut generation = any
        .generation
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let deadline = Instant::now() + READINESS_WAIT_TIMEOUT;
    loop {
        let lanes = observe_both(workflow, activity);
        if lanes.iter().any(|lane| lane.pending) {
            return ReadinessWait::Ready;
        }
        if let Some(error) = lanes.iter().find_map(|lane| lane.error.clone()) {
            return ReadinessWait::Error(error);
        }
        if lanes.iter().any(|lane| lane.closed) {
            return ReadinessWait::Shutdown;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return ReadinessWait::TimedOut;
        }
        // `wait_timeout` releases `generation` atomically, so a producer that
        // committed after the observation above cannot notify before this
        // thread is blocked. Every wake loops back to recheck both lanes.
        let (next, _timeout) = any
            .wake
            .wait_timeout(generation, remaining)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        generation = next;
    }
}

/// Rust-owned pair of guarded Core poll lanes for one worker.
///
/// Exactly one Tokio task invokes each Core poll API that the worker's
/// configured task types enable. The channels are only consumed by the owner
/// Domain through non-blocking `try_take_*` calls, so a long Core poll can
/// never block lifecycle or completion messages in the OCaml supervisor
/// mailbox.
pub struct PollLanes {
    worker: Arc<Worker>,
    ledger: Arc<Mutex<TaskLedger>>,
    workflow_ready: mpsc::UnboundedReceiver<ReadyTask<WorkflowActivation>>,
    activity_ready: mpsc::UnboundedReceiver<ReadyTask<ActivityTask>>,
    workflow_signal: Arc<Readiness>,
    activity_signal: Arc<Readiness>,
    workflow_lane: Option<JoinHandle<()>>,
    activity_lane: Option<JoinHandle<()>>,
    /// Senders retained for lanes that a live worker deliberately did not
    /// start. Holding them keeps the receivers connected, so a disabled lane
    /// reports "no work" rather than the disconnect that marks a lane closed.
    /// They never send and are dropped with the lanes.
    idle_senders: IdleSenders,
    shutdown_started: bool,
}

/// Producer halves kept alive for disabled live-worker lanes; see
/// [`PollLanes::start`].
type IdleSenders = (
    Option<mpsc::UnboundedSender<ReadyTask<WorkflowActivation>>>,
    Option<mpsc::UnboundedSender<ReadyTask<ActivityTask>>>,
);

impl PollLanes {
    /// Starts the sole workflow poll and sole activity poll loops that the
    /// worker's configured Core task types require.
    ///
    /// The lane plan is derived from the configuration Core was actually
    /// given (see [`poll_lanes_for`]), so the bridge can neither poll a task
    /// kind that Core disabled nor skip one that it enabled. A lane that is
    /// not started stays *open and idle*: non-blocking takes report no work
    /// and readiness waits time out exactly as for a quiet lane, until
    /// [`Self::initiate_shutdown`] closes both lanes. Closing it eagerly would
    /// make a readiness wait report shutdown while the worker is still
    /// running, which the OCaml owner treats as a lifecycle error.
    pub fn start(worker: Worker, handle: &tokio::runtime::Handle) -> Self {
        let (workflow, activity) = poll_lanes_for(&worker.get_config().task_types);
        Self::start_lanes(worker, handle, workflow, activity, false)
    }

    /// Starts only the workflow poll loop for a replay worker.
    ///
    /// Core replay workers deliberately disable remote activities. Keeping the
    /// activity lane absent, rather than starting a poll that immediately
    /// returns `ShutDown`, makes that invariant explicit and prevents a replay
    /// owner from accidentally treating an unavailable activity lane as a
    /// worker failure. The shared ledger and readiness machinery remain the
    /// same as the live-worker path.
    pub fn start_workflow_only(worker: Worker, handle: &tokio::runtime::Handle) -> Self {
        Self::start_lanes(worker, handle, true, false, true)
    }

    /// Builds the guarded lanes shared by live and workflow-only replay
    /// workers. The booleans are internal to construction so the public
    /// methods cannot create a partially configured lane graph.
    ///
    /// `close_absent` selects how a lane without a producer behaves. Replay
    /// marks it closed before publication so a defensive wait cannot sleep on
    /// a condition replay can never satisfy. Live workers keep it open and
    /// idle by retaining its sender; see [`Self::start`].
    fn start_lanes(
        worker: Worker,
        handle: &tokio::runtime::Handle,
        start_workflow_lane: bool,
        start_activity_lane: bool,
        close_absent: bool,
    ) -> Self {
        let worker = Arc::new(worker);
        let ledger = Arc::new(Mutex::new(TaskLedger::new()));
        // Core's configured outstanding-task permits provide the actual queue
        // bound. An unbounded Tokio handoff is required here because awaiting a
        // full bounded send would prevent the serialized supervisor from
        // joining poll lanes during shutdown.
        let (workflow_sender, workflow_ready) = mpsc::unbounded_channel();
        let (activity_sender, activity_ready) = mpsc::unbounded_channel();
        // One combined wake lets the owner wait on both lanes at once (#806).
        let any_lane_wake = Arc::new(AnyLaneWake::default());
        let workflow_signal = Arc::new(Readiness::sharing(&any_lane_wake));
        let activity_signal = Arc::new(Readiness::sharing(&any_lane_wake));
        let mut idle_senders: IdleSenders = (None, None);

        let workflow_lane = if start_workflow_lane {
            Some(handle.spawn(run_workflow_lane(
                Arc::clone(&worker),
                Arc::clone(&ledger),
                workflow_sender,
                Arc::clone(&workflow_signal),
            )))
        } else {
            if close_absent {
                workflow_signal.close();
            } else {
                idle_senders.0 = Some(workflow_sender);
            }
            None
        };
        let activity_lane = if start_activity_lane {
            Some(handle.spawn(run_activity_lane(
                Arc::clone(&worker),
                Arc::clone(&ledger),
                activity_sender,
                Arc::clone(&activity_signal),
            )))
        } else {
            if close_absent {
                activity_signal.close();
            } else {
                idle_senders.1 = Some(activity_sender);
            }
            None
        };
        Self {
            worker,
            ledger,
            workflow_ready,
            activity_ready,
            workflow_signal,
            activity_signal,
            workflow_lane,
            activity_lane,
            idle_senders,
            shutdown_started: false,
        }
    }

    /// Reports whether a Core workflow poll loop was started for this worker.
    pub fn polls_workflow_tasks(&self) -> bool {
        self.workflow_lane.is_some()
    }

    /// Reports whether a Core activity poll loop was started for this worker.
    ///
    /// A worker with workflows but no registered activities still starts this
    /// lane so Core can dispatch its local activities, but Core never polls
    /// the server for remote activity tasks on its behalf; see
    /// [`Self::task_types`].
    pub fn polls_activity_tasks(&self) -> bool {
        self.activity_lane.is_some()
    }

    /// Returns the Core task types this worker was constructed with.
    pub fn task_types(&self) -> WorkerTaskTypes {
        self.worker.get_config().task_types
    }

    /// Takes one ready activation without waiting for Core or a channel lock.
    ///
    /// When the ready queue holds an activation but the ledger cannot lease it,
    /// the dequeued activation is force-failed to Core before the lane error is
    /// returned. The owner must supply the worker's Tokio handle so this
    /// synchronous handoff can complete the undeliverable activation without
    /// leaving an outstanding Core task.
    pub fn try_take_workflow(
        &mut self,
        handle: &tokio::runtime::Handle,
    ) -> Option<ReadyTask<WorkflowActivation>> {
        let ready = self
            .workflow_signal
            .take_or_failure(&mut self.workflow_ready)?;
        match ready {
            Ok(activation) => {
                let lease = self
                    .ledger
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .lease_workflow_activation(&activation.run_id, activation.is_only_eviction());
                match lease {
                    Ok(()) => Some(Ok(activation)),
                    Err(error) => {
                        let run_id = activation.run_id.clone();
                        // Drop the activation after capturing its identity so
                        // we cannot accidentally deliver it after Core has been
                        // told the task failed admission handoff.
                        drop(activation);
                        // AlreadyLeased means the first handoff still owns this
                        // run_id. Force-failing would complete that live lease.
                        if matches!(error, CompleteError::AlreadyLeased) {
                            return Some(Err(PollLaneError::DuplicateIdentity));
                        }
                        handle.block_on(force_fail_undeliverable_workflow(
                            self.worker.as_ref(),
                            &run_id,
                            "workflow activation lease handoff failed",
                        ));
                        self.ledger
                            .lock()
                            .unwrap_or_else(|err| err.into_inner())
                            .abandon_workflow_admission(&run_id);
                        Some(Err(PollLaneError::Admission(AdmitError::InvalidIdentity)))
                    }
                }
            }
            Err(error) => Some(Err(error)),
        }
    }

    /// Takes one ready remote activity without blocking the supervisor Domain.
    ///
    /// Start-task lease failures force-fail the dequeued task through Core so
    /// the opaque token cannot remain outstanding after the language side
    /// never observes it. Cancellation updates share the start token's one
    /// completion debt, but are handed to OCaml while that token remains in the
    /// ledger; the cancellation handoff never acquires a second lease. Only a
    /// cancellation that races with a completed start is dropped as stale.
    /// See [`Self::try_take_workflow`] for the handle requirement.
    pub fn try_take_activity(
        &mut self,
        handle: &tokio::runtime::Handle,
    ) -> Option<ReadyTask<ActivityTask>> {
        loop {
            let ready = self
                .activity_signal
                .take_or_failure(&mut self.activity_ready)?;
            match ready {
                Err(error) => return Some(Err(error)),
                Ok(task) => {
                    let kind = if matches!(
                        task.variant.as_ref(),
                        Some(activity_task::Variant::Cancel(_))
                    ) {
                        ActivityAdmission::Cancel
                    } else {
                        ActivityAdmission::Start
                    };
                    let lease = self
                        .ledger
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .handoff_activity(&task.task_token, kind);
                    match lease {
                        Ok(()) => return Some(Ok(task)),
                        Err(error) if is_stale_activity_cancellation(&task, error) => {
                            // The start completed before this queued update was
                            // drained. There is no remaining Core debt for a
                            // cancellation-only notification to retire.
                            drop(task);
                        }
                        Err(error) => {
                            let task_token = task.task_token.clone();
                            drop(task);
                            // AlreadyLeased means the first handoff still owns
                            // this token. Force-failing would complete that
                            // live lease.
                            if matches!(error, CompleteError::AlreadyLeased) {
                                return Some(Err(PollLaneError::DuplicateIdentity));
                            }
                            handle.block_on(force_fail_undeliverable_activity(
                                self.worker.as_ref(),
                                &task_token,
                                "activity task lease handoff failed",
                            ));
                            self.ledger
                                .lock()
                                .unwrap_or_else(|err| err.into_inner())
                                .abandon_activity_admission(&task_token);
                            return Some(Err(PollLaneError::Admission(
                                AdmitError::InvalidIdentity,
                            )));
                        }
                    }
                }
            }
        }
    }

    /// Closes admission before waking both Core poll futures for shutdown.
    ///
    /// The caller must enter the worker's Tokio runtime before this synchronous
    /// method because Core spawns its deregistration task internally.
    pub fn initiate_shutdown(&mut self) {
        if self.shutdown_started {
            return;
        }
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .begin_draining();
        // Wake any supervisor wait immediately. In-flight Core polls may still
        // enqueue tasks; `Readiness::wait` prioritizes those pending messages
        // before returning the terminal shutdown state.
        self.workflow_signal.close();
        self.activity_signal.close();
        self.worker.initiate_shutdown();
        self.shutdown_started = true;
    }

    /// Records a shutdown that Core initiated itself, without sending a second
    /// shutdown signal into the worker.
    ///
    /// Replay workers cancel their own Core shutdown token after the history
    /// stream ends. The bridge still needs its ledger's `Draining` phase before
    /// the shared finalizer can consume the worker, but calling
    /// [`Self::initiate_shutdown`] before joining would risk cancelling a
    /// history that had not reached Core yet. This method is therefore reserved
    /// for an already-observed terminal lane and changes only bridge ledger
    /// state. It deliberately leaves `shutdown_started` false so an explicit
    /// later disposal can still send Core's cancellation signal if a join or
    /// finalization error makes that necessary.
    pub fn mark_natural_shutdown(&mut self) {
        if self.shutdown_started {
            return;
        }
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .begin_draining();
    }

    /// Waits for the next workflow-lane message without holding the OCaml lock.
    pub fn wait_workflow(&self) -> ReadinessWait {
        self.workflow_signal.wait()
    }

    /// Waits for the next activity-lane message without holding the OCaml lock.
    pub fn wait_activity(&self) -> ReadinessWait {
        self.activity_signal.wait()
    }

    /// Waits for the next message on either lane without holding the OCaml
    /// lock. The live worker loop uses this for its single idle native wait,
    /// so work arriving on the lane it did not expect wakes the owner at once
    /// instead of after the bounded timeout (#806). A disabled lane of a live
    /// worker stays open and idle, so it never satisfies this wait.
    pub fn wait_any(&self) -> ReadinessWait {
        wait_any_lane(&self.workflow_signal, &self.activity_signal)
    }

    /// Sleeps for one bounded completion retry interval on the supervisor
    /// owner Domain.
    ///
    /// This is intentionally a timer rather than a readiness wait: unrelated
    /// activity work may already be queued, and that readiness must not permit
    /// a retained completion to spin. The C binding releases the OCaml runtime
    /// lock while the caller is inside this method, so workflow fibers and
    /// other Domains remain schedulable.
    pub fn wait_activity_completion_retry_backoff(&self) {
        std::thread::sleep(ACTIVITY_COMPLETION_RETRY_BACKOFF);
    }

    /// Prevents immediate Core redelivery of an unrepresentable activation
    /// from spinning through the supervisor loop. The C binding releases the
    /// OCaml runtime lock while the caller is inside this bounded timer.
    pub fn wait_workflow_delivery_rejection_backoff(&self) {
        std::thread::sleep(WORKFLOW_DELIVERY_REJECTION_BACKOFF);
    }

    /// Waits until both guarded poll futures have observed Core shutdown.
    ///
    /// Both join handles are always awaited, even when the first lane reports a
    /// failure.  A failed workflow lane must not leave the activity lane
    /// producing tasks while dispose or a retrying shutdown path assumes that
    /// every producer has stopped.
    pub async fn join_poll_lanes(&mut self) -> Result<(), PollLaneError> {
        let mut first_error = None;
        if let Some(workflow_lane) = self.workflow_lane.take()
            && let Err(error) = workflow_lane.await
        {
            first_error = Some(PollLaneError::Core(format!(
                "workflow poll lane failed: {error}"
            )));
        }
        if let Some(activity_lane) = self.activity_lane.take()
            && let Err(error) = activity_lane.await
        {
            // Preserve the first failure for the caller while still waiting
            // for the second lane to stop publishing messages.
            if first_error.is_none() {
                first_error = Some(PollLaneError::Core(format!(
                    "activity poll lane failed: {error}"
                )));
            }
        }
        // Both producer handles are consumed at this point. No future poll
        // can race the ledger, so disposal tombstones no longer provide a
        // safety property and must be released even if the caller skips a
        // redundant post-join force-completion pass.
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear_dispose_retired();
        first_error.map_or(Ok(()), Err)
    }

    /// Joins a workflow-only replay lane while acknowledging every activation
    /// that Core publishes during shutdown.
    ///
    /// Replay Core may emit a cache-eviction activation in response to an
    /// empty completion. That activation can arrive after the shutdown token
    /// is cancelled, so awaiting the poll task without draining its queue
    /// would deadlock: Core keeps the poll future alive until the eviction is
    /// acknowledged. The join loop therefore races the lane handle against
    /// the ready queue and sends only shutdown-safe empty completions. A
    /// replay activation never receives the live-worker failure completion.
    pub async fn join_replay_poll_lane(&mut self) -> Result<(), PollLaneError> {
        let Some(workflow_lane) = self.workflow_lane.take() else {
            return Ok(());
        };
        tokio::pin!(workflow_lane);
        let mut first_error = None;
        loop {
            tokio::select! {
                result = &mut workflow_lane => {
                    if let Err(error) = result {
                        first_error = Some(PollLaneError::Core(format!(
                            "workflow poll lane failed: {error}"
                        )));
                    }
                    break;
                }
                ready = self.workflow_signal.take_async(&mut self.workflow_ready) => {
                    let Some(ready) = ready else {
                        // The sender is dropped only after the poll lane has
                        // exited, so the join branch will become ready next.
                        continue;
                    };
                    if let Ok(activation) = ready {
                        let run_id = activation.run_id.clone();
                        self.ledger
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .force_remove_workflow(&run_id);
                        let completion = WorkflowActivationCompletion::empty(run_id);
                        let _ = self.worker.complete_workflow_activation(completion).await;
                    }
                }
            }
        }
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear_dispose_retired();
        first_error.map_or(Ok(()), Err)
    }

    /// Retires every outstanding task and joins both poll lanes, waiting at
    /// most `bound`.
    ///
    /// The caller must already have called [`Self::initiate_shutdown`]. From
    /// then on nothing but this method can complete a task: the supervisor
    /// Domain that would deliver OCaml completions is blocked in this call, and
    /// OCaml stopped its run loop before requesting shutdown. Core, however,
    /// reports `ShutDown` from a poll API only after every task it produced has
    /// been completed. Joining without completing them therefore hung forever
    /// whenever a handoff was queued or a lease was abandoned (issue #769).
    ///
    /// The method completes every debt exactly once and keeps doing so while
    /// the lanes run, because polls already in flight, and Core's own
    /// follow-up cache evictions, publish new tasks until each lane sees
    /// `ShutDown`:
    ///
    /// * a task leased to OCaml is taken from the ledger and completed (see
    ///   [`TaskLedger::take_leased_for_shutdown`]) and counted as abandoned;
    /// * a queued handoff is removed from the ledger, completed from its own
    ///   queue message, and counted as undelivered;
    /// * an activity cancellation is dropped: it updates its start's single
    ///   debt, which one of the two cases above retires;
    /// * a queued lane diagnostic is dropped; the lane already completed any
    ///   task it could safely complete before publishing it.
    ///
    /// Every identity leaves the ledger before its Core completion is awaited,
    /// so a same-run eviction published in response is admitted as a new debt
    /// rather than a duplicate. A pure eviction is acknowledged empty and
    /// every other task is failed; see [`complete_workflow_for_shutdown`].
    ///
    /// On success both lane handles are consumed and the ledger is empty, so
    /// the caller may finalize. On [`ShutdownDrainError::TimedOut`] the
    /// unjoined handles stay owned by `self` and the ledger is marked as
    /// having lost a lease: cancelling the drain may have removed an identity
    /// whose Core completion never ran, so the counts no longer prove that
    /// finalization is safe and disposal must use its last-resort path.
    pub async fn drain_and_join_for_shutdown(
        &mut self,
        bound: Duration,
    ) -> Result<ShutdownRetirement, ShutdownDrainError> {
        let mut retirement = ShutdownRetirement::default();
        match tokio::time::timeout(bound, self.drain_and_join(&mut retirement)).await {
            Ok(Ok(())) => Ok(retirement),
            Ok(Err(error)) => Err(ShutdownDrainError::Lane(error)),
            Err(_elapsed) => {
                self.ledger
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .mark_lost_poll_lease();
                Err(ShutdownDrainError::TimedOut)
            }
        }
    }

    /// Unbounded body of [`Self::drain_and_join_for_shutdown`]; the caller
    /// supplies the deadline. Both lanes are always joined, even after the
    /// first reports a failure, so no producer outlives a successful return.
    async fn drain_and_join(
        &mut self,
        retirement: &mut ShutdownRetirement,
    ) -> Result<(), PollLaneError> {
        let mut first_error = None;
        // A started lane drops its sender when it exits; the receiver then
        // yields `None` forever and must stop being selected. A disabled lane's
        // sender is retained in `idle_senders`, so its receive simply pends.
        let mut workflow_queue_open = true;
        let mut activity_queue_open = true;
        while self.workflow_lane.is_some() || self.activity_lane.is_some() {
            self.retire_visible_for_shutdown(retirement).await;
            tokio::select! {
                joined = join_lane(&mut self.workflow_lane) => {
                    self.workflow_lane = None;
                    if let Err(error) = joined {
                        first_error.get_or_insert(PollLaneError::Core(format!(
                            "workflow poll lane failed: {error}"
                        )));
                    }
                }
                joined = join_lane(&mut self.activity_lane) => {
                    self.activity_lane = None;
                    if let Err(error) = joined {
                        first_error.get_or_insert(PollLaneError::Core(format!(
                            "activity poll lane failed: {error}"
                        )));
                    }
                }
                ready = self.workflow_signal.take_async(&mut self.workflow_ready),
                    if workflow_queue_open =>
                {
                    match ready {
                        Some(message) => self.retire_queued_workflow(message, retirement).await,
                        None => workflow_queue_open = false,
                    }
                }
                ready = self.activity_signal.take_async(&mut self.activity_ready),
                    if activity_queue_open =>
                {
                    match ready {
                        Some(message) => self.retire_queued_activity(message, retirement).await,
                        None => activity_queue_open = false,
                    }
                }
            }
        }
        // No producer remains, so this pass retires the last tasks published
        // between the final wait and each lane's exit.
        self.retire_visible_for_shutdown(retirement).await;
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear_dispose_retired();
        first_error.map_or(Ok(()), Err)
    }

    /// Retires every abandoned lease and every message already queued.
    async fn retire_visible_for_shutdown(&mut self, retirement: &mut ShutdownRetirement) {
        let (workflows, activities) = self
            .ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take_leased_for_shutdown();
        retirement.abandoned_leases += workflows.len() + activities.len();
        for (run_id, eviction_only) in workflows {
            complete_workflow_for_shutdown(
                self.worker.as_ref(),
                &run_id,
                eviction_only,
                "workflow activation leased to OCaml was never completed",
            )
            .await;
        }
        for task_token in activities {
            fail_activity_for_shutdown(
                self.worker.as_ref(),
                &task_token,
                "activity task leased to OCaml was never completed",
            )
            .await;
        }
        while let Some(message) = self.workflow_signal.take(&mut self.workflow_ready) {
            self.retire_queued_workflow(message, retirement).await;
        }
        while let Some(message) = self.activity_signal.take(&mut self.activity_ready) {
            self.retire_queued_activity(message, retirement).await;
        }
    }

    /// Completes one workflow activation that never reached OCaml.
    async fn retire_queued_workflow(
        &self,
        message: ReadyTask<WorkflowActivation>,
        retirement: &mut ShutdownRetirement,
    ) {
        let Ok(activation) = message else {
            return;
        };
        let eviction_only = activation.is_only_eviction();
        let run_id = activation.run_id;
        // Retire before awaiting Core; see `drain_and_join_for_shutdown`.
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .force_remove_workflow(&run_id);
        retirement.undelivered += 1;
        complete_workflow_for_shutdown(
            self.worker.as_ref(),
            &run_id,
            eviction_only,
            "workflow activation was not delivered before shutdown",
        )
        .await;
    }

    /// Fails one activity start that never reached OCaml. A cancellation has
    /// no debt of its own and is dropped.
    async fn retire_queued_activity(
        &self,
        message: ReadyTask<ActivityTask>,
        retirement: &mut ShutdownRetirement,
    ) {
        let Ok(task) = message else {
            return;
        };
        if matches!(task.variant, Some(activity_task::Variant::Cancel(_))) {
            return;
        }
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .force_remove_activity(&task.task_token);
        retirement.undelivered += 1;
        fail_activity_for_shutdown(
            self.worker.as_ref(),
            &task.task_token,
            "activity task was not delivered before shutdown",
        )
        .await;
    }

    /// Runs [`Self::finalize`] in a Tokio task and waits for it at most
    /// `bound`. Must run inside the worker's Tokio runtime.
    ///
    /// The task owns the worker until Core's `finalize_shutdown` returns, so
    /// the worker is never dropped mid-finalization: only that function
    /// performs Core's `finalize_unregister`, and skipping it would leave the
    /// worker registered with the client (see [`release_unvalidated_worker`]).
    /// Elapsing only stops the caller's wait.
    pub async fn finalize_bounded(self, bound: Duration) -> BoundedFinalize {
        let finalize = tokio::spawn(self.finalize());
        match tokio::time::timeout(bound, finalize).await {
            Ok(Ok(Ok(()))) => BoundedFinalize::Finalized,
            Ok(Ok(Err((lanes, error)))) => BoundedFinalize::Refused(Box::new(lanes), error),
            Ok(Err(_join_error)) => BoundedFinalize::Panicked,
            Err(_elapsed) => BoundedFinalize::Detached,
        }
    }

    /// Reports whether every task admitted before shutdown has completed.
    pub fn can_finalize(&self) -> bool {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .can_finalize()
    }

    /// Reports whether any workflow or activity completion debt remains.
    ///
    /// Replay reaches Core's terminal shutdown before the bridge marks its
    /// ledger as `Draining`, so replay finalization needs this count-only view
    /// to validate ownership without prematurely initiating shutdown. Live
    /// disposal should continue to use [`Self::can_finalize`], which also
    /// requires the explicit draining phase.
    pub fn has_outstanding_tasks(&self) -> bool {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .outstanding()
            != 0
    }

    /// Best-effort Core completion for every task still owned by this worker.
    ///
    /// Used by runtime dispose/free when OCaml cannot finish leased work. The
    /// method completes every ledger entry and every queued handoff exactly
    /// once so [`Self::finalize`] is not blocked by outstanding completion
    /// debt. Dispose calls it once before joining the poll lanes and once
    /// after both joins, because a poll already in flight can publish a task
    /// between those two points. Errors from Core are ignored: dispose must
    /// still release the process graph.
    ///
    /// A pure cache eviction is acknowledged with an empty completion and
    /// every other activation is failed (see
    /// [`complete_workflow_for_dispose`]). For a ledger entry the eviction
    /// bit comes from the ledger itself, which records it in the same
    /// critical section that admits the run (see
    /// [`TaskLedger::admit_polled_workflow_activation`]). The snapshot is
    /// therefore correct even for an entry the workflow lane admitted but has
    /// not yet enqueued, whose message a queue drain could not observe and
    /// whose later queue entry is skipped as already completed (issue #775).
    /// A queued activation admitted after the snapshot carries its own bit.
    pub async fn force_complete_outstanding_for_dispose(&mut self) {
        // Complete ledger debt first so Core can finish poll loops that are
        // blocked waiting for outstanding-task permits during shutdown.
        let (workflows, activity_tokens) = self
            .ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take_all_outstanding();
        let mut completed_workflow_ids: HashSet<String> =
            workflows.iter().map(|(run_id, _)| run_id.clone()).collect();
        for (run_id, eviction_only) in &workflows {
            complete_workflow_for_dispose(
                self.worker.as_ref(),
                run_id,
                *eviction_only,
                "runtime dispose retired outstanding workflow lease",
            )
            .await;
        }
        let mut completed_activity_tokens: HashSet<Vec<u8>> = HashSet::new();
        for task_token in activity_tokens {
            completed_activity_tokens.insert(task_token.clone());
            force_fail_undeliverable_activity(
                self.worker.as_ref(),
                &task_token,
                "runtime dispose retired outstanding activity lease",
            )
            .await;
        }

        // Drain the ready queue, including activations a poll lane published
        // while the ledger debt above was being completed.
        let mut queued_workflows: Vec<(String, bool)> = Vec::new();
        while let Some(ready) = self.workflow_signal.take(&mut self.workflow_ready) {
            if let Ok(activation) = ready {
                let eviction_only = activation.is_only_eviction();
                queued_workflows.push((activation.run_id, eviction_only));
            }
        }
        for (run_id, eviction_only) in queued_workflows {
            // Already completed above if the run was still in the ledger; a
            // second completion for the same run_id is unsafe. Retire before
            // awaiting Core so a same-identity poll that races this queue
            // drain is rejected rather than admitted as a new obligation for
            // the post-join pass.
            if Self::claim_dispose_workflow_identity(
                &self.ledger,
                &mut completed_workflow_ids,
                &run_id,
            ) {
                complete_workflow_for_dispose(
                    self.worker.as_ref(),
                    &run_id,
                    eviction_only,
                    "runtime dispose drained undelivered workflow activation",
                )
                .await;
            }
        }
        while let Some(ready) = self.activity_signal.take(&mut self.activity_ready) {
            if let Ok(task) = ready {
                // Skip pure cancel updates and any token already completed
                // from the ledger so dispose cannot double-complete Core.
                let is_cancel = matches!(task.variant, Some(activity_task::Variant::Cancel(_)));
                // Cancellation updates do not own a second Core completion.
                // For starts, retire before awaiting Core for the same reason
                // as workflows: a duplicate poll cannot become a fresh
                // admission while this completion is in flight.
                if is_cancel
                    || !Self::claim_dispose_activity_identity(
                        &self.ledger,
                        &mut completed_activity_tokens,
                        &task.task_token,
                    )
                {
                    continue;
                }
                force_fail_undeliverable_activity(
                    self.worker.as_ref(),
                    &task.task_token,
                    "runtime dispose drained undelivered activity task",
                )
                .await;
            }
        }
    }

    /// Abandons replay-owned workflow activations during explicit disposal.
    ///
    /// Replay has no server-side workflow task to fail safely during explicit
    /// disposal: a replay activation may be an eviction notification rather
    /// than an active workflow task, and Core panics if a non-empty failure is
    /// sent for that state. An empty completion is the safe Core
    /// acknowledgement for every queued or leased replay activation, and is
    /// explicitly tolerated when shutdown races the local workflow stream.
    /// The bridge ledger is retired regardless of the Core acknowledgement
    /// result so the native owner can be finalized. The caller invokes this
    /// before joining the poll lane (to release an activation that would
    /// otherwise keep Core's poll future alive) and again after the join (to
    /// retire any activation published at the join boundary).
    pub async fn abandon_replay_for_dispose(&mut self) {
        let (mut workflow_ids, _) = self
            .ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take_all_outstanding_for_replay();
        let mut seen_workflows: HashSet<String> = workflow_ids.iter().cloned().collect();

        // `take_all_outstanding_for_replay` includes both activations already leased to
        // OCaml and activations still waiting in the Rust handoff queue.  The
        // queue itself is drained below so its pending-count signal reaches
        // the same closed state as the joined producer.
        while let Some(ready) = self.workflow_signal.take(&mut self.workflow_ready) {
            if let Ok(activation) = ready {
                let run_id = activation.run_id;
                // An activation admitted after the ledger snapshot still
                // owns a Core completion debt. Remove it now and include it
                // exactly once in this empty-completion pass.
                self.ledger
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .force_remove_workflow(&run_id);
                if seen_workflows.insert(run_id.clone()) {
                    workflow_ids.push(run_id);
                }
            }
        }

        for run_id in workflow_ids {
            let completion = WorkflowActivationCompletion::empty(run_id);
            let _ = self.worker.complete_workflow_activation(completion).await;
        }
    }

    /// Claims one workflow identity for this disposal pass and marks it
    /// retired in the shared ledger before any asynchronous Core completion.
    ///
    /// The local set prevents duplicate queue entries from causing duplicate
    /// Core completions in one pass; the ledger tombstone closes the wider
    /// window in which a poll lane could admit the same identity concurrently.
    fn claim_dispose_workflow_identity(
        ledger: &Mutex<TaskLedger>,
        completed: &mut HashSet<String>,
        run_id: &str,
    ) -> bool {
        if !completed.insert(run_id.to_owned()) {
            return false;
        }
        ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retire_workflow_for_dispose(run_id);
        true
    }

    /// Claims one activity start token for this disposal pass and marks it
    /// retired before its best-effort Core completion is awaited. Cancellation
    /// updates are filtered by the caller because they do not own completion
    /// debt of their own.
    fn claim_dispose_activity_identity(
        ledger: &Mutex<TaskLedger>,
        completed: &mut HashSet<Vec<u8>>,
        task_token: &[u8],
    ) -> bool {
        if !completed.insert(task_token.to_vec()) {
            return false;
        }
        ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retire_activity_for_dispose(task_token);
        true
    }

    /// Sends a leased workflow completion to Core, then retires its debt.
    ///
    /// Retire the ledger lease *before* the Core completion await, mirroring
    /// [`Self::reject_workflow_delivery_with_reason`]. A successful terminal
    /// completion causes Core to schedule a follow-up cache-eviction
    /// activation for the same run_id, which the background poll lane can
    /// observe while this call is still in flight. If the run were still
    /// recorded in the ledger at that moment, the poll lane would classify
    /// the legitimate eviction as `Admission::Duplicate` and raise a terminal
    /// `PollLaneError::DuplicateIdentity`, surfacing as a fatal
    /// `STATUS_WORKER` on an otherwise healthy worker — and the eviction's
    /// own completion debt would be dropped on the floor rather than
    /// acknowledged. Retiring first guarantees the eviction is instead
    /// admitted as a new obligation, which is exactly what the OCaml adapter
    /// expects (see `submit_eviction_acknowledgement` in
    /// `native_worker_execution.ml`, which acknowledges an eviction for a run
    /// already removed from its registry).
    ///
    /// The ledger entry is restored when Core rejects the completion: a
    /// rejected request never lands with Core, so no follow-up eviction can
    /// have been scheduled for this run_id during the await, and the
    /// language side is expected to correct the validation failure and retry
    /// rather than losing track of ownership after a partial bridge
    /// conversion.
    pub async fn complete_workflow(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), WorkerBridgeError> {
        let run_id = completion.run_id.clone();
        let was_eviction = {
            let mut ledger = self
                .ledger
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let was_eviction = ledger.is_eviction_lease(&run_id);
            ledger
                .complete_workflow(&run_id)
                .map_err(WorkerBridgeError::Completion)?;
            was_eviction
        };
        // Record the ledger state at the exact handoff to Core. The
        // retirement above must already have removed this run, so a correct
        // ordering always probes `false`. A regression that retired after
        // this await would probe `true`, deterministically failing the
        // ordering test.
        #[cfg(test)]
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .probe_complete_completion(&run_id);
        match self.worker.complete_workflow_activation(completion).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let mut ledger = self
                    .ledger
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if ledger.restore_rejected_workflow_completion(&run_id) && was_eviction {
                    ledger.restore_eviction_lease(&run_id);
                }
                Err(WorkerBridgeError::CoreWorkflow(error.to_string()))
            }
        }
    }

    /// Sends a leased remote-activity completion to Core, then retires it.
    pub async fn complete_activity(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), WorkerBridgeError> {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .ensure_activity_leased(&completion.task_token)
            .map_err(WorkerBridgeError::Completion)?;
        let task_token = completion.task_token.clone();
        self.worker
            .complete_activity_task(completion)
            .await
            .map_err(|error| WorkerBridgeError::CoreActivity(error.to_string()))?;
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .complete_activity(&task_token)
            .map_err(WorkerBridgeError::Completion)
    }

    /// Records progress for a leased activity without retiring its ledger
    /// entry. Core performs any batching and network work internally; the
    /// bridge only checks ownership before handing over the owned protobuf.
    /// The pinned Core API does not return heartbeat response flags here:
    /// cancellation, pause, and reset are delivered asynchronously as a later
    /// `ActivityTask::Cancel` and therefore cannot be reported synchronously.
    pub fn record_activity_heartbeat(
        &self,
        heartbeat: ActivityHeartbeat,
    ) -> Result<(), WorkerBridgeError> {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .ensure_activity_leased(&heartbeat.task_token)
            .map_err(WorkerBridgeError::Completion)?;
        self.worker.record_activity_heartbeat(heartbeat);
        Ok(())
    }

    /// Fails an activation that could not cross the semantic JSON boundary,
    /// or acknowledges it empty when it is a pure cache eviction (see
    /// [`Self::reject_workflow_delivery_with_reason`]).
    ///
    /// The activation was leased by [`Self::try_take_workflow`] but was never
    /// exposed to OCaml, so no language-side caller can return its completion.
    /// This method makes exactly one Core completion attempt and then retires
    /// the private debt even if Core rejects that attempt. A rejection is a
    /// fatal worker error, but it must not also fabricate an eternally leased
    /// task that prevents deterministic shutdown.
    pub async fn reject_workflow_delivery(&self, run_id: &str) -> Result<(), WorkerBridgeError> {
        self.reject_workflow_delivery_with_reason(
            run_id,
            "semantic workflow activation conversion failed",
        )
        .await
    }

    /// Rejects a leased activation with a privacy-safe static conversion
    /// category and then retires the lease exactly once.
    ///
    /// A pure cache eviction owns no workflow task, so it is never failed:
    /// Core accepts only an empty acknowledgement for it (issue #814), and a
    /// failure would leave the eviction outstanding in release Core and panic
    /// debug Core's workflow stream. The eviction is therefore acknowledged
    /// empty, which is exactly what the language side would have sent had the
    /// activation crossed the boundary; the static `reason` is not reported
    /// to Core in that case. Every other activation belongs to a real
    /// workflow task and is failed with [`workflow_rejection_message`].
    pub async fn reject_workflow_delivery_with_reason(
        &self,
        run_id: &str,
        reason: &'static str,
    ) -> Result<(), WorkerBridgeError> {
        // Retire the ledger lease *before* the Core completion await.
        //
        // Failing an activation causes Core to schedule a follow-up cache
        // eviction for the same run_id. That eviction can be published by the
        // background poll lane while this rejection is still in flight. If the
        // run were still recorded in the ledger at that moment, the poll lane
        // would classify the legitimate eviction as `Admission::Duplicate` and
        // raise a terminal `PollLaneError::DuplicateIdentity`, surfacing as a
        // fatal `STATUS_WORKER` from an otherwise healthy poll. Retiring first
        // guarantees the eviction is admitted as a new obligation instead.
        //
        // `retire_rejected_workflow` both validates that the run is leased and
        // removes it, so it replaces the earlier lease check. Rejection retires
        // the lease unconditionally — even when Core rejects the generated
        // failure — so performing the retirement up front does not change the
        // ownership outcome: the run is retired exactly once regardless of the
        // Core result. This mirrors the retire-before-await ordering already
        // used by the dispose queue drain in
        // `force_complete_outstanding_for_dispose`.
        let eviction_only = {
            let mut ledger = self
                .ledger
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            // Read the eviction bit first: retirement clears it.
            let eviction_only = ledger.is_eviction_lease(run_id);
            ledger
                .retire_rejected_workflow(run_id)
                .map_err(WorkerBridgeError::Completion)?;
            eviction_only
        };
        let completion = if eviction_only {
            WorkflowActivationCompletion::empty(run_id)
        } else {
            WorkflowActivationCompletion::fail(
                run_id,
                workflow_rejection_message(reason).into(),
                Some(WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure),
            )
        };
        // Record the ledger state at the exact handoff to Core. The retirement
        // above must already have removed this run, so a correct ordering
        // always probes `false`. A regression that retired after this await
        // would probe `true`, deterministically failing the ordering test.
        #[cfg(test)]
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .probe_reject_completion(run_id);
        self.worker
            .complete_workflow_activation(completion)
            .await
            .map_err(|error| WorkerBridgeError::CoreWorkflow(error.to_string()))
    }

    /// Test-only view of the reject-completion ordering probes recorded by the
    /// shared ledger. Each entry is `false` when the corresponding rejection
    /// retired its lease before handing the failure to Core.
    #[cfg(test)]
    pub(crate) fn reject_completion_probes(&self) -> Vec<bool> {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .reject_completion_probes()
            .to_vec()
    }

    /// Test-only view of the terminal-completion ordering probes recorded by
    /// the shared ledger. Each entry is `false` when the corresponding
    /// completion retired its lease before handing the completion to Core.
    #[cfg(test)]
    pub(crate) fn complete_completion_probes(&self) -> Vec<bool> {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .complete_completion_probes()
            .to_vec()
    }

    /// Fails a remote activity task that semantic conversion could not expose.
    ///
    /// As with workflow rejection, the generated failure is attempted once
    /// and the inaccessible token is retired on every outcome. Retaining it
    /// after conversion failure would make graceful shutdown impossible
    /// because OCaml never received the token needed to complete it.
    ///
    /// The generated failure is [`unrepresentable_activity_failure`] for the
    /// static `reason`. Core's acceptance of that completion also releases the
    /// task's activity slot, so the poll lane can take the next task.
    pub async fn reject_activity_delivery(
        &self,
        task_token: &[u8],
        reason: &'static str,
    ) -> Result<(), WorkerBridgeError> {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .ensure_activity_leased(task_token)
            .map_err(WorkerBridgeError::Completion)?;
        let completion = ActivityTaskCompletion {
            task_token: task_token.to_vec(),
            result: Some(ActivityExecutionResult::fail(
                unrepresentable_activity_failure(reason),
            )),
        };
        let core_result = self
            .worker
            .complete_activity_task(completion)
            .await
            .map_err(|error| WorkerBridgeError::CoreActivity(error.to_string()));
        let ledger_result = self
            .ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retire_rejected_activity(task_token)
            .map_err(WorkerBridgeError::Completion);
        core_result.and(ledger_result)
    }

    /// Consumes a fully drained worker and runs Core's terminal finalizer.
    ///
    /// On failure the original [`PollLanes`] is returned so the caller can still
    /// force-complete outstanding tasks or retry. A failed finalize must not
    /// drop the only handle that can still talk to Core.
    pub async fn finalize(self) -> Result<(), (Self, WorkerBridgeError)> {
        let (outstanding, lost_poll_lease) = {
            let ledger_state = self
                .ledger
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            (ledger_state.outstanding(), ledger_state.lost_poll_lease)
        };
        if outstanding != 0 {
            return Err((self, WorkerBridgeError::OutstandingTasks(outstanding)));
        }
        if lost_poll_lease {
            return Err((self, WorkerBridgeError::LostPollLease));
        }
        let Self {
            worker,
            ledger,
            workflow_ready,
            activity_ready,
            workflow_signal,
            activity_signal,
            workflow_lane,
            activity_lane,
            idle_senders,
            shutdown_started,
        } = self;
        match Arc::try_unwrap(worker) {
            Ok(worker) => {
                worker.finalize_shutdown().await;
                Ok(())
            }
            Err(worker) => Err((
                Self {
                    worker,
                    ledger,
                    workflow_ready,
                    activity_ready,
                    workflow_signal,
                    activity_signal,
                    workflow_lane,
                    activity_lane,
                    idle_senders,
                    shutdown_started,
                },
                WorkerBridgeError::WorkerStillShared,
            )),
        }
    }

    /// Retains a second Core worker owner for replay disposal tests.
    ///
    /// This test-only hook keeps the worker field private to the bridge while
    /// allowing the replay module to model a competing in-flight owner.
    #[cfg(test)]
    pub(crate) fn retain_worker_for_test(&self) -> Arc<Worker> {
        Arc::clone(&self.worker)
    }

    /// Replaces the workflow poll task with an explicitly aborted pending task
    /// for deterministic replay join-failure coverage.
    ///
    /// The original Core poll handle is first aborted and awaited, so the test
    /// never leaves a detached producer behind. A fresh Tokio task that cannot
    /// complete on its own is then aborted and installed as the owned handle;
    /// replay disposal observes exactly one join failure without depending on
    /// whether Core's real poll had already reached natural shutdown.
    /// Production callers always use the Core shutdown path instead.
    #[cfg(test)]
    pub(crate) async fn abort_workflow_lane_for_test(&mut self) {
        let workflow_lane = self
            .workflow_lane
            .take()
            .expect("poll lanes must own a workflow task");
        workflow_lane.abort();
        let _ = workflow_lane.await;

        let replacement = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        replacement.abort();
        self.workflow_lane = Some(replacement);
    }
}

/// Polls workflow activations serially and records ownership before enqueueing.
///
/// Every activation returned by Core must either be delivered to OCaml or
/// force-failed back to Core. Admission rejections therefore complete the
/// undeliverable activation before the lane error is published.
async fn run_workflow_lane(
    worker: Arc<Worker>,
    ledger: Arc<Mutex<TaskLedger>>,
    sender: mpsc::UnboundedSender<ReadyTask<WorkflowActivation>>,
    signal: Arc<Readiness>,
) {
    loop {
        let activation = match worker.poll_workflow_activation().await {
            Ok(activation) => activation,
            Err(PollError::ShutDown) => {
                signal.close();
                return;
            }
            Err(error) => {
                let error = PollLaneError::Core(error.to_string());
                let _ = signal.enqueue(&sender, Err(error.clone()));
                signal.fail(error);
                return;
            }
        };
        let admission = ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .admit_polled_workflow_activation(&activation.run_id, activation.is_only_eviction());
        match admission {
            Ok(Admission::New) => {
                let run_id = activation.run_id.clone();
                if !signal.enqueue(&sender, Ok(activation)) {
                    // The ready channel is closed. Retire the just-admitted
                    // identity and complete Core so disposal cannot leave an
                    // uncompleted poll debt behind.
                    ledger
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .abandon_workflow_admission(&run_id);
                    force_fail_undeliverable_workflow(
                        worker.as_ref(),
                        &run_id,
                        "workflow ready channel closed after admission",
                    )
                    .await;
                    return;
                }
            }
            Ok(Admission::Duplicate) | Ok(Admission::ExistingCancellation) => {
                // Core workflow completions are keyed only by run_id. Force-
                // failing a duplicate would complete the already-outstanding
                // activation that is still queued or leased to OCaml. Drop the
                // duplicate delivery, persist a fatal drain blocker, and
                // surface a lane error instead.
                ledger
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .mark_lost_poll_lease();
                drop(activation);
                if !signal.enqueue(&sender, Err(PollLaneError::DuplicateIdentity)) {
                    return;
                }
            }
            Err(AdmitError::Retired) if activation.is_only_eviction() => {
                // Disposal already completed this run's previous activation,
                // and Core answers a failed workflow task with a cache
                // eviction for the same run. That eviction is a fresh Core
                // debt, not a duplicate of the retired one: Core keeps at
                // most one activation outstanding per run, so it cannot
                // publish it until the disposal completion was accepted.
                // Dropping it would keep the workflow poll from ever
                // reporting `ShutDown` and wedge the dispose lane join
                // (issue #775). Acknowledge it with the only completion Core
                // accepts for an eviction; it never enters the ledger, so no
                // later pass can complete it again.
                let completion = WorkflowActivationCompletion::empty(activation.run_id);
                let _ = worker.complete_workflow_activation(completion).await;
            }
            Err(error @ AdmitError::Retired) => {
                // Disposal already force-failed this identity, and this is not
                // Core's follow-up eviction. A second poll with the same run
                // ID is therefore diagnostic only: dropping it is safer than
                // sending a duplicate Core completion.
                ledger
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .mark_lost_poll_lease();
                drop(activation);
                if !signal.enqueue(&sender, Err(PollLaneError::Admission(error))) {
                    return;
                }
            }
            Err(error) => {
                let run_id = activation.run_id.clone();
                let reason = match error {
                    AdmitError::InvalidIdentity => "invalid workflow run identity",
                    AdmitError::Draining => "worker is draining and cannot admit new work",
                    AdmitError::UnknownActivityCancellation => {
                        "unexpected activity cancellation during workflow admission"
                    }
                    AdmitError::Retired => unreachable!("retired workflow handled above"),
                };
                // InvalidIdentity / Draining still leave a Core poll debt that
                // only a completion can retire. Force-fail that undeliverable
                // activation before publishing the admission error.
                force_fail_undeliverable_workflow(worker.as_ref(), &run_id, reason).await;
                if !signal.enqueue(&sender, Err(PollLaneError::Admission(error))) {
                    return;
                }
            }
        }
    }
}

/// Reports whether an activity admission outcome is a cancellation update the
/// poll lane drops silently.
///
/// Core represents cancellation as an update on the original Start token, not
/// as a second completion debt. A Cancel whose Start is unknown (it completed
/// between Core's poll returning and this admission), already retired by
/// disposal, already cancelled, malformed, or polled while draining has no
/// Start lease left to update. Completing it would fabricate a second
/// completion for the token, and publishing a lane error would make one
/// benign race terminate `Worker.run` for the whole task queue (issue #801).
/// Only a Cancel that updates a live Start (`ExistingCancellation`) is
/// delivered.
#[doc(hidden)]
pub fn is_ignorable_activity_cancellation(
    kind: ActivityAdmission,
    admission: &Result<Admission, AdmitError>,
) -> bool {
    kind == ActivityAdmission::Cancel && matches!(admission, Ok(Admission::Duplicate) | Err(_))
}

/// Polls remote activities serially and associates cancellation with its start.
///
/// Start tasks that cannot be delivered are force-failed to Core so their
/// completion debt cannot stall shutdown. Cancel notifications that do not
/// update a live Start create no Core obligation and are dropped without a
/// completion or lane error (see [`is_ignorable_activity_cancellation`]).
async fn run_activity_lane(
    worker: Arc<Worker>,
    ledger: Arc<Mutex<TaskLedger>>,
    sender: mpsc::UnboundedSender<ReadyTask<ActivityTask>>,
    signal: Arc<Readiness>,
) {
    loop {
        let task = match worker.poll_activity_task().await {
            Ok(task) => task,
            Err(PollError::ShutDown) => {
                signal.close();
                return;
            }
            Err(error) => {
                let error = PollLaneError::Core(error.to_string());
                let _ = signal.enqueue(&sender, Err(error.clone()));
                signal.fail(error);
                return;
            }
        };
        let kind = match task.variant {
            Some(activity_task::Variant::Start(_)) => ActivityAdmission::Start,
            Some(activity_task::Variant::Cancel(_)) => ActivityAdmission::Cancel,
            None => {
                // A missing variant still consumed a Core poll slot. Fail it
                // so the opaque token cannot remain outstanding forever.
                force_fail_undeliverable_activity(
                    worker.as_ref(),
                    &task.task_token,
                    "activity task has no start or cancel variant",
                )
                .await;
                let error = PollLaneError::InvalidActivityVariant;
                if !signal.enqueue(&sender, Err(error)) {
                    return;
                }
                continue;
            }
        };
        let admission = ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .admit_polled_activity(&task.task_token, kind);
        if is_ignorable_activity_cancellation(kind, &admission) {
            // See `is_ignorable_activity_cancellation`: an unknown, retired,
            // or repeated cancellation owns no Core completion debt, so it
            // is dropped without a completion and without a lane error.
            drop(task);
            continue;
        }
        match admission {
            Ok(Admission::New | Admission::ExistingCancellation) => {
                let task_token = task.task_token.clone();
                let admitted_start = kind == ActivityAdmission::Start;
                if !signal.enqueue(&sender, Ok(task)) {
                    // Ready channel closed after admission. Only Start-shaped
                    // debts need a Core completion; cancel notifications do not.
                    if admitted_start {
                        ledger
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .abandon_activity_admission(&task_token);
                        force_fail_undeliverable_activity(
                            worker.as_ref(),
                            &task_token,
                            "activity ready channel closed after admission",
                        )
                        .await;
                    }
                    return;
                }
            }
            Ok(Admission::Duplicate) => {
                // Completions are keyed only by task token. Force-failing a
                // duplicate Start would complete the already-outstanding lease
                // still queued or held by OCaml. Drop the duplicate delivery
                // and surface a lane error instead, matching the workflow
                // duplicate path, and persist the fatal drain blocker. A
                // duplicate Cancel never reaches this arm: it was dropped
                // above as an ignorable update.
                ledger
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .mark_lost_poll_lease();
                drop(task);
                if !signal.enqueue(&sender, Err(PollLaneError::DuplicateIdentity)) {
                    return;
                }
            }
            Err(error) => {
                // Only Start admissions reach this arm; every Cancel admission
                // error was dropped above. A Start owns a Core completion
                // debt, so an invalid or draining Start is force-failed.
                if matches!(error, AdmitError::InvalidIdentity | AdmitError::Draining) {
                    let reason = match error {
                        AdmitError::InvalidIdentity => "invalid activity task token",
                        AdmitError::Draining => "worker is draining and cannot admit new work",
                        _ => unreachable!(),
                    };
                    force_fail_undeliverable_activity(worker.as_ref(), &task.task_token, reason)
                        .await;
                } else {
                    ledger
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .mark_lost_poll_lease();
                    drop(task);
                }
                if !signal.enqueue(&sender, Err(PollLaneError::Admission(error))) {
                    return;
                }
            }
        }
    }
}

/// Describes whether a newly polled Core task changed outstanding ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// The task creates one new completion obligation to Core.
    New,
    /// The task identity is already outstanding and must not be delivered twice.
    Duplicate,
    /// An activity cancellation updates the already outstanding start task.
    ExistingCancellation,
}

/// Distinguishes the two activity messages emitted by Core's activity poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivityAdmission {
    /// Core supplied the initial remote activity task.
    Start,
    /// Core requested cancellation of a previously started remote activity.
    Cancel,
}

/// A task could not be admitted into the worker's ownership ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmitError {
    /// Shutdown has begun, so no new task may cross into the language runtime.
    Draining,
    /// The task identity is empty or exceeds the bridge transport ceiling.
    InvalidIdentity,
    /// Core supplied cancellation for an activity the bridge does not own.
    UnknownActivityCancellation,
    /// Disposal already force-completed this identity, so a late duplicate
    /// poll must be dropped rather than completed a second time.
    Retired,
}

/// Converts one poll-lane failure into a bounded diagnostic category for the
/// public ABI.  In particular, the `Core` string is deliberately ignored
/// because it can contain gRPC status text or other remote data.
pub fn public_poll_lane_error_message(error: &PollLaneError) -> &'static str {
    match error {
        PollLaneError::Core(_) => "Temporal worker poll lane failed",
        PollLaneError::Admission(AdmitError::Draining) => {
            "Temporal worker poll lane rejected work while draining"
        }
        PollLaneError::Admission(AdmitError::InvalidIdentity) => {
            "Temporal worker poll lane received an invalid task identity"
        }
        PollLaneError::Admission(AdmitError::UnknownActivityCancellation) => {
            "Temporal worker poll lane received an unknown activity cancellation"
        }
        PollLaneError::Admission(AdmitError::Retired) => {
            "Temporal worker poll lane received an identity already retired during disposal"
        }
        PollLaneError::DuplicateIdentity => {
            "Temporal worker poll lane received a duplicate task identity"
        }
        PollLaneError::InvalidActivityVariant => {
            "Temporal worker poll lane received an invalid activity variant"
        }
    }
}

/// A language completion did not match one outstanding Core task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompleteError {
    /// No workflow activation with this run identifier is outstanding.
    UnknownWorkflow,
    /// No activity with this opaque task token is outstanding.
    UnknownActivity,
    /// The task is still Rust-owned and has not been handed to OCaml.
    NotLeased,
    /// The task was already leased to OCaml and cannot be leased again.
    AlreadyLeased,
}

/// Admission phase controlling poll delivery and final shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    /// Both poll lanes may admit tasks.
    Open,
    /// Poll delivery is closed while existing tasks may still complete.
    Draining,
}

/// Single source of truth for Core tasks owed a language-side completion.
///
/// Callers protect one instance with a short-held mutex. No method performs
/// I/O or awaits, so the mutex is never retained across a Core future. A task
/// is inserted immediately after polling and before it is made visible to
/// OCaml; it is removed only after Core accepts the matching completion.
#[derive(Debug)]
pub struct TaskLedger {
    phase: Phase,
    workflows: HashMap<String, bool>,
    activities: HashMap<Vec<u8>, ActivityState>,
    /// Whether a Core poll result was dropped because the bridge could not
    /// safely correlate or complete its lease.
    ///
    /// Duplicate identities and stale cancellation-shaped tasks cannot be
    /// force-completed without risking completion of a different outstanding
    /// lease with the same public key. Once such a task has been observed,
    /// ordinary ledger counts are no longer a complete shutdown-safety proof,
    /// so finalization remains blocked for the lifetime of the worker.
    lost_poll_lease: bool,
    /// Workflow identities force-completed during disposal. The tombstones
    /// remain until both poll lanes join so a late duplicate result cannot be
    /// admitted as a fresh completion obligation.
    retired_workflows: HashSet<String>,
    /// Activity start tokens force-completed during disposal. Like workflow
    /// tombstones, these are bounded by the identities seen in one disposal
    /// window and are cleared after all producers stop.
    retired_activities: HashSet<Vec<u8>>,
    /// Outstanding workflow identities whose activation contained only a cache
    /// eviction, whether still queued or already leased to OCaml. Shutdown
    /// must acknowledge an abandoned eviction with an empty completion: Core
    /// has no workflow task to fail for it, and a failure completion would
    /// leave the eviction outstanding. The bit is written in the same critical
    /// section that admits the workflow entry (see
    /// [`Self::admit_polled_workflow_activation`]), so a disposal snapshot
    /// taken after admission but before the poll lane enqueues the message
    /// still classifies the entry correctly. Membership is removed whenever
    /// the workflow entry leaves the ledger, so it never outgrows the
    /// outstanding workflow map.
    eviction_activations: HashSet<String>,
    /// Test-only trace of whether each rejected run was still recorded in the
    /// ledger at the instant its rejection failure was handed to Core. The
    /// reject path must retire the lease *before* that completion, because the
    /// completion is what makes Core schedule the follow-up cache eviction for
    /// the same run_id; an entry of `true` would mean the eviction could be
    /// admitted as a spurious duplicate. See
    /// `PollLanes::reject_workflow_delivery_with_reason`.
    #[cfg(test)]
    reject_completion_probes: Vec<bool>,
    /// Test-only trace of whether each terminally-completed run was still
    /// recorded in the ledger at the instant its completion was handed to
    /// Core. Mirrors `reject_completion_probes` for
    /// `PollLanes::complete_workflow`: a correct ordering always probes
    /// `false`.
    #[cfg(test)]
    complete_completion_probes: Vec<bool>,
}

/// Mutable delivery state associated with one activity completion debt.
#[derive(Clone, Copy, Debug, Default)]
struct ActivityState {
    cancelled: bool,
    leased: bool,
}

impl TaskLedger {
    /// Creates an open ledger with no outstanding completion obligations.
    pub fn new() -> Self {
        Self {
            phase: Phase::Open,
            workflows: HashMap::new(),
            activities: HashMap::new(),
            lost_poll_lease: false,
            retired_workflows: HashSet::new(),
            retired_activities: HashSet::new(),
            eviction_activations: HashSet::new(),
            #[cfg(test)]
            reject_completion_probes: Vec::new(),
            #[cfg(test)]
            complete_completion_probes: Vec::new(),
        }
    }

    /// Records whether `run_id` is still an outstanding workflow obligation at
    /// the moment its rejection failure is about to be sent to Core. Used only
    /// by tests to assert the retire-before-complete ordering deterministically.
    #[cfg(test)]
    fn probe_reject_completion(&mut self, run_id: &str) {
        self.reject_completion_probes
            .push(self.workflows.contains_key(run_id));
    }

    /// Returns the recorded reject-completion ordering probes for assertions.
    #[cfg(test)]
    fn reject_completion_probes(&self) -> &[bool] {
        &self.reject_completion_probes
    }

    /// Records whether `run_id` is still an outstanding workflow obligation at
    /// the moment its terminal completion is about to be sent to Core. Used
    /// only by tests to assert the retire-before-complete ordering
    /// deterministically. Mirrors [`Self::probe_reject_completion`].
    #[cfg(test)]
    fn probe_complete_completion(&mut self, run_id: &str) {
        self.complete_completion_probes
            .push(self.workflows.contains_key(run_id));
    }

    /// Returns the recorded complete-completion ordering probes for assertions.
    #[cfg(test)]
    fn complete_completion_probes(&self) -> &[bool] {
        &self.complete_completion_probes
    }

    /// Records one workflow activation before its delivery to OCaml.
    pub fn admit_workflow(&mut self, run_id: &str) -> Result<Admission, AdmitError> {
        self.ensure_open()?;
        self.record_workflow(run_id, false)
    }

    /// Records a task returned by a Core poll already in flight at shutdown.
    ///
    /// Unlike [`Self::admit_workflow`], this intentionally bypasses the open
    /// phase check: a poll that began before shutdown may still complete after
    /// the ledger has entered draining. The dispose path's post-join pass must
    /// be able to harvest that late identity.
    #[doc(hidden)]
    pub fn admit_polled_workflow(&mut self, run_id: &str) -> Result<Admission, AdmitError> {
        self.record_workflow(run_id, false)
    }

    /// Records a polled workflow activation together with whether it is a pure
    /// cache eviction.
    ///
    /// This is the workflow poll lane's admission. The eviction bit is stored
    /// atomically with the new ledger entry, before the lane enqueues the
    /// activation for OCaml, so [`Self::take_all_outstanding`] reports the
    /// correct completion shape even for an admission whose message is not
    /// yet visible in the ready queue. Like [`Self::admit_polled_workflow`],
    /// it bypasses the open-phase check.
    #[doc(hidden)]
    pub fn admit_polled_workflow_activation(
        &mut self,
        run_id: &str,
        eviction_only: bool,
    ) -> Result<Admission, AdmitError> {
        self.record_workflow(run_id, eviction_only)
    }

    /// Applies workflow identity rules after admission-phase handling.
    ///
    /// The eviction bit is recorded only for a new entry; a duplicate must not
    /// overwrite the classification of the activation that already owns the
    /// identity.
    fn record_workflow(
        &mut self,
        run_id: &str,
        eviction_only: bool,
    ) -> Result<Admission, AdmitError> {
        if run_id.is_empty() || run_id.len() > MAX_RUN_ID_BYTES {
            return Err(AdmitError::InvalidIdentity);
        }
        if self.retired_workflows.contains(run_id) {
            return Err(AdmitError::Retired);
        }
        match self.workflows.entry(run_id.to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(false);
                if eviction_only {
                    self.eviction_activations.insert(run_id.to_owned());
                } else {
                    self.eviction_activations.remove(run_id);
                }
                Ok(Admission::New)
            }
            Entry::Occupied(_) => Ok(Admission::Duplicate),
        }
    }

    /// Records an activity start or associates cancellation with its start.
    ///
    /// The boolean map value remembers whether cancellation has already been
    /// observed. Repeated cancellation is a duplicate delivery, while the
    /// first cancellation preserves the original single completion debt.
    pub fn admit_activity(
        &mut self,
        task_token: &[u8],
        kind: ActivityAdmission,
    ) -> Result<Admission, AdmitError> {
        if kind == ActivityAdmission::Start {
            self.ensure_open()?;
        }
        self.record_activity(task_token, kind)
    }

    /// Records an activity returned by a Core poll already in flight at shutdown.
    ///
    /// The poll lane may admit both a start and its cancellation while draining;
    /// the final dispose pass retires any resulting identity after the lane has
    /// stopped publishing messages.
    #[doc(hidden)]
    pub fn admit_polled_activity(
        &mut self,
        task_token: &[u8],
        kind: ActivityAdmission,
    ) -> Result<Admission, AdmitError> {
        self.record_activity(task_token, kind)
    }

    /// Applies token and cancellation rules after admission-phase handling.
    fn record_activity(
        &mut self,
        task_token: &[u8],
        kind: ActivityAdmission,
    ) -> Result<Admission, AdmitError> {
        if task_token.is_empty() || task_token.len() > MAX_TASK_TOKEN_BYTES {
            return Err(AdmitError::InvalidIdentity);
        }
        if self.retired_activities.contains(task_token) {
            return Err(AdmitError::Retired);
        }
        match (kind, self.activities.get_mut(task_token)) {
            (ActivityAdmission::Start, Some(_)) => Ok(Admission::Duplicate),
            (ActivityAdmission::Start, None) => {
                self.activities
                    .insert(task_token.to_vec(), ActivityState::default());
                Ok(Admission::New)
            }
            (ActivityAdmission::Cancel, Some(state)) if !state.cancelled => {
                state.cancelled = true;
                Ok(Admission::ExistingCancellation)
            }
            (ActivityAdmission::Cancel, Some(_)) => Ok(Admission::Duplicate),
            (ActivityAdmission::Cancel, None) => Err(AdmitError::UnknownActivityCancellation),
        }
    }

    /// Marks a ready workflow activation as handed to the OCaml supervisor.
    ///
    /// A second lease for an already-leased identity is rejected so a confused
    /// handoff cannot silently pretend two OCaml owners exist.
    pub fn lease_workflow(&mut self, run_id: &str) -> Result<(), CompleteError> {
        match self.workflows.get_mut(run_id) {
            Some(leased) if !*leased => {
                *leased = true;
                Ok(())
            }
            Some(_) => Err(CompleteError::AlreadyLeased),
            None => Err(CompleteError::UnknownWorkflow),
        }
    }

    /// Leases a ready workflow activation and records whether it is a pure
    /// cache eviction.
    ///
    /// This is the supervisor handoff used by [`PollLanes::try_take_workflow`].
    /// The eviction bit lets worker shutdown choose the only completion Core
    /// accepts for an abandoned lease (see [`Self::take_leased_for_shutdown`]).
    /// The poll lane already recorded the same bit at admission (see
    /// [`Self::admit_polled_workflow_activation`]); restating it from the
    /// dequeued activation keeps the lease authoritative for callers that
    /// admitted without one.
    pub fn lease_workflow_activation(
        &mut self,
        run_id: &str,
        eviction_only: bool,
    ) -> Result<(), CompleteError> {
        self.lease_workflow(run_id)?;
        if eviction_only {
            self.eviction_activations.insert(run_id.to_owned());
        } else {
            self.eviction_activations.remove(run_id);
        }
        Ok(())
    }

    /// Reports whether the outstanding workflow `run_id` (queued or leased to
    /// OCaml) is a pure cache eviction.
    pub fn is_eviction_lease(&self, run_id: &str) -> bool {
        self.eviction_activations.contains(run_id)
    }

    /// Removes a workflow admission that will never be leased to OCaml.
    ///
    /// Used after a dequeued activation fails lease handoff and has been
    /// force-failed to Core. Only unleased entries are removed so a concurrent
    /// legitimate lease cannot be erased by a confused identity.
    pub fn abandon_workflow_admission(&mut self, run_id: &str) {
        // Only unleased admissions may be dropped; a true lease must survive.
        if let Some(false) = self.workflows.get(run_id) {
            self.workflows.remove(run_id);
            self.eviction_activations.remove(run_id);
        }
    }

    /// Marks a ready activity task as handed to the OCaml supervisor.
    ///
    /// Mirrors [`Self::lease_workflow`]: double lease is a hard error.
    pub fn lease_activity(&mut self, task_token: &[u8]) -> Result<(), CompleteError> {
        match self.activities.get_mut(task_token) {
            Some(state) if !state.leased => {
                state.leased = true;
                Ok(())
            }
            Some(_) => Err(CompleteError::AlreadyLeased),
            None => Err(CompleteError::UnknownActivity),
        }
    }

    /// Hands one activity poll result to the OCaml supervisor.
    ///
    /// A `Start` transfers the single completion lease and therefore uses
    /// [`Self::lease_activity`]. A `Cancel` is an update to that same start,
    /// not another Core completion obligation; it is deliverable as long as
    /// the token remains tracked, regardless of whether the start was already
    /// leased. Keeping this distinction in the ledger prevents the poll lane
    /// from dropping a real cancellation or inventing a second completion debt.
    #[doc(hidden)]
    pub fn handoff_activity(
        &mut self,
        task_token: &[u8],
        kind: ActivityAdmission,
    ) -> Result<(), CompleteError> {
        match kind {
            ActivityAdmission::Start => self.lease_activity(task_token),
            ActivityAdmission::Cancel => self.ensure_activity_cancellation(task_token),
        }
    }

    /// Confirms that a cancellation update still refers to a tracked start.
    ///
    /// This check does not change the start's lease bit or completion debt. A
    /// missing token means the start completed before the queued cancellation
    /// reached the owner Domain, so the update is stale and can be discarded.
    fn ensure_activity_cancellation(&self, task_token: &[u8]) -> Result<(), CompleteError> {
        if self.activities.contains_key(task_token) {
            Ok(())
        } else {
            Err(CompleteError::UnknownActivity)
        }
    }

    /// Removes an activity admission that will never be leased to OCaml.
    ///
    /// Mirrors [`Self::abandon_workflow_admission`] for remote activity tokens.
    pub fn abandon_activity_admission(&mut self, task_token: &[u8]) {
        match self.activities.get(task_token) {
            Some(state) if !state.leased => {
                self.activities.remove(task_token);
            }
            Some(_) | None => {}
        }
    }

    /// Removes the exact workflow completion obligation named by `run_id`.
    pub fn complete_workflow(&mut self, run_id: &str) -> Result<(), CompleteError> {
        match self.workflows.get(run_id) {
            Some(true) => {
                self.workflows.remove(run_id);
                self.eviction_activations.remove(run_id);
                Ok(())
            }
            Some(false) => Err(CompleteError::NotLeased),
            None => Err(CompleteError::UnknownWorkflow),
        }
    }

    /// Restores a workflow lease that [`Self::complete_workflow`] retired
    /// optimistically before its Core completion await, after Core rejected
    /// that completion.
    ///
    /// `PollLanes::complete_workflow` retires the ledger entry before
    /// sending the completion to Core so a legitimate post-completion cache
    /// eviction is never misclassified as a duplicate (see its doc comment).
    /// But Core did not actually accept this completion, so the run never
    /// became complete from Core's point of view and no such eviction could
    /// have been scheduled for it during the await. The language side is
    /// expected to retry with a corrected completion, so the lease must be
    /// restored exactly as it was.
    ///
    /// If some other admission raced this run_id back into the ledger during
    /// the await — which the reasoning above says cannot happen for a
    /// genuinely rejected completion — the existing entry is left untouched
    /// rather than silently overwritten, since blindly stamping it leased
    /// would corrupt an unrelated obligation.
    ///
    /// Returns whether the lease was restored, so the caller can also restore
    /// its eviction bit with [`Self::restore_eviction_lease`].
    pub fn restore_rejected_workflow_completion(&mut self, run_id: &str) -> bool {
        match self.workflows.entry(run_id.to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(true);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Restores the eviction bit of a lease that
    /// [`Self::restore_rejected_workflow_completion`] just restored.
    pub fn restore_eviction_lease(&mut self, run_id: &str) {
        self.eviction_activations.insert(run_id.to_owned());
    }

    /// Verifies that a workflow completion is authorized without mutating it.
    pub fn ensure_workflow_leased(&self, run_id: &str) -> Result<(), CompleteError> {
        match self.workflows.get(run_id) {
            Some(true) => Ok(()),
            Some(false) => Err(CompleteError::NotLeased),
            None => Err(CompleteError::UnknownWorkflow),
        }
    }

    /// Removes the exact activity completion obligation named by its token.
    pub fn complete_activity(&mut self, task_token: &[u8]) -> Result<(), CompleteError> {
        match self.activities.get(task_token) {
            Some(state) if state.leased => {
                self.activities.remove(task_token);
                Ok(())
            }
            Some(_) => Err(CompleteError::NotLeased),
            None => Err(CompleteError::UnknownActivity),
        }
    }

    /// Retires a leased workflow that failed before OCaml could observe it.
    ///
    /// This is distinct from ordinary completion only at the call site: both
    /// consume one exact leased debt, but rejection is permitted solely after
    /// the bridge has attempted its own failure completion with Core.
    pub fn retire_rejected_workflow(&mut self, run_id: &str) -> Result<(), CompleteError> {
        self.complete_workflow(run_id)
    }

    /// Retires a leased activity token that semantic conversion could not
    /// expose, after the bridge has attempted a generated failure completion.
    pub fn retire_rejected_activity(&mut self, task_token: &[u8]) -> Result<(), CompleteError> {
        self.complete_activity(task_token)
    }

    /// Verifies that an activity completion is authorized without mutating it.
    pub fn ensure_activity_leased(&self, task_token: &[u8]) -> Result<(), CompleteError> {
        match self.activities.get(task_token) {
            Some(state) if state.leased => Ok(()),
            Some(_) => Err(CompleteError::NotLeased),
            None => Err(CompleteError::UnknownActivity),
        }
    }

    /// Closes admission before Core shutdown wakes the two poll lanes.
    pub fn begin_draining(&mut self) {
        self.phase = Phase::Draining;
    }

    /// Records that a Core-returned task was dropped without a safe completion.
    ///
    /// This is reserved for admission failures where the task identity is not
    /// sufficient to complete only the just-polled lease. The poll lane still
    /// publishes a fatal diagnostic, but this durable ledger bit ensures that
    /// shutdown cannot later finalize solely because ordinary outstanding
    /// counts reached zero.
    pub fn mark_lost_poll_lease(&mut self) {
        self.lost_poll_lease = true;
    }

    /// Returns whether Core finalization can consume the worker safely.
    pub fn can_finalize(&self) -> bool {
        self.phase == Phase::Draining && self.outstanding() == 0 && !self.lost_poll_lease
    }

    /// Returns the number of workflow activations awaiting completion.
    pub fn outstanding_workflows(&self) -> usize {
        self.workflows.len()
    }

    /// Returns the number of remote activities awaiting completion.
    pub fn outstanding_activities(&self) -> usize {
        self.activities.len()
    }

    /// Returns all completion obligations currently owned by the bridge.
    pub fn outstanding(&self) -> usize {
        self.outstanding_workflows() + self.outstanding_activities()
    }

    /// Unconditionally removes one workflow identity during dispose cleanup.
    pub fn force_remove_workflow(&mut self, run_id: &str) {
        self.workflows.remove(run_id);
        self.eviction_activations.remove(run_id);
    }

    /// Unconditionally removes one activity token during dispose cleanup.
    pub fn force_remove_activity(&mut self, task_token: &[u8]) {
        self.activities.remove(task_token);
    }

    /// Removes one workflow identity and records that disposal has already
    /// completed it. This operation is performed while the ledger mutex is
    /// held, before the asynchronous Core failure is awaited, closing the
    /// late-poll admission window without holding a lock across I/O.
    pub fn retire_workflow_for_dispose(&mut self, run_id: &str) {
        self.workflows.remove(run_id);
        self.eviction_activations.remove(run_id);
        self.retired_workflows.insert(run_id.to_owned());
    }

    /// Removes one activity token and records that disposal has already
    /// completed it. Cancellation-only notifications are filtered by the poll
    /// lane before this method is called because they do not own a completion.
    pub fn retire_activity_for_dispose(&mut self, task_token: &[u8]) {
        self.activities.remove(task_token);
        self.retired_activities.insert(task_token.to_vec());
    }

    /// Clears disposal tombstones after both poll producers have joined.
    ///
    /// No producer can admit another identity after that barrier, so retaining
    /// these sets would only leak memory across repeated worker lifecycles.
    fn clear_dispose_retired(&mut self) {
        self.retired_workflows.clear();
        self.retired_activities.clear();
    }

    /// Takes every outstanding identity so dispose can complete each once.
    ///
    /// Each workflow is returned with its eviction bit. The bit is recorded at
    /// admission (see [`Self::admit_polled_workflow_activation`]), so it is
    /// correct for leased entries, for queued entries, and for entries the
    /// poll lane has admitted but not yet enqueued; the caller never has to
    /// reconcile it against the ready queue. Every returned identity is
    /// tombstoned so a concurrent poll of the same
    /// identity cannot be admitted as a second debt while disposal completes
    /// it.
    pub fn take_all_outstanding(&mut self) -> (Vec<(String, bool)>, Vec<Vec<u8>>) {
        let workflows: Vec<(String, bool)> = self
            .workflows
            .drain()
            .map(|(run_id, _)| {
                let eviction_only = self.eviction_activations.contains(&run_id);
                (run_id, eviction_only)
            })
            .collect();
        self.eviction_activations.clear();
        self.retired_workflows
            .extend(workflows.iter().map(|(run_id, _)| run_id.clone()));
        let activities: Vec<Vec<u8>> = self
            .activities
            .drain()
            .map(|(task_token, _)| task_token)
            .collect();
        self.retired_activities.extend(activities.iter().cloned());
        (workflows, activities)
    }

    /// Takes all outstanding identities for replay abandonment without
    /// creating live-worker disposal tombstones. Replay may legitimately emit
    /// a same-run eviction after an empty completion, so a retired marker
    /// would incorrectly reject that follow-up activation before it can be
    /// acknowledged. The replay join loop remains responsible for draining
    /// any identity published after this snapshot.
    pub fn take_all_outstanding_for_replay(&mut self) -> (Vec<String>, Vec<Vec<u8>>) {
        let workflows = self.workflows.drain().map(|(run_id, _)| run_id).collect();
        self.eviction_activations.clear();
        let activities = self
            .activities
            .drain()
            .map(|(task_token, _)| task_token)
            .collect();
        (workflows, activities)
    }

    /// Removes every task currently leased to OCaml so worker shutdown can
    /// complete each one exactly once on OCaml's behalf.
    ///
    /// Each workflow is returned with its eviction bit (see
    /// [`Self::lease_workflow_activation`]). Unleased entries stay in place:
    /// each of them has exactly one ready-queue message (or one about to be
    /// enqueued by its poll lane), and shutdown retires it from that message so
    /// one identity can never receive two completions. No tombstone is written,
    /// unlike [`Self::take_all_outstanding`]: Core legitimately answers a
    /// failed workflow task with a cache eviction for the same run, which
    /// shutdown must admit and acknowledge for Core to report `ShutDown`.
    pub fn take_leased_for_shutdown(&mut self) -> (Vec<(String, bool)>, Vec<Vec<u8>>) {
        let leased_workflows: Vec<String> = self
            .workflows
            .iter()
            .filter(|(_, leased)| **leased)
            .map(|(run_id, _)| run_id.clone())
            .collect();
        let workflows = leased_workflows
            .into_iter()
            .map(|run_id| {
                self.workflows.remove(&run_id);
                let eviction_only = self.eviction_activations.remove(&run_id);
                (run_id, eviction_only)
            })
            .collect();
        let activities: Vec<Vec<u8>> = self
            .activities
            .iter()
            .filter(|(_, state)| state.leased)
            .map(|(task_token, _)| task_token.clone())
            .collect();
        for task_token in &activities {
            self.activities.remove(task_token);
        }
        (workflows, activities)
    }

    /// Rejects admission once shutdown has atomically entered draining.
    fn ensure_open(&self) -> Result<(), AdmitError> {
        match self.phase {
            Phase::Open => Ok(()),
            Phase::Draining => Err(AdmitError::Draining),
        }
    }
}

impl Default for TaskLedger {
    /// Uses the same empty open state as [`TaskLedger::new`].
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod readiness_tests {
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
        thread::sleep(Duration::from_millis(10));
        signal.close();

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

    /// Builds the workflow/activity signal pair exactly as a live worker does.
    fn lane_pair() -> (Arc<Readiness>, Arc<Readiness>) {
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
                    match wait_any_lane(&waiter_workflow, &waiter_activity) {
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
        assert_eq!(wait_any_lane(&workflow, &activity), ReadinessWait::Ready);
        assert_eq!(wait_any_lane(&workflow, &activity), ReadinessWait::Ready);
        assert!(activity.take(&mut receiver).is_some());
        assert_eq!(
            wait_any_lane(&workflow, &activity),
            ReadinessWait::Error(error)
        );

        let (workflow, activity) = lane_pair();
        activity.close();
        assert_eq!(wait_any_lane(&workflow, &activity), ReadinessWait::Shutdown);
    }

    /// Closing either lane wakes a combined waiter instead of leaving it
    /// blocked until the bounded timeout.
    #[test]
    fn combined_wait_wakes_on_shutdown() {
        let (workflow, activity) = lane_pair();
        let waiter_workflow = Arc::clone(&workflow);
        let waiter_activity = Arc::clone(&activity);
        let waiter = thread::spawn(move || wait_any_lane(&waiter_workflow, &waiter_activity));
        thread::sleep(Duration::from_millis(10));
        workflow.close();

        assert_eq!(
            waiter.join().expect("waiter must not panic"),
            ReadinessWait::Shutdown
        );
    }

    /// Two quiet lanes still return control to the supervisor within the
    /// documented bound, preserving shutdown responsiveness.
    #[test]
    fn quiet_combined_wait_is_bounded() {
        let (workflow, activity) = lane_pair();

        assert_eq!(wait_any_lane(&workflow, &activity), ReadinessWait::TimedOut);
    }
}

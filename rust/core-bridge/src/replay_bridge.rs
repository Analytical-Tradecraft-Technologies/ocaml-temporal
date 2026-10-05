//! Private replay-worker plumbing for deterministic history verification.
//!
//! The public OCaml API keeps this module private while the supervisor uses it
//! as the replay ownership boundary. Temporal Core owns replay state, Rust
//! owns the bounded history feeder, and no protobuf value crosses into OCaml.
//! Histories arrive as strict JSON with a canonical base64 protobuf body, so
//! the C/OCaml adapter can exchange a small auditable document without
//! importing Temporal's generated types.

// The Rust replay entry points remain private to the bridge crate. The OCaml
// supervisor reaches them only through the checked C ABI, which keeps Core
// handles, futures, and completion leases out of the public library surface.
#![allow(dead_code)]
#![allow(clippy::enum_variant_names)]

use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost::Message;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::sync::Arc;
use std::{fmt, panic::AssertUnwindSafe, panic::catch_unwind};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::WorkflowActivation, workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::history::v1::History,
};
use temporalio_sdk_core::replay::{
    HistoryFeeder, HistoryForReplay, HistoryInfo, ReplayWorkerInput,
};
use temporalio_sdk_core::{CoreRuntime, Worker, WorkerConfig};
use tokio::runtime::Handle;

use crate::protocol::{self, MAX_PAYLOAD_BYTES, MAX_STRING_BYTES};
use crate::worker_bridge::{
    AdmitError, PollLaneError, PollLanes, ReadinessWait, ReadyTask, WorkerBridgeError,
};

/// Closed validation categories used internally before a history can reach
/// Core. Details from malformed protobuf input are intentionally discarded so
/// future ABI diagnostics cannot echo workflow-controlled bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplayInputError {
    /// The outer JSON document was malformed or had an unexpected field.
    InvalidDocument,
    /// The workflow ID was empty, oversized, or contained a NUL byte.
    InvalidWorkflowId,
    /// The history payload did not use canonical padded base64.
    InvalidEncoding,
    /// The decoded bytes were not a valid Temporal History protobuf.
    InvalidProtobuf,
    /// Core's history invariant checks rejected the event sequence.
    InvalidHistory,
}

impl fmt::Display for ReplayInputError {
    /// Formats a stable category without including input data or parser text.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidDocument => "replay history JSON failed validation",
            Self::InvalidWorkflowId => "replay history workflow_id failed validation",
            Self::InvalidEncoding => "replay history payload is not canonical base64",
            Self::InvalidProtobuf => "replay history is not a valid Temporal protobuf",
            Self::InvalidHistory => "replay history failed Core invariant validation",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ReplayInputError {}

/// Strict wire representation of one replay history document.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplayHistoryDocument {
    /// Workflow identity is absent from Temporal's History protobuf and must
    /// be attached by the caller before Core can construct a replay task.
    workflow_id: String,
    /// The history's protobuf bytes wrapped in the bridge's binary-safe form.
    history: ReplayPayload,
}

/// Canonical base64 wrapper for one protobuf byte sequence.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplayPayload {
    /// Only `base64` is accepted; no implicit text or hex conversion exists.
    encoding: String,
    /// Padded standard-base64 bytes, compared against a re-encoding below.
    data: String,
}

/// Bounds and validates the workflow identity attached to a replay history.
fn validate_workflow_id(workflow_id: &str) -> Result<(), ReplayInputError> {
    if workflow_id.is_empty()
        || workflow_id.len() > MAX_STRING_BYTES
        || workflow_id.as_bytes().contains(&0)
    {
        return Err(ReplayInputError::InvalidWorkflowId);
    }
    Ok(())
}

/// Decodes one canonical base64 protobuf payload after validating the wire
/// object's exact fields and the shared JSON resource limits.
fn decode_history_bytes(payload: &ReplayPayload) -> Result<Vec<u8>, ReplayInputError> {
    if payload.encoding != "base64" {
        return Err(ReplayInputError::InvalidEncoding);
    }
    let decoded = STANDARD
        .decode(payload.data.as_bytes())
        .map_err(|_| ReplayInputError::InvalidEncoding)?;
    if decoded.len() > MAX_PAYLOAD_BYTES || STANDARD.encode(&decoded) != payload.data {
        return Err(ReplayInputError::InvalidEncoding);
    }
    Ok(decoded)
}

/// Parses a replay document through the duplicate-aware shared JSON parser,
/// then applies the closed Rust representation. Running both checks matters:
/// `serde(deny_unknown_fields)` rejects typos while the shared parser rejects
/// duplicate members that serde's map-like decoding would otherwise overwrite.
fn decode_wire(input: &str) -> Result<ReplayHistoryDocument, ReplayInputError> {
    protocol::decode_payload_object(input).map_err(|_| ReplayInputError::InvalidDocument)?;
    serde_json::from_str(input).map_err(|_| ReplayInputError::InvalidDocument)
}

/// Converts one validated JSON replay document into Core's replay input.
///
/// `HistoryInfo::new_from_history` is the same invariant gate used by Core's
/// replay worker. It ensures the first event, workflow type, run ID, and
/// workflow-task boundaries are valid before the history enters the
/// `HistoryFeeder`; this prevents Core's later history stream assertion from
/// becoming a panic caused by untrusted bridge input.
pub(crate) fn decode_history_document(input: &str) -> Result<HistoryForReplay, ReplayInputError> {
    let document = decode_wire(input)?;
    validate_workflow_id(&document.workflow_id)?;
    let bytes = decode_history_bytes(&document.history)?;
    let history =
        History::decode(bytes.as_slice()).map_err(|_| ReplayInputError::InvalidProtobuf)?;
    let info = catch_unwind(AssertUnwindSafe(|| {
        HistoryInfo::new_from_history(&history, None)
    }))
    .map_err(|_| ReplayInputError::InvalidHistory)?
    .map_err(|_| ReplayInputError::InvalidHistory)?;
    if info.orig_run_id().is_empty() {
        return Err(ReplayInputError::InvalidHistory);
    }
    Ok(HistoryForReplay::new(info, document.workflow_id))
}

/// Encodes one validated history into the JSON document consumed by
/// [`decode_history_document`]. The immediate decode round trip catches a
/// future schema or canonicalization change before the value is handed to a
/// feeder.
pub(crate) fn encode_history_document(
    workflow_id: &str,
    history: &History,
) -> Result<String, ReplayInputError> {
    validate_workflow_id(workflow_id)?;
    let info = catch_unwind(AssertUnwindSafe(|| {
        HistoryInfo::new_from_history(history, None)
    }))
    .map_err(|_| ReplayInputError::InvalidHistory)?
    .map_err(|_| ReplayInputError::InvalidHistory)?;
    if info.orig_run_id().is_empty() {
        return Err(ReplayInputError::InvalidHistory);
    }
    let canonical_history: History = info.into();
    let bytes = canonical_history.encode_to_vec();
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(ReplayInputError::InvalidProtobuf);
    }
    let document = ReplayHistoryDocument {
        workflow_id: workflow_id.to_owned(),
        history: ReplayPayload {
            encoding: "base64".to_owned(),
            data: STANDARD.encode(bytes),
        },
    };
    let encoded =
        serde_json::to_string(&document).map_err(|_| ReplayInputError::InvalidDocument)?;
    decode_history_document(&encoded)?;
    Ok(encoded)
}

/// Stable categories for failures while creating or draining a replay worker.
#[derive(Debug)]
pub(crate) enum ReplayWorkerError {
    /// The feeder input was already closed by `finish_input` or finalization.
    FeederClosed,
    /// Finalization was requested before Core had naturally drained the input.
    ///
    /// The worker is returned alongside this error. Callers must either keep
    /// polling and completing activations, then retry `finalize`, or choose the
    /// explicit `dispose` path when abandoning the replay is intentional.
    ReplayNotDrained {
        /// Whether the history feeder had been closed by the caller.
        input_finished: bool,
        /// Whether the caller observed the workflow lane's terminal shutdown.
        workflow_shutdown_observed: bool,
        /// Whether Core's task ledger had no outstanding completion debt.
        outstanding_tasks: bool,
    },
    /// A history failed before it could be sent to Core.
    InvalidHistory(ReplayInputError),
    /// Core could not construct the replay worker.
    CoreInitialization,
    /// A guarded poll lane stopped unexpectedly.
    PollLane(PollLaneError),
    /// Core could not finalize after all lanes were joined.
    Finalization(WorkerBridgeError),
}

impl fmt::Display for ReplayWorkerError {
    /// Formats a stable lifecycle category while retaining the nested error
    /// only for Rust-side classification; input and server text never cross
    /// the future OCaml ABI through this formatter.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FeederClosed => formatter.write_str("replay history feeder is closed"),
            Self::ReplayNotDrained { .. } => {
                formatter.write_str("replay worker input was not fully drained")
            }
            Self::InvalidHistory(error) => write!(formatter, "invalid replay history: {error}"),
            Self::CoreInitialization => {
                formatter.write_str("Core replay worker initialization failed")
            }
            Self::PollLane(error) => {
                let category = match error {
                    PollLaneError::Core(_) => "Core polling failed",
                    PollLaneError::Admission(AdmitError::Retired) => {
                        "poll identity was already retired during disposal"
                    }
                    PollLaneError::Admission(_) => "poll task admission failed",
                    PollLaneError::DuplicateIdentity => "poll task identity was duplicated",
                    PollLaneError::InvalidActivityVariant => "poll activity variant was invalid",
                };
                formatter.write_str(category)
            }
            Self::Finalization(error) => {
                let category = match error {
                    WorkerBridgeError::Completion(_) => "worker completion was invalid",
                    WorkerBridgeError::CoreWorkflow(_) => "Core workflow completion failed",
                    WorkerBridgeError::CoreActivity(_) => "Core activity completion failed",
                    WorkerBridgeError::RetryableActivityCompletion => {
                        "worker completion transport was temporarily unavailable"
                    }
                    WorkerBridgeError::OutstandingTasks(_) => "worker has outstanding tasks",
                    WorkerBridgeError::LostPollLease => "worker has an uncompleted poll lease",
                    WorkerBridgeError::WorkerStillShared => "worker ownership was not released",
                };
                formatter.write_str(category)
            }
        }
    }
}

impl std::error::Error for ReplayWorkerError {}

/// Owns one workflow-only Core replay worker and its bounded history feeder.
///
/// The feeder has capacity one. A caller can therefore submit histories in a
/// deterministic order without building an unbounded queue, while the
/// workflow-only `PollLanes` keeps Core's long poll away from the OCaml owner
/// Domain. Dropping the feeder closes the input stream. A normal `finalize`
/// joins every lane only after the caller has observed Core's natural shutdown;
/// the separate `dispose` path is the only operation allowed to abandon
/// unfinished replay work. Replay disposal uses Core's empty completion for
/// every abandoned activation because a replay eviction has no live workflow
/// task to fail.
pub(crate) struct ReplayWorker {
    /// Guarded workflow handoff and completion ledger shared with live workers.
    lanes: PollLanes,
    /// Sender retained until the caller has submitted the complete corpus.
    feeder: Option<HistoryFeeder>,
    /// Set only after the owner observes `ReadinessWait::Shutdown` on the
    /// workflow lane. This is stronger than an empty queue: Core sets it only
    /// after the replay stream has ended and no further activation can be
    /// produced.
    workflow_shutdown_observed: bool,
}

impl ReplayWorker {
    /// Creates a workflow-only Core replay worker without a Temporal client.
    ///
    /// Core's `ReplayWorkerInput` overrides the ordinary worker configuration
    /// to one cached workflow and one workflow poller. We still pass the
    /// caller's namespace and task queue because Core uses them in activation
    /// metadata and diagnostics.
    pub(crate) fn start(
        core: &CoreRuntime,
        config: WorkerConfig,
    ) -> Result<Self, ReplayWorkerError> {
        let (feeder, stream) = HistoryFeeder::new(1);
        let handle = core.tokio_handle();
        let worker: Worker = {
            // Core's constructor registers Tokio tasks synchronously, so the
            // owned runtime must be entered for the duration of construction.
            let _runtime_guard = handle.enter();
            temporalio_sdk_core::init_replay_worker(ReplayWorkerInput::new(config, stream))
        }
        .map_err(|_| ReplayWorkerError::CoreInitialization)?;
        Ok(Self {
            lanes: PollLanes::start_workflow_only(worker, &handle),
            feeder: Some(feeder),
            workflow_shutdown_observed: false,
        })
    }

    /// Validates and queues one history while preserving FIFO replay order.
    ///
    /// The bounded `HistoryFeeder::feed` future is driven on Core's runtime;
    /// no Tokio task retains an OCaml pointer or mutates this owner. The
    /// future may wait for the one-slot queue to drain, which is intentional
    /// backpressure rather than an unbounded allocation path.
    pub(crate) fn feed_json(
        &mut self,
        handle: &Handle,
        input: &str,
    ) -> Result<(), ReplayWorkerError> {
        let history = decode_history_document(input).map_err(ReplayWorkerError::InvalidHistory)?;
        let feeder = self
            .feeder
            .as_ref()
            .ok_or(ReplayWorkerError::FeederClosed)?;
        handle
            .block_on(feeder.feed(history))
            .map_err(|_| ReplayWorkerError::FeederClosed)
    }

    /// Closes the history stream. Core will finish queued histories and then
    /// initiate replay-worker shutdown once the final activation is handled.
    pub(crate) fn finish_input(&mut self) {
        self.feeder.take();
    }

    /// Waits for the workflow lane's next replay event.
    ///
    /// The owner must keep calling this method, draining and completing every
    /// `Ready` activation, until it returns `Shutdown`. Recording that terminal
    /// observation prevents `finalize` from mistaking an empty handoff queue
    /// for a replay that Core has actually finished.
    pub(crate) fn wait_workflow(&mut self) -> ReadinessWait {
        let readiness = self.lanes.wait_workflow();
        if matches!(readiness, ReadinessWait::Shutdown) {
            self.workflow_shutdown_observed = true;
        }
        readiness
    }

    /// Takes one workflow activation for the owner Domain to execute.
    pub(crate) fn try_take_workflow(
        &mut self,
        handle: &Handle,
    ) -> Option<ReadyTask<WorkflowActivation>> {
        self.lanes.try_take_workflow(handle)
    }

    /// Completes one activation and retires its Core completion debt.
    pub(crate) async fn complete_workflow(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), ReplayWorkerError> {
        self.lanes
            .complete_workflow(completion)
            .await
            .map_err(ReplayWorkerError::Finalization)
    }

    /// Fails one activation that was leased by the replay poll lane but could
    /// not be represented by the semantic OCaml document. A pure cache
    /// eviction owns no workflow task and is acknowledged empty instead.
    ///
    /// Replay uses the same one-shot completion debt as a live worker. Keeping
    /// this recovery operation on the Rust-owned worker means a malformed
    /// activation cannot strand Core while the owner still has a typed error
    /// to report. The static reason is deliberately bounded and does not
    /// include workflow-controlled identifiers or payloads.
    pub(crate) async fn reject_workflow_delivery(
        &self,
        run_id: &str,
        reason: &'static str,
    ) -> Result<(), ReplayWorkerError> {
        self.lanes
            .reject_workflow_delivery_with_reason(run_id, reason)
            .await
            .map_err(ReplayWorkerError::Finalization)
    }

    /// Finalizes a replay that Core has already drained naturally.
    ///
    /// This is deliberately not a disposal operation. Core's replay worker
    /// owns the history stream and only shuts down after the stream is closed,
    /// each activation is completed, and the next poll observes end-of-input.
    /// Calling `initiate_shutdown` here would cancel a queued history and make
    /// a successful-looking replay that never ran. The owner therefore gets a
    /// typed precondition error, with the worker retained for another drain
    /// attempt, until all three conditions are true:
    ///
    /// 1. `finish_input` has closed the feeder;
    /// 2. `wait_workflow` has returned `Shutdown`; and
    /// 3. the bridge ledger has no outstanding completion debt.
    // The error deliberately returns the worker itself so ownership is never
    // lost on failure; boxing it would only move the same value to the heap.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn finalize(
        mut self,
        handle: &Handle,
    ) -> Result<(), (Self, ReplayWorkerError)> {
        let input_finished = self.feeder.is_none();
        let workflow_shutdown_observed = self.workflow_shutdown_observed;
        let outstanding_tasks = self.lanes.has_outstanding_tasks();
        if !input_finished || !workflow_shutdown_observed || outstanding_tasks {
            return Err((
                self,
                ReplayWorkerError::ReplayNotDrained {
                    input_finished,
                    workflow_shutdown_observed,
                    outstanding_tasks,
                },
            ));
        }
        // Core has already reported the workflow lane's terminal shutdown, so
        // changing only the bridge ledger to `Draining` cannot cancel queued
        // replay work. `mark_natural_shutdown` deliberately does not call
        // Core's shutdown token a second time.
        self.lanes.mark_natural_shutdown();
        let join_result = self.lanes.join_poll_lanes().await;
        if let Err(error) = join_result {
            // `join_poll_lanes` has already awaited every producer, so no
            // Tokio task can publish another activation. The lane error still
            // requires an explicit disposal sequence: initiate Core shutdown
            // and retire any ledger debt, then return the intact owner to the
            // caller instead of dropping an unfinalized native graph.
            {
                let _runtime_guard = handle.enter();
                self.lanes.initiate_shutdown();
            }
            // A replay lane can fail after Core has published an eviction.
            // Never send the live-worker failure completion here: Core accepts
            // only an empty acknowledgement for that replay-only state.
            self.lanes.abandon_replay_for_dispose().await;
            return Err((self, ReplayWorkerError::PollLane(error)));
        }
        self.lanes.finalize().await.map_err(|(lanes, error)| {
            (
                Self {
                    lanes,
                    feeder: self.feeder,
                    workflow_shutdown_observed: self.workflow_shutdown_observed,
                },
                ReplayWorkerError::Finalization(error),
            )
        })
    }

    /// Abandons a replay explicitly after a precondition or caller error.
    ///
    /// Unlike [`Self::finalize`], this path abandons queued or leased replay
    /// work because the caller has chosen disposal over replay correctness.
    /// It first acknowledges currently owned activations with Core's
    /// shutdown-safe empty completion, then initiates shutdown and drains any
    /// eviction activation published while the poll lane exits. A join failure
    /// returns the retained worker and a typed lane error without attempting
    /// finalization; all join handles have been consumed, so a retry cannot
    /// leave a detached Tokio task behind. Core's terminal finalizer is
    /// attempted only after this abandonment barrier. If it refuses, the
    /// worker is returned with the typed error instead of being dropped; the
    /// caller must retry disposal or take another explicit ownership-preserving
    /// recovery action.
    // The error deliberately returns the worker itself so ownership is never
    // lost on failure; boxing it would only move the same value to the heap.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn dispose(
        mut self,
        handle: &Handle,
    ) -> Result<(), (Self, ReplayWorkerError)> {
        self.finish_input();
        self.lanes.abandon_replay_for_dispose().await;
        {
            let _runtime_guard = handle.enter();
            self.lanes.initiate_shutdown();
        }
        // The lane may publish an eviction only after the first empty
        // completion is accepted. Join it through the replay-aware drain so
        // that activation is acknowledged instead of leaving Core's poll
        // future waiting forever.
        let join_result = self.lanes.join_replay_poll_lane().await;
        if let Err(error) = join_result {
            // Every join handle has been consumed, so a retry cannot leave a
            // detached producer behind. Keep the lane graph intact and make
            // the lane failure explicit instead of consuming it in a
            // finalization attempt whose error would be harder to recover.
            return Err((self, ReplayWorkerError::PollLane(error)));
        }
        // Replay activations are not live server workflow tasks. In
        // particular, a queued activation can be a cache eviction for which
        // Core rejects the non-empty failure completion used by live-worker
        // disposal. After both producers have stopped, acknowledge replay
        // obligations with Core's shutdown-safe empty completion instead.
        self.lanes.abandon_replay_for_dispose().await;
        let lanes = self.lanes;
        let feeder = self.feeder;
        let workflow_shutdown_observed = self.workflow_shutdown_observed;
        lanes.finalize().await.map_err(|(lanes, error)| {
            (
                Self {
                    lanes,
                    feeder,
                    workflow_shutdown_observed,
                },
                ReplayWorkerError::Finalization(error),
            )
        })
    }

    /// Retains a second Core worker owner for the disposal ownership test.
    ///
    /// This is compiled only into the Rust test crate. The production bridge
    /// keeps the Core worker private and exposes no raw ownership handle.
    #[cfg(test)]
    pub(crate) fn retain_worker_for_test(&self) -> Arc<Worker> {
        self.lanes.retain_worker_for_test()
    }

    /// Replaces the workflow poll task with an awaited, deliberately aborted
    /// pending task so disposal can verify that a joined lane failure is
    /// reported with the replay worker still owned. Awaiting the original
    /// handle prevents a natural Core shutdown from making this test timing
    /// dependent.
    ///
    /// This is a deterministic test hook only; production shutdown always
    /// asks Core to stop and then awaits the lane naturally.
    #[cfg(test)]
    pub(crate) async fn abort_workflow_lane_for_test(&mut self) {
        self.lanes.abort_workflow_lane_for_test().await;
    }

    /// Exposes the ledger's reject-completion ordering probes so a test can
    /// assert that each rejection retired its lease before Core was asked to
    /// fail the activation.
    #[cfg(test)]
    pub(crate) fn reject_completion_probes(&self) -> Vec<bool> {
        self.lanes.reject_completion_probes()
    }

    /// Exposes the ledger's terminal-completion ordering probes so a test can
    /// assert that each completion retired its lease before Core was asked to
    /// complete the activation. Mirrors [`Self::reject_completion_probes`].
    #[cfg(test)]
    pub(crate) fn complete_completion_probes(&self) -> Vec<bool> {
        self.lanes.complete_completion_probes()
    }
}

#[cfg(test)]
#[path = "../tests/support/replay_bridge.rs"]
mod replay_tests;

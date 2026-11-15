//! Closed JSON protocol and Core adapter for the first native client slice.
//!
//! This module deliberately exposes neither `temporalio_client::Client` nor
//! protobuf values to OCaml.  The bridge accepts small, lossless request
//! documents for starting a workflow, observing one run, and requesting
//! control, signal, query, and update operations against a run. Every request
//! after start names either one exact run or, with an empty `run_id`, the
//! workflow's current run as Temporal resolves it (#791).
//! A wait never follows a continued-as-new successor: callers can inspect the
//! successor metadata and decide what to do in OCaml.

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use temporalio_client::grpc::WorkflowService;
use temporalio_client::request_extensions::{NoRetryOnMatching, RetryConfigForCall};
use temporalio_client::tonic::metadata::MetadataValue;
use temporalio_client::tonic::{Code, IntoRequest, Request, Response, Status};
use temporalio_client::{Connection, RetryOptions};
use temporalio_common::protos::temporal::api::{
    common::v1::{
        Memo, Payload as CorePayload, Payloads, SearchAttributes, WorkflowExecution, WorkflowType,
    },
    enums::v1::{
        HistoryEventFilterType, UpdateWorkflowExecutionLifecycleStage, WorkflowIdConflictPolicy,
        WorkflowIdReusePolicy,
    },
    history::v1::{HistoryEvent, history_event::Attributes},
    query::v1::WorkflowQuery,
    taskqueue::v1::TaskQueue,
    update::{self, v1::WaitPolicy},
    workflowservice::v1::{
        CountWorkflowExecutionsRequest, DescribeWorkflowExecutionRequest,
        GetWorkflowExecutionHistoryRequest, ListWorkflowExecutionsRequest,
        PollWorkflowExecutionUpdateRequest, QueryWorkflowRequest as QueryWorkflowExecutionRequest,
        RequestCancelWorkflowExecutionRequest, ResetWorkflowExecutionRequest,
        SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
        TerminateWorkflowExecutionRequest, UpdateWorkflowExecutionRequest,
        UpdateWorkflowExecutionResponse,
    },
};

use crate::workflow_protocol::ProtoTime as _;
use crate::{protocol, workflow_protocol};

// Signal-with-start and update-with-start (#1151) live in a child module so
// they can reuse this module's private start, update, and status helpers.
mod with_start;
pub(crate) use with_start::{
    SignalWithStartRequest, UpdateWithStartOutcome, UpdateWithStartRequest,
    decode_signal_with_start_request, decode_update_with_start_request,
    encode_update_with_start_outcome, same_signal_with_start_request,
    same_update_with_start_request, signal_with_start_workflow, update_with_start_workflow,
};

/// One exact execution identity carried by client responses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRef {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Concrete run identifier.
    pub run_id: String,
}

/// One named payload in a workflow-start memo or search-attribute map.
/// Arrays are used at the JSON boundary so duplicate names can be rejected
/// before conversion to protobuf maps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataField {
    /// User-visible metadata key.
    pub key: String,
    /// Payload retained under that key.
    pub value: workflow_protocol::Payload,
}

/// What Temporal does when a start names a workflow ID whose current run is
/// still open.
///
/// The variants mirror Temporal's `WorkflowIdConflictPolicy` without the
/// `UNSPECIFIED` value: the bridge always sends an explicit policy so the
/// behaviour cannot silently depend on a server-side default. The JSON
/// spelling is the snake-case variant name.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdConflictPolicy {
    /// Reject the start with `WorkflowExecutionAlreadyStarted`, which the
    /// bridge reports as the closed `already_started` error. This is the
    /// default when an older request document omits the field.
    #[default]
    Fail,
    /// Return the running execution instead of starting another one. The
    /// response then names the existing run with `started = false`.
    UseExisting,
    /// Terminate the running execution and start a new run.
    TerminateExisting,
}

impl IdConflictPolicy {
    /// Converts the closed bridge value into the protobuf enum number.
    fn to_core(self) -> i32 {
        let policy = match self {
            Self::Fail => WorkflowIdConflictPolicy::Fail,
            Self::UseExisting => WorkflowIdConflictPolicy::UseExisting,
            Self::TerminateExisting => WorkflowIdConflictPolicy::TerminateExisting,
        };
        i32::from(policy)
    }
}

/// What Temporal does when a start names a workflow ID whose most recent run
/// has already closed.
///
/// The variants mirror Temporal's `WorkflowIdReusePolicy` without
/// `UNSPECIFIED` (the bridge always sends an explicit value) and without the
/// deprecated `TERMINATE_IF_RUNNING`, whose behaviour belongs to
/// [`IdConflictPolicy::TerminateExisting`] and which Temporal rejects next to
/// an explicit conflict policy. The JSON spelling is the snake-case name.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdReusePolicy {
    /// Start a new run whatever the closed run's outcome. This is Temporal's
    /// default and the value of an omitted field.
    #[default]
    AllowDuplicate,
    /// Start a new run only when the closed run did not complete
    /// successfully (failed, cancelled, terminated, or timed out).
    AllowDuplicateFailedOnly,
    /// Never reuse the workflow ID while Temporal retains the closed run.
    RejectDuplicate,
}

impl IdReusePolicy {
    /// Converts the closed bridge value into the protobuf enum number.
    fn to_core(self) -> i32 {
        let policy = match self {
            Self::AllowDuplicate => WorkflowIdReusePolicy::AllowDuplicate,
            Self::AllowDuplicateFailedOnly => WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
            Self::RejectDuplicate => WorkflowIdReusePolicy::RejectDuplicate,
        };
        i32::from(policy)
    }
}

/// Largest caller-selected RPC timeout, in milliseconds (#499).
///
/// Control-plane acknowledgements, queries, and update admission run
/// synchronously on the supervisor's owner Domain, so a caller's timeout is
/// also how long one call may hold it. One minute keeps that bounded while
/// still exceeding every built-in default.
pub const MAX_RPC_TIMEOUT_MS: u64 = 60_000;

/// Largest workflow task timeout accepted by the bridge, in milliseconds.
///
/// Temporal silently lowers a larger workflow task timeout to this value; the
/// bridge rejects it instead so the caller never believes a longer bound is
/// in force.
pub const MAX_WORKFLOW_TASK_TIMEOUT_MS: u64 = 120_000;

/// Largest workflow execution or run timeout, in milliseconds: the protobuf
/// `Duration` maximum of 315,576,000,000 seconds plus 999 milliseconds, which
/// is also the public `Duration.t` bound.
pub const MAX_WORKFLOW_TIMEOUT_MS: u64 = 315_576_000_000_999;

/// Converts a validated millisecond count into a protobuf duration.
///
/// The bounds above keep every accepted value representable, so the
/// conversion fails only if a caller skipped validation; that is reported as
/// a Core conversion error rather than a panic.
fn milliseconds_to_core(
    milliseconds: u64,
) -> Result<prost_wkt_types::Duration, ClientOperationError> {
    let invalid =
        || ClientOperationError::Core(workflow_protocol::invalid_core("duration is out of range"));
    Ok(prost_wkt_types::Duration {
        seconds: i64::try_from(milliseconds / 1_000).map_err(|_| invalid())?,
        nanos: i32::try_from((milliseconds % 1_000) * 1_000_000).map_err(|_| invalid())?,
    })
}

/// Converts an optional validated millisecond count into an optional
/// protobuf duration; absent stays absent so Temporal applies its default.
fn optional_milliseconds_to_core(
    milliseconds: Option<u64>,
) -> Result<Option<prost_wkt_types::Duration>, ClientOperationError> {
    milliseconds.map(milliseconds_to_core).transpose()
}

/// Returns the total budget of one bounded client RPC: the caller's
/// validated `rpc_timeout_ms` when present, otherwise the operation default.
fn rpc_budget(rpc_timeout_ms: Option<u64>, default: Duration) -> Duration {
    rpc_timeout_ms.map_or(default, Duration::from_millis)
}

/// Validates an optional caller-selected RPC timeout: it must be between one
/// millisecond and [`MAX_RPC_TIMEOUT_MS`].
fn validate_rpc_timeout(value: Option<u64>) -> Result<(), protocol::ProtocolError> {
    match value {
        Some(milliseconds) if !(1..=MAX_RPC_TIMEOUT_MS).contains(&milliseconds) => {
            Err(protocol::ProtocolError::invalid(
                "$.rpc_timeout_ms",
                "rpc timeout must be between 1 and 60000 milliseconds",
            ))
        }
        Some(_) | None => Ok(()),
    }
}

/// Request to start one workflow execution with raw Temporal payloads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartWorkflowRequest {
    /// Caller-chosen identifier for this logical start operation.
    ///
    /// The bridge passes this value unchanged to Temporal's
    /// `StartWorkflowExecution.request_id` field.  It is deliberately part of
    /// the private protocol rather than generated for each transport attempt:
    /// a retry or reconciliation path must be able to refer to the same
    /// logical request without accidentally creating a second operation.
    pub request_id: String,
    /// Namespace in which Core should start the execution.
    pub namespace: String,
    /// User-supplied workflow identifier.
    pub workflow_id: String,
    /// Registered workflow type name.
    pub workflow_type: String,
    /// Normal task queue receiving the first workflow task.
    pub task_queue: String,
    /// Ordered input payloads; an empty vector means no workflow arguments.
    pub input: Vec<workflow_protocol::Payload>,
    /// Optional memo fields copied to Temporal's start request.
    #[serde(default)]
    pub memo: Vec<MetadataField>,
    /// Optional indexed search attributes copied to Temporal's start request.
    #[serde(default)]
    pub search_attributes: Vec<MetadataField>,
    /// Behaviour when the workflow ID already has an open run. The OCaml
    /// encoder always sends it; an omitted field means [`IdConflictPolicy::Fail`].
    /// It participates in request equality, so retrying a pending request ID
    /// with a different policy is rejected rather than aliased.
    #[serde(default)]
    pub id_conflict_policy: IdConflictPolicy,
    /// Behaviour when the workflow ID's most recent run is closed. The OCaml
    /// encoder always sends it; an omitted field means
    /// [`IdReusePolicy::AllowDuplicate`], Temporal's default.
    #[serde(default)]
    pub id_reuse_policy: IdReusePolicy,
    /// Workflow execution timeout in milliseconds, covering the whole chain
    /// of runs including retries and continue-as-new. Absent means unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_timeout_ms: Option<u64>,
    /// Workflow run timeout in milliseconds, covering one run. Absent means
    /// the execution timeout (or unlimited).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_timeout_ms: Option<u64>,
    /// Workflow task timeout in milliseconds. Absent means the server default
    /// of ten seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_timeout_ms: Option<u64>,
    /// Server-side retry policy for the workflow run. Absent means no retry,
    /// Temporal's default for workflows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<workflow_protocol::RetryPolicy>,
    /// Cron schedule of the workflow chain (#1158). Absent means no cron
    /// schedule. Temporal parses the expression and rejects an invalid one;
    /// the bridge only bounds it like every other identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron_schedule: Option<String>,
    /// Delay before Temporal dispatches the run's first workflow task, in
    /// milliseconds (#1158). Absent means no delay. It cannot be combined
    /// with [`StartWorkflowRequest::cron_schedule`], which Temporal rejects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_delay_ms: Option<u64>,
    /// Caller-selected total budget of this start RPC in milliseconds,
    /// replacing [`START_RPC_TIMEOUT`]. It bounds only the client call, never
    /// the workflow, so it is excluded from [`same_start_request`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// Successful result returned by the start operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartWorkflowResponse {
    /// Execution allocated by Temporal Server, or the existing open execution
    /// returned under [`IdConflictPolicy::UseExisting`].
    pub execution: ExecutionRef,
    /// Temporal's `StartWorkflowExecutionResponse.started`: `true` when this
    /// request created the run (including a deduplicated retry of the same
    /// request ID), `false` when `UseExisting` returned another start's run.
    pub started: bool,
}

/// Opaque ticket returned while an asynchronous start is still in flight.
///
/// The ticket is intentionally a closed JSON object rather than a raw UUID at
/// the FFI boundary.  That keeps the representation extensible while the
/// supervisor treats the value as an opaque capability and never constructs
/// or parses it itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartTicketDocument {
    /// Random, process-local identifier for the pending operation.
    ticket: String,
}

/// Result of one asynchronous start ticket.
///
/// The operation is deliberately represented as a successful JSON document
/// after a ticket becomes terminal.  `Rejected` means the server returned a
/// response that proves the start was not accepted.  `Unknown` is different:
/// the transport or response conversion failed after the request may have
/// reached Temporal, so the bridge never retries it or pretends it failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StartWorkflowOutcome {
    /// Temporal returned an execution reference for this logical start.
    Accepted(StartWorkflowResponse),
    /// A deterministic server/client error proves that no start was accepted.
    Rejected(ClientOperationError),
    /// The request outcome cannot be proven from the available response.
    Unknown {
        /// Stable request identifier supplied to Temporal.
        request_id: String,
        /// Workflow identity used for later reconciliation.
        workflow_id: String,
    },
}

/// Request to wait for one workflow run.
///
/// There is intentionally no `follow_runs` field.  This ABI operation has
/// fixed `follow_runs = false` semantics, so a continued-as-new event is a
/// terminal result for the observed run and cannot silently switch identity.
/// An empty `run_id` observes the run that is current when Temporal handles
/// the history poll; the OCaml client decides whether to follow successors.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitWorkflowRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run identifier to observe without following successors, or empty
    /// to observe whichever run is current when Temporal handles the poll.
    pub run_id: String,
}

/// Request to ask Temporal to cancel one exact workflow run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelWorkflowRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run identifier, or empty to select the workflow's current run.
    pub run_id: String,
    /// Caller-owned idempotency key for the cancellation RPC.
    pub request_id: String,
    /// Operator-facing reason copied to Temporal. Empty is permitted.
    pub reason: String,
    /// Caller-selected total budget of this RPC in milliseconds, replacing
    /// the operation's default budget. Absent keeps the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// Positive acknowledgement returned after Temporal accepts the request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelWorkflowResponse {
    /// Always true for a response emitted by this bridge.
    pub acknowledged: bool,
}

/// Request to terminate one exact workflow run immediately.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminateWorkflowRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run identifier, or empty to select the workflow's current run.
    pub run_id: String,
    /// Operator-facing reason copied to Temporal.
    pub reason: String,
    /// Caller-selected total budget of this RPC in milliseconds, replacing
    /// the operation's default budget. Absent keeps the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// Positive acknowledgement returned after Temporal accepts termination.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminateWorkflowResponse {
    /// Always true for a response emitted by this bridge.
    pub acknowledged: bool,
}

/// Request to reset one exact workflow run to a workflow-task boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResetWorkflowRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run whose workflow-task history supplies the reset point, or empty
    /// to reset the workflow's current run.
    pub run_id: String,
    /// Caller-owned idempotency key for this reset operation.
    pub request_id: String,
    /// Operator-facing reason copied to Temporal; empty is permitted.
    pub reason: String,
    /// Workflow task finish/start event at which the new run should begin.
    pub workflow_task_finish_event_id: i64,
    /// Caller-selected total budget of this RPC in milliseconds, replacing
    /// the operation's default budget. Absent keeps the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// New exact run identity returned by Temporal after a successful reset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResetWorkflowResponse {
    /// New exact execution identity returned by Temporal.
    pub execution: ExecutionRef,
}

/// Request to deliver one signal to one exact workflow run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalWorkflowRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run identifier, or empty to select the workflow's current run.
    pub run_id: String,
    /// Registered Temporal signal name.
    pub signal_name: String,
    /// Caller-owned idempotency key for this logical signal operation.
    pub request_id: String,
    /// Ordered signal input payloads.
    pub input: Vec<workflow_protocol::Payload>,
    /// Caller-selected total budget of this RPC in milliseconds, replacing
    /// the operation's default budget. Absent keeps the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// Positive acknowledgement returned after Temporal accepts a signal RPC.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalWorkflowResponse {
    /// Always true for a response emitted by this bridge.
    pub acknowledged: bool,
}

/// Request to execute one output-only query against one exact workflow run.
/// The input list is retained in the bridge protocol even though the public
/// first slice sends no query arguments, matching Temporal's normal
/// `WorkflowQuery.query_args` container and avoiding an ad-hoc wire shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryWorkflowRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run identifier, or empty to select the workflow's current run.
    pub run_id: String,
    /// Registered workflow query name.
    pub query_type: String,
    /// Ordered query argument payloads; currently required to be empty by the
    /// public OCaml API, but validated and forwarded losslessly if populated.
    pub input: Vec<workflow_protocol::Payload>,
    /// Caller-selected total budget of this RPC in milliseconds, replacing
    /// the operation's default budget. Absent keeps the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// Successful output-only query result returned by Temporal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryWorkflowResponse {
    /// Ordered result payloads from the workflow query handler.
    pub result: Vec<workflow_protocol::Payload>,
}

/// Request to start one named workflow update and wait until a worker accepts it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateWorkflowRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run identifier, or empty to select the workflow's current run.
    pub run_id: String,
    /// Caller-owned workflow-scoped update idempotency key.
    pub update_id: String,
    /// Registered workflow update handler name.
    pub update_name: String,
    /// Ordered update argument payloads.
    pub input: Vec<workflow_protocol::Payload>,
    /// Caller-selected total budget of this RPC in milliseconds, replacing
    /// the operation's default budget. Absent keeps the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// Request to poll a previously accepted workflow update until completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollWorkflowUpdateRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run identifier returned by the acceptance response, or empty to
    /// select the workflow's current run.
    pub run_id: String,
    /// Workflow-scoped update id returned by the acceptance response.
    pub update_id: String,
}

/// Terminal result of one workflow update.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UpdateOutcome {
    /// The handler completed successfully with ordered result payloads.
    Completed {
        result: Vec<workflow_protocol::Payload>,
    },
    /// The handler rejected or failed with a structured Temporal failure.
    Failed {
        failure: Box<workflow_protocol::Failure>,
    },
}

/// Response from update acceptance, optionally containing a fast completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateWorkflowResponse {
    /// Update id echoed by Temporal's update reference.
    pub update_id: String,
    /// Execution that accepted the update. When the request selected the
    /// current run, this is the concrete run Temporal resolved.
    pub execution: ExecutionRef,
    /// Present when the server completed the update before acceptance returned.
    pub outcome: Option<UpdateOutcome>,
}

/// Response from a completed-update poll.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollWorkflowUpdateResponse {
    /// Terminal outcome, or `None` when the bounded poll has no result yet.
    pub outcome: Option<UpdateOutcome>,
}

/// One visibility row reduced to stable fields useful to an SDK caller.
/// Temporal's visibility response contains many server/version-specific
/// fields; the bridge intentionally exposes only the durable identity and
/// routing/status fields that can be validated without copying arbitrary
/// memo/search-attribute payloads across the FFI boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityExecution {
    /// Durable workflow identifier.
    pub workflow_id: String,
    /// Concrete run identifier.
    pub run_id: String,
    /// Registered workflow type.
    pub workflow_type: String,
    /// Task queue assigned to the execution.
    pub task_queue: String,
    /// Closed vocabulary status name from Temporal visibility.
    pub status: String,
}

/// One bounded visibility page. The token is base64 for opaque protobuf bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityPage {
    /// Rows returned by the server in its ordering.
    pub executions: Vec<VisibilityExecution>,
    /// Opaque token for the next page, or null when exhausted.
    pub next_page_token: Option<String>,
}

/// Request for one visibility page. Pagination is explicit so callers never
/// accidentally issue an unbounded list operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityRequest {
    /// Namespace queried by Temporal.
    pub namespace: String,
    /// Temporal visibility query expression.
    pub query: String,
    /// Number of rows requested, bounded to protect the bridge.
    pub page_size: u32,
    /// Opaque base64 token returned by a previous page.
    pub next_page_token: Option<String>,
    /// Caller-selected total budget of this RPC in milliseconds, replacing
    /// the ten-second default. Absent keeps the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// Request for the complete recorded history of one run (#1137). Unlike
/// [`VisibilityRequest`], pagination is internal: the bridge follows every
/// continuation token and returns one binary `temporal.api.history.v1.History`
/// message, bounded by [`protocol::MAX_PAYLOAD_BYTES`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run identifier, or empty to select the workflow's current run, which
    /// Temporal resolves on the first page and pins through the token.
    pub run_id: String,
    /// Most events per page, between 1 and 1000; absent keeps the server's
    /// default. The server caps it at its `limit.historyMaxPageSize` (256 by
    /// default). Only the number of round trips depends on it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    /// Caller-selected budget of each page round trip in milliseconds,
    /// replacing the thirty-second default (#1210). Absent keeps the
    /// default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// Request to describe one run (#1152): exact, or with an empty `run_id` the
/// workflow's current run, which Temporal resolves when it handles the RPC.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Run identifier, or empty to select the workflow's current run.
    pub run_id: String,
    /// Caller-selected total budget of this RPC in milliseconds, replacing
    /// the ten-second default. Absent keeps the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// One activity the described run has scheduled and not yet resolved.
///
/// Only the identity, state and attempt cross the boundary; heartbeat
/// details, failures and timing stay in Rust, as for visibility rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingActivity {
    /// Workflow-scoped activity identifier.
    pub activity_id: String,
    /// Registered activity type name.
    pub activity_type: String,
    /// Closed state name from [`pending_activity_state`].
    pub state: String,
    /// Current attempt, starting at 1; 0 only if the server omitted it.
    pub attempt: u32,
}

/// One child workflow the described run has initiated and not yet seen close.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingChild {
    /// The child's workflow identifier.
    pub workflow_id: String,
    /// The child's run, or `None` while its start is still being initiated.
    pub run_id: Option<String>,
    /// The child's registered workflow type.
    pub workflow_type: String,
}

/// The reduced `DescribeWorkflowExecution` answer for one run (#1152).
///
/// It carries the run's identity as Temporal resolved it, its routing and
/// status fields, its start and close times, history length, the memo and
/// search attributes as named payloads sorted by key, and the pending
/// activities and children in server order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeResponse {
    /// The described run; `run_id` is always concrete.
    pub execution: ExecutionRef,
    /// Registered workflow type.
    pub workflow_type: String,
    /// Task queue the run was started on.
    pub task_queue: String,
    /// Status name, in the visibility vocabulary of [`visibility_status`].
    pub status: String,
    /// When the run started.
    pub start_time: workflow_protocol::Timestamp,
    /// When the run closed, or `None` while it is open.
    pub close_time: Option<workflow_protocol::Timestamp>,
    /// Number of events in the run's history.
    pub history_length: u64,
    /// Memo payloads, sorted by key.
    pub memo: Vec<MetadataField>,
    /// Search-attribute payloads as the server reports them, sorted by key.
    pub search_attributes: Vec<MetadataField>,
    /// Activities scheduled and not yet resolved.
    pub pending_activities: Vec<PendingActivity>,
    /// Child workflows initiated and not yet closed.
    pub pending_children: Vec<PendingChild>,
}

/// Request to count the executions a visibility query matches (#1152).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CountRequest {
    /// Namespace queried by Temporal.
    pub namespace: String,
    /// Temporal visibility query expression; empty counts every execution.
    pub query: String,
    /// Caller-selected total budget of this RPC in milliseconds, replacing
    /// the ten-second default. Absent keeps the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
}

/// The number of executions a visibility query matched. For a `GROUP BY`
/// query it is the total over every group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CountResponse {
    /// Matching executions.
    pub count: u64,
}

/// Metadata for a successor run created by continued-as-new.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessorRef {
    /// Namespace inherited by the successor run.
    pub namespace: String,
    /// Workflow identifier inherited by the successor run.
    pub workflow_id: String,
    /// New concrete run identifier.
    pub run_id: String,
}

/// Terminal outcome of one exact workflow run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowOutcome {
    /// The run completed and returned zero or more payloads.
    Completed {
        /// Result payloads in the order returned by Temporal.
        result: Vec<workflow_protocol::Payload>,
        /// Successor metadata, if the completion event links to a successor.
        successor: Option<SuccessorRef>,
    },
    /// The run failed with a structured Temporal failure.
    Failed {
        /// Failure tree supplied by Temporal.
        ///
        /// A failure tree is recursive and materially larger than the other
        /// terminal outcome variants.  Keeping it behind one allocation
        /// keeps the outcome enum compact while preserving the exact JSON
        /// object shape and ownership of the tree.
        failure: Box<workflow_protocol::Failure>,
        /// Successor metadata, if the failed event links to a successor.
        successor: Option<SuccessorRef>,
    },
    /// The run was cancelled by a request or workflow code.
    Cancelled {
        /// Cancellation details supplied by the workflow.
        details: Vec<workflow_protocol::Payload>,
    },
    /// The run was terminated by an operator or policy.
    Terminated {
        /// Termination details supplied by the operator.
        details: Vec<workflow_protocol::Payload>,
    },
    /// The run timed out.
    TimedOut {
        /// Successor metadata, if the timeout event links to a successor.
        successor: Option<SuccessorRef>,
    },
    /// The run continued as new and linked to the successor shown here.
    ContinuedAsNew {
        /// The next run in the execution chain.
        successor: SuccessorRef,
    },
}

/// Response containing the waited run selector and its terminal outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitWorkflowResponse {
    /// The request's execution, echoed unchanged. Its run ID is empty when
    /// the wait selected the current run, because Temporal's history
    /// response does not name the run it resolved.
    pub execution: ExecutionRef,
    /// Terminal outcome of that run; no successor is followed automatically.
    pub outcome: WorkflowOutcome,
}

/// Structured native client operation failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientOperationError {
    /// Temporal rejected the requested workflow ID as already running or
    /// closed according to its conflict/reuse policy.
    AlreadyStarted {
        /// ID supplied in the start request.
        workflow_id: String,
        /// Existing run ID when Temporal supplied one in status details.
        existing_run_id: Option<String>,
    },
    /// A gRPC call failed.  Only the stable status code crosses the bridge;
    /// server text may contain user payloads and is intentionally discarded.
    Rpc {
        /// Lowercase tonic status code name.
        code: String,
    },
    /// The workflow's query handler failed, or the worker reported the query
    /// name as unknown. Temporal answers such a query with `InvalidArgument`
    /// plus a `QueryFailedFailure` status detail; only that detail
    /// distinguishes it from a malformed request. Unlike other RPC failures,
    /// the handler's message is kept, because it is the application's answer
    /// to its own caller rather than server prose; it is bounded by
    /// [`bounded_query_failure_message`].
    QueryFailed {
        /// Bounded, NUL-free handler failure message; may be empty.
        message: String,
    },
    /// Core returned an event or payload outside this closed semantic slice.
    Core(workflow_protocol::CoreConversionError),
}

/// Largest query handler failure message, in UTF-8 bytes, that crosses the
/// bridge. A handler controls its own message, so the bound keeps one failed
/// query from producing an arbitrarily large error buffer; a longer message is
/// truncated at a character boundary. OCaml enforces the same limit.
pub(crate) const MAX_QUERY_FAILURE_MESSAGE_BYTES: usize = 4_096;

/// Closed JSON body used when a start call reports `AlreadyStarted`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ClientErrorDocument {
    /// Existing workflow identity returned by Temporal status details.
    AlreadyStarted {
        /// Workflow ID that could not be started.
        workflow_id: String,
        /// Existing run ID, when status details contained one.
        existing_run_id: Option<String>,
    },
    /// Stable gRPC status code for non-AlreadyStarted failures.
    Rpc {
        /// Lowercase tonic status code name.
        code: String,
    },
    /// Query handler failure with its bounded message; see
    /// [`ClientOperationError::QueryFailed`].
    QueryFailed {
        /// Bounded, NUL-free handler failure message.
        message: String,
    },
    /// Stable category for a Core event that cannot cross the semantic bridge.
    Protocol {
        /// Closed Core conversion category.
        code: String,
    },
}

/// Wire representation of one terminal asynchronous-start outcome.
///
/// This is kept separate from [`StartWorkflowOutcome`] so the latter can carry
/// the rich Rust error enum while the JSON boundary remains closed and safe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StartWorkflowOutcomeDocument {
    /// The server allocated a concrete run or returned an existing one.
    Accepted {
        /// Exact execution returned by Temporal.
        execution: ExecutionRef,
        /// Whether this request created the run; see [`StartWorkflowResponse`].
        started: bool,
    },
    /// The start was rejected with a structured, privacy-safe error body.
    Rejected {
        /// Stable error category and code.
        error: ClientErrorDocument,
    },
    /// The request may have reached Temporal, but no acceptance is proven.
    Unknown {
        /// Stable logical request identifier supplied by the caller.
        request_id: String,
        /// Workflow ID useful to a later reconciliation operation.
        workflow_id: String,
    },
}

/// Converts an internal client error into the closed JSON document used by
/// both synchronous failures and asynchronous rejected outcomes.
fn error_document(error: &ClientOperationError) -> ClientErrorDocument {
    match error {
        ClientOperationError::AlreadyStarted {
            workflow_id,
            existing_run_id,
        } => ClientErrorDocument::AlreadyStarted {
            workflow_id: workflow_id.clone(),
            existing_run_id: existing_run_id.clone(),
        },
        ClientOperationError::Rpc { code } => ClientErrorDocument::Rpc { code: code.clone() },
        ClientOperationError::QueryFailed { message } => ClientErrorDocument::QueryFailed {
            message: message.clone(),
        },
        ClientOperationError::Core(conversion) => ClientErrorDocument::Protocol {
            code: match conversion.code {
                workflow_protocol::CoreConversionErrorCode::Unsupported => {
                    "core_unsupported".to_owned()
                }
                workflow_protocol::CoreConversionErrorCode::InvalidCore => {
                    "core_invalid".to_owned()
                }
            },
        },
    }
}

/// Validates the closed object used by an asynchronous-start ticket result.
fn validate_start_outcome_document(
    document: &StartWorkflowOutcomeDocument,
) -> Result<(), protocol::ProtocolError> {
    match document {
        StartWorkflowOutcomeDocument::Accepted { execution, .. } => {
            validate_execution(execution, "$.execution")?
        }
        // A query handler failure can never reject a workflow start.
        StartWorkflowOutcomeDocument::Rejected { error } => validate_error_document(
            error,
            "$.error",
            true,
            "query_failed is not a valid start rejection",
        )?,
        StartWorkflowOutcomeDocument::Unknown {
            request_id,
            workflow_id,
        } => validate_unknown_start(request_id, workflow_id)?,
    }
    Ok(())
}

/// Validates the reconciliation identity of an uncertain start outcome
/// (`$.request_id`, then `$.workflow_id`), shared by plain starts and
/// update-with-start.
fn validate_unknown_start(
    request_id: &str,
    workflow_id: &str,
) -> Result<(), protocol::ProtocolError> {
    validate_identifier_fields(
        "$",
        [("request_id", request_id), ("workflow_id", workflow_id)],
    )
}

/// Validates one error document at `path`. A query failure is never valid
/// in an outcome document, and an already-started failure only where
/// `already_started` allows it (a start rejection); either refusal is
/// reported at `{path}.kind` with `invalid_kind`.
fn validate_error_document(
    error: &ClientErrorDocument,
    path: &str,
    already_started: bool,
    invalid_kind: &'static str,
) -> Result<(), protocol::ProtocolError> {
    match error {
        ClientErrorDocument::AlreadyStarted {
            workflow_id,
            existing_run_id,
        } if already_started => {
            validate_identifier(workflow_id, &format!("{path}.workflow_id"))?;
            if let Some(run_id) = existing_run_id {
                validate_identifier(run_id, &format!("{path}.existing_run_id"))?;
            }
            Ok(())
        }
        ClientErrorDocument::Rpc { code } | ClientErrorDocument::Protocol { code } => {
            validate_identifier(code, &format!("{path}.code"))
        }
        ClientErrorDocument::AlreadyStarted { .. } | ClientErrorDocument::QueryFailed { .. } => {
            Err(protocol::ProtocolError::invalid(
                format!("{path}.kind"),
                invalid_kind,
            ))
        }
    }
}

impl ClientOperationError {
    /// Encodes a privacy-safe structured error body for the ABI error buffer.
    pub(crate) fn to_json(&self) -> String {
        let document = error_document(self);
        // All variants contain validated identifiers, a bounded tonic code,
        // or a bounded query failure message; serialization cannot fail.  Keep
        // a defensive fallback that cannot expose a Rust panic through the ABI
        // if a future variant changes.
        serde_json::to_string(&document)
            .unwrap_or_else(|_| "{\"kind\":\"rpc\",\"code\":\"internal\"}".to_owned())
    }

    /// Reports whether a failed start has an outcome that cannot be proven.
    ///
    /// Transport failures such as `Unavailable` may occur after Temporal has
    /// accepted the request, so callers must reconcile them rather than retry
    /// with a new logical request.  Validation and authorization failures are
    /// deterministic rejections; `AlreadyStarted` is also terminal because
    /// the server supplied the conflicting workflow identity.
    pub(crate) fn uncertain_start(&self) -> bool {
        match self {
            // `QueryFailed` is only produced by the query operation; it is
            // listed so the match stays exhaustive and terminal.
            Self::AlreadyStarted { .. } | Self::QueryFailed { .. } => false,
            Self::Core(_) => true,
            Self::Rpc { code } => matches!(
                code.as_str(),
                "cancelled"
                    | "unknown"
                    | "deadline_exceeded"
                    | "resource_exhausted"
                    | "aborted"
                    | "internal"
                    | "unavailable"
                    | "data_loss"
            ),
        }
    }
}

/// Strictly parses one client request document.
///
/// `parse` selects the strict structural parser: [`protocol::decode_object`]
/// for ordinary requests, or [`protocol::decode_payload_object`] when base64
/// payload data may use the larger per-field allowance. It rejects duplicate
/// members and enforces byte limits before Serde sees the input. Serde's
/// diagnostic is replaced by `invalid` so input-controlled member names are not
/// reflected, and `validate` then enforces the request's semantic bounds.
fn decode_request<T: serde::de::DeserializeOwned>(
    input: &str,
    parse: fn(&str) -> Result<protocol::JsonValue, protocol::ProtocolError>,
    invalid: &'static str,
    validate: fn(&T) -> Result<(), protocol::ProtocolError>,
) -> Result<T, protocol::ProtocolError> {
    parse(input)?;
    let request =
        serde_json::from_str(input).map_err(|_| protocol::ProtocolError::invalid("$", invalid))?;
    validate(&request)?;
    Ok(request)
}

/// Defines the public strict decoders of client request documents.
///
/// Each entry is written with the exact signature it produces and names the
/// three parts [`decode_request`] combines: the structural parser (`parse`),
/// the Serde diagnostic (`invalid`), and the semantic validator (`validate`).
/// The parser choice is deliberately spelled out per entry: requests that
/// carry payloads use [`protocol::decode_payload_object`], every other
/// request the ordinary [`protocol::decode_object`]. The entry's own doc
/// comment is emitted on the generated function.
macro_rules! request_decoders {
    ($(
        $(#[$meta:meta])*
        $vis:vis fn $name:ident(input: &str $(,)?) -> Result<$request:ty, protocol::ProtocolError> {
            parse: $parse:path,
            invalid: $invalid:literal,
            validate: $validate:path $(,)?
        }
    )*) => {$(
        $(#[$meta])*
        $vis fn $name(input: &str) -> Result<$request, protocol::ProtocolError> {
            decode_request(input, $parse, $invalid, $validate)
        }
    )*};
}
// A path import, so that the `with_start` child module (declared above this
// definition) reaches the macro through its `use super::*`.
use request_decoders;

/// Defines the public encoders of client response documents.
///
/// Each entry is written with the exact signature it produces and lists the
/// `check` expressions that must succeed, in order, before the response is
/// rendered by [`encode_document`], which strictly reparses it before
/// ownership leaves Rust. An entry with no checks encodes a response whose
/// fields carry no semantic bounds beyond its closed Serde shape. The entry's
/// own doc comment is emitted on the generated function.
macro_rules! response_encoders {
    ($(
        $(#[$meta:meta])*
        $vis:vis fn $name:ident(
            $response:ident: &$document:ty $(,)?
        ) -> Result<String, protocol::ProtocolError> {
            $(check: $check:expr),* $(,)?
        }
    )*) => {$(
        $(#[$meta])*
        $vis fn $name($response: &$document) -> Result<String, protocol::ProtocolError> {
            $($check?;)*
            encode_document($response)
        }
    )*};
}

request_decoders! {
    /// Strictly parses one start request after duplicate-key and byte-limit checks.
    pub fn decode_start_request(
        input: &str,
    ) -> Result<StartWorkflowRequest, protocol::ProtocolError> {
        parse: protocol::decode_payload_object,
        invalid: "invalid client start request",
        validate: validate_start_request,
    }

    /// Strictly parses one exact-run wait request.
    pub fn decode_wait_request(
        input: &str,
    ) -> Result<WaitWorkflowRequest, protocol::ProtocolError> {
        parse: protocol::decode_object,
        invalid: "invalid client wait request",
        validate: validate_wait_request,
    }

    /// Strictly parses one exact-run cancellation request.
    pub fn decode_cancel_request(
        input: &str,
    ) -> Result<CancelWorkflowRequest, protocol::ProtocolError> {
        parse: protocol::decode_object,
        invalid: "invalid client cancel request",
        validate: validate_cancel_request,
    }

    /// Strictly parses one exact-run termination request.
    pub fn decode_terminate_request(
        input: &str,
    ) -> Result<TerminateWorkflowRequest, protocol::ProtocolError> {
        parse: protocol::decode_object,
        invalid: "invalid client terminate request",
        validate: validate_terminate_request,
    }

    /// Strictly parses one exact-run reset request.
    pub fn decode_reset_request(
        input: &str,
    ) -> Result<ResetWorkflowRequest, protocol::ProtocolError> {
        parse: protocol::decode_object,
        invalid: "invalid client reset request",
        validate: validate_reset_request,
    }

    /// Strictly parses one exact-run signal request.
    ///
    /// Like [`decode_start_request`], this uses the payload-aware parser: base64
    /// payload data may use the 128 MiB per-field allowance, while
    /// [`validate_signal_request`] immediately reapplies the 65,536-byte text
    /// limit to every identifier and payload metadata key. Using the generic
    /// object parser here would cap signal input at 49,152 raw bytes (issue #771).
    pub fn decode_signal_request(
        input: &str,
    ) -> Result<SignalWorkflowRequest, protocol::ProtocolError> {
        parse: protocol::decode_payload_object,
        invalid: "invalid client signal request",
        validate: validate_signal_request,
    }

    /// Strictly parses one exact-run query request.
    ///
    /// Query arguments are payloads, so the payload-aware parser applies the same
    /// per-field and whole-document bounds as start and signal input; the semantic
    /// validator then bounds identifiers and metadata keys as ordinary text.
    pub fn decode_query_request(
        input: &str,
    ) -> Result<QueryWorkflowRequest, protocol::ProtocolError> {
        parse: protocol::decode_payload_object,
        invalid: "invalid client query request",
        validate: validate_query_request,
    }

    /// Strictly parses a request that starts one workflow update.
    ///
    /// Update arguments use the payload-aware parser for the same reason as
    /// signal input; [`validate_update_request`] keeps every identifier and
    /// metadata key within the ordinary text limit.
    pub fn decode_update_request(
        input: &str,
    ) -> Result<UpdateWorkflowRequest, protocol::ProtocolError> {
        parse: protocol::decode_payload_object,
        invalid: "invalid client update request",
        validate: validate_update_request,
    }

    /// Strictly parses a request that polls a previously admitted update.
    pub fn decode_poll_update_request(
        input: &str,
    ) -> Result<PollWorkflowUpdateRequest, protocol::ProtocolError> {
        parse: protocol::decode_object,
        invalid: "invalid client update poll request",
        validate: validate_poll_update_request,
    }

    /// Strictly parses one bounded visibility page request.
    pub fn decode_visibility_request(
        input: &str,
    ) -> Result<VisibilityRequest, protocol::ProtocolError> {
        parse: protocol::decode_object,
        invalid: "invalid visibility request",
        validate: validate_visibility_request,
    }

    /// Strictly parses one complete-history fetch request.
    pub fn decode_history_request(input: &str) -> Result<HistoryRequest, protocol::ProtocolError> {
        parse: protocol::decode_object,
        invalid: "invalid history request",
        validate: validate_history_request,
    }

    /// Strictly parses one describe request (#1152).
    pub fn decode_describe_request(
        input: &str,
    ) -> Result<DescribeRequest, protocol::ProtocolError> {
        parse: protocol::decode_object,
        invalid: "invalid describe request",
        validate: validate_describe_request,
    }

    /// Strictly parses one visibility count request (#1152).
    pub fn decode_count_request(input: &str) -> Result<CountRequest, protocol::ProtocolError> {
        parse: protocol::decode_object,
        invalid: "invalid count request",
        validate: validate_count_request,
    }
}

/// Compares two validated start requests using every logical field, including
/// workflow type, task queue, binary payloads, and workflow policies.  The ABI
/// uses this when a caller retries a pending request ID: only an exact
/// semantic retry may reuse the existing ticket, while a changed request is
/// rejected as a protocol error instead of being silently aliased to another
/// start. The caller's RPC timeout is ignored: it bounds one client call, not
/// the workflow, so a retry may legitimately wait longer or shorter.
pub(crate) fn same_start_request(
    left: &StartWorkflowRequest,
    right: &StartWorkflowRequest,
) -> bool {
    StartWorkflowRequest {
        rpc_timeout_ms: None,
        ..left.clone()
    } == StartWorkflowRequest {
        rpc_timeout_ms: None,
        ..right.clone()
    }
}

/// Decodes one opaque start ticket returned by the asynchronous begin call.
pub(crate) fn decode_start_ticket(input: &str) -> Result<String, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let document: StartTicketDocument = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid start ticket"))?;
    validate_identifier(&document.ticket, "$.ticket")?;
    Ok(document.ticket)
}

/// Encodes one opaque start ticket and strictly reparses the result before it
/// leaves Rust.  The private ticket value is never exposed as an OCaml string
/// until the normal native result decoder copies the response buffer.
pub(crate) fn encode_start_ticket(ticket: &str) -> Result<String, protocol::ProtocolError> {
    validate_identifier(ticket, "$.ticket")?;
    encode_document(&StartTicketDocument {
        ticket: ticket.to_owned(),
    })
}

response_encoders! {
    /// Encodes and reparses a start response before ownership leaves Rust.
    pub fn encode_start_response(
        response: &StartWorkflowResponse,
    ) -> Result<String, protocol::ProtocolError> {
        check: validate_execution(&response.execution, "$.execution"),
    }

    /// Encodes and reparses an exact-run wait response before ownership leaves Rust.
    pub fn encode_wait_response(
        response: &WaitWorkflowResponse,
    ) -> Result<String, protocol::ProtocolError> {
        check: validate_wait_response(response),
    }

    /// Encodes and reparses a positive cancellation acknowledgement before it
    /// crosses the native boundary.
    pub fn encode_cancel_response(
        response: &CancelWorkflowResponse,
    ) -> Result<String, protocol::ProtocolError> {
        // Only a successful RPC produces an acknowledgement, so `false` would
        // misreport an operation that never happened.
        check: require(
            response.acknowledged,
            "$.acknowledged",
            "cancellation acknowledgement must be true",
        ),
    }

    /// Encodes and reparses a positive termination acknowledgement.
    pub fn encode_terminate_response(
        response: &TerminateWorkflowResponse,
    ) -> Result<String, protocol::ProtocolError> {
        check: require(
            response.acknowledged,
            "$.acknowledged",
            "termination acknowledgement must be true",
        ),
    }

    /// Encodes and reparses the new execution identity returned after reset.
    pub fn encode_reset_response(
        response: &ResetWorkflowResponse,
    ) -> Result<String, protocol::ProtocolError> {
        check: validate_execution(&response.execution, "$.execution"),
    }

    /// Encodes and reparses a positive signal acknowledgement before it crosses
    /// the native boundary.
    pub fn encode_signal_response(
        response: &SignalWorkflowResponse,
    ) -> Result<String, protocol::ProtocolError> {
        check: require(
            response.acknowledged,
            "$.acknowledged",
            "signal acknowledgement must be true",
        ),
    }

    /// Encodes and reparses one query result before ownership leaves Rust.
    pub fn encode_query_response(
        response: &QueryWorkflowResponse,
    ) -> Result<String, protocol::ProtocolError> {
        check: validate_payloads(&response.result, "$.result"),
    }

    /// Encodes and reparses update admission before ownership leaves Rust.
    pub fn encode_update_response(
        response: &UpdateWorkflowResponse,
    ) -> Result<String, protocol::ProtocolError> {
        check: validate_update_response(response),
    }

    /// Encodes and reparses a bounded update poll response before ownership leaves
    /// Rust. A missing outcome is a normal pending result, not an error.
    pub fn encode_poll_update_response(
        response: &PollWorkflowUpdateResponse,
    ) -> Result<String, protocol::ProtocolError> {
        check: validate_update_outcome(response.outcome.as_ref()),
    }

    /// Encodes and reparses one visibility page before it crosses the ABI.
    pub fn encode_visibility_response(
        response: &VisibilityPage,
    ) -> Result<String, protocol::ProtocolError> {
        check: validate_visibility_page(response),
    }

    /// Validates, encodes and reparses one describe answer before it crosses the
    /// ABI.
    pub fn encode_describe_response(
        response: &DescribeResponse,
    ) -> Result<String, protocol::ProtocolError> {
        check: validate_describe_response(response),
    }

    /// Encodes and reparses one count answer before it crosses the ABI.
    pub fn encode_count_response(
        response: &CountResponse,
    ) -> Result<String, protocol::ProtocolError> {}
}

/// Maximum transport attempts, including the first, that one exact-run history
/// long poll may make while Core retries transient failures (#820).
///
/// With Core's default backoff (100 ms growing by 1.7x to a 5 s cap, +/-20%
/// jitter) thirty attempts span roughly two minutes, which covers a routine
/// Temporal Server restart. The count restarts with every long poll the
/// server answers, so it bounds one continuous outage rather than the life of
/// the wait.
const WAIT_MAX_ATTEMPTS: usize = 30;

/// gRPC metadata key through which a client lists the optional Temporal
/// protocol features it understands (Temporal Server `headers` package).
pub const SUPPORTED_FEATURES_HEADER: &str = "supported-features";

/// Temporal feature name declaring that the client follows the
/// `new_execution_run_id` of a completed, failed, or timed-out close event.
///
/// Without it Temporal Server rewrites such a close event, when the history
/// request filters for the close event only, into a synthetic
/// `WorkflowExecutionContinuedAsNew` event for compatibility with SDKs from
/// before 2021 (#971). The rewrite would make a run that a retry policy or
/// cron schedule chained to a successor indistinguishable from an explicit
/// continue-as-new and would discard the run's failure. `wait` already maps
/// every successor-bearing close event, so it always advertises the feature.
pub const FOLLOWS_NEXT_RUN_ID_FEATURE: &str = "follows-next-run-id";

/// Core retry policy for the exact-run history long poll behind `wait`.
///
/// Core measures its elapsed-time limit from the start of the call, and a
/// healthy long poll can legitimately stay open for most of a minute before
/// the server fails. Under Core's default ten-second limit such a poll would
/// therefore not be retried at all when the server goes away mid-poll, which
/// is exactly the outage `wait` must survive. The wait instead disables the
/// elapsed-time limit and bounds the outage by [`WAIT_MAX_ATTEMPTS`]. Only
/// Core's transient status codes are retried; a definitive rejection such as
/// `not_found` still ends the wait on its first occurrence.
fn wait_retry_options() -> RetryOptions {
    RetryOptions::builder()
        .max_elapsed_time(None)
        .max_retries(WAIT_MAX_ATTEMPTS)
        .build()
}

/// Total budget of one control-plane RPC (signal, cancel, reset, terminate),
/// including Core's retries (#820).
///
/// These acknowledgements run synchronously on the supervisor's owner Domain,
/// so the budget keeps a stalled or unavailable server from holding it. It is
/// three seconds rather than one so that the `resource_exhausted` retry Core
/// performs actually happens: Core waits its throttle backoff of 1 s +/-20%
/// (at most 1.2 s) before the second attempt, which a one-second budget
/// usually cut off with `deadline_exceeded`. Three seconds leaves room for
/// that wait, the re-sent attempt, and several fast `unavailable` retries; a
/// second consecutive `resource_exhausted` (a further 2 s +/-20% wait) does
/// not fit and ends in the caller's deadline error.
const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(3);

/// Wraps one bounded client RPC message for Core's retrying `Connection`.
///
/// Every client RPC except the history long poll has a fixed total budget that
/// keeps the supervisor's synchronous turn finite. Core's default retry window
/// (ten seconds) is unrelated to those budgets, so it is replaced by Core's
/// default backoff limited to `budget`: Core stops scheduling retries once the
/// next backoff would end after the budget and returns the last transport
/// status (for example `unavailable`) instead of letting the caller's outer
/// timeout discard it. The same budget is the gRPC deadline of each transport
/// attempt, so the server also abandons work the client will no longer wait
/// for. Callers keep their outer `tokio::time::timeout`, which still caps the
/// call when the budget expires while an attempt is in flight or while Core is
/// sleeping a `resource_exhausted` throttle delay; that cap yields the
/// caller's deadline error instead of a transport status.
///
/// The throttle delay is not covered by the retry window: the pinned Core
/// accepts a `resource_exhausted` retry using its ordinary backoff (about
/// 100 ms at first) and only then lengthens the wait to its separate throttle
/// backoff (1 s, then 2 s, 4 s, ... up to 10 s, each +/-20%) without
/// rechecking the window. [`CONTROL_RPC_TIMEOUT`] is sized for that.
///
/// Which status codes are retried is Core's decision (`unavailable`,
/// `resource_exhausted`, `unknown`, `internal`, `aborted`, `out_of_range`,
/// `data_loss`, and transport-level cancellation); every other status is
/// returned on its first occurrence. Each retry re-sends the identical
/// message, so the server's idempotency key (`request_id` or update ID)
/// deduplicates a retried mutation whose first attempt was applied.
fn budgeted_request<T>(message: T, budget: Duration) -> Request<T> {
    let mut request = message.into_request();
    request.set_timeout(budget);
    request.extensions_mut().insert(RetryConfigForCall(
        RetryOptions::builder()
            .max_elapsed_time(Some(budget))
            .build(),
    ));
    request
}

/// Wraps a status mapper for one budgeted RPC so that an expired budget is
/// always reported as the typed `deadline_exceeded` error (#499).
///
/// Each attempt carries the remaining budget as its gRPC deadline, and the
/// transport reports its own expiry of that deadline as `cancelled` (or the
/// server reports `deadline_exceeded`) when it wins the race with the outer
/// timeout. Once `budget` has elapsed since `started`, either status means
/// the caller's deadline expired, so it is reported as `deadline_exceeded`
/// regardless of which timer fired first. Every other status, and these
/// statuses before the budget has elapsed, use `map` unchanged.
fn budget_status<F>(
    started: tokio::time::Instant,
    budget: Duration,
    map: F,
) -> impl FnOnce(Status) -> ClientOperationError
where
    F: FnOnce(Status) -> ClientOperationError,
{
    move |status| {
        if matches!(status.code(), Code::Cancelled | Code::DeadlineExceeded)
            && started.elapsed() >= budget
        {
            deadline_exceeded()
        } else {
            map(status)
        }
    }
}

/// The typed error every budgeted client operation reports once its budget
/// has expired, whichever timer observed the expiry (#499).
fn deadline_exceeded() -> ClientOperationError {
    ClientOperationError::Rpc {
        code: "deadline_exceeded".to_owned(),
    }
}

/// Awaits one budgeted workflow-service call and maps both of its failure
/// modes the way every budgeted client operation reports them.
///
/// `call` is the already-constructed RPC future (its request should carry the
/// same `budget` through [`budgeted_request`]), and `started` is the instant
/// the operation's budget began. The outer `budget` timeout is the hard bound
/// on the owner's wait: when it fires the operation is the typed
/// `deadline_exceeded` RPC error. A returned status is mapped by `map`
/// through [`budget_status`], so a transport or server deadline that expires
/// together with the budget is reported the same way (#499). On success the
/// response message is returned without its gRPC metadata.
async fn budgeted_rpc<T, M>(
    budget: Duration,
    started: tokio::time::Instant,
    call: impl std::future::Future<Output = Result<Response<T>, Status>>,
    map: M,
) -> Result<T, ClientOperationError>
where
    M: FnOnce(Status) -> ClientOperationError,
{
    match tokio::time::timeout(budget, call).await {
        Ok(result) => Ok(result
            .map_err(budget_status(started, budget, map))?
            .into_inner()),
        Err(_) => Err(deadline_exceeded()),
    }
}

/// The clock of one budgeted client RPC: its total budget and the instant
/// that budget began.
///
/// Keeping the two together means the request deadline and Core retry window
/// ([`budgeted_request`]) and the outer timeout and status mapping
/// ([`budgeted_rpc`]) of one call are always measured from the same budget
/// and start instant.
#[derive(Clone, Copy)]
struct RpcClock {
    /// Total budget of the call.
    budget: Duration,
    /// Instant at which the budget began.
    started: tokio::time::Instant,
}

impl RpcClock {
    /// Starts, now, the caller's `rpc_timeout_ms` budget or `default` (see
    /// [`rpc_budget`]).
    fn start(rpc_timeout_ms: Option<u64>, default: Duration) -> Self {
        Self::starting_now(rpc_budget(rpc_timeout_ms, default))
    }

    /// Starts an already selected `budget` now, as each history page does.
    fn starting_now(budget: Duration) -> Self {
        Self {
            budget,
            started: tokio::time::Instant::now(),
        }
    }

    /// Wraps `message` for Core's retrying connection with this budget.
    fn request<T>(self, message: T) -> Request<T> {
        budgeted_request(message, self.budget)
    }

    /// Awaits `call`, built from [`Self::request`], within this budget.
    async fn call<T, M>(
        self,
        call: impl std::future::Future<Output = Result<Response<T>, Status>>,
        map: M,
    ) -> Result<T, ClientOperationError>
    where
        M: FnOnce(Status) -> ClientOperationError,
    {
        budgeted_rpc(self.budget, self.started, call, map).await
    }
}

/// Reports whether a failed `TerminateWorkflowExecution` attempt must be
/// returned instead of retried.
///
/// Terminate has no idempotency key, so Core may re-send it only after a
/// status proving that the server did not apply the first attempt:
/// `resource_exhausted`, which Temporal's rate limiters and admission checks
/// return before the command is processed. Every other status is returned at
/// once. In particular `unavailable` is not retried: besides a refused
/// connection it is also what the transport reports when the connection
/// drops after the server applied the termination but before the
/// acknowledgement arrived, and a blind re-send of that applied termination
/// would report a misleading `not_found` for the already terminated run.
/// [`terminate_status_error`] reports such an ambiguous status as
/// `termination_outcome_uncertain`.
fn terminate_retry_forbidden(status: &Status) -> bool {
    status.code() != Code::ResourceExhausted
}

/// Converts a terminate failure into the typed bridge error.
///
/// `unavailable`, `deadline_exceeded`, and `cancelled` can be reported after
/// the server applied the termination (a dropped connection, or the
/// per-attempt gRPC deadline expiring while the acknowledgement was in
/// flight), and the RPC has no idempotency key that would make a re-send
/// equivalent. They therefore become `termination_outcome_uncertain`, the
/// same result as the outer deadline, which callers reconcile with `wait`.
/// Every other status keeps its stable gRPC code.
fn terminate_status_error(status: Status) -> ClientOperationError {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => ClientOperationError::Rpc {
            code: "termination_outcome_uncertain".to_owned(),
        },
        _ => map_rpc_status(status),
    }
}

/// Builds a budgeted terminate request (see [`budgeted_request`]) whose Core
/// retries are restricted by [`terminate_retry_forbidden`].
fn terminate_request(
    message: TerminateWorkflowExecutionRequest,
    budget: Duration,
) -> Request<TerminateWorkflowExecutionRequest> {
    let mut request = budgeted_request(message, budget);
    request.extensions_mut().insert(NoRetryOnMatching {
        predicate: terminate_retry_forbidden,
    });
    request
}

/// Lists one visibility page through Temporal's official workflow service.
/// The opaque page token is copied as bytes only inside Rust and is base64 at
/// the JSON boundary; no server-owned protobuf or byte buffer is retained.
pub async fn list_visibility(
    connection: Connection,
    request: VisibilityRequest,
) -> Result<VisibilityPage, ClientOperationError> {
    const VISIBILITY_RPC_TIMEOUT: Duration = Duration::from_secs(10);
    let clock = RpcClock::start(request.rpc_timeout_ms, VISIBILITY_RPC_TIMEOUT);
    let token = request
        .next_page_token
        .as_deref()
        .map(|value| {
            base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|_| {
                    ClientOperationError::Core(workflow_protocol::invalid_core(
                        "invalid visibility page token",
                    ))
                })
        })
        .transpose()?;
    let mut service = connection.clone();
    let response = clock
        .call(
            service.list_workflow_executions(clock.request(ListWorkflowExecutionsRequest {
                namespace: request.namespace,
                query: request.query,
                page_size: request.page_size as i32,
                next_page_token: token.unwrap_or_default(),
            })),
            map_rpc_status,
        )
        .await?;
    let mut executions = Vec::with_capacity(response.executions.len());
    for info in response.executions {
        let execution = info.execution.ok_or_else(|| {
            ClientOperationError::Core(workflow_protocol::invalid_core(
                "visibility row has no execution identity",
            ))
        })?;
        let workflow_type = info.r#type.ok_or_else(|| {
            ClientOperationError::Core(workflow_protocol::invalid_core(
                "visibility row has no workflow type",
            ))
        })?;
        executions.push(VisibilityExecution {
            workflow_id: execution.workflow_id,
            run_id: execution.run_id,
            workflow_type: workflow_type.name,
            task_queue: info.task_queue,
            status: visibility_status(info.status)?,
        });
    }
    Ok(VisibilityPage {
        executions,
        next_page_token: if response.next_page_token.is_empty() {
            None
        } else {
            Some(base64::engine::general_purpose::STANDARD.encode(response.next_page_token))
        },
    })
}

/// Fetches every event of one run's history through Temporal's official
/// workflow service and returns it as one binary
/// `temporal.api.history.v1.History` message (#1137).
///
/// Each page is re-encoded and appended; protobuf concatenation of the
/// repeated `events` field yields the complete history, the same form
/// `Temporal.Replay.History.of_protobuf` accepts. The fetch never waits for
/// new events, so an open run yields the events recorded so far.
///
/// Each page round trip has its own budget (`rpc_timeout_ms`, default
/// [`HISTORY_PAGE_RPC_TIMEOUT`]), as Core's own history paginator does
/// (#1210). The server serves at most a few hundred events per page, so a
/// history at the server's default limits needs hundreds of sequential round
/// trips, which no single bounded budget can cover on a high-latency link; a
/// retry of a timed-out whole fetch would also restart at page one. Expiry of
/// one page's budget is the typed `deadline_exceeded` RPC error. The total
/// fetch is still bounded by the history itself: a history larger than
/// [`protocol::MAX_PAYLOAD_BYTES`] or one with no events is rejected rather
/// than returned truncated, and client shutdown aborts the task.
pub async fn fetch_history(
    connection: Connection,
    request: HistoryRequest,
) -> Result<Vec<u8>, ClientOperationError> {
    use prost::Message;

    let budget = rpc_budget(request.rpc_timeout_ms, HISTORY_PAGE_RPC_TIMEOUT);
    let mut encoded = Vec::new();
    let mut next_page_token = Vec::new();
    let mut service = connection.clone();
    loop {
        let page = GetWorkflowExecutionHistoryRequest {
            namespace: request.namespace.clone(),
            execution: core_execution(request.workflow_id.clone(), request.run_id.clone()),
            // Validation bounds the size to 1..=1000, so it always fits; zero
            // asks for the server's default. The server caps it at its own
            // `limit.historyMaxPageSize` (256 by default).
            maximum_page_size: request
                .page_size
                .and_then(|size| i32::try_from(size).ok())
                .unwrap_or(0),
            next_page_token: std::mem::take(&mut next_page_token),
            wait_new_event: false,
            history_event_filter_type: i32::from(HistoryEventFilterType::AllEvent),
            skip_archival: false,
        };
        let clock = RpcClock::starting_now(budget);
        let response = clock
            .call(
                service.get_workflow_execution_history(clock.request(page)),
                map_rpc_status,
            )
            .await?;
        if let Some(history) = response.history
            && !history.events.is_empty()
        {
            let page = history.encode_to_vec();
            if encoded.len() + page.len() > protocol::MAX_PAYLOAD_BYTES {
                return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
                    "workflow history exceeds the bridge payload limit",
                )));
            }
            encoded.extend_from_slice(&page);
        }
        if response.next_page_token.is_empty() {
            break;
        }
        next_page_token = response.next_page_token;
    }
    if encoded.is_empty() {
        return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal returned a workflow history with no events",
        )));
    }
    Ok(encoded)
}

/// The default budget of one history page round trip when the caller gave
/// no `rpc_timeout_ms` (#1210).
const HISTORY_PAGE_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// The default total budget of one describe or count RPC when the caller gave
/// no `rpc_timeout_ms`; the same as one visibility page's.
const READ_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Describes one run through Temporal's official workflow service (#1152)
/// and reduces the answer to [`DescribeResponse`].
///
/// The RPC is read-only, so Core may retry it within the budget like any
/// other idempotent call. The answer must name the requested namespace and
/// workflow ID and, for an exact request, the requested run; anything else,
/// a missing execution record or start time, or an unspecified status is a
/// malformed response (`core_invalid`). Memo and search-attribute payloads
/// are converted with the same limits as every other inbound payload and
/// sorted by key, so the document does not depend on protobuf map order.
pub async fn describe_workflow(
    connection: Connection,
    request: DescribeRequest,
) -> Result<DescribeResponse, ClientOperationError> {
    let clock = RpcClock::start(request.rpc_timeout_ms, READ_RPC_TIMEOUT);
    let mut service = connection.clone();
    let response = clock
        .call(
            service.describe_workflow_execution(clock.request(DescribeWorkflowExecutionRequest {
                namespace: request.namespace.clone(),
                execution: core_execution(request.workflow_id.clone(), request.run_id.clone()),
            })),
            map_rpc_status,
        )
        .await?;
    let malformed = |message| ClientOperationError::Core(workflow_protocol::invalid_core(message));
    let info = response
        .workflow_execution_info
        .ok_or_else(|| malformed("describe response has no execution record"))?;
    let execution = info
        .execution
        .ok_or_else(|| malformed("described execution has no identity"))?;
    if execution.workflow_id != request.workflow_id
        || execution.run_id.is_empty()
        || (!request.run_id.is_empty() && execution.run_id != request.run_id)
    {
        return Err(malformed("Temporal described a different execution"));
    }
    let workflow_type = info
        .r#type
        .ok_or_else(|| malformed("described execution has no workflow type"))?
        .name;
    let start_time = info
        .start_time
        .as_ref()
        .map(described_time)
        .ok_or_else(|| malformed("described execution has no start time"))??;
    let close_time = info.close_time.as_ref().map(described_time).transpose()?;
    let history_length = u64::try_from(info.history_length)
        .map_err(|_| malformed("described history length is negative"))?;
    let memo = described_fields(info.memo.map(|memo| memo.fields))?;
    let search_attributes =
        described_fields(info.search_attributes.map(|search| search.indexed_fields))?;
    let pending_activities = response
        .pending_activities
        .into_iter()
        .map(|activity| PendingActivity {
            activity_id: activity.activity_id,
            activity_type: activity
                .activity_type
                .map(|activity_type| activity_type.name)
                .unwrap_or_default(),
            state: pending_activity_state(activity.state),
            attempt: u32::try_from(activity.attempt).unwrap_or(0),
        })
        .collect();
    let pending_children = response
        .pending_children
        .into_iter()
        .map(|child| PendingChild {
            workflow_id: child.workflow_id,
            run_id: (!child.run_id.is_empty()).then_some(child.run_id),
            workflow_type: child.workflow_type_name,
        })
        .collect();
    Ok(DescribeResponse {
        execution: ExecutionRef {
            namespace: request.namespace,
            workflow_id: execution.workflow_id,
            run_id: execution.run_id,
        },
        workflow_type,
        task_queue: info.task_queue,
        status: visibility_status(info.status)?,
        start_time,
        close_time,
        history_length,
        memo,
        search_attributes,
        pending_activities,
        pending_children,
    })
}

/// Copies one described timestamp exactly, rejecting a fraction outside the
/// protobuf range.
fn described_time(
    value: &prost_wkt_types::Timestamp,
) -> Result<workflow_protocol::Timestamp, ClientOperationError> {
    workflow_protocol::validate_time(value.seconds, value.nanos, false, "$").map_err(|_| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "described timestamp is invalid",
        ))
    })?;
    Ok(workflow_protocol::Timestamp {
        seconds: value.seconds,
        nanoseconds: value.nanos,
    })
}

/// Converts one described memo or search-attribute map into named payloads
/// sorted by key. An absent map is empty.
fn described_fields(
    fields: Option<HashMap<String, CorePayload>>,
) -> Result<Vec<MetadataField>, ClientOperationError> {
    let mut named = fields
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| {
            Ok(MetadataField {
                key,
                value: workflow_protocol::payload_from_core(&value)
                    .map_err(ClientOperationError::Core)?,
            })
        })
        .collect::<Result<Vec<_>, ClientOperationError>>()?;
    named.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(named)
}

/// Counts the executions a visibility query matches through Temporal's
/// official workflow service (#1152). For a `GROUP BY` query the server's
/// `count` is already the total over every group, which is what is returned.
pub async fn count_workflows(
    connection: Connection,
    request: CountRequest,
) -> Result<CountResponse, ClientOperationError> {
    let clock = RpcClock::start(request.rpc_timeout_ms, READ_RPC_TIMEOUT);
    let mut service = connection.clone();
    let response = clock
        .call(
            service.count_workflow_executions(clock.request(CountWorkflowExecutionsRequest {
                namespace: request.namespace,
                query: request.query,
            })),
            map_rpc_status,
        )
        .await?;
    let count = u64::try_from(response.count).map_err(|_| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "visibility count is negative",
        ))
    })?;
    Ok(CountResponse { count })
}

/// Requests cancellation of one run (exact, or current for an empty run ID)
/// through Temporal's official workflow service. The connection is cloned by
/// the owner Domain and no Tokio task calls back into OCaml.
pub async fn cancel_workflow(
    connection: Connection,
    request: CancelWorkflowRequest,
) -> Result<CancelWorkflowResponse, ClientOperationError> {
    // A cancellation acknowledgement is a control-plane RPC, not a workflow
    // wait. Bound it so a stalled server cannot hold the owner Domain mailbox
    // forever; callers can retry the same request ID when the outcome is
    // uncertain.
    let clock = RpcClock::start(request.rpc_timeout_ms, CONTROL_RPC_TIMEOUT);
    let mut service = connection.clone();
    let request = RequestCancelWorkflowExecutionRequest {
        namespace: request.namespace,
        workflow_execution: core_execution(request.workflow_id, request.run_id),
        identity: connection.identity().to_owned(),
        request_id: request.request_id,
        reason: request.reason,
        ..Default::default()
    };
    clock
        .call(
            service.request_cancel_workflow_execution(clock.request(request)),
            map_rpc_status,
        )
        .await?;
    Ok(CancelWorkflowResponse { acknowledged: true })
}

/// Resets one workflow run (exact, or current for an empty run ID) through
/// Temporal's official workflow service.
/// The event ID and request ID are copied into protobuf owned by this RPC; no
/// OCaml memory is retained after the owner Domain call returns.
pub async fn reset_workflow(
    connection: Connection,
    request: ResetWorkflowRequest,
) -> Result<ResetWorkflowResponse, ClientOperationError> {
    let clock = RpcClock::start(request.rpc_timeout_ms, CONTROL_RPC_TIMEOUT);
    let mut service = connection.clone();
    let namespace = request.namespace.clone();
    let workflow_id = request.workflow_id.clone();
    let response = clock
        .call(
            service.reset_workflow_execution(clock.request(ResetWorkflowExecutionRequest {
                namespace: request.namespace,
                workflow_execution: core_execution(request.workflow_id, request.run_id),
                reason: request.reason,
                workflow_task_finish_event_id: request.workflow_task_finish_event_id,
                request_id: request.request_id,
                identity: connection.identity().to_owned(),
                ..Default::default()
            })),
            map_rpc_status,
        )
        .await?;
    if response.run_id.is_empty() {
        return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal reset response omitted run ID",
        )));
    }
    let response = ResetWorkflowResponse {
        execution: ExecutionRef {
            namespace,
            workflow_id,
            run_id: response.run_id,
        },
    };
    validate_execution(&response.execution, "$.execution").map_err(|_| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal reset response had invalid execution identity",
        ))
    })?;
    Ok(response)
}

/// Terminates one workflow run (exact, or current for an empty run ID) through
/// Temporal's official service.
/// Termination is intentionally separate from cancellation: the server writes
/// an immutable terminated event immediately and does not wait for workflow
/// code to observe a cancellation request.
pub async fn terminate_workflow(
    connection: Connection,
    request: TerminateWorkflowRequest,
) -> Result<TerminateWorkflowResponse, ClientOperationError> {
    let budget = rpc_budget(request.rpc_timeout_ms, CONTROL_RPC_TIMEOUT);
    let mut service = connection.clone();
    let request = TerminateWorkflowExecutionRequest {
        namespace: request.namespace,
        workflow_execution: core_execution(request.workflow_id, request.run_id),
        reason: request.reason,
        identity: connection.identity().to_owned(),
        ..Default::default()
    };
    match tokio::time::timeout(
        budget,
        service.terminate_workflow_execution(terminate_request(request, budget)),
    )
    .await
    {
        Ok(result) => {
            result.map_err(terminate_status_error)?;
        }
        Err(_) => {
            return Err(ClientOperationError::Rpc {
                // TerminateWorkflowExecution has no request ID. If the
                // transport deadline expires after the server accepted the
                // command, retrying cannot be correlated with this attempt;
                // preserve that ambiguity explicitly for the OCaml caller.
                code: "termination_outcome_uncertain".to_owned(),
            });
        }
    }
    Ok(TerminateWorkflowResponse { acknowledged: true })
}

/// Delivers one signal to one workflow run (exact, or current for an empty run
/// ID) through Temporal's official workflow service. The request identity and
/// payloads are copied into the protobuf message; no OCaml memory is retained
/// by the asynchronous RPC.
pub async fn signal_workflow(
    connection: Connection,
    request: SignalWorkflowRequest,
) -> Result<SignalWorkflowResponse, ClientOperationError> {
    // Signal delivery is a control-plane acknowledgement rather than a wait
    // for workflow code to process the message. Keep the owner Domain
    // responsive when the server is unavailable; callers can retry the same
    // request ID after an uncertain timeout.
    let clock = RpcClock::start(request.rpc_timeout_ms, CONTROL_RPC_TIMEOUT);
    let payloads = payloads_to_core(&request.input)?;
    let mut service = connection.clone();
    let request = SignalWorkflowExecutionRequest {
        namespace: request.namespace,
        workflow_execution: core_execution(request.workflow_id, request.run_id),
        signal_name: request.signal_name,
        input: Some(payloads),
        identity: connection.identity().to_owned(),
        request_id: request.request_id,
        ..Default::default()
    };
    clock
        .call(
            service.signal_workflow_execution(clock.request(request)),
            map_rpc_status,
        )
        .await?;
    Ok(SignalWorkflowResponse { acknowledged: true })
}

/// Executes one output-only query through Temporal's official workflow
/// service. Query rejection is represented as a stable failed-precondition
/// error rather than exposing server-controlled status text or protobuf
/// details through the OCaml protocol. The request uses the non-rejecting
/// condition (`QUERY_REJECT_CONDITION_NONE = 1`) so a closed workflow may still
/// answer a read-only query, matching the default behavior of other SDKs.
pub async fn query_workflow(
    connection: Connection,
    request: QueryWorkflowRequest,
) -> Result<QueryWorkflowResponse, ClientOperationError> {
    // Query evaluation may require a workflow task to be scheduled and
    // replayed before Temporal can reply. A one-second control-plane bound
    // incorrectly rejects healthy slow workers, so the default is a longer
    // bounded service budget; a caller may select its own (#499).
    const QUERY_RPC_TIMEOUT: Duration = Duration::from_secs(30);
    let clock = RpcClock::start(request.rpc_timeout_ms, QUERY_RPC_TIMEOUT);
    let query_args = payloads_to_core(&request.input)?;
    let mut service = connection.clone();
    let request = QueryWorkflowExecutionRequest {
        namespace: request.namespace,
        execution: core_execution(request.workflow_id, request.run_id),
        query: Some(WorkflowQuery {
            query_type: request.query_type,
            query_args: Some(query_args),
            ..Default::default()
        }),
        // The generated prost field is an i32 enum. Keep the numeric value
        // local and documented so this bridge does not depend on generated
        // enum variant names that can change with protobuf regeneration.
        query_reject_condition: 1,
    };
    let response = clock
        .call(
            service.query_workflow(clock.request(request)),
            map_query_status,
        )
        .await?;
    if response.query_rejected.is_some() {
        return Err(ClientOperationError::Rpc {
            code: "failed_precondition".to_owned(),
        });
    }
    Ok(QueryWorkflowResponse {
        result: payloads_from_core(response.query_result.as_ref())
            .map_err(ClientOperationError::Core)?,
    })
}

/// Total time `update_workflow` may spend waiting for Temporal to report that
/// an update was accepted. The bound keeps the supervisor's synchronous client
/// turn finite; it spans every re-issued `UpdateWorkflowExecution` attempt, not
/// each attempt separately.
const UPDATE_ACCEPTANCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Minimum pause before re-issuing an update whose response reported only the
/// `Admitted` stage. A conforming server answers `Admitted` only when its own
/// long poll expires, so the pause adds no latency in practice; it prevents a
/// misbehaving server that answers immediately from driving a hot RPC loop for
/// the whole acceptance budget.
const UPDATE_READMISSION_PAUSE: Duration = Duration::from_millis(100);

/// The shared total budget of an update acceptance loop, which
/// [`update_workflow_within`] and update-with-start both run.
///
/// Every attempt carries what remains of the budget as its gRPC deadline, so
/// a real server ends its long poll (answering `Admitted`) before the local
/// timeout fires, and an update answered only as admitted is re-issued after
/// [`UPDATE_READMISSION_PAUSE`] while time remains. Expiry is reported by the
/// caller as [`deadline_exceeded`].
#[derive(Clone, Copy)]
struct AcceptanceDeadline(tokio::time::Instant);

/// One budgeted attempt of an update acceptance loop.
struct AcceptanceAttempt<T> {
    /// The re-sent message, carrying what remains of the budget.
    request: Request<T>,
    /// When the attempt began, for [`budget_status`].
    started: tokio::time::Instant,
    /// The budget the attempt was given, for [`budget_status`].
    remaining: Duration,
}

impl<T> AcceptanceAttempt<T> {
    /// Wraps a status mapper so that a transport or server deadline expiring
    /// with this attempt's budget is the typed `deadline_exceeded` error.
    ///
    /// The returned mapper copies the attempt's clock rather than borrowing
    /// the attempt, so the request can still be moved into the call.
    fn status_map<M>(&self, map: M) -> impl FnOnce(Status) -> ClientOperationError + use<T, M>
    where
        M: FnOnce(Status) -> ClientOperationError,
    {
        budget_status(self.started, self.remaining, map)
    }
}

impl AcceptanceDeadline {
    /// Starts a budget that expires `budget` from now.
    fn after(budget: Duration) -> Self {
        Self(tokio::time::Instant::now() + budget)
    }

    /// The instant at which the budget expires.
    fn instant(self) -> tokio::time::Instant {
        self.0
    }

    /// Prepares one more attempt of `message`, or returns `None` once the
    /// budget has expired.
    ///
    /// Core may retry a transient transport failure inside the attempt; both
    /// its retry window and every transport attempt's deadline are limited to
    /// what remains of the shared budget ([`budgeted_request`]).
    fn next_attempt<T: Clone>(self, message: &T) -> Option<AcceptanceAttempt<T>> {
        let remaining = self
            .0
            .saturating_duration_since(tokio::time::Instant::now());
        (!remaining.is_zero()).then(|| AcceptanceAttempt {
            request: budgeted_request(message.clone(), remaining),
            started: tokio::time::Instant::now(),
            remaining,
        })
    }

    /// Waits out [`UPDATE_READMISSION_PAUSE`] before an admitted-only update
    /// is re-issued. Returns `false` without waiting when the pause would
    /// reach the deadline, because no further attempt could then be made.
    async fn pause_for_readmission(self) -> bool {
        let pause_until = tokio::time::Instant::now() + UPDATE_READMISSION_PAUSE;
        if pause_until >= self.0 {
            return false;
        }
        tokio::time::sleep_until(pause_until).await;
        true
    }
}

/// Starts one update through Temporal's workflow service and waits until the
/// workflow worker has accepted it. Temporal deliberately does not allow the
/// asynchronous `Admitted` stage for the public start-update flow; `Accepted`
/// is the interoperable stage for a handle that will be polled separately.
///
/// Temporal answers a `wait_policy = Accepted` request with stage `Admitted`
/// and no outcome when its long poll expires before a worker processes the
/// update. An admitted update is not durable and may still be rejected, so
/// that response must never become a handle. Like the Go and Python SDKs, the
/// request is re-issued with the same update ID (Temporal deduplicates it)
/// until the server reports `Accepted`, a terminal outcome (completion or
/// validator rejection), or the shared [`UPDATE_ACCEPTANCE_TIMEOUT`] expires,
/// which is reported as the typed `deadline_exceeded` RPC error. A request's
/// `rpc_timeout_ms` replaces that shared budget.
pub async fn update_workflow(
    connection: Connection,
    request: UpdateWorkflowRequest,
) -> Result<UpdateWorkflowResponse, ClientOperationError> {
    let budget = rpc_budget(request.rpc_timeout_ms, UPDATE_ACCEPTANCE_TIMEOUT);
    update_workflow_within(connection, request, budget).await
}

/// Implements [`update_workflow`] with an explicit total acceptance budget so
/// tests can observe the deadline path without waiting for the production
/// bound. Every attempt carries the remaining budget as its gRPC deadline, so
/// a real server ends its long poll (answering `Admitted`) before the local
/// timeout fires and the loop can decide whether time remains for a retry.
pub(crate) async fn update_workflow_within(
    connection: Connection,
    request: UpdateWorkflowRequest,
    budget: Duration,
) -> Result<UpdateWorkflowResponse, ClientOperationError> {
    let rpc_request = update_execution_request(&request, connection.identity())?;
    let deadline = AcceptanceDeadline::after(budget);
    let mut service = connection.clone();
    loop {
        let Some(attempt) = deadline.next_attempt(&rpc_request) else {
            return Err(deadline_exceeded());
        };
        let map = attempt.status_map(map_rpc_status);
        let response = tokio::time::timeout_at(
            deadline.instant(),
            service.update_workflow_execution(attempt.request),
        )
        .await
        .map_err(|_| deadline_exceeded())?
        .map_err(map)?
        .into_inner();
        // Validate identity on every attempt, including admitted-only ones, so
        // a server answering for another update fails closed immediately.
        let response_stage = response.stage;
        let converted = update_response_from_core(&request, response)?;
        if converted.outcome.is_some() {
            // An outcome is final whatever stage accompanies it: the server
            // only attaches one once the update completed or was rejected, so
            // a stage this bridge does not know (a newer server's) cannot
            // change its meaning (#1078).
            return Ok(converted);
        }
        if update_stage_is_accepted(response_stage)? {
            return Ok(converted);
        }
        // Admitted, an older server's unspecified stage, or a stage this
        // bridge does not know, without an outcome: the update is not known
        // to be durable. Re-issue it if the budget still allows one more
        // attempt.
        if !deadline.pause_for_readmission().await {
            return Err(deadline_exceeded());
        }
    }
}

/// Builds Temporal's `UpdateWorkflowExecutionRequest` for one validated update
/// request, waiting for the `Accepted` stage. `update_workflow` sends it
/// directly; update-with-start (#1151) sends it as the second operation of an
/// `ExecuteMultiOperation` with an empty run ID, as Temporal requires there.
fn update_execution_request(
    request: &UpdateWorkflowRequest,
    identity: &str,
) -> Result<UpdateWorkflowExecutionRequest, ClientOperationError> {
    let input = payloads_to_core(&request.input)?;
    Ok(UpdateWorkflowExecutionRequest {
        namespace: request.namespace.clone(),
        workflow_execution: core_execution(request.workflow_id.clone(), request.run_id.clone()),
        wait_policy: Some(WaitPolicy {
            lifecycle_stage: i32::from(UpdateWorkflowExecutionLifecycleStage::Accepted),
        }),
        request: Some(update::v1::Request {
            meta: Some(update::v1::Meta {
                update_id: request.update_id.clone(),
                identity: identity.to_owned(),
            }),
            input: Some(update::v1::Input {
                name: request.update_name.clone(),
                args: Some(input),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Classifies the lifecycle stage of an update response that carried no
/// outcome. Returns `true` once the update is at least accepted, `false` for
/// `Admitted` and `Unspecified` (both mean "not yet accepted" and are retried,
/// matching the official SDKs' `stage < Accepted` loop), and a Core conversion
/// error for `Completed` without an outcome, which no conforming server sends.
///
/// A numeric stage this bridge does not know, as a newer server might add,
/// is also classified as "not yet accepted" (#1078). That is the conservative
/// reading: re-issuing the update under the same update ID is deduplicated
/// by Temporal, so the loop either observes a known stage or ends with the
/// typed `deadline_exceeded` error, and an update whose durability is not
/// established never becomes a handle.
fn update_stage_is_accepted(stage: i32) -> Result<bool, ClientOperationError> {
    match UpdateWorkflowExecutionLifecycleStage::try_from(stage) {
        Ok(UpdateWorkflowExecutionLifecycleStage::Accepted) => Ok(true),
        Ok(UpdateWorkflowExecutionLifecycleStage::Completed) => Err(ClientOperationError::Core(
            workflow_protocol::invalid_core("Temporal completed update omitted its outcome"),
        )),
        Ok(
            UpdateWorkflowExecutionLifecycleStage::Unspecified
            | UpdateWorkflowExecutionLifecycleStage::Admitted,
        )
        | Err(_) => Ok(false),
    }
}

/// Converts one `UpdateWorkflowExecution` response into the closed bridge
/// response after checking that it names the requested update and run (or,
/// for a current-run request, a concrete run of the requested workflow).
/// The stage is interpreted by the caller; this function only validates
/// identity and converts an optional outcome.
fn update_response_from_core(
    request: &UpdateWorkflowRequest,
    response: UpdateWorkflowExecutionResponse,
) -> Result<UpdateWorkflowResponse, ClientOperationError> {
    let update_ref = response.update_ref.ok_or_else(|| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal update admission omitted update_ref",
        ))
    })?;
    let execution = update_ref.workflow_execution.ok_or_else(|| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal update admission omitted workflow execution",
        ))
    })?;
    if update_ref.update_id != request.update_id {
        return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal update response changed update_id",
        )));
    }
    let execution = ExecutionRef {
        namespace: request.namespace.clone(),
        workflow_id: execution.workflow_id,
        run_id: if execution.run_id.is_empty() {
            request.run_id.clone()
        } else {
            execution.run_id
        },
    };
    // An exact request must be answered for that run. A current-run request
    // (empty run ID) adopts the run Temporal resolved, so later polls of the
    // update stay on the run that accepted it even if the workflow continues
    // as new; Temporal always reports that run, and a response without one
    // fails closed rather than producing a handle with no run identity.
    let changed_run = !request.run_id.is_empty() && execution.run_id != request.run_id;
    if execution.workflow_id != request.workflow_id || changed_run {
        return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal update response changed workflow execution",
        )));
    }
    if execution.run_id.is_empty() {
        return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal update response omitted the run that accepted it",
        )));
    }
    let outcome = response
        .outcome
        .as_ref()
        .map(update_outcome_from_core)
        .transpose()
        .map_err(ClientOperationError::Core)?;
    Ok(UpdateWorkflowResponse {
        update_id: update_ref.update_id,
        execution,
        outcome,
    })
}

/// Total time one `poll_workflow_update` call may spend, including Core's
/// retries of transient transport failures, before it reports the update as
/// still pending.
const UPDATE_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Polls one update until completion, bounded so the supervisor remains
/// responsive to shutdown and unrelated client requests.
pub async fn poll_workflow_update(
    connection: Connection,
    request: PollWorkflowUpdateRequest,
) -> Result<PollWorkflowUpdateResponse, ClientOperationError> {
    let mut service = connection.clone();
    let response = match tokio::time::timeout(
        UPDATE_POLL_TIMEOUT,
        service.poll_workflow_execution_update(budgeted_request(
            PollWorkflowExecutionUpdateRequest {
                namespace: request.namespace,
                update_ref: Some(update::v1::UpdateRef {
                    workflow_execution: core_execution(request.workflow_id, request.run_id),
                    update_id: request.update_id,
                }),
                identity: connection.identity().to_owned(),
                wait_policy: Some(WaitPolicy {
                    lifecycle_stage: i32::from(UpdateWorkflowExecutionLifecycleStage::Completed),
                }),
            },
            UPDATE_POLL_TIMEOUT,
        )),
    )
    .await
    {
        Ok(result) => result.map_err(map_rpc_status)?.into_inner(),
        Err(_) => {
            // A long-running update is still admitted and may complete after
            // this bounded poll window.  Returning a pending JSON response
            // lets the OCaml caller retry without turning a healthy update
            // into a terminal transport error.
            return Ok(PollWorkflowUpdateResponse { outcome: None });
        }
    };
    Ok(PollWorkflowUpdateResponse {
        outcome: response
            .outcome
            .as_ref()
            .map(update_outcome_from_core)
            .transpose()
            .map_err(ClientOperationError::Core)?,
    })
}

/// Converts the protobuf oneof used by Temporal's update response into the
/// closed semantic result without retaining protobuf ownership.
fn update_outcome_from_core(
    outcome: &update::v1::Outcome,
) -> Result<UpdateOutcome, workflow_protocol::CoreConversionError> {
    match outcome.value.as_ref() {
        Some(update::v1::outcome::Value::Success(payloads)) => Ok(UpdateOutcome::Completed {
            result: payloads_from_core(Some(payloads))?,
        }),
        Some(update::v1::outcome::Value::Failure(failure)) => Ok(UpdateOutcome::Failed {
            failure: Box::new(workflow_protocol::failure_from_core(failure)?),
        }),
        None => Err(workflow_protocol::invalid_core(
            "Temporal update outcome omitted success and failure",
        )),
    }
}

/// Encodes one terminal asynchronous-start outcome and reparses the bytes
/// before ownership leaves Rust.  The closed object keeps `Unknown` explicit
/// instead of overloading a transport error status that could be mistaken for
/// a proven rejection.
pub(crate) fn encode_start_outcome(
    outcome: &StartWorkflowOutcome,
) -> Result<String, protocol::ProtocolError> {
    let document = match outcome {
        StartWorkflowOutcome::Accepted(response) => StartWorkflowOutcomeDocument::Accepted {
            execution: response.execution.clone(),
            started: response.started,
        },
        StartWorkflowOutcome::Rejected(error) => StartWorkflowOutcomeDocument::Rejected {
            error: error_document(error),
        },
        StartWorkflowOutcome::Unknown {
            request_id,
            workflow_id,
        } => StartWorkflowOutcomeDocument::Unknown {
            request_id: request_id.clone(),
            workflow_id: workflow_id.clone(),
        },
    };
    validate_start_outcome_document(&document)?;
    encode_document(&document)
}

/// Maximum time one start RPC may block the supervisor owner or a Tokio task.
///
/// A hung Temporal start must not pin the sole owner Domain forever (sync ABI)
/// or exhaust the pending-start registry (async begin). The timeout is applied
/// outside the gRPC future so cancellation drops the in-flight request. The
/// resulting `deadline_exceeded` status is classified as an uncertain start:
/// Temporal may have accepted the workflow after the client gave up.
const START_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts one workflow through Core's retrying connection, retaining the same
/// request and idempotency key across transport attempts within one deadline.
///
/// The deadline is the request's `rpc_timeout_ms` or [`START_RPC_TIMEOUT`].
/// Like every other budgeted client RPC ([`budgeted_request`]), it is both
/// the outer timeout and Core's retry window and per-attempt gRPC deadline,
/// so a start with a long `rpc_timeout` keeps retrying a transient failure
/// for that long instead of stopping at Core's default 10 s window (#1220).
/// It bounds only this client call; the workflow timeouts and retry policy
/// are separate server-side execution policies copied into the request.
pub async fn start_workflow(
    connection: Connection,
    request: StartWorkflowRequest,
) -> Result<StartWorkflowResponse, ClientOperationError> {
    let rpc_request = start_execution_request(&request, connection.identity())?;
    let clock = RpcClock::start(request.rpc_timeout_ms, START_RPC_TIMEOUT);
    // Connection owns Core's retry policy; its underlying workflow service
    // skips that policy and gives up on the first transient transport error.
    let mut service = connection.clone();
    let response = clock
        .call(
            service.start_workflow_execution(clock.request(rpc_request)),
            |status| map_start_status(&request.workflow_id, status),
        )
        .await?;
    start_response_from_core(&request, response.run_id, response.started)
}

/// Builds Temporal's `StartWorkflowExecutionRequest` for one validated start
/// request. `start_workflow` sends it directly; update-with-start (#1151)
/// sends it as the first operation of an `ExecuteMultiOperation`, and
/// signal-with-start copies its fields, so every start path applies the same
/// policies, timeouts, metadata, and identity.
fn start_execution_request(
    request: &StartWorkflowRequest,
    identity: &str,
) -> Result<StartWorkflowExecutionRequest, ClientOperationError> {
    let payloads = payloads_to_core(&request.input)?;
    // Temporal represents each indexed search attribute as one payload;
    // callers that need multiple values encode a list with their codec first.
    let memo = metadata_payload_map(&request.memo)
        .map_err(ClientOperationError::Core)?
        .map(|fields| Memo { fields });
    let search_attributes = metadata_payload_map(&request.search_attributes)
        .map_err(ClientOperationError::Core)?
        .map(|indexed_fields| SearchAttributes { indexed_fields });
    let retry_policy = request
        .retry_policy
        .as_ref()
        .map(workflow_protocol::retry_policy_to_core)
        .transpose()
        .map_err(ClientOperationError::Core)?;
    Ok(StartWorkflowExecutionRequest {
        namespace: request.namespace.clone(),
        input: Some(payloads),
        workflow_id: request.workflow_id.clone(),
        workflow_type: Some(WorkflowType {
            name: request.workflow_type.clone(),
        }),
        task_queue: Some(TaskQueue {
            name: request.task_queue.clone(),
            kind: 0,
            normal_name: String::new(),
        }),
        memo,
        search_attributes,
        identity: identity.to_owned(),
        request_id: request.request_id.clone(),
        // Always explicit: an UNSPECIFIED policy would defer to the
        // server default, which the public API documents as `Fail`.
        workflow_id_conflict_policy: request.id_conflict_policy.to_core(),
        // Also always explicit, for the same reason (#499).
        workflow_id_reuse_policy: request.id_reuse_policy.to_core(),
        // Absent timeouts and retry policy keep Temporal's defaults.
        workflow_execution_timeout: optional_milliseconds_to_core(request.execution_timeout_ms)?,
        workflow_run_timeout: optional_milliseconds_to_core(request.run_timeout_ms)?,
        workflow_task_timeout: optional_milliseconds_to_core(request.task_timeout_ms)?,
        retry_policy,
        // An absent schedule is Temporal's empty string; validation already
        // refused an explicit empty one and its combination with a delay.
        cron_schedule: request.cron_schedule.clone().unwrap_or_default(),
        workflow_start_delay: optional_milliseconds_to_core(request.start_delay_ms)?,
        ..Default::default()
    })
}

/// Converts the run ID and `started` flag of a successful start-like
/// response (start, signal-with-start, or the start operation of
/// update-with-start) into the closed response for `request`.
///
/// Only USE_EXISTING can succeed without creating a run. Servers that
/// predate `started` leave it false, so the other policies (whose success
/// proves this logical request created the run, possibly as a request-ID
/// deduplicated retry) report a new run regardless of the field. An empty or
/// invalid run ID is a Core conversion error.
fn start_response_from_core(
    request: &StartWorkflowRequest,
    run_id: String,
    started: bool,
) -> Result<StartWorkflowResponse, ClientOperationError> {
    let started = started || request.id_conflict_policy != IdConflictPolicy::UseExisting;
    if run_id.is_empty() {
        return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal start response omitted run ID",
        )));
    }
    let execution = ExecutionRef {
        namespace: request.namespace.clone(),
        workflow_id: request.workflow_id.clone(),
        run_id,
    };
    validate_execution(&execution, "$.execution").map_err(|_| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal start response had invalid execution identity",
        ))
    })?;
    Ok(StartWorkflowResponse { execution, started })
}

/// Waits for the run named by `request`, or for the workflow's current run
/// when its run ID is empty, never following successors.
///
/// Each poll advertises [`FOLLOWS_NEXT_RUN_ID_FEATURE`], so Temporal returns
/// the run's real close event: a run that a retry policy or cron schedule
/// chained to a successor reports `Failed`, `TimedOut`, or `Completed` with
/// that successor, and only an explicit continue-as-new reports
/// `ContinuedAsNew`.
///
/// With an empty run ID Temporal resolves the current run on the first poll
/// and pins later pages to it through the continuation token. If a long poll
/// ends with neither an event nor a token, the next poll resolves the current
/// run again; that can only skip a run which has already been superseded, so
/// a caller that follows successors observes the same final run.
pub async fn wait_workflow(
    connection: Connection,
    request: WaitWorkflowRequest,
) -> Result<WaitWorkflowResponse, ClientOperationError> {
    let execution = ExecutionRef {
        namespace: request.namespace.clone(),
        workflow_id: request.workflow_id.clone(),
        run_id: request.run_id.clone(),
    };
    let mut next_page_token = Vec::new();
    let mut service = connection.clone();

    loop {
        let mut poll = GetWorkflowExecutionHistoryRequest {
            namespace: request.namespace.clone(),
            execution: core_execution(request.workflow_id.clone(), request.run_id.clone()),
            next_page_token: std::mem::take(&mut next_page_token),
            skip_archival: false,
            wait_new_event: true,
            history_event_filter_type: i32::from(HistoryEventFilterType::CloseEvent),
            ..Default::default()
        }
        .into_request();
        // Core marks this `wait_new_event` request as a user long poll and
        // gives it Core's long-poll transport deadline; the bridge supplies
        // only the retry window documented on `wait_retry_options`.
        poll.extensions_mut()
            .insert(RetryConfigForCall(wait_retry_options()));
        // Ask Temporal for the true close event rather than its legacy
        // continued-as-new rewrite (#971). Core's retry path copies request
        // metadata, so every transport attempt carries the header.
        poll.metadata_mut().insert(
            SUPPORTED_FEATURES_HEADER,
            MetadataValue::from_static(FOLLOWS_NEXT_RUN_ID_FEATURE),
        );
        let response = service
            .get_workflow_execution_history(poll)
            .await
            .map_err(map_rpc_status)?
            .into_inner();

        if let Some(history) = response.history
            && let Some(event) = history.events.into_iter().last()
        {
            let outcome = outcome_from_event(event, &request.namespace, &request.workflow_id)?;
            let result = WaitWorkflowResponse { execution, outcome };
            validate_wait_response(&result).map_err(|_| {
                ClientOperationError::Core(workflow_protocol::invalid_core(
                    "Temporal close event violated client result invariants",
                ))
            })?;
            return Ok(result);
        }

        // Pagination tokens are returned for already-closed histories.  If a
        // long poll returns no close event, retry with an empty token and keep
        // the same run identity; this is the exact-run equivalent of Core's
        // `follow_runs = false` option.
        next_page_token = response.next_page_token;
    }
}

/// Converts a terminal history event without following a successor run.
fn outcome_from_event(
    event: HistoryEvent,
    namespace: &str,
    workflow_id: &str,
) -> Result<WorkflowOutcome, ClientOperationError> {
    let attributes = event.attributes.ok_or_else(|| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal close event omitted attributes",
        ))
    })?;
    let successor = |run_id: &str| successor_ref(namespace, workflow_id, run_id);
    let outcome = match attributes {
        Attributes::WorkflowExecutionCompletedEventAttributes(attributes) => {
            WorkflowOutcome::Completed {
                result: payloads_from_core(attributes.result.as_ref())
                    .map_err(ClientOperationError::Core)?,
                successor: successor(&attributes.new_execution_run_id),
            }
        }
        Attributes::WorkflowExecutionFailedEventAttributes(attributes) => {
            let failure = attributes
                .failure
                .as_ref()
                .ok_or_else(|| {
                    ClientOperationError::Core(workflow_protocol::invalid_core(
                        "Temporal failed event omitted failure details",
                    ))
                })
                .and_then(|failure| {
                    workflow_protocol::failure_from_core(failure)
                        .map_err(ClientOperationError::Core)
                })?;
            WorkflowOutcome::Failed {
                failure: Box::new(failure),
                successor: successor(&attributes.new_execution_run_id),
            }
        }
        Attributes::WorkflowExecutionCanceledEventAttributes(attributes) => {
            WorkflowOutcome::Cancelled {
                details: payloads_from_core(attributes.details.as_ref())
                    .map_err(ClientOperationError::Core)?,
            }
        }
        Attributes::WorkflowExecutionTerminatedEventAttributes(attributes) => {
            WorkflowOutcome::Terminated {
                details: payloads_from_core(attributes.details.as_ref())
                    .map_err(ClientOperationError::Core)?,
            }
        }
        Attributes::WorkflowExecutionTimedOutEventAttributes(attributes) => {
            WorkflowOutcome::TimedOut {
                successor: successor(&attributes.new_execution_run_id),
            }
        }
        Attributes::WorkflowExecutionContinuedAsNewEventAttributes(attributes) => {
            let successor = successor(&attributes.new_execution_run_id).ok_or_else(|| {
                ClientOperationError::Core(workflow_protocol::invalid_core(
                    "Temporal continued-as-new event omitted successor run ID",
                ))
            })?;
            WorkflowOutcome::ContinuedAsNew { successor }
        }
        _ => {
            return Err(ClientOperationError::Core(workflow_protocol::unsupported(
                "Temporal returned an unsupported close event",
            )));
        }
    };
    Ok(outcome)
}

/// Builds the protobuf address of one workflow run. An empty `run_id`
/// selects the workflow's current run, which every exact-run RPC accepts and
/// Temporal resolves when it handles the call (#791).
fn core_execution(workflow_id: String, run_id: String) -> Option<WorkflowExecution> {
    Some(WorkflowExecution {
        workflow_id,
        run_id,
    })
}

/// Converts Core payloads without dropping order or metadata.
fn payloads_from_core(
    value: Option<&Payloads>,
) -> Result<Vec<workflow_protocol::Payload>, workflow_protocol::CoreConversionError> {
    value
        .map(|payloads| {
            payloads
                .payloads
                .iter()
                .map(workflow_protocol::payload_from_core)
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

/// Converts the semantic payloads of an outgoing request into the protobuf
/// payload container, reporting a payload Core cannot carry as the typed
/// client error every request builder returns.
fn payloads_to_core(
    values: &[workflow_protocol::Payload],
) -> Result<Payloads, ClientOperationError> {
    Ok(Payloads {
        payloads: values
            .iter()
            .map(workflow_protocol::payload_to_core)
            .collect::<Result<_, _>>()
            .map_err(ClientOperationError::Core)?,
    })
}

/// Converts named OCaml metadata to the key-to-payload map shared by memo and
/// search attributes.
///
/// An empty list is `None`, so the request leaves the field unset rather than
/// sending an empty map. Keys were already validated as unique by
/// [`validate_metadata`]; each value is converted with the same payload
/// validation as every other outgoing payload, and the first invalid one
/// fails the whole conversion.
fn metadata_payload_map(
    fields: &[MetadataField],
) -> Result<Option<HashMap<String, CorePayload>>, workflow_protocol::CoreConversionError> {
    if fields.is_empty() {
        return Ok(None);
    }
    let mut values = HashMap::with_capacity(fields.len());
    for field in fields {
        let payload = workflow_protocol::payload_to_core(&field.value)?;
        values.insert(field.key.clone(), payload);
    }
    Ok(Some(values))
}

/// Builds successor metadata only for a nonempty Core run ID.
fn successor_ref(namespace: &str, workflow_id: &str, run_id: &str) -> Option<SuccessorRef> {
    (!run_id.is_empty()).then(|| SuccessorRef {
        namespace: namespace.to_owned(),
        workflow_id: workflow_id.to_owned(),
        run_id: run_id.to_owned(),
    })
}

/// Maps a start RPC failure into the closed privacy-safe client error set.
///
/// `AlreadyExists` has Temporal-specific AlreadyStarted meaning only for the
/// start operation; all other statuses use the stable generic RPC mapping.
fn map_start_status(workflow_id: &str, status: Status) -> ClientOperationError {
    if status.code() == Code::AlreadyExists {
        let existing_run_id = temporalio_common::protos::utilities::decode_status_detail::<
            temporalio_common::protos::temporal::api::errordetails::v1::WorkflowExecutionAlreadyStartedFailure,
        >(status.details())
        .map(|detail| detail.run_id)
        // Status details are server input too.  Do not reflect an empty or
        // oversized run ID into the ABI's bounded JSON error document.  NUL
        // bytes are rejected as well: Rust strings can contain them, but the
        // bilateral identifier contract deliberately cannot.  Treating a
        // malformed optional detail as absent preserves the useful
        // AlreadyStarted category without emitting a body that OCaml must
        // reject after the status has already been classified.
        .filter(|run_id| {
            !run_id.is_empty()
                && run_id.len() <= protocol::MAX_STRING_BYTES
                && !run_id.contains('\0')
        });
        ClientOperationError::AlreadyStarted {
            workflow_id: workflow_id.to_owned(),
            existing_run_id,
        }
    } else {
        map_rpc_status(status)
    }
}

/// Maps a query RPC failure, recognizing Temporal's query handler failure.
///
/// Temporal reports a failed query handler (or a query name the worker does
/// not know) as `InvalidArgument` with a `QueryFailedFailure` status detail and
/// the handler's message as the status message. The detail, not the code,
/// identifies it: a malformed request is also `InvalidArgument` but carries no
/// such detail, and an old server that omits the detail is indistinguishable
/// from one, so it stays a plain RPC error. When the detail carries the full
/// failure, its message is preferred because it is what the handler raised;
/// otherwise the status message (the server's copy of it) is used.
fn map_query_status(status: Status) -> ClientOperationError {
    if status.code() == Code::InvalidArgument
        && let Some(detail) = typed_status_detail::<
            temporalio_common::protos::temporal::api::errordetails::v1::QueryFailedFailure,
        >(status.details(), QUERY_FAILED_TYPE_URL)
    {
        let message = match detail.failure {
            Some(failure) if !failure.message.is_empty() => failure.message,
            _ => status.message().to_owned(),
        };
        return ClientOperationError::QueryFailed {
            message: bounded_query_failure_message(&message),
        };
    }
    map_rpc_status(status)
}

/// Fully qualified `Any` type URL of Temporal's query failure status detail.
const QUERY_FAILED_TYPE_URL: &str =
    "type.googleapis.com/temporal.api.errordetails.v1.QueryFailedFailure";

/// Decodes the first detail of encoded `google.rpc.Status` bytes as `M`
/// (the query failure, or update-with-start's multi-operation failure).
///
/// Core's generic `decode_status_detail` ignores the `Any` type URL, and
/// protobuf decoding is lenient enough that an unrelated detail (for example
/// a `BadRequest`) could decode as `M`, so the detail is used only when its
/// type URL is exactly `type_url`. Malformed bytes or a missing detail are
/// treated as absent.
fn typed_status_detail<M: prost::Message + Default>(details: &[u8], type_url: &str) -> Option<M> {
    use prost::Message;
    let status = temporalio_common::protos::google::rpc::Status::decode(details).ok()?;
    let detail = status.details.first()?;
    if detail.type_url != type_url {
        return None;
    }
    M::decode(detail.value.as_slice()).ok()
}

/// Makes a handler-controlled message safe for the closed error document:
/// NUL, which the bilateral string contract rejects, becomes U+FFFD, and the
/// result is truncated at a character boundary to at most
/// [`MAX_QUERY_FAILURE_MESSAGE_BYTES`] bytes. Other control characters are
/// left to JSON escaping.
pub(crate) fn bounded_query_failure_message(message: &str) -> String {
    let mut bounded = String::with_capacity(message.len().min(MAX_QUERY_FAILURE_MESSAGE_BYTES));
    for character in message.chars() {
        let character = if character == '\0' {
            char::REPLACEMENT_CHARACTER
        } else {
            character
        };
        if bounded.len() + character.len_utf8() > MAX_QUERY_FAILURE_MESSAGE_BYTES {
            break;
        }
        bounded.push(character);
    }
    bounded
}

/// Maps a non-start RPC failure without inventing start-specific semantics.
///
/// The exact-run history operation can theoretically receive any gRPC status,
/// including `AlreadyExists` from an intermediary.  It must remain an ordinary
/// RPC error there: only `StartWorkflowExecution` has an AlreadyStarted meaning.
fn map_rpc_status(status: Status) -> ClientOperationError {
    ClientOperationError::Rpc {
        code: rpc_code(status.code()).to_owned(),
    }
}

/// Returns the stable snake-case name used by the closed client-error JSON.
///
/// Tonic's `Debug` spelling is Rust-style (`AlreadyExists`) and lowercasing it
/// would collapse word boundaries (`alreadyexists`).  An explicit table keeps
/// the error ABI stable if tonic changes its formatting implementation.
/// Connection diagnostics reuse this table so a status is spelled the same
/// way in every bridge-generated message.
pub(crate) fn rpc_code(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

/// Validates one start request's identifiers and payload ownership bounds.
fn validate_start_request(value: &StartWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier_fields(
        "$",
        [
            ("request_id", &value.request_id),
            ("namespace", &value.namespace),
            ("workflow_id", &value.workflow_id),
            ("workflow_type", &value.workflow_type),
            ("task_queue", &value.task_queue),
        ],
    )?;
    validate_payloads(&value.input, "$.input")?;
    validate_metadata(&value.memo, "$.memo")?;
    validate_metadata(&value.search_attributes, "$.search_attributes")?;
    validate_start_policies(value)?;
    validate_rpc_timeout(value.rpc_timeout_ms)
}

/// Validates the workflow execution policies of a start request (#499).
///
/// Every workflow timeout must be positive, because Temporal reads zero as
/// "unset" and would silently apply its default. The task timeout is at most
/// [`MAX_WORKFLOW_TASK_TIMEOUT_MS`], a run timeout may not exceed the
/// execution timeout, and a task timeout may not exceed the run timeout (the
/// execution timeout when no run timeout is set): Temporal would silently
/// lower each of those values, so the bridge rejects them instead. Every
/// combination of the reuse and conflict policies is valid: the pinned
/// server accepts `reject_duplicate` with `use_existing` (attach to an open
/// run, refuse a closed one).
fn validate_start_policies(value: &StartWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    for (timeout, path, maximum) in [
        (
            value.execution_timeout_ms,
            "$.execution_timeout_ms",
            MAX_WORKFLOW_TIMEOUT_MS,
        ),
        (
            value.run_timeout_ms,
            "$.run_timeout_ms",
            MAX_WORKFLOW_TIMEOUT_MS,
        ),
        (
            value.task_timeout_ms,
            "$.task_timeout_ms",
            MAX_WORKFLOW_TASK_TIMEOUT_MS,
        ),
    ] {
        if let Some(milliseconds) = timeout {
            if milliseconds == 0 {
                return Err(protocol::ProtocolError::invalid(
                    path,
                    "workflow timeout must be positive",
                ));
            }
            if milliseconds > maximum {
                return Err(protocol::ProtocolError::invalid(
                    path,
                    "workflow timeout exceeds its maximum",
                ));
            }
        }
    }
    if let (Some(execution), Some(run)) = (value.execution_timeout_ms, value.run_timeout_ms)
        && run > execution
    {
        return Err(protocol::ProtocolError::invalid(
            "$.run_timeout_ms",
            "run timeout exceeds the execution timeout",
        ));
    }
    // Temporal runs a run without its own timeout under the execution
    // timeout, and lowers a longer task timeout to that run bound.
    if let (Some(task), Some(run)) = (
        value.task_timeout_ms,
        value.run_timeout_ms.or(value.execution_timeout_ms),
    ) && task > run
    {
        return Err(protocol::ProtocolError::invalid(
            "$.task_timeout_ms",
            "task timeout exceeds the run timeout",
        ));
    }
    if let Some(policy) = &value.retry_policy {
        workflow_protocol::validate_retry_policy(policy, "$.retry_policy")?;
    }
    validate_start_schedule(value)
}

/// Validates the cron schedule and start delay of a start request (#1158).
///
/// A schedule is bounded like every identifier; Temporal parses the cron
/// expression itself and answers an invalid one with `invalid_argument`. A
/// delay must be positive, because Temporal reads zero as "no delay", and no
/// longer than a workflow timeout. Temporal refuses a request carrying both,
/// so the bridge rejects that combination before any RPC.
fn validate_start_schedule(value: &StartWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    if let Some(schedule) = &value.cron_schedule {
        validate_identifier(schedule, "$.cron_schedule")?;
    }
    if let Some(milliseconds) = value.start_delay_ms {
        if milliseconds == 0 {
            return Err(protocol::ProtocolError::invalid(
                "$.start_delay_ms",
                "start delay must be positive",
            ));
        }
        if milliseconds > MAX_WORKFLOW_TIMEOUT_MS {
            return Err(protocol::ProtocolError::invalid(
                "$.start_delay_ms",
                "start delay exceeds its maximum",
            ));
        }
        if value.cron_schedule.is_some() {
            return Err(protocol::ProtocolError::invalid(
                "$.start_delay_ms",
                "start delay cannot be combined with a cron schedule",
            ));
        }
    }
    Ok(())
}

/// Validates metadata names, duplicate rejection, and payload conversion
/// before any protobuf map is constructed. Duplicate names would otherwise be
/// silently overwritten by a `HashMap` and make retries ambiguous.
fn validate_metadata(fields: &[MetadataField], path: &str) -> Result<(), protocol::ProtocolError> {
    let mut names = std::collections::HashSet::new();
    for (index, field) in fields.iter().enumerate() {
        let field_path = format!("{path}[{index}].key");
        validate_identifier(&field.key, &field_path)?;
        if !names.insert(field.key.as_str()) {
            return Err(protocol::ProtocolError::invalid(
                &field_path,
                "duplicate metadata key",
            ));
        }
        workflow_protocol::payload_to_core(&field.value).map_err(|_| {
            protocol::ProtocolError::invalid(
                format!("{path}[{index}].value"),
                "invalid Temporal payload",
            )
        })?;
    }
    Ok(())
}

/// Validates the members every exact-run client request shares: with
/// `timed`, first the caller's RPC budget ([`validate_rpc_timeout`]), then the
/// namespace, workflow ID, and run selector ([`validate_run_target`]).
///
/// This is a macro rather than a function because each request is its own
/// closed Serde struct: the members share names, not a type. It expands to
/// exactly those calls, in that order, and evaluates to their `Result`.
macro_rules! validate_exact_run {
    ($request:expr) => {
        validate_run_target(&$request.namespace, &$request.workflow_id, &$request.run_id)
    };
    (timed $request:expr) => {
        validate_rpc_timeout($request.rpc_timeout_ms).and_then(|()| validate_exact_run!($request))
    };
}

/// Validates the namespace, workflow ID, and run selector that every
/// exact-run request uses to address one workflow execution.
fn validate_run_target(
    namespace: &str,
    workflow_id: &str,
    run_id: &str,
) -> Result<(), protocol::ProtocolError> {
    validate_identifier(namespace, "$.namespace")?;
    validate_identifier(workflow_id, "$.workflow_id")?;
    validate_run_selector(run_id, "$.run_id")
}

/// Bounds one free-form operator reason by the protocol string limit and
/// refuses NUL bytes, which Core's string fields cannot carry.
fn validate_reason(reason: &str) -> Result<(), protocol::ProtocolError> {
    if reason.len() > protocol::MAX_STRING_BYTES {
        return Err(protocol::ProtocolError::invalid(
            "$.reason",
            "reason exceeds the protocol string safety limit",
        ));
    }
    if reason.contains('\0') {
        return Err(protocol::ProtocolError::invalid(
            "$.reason",
            "reason contains a NUL byte",
        ));
    }
    Ok(())
}

/// Requires every payload of the list at `path` (request arguments, query
/// results, or update and workflow outcomes) to convert into a Temporal
/// payload, reporting the first one that cannot as `invalid Temporal payload`
/// at `path`.
fn validate_payloads(
    payloads: &[workflow_protocol::Payload],
    path: &str,
) -> Result<(), protocol::ProtocolError> {
    for payload in payloads {
        workflow_protocol::payload_to_core(payload)
            .map_err(|_| protocol::ProtocolError::invalid(path, "invalid Temporal payload"))?;
    }
    Ok(())
}

/// Validates one exact-run wait request.
fn validate_wait_request(value: &WaitWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(value)
}

/// Validates one exact-run cancellation request and keeps its reason bounded.
fn validate_cancel_request(value: &CancelWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(timed value)?;
    validate_identifier(&value.request_id, "$.request_id")?;
    validate_reason(&value.reason)
}

/// Validates one exact-run termination request and keeps its reason bounded.
fn validate_terminate_request(
    value: &TerminateWorkflowRequest,
) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(timed value)?;
    validate_reason(&value.reason)
}

/// Validates one exact-run reset request and its Temporal event boundary.
fn validate_reset_request(value: &ResetWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(timed value)?;
    validate_identifier(&value.request_id, "$.request_id")?;
    if value.workflow_task_finish_event_id <= 1 {
        return Err(protocol::ProtocolError::invalid(
            "$.workflow_task_finish_event_id",
            "event ID must be greater than 1",
        ));
    }
    validate_reason(&value.reason)
}

/// Validates one exact-run signal request and every payload conversion before
/// it can reach the official Temporal service.
fn validate_signal_request(value: &SignalWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(timed value)?;
    validate_identifier_fields(
        "$",
        [
            ("signal_name", &value.signal_name),
            ("request_id", &value.request_id),
        ],
    )?;
    validate_payloads(&value.input, "$.input")
}

/// Validates one exact-run query request and every argument payload before it
/// can reach Temporal's workflow service. The public API currently sends an
/// empty argument list, but accepting and validating the closed list keeps
/// protocol behavior explicit for the future typed-input extension.
fn validate_query_request(value: &QueryWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(timed value)?;
    validate_identifier(&value.query_type, "$.query_type")?;
    validate_payloads(&value.input, "$.input")
}

/// Validates one update admission request and every argument payload.
fn validate_update_request(value: &UpdateWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(timed value)?;
    validate_update_message(
        &value.update_id,
        &value.update_name,
        &value.input,
        "$.input",
    )
}

/// Validates the update half every update request shares, in order: the
/// update ID at `$.update_id`, the handler name at `$.update_name`, and the
/// argument payloads at `input_path`. The plain update request and the
/// update half of update-with-start differ only in where their arguments sit.
fn validate_update_message(
    update_id: &str,
    update_name: &str,
    input: &[workflow_protocol::Payload],
    input_path: &str,
) -> Result<(), protocol::ProtocolError> {
    validate_identifier_fields(
        "$",
        [("update_id", update_id), ("update_name", update_name)],
    )?;
    validate_payloads(input, input_path)
}

/// Validates one update poll request.
fn validate_poll_update_request(
    value: &PollWorkflowUpdateRequest,
) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(value)?;
    validate_identifier(&value.update_id, "$.update_id")
}

/// Validates update output payloads and recursive failure trees before JSON
/// serialization crosses the native boundary.
fn validate_update_response(value: &UpdateWorkflowResponse) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.update_id, "$.update_id")?;
    validate_execution(&value.execution, "$.execution")?;
    validate_update_outcome(value.outcome.as_ref())
}

/// Validates either successful update payloads or a recursive failure tree.
/// An absent outcome is valid: the update is accepted but still pending.
fn validate_update_outcome(value: Option<&UpdateOutcome>) -> Result<(), protocol::ProtocolError> {
    match value {
        None => Ok(()),
        Some(UpdateOutcome::Completed { result }) => validate_payloads(result, "$.outcome.result"),
        Some(UpdateOutcome::Failed { failure }) => {
            workflow_protocol::validate_failure(failure, "$.outcome.failure").map_err(|_| {
                protocol::ProtocolError::invalid("$.outcome.failure", "invalid Temporal failure")
            })
        }
    }
}

/// Validates a visibility request before any RPC or token decoding occurs.
fn validate_visibility_request(value: &VisibilityRequest) -> Result<(), protocol::ProtocolError> {
    validate_rpc_timeout(value.rpc_timeout_ms)?;
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_visibility_query(&value.query)?;
    if !(1..=1_000).contains(&value.page_size) {
        return Err(protocol::ProtocolError::invalid(
            "$.page_size",
            "page_size must be between 1 and 1000",
        ));
    }
    if let Some(token) = &value.next_page_token {
        if token.len() > protocol::MAX_STRING_BYTES || token.contains('\0') {
            return Err(protocol::ProtocolError::invalid(
                "$.next_page_token",
                "page token exceeds the protocol string safety limit or contains NUL",
            ));
        }
        validate_page_token(token)?;
    }
    Ok(())
}

/// Requires an opaque visibility page token to be base64, the only form in
/// which the bridge hands Temporal's token bytes across the JSON boundary.
fn validate_page_token(token: &str) -> Result<(), protocol::ProtocolError> {
    base64::engine::general_purpose::STANDARD
        .decode(token)
        .map(drop)
        .map_err(|_| {
            protocol::ProtocolError::invalid("$.next_page_token", "invalid base64 page token")
        })
}

/// Validates a history fetch request before any RPC occurs.
fn validate_history_request(value: &HistoryRequest) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(timed value)?;
    match value.page_size {
        Some(size) if !(1..=1_000).contains(&size) => Err(protocol::ProtocolError::invalid(
            "$.page_size",
            "page_size must be between 1 and 1000",
        )),
        Some(_) | None => Ok(()),
    }
}

/// Validates a describe request before any RPC occurs.
fn validate_describe_request(value: &DescribeRequest) -> Result<(), protocol::ProtocolError> {
    validate_exact_run!(timed value)
}

/// Validates a count request before any RPC occurs; the query follows the
/// same rule as a visibility listing's.
fn validate_count_request(value: &CountRequest) -> Result<(), protocol::ProtocolError> {
    validate_rpc_timeout(value.rpc_timeout_ms)?;
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_visibility_query(&value.query)
}

/// Bounds a visibility query by the protocol string limit and refuses NUL.
/// An empty query is valid and matches every execution.
fn validate_visibility_query(query: &str) -> Result<(), protocol::ProtocolError> {
    if query.len() > protocol::MAX_STRING_BYTES || query.contains('\0') {
        Err(protocol::ProtocolError::invalid(
            "$.query",
            "query exceeds the protocol string safety limit or contains NUL",
        ))
    } else {
        Ok(())
    }
}

/// Validates a reduced describe answer: concrete identities, the closed
/// status and pending-activity vocabularies, protobuf time ranges, and
/// unique, valid metadata keys and payloads.
fn validate_describe_response(value: &DescribeResponse) -> Result<(), protocol::ProtocolError> {
    validate_execution(&value.execution, "$.execution")?;
    validate_identifier_fields(
        "$",
        [
            ("workflow_type", &value.workflow_type),
            ("task_queue", &value.task_queue),
        ],
    )?;
    if !is_visibility_status_name(&value.status) || value.status == "unspecified" {
        return Err(protocol::ProtocolError::invalid(
            "$.status",
            "unknown execution status",
        ));
    }
    value.start_time.validate_at("$.start_time")?;
    value.close_time.validate_at("$.close_time")?;
    if i64::try_from(value.history_length).is_err() {
        return Err(protocol::ProtocolError::invalid(
            "$.history_length",
            "history length exceeds the signed 64-bit range",
        ));
    }
    validate_metadata(&value.memo, "$.memo")?;
    validate_metadata(&value.search_attributes, "$.search_attributes")?;
    for (index, activity) in value.pending_activities.iter().enumerate() {
        let path = format!("$.pending_activities[{index}]");
        validate_identifier_fields(
            &path,
            [
                ("activity_id", &activity.activity_id),
                ("activity_type", &activity.activity_type),
            ],
        )?;
        require(
            is_pending_activity_state_name(&activity.state),
            format!("{path}.state"),
            "unknown pending activity state",
        )?;
        if i32::try_from(activity.attempt).is_err() {
            return Err(protocol::ProtocolError::invalid(
                format!("{path}.attempt"),
                "attempt exceeds the signed 32-bit range",
            ));
        }
    }
    for (index, child) in value.pending_children.iter().enumerate() {
        let path = format!("$.pending_children[{index}]");
        validate_identifier(&child.workflow_id, &format!("{path}.workflow_id"))?;
        if let Some(run_id) = &child.run_id {
            validate_identifier(run_id, &format!("{path}.run_id"))?;
        }
        validate_identifier(&child.workflow_type, &format!("{path}.workflow_type"))?;
    }
    Ok(())
}

/// Highest `PendingActivityState` wire value this bridge names.
const MAX_KNOWN_PENDING_ACTIVITY_STATE: i32 = 5;

/// Converts Temporal's numeric `PendingActivityState` into a stable name.
///
/// Known states map to fixed lowercase names; any other value, including the
/// unspecified `0`, is `unknown_<n>` with the canonical decimal wire value,
/// as for execution statuses (#1078), so a state a newer server adds never
/// fails a whole describe.
fn pending_activity_state(state: i32) -> String {
    match state {
        1 => "scheduled".to_owned(),
        2 => "started".to_owned(),
        3 => "cancel_requested".to_owned(),
        4 => "paused".to_owned(),
        5 => "pause_requested".to_owned(),
        _ => format!("{UNKNOWN_STATUS_PREFIX}{state}"),
    }
}

/// Returns whether `state` is a name [`pending_activity_state`] can produce:
/// a fixed name, or `unknown_<n>` in canonical decimal for a value without
/// one, so one wire value has exactly one spelling.
fn is_pending_activity_state_name(state: &str) -> bool {
    match state {
        "scheduled" | "started" | "cancel_requested" | "paused" | "pause_requested" => true,
        _ => is_unknown_wire_name(state, 1..=MAX_KNOWN_PENDING_ACTIVITY_STATE),
    }
}

/// Returns whether `name` is the fallback spelling of an enum wire value this
/// bridge does not name: [`UNKNOWN_STATUS_PREFIX`] followed by the canonical
/// decimal form of a value outside `named`, so one wire value has exactly
/// one spelling. Execution statuses and pending-activity states share it.
fn is_unknown_wire_name(name: &str, named: std::ops::RangeInclusive<i32>) -> bool {
    name.strip_prefix(UNKNOWN_STATUS_PREFIX)
        .and_then(|digits| digits.parse::<i32>().ok().map(|value| (digits, value)))
        .is_some_and(|(digits, value)| !named.contains(&value) && value.to_string() == digits)
}

/// Validates every reduced visibility row and the opaque continuation token.
fn validate_visibility_page(value: &VisibilityPage) -> Result<(), protocol::ProtocolError> {
    for (index, execution) in value.executions.iter().enumerate() {
        let path = format!("$.executions[{index}]");
        validate_identifier_fields(
            &path,
            [
                ("workflow_id", &execution.workflow_id),
                ("run_id", &execution.run_id),
                ("workflow_type", &execution.workflow_type),
                ("task_queue", &execution.task_queue),
            ],
        )?;
        require(
            is_visibility_status_name(&execution.status),
            format!("{path}.status"),
            "unknown visibility status",
        )?;
    }
    value
        .next_page_token
        .as_deref()
        .map_or(Ok(()), validate_page_token)
}

/// Highest `WorkflowExecutionStatus` wire value this bridge names. Other
/// nonzero values are reported with [`UNKNOWN_STATUS_PREFIX`].
const MAX_KNOWN_VISIBILITY_STATUS: i32 = 8;

/// Prefix of the fallback status name for a `WorkflowExecutionStatus` this
/// bridge does not know, followed by the canonical decimal wire value (for
/// example `unknown_9`). The OCaml decoder accepts exactly the same shape.
const UNKNOWN_STATUS_PREFIX: &str = "unknown_";

/// Converts Temporal's numeric visibility enum into a stable status string.
///
/// Known statuses map to fixed lowercase names. A value a newer server may
/// add is reported as `unknown_<n>` rather than failing the whole listing
/// (#1078), so callers can still page through and identify the row. Only
/// `0` (unspecified) is rejected: every server sets a status on a
/// visibility row, so an omitted one is a malformed response.
fn visibility_status(status: i32) -> Result<String, ClientOperationError> {
    let known = match status {
        0 => {
            return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
                "visibility row has an unspecified execution status",
            )));
        }
        1 => "running",
        2 => "completed",
        3 => "failed",
        4 => "canceled",
        5 => "terminated",
        6 => "continued_as_new",
        7 => "timed_out",
        8 => "paused",
        _ => return Ok(format!("{UNKNOWN_STATUS_PREFIX}{status}")),
    };
    Ok(known.to_owned())
}

/// Returns whether `status` is a name [`visibility_status`] can produce (or
/// the legacy `unspecified` name the schema has always allowed). An unknown
/// status must use the canonical decimal form of a value outside the named
/// range, so one wire value has exactly one spelling.
fn is_visibility_status_name(status: &str) -> bool {
    match status {
        "running" | "completed" | "failed" | "canceled" | "terminated" | "continued_as_new"
        | "timed_out" | "paused" | "unspecified" => true,
        _ => is_unknown_wire_name(status, 0..=MAX_KNOWN_VISIBILITY_STATUS),
    }
}

/// Validates one execution reference.
fn validate_execution(value: &ExecutionRef, path: &str) -> Result<(), protocol::ProtocolError> {
    validate_execution_identity(&value.namespace, &value.workflow_id, &value.run_id, path)
}

/// Validates a concrete execution identity at `{path}.namespace`,
/// `{path}.workflow_id`, and `{path}.run_id`, in that order. Execution
/// references and successor metadata share this rule; a request's run
/// selector, which may be empty, uses [`validate_run_target`] instead.
fn validate_execution_identity(
    namespace: &str,
    workflow_id: &str,
    run_id: &str,
    path: &str,
) -> Result<(), protocol::ProtocolError> {
    validate_identifier_fields(
        path,
        [
            ("namespace", namespace),
            ("workflow_id", workflow_id),
            ("run_id", run_id),
        ],
    )
}

/// Validates one outcome: its payloads or failure first, then the successor
/// metadata it carries (required for continued-as-new).
fn validate_outcome(value: &WorkflowOutcome) -> Result<(), protocol::ProtocolError> {
    match value {
        WorkflowOutcome::Completed { result, .. } => validate_payloads(result, "$.outcome.result")?,
        WorkflowOutcome::Failed { failure, .. } => {
            workflow_protocol::validate_failure(failure, "$.outcome.failure")?
        }
        WorkflowOutcome::Cancelled { details } | WorkflowOutcome::Terminated { details } => {
            validate_payloads(details, "$.outcome.details")?
        }
        WorkflowOutcome::TimedOut { .. } | WorkflowOutcome::ContinuedAsNew { .. } => {}
    }
    outcome_successor(value).map_or(Ok(()), validate_successor_shape)
}

/// Returns the successor run an outcome names, if any: optional for
/// completion, failure, and timeout (a retry or cron successor), required for
/// continue-as-new, and impossible for cancellation and termination.
fn outcome_successor(value: &WorkflowOutcome) -> Option<&SuccessorRef> {
    match value {
        WorkflowOutcome::Completed { successor, .. }
        | WorkflowOutcome::Failed { successor, .. }
        | WorkflowOutcome::TimedOut { successor } => successor.as_ref(),
        WorkflowOutcome::ContinuedAsNew { successor } => Some(successor),
        WorkflowOutcome::Cancelled { .. } | WorkflowOutcome::Terminated { .. } => None,
    }
}

/// Validates the shape of successor metadata without assuming its parent run.
fn validate_successor_shape(value: &SuccessorRef) -> Result<(), protocol::ProtocolError> {
    validate_execution_identity(
        &value.namespace,
        &value.workflow_id,
        &value.run_id,
        "$.outcome.successor",
    )
}

/// Enforces that a successor remains in the same execution chain and is a new
/// concrete run. Temporal supplies these values, but checking them here keeps
/// a malformed server response from changing identity at the OCaml boundary.
fn validate_successor_for_execution(
    successor: &SuccessorRef,
    execution: &ExecutionRef,
) -> Result<(), protocol::ProtocolError> {
    validate_successor_shape(successor)?;
    if successor.namespace != execution.namespace {
        return Err(protocol::ProtocolError::invalid(
            "$.outcome.successor.namespace",
            "successor namespace does not match the waited execution",
        ));
    }
    if successor.workflow_id != execution.workflow_id {
        return Err(protocol::ProtocolError::invalid(
            "$.outcome.successor.workflow_id",
            "successor workflow ID does not match the waited execution",
        ));
    }
    if successor.run_id == execution.run_id {
        return Err(protocol::ProtocolError::invalid(
            "$.outcome.successor.run_id",
            "successor run ID must differ from the waited run",
        ));
    }
    Ok(())
}

/// Validates a complete wait response. The execution echoes the request, so
/// its run ID is empty when the wait selected the workflow's current run:
/// Temporal's history response does not name the run it resolved. A successor
/// still has to name a concrete run, which then differs from the empty
/// selector automatically.
fn validate_wait_response(value: &WaitWorkflowResponse) -> Result<(), protocol::ProtocolError> {
    validate_identifier_fields(
        "$.execution",
        [
            ("namespace", &value.execution.namespace),
            ("workflow_id", &value.execution.workflow_id),
        ],
    )?;
    validate_run_selector(&value.execution.run_id, "$.execution.run_id")?;
    validate_outcome(&value.outcome)?;
    outcome_successor(&value.outcome).map_or(Ok(()), |successor| {
        validate_successor_for_execution(successor, &value.execution)
    })
}

/// Serializes, strictly reparses, and validates a client response.
fn encode_document<T: Serialize + for<'de> Deserialize<'de> + Clone + PartialEq>(
    value: &T,
) -> Result<String, protocol::ProtocolError> {
    let output = serde_json::to_string(value)
        .map_err(|_| protocol::ProtocolError::invalid("$", "could not encode client JSON"))?;
    protocol::decode_payload_object(&output)?;
    let reparsed: T = serde_json::from_str(&output)
        .map_err(|_| protocol::ProtocolError::invalid("$", "client JSON did not round trip"))?;
    // A second typed parse proves serde's closed-shape checks ran on output;
    // semantic validators are called by the operation-specific caller.
    if reparsed != *value {
        return Err(protocol::ProtocolError::invalid(
            "$",
            "client JSON did not preserve its typed value",
        ));
    }
    Ok(output)
}

/// Validates the run selector of a client request. An empty string selects
/// the workflow's current run, which Temporal resolves when it handles the
/// RPC (#791); any other value must be a valid identifier naming one exact
/// run. Responses never use this rule: a run Temporal reports is always a
/// concrete, non-empty identifier.
fn validate_run_selector(value: &str, path: &str) -> Result<(), protocol::ProtocolError> {
    if value.is_empty() {
        Ok(())
    } else {
        validate_identifier(value, path)
    }
}

/// Validates named identifier members of one document object, in order, each
/// at `{path}.{name}`. Pairing every member with its name keeps the reported
/// path from drifting away from the field that was checked.
fn validate_identifier_fields<'a, S: AsRef<str> + ?Sized + 'a>(
    path: &str,
    fields: impl IntoIterator<Item = (&'a str, &'a S)>,
) -> Result<(), protocol::ProtocolError> {
    fields.into_iter().try_for_each(|(name, value)| {
        validate_identifier(value.as_ref(), &format!("{path}.{name}"))
    })
}

/// Refuses a document whose `condition` does not hold, reporting `message`
/// at `path`: acknowledgement flags and the closed vocabularies of response
/// rows use it.
fn require(
    condition: bool,
    path: impl Into<String>,
    message: &'static str,
) -> Result<(), protocol::ProtocolError> {
    if condition {
        Ok(())
    } else {
        Err(protocol::ProtocolError::invalid(path, message))
    }
}

/// Validates a bounded nonempty UTF-8 identifier.
fn validate_identifier(value: &str, path: &str) -> Result<(), protocol::ProtocolError> {
    if value.is_empty() || value.len() > protocol::MAX_STRING_BYTES {
        Err(protocol::ProtocolError::invalid(
            path,
            "identifier is empty or exceeds the protocol string safety limit",
        ))
    } else if value.contains('\0') {
        Err(protocol::ProtocolError::invalid(
            path,
            "identifier contains a NUL byte",
        ))
    } else {
        Ok(())
    }
}

// Unit tests live in a separate file (repository rule); the `#[path]` module
// keeps them able to reach this file's private helpers.
#[cfg(test)]
#[path = "../tests/support/client_protocol_unit.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/support/client_update.rs"]
mod update_acceptance_tests;

#[cfg(test)]
#[path = "../tests/support/client_retry.rs"]
mod client_retry_tests;

#[cfg(test)]
#[path = "../tests/support/client_errors.rs"]
mod client_error_tests;

#[cfg(test)]
#[path = "../tests/support/client_current_run.rs"]
mod current_run_tests;

#[cfg(test)]
#[path = "../tests/support/client_start_schedule.rs"]
mod start_schedule_tests;

//! Closed JSON protocol and Core adapter for the first native client slice.
//!
//! This module deliberately exposes neither `temporalio_client::Client` nor
//! protobuf values to OCaml.  The bridge accepts small, lossless request
//! documents for starting a workflow, observing one exact run, and requesting
//! cancellation and output-only query operations against that same exact run.
//! A wait never follows a continued-as-new successor: callers can inspect the
//! successor metadata and decide what to do in OCaml.

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use temporalio_client::Connection;
use temporalio_client::tonic::{Code, IntoRequest, Status};
use temporalio_common::protos::temporal::api::{
    common::v1::{Memo, Payloads, SearchAttributes, WorkflowExecution, WorkflowType},
    enums::v1::{HistoryEventFilterType, UpdateWorkflowExecutionLifecycleStage},
    history::v1::{HistoryEvent, history_event::Attributes},
    query::v1::WorkflowQuery,
    taskqueue::v1::TaskQueue,
    update::{self, v1::WaitPolicy},
    workflowservice::v1::{
        GetWorkflowExecutionHistoryRequest, ListWorkflowExecutionsRequest,
        PollWorkflowExecutionUpdateRequest, QueryWorkflowRequest as QueryWorkflowExecutionRequest,
        RequestCancelWorkflowExecutionRequest, ResetWorkflowExecutionRequest,
        SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
        TerminateWorkflowExecutionRequest, UpdateWorkflowExecutionRequest,
    },
};

use crate::{protocol, workflow_protocol};

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
}

/// Successful result returned by the start operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartWorkflowResponse {
    /// Execution allocated by Temporal Server.
    pub execution: ExecutionRef,
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

/// Request to wait for one exact workflow run.
///
/// There is intentionally no `follow_runs` field.  This ABI operation has
/// fixed `follow_runs = false` semantics, so a continued-as-new event is a
/// terminal result for the requested run and cannot silently switch identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitWorkflowRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Concrete run identifier to observe without following successors.
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
    /// Concrete run identifier; an empty run is never treated as "latest".
    pub run_id: String,
    /// Caller-owned idempotency key for the cancellation RPC.
    pub request_id: String,
    /// Operator-facing reason copied to Temporal. Empty is permitted.
    pub reason: String,
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
    /// Concrete run identifier; an empty run is never treated as "latest".
    pub run_id: String,
    /// Operator-facing reason copied to Temporal.
    pub reason: String,
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
    /// Run whose workflow-task history supplies the reset point.
    pub run_id: String,
    /// Caller-owned idempotency key for this reset operation.
    pub request_id: String,
    /// Operator-facing reason copied to Temporal; empty is permitted.
    pub reason: String,
    /// Workflow task finish/start event at which the new run should begin.
    pub workflow_task_finish_event_id: i64,
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
    /// Concrete run identifier; an empty run is never treated as "latest".
    pub run_id: String,
    /// Registered Temporal signal name.
    pub signal_name: String,
    /// Caller-owned idempotency key for this logical signal operation.
    pub request_id: String,
    /// Ordered signal input payloads.
    pub input: Vec<workflow_protocol::Payload>,
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
    /// Concrete run identifier; an empty run is never treated as "latest".
    pub run_id: String,
    /// Registered workflow query name.
    pub query_type: String,
    /// Ordered query argument payloads; currently required to be empty by the
    /// public OCaml API, but validated and forwarded losslessly if populated.
    pub input: Vec<workflow_protocol::Payload>,
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
    /// Concrete run identifier; an empty run is never treated as latest.
    pub run_id: String,
    /// Caller-owned workflow-scoped update idempotency key.
    pub update_id: String,
    /// Registered workflow update handler name.
    pub update_name: String,
    /// Ordered update argument payloads.
    pub input: Vec<workflow_protocol::Payload>,
}

/// Request to poll a previously accepted workflow update until completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollWorkflowUpdateRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Concrete run identifier returned by start or retained by the caller.
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
    /// Exact execution used by the update.
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

/// Response containing the exact run identity and its terminal outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitWorkflowResponse {
    /// Run whose close event was observed.
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
    /// Core returned an event or payload outside this closed semantic slice.
    Core(workflow_protocol::CoreConversionError),
}

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
    /// The server allocated a concrete run.
    Accepted {
        /// Exact execution returned by Temporal.
        execution: ExecutionRef,
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
        StartWorkflowOutcomeDocument::Accepted { execution } => {
            validate_execution(execution, "$.execution")?
        }
        StartWorkflowOutcomeDocument::Rejected { error } => match error {
            ClientErrorDocument::AlreadyStarted {
                workflow_id,
                existing_run_id,
            } => {
                validate_identifier(workflow_id, "$.error.workflow_id")?;
                if let Some(run_id) = existing_run_id {
                    validate_identifier(run_id, "$.error.existing_run_id")?;
                }
            }
            ClientErrorDocument::Rpc { code } => validate_identifier(code, "$.error.code")?,
            ClientErrorDocument::Protocol { code } => validate_identifier(code, "$.error.code")?,
        },
        StartWorkflowOutcomeDocument::Unknown {
            request_id,
            workflow_id,
        } => {
            validate_identifier(request_id, "$.request_id")?;
            validate_identifier(workflow_id, "$.workflow_id")?;
        }
    }
    Ok(())
}

impl ClientOperationError {
    /// Encodes a privacy-safe structured error body for the ABI error buffer.
    pub(crate) fn to_json(&self) -> String {
        let document = error_document(self);
        // All variants contain validated identifiers or a bounded tonic code;
        // serialization cannot fail.  Keep a defensive fallback that cannot
        // expose a Rust panic through the ABI if a future variant changes.
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
            Self::AlreadyStarted { .. } => false,
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

/// Strictly parses one start request after duplicate-key and byte-limit checks.
pub fn decode_start_request(input: &str) -> Result<StartWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_payload_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client start request"))?;
    validate_start_request(&request)?;
    Ok(request)
}

/// Compares two validated start requests using every logical field, including
/// workflow type, task queue, and binary payloads.  The ABI uses this when a
/// caller retries a pending request ID: only an exact semantic retry may reuse
/// the existing ticket, while a changed request is rejected as a protocol
/// error instead of being silently aliased to another start.
pub(crate) fn same_start_request(
    left: &StartWorkflowRequest,
    right: &StartWorkflowRequest,
) -> bool {
    left == right
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

/// Strictly parses one exact-run wait request.
pub fn decode_wait_request(input: &str) -> Result<WaitWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client wait request"))?;
    validate_wait_request(&request)?;
    Ok(request)
}

/// Strictly parses one exact-run cancellation request.
pub fn decode_cancel_request(
    input: &str,
) -> Result<CancelWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client cancel request"))?;
    validate_cancel_request(&request)?;
    Ok(request)
}

/// Strictly parses one exact-run termination request.
pub fn decode_terminate_request(
    input: &str,
) -> Result<TerminateWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client terminate request"))?;
    validate_terminate_request(&request)?;
    Ok(request)
}

/// Strictly parses one exact-run reset request.
pub fn decode_reset_request(input: &str) -> Result<ResetWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client reset request"))?;
    validate_reset_request(&request)?;
    Ok(request)
}

/// Encodes and reparses the new execution identity returned after reset.
pub fn encode_reset_response(
    response: &ResetWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    validate_identifier(&response.execution.namespace, "$.execution.namespace")?;
    validate_identifier(&response.execution.workflow_id, "$.execution.workflow_id")?;
    validate_identifier(&response.execution.run_id, "$.execution.run_id")?;
    encode_document(response)
}

/// Strictly parses one exact-run signal request.
pub fn decode_signal_request(
    input: &str,
) -> Result<SignalWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client signal request"))?;
    validate_signal_request(&request)?;
    Ok(request)
}

/// Strictly parses one exact-run output-only query request.
pub fn decode_query_request(input: &str) -> Result<QueryWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client query request"))?;
    validate_query_request(&request)?;
    Ok(request)
}

/// Strictly parses a request that starts one workflow update.
pub fn decode_update_request(
    input: &str,
) -> Result<UpdateWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client update request"))?;
    validate_update_request(&request)?;
    Ok(request)
}

/// Strictly parses a request that polls a previously admitted update.
pub fn decode_poll_update_request(
    input: &str,
) -> Result<PollWorkflowUpdateRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client update poll request"))?;
    validate_poll_update_request(&request)?;
    Ok(request)
}

/// Strictly parses one bounded visibility page request.
pub fn decode_visibility_request(
    input: &str,
) -> Result<VisibilityRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid visibility request"))?;
    validate_visibility_request(&request)?;
    Ok(request)
}

/// Encodes and reparses a start response before ownership leaves Rust.
pub fn encode_start_response(
    response: &StartWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    validate_execution(&response.execution, "$.execution")?;
    encode_document(response)
}

/// Encodes and reparses a positive cancellation acknowledgement before it
/// crosses the native boundary.
pub fn encode_cancel_response(
    response: &CancelWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    if !response.acknowledged {
        return Err(protocol::ProtocolError::invalid(
            "$.acknowledged",
            "cancellation acknowledgement must be true",
        ));
    }
    encode_document(response)
}

/// Encodes and reparses a positive termination acknowledgement.
pub fn encode_terminate_response(
    response: &TerminateWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    if !response.acknowledged {
        return Err(protocol::ProtocolError::invalid(
            "$.acknowledged",
            "termination acknowledgement must be true",
        ));
    }
    encode_document(response)
}

/// Encodes and reparses a positive signal acknowledgement before it crosses
/// the native boundary.
pub fn encode_signal_response(
    response: &SignalWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    if !response.acknowledged {
        return Err(protocol::ProtocolError::invalid(
            "$.acknowledged",
            "signal acknowledgement must be true",
        ));
    }
    encode_document(response)
}

/// Encodes and reparses one query result before ownership leaves Rust.
pub fn encode_query_response(
    response: &QueryWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    validate_query_response(response)?;
    encode_document(response)
}

/// Encodes and reparses update admission before ownership leaves Rust.
pub fn encode_update_response(
    response: &UpdateWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    validate_update_response(response)?;
    encode_document(response)
}

/// Encodes and reparses a bounded update poll response before ownership leaves
/// Rust. A missing outcome is a normal pending result, not an error.
pub fn encode_poll_update_response(
    response: &PollWorkflowUpdateResponse,
) -> Result<String, protocol::ProtocolError> {
    validate_poll_update_response(response)?;
    encode_document(response)
}

/// Encodes and reparses one visibility page before it crosses the ABI.
pub fn encode_visibility_response(
    response: &VisibilityPage,
) -> Result<String, protocol::ProtocolError> {
    validate_visibility_page(response)?;
    encode_document(response)
}

/// Lists one visibility page through Temporal's official workflow service.
/// The opaque page token is copied as bytes only inside Rust and is base64 at
/// the JSON boundary; no server-owned protobuf or byte buffer is retained.
pub async fn list_visibility(
    connection: Connection,
    request: VisibilityRequest,
) -> Result<VisibilityPage, ClientOperationError> {
    const VISIBILITY_RPC_TIMEOUT: Duration = Duration::from_secs(10);
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
    let mut service = connection.workflow_service();
    let response = match tokio::time::timeout(
        VISIBILITY_RPC_TIMEOUT,
        service.list_workflow_executions(
            ListWorkflowExecutionsRequest {
                namespace: request.namespace,
                query: request.query,
                page_size: request.page_size as i32,
                next_page_token: token.unwrap_or_default(),
            }
            .into_request(),
        ),
    )
    .await
    {
        Ok(result) => result.map_err(map_rpc_status)?.into_inner(),
        Err(_) => {
            return Err(ClientOperationError::Rpc {
                code: "deadline_exceeded".to_owned(),
            });
        }
    };
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

/// Requests cancellation of one exact run through Temporal's official
/// workflow service. The connection is cloned by the owner Domain and no
/// Tokio task calls back into OCaml.
pub async fn cancel_workflow(
    connection: Connection,
    request: CancelWorkflowRequest,
) -> Result<CancelWorkflowResponse, ClientOperationError> {
    // A cancellation acknowledgement is a control-plane RPC, not a workflow
    // wait. Bound it so a stalled server cannot hold the owner Domain mailbox
    // forever; callers can retry the same request ID when the outcome is
    // uncertain.
    const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(1);
    let mut service = connection.workflow_service();
    let request = RequestCancelWorkflowExecutionRequest {
        namespace: request.namespace,
        workflow_execution: Some(WorkflowExecution {
            workflow_id: request.workflow_id,
            run_id: request.run_id,
        }),
        identity: connection.identity().to_owned(),
        request_id: request.request_id,
        reason: request.reason,
        ..Default::default()
    };
    match tokio::time::timeout(
        CONTROL_RPC_TIMEOUT,
        service.request_cancel_workflow_execution(request.into_request()),
    )
    .await
    {
        Ok(result) => {
            result.map_err(map_rpc_status)?;
        }
        Err(_) => {
            return Err(ClientOperationError::Rpc {
                code: "deadline_exceeded".to_owned(),
            });
        }
    }
    Ok(CancelWorkflowResponse { acknowledged: true })
}

/// Resets one exact workflow run through Temporal's official workflow service.
/// The event ID and request ID are copied into protobuf owned by this RPC; no
/// OCaml memory is retained after the owner Domain call returns.
pub async fn reset_workflow(
    connection: Connection,
    request: ResetWorkflowRequest,
) -> Result<ResetWorkflowResponse, ClientOperationError> {
    const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(1);
    let mut service = connection.workflow_service();
    let namespace = request.namespace.clone();
    let workflow_id = request.workflow_id.clone();
    let response = match tokio::time::timeout(
        CONTROL_RPC_TIMEOUT,
        service.reset_workflow_execution(
            ResetWorkflowExecutionRequest {
                namespace: request.namespace,
                workflow_execution: Some(WorkflowExecution {
                    workflow_id: request.workflow_id,
                    run_id: request.run_id,
                }),
                reason: request.reason,
                workflow_task_finish_event_id: request.workflow_task_finish_event_id,
                request_id: request.request_id,
                identity: connection.identity().to_owned(),
                ..Default::default()
            }
            .into_request(),
        ),
    )
    .await
    {
        Ok(result) => result.map_err(map_rpc_status)?.into_inner(),
        Err(_) => {
            return Err(ClientOperationError::Rpc {
                code: "deadline_exceeded".to_owned(),
            });
        }
    };
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
    validate_identifier(&response.execution.namespace, "$.execution.namespace")
        .and_then(|_| {
            validate_identifier(&response.execution.workflow_id, "$.execution.workflow_id")
        })
        .and_then(|_| validate_identifier(&response.execution.run_id, "$.execution.run_id"))
        .map_err(|_| {
            ClientOperationError::Core(workflow_protocol::invalid_core(
                "Temporal reset response had invalid execution identity",
            ))
        })?;
    Ok(response)
}

/// Terminates one exact workflow run through Temporal's official service.
/// Termination is intentionally separate from cancellation: the server writes
/// an immutable terminated event immediately and does not wait for workflow
/// code to observe a cancellation request.
pub async fn terminate_workflow(
    connection: Connection,
    request: TerminateWorkflowRequest,
) -> Result<TerminateWorkflowResponse, ClientOperationError> {
    const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(1);
    let mut service = connection.workflow_service();
    let request = TerminateWorkflowExecutionRequest {
        namespace: request.namespace,
        workflow_execution: Some(WorkflowExecution {
            workflow_id: request.workflow_id,
            run_id: request.run_id,
        }),
        reason: request.reason,
        identity: connection.identity().to_owned(),
        ..Default::default()
    };
    match tokio::time::timeout(
        CONTROL_RPC_TIMEOUT,
        service.terminate_workflow_execution(request.into_request()),
    )
    .await
    {
        Ok(result) => {
            result.map_err(map_rpc_status)?;
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

/// Delivers one signal to one exact workflow run through Temporal's official
/// workflow service. The request identity and payloads are copied into the
/// protobuf message; no OCaml memory is retained by the asynchronous RPC.
pub async fn signal_workflow(
    connection: Connection,
    request: SignalWorkflowRequest,
) -> Result<SignalWorkflowResponse, ClientOperationError> {
    // Signal delivery is a control-plane acknowledgement rather than a wait
    // for workflow code to process the message. Keep the owner Domain
    // responsive when the server is unavailable; callers can retry the same
    // request ID after an uncertain timeout.
    const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(1);
    let payloads = payloads_to_core(&request.input).map_err(ClientOperationError::Core)?;
    let mut service = connection.workflow_service();
    let request = SignalWorkflowExecutionRequest {
        namespace: request.namespace,
        workflow_execution: Some(WorkflowExecution {
            workflow_id: request.workflow_id,
            run_id: request.run_id,
        }),
        signal_name: request.signal_name,
        input: Some(payloads),
        identity: connection.identity().to_owned(),
        request_id: request.request_id,
        ..Default::default()
    };
    match tokio::time::timeout(
        CONTROL_RPC_TIMEOUT,
        service.signal_workflow_execution(request.into_request()),
    )
    .await
    {
        Ok(result) => {
            result.map_err(map_rpc_status)?;
        }
        Err(_) => {
            return Err(ClientOperationError::Rpc {
                code: "deadline_exceeded".to_owned(),
            });
        }
    }
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
    // incorrectly rejects healthy slow workers, so use the same bounded
    // service budget as the rest of this client slice while the public API
    // does not yet expose a caller-selected deadline.
    const QUERY_RPC_TIMEOUT: Duration = Duration::from_secs(30);
    let query_args = payloads_to_core(&request.input).map_err(ClientOperationError::Core)?;
    let mut service = connection.workflow_service();
    let request = QueryWorkflowExecutionRequest {
        namespace: request.namespace,
        execution: Some(WorkflowExecution {
            workflow_id: request.workflow_id,
            run_id: request.run_id,
        }),
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
    let response = match tokio::time::timeout(
        QUERY_RPC_TIMEOUT,
        service.query_workflow(request.into_request()),
    )
    .await
    {
        Ok(result) => result.map_err(map_rpc_status)?.into_inner(),
        Err(_) => {
            return Err(ClientOperationError::Rpc {
                code: "deadline_exceeded".to_owned(),
            });
        }
    };
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

/// Starts one update through Temporal's workflow service and waits until the
/// workflow worker has accepted it. Temporal deliberately does not allow the
/// asynchronous `Admitted` stage for the public start-update flow; `Accepted`
/// is the interoperable stage for a handle that will be polled separately.
pub async fn update_workflow(
    connection: Connection,
    request: UpdateWorkflowRequest,
) -> Result<UpdateWorkflowResponse, ClientOperationError> {
    let input = payloads_to_core(&request.input).map_err(ClientOperationError::Core)?;
    let mut service = connection.workflow_service();
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        service.update_workflow_execution(
            UpdateWorkflowExecutionRequest {
                namespace: request.namespace.clone(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id: request.workflow_id.clone(),
                    run_id: request.run_id.clone(),
                }),
                wait_policy: Some(WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Accepted as i32,
                }),
                request: Some(update::v1::Request {
                    meta: Some(update::v1::Meta {
                        update_id: request.update_id.clone(),
                        identity: connection.identity().to_owned(),
                    }),
                    input: Some(update::v1::Input {
                        name: request.update_name,
                        args: Some(input),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }
            .into_request(),
        ),
    )
    .await
    .map_err(|_| ClientOperationError::Rpc {
        code: "deadline_exceeded".to_owned(),
    })?
    .map_err(map_rpc_status)?
    .into_inner();
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
        namespace: request.namespace,
        workflow_id: execution.workflow_id,
        run_id: if execution.run_id.is_empty() {
            request.run_id.clone()
        } else {
            execution.run_id
        },
    };
    if execution.workflow_id != request.workflow_id || execution.run_id != request.run_id {
        return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal update response changed workflow execution",
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

/// Polls one update until completion, bounded so the supervisor remains
/// responsive to shutdown and unrelated client requests.
pub async fn poll_workflow_update(
    connection: Connection,
    request: PollWorkflowUpdateRequest,
) -> Result<PollWorkflowUpdateResponse, ClientOperationError> {
    let mut service = connection.workflow_service();
    let response = match tokio::time::timeout(
        Duration::from_secs(30),
        service.poll_workflow_execution_update(
            PollWorkflowExecutionUpdateRequest {
                namespace: request.namespace,
                update_ref: Some(update::v1::UpdateRef {
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id: request.workflow_id,
                        run_id: request.run_id,
                    }),
                    update_id: request.update_id,
                }),
                identity: connection.identity().to_owned(),
                wait_policy: Some(WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                }),
            }
            .into_request(),
        ),
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

/// Encodes and reparses an exact-run wait response before ownership leaves Rust.
pub fn encode_wait_response(
    response: &WaitWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    validate_wait_response(response)?;
    encode_document(response)
}

/// Maximum time one start RPC may block the supervisor owner or a Tokio task.
///
/// A hung Temporal start must not pin the sole owner Domain forever (sync ABI)
/// or exhaust the pending-start registry (async begin). The timeout is applied
/// outside the gRPC future so cancellation drops the in-flight request. The
/// resulting `deadline_exceeded` status is classified as an uncertain start:
/// Temporal may have accepted the workflow after the client gave up.
const START_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts one workflow through Core's raw workflow service trait.
pub async fn start_workflow(
    connection: Connection,
    request: StartWorkflowRequest,
) -> Result<StartWorkflowResponse, ClientOperationError> {
    let payloads = payloads_to_core(&request.input).map_err(ClientOperationError::Core)?;
    let memo = metadata_to_memo(&request.memo).map_err(ClientOperationError::Core)?;
    let search_attributes = metadata_to_search_attributes(&request.search_attributes)
        .map_err(ClientOperationError::Core)?;
    let workflow_id = request.workflow_id.clone();
    let mut service = connection.workflow_service();
    let response = match tokio::time::timeout(
        START_RPC_TIMEOUT,
        service.start_workflow_execution(
            StartWorkflowExecutionRequest {
                namespace: request.namespace.clone(),
                input: Some(payloads),
                workflow_id: request.workflow_id,
                workflow_type: Some(WorkflowType {
                    name: request.workflow_type,
                }),
                task_queue: Some(TaskQueue {
                    name: request.task_queue,
                    kind: 0,
                    normal_name: String::new(),
                }),
                memo,
                search_attributes,
                identity: connection.identity().to_owned(),
                request_id: request.request_id,
                // All other start policies intentionally use server defaults;
                // metadata is the only optional field in this protocol slice.
                ..Default::default()
            }
            .into_request(),
        ),
    )
    .await
    {
        Ok(result) => result.map_err(|status| map_start_status(&workflow_id, status))?,
        Err(_elapsed) => {
            return Err(ClientOperationError::Rpc {
                code: "deadline_exceeded".to_owned(),
            });
        }
    }
    .into_inner();

    let run_id = response.run_id;
    if run_id.is_empty() {
        return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal start response omitted run ID",
        )));
    }
    let execution = ExecutionRef {
        namespace: request.namespace,
        workflow_id,
        run_id,
    };
    validate_execution(&execution, "$.execution").map_err(|_| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal start response had invalid execution identity",
        ))
    })?;
    Ok(StartWorkflowResponse { execution })
}

/// Waits for the exact run named by `request`, never following successors.
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
    let mut service = connection.workflow_service();

    loop {
        let response = service
            .get_workflow_execution_history(
                GetWorkflowExecutionHistoryRequest {
                    namespace: request.namespace.clone(),
                    execution: Some(WorkflowExecution {
                        workflow_id: request.workflow_id.clone(),
                        run_id: request.run_id.clone(),
                    }),
                    next_page_token: std::mem::take(&mut next_page_token),
                    skip_archival: true,
                    wait_new_event: true,
                    history_event_filter_type: HistoryEventFilterType::CloseEvent as i32,
                    ..Default::default()
                }
                .into_request(),
            )
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

/// Converts semantic payloads into the protobuf payload container.
fn payloads_to_core(
    values: &[workflow_protocol::Payload],
) -> Result<Payloads, workflow_protocol::CoreConversionError> {
    Ok(Payloads {
        payloads: values
            .iter()
            .map(workflow_protocol::payload_to_core)
            .collect::<Result<_, _>>()?,
    })
}

/// Converts named OCaml metadata to Temporal's memo map while preserving the
/// validation guarantees established by [`validate_metadata`].
fn metadata_to_memo(
    fields: &[MetadataField],
) -> Result<Option<Memo>, workflow_protocol::CoreConversionError> {
    if fields.is_empty() {
        return Ok(None);
    }
    let mut values = HashMap::with_capacity(fields.len());
    for field in fields {
        let payload = workflow_protocol::payload_to_core(&field.value)?;
        values.insert(field.key.clone(), payload);
    }
    Ok(Some(Memo { fields: values }))
}

/// Converts named OCaml search attributes to Temporal's indexed-field map.
/// Temporal represents each indexed field as one payload; callers that need
/// multiple values should encode a list using their normal codec first.
fn metadata_to_search_attributes(
    fields: &[MetadataField],
) -> Result<Option<SearchAttributes>, workflow_protocol::CoreConversionError> {
    if fields.is_empty() {
        return Ok(None);
    }
    let mut values = HashMap::with_capacity(fields.len());
    for field in fields {
        let payload = workflow_protocol::payload_to_core(&field.value)?;
        values.insert(field.key.clone(), payload);
    }
    Ok(Some(SearchAttributes {
        indexed_fields: values,
    }))
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
fn rpc_code(code: Code) -> &'static str {
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
    validate_identifier(&value.request_id, "$.request_id")?;
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.workflow_type, "$.workflow_type")?;
    validate_identifier(&value.task_queue, "$.task_queue")?;
    for payload in &value.input {
        workflow_protocol::payload_to_core(payload)
            .map_err(|_| protocol::ProtocolError::invalid("$.input", "invalid Temporal payload"))?;
    }
    validate_metadata(&value.memo, "$.memo")?;
    validate_metadata(&value.search_attributes, "$.search_attributes")?;
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

/// Validates one exact-run wait request.
fn validate_wait_request(value: &WaitWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.run_id, "$.run_id")?;
    Ok(())
}

/// Validates one exact-run cancellation request and keeps its reason bounded.
fn validate_cancel_request(value: &CancelWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.run_id, "$.run_id")?;
    validate_identifier(&value.request_id, "$.request_id")?;
    if value.reason.len() > protocol::MAX_STRING_BYTES {
        return Err(protocol::ProtocolError::invalid(
            "$.reason",
            "reason exceeds the protocol string safety limit",
        ));
    }
    if value.reason.contains('\0') {
        return Err(protocol::ProtocolError::invalid(
            "$.reason",
            "reason contains a NUL byte",
        ));
    }
    Ok(())
}

/// Validates one exact-run termination request and keeps its reason bounded.
fn validate_terminate_request(
    value: &TerminateWorkflowRequest,
) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.run_id, "$.run_id")?;
    if value.reason.len() > protocol::MAX_STRING_BYTES {
        return Err(protocol::ProtocolError::invalid(
            "$.reason",
            "reason exceeds the protocol string safety limit",
        ));
    }
    if value.reason.contains('\0') {
        return Err(protocol::ProtocolError::invalid(
            "$.reason",
            "reason contains a NUL byte",
        ));
    }
    Ok(())
}

/// Validates one exact-run reset request and its non-negative event boundary.
fn validate_reset_request(value: &ResetWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.run_id, "$.run_id")?;
    validate_identifier(&value.request_id, "$.request_id")?;
    if value.workflow_task_finish_event_id < 0 {
        return Err(protocol::ProtocolError::invalid(
            "$.workflow_task_finish_event_id",
            "event ID must be non-negative",
        ));
    }
    if value.reason.len() > protocol::MAX_STRING_BYTES {
        return Err(protocol::ProtocolError::invalid(
            "$.reason",
            "reason exceeds the protocol string safety limit",
        ));
    }
    if value.reason.contains('\0') {
        return Err(protocol::ProtocolError::invalid(
            "$.reason",
            "reason contains a NUL byte",
        ));
    }
    Ok(())
}

/// Validates one exact-run signal request and every payload conversion before
/// it can reach the official Temporal service.
fn validate_signal_request(value: &SignalWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.run_id, "$.run_id")?;
    validate_identifier(&value.signal_name, "$.signal_name")?;
    validate_identifier(&value.request_id, "$.request_id")?;
    for payload in &value.input {
        workflow_protocol::payload_to_core(payload)
            .map_err(|_| protocol::ProtocolError::invalid("$.input", "invalid Temporal payload"))?;
    }
    Ok(())
}

/// Validates one exact-run query request and every argument payload before it
/// can reach Temporal's workflow service. The public API currently sends an
/// empty argument list, but accepting and validating the closed list keeps
/// protocol behavior explicit for the future typed-input extension.
fn validate_query_request(value: &QueryWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.run_id, "$.run_id")?;
    validate_identifier(&value.query_type, "$.query_type")?;
    for payload in &value.input {
        workflow_protocol::payload_to_core(payload)
            .map_err(|_| protocol::ProtocolError::invalid("$.input", "invalid Temporal payload"))?;
    }
    Ok(())
}

/// Validates every payload in a successful query response before it leaves the
/// Rust-owned connection and crosses the JSON ABI.
fn validate_query_response(value: &QueryWorkflowResponse) -> Result<(), protocol::ProtocolError> {
    for payload in &value.result {
        workflow_protocol::payload_to_core(payload).map_err(|_| {
            protocol::ProtocolError::invalid("$.result", "invalid Temporal payload")
        })?;
    }
    Ok(())
}

/// Validates one update admission request and every argument payload.
fn validate_update_request(value: &UpdateWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.run_id, "$.run_id")?;
    validate_identifier(&value.update_id, "$.update_id")?;
    validate_identifier(&value.update_name, "$.update_name")?;
    for payload in &value.input {
        workflow_protocol::payload_to_core(payload)
            .map_err(|_| protocol::ProtocolError::invalid("$.input", "invalid Temporal payload"))?;
    }
    Ok(())
}

/// Validates one update poll request.
fn validate_poll_update_request(
    value: &PollWorkflowUpdateRequest,
) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.run_id, "$.run_id")?;
    validate_identifier(&value.update_id, "$.update_id")?;
    Ok(())
}

/// Validates update output payloads and recursive failure trees before JSON
/// serialization crosses the native boundary.
fn validate_update_response(value: &UpdateWorkflowResponse) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.update_id, "$.update_id")?;
    validate_execution(&value.execution, "$.execution")?;
    if let Some(outcome) = &value.outcome {
        validate_update_outcome(outcome)?;
    }
    Ok(())
}

/// Validates an optional bounded poll outcome.
fn validate_poll_update_response(
    value: &PollWorkflowUpdateResponse,
) -> Result<(), protocol::ProtocolError> {
    if let Some(outcome) = &value.outcome {
        validate_update_outcome(outcome)?;
    }
    Ok(())
}

/// Validates either successful update payloads or a recursive failure tree.
fn validate_update_outcome(value: &UpdateOutcome) -> Result<(), protocol::ProtocolError> {
    match value {
        UpdateOutcome::Completed { result } => {
            for payload in result {
                workflow_protocol::payload_to_core(payload).map_err(|_| {
                    protocol::ProtocolError::invalid("$.outcome.result", "invalid Temporal payload")
                })?;
            }
        }
        UpdateOutcome::Failed { failure } => {
            workflow_protocol::validate_failure(failure, "$.outcome.failure").map_err(|_| {
                protocol::ProtocolError::invalid("$.outcome.failure", "invalid Temporal failure")
            })?;
        }
    }
    Ok(())
}

/// Validates a visibility request before any RPC or token decoding occurs.
fn validate_visibility_request(value: &VisibilityRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    if value.query.len() > protocol::MAX_STRING_BYTES || value.query.contains('\0') {
        return Err(protocol::ProtocolError::invalid(
            "$.query",
            "query exceeds the protocol string safety limit or contains NUL",
        ));
    }
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
        base64::engine::general_purpose::STANDARD
            .decode(token)
            .map_err(|_| {
                protocol::ProtocolError::invalid("$.next_page_token", "invalid base64 page token")
            })?;
    }
    Ok(())
}

/// Validates every reduced visibility row and the opaque continuation token.
fn validate_visibility_page(value: &VisibilityPage) -> Result<(), protocol::ProtocolError> {
    for (index, execution) in value.executions.iter().enumerate() {
        let path = format!("$.executions[{index}]");
        validate_identifier(&execution.workflow_id, &format!("{path}.workflow_id"))?;
        validate_identifier(&execution.run_id, &format!("{path}.run_id"))?;
        validate_identifier(&execution.workflow_type, &format!("{path}.workflow_type"))?;
        validate_identifier(&execution.task_queue, &format!("{path}.task_queue"))?;
        validate_identifier(&execution.status, &format!("{path}.status"))?;
    }
    if let Some(token) = &value.next_page_token {
        base64::engine::general_purpose::STANDARD
            .decode(token)
            .map_err(|_| {
                protocol::ProtocolError::invalid("$.next_page_token", "invalid base64 page token")
            })?;
    }
    Ok(())
}

/// Converts Temporal's numeric visibility enum into a stable closed string.
fn visibility_status(status: i32) -> Result<String, ClientOperationError> {
    match status {
        1 => Ok("running"),
        2 => Ok("completed"),
        3 => Ok("failed"),
        4 => Ok("canceled"),
        5 => Ok("terminated"),
        6 => Ok("continued_as_new"),
        7 => Ok("timed_out"),
        8 => Ok("paused"),
        _ => Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "visibility row has an unknown execution status",
        ))),
    }
    .map(str::to_owned)
}

/// Validates one execution reference.
fn validate_execution(value: &ExecutionRef, path: &str) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, &format!("{path}.namespace"))?;
    validate_identifier(&value.workflow_id, &format!("{path}.workflow_id"))?;
    validate_identifier(&value.run_id, &format!("{path}.run_id"))?;
    Ok(())
}

/// Validates one outcome, including required continued-as-new successor data.
fn validate_outcome(value: &WorkflowOutcome) -> Result<(), protocol::ProtocolError> {
    let validate_successor = |successor: &SuccessorRef| validate_successor_shape(successor);
    match value {
        WorkflowOutcome::Completed { result, successor } => {
            for payload in result {
                workflow_protocol::payload_to_core(payload).map_err(|_| {
                    protocol::ProtocolError::invalid("$.outcome.result", "invalid Temporal payload")
                })?;
            }
            if let Some(successor) = successor {
                validate_successor(successor)?;
            }
        }
        WorkflowOutcome::Failed { failure, successor } => {
            workflow_protocol::validate_failure(failure, "$.outcome.failure")?;
            if let Some(successor) = successor {
                validate_successor(successor)?;
            }
        }
        WorkflowOutcome::Cancelled { details } | WorkflowOutcome::Terminated { details } => {
            for payload in details {
                workflow_protocol::payload_to_core(payload).map_err(|_| {
                    protocol::ProtocolError::invalid(
                        "$.outcome.details",
                        "invalid Temporal payload",
                    )
                })?;
            }
        }
        WorkflowOutcome::TimedOut { successor } => {
            if let Some(successor) = successor {
                validate_successor(successor)?;
            }
        }
        WorkflowOutcome::ContinuedAsNew { successor } => validate_successor(successor)?,
    }
    Ok(())
}

/// Validates the shape of successor metadata without assuming its parent run.
fn validate_successor_shape(value: &SuccessorRef) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.outcome.successor.namespace")?;
    validate_identifier(&value.workflow_id, "$.outcome.successor.workflow_id")?;
    validate_identifier(&value.run_id, "$.outcome.successor.run_id")
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

/// Validates a complete wait response.
fn validate_wait_response(value: &WaitWorkflowResponse) -> Result<(), protocol::ProtocolError> {
    validate_execution(&value.execution, "$.execution")?;
    validate_outcome(&value.outcome)?;
    let successor = match &value.outcome {
        WorkflowOutcome::Completed { successor, .. }
        | WorkflowOutcome::Failed { successor, .. }
        | WorkflowOutcome::TimedOut { successor } => successor.as_ref(),
        WorkflowOutcome::ContinuedAsNew { successor } => Some(successor),
        WorkflowOutcome::Cancelled { .. } | WorkflowOutcome::Terminated { .. } => None,
    };
    if let Some(successor) = successor {
        validate_successor_for_execution(successor, &value.execution)?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;
    use temporalio_client::tonic::codegen::Bytes;

    #[test]
    /// Unknown and omitted Temporal visibility enum values fail closed instead
    /// of being converted into a valid-looking status string.
    fn unknown_visibility_status_is_rejected() {
        assert!(visibility_status(0).is_err());
        assert!(visibility_status(99).is_err());
        assert_eq!(visibility_status(1).unwrap(), "running");
    }

    /// Builds the smallest valid start document used by strict-parser tests.
    fn start_json() -> String {
        serde_json::json!({
            "request_id":"request-1",
            "namespace":"default",
            "workflow_id":"workflow-1",
            "workflow_type":"smoke",
            "task_queue":"queue",
            "input":[{"metadata":{},"data":{"encoding":"base64","data":STANDARD.encode(b"input")}}]
        })
        .to_string()
    }

    /// Builds a valid exact-run cancellation request, including the stable
    /// request ID that lets a caller retry a timed-out RPC safely.
    fn cancel_json() -> String {
        serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "request_id":"cancel-1",
            "reason":"operator requested cancellation"
        })
        .to_string()
    }

    /// Builds an exact-run reset request whose event boundary is represented
    /// as an integer literal so large Temporal event IDs cannot round through
    /// a floating-point JSON representation.
    fn reset_json() -> String {
        serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "request_id":"reset-1",
            "reason":"replay from workflow task",
            "workflow_task_finish_event_id":4
        })
        .to_string()
    }

    /// Builds a valid exact-run signal request used by strict protocol tests.
    fn signal_json() -> String {
        serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "signal_name":"add_document",
            "request_id":"signal-1",
            "input":[]
        })
        .to_string()
    }

    /// Builds a valid output-only exact-run query request.
    fn query_json() -> String {
        serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "query_type":"current_state",
            "input":[]
        })
        .to_string()
    }

    #[test]
    /// Rejects a syntactically valid request containing an unknown member.
    fn start_request_requires_closed_payload_shape() {
        let mut json = start_json();
        // Insert the unknown member before the existing closing brace so the
        // test exercises strict-field validation rather than JSON syntax.
        json.insert_str(json.len() - 1, ",\"unexpected\":true");
        assert!(decode_start_request(&json).is_err());
    }

    #[test]
    /// Requires the stable logical ID used to correlate asynchronous retries.
    fn start_request_requires_request_id() {
        let json = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "workflow_type":"smoke",
            "task_queue":"queue",
            "input":[]
        })
        .to_string();
        assert!(decode_start_request(&json).is_err());
    }

    #[test]
    /// A reused request ID may alias only an identical semantic request.
    fn pending_request_matching_checks_all_start_fields() {
        let original = decode_start_request(&start_json()).expect("start request decodes");
        let mut changed = original.clone();
        changed.task_queue = "other-queue".to_owned();

        assert!(same_start_request(&original, &original));
        assert!(!same_start_request(&original, &changed));

        changed.task_queue = original.task_queue.clone();
        changed.input[0].data.push(0);
        assert!(!same_start_request(&original, &changed));
    }

    #[test]
    /// Tickets round-trip through a closed object and reject extra members.
    fn start_ticket_round_trips_as_an_opaque_value() {
        let encoded = encode_start_ticket("ticket-1").expect("ticket encodes");
        assert_eq!(
            decode_start_ticket(&encoded).expect("ticket decodes"),
            "ticket-1"
        );
        assert!(decode_start_ticket(r#"{"ticket":"ticket-1","extra":true}"#).is_err());
    }

    #[test]
    /// Terminal outcomes preserve acceptance and expose uncertain transport
    /// failures without pretending that Temporal rejected the request.
    fn start_outcomes_are_closed_and_uncertainty_is_explicit() {
        let accepted = StartWorkflowOutcome::Accepted(StartWorkflowResponse {
            execution: ExecutionRef {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
            },
        });
        assert_eq!(
            encode_start_outcome(&accepted).expect("accepted outcome encodes"),
            r#"{"kind":"accepted","execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"}}"#
        );

        let rejected = ClientOperationError::Rpc {
            code: "invalid_argument".to_owned(),
        };
        assert!(!rejected.uncertain_start());
        assert!(
            ClientOperationError::Rpc {
                code: "unavailable".to_owned()
            }
            .uncertain_start()
        );
        // Start RPC timeouts use the same uncertain classification so a hung
        // server cannot be treated as a proven rejection.
        assert!(
            ClientOperationError::Rpc {
                code: "deadline_exceeded".to_owned()
            }
            .uncertain_start()
        );
        let unknown = StartWorkflowOutcome::Unknown {
            request_id: "request-1".to_owned(),
            workflow_id: "workflow-1".to_owned(),
        };
        assert_eq!(
            encode_start_outcome(&unknown).expect("unknown outcome encodes"),
            r#"{"kind":"unknown","request_id":"request-1","workflow_id":"workflow-1"}"#
        );
    }

    #[test]
    /// Rejects duplicate members before serde's map conversion can erase one.
    fn duplicate_start_member_is_rejected_before_serde_map_conversion() {
        let json = r#"{"request_id":"request-1","namespace":"default","namespace":"other","workflow_id":"id","workflow_type":"type","task_queue":"queue","input":[]}"#;
        assert!(decode_start_request(json).is_err());
    }

    #[test]
    /// Rejects an attempted opt-in to successor following on exact-run waits.
    fn exact_wait_request_has_no_follow_runs_escape_hatch() {
        let json =
            r#"{"namespace":"default","workflow_id":"id","run_id":"run","follow_runs":true}"#;
        assert!(decode_wait_request(json).is_err());
    }

    #[test]
    /// Decodes every cancellation field without treating an empty reason as a
    /// missing field, and emits only a positive acknowledgement.
    fn cancellation_request_and_acknowledgement_round_trip() {
        let request = decode_cancel_request(&cancel_json()).expect("cancel request decodes");
        assert_eq!(request.namespace, "default");
        assert_eq!(request.workflow_id, "workflow-1");
        assert_eq!(request.run_id, "run-1");
        assert_eq!(request.request_id, "cancel-1");
        assert_eq!(request.reason, "operator requested cancellation");

        let empty_reason = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "request_id":"cancel-2",
            "reason":""
        })
        .to_string();
        assert_eq!(decode_cancel_request(&empty_reason).unwrap().reason, "");

        let encoded = encode_cancel_response(&CancelWorkflowResponse { acknowledged: true })
            .expect("positive cancellation acknowledgement encodes");
        assert_eq!(encoded, r#"{"acknowledged":true}"#);
    }

    #[test]
    /// Termination has its own closed request type and rejects cancellation's
    /// idempotency-only member rather than silently ignoring it.
    fn termination_request_and_acknowledgement_round_trip() {
        let request = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "reason":"operator test"
        })
        .to_string();
        let decoded = decode_terminate_request(&request).expect("terminate request decodes");
        assert_eq!(decoded.reason, "operator test");
        assert!(
            decode_terminate_request(
                &serde_json::json!({
                    "namespace":"default",
                    "workflow_id":"workflow-1",
                    "run_id":"run-1",
                    "reason":"operator test",
                    "request_id":"not-supported"
                })
                .to_string()
            )
            .is_err()
        );
        assert_eq!(
            encode_terminate_response(&TerminateWorkflowResponse { acknowledged: true })
                .expect("terminate acknowledgement encodes"),
            r#"{"acknowledged":true}"#
        );
        assert!(
            encode_terminate_response(&TerminateWorkflowResponse {
                acknowledged: false
            })
            .is_err()
        );
    }

    #[test]
    /// Rejects cancellation documents that could lose identity or change the
    /// operation's meaning when decoded by a permissive JSON map.
    fn cancellation_request_is_closed_and_bounded() {
        let mut unknown = cancel_json();
        unknown.insert_str(unknown.len() - 1, ",\"unexpected\":true");
        assert!(decode_cancel_request(&unknown).is_err());

        let duplicate = r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","request_id":"cancel-1","request_id":"other","reason":""}"#;
        assert!(decode_cancel_request(duplicate).is_err());

        for field in ["namespace", "workflow_id", "run_id", "request_id"] {
            let document = serde_json::json!({
                "namespace":"default",
                "workflow_id":"workflow-1",
                "run_id":"run-1",
                "request_id":"cancel-1",
                "reason":""
            });
            let mut object = document.as_object().unwrap().clone();
            object.insert(field.to_owned(), serde_json::Value::String(String::new()));
            assert!(
                decode_cancel_request(&serde_json::Value::Object(object).to_string()).is_err(),
                "empty {field} must be rejected"
            );
        }

        let nul_reason = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "request_id":"cancel-1",
            "reason":"contains\0nul"
        })
        .to_string();
        assert!(decode_cancel_request(&nul_reason).is_err());

        let oversized_reason = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "request_id":"cancel-1",
            "reason": "x".repeat(protocol::MAX_STRING_BYTES + 1)
        })
        .to_string();
        assert!(decode_cancel_request(&oversized_reason).is_err());
    }

    #[test]
    /// Reset requests preserve the exact run and event boundary, reject
    /// unknown members, and encode the new run identity under `execution`.
    fn reset_request_and_response_are_strict() {
        let request = decode_reset_request(&reset_json()).expect("reset request decodes");
        assert_eq!(request.namespace, "default");
        assert_eq!(request.workflow_id, "workflow-1");
        assert_eq!(request.run_id, "run-1");
        assert_eq!(request.workflow_task_finish_event_id, 4);

        let response = ResetWorkflowResponse {
            execution: ExecutionRef {
                namespace: request.namespace.clone(),
                workflow_id: request.workflow_id.clone(),
                run_id: "run-2".to_owned(),
            },
        };
        assert_eq!(
            encode_reset_response(&response).expect("reset response encodes"),
            r#"{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}}"#
        );

        let mut unknown = reset_json();
        unknown.insert_str(unknown.len() - 1, ",\"unexpected\":true");
        assert!(decode_reset_request(&unknown).is_err());
    }

    #[test]
    /// Negative event boundaries are rejected before any Temporal RPC can be
    /// attempted, preventing an accidental reset to an invalid history point.
    fn reset_request_rejects_negative_event_boundary() {
        let json = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "request_id":"reset-1",
            "reason":"",
            "workflow_task_finish_event_id":-1
        })
        .to_string();
        assert!(decode_reset_request(&json).is_err());
    }

    #[test]
    /// Refuses a false acknowledgement so transport success cannot be
    /// confused with Temporal accepting a cancellation request.
    fn cancellation_acknowledgement_must_be_true_and_closed() {
        assert!(
            encode_cancel_response(&CancelWorkflowResponse {
                acknowledged: false
            })
            .is_err()
        );
        assert!(
            serde_json::from_str::<CancelWorkflowResponse>(
                r#"{"acknowledged":true,"unexpected":true}"#
            )
            .is_err()
        );
    }

    #[test]
    /// Signals preserve exact-run identity, payload ordering, and positive
    /// acknowledgement semantics across the closed JSON boundary.
    fn signal_request_and_acknowledgement_round_trip() {
        let request = decode_signal_request(&signal_json()).expect("signal request decodes");
        assert_eq!(request.namespace, "default");
        assert_eq!(request.workflow_id, "workflow-1");
        assert_eq!(request.run_id, "run-1");
        assert_eq!(request.signal_name, "add_document");
        assert_eq!(request.request_id, "signal-1");
        assert!(request.input.is_empty());

        let encoded = encode_signal_response(&SignalWorkflowResponse { acknowledged: true })
            .expect("positive signal acknowledgement encodes");
        assert_eq!(encoded, r#"{"acknowledged":true}"#);
    }

    #[test]
    /// Rejects signal documents that could redirect a message or silently
    /// discard an input field through duplicate/unknown JSON members.
    fn signal_request_is_closed_and_bounded() {
        let mut unknown = signal_json();
        unknown.insert_str(unknown.len() - 1, ",\"unexpected\":true");
        assert!(decode_signal_request(&unknown).is_err());

        let duplicate = r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","signal_name":"add_document","request_id":"signal-1","request_id":"other","input":[]}"#;
        assert!(decode_signal_request(duplicate).is_err());

        for field in [
            "namespace",
            "workflow_id",
            "run_id",
            "signal_name",
            "request_id",
        ] {
            let document = serde_json::json!({
                "namespace":"default",
                "workflow_id":"workflow-1",
                "run_id":"run-1",
                "signal_name":"add_document",
                "request_id":"signal-1",
                "input":[]
            });
            let mut object = document.as_object().unwrap().clone();
            object.insert(field.to_owned(), serde_json::Value::String(String::new()));
            assert!(
                decode_signal_request(&serde_json::Value::Object(object).to_string()).is_err(),
                "empty {field} must be rejected"
            );
        }
    }

    #[test]
    /// Refuses false signal acknowledgements and unknown response members.
    fn signal_acknowledgement_must_be_true_and_closed() {
        assert!(
            encode_signal_response(&SignalWorkflowResponse {
                acknowledged: false
            })
            .is_err()
        );
        assert!(
            serde_json::from_str::<SignalWorkflowResponse>(
                r#"{"acknowledged":true,"unexpected":true}"#
            )
            .is_err()
        );
    }

    #[test]
    /// Query requests preserve exact-run identity and reject unknown or
    /// duplicate members before any Temporal connection is consulted.
    fn query_request_is_closed_and_bounded() {
        let request = decode_query_request(&query_json()).expect("query request decodes");
        assert_eq!(request.query_type, "current_state");
        assert!(request.input.is_empty());

        let mut unknown = query_json();
        unknown.insert_str(unknown.len() - 1, ",\"unexpected\":true");
        assert!(decode_query_request(&unknown).is_err());

        let duplicate = r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","query_type":"current_state","query_type":"other","input":[]}"#;
        assert!(decode_query_request(duplicate).is_err());

        for field in ["namespace", "workflow_id", "run_id", "query_type"] {
            let document = serde_json::json!({
                "namespace":"default",
                "workflow_id":"workflow-1",
                "run_id":"run-1",
                "query_type":"current_state",
                "input":[]
            });
            let mut object = document.as_object().unwrap().clone();
            object.insert(field.to_owned(), serde_json::Value::String(String::new()));
            assert!(
                decode_query_request(&serde_json::Value::Object(object).to_string()).is_err(),
                "empty {field} must be rejected"
            );
        }
    }

    #[test]
    /// Query responses round-trip an ordered payload list and remain closed to
    /// unknown JSON members at the native boundary.
    fn query_response_round_trip_is_closed() {
        let response = QueryWorkflowResponse { result: Vec::new() };
        let encoded = encode_query_response(&response).expect("query response encodes");
        assert_eq!(encoded, r#"{"result":[]}"#);
        assert!(
            serde_json::from_str::<QueryWorkflowResponse>(r#"{"result":[],"unexpected":true}"#)
                .is_err()
        );
    }

    #[test]
    /// Fails closed instead of polling forever when Core returns no attributes.
    fn close_event_without_attributes_is_rejected() {
        assert!(outcome_from_event(HistoryEvent::default(), "default", "id").is_err());
    }

    #[test]
    /// Converts every supported Core close event without following successors.
    fn terminal_history_events_map_to_closed_outcomes() {
        let cases = [
            (
                Attributes::WorkflowExecutionCompletedEventAttributes(Default::default()),
                WorkflowOutcome::Completed {
                    result: Vec::new(),
                    successor: None,
                },
            ),
            (
                Attributes::WorkflowExecutionCanceledEventAttributes(Default::default()),
                WorkflowOutcome::Cancelled {
                    details: Vec::new(),
                },
            ),
            (
                Attributes::WorkflowExecutionTerminatedEventAttributes(Default::default()),
                WorkflowOutcome::Terminated {
                    details: Vec::new(),
                },
            ),
            (
                Attributes::WorkflowExecutionTimedOutEventAttributes(Default::default()),
                WorkflowOutcome::TimedOut { successor: None },
            ),
            (
                Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
                    temporalio_common::protos::temporal::api::history::v1::
                        WorkflowExecutionContinuedAsNewEventAttributes {
                        new_execution_run_id: "run-2".to_owned(),
                        ..Default::default()
                    },
                ),
                WorkflowOutcome::ContinuedAsNew {
                    successor: SuccessorRef {
                        namespace: "default".to_owned(),
                        workflow_id: "id".to_owned(),
                        run_id: "run-2".to_owned(),
                    },
                },
            ),
        ];
        for (attributes, expected) in cases {
            let event = HistoryEvent {
                attributes: Some(attributes),
                ..Default::default()
            };
            assert_eq!(
                outcome_from_event(event, "default", "id").unwrap(),
                expected
            );
        }
    }

    #[test]
    /// Rejects terminal Core events that omit required failure/successor data.
    fn malformed_terminal_history_events_fail_closed() {
        let failed = HistoryEvent {
            attributes: Some(Attributes::WorkflowExecutionFailedEventAttributes(
                Default::default(),
            )),
            ..Default::default()
        };
        assert!(outcome_from_event(failed, "default", "id").is_err());

        let continued = HistoryEvent {
            attributes: Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
                Default::default(),
            )),
            ..Default::default()
        };
        assert!(outcome_from_event(continued, "default", "id").is_err());
    }

    #[test]
    /// Confirms continued-as-new output retains the successor run identity.
    fn continued_as_new_response_preserves_successor_metadata() {
        let response = WaitWorkflowResponse {
            execution: ExecutionRef {
                namespace: "default".to_owned(),
                workflow_id: "id".to_owned(),
                run_id: "run-1".to_owned(),
            },
            outcome: WorkflowOutcome::ContinuedAsNew {
                successor: SuccessorRef {
                    namespace: "default".to_owned(),
                    workflow_id: "id".to_owned(),
                    run_id: "run-2".to_owned(),
                },
            },
        };
        let encoded = encode_wait_response(&response).unwrap();
        let decoded: WaitWorkflowResponse = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, response);
        assert!(encoded.contains("run-2"));
    }

    #[test]
    /// Keeps the public failure JSON unchanged after boxing the recursive
    /// failure tree used to keep `WorkflowOutcome` compact in memory.
    fn failed_response_round_trips_with_boxed_failure_tree() {
        let response = WaitWorkflowResponse {
            execution: ExecutionRef {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
            },
            outcome: WorkflowOutcome::Failed {
                failure: Box::new(workflow_protocol::Failure {
                    message: "failed".to_owned(),
                    source: "workflow".to_owned(),
                    stack_trace: String::new(),
                    encoded_attributes: None,
                    cause: None,
                    info: workflow_protocol::FailureInfo::Application {
                        type_name: "application".to_owned(),
                        non_retryable: false,
                        details: Vec::new(),
                    },
                }),
                successor: None,
            },
        };

        let encoded = encode_wait_response(&response).expect("failed response encodes");
        let decoded: WaitWorkflowResponse =
            serde_json::from_str(&encoded).expect("failed response decodes");
        assert_eq!(decoded, response);
        assert!(encoded.contains(r#""kind":"failed""#));
    }

    #[test]
    /// Rejects a successor that changes namespace or workflow identity.
    fn successor_must_remain_in_the_waited_execution() {
        let response = WaitWorkflowResponse {
            execution: ExecutionRef {
                namespace: "default".to_owned(),
                workflow_id: "id".to_owned(),
                run_id: "run-1".to_owned(),
            },
            outcome: WorkflowOutcome::ContinuedAsNew {
                successor: SuccessorRef {
                    namespace: "other".to_owned(),
                    workflow_id: "id".to_owned(),
                    run_id: "run-2".to_owned(),
                },
            },
        };
        assert!(encode_wait_response(&response).is_err());
    }

    #[test]
    /// Rejects a successor that reuses the run being observed.
    fn successor_must_use_a_new_run_id() {
        let response = WaitWorkflowResponse {
            execution: ExecutionRef {
                namespace: "default".to_owned(),
                workflow_id: "id".to_owned(),
                run_id: "run-1".to_owned(),
            },
            outcome: WorkflowOutcome::Completed {
                result: Vec::new(),
                successor: Some(SuccessorRef {
                    namespace: "default".to_owned(),
                    workflow_id: "id".to_owned(),
                    run_id: "run-1".to_owned(),
                }),
            },
        };
        assert!(encode_wait_response(&response).is_err());
    }

    #[test]
    /// Keeps AlreadyStarted diagnostics closed and free of raw RPC text.
    fn already_started_error_is_closed_and_does_not_include_rpc_text() {
        let error = ClientOperationError::AlreadyStarted {
            workflow_id: "id".to_owned(),
            existing_run_id: Some("run".to_owned()),
        };
        let value: ClientErrorDocument = serde_json::from_str(&error.to_json()).unwrap();
        assert_eq!(
            value,
            ClientErrorDocument::AlreadyStarted {
                workflow_id: "id".to_owned(),
                existing_run_id: Some("run".to_owned())
            }
        );
        assert!(!error.to_json().contains("server"));
    }

    #[test]
    /// Keeps a timed-out termination distinguishable from an ordinary RPC
    /// rejection because the server may have accepted the command already.
    fn termination_timeout_error_is_explicitly_uncertain() {
        let error = ClientOperationError::Rpc {
            code: "termination_outcome_uncertain".to_owned(),
        };
        assert_eq!(
            error.to_json(),
            r#"{"kind":"rpc","code":"termination_outcome_uncertain"}"#
        );
    }

    #[test]
    /// Converts a start-only AlreadyExists status into the typed error variant.
    fn start_status_maps_already_exists_to_already_started() {
        let error = map_start_status("workflow-1", Status::new(Code::AlreadyExists, "server"));
        assert_eq!(
            error,
            ClientOperationError::AlreadyStarted {
                workflow_id: "workflow-1".to_owned(),
                existing_run_id: None,
            }
        );
    }

    /// Constructs one length-delimited protobuf field for the small status
    /// detail fixture below. The test values are deliberately short, so the
    /// one-byte length encoding keeps the fixture readable without adding a
    /// protobuf dependency solely to manufacture a gRPC error.
    fn length_delimited_field(tag: u8, value: &[u8]) -> Vec<u8> {
        assert!(value.len() < 128, "fixture field must fit one-byte length");
        let mut field = vec![tag, value.len() as u8];
        field.extend_from_slice(value);
        field
    }

    /// Encodes the `google.rpc.Status -> Any -> AlreadyStartedFailure` shape
    /// returned by Temporal so the test exercises the same decoder path used
    /// for real server errors, including Rust strings with an embedded NUL.
    fn already_started_status_with_run_id(run_id: &str) -> Status {
        let failure = length_delimited_field(0x12, run_id.as_bytes());
        let type_url =
            b"type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure";
        let mut any = length_delimited_field(0x0a, type_url);
        any.extend(length_delimited_field(0x12, &failure));
        let details = length_delimited_field(0x1a, &any);
        Status::with_details(Code::AlreadyExists, "server", Bytes::from(details))
    }

    #[test]
    /// Preserves the AlreadyStarted category while omitting malformed
    /// server-provided run IDs that violate the bilateral JSON contract.
    fn already_started_status_drops_nul_run_id_from_error_body() {
        let valid = map_start_status("workflow-1", already_started_status_with_run_id("run-1"));
        assert_eq!(
            valid.to_json(),
            r#"{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":"run-1"}"#
        );

        let malformed = map_start_status(
            "workflow-1",
            already_started_status_with_run_id("run-\0-invalid"),
        );
        assert_eq!(
            malformed.to_json(),
            r#"{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}"#
        );
    }

    #[test]
    /// Keeps wait failures as RPC errors even when a proxy returns
    /// `AlreadyExists`, which has start-only meaning in this adapter.
    fn wait_status_does_not_use_start_error_mapping() {
        let error = map_rpc_status(Status::new(Code::AlreadyExists, "server text"));
        assert_eq!(
            error,
            ClientOperationError::Rpc {
                code: "already_exists".to_owned()
            }
        );
    }

    #[test]
    /// Uses explicit snake-case names instead of tonic's unstable debug text.
    fn rpc_status_codes_preserve_word_boundaries() {
        assert_eq!(
            map_rpc_status(Status::new(Code::DeadlineExceeded, "timeout")),
            ClientOperationError::Rpc {
                code: "deadline_exceeded".to_owned()
            }
        );
        assert_eq!(
            map_rpc_status(Status::new(Code::PermissionDenied, "denied")),
            ClientOperationError::Rpc {
                code: "permission_denied".to_owned()
            }
        );
    }

    #[test]
    /// Maps Core conversion categories to stable, closed error codes.
    fn core_conversion_errors_use_stable_codes() {
        let error =
            ClientOperationError::Core(workflow_protocol::unsupported("unsupported client option"));
        assert_eq!(
            error.to_json(),
            r#"{"kind":"protocol","code":"core_unsupported"}"#
        );
    }

    #[test]
    /// Accepts a workflow start with no argument payloads.
    fn empty_payload_vector_is_valid_for_no_argument_workflow() {
        let json = serde_json::json!({
            "request_id":"request-1",
            "namespace":"default",
            "workflow_id":"workflow-1",
            "workflow_type":"smoke",
            "task_queue":"queue",
            "input":[]
        })
        .to_string();
        assert_eq!(decode_start_request(&json).unwrap().input, Vec::new());
    }

    #[test]
    /// Retains named start metadata as owned values and rejects duplicate
    /// keys before protobuf map conversion can overwrite one entry.
    fn start_metadata_is_closed_and_duplicate_safe() {
        let json = serde_json::json!({
            "request_id":"request-1",
            "namespace":"default",
            "workflow_id":"workflow-1",
            "workflow_type":"smoke",
            "task_queue":"queue",
            "input":[],
            "memo":[{"key":"owner","value":{"metadata":{},"data":{"encoding":"base64","data":"b3duZXI="}}}],
            "search_attributes":[{"key":"priority","value":{"metadata":{},"data":{"encoding":"base64","data":"aGlnaA=="}}}]
        })
        .to_string();
        let request = decode_start_request(&json).unwrap();
        assert_eq!(request.memo.len(), 1);
        assert_eq!(request.search_attributes[0].key, "priority");

        // Build the duplicate through a JSON value rather than textual
        // replacement: serde_json is free to choose object-key order, while
        // the validation contract concerns duplicate array entries only.
        let mut duplicate: serde_json::Value = serde_json::from_str(&json).unwrap();
        duplicate["memo"] = serde_json::json!([
            {"key":"owner","value":{"metadata":{},"data":{"encoding":"base64","data":"b3duZXI="}}},
            {"key":"owner","value":{"metadata":{},"data":{"encoding":"base64","data":"b3duZXI="}}}
        ]);
        let duplicate = duplicate.to_string();
        assert!(decode_start_request(&duplicate).is_err());
    }

    #[test]
    /// Keeps update admission and polling round trips closed and preserves a
    /// pending outcome as JSON `null` rather than inventing a terminal value.
    fn update_request_and_pending_response_round_trip() {
        let request = UpdateWorkflowRequest {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
            update_id: "update-1".to_owned(),
            update_name: "set_state".to_owned(),
            input: Vec::new(),
        };
        let encoded = serde_json::to_string(&request).unwrap();
        assert_eq!(decode_update_request(&encoded).unwrap(), request);
        let response = UpdateWorkflowResponse {
            update_id: request.update_id.clone(),
            execution: ExecutionRef {
                namespace: request.namespace.clone(),
                workflow_id: request.workflow_id.clone(),
                run_id: request.run_id.clone(),
            },
            outcome: None,
        };
        let encoded_response = encode_update_response(&response).unwrap();
        assert!(encoded_response.contains(r#""outcome":null"#));
        assert_eq!(
            serde_json::from_str::<UpdateWorkflowResponse>(&encoded_response).unwrap(),
            response
        );
    }

    #[test]
    /// Rejects update responses with an empty echoed ID before they can cross
    /// the native boundary; OCaml performs the request-correlation check.
    fn update_response_rejects_invalid_update_id() {
        let response = UpdateWorkflowResponse {
            update_id: String::new(),
            execution: ExecutionRef {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
            },
            outcome: Some(UpdateOutcome::Completed { result: Vec::new() }),
        };
        assert!(encode_update_response(&response).is_err());
    }

    #[test]
    /// Preserves a structured failure outcome through the bilateral JSON
    /// codec instead of flattening it into server diagnostic text.
    fn update_failure_response_round_trip() {
        let response = PollWorkflowUpdateResponse {
            outcome: Some(UpdateOutcome::Failed {
                failure: Box::new(workflow_protocol::Failure {
                    message: "failed".to_owned(),
                    source: "worker".to_owned(),
                    stack_trace: String::new(),
                    encoded_attributes: None,
                    cause: None,
                    info: workflow_protocol::FailureInfo::Application {
                        type_name: "Failure".to_owned(),
                        non_retryable: true,
                        details: Vec::new(),
                    },
                }),
            }),
        };
        let encoded = encode_poll_update_response(&response).unwrap();
        assert_eq!(
            serde_json::from_str::<PollWorkflowUpdateResponse>(&encoded).unwrap(),
            response
        );
    }
}

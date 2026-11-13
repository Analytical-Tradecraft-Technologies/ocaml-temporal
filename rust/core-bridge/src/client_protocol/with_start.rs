//! Signal-with-start and update-with-start (#1151).
//!
//! Both operations deliver a message to a workflow, starting it first when
//! its workflow ID has no open run, so an entity workflow can be "ensured
//! running, then messaged" without a window in which the run closes between a
//! separate start and signal. Each request embeds the closed start document
//! of [`StartWorkflowRequest`] unchanged, so the start options, validation,
//! request-ID idempotency, and uncertain-start classification are exactly
//! those of an ordinary start.
//!
//! - Signal-with-start is one `SignalWithStartWorkflowExecution` RPC. Its
//!   outcome is the ordinary accepted/rejected/unknown start outcome: the
//!   signal is part of the same atomic server operation, so an accepted
//!   outcome proves the signal was recorded too.
//! - Update-with-start is an `ExecuteMultiOperation` of `[StartWorkflow,
//!   UpdateWorkflow]`. Like [`update_workflow`], an update that Temporal has
//!   only admitted is re-issued (with the same request ID and update ID, which
//!   Temporal deduplicates) until it is accepted or completed or the budget
//!   expires. Once any attempt has answered, the start is known to have been
//!   applied, so a later failure is reported beside the accepted start rather
//!   than as an uncertain start; see [`UpdateWithStartOutcome`].
//!
//! Validation paths inside the embedded start document are reported relative
//! to that document (for example `$.workflow_id`), as for a plain start.

use super::*;
use prost::Message as _;
use temporalio_common::protos::google::rpc::Status as RpcStatus;
use temporalio_common::protos::temporal::api::errordetails::v1::MultiOperationExecutionFailure;
use temporalio_common::protos::temporal::api::workflowservice::v1::{
    ExecuteMultiOperationRequest, ExecuteMultiOperationResponse,
    SignalWithStartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
    execute_multi_operation_request, execute_multi_operation_response,
};

/// Request to signal a workflow, starting it first when its workflow ID has
/// no open run (#1151).
///
/// `start.id_conflict_policy` decides what happens to an open run: the
/// default `use_existing` signals it, and `terminate_existing` terminates it
/// and signals a new run. `fail` is rejected, as Temporal itself rejects it
/// for this RPC. `start.request_id` deduplicates both the start and the
/// signal, and `start.rpc_timeout_ms` bounds the whole call (default
/// [`START_RPC_TIMEOUT`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SignalWithStartRequest {
    /// The start applied when the workflow ID has no open run.
    pub start: StartWorkflowRequest,
    /// Registered Temporal signal name.
    pub signal_name: String,
    /// Ordered signal input payloads.
    pub signal_input: Vec<workflow_protocol::Payload>,
}

/// Request to start a workflow if needed and admit one update on it (#1151).
///
/// Temporal requires an explicit conflict policy for update-with-start; the
/// OCaml encoder always sends one, and every policy is forwarded unchanged.
/// The update targets the run the start resolves (never an exact run), and
/// `start.rpc_timeout_ms` bounds the whole call, including re-issued attempts
/// while the update is only admitted (default [`UPDATE_ACCEPTANCE_TIMEOUT`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateWithStartRequest {
    /// The start applied when the workflow ID has no open run.
    pub start: StartWorkflowRequest,
    /// Caller-owned workflow-scoped update idempotency key.
    pub update_id: String,
    /// Registered workflow update handler name.
    pub update_name: String,
    /// Ordered update argument payloads.
    pub update_input: Vec<workflow_protocol::Payload>,
}

/// Terminal result of one update-with-start call.
///
/// `Accepted` means Temporal answered at least once, which proves the start
/// was applied: `start` is the first answer's start response, and `update`
/// is either the accepted (or already completed or rejected) update or the
/// failure that ended the wait for acceptance, such as `deadline_exceeded`.
/// `Rejected` means the first attempt failed with an error that proves
/// nothing was applied; Temporal applies both operations atomically, so this
/// covers a failure attributed to either operation. `Unknown` means the
/// first attempt's outcome cannot be proven, exactly as for a start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UpdateWithStartOutcome {
    /// The start was applied; the update result follows.
    Accepted {
        /// The start operation's response from the first answer.
        start: StartWorkflowResponse,
        /// The update's admission response, or why it was not accepted.
        update: Result<UpdateWorkflowResponse, ClientOperationError>,
    },
    /// Nothing was applied.
    Rejected(ClientOperationError),
    /// The request may have been applied; reconcile by request ID.
    Unknown {
        /// Stable request identifier supplied by the caller.
        request_id: String,
        /// Workflow identity used for later reconciliation.
        workflow_id: String,
    },
}

/// Wire form of the update half of an accepted update-with-start.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum UpdateResultDocument {
    /// Temporal answered for the update: it was accepted, or it already has
    /// an outcome (a completion or a validator rejection).
    Response {
        /// Update ID echoed by Temporal.
        update_id: String,
        /// The run that accepted the update.
        execution: ExecutionRef,
        /// The outcome when the update already completed or was rejected.
        outcome: Option<UpdateOutcome>,
    },
    /// The update was not accepted before the call ended.
    Error {
        /// Stable, privacy-safe error category and code.
        error: ClientErrorDocument,
    },
}

/// Wire form of [`UpdateWithStartOutcome`]; the success value of the
/// `UPDATE_WITH_START` client call selector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum UpdateWithStartOutcomeDocument {
    /// The start was applied.
    Accepted {
        /// The run the start created or attached to.
        execution: ExecutionRef,
        /// Whether this request created the run; see [`StartWorkflowResponse`].
        started: bool,
        /// The update half.
        update: UpdateResultDocument,
    },
    /// Nothing was applied.
    Rejected {
        /// Stable, privacy-safe error category and code.
        error: ClientErrorDocument,
    },
    /// The outcome cannot be proven.
    Unknown {
        /// Stable logical request identifier supplied by the caller.
        request_id: String,
        /// Workflow ID useful to a later reconciliation operation.
        workflow_id: String,
    },
}

/// Strictly parses one signal-with-start request after duplicate-key and
/// byte-limit checks.
pub(crate) fn decode_signal_with_start_request(
    input: &str,
) -> Result<SignalWithStartRequest, protocol::ProtocolError> {
    decode_request(
        input,
        protocol::decode_payload_object,
        "invalid client signal-with-start request",
        validate_signal_with_start_request,
    )
}

/// Strictly parses one update-with-start request after duplicate-key and
/// byte-limit checks.
pub(crate) fn decode_update_with_start_request(
    input: &str,
) -> Result<UpdateWithStartRequest, protocol::ProtocolError> {
    decode_request(
        input,
        protocol::decode_payload_object,
        "invalid client update-with-start request",
        validate_update_with_start_request,
    )
}

/// Compares two signal-with-start requests as one logical operation: the
/// same start (ignoring its RPC timeout, see [`same_start_request`]) and the
/// same signal. A pending request ID may only be reused for such a retry.
pub(crate) fn same_signal_with_start_request(
    left: &SignalWithStartRequest,
    right: &SignalWithStartRequest,
) -> bool {
    same_start_request(&left.start, &right.start)
        && left.signal_name == right.signal_name
        && left.signal_input == right.signal_input
}

/// Compares two update-with-start requests as one logical operation, as
/// [`same_signal_with_start_request`] does.
pub(crate) fn same_update_with_start_request(
    left: &UpdateWithStartRequest,
    right: &UpdateWithStartRequest,
) -> bool {
    same_start_request(&left.start, &right.start)
        && left.update_id == right.update_id
        && left.update_name == right.update_name
        && left.update_input == right.update_input
}

/// Validates the embedded start and the signal. A `fail` conflict policy is
/// rejected before any RPC: Temporal answers it with `invalid_argument`.
fn validate_signal_with_start_request(
    value: &SignalWithStartRequest,
) -> Result<(), protocol::ProtocolError> {
    validate_start_request(&value.start)?;
    if value.start.id_conflict_policy == IdConflictPolicy::Fail {
        return Err(protocol::ProtocolError::invalid(
            "$.start.id_conflict_policy",
            "signal-with-start does not accept the fail conflict policy",
        ));
    }
    validate_identifier(&value.signal_name, "$.signal_name")?;
    validate_message_payloads(&value.signal_input, "$.signal_input")
}

/// Validates the embedded start and the update.
fn validate_update_with_start_request(
    value: &UpdateWithStartRequest,
) -> Result<(), protocol::ProtocolError> {
    validate_start_request(&value.start)?;
    validate_identifier(&value.update_id, "$.update_id")?;
    validate_identifier(&value.update_name, "$.update_name")?;
    validate_message_payloads(&value.update_input, "$.update_input")
}

/// Requires every message argument at `path` to convert into a Temporal
/// payload before the request can reach the service.
fn validate_message_payloads(
    input: &[workflow_protocol::Payload],
    path: &str,
) -> Result<(), protocol::ProtocolError> {
    for payload in input {
        workflow_protocol::payload_to_core(payload)
            .map_err(|_| protocol::ProtocolError::invalid(path, "invalid Temporal payload"))?;
    }
    Ok(())
}

/// Signals a workflow, starting it first when its workflow ID has no open
/// run, through one `SignalWithStartWorkflowExecution` RPC.
///
/// The start fields are built exactly as for [`start_workflow`], so the two
/// paths cannot drift, and the budget is the start's `rpc_timeout_ms` or
/// [`START_RPC_TIMEOUT`]. Failures are classified like a start's: an
/// `already_exists` status (a reuse policy refusing a closed run) is the
/// typed already-started error, and the abi layer reports a transport
/// failure that may follow delivery as an uncertain start.
pub(crate) async fn signal_with_start_workflow(
    connection: Connection,
    request: SignalWithStartRequest,
) -> Result<StartWorkflowResponse, ClientOperationError> {
    let start = start_execution_request(&request.start, connection.identity())?;
    let signal_input =
        payloads_to_core(&request.signal_input).map_err(ClientOperationError::Core)?;
    let rpc_request = SignalWithStartWorkflowExecutionRequest {
        namespace: start.namespace,
        workflow_id: start.workflow_id,
        workflow_type: start.workflow_type,
        task_queue: start.task_queue,
        input: start.input,
        workflow_execution_timeout: start.workflow_execution_timeout,
        workflow_run_timeout: start.workflow_run_timeout,
        workflow_task_timeout: start.workflow_task_timeout,
        identity: start.identity,
        request_id: start.request_id,
        workflow_id_reuse_policy: start.workflow_id_reuse_policy,
        workflow_id_conflict_policy: start.workflow_id_conflict_policy,
        signal_name: request.signal_name.clone(),
        signal_input: Some(signal_input),
        retry_policy: start.retry_policy,
        memo: start.memo,
        search_attributes: start.search_attributes,
        // Cron schedules and start delays stay unsupported, as for start.
        ..Default::default()
    };
    let budget = rpc_budget(request.start.rpc_timeout_ms, START_RPC_TIMEOUT);
    let started = tokio::time::Instant::now();
    let mut service = connection.clone();
    let response = budgeted_rpc(
        budget,
        started,
        service.signal_with_start_workflow_execution(budgeted_request(rpc_request, budget)),
        |status| map_start_status(&request.start.workflow_id, status),
    )
    .await?;
    start_response_from_core(&request.start, response.run_id, response.started)
}

/// Starts a workflow if needed and admits one update on it through
/// `ExecuteMultiOperation`, within the request's budget (its start's
/// `rpc_timeout_ms` or [`UPDATE_ACCEPTANCE_TIMEOUT`]).
pub(crate) async fn update_with_start_workflow(
    connection: Connection,
    request: UpdateWithStartRequest,
) -> UpdateWithStartOutcome {
    let budget = rpc_budget(request.start.rpc_timeout_ms, UPDATE_ACCEPTANCE_TIMEOUT);
    update_with_start_within(connection, request, budget).await
}

/// Implements [`update_with_start_workflow`] with an explicit total budget.
///
/// Every attempt carries the remaining budget as its gRPC deadline. The
/// first answer fixes the start response; each answer's update half is
/// checked like [`update_workflow`]'s and, while it is only admitted, the
/// identical request is re-issued after [`UPDATE_READMISSION_PAUSE`].
pub(crate) async fn update_with_start_within(
    connection: Connection,
    request: UpdateWithStartRequest,
    budget: Duration,
) -> UpdateWithStartOutcome {
    let start = &request.start;
    // The update half is checked against a current-run request: Temporal
    // forbids a run ID on update-with-start and resolves the started run.
    let update_request = UpdateWorkflowRequest {
        namespace: start.namespace.clone(),
        workflow_id: start.workflow_id.clone(),
        run_id: String::new(),
        update_id: request.update_id.clone(),
        update_name: request.update_name.clone(),
        input: request.update_input.clone(),
        rpc_timeout_ms: None,
    };
    let rpc_request =
        match multi_operation_request(&request, &update_request, connection.identity()) {
            Ok(rpc_request) => rpc_request,
            // Nothing has been sent, so a conversion failure is a rejection.
            Err(error) => return UpdateWithStartOutcome::Rejected(error),
        };
    let deadline_exceeded = || ClientOperationError::Rpc {
        code: "deadline_exceeded".to_owned(),
    };
    let deadline = tokio::time::Instant::now() + budget;
    let mut service = connection.clone();
    let mut accepted_start: Option<StartWorkflowResponse> = None;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return settle(start, accepted_start, deadline_exceeded());
        }
        let attempt = budgeted_request(rpc_request.clone(), remaining);
        let attempt_started = tokio::time::Instant::now();
        let response =
            match tokio::time::timeout_at(deadline, service.execute_multi_operation(attempt)).await
            {
                Err(_) => return settle(start, accepted_start, deadline_exceeded()),
                Ok(Err(status)) => {
                    let map = |status| map_multi_operation_status(&start.workflow_id, status);
                    let error = budget_status(attempt_started, remaining, map)(status);
                    return settle(start, accepted_start, error);
                }
                Ok(Ok(response)) => response.into_inner(),
            };
        let (start_response, update_response) = match split_multi_operation_response(response) {
            Ok(pair) => pair,
            Err(error) => return settle(start, accepted_start, error),
        };
        let start_response = match accepted_start.take() {
            Some(first) => first,
            None => {
                match start_response_from_core(start, start_response.run_id, start_response.started)
                {
                    Ok(first) => first,
                    Err(error) => return settle(start, None, error),
                }
            }
        };
        let stage = update_response.stage;
        let update = match update_response_from_core(&update_request, update_response) {
            Ok(update) => update,
            Err(error) => return settle(start, Some(start_response), error),
        };
        // As in `update_workflow_within`: an outcome is final whatever its
        // stage, and an unknown or admitted stage without one is retried.
        let accepted = update.outcome.is_some()
            || match update_stage_is_accepted(stage) {
                Ok(accepted) => accepted,
                Err(error) => return settle(start, Some(start_response), error),
            };
        if accepted {
            return UpdateWithStartOutcome::Accepted {
                start: start_response,
                update: Ok(update),
            };
        }
        accepted_start = Some(start_response);
        let pause_until = tokio::time::Instant::now() + UPDATE_READMISSION_PAUSE;
        if pause_until >= deadline {
            return settle(start, accepted_start, deadline_exceeded());
        }
        tokio::time::sleep_until(pause_until).await;
    }
}

/// Builds the terminal outcome of a call that ended with `error`. After an
/// answer the start is known to be applied and the error belongs to the
/// update; before one, the error is a start failure, uncertain exactly when
/// a plain start's would be.
fn settle(
    start: &StartWorkflowRequest,
    accepted_start: Option<StartWorkflowResponse>,
    error: ClientOperationError,
) -> UpdateWithStartOutcome {
    match accepted_start {
        Some(start) => UpdateWithStartOutcome::Accepted {
            start,
            update: Err(error),
        },
        None if error.uncertain_start() => UpdateWithStartOutcome::Unknown {
            request_id: start.request_id.clone(),
            workflow_id: start.workflow_id.clone(),
        },
        None => UpdateWithStartOutcome::Rejected(error),
    }
}

/// Builds the `[StartWorkflow, UpdateWorkflow]` multi-operation request.
fn multi_operation_request(
    request: &UpdateWithStartRequest,
    update_request: &UpdateWorkflowRequest,
    identity: &str,
) -> Result<ExecuteMultiOperationRequest, ClientOperationError> {
    use execute_multi_operation_request::{Operation, operation::Operation as Kind};
    let start = start_execution_request(&request.start, identity)?;
    let update = update_execution_request(update_request, identity)?;
    Ok(ExecuteMultiOperationRequest {
        namespace: request.start.namespace.clone(),
        operations: vec![
            Operation {
                operation: Some(Kind::StartWorkflow(start)),
            },
            Operation {
                operation: Some(Kind::UpdateWorkflow(update)),
            },
        ],
        // Routing key; Temporal requires it to equal the start's workflow ID.
        resource_id: request.start.workflow_id.clone(),
    })
}

/// Splits a multi-operation answer into its start and update responses,
/// which must be exactly two, in request order.
fn split_multi_operation_response(
    response: ExecuteMultiOperationResponse,
) -> Result<
    (
        StartWorkflowExecutionResponse,
        UpdateWorkflowExecutionResponse,
    ),
    ClientOperationError,
> {
    use execute_multi_operation_response::response::Response;
    let mut responses = response.responses.into_iter().map(|answer| answer.response);
    match (responses.next(), responses.next(), responses.next()) {
        (
            Some(Some(Response::StartWorkflow(start))),
            Some(Some(Response::UpdateWorkflow(update))),
            None,
        ) => Ok((start, update)),
        _ => Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal update-with-start answer did not hold one start and one update response",
        ))),
    }
}

/// Fully qualified `Any` type URL of the multi-operation failure detail.
const MULTI_OPERATION_FAILURE_TYPE_URL: &str =
    "type.googleapis.com/temporal.api.errordetails.v1.MultiOperationExecutionFailure";

/// Suffix of the `Any` type URL marking an operation Temporal aborted
/// because another operation of the same request failed.
const MULTI_OPERATION_ABORTED_SUFFIX: &str =
    "temporal.api.failure.v1.MultiOperationExecutionAborted";

/// Maps a multi-operation failure to the operation that caused it.
///
/// Temporal reports one status per operation in a
/// `MultiOperationExecutionFailure` detail; every operation except the
/// failed one carries a `MultiOperationExecutionAborted` detail. A failed
/// start is mapped like a plain start's status, so a conflict is the typed
/// already-started error with the existing run; a failed update keeps its
/// status code. A status without that detail (a transport failure, for
/// example) is mapped as an ordinary RPC status. The detail's type URL is
/// checked before decoding, as for query failures, because lenient protobuf
/// decoding could otherwise accept an unrelated detail.
fn map_multi_operation_status(workflow_id: &str, status: Status) -> ClientOperationError {
    let Some(failure) = multi_operation_failure(status.details()) else {
        return map_rpc_status(status);
    };
    let culprit = failure
        .statuses
        .into_iter()
        .enumerate()
        .find(|(_, operation)| {
            operation.code != Code::Ok as i32
                && !operation
                    .details
                    .iter()
                    .any(|detail| detail.type_url.ends_with(MULTI_OPERATION_ABORTED_SUFFIX))
        });
    match culprit {
        Some((0, operation)) => {
            let details = RpcStatus {
                code: operation.code,
                message: String::new(),
                details: operation.details,
            }
            .encode_to_vec();
            map_start_status(
                workflow_id,
                Status::with_details(Code::from(operation.code), "", details.into()),
            )
        }
        Some((_, operation)) => ClientOperationError::Rpc {
            code: rpc_code(Code::from(operation.code)).to_owned(),
        },
        None => map_rpc_status(status),
    }
}

/// Decodes the `MultiOperationExecutionFailure` status detail, or `None`
/// when the first detail is absent, malformed, or of another type.
fn multi_operation_failure(details: &[u8]) -> Option<MultiOperationExecutionFailure> {
    let status = RpcStatus::decode(details).ok()?;
    let detail = status.details.first()?;
    if detail.type_url != MULTI_OPERATION_FAILURE_TYPE_URL {
        return None;
    }
    MultiOperationExecutionFailure::decode(detail.value.as_slice()).ok()
}

/// Encodes one update-with-start outcome and validates the strictly
/// reparsed document before ownership leaves Rust.
pub(crate) fn encode_update_with_start_outcome(
    outcome: &UpdateWithStartOutcome,
) -> Result<String, protocol::ProtocolError> {
    let document = match outcome {
        UpdateWithStartOutcome::Accepted { start, update } => {
            UpdateWithStartOutcomeDocument::Accepted {
                execution: start.execution.clone(),
                started: start.started,
                update: match update {
                    Ok(response) => UpdateResultDocument::Response {
                        update_id: response.update_id.clone(),
                        execution: response.execution.clone(),
                        outcome: response.outcome.clone(),
                    },
                    Err(error) => UpdateResultDocument::Error {
                        error: error_document(error),
                    },
                },
            }
        }
        UpdateWithStartOutcome::Rejected(error) => UpdateWithStartOutcomeDocument::Rejected {
            error: error_document(error),
        },
        UpdateWithStartOutcome::Unknown {
            request_id,
            workflow_id,
        } => UpdateWithStartOutcomeDocument::Unknown {
            request_id: request_id.clone(),
            workflow_id: workflow_id.clone(),
        },
    };
    validate_update_with_start_outcome(&document)?;
    encode_document(&document)
}

/// Validates one outcome document: identities are valid, a start rejection
/// is never a query failure, and the update half is never an
/// already-started or query failure.
fn validate_update_with_start_outcome(
    document: &UpdateWithStartOutcomeDocument,
) -> Result<(), protocol::ProtocolError> {
    match document {
        UpdateWithStartOutcomeDocument::Accepted {
            execution, update, ..
        } => {
            validate_execution(execution, "$.execution")?;
            match update {
                UpdateResultDocument::Response {
                    update_id,
                    execution,
                    outcome,
                } => {
                    validate_identifier(update_id, "$.update.update_id")?;
                    validate_execution(execution, "$.update.execution")?;
                    if let Some(outcome) = outcome {
                        validate_update_outcome(outcome)?;
                    }
                }
                UpdateResultDocument::Error { error } => {
                    validate_error_document(error, "$.update.error", false)?
                }
            }
        }
        UpdateWithStartOutcomeDocument::Rejected { error } => {
            validate_error_document(error, "$.error", true)?
        }
        UpdateWithStartOutcomeDocument::Unknown {
            request_id,
            workflow_id,
        } => {
            validate_identifier(request_id, "$.request_id")?;
            validate_identifier(workflow_id, "$.workflow_id")?;
        }
    }
    Ok(())
}

/// Validates one error document at `path`. A query failure is never valid
/// here, and an already-started failure only where `already_started` allows
/// it (a start rejection).
fn validate_error_document(
    error: &ClientErrorDocument,
    path: &str,
    already_started: bool,
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
                "error kind is not valid for this update-with-start outcome",
            ))
        }
    }
}

// Unit tests live in a separate file (repository rule); the `#[path]` module
// keeps them able to reach this file's private helpers.
#[cfg(test)]
#[path = "../../tests/support/with_start_unit.rs"]
mod with_start_unit_tests;

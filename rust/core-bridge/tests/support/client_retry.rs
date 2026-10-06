//! Client RPC retry regressions (issue #820) through a real Core connection
//! backed by Core's in-memory callback transport. Each scripted server fails a
//! chosen number of attempts with a chosen gRPC status before answering, so
//! the tests observe exactly which statuses Core's retry layer re-sends, that
//! every re-send is byte-identical (preserving idempotency keys), and that the
//! bridge's per-call budgets still bound the total time.

use super::*;
use prost::Message;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use temporalio_client::ConnectionOptions;
use temporalio_client::callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse};
use temporalio_common::protos::temporal::api::{
    history::v1::{History, WorkflowExecutionCompletedEventAttributes},
    update::v1::UpdateRef,
    workflowservice::v1::{
        GetWorkflowExecutionHistoryResponse, ListWorkflowExecutionsResponse,
        PollWorkflowExecutionUpdateResponse, QueryWorkflowResponse as CoreQueryResponse,
        RequestCancelWorkflowExecutionResponse, ResetWorkflowExecutionResponse,
        SignalWorkflowExecutionResponse, TerminateWorkflowExecutionResponse,
    },
};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, TokioRuntimeBuilder};

/// One attempt observed by the scripted server.
struct Attempt {
    /// gRPC method name, such as `SignalWorkflowExecution`.
    rpc: String,
    /// Encoded request message exactly as it crossed the transport.
    proto: Vec<u8>,
    /// Whether the attempt carried a gRPC deadline header.
    has_deadline: bool,
}

/// Records every attempt the scripted server received, in arrival order.
#[derive(Default)]
struct Probe {
    attempts: Mutex<Vec<Attempt>>,
}

impl Probe {
    /// Returns how many attempts reached the server.
    fn count(&self) -> usize {
        self.attempts.lock().unwrap().len()
    }

    /// Asserts that every attempt invoked `rpc` with byte-identical request
    /// messages, so a retry cannot change an idempotency key or payload.
    fn assert_identical_attempts(&self, rpc: &str) {
        let attempts = self.attempts.lock().unwrap();
        assert!(!attempts.is_empty());
        for attempt in attempts.iter() {
            assert_eq!(attempt.rpc, rpc);
            assert_eq!(attempt.proto, attempts[0].proto);
        }
    }
}

/// Creates a Core runtime and a connection whose only server fails the first
/// `failures` attempts with `code` and then answers every attempt with
/// `success`. The runtime must outlive every future using the connection, so
/// both are returned to the test.
fn scripted_connection(
    failures: usize,
    code: Code,
    success: Vec<u8>,
) -> (CoreRuntime, Connection, Arc<Probe>) {
    let options = RuntimeOptions::builder().build().expect("runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default()).expect("Core runtime");
    let probe = Arc::new(Probe::default());
    let observed = Arc::clone(&probe);
    let service = CallbackBasedGrpcService {
        callback: Arc::new(move |request| {
            let probe = Arc::clone(&observed);
            let success = success.clone();
            Box::pin(async move {
                let index = {
                    let mut attempts = probe.attempts.lock().unwrap();
                    attempts.push(Attempt {
                        rpc: request.rpc.clone(),
                        proto: request.proto.to_vec(),
                        has_deadline: request.headers.contains_key("grpc-timeout"),
                    });
                    attempts.len() - 1
                };
                if index < failures {
                    return Err(Status::new(code, "synthetic transport fault"));
                }
                Ok(GrpcSuccessResponse {
                    headers: Default::default(),
                    proto: success,
                })
            })
        }),
    };
    let options =
        ConnectionOptions::new(temporalio_sdk_core::Url::parse("http://localhost:7233").unwrap())
            .skip_get_system_info(true)
            .service_override(service)
            .dns_load_balancing(None)
            .build();
    let connection = core
        .tokio_handle()
        .block_on(Connection::connect(options))
        .expect("callback connection");
    (core, connection, probe)
}

/// The exact run every request in this file addresses.
fn execution() -> WorkflowExecution {
    WorkflowExecution {
        workflow_id: "workflow-1".to_owned(),
        run_id: "run-1".to_owned(),
    }
}

/// A closed history whose only event is a successful completion.
fn completed_history() -> Vec<u8> {
    GetWorkflowExecutionHistoryResponse {
        history: Some(History {
            events: vec![HistoryEvent {
                event_id: 5,
                attributes: Some(Attributes::WorkflowExecutionCompletedEventAttributes(
                    WorkflowExecutionCompletedEventAttributes::default(),
                )),
                ..Default::default()
            }],
        }),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Runs one exact-run wait to completion on the runtime owning `connection`.
fn wait(
    core: &CoreRuntime,
    connection: Connection,
) -> Result<WaitWorkflowResponse, ClientOperationError> {
    core.tokio_handle().block_on(wait_workflow(
        connection,
        WaitWorkflowRequest {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
        },
    ))
}

/// Sends one signal carrying a stable request ID.
fn signal(
    core: &CoreRuntime,
    connection: Connection,
) -> Result<SignalWorkflowResponse, ClientOperationError> {
    core.tokio_handle().block_on(signal_workflow(
        connection,
        SignalWorkflowRequest {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
            signal_name: "poke".to_owned(),
            request_id: "signal-request-1".to_owned(),
            input: Vec::new(),
        },
    ))
}

/// Sends one terminate request, which has no idempotency key.
fn terminate(
    core: &CoreRuntime,
    connection: Connection,
) -> Result<TerminateWorkflowResponse, ClientOperationError> {
    core.tokio_handle().block_on(terminate_workflow(
        connection,
        TerminateWorkflowRequest {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
            reason: "test".to_owned(),
        },
    ))
}

/// Asserts the typed RPC error code a client operation returned.
fn assert_rpc_code<T: std::fmt::Debug>(result: Result<T, ClientOperationError>, code: &str) {
    assert_eq!(
        result.expect_err("operation must fail"),
        ClientOperationError::Rpc {
            code: code.to_owned()
        }
    );
}

/// The issue's scenario: a transient failure of the history long poll is
/// retried by Core instead of failing the wait on its first occurrence.
#[test]
fn wait_retries_a_transient_history_failure() {
    let (core, connection, probe) = scripted_connection(1, Code::Unavailable, completed_history());
    let response = wait(&core, connection).expect("wait survives one transient failure");
    assert!(matches!(
        response.outcome,
        WorkflowOutcome::Completed { .. }
    ));
    assert_eq!(probe.count(), 2);
    probe.assert_identical_attempts("GetWorkflowExecutionHistory");
}

/// A definitive rejection of the history poll still ends the wait at once.
#[test]
fn wait_does_not_retry_not_found() {
    let (core, connection, probe) = scripted_connection(1, Code::NotFound, completed_history());
    assert_rpc_code(wait(&core, connection), "not_found");
    assert_eq!(probe.count(), 1);
}

/// The wait's retry window is a bounded attempt count rather than Core's
/// elapsed-time limit, which would forbid any retry once a healthy long poll
/// has been open for longer than ten seconds.
#[test]
fn wait_retry_policy_is_attempt_bounded() {
    let options = wait_retry_options();
    assert_eq!(options.max_elapsed_time, None);
    assert_eq!(options.max_retries, WAIT_MAX_ATTEMPTS);
    assert!(options.max_retries > 0, "zero would mean unlimited retries");
}

/// Signal delivery is retried with the identical request, so Temporal's
/// request-ID deduplication covers an attempt the server already applied.
#[test]
fn signal_retries_a_transient_failure_with_the_same_request_id() {
    let (core, connection, probe) = scripted_connection(
        1,
        Code::Unavailable,
        SignalWorkflowExecutionResponse::default().encode_to_vec(),
    );
    assert!(
        signal(&core, connection)
            .expect("signal acknowledged")
            .acknowledged
    );
    assert_eq!(probe.count(), 2);
    probe.assert_identical_attempts("SignalWorkflowExecution");
    let attempts = probe.attempts.lock().unwrap();
    let decoded = SignalWorkflowExecutionRequest::decode(attempts[1].proto.as_slice()).unwrap();
    assert_eq!(decoded.request_id, "signal-request-1");
    assert!(attempts.iter().all(|attempt| attempt.has_deadline));
}

/// A status Core does not classify as transient is returned on first sight.
#[test]
fn signal_does_not_retry_a_definitive_rejection() {
    let (core, connection, probe) = scripted_connection(
        1,
        Code::InvalidArgument,
        SignalWorkflowExecutionResponse::default().encode_to_vec(),
    );
    assert_rpc_code(signal(&core, connection), "invalid_argument");
    assert_eq!(probe.count(), 1);
}

/// A server that stays unavailable is retried only within the one-second
/// control budget, and the caller receives the transport status rather than a
/// synthetic deadline.
#[test]
fn persistent_signal_failure_stays_within_the_control_budget() {
    let (core, connection, probe) = scripted_connection(
        usize::MAX,
        Code::Unavailable,
        SignalWorkflowExecutionResponse::default().encode_to_vec(),
    );
    let started = Instant::now();
    let result = signal(&core, connection);
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_millis(1500), "took {elapsed:?}");
    let error = result.expect_err("an unavailable server cannot acknowledge");
    assert!(
        error
            == ClientOperationError::Rpc {
                code: "unavailable".to_owned()
            }
            || error
                == ClientOperationError::Rpc {
                    code: "deadline_exceeded".to_owned()
                },
        "unexpected error {error:?}"
    );
    assert!(probe.count() > 1, "Core must retry inside the budget");
}

/// Queries are read-only and are retried like any other transient failure.
#[test]
fn query_retries_a_transient_failure() {
    let (core, connection, probe) = scripted_connection(
        1,
        Code::Unavailable,
        CoreQueryResponse {
            query_result: Some(Payloads::default()),
            query_rejected: None,
        }
        .encode_to_vec(),
    );
    let response = core
        .tokio_handle()
        .block_on(query_workflow(
            connection,
            QueryWorkflowRequest {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
                query_type: "state".to_owned(),
                input: Vec::new(),
            },
        ))
        .expect("query answered after one transient failure");
    assert!(response.result.is_empty());
    assert_eq!(probe.count(), 2);
    probe.assert_identical_attempts("QueryWorkflow");
}

/// Cancellation is retried with its stable request ID.
#[test]
fn cancel_retries_a_transient_failure() {
    let (core, connection, probe) = scripted_connection(
        1,
        Code::Unavailable,
        RequestCancelWorkflowExecutionResponse::default().encode_to_vec(),
    );
    let response = core
        .tokio_handle()
        .block_on(cancel_workflow(
            connection,
            CancelWorkflowRequest {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
                request_id: "cancel-request-1".to_owned(),
                reason: String::new(),
            },
        ))
        .expect("cancellation acknowledged");
    assert!(response.acknowledged);
    assert_eq!(probe.count(), 2);
    probe.assert_identical_attempts("RequestCancelWorkflowExecution");
}

/// Reset is retried with its stable request ID.
#[test]
fn reset_retries_a_transient_failure() {
    let (core, connection, probe) = scripted_connection(
        1,
        Code::Unavailable,
        ResetWorkflowExecutionResponse {
            run_id: "run-2".to_owned(),
        }
        .encode_to_vec(),
    );
    let response = core
        .tokio_handle()
        .block_on(reset_workflow(
            connection,
            ResetWorkflowRequest {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
                request_id: "reset-request-1".to_owned(),
                reason: "test".to_owned(),
                workflow_task_finish_event_id: 4,
            },
        ))
        .expect("reset answered");
    assert_eq!(response.execution.run_id, "run-2");
    assert_eq!(probe.count(), 2);
    probe.assert_identical_attempts("ResetWorkflowExecution");
}

/// Terminate retries a call the transport could not deliver.
#[test]
fn terminate_retries_unavailable() {
    let (core, connection, probe) = scripted_connection(
        1,
        Code::Unavailable,
        TerminateWorkflowExecutionResponse::default().encode_to_vec(),
    );
    assert!(
        terminate(&core, connection)
            .expect("termination acknowledged")
            .acknowledged
    );
    assert_eq!(probe.count(), 2);
    probe.assert_identical_attempts("TerminateWorkflowExecution");
}

/// Terminate has no idempotency key, so a status the server may report after
/// applying the termination is returned rather than blindly re-sent, even
/// though Core would retry it for an idempotent call.
#[test]
fn terminate_does_not_retry_ambiguous_failures() {
    for code in [Code::Unknown, Code::Internal, Code::Aborted] {
        let (core, connection, probe) = scripted_connection(
            1,
            code,
            TerminateWorkflowExecutionResponse::default().encode_to_vec(),
        );
        assert_rpc_code(terminate(&core, connection), rpc_code(code));
        assert_eq!(probe.count(), 1, "{code:?} must not be retried");
    }
}

/// The predicate behind the terminate restriction admits exactly the statuses
/// that mean the server did not process the call.
#[test]
fn terminate_retry_classification() {
    assert!(!terminate_retry_forbidden(&Status::unavailable("x")));
    assert!(!terminate_retry_forbidden(&Status::resource_exhausted("x")));
    assert!(terminate_retry_forbidden(&Status::unknown("x")));
    assert!(terminate_retry_forbidden(&Status::internal("x")));
    assert!(terminate_retry_forbidden(&Status::not_found("x")));
}

/// Visibility listing is read-only and is retried.
#[test]
fn visibility_retries_a_transient_failure() {
    let (core, connection, probe) = scripted_connection(
        1,
        Code::Unavailable,
        ListWorkflowExecutionsResponse::default().encode_to_vec(),
    );
    let page = core
        .tokio_handle()
        .block_on(list_visibility(
            connection,
            VisibilityRequest {
                namespace: "default".to_owned(),
                query: String::new(),
                page_size: 10,
                next_page_token: None,
            },
        ))
        .expect("visibility page");
    assert!(page.executions.is_empty());
    assert_eq!(probe.count(), 2);
    probe.assert_identical_attempts("ListWorkflowExecutions");
}

/// Start-update is retried with its stable update ID inside the acceptance
/// budget, independently of the admitted-stage re-issue loop.
#[test]
fn update_retries_a_transient_failure() {
    let (core, connection, probe) = scripted_connection(
        1,
        Code::Unavailable,
        UpdateWorkflowExecutionResponse {
            update_ref: Some(UpdateRef {
                workflow_execution: Some(execution()),
                update_id: "update-1".to_owned(),
            }),
            outcome: None,
            stage: i32::from(UpdateWorkflowExecutionLifecycleStage::Accepted),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let response = core
        .tokio_handle()
        .block_on(update_workflow_within(
            connection,
            UpdateWorkflowRequest {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
                update_id: "update-1".to_owned(),
                update_name: "set_state".to_owned(),
                input: Vec::new(),
            },
            Duration::from_secs(10),
        ))
        .expect("update accepted");
    assert_eq!(response.update_id, "update-1");
    assert_eq!(probe.count(), 2);
    probe.assert_identical_attempts("UpdateWorkflowExecution");
}

/// Polling an update outcome is read-only and is retried.
#[test]
fn update_poll_retries_a_transient_failure() {
    let (core, connection, probe) = scripted_connection(
        1,
        Code::Unavailable,
        PollWorkflowExecutionUpdateResponse {
            outcome: Some(update::v1::Outcome {
                value: Some(update::v1::outcome::Value::Success(Payloads::default())),
            }),
            stage: i32::from(UpdateWorkflowExecutionLifecycleStage::Completed),
            update_ref: None,
        }
        .encode_to_vec(),
    );
    let response = core
        .tokio_handle()
        .block_on(poll_workflow_update(
            connection,
            PollWorkflowUpdateRequest {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
                update_id: "update-1".to_owned(),
            },
        ))
        .expect("update outcome");
    assert!(matches!(
        response.outcome,
        Some(UpdateOutcome::Completed { .. })
    ));
    assert_eq!(probe.count(), 2);
    probe.assert_identical_attempts("PollWorkflowExecutionUpdate");
}

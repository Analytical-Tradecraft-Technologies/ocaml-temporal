//! Start-update acceptance regressions (issue #772) through a real Core
//! connection backed by Core's in-memory callback transport. The scripted
//! server answers `UpdateWorkflowExecution` with an `Admitted`-only response a
//! chosen number of times before its final reply, which is exactly what
//! Temporal Server does when its long poll expires before a worker processes
//! the update.

use super::*;
use prost::Message;
use std::sync::{Arc, Mutex};
use temporalio_client::ConnectionOptions;
use temporalio_client::callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse};
use temporalio_client::tonic::Status as RpcStatus;
use temporalio_common::protos::temporal::api::{
    failure::v1::Failure as CoreFailure, update::v1::UpdateRef,
};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, TokioRuntimeBuilder};

/// Reply the scripted server sends once its admitted-only answers run out.
#[derive(Clone, Copy)]
enum Final {
    /// Stage `Accepted` with no outcome: a durable, still-running update.
    Accepted,
    /// Stage `Completed` with a failure outcome: the validator rejected it.
    Rejected,
    /// Keep answering `Admitted` forever, so only the budget ends the call.
    NeverAccepted,
    /// Stage `Completed` but no outcome, which is an invalid server answer.
    CompletedWithoutOutcome,
}

/// Records every update request the scripted server received.
#[derive(Default)]
struct Probe {
    requests: Mutex<Vec<UpdateWorkflowExecutionRequest>>,
}

/// Builds the update reference Temporal echoes for the request under test.
fn update_ref() -> UpdateRef {
    UpdateRef {
        workflow_execution: Some(WorkflowExecution {
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
        }),
        update_id: "update-1".to_owned(),
    }
}

/// Builds the response the scripted server sends for attempt `index`
/// (zero-based): `admitted` admitted-only answers, then the `last` reply.
fn scripted_response(
    index: usize,
    admitted: usize,
    last: Final,
) -> UpdateWorkflowExecutionResponse {
    let admitted_only = UpdateWorkflowExecutionResponse {
        update_ref: Some(update_ref()),
        outcome: None,
        stage: UpdateWorkflowExecutionLifecycleStage::Admitted as i32,
        ..Default::default()
    };
    if index < admitted {
        return admitted_only;
    }
    match last {
        Final::NeverAccepted => admitted_only,
        Final::Accepted => UpdateWorkflowExecutionResponse {
            update_ref: Some(update_ref()),
            outcome: None,
            stage: UpdateWorkflowExecutionLifecycleStage::Accepted as i32,
            ..Default::default()
        },
        Final::Rejected => UpdateWorkflowExecutionResponse {
            update_ref: Some(update_ref()),
            outcome: Some(update::v1::Outcome {
                value: Some(update::v1::outcome::Value::Failure(CoreFailure {
                    message: "validator rejected the update".to_owned(),
                    ..Default::default()
                })),
            }),
            stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
            ..Default::default()
        },
        Final::CompletedWithoutOutcome => UpdateWorkflowExecutionResponse {
            update_ref: Some(update_ref()),
            outcome: None,
            stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
            ..Default::default()
        },
    }
}

/// Creates a Core runtime and a connection whose only server is the scripted
/// update callback. The runtime must outlive every future using the
/// connection, so both are returned to the test.
fn scripted_connection(admitted: usize, last: Final) -> (CoreRuntime, Connection, Arc<Probe>) {
    let options = RuntimeOptions::builder().build().expect("runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default()).expect("Core runtime");
    let probe = Arc::new(Probe::default());
    let observed = Arc::clone(&probe);
    let service = CallbackBasedGrpcService {
        callback: Arc::new(move |request| {
            let probe = Arc::clone(&observed);
            Box::pin(async move {
                if request.rpc == "GetSystemInfo" {
                    return Err(RpcStatus::unimplemented(
                        "Method temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo is unimplemented",
                    ));
                }
                assert_eq!(request.rpc, "UpdateWorkflowExecution");
                let decoded = UpdateWorkflowExecutionRequest::decode(request.proto)
                    .expect("valid update request");
                let index = {
                    let mut requests = probe.requests.lock().unwrap();
                    requests.push(decoded);
                    requests.len() - 1
                };
                Ok(GrpcSuccessResponse {
                    headers: Default::default(),
                    proto: scripted_response(index, admitted, last).encode_to_vec(),
                })
            })
        }),
    };
    let options =
        ConnectionOptions::new(temporalio_sdk_core::Url::parse("http://localhost:7233").unwrap())
            .service_override(service)
            .dns_load_balancing(None)
            .build();
    let connection = core
        .tokio_handle()
        .block_on(Connection::connect(options))
        .expect("callback connection");
    (core, connection, probe)
}

/// The update request every test sends.
fn update_request() -> UpdateWorkflowRequest {
    UpdateWorkflowRequest {
        namespace: "default".to_owned(),
        workflow_id: "workflow-1".to_owned(),
        run_id: "run-1".to_owned(),
        update_id: "update-1".to_owned(),
        update_name: "set_state".to_owned(),
        input: Vec::new(),
    }
}

/// Runs one bounded start-update call on the runtime that owns `connection`.
fn start_update(
    core: &CoreRuntime,
    connection: Connection,
    budget: Duration,
) -> Result<UpdateWorkflowResponse, ClientOperationError> {
    core.tokio_handle()
        .block_on(update_workflow_within(connection, update_request(), budget))
}

/// Admitted-only answers are re-issued with the same update ID until the
/// server reports acceptance; only then is a pending handle response returned.
#[test]
fn admitted_update_is_reissued_until_accepted() {
    let (core, connection, probe) = scripted_connection(2, Final::Accepted);
    let response = start_update(&core, connection, Duration::from_secs(10))
        .expect("update accepted after admission");
    assert_eq!(response.update_id, "update-1");
    assert_eq!(response.execution.run_id, "run-1");
    assert_eq!(response.outcome, None);
    let requests = probe.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for request in requests.iter() {
        let meta = request.request.as_ref().unwrap().meta.as_ref().unwrap();
        assert_eq!(meta.update_id, "update-1");
        assert_eq!(
            request.wait_policy.as_ref().unwrap().lifecycle_stage,
            UpdateWorkflowExecutionLifecycleStage::Accepted as i32
        );
    }
}

/// A validator rejection that arrives after admitted-only answers is returned
/// as the typed failure outcome, which the OCaml client reports as `Error`.
#[test]
fn admitted_update_rejection_is_returned_as_failure_outcome() {
    let (core, connection, probe) = scripted_connection(1, Final::Rejected);
    let response = start_update(&core, connection, Duration::from_secs(10))
        .expect("rejection is a typed outcome");
    match response.outcome {
        Some(UpdateOutcome::Failed { failure }) => {
            assert_eq!(failure.message, "validator rejected the update");
        }
        other => panic!("expected a rejected outcome, got {other:?}"),
    }
    assert_eq!(probe.requests.lock().unwrap().len(), 2);
}

/// An update that is never accepted within the budget is a typed deadline
/// error, never a handle for an update that is not durable.
#[test]
fn never_accepted_update_returns_deadline_error() {
    let (core, connection, probe) = scripted_connection(0, Final::NeverAccepted);
    let error = start_update(&core, connection, Duration::from_millis(450))
        .expect_err("admitted-only update must not return a handle");
    assert_eq!(
        error,
        ClientOperationError::Rpc {
            code: "deadline_exceeded".to_owned(),
        }
    );
    // The readmission pause bounds the retry rate against a server that
    // answers immediately: at most one attempt per pause within the budget.
    let attempts = probe.requests.lock().unwrap().len();
    assert!(
        (2..=5).contains(&attempts),
        "unexpected attempts: {attempts}"
    );
}

/// A `Completed` stage without an outcome fails closed as a Core defect.
#[test]
fn completed_stage_without_outcome_fails_closed() {
    let (core, connection, _probe) = scripted_connection(0, Final::CompletedWithoutOutcome);
    let error = start_update(&core, connection, Duration::from_secs(10))
        .expect_err("completed stage needs an outcome");
    assert!(matches!(error, ClientOperationError::Core(_)));
}

/// Stage classification retries only the not-yet-accepted stages and rejects
/// stages this bridge does not know.
#[test]
fn lifecycle_stage_classification() {
    assert!(
        update_stage_is_accepted(UpdateWorkflowExecutionLifecycleStage::Accepted as i32).unwrap()
    );
    assert!(
        !update_stage_is_accepted(UpdateWorkflowExecutionLifecycleStage::Admitted as i32).unwrap()
    );
    assert!(!update_stage_is_accepted(0).unwrap());
    assert!(update_stage_is_accepted(99).is_err());
}

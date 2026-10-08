//! Current-run client regressions (issue #791) and completed-successor
//! retention (issue #837) through a real Core connection whose only server is
//! Core's in-memory callback transport.
//!
//! Every operation is decoded from its closed JSON document with an empty
//! `run_id`, so the tests cover the bridge validators as well as the protobuf
//! request Temporal receives. The scripted server records the workflow
//! execution each RPC named and answers as Temporal does for the current run:
//! an update reference names the run it resolved, and a history poll returns
//! that run's close event without naming the run.

use super::*;
use prost::Message;
use std::sync::{Arc, Mutex};
use temporalio_client::ConnectionOptions;
use temporalio_client::callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse};
use temporalio_client::tonic::Status as RpcStatus;
use temporalio_common::protos::temporal::api::{
    common::v1::Payload as CorePayload,
    history::v1::{History, WorkflowExecutionCompletedEventAttributes, history_event},
    update::v1::UpdateRef,
    workflowservice::v1::{
        GetWorkflowExecutionHistoryResponse, PollWorkflowExecutionUpdateResponse,
        QueryWorkflowResponse as CoreQueryResponse, ResetWorkflowExecutionResponse,
    },
};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, TokioRuntimeBuilder};

/// Run ID the scripted server reports for the workflow's current run.
const CURRENT_RUN: &str = "current-run";

/// Run ID of the cron successor linked from the completed event.
const NEXT_RUN: &str = "next-run";

/// What the scripted server answers to `UpdateWorkflowExecution`.
#[derive(Clone, Copy)]
enum UpdateReply {
    /// Name the resolved current run, as Temporal Server does.
    Resolved,
    /// Leave the run ID empty, which the bridge must reject.
    Omitted,
    /// Name a run other than the one an exact request asked for.
    Different,
}

/// Records the RPC name and workflow execution of every request received.
#[derive(Default)]
struct Probe {
    calls: Mutex<Vec<(String, WorkflowExecution)>>,
}

impl Probe {
    /// Returns the run ID each recorded RPC sent, in arrival order.
    fn run_ids(&self) -> Vec<(String, String)> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(rpc, execution)| (rpc.clone(), execution.run_id.clone()))
            .collect()
    }
}

/// Decodes the workflow execution a recorded RPC named. Each arm uses the
/// official request message for that RPC, so a field mix-up fails here.
fn decoded_execution(
    rpc: &str,
    proto: temporalio_client::tonic::codegen::Bytes,
) -> WorkflowExecution {
    let execution = match rpc {
        "GetWorkflowExecutionHistory" => {
            GetWorkflowExecutionHistoryRequest::decode(proto)
                .expect("history request")
                .execution
        }
        "SignalWorkflowExecution" => {
            SignalWorkflowExecutionRequest::decode(proto)
                .expect("signal request")
                .workflow_execution
        }
        "QueryWorkflow" => {
            QueryWorkflowExecutionRequest::decode(proto)
                .expect("query request")
                .execution
        }
        "RequestCancelWorkflowExecution" => {
            RequestCancelWorkflowExecutionRequest::decode(proto)
                .expect("cancel request")
                .workflow_execution
        }
        "TerminateWorkflowExecution" => {
            TerminateWorkflowExecutionRequest::decode(proto)
                .expect("terminate request")
                .workflow_execution
        }
        "ResetWorkflowExecution" => {
            ResetWorkflowExecutionRequest::decode(proto)
                .expect("reset request")
                .workflow_execution
        }
        "UpdateWorkflowExecution" => {
            UpdateWorkflowExecutionRequest::decode(proto)
                .expect("update request")
                .workflow_execution
        }
        "PollWorkflowExecutionUpdate" => PollWorkflowExecutionUpdateRequest::decode(proto)
            .expect("poll update request")
            .update_ref
            .and_then(|update_ref| update_ref.workflow_execution),
        other => panic!("unexpected RPC {other}"),
    };
    execution.expect("request named a workflow execution")
}

/// Builds the protobuf reply the scripted server sends for `rpc`.
fn scripted_reply(rpc: &str, update: UpdateReply) -> Vec<u8> {
    match rpc {
        "GetWorkflowExecutionHistory" => GetWorkflowExecutionHistoryResponse {
            history: Some(History {
                events: vec![HistoryEvent {
                    event_id: 5,
                    attributes: Some(
                        history_event::Attributes::WorkflowExecutionCompletedEventAttributes(
                            WorkflowExecutionCompletedEventAttributes {
                                result: Some(Payloads {
                                    payloads: vec![CorePayload {
                                        metadata: HashMap::from([(
                                            "encoding".to_owned(),
                                            b"json/plain".to_vec(),
                                        )]),
                                        data: b"\"done\"".to_vec(),
                                        ..Default::default()
                                    }],
                                }),
                                new_execution_run_id: NEXT_RUN.to_owned(),
                                ..Default::default()
                            },
                        ),
                    ),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        }
        .encode_to_vec(),
        "QueryWorkflow" => CoreQueryResponse {
            query_result: Some(Payloads::default()),
            query_rejected: None,
        }
        .encode_to_vec(),
        "ResetWorkflowExecution" => ResetWorkflowExecutionResponse {
            run_id: "reset-run".to_owned(),
        }
        .encode_to_vec(),
        "UpdateWorkflowExecution" => {
            let run_id = match update {
                UpdateReply::Resolved => CURRENT_RUN,
                UpdateReply::Omitted => "",
                UpdateReply::Different => "other-run",
            };
            UpdateWorkflowExecutionResponse {
                update_ref: Some(UpdateRef {
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id: "workflow-1".to_owned(),
                        run_id: run_id.to_owned(),
                    }),
                    update_id: "update-1".to_owned(),
                }),
                outcome: None,
                stage: i32::from(UpdateWorkflowExecutionLifecycleStage::Accepted),
                ..Default::default()
            }
            .encode_to_vec()
        }
        "PollWorkflowExecutionUpdate" => PollWorkflowExecutionUpdateResponse {
            outcome: Some(update::v1::Outcome {
                value: Some(update::v1::outcome::Value::Success(Payloads::default())),
            }),
            ..Default::default()
        }
        .encode_to_vec(),
        // Signal, cancel, and terminate acknowledge with an empty message.
        _ => Vec::new(),
    }
}

/// Creates a Core runtime and a connection whose only server is the scripted
/// callback. The runtime must outlive every future using the connection, so
/// both are returned to the test.
fn scripted_connection(update: UpdateReply) -> (CoreRuntime, Connection, Arc<Probe>) {
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
                let execution = decoded_execution(&request.rpc, request.proto);
                probe
                    .calls
                    .lock()
                    .unwrap()
                    .push((request.rpc.clone(), execution));
                Ok(GrpcSuccessResponse {
                    headers: Default::default(),
                    proto: scripted_reply(&request.rpc, update),
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

/// Every run-addressed operation accepts an empty run ID at the JSON boundary
/// and forwards it unchanged, so Temporal resolves the workflow's current run.
#[test]
fn current_run_requests_reach_temporal_with_an_empty_run_id() {
    let (core, connection, probe) = scripted_connection(UpdateReply::Resolved);
    let signal = decode_signal_request(
        r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"","signal_name":"add","request_id":"signal-1","input":[]}"#,
    )
    .expect("current-run signal document");
    core.tokio_handle()
        .block_on(signal_workflow(connection.clone(), signal))
        .expect("signal acknowledged");
    let query = decode_query_request(
        r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"","query_type":"state","input":[]}"#,
    )
    .expect("current-run query document");
    core.tokio_handle()
        .block_on(query_workflow(connection.clone(), query))
        .expect("query answered");
    let cancel = decode_cancel_request(
        r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"","request_id":"cancel-1","reason":""}"#,
    )
    .expect("current-run cancel document");
    core.tokio_handle()
        .block_on(cancel_workflow(connection.clone(), cancel))
        .expect("cancel acknowledged");
    let terminate = decode_terminate_request(
        r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"","reason":""}"#,
    )
    .expect("current-run terminate document");
    core.tokio_handle()
        .block_on(terminate_workflow(connection.clone(), terminate))
        .expect("terminate acknowledged");
    let reset = decode_reset_request(
        r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"","request_id":"reset-1","reason":"","workflow_task_finish_event_id":3}"#,
    )
    .expect("current-run reset document");
    let reset = core
        .tokio_handle()
        .block_on(reset_workflow(connection.clone(), reset))
        .expect("reset accepted");
    assert_eq!(reset.execution.run_id, "reset-run");
    let poll = decode_poll_update_request(
        r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"","update_id":"update-1"}"#,
    )
    .expect("current-run poll document");
    core.tokio_handle()
        .block_on(poll_workflow_update(connection.clone(), poll))
        .expect("poll answered");
    let rpcs: Vec<String> = probe
        .run_ids()
        .into_iter()
        .map(|(rpc, run_id)| {
            assert_eq!(run_id, "", "{rpc} must not invent a run ID");
            rpc
        })
        .collect();
    assert_eq!(
        rpcs,
        [
            "SignalWorkflowExecution",
            "QueryWorkflow",
            "RequestCancelWorkflowExecution",
            "TerminateWorkflowExecution",
            "ResetWorkflowExecution",
            "PollWorkflowExecutionUpdate",
        ]
    );
}

/// A current-run wait sends an empty run ID, echoes the empty selector in its
/// response, and keeps the completed event's cron successor (#837) through
/// the strict response encoder and decoder.
#[test]
fn current_run_wait_echoes_selector_and_keeps_completed_successor() {
    let (core, connection, probe) = scripted_connection(UpdateReply::Resolved);
    let request =
        decode_wait_request(r#"{"namespace":"default","workflow_id":"workflow-1","run_id":""}"#)
            .expect("current-run wait document");
    let response = core
        .tokio_handle()
        .block_on(wait_workflow(connection, request))
        .expect("close event observed");
    assert_eq!(response.execution.run_id, "");
    match &response.outcome {
        WorkflowOutcome::Completed { result, successor } => {
            assert_eq!(result.len(), 1);
            assert_eq!(
                successor
                    .as_ref()
                    .map(|successor| successor.run_id.as_str()),
                Some(NEXT_RUN)
            );
        }
        other => panic!("expected a completed outcome, got {other:?}"),
    }
    let encoded = encode_wait_response(&response).expect("current-run response encodes");
    assert_eq!(
        serde_json::from_str::<WaitWorkflowResponse>(&encoded).unwrap(),
        response
    );
    assert_eq!(
        probe.run_ids(),
        [("GetWorkflowExecutionHistory".to_owned(), String::new())]
    );
}

/// A current-run update adopts the run Temporal resolved, so later polls of
/// the update target the run that accepted it.
#[test]
fn current_run_update_adopts_the_resolved_run() {
    let (core, connection, probe) = scripted_connection(UpdateReply::Resolved);
    let request = decode_update_request(
        r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"","update_id":"update-1","update_name":"set","input":[]}"#,
    )
    .expect("current-run update document");
    let response = core
        .tokio_handle()
        .block_on(update_workflow_within(
            connection,
            request,
            Duration::from_secs(10),
        ))
        .expect("update accepted");
    assert_eq!(response.execution.run_id, CURRENT_RUN);
    encode_update_response(&response).expect("resolved run encodes");
    assert_eq!(
        probe.run_ids(),
        [("UpdateWorkflowExecution".to_owned(), String::new())]
    );
}

/// A current-run update whose response names no run fails closed instead of
/// producing an update handle without a run identity.
#[test]
fn current_run_update_without_resolved_run_fails_closed() {
    let (core, connection, _probe) = scripted_connection(UpdateReply::Omitted);
    let request = decode_update_request(
        r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"","update_id":"update-1","update_name":"set","input":[]}"#,
    )
    .expect("current-run update document");
    let error = core
        .tokio_handle()
        .block_on(update_workflow_within(
            connection,
            request,
            Duration::from_secs(10),
        ))
        .expect_err("a response without a run is invalid");
    assert!(matches!(error, ClientOperationError::Core(_)));
}

/// An exact-run update must still be answered for exactly that run.
#[test]
fn exact_run_update_rejects_a_different_run() {
    let (core, connection, _probe) = scripted_connection(UpdateReply::Different);
    let request = decode_update_request(
        r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"current-run","update_id":"update-1","update_name":"set","input":[]}"#,
    )
    .expect("exact-run update document");
    let error = core
        .tokio_handle()
        .block_on(update_workflow_within(
            connection,
            request,
            Duration::from_secs(10),
        ))
        .expect_err("a different run is a server defect");
    assert!(matches!(error, ClientOperationError::Core(_)));
}

/// The relaxed selector still rejects NUL bytes and oversized identifiers,
/// and start requests and responses keep requiring a concrete run.
#[test]
fn run_selector_rejects_malformed_non_empty_values() {
    assert!(
        decode_wait_request(
            r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"a\u0000b"}"#
        )
        .is_err()
    );
    let oversized = "r".repeat(protocol::MAX_STRING_BYTES + 1);
    let document = format!(
        r#"{{"namespace":"default","workflow_id":"workflow-1","run_id":"{oversized}","signal_name":"add","request_id":"signal-1","input":[]}}"#
    );
    assert!(decode_signal_request(&document).is_err());
    let response = StartWorkflowResponse {
        execution: ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: String::new(),
        },
        started: true,
    };
    assert!(encode_start_response(&response).is_err());
}

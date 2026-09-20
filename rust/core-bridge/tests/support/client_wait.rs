//! Production wait-path regressions using Core's in-memory gRPC transport.
//! Delays exceed the owner time slice; counters distinguish retained requests
//! from a retry loop which repeatedly cancels and restarts the same RPC.

use super::*;
use prost::Message;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use temporalio_client::callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse};
use temporalio_client::tonic::Status as RpcStatus;
use temporalio_common::protos::temporal::api::{
    enums::v1::{EventType, HistoryEventFilterType},
    history::v1::{
        History, HistoryEvent, WorkflowExecutionCompletedEventAttributes, history_event,
    },
    workflowservice::v1::{
        GetWorkflowExecutionHistoryRequest, GetWorkflowExecutionHistoryResponse,
        RequestCancelWorkflowExecutionRequest,
    },
};

/// Synthetic server behavior selected independently for each connected owner.
#[derive(Clone, Copy)]
enum Reply {
    Completed,
    Paginated,
    Pending,
    Denied,
}

/// Counts actual RPC lifetime events rather than bridge polling attempts.
#[derive(Default)]
struct Probe {
    requests: Mutex<Vec<GetWorkflowExecutionHistoryRequest>>,
    cancellations: Mutex<Vec<RequestCancelWorkflowExecutionRequest>>,
    completed: AtomicUsize,
    dropped: AtomicUsize,
}

/// Records cancellation as well as ordinary RPC completion.
struct RpcDrop(Arc<Probe>);

impl Drop for RpcDrop {
    /// A dropped in-flight callback proves that its request was released.
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// Builds a real Core connection whose history RPCs take 150 ms per page.
fn connected_runtime(reply: Reply) -> (Runtime, Arc<Probe>) {
    let options = RuntimeOptions::builder().build().expect("runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default()).expect("Core runtime");
    let mut runtime = Runtime::new(core).expect("runtime cleanup thread");
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
                if request.rpc == "RequestCancelWorkflowExecution" {
                    probe.cancellations.lock().unwrap().push(
                        RequestCancelWorkflowExecutionRequest::decode(request.proto)
                            .expect("valid cancellation request"),
                    );
                    return Ok(GrpcSuccessResponse {
                        headers: Default::default(),
                        proto: Vec::new(),
                    });
                }
                assert_eq!(request.rpc, "GetWorkflowExecutionHistory");
                let request = GetWorkflowExecutionHistoryRequest::decode(request.proto)
                    .expect("valid history request");
                probe.requests.lock().unwrap().push(request.clone());
                let _drop = RpcDrop(Arc::clone(&probe));
                if matches!(reply, Reply::Pending) {
                    std::future::pending::<()>().await;
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
                probe.completed.fetch_add(1, Ordering::SeqCst);
                if matches!(reply, Reply::Denied) {
                    return Err(RpcStatus::permission_denied("synthetic denial"));
                }
                let response = if matches!(reply, Reply::Paginated)
                    && request.next_page_token.is_empty()
                {
                    GetWorkflowExecutionHistoryResponse {
                        next_page_token: b"next-page".to_vec(),
                        ..Default::default()
                    }
                } else {
                    GetWorkflowExecutionHistoryResponse {
                        history: Some(History {
                            events: vec![HistoryEvent {
                                event_id: 7,
                                event_type: EventType::WorkflowExecutionCompleted as i32,
                                attributes: Some(history_event::Attributes::WorkflowExecutionCompletedEventAttributes(
                                    WorkflowExecutionCompletedEventAttributes::default(),
                                )),
                                ..Default::default()
                            }],
                        }),
                        ..Default::default()
                    }
                };
                Ok(GrpcSuccessResponse {
                    headers: Default::default(),
                    proto: response.encode_to_vec(),
                })
            })
        }),
    };
    let options =
        ConnectionOptions::new(temporalio_sdk_core::Url::parse("http://localhost:7233").unwrap())
            .service_override(service)
            .dns_load_balancing(None)
            .build();
    runtime.client = Some(
        runtime
            .core
            .as_ref()
            .unwrap()
            .tokio_handle()
            .block_on(Connection::connect(options))
            .expect("callback connection"),
    );
    (runtime, probe)
}

/// Uses the existing exact-run protocol without introducing a request ticket.
fn request(run: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "namespace": "default", "workflow_id": "workflow", "run_id": run,
    }))
    .unwrap()
}

/// Polls through bounded owner turns and fails quickly on the old livelock.
fn terminal(runtime: &mut Runtime, request: &[u8]) -> Operation {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match runtime.wait_workflow_json(request) {
            Err(error) if error.status == STATUS_NOT_READY => {
                assert!(
                    Instant::now() < deadline,
                    "slow successful history request never completed"
                );
            }
            result => return result,
        }
    }
}

/// A response slower than the mailbox slice must complete with exactly one RPC.
#[test]
fn delayed_history_survives_owner_windows() {
    let (mut runtime, probe) = connected_runtime(Reply::Completed);
    let response = terminal(&mut runtime, &request("run-1")).expect("terminal result");
    let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(response["execution"]["run_id"], "run-1");
    assert_eq!(probe.requests.lock().unwrap().len(), 1);
    assert_eq!(probe.completed.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert!(runtime.pending_waits.is_empty());
    terminal(&mut runtime, &request("run-1")).expect("repeated wait after completion");
    assert_eq!(probe.requests.lock().unwrap().len(), 2);
    assert!(runtime.pending_waits.is_empty());
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Both a slow first page and a slow terminal page retain their pagination state.
#[test]
fn pagination_survives_multiple_owner_windows() {
    let (mut runtime, probe) = connected_runtime(Reply::Paginated);
    terminal(&mut runtime, &request("paginated-run")).expect("terminal result");
    let requests = probe.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].next_page_token.is_empty());
    assert_eq!(requests[1].next_page_token, b"next-page");
    for request in requests.iter() {
        assert_eq!(request.namespace, "default");
        assert_eq!(request.execution.as_ref().unwrap().run_id, "paginated-run");
        assert!(request.wait_new_event);
        assert_eq!(
            request.history_event_filter_type,
            HistoryEventFilterType::CloseEvent as i32
        );
    }
    drop(requests);
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Interleaved waiters cannot replace each other's execution or request state.
#[test]
fn concurrent_exact_runs_make_progress_independently() {
    let (mut runtime, probe) = connected_runtime(Reply::Completed);
    for run in ["run-a", "run-b"] {
        assert_eq!(
            runtime
                .wait_workflow_json(&request(run))
                .unwrap_err()
                .status,
            STATUS_NOT_READY
        );
    }
    for run in ["run-a", "run-b"] {
        let bytes = terminal(&mut runtime, &request(run)).expect("terminal result");
        let result: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["execution"]["run_id"], run);
    }
    assert_eq!(probe.requests.lock().unwrap().len(), 2);
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// A delayed non-retryable RPC error releases the entry just like a result.
#[test]
fn delayed_error_is_delivered_without_restarting() {
    let (mut runtime, probe) = connected_runtime(Reply::Denied);
    assert_eq!(
        terminal(&mut runtime, &request("denied"))
            .unwrap_err()
            .status,
        STATUS_CONNECTION
    );
    assert_eq!(probe.requests.lock().unwrap().len(), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert!(runtime.pending_waits.is_empty());
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// A pending history request leaves the owner available for workflow control
/// and is cancelled promptly when its client disconnects.
#[test]
fn cancellation_and_disconnect_remain_responsive() {
    let (mut runtime, probe) = connected_runtime(Reply::Pending);
    assert_eq!(
        runtime
            .wait_workflow_json(&request("open"))
            .unwrap_err()
            .status,
        STATUS_NOT_READY
    );
    let cancel = serde_json::to_vec(&serde_json::json!({
        "namespace": "default", "workflow_id": "workflow", "run_id": "open",
        "request_id": "cancel-open", "reason": "wait regression",
    }))
    .unwrap();
    let started = Instant::now();
    runtime
        .cancel_workflow_json(&cancel)
        .expect("cancellation while waiting");
    assert!(started.elapsed() < Duration::from_secs(2));
    let requests = probe.cancellations.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].workflow_execution.as_ref().unwrap().run_id,
        "open"
    );
    drop(requests);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
    runtime
        .disconnect_client()
        .expect("disconnect while waiting");
    assert!(runtime.pending_waits.is_empty());
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Admission is bounded without blocking an already retained wait at capacity.
#[test]
fn pending_wait_capacity_rejects_only_new_executions() {
    let (mut runtime, probe) = connected_runtime(Reply::Pending);
    for index in 0..MAX_PENDING_WAITS {
        assert_eq!(
            runtime
                .wait_workflow_json(&request(&format!("run-{index}")))
                .unwrap_err()
                .status,
            STATUS_NOT_READY
        );
    }
    assert_eq!(
        runtime
            .wait_workflow_json(&request("excess"))
            .unwrap_err()
            .status,
        STATUS_INVALID_STATE
    );
    assert_eq!(
        runtime
            .wait_workflow_json(&request("run-0"))
            .unwrap_err()
            .status,
        STATUS_NOT_READY
    );
    assert_eq!(probe.requests.lock().unwrap().len(), MAX_PENDING_WAITS);
    assert_eq!(runtime.pending_waits.len(), MAX_PENDING_WAITS);
    assert_eq!(runtime.close(true), STATUS_OK);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), MAX_PENDING_WAITS);
}

/// Explicit shutdown and GC fallback must release every retained request.
#[test]
fn shutdown_cancels_pending_history_requests() {
    for wait in [true, false] {
        let (mut runtime, probe) = connected_runtime(Reply::Pending);
        for _ in 0..2 {
            assert_eq!(
                runtime
                    .wait_workflow_json(&request("open"))
                    .unwrap_err()
                    .status,
                STATUS_NOT_READY
            );
        }
        assert_eq!(probe.requests.lock().unwrap().len(), 1);
        assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
        let started = Instant::now();
        assert_eq!(runtime.close(wait), STATUS_OK);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    }
}

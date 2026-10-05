//! Start-path transport regressions through real Core connections and ABI
//! tickets. The callback transport injects failures without a network server.

use super::*;
use prost::Message;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use temporalio_client::callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse};
use temporalio_client::tonic::Status as RpcStatus;
use temporalio_common::protos::temporal::api::workflowservice::v1::{
    StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
};

/// Fault selected for one isolated client connection.
#[derive(Clone, Copy)]
enum Reply {
    Recovering,
    Denied,
    Hung,
}

/// Records wire requests and the release of their in-flight transport futures.
#[derive(Default)]
struct Probe {
    requests: Mutex<Vec<StartWorkflowExecutionRequest>>,
    dropped: AtomicUsize,
}

/// Counts cancellation as well as normal transport completion.
struct RpcDrop(Arc<Probe>);

impl Drop for RpcDrop {
    /// A timed-out callback must be dropped before its ticket is retired.
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// Connects Core to a transport which recovers after two unavailable replies,
/// rejects immediately, or leaves a request pending until its deadline.
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
                assert_eq!(request.rpc, "StartWorkflowExecution");
                let request = StartWorkflowExecutionRequest::decode(request.proto)
                    .expect("valid start request");
                let attempt = {
                    let mut requests = probe.requests.lock().unwrap();
                    requests.push(request);
                    requests.len()
                };
                let _drop = RpcDrop(Arc::clone(&probe));
                match reply {
                    Reply::Recovering if attempt <= 2 => {
                        return Err(RpcStatus::unavailable("synthetic connection interruption"));
                    }
                    Reply::Denied => return Err(RpcStatus::permission_denied("synthetic denial")),
                    Reply::Hung => std::future::pending::<()>().await,
                    Reply::Recovering => {}
                }
                Ok(GrpcSuccessResponse {
                    headers: Default::default(),
                    proto: StartWorkflowExecutionResponse {
                        run_id: "run-1".to_owned(),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                })
            })
        }),
    };
    let options = ConnectionOptions::new(
        temporalio_sdk_core::Url::parse("http://localhost:7233").expect("test URL"),
    )
    .skip_get_system_info(true)
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

/// Admits one logical start, then observes it through bounded owner turns.
fn start(runtime: &mut Runtime) -> serde_json::Value {
    let request = br#"{"request_id":"stable-request-1","namespace":"default","workflow_id":"workflow-1","workflow_type":"Workflow","task_queue":"queue","input":[]}"#;
    let ticket = runtime
        .begin_start_workflow_json(request)
        .expect("start ticket");
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        match runtime.wait_start_workflow_json(&ticket) {
            Err(error) if error.status == STATUS_NOT_READY => {
                assert!(Instant::now() < deadline, "start must remain bounded");
            }
            result => return serde_json::from_slice(&result.expect("terminal start")).unwrap(),
        }
    }
}

/// Transient failures must be handled by Core, preserving the entire wire
/// request and its idempotency key rather than admitting a new logical start.
#[test]
fn transient_start_failures_retry_the_identical_request() {
    let (mut runtime, probe) = connected_runtime(Reply::Recovering);
    let outcome = start(&mut runtime);
    assert_eq!(outcome["kind"], "accepted");
    assert_eq!(outcome["execution"]["run_id"], "run-1");
    let requests = probe.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].request_id, "stable-request-1");
    assert!(requests.iter().all(|request| request == &requests[0]));
    assert!(runtime.pending_starts.is_empty());
}

/// A definitive rejection must not be retried or reported as uncertain.
#[test]
fn permanent_start_rejection_is_not_retried() {
    let (mut runtime, probe) = connected_runtime(Reply::Denied);
    let outcome = start(&mut runtime);
    assert_eq!(outcome["kind"], "rejected");
    assert_eq!(outcome["error"]["code"], "permission_denied");
    assert_eq!(probe.requests.lock().unwrap().len(), 1);
    assert!(runtime.pending_starts.is_empty());
}

/// Core retries must remain inside the existing ten-second overall deadline;
/// an unanswered request retains uncertainty and releases its transport task.
#[test]
fn hung_start_keeps_the_existing_deadline_and_releases_the_request() {
    let (mut runtime, probe) = connected_runtime(Reply::Hung);
    let started = Instant::now();
    let outcome = start(&mut runtime);
    assert_eq!(outcome["kind"], "unknown");
    assert_eq!(outcome["request_id"], "stable-request-1");
    assert!(started.elapsed() < Duration::from_secs(12));
    assert_eq!(probe.requests.lock().unwrap().len(), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert!(runtime.pending_starts.is_empty());
}

/// Explicit client disconnect is part of SDK shutdown and must succeed while
/// another caller still holds a ticket whose RPC already reached the
/// transport. The ticket is retired exactly once, the in-flight transport
/// future is cancelled and released before disconnect returns, and a later
/// read of the abandoned ticket is a lifecycle error (which the OCaml adapter
/// reports as an uncertain start) rather than a fabricated outcome.
#[test]
fn disconnect_aborts_an_in_flight_start_and_releases_its_request() {
    let (mut runtime, probe) = connected_runtime(Reply::Hung);
    let request = br#"{"request_id":"stable-request-1","namespace":"default","workflow_id":"workflow-1","workflow_type":"Workflow","task_queue":"queue","input":[]}"#;
    let ticket = runtime
        .begin_start_workflow_json(request)
        .expect("start ticket");

    // Wait until the request is on the wire, so Temporal could have accepted
    // it and the start outcome is genuinely uncertain at disconnect time.
    let deadline = Instant::now() + Duration::from_secs(5);
    while probe.requests.lock().unwrap().is_empty() {
        assert!(
            Instant::now() < deadline,
            "start RPC never reached transport"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let disconnected = runtime.disconnect_client();
    assert!(
        disconnected.is_ok(),
        "disconnect must not fail on an in-flight start: {:?}",
        disconnected.err().map(|failure| failure.message)
    );
    assert!(runtime.pending_starts.is_empty());
    assert!(runtime.client.is_none());
    assert_eq!(probe.requests.lock().unwrap().len(), 1);
    assert_eq!(
        probe.dropped.load(Ordering::SeqCst),
        1,
        "the aborted start task must be joined before disconnect returns"
    );

    let read = runtime
        .wait_start_workflow_json(&ticket)
        .expect_err("an abandoned ticket has no observable outcome");
    assert_eq!(read.status, STATUS_INVALID_STATE);

    // Disconnect stays idempotent, and close has no start left to release.
    assert!(runtime.disconnect_client().is_ok());
    assert_eq!(runtime.close(true), STATUS_OK);
}

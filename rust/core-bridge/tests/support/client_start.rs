//! Start-path transport regressions through real Core connections and ABI
//! tickets. The callback transport injects failures without a network server.

use super::*;
use prost::Message;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use temporalio_client::callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse};
use temporalio_client::tonic::Status as RpcStatus;
use temporalio_common::protos::temporal::api::enums::v1::WorkflowIdConflictPolicy;
use temporalio_common::protos::temporal::api::workflowservice::v1::{
    StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
};

/// Fault selected for one isolated client connection.
#[derive(Clone, Copy)]
enum Reply {
    Recovering,
    Denied,
    Hung,
    /// Answers like a server applying `USE_EXISTING` to an open run: the
    /// existing run ID with `started = false`.
    Existing,
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
    let mut runtime = Runtime::new(core, None).expect("runtime cleanup thread");
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
                    Reply::Existing => {
                        return Ok(GrpcSuccessResponse {
                            headers: Default::default(),
                            proto: StartWorkflowExecutionResponse {
                                run_id: "existing-run".to_owned(),
                                started: false,
                                ..Default::default()
                            }
                            .encode_to_vec(),
                        });
                    }
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
    start_document(runtime, request)
}

/// Admits one start with an explicit workflow ID conflict policy name.
fn start_with_policy(runtime: &mut Runtime, policy: &str) -> serde_json::Value {
    let request = format!(
        r#"{{"request_id":"stable-request-1","namespace":"default","workflow_id":"workflow-1","workflow_type":"Workflow","task_queue":"queue","input":[],"id_conflict_policy":"{policy}"}}"#
    );
    start_document(runtime, request.as_bytes())
}

/// Admits one encoded start request and polls its ticket to a terminal
/// outcome within the start deadline.
fn start_document(runtime: &mut Runtime, request: &[u8]) -> serde_json::Value {
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

/// Each closed policy name reaches the wire as Temporal's explicit enum
/// value; an omitted policy is sent as `FAIL` rather than `UNSPECIFIED`. A
/// server that predates `StartWorkflowExecutionResponse.started` leaves it
/// false, so a successful non-`USE_EXISTING` start is still reported as a
/// newly started run.
#[test]
fn id_conflict_policy_reaches_the_start_request() {
    for (policy, expected) in [
        (None, WorkflowIdConflictPolicy::Fail),
        (Some("fail"), WorkflowIdConflictPolicy::Fail),
        (
            Some("terminate_existing"),
            WorkflowIdConflictPolicy::TerminateExisting,
        ),
    ] {
        let (mut runtime, probe) = connected_runtime(Reply::Recovering);
        let outcome = match policy {
            None => start(&mut runtime),
            Some(policy) => start_with_policy(&mut runtime, policy),
        };
        assert_eq!(outcome["kind"], "accepted");
        assert_eq!(outcome["execution"]["run_id"], "run-1");
        assert_eq!(outcome["started"], true);
        let requests = probe.requests.lock().unwrap();
        assert!(
            requests
                .iter()
                .all(|request| request.workflow_id_conflict_policy == i32::from(expected))
        );
    }
}

/// `USE_EXISTING` returns the open run Temporal reports, not a fabricated new
/// run, and preserves `started = false` so OCaml can tell the two apart.
#[test]
fn use_existing_returns_the_existing_run_id() {
    let (mut runtime, probe) = connected_runtime(Reply::Existing);
    let outcome = start_with_policy(&mut runtime, "use_existing");
    assert_eq!(outcome["kind"], "accepted");
    assert_eq!(outcome["execution"]["run_id"], "existing-run");
    assert_eq!(outcome["started"], false);
    let requests = probe.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].workflow_id_conflict_policy,
        i32::from(WorkflowIdConflictPolicy::UseExisting)
    );
    assert_eq!(requests[0].request_id, "stable-request-1");
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

/// Builds a distinct start document so each admission occupies its own slot.
fn start_request(index: usize) -> Vec<u8> {
    format!(
        r#"{{"request_id":"capacity-request-{index}","namespace":"default","workflow_id":"workflow-{index}","workflow_type":"Workflow","task_queue":"queue","input":[]}}"#
    )
    .into_bytes()
}

/// A full start registry rejects only new logical starts, with the dedicated
/// retryable capacity status rather than the closed-client invalid-state
/// status. The client stays connected, an identical retry of an admitted
/// request still returns its original ticket, and freeing one slot admits the
/// previously rejected start.
#[test]
fn pending_start_capacity_rejects_only_new_requests() {
    let (mut runtime, _probe) = connected_runtime(Reply::Hung);
    let mut tickets = Vec::with_capacity(MAX_PENDING_STARTS);
    for index in 0..MAX_PENDING_STARTS {
        tickets.push(
            runtime
                .begin_start_workflow_json(&start_request(index))
                .expect("start admitted below capacity"),
        );
    }

    let excess = runtime
        .begin_start_workflow_json(&start_request(MAX_PENDING_STARTS))
        .expect_err("start beyond capacity must be rejected");
    assert_eq!(excess.status, STATUS_RESOURCE_EXHAUSTED);
    assert!(
        excess
            .message
            .contains(&format!("limit {MAX_PENDING_STARTS}")),
        "{}",
        excess.message
    );
    assert_eq!(runtime.pending_starts.len(), MAX_PENDING_STARTS);
    assert!(runtime.client.is_some());

    let retry = runtime
        .begin_start_workflow_json(&start_request(0))
        .expect("identical retry at capacity reuses its ticket");
    assert_eq!(retry, tickets[0]);

    // Retire one ticket as the owner would after a terminal result, joining
    // its aborted task so no transport future outlives the slot. The freed
    // slot must admit the start that was rejected above.
    let ticket: serde_json::Value = serde_json::from_slice(&tickets[0]).unwrap();
    let pending = runtime
        .pending_starts
        .remove(ticket["ticket"].as_str().expect("ticket field"))
        .expect("first start is pending");
    pending.task.abort();
    let _ = runtime
        .core
        .as_ref()
        .unwrap()
        .tokio_handle()
        .block_on(pending.task);
    runtime
        .begin_start_workflow_json(&start_request(MAX_PENDING_STARTS))
        .expect("start admitted after a slot is released");
    assert_eq!(runtime.pending_starts.len(), MAX_PENDING_STARTS);

    assert!(runtime.disconnect_client().is_ok());
    assert!(runtime.pending_starts.is_empty());
    assert_eq!(runtime.close(true), STATUS_OK);
}

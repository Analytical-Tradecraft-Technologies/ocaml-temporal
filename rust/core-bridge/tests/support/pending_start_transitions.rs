use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::task::Poll;

use temporalio_client::callback_based::CallbackBasedGrpcService;
use temporalio_client::tonic::Status;

/// Builds an isolated Core runtime for ticket-state tests.
fn runtime() -> Runtime {
    let options = RuntimeOptions::builder()
        .build()
        .expect("default Temporal Core runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default())
        .expect("default Temporal Core runtime");
    Runtime::with_owned_core(core).expect("runtime cleanup thread starts")
}

/// Adds an in-memory client connection without contacting Temporal Server.
///
/// `read_start_workflow_json` checks the lifecycle before it reads a ticket,
/// so the transition tests need a connected runtime even though the pending
/// entries themselves are synthetic.  The SDK client's callback transport
/// answers only the initial capability probe with the documented unimplemented
/// response; no network socket or external service is involved.
fn connected_runtime() -> Runtime {
    let mut runtime = runtime();
    let service = CallbackBasedGrpcService {
        callback: Arc::new(|request| {
            Box::pin(async move {
                assert_eq!(request.rpc, "GetSystemInfo");
                Err(Status::unimplemented(
                    "Method temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo is unimplemented",
                ))
            })
        }),
    };
    let options = ConnectionOptions::new(
        temporalio_sdk_core::Url::parse("http://localhost:7233")
            .expect("test connection URL is valid"),
    )
    .service_override(service)
    .dns_load_balancing(None)
    .build();
    let connection = runtime
        .core
        .as_ref()
        .expect("runtime owns Core while connecting")
        .tokio_handle()
        .block_on(Connection::connect(options))
        .expect("callback transport accepts the capability probe");
    runtime.client = Some(connection);
    runtime
}

/// Returns the stable request retained by every synthetic pending ticket.
fn request() -> Arc<client_protocol::StartWorkflowRequest> {
    Arc::new(client_protocol::StartWorkflowRequest {
        request_id: "request-1".to_owned(),
        namespace: "default".to_owned(),
        workflow_id: "workflow-1".to_owned(),
        workflow_type: "Workflow".to_owned(),
        task_queue: "queue".to_owned(),
        input: Vec::new(),
        memo: Vec::new(),
        search_attributes: Vec::new(),
        id_conflict_policy: client_protocol::IdConflictPolicy::Fail,
    })
}

/// Returns the accepted response used by terminal-transition assertions.
fn response() -> client_protocol::StartWorkflowResponse {
    client_protocol::StartWorkflowResponse {
        execution: client_protocol::ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
        },
        started: true,
    }
}

/// Encodes the private ticket document expected by the owner-side reader.
fn ticket_document(ticket: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "ticket": ticket }))
        .expect("ticket document is serializable")
}

/// Inserts one synthetic pending start with the same ownership shape as a
/// live asynchronous start: request, one-shot receiver, and Tokio task.
fn insert_pending(
    runtime: &mut Runtime,
    ticket: &str,
    receiver: Receiver<
        std::result::Result<
            client_protocol::StartWorkflowResponse,
            client_protocol::ClientOperationError,
        >,
    >,
    task: tokio::task::JoinHandle<()>,
) {
    runtime.pending_starts.insert(
        ticket.to_owned(),
        PendingStart {
            request: request(),
            receiver,
            task,
        },
    );
}

/// Future that remains pending until its owner-side abort path cancels and
/// joins the Tokio task.
struct AbortProbe(Arc<AtomicUsize>);

impl Future for AbortProbe {
    type Output = ();

    /// Keep the task alive so dropping a join handle alone would be observable.
    fn poll(self: Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for AbortProbe {
    /// Count the task-owned future being released after cancellation.
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

/// Future which publishes a terminal result and then remains alive until the
/// owner-side cancellation path aborts and joins it. Keeping the send and the
/// pending state in one future makes the completion/shutdown race deterministic
/// without relying on a wall-clock sleep.
struct ResultThenPendingProbe {
    sender: Option<
        Sender<
            std::result::Result<
                client_protocol::StartWorkflowResponse,
                client_protocol::ClientOperationError,
            >,
        >,
    >,
    published: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl Future for ResultThenPendingProbe {
    type Output = ();

    /// Send exactly one terminal result, then deliberately remain pending so
    /// shutdown must cancel and join the task rather than merely observe its
    /// channel message and detach its connection clone.
    fn poll(mut self: Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if let Some(sender) = self.sender.take() {
            sender
                .send(Ok(response()))
                .expect("synthetic receiver remains retained by the pending ticket");
            self.published.fetch_add(1, Ordering::Release);
        }
        Poll::Pending
    }
}

impl Drop for ResultThenPendingProbe {
    /// Count cancellation after publication so the test proves the Tokio task
    /// was joined even though its caller-side result was already available.
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Release);
    }
}

/// A non-blocking read reports `NOT_READY` without removing or consuming the
/// ticket, allowing the supervisor to retry the same operation later.
#[test]
fn not_ready_preserves_ticket() {
    let mut runtime = connected_runtime();
    let (_sender, receiver) = channel();
    let task = runtime
        .core
        .as_ref()
        .expect("runtime owns Core")
        .tokio_handle()
        .spawn(async {});
    insert_pending(&mut runtime, "ticket-not-ready", receiver, task);

    let failure = runtime
        .poll_start_workflow_json(&ticket_document("ticket-not-ready"))
        .expect_err("empty ticket channel reports not-ready");
    assert_eq!(failure.status, STATUS_NOT_READY);
    assert!(runtime.pending_starts.contains_key("ticket-not-ready"));
    assert!(matches!(
        runtime
            .pending_starts
            .get_mut("ticket-not-ready")
            .expect("not-ready ticket remains retained")
            .receiver
            .try_recv(),
        Err(TryRecvError::Empty)
    ));

    assert_eq!(runtime.close(true), STATUS_OK);
}

/// A terminal accepted result retires its ticket before returning, and a
/// second read of the same ticket is rejected as an unknown protocol value.
#[test]
fn terminal_result_retires_ticket_exactly_once() {
    let mut runtime = connected_runtime();
    let (sender, receiver) = channel();
    sender
        .send(Ok(response()))
        .expect("synthetic terminal result has a live receiver");
    let task = runtime
        .core
        .as_ref()
        .expect("runtime owns Core")
        .tokio_handle()
        .spawn(async {});
    insert_pending(&mut runtime, "ticket-accepted", receiver, task);

    let encoded = runtime
        .poll_start_workflow_json(&ticket_document("ticket-accepted"))
        .expect("accepted result is encoded");
    let document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("accepted outcome is valid JSON");
    assert_eq!(document["kind"], "accepted");
    assert!(!runtime.pending_starts.contains_key("ticket-accepted"));

    let repeated = runtime
        .poll_start_workflow_json(&ticket_document("ticket-accepted"))
        .expect_err("retired ticket cannot be read twice");
    assert_eq!(repeated.status, STATUS_PROTOCOL);
    assert!(
        repeated
            .message
            .contains("unknown Temporal workflow start ticket")
    );

    assert_eq!(runtime.close(true), STATUS_OK);
}

/// If the one-shot sender disappears, the owner emits an explicit `unknown`
/// outcome and retires the ticket so callers can reconcile with Temporal.
#[test]
fn dropped_sender_maps_to_unknown_and_retires_ticket() {
    let mut runtime = connected_runtime();
    let (sender, receiver) = channel();
    drop(sender);
    let task = runtime
        .core
        .as_ref()
        .expect("runtime owns Core")
        .tokio_handle()
        .spawn(async {});
    insert_pending(&mut runtime, "ticket-unknown", receiver, task);

    let encoded = runtime
        .poll_start_workflow_json(&ticket_document("ticket-unknown"))
        .expect("disconnected sender maps to unknown");
    let document: serde_json::Value =
        serde_json::from_slice(&encoded).expect("unknown outcome is valid JSON");
    assert_eq!(document["kind"], "unknown");
    assert_eq!(document["request_id"], "request-1");
    assert_eq!(document["workflow_id"], "workflow-1");
    assert!(!runtime.pending_starts.contains_key("ticket-unknown"));

    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Explicit shutdown aborts every pending task, joins all handles on the
/// owner side, and drains the registry before returning to its caller.
#[test]
fn explicit_abort_joins_and_drains_pending_tasks() {
    let mut runtime = runtime();
    let dropped = Arc::new(AtomicUsize::new(0));
    let (_sender_one, receiver_one) = channel();
    let (_sender_two, receiver_two) = channel();
    let handle = runtime
        .core
        .as_ref()
        .expect("runtime owns Core")
        .tokio_handle();
    let task_one = handle.spawn(AbortProbe(Arc::clone(&dropped)));
    let task_two = handle.spawn(AbortProbe(Arc::clone(&dropped)));
    insert_pending(&mut runtime, "ticket-abort-one", receiver_one, task_one);
    insert_pending(&mut runtime, "ticket-abort-two", receiver_two, task_two);

    let returned_handles = runtime.abort_pending_starts(true);
    assert!(returned_handles.is_empty());
    assert!(runtime.pending_starts.is_empty());
    assert_eq!(dropped.load(Ordering::Acquire), 2);

    assert_eq!(runtime.close(true), STATUS_OK);
}

/// A terminal response can race with shutdown after it has been published but
/// before its task exits. The owner must drain the ticket registry, cancel and
/// join that still-running task, and then release Core without double ownership
/// of either the response channel or the task handle.
#[test]
fn abort_joins_task_after_terminal_response_is_published() {
    let mut runtime = runtime();
    let published = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = channel();
    let handle = runtime
        .core
        .as_ref()
        .expect("runtime owns Core")
        .tokio_handle();
    let task = handle.spawn(ResultThenPendingProbe {
        sender: Some(sender),
        published: Arc::clone(&published),
        dropped: Arc::clone(&dropped),
    });
    insert_pending(&mut runtime, "ticket-published", receiver, task);

    while published.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }
    let returned_handles = runtime.abort_pending_starts(true);
    assert!(returned_handles.is_empty());
    assert!(runtime.pending_starts.is_empty());
    assert_eq!(dropped.load(Ordering::Acquire), 1);

    assert_eq!(runtime.close(true), STATUS_OK);
}

use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Future that never completes and records when Tokio drops it.  Keeping the
/// marker in the future itself means the test observes task destruction even
/// when cancellation wins before Tokio first polls the task.
struct PendingProbe(Arc<AtomicBool>);

impl Future for PendingProbe {
    type Output = ();

    /// Keep the task pending until the cleanup path aborts and joins it.
    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingProbe {
    /// Publish the exact point at which the task-owned future is released.
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Future that publishes a terminal ticket result and then remains pending.
/// Nonblocking disposal must still abort and join this task: observing the
/// result is not enough because its task may retain a Core connection clone.
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
    dropped: Arc<AtomicBool>,
}

impl Future for ResultThenPendingProbe {
    type Output = ();

    /// Publish exactly one terminal result, then stay pending until cleanup
    /// aborts the Tokio task and joins its handle on the cleanup thread.
    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(sender) = self.sender.take() {
            sender
                .send(Ok(client_temporal_response()))
                .expect("synthetic pending ticket retains its receiver");
            self.published.fetch_add(1, Ordering::Release);
        }
        Poll::Pending
    }
}

impl Drop for ResultThenPendingProbe {
    /// Record task destruction so the test can distinguish joining from merely
    /// dropping a `JoinHandle`, which would detach the task.
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

/// Builds the accepted response used by the nonblocking-disposal race probe.
fn client_temporal_response() -> client_protocol::StartWorkflowResponse {
    client_protocol::StartWorkflowResponse {
        execution: client_protocol::ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "workflow".to_owned(),
            run_id: "run".to_owned(),
        },
        started: true,
    }
}

/// Proves GC-style non-blocking disposal transfers aborted start handles to
/// the cleanup thread and waits there before dropping Core.  If the handles
/// were merely dropped by [`Runtime::close`], the Tokio task would be detached
/// and this marker could remain false after the runtime graph was released.
#[test]
fn nonblocking_close_joins_aborted_start_tasks_before_core_drop() {
    let options = RuntimeOptions::builder()
        .build()
        .expect("default Temporal Core runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default())
        .expect("default Temporal Core runtime");
    let mut runtime = Runtime::new(core, None).expect("runtime cleanup thread starts");

    let dropped = Arc::new(AtomicBool::new(false));
    let task = runtime
        .core
        .as_ref()
        .expect("runtime owns Core before close")
        .tokio_handle()
        .spawn(PendingProbe(Arc::clone(&dropped)));
    let (_sender, receiver) = channel();
    runtime.pending_starts.insert(
        "ticket".to_owned(),
        PendingStart {
            request: Arc::new(client_protocol::StartWorkflowRequest {
                request_id: "request".to_owned(),
                namespace: "default".to_owned(),
                workflow_id: "workflow".to_owned(),
                workflow_type: "workflow".to_owned(),
                task_queue: "queue".to_owned(),
                input: Vec::new(),
                memo: Vec::new(),
                search_attributes: Vec::new(),
                id_conflict_policy: client_protocol::IdConflictPolicy::Fail,
            }),
            receiver,
            task,
        },
    );

    assert_eq!(runtime.close(false), STATUS_OK);

    let deadline = Instant::now() + Duration::from_secs(5);
    while !dropped.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "cleanup thread did not join the aborted start task"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Proves GC-style disposal joins a pending start even after its task has
/// already published a terminal response. This is a distinct lifecycle race
/// from an entirely pending task: the response channel can look complete while
/// the Tokio task still owns a cloned connection and must be cancelled before
/// Core is released.
#[test]
fn nonblocking_close_joins_after_terminal_result_publication() {
    let options = RuntimeOptions::builder()
        .build()
        .expect("default Temporal Core runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default())
        .expect("default Temporal Core runtime");
    let mut runtime = Runtime::new(core, None).expect("runtime cleanup thread starts");

    let published = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = channel();
    let task = runtime
        .core
        .as_ref()
        .expect("runtime owns Core before close")
        .tokio_handle()
        .spawn(ResultThenPendingProbe {
            sender: Some(sender),
            published: Arc::clone(&published),
            dropped: Arc::clone(&dropped),
        });
    runtime.pending_starts.insert(
        "published-ticket".to_owned(),
        PendingStart {
            request: Arc::new(client_protocol::StartWorkflowRequest {
                request_id: "request".to_owned(),
                namespace: "default".to_owned(),
                workflow_id: "workflow".to_owned(),
                workflow_type: "workflow".to_owned(),
                task_queue: "queue".to_owned(),
                input: Vec::new(),
                memo: Vec::new(),
                search_attributes: Vec::new(),
                id_conflict_policy: client_protocol::IdConflictPolicy::Fail,
            }),
            receiver,
            task,
        },
    );

    let publication_deadline = Instant::now() + Duration::from_secs(5);
    while published.load(Ordering::Acquire) == 0 {
        assert!(
            Instant::now() < publication_deadline,
            "synthetic pending task did not publish its terminal result"
        );
        std::thread::yield_now();
    }
    assert_eq!(runtime.close(false), STATUS_OK);

    let deadline = Instant::now() + Duration::from_secs(5);
    while !dropped.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "cleanup thread did not join the task after publishing its result"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

// Keep transition coverage in its own support file while reusing this module's
// existing private `abi.rs` test context.  The production module declaration
// already loads this support file, so no runtime code is changed just to make
// the additional unit tests visible.
#[path = "pending_start_transitions.rs"]
mod pending_start_transitions;

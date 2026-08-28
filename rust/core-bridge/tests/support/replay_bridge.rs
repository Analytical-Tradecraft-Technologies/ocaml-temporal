//! Tests for the private replay bridge.
//!
//! This file is included as a child module by `src/replay_bridge.rs`. Keeping
//! the test implementation here makes the production module easier to audit
//! while retaining access to its private validation and lifecycle helpers.

use super::*;
use prost_wkt_types::{Duration as PbDuration, Timestamp};
use std::time::Duration;
use temporalio_common::protos::coresdk::workflow_completion::WorkflowActivationCompletion;
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowType,
    enums::v1::EventType,
    history::v1::{
        HistoryEvent, WorkflowExecutionCompletedEventAttributes,
        WorkflowExecutionStartedEventAttributes, WorkflowTaskCompletedEventAttributes,
        WorkflowTaskScheduledEventAttributes, WorkflowTaskStartedEventAttributes, history_event,
    },
    taskqueue::v1::TaskQueue,
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk_core::{
    PollerBehavior, RuntimeOptions, TokioRuntimeBuilder, WorkerVersioningStrategy,
};

/// Builds a small monotonic protobuf timestamp for a history event.
///
/// Core's workflow machines reject a `WorkflowTaskStarted` event that has no
/// `event_time`, so every event carries one. A missing timestamp is not caught
/// by Core's structural replay-invariant validator; it only surfaces as a fatal
/// machines error while the task is applied, which in turn makes Core emit an
/// autonomous non-empty auto-fail completion. That completion can race replay
/// disposal's stream teardown and trip Core's "A non-empty completion was not
/// processed" panic, so the fixture must supply valid timestamps.
fn event_time(seconds: i64) -> prost_wkt_types::Timestamp {
    prost_wkt_types::Timestamp { seconds, nanos: 0 }
}

/// Builds a minimal complete history accepted by Core's replay invariant
/// validator and by its workflow machines without importing Core's test-only
/// history-builder feature.
fn complete_history() -> History {
    let started = HistoryEvent {
        event_id: 1,
        event_type: EventType::WorkflowExecutionStarted as i32,
        event_time: Some(event_time(1)),
        attributes: Some(
            history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                WorkflowExecutionStartedEventAttributes {
                    workflow_type: Some(WorkflowType {
                        name: "replay-test".to_owned(),
                    }),
                    task_queue: Some(TaskQueue {
                        name: "replay-test".to_owned(),
                        ..Default::default()
                    }),
                    original_execution_run_id: "run-replay-test".to_owned(),
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    let scheduled = HistoryEvent {
        event_id: 2,
        event_type: EventType::WorkflowTaskScheduled as i32,
        event_time: Some(event_time(2)),
        attributes: Some(
            history_event::Attributes::WorkflowTaskScheduledEventAttributes(
                WorkflowTaskScheduledEventAttributes::default(),
            ),
        ),
        ..Default::default()
    };
    let task_started = HistoryEvent {
        event_id: 3,
        event_type: EventType::WorkflowTaskStarted as i32,
        event_time: Some(event_time(3)),
        attributes: Some(
            history_event::Attributes::WorkflowTaskStartedEventAttributes(
                WorkflowTaskStartedEventAttributes {
                    scheduled_event_id: 2,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    let task_completed = HistoryEvent {
        event_id: 4,
        event_type: EventType::WorkflowTaskCompleted as i32,
        event_time: Some(event_time(4)),
        attributes: Some(
            history_event::Attributes::WorkflowTaskCompletedEventAttributes(
                WorkflowTaskCompletedEventAttributes {
                    scheduled_event_id: 2,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    let completed = HistoryEvent {
        event_id: 5,
        event_type: EventType::WorkflowExecutionCompleted as i32,
        event_time: Some(event_time(5)),
        attributes: Some(
            history_event::Attributes::WorkflowExecutionCompletedEventAttributes(
                WorkflowExecutionCompletedEventAttributes {
                    workflow_task_completed_event_id: 4,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    History {
        events: vec![started, scheduled, task_started, task_completed, completed],
    }
}

/// Builds a history whose first workflow task is still open (started but not
/// completed), with the timestamps and timeouts Core needs to construct a
/// live, failable workflow task rather than an immediate eviction.
///
/// A replay activation from this history represents an active Core workflow
/// task, so rejecting it exercises the failure-completion path that makes Core
/// schedule a follow-up cache eviction for the same run_id — the exact
/// sequence the ordering regression depends on. The extra fields mirror the
/// integration fixture in `tests/support/replay_fixture.rs`; without the event
/// timestamps Core turns the incomplete fixture into an eviction instead.
fn open_workflow_task_history() -> History {
    fn event_time(event_id: i64) -> Option<Timestamp> {
        Some(Timestamp {
            seconds: event_id,
            nanos: 0,
        })
    }
    fn task_timeout() -> Option<PbDuration> {
        Some(PbDuration {
            seconds: 10,
            nanos: 0,
        })
    }
    let started = HistoryEvent {
        event_id: 1,
        event_time: event_time(1),
        event_type: EventType::WorkflowExecutionStarted as i32,
        attributes: Some(
            history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                WorkflowExecutionStartedEventAttributes {
                    workflow_type: Some(WorkflowType {
                        name: "replay-test".to_owned(),
                    }),
                    task_queue: Some(TaskQueue {
                        name: "replay-test".to_owned(),
                        ..Default::default()
                    }),
                    workflow_task_timeout: task_timeout(),
                    original_execution_run_id: "run-replay-test".to_owned(),
                    first_execution_run_id: "run-replay-test".to_owned(),
                    attempt: 1,
                    first_workflow_task_backoff: Some(PbDuration::default()),
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    let scheduled = HistoryEvent {
        event_id: 2,
        event_time: event_time(2),
        event_type: EventType::WorkflowTaskScheduled as i32,
        attributes: Some(
            history_event::Attributes::WorkflowTaskScheduledEventAttributes(
                WorkflowTaskScheduledEventAttributes {
                    task_queue: Some(TaskQueue {
                        name: "replay-test".to_owned(),
                        ..Default::default()
                    }),
                    start_to_close_timeout: task_timeout(),
                    attempt: 1,
                },
            ),
        ),
        ..Default::default()
    };
    let task_started = HistoryEvent {
        event_id: 3,
        event_time: event_time(3),
        event_type: EventType::WorkflowTaskStarted as i32,
        attributes: Some(
            history_event::Attributes::WorkflowTaskStartedEventAttributes(
                WorkflowTaskStartedEventAttributes {
                    identity: "replay-test-worker".to_owned(),
                    scheduled_event_id: 2,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    History {
        events: vec![started, scheduled, task_started],
    }
}

/// Builds the smallest valid worker configuration before replay overrides
/// it to workflow-only mode.
fn replay_config() -> WorkerConfig {
    WorkerConfig::builder()
        .namespace("replay-test")
        .task_queue("replay-test")
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "replay-test".to_owned(),
        })
        .task_types(WorkerTaskTypes::workflow_only())
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(1))
        .max_outstanding_workflow_tasks(1usize)
        .ignore_evicts_on_shutdown(true)
        .build()
        .expect("replay test worker configuration should be valid")
}

/// Creates the local Core runtime used by the construction test.
fn core_runtime() -> CoreRuntime {
    let options = RuntimeOptions::builder()
        .build()
        .expect("replay test runtime options should be valid");
    CoreRuntime::new(options, TokioRuntimeBuilder::default())
        .expect("replay test Core runtime should start")
}

/// A valid history can be encoded and decoded without changing its wire
/// identity or bypassing duplicate-aware JSON validation.
#[test]
fn history_document_round_trips() {
    let history = complete_history();
    let encoded = encode_history_document("workflow-replay-test", &history)
        .expect("complete history should encode");
    assert!(encoded.contains("\"encoding\":\"base64\""));
    decode_history_document(&encoded).expect("encoded history should decode");
}

/// Duplicate and unknown fields are rejected before protobuf decoding.
#[test]
fn history_document_is_closed_and_duplicate_aware() {
    let duplicate =
        r#"{"workflow_id":"run","workflow_id":"other","history":{"encoding":"base64","data":""}}"#;
    assert!(matches!(
        decode_history_document(duplicate),
        Err(ReplayInputError::InvalidDocument)
    ));
    let unknown = r#"{"workflow_id":"run","history":{"encoding":"base64","data":"","extra":true}}"#;
    assert!(matches!(
        decode_history_document(unknown),
        Err(ReplayInputError::InvalidDocument)
    ));
}

/// A non-canonical payload and a structurally invalid history never reach
/// Core's replay constructor.
#[test]
fn history_document_rejects_encoding_and_history_errors() {
    let wrong_encoding = r#"{"workflow_id":"run","history":{"encoding":"hex","data":"00"}}"#;
    assert!(matches!(
        decode_history_document(wrong_encoding),
        Err(ReplayInputError::InvalidEncoding)
    ));
    let invalid_history = r#"{"workflow_id":"run","history":{"encoding":"base64","data":"AA=="}}"#;
    assert!(matches!(
        decode_history_document(invalid_history),
        Err(ReplayInputError::InvalidProtobuf)
    ));
}

/// Core can construct and shut down a workflow-only replay worker with no
/// network client, proving feeder and lane ownership are self-contained.
#[test]
fn replay_worker_starts_and_finalizes_without_client() {
    let core = core_runtime();
    let handle = core.tokio_handle().clone();
    let mut worker = ReplayWorker::start(&core, replay_config())
        .expect("replay worker should construct without a client");
    worker.finish_input();
    wait_until_workflow_shutdown(&mut worker);
    finalize_or_panic(worker, &handle);
    // Keep the duration import meaningful in this test module: Core's
    // bounded readiness contract must remain a finite wait if the worker
    // implementation changes its shutdown ordering.
    assert!(crate::worker_bridge::READINESS_WAIT_TIMEOUT <= Duration::from_secs(1));
}

/// Waits for Core's replay worker to close its workflow lane after all input
/// has been consumed. A finite retry bound keeps the test deterministic while
/// allowing the poll task to cross the Tokio-to-owner handoff.
fn wait_until_workflow_shutdown(worker: &mut ReplayWorker) {
    for _ in 0..20 {
        match worker.wait_workflow() {
            crate::worker_bridge::ReadinessWait::Shutdown => return,
            crate::worker_bridge::ReadinessWait::TimedOut => {}
            crate::worker_bridge::ReadinessWait::Ready => {
                panic!("replay worker produced an activation without supplied history")
            }
            crate::worker_bridge::ReadinessWait::Error(error) => {
                panic!("replay worker failed while shutting down: {error:?}")
            }
        }
    }
    panic!("replay worker did not reach shutdown within the bounded test wait");
}

/// Finalizes a test worker while retaining ownership long enough to dispose it
/// if the assertion unexpectedly fails. This avoids requiring `ReplayWorker`
/// (and its native lane graph) to implement `Debug` merely for `expect`.
fn finalize_or_panic(worker: ReplayWorker, handle: &tokio::runtime::Handle) {
    match handle.block_on(worker.finalize(handle)) {
        Ok(()) => {}
        Err((returned, error)) => {
            dispose_or_panic(returned, handle);
            panic!("replay worker should finalize cleanly: {error:?}");
        }
    }
}

/// Disposes a test worker and turns an unexpected retained-owner error into a
/// test failure without requiring native handles to implement `Debug`.
fn dispose_or_panic(worker: ReplayWorker, handle: &tokio::runtime::Handle) {
    match handle.block_on(worker.dispose(handle)) {
        Ok(()) => {}
        Err((returned, error)) => match handle.block_on(returned.dispose(handle)) {
            Ok(()) => panic!("replay worker disposal required a retry: {error:?}"),
            Err((_retained, retry_error)) => {
                panic!("replay worker disposal failed twice: {error:?}; {retry_error:?}")
            }
        },
    }
}

/// A valid history is admitted to the one-slot feeder before shutdown,
/// exercising the same path used by a future OCaml replay driver.
#[test]
fn replay_worker_accepts_one_history_document() {
    let core = core_runtime();
    let handle = core.tokio_handle().clone();
    let mut worker =
        ReplayWorker::start(&core, replay_config()).expect("replay worker should construct");
    let document = encode_history_document("workflow-replay-test", &complete_history())
        .expect("history should encode");
    worker
        .feed_json(&handle, &document)
        .expect("bounded feeder should accept one history");
    // The poll lane publishes from a separate Tokio task. Each wait is capped
    // at the bridge's 100 ms supervisor bound; retrying a finite number of
    // times proves eventual publication without making the test depend on a
    // scheduler-specific handoff delay.
    let mut activation_ready = false;
    for _ in 0..20 {
        match worker.wait_workflow() {
            crate::worker_bridge::ReadinessWait::Ready => {
                activation_ready = true;
                break;
            }
            crate::worker_bridge::ReadinessWait::TimedOut => {}
            crate::worker_bridge::ReadinessWait::Shutdown => {
                panic!("replay workflow lane shut down before its activation")
            }
            crate::worker_bridge::ReadinessWait::Error(error) => {
                panic!("replay workflow lane failed before its activation: {error:?}")
            }
        }
    }
    assert!(
        activation_ready,
        "Core did not publish a replay activation within the bounded test wait"
    );
    let activation = worker
        .try_take_workflow(&handle)
        .expect("replay activation should be queued")
        .expect("replay activation should satisfy bridge admission");
    assert_eq!(activation.run_id, "run-replay-test");
    // A genuinely valid history replays into a real workflow-initialization
    // activation, not a fatal-machines-error eviction. See `event_time` for why
    // the distinction matters to replay disposal safety.
    assert!(
        activation_is_initialize_workflow(&activation),
        "first replay activation should initialize the workflow, got {:?}",
        activation.jobs
    );
    handle
        .block_on(worker.complete_workflow(WorkflowActivationCompletion::empty(&activation.run_id)))
        .expect("replay activation should accept an empty deterministic completion");
    worker.finish_input();
    // Because no workflow code runs in this bridge-only test, the empty
    // completion is a replay nondeterminism: Core responds with a cache
    // eviction that must be acknowledged with an empty completion before the
    // lane can reach its terminal shutdown.
    drain_replay_evictions_until_shutdown(&mut worker, &handle);
    finalize_or_panic(worker, &handle);
}

/// Reports whether an activation's jobs initialize a workflow rather than only
/// evicting it. The pre-fix history fixture omitted the `WorkflowTaskStarted`
/// timestamp, so Core delivered a fatal-error eviction here instead; this guard
/// keeps that regression from returning silently.
fn activation_is_initialize_workflow(activation: &WorkflowActivation) -> bool {
    activation
        .jobs
        .iter()
        .any(|job| format!("{:?}", job.variant).contains("InitializeWorkflow"))
}

/// Drains replay cache evictions with empty completions until the workflow lane
/// reports its terminal shutdown. Each eviction is a legitimate replay-only
/// activation, so it is acknowledged rather than treated as unexpected work.
fn drain_replay_evictions_until_shutdown(
    worker: &mut ReplayWorker,
    handle: &tokio::runtime::Handle,
) {
    for _ in 0..40 {
        match worker.wait_workflow() {
            crate::worker_bridge::ReadinessWait::Shutdown => return,
            crate::worker_bridge::ReadinessWait::TimedOut => {}
            crate::worker_bridge::ReadinessWait::Ready => {
                if let Some(Ok(activation)) = worker.try_take_workflow(handle) {
                    handle
                        .block_on(
                            worker.complete_workflow(WorkflowActivationCompletion::empty(
                                &activation.run_id,
                            )),
                        )
                        .expect("replay eviction should accept an empty completion");
                }
            }
            crate::worker_bridge::ReadinessWait::Error(error) => {
                panic!("replay workflow lane failed while draining evictions: {error:?}")
            }
        }
    }
    panic!("replay worker did not reach shutdown within the bounded eviction drain");
}

/// A feeder close alone does not prove that Core processed the queued history.
/// Finalization therefore returns a typed precondition error and retains the
/// worker so the owner can continue draining or explicitly dispose it.
#[test]
fn replay_worker_rejects_finalize_before_history_is_drained() {
    let core = core_runtime();
    let handle = core.tokio_handle().clone();
    let mut worker =
        ReplayWorker::start(&core, replay_config()).expect("replay worker should construct");
    let document = encode_history_document("workflow-replay-test", &complete_history())
        .expect("history should encode");
    worker
        .feed_json(&handle, &document)
        .expect("bounded feeder should accept one history");
    worker.finish_input();

    let (worker, error) = match handle.block_on(worker.finalize(&handle)) {
        Ok(()) => panic!("finalization must not claim a queued history was replayed"),
        Err(result) => result,
    };
    assert!(matches!(
        error,
        ReplayWorkerError::ReplayNotDrained {
            input_finished: true,
            workflow_shutdown_observed: false,
            ..
        }
    ));
    // The retained worker remains usable for the explicit abandonment path;
    // replay disposal acknowledges the queued activation with an empty Core
    // completion, then joins and finalizes all native resources.
    dispose_or_panic(worker, &handle);
}

/// The replay history fixture must be valid for Core's workflow machines, not
/// only for its structural replay-invariant validator.
///
/// This is a deterministic regression guard for the intermittent OCaml 5.2 CI
/// failure. When the fixture omitted the `WorkflowTaskStarted` timestamp, Core
/// accepted the document but hit a fatal machines error while applying the
/// task. That fatal error made Core emit an autonomous, non-empty auto-fail
/// completion through its internal poll loop. During replay disposal, Core's
/// workflow stream could terminate before that in-flight completion was
/// processed, tripping Core's "A non-empty completion was not processed"
/// panic. A genuinely valid history never produces that autonomous completion,
/// so the panic cannot occur. The first activation is therefore asserted to be
/// a real workflow initialization rather than a fatal-machines-error eviction,
/// which fails deterministically if the invalid history is reintroduced.
#[test]
fn replay_history_first_activation_initializes_workflow() {
    let core = core_runtime();
    let handle = core.tokio_handle().clone();
    let mut worker =
        ReplayWorker::start(&core, replay_config()).expect("replay worker should construct");
    let document = encode_history_document("workflow-replay-test", &complete_history())
        .expect("history should encode");
    worker
        .feed_json(&handle, &document)
        .expect("bounded feeder should accept one history");
    worker.finish_input();

    let mut activation = None;
    for _ in 0..20 {
        match worker.wait_workflow() {
            crate::worker_bridge::ReadinessWait::Ready => {
                activation = worker
                    .try_take_workflow(&handle)
                    .and_then(|taken| taken.ok());
                break;
            }
            crate::worker_bridge::ReadinessWait::TimedOut => {}
            crate::worker_bridge::ReadinessWait::Shutdown => {
                panic!("replay lane shut down before delivering its first activation")
            }
            crate::worker_bridge::ReadinessWait::Error(error) => {
                panic!("replay lane failed before its first activation: {error:?}")
            }
        }
    }
    let activation = activation.expect("replay worker should deliver a first activation");
    assert_eq!(activation.run_id, "run-replay-test");
    assert!(
        activation_is_initialize_workflow(&activation),
        "a valid replay history must initialize the workflow; a fatal machines \
         error would instead deliver an eviction and spawn an autonomous \
         non-empty completion that races disposal shutdown. Jobs: {:?}",
        activation.jobs
    );
    // This guard has finished once it has inspected the first activation. Do
    // not submit an empty completion against a history whose workflow already
    // completed: that is intentionally nondeterministic and asks Core to
    // synthesize and drain an eviction, behavior covered by the dedicated
    // replay-completion tests below. Explicit disposal is the bounded cleanup
    // path for abandoning this leased activation and avoids making a fixture
    // validation test depend on Core's asynchronous nondeterminism shutdown.
    dispose_or_panic(worker, &handle);
}

/// Disposal reports a terminal Core finalization failure while retaining the
/// lane graph, allowing the caller to release the competing owner and retry.
#[test]
fn replay_dispose_retains_worker_when_core_is_still_shared() {
    let core = core_runtime();
    let handle = core.tokio_handle().clone();
    let worker = ReplayWorker::start(&core, replay_config())
        .expect("replay worker should construct without a client");
    // Holding a second Arc is a deterministic stand-in for an in-flight Core
    // operation that still owns the worker when terminal finalization runs.
    let keepalive = worker.retain_worker_for_test();
    let (worker, error) = match handle.block_on(worker.dispose(&handle)) {
        Ok(()) => panic!("disposal must report a still-shared Core worker"),
        Err(result) => result,
    };
    assert!(matches!(
        error,
        ReplayWorkerError::Finalization(WorkerBridgeError::WorkerStillShared)
    ));
    // The failed result still owns all lane state. Once the competing owner is
    // released, retrying the same public disposal operation can finalize it.
    drop(keepalive);
    dispose_or_panic(worker, &handle);
}

/// Disposal reports a poll-task join failure while retaining the worker for a
/// retry after every producer handle has been consumed.
#[test]
fn replay_dispose_retains_worker_when_poll_lane_join_fails() {
    let core = core_runtime();
    let handle = core.tokio_handle().clone();
    let mut worker = ReplayWorker::start(&core, replay_config())
        .expect("replay worker should construct without a client");
    handle.block_on(worker.abort_workflow_lane_for_test());
    let (worker, error) = match handle.block_on(worker.dispose(&handle)) {
        Ok(()) => panic!("disposal must report the aborted poll lane"),
        Err(result) => result,
    };
    assert!(matches!(
        error,
        ReplayWorkerError::PollLane(PollLaneError::Core(_))
    ));
    // The replay-aware join consumed the aborted handle. A retry therefore
    // has no detached producer left to race with finalization and can release
    // Core.
    dispose_or_panic(worker, &handle);
}

/// Waits for the workflow lane to publish one activation and leases it for the
/// owner. The bounded retry keeps the test deterministic across the
/// Tokio-to-owner handoff without depending on a scheduler-specific delay.
fn wait_and_lease_activation(
    worker: &mut ReplayWorker,
    handle: &tokio::runtime::Handle,
) -> WorkflowActivation {
    for _ in 0..20 {
        match worker.wait_workflow() {
            crate::worker_bridge::ReadinessWait::Ready => {
                return worker
                    .try_take_workflow(handle)
                    .expect("replay activation should be queued")
                    .expect("replay activation should satisfy bridge admission");
            }
            crate::worker_bridge::ReadinessWait::TimedOut => {}
            crate::worker_bridge::ReadinessWait::Shutdown => {
                panic!("replay workflow lane shut down before its activation")
            }
            crate::worker_bridge::ReadinessWait::Error(error) => {
                panic!("replay workflow lane failed before its activation: {error:?}")
            }
        }
    }
    panic!("Core did not publish a replay activation within the bounded test wait");
}

/// Deterministic regression guard for the replay reject/eviction retirement
/// race that produced an intermittent `STATUS_WORKER` on CI.
///
/// Rejecting a leased replay activation fails its Core workflow task, and Core
/// responds by scheduling a follow-up cache-eviction activation for the same
/// run_id on the background poll lane. If the bridge retires the rejected lease
/// only *after* awaiting that completion, the poll lane can observe the
/// eviction while the run is still recorded and misclassify it as
/// `Admission::Duplicate`, raising a terminal `PollLaneError::DuplicateIdentity`.
///
/// Rather than racing the poll lane — which is timing sensitive and therefore
/// flaky to observe — this test pins the exact ordering invariant the fix
/// establishes: the reject path records, through the shared ledger, whether the
/// run was still an outstanding obligation at the instant its failure was
/// handed to Core. A correct implementation always retires first, so the probe
/// is `false`. The pre-fix ordering retired after the completion and would
/// probe `true`, failing this assertion on every run regardless of scheduling.
#[test]
fn replay_reject_retires_lease_before_core_completion() {
    let core = core_runtime();
    let handle = core.tokio_handle().clone();
    let mut worker =
        ReplayWorker::start(&core, replay_config()).expect("replay worker should construct");
    let document = encode_history_document("workflow-replay-test", &open_workflow_task_history())
        .expect("open-task history should encode");
    worker
        .feed_json(&handle, &document)
        .expect("bounded feeder should accept one history");

    let activation = wait_and_lease_activation(&mut worker, &handle);
    assert_eq!(activation.run_id, "run-replay-test");

    handle
        .block_on(worker.reject_workflow_delivery(&activation.run_id, "test replay rejection"))
        .expect("rejecting a leased replay activation should succeed");

    assert_eq!(
        worker.reject_completion_probes(),
        vec![false],
        "rejection must retire the ledger lease before handing the failure to Core, \
         so the follow-up eviction is never admitted as a duplicate"
    );

    // The rejection left a follow-up eviction in flight; disposal abandons it
    // and releases every native resource so the test cannot leak a worker.
    dispose_or_panic(worker, &handle);
}

/// Deterministic regression guard for the same eviction-retirement race as
/// [`replay_reject_retires_lease_before_core_completion`], but for a
/// successful terminal completion rather than a rejection.
///
/// A successful terminal workflow completion causes Core to schedule a
/// follow-up cache-eviction activation for the same run_id, exactly like a
/// rejection does. If the bridge retires the completed lease only *after*
/// awaiting that completion, the poll lane can observe the eviction while the
/// run is still recorded and misclassify it as `Admission::Duplicate`,
/// raising a terminal `PollLaneError::DuplicateIdentity` on an otherwise
/// healthy worker — silently dropping the eviction's own completion debt in
/// the process. As with the reject regression, this pins the exact ordering
/// invariant the fix establishes rather than racing the poll lane: a correct
/// implementation always retires the lease before handing the completion to
/// Core, so the probe is `false`.
#[test]
fn replay_complete_retires_lease_before_core_completion() {
    let core = core_runtime();
    let handle = core.tokio_handle().clone();
    let mut worker =
        ReplayWorker::start(&core, replay_config()).expect("replay worker should construct");
    let document = encode_history_document("workflow-replay-test", &open_workflow_task_history())
        .expect("open-task history should encode");
    worker
        .feed_json(&handle, &document)
        .expect("bounded feeder should accept one history");

    let activation = wait_and_lease_activation(&mut worker, &handle);
    assert_eq!(activation.run_id, "run-replay-test");

    handle
        .block_on(worker.complete_workflow(WorkflowActivationCompletion::empty(&activation.run_id)))
        .expect("completing a leased replay activation should succeed");

    assert_eq!(
        worker.complete_completion_probes(),
        vec![false],
        "completion must retire the ledger lease before handing it to Core, \
         so the follow-up eviction is never admitted as a duplicate"
    );

    // The empty completion is a replay nondeterminism against this open-task
    // history, leaving a follow-up eviction in flight; disposal abandons it
    // and releases every native resource so the test cannot leak a worker.
    dispose_or_panic(worker, &handle);
}

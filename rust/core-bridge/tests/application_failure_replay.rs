//! Regress final activity failures through pinned Core and the production bridge.
//!
//! An unrelated control history is queued on the same replay worker as the two
//! non-default application failures. The test proves Core emits real terminal
//! activity resolutions and the bridge retains their nested failure metadata.
//! Public Worker.run lease retirement is outside this replay test's scope.

use std::collections::{HashMap, HashSet};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ocaml_temporal_core_bridge::workflow_protocol::{self, ActivationJob, ActivityResolution};
use prost::Message;
use prost_wkt_types::Timestamp;
use temporalio_common::{
    protos::{
        coresdk::{
            activity_result::activity_resolution::Status as CoreActivityStatus,
            workflow_activation::workflow_activation_job::Variant,
            workflow_commands::{CompleteWorkflowExecution, ScheduleActivity},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            enums::v1::{ApplicationErrorCategory, EventType, RetryState},
            failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
            history::v1::{history_event::Attributes, *},
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, RuntimeOptions, TokioRuntimeBuilder, WorkerConfig,
    WorkerVersioningStrategy,
    replay::{HistoryFeeder, HistoryForReplay, ReplayWorkerInput},
};

#[allow(dead_code)]
#[path = "support/replay_fixture.rs"]
mod fixture;

/// One variation of the terminal application failure carried by Core history.
#[derive(Clone, Copy)]
struct FailureCase {
    workflow_id: &'static str,
    category: ApplicationErrorCategory,
    retry_delay: bool,
}

/// The last history checks that another queued workflow can make progress.
const CASES: [FailureCase; 3] = [
    FailureCase {
        workflow_id: "replay-benign",
        category: ApplicationErrorCategory::Benign,
        retry_delay: false,
    },
    FailureCase {
        workflow_id: "replay-delay",
        category: ApplicationErrorCategory::Unspecified,
        retry_delay: true,
    },
    FailureCase {
        workflow_id: "replay-unrelated-control",
        category: ApplicationErrorCategory::Unspecified,
        retry_delay: false,
    },
];

/// Reuse the repository's initialized workflow history fixture.
fn base_history(workflow_id: &str) -> History {
    let value: serde_json::Value =
        serde_json::from_str(&fixture::complete_history_document(workflow_id)).unwrap();
    let mut history = History::decode(
        STANDARD
            .decode(value["history"]["data"].as_str().unwrap())
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    // The shared fixture has one fixed run ID. Every queued replay history
    // needs a distinct one because Core uses it as the execution identity.
    let Some(Attributes::WorkflowExecutionStartedEventAttributes(started)) =
        history.events[0].attributes.as_mut()
    else {
        panic!("fixture must start with WorkflowExecutionStarted");
    };
    let run_id = format!("run-{workflow_id}");
    started.original_execution_run_id = run_id.clone();
    started.first_execution_run_id = run_id;
    history
}

/// Append a history event with the monotonically increasing timestamp Core expects.
fn push(history: &mut History, kind: EventType, attributes: impl Into<Attributes>) {
    let id = history.events.len() as i64 + 1;
    history.events.push(HistoryEvent {
        event_id: id,
        event_time: Some(Timestamp {
            seconds: id,
            nanos: 0,
        }),
        event_type: kind as i32,
        attributes: Some(attributes.into()),
        ..Default::default()
    });
}

/// Append a completed workflow task and return its completion event ID.
fn task(history: &mut History) -> i64 {
    let scheduled = history.events.len() as i64 + 1;
    push(
        history,
        EventType::WorkflowTaskScheduled,
        WorkflowTaskScheduledEventAttributes::default(),
    );
    push(
        history,
        EventType::WorkflowTaskStarted,
        WorkflowTaskStartedEventAttributes {
            scheduled_event_id: scheduled,
            ..Default::default()
        },
    );
    push(
        history,
        EventType::WorkflowTaskCompleted,
        WorkflowTaskCompletedEventAttributes {
            scheduled_event_id: scheduled,
            ..Default::default()
        },
    );
    history.events.len() as i64
}

/// Build a final-retry activity failure supplied by a foreign SDK worker.
fn activity_history(case: FailureCase) -> History {
    let mut history = base_history(case.workflow_id);
    history.events.truncate(4);
    push(
        &mut history,
        EventType::ActivityTaskScheduled,
        ActivityTaskScheduledEventAttributes {
            activity_id: "ocaml-activity-1".into(),
            activity_type: Some(
                temporalio_common::protos::temporal::api::common::v1::ActivityType {
                    name: "foreign-activity".into(),
                },
            ),
            workflow_task_completed_event_id: 4,
            ..Default::default()
        },
    );
    push(
        &mut history,
        EventType::ActivityTaskStarted,
        ActivityTaskStartedEventAttributes {
            scheduled_event_id: 5,
            identity: "foreign-sdk".into(),
            attempt: 1,
            ..Default::default()
        },
    );
    push(
        &mut history,
        EventType::ActivityTaskFailed,
        ActivityTaskFailedEventAttributes {
            scheduled_event_id: 5,
            started_event_id: 6,
            identity: "foreign-sdk".into(),
            retry_state: RetryState::MaximumAttemptsReached as i32,
            failure: Some(Failure {
                message: "expected business failure".into(),
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        r#type: "BusinessError".into(),
                        category: case.category as i32,
                        next_retry_delay: case.retry_delay.then_some(prost_wkt_types::Duration {
                            seconds: 3,
                            nanos: 7,
                        }),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    let completed = task(&mut history);
    push(
        &mut history,
        EventType::WorkflowExecutionCompleted,
        WorkflowExecutionCompletedEventAttributes {
            workflow_task_completed_event_id: completed,
            ..Default::default()
        },
    );
    history
}

/// Assert that Core's final-retry resolution survives conversion and JSON.
fn assert_resolution(
    activation: &temporalio_common::protos::coresdk::workflow_activation::WorkflowActivation,
    case: FailureCase,
) {
    let core_resolution = activation
        .jobs
        .iter()
        .find_map(|job| match job.variant.as_ref() {
            Some(Variant::ResolveActivity(result)) => Some(result),
            _ => None,
        });
    let core_resolution = core_resolution.expect("pinned Core must resolve the failed activity");
    assert_eq!(core_resolution.seq, 1);
    assert!(matches!(
        core_resolution
            .result
            .as_ref()
            .and_then(|result| result.status.as_ref()),
        Some(CoreActivityStatus::Failed(_))
    ));

    let semantic = workflow_protocol::activation_from_core(activation)
        .expect("known application options must convert from pinned Core");
    let failure = semantic.jobs.iter().find_map(|job| match job {
        ActivationJob::ResolveActivity {
            seq: 1,
            result: ActivityResolution::Failed { failure },
        } => Some(failure),
        _ => None,
    });
    let failure = failure.expect("bridge must retain the failed activity resolution");
    assert!(matches!(
        &failure.info,
        workflow_protocol::FailureInfo::Activity {
            retry_state: workflow_protocol::RetryState::MaximumAttemptsReached,
            ..
        }
    ));
    let cause = failure
        .cause
        .as_ref()
        .expect("application cause must survive");
    assert_eq!(cause.message, "expected business failure");
    let workflow_protocol::FailureInfo::Application {
        type_name,
        category,
        next_retry_delay,
        ..
    } = &cause.info
    else {
        panic!("activity cause must remain an application failure");
    };
    assert_eq!(type_name, "BusinessError");
    assert_eq!(
        *category,
        if case.category == ApplicationErrorCategory::Benign {
            workflow_protocol::ApplicationFailureCategory::Benign
        } else {
            workflow_protocol::ApplicationFailureCategory::Unspecified
        }
    );
    assert_eq!(
        next_retry_delay
            .as_ref()
            .map(|delay| (delay.seconds, delay.nanoseconds)),
        case.retry_delay.then_some((3, 7))
    );

    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );
    let json: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    let activity = json["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|job| job["kind"] == "resolve_activity")
        .expect("JSON retains the activity resolution");
    let application = &activity["result"]["failure"]["cause"]["info"];
    assert_eq!(application["kind"], "application");
    if case.category == ApplicationErrorCategory::Benign {
        assert_eq!(application["category"], "benign");
    } else {
        assert!(application.get("category").is_none());
    }
    if case.retry_delay {
        assert_eq!(application["next_retry_delay"]["seconds"], 3);
        assert_eq!(application["next_retry_delay"]["nanoseconds"], 7);
    } else {
        assert!(application.get("next_retry_delay").is_none());
    }
}

/// Replay all three final-retry histories through one pinned Core worker.
#[test]
fn terminal_application_options_replay_with_other_workflow_progress() {
    let core = CoreRuntime::new(
        RuntimeOptions::builder().build().unwrap(),
        TokioRuntimeBuilder::default(),
    )
    .unwrap();
    let handle = core.tokio_handle();
    let config = WorkerConfig::builder()
        .namespace("audit")
        .task_queue("replay-test")
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "audit".into(),
        })
        .task_types(WorkerTaskTypes::workflow_only())
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(1))
        .max_outstanding_workflow_tasks(1usize)
        .ignore_evicts_on_shutdown(true)
        .build()
        .unwrap();
    let (feeder, stream) = HistoryFeeder::new(CASES.len());
    let worker = {
        let _guard = handle.enter();
        temporalio_sdk_core::init_replay_worker(ReplayWorkerInput::new(config, stream)).unwrap()
    };
    handle.block_on(async {
        for case in CASES {
            feeder
                .feed(HistoryForReplay::new(
                    activity_history(case),
                    case.workflow_id,
                ))
                .await
                .unwrap();
        }
        drop(feeder);

        let mut runs = HashMap::new();
        let mut resolved = HashSet::new();
        let mut resolution_order = Vec::new();
        for _ in 0..24 {
            let activation = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                worker.poll_workflow_activation(),
            )
            .await
            .expect("Core replay must yield another activation")
            .expect("Core replay must not end before all workflows resolve");
            let mut commands = vec![];
            for job in &activation.jobs {
                match job.variant.as_ref().unwrap() {
                    Variant::InitializeWorkflow(initial) => {
                        let case = CASES
                            .iter()
                            .copied()
                            .find(|case| case.workflow_id == initial.workflow_id)
                            .expect("initialize belongs to a queued replay history");
                        runs.insert(activation.run_id.clone(), case);
                        commands.push(
                            ScheduleActivity {
                                seq: 1,
                                activity_id: "ocaml-activity-1".into(),
                                activity_type: "foreign-activity".into(),
                                task_queue: "replay-test".into(),
                                start_to_close_timeout: Some(prost_wkt_types::Duration {
                                    seconds: 60,
                                    nanos: 0,
                                }),
                                ..Default::default()
                            }
                            .into(),
                        );
                    }
                    Variant::ResolveActivity(_) => {
                        let case = *runs
                            .get(&activation.run_id)
                            .expect("activity resolution belongs to an initialized run");
                        assert_resolution(&activation, case);
                        commands.push(CompleteWorkflowExecution { result: None }.into());
                        assert!(
                            resolved.insert(case.workflow_id),
                            "one resolution per history"
                        );
                        resolution_order.push(case.workflow_id);
                    }
                    _ => {}
                }
            }
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                    activation.run_id,
                    commands,
                ))
                .await
                .unwrap();
            if resolved.len() == CASES.len() {
                break;
            }
        }
        assert_eq!(
            resolved.len(),
            CASES.len(),
            "every queued history must resolve"
        );
        assert_eq!(
            resolution_order,
            CASES.map(|case| case.workflow_id),
            "the unrelated queued history must resolve after both option-bearing failures"
        );

        worker.initiate_shutdown();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while worker.poll_workflow_activation().await.is_ok() {}
            worker.finalize_shutdown().await;
        })
        .await
        .unwrap();
    });
}

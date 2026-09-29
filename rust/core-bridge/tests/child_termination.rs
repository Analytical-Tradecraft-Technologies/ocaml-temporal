//! Regression for ordinary child termination through the pinned Core state machine.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ocaml_temporal_core_bridge::workflow_protocol;
use prost::Message;
use prost_wkt_types::Timestamp;
use temporalio_common::{
    protos::{
        coresdk::{
            workflow_activation::workflow_activation_job::Variant,
            workflow_commands::{CompleteWorkflowExecution, StartChildWorkflowExecution},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::{WorkflowExecution, WorkflowType},
            enums::v1::EventType,
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

/// Reuse the repository's proven initialization fixture before adding child events.
fn base_history() -> History {
    let value: serde_json::Value =
        serde_json::from_str(&fixture::complete_history_document("audit-parent")).unwrap();
    History::decode(
        STANDARD
            .decode(value["history"]["data"].as_str().unwrap())
            .unwrap()
            .as_slice(),
    )
    .unwrap()
}

/// Append a timestamped event without pulling in Core's optional test utilities.
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

/// Append a complete workflow task and retain event ids needed by its terminal event.
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

/// The timeout control and termination case have otherwise identical histories.
fn child_history(terminated: bool) -> History {
    let mut history = base_history();
    history.events.truncate(4);
    let execution = WorkflowExecution {
        workflow_id: "audit-child".into(),
        run_id: "audit-child-run".into(),
    };
    let workflow_type = WorkflowType {
        name: "child".into(),
    };
    push(
        &mut history,
        EventType::StartChildWorkflowExecutionInitiated,
        StartChildWorkflowExecutionInitiatedEventAttributes {
            namespace: "audit".into(),
            workflow_id: execution.workflow_id.clone(),
            workflow_type: Some(workflow_type.clone()),
            workflow_task_completed_event_id: 4,
            ..Default::default()
        },
    );
    push(
        &mut history,
        EventType::ChildWorkflowExecutionStarted,
        ChildWorkflowExecutionStartedEventAttributes {
            namespace: "audit".into(),
            workflow_execution: Some(execution.clone()),
            workflow_type: Some(workflow_type.clone()),
            initiated_event_id: 5,
            ..Default::default()
        },
    );
    task(&mut history);
    if terminated {
        push(
            &mut history,
            EventType::ChildWorkflowExecutionTerminated,
            ChildWorkflowExecutionTerminatedEventAttributes {
                namespace: "audit".into(),
                workflow_execution: Some(execution),
                workflow_type: Some(workflow_type),
                initiated_event_id: 5,
                started_event_id: 6,
                ..Default::default()
            },
        );
    } else {
        push(
            &mut history,
            EventType::ChildWorkflowExecutionTimedOut,
            ChildWorkflowExecutionTimedOutEventAttributes {
                namespace: "audit".into(),
                workflow_execution: Some(execution),
                workflow_type: Some(workflow_type),
                initiated_event_id: 5,
                started_event_id: 6,
                ..Default::default()
            },
        );
    }
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

/// Replay an actual Core state-machine result, then apply the real conversion.
fn probe_child(terminated: bool) {
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
    let (feeder, stream) = HistoryFeeder::new(1);
    let worker = {
        let _guard = handle.enter();
        temporalio_sdk_core::init_replay_worker(ReplayWorkerInput::new(config, stream)).unwrap()
    };
    handle.block_on(async {
        feeder
            .feed(HistoryForReplay::new(
                child_history(terminated),
                "audit-parent",
            ))
            .await
            .unwrap();
        drop(feeder);
        let mut observed = false;
        for _ in 0..6 {
            let activation = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                worker.poll_workflow_activation(),
            )
            .await
            .unwrap()
            .unwrap();
            let mut commands = vec![];
            for job in &activation.jobs {
                match job.variant.as_ref().unwrap() {
                    Variant::InitializeWorkflow(_) => commands.push(
                        StartChildWorkflowExecution {
                            seq: 1,
                            namespace: "audit".into(),
                            workflow_id: "audit-child".into(),
                            workflow_type: "child".into(),
                            task_queue: "replay-test".into(),
                            ..Default::default()
                        }
                        .into(),
                    ),
                    Variant::ResolveChildWorkflowExecution(result) => {
                        let converted = workflow_protocol::activation_from_core(&activation)
                            .expect("ordinary child terminal results must reach the parent");
                        let encoded = workflow_protocol::encode_activation(&converted).unwrap();
                        assert_eq!(
                            workflow_protocol::decode_activation(&encoded).unwrap(),
                            converted
                        );
                        let json: serde_json::Value = serde_json::from_str(&encoded).unwrap();
                        assert_eq!(
                            json["jobs"][0]["result"]["failure"]["cause"]["info"]["kind"],
                            if terminated { "terminated" } else { "timeout" }
                        );
                        assert_eq!(result.seq, 1);
                        commands.push(CompleteWorkflowExecution { result: None }.into());
                        observed = true;
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
            if observed {
                break;
            }
        }
        assert!(observed, "must reach a real child terminal event");
        worker.initiate_shutdown();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while worker.poll_workflow_activation().await.is_ok() {}
            worker.finalize_shutdown().await;
        })
        .await
        .unwrap();
    });
}

/// Both outcomes must cross strict JSON validation after Core replay.
#[test]
fn child_termination_and_timeout_reach_the_parent() {
    probe_child(false);
    probe_child(true);
}

use std::{collections::BTreeMap, fs, path::PathBuf};

use ocaml_temporal_core_bridge::workflow_protocol;
use temporalio_protos::coresdk::{
    child_workflow as core_child_workflow, common as core_common,
    workflow_activation as core_activation, workflow_commands as core_commands,
    workflow_completion as core_completion,
};

/// Locates one language-neutral semantic fixture in the repository.
fn fixture_path(parts: &[&str]) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.extend([
        "..",
        "..",
        "test",
        "bridge",
        "fixtures",
        "workflow-protocol",
    ]);
    path.extend(parts);
    path
}

/// Reads a shared fixture without embedding malformed source in diagnostics.
fn fixture(parts: &[&str]) -> String {
    fs::read_to_string(fixture_path(parts)).expect("workflow fixture must be readable")
}

/// Builds a normalized Core retry policy for activation conversion tests.
/// Keeping the protobuf value valid exercises the same required-duration and
/// coefficient invariants that a real retry-enabled child workflow supplies.
fn valid_core_retry_policy() -> temporalio_protos::temporal::api::common::v1::RetryPolicy {
    temporalio_protos::temporal::api::common::v1::RetryPolicy {
        initial_interval: Some(prost_wkt_types::Duration {
            seconds: 1,
            nanos: 0,
        }),
        backoff_coefficient: 2.0,
        maximum_interval: Some(prost_wkt_types::Duration {
            seconds: 5,
            nanos: 0,
        }),
        maximum_attempts: 2,
        non_retryable_error_types: vec!["PermanentChildFailure".to_owned()],
    }
}

/// Reset histories replace the run seed before resuming a timer. The bridge
/// must preserve that order and the complete unsigned seed range through JSON.
#[test]
fn converts_reset_random_seed_before_timer() {
    use core_activation::workflow_activation_job::Variant;

    for seed in [0, 1, i64::MAX as u64 + 1, u64::MAX] {
        let activation = core_activation::WorkflowActivation {
            run_id: "reset-run".to_owned(),
            timestamp: Some(prost_wkt_types::Timestamp::default()),
            jobs: vec![
                core_activation::WorkflowActivationJob {
                    variant: Some(Variant::UpdateRandomSeed(
                        core_activation::UpdateRandomSeed {
                            randomness_seed: seed,
                        },
                    )),
                },
                core_activation::WorkflowActivationJob {
                    variant: Some(Variant::FireTimer(core_activation::FireTimer { seq: 1 })),
                },
            ],
            ..Default::default()
        };
        let semantic = workflow_protocol::activation_from_core(&activation)
            .expect("reset seed update must reach the language runtime");
        let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
        let json: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(json["jobs"][0]["kind"], "update_random_seed");
        assert_eq!(json["jobs"][0]["randomness_seed"], seed.to_string());
        assert_eq!(json["jobs"][1]["kind"], "fire_timer");
        assert_eq!(
            workflow_protocol::decode_activation(&encoded).unwrap(),
            semantic
        );
    }
}

/// Requires a malformed activation to fail as the stable protocol error type.
/// The path and message assertions ensure callers receive bounded diagnostics
/// instead of a serde panic or an untyped transport failure.
fn require_invalid_activation(name: &str) {
    let error =
        workflow_protocol::decode_activation(&fixture(&["invalid", &format!("{name}.json")]))
            .expect_err("malformed activation was accepted");
    assert_eq!(
        error.code, "invalid_message",
        "{name} returned another error"
    );
    assert!(!error.path.is_empty(), "{name} omitted its validation path");
    assert!(
        !error.message.is_empty(),
        "{name} omitted its safe diagnostic"
    );
}

/// Proves all first-slice activation jobs normalize identically to OCaml.
#[test]
fn accepts_and_normalizes_workflow_activations() {
    for name in [
        "activation",
        "eviction",
        "realistic-initialize",
        "child-initialize",
        "child-resolution",
        "child-cancellation-before-start",
        "patch-activation",
        "reset-activation",
    ] {
        let input = fixture(&["valid", &format!("{name}.input.json")]);
        let expected = fixture(&["valid", &format!("{name}.normalized.json")]);
        let value = workflow_protocol::decode_activation(&input).unwrap();
        assert_eq!(
            workflow_protocol::encode_activation(&value).unwrap(),
            expected.trim()
        );
        workflow_protocol::decode_activation(expected.trim()).unwrap();
    }
}

/// Rejects malformed reset seeds at both decoded and directly typed boundaries.
#[test]
fn rejects_invalid_reset_seeds() {
    let activation =
        workflow_protocol::decode_activation(&fixture(&["valid", "reset-activation.input.json"]))
            .unwrap();
    for seed in ["", "01", "-1", "+1", "1.0", "18446744073709551616"] {
        let mut invalid = activation.clone();
        invalid.jobs = vec![workflow_protocol::ActivationJob::UpdateRandomSeed {
            randomness_seed: seed.to_owned(),
        }];
        assert!(workflow_protocol::encode_activation(&invalid).is_err());
        assert!(
            workflow_protocol::decode_activation(&serde_json::to_string(&invalid).unwrap())
                .is_err()
        );
    }
    for job in [
        r#"{"kind":"update_random_seed"}"#,
        r#"{"kind":"update_random_seed","randomness_seed":1}"#,
        r#"{"kind":"update_random_seed","randomness_seed":"1","extra":true}"#,
        r#"{"kind":"update_random_seed","randomness_seed":"1","randomness_seed":"2"}"#,
    ] {
        let json = format!(
            r#"{{"run_id":"reset-run","timestamp":null,"is_replaying":true,"history_length":12,"jobs":[{job}]}}"#
        );
        assert!(workflow_protocol::decode_activation(&json).is_err());
    }
}

/// Proves all first-slice completion commands retain their source order.
#[test]
fn accepts_and_normalizes_workflow_completion() {
    let input = fixture(&["valid", "completion.input.json"]);
    let expected = fixture(&["valid", "completion.normalized.json"]);
    let value = workflow_protocol::decode_completion(&input).unwrap();
    assert_eq!(
        workflow_protocol::encode_completion(&value).unwrap(),
        expected.trim()
    );
    workflow_protocol::decode_completion(expected.trim()).unwrap();
}

/// Proves patch notifications and marker commands survive the semantic JSON
/// boundary and the pinned Core protobuf conversion without deduplication.
/// The language runtime emits one marker command for every patch API call and
/// Core owns durable-history deduplication, so retaining two identical bridge
/// commands here is a replay-safety requirement. Active and deprecated marker
/// modes both survive the private bridge losslessly.
#[test]
fn converts_patch_markers_losslessly() {
    use core_activation::workflow_activation_job::Variant;

    let activation = core_activation::WorkflowActivation {
        run_id: "patch-run".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp {
            seconds: 4,
            nanos: 7,
        }),
        is_replaying: true,
        history_length: 12,
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::NotifyHasPatch(core_activation::NotifyHasPatch {
                patch_id: "orders.v2".to_owned(),
            })),
        }],
        ..Default::default()
    };
    let semantic = workflow_protocol::activation_from_core(&activation).unwrap();
    assert_eq!(
        semantic.jobs,
        vec![workflow_protocol::ActivationJob::NotifyHasPatch {
            patch_id: "orders.v2".to_owned(),
        }]
    );
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );

    let command = workflow_protocol::CompletionCommand::SetPatchMarker {
        patch_id: "orders.v2".to_owned(),
        deprecated: false,
    };
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "patch-run".to_owned(),
        commands: vec![
            command.clone(),
            command,
            workflow_protocol::CompletionCommand::SetPatchMarker {
                patch_id: "orders.v1".to_owned(),
                deprecated: true,
            },
            workflow_protocol::CompletionCommand::SetPatchMarker {
                patch_id: "orders.v1".to_owned(),
                deprecated: true,
            },
        ],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );
    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("patch-marker completion must be successful");
    };
    assert_eq!(success.commands.len(), 4);
    let expected = [
        ("orders.v2", false),
        ("orders.v2", false),
        ("orders.v1", true),
        ("orders.v1", true),
    ];
    for (command, (expected_id, expected_deprecated)) in success.commands.iter().zip(expected) {
        let Some(core_commands::workflow_command::Variant::SetPatchMarker(marker)) =
            command.variant.as_ref()
        else {
            panic!("semantic patch marker must map to Core's patch-marker command");
        };
        assert_eq!(marker.patch_id, expected_id);
        assert_eq!(marker.deprecated, expected_deprecated);
    }
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );

    let fixture_input = fixture(&["valid", "patch-completion.input.json"]);
    let fixture_expected = fixture(&["valid", "patch-completion.normalized.json"]);
    let fixture_value = workflow_protocol::decode_completion(&fixture_input).unwrap();
    assert_eq!(
        workflow_protocol::encode_completion(&fixture_value).unwrap(),
        fixture_expected.trim()
    );
}

/// Proves search-attribute upserts retain sorted map keys, payload bytes, and
/// the corresponding official Core command on both conversion directions.
#[test]
fn converts_search_attribute_upserts_losslessly() {
    let mut attributes = std::collections::BTreeMap::new();
    attributes.insert(
        "status".to_owned(),
        workflow_protocol::Payload {
            metadata: [("encoding".to_owned(), b"json/plain".to_vec())]
                .into_iter()
                .collect(),
            data: br#"\"ready\""#.to_vec(),
        },
    );
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "search-run".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::UpsertSearchAttributes {
                search_attributes: attributes.clone(),
            },
        ],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );
    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("search-attribute completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::UpsertWorkflowSearchAttributes(command)) =
        success.commands[0].variant.as_ref()
    else {
        panic!("search-attribute command did not map to Core");
    };
    assert_eq!(
        command
            .search_attributes
            .as_ref()
            .unwrap()
            .indexed_fields
            .len(),
        1
    );
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );
}

/// Rejects malformed patch documents and the query-plus-notification shape
/// that Core itself forbids when preparing query-only activations.
#[test]
fn rejects_invalid_patch_marker_documents() {
    let empty_activation = r#"{"run_id":"r","timestamp":{"seconds":0,"nanoseconds":0},"is_replaying":true,"history_length":1,"jobs":[{"kind":"notify_has_patch","patch_id":""}]}"#;
    assert!(workflow_protocol::decode_activation(empty_activation).is_err());

    let empty_completion = r#"{"run_id":"r","commands":[{"kind":"set_patch_marker","patch_id":"","deprecated":false}]}"#;
    assert!(workflow_protocol::decode_completion(empty_completion).is_err());

    let missing_deprecated =
        r#"{"run_id":"r","commands":[{"kind":"set_patch_marker","patch_id":"orders.v2"}]}"#;
    assert!(workflow_protocol::decode_completion(missing_deprecated).is_err());

    let conflicting_modes = r#"{"run_id":"r","commands":[{"kind":"set_patch_marker","patch_id":"orders.v2","deprecated":false},{"kind":"set_patch_marker","patch_id":"orders.v2","deprecated":true}]}"#;
    assert!(workflow_protocol::decode_completion(conflicting_modes).is_err());

    let conflicting_semantic = workflow_protocol::Completion {
        task_failure: None,
        run_id: "r".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::SetPatchMarker {
                patch_id: "orders.v2".to_owned(),
                deprecated: true,
            },
            workflow_protocol::CompletionCommand::SetPatchMarker {
                patch_id: "orders.v2".to_owned(),
                deprecated: false,
            },
        ],
    };
    assert!(workflow_protocol::encode_completion(&conflicting_semantic).is_err());
    assert!(workflow_protocol::completion_to_core(&conflicting_semantic).is_err());

    let unknown_field = r#"{"run_id":"r","timestamp":{"seconds":0,"nanoseconds":0},"is_replaying":true,"history_length":1,"jobs":[{"kind":"notify_has_patch","patch_id":"orders.v2","extra":true}]}"#;
    assert!(workflow_protocol::decode_activation(unknown_field).is_err());

    let query_with_notification = r#"{"run_id":"r","timestamp":{"seconds":0,"nanoseconds":0},"is_replaying":true,"history_length":1,"jobs":[{"kind":"query_workflow","query_id":"q","query_type":"state","arguments":[],"headers":{}},{"kind":"notify_has_patch","patch_id":"orders.v2"}]}"#;
    assert!(workflow_protocol::decode_activation(query_with_notification).is_err());

    for invalid_id in ["".to_owned(), "patch\0id".to_owned(), "x".repeat(65_537)] {
        let semantic_activation = workflow_protocol::Activation {
            run_id: "r".to_owned(),
            timestamp: Some(workflow_protocol::Timestamp {
                seconds: 0,
                nanoseconds: 0,
            }),
            is_replaying: true,
            history_length: 1,
            jobs: vec![workflow_protocol::ActivationJob::NotifyHasPatch {
                patch_id: invalid_id.clone(),
            }],
            metadata: None,
        };
        assert!(workflow_protocol::encode_activation(&semantic_activation).is_err());

        let semantic_completion = workflow_protocol::Completion {
            task_failure: None,
            run_id: "r".to_owned(),
            commands: vec![workflow_protocol::CompletionCommand::SetPatchMarker {
                patch_id: invalid_id.clone(),
                deprecated: false,
            }],
        };
        assert!(workflow_protocol::encode_completion(&semantic_completion).is_err());

        let core_activation = core_activation::WorkflowActivation {
            run_id: "r".to_owned(),
            timestamp: Some(prost_wkt_types::Timestamp::default()),
            jobs: vec![core_activation::WorkflowActivationJob {
                variant: Some(
                    core_activation::workflow_activation_job::Variant::NotifyHasPatch(
                        core_activation::NotifyHasPatch {
                            patch_id: invalid_id.clone(),
                        },
                    ),
                ),
            }],
            ..Default::default()
        };
        assert_eq!(
            workflow_protocol::activation_from_core(&core_activation)
                .unwrap_err()
                .code,
            workflow_protocol::CoreConversionErrorCode::InvalidCore
        );

        let core_completion = core_completion::WorkflowActivationCompletion {
            run_id: "r".to_owned(),
            status: Some(
                core_completion::workflow_activation_completion::Status::Successful(
                    core_completion::Success {
                        commands: vec![core_commands::WorkflowCommand {
                            user_metadata: None,
                            variant: Some(
                                core_commands::workflow_command::Variant::SetPatchMarker(
                                    core_commands::SetPatchMarker {
                                        patch_id: invalid_id,
                                        deprecated: false,
                                    },
                                ),
                            ),
                        }],
                        used_internal_flags: Vec::new(),
                        versioning_behavior: 0,
                    },
                ),
            ),
        };
        assert_eq!(
            workflow_protocol::completion_from_core(&core_completion)
                .unwrap_err()
                .code,
            workflow_protocol::CoreConversionErrorCode::InvalidCore
        );
    }

    let query_and_notification = core_activation::WorkflowActivation {
        run_id: "r".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![
            core_activation::WorkflowActivationJob {
                variant: Some(
                    core_activation::workflow_activation_job::Variant::QueryWorkflow(
                        core_activation::QueryWorkflow {
                            query_id: "q".to_owned(),
                            query_type: "state".to_owned(),
                            arguments: Vec::new(),
                            headers: Default::default(),
                        },
                    ),
                ),
            },
            core_activation::WorkflowActivationJob {
                variant: Some(
                    core_activation::workflow_activation_job::Variant::NotifyHasPatch(
                        core_activation::NotifyHasPatch {
                            patch_id: "orders.v2".to_owned(),
                        },
                    ),
                ),
            },
        ],
        ..Default::default()
    };
    assert_eq!(
        workflow_protocol::activation_from_core(&query_and_notification)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );
}

/// Proves the minimal child-workflow start command survives JSON, semantic
/// validation, and the official Core command conversion without losing its
/// operation identity or input payload.
#[test]
fn converts_start_child_workflow_command() {
    let input = workflow_protocol::Payload {
        metadata: [("encoding".to_owned(), b"binary/null".to_vec())].into(),
        data: Vec::new(),
    };
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "parent-run".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::StartChildWorkflow {
            seq: 2,
            workflow_id: "child/1".to_owned(),
            workflow_type: "child".to_owned(),
            input: vec![input.clone()],
            retry_policy: None,
            cancellation_type: workflow_protocol::ChildWorkflowCancellationType::TryCancel,
        }],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert!(encoded.contains("\"kind\":\"start_child_workflow\""));
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );

    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("child completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::StartChildWorkflowExecution(child)) =
        success.commands[0].variant.as_ref()
    else {
        panic!("child command must map to Core's start-child variant");
    };
    assert_eq!(child.seq, 2);
    assert_eq!(child.workflow_id, "child/1");
    assert_eq!(child.workflow_type, "child");
    assert_eq!(child.input.len(), 1);
    assert_eq!(child.cancellation_type, 1);
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );

    let mut unsupported = core;
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        unsupported.status.as_mut()
    else {
        panic!("child completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::StartChildWorkflowExecution(child)) =
        success.commands[0].variant.as_mut()
    else {
        panic!("child command must map to Core's start-child variant");
    };
    child.task_queue = "child-queue".to_owned();
    assert_eq!(
        workflow_protocol::completion_from_core(&unsupported)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::Unsupported
    );
}

/// Proves the local-activity command and its distinct cancellation command are
/// preserved through the strict semantic JSON protocol and Core protobuf
/// conversion. Local activities intentionally omit remote task-queue fields,
/// but retain retry, timeout, attempt, and replay timestamp metadata.
#[test]
fn converts_local_activity_and_cancellation_commands() {
    let input = workflow_protocol::Payload {
        metadata: [("encoding".to_owned(), b"binary/null".to_vec())].into(),
        data: Vec::new(),
    };
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "local-run".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::ScheduleLocalActivity {
                seq: 3,
                activity_id: "local-1".to_owned(),
                activity_type: "local".to_owned(),
                attempt: 1,
                original_schedule_time: Some(workflow_protocol::Timestamp {
                    seconds: 10,
                    nanoseconds: 20,
                }),
                arguments: vec![input],
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: Some(workflow_protocol::Duration {
                    seconds: 30,
                    nanoseconds: 0,
                }),
                retry_policy: None,
                local_retry_threshold: None,
                cancellation_type:
                    workflow_protocol::ActivityCancellationType::WaitCancellationCompleted,
            },
            workflow_protocol::CompletionCommand::RequestCancelLocalActivity { seq: 3 },
        ],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert!(encoded.contains("schedule_local_activity"));
    assert!(encoded.contains("request_cancel_local_activity"));
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );

    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("local completion must be successful");
    };
    assert!(matches!(
        success.commands[0].variant,
        Some(core_commands::workflow_command::Variant::ScheduleLocalActivity(_))
    ));
    assert!(matches!(
        success.commands[1].variant,
        Some(core_commands::workflow_command::Variant::RequestCancelLocalActivity(_))
    ));
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );
}

/// Proves external signal and cancellation commands map to the official Core
/// command variants and back without changing target identity, payload bytes,
/// headers, or operation ordering. The round-trip is important because the
/// resolution sequence is how the OCaml workflow future is resumed.
#[test]
fn converts_external_workflow_commands() {
    let input = workflow_protocol::Payload {
        metadata: [("encoding".to_owned(), b"json/plain".to_vec())].into(),
        data: br#""hello""#.to_vec(),
    };
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "parent-run".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::SignalExternalWorkflow {
                seq: 7,
                workflow_id: "target-workflow".to_owned(),
                run_id: "target-run".to_owned(),
                signal_name: "greeting".to_owned(),
                input: vec![input.clone()],
                child_workflow_only: false,
                headers: [("trace-id".to_owned(), input.clone())].into(),
            },
            workflow_protocol::CompletionCommand::RequestCancelExternalWorkflow {
                seq: 8,
                workflow_id: "target-workflow".to_owned(),
                run_id: "target-run".to_owned(),
                reason: "parent shutdown".to_owned(),
            },
        ],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );

    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("external workflow completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::SignalExternalWorkflowExecution(signal)) =
        success.commands[0].variant.as_ref()
    else {
        panic!("signal command must map to Core's external-signal variant");
    };
    assert_eq!(signal.seq, 7);
    assert_eq!(signal.signal_name, "greeting");
    assert_eq!(signal.args.len(), 1);
    assert_eq!(signal.headers.len(), 1);
    let Some(core_commands::signal_external_workflow_execution::Target::WorkflowExecution(target)) =
        signal.target.as_ref()
    else {
        panic!("signal command must retain a workflow-execution target");
    };
    assert_eq!(target.workflow_id, "target-workflow");
    assert_eq!(target.run_id, "target-run");

    let Some(core_commands::workflow_command::Variant::RequestCancelExternalWorkflowExecution(
        cancel,
    )) = success.commands[1].variant.as_ref()
    else {
        panic!("cancel command must map to Core's external-cancel variant");
    };
    assert_eq!(cancel.seq, 8);
    assert_eq!(cancel.reason, "parent shutdown");
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );
}

/// Ensures a live worker supplies its configured namespace to Core even
/// though namespace is intentionally absent from the workflow-level command.
/// Core copies this field into child failure metadata, including the
/// pre-start cancellation path; omitting it makes a valid cancellation
/// activation impossible to represent at the OCaml boundary.
#[test]
fn injects_worker_namespace_into_child_workflow_command() {
    let activation = workflow_protocol::Activation {
        run_id: "parent-run".to_owned(),
        timestamp: Some(workflow_protocol::Timestamp {
            seconds: 0,
            nanoseconds: 0,
        }),
        is_replaying: false,
        history_length: 0,
        jobs: Vec::new(),
        metadata: None,
    };
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "parent-run".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::StartChildWorkflow {
            seq: 1,
            workflow_id: "child/1".to_owned(),
            workflow_type: "child".to_owned(),
            input: Vec::new(),
            retry_policy: None,
            cancellation_type: workflow_protocol::ChildWorkflowCancellationType::TryCancel,
        }],
    };
    let core = workflow_protocol::completion_to_core_for_activation_with_namespace(
        &activation,
        &completion,
        "namespace-under-test",
    )
    .expect("valid worker namespace must be accepted");
    assert_eq!(
        workflow_protocol::completion_to_core_for_activation_with_namespace(
            &activation,
            &completion,
            "",
        )
        .unwrap_err()
        .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("child completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::StartChildWorkflowExecution(child)) =
        success.commands[0].variant.as_ref()
    else {
        panic!("child command must map to Core's start-child variant");
    };
    assert_eq!(child.namespace, "namespace-under-test");
}

/// Proves every child cancellation policy survives the JSON semantic protocol
/// and the official Core command representation without relying on a numeric
/// enum value at the OCaml/Rust boundary.  The policy is durable workflow
/// history: dropping one variant here would change how cancellation races are
/// replayed by a parent workflow.
#[test]
fn converts_all_child_cancellation_policies() {
    let input = workflow_protocol::Payload {
        metadata: [("encoding".to_owned(), b"binary/null".to_vec())].into(),
        data: Vec::new(),
    };
    let policies = [
        (
            workflow_protocol::ChildWorkflowCancellationType::TryCancel,
            core_child_workflow::ChildWorkflowCancellationType::TryCancel,
            "try_cancel",
        ),
        (
            workflow_protocol::ChildWorkflowCancellationType::WaitCancellationCompleted,
            core_child_workflow::ChildWorkflowCancellationType::WaitCancellationCompleted,
            "wait_cancellation_completed",
        ),
        (
            workflow_protocol::ChildWorkflowCancellationType::Abandon,
            core_child_workflow::ChildWorkflowCancellationType::Abandon,
            "abandon",
        ),
        (
            workflow_protocol::ChildWorkflowCancellationType::WaitCancellationRequested,
            core_child_workflow::ChildWorkflowCancellationType::WaitCancellationRequested,
            "wait_cancellation_requested",
        ),
    ];

    for (policy, core_policy, wire_name) in policies {
        let completion = workflow_protocol::Completion {
            task_failure: None,
            run_id: "parent-run".to_owned(),
            commands: vec![workflow_protocol::CompletionCommand::StartChildWorkflow {
                seq: 2,
                workflow_id: "child/1".to_owned(),
                workflow_type: "child".to_owned(),
                input: vec![input.clone()],
                retry_policy: None,
                cancellation_type: policy,
            }],
        };
        let encoded = workflow_protocol::encode_completion(&completion).unwrap();
        assert!(
            encoded.contains(&format!("\"cancellation_type\":\"{wire_name}\"")),
            "semantic JSON omitted child policy {wire_name}: {encoded}"
        );
        assert_eq!(
            workflow_protocol::decode_completion(&encoded).unwrap(),
            completion
        );

        let core = workflow_protocol::completion_to_core(&completion).unwrap();
        let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
            core.status.as_ref()
        else {
            panic!("child completion must be successful");
        };
        let Some(core_commands::workflow_command::Variant::StartChildWorkflowExecution(child)) =
            success.commands[0].variant.as_ref()
        else {
            panic!("child command must map to Core's start-child variant");
        };
        assert_eq!(child.cancellation_type, core_policy as i32);
        assert_eq!(
            workflow_protocol::completion_from_core(&core).unwrap(),
            completion
        );
    }
}

/// Proves an explicit child cancellation retains its sequence and reason when
/// translated to Core and back through the semantic protocol.
#[test]
fn converts_cancel_child_workflow_command() {
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "parent-run".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::CancelChildWorkflow {
            seq: 7,
            reason: "stop child".to_owned(),
        }],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert!(encoded.contains("\"kind\":\"cancel_child_workflow\""));
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );
    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("child cancellation must be successful");
    };
    let Some(core_commands::workflow_command::Variant::CancelChildWorkflowExecution(cancel)) =
        success.commands[0].variant.as_ref()
    else {
        panic!("child cancellation must map to Core's cancel-child variant");
    };
    assert_eq!(cancel.child_workflow_seq, 7);
    assert_eq!(cancel.reason, "stop child");
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );
}

/// Proves both decoders reject cancellation text that could not be retained
/// safely in deterministic history.  The NUL case arrives through a legal JSON
/// escape, so this also guards against validating only the serialized bytes.
#[test]
fn rejects_invalid_child_cancellation_commands() {
    for reason in ["", "\u{0}"] {
        let document = serde_json::json!({
            "run_id": "parent-run",
            "commands": [{
                "kind": "cancel_child_workflow",
                "seq": 7,
                "reason": reason,
            }],
        });
        assert!(
            workflow_protocol::decode_completion(&document.to_string()).is_err(),
            "invalid reason was accepted: {reason:?}"
        );
    }
    let oversized = serde_json::json!({
        "run_id": "parent-run",
        "commands": [{
            "kind": "cancel_child_workflow",
            "seq": 7,
            "reason": "x".repeat(65_537),
        }],
    });
    assert!(workflow_protocol::decode_completion(&oversized.to_string()).is_err());
    for (workflow_id, workflow_type) in [("child\0", "child"), ("child", "child\0")] {
        let document = serde_json::json!({
            "run_id": "parent-run",
            "commands": [{
                "kind": "start_child_workflow",
                "seq": 7,
                "workflow_id": workflow_id,
                "workflow_type": workflow_type,
                "input": [],
                "retry_policy": null,
                "cancellation_type": "try_cancel",
            }],
        });
        assert!(
            workflow_protocol::decode_completion(&document.to_string()).is_err(),
            "NUL-containing child identifier was accepted: {workflow_id:?}/{workflow_type:?}"
        );
    }
    let outgoing_nul = workflow_protocol::Completion {
        task_failure: None,
        run_id: "parent-run".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::StartChildWorkflow {
            seq: 7,
            workflow_id: "child\0".to_owned(),
            workflow_type: "child".to_owned(),
            input: Vec::new(),
            retry_policy: None,
            cancellation_type: workflow_protocol::ChildWorkflowCancellationType::TryCancel,
        }],
    };
    assert!(workflow_protocol::encode_completion(&outgoing_nul).is_err());
    let unknown_policy = serde_json::json!({
        "run_id": "parent-run",
        "commands": [{
            "kind": "start_child_workflow",
            "seq": 7,
            "workflow_id": "child",
            "workflow_type": "child",
            "input": [],
            "retry_policy": null,
            "cancellation_type": "unknown",
        }],
    });
    assert!(workflow_protocol::decode_completion(&unknown_policy.to_string()).is_err());
}

/// Proves continue-as-new retains its workflow identity and arguments while
/// rejecting Core options that the deliberately small semantic protocol does
/// not expose yet.
#[test]
fn converts_continue_as_new_command() {
    let input = workflow_protocol::Payload {
        metadata: [("encoding".to_owned(), b"binary/null".to_vec())].into(),
        data: Vec::new(),
    };
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "current-run".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::ContinueAsNew {
            workflow_type: "counter".to_owned(),
            input: vec![input],
        }],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert!(encoded.contains("\"kind\":\"continue_as_new\""));
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );
    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("continue-as-new completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::ContinueAsNewWorkflowExecution(command)) =
        success.commands[0].variant.as_ref()
    else {
        panic!("continue-as-new must map to Core's continue-as-new variant");
    };
    assert_eq!(command.workflow_type, "counter");
    assert_eq!(command.arguments.len(), 1);
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );
    let mut unsupported = core;
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        unsupported.status.as_mut()
    else {
        panic!("continue-as-new completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::ContinueAsNewWorkflowExecution(command)) =
        success.commands[0].variant.as_mut()
    else {
        panic!("continue-as-new must map to Core's continue-as-new variant");
    };
    command.task_queue = "other".to_owned();
    assert_eq!(
        workflow_protocol::completion_from_core(&unsupported)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::Unsupported
    );
}

/// Keeps the terminal-command invariant explicit for continue-as-new. A
/// language worker must never send a follow-up timer or child command after
/// asking Core to replace the current run; accepting that shape would make the
/// server and the deterministic OCaml scheduler disagree about command order.
#[test]
fn rejects_continue_as_new_with_follow_up_command() {
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "current-run".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::ContinueAsNew {
                workflow_type: "counter".to_owned(),
                input: Vec::new(),
            },
            workflow_protocol::CompletionCommand::StartTimer {
                seq: 1,
                start_to_fire_timeout: workflow_protocol::Duration {
                    seconds: 1,
                    nanoseconds: 0,
                },
            },
        ],
    };
    assert!(workflow_protocol::encode_completion(&completion).is_err());
    assert!(workflow_protocol::completion_to_core(&completion).is_err());
}

/// Proves semantic exactness and range rules independently of JSON structure.
#[test]
fn rejects_malformed_workflow_documents() {
    for name in [
        "activation-duplicate-field",
        "activation-unknown-job",
        "activation-seq-negative",
        "activation-seq-too-large",
        "activation-invalid-base64",
        "activation-missing-field",
        "activation-eviction-mixed",
        "activation-child-start-missing-run-id",
        "activation-child-start-empty-run-id",
        "activation-child-start-invalid-cause",
        "activation-child-terminal-missing-payload",
        "activation-child-terminal-missing-failure-info",
        "activation-child-failure-empty-run-id-after-start",
        "activation-child-failure-empty-workflow-id",
        "activation-child-failure-negative-event-id",
        "activation-child-unknown-terminal-kind",
    ] {
        require_invalid_activation(name);
    }
    for name in [
        "completion-unknown-command",
        "completion-terminal-not-last",
        "completion-invalid-duration",
        "completion-no-activity-timeout",
        "completion-unknown-nested",
        "completion-duplicate-field",
    ] {
        assert!(
            workflow_protocol::decode_completion(&fixture(&["invalid", &format!("{name}.json")]))
                .is_err(),
            "{name} was accepted"
        );
    }
}

/// Proves the pinned Core boundary preserves supported values and rejects
/// omitted metadata instead of silently dropping it.
#[test]
fn converts_pinned_core_values_losslessly() {
    use core_activation::workflow_activation_job::Variant;
    let activation = core_activation::WorkflowActivation {
        run_id: "run-1".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp {
            seconds: 12,
            nanos: 34,
        }),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::FireTimer(core_activation::FireTimer { seq: 7 })),
        }],
        ..Default::default()
    };
    let semantic = workflow_protocol::activation_from_core(&activation).unwrap();
    assert!(matches!(
        semantic.jobs.as_slice(),
        [workflow_protocol::ActivationJob::FireTimer { seq: 7 }]
    ));

    let completion =
        workflow_protocol::decode_completion(&fixture(&["valid", "completion.input.json"]))
            .unwrap();
    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );

    let mut continuation_activation = activation.clone();
    continuation_activation.jobs[0].variant = Some(Variant::InitializeWorkflow(
        core_activation::InitializeWorkflow {
            workflow_type: "workflow".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            randomness_seed: 1,
            attempt: 1,
            first_execution_run_id: "first-run".to_owned(),
            continued_from_execution_run_id: "previous-run".to_owned(),
            continued_initiator: 1,
            ..Default::default()
        },
    ));
    let continuation = workflow_protocol::activation_from_core(&continuation_activation)
        .expect("continuation metadata should be represented")
        .jobs
        .into_iter()
        .find_map(|job| match job {
            workflow_protocol::ActivationJob::InitializeWorkflow { context, .. } => context,
            _ => None,
        })
        .and_then(|context| context.continuation)
        .expect("continuation metadata should be retained");
    assert_eq!(continuation.continued_from_execution_run_id, "previous-run");
    assert_eq!(
        continuation.initiator,
        workflow_protocol::ContinueAsNewInitiator::Workflow
    );

    let mut malformed_payload_activation = activation;
    malformed_payload_activation.jobs[0].variant = Some(Variant::InitializeWorkflow(
        core_activation::InitializeWorkflow {
            workflow_type: "workflow".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            arguments: vec![temporalio_protos::temporal::api::common::v1::Payload {
                metadata: [(String::new(), Vec::new())].into(),
                ..Default::default()
            }],
            randomness_seed: 1,
            first_execution_run_id: "first-run".to_owned(),
            attempt: 1,
            ..Default::default()
        },
    ));
    assert_eq!(
        workflow_protocol::activation_from_core(&malformed_payload_activation)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let mut unsupported_completion = core;
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        unsupported_completion.status.as_mut()
    else {
        panic!("test completion must be successful");
    };
    success.commands[0].user_metadata = Some(Default::default());
    assert_eq!(
        workflow_protocol::completion_from_core(&unsupported_completion)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::Unsupported
    );
}

/// Proves Core's inherited continuation options do not make a valid successor
/// activation fail admission. Retry policy is retained as typed context, while
/// the other options remain compatibility metadata accepted only when
/// continuation provenance is present; an ordinary root activation still
/// rejects the unrepresented metadata instead of silently dropping it.
#[test]
fn accepts_inherited_initialize_options_on_continuation() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::temporal::api::common::v1::{Memo, SearchAttributes};

    let inherited = core_activation::InitializeWorkflow {
        workflow_type: "workflow".to_owned(),
        workflow_id: "workflow-1".to_owned(),
        randomness_seed: 1,
        attempt: 1,
        first_execution_run_id: "first-run".to_owned(),
        continued_from_execution_run_id: "previous-run".to_owned(),
        continued_initiator: 1,
        retry_policy: Some(valid_core_retry_policy()),
        cron_schedule: "0 * * * * *".to_owned(),
        workflow_execution_expiration_time: Some(prost_wkt_types::Timestamp {
            seconds: 1,
            nanos: 2,
        }),
        cron_schedule_to_schedule_interval: Some(prost_wkt_types::Duration {
            seconds: 3,
            nanos: 4,
        }),
        memo: Some(Memo::default()),
        search_attributes: Some(SearchAttributes::default()),
        ..Default::default()
    };
    let activation = core_activation::WorkflowActivation {
        run_id: "run-1".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::InitializeWorkflow(inherited.clone())),
        }],
        ..Default::default()
    };

    let context = workflow_protocol::activation_from_core(&activation)
        .expect("inherited continuation options should be compatibility metadata")
        .jobs
        .into_iter()
        .find_map(|job| match job {
            workflow_protocol::ActivationJob::InitializeWorkflow { context, .. } => context,
            _ => None,
        })
        .expect("initialize context should be present");
    assert_eq!(
        context
            .retry_policy
            .expect("Core retry policy should be represented")
            .maximum_attempts,
        2
    );

    let mut root = inherited;
    root.continued_from_execution_run_id.clear();
    root.continued_initiator = 0;
    let root_activation = core_activation::WorkflowActivation {
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::InitializeWorkflow(root)),
        }],
        ..Default::default()
    };
    assert_eq!(
        workflow_protocol::activation_from_core(&root_activation)
            .expect_err("root metadata must not be silently discarded")
            .code,
        workflow_protocol::CoreConversionErrorCode::Unsupported
    );
}

/// Proves an incoming signal maps from the pinned Core protobuf oneof to the
/// lossless semantic job. Signal payloads, sender identity, and headers all
/// participate in replay, so dropping any one of them would make a future
/// OCaml signal handler observe a different event than Core delivered.
#[test]
fn converts_signal_workflow_activation_losslessly() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::temporal::api::common::v1::Payload as CorePayload;

    let payload = CorePayload {
        metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
        data: b"signal-value".to_vec(),
        ..Default::default()
    };
    let activation = core_activation::WorkflowActivation {
        run_id: "run-signal".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::SignalWorkflow(core_activation::SignalWorkflow {
                signal_name: "order_updated".to_owned(),
                input: vec![payload.clone()],
                identity: "séndér".to_owned(),
                headers: [("trace".to_owned(), payload)].into(),
            })),
        }],
        ..Default::default()
    };

    let semantic = workflow_protocol::activation_from_core(&activation).unwrap();
    match semantic.jobs.as_slice() {
        [
            workflow_protocol::ActivationJob::SignalWorkflow {
                signal_name,
                input,
                identity,
                headers,
            },
        ] => {
            assert_eq!(signal_name, "order_updated");
            assert_eq!(identity, "séndér");
            assert_eq!(input.len(), 1);
            assert_eq!(headers.len(), 1);
            assert_eq!(input[0].data, b"signal-value");
            assert_eq!(headers.get("trace").unwrap().data, b"signal-value");
        }
        jobs => panic!("unexpected signal activation jobs: {jobs:?}"),
    }
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );
}

/// Proves a sender identity cannot smuggle an embedded NUL through the
/// semantic encoder. Rust strings are UTF-8 by construction, so the bilateral
/// UTF-8 invariant is represented by accepting a non-ASCII identity in the
/// normal round-trip above while this test covers the byte-level rejection
/// that Rust can observe directly.
#[test]
fn rejects_signal_identity_with_nul() {
    use core_activation::workflow_activation_job::Variant;

    let activation = core_activation::WorkflowActivation {
        run_id: "run-signal-invalid".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::SignalWorkflow(core_activation::SignalWorkflow {
                signal_name: "order_updated".to_owned(),
                input: Vec::new(),
                identity: "sender\0".to_owned(),
                headers: Default::default(),
            })),
        }],
        ..Default::default()
    };

    let error = workflow_protocol::activation_from_core(&activation)
        .expect_err("NUL-containing signal identity was accepted");
    assert_eq!(
        error.code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let semantic = workflow_protocol::Activation {
        run_id: "run-signal-invalid".to_owned(),
        timestamp: Some(workflow_protocol::Timestamp {
            seconds: 0,
            nanoseconds: 0,
        }),
        is_replaying: false,
        history_length: 0,
        jobs: vec![workflow_protocol::ActivationJob::SignalWorkflow {
            signal_name: "order_updated".to_owned(),
            input: Vec::new(),
            identity: "sender\0".to_owned(),
            headers: BTreeMap::new(),
        }],
        metadata: None,
    };
    let error = workflow_protocol::encode_activation(&semantic)
        .expect_err("NUL-containing semantic identity was accepted");
    assert_eq!(error.code, "invalid_message");
    assert!(error.path.contains("identity"));
}

/// Proves query jobs preserve the query identifier, repeated arguments, and
/// headers when translating the pinned Core activation into the semantic
/// protocol. Query activations are deliberately isolated from ordinary
/// workflow jobs so the owner-domain dispatcher can answer synchronously.
#[test]
fn converts_query_workflow_activation_losslessly() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::temporal::api::common::v1::Payload as CorePayload;

    let payload = CorePayload {
        metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
        data: b"query-argument".to_vec(),
        ..Default::default()
    };
    let activation = core_activation::WorkflowActivation {
        run_id: "run-query".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::QueryWorkflow(core_activation::QueryWorkflow {
                query_id: "query-42".to_owned(),
                query_type: "status".to_owned(),
                arguments: vec![payload.clone(), payload.clone()],
                headers: [("trace".to_owned(), payload.clone())].into(),
            })),
        }],
        ..Default::default()
    };

    let semantic = workflow_protocol::activation_from_core(&activation).unwrap();
    match semantic.jobs.as_slice() {
        [
            workflow_protocol::ActivationJob::QueryWorkflow {
                query_id,
                query_type,
                arguments,
                headers,
            },
        ] => {
            assert_eq!(query_id, "query-42");
            assert_eq!(query_type, "status");
            assert_eq!(arguments.len(), 2);
            assert_eq!(arguments[0].data, b"query-argument");
            assert_eq!(headers["trace"].data, b"query-argument");
        }
        jobs => panic!("unexpected query activation jobs: {jobs:?}"),
    }
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );

    // Core identifies the legacy PollWFTResp query by a reserved ID. The
    // bridge must retain that exact value for its legacy response routing.
    let mut legacy = activation;
    let Some(Variant::QueryWorkflow(query)) = legacy.jobs[0].variant.as_mut() else {
        panic!("query activation must contain a query job");
    };
    query.query_id = "legacy_query".to_owned();
    let legacy_semantic = workflow_protocol::activation_from_core(&legacy).unwrap();
    assert!(matches!(
        &legacy_semantic.jobs[0],
        workflow_protocol::ActivationJob::QueryWorkflow { query_id, .. }
            if query_id == "legacy_query"
    ));
}

/// Proves successful and failed query results map to Core's query oneof and
/// back without losing payload bytes or recursive failure information. The
/// activation-aware conversion also rejects a missing or extra query result,
/// preventing an answer from being assigned to the wrong query.
#[test]
fn converts_query_results_and_matches_activation_ids() {
    use core_commands::query_result::Variant as QueryVariant;

    let payload = workflow_protocol::Payload {
        metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
        data: b"query-result".to_vec(),
    };
    let activation = workflow_protocol::Activation {
        run_id: "run-query".to_owned(),
        timestamp: Some(workflow_protocol::Timestamp {
            seconds: 0,
            nanoseconds: 0,
        }),
        is_replaying: false,
        history_length: 1,
        jobs: vec![workflow_protocol::ActivationJob::QueryWorkflow {
            query_id: "query-42".to_owned(),
            query_type: "status".to_owned(),
            arguments: Vec::new(),
            headers: BTreeMap::new(),
        }],
        metadata: None,
    };
    let succeeded = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-query".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::QueryResult {
            query_id: "query-42".to_owned(),
            result: workflow_protocol::QueryResult::Succeeded {
                payload: payload.clone(),
            },
        }],
    };
    let core = workflow_protocol::completion_to_core_for_activation(&activation, &succeeded)
        .expect("matching query result must convert");
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("query completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::RespondToQuery(query)) =
        success.commands[0].variant.as_ref()
    else {
        panic!("query result must map to RespondToQuery");
    };
    assert_eq!(query.query_id, "query-42");
    assert!(matches!(
        query.variant.as_ref(),
        Some(QueryVariant::Succeeded(_))
    ));
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        succeeded
    );

    let failure = workflow_protocol::Failure {
        message: "query failed".to_owned(),
        source: "ocaml".to_owned(),
        stack_trace: String::new(),
        encoded_attributes: None,
        cause: None,
        info: workflow_protocol::FailureInfo::Application {
            type_name: "QueryError".to_owned(),
            non_retryable: true,
            details: Vec::new(),
        },
    };
    let failed = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-query".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::QueryResult {
            query_id: "query-42".to_owned(),
            result: workflow_protocol::QueryResult::Failed {
                failure: Box::new(failure.clone()),
            },
        }],
    };
    let failed_core =
        workflow_protocol::completion_to_core_for_activation(&activation, &failed).unwrap();
    assert_eq!(
        workflow_protocol::completion_from_core(&failed_core).unwrap(),
        failed
    );

    let second_query_activation = workflow_protocol::Activation {
        jobs: vec![
            workflow_protocol::ActivationJob::QueryWorkflow {
                query_id: "query-42".to_owned(),
                query_type: "status".to_owned(),
                arguments: Vec::new(),
                headers: BTreeMap::new(),
            },
            workflow_protocol::ActivationJob::QueryWorkflow {
                query_id: "query-43".to_owned(),
                query_type: "status".to_owned(),
                arguments: Vec::new(),
                headers: BTreeMap::new(),
            },
        ],
        ..activation.clone()
    };
    let missing = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-query".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::QueryResult {
            query_id: "query-42".to_owned(),
            result: workflow_protocol::QueryResult::Succeeded {
                payload: workflow_protocol::Payload {
                    metadata: BTreeMap::new(),
                    data: b"one".to_vec(),
                },
            },
        }],
    };
    assert_eq!(
        workflow_protocol::completion_to_core_for_activation(&second_query_activation, &missing)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let extra = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-query".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::QueryResult {
                query_id: "query-42".to_owned(),
                result: workflow_protocol::QueryResult::Succeeded {
                    payload: workflow_protocol::Payload {
                        metadata: BTreeMap::new(),
                        data: b"one".to_vec(),
                    },
                },
            },
            workflow_protocol::CompletionCommand::QueryResult {
                query_id: "query-42".to_owned(),
                result: workflow_protocol::QueryResult::Succeeded {
                    payload: workflow_protocol::Payload {
                        metadata: BTreeMap::new(),
                        data: b"duplicate".to_vec(),
                    },
                },
            },
        ],
    };
    assert_eq!(
        workflow_protocol::completion_to_core_for_activation(&activation, &extra)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let ordinary_activation = workflow_protocol::Activation {
        jobs: vec![workflow_protocol::ActivationJob::FireTimer { seq: 1 }],
        ..activation.clone()
    };
    assert_eq!(
        workflow_protocol::completion_to_core_for_activation(&ordinary_activation, &failed)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let mixed = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-query".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::QueryResult {
                query_id: "query-42".to_owned(),
                result: workflow_protocol::QueryResult::Succeeded {
                    payload: workflow_protocol::Payload {
                        metadata: BTreeMap::new(),
                        data: b"answer".to_vec(),
                    },
                },
            },
            workflow_protocol::CompletionCommand::StartTimer {
                seq: 1,
                start_to_fire_timeout: workflow_protocol::Duration {
                    seconds: 1,
                    nanoseconds: 0,
                },
            },
        ],
    };
    assert_eq!(
        workflow_protocol::completion_to_core_for_activation(&activation, &mixed)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let mismatched = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-query".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::QueryResult {
            query_id: "other-query".to_owned(),
            result: workflow_protocol::QueryResult::Succeeded { payload },
        }],
    };
    assert_eq!(
        workflow_protocol::completion_to_core_for_activation(&activation, &mismatched)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );
}

/// Proves the pinned Core `DoUpdate` job retains its authoritative top-level
/// update identifier, metadata, repeated input, headers, and replay-validation
/// state through the bilateral semantic protocol.  Core may strip the
/// duplicate nested metadata ID, so conversion must rebuild that semantic
/// field from `DoUpdate.id` while still rejecting a non-empty conflicting copy.
#[test]
fn converts_do_update_activation_losslessly() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::temporal::api::{common::v1::Payload as CorePayload, update::v1::Meta};

    let payload = CorePayload {
        metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
        data: b"update-input".to_vec(),
        ..Default::default()
    };
    let activation = core_activation::WorkflowActivation {
        run_id: "run-update".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::DoUpdate(core_activation::DoUpdate {
                id: "update-42".to_owned(),
                protocol_instance_id: "protocol-42".to_owned(),
                name: "set-status".to_owned(),
                input: vec![payload.clone()],
                headers: [("trace".to_owned(), payload.clone())].into(),
                meta: Some(Meta {
                    update_id: "update-42".to_owned(),
                    identity: "client".to_owned(),
                }),
                run_validator: false,
            })),
        }],
        ..Default::default()
    };

    let semantic = workflow_protocol::activation_from_core(&activation).unwrap();
    assert!(matches!(
        &semantic.jobs[0],
        workflow_protocol::ActivationJob::DoUpdate {
            id,
            protocol_instance_id,
            name,
            input,
            headers,
            meta,
            run_validator,
        } if id == "update-42"
            && protocol_instance_id == "protocol-42"
            && name == "set-status"
            && input.len() == 1
            && input[0].data == b"update-input"
            && headers["trace"].data == b"update-input"
            && meta.update_id == "update-42"
            && meta.identity == "client"
            && !run_validator
    ));
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );

    // Core's activation protobuf can omit the duplicate nested ID.  The
    // semantic bridge must preserve the workflow-visible identity instead of
    // rejecting this valid default-valued field.
    let mut stripped_metadata = activation.clone();
    let Some(Variant::DoUpdate(update)) = stripped_metadata.jobs[0].variant.as_mut() else {
        panic!("update activation must contain a DoUpdate job");
    };
    update.meta.as_mut().unwrap().update_id.clear();
    let stripped_semantic = workflow_protocol::activation_from_core(&stripped_metadata)
        .expect("Core update with stripped nested metadata ID was rejected");
    assert!(matches!(
        &stripped_semantic.jobs[0],
        workflow_protocol::ActivationJob::DoUpdate { id, meta, .. }
            if id == "update-42" && meta.update_id == "update-42"
    ));

    // A populated but conflicting duplicate is still malformed Core input and
    // must not be silently normalized into a different workflow identity.
    let mut conflicting_metadata = activation.clone();
    let Some(Variant::DoUpdate(update)) = conflicting_metadata.jobs[0].variant.as_mut() else {
        panic!("update activation must contain a DoUpdate job");
    };
    update.meta.as_mut().unwrap().update_id = "other-update".to_owned();
    let conflict_error = workflow_protocol::activation_from_core(&conflicting_metadata)
        .expect_err("conflicting Core update metadata ID was accepted");
    assert_eq!(
        conflict_error.code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let mut missing_meta = activation;
    let Some(Variant::DoUpdate(update)) = missing_meta.jobs[0].variant.as_mut() else {
        panic!("update activation must contain a DoUpdate job");
    };
    update.meta = None;
    let error = workflow_protocol::activation_from_core(&missing_meta)
        .expect_err("Core update without metadata was accepted");
    assert_eq!(
        error.code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );
}

/// Proves accepted, rejected, and completed update responses preserve their
/// protocol-instance correlation through Core conversion.  It also exercises
/// the activation phase rules: immediate acceptance plus completion is valid,
/// a later completion-only response is valid, and a second terminal response
/// is rejected before any protobuf is built.
#[test]
fn converts_update_responses_and_enforces_phases() {
    let payload = workflow_protocol::Payload {
        metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
        data: b"updated".to_vec(),
    };
    let activation = workflow_protocol::Activation {
        run_id: "run-update".to_owned(),
        timestamp: Some(workflow_protocol::Timestamp {
            seconds: 0,
            nanoseconds: 0,
        }),
        is_replaying: false,
        history_length: 1,
        jobs: vec![workflow_protocol::ActivationJob::DoUpdate {
            id: "update-42".to_owned(),
            protocol_instance_id: "protocol-42".to_owned(),
            name: "set-status".to_owned(),
            input: vec![payload.clone()],
            headers: [("trace".to_owned(), payload.clone())].into(),
            meta: workflow_protocol::UpdateMeta {
                identity: "client".to_owned(),
                update_id: "update-42".to_owned(),
            },
            run_validator: true,
        }],
        metadata: None,
    };
    let immediate = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-update".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::UpdateResponse {
                protocol_instance_id: "protocol-42".to_owned(),
                response: workflow_protocol::UpdateResponseResult::Accepted,
            },
            workflow_protocol::CompletionCommand::UpdateResponse {
                protocol_instance_id: "protocol-42".to_owned(),
                response: workflow_protocol::UpdateResponseResult::Completed {
                    payload: payload.clone(),
                },
            },
        ],
    };
    let core = workflow_protocol::completion_to_core_for_activation(&activation, &immediate)
        .expect("accepted and completed responses must convert");
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("update completion must be successful");
    };
    assert_eq!(success.commands.len(), 2);
    assert!(matches!(
        success.commands[0].variant.as_ref(),
        Some(core_commands::workflow_command::Variant::UpdateResponse(response))
            if response.protocol_instance_id == "protocol-42"
                && matches!(
                    response.response.as_ref(),
                    Some(core_commands::update_response::Response::Accepted(()))
                )
    ));
    assert!(matches!(
        success.commands[1].variant.as_ref(),
        Some(core_commands::workflow_command::Variant::UpdateResponse(response))
            if matches!(
                response.response.as_ref(),
                Some(core_commands::update_response::Response::Completed(value))
                    if value.data == b"updated"
            )
    ));
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        immediate
    );

    let later = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-update".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::UpdateResponse {
            protocol_instance_id: "protocol-42".to_owned(),
            response: workflow_protocol::UpdateResponseResult::Completed { payload },
        }],
    };
    workflow_protocol::completion_to_core_for_activation(&activation, &later)
        .expect("completion-only response must be valid in a later activation");

    let accepted_then_rejected = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-update".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::UpdateResponse {
                protocol_instance_id: "protocol-42".to_owned(),
                response: workflow_protocol::UpdateResponseResult::Accepted,
            },
            workflow_protocol::CompletionCommand::UpdateResponse {
                protocol_instance_id: "protocol-42".to_owned(),
                response: workflow_protocol::UpdateResponseResult::Rejected {
                    failure: Box::new(workflow_protocol::Failure {
                        message: "handler failed".to_owned(),
                        source: "ocaml".to_owned(),
                        stack_trace: String::new(),
                        encoded_attributes: None,
                        cause: None,
                        info: workflow_protocol::FailureInfo::Application {
                            type_name: "UpdateError".to_owned(),
                            non_retryable: true,
                            details: Vec::new(),
                        },
                    }),
                },
            },
        ],
    };
    workflow_protocol::completion_to_core_for_activation(&activation, &accepted_then_rejected)
        .expect("accepted then rejected is a valid terminal update response");

    let duplicate_terminal = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-update".to_owned(),
        commands: vec![
            workflow_protocol::CompletionCommand::UpdateResponse {
                protocol_instance_id: "protocol-42".to_owned(),
                response: workflow_protocol::UpdateResponseResult::Completed {
                    payload: workflow_protocol::Payload {
                        metadata: BTreeMap::new(),
                        data: b"one".to_vec(),
                    },
                },
            },
            workflow_protocol::CompletionCommand::UpdateResponse {
                protocol_instance_id: "protocol-42".to_owned(),
                response: workflow_protocol::UpdateResponseResult::Completed {
                    payload: workflow_protocol::Payload {
                        metadata: BTreeMap::new(),
                        data: b"two".to_vec(),
                    },
                },
            },
        ],
    };
    let error =
        workflow_protocol::completion_to_core_for_activation(&activation, &duplicate_terminal)
            .expect_err("duplicate update terminal responses were accepted");
    assert_eq!(
        error.code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );
}

/// Proves ordinary first-task metadata survives Core conversion and semantic
/// JSON instead of being rejected as an unsupported default-only initializer.
#[test]
fn preserves_realistic_first_workflow_activation() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::coresdk::workflow_activation::remove_from_cache::EvictionReason;

    let initialize = core_activation::InitializeWorkflow {
        workflow_type: "order_workflow".to_owned(),
        workflow_id: "order-42".to_owned(),
        arguments: vec![Default::default()],
        randomness_seed: u64::MAX,
        headers: [("trace".to_owned(), Default::default())].into(),
        identity: "starter".to_owned(),
        workflow_execution_timeout: Some(prost_wkt_types::Duration {
            seconds: 3_600,
            nanos: 0,
        }),
        workflow_run_timeout: Some(prost_wkt_types::Duration {
            seconds: 600,
            nanos: 0,
        }),
        workflow_task_timeout: Some(prost_wkt_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        first_execution_run_id: "first-run".to_owned(),
        attempt: 1,
        start_time: Some(prost_wkt_types::Timestamp {
            seconds: 1_720_000_000,
            nanos: 123,
        }),
        // Temporal Server emits an explicit zero first-task backoff for an
        // ordinary non-cron start. The bridge accepts this canonical default
        // but does not expose it as workflow metadata.
        cron_schedule_to_schedule_interval: Some(prost_wkt_types::Duration::default()),
        priority: Some(temporalio_protos::temporal::api::common::v1::Priority {
            priority_key: 2,
            fairness_key: "tenant-a".to_owned(),
            fairness_weight: 1.5,
        }),
        ..Default::default()
    };
    let core = core_activation::WorkflowActivation {
        run_id: "run-1".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp {
            seconds: 1_720_000_001,
            nanos: 456,
        }),
        is_replaying: false,
        history_length: 3,
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::InitializeWorkflow(initialize)),
        }],
        available_internal_flags: vec![1, 2],
        history_size_bytes: 9_999,
        continue_as_new_suggested: true,
        deployment_version_for_current_task: Some(core_common::WorkerDeploymentVersion {
            deployment_name: "prod".to_owned(),
            build_id: "build-1".to_owned(),
        }),
        last_sdk_version: "1.2.3".to_owned(),
        suggest_continue_as_new_reasons: vec![1],
        target_worker_deployment_version_changed: true,
    };
    let semantic = workflow_protocol::activation_from_core(&core).unwrap();
    let context = match &semantic.jobs[0] {
        workflow_protocol::ActivationJob::InitializeWorkflow {
            context: Some(context),
            ..
        } => context,
        _ => panic!("realistic activation must retain initialization context"),
    };
    assert_eq!(context.identity, "starter");
    assert_eq!(
        context.priority.as_ref().unwrap().fairness_weight_bits,
        1.5_f32.to_bits()
    );
    assert_eq!(
        semantic.metadata.as_ref().unwrap().history_size_bytes,
        "9999"
    );
    let json = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&json).unwrap(),
        semantic
    );

    // A non-zero first-task backoff carries scheduling semantics and must not
    // be silently discarded while this protocol slice has no representation
    // for it.
    let mut unsupported_backoff = core.clone();
    let Some(Variant::InitializeWorkflow(initialize)) =
        unsupported_backoff.jobs[0].variant.as_mut()
    else {
        panic!("realistic activation must contain initialization");
    };
    initialize.cron_schedule_to_schedule_interval = Some(prost_wkt_types::Duration {
        seconds: 1,
        nanos: 0,
    });
    let error = workflow_protocol::activation_from_core(&unsupported_backoff).unwrap_err();
    assert_eq!(
        error.code,
        workflow_protocol::CoreConversionErrorCode::Unsupported
    );

    let mut unknown_suggestion = core;
    unknown_suggestion.suggest_continue_as_new_reasons = vec![999];
    assert_eq!(
        workflow_protocol::activation_from_core(&unknown_suggestion)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    // Keep the imported pinned enum exercised so an upstream name drift is a
    // compile-time failure in this boundary test.
    assert_eq!(EvictionReason::CacheFull as i32, 1);
}

/// Proves a successor activation preserves Core's continuation failure and
/// completion payloads in addition to the previous run identity. These fields
/// explain why a run was continued and must not be discarded at the bridge.
#[test]
fn preserves_continuation_terminal_metadata() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    };

    let continuation_failure = Failure {
        message: "previous run failed".to_owned(),
        source: "core".to_owned(),
        stack_trace: "stack".to_owned(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "example".to_owned(),
                non_retryable: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let initialize = core_activation::InitializeWorkflow {
        workflow_type: "workflow".to_owned(),
        workflow_id: "workflow-1".to_owned(),
        randomness_seed: 1,
        attempt: 1,
        first_execution_run_id: "first-run".to_owned(),
        continued_from_execution_run_id: "previous-run".to_owned(),
        continued_initiator: 1,
        continued_failure: Some(continuation_failure),
        last_completion_result: Some(Payloads {
            payloads: vec![Payload {
                data: b"last-result".to_vec(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let activation = core_activation::WorkflowActivation {
        run_id: "run-1".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::InitializeWorkflow(initialize)),
        }],
        ..Default::default()
    };

    let semantic = workflow_protocol::activation_from_core(&activation)
        .expect("continuation terminal metadata should convert");
    let continuation = match &semantic.jobs[0] {
        workflow_protocol::ActivationJob::InitializeWorkflow {
            context: Some(context),
            ..
        } => context
            .continuation
            .as_ref()
            .expect("continuation context")
            .clone(),
        _ => panic!("expected initialize workflow job"),
    };
    assert_eq!(
        continuation.initiator,
        workflow_protocol::ContinueAsNewInitiator::Workflow
    );
    assert_eq!(
        continuation
            .continued_failure
            .as_ref()
            .expect("continuation failure")
            .message,
        "previous run failed"
    );
    assert_eq!(
        continuation
            .last_completion_result
            .as_ref()
            .expect("last completion result")[0]
            .data,
        b"last-result"
    );
    let encoded = workflow_protocol::encode_activation(&semantic)
        .expect("continuation metadata should encode");
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );
}

/// Proves a child activation retains both its namespaced parent and the root
/// workflow execution instead of treating normal child metadata as unsupported.
#[test]
fn preserves_child_workflow_identity() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::{
        coresdk::common::NamespacedWorkflowExecution, temporal::api::common::v1::WorkflowExecution,
    };

    let child = core_activation::InitializeWorkflow {
        workflow_type: "child_workflow".to_owned(),
        workflow_id: "child-1".to_owned(),
        randomness_seed: 42,
        parent_workflow_info: Some(NamespacedWorkflowExecution {
            namespace: "default".to_owned(),
            workflow_id: "parent-1".to_owned(),
            run_id: "parent-run".to_owned(),
        }),
        first_execution_run_id: "child-first-run".to_owned(),
        attempt: 1,
        root_workflow: Some(WorkflowExecution {
            workflow_id: "root-1".to_owned(),
            run_id: "root-run".to_owned(),
        }),
        retry_policy: Some(valid_core_retry_policy()),
        ..Default::default()
    };
    let core = core_activation::WorkflowActivation {
        run_id: "child-run".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::InitializeWorkflow(child)),
        }],
        ..Default::default()
    };
    let semantic = workflow_protocol::activation_from_core(&core).unwrap();
    let context = match &semantic.jobs[0] {
        workflow_protocol::ActivationJob::InitializeWorkflow {
            context: Some(context),
            ..
        } => context,
        _ => panic!("child activation must retain initialization context"),
    };
    assert_eq!(
        context.parent_workflow.as_ref().unwrap().namespace,
        "default"
    );
    assert_eq!(
        context.root_workflow.as_ref().unwrap().workflow_id,
        "root-1"
    );
    assert_eq!(
        context
            .retry_policy
            .as_ref()
            .expect("child retry policy should cross the bridge")
            .maximum_attempts,
        2
    );
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );
}

/// Proves Core's two child-workflow activation events remain separate semantic
/// jobs.  The start acknowledgment carries only the run ID; the later result
/// carries the payload or typed child failure.  Keeping both events in source
/// order is required for the OCaml future store to distinguish a start failure
/// from a child that is still running.
#[test]
fn converts_child_workflow_resolution_lifecycle() {
    use core_activation::resolve_child_workflow_execution_start::Status as StartStatus;
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::coresdk::{child_workflow, workflow_activation};
    use temporalio_protos::temporal::api::{
        common::v1 as api_common, enums::v1 as api_enums, failure::v1 as api_failure,
    };
    use workflow_activation::{
        ResolveChildWorkflowExecution, ResolveChildWorkflowExecutionStart,
        ResolveChildWorkflowExecutionStartSuccess,
    };

    let payload = api_common::Payload {
        metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
        data: b"child-result".to_vec(),
        external_payloads: Vec::new(),
    };
    let child_failure = api_failure::Failure {
        message: "child failed".to_owned(),
        source: "server".to_owned(),
        stack_trace: String::new(),
        encoded_attributes: None,
        cause: None,
        failure_info: Some(
            api_failure::failure::FailureInfo::ChildWorkflowExecutionFailureInfo(
                api_failure::ChildWorkflowExecutionFailureInfo {
                    namespace: "default".to_owned(),
                    workflow_execution: Some(api_common::WorkflowExecution {
                        workflow_id: "child-42".to_owned(),
                        run_id: "child-run-42".to_owned(),
                    }),
                    workflow_type: Some(api_common::WorkflowType {
                        name: "child_workflow".to_owned(),
                    }),
                    initiated_event_id: 101,
                    started_event_id: 102,
                    retry_state: api_enums::RetryState::Timeout as i32,
                },
            ),
        ),
    };
    let core = workflow_activation::WorkflowActivation {
        run_id: "parent-run".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![
            workflow_activation::WorkflowActivationJob {
                variant: Some(Variant::ResolveChildWorkflowExecutionStart(
                    ResolveChildWorkflowExecutionStart {
                        seq: 7,
                        status: Some(StartStatus::Succeeded(
                            ResolveChildWorkflowExecutionStartSuccess {
                                run_id: "child-run-42".to_owned(),
                            },
                        )),
                    },
                )),
            },
            workflow_activation::WorkflowActivationJob {
                variant: Some(Variant::ResolveChildWorkflowExecution(
                    ResolveChildWorkflowExecution {
                        seq: 7,
                        result: Some(child_workflow::ChildWorkflowResult {
                            status: Some(child_workflow::child_workflow_result::Status::Completed(
                                child_workflow::Success {
                                    result: Some(payload.clone()),
                                },
                            )),
                        }),
                    },
                )),
            },
        ],
        ..Default::default()
    };

    let semantic = workflow_protocol::activation_from_core(&core).unwrap();
    assert!(matches!(
        semantic.jobs.as_slice(),
        [
            workflow_protocol::ActivationJob::ResolveChildWorkflowStart {
                seq: 7,
                result: workflow_protocol::ChildWorkflowStartResolution::Succeeded {
                    run_id
                }
            },
            workflow_protocol::ActivationJob::ResolveChildWorkflow {
                seq: 7,
                result: workflow_protocol::ChildWorkflowResolution::Completed {
                    payload: Some(_)
                }
            }
        ] if run_id == "child-run-42"
    ));
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );

    // A child failure uses the dedicated Temporal failure-info variant and
    // keeps every identity/event field through the same activation conversion.
    let failed = workflow_activation::WorkflowActivation {
        run_id: "parent-run".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![workflow_activation::WorkflowActivationJob {
            variant: Some(Variant::ResolveChildWorkflowExecution(
                ResolveChildWorkflowExecution {
                    seq: 8,
                    result: Some(child_workflow::ChildWorkflowResult {
                        status: Some(child_workflow::child_workflow_result::Status::Failed(
                            child_workflow::Failure {
                                failure: Some(child_failure),
                            },
                        )),
                    }),
                },
            )),
        }],
        ..Default::default()
    };
    let failed = workflow_protocol::activation_from_core(&failed).unwrap();
    match &failed.jobs[0] {
        workflow_protocol::ActivationJob::ResolveChildWorkflow {
            result: workflow_protocol::ChildWorkflowResolution::Failed { failure },
            ..
        } => match &failure.info {
            workflow_protocol::FailureInfo::ChildWorkflow {
                namespace,
                workflow_id,
                run_id,
                workflow_type,
                initiated_event_id,
                started_event_id,
                retry_state,
            } => {
                assert_eq!(namespace, "default");
                assert_eq!(workflow_id, "child-42");
                assert_eq!(run_id, "child-run-42");
                assert_eq!(workflow_type, "child_workflow");
                assert_eq!((*initiated_event_id, *started_event_id), (101, 102));
                assert_eq!(*retry_state, workflow_protocol::RetryState::Timeout);
            }
            info => panic!("unexpected child failure info: {info:?}"),
        },
        job => panic!("unexpected failed child job: {job:?}"),
    }
}

/// Proves Core timeout failure metadata survives activation conversion and
/// workflow-completion conversion. An absent Core heartbeat-details field is
/// intentionally normalized to the empty semantic list.
#[test]
fn converts_timeout_failure_info_losslessly() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::coresdk::{activity_result, workflow_activation};
    use temporalio_protos::temporal::api::{
        common::v1 as api_common, enums::v1 as api_enums, failure::v1 as api_failure,
    };

    let heartbeat_payload = api_common::Payload {
        metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
        data: b"heartbeat".to_vec(),
        external_payloads: Vec::new(),
    };
    let timeout_failure =
        |timeout_type: i32, details: Option<api_common::Payloads>| api_failure::Failure {
            message: "activity timed out".to_owned(),
            source: "server".to_owned(),
            stack_trace: String::new(),
            encoded_attributes: None,
            cause: None,
            failure_info: Some(api_failure::failure::FailureInfo::TimeoutFailureInfo(
                api_failure::TimeoutFailureInfo {
                    timeout_type,
                    last_heartbeat_details: details,
                },
            )),
        };
    let activation_for = |failure: api_failure::Failure| core_activation::WorkflowActivation {
        run_id: "parent-run".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::ResolveActivity(
                workflow_activation::ResolveActivity {
                    seq: 11,
                    result: Some(activity_result::ActivityResolution {
                        status: Some(activity_result::activity_resolution::Status::Failed(
                            activity_result::Failure {
                                failure: Some(failure),
                            },
                        )),
                    }),
                    is_local: false,
                },
            )),
        }],
        ..Default::default()
    };

    let with_details = activation_for(timeout_failure(
        api_enums::TimeoutType::Heartbeat as i32,
        Some(api_common::Payloads {
            payloads: vec![heartbeat_payload.clone()],
        }),
    ));
    let semantic = workflow_protocol::activation_from_core(&with_details).unwrap();
    match &semantic.jobs[0] {
        workflow_protocol::ActivationJob::ResolveActivity {
            result: workflow_protocol::ActivityResolution::Failed { failure },
            ..
        } => match &failure.info {
            workflow_protocol::FailureInfo::Timeout {
                timeout_type,
                last_heartbeat_details,
            } => {
                assert_eq!(*timeout_type, workflow_protocol::TimeoutType::Heartbeat);
                assert_eq!(last_heartbeat_details.len(), 1);
                assert_eq!(last_heartbeat_details[0].data, b"heartbeat");
            }
            info => panic!("unexpected timeout failure info: {info:?}"),
        },
        job => panic!("unexpected timeout activity job: {job:?}"),
    }
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );

    let absent = workflow_protocol::activation_from_core(&activation_for(timeout_failure(
        api_enums::TimeoutType::StartToClose as i32,
        None,
    )))
    .unwrap();
    match &absent.jobs[0] {
        workflow_protocol::ActivationJob::ResolveActivity {
            result: workflow_protocol::ActivityResolution::Failed { failure },
            ..
        } => match &failure.info {
            workflow_protocol::FailureInfo::Timeout {
                timeout_type,
                last_heartbeat_details,
            } => {
                assert_eq!(*timeout_type, workflow_protocol::TimeoutType::StartToClose);
                assert!(last_heartbeat_details.is_empty());
            }
            info => panic!("unexpected absent timeout info: {info:?}"),
        },
        job => panic!("unexpected absent timeout activity job: {job:?}"),
    }

    let semantic_failure = workflow_protocol::Failure {
        message: "activity timed out".to_owned(),
        source: "server".to_owned(),
        stack_trace: String::new(),
        encoded_attributes: None,
        cause: None,
        info: workflow_protocol::FailureInfo::Timeout {
            timeout_type: workflow_protocol::TimeoutType::Heartbeat,
            last_heartbeat_details: vec![workflow_protocol::Payload {
                metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
                data: b"heartbeat".to_vec(),
            }],
        },
    };
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "parent-run".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
            failure: semantic_failure.clone(),
        }],
    };
    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("timeout completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::FailWorkflowExecution(command)) =
        success.commands[0].variant.as_ref()
    else {
        panic!("timeout completion must map to Core's fail command");
    };
    let Some(api_failure::failure::FailureInfo::TimeoutFailureInfo(info)) = command
        .failure
        .as_ref()
        .and_then(|failure| failure.failure_info.as_ref())
    else {
        panic!("timeout completion lost its Core failure-info variant");
    };
    assert_eq!(info.timeout_type, api_enums::TimeoutType::Heartbeat as i32);
    assert_eq!(
        info.last_heartbeat_details
            .as_ref()
            .map(|details| details.payloads.len()),
        Some(1)
    );
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );

    let empty_completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "parent-run".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
            failure: workflow_protocol::Failure {
                info: workflow_protocol::FailureInfo::Timeout {
                    timeout_type: workflow_protocol::TimeoutType::StartToClose,
                    last_heartbeat_details: Vec::new(),
                },
                ..semantic_failure
            },
        }],
    };
    let empty_core = workflow_protocol::completion_to_core(&empty_completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        empty_core.status.as_ref()
    else {
        panic!("empty timeout completion must be successful");
    };
    let Some(core_commands::workflow_command::Variant::FailWorkflowExecution(command)) =
        success.commands[0].variant.as_ref()
    else {
        panic!("empty timeout completion must map to Core's fail command");
    };
    let Some(api_failure::failure::FailureInfo::TimeoutFailureInfo(info)) = command
        .failure
        .as_ref()
        .and_then(|failure| failure.failure_info.as_ref())
    else {
        panic!("empty timeout completion lost its Core failure-info variant");
    };
    assert!(info.last_heartbeat_details.is_none());
    assert_eq!(
        workflow_protocol::completion_from_core(&empty_core).unwrap(),
        empty_completion
    );

    let unknown = activation_for(timeout_failure(99, None));
    assert_eq!(
        workflow_protocol::activation_from_core(&unknown)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );
}

/// Converts Core's local-activity DoBackoff result and rejects the same result
/// when Core marks the activity as remote. This keeps the nonterminal retry
/// signal on the only lane where the language runtime can re-schedule it.
#[test]
fn converts_local_activity_backoff_losslessly() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::coresdk::{activity_result, workflow_activation};

    let activation = core_activation::WorkflowActivation {
        run_id: "local-run".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::ResolveActivity(
                workflow_activation::ResolveActivity {
                    seq: 7,
                    result: Some(activity_result::ActivityResolution {
                        status: Some(activity_result::activity_resolution::Status::Backoff(
                            activity_result::DoBackoff {
                                attempt: 2,
                                backoff_duration: Some(prost_wkt_types::Duration {
                                    seconds: 3,
                                    nanos: 400_000_000,
                                }),
                                original_schedule_time: Some(prost_wkt_types::Timestamp {
                                    seconds: 10,
                                    nanos: 20,
                                }),
                            },
                        )),
                    }),
                    is_local: true,
                },
            )),
        }],
        ..Default::default()
    };
    let semantic = workflow_protocol::activation_from_core(&activation).unwrap();
    assert_eq!(
        semantic.jobs[0],
        workflow_protocol::ActivationJob::ResolveActivity {
            seq: 7,
            result: workflow_protocol::ActivityResolution::Backoff {
                attempt: 2,
                backoff_duration: workflow_protocol::Duration {
                    seconds: 3,
                    nanoseconds: 400_000_000,
                },
                original_schedule_time: Some(workflow_protocol::Timestamp {
                    seconds: 10,
                    nanoseconds: 20,
                }),
            },
        }
    );
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );

    let mut remote = activation;
    let Some(Variant::ResolveActivity(job)) = remote.jobs[0].variant.as_mut() else {
        panic!("backoff test job must be ResolveActivity");
    };
    job.is_local = false;
    assert_eq!(
        workflow_protocol::activation_from_core(&remote)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::Unsupported
    );
}

/// Proves the pinned Core cancellation-before-start activation retains the
/// empty child run ID that Core emits before it has a
/// `ChildWorkflowExecutionStarted` event.  This exercises the protobuf
/// conversion path that previously rejected the live acceptance activation
/// before the semantic JSON document could be delivered to OCaml.
#[test]
fn accepts_core_child_cancellation_before_start_without_fabricating_run_id() {
    use core_activation::resolve_child_workflow_execution_start::Status as StartStatus;
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::coresdk::child_workflow;
    use temporalio_protos::coresdk::workflow_activation::{
        ResolveChildWorkflowExecution, ResolveChildWorkflowExecutionStart,
        ResolveChildWorkflowExecutionStartCancelled,
    };
    use temporalio_protos::temporal::api::{
        common::v1 as api_common, enums::v1 as api_enums, failure::v1 as api_failure,
    };

    let failure = api_failure::Failure {
        message: "Child Workflow Execution cancelled before scheduled".to_owned(),
        cause: Some(Box::new(api_failure::Failure {
            failure_info: Some(api_failure::failure::FailureInfo::CanceledFailureInfo(
                api_failure::CanceledFailureInfo::default(),
            )),
            ..Default::default()
        })),
        failure_info: Some(
            api_failure::failure::FailureInfo::ChildWorkflowExecutionFailureInfo(
                api_failure::ChildWorkflowExecutionFailureInfo {
                    namespace: "default".to_owned(),
                    workflow_execution: Some(api_common::WorkflowExecution {
                        workflow_id: "child-id-1".to_owned(),
                        run_id: String::new(),
                    }),
                    workflow_type: Some(api_common::WorkflowType {
                        name: "child".to_owned(),
                    }),
                    initiated_event_id: 1,
                    started_event_id: 0,
                    retry_state: api_enums::RetryState::NonRetryableFailure as i32,
                },
            ),
        ),
        ..Default::default()
    };
    let activation = core_activation::WorkflowActivation {
        run_id: "parent-run".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::ResolveChildWorkflowExecutionStart(
                ResolveChildWorkflowExecutionStart {
                    seq: 1,
                    status: Some(StartStatus::Cancelled(
                        ResolveChildWorkflowExecutionStartCancelled {
                            failure: Some(failure.clone()),
                        },
                    )),
                },
            )),
        }],
        ..Default::default()
    };

    let semantic = workflow_protocol::activation_from_core(&activation).unwrap();
    match &semantic.jobs[0] {
        workflow_protocol::ActivationJob::ResolveChildWorkflowStart {
            result: workflow_protocol::ChildWorkflowStartResolution::Cancelled { failure },
            ..
        } => match &failure.info {
            workflow_protocol::FailureInfo::ChildWorkflow {
                run_id,
                started_event_id,
                ..
            } => {
                assert!(run_id.is_empty());
                assert_eq!(*started_event_id, 0);
            }
            info => panic!("unexpected child failure info: {info:?}"),
        },
        job => panic!("unexpected child cancellation job: {job:?}"),
    }
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );

    // An empty run ID after the child-start event would be ambiguous and must
    // remain a protocol error rather than being accepted as another Core
    // cancellation shape.
    let mut invalid = activation;
    let Some(Variant::ResolveChildWorkflowExecutionStart(start)) = invalid.jobs[0].variant.as_mut()
    else {
        panic!("cancellation activation must contain a start resolution");
    };
    let Some(StartStatus::Cancelled(cancelled)) = start.status.as_mut() else {
        panic!("cancellation activation must contain a cancelled status");
    };
    cancelled
        .failure
        .as_mut()
        .expect("cancellation must contain a failure")
        .failure_info = Some(
        api_failure::failure::FailureInfo::ChildWorkflowExecutionFailureInfo(
            api_failure::ChildWorkflowExecutionFailureInfo {
                namespace: "default".to_owned(),
                workflow_execution: Some(api_common::WorkflowExecution {
                    workflow_id: "child-id-1".to_owned(),
                    run_id: String::new(),
                }),
                workflow_type: Some(api_common::WorkflowType {
                    name: "child".to_owned(),
                }),
                initiated_event_id: 2,
                started_event_id: 7,
                retry_state: api_enums::RetryState::NonRetryableFailure as i32,
            },
        ),
    );
    assert_eq!(
        workflow_protocol::activation_from_core(&invalid)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    // Core can also report the same pre-start cancellation through the
    // terminal child-resolution job for an immediate TryCancel/Abandon path.
    // Both protobuf variants must share the narrow empty-run-id exception.
    let terminal = core_activation::WorkflowActivation {
        run_id: "parent-run".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::ResolveChildWorkflowExecution(
                ResolveChildWorkflowExecution {
                    seq: 1,
                    result: Some(child_workflow::ChildWorkflowResult {
                        status: Some(child_workflow::child_workflow_result::Status::Cancelled(
                            child_workflow::Cancellation {
                                failure: Some(failure),
                            },
                        )),
                    }),
                },
            )),
        }],
        ..Default::default()
    };
    let terminal_semantic = workflow_protocol::activation_from_core(&terminal).unwrap();
    match &terminal_semantic.jobs[0] {
        workflow_protocol::ActivationJob::ResolveChildWorkflow {
            result: workflow_protocol::ChildWorkflowResolution::Cancelled { failure },
            ..
        } => match &failure.info {
            workflow_protocol::FailureInfo::ChildWorkflow {
                run_id,
                started_event_id,
                ..
            } => {
                assert!(run_id.is_empty());
                assert_eq!(*started_event_id, 0);
            }
            info => panic!("unexpected terminal child cancellation info: {info:?}"),
        },
        job => panic!("unexpected terminal child cancellation job: {job:?}"),
    }
}

/// Proves absent oneofs and the eviction acknowledgement rule fail closed.
#[test]
fn enforces_core_absence_and_eviction_invariants() {
    let invalid = core_activation::WorkflowActivation {
        run_id: "run-1".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob { variant: None }],
        ..Default::default()
    };
    assert_eq!(
        workflow_protocol::activation_from_core(&invalid)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let eviction =
        workflow_protocol::decode_activation(&fixture(&["valid", "eviction.input.json"])).unwrap();
    let nonempty = workflow_protocol::Completion {
        task_failure: None,
        run_id: eviction.run_id.clone(),
        commands: vec![workflow_protocol::CompletionCommand::CancelTimer { seq: 1 }],
    };
    assert_eq!(
        workflow_protocol::completion_to_core_for_activation(&eviction, &nonempty)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    // Core's cache-removal acknowledgement is a successful completion with
    // no commands or metadata; this is the only valid response after a
    // terminal run has already left the language worker's registry.
    let empty = workflow_protocol::Completion {
        task_failure: None,
        run_id: eviction.run_id.clone(),
        commands: Vec::new(),
    };
    let empty_core =
        workflow_protocol::completion_to_core_for_activation(&eviction, &empty).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        empty_core.status.as_ref()
    else {
        panic!("cache eviction acknowledgement must be successful");
    };
    assert!(success.commands.is_empty());
    assert!(success.used_internal_flags.is_empty());
    assert_eq!(success.versioning_behavior, 0);

    let absent_command = core_commands::WorkflowCommand {
        user_metadata: None,
        variant: None,
    };
    let core = core_completion::WorkflowActivationCompletion {
        run_id: "run-1".to_owned(),
        status: Some(
            core_completion::workflow_activation_completion::Status::Successful(
                core_completion::Success {
                    commands: vec![absent_command],
                    used_internal_flags: Vec::new(),
                    versioning_behavior: 0,
                },
            ),
        ),
    };
    assert_eq!(
        workflow_protocol::completion_from_core(&core)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );
}

/// Proves the official pinned-Core eviction constructor, including its absent
/// timestamp, crosses the semantic boundary without fabrication or rejection.
#[test]
fn converts_official_core_eviction_activation() {
    use temporalio_protos::coresdk::workflow_activation::{
        create_evict_activation, remove_from_cache::EvictionReason,
    };

    let core = create_evict_activation(
        "evicted-run".to_owned(),
        "cache is full".to_owned(),
        EvictionReason::CacheFull,
    );
    assert!(core.timestamp.is_none());
    let semantic = workflow_protocol::activation_from_core(&core).unwrap();
    assert!(semantic.timestamp.is_none());
    assert!(matches!(
        semantic.jobs.as_slice(),
        [workflow_protocol::ActivationJob::RemoveFromCache { .. }]
    ));
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );
}

/// Proves maximum payload representation does not weaken ordinary text limits.
#[test]
fn accepts_large_nested_payload_but_rejects_large_text() {
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-large".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::CompleteWorkflow {
            result: Some(workflow_protocol::Payload {
                metadata: BTreeMap::new(),
                data: vec![b'x'; 50_000],
            }),
        }],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );

    let oversized = "x".repeat(65_537);
    let activation = serde_json::json!({
        "run_id": "r",
        "timestamp": {"seconds": 0, "nanoseconds": 0},
        "is_replaying": false,
        "history_length": 0,
        "jobs": [{"kind": "cancel_workflow", "reason": oversized}]
    });
    assert!(workflow_protocol::decode_activation(&activation.to_string()).is_err());
}

/// Proves unordered input maps normalize identically in both implementations.
#[test]
fn canonicalizes_payload_metadata_keys() {
    let input = r#"{"run_id":"run-map","commands":[{"kind":"complete_workflow","result":{"metadata":{"z-key":{"encoding":"base64","data":"eg=="},"a-key":{"encoding":"base64","data":"YQ=="}},"data":{"encoding":"base64","data":""}}}]}"#;
    let value = workflow_protocol::decode_completion(input).unwrap();
    let output = workflow_protocol::encode_completion(&value).unwrap();
    assert!(output.find("\"a-key\"").unwrap() < output.find("\"z-key\"").unwrap());
}

/// Proves configurable Temporal identifiers can exceed the server's default
/// limit while still obeying the bridge's bounded-string safety policy.
#[test]
fn preserves_identifiers_above_255_bytes() {
    let long_id = "i".repeat(300);
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: long_id.clone(),
        commands: Vec::new(),
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert_eq!(
        workflow_protocol::decode_completion(&encoded)
            .unwrap()
            .run_id,
        long_id
    );

    let oversized = workflow_protocol::Completion {
        task_failure: None,
        run_id: "i".repeat(65_537),
        commands: Vec::new(),
    };
    assert!(workflow_protocol::encode_completion(&oversized).is_err());
}

/// Proves initialization is unique and first, and a missing timestamp cannot
/// escape the single synthetic-eviction exception.
#[test]
fn enforces_activation_cross_field_invariants() {
    let mut initialized = workflow_protocol::decode_activation(&fixture(&[
        "valid",
        "realistic-initialize.input.json",
    ]))
    .unwrap();
    let initialize = initialized.jobs[0].clone();
    initialized.jobs.push(initialize.clone());
    assert!(workflow_protocol::encode_activation(&initialized).is_err());

    initialized.jobs = vec![
        workflow_protocol::ActivationJob::FireTimer { seq: 0 },
        initialize,
    ];
    assert!(workflow_protocol::encode_activation(&initialized).is_err());

    initialized.jobs.truncate(1);
    initialized.timestamp = None;
    assert!(workflow_protocol::encode_activation(&initialized).is_err());

    initialized.timestamp = Some(workflow_protocol::Timestamp {
        seconds: 0,
        nanoseconds: 0,
    });
    initialized.jobs = vec![workflow_protocol::ActivationJob::FireTimer { seq: 0 }];
    let encoded = workflow_protocol::encode_activation(&initialized).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap().jobs,
        initialized.jobs
    );
}

/// Proves collection accounting does not impose a smaller workflow-job limit
/// than the aggregate document boundary itself.
#[test]
fn accepts_more_than_256_small_activation_jobs() {
    let mut activation = workflow_protocol::decode_activation(&fixture(&[
        "valid",
        "realistic-initialize.input.json",
    ]))
    .unwrap();
    activation.jobs = (0..300)
        .map(|seq| workflow_protocol::ActivationJob::FireTimer { seq })
        .collect();
    let encoded = workflow_protocol::encode_activation(&activation).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        activation
    );
}

/// Builds a failure whose nested info can be varied without obscuring the
/// boundary invariant under test.
fn failure_with_info(info: workflow_protocol::FailureInfo) -> workflow_protocol::Failure {
    workflow_protocol::Failure {
        message: String::new(),
        source: String::new(),
        stack_trace: String::new(),
        encoded_attributes: None,
        cause: None,
        info,
    }
}

/// Builds a recursive application-failure chain used to exercise the shared
/// parser's stack-safety depth boundary.
fn nested_application_failure(cause_count: usize) -> workflow_protocol::Failure {
    (0..cause_count).fold(
        failure_with_info(workflow_protocol::FailureInfo::Application {
            type_name: String::new(),
            non_retryable: false,
            details: Vec::new(),
        }),
        |cause, _| workflow_protocol::Failure {
            cause: Some(Box::new(cause)),
            ..failure_with_info(workflow_protocol::FailureInfo::Application {
                type_name: String::new(),
                non_retryable: false,
                details: Vec::new(),
            })
        },
    )
}

/// Proves recursive failures can exceed the former 16-level limit while the
/// serde_json-aligned stack-safety boundary still rejects hostile depth.
#[test]
fn enforces_recursive_failure_depth_safely() {
    let completion = |cause_count| workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-nested-failure".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
            failure: nested_application_failure(cause_count),
        }],
    };
    workflow_protocol::encode_completion(&completion(32))
        .expect("32 recursive causes must fit the parser safety boundary");
    assert!(workflow_protocol::encode_completion(&completion(130)).is_err());
}

/// Proves application failure type follows the schema's bounded-text contract
/// and is not incorrectly treated as a nonempty Temporal identifier.
#[test]
fn accepts_application_failure_type_as_bounded_text() {
    let application = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-failure".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
            failure: failure_with_info(workflow_protocol::FailureInfo::Application {
                type_name: String::new(),
                non_retryable: false,
                details: Vec::new(),
            }),
        }],
    };
    workflow_protocol::encode_completion(&application)
        .expect("an empty application failure type is valid bounded text");
}

/// Proves activity failure event IDs and worker identity are validated after
/// payload-aware parsing has admitted the larger base64 string ceiling.
#[test]
fn rejects_invalid_activity_failure_fields() {
    let invalid_activity =
        |scheduled_event_id, started_event_id, identity: String| workflow_protocol::Completion {
            task_failure: None,
            run_id: "run-failure".to_owned(),
            commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
                failure: failure_with_info(workflow_protocol::FailureInfo::Activity {
                    scheduled_event_id,
                    started_event_id,
                    identity,
                    activity_type: "activity".to_owned(),
                    activity_id: "activity-1".to_owned(),
                    retry_state: workflow_protocol::RetryState::Unspecified,
                }),
            }],
        };
    assert!(workflow_protocol::encode_completion(&invalid_activity(-1, 0, String::new())).is_err());
    assert!(workflow_protocol::encode_completion(&invalid_activity(0, -1, String::new())).is_err());
    assert!(
        workflow_protocol::encode_completion(&invalid_activity(0, 0, "i".repeat(65_537))).is_err()
    );
}

/// Proves relaxed payload parsing cannot bypass the normal header-key limit.
#[test]
fn validates_initialize_header_keys() {
    let mut activation = workflow_protocol::decode_activation(&fixture(&[
        "valid",
        "realistic-initialize.input.json",
    ]))
    .unwrap();
    let workflow_protocol::ActivationJob::InitializeWorkflow {
        context: Some(context),
        ..
    } = &mut activation.jobs[0]
    else {
        panic!("fixture must contain initialization context");
    };
    context.headers.insert(
        String::new(),
        workflow_protocol::Payload {
            metadata: BTreeMap::new(),
            data: Vec::new(),
        },
    );
    assert!(workflow_protocol::encode_activation(&activation).is_err());
}

/// Proves two server-default-sized payload byte fields, and their canonical
/// base64 expansion, fit in one validated semantic document.
#[test]
fn accepts_batched_default_temporal_payloads() {
    const DEFAULT_TEMPORAL_BLOB_BYTES: usize = 2 * 1024 * 1024;
    let bytes = vec![b'x'; DEFAULT_TEMPORAL_BLOB_BYTES];
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-batched-payloads".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::CompleteWorkflow {
            result: Some(workflow_protocol::Payload {
                metadata: [("second".to_owned(), bytes.clone())].into(),
                data: bytes,
            }),
        }],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );
}

/// Removes one member from a fixture object selected by JSON Pointer.
fn fixture_without_field(document: &str, object_pointer: &str, field: &str) -> String {
    let mut value: serde_json::Value = serde_json::from_str(document).unwrap();
    value
        .pointer_mut(object_pointer)
        .and_then(serde_json::Value::as_object_mut)
        .expect("test pointer must select an object")
        .remove(field)
        .expect("test field must exist");
    serde_json::to_string(&value).unwrap()
}

/// Proves every schema-required nullable field must be explicitly present;
/// omission cannot be silently normalized into JSON null by Serde.
#[test]
fn rejects_omitted_required_nullable_fields() {
    let completion = fixture(&["valid", "completion.input.json"]);
    for field in [
        "schedule_to_close_timeout",
        "schedule_to_start_timeout",
        "start_to_close_timeout",
        "heartbeat_timeout",
    ] {
        assert!(
            workflow_protocol::decode_completion(&fixture_without_field(
                &completion,
                "/commands/0",
                field,
            ))
            .is_err(),
            "schedule_activity.{field} omission must fail",
        );
    }
    assert!(
        workflow_protocol::decode_completion(&fixture_without_field(
            &completion,
            "/commands/4",
            "result",
        ))
        .is_err()
    );

    let activation = fixture(&["valid", "activation.input.json"]);
    for (pointer, field) in [
        ("", "timestamp"),
        ("/jobs/1/result", "payload"),
        ("/jobs/2/result/failure", "encoded_attributes"),
        ("/jobs/2/result/failure", "cause"),
    ] {
        assert!(
            workflow_protocol::decode_activation(&fixture_without_field(
                &activation,
                pointer,
                field,
            ))
            .is_err(),
            "activation field {pointer}/{field} omission must fail",
        );
    }

    let initialize = fixture(&["valid", "realistic-initialize.input.json"]);
    for field in [
        "parent_workflow",
        "workflow_execution_timeout",
        "workflow_run_timeout",
        "workflow_task_timeout",
        "start_time",
        "root_workflow",
        "priority",
        "continuation",
    ] {
        assert!(
            workflow_protocol::decode_activation(&fixture_without_field(
                &initialize,
                "/jobs/0/context",
                field,
            ))
            .is_err(),
            "initialize context field {field} omission must fail",
        );
    }
    assert!(
        workflow_protocol::decode_activation(&fixture_without_field(
            &initialize,
            "/metadata",
            "deployment_version_for_current_task",
        ))
        .is_err()
    );
}

/// The bilateral fixture represents Core's failed status, never a successful
/// FailWorkflowExecution command. A poisoned activation cannot leak commands,
/// and query/eviction leases retain their operation-specific acknowledgements.
#[test]
fn task_failure_preserves_the_workflow_execution() {
    let completion =
        workflow_protocol::decode_completion(&fixture(&["valid", "task-failure.input.json"]))
            .unwrap();
    assert_eq!(
        workflow_protocol::encode_completion(&completion).unwrap(),
        fixture(&["valid", "task-failure.normalized.json"]).trim()
    );
    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    let Some(core_completion::workflow_activation_completion::Status::Failed(failed)) =
        core.status.as_ref()
    else {
        panic!("expected failed workflow task");
    };
    assert_eq!(failed.force_cause,
        i32::from(temporalio_protos::temporal::api::enums::v1::WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure));
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );
    assert!(
        workflow_protocol::decode_completion(&fixture(&[
            "invalid",
            "task-failure-with-commands.json",
        ]))
        .is_err()
    );
    let mut activation =
        workflow_protocol::decode_activation(&fixture(&["valid", "eviction.input.json"])).unwrap();
    activation.run_id.clone_from(&completion.run_id);
    assert!(
        workflow_protocol::completion_to_core_for_activation(&activation, &completion).is_err()
    );
    activation.jobs = vec![workflow_protocol::ActivationJob::QueryWorkflow {
        query_id: "query-id".into(),
        query_type: "status".into(),
        arguments: vec![],
        headers: BTreeMap::new(),
    }];
    assert!(
        workflow_protocol::completion_to_core_for_activation(&activation, &completion).is_err()
    );
    let failure = completion.task_failure.clone().unwrap();
    let invalid = workflow_protocol::Completion {
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow { failure }],
        ..completion
    };
    assert!(workflow_protocol::completion_to_core(&invalid).is_err());
}

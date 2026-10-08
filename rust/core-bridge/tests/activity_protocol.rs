use ocaml_temporal_core_bridge::activity_protocol::{
    ActivityCancel, ActivityCancelReason, ActivityCompletion, ActivityCompletionResult,
    ActivityHeartbeat, ActivityTask, ActivityTaskVariant, completion_to_core, decode_completion,
    decode_heartbeat, decode_task, encode_completion, encode_heartbeat, encode_task,
    task_from_core,
};
use ocaml_temporal_core_bridge::workflow_protocol::Payload;
use std::collections::BTreeMap;
use temporalio_protos::coresdk::activity_result as core_activity_result;
use temporalio_protos::coresdk::activity_task as core_activity_task;

/// A complete start-task fixture exercises every nullable field, the retry
/// policy, priority, headers, and both payload collection shapes.
const VALID_START_TASK: &str = r#"{"task_token":"AAEC/v8=","variant":{"kind":"start","workflow_namespace":"default","workflow_type":"example.workflow","workflow_execution":{"workflow_id":"workflow-1","run_id":"run-1"},"activity_id":"activity-1","activity_type":"example.activity","is_local":false,"header_fields":{"trace":{"metadata":{},"data":{"encoding":"base64","data":"aGVhZGVy"}}},"input":[{"metadata":{"encoding":{"encoding":"base64","data":"anNvbi9wbGFpbg=="}},"data":{"encoding":"base64","data":"eyJ2YWx1ZSI6MX0="}}],"heartbeat_details":[],"scheduled_time":{"seconds":10,"nanoseconds":20},"current_attempt_scheduled_time":null,"started_time":{"seconds":11,"nanoseconds":0},"attempt":1,"schedule_to_close_timeout":{"seconds":60,"nanoseconds":0},"start_to_close_timeout":{"seconds":30,"nanoseconds":0},"heartbeat_timeout":null,"retry_policy":{"initial_interval":{"seconds":1,"nanoseconds":0},"backoff_coefficient_bits":"4611686018427387904","maximum_interval":{"seconds":60,"nanoseconds":0},"maximum_attempts":3,"non_retryable_error_types":["InvalidInput"]},"priority":{"priority_key":2,"fairness_key":"tenant","fairness_weight_bits":1065353216},"standalone_run_id":""}}"#;

/// Replaces exactly one fixture fragment so each malformed document targets a
/// single semantic rule and fails loudly if the fixture is later reshaped.
fn replace_once(source: &str, before: &str, after: &str) -> String {
    let offset = source
        .find(before)
        .unwrap_or_else(|| panic!("missing test fragment {before}"));
    let mut output = String::with_capacity(source.len() + after.len() - before.len());
    output.push_str(&source[..offset]);
    output.push_str(after);
    output.push_str(&source[offset + before.len()..]);
    output
}

/// Activity task tokens survive canonical JSON and return to Core unchanged.
#[test]
fn activity_completion_preserves_opaque_task_token() {
    let completion = ActivityCompletion {
        task_token: "AAEC/v8=".to_owned(),
        result: ActivityCompletionResult::Completed {
            result: Payload {
                metadata: BTreeMap::new(),
                data: Vec::new(),
            },
        },
    };

    let encoded = encode_completion(&completion).expect("completion should encode");
    assert_eq!(decode_completion(&encoded), Ok(completion.clone()));
    assert_eq!(
        completion_to_core(&completion)
            .expect("completion should convert")
            .task_token,
        vec![0, 1, 2, 254, 255]
    );
}

/// The semantic activity decoder rejects unknown fields and non-canonical or
/// empty tokens before any completion can reach Core.
#[test]
fn activity_documents_are_closed_and_tokens_are_canonical() {
    assert!(
        decode_completion(
            r#"{"task_token":"AA==","result":{"kind":"will_complete_async"},"extra":true}"#
        )
        .is_err()
    );
    assert!(
        decode_completion(r#"{"task_token":"","result":{"kind":"will_complete_async"}}"#).is_err()
    );
    assert!(
        decode_completion(r#"{"task_token":"AA","result":{"kind":"will_complete_async"}}"#)
            .is_err()
    );
}

/// The tagged completion-result enum is itself closed, not just the wrapping
/// `ActivityCompletion` struct: an unknown member nested inside a `result`
/// variant's fields must fail closed the same way it does for every
/// analogous workflow-side tagged enum, instead of being silently dropped by
/// serde's default behavior.
#[test]
fn activity_completion_result_variants_reject_unknown_fields() {
    assert!(
        decode_completion(
            r#"{"task_token":"AA==","result":{"kind":"completed","result":{"metadata":{},"data":{"encoding":"base64","data":""}},"injected":true}}"#
        )
        .is_err()
    );
}

/// A successful completion must carry a payload (issue #954).
///
/// Core's `validate_activity_completion` rejects `Success { result: None }`
/// as malformed, so a `null` or absent payload is refused at decode, before
/// any Core call. A void result is an encoded payload with empty data, and
/// every decoded success converts to a Core result that has a payload.
#[test]
fn activity_completed_result_requires_a_payload() {
    let null =
        decode_completion(r#"{"task_token":"AA==","result":{"kind":"completed","result":null}}"#)
            .expect_err("a null payload must be rejected");
    assert_eq!(null.code, "invalid_message");
    assert_eq!(null.path, "$.result.result");
    assert!(
        decode_completion(r#"{"task_token":"AA==","result":{"kind":"completed"}}"#).is_err(),
        "an absent payload must be rejected"
    );

    let void = decode_completion(
        r#"{"task_token":"AA==","result":{"kind":"completed","result":{"metadata":{},"data":{"encoding":"base64","data":""}}}}"#,
    )
    .expect("an empty payload is a valid void result");
    let core = completion_to_core(&void).expect("void completion converts");
    let status = core
        .result
        .and_then(|result| result.status)
        .expect("conversion sets a status");
    match status {
        core_activity_result::activity_execution_result::Status::Completed(success) => {
            let payload = success.result.expect("Core receives a result payload");
            assert!(payload.data.is_empty());
            assert!(payload.metadata.is_empty());
        }
        other => panic!("void completion converted to {other:?}"),
    }
}

/// Heartbeats preserve the opaque token and binary detail payloads while
/// keeping the JSON object closed for forward-compatible validation.
#[test]
fn activity_heartbeat_round_trips_binary_details() {
    let mut metadata = BTreeMap::new();
    metadata.insert("encoding".to_owned(), b"binary/plain".to_vec());
    let heartbeat = ActivityHeartbeat {
        task_token: "AAEC/v8=".to_owned(),
        details: vec![Payload {
            metadata,
            data: vec![0, 1, 2, 254, 255],
        }],
    };

    let encoded = encode_heartbeat(&heartbeat).expect("heartbeat should encode");
    assert_eq!(decode_heartbeat(&encoded), Ok(heartbeat));
    assert!(encoded.contains("\"task_token\":\"AAEC/v8=\""));
    assert!(encoded.contains("\"details\""));
}

/// Proves that heartbeat details are copied into owned Rust values instead of
/// borrowing the temporary JSON input. The bridge must be able to retain the
/// decoded payload until Core consumes it after the parser's input buffer has
/// been released.
#[test]
fn activity_heartbeat_owns_input_after_decode() {
    let input = String::from(
        r#"{"task_token":"AAEC/v8=","details":[{"metadata":{"encoding":{"encoding":"base64","data":"YmluYXJ5L3BsYWlu"}},"data":{"encoding":"base64","data":"AAEC/v8="}}]}"#,
    );

    let heartbeat = decode_heartbeat(&input).expect("heartbeat should decode");
    drop(input);

    assert_eq!(heartbeat.task_token, "AAEC/v8=");
    assert_eq!(heartbeat.details.len(), 1);
    assert_eq!(heartbeat.details[0].data, vec![0, 1, 2, 254, 255]);
    assert_eq!(
        heartbeat.details[0]
            .metadata
            .get("encoding")
            .expect("encoding metadata should survive input drop"),
        &b"binary/plain".to_vec()
    );
}

/// The heartbeat decoder rejects unknown fields, empty/non-canonical tokens,
/// and payload encodings that are not the canonical binary representation.
#[test]
fn activity_heartbeat_rejects_unsafe_documents() {
    assert!(decode_heartbeat(r#"{"task_token":"AAEC/v8=","details":[],"extra":true}"#).is_err());
    assert!(decode_heartbeat(r#"{"task_token":"","details":[]}"#).is_err());
    assert!(decode_heartbeat(r#"{"task_token":"AAE","details":[]}"#).is_err());
    assert!(decode_heartbeat(
        r#"{"task_token":"AAEC/v8=","details":[{"metadata":{},"data":{"encoding":"raw","data":"AA=="}}]}"#
    )
    .is_err());
}

/// Cancellation tasks retain the stable reason while preserving the single
/// original task-token completion obligation.
#[test]
fn activity_cancellation_task_round_trips() {
    let task = ActivityTask {
        task_token: "dG9rZW4=".to_owned(),
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::WorkerShutdown,
            details: None,
        }),
    };

    let encoded = encode_task(&task).expect("task should encode");
    assert_eq!(decode_task(&encoded), Ok(task));
}

/// Core reports the primary cancellation reason and the independent facts in
/// one asynchronous `ActivityTask::Cancel`.  This fixture exercises each fact
/// that can be set by a heartbeat response, then crosses the exact Core -> Rust
/// conversion and the Rust -> OCaml JSON handoff.  Keeping the assertions on
/// the typed value (rather than only matching JSON text) catches field swaps or
/// accidental derivation of one flag from another.
#[test]
fn activity_cancellation_details_survive_core_to_json_round_trip() {
    let cases = [
        (
            core_activity_task::ActivityCancelReason::Cancelled,
            ActivityCancelReason::Cancelled,
            core_activity_task::ActivityCancellationDetails {
                is_not_found: false,
                is_cancelled: true,
                is_paused: false,
                is_timed_out: false,
                is_worker_shutdown: false,
                is_reset: false,
            },
        ),
        (
            core_activity_task::ActivityCancelReason::Paused,
            ActivityCancelReason::Paused,
            core_activity_task::ActivityCancellationDetails {
                is_not_found: false,
                is_cancelled: false,
                is_paused: true,
                is_timed_out: false,
                is_worker_shutdown: false,
                is_reset: false,
            },
        ),
        (
            core_activity_task::ActivityCancelReason::Reset,
            ActivityCancelReason::Reset,
            core_activity_task::ActivityCancellationDetails {
                is_not_found: false,
                is_cancelled: false,
                is_paused: false,
                is_timed_out: false,
                is_worker_shutdown: false,
                is_reset: true,
            },
        ),
        (
            core_activity_task::ActivityCancelReason::Cancelled,
            ActivityCancelReason::Cancelled,
            core_activity_task::ActivityCancellationDetails {
                is_not_found: false,
                is_cancelled: true,
                is_paused: true,
                is_timed_out: false,
                is_worker_shutdown: false,
                is_reset: true,
            },
        ),
    ];

    for (reason, expected_reason, details) in cases {
        let core = core_activity_task::ActivityTask {
            task_token: vec![0, 1, 2, 254, 255],
            variant: Some(core_activity_task::activity_task::Variant::Cancel(
                core_activity_task::Cancel {
                    reason: reason as i32,
                    details: Some(details),
                },
            )),
        };
        let expected_details =
            ocaml_temporal_core_bridge::activity_protocol::ActivityCancellationDetails {
                is_not_found: details.is_not_found,
                is_cancelled: details.is_cancelled,
                is_paused: details.is_paused,
                is_timed_out: details.is_timed_out,
                is_worker_shutdown: details.is_worker_shutdown,
                is_reset: details.is_reset,
            };

        let semantic = task_from_core(&core).expect("Core cancellation should convert");
        let ActivityTaskVariant::Cancel(cancel) = &semantic.variant else {
            panic!("Core cancellation should remain a Cancel task");
        };
        assert_eq!(cancel.reason, expected_reason);
        assert_eq!(cancel.details, Some(expected_details));

        let encoded = encode_task(&semantic).expect("cancellation task should encode");
        assert_eq!(decode_task(&encoded), Ok(semantic));
    }
}

/// A fully populated start task can cross both directions without changing
/// the typed value, including exact retry-policy bit strings and payloads.
#[test]
fn activity_start_task_round_trips() {
    let task = decode_task(VALID_START_TASK).expect("valid start task should decode");
    let encoded = encode_task(&task).expect("valid start task should encode");
    assert_eq!(decode_task(&encoded), Ok(task));
}

/// Core's locality bit must survive the complete task handoff so the OCaml
/// adapter can reject a deferred local result before it reaches Core.
#[test]
fn activity_start_preserves_core_locality_through_json() {
    for is_local in [false, true] {
        let core = core_activity_task::ActivityTask {
            task_token: vec![0, 1, 2, 254, 255],
            variant: Some(core_activity_task::activity_task::Variant::Start(
                core_activity_task::Start {
                    workflow_namespace: "default".to_owned(),
                    workflow_type: "example.workflow".to_owned(),
                    workflow_execution: Some(
                        temporalio_common::protos::temporal::api::common::v1::WorkflowExecution {
                            workflow_id: "workflow-1".to_owned(),
                            run_id: "run-1".to_owned(),
                        },
                    ),
                    activity_id: "activity-1".to_owned(),
                    activity_type: "example.activity".to_owned(),
                    is_local,
                    ..Default::default()
                },
            )),
        };

        let semantic = task_from_core(&core).expect("Core start should convert");
        let ActivityTaskVariant::Start(start) = &semantic.variant else {
            panic!("Core start must remain a start task");
        };
        assert_eq!(start.is_local, is_local);

        let encoded = encode_task(&semantic).expect("start should encode");
        assert!(encoded.contains(&format!("\"is_local\":{is_local}")));
        assert_eq!(decode_task(&encoded), Ok(semantic));
    }
}

/// Locality is required, not an optional hint that could silently turn a
/// local activity into a remote one when a task document is malformed.
#[test]
fn activity_start_requires_boolean_locality() {
    let missing = replace_once(VALID_START_TASK, "\"is_local\":false,", "");
    assert!(decode_task(&missing).is_err());

    let non_boolean = replace_once(
        VALID_START_TASK,
        "\"is_local\":false",
        "\"is_local\":\"false\"",
    );
    assert!(decode_task(&non_boolean).is_err());
}

/// Rust rejects the same nested values that the OCaml adapter rejects before
/// either side can invoke an activity with an unsafe execution context.
#[test]
fn activity_start_rejects_nested_semantic_mismatches() {
    let oversized_fairness_key = format!("\"fairness_key\":\"{}\"", "x".repeat(65));
    let oversized_standalone_run_id = format!("\"standalone_run_id\":\"{}\"", "x".repeat(65_537));
    let oversized_non_retryable_type = format!("\"{}\"", "x".repeat(65_537));
    let malformed = [
        (
            "negative retry initial interval",
            replace_once(
                VALID_START_TASK,
                "\"initial_interval\":{\"seconds\":1",
                "\"initial_interval\":{\"seconds\":-1",
            ),
        ),
        (
            "retry maximum interval nanoseconds",
            replace_once(
                VALID_START_TASK,
                "\"maximum_interval\":{\"seconds\":60,\"nanoseconds\":0}",
                "\"maximum_interval\":{\"seconds\":60,\"nanoseconds\":1000000000}",
            ),
        ),
        (
            "noncanonical retry coefficient",
            replace_once(
                VALID_START_TASK,
                "\"backoff_coefficient_bits\":\"4611686018427387904\"",
                "\"backoff_coefficient_bits\":\"01\"",
            ),
        ),
        (
            "retry coefficient outside u64",
            replace_once(
                VALID_START_TASK,
                "\"backoff_coefficient_bits\":\"4611686018427387904\"",
                "\"backoff_coefficient_bits\":\"18446744073709551616\"",
            ),
        ),
        (
            "empty header key",
            replace_once(VALID_START_TASK, "\"trace\":", "\"\":"),
        ),
        (
            "fairness key exceeds Core limit",
            replace_once(
                VALID_START_TASK,
                "\"fairness_key\":\"tenant\"",
                &oversized_fairness_key,
            ),
        ),
        (
            "standalone run ID exceeds text limit",
            replace_once(
                VALID_START_TASK,
                "\"standalone_run_id\":\"\"",
                &oversized_standalone_run_id,
            ),
        ),
        (
            "non-retryable error type exceeds text limit",
            replace_once(
                VALID_START_TASK,
                "\"InvalidInput\"",
                &oversized_non_retryable_type,
            ),
        ),
        (
            "maximum attempts outside signed i32",
            replace_once(
                VALID_START_TASK,
                "\"maximum_attempts\":3",
                "\"maximum_attempts\":2147483648",
            ),
        ),
    ];

    for (name, json) in malformed {
        assert!(decode_task(&json).is_err(), "{name} should be rejected");
    }
}

/// The signed int32 retry-attempt domain is bilateral: both endpoints remain
/// accepted even though a future semantic policy may narrow their meaning.
#[test]
fn retry_maximum_attempts_accepts_signed_i32_boundaries() {
    for value in ["-2147483648", "2147483647"] {
        let json = replace_once(
            VALID_START_TASK,
            "\"maximum_attempts\":3",
            &format!("\"maximum_attempts\":{value}"),
        );
        decode_task(&json).expect("signed i32 boundary should decode");
    }
}

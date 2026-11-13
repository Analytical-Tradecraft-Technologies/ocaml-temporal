//! Unit regressions for the cron schedule and start delay of a client start
//! (#1158). They exercise the private start-document decoder and
//! `start_execution_request`, the one builder every start path (start,
//! signal-with-start, update-with-start) uses, so a schedule cannot reach
//! Temporal unvalidated or be dropped on the way. The module is compiled into
//! the crate through a `#[path]` declaration so production code and tests
//! stay in separate files.

use super::*;

/// A valid start document with `members` added (or replaced).
fn start_document(members: serde_json::Value) -> String {
    let mut document = serde_json::json!({
        "request_id": "request-1",
        "namespace": "default",
        "workflow_id": "workflow-1",
        "workflow_type": "scheduled",
        "task_queue": "queue",
        "input": [],
    });
    for (key, value) in members.as_object().expect("members are an object") {
        document[key] = value.clone();
    }
    document.to_string()
}

/// A cron schedule and a start delay each reach Temporal's request exactly,
/// and an omitted option keeps Temporal's "none" value: the empty schedule
/// and an absent delay.
#[test]
fn schedule_options_reach_the_start_request() {
    let plain = decode_start_request(&start_document(serde_json::json!({})))
        .expect("a start without schedule options decodes");
    assert_eq!(
        (plain.cron_schedule.as_deref(), plain.start_delay_ms),
        (None, None)
    );
    let core = start_execution_request(&plain, "identity").expect("plain start builds");
    assert_eq!(core.cron_schedule, "");
    assert_eq!(core.workflow_start_delay, None);

    let cron = decode_start_request(&start_document(serde_json::json!({
        "cron_schedule": "@every 5s",
    })))
    .expect("a cron start decodes");
    let core = start_execution_request(&cron, "identity").expect("cron start builds");
    assert_eq!(core.cron_schedule, "@every 5s");
    assert_eq!(core.workflow_start_delay, None);

    let delayed = decode_start_request(&start_document(serde_json::json!({
        "start_delay_ms": 1_500,
    })))
    .expect("a delayed start decodes");
    let core = start_execution_request(&delayed, "identity").expect("delayed start builds");
    assert_eq!(core.cron_schedule, "");
    assert_eq!(
        core.workflow_start_delay,
        Some(prost_wkt_types::Duration {
            seconds: 1,
            nanos: 500_000_000,
        })
    );

    let longest = decode_start_request(&start_document(serde_json::json!({
        "start_delay_ms": MAX_WORKFLOW_TIMEOUT_MS,
    })))
    .expect("the maximum delay decodes");
    assert_eq!(longest.start_delay_ms, Some(MAX_WORKFLOW_TIMEOUT_MS));
}

/// Temporal reads a zero delay as "no delay" and refuses a schedule combined
/// with a delay, and an empty or NUL-bearing schedule is not an identifier:
/// each is a protocol error naming its field, before any RPC. A negative
/// delay fails the closed document shape itself.
#[test]
fn invalid_schedule_options_are_rejected() {
    for (members, path) in [
        (serde_json::json!({"start_delay_ms": 0}), "$.start_delay_ms"),
        (
            serde_json::json!({"start_delay_ms": MAX_WORKFLOW_TIMEOUT_MS + 1}),
            "$.start_delay_ms",
        ),
        (
            serde_json::json!({"cron_schedule": "* * * * *", "start_delay_ms": 1_000}),
            "$.start_delay_ms",
        ),
        (serde_json::json!({"cron_schedule": ""}), "$.cron_schedule"),
        (
            serde_json::json!({"cron_schedule": "* *\u{0} * * *"}),
            "$.cron_schedule",
        ),
        (serde_json::json!({"start_delay_ms": -1}), "$"),
    ] {
        let error = decode_start_request(&start_document(members.clone()))
            .expect_err("invalid schedule options are rejected");
        assert!(
            error.path.starts_with(path),
            "{members}: expected {path}, got {error:?}"
        );
    }
}

/// A pending request ID may only be retried with the same logical start, so
/// a retry that changes the schedule or the delay is a different request.
#[test]
fn schedule_options_take_part_in_request_equality() {
    let base = decode_start_request(&start_document(serde_json::json!({
        "cron_schedule": "@every 5s",
    })))
    .expect("cron start decodes");
    let other_schedule = StartWorkflowRequest {
        cron_schedule: Some("@every 6s".to_owned()),
        ..base.clone()
    };
    let delayed = StartWorkflowRequest {
        cron_schedule: None,
        start_delay_ms: Some(5_000),
        ..base.clone()
    };
    assert!(same_start_request(&base, &base.clone()));
    assert!(!same_start_request(&base, &other_schedule));
    assert!(!same_start_request(&base, &delayed));
}

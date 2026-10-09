//! Worker-config regressions for the public resource and shutdown options
//! (#498).
//!
//! The documents here are the exact bytes the OCaml encoder produces (the
//! OCaml bridge test asserts the same strings), so these tests pin the whole
//! OCaml-to-Core path: strict decoding, bridge validation, and every
//! resulting `WorkerConfig` value.

use std::time::Duration;

use super::{STATUS_CONFIGURATION, WorkerConfigInput, decode_config};
use temporalio_sdk_core::{PollerBehavior, WorkerConfig};

/// The document the OCaml encoder sends for a worker that sets every #498
/// option. `test/bridge/test_ocaml_bridge.ml` asserts the same string.
const TUNED_DOCUMENT: &str = concat!(
    "{\"namespace\":\"tuning-test\",\"task_queue\":\"tuning-test\",",
    "\"build_id\":\"tuning-test\",\"versioning\":{\"kind\":\"none\"},",
    "\"max_cached_workflows\":4,\"max_outstanding_workflow_tasks\":8,",
    "\"max_concurrent_workflow_task_polls\":6,",
    "\"graceful_shutdown_timeout_ms\":1500,",
    "\"task_types\":{\"workflows\":true,\"activities\":true},",
    "\"tuning\":{\"workflow_task_poller_autoscaling\":",
    "{\"minimum\":2,\"maximum\":6,\"initial\":3},",
    "\"sticky_queue_schedule_to_start_timeout_ms\":2500,",
    "\"max_heartbeat_throttle_interval_ms\":20000,",
    "\"default_heartbeat_throttle_interval_ms\":5000,",
    "\"max_worker_activities_per_second\":2.5,",
    "\"max_task_queue_activities_per_second\":40.5}}"
);

/// The document the OCaml encoder sends for a default public worker: no
/// `tuning` member, byte-for-byte the pre-#498 encoding.
const DEFAULT_DOCUMENT: &str = concat!(
    "{\"namespace\":\"tuning-test\",\"task_queue\":\"tuning-test\",",
    "\"build_id\":\"tuning-test\",\"versioning\":{\"kind\":\"none\"},",
    "\"max_cached_workflows\":1000,\"max_outstanding_workflow_tasks\":1000,",
    "\"max_concurrent_workflow_task_polls\":2,",
    "\"graceful_shutdown_timeout_ms\":30000,",
    "\"task_types\":{\"workflows\":true,\"activities\":true}}"
);

/// Decodes one document through the same strict decoder as the C ABI.
fn decode(text: &str) -> Result<WorkerConfigInput, super::Failure> {
    // SAFETY: `text` owns `text.len()` initialized bytes for the whole call.
    unsafe { decode_config::<WorkerConfigInput>(text.as_ptr(), text.len()) }
}

/// Decodes and converts one document into Core's configuration.
fn core(text: &str) -> Result<WorkerConfig, super::Failure> {
    decode(text)?.into_core()
}

/// Replaces one `tuning` member of [`TUNED_DOCUMENT`] by text substitution,
/// so each negative case differs from the valid document in one place.
fn tuned_with(from: &str, to: &str) -> String {
    assert!(TUNED_DOCUMENT.contains(from), "fixture lacks {from}");
    TUNED_DOCUMENT.replacen(from, to, 1)
}

/// Expects a configuration failure whose message names `field`.
fn expect_rejected(text: &str, field: &str) {
    match core(text) {
        Ok(_) => panic!("document with invalid {field} was accepted"),
        Err(failure) => {
            assert_eq!(failure.status, STATUS_CONFIGURATION);
            assert!(
                failure.message.contains(field),
                "message {:?} does not name {field}",
                failure.message
            );
        }
    }
}

/// Every option reaches the exact Core field it controls. The workflow-task
/// limit is lowered to `max(cache, 2)` by the existing slot normalization,
/// and activity slots stay pinned to the serial executor (#777).
#[test]
fn tuned_document_maps_every_option_onto_core() {
    let config = core(TUNED_DOCUMENT).expect("tuned document should be valid");
    assert_eq!(config.max_cached_workflows, 4);
    assert_eq!(config.max_outstanding_workflow_tasks, Some(4));
    assert_eq!(
        config.workflow_task_poller_behavior,
        PollerBehavior::Autoscaling {
            minimum: 2,
            maximum: 6,
            initial: 3,
        }
    );
    assert_eq!(
        config.graceful_shutdown_period,
        Some(Duration::from_millis(1_500))
    );
    assert_eq!(
        config.sticky_queue_schedule_to_start_timeout,
        Duration::from_millis(2_500)
    );
    assert_eq!(
        config.max_heartbeat_throttle_interval,
        Duration::from_secs(20)
    );
    assert_eq!(
        config.default_heartbeat_throttle_interval,
        Duration::from_secs(5)
    );
    assert_eq!(config.max_worker_activities_per_second, Some(2.5));
    assert_eq!(config.max_task_queue_activities_per_second, Some(40.5));
    assert_eq!(config.max_outstanding_activities, Some(1));
    assert_eq!(config.max_outstanding_local_activities, Some(1));
    assert_eq!(
        config.activity_task_poller_behavior,
        PollerBehavior::SimpleMaximum(1)
    );
}

/// A default public worker keeps the historical fixed pollers, the 30 s
/// grace period and Core's own defaults for every tuning setting.
#[test]
fn default_document_keeps_core_defaults() {
    let config = core(DEFAULT_DOCUMENT).expect("default document should be valid");
    assert_eq!(config.max_cached_workflows, 1_000);
    assert_eq!(config.max_outstanding_workflow_tasks, Some(1_000));
    assert_eq!(
        config.workflow_task_poller_behavior,
        PollerBehavior::SimpleMaximum(2)
    );
    assert_eq!(
        config.graceful_shutdown_period,
        Some(Duration::from_secs(30))
    );
    assert_eq!(
        config.sticky_queue_schedule_to_start_timeout,
        Duration::from_secs(10)
    );
    assert_eq!(
        config.max_heartbeat_throttle_interval,
        Duration::from_secs(60)
    );
    assert_eq!(
        config.default_heartbeat_throttle_interval,
        Duration::from_secs(30)
    );
    assert_eq!(config.max_worker_activities_per_second, None);
    assert_eq!(config.max_task_queue_activities_per_second, None);
}

/// An explicit empty `tuning` object is equivalent to omitting it.
#[test]
fn empty_tuning_object_is_default() {
    let text = DEFAULT_DOCUMENT.replacen("}}", "},\"tuning\":{}}", 1);
    let config = core(&text).expect("empty tuning should be valid");
    assert_eq!(
        config.sticky_queue_schedule_to_start_timeout,
        Duration::from_secs(10)
    );
}

/// The tuning object is closed, like every other bridge document, so a
/// misspelled option fails instead of silently keeping a default.
#[test]
fn unknown_tuning_member_is_rejected() {
    let text = tuned_with(
        "\"max_worker_activities_per_second\"",
        "\"max_worker_activity_per_second\"",
    );
    assert!(decode(&text).is_err());
}

/// Autoscaling bounds must be ordered and agree with the poller maximum.
#[test]
fn autoscaling_bounds_are_validated() {
    expect_rejected(
        &tuned_with("\"minimum\":2", "\"minimum\":0"),
        "workflow_task_poller_autoscaling.minimum",
    );
    expect_rejected(
        &tuned_with("\"initial\":3", "\"initial\":7"),
        "workflow_task_poller_autoscaling.initial",
    );
    expect_rejected(
        &tuned_with("\"initial\":3", "\"initial\":1"),
        "workflow_task_poller_autoscaling.initial",
    );
    expect_rejected(
        &tuned_with(
            "{\"minimum\":2,\"maximum\":6,\"initial\":3}",
            "{\"minimum\":5,\"maximum\":4,\"initial\":4}",
        ),
        "workflow_task_poller_autoscaling.maximum",
    );
    expect_rejected(
        &tuned_with("\"maximum\":6", "\"maximum\":5"),
        "must equal max_concurrent_workflow_task_polls",
    );
}

/// Durations must be positive and at most one day; values beyond the
/// accepted range never reach Core's protobuf conversion.
#[test]
fn tuning_durations_are_bounded() {
    for (from, field) in [
        (
            "\"sticky_queue_schedule_to_start_timeout_ms\":2500",
            "sticky_queue_schedule_to_start_timeout_ms",
        ),
        (
            "\"max_heartbeat_throttle_interval_ms\":20000",
            "max_heartbeat_throttle_interval_ms",
        ),
        (
            "\"default_heartbeat_throttle_interval_ms\":5000",
            "default_heartbeat_throttle_interval_ms",
        ),
    ] {
        let zero = format!("\"{field}\":0");
        expect_rejected(&tuned_with(from, &zero), field);
        let too_long = format!("\"{field}\":86400001");
        expect_rejected(&tuned_with(from, &too_long), field);
    }
    // Negative values are not representable as `u64`, so decoding fails.
    assert!(
        decode(&tuned_with(
            "\"sticky_queue_schedule_to_start_timeout_ms\":2500",
            "\"sticky_queue_schedule_to_start_timeout_ms\":-1",
        ))
        .is_err()
    );
}

/// An explicit default heartbeat interval above the explicit maximum is a
/// contradiction Core would otherwise clip silently.
#[test]
fn heartbeat_default_must_not_exceed_maximum() {
    expect_rejected(
        &tuned_with(
            "\"default_heartbeat_throttle_interval_ms\":5000",
            "\"default_heartbeat_throttle_interval_ms\":20001",
        ),
        "must not exceed max_heartbeat_throttle_interval_ms",
    );
}

/// Rates must be positive and normal; zero, negative and subnormal values
/// fail at the bridge rather than in Core or on the server.
#[test]
fn activity_rates_must_be_positive() {
    for field in [
        "max_worker_activities_per_second",
        "max_task_queue_activities_per_second",
    ] {
        let from = if field == "max_worker_activities_per_second" {
            format!("\"{field}\":2.5")
        } else {
            format!("\"{field}\":40.5")
        };
        for invalid in ["0", "0.0", "-1.5", "5e-324"] {
            let to = format!("\"{field}\":{invalid}");
            expect_rejected(&tuned_with(&from, &to), field);
        }
    }
}

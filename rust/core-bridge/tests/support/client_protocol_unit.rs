//! Unit tests for the private client protocol: visibility status names,
//! closed request and response document validation, terminal history-event
//! and gRPC status mapping, and encode/decode round trips of client documents.
//! They live outside `client_protocol.rs` (repository rule) and are mounted
//! there through a `#[path]` module so they can reach its private helpers.

use super::*;
use base64::engine::general_purpose::STANDARD;
use temporalio_client::tonic::codegen::Bytes;

#[test]
/// An omitted visibility status fails closed, while a value a newer
/// server may add degrades to the documented `unknown_<n>` name (#1078).
fn visibility_status_names_unknown_values() {
    assert!(visibility_status(0).is_err());
    assert_eq!(visibility_status(1).unwrap(), "running");
    assert_eq!(visibility_status(8).unwrap(), "paused");
    assert_eq!(visibility_status(9).unwrap(), "unknown_9");
    assert_eq!(visibility_status(-3).unwrap(), "unknown_-3");
    for status in [1, 8, 9, 99, -3, i32::MAX, i32::MIN] {
        let name = visibility_status(status).unwrap();
        assert!(is_visibility_status_name(&name), "{name}");
    }
}

#[test]
/// The encoder accepts only names the converter can produce, with one
/// canonical spelling per unknown wire value, matching the OCaml decoder.
fn visibility_status_name_vocabulary() {
    for accepted in [
        "running",
        "paused",
        "unspecified",
        "unknown_9",
        "unknown_-1",
    ] {
        assert!(is_visibility_status_name(accepted), "{accepted}");
    }
    for rejected in [
        "future_status",
        "unknown_",
        "unknown_1",
        "unknown_0",
        "unknown_8",
        "unknown_09",
        "unknown_+9",
        "unknown_-0",
        "unknown_9x",
        "unknown_99999999999",
    ] {
        assert!(!is_visibility_status_name(rejected), "{rejected}");
    }
}

/// Builds the smallest valid start document used by strict-parser tests.
fn start_json() -> String {
    serde_json::json!({
        "request_id":"request-1",
        "namespace":"default",
        "workflow_id":"workflow-1",
        "workflow_type":"smoke",
        "task_queue":"queue",
        "input":[{"metadata":{},"data":{"encoding":"base64","data":STANDARD.encode(b"input")}}]
    })
    .to_string()
}

/// Builds a valid exact-run cancellation request, including the stable
/// request ID that lets a caller retry a timed-out RPC safely.
fn cancel_json() -> String {
    serde_json::json!({
        "namespace":"default",
        "workflow_id":"workflow-1",
        "run_id":"run-1",
        "request_id":"cancel-1",
        "reason":"operator requested cancellation"
    })
    .to_string()
}

/// Builds an exact-run reset request whose event boundary is represented
/// as an integer literal so large Temporal event IDs cannot round through
/// a floating-point JSON representation.
fn reset_json() -> String {
    serde_json::json!({
        "namespace":"default",
        "workflow_id":"workflow-1",
        "run_id":"run-1",
        "request_id":"reset-1",
        "reason":"replay from workflow task",
        "workflow_task_finish_event_id":4
    })
    .to_string()
}

/// Builds a valid exact-run signal request used by strict protocol tests.
fn signal_json() -> String {
    serde_json::json!({
        "namespace":"default",
        "workflow_id":"workflow-1",
        "run_id":"run-1",
        "signal_name":"add_document",
        "request_id":"signal-1",
        "input":[]
    })
    .to_string()
}

/// Builds a valid output-only exact-run query request.
fn query_json() -> String {
    serde_json::json!({
        "namespace":"default",
        "workflow_id":"workflow-1",
        "run_id":"run-1",
        "query_type":"current_state",
        "input":[]
    })
    .to_string()
}

#[test]
/// Rejects a syntactically valid request containing an unknown member.
fn start_request_requires_closed_payload_shape() {
    let mut json = start_json();
    // Insert the unknown member before the existing closing brace so the
    // test exercises strict-field validation rather than JSON syntax.
    json.insert_str(json.len() - 1, ",\"unexpected\":true");
    assert!(decode_start_request(&json).is_err());
}

#[test]
/// Requires the stable logical ID used to correlate asynchronous retries.
fn start_request_requires_request_id() {
    let json = serde_json::json!({
        "namespace":"default",
        "workflow_id":"workflow-1",
        "workflow_type":"smoke",
        "task_queue":"queue",
        "input":[]
    })
    .to_string();
    assert!(decode_start_request(&json).is_err());
}

#[test]
/// A reused request ID may alias only an identical semantic request.
fn pending_request_matching_checks_all_start_fields() {
    let original = decode_start_request(&start_json()).expect("start request decodes");
    let mut changed = original.clone();
    changed.task_queue = "other-queue".to_owned();

    assert!(same_start_request(&original, &original));
    assert!(!same_start_request(&original, &changed));

    changed.task_queue = original.task_queue.clone();
    changed.input[0].data.push(0);
    assert!(!same_start_request(&original, &changed));

    let mut changed_policy = original.clone();
    changed_policy.id_conflict_policy = IdConflictPolicy::UseExisting;
    assert!(!same_start_request(&original, &changed_policy));

    // Execution policies are part of the logical start (#499).
    let mut changed_reuse = original.clone();
    changed_reuse.id_reuse_policy = IdReusePolicy::RejectDuplicate;
    assert!(!same_start_request(&original, &changed_reuse));
    let mut changed_timeout = original.clone();
    changed_timeout.execution_timeout_ms = Some(1_000);
    assert!(!same_start_request(&original, &changed_timeout));

    // The caller's RPC deadline bounds one call, not the workflow, so a
    // retry with another deadline is still the same logical start.
    let mut changed_deadline = original.clone();
    changed_deadline.rpc_timeout_ms = Some(1_000);
    assert!(same_start_request(&original, &changed_deadline));
}

#[test]
/// The reuse policy uses closed snake-case names, defaults to
/// `allow_duplicate` when omitted, maps to Temporal's non-`UNSPECIFIED`
/// enum numbers, and has no `terminate_if_running` spelling.
fn start_request_id_reuse_policy_is_closed_and_explicit() {
    let omitted = decode_start_request(&start_json()).expect("start request decodes");
    assert_eq!(omitted.id_reuse_policy, IdReusePolicy::AllowDuplicate);

    for (name, policy, core) in [
        (
            "allow_duplicate",
            IdReusePolicy::AllowDuplicate,
            WorkflowIdReusePolicy::AllowDuplicate,
        ),
        (
            "allow_duplicate_failed_only",
            IdReusePolicy::AllowDuplicateFailedOnly,
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        ),
        (
            "reject_duplicate",
            IdReusePolicy::RejectDuplicate,
            WorkflowIdReusePolicy::RejectDuplicate,
        ),
    ] {
        let mut json: serde_json::Value =
            serde_json::from_str(&start_json()).expect("fixture is JSON");
        json["id_reuse_policy"] = serde_json::json!(name);
        let request = decode_start_request(&json.to_string()).expect("policy decodes");
        assert_eq!(request.id_reuse_policy, policy);
        assert_eq!(policy.to_core(), i32::from(core));
    }

    for invalid in [
        serde_json::json!("terminate_if_running"),
        serde_json::json!("unspecified"),
        serde_json::json!(2),
        serde_json::Value::Null,
    ] {
        let mut json: serde_json::Value =
            serde_json::from_str(&start_json()).expect("fixture is JSON");
        json["id_reuse_policy"] = invalid;
        assert!(decode_start_request(&json.to_string()).is_err());
    }
}

#[test]
/// Workflow timeouts must be positive and bounded, a run timeout may not
/// exceed the execution timeout, and the task timeout is capped at
/// Temporal's 120 seconds and at the run (or execution) timeout. Every
/// policy combination is accepted. Accepted values round-trip without
/// rounding.
fn start_request_execution_policies_are_validated() {
    let with = |members: serde_json::Value| {
        let mut json: serde_json::Value =
            serde_json::from_str(&start_json()).expect("fixture is JSON");
        for (key, value) in members.as_object().expect("object").iter() {
            json[key] = value.clone();
        }
        decode_start_request(&json.to_string())
    };
    let accepted = with(serde_json::json!({
        "execution_timeout_ms": MAX_WORKFLOW_TIMEOUT_MS,
        "run_timeout_ms": MAX_WORKFLOW_TASK_TIMEOUT_MS,
        "task_timeout_ms": MAX_WORKFLOW_TASK_TIMEOUT_MS,
        "rpc_timeout_ms": MAX_RPC_TIMEOUT_MS,
    }))
    .expect("bounded policies decode");
    assert_eq!(accepted.execution_timeout_ms, Some(MAX_WORKFLOW_TIMEOUT_MS));
    assert_eq!(
        milliseconds_to_core(MAX_WORKFLOW_TIMEOUT_MS).expect("representable"),
        prost_wkt_types::Duration {
            seconds: 315_576_000_000,
            nanos: 999_000_000,
        }
    );
    assert!(
        with(serde_json::json!({
            "id_conflict_policy": "use_existing",
            "id_reuse_policy": "reject_duplicate",
        }))
        .is_ok()
    );
    // A zero interval or the all-zero coefficient bits are Temporal's "unset"
    // values (#1145): the server applies its defaults, so they are accepted.
    assert!(
        with(serde_json::json!({"retry_policy": {
            "initial_interval": {"seconds": 0, "nanoseconds": 0},
            "backoff_coefficient_bits": "0",
            "maximum_interval": {"seconds": 1, "nanoseconds": 0},
            "maximum_attempts": 2,
            "non_retryable_error_types": []
        }}))
        .is_ok()
    );

    for rejected in [
        serde_json::json!({"execution_timeout_ms": 0}),
        serde_json::json!({"run_timeout_ms": 0}),
        serde_json::json!({"task_timeout_ms": 0}),
        serde_json::json!({"execution_timeout_ms": MAX_WORKFLOW_TIMEOUT_MS + 1}),
        serde_json::json!({"task_timeout_ms": MAX_WORKFLOW_TASK_TIMEOUT_MS + 1}),
        serde_json::json!({"execution_timeout_ms": -1}),
        serde_json::json!({"execution_timeout_ms": 1.5}),
        serde_json::json!({"execution_timeout_ms": 1_000, "run_timeout_ms": 1_001}),
        serde_json::json!({"run_timeout_ms": 1_000, "task_timeout_ms": 1_001}),
        serde_json::json!({"execution_timeout_ms": 1_000, "task_timeout_ms": 1_001}),
        serde_json::json!({"rpc_timeout_ms": 0}),
        serde_json::json!({"rpc_timeout_ms": MAX_RPC_TIMEOUT_MS + 1}),
        // Both intervals set, with the maximum below the initial interval.
        serde_json::json!({"retry_policy": {
            "initial_interval": {"seconds": 2, "nanoseconds": 0},
            "backoff_coefficient_bits": 2.0_f64.to_bits().to_string(),
            "maximum_interval": {"seconds": 1, "nanoseconds": 0},
            "maximum_attempts": 2,
            "non_retryable_error_types": []
        }}),
        // A set coefficient below 1.0 (zero bits alone mean "unset").
        serde_json::json!({"retry_policy": {
            "initial_interval": {"seconds": 0, "nanoseconds": 0},
            "backoff_coefficient_bits": 0.5_f64.to_bits().to_string(),
            "maximum_interval": {"seconds": 0, "nanoseconds": 0},
            "maximum_attempts": 2,
            "non_retryable_error_types": []
        }}),
    ] {
        assert!(
            with(rejected.clone()).is_err(),
            "{rejected} must be rejected"
        );
    }
}

#[test]
/// The transport reports its own expiry of an attempt's deadline as
/// `cancelled`. Once the caller's budget has elapsed that status, like a
/// server `deadline_exceeded`, is the typed `deadline_exceeded` error;
/// before then, and for every other status, the ordinary mapping applies.
fn expired_budget_statuses_are_deadline_exceeded() {
    let deadline = ClientOperationError::Rpc {
        code: "deadline_exceeded".to_owned(),
    };
    let expired = tokio::time::Instant::now() - Duration::from_millis(5);
    for code in [Code::Cancelled, Code::DeadlineExceeded] {
        assert_eq!(
            budget_status(expired, Duration::from_millis(1), map_rpc_status)(Status::new(
                code, "timeout"
            )),
            deadline
        );
    }
    assert_eq!(
        budget_status(expired, Duration::from_millis(1), map_rpc_status)(Status::new(
            Code::Unavailable,
            "down"
        )),
        ClientOperationError::Rpc {
            code: "unavailable".to_owned()
        }
    );
    let fresh = tokio::time::Instant::now();
    assert_eq!(
        budget_status(fresh, Duration::from_secs(60), map_rpc_status)(Status::new(
            Code::Cancelled,
            "cancelled"
        )),
        ClientOperationError::Rpc {
            code: "cancelled".to_owned()
        }
    );
}

#[test]
/// Every bounded client request accepts an optional caller RPC timeout
/// between 1 ms and one minute and rejects anything else; the default
/// budget applies only when the member is absent.
fn client_requests_validate_rpc_timeouts() {
    /// Reports whether one request document decodes.
    type Decodes = fn(&str) -> bool;
    let documents: [(&str, Decodes); 7] = [
        (
            r#"{"namespace":"default","workflow_id":"w","run_id":"","request_id":"r","reason":""}"#,
            |input| decode_cancel_request(input).is_ok(),
        ),
        (
            r#"{"namespace":"default","workflow_id":"w","run_id":"","reason":""}"#,
            |input| decode_terminate_request(input).is_ok(),
        ),
        (
            r#"{"namespace":"default","workflow_id":"w","run_id":"","request_id":"r","reason":"","workflow_task_finish_event_id":3}"#,
            |input| decode_reset_request(input).is_ok(),
        ),
        (
            r#"{"namespace":"default","workflow_id":"w","run_id":"","signal_name":"s","request_id":"r","input":[]}"#,
            |input| decode_signal_request(input).is_ok(),
        ),
        (
            r#"{"namespace":"default","workflow_id":"w","run_id":"","query_type":"q","input":[]}"#,
            |input| decode_query_request(input).is_ok(),
        ),
        (
            r#"{"namespace":"default","workflow_id":"w","run_id":"","update_id":"u","update_name":"n","input":[]}"#,
            |input| decode_update_request(input).is_ok(),
        ),
        (
            r#"{"namespace":"default","query":"","page_size":10,"next_page_token":null}"#,
            |input| decode_visibility_request(input).is_ok(),
        ),
    ];
    for (document, decodes) in documents {
        assert!(decodes(document), "{document}");
        let mut json: serde_json::Value = serde_json::from_str(document).expect("JSON");
        for (timeout, valid) in [
            (serde_json::json!(1), true),
            (serde_json::json!(MAX_RPC_TIMEOUT_MS), true),
            (serde_json::json!(0), false),
            (serde_json::json!(MAX_RPC_TIMEOUT_MS + 1), false),
            (serde_json::json!("1000"), false),
        ] {
            json["rpc_timeout_ms"] = timeout.clone();
            assert_eq!(
                decodes(&json.to_string()),
                valid,
                "{document} with {timeout}"
            );
        }
    }
    assert_eq!(rpc_budget(None, CONTROL_RPC_TIMEOUT), CONTROL_RPC_TIMEOUT);
    assert_eq!(
        rpc_budget(Some(250), CONTROL_RPC_TIMEOUT),
        Duration::from_millis(250)
    );
}

#[test]
/// The conflict policy uses closed snake-case names, defaults to `Fail`
/// when omitted, and maps to Temporal's non-`UNSPECIFIED` enum numbers.
fn start_request_id_conflict_policy_is_closed_and_explicit() {
    let omitted = decode_start_request(&start_json()).expect("start request decodes");
    assert_eq!(omitted.id_conflict_policy, IdConflictPolicy::Fail);

    for (name, policy, core) in [
        (
            "fail",
            IdConflictPolicy::Fail,
            WorkflowIdConflictPolicy::Fail,
        ),
        (
            "use_existing",
            IdConflictPolicy::UseExisting,
            WorkflowIdConflictPolicy::UseExisting,
        ),
        (
            "terminate_existing",
            IdConflictPolicy::TerminateExisting,
            WorkflowIdConflictPolicy::TerminateExisting,
        ),
    ] {
        let mut json: serde_json::Value =
            serde_json::from_str(&start_json()).expect("fixture is JSON");
        json["id_conflict_policy"] = serde_json::json!(name);
        let request = decode_start_request(&json.to_string()).expect("policy decodes");
        assert_eq!(request.id_conflict_policy, policy);
        assert_eq!(policy.to_core(), i32::from(core));
    }

    for invalid in [
        serde_json::json!("unspecified"),
        serde_json::json!("FAIL"),
        serde_json::json!(1),
        serde_json::Value::Null,
    ] {
        let mut json: serde_json::Value =
            serde_json::from_str(&start_json()).expect("fixture is JSON");
        json["id_conflict_policy"] = invalid;
        assert!(decode_start_request(&json.to_string()).is_err());
    }
}

#[test]
/// Tickets round-trip through a closed object and reject extra members.
fn start_ticket_round_trips_as_an_opaque_value() {
    let encoded = encode_start_ticket("ticket-1").expect("ticket encodes");
    assert_eq!(
        decode_start_ticket(&encoded).expect("ticket decodes"),
        "ticket-1"
    );
    assert!(decode_start_ticket(r#"{"ticket":"ticket-1","extra":true}"#).is_err());
}

#[test]
/// Terminal outcomes preserve acceptance and expose uncertain transport
/// failures without pretending that Temporal rejected the request.
fn start_outcomes_are_closed_and_uncertainty_is_explicit() {
    let accepted = StartWorkflowOutcome::Accepted(StartWorkflowResponse {
        execution: ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
        },
        started: true,
    });
    assert_eq!(
        encode_start_outcome(&accepted).expect("accepted outcome encodes"),
        r#"{"kind":"accepted","execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"started":true}"#
    );

    let rejected = ClientOperationError::Rpc {
        code: "invalid_argument".to_owned(),
    };
    assert!(!rejected.uncertain_start());
    assert!(
        ClientOperationError::Rpc {
            code: "unavailable".to_owned()
        }
        .uncertain_start()
    );
    // Start RPC timeouts use the same uncertain classification so a hung
    // server cannot be treated as a proven rejection.
    assert!(
        ClientOperationError::Rpc {
            code: "deadline_exceeded".to_owned()
        }
        .uncertain_start()
    );
    let unknown = StartWorkflowOutcome::Unknown {
        request_id: "request-1".to_owned(),
        workflow_id: "workflow-1".to_owned(),
    };
    assert_eq!(
        encode_start_outcome(&unknown).expect("unknown outcome encodes"),
        r#"{"kind":"unknown","request_id":"request-1","workflow_id":"workflow-1"}"#
    );
}

#[test]
/// Rejects duplicate members before serde's map conversion can erase one.
fn duplicate_start_member_is_rejected_before_serde_map_conversion() {
    let json = r#"{"request_id":"request-1","namespace":"default","namespace":"other","workflow_id":"id","workflow_type":"type","task_queue":"queue","input":[]}"#;
    assert!(decode_start_request(json).is_err());
}

#[test]
/// Rejects an attempted opt-in to successor following on exact-run waits.
fn exact_wait_request_has_no_follow_runs_escape_hatch() {
    let json = r#"{"namespace":"default","workflow_id":"id","run_id":"run","follow_runs":true}"#;
    assert!(decode_wait_request(json).is_err());
}

#[test]
/// Decodes every cancellation field without treating an empty reason as a
/// missing field, and emits only a positive acknowledgement.
fn cancellation_request_and_acknowledgement_round_trip() {
    let request = decode_cancel_request(&cancel_json()).expect("cancel request decodes");
    assert_eq!(request.namespace, "default");
    assert_eq!(request.workflow_id, "workflow-1");
    assert_eq!(request.run_id, "run-1");
    assert_eq!(request.request_id, "cancel-1");
    assert_eq!(request.reason, "operator requested cancellation");

    let empty_reason = serde_json::json!({
        "namespace":"default",
        "workflow_id":"workflow-1",
        "run_id":"run-1",
        "request_id":"cancel-2",
        "reason":""
    })
    .to_string();
    assert_eq!(decode_cancel_request(&empty_reason).unwrap().reason, "");

    let encoded = encode_cancel_response(&CancelWorkflowResponse { acknowledged: true })
        .expect("positive cancellation acknowledgement encodes");
    assert_eq!(encoded, r#"{"acknowledged":true}"#);
}

#[test]
/// Termination has its own closed request type and rejects cancellation's
/// idempotency-only member rather than silently ignoring it.
fn termination_request_and_acknowledgement_round_trip() {
    let request = serde_json::json!({
        "namespace":"default",
        "workflow_id":"workflow-1",
        "run_id":"run-1",
        "reason":"operator test"
    })
    .to_string();
    let decoded = decode_terminate_request(&request).expect("terminate request decodes");
    assert_eq!(decoded.reason, "operator test");
    assert!(
        decode_terminate_request(
            &serde_json::json!({
                "namespace":"default",
                "workflow_id":"workflow-1",
                "run_id":"run-1",
                "reason":"operator test",
                "request_id":"not-supported"
            })
            .to_string()
        )
        .is_err()
    );
    assert_eq!(
        encode_terminate_response(&TerminateWorkflowResponse { acknowledged: true })
            .expect("terminate acknowledgement encodes"),
        r#"{"acknowledged":true}"#
    );
    assert!(
        encode_terminate_response(&TerminateWorkflowResponse {
            acknowledged: false
        })
        .is_err()
    );
}

#[test]
/// Rejects cancellation documents that could lose identity or change the
/// operation's meaning when decoded by a permissive JSON map.
fn cancellation_request_is_closed_and_bounded() {
    let mut unknown = cancel_json();
    unknown.insert_str(unknown.len() - 1, ",\"unexpected\":true");
    assert!(decode_cancel_request(&unknown).is_err());

    let duplicate = r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","request_id":"cancel-1","request_id":"other","reason":""}"#;
    assert!(decode_cancel_request(duplicate).is_err());

    // An empty run_id selects the current run (#791) and is accepted.
    for field in ["namespace", "workflow_id", "request_id"] {
        let document = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "request_id":"cancel-1",
            "reason":""
        });
        let mut object = document.as_object().unwrap().clone();
        object.insert(field.to_owned(), serde_json::Value::String(String::new()));
        assert!(
            decode_cancel_request(&serde_json::Value::Object(object).to_string()).is_err(),
            "empty {field} must be rejected"
        );
    }

    let nul_reason = serde_json::json!({
        "namespace":"default",
        "workflow_id":"workflow-1",
        "run_id":"run-1",
        "request_id":"cancel-1",
        "reason":"contains\0nul"
    })
    .to_string();
    assert!(decode_cancel_request(&nul_reason).is_err());

    let oversized_reason = serde_json::json!({
        "namespace":"default",
        "workflow_id":"workflow-1",
        "run_id":"run-1",
        "request_id":"cancel-1",
        "reason": "x".repeat(protocol::MAX_STRING_BYTES + 1)
    })
    .to_string();
    assert!(decode_cancel_request(&oversized_reason).is_err());
}

#[test]
/// Reset requests preserve the exact run and event boundary, reject
/// unknown members, and encode the new run identity under `execution`.
fn reset_request_and_response_are_strict() {
    let request = decode_reset_request(&reset_json()).expect("reset request decodes");
    assert_eq!(request.namespace, "default");
    assert_eq!(request.workflow_id, "workflow-1");
    assert_eq!(request.run_id, "run-1");
    assert_eq!(request.workflow_task_finish_event_id, 4);

    let response = ResetWorkflowResponse {
        execution: ExecutionRef {
            namespace: request.namespace.clone(),
            workflow_id: request.workflow_id.clone(),
            run_id: "run-2".to_owned(),
        },
    };
    assert_eq!(
        encode_reset_response(&response).expect("reset response encodes"),
        r#"{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}}"#
    );

    let mut unknown = reset_json();
    unknown.insert_str(unknown.len() - 1, ",\"unexpected\":true");
    assert!(decode_reset_request(&unknown).is_err());
}

#[test]
/// Event IDs at or below the first history event cannot be workflow-task
/// finish events, so reject them before attempting a Temporal RPC.
fn reset_request_rejects_early_event_boundaries() {
    for event_id in [-1, 0, 1] {
        let json = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "request_id":"reset-1",
            "reason":"",
            "workflow_task_finish_event_id":event_id
        })
        .to_string();
        assert!(decode_reset_request(&json).is_err());
    }
}

#[test]
/// Refuses a false acknowledgement so transport success cannot be
/// confused with Temporal accepting a cancellation request.
fn cancellation_acknowledgement_must_be_true_and_closed() {
    assert!(
        encode_cancel_response(&CancelWorkflowResponse {
            acknowledged: false
        })
        .is_err()
    );
    assert!(
        serde_json::from_str::<CancelWorkflowResponse>(
            r#"{"acknowledged":true,"unexpected":true}"#
        )
        .is_err()
    );
}

#[test]
/// Signals preserve exact-run identity, payload ordering, and positive
/// acknowledgement semantics across the closed JSON boundary.
fn signal_request_and_acknowledgement_round_trip() {
    let request = decode_signal_request(&signal_json()).expect("signal request decodes");
    assert_eq!(request.namespace, "default");
    assert_eq!(request.workflow_id, "workflow-1");
    assert_eq!(request.run_id, "run-1");
    assert_eq!(request.signal_name, "add_document");
    assert_eq!(request.request_id, "signal-1");
    assert!(request.input.is_empty());

    let encoded = encode_signal_response(&SignalWorkflowResponse { acknowledged: true })
        .expect("positive signal acknowledgement encodes");
    assert_eq!(encoded, r#"{"acknowledged":true}"#);
}

#[test]
/// Rejects signal documents that could redirect a message or silently
/// discard an input field through duplicate/unknown JSON members.
fn signal_request_is_closed_and_bounded() {
    let mut unknown = signal_json();
    unknown.insert_str(unknown.len() - 1, ",\"unexpected\":true");
    assert!(decode_signal_request(&unknown).is_err());

    let duplicate = r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","signal_name":"add_document","request_id":"signal-1","request_id":"other","input":[]}"#;
    assert!(decode_signal_request(duplicate).is_err());

    // An empty run_id selects the current run (#791) and is accepted.
    for field in ["namespace", "workflow_id", "signal_name", "request_id"] {
        let document = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "signal_name":"add_document",
            "request_id":"signal-1",
            "input":[]
        });
        let mut object = document.as_object().unwrap().clone();
        object.insert(field.to_owned(), serde_json::Value::String(String::new()));
        assert!(
            decode_signal_request(&serde_json::Value::Object(object).to_string()).is_err(),
            "empty {field} must be rejected"
        );
    }
}

#[test]
/// Refuses false signal acknowledgements and unknown response members.
fn signal_acknowledgement_must_be_true_and_closed() {
    assert!(
        encode_signal_response(&SignalWorkflowResponse {
            acknowledged: false
        })
        .is_err()
    );
    assert!(
        serde_json::from_str::<SignalWorkflowResponse>(
            r#"{"acknowledged":true,"unexpected":true}"#
        )
        .is_err()
    );
}

#[test]
/// Query requests preserve exact-run identity and reject unknown or
/// duplicate members before any Temporal connection is consulted.
fn query_request_is_closed_and_bounded() {
    let request = decode_query_request(&query_json()).expect("query request decodes");
    assert_eq!(request.query_type, "current_state");
    assert!(request.input.is_empty());

    let mut unknown = query_json();
    unknown.insert_str(unknown.len() - 1, ",\"unexpected\":true");
    assert!(decode_query_request(&unknown).is_err());

    let duplicate = r#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","query_type":"current_state","query_type":"other","input":[]}"#;
    assert!(decode_query_request(duplicate).is_err());

    // An empty run_id selects the current run (#791) and is accepted.
    for field in ["namespace", "workflow_id", "query_type"] {
        let document = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "run_id":"run-1",
            "query_type":"current_state",
            "input":[]
        });
        let mut object = document.as_object().unwrap().clone();
        object.insert(field.to_owned(), serde_json::Value::String(String::new()));
        assert!(
            decode_query_request(&serde_json::Value::Object(object).to_string()).is_err(),
            "empty {field} must be rejected"
        );
    }
}

#[test]
/// Query responses round-trip an ordered payload list and remain closed to
/// unknown JSON members at the native boundary.
fn query_response_round_trip_is_closed() {
    let response = QueryWorkflowResponse { result: Vec::new() };
    let encoded = encode_query_response(&response).expect("query response encodes");
    assert_eq!(encoded, r#"{"result":[]}"#);
    assert!(
        serde_json::from_str::<QueryWorkflowResponse>(r#"{"result":[],"unexpected":true}"#)
            .is_err()
    );
}

#[test]
/// Fails closed instead of polling forever when Core returns no attributes.
fn close_event_without_attributes_is_rejected() {
    assert!(outcome_from_event(HistoryEvent::default(), "default", "id").is_err());
}

#[test]
/// Converts every supported Core close event without following successors.
fn terminal_history_events_map_to_closed_outcomes() {
    let cases = [
        (
            Attributes::WorkflowExecutionCompletedEventAttributes(Default::default()),
            WorkflowOutcome::Completed {
                result: Vec::new(),
                successor: None,
            },
        ),
        (
            Attributes::WorkflowExecutionCanceledEventAttributes(Default::default()),
            WorkflowOutcome::Cancelled {
                details: Vec::new(),
            },
        ),
        (
            Attributes::WorkflowExecutionTerminatedEventAttributes(Default::default()),
            WorkflowOutcome::Terminated {
                details: Vec::new(),
            },
        ),
        (
            Attributes::WorkflowExecutionTimedOutEventAttributes(Default::default()),
            WorkflowOutcome::TimedOut { successor: None },
        ),
        (
            Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
                temporalio_common::protos::temporal::api::history::v1::
                    WorkflowExecutionContinuedAsNewEventAttributes {
                    new_execution_run_id: "run-2".to_owned(),
                    ..Default::default()
                },
            ),
            WorkflowOutcome::ContinuedAsNew {
                successor: SuccessorRef {
                    namespace: "default".to_owned(),
                    workflow_id: "id".to_owned(),
                    run_id: "run-2".to_owned(),
                },
            },
        ),
    ];
    for (attributes, expected) in cases {
        let event = HistoryEvent {
            attributes: Some(attributes),
            ..Default::default()
        };
        assert_eq!(
            outcome_from_event(event, "default", "id").unwrap(),
            expected
        );
    }
}

#[test]
/// Keeps the close event's own kind when Temporal links it to a successor
/// run (#971): a retried failure stays `Failed`, a retried timeout stays
/// `TimedOut`, and a cron completion stays `Completed`, each carrying the
/// successor. Only a continue-as-new event produces `ContinuedAsNew`.
fn successor_bearing_close_events_keep_their_kind() {
    use temporalio_common::protos::temporal::api::{
        failure::v1::Failure,
        history::v1::{
            WorkflowExecutionCompletedEventAttributes, WorkflowExecutionFailedEventAttributes,
            WorkflowExecutionTimedOutEventAttributes,
        },
    };
    let next = SuccessorRef {
        namespace: "default".to_owned(),
        workflow_id: "id".to_owned(),
        run_id: "run-2".to_owned(),
    };
    let outcome = |attributes| {
        outcome_from_event(
            HistoryEvent {
                attributes: Some(attributes),
                ..Default::default()
            },
            "default",
            "id",
        )
        .unwrap()
    };

    let failed = outcome(Attributes::WorkflowExecutionFailedEventAttributes(
        WorkflowExecutionFailedEventAttributes {
            failure: Some(Failure {
                message: "retried".to_owned(),
                ..Default::default()
            }),
            new_execution_run_id: "run-2".to_owned(),
            ..Default::default()
        },
    ));
    let WorkflowOutcome::Failed { failure, successor } = failed else {
        panic!("a retried failure must remain a failed outcome");
    };
    assert_eq!(failure.message, "retried");
    assert_eq!(successor, Some(next.clone()));

    assert_eq!(
        outcome(Attributes::WorkflowExecutionTimedOutEventAttributes(
            WorkflowExecutionTimedOutEventAttributes {
                new_execution_run_id: "run-2".to_owned(),
                ..Default::default()
            },
        )),
        WorkflowOutcome::TimedOut {
            successor: Some(next.clone()),
        }
    );
    assert_eq!(
        outcome(Attributes::WorkflowExecutionCompletedEventAttributes(
            WorkflowExecutionCompletedEventAttributes {
                new_execution_run_id: "run-2".to_owned(),
                ..Default::default()
            },
        )),
        WorkflowOutcome::Completed {
            result: Vec::new(),
            successor: Some(next),
        }
    );
}

#[test]
/// Rejects terminal Core events that omit required failure/successor data.
fn malformed_terminal_history_events_fail_closed() {
    let failed = HistoryEvent {
        attributes: Some(Attributes::WorkflowExecutionFailedEventAttributes(
            Default::default(),
        )),
        ..Default::default()
    };
    assert!(outcome_from_event(failed, "default", "id").is_err());

    let continued = HistoryEvent {
        attributes: Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
            Default::default(),
        )),
        ..Default::default()
    };
    assert!(outcome_from_event(continued, "default", "id").is_err());
}

/// The exact-run client shares the same Core application conversion as
/// workflow activations, including benign severity and exact retry delay.
#[test]
fn terminal_client_failure_retains_application_options() {
    use temporalio_common::protos::temporal::api::{
        enums::v1::ApplicationErrorCategory,
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
        history::v1::WorkflowExecutionFailedEventAttributes,
    };
    let failure = Failure {
        message: "expected failure".into(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "ExpectedError".into(),
                category: i32::from(ApplicationErrorCategory::Benign),
                next_retry_delay: Some(prost_wkt_types::Duration {
                    seconds: 3,
                    nanos: 7,
                }),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let event = HistoryEvent {
        attributes: Some(Attributes::WorkflowExecutionFailedEventAttributes(
            WorkflowExecutionFailedEventAttributes {
                failure: Some(failure),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let outcome = outcome_from_event(event, "default", "id").unwrap();
    let WorkflowOutcome::Failed { failure, .. } = &outcome else {
        panic!("expected failed client outcome");
    };
    assert!(matches!(
        &failure.info,
        workflow_protocol::FailureInfo::Application {
            category: workflow_protocol::ApplicationFailureCategory::Benign,
            next_retry_delay: Some(workflow_protocol::Duration {
                seconds: 3,
                nanoseconds: 7,
            }),
            ..
        }
    ));
    let response = WaitWorkflowResponse {
        execution: ExecutionRef {
            namespace: "default".into(),
            workflow_id: "id".into(),
            run_id: "run".into(),
        },
        outcome,
    };
    let encoded = encode_wait_response(&response).unwrap();
    assert!(encoded.contains("\"category\":\"benign\""));
    assert!(encoded.contains("\"next_retry_delay\""));
}

#[test]
/// Confirms continued-as-new output retains the successor run identity.
fn continued_as_new_response_preserves_successor_metadata() {
    let response = WaitWorkflowResponse {
        execution: ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "id".to_owned(),
            run_id: "run-1".to_owned(),
        },
        outcome: WorkflowOutcome::ContinuedAsNew {
            successor: SuccessorRef {
                namespace: "default".to_owned(),
                workflow_id: "id".to_owned(),
                run_id: "run-2".to_owned(),
            },
        },
    };
    let encoded = encode_wait_response(&response).unwrap();
    let decoded: WaitWorkflowResponse = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, response);
    assert!(encoded.contains("run-2"));
}

#[test]
/// Keeps the public failure JSON unchanged after boxing the recursive
/// failure tree used to keep `WorkflowOutcome` compact in memory.
fn failed_response_round_trips_with_boxed_failure_tree() {
    let response = WaitWorkflowResponse {
        execution: ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
        },
        outcome: WorkflowOutcome::Failed {
            failure: Box::new(workflow_protocol::Failure {
                message: "failed".to_owned(),
                source: "workflow".to_owned(),
                stack_trace: String::new(),
                encoded_attributes: None,
                cause: None,
                info: workflow_protocol::FailureInfo::Application {
                    type_name: "application".to_owned(),
                    non_retryable: false,
                    details: Vec::new(),
                    category: workflow_protocol::ApplicationFailureCategory::Unspecified,
                    next_retry_delay: None,
                },
            }),
            successor: None,
        },
    };

    let encoded = encode_wait_response(&response).expect("failed response encodes");
    let decoded: WaitWorkflowResponse =
        serde_json::from_str(&encoded).expect("failed response decodes");
    assert_eq!(decoded, response);
    assert!(encoded.contains(r#""kind":"failed""#));
}

#[test]
/// Rejects a successor that changes namespace or workflow identity.
fn successor_must_remain_in_the_waited_execution() {
    let response = WaitWorkflowResponse {
        execution: ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "id".to_owned(),
            run_id: "run-1".to_owned(),
        },
        outcome: WorkflowOutcome::ContinuedAsNew {
            successor: SuccessorRef {
                namespace: "other".to_owned(),
                workflow_id: "id".to_owned(),
                run_id: "run-2".to_owned(),
            },
        },
    };
    assert!(encode_wait_response(&response).is_err());
}

#[test]
/// Rejects a successor that reuses the run being observed.
fn successor_must_use_a_new_run_id() {
    let response = WaitWorkflowResponse {
        execution: ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "id".to_owned(),
            run_id: "run-1".to_owned(),
        },
        outcome: WorkflowOutcome::Completed {
            result: Vec::new(),
            successor: Some(SuccessorRef {
                namespace: "default".to_owned(),
                workflow_id: "id".to_owned(),
                run_id: "run-1".to_owned(),
            }),
        },
    };
    assert!(encode_wait_response(&response).is_err());
}

#[test]
/// Keeps AlreadyStarted diagnostics closed and free of raw RPC text.
fn already_started_error_is_closed_and_does_not_include_rpc_text() {
    let error = ClientOperationError::AlreadyStarted {
        workflow_id: "id".to_owned(),
        existing_run_id: Some("run".to_owned()),
    };
    let value: ClientErrorDocument = serde_json::from_str(&error.to_json()).unwrap();
    assert_eq!(
        value,
        ClientErrorDocument::AlreadyStarted {
            workflow_id: "id".to_owned(),
            existing_run_id: Some("run".to_owned())
        }
    );
    assert!(!error.to_json().contains("server"));
}

#[test]
/// Keeps a timed-out termination distinguishable from an ordinary RPC
/// rejection because the server may have accepted the command already.
fn termination_timeout_error_is_explicitly_uncertain() {
    let error = ClientOperationError::Rpc {
        code: "termination_outcome_uncertain".to_owned(),
    };
    assert_eq!(
        error.to_json(),
        r#"{"kind":"rpc","code":"termination_outcome_uncertain"}"#
    );
}

#[test]
/// Converts a start-only AlreadyExists status into the typed error variant.
fn start_status_maps_already_exists_to_already_started() {
    let error = map_start_status("workflow-1", Status::new(Code::AlreadyExists, "server"));
    assert_eq!(
        error,
        ClientOperationError::AlreadyStarted {
            workflow_id: "workflow-1".to_owned(),
            existing_run_id: None,
        }
    );
}

/// Constructs one length-delimited protobuf field for the small status
/// detail fixture below. The test values are deliberately short, so the
/// one-byte length encoding keeps the fixture readable without adding a
/// protobuf dependency solely to manufacture a gRPC error.
fn length_delimited_field(tag: u8, value: &[u8]) -> Vec<u8> {
    assert!(value.len() < 128, "fixture field must fit one-byte length");
    let mut field = vec![tag, value.len() as u8];
    field.extend_from_slice(value);
    field
}

/// Encodes the `google.rpc.Status -> Any -> AlreadyStartedFailure` shape
/// returned by Temporal so the test exercises the same decoder path used
/// for real server errors, including Rust strings with an embedded NUL.
fn already_started_status_with_run_id(run_id: &str) -> Status {
    let failure = length_delimited_field(0x12, run_id.as_bytes());
    let type_url =
        b"type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure";
    let mut any = length_delimited_field(0x0a, type_url);
    any.extend(length_delimited_field(0x12, &failure));
    let details = length_delimited_field(0x1a, &any);
    Status::with_details(Code::AlreadyExists, "server", Bytes::from(details))
}

#[test]
/// Preserves the AlreadyStarted category while omitting malformed
/// server-provided run IDs that violate the bilateral JSON contract.
fn already_started_status_drops_nul_run_id_from_error_body() {
    let valid = map_start_status("workflow-1", already_started_status_with_run_id("run-1"));
    assert_eq!(
        valid.to_json(),
        r#"{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":"run-1"}"#
    );

    let malformed = map_start_status(
        "workflow-1",
        already_started_status_with_run_id("run-\0-invalid"),
    );
    assert_eq!(
        malformed.to_json(),
        r#"{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}"#
    );
}

#[test]
/// Keeps wait failures as RPC errors even when a proxy returns
/// `AlreadyExists`, which has start-only meaning in this adapter.
fn wait_status_does_not_use_start_error_mapping() {
    let error = map_rpc_status(Status::new(Code::AlreadyExists, "server text"));
    assert_eq!(
        error,
        ClientOperationError::Rpc {
            code: "already_exists".to_owned()
        }
    );
}

#[test]
/// Uses explicit snake-case names instead of tonic's unstable debug text.
fn rpc_status_codes_preserve_word_boundaries() {
    assert_eq!(
        map_rpc_status(Status::new(Code::DeadlineExceeded, "timeout")),
        ClientOperationError::Rpc {
            code: "deadline_exceeded".to_owned()
        }
    );
    assert_eq!(
        map_rpc_status(Status::new(Code::PermissionDenied, "denied")),
        ClientOperationError::Rpc {
            code: "permission_denied".to_owned()
        }
    );
}

#[test]
/// Maps Core conversion categories to stable, closed error codes.
fn core_conversion_errors_use_stable_codes() {
    let error =
        ClientOperationError::Core(workflow_protocol::unsupported("unsupported client option"));
    assert_eq!(
        error.to_json(),
        r#"{"kind":"protocol","code":"core_unsupported"}"#
    );
}

#[test]
/// Accepts a workflow start with no argument payloads.
fn empty_payload_vector_is_valid_for_no_argument_workflow() {
    let json = serde_json::json!({
        "request_id":"request-1",
        "namespace":"default",
        "workflow_id":"workflow-1",
        "workflow_type":"smoke",
        "task_queue":"queue",
        "input":[]
    })
    .to_string();
    assert_eq!(decode_start_request(&json).unwrap().input, Vec::new());
}

#[test]
/// Retains named start metadata as owned values and rejects duplicate
/// keys before protobuf map conversion can overwrite one entry.
fn start_metadata_is_closed_and_duplicate_safe() {
    let json = serde_json::json!({
        "request_id":"request-1",
        "namespace":"default",
        "workflow_id":"workflow-1",
        "workflow_type":"smoke",
        "task_queue":"queue",
        "input":[],
        "memo":[{"key":"owner","value":{"metadata":{},"data":{"encoding":"base64","data":"b3duZXI="}}}],
        "search_attributes":[{"key":"priority","value":{"metadata":{},"data":{"encoding":"base64","data":"aGlnaA=="}}}]
    })
    .to_string();
    let request = decode_start_request(&json).unwrap();
    assert_eq!(request.memo.len(), 1);
    assert_eq!(request.search_attributes[0].key, "priority");

    // Build the duplicate through a JSON value rather than textual
    // replacement: serde_json is free to choose object-key order, while
    // the validation contract concerns duplicate array entries only.
    let mut duplicate: serde_json::Value = serde_json::from_str(&json).unwrap();
    duplicate["memo"] = serde_json::json!([
        {"key":"owner","value":{"metadata":{},"data":{"encoding":"base64","data":"b3duZXI="}}},
        {"key":"owner","value":{"metadata":{},"data":{"encoding":"base64","data":"b3duZXI="}}}
    ]);
    let duplicate = duplicate.to_string();
    assert!(decode_start_request(&duplicate).is_err());
}

#[test]
/// Keeps update admission and polling round trips closed and preserves a
/// pending outcome as JSON `null` rather than inventing a terminal value.
fn update_request_and_pending_response_round_trip() {
    let request = UpdateWorkflowRequest {
        namespace: "default".to_owned(),
        workflow_id: "workflow-1".to_owned(),
        run_id: "run-1".to_owned(),
        update_id: "update-1".to_owned(),
        update_name: "set_state".to_owned(),
        input: Vec::new(),
        rpc_timeout_ms: None,
    };
    let encoded = serde_json::to_string(&request).unwrap();
    assert_eq!(decode_update_request(&encoded).unwrap(), request);
    let response = UpdateWorkflowResponse {
        update_id: request.update_id.clone(),
        execution: ExecutionRef {
            namespace: request.namespace.clone(),
            workflow_id: request.workflow_id.clone(),
            run_id: request.run_id.clone(),
        },
        outcome: None,
    };
    let encoded_response = encode_update_response(&response).unwrap();
    assert!(encoded_response.contains(r#""outcome":null"#));
    assert_eq!(
        serde_json::from_str::<UpdateWorkflowResponse>(&encoded_response).unwrap(),
        response
    );
}

#[test]
/// Rejects update responses with an empty echoed ID before they can cross
/// the native boundary; OCaml performs the request-correlation check.
fn update_response_rejects_invalid_update_id() {
    let response = UpdateWorkflowResponse {
        update_id: String::new(),
        execution: ExecutionRef {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
        },
        outcome: Some(UpdateOutcome::Completed { result: Vec::new() }),
    };
    assert!(encode_update_response(&response).is_err());
}

#[test]
/// Preserves a structured failure outcome through the bilateral JSON
/// codec instead of flattening it into server diagnostic text.
fn update_failure_response_round_trip() {
    let response = PollWorkflowUpdateResponse {
        outcome: Some(UpdateOutcome::Failed {
            failure: Box::new(workflow_protocol::Failure {
                message: "failed".to_owned(),
                source: "worker".to_owned(),
                stack_trace: String::new(),
                encoded_attributes: None,
                cause: None,
                info: workflow_protocol::FailureInfo::Application {
                    type_name: "Failure".to_owned(),
                    non_retryable: true,
                    details: Vec::new(),
                    category: workflow_protocol::ApplicationFailureCategory::Unspecified,
                    next_retry_delay: None,
                },
            }),
        }),
    };
    let encoded = encode_poll_update_response(&response).unwrap();
    assert_eq!(
        serde_json::from_str::<PollWorkflowUpdateResponse>(&encoded).unwrap(),
        response
    );
}

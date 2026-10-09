use std::ptr;

use ocaml_temporal_core_bridge::worker_bridge::{
    AdmitError, CompleteError, PollLaneError, WorkerBridgeError, public_poll_lane_error_message,
    public_worker_error_message,
};
use ocaml_temporal_core_bridge::{
    ABI_VERSION, Buffer, Result as AbiResult, STATUS_ABI_MISMATCH, STATUS_INVALID_ARGUMENT,
    STATUS_INVALID_STATE, STATUS_NOT_READY, STATUS_OK, STATUS_PANIC, STATUS_PROTOCOL,
    STATUS_RETRYABLE, ocaml_temporal_core_v4_check_abi_version,
    ocaml_temporal_core_v4_client_await_call, ocaml_temporal_core_v4_client_release_call,
    ocaml_temporal_core_v4_client_submit_json, ocaml_temporal_core_v4_conformance_wait_ms,
    ocaml_temporal_core_v4_echo, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_dispose, ocaml_temporal_core_v4_runtime_free,
    ocaml_temporal_core_v4_runtime_new, ocaml_temporal_core_v4_worker_complete_activity_json,
    ocaml_temporal_core_v4_worker_complete_workflow_json,
    ocaml_temporal_core_v4_worker_record_activity_heartbeat_json,
    ocaml_temporal_core_v4_worker_reject_activity_json,
    ocaml_temporal_core_v4_worker_reject_workflow_json,
    ocaml_temporal_core_v4_worker_try_poll_activity,
    ocaml_temporal_core_v4_worker_try_poll_workflow, ocaml_temporal_core_v4_worker_wait_activity,
    ocaml_temporal_core_v4_worker_wait_activity_completion_retry_backoff,
    ocaml_temporal_core_v4_worker_wait_any, ocaml_temporal_core_v4_worker_wait_workflow,
    test_invoke_panic, test_worker_bridge_status,
};

/// Produces writable initialized storage matching the C caller contract.
fn empty_result() -> AbiResult {
    AbiResult::default()
}

/// Copies a live bridge buffer for assertions without taking its ownership.
fn bytes(buffer: &Buffer) -> Vec<u8> {
    if buffer.ptr.is_null() {
        assert_eq!(buffer.len, 0);
        Vec::new()
    } else {
        // SAFETY: Tests read buffers returned by the bridge before freeing the
        // containing result, so the allocation remains live for this copy.
        unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len).to_vec() }
    }
}

#[test]
/// Proves Core completion diagnostics are reduced to closed categories before
/// the ABI maps them into an OCaml-facing result.
fn worker_error_categories_never_include_core_diagnostics() {
    let hostile = "tonic::Status { message: secret-core-diagnostic }";
    let errors = [
        WorkerBridgeError::CoreWorkflow(hostile.to_owned()),
        WorkerBridgeError::CoreActivity(hostile.to_owned()),
        WorkerBridgeError::Completion(CompleteError::UnknownWorkflow),
        WorkerBridgeError::Completion(CompleteError::UnknownActivity),
        WorkerBridgeError::Completion(CompleteError::NotLeased),
        WorkerBridgeError::Completion(CompleteError::AlreadyLeased),
        WorkerBridgeError::OutstandingTasks(7),
        WorkerBridgeError::WorkerStillShared,
        WorkerBridgeError::RetryableActivityCompletion,
    ];

    for error in errors {
        let message = public_worker_error_message(&error);
        assert!(!message.contains(hostile), "{message}");
        assert!(!message.contains("tonic"), "{message}");
        assert!(!message.contains("secret-core-diagnostic"), "{message}");
    }
}

#[test]
/// Confirms the reserved completion retry category has an explicit ABI status
/// while ordinary Core failures remain the non-retryable worker category.
fn retryable_completion_uses_dedicated_status() {
    assert_eq!(
        test_worker_bridge_status(WorkerBridgeError::RetryableActivityCompletion),
        STATUS_RETRYABLE
    );
    assert_eq!(
        test_worker_bridge_status(WorkerBridgeError::CoreActivity(
            "transport outcome unavailable".to_owned()
        )),
        ocaml_temporal_core_bridge::STATUS_WORKER
    );
}

#[test]
/// Proves a poll-lane Core/gRPC diagnostic is discarded at the ABI mapping
/// point while non-Core lane categories remain closed constants as well.
fn poll_lane_categories_never_include_core_diagnostics() {
    let hostile = "tonic::Status { message: secret-core-diagnostic }";
    let errors = [
        PollLaneError::Core(hostile.to_owned()),
        PollLaneError::Admission(AdmitError::Draining),
        PollLaneError::Admission(AdmitError::InvalidIdentity),
        PollLaneError::Admission(AdmitError::UnknownActivityCancellation),
        PollLaneError::Admission(AdmitError::Retired),
        PollLaneError::DuplicateIdentity,
        PollLaneError::InvalidActivityVariant,
    ];

    for error in errors {
        let message = public_poll_lane_error_message(&error);
        assert!(!message.contains(hostile), "{message}");
        assert!(!message.contains("tonic"), "{message}");
        assert!(!message.contains("secret-core-diagnostic"), "{message}");
    }
}

#[test]
/// Confirms supported negotiation returns a canonical empty success.
fn negotiates_the_supported_abi_version() {
    let mut result = empty_result();

    let status = unsafe { ocaml_temporal_core_v4_check_abi_version(ABI_VERSION, &mut result) };

    assert_eq!(status, STATUS_OK);
    assert_eq!(result.status, STATUS_OK);
    assert!(result.value.ptr.is_null());
    assert!(result.error.ptr.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// Confirms mismatches return Rust-owned diagnostics that callers can free.
fn reports_an_owned_error_for_an_unsupported_version() {
    let mut result = empty_result();

    let status = unsafe { ocaml_temporal_core_v4_check_abi_version(ABI_VERSION + 1, &mut result) };

    assert_eq!(status, STATUS_ABI_MISMATCH);
    assert_eq!(result.status, STATUS_ABI_MISMATCH);
    assert!(result.value.ptr.is_null());
    assert!(!bytes(&result.error).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// Rejects the previous ABI number so an old OCaml/C object cannot silently
/// talk to the new worker-versioning contract.  This is the mixed-artifact
/// case that the ABI bump is intended to catch before worker startup.
fn rejects_the_previous_abi_version() {
    let mut result = empty_result();

    let status = unsafe { ocaml_temporal_core_v4_check_abi_version(ABI_VERSION - 1, &mut result) };

    assert_eq!(status, STATUS_ABI_MISMATCH);
    assert_eq!(result.status, STATUS_ABI_MISMATCH);
    assert!(result.value.ptr.is_null());
    assert!(!bytes(&result.error).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// Proves an error diagnostic has the same one-owner, repeat-safe cleanup
/// contract as a successful value. The second call must not dereference or
/// release the first call's allocation a second time.
fn result_free_is_idempotent_for_an_error_result() {
    let mut result = empty_result();

    assert_eq!(
        unsafe { ocaml_temporal_core_v4_check_abi_version(ABI_VERSION + 1, &mut result) },
        STATUS_ABI_MISMATCH
    );
    assert!(!result.error.ptr.is_null());

    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(result, empty_result());
}

#[test]
/// Exercises non-empty ownership transfer and canonical zero-length handling.
fn owns_echoed_bytes_and_supports_zero_length_buffers() {
    let input = b"activation";
    let mut result = empty_result();

    let status = unsafe { ocaml_temporal_core_v4_echo(input.as_ptr(), input.len(), &mut result) };

    assert_eq!(status, STATUS_OK);
    assert_eq!(bytes(&result.value), input);
    assert!(result.error.ptr.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );

    let status = unsafe { ocaml_temporal_core_v4_echo(ptr::null(), 0, &mut result) };
    assert_eq!(status, STATUS_OK);
    assert!(result.value.ptr.is_null());
    assert_eq!(result.value.len, 0);
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// Ensures required null pointers fail without dereference or allocation leak.
fn rejects_null_required_pointers() {
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_check_abi_version(ABI_VERSION, ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_echo(ptr::null(), 1, ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
}

/// Keeps Core-facing activation rejection diagnostics bounded to static bridge
/// categories instead of reflecting workflow-controlled identifiers or data.
#[test]
fn workflow_rejection_message_contains_only_static_reason() {
    let message = ocaml_temporal_core_bridge::worker_bridge::workflow_rejection_message(
        "Core activation job kind is not supported",
    );

    assert_eq!(
        message,
        "OCaml bridge could not represent the workflow activation: Core activation job kind is not supported"
    );
    assert!(!message.contains("workflow_id"));
    assert!(!message.contains("run_id"));
    assert!(!message.contains("payload"));
}

/// Confirms every new private poll/completion symbol initializes its result and
/// rejects a missing runtime before touching semantic input.
#[test]
fn task_bridge_exports_reject_null_runtime_handles() {
    let mut result = empty_result();
    let statuses = [
        unsafe { ocaml_temporal_core_v4_worker_try_poll_workflow(ptr::null_mut(), &mut result) },
        {
            assert_eq!(
                unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
                STATUS_OK
            );
            unsafe { ocaml_temporal_core_v4_worker_try_poll_activity(ptr::null_mut(), &mut result) }
        },
        {
            assert_eq!(
                unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
                STATUS_OK
            );
            unsafe { ocaml_temporal_core_v4_worker_wait_workflow(ptr::null_mut(), &mut result) }
        },
        {
            assert_eq!(
                unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
                STATUS_OK
            );
            unsafe { ocaml_temporal_core_v4_worker_wait_activity(ptr::null_mut(), &mut result) }
        },
        {
            assert_eq!(
                unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
                STATUS_OK
            );
            unsafe { ocaml_temporal_core_v4_worker_wait_any(ptr::null_mut(), &mut result) }
        },
        {
            assert_eq!(
                unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
                STATUS_OK
            );
            unsafe {
                ocaml_temporal_core_v4_worker_wait_activity_completion_retry_backoff(
                    ptr::null_mut(),
                    &mut result,
                )
            }
        },
        {
            assert_eq!(
                unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
                STATUS_OK
            );
            unsafe {
                ocaml_temporal_core_v4_worker_complete_workflow_json(
                    ptr::null_mut(),
                    ptr::null(),
                    0,
                    &mut result,
                )
            }
        },
        {
            assert_eq!(
                unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
                STATUS_OK
            );
            unsafe {
                ocaml_temporal_core_v4_worker_complete_activity_json(
                    ptr::null_mut(),
                    ptr::null(),
                    0,
                    &mut result,
                )
            }
        },
        {
            assert_eq!(
                unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
                STATUS_OK
            );
            unsafe {
                ocaml_temporal_core_v4_worker_reject_workflow_json(
                    ptr::null_mut(),
                    ptr::null(),
                    0,
                    &mut result,
                )
            }
        },
        {
            assert_eq!(
                unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
                STATUS_OK
            );
            unsafe {
                ocaml_temporal_core_v4_worker_reject_activity_json(
                    ptr::null_mut(),
                    ptr::null(),
                    0,
                    &mut result,
                )
            }
        },
    ];
    for status in statuses {
        assert_eq!(status, STATUS_INVALID_ARGUMENT);
    }
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
}

/// A live runtime cannot reject a syntactically valid document until a poll
/// has retained that exact semantic delivery, even before worker-state checks.
#[test]
fn task_rejection_requires_retained_delivery_before_worker_state() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );

    let workflow = br#"{"run_id":"unleased-run","timestamp":{"seconds":1,"nanoseconds":0},"is_replaying":false,"history_length":1,"jobs":[]}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v4_worker_reject_workflow_json(
                runtime,
                workflow.as_ptr(),
                workflow.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );

    let activity = br#"{"task_token":"AAEC","variant":{"kind":"cancel","reason":"worker_shutdown","details":null}}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v4_worker_reject_activity_json(
                runtime,
                activity.as_ptr(),
                activity.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

/// Proves malformed heartbeat input is rejected before worker access and that
/// the owned error buffer can be freed before the same result slot is reused.
/// The second, valid heartbeat reaches the lifecycle check and therefore
/// demonstrates that no malformed payload state leaked into the next call.
#[test]
fn malformed_heartbeat_is_rejected_and_result_cleanup_is_reusable() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );

    let malformed = br#"{"task_token":"AA==","details":[{"metadata":{},"data":{"encoding":"raw","data":"AA=="}}]}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v4_worker_record_activity_heartbeat_json(
                runtime,
                malformed.as_ptr(),
                malformed.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(result.status, STATUS_PROTOCOL);
    assert!(!result.error.ptr.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(result, empty_result());

    let valid = br#"{"task_token":"AA==","details":[]}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v4_worker_record_activity_heartbeat_json(
                runtime,
                valid.as_ptr(),
                valid.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert_eq!(result.status, STATUS_INVALID_STATE);
    assert!(!result.error.ptr.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

/// Verifies readiness waits reject a missing worker without entering a native
/// condvar wait, preserving the owner-domain lifecycle contract.
#[test]
fn readiness_waits_require_a_running_worker() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_worker_wait_workflow(runtime, &mut result) },
        ocaml_temporal_core_bridge::STATUS_INVALID_STATE
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_worker_wait_activity(runtime, &mut result) },
        ocaml_temporal_core_bridge::STATUS_INVALID_STATE
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_worker_wait_any(runtime, &mut result) },
        ocaml_temporal_core_bridge::STATUS_INVALID_STATE
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Verifies explicit cleanup resets ownership and tolerates repeated cleanup.
fn result_free_is_idempotent_for_the_same_result_object() {
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_echo(b"owned".as_ptr(), 5, &mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(result, empty_result());
}

#[test]
/// Proves a Rust panic becomes an owned error rather than unwinding through C.
fn contains_rust_panics_as_owned_errors() {
    let mut result = empty_result();

    let status = unsafe { test_invoke_panic(&mut result) };

    assert_eq!(status, STATUS_PANIC);
    assert_eq!(result.status, STATUS_PANIC);
    assert!(result.value.ptr.is_null());
    assert!(!bytes(&result.error).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// Submitted client calls (#807) reject a null runtime, and awaiting a call
/// is bounded: an identifier that was never issued reports the closed
/// invalid-state status without blocking, and an over-long wait is refused.
fn submitted_client_calls_validate_their_arguments() {
    let wait = br#"{"namespace":"default","workflow_id":"w","run_id":"r"}"#;
    let mut result = empty_result();
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v4_client_submit_json(
                ptr::null_mut(),
                2,
                wait.as_ptr(),
                wait.len(),
                &mut result,
            )
        },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_client_await_call(1, u64::MAX, 0, &mut result) },
        STATUS_INVALID_STATE
    );
    assert!(!bytes(&result.error).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_ne!(
        unsafe { ocaml_temporal_core_v4_client_await_call(1, u64::MAX, 60_000, &mut result) },
        STATUS_NOT_READY
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_client_await_call(1, 1, 60_001, &mut result) },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    // Releasing an unknown call is a successful, silent no-op.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_client_release_call(1, u64::MAX, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// Keeps the blocking conformance probe bounded for tests and accidental calls.
fn bounds_the_conformance_wait() {
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_conformance_wait_ms(0, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_conformance_wait_ms(1_001, &mut result) },
        STATUS_INVALID_ARGUMENT
    );
    assert!(!bytes(&result.error).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// Creates one Core/Tokio owner and proves close clears the caller's handle.
fn creates_and_idempotently_closes_a_runtime() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();

    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert!(!runtime.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Rejects missing runtime/result output storage without creating a handle.
fn runtime_creation_rejects_null_output_pointers() {
    // A non-null sentinel proves the function canonicalizes the output slot
    // before returning for the independently invalid result pointer.
    let mut runtime = std::ptr::dangling_mut();
    let mut result = empty_result();

    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_new(ptr::null_mut(), &mut result) },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
    assert!(runtime.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
}

#[test]
/// Transfers GC fallback cleanup without making the caller wait for Core drop.
fn disposes_a_runtime_asynchronously_and_clears_the_handle() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_dispose(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_dispose(&mut runtime) },
        STATUS_OK
    );
}

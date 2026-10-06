use std::ptr;

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, STATUS_CONFIGURATION, STATUS_CONNECTION, STATUS_INVALID_ARGUMENT,
    STATUS_INVALID_STATE, STATUS_OK, ocaml_temporal_core_v3_client_connect_json,
    ocaml_temporal_core_v3_client_disconnect, ocaml_temporal_core_v3_result_free,
    ocaml_temporal_core_v3_runtime_free, ocaml_temporal_core_v3_runtime_new,
    ocaml_temporal_core_v3_worker_shutdown, ocaml_temporal_core_v3_worker_start_json,
};

/// Returns initialized writable result storage matching the public C contract.
fn empty_result() -> AbiResult {
    AbiResult::default()
}

/// Copies one live ABI buffer without taking ownership from its result.
fn bytes(buffer: &Buffer) -> Vec<u8> {
    if buffer.ptr.is_null() {
        assert_eq!(buffer.len, 0);
        Vec::new()
    } else {
        // SAFETY: The bridge owns this readable allocation until the containing
        // result is released at the end of the assertion.
        unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len).to_vec() }
    }
}

/// Creates one live runtime and releases the empty success result immediately.
fn runtime() -> *mut ocaml_temporal_core_bridge::Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        // SAFETY: Both output locations are writable and exclusively owned.
        unsafe { ocaml_temporal_core_v3_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    // SAFETY: The bridge initialized `result` and this test owns it uniquely.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_result_free(&mut result) },
        STATUS_OK
    );
    assert!(!runtime.is_null());
    runtime
}

/// Invokes the JSON client constructor against one live runtime.
fn connect(runtime: *mut ocaml_temporal_core_bridge::Runtime, config: &[u8]) -> AbiResult {
    let mut result = empty_result();
    // SAFETY: The runtime is live, the byte span remains readable for the
    // complete blocking call, and the output is uniquely writable.
    unsafe {
        ocaml_temporal_core_v3_client_connect_json(
            runtime,
            config.as_ptr(),
            config.len(),
            &mut result,
        );
    }
    result
}

/// Invokes workflow-worker construction against one connected runtime.
fn start_worker(runtime: *mut ocaml_temporal_core_bridge::Runtime, config: &[u8]) -> AbiResult {
    let mut result = empty_result();
    // SAFETY: The runtime, input, and result satisfy the same exclusive
    // ownership contract as `connect`.
    unsafe {
        ocaml_temporal_core_v3_worker_start_json(
            runtime,
            config.as_ptr(),
            config.len(),
            &mut result,
        );
    }
    result
}

/// Frees a live result after checking its status and returning its diagnostic.
fn consume(mut result: AbiResult, expected_status: i32) -> String {
    assert_eq!(result.status, expected_status);
    let message = String::from_utf8(bytes(&result.error)).expect("diagnostic is UTF-8");
    // SAFETY: This helper has exclusive ownership of the initialized result.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_result_free(&mut result) },
        STATUS_OK
    );
    message
}

/// Strict lifecycle decoding rejects unknown fields before attempting network
/// I/O, so a typo cannot silently select a default connection behavior. The
/// parser detail is intentionally not echoed: an unknown input field name is
/// application-controlled text and must not cross the native result boundary.
#[test]
fn client_configuration_rejects_unknown_fields_without_echoing_them() {
    let mut runtime = runtime();

    let config = br#"{
        "target_url":"http://127.0.0.1:7233",
        "identity":"test-worker",
        "unexpected":true
    }"#;
    let message = consume(connect(runtime, config), STATUS_CONFIGURATION);
    assert_eq!(message, "invalid lifecycle configuration JSON");
    // SAFETY: The runtime slot is live and uniquely owned.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

/// Syntax failures use the same closed diagnostic as shape failures, so parser
/// implementation details never become part of the C/OCaml error contract.
#[test]
fn malformed_lifecycle_configuration_has_a_closed_diagnostic() {
    let mut runtime = runtime();
    let config = br#"{"target_url":"http://127.0.0.1:7233","identity":"worker","secret"}"#;
    let message = consume(connect(runtime, config), STATUS_CONFIGURATION);
    assert_eq!(message, "invalid lifecycle configuration JSON");

    // SAFETY: Parsing failed before any child was published, and this test
    // retains exclusive ownership of the runtime pointer slot.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

/// URL and required-string validation is completed before any connection is
/// attempted and reports configuration, rather than transport, failure.
#[test]
fn client_configuration_validates_url_and_identity() {
    let mut runtime = runtime();
    let invalid_url = br#"{"target_url":"not a URL","identity":"worker"}"#;
    let message = consume(connect(runtime, invalid_url), STATUS_CONFIGURATION);
    assert!(message.contains("target_url"), "{message}");

    let empty_identity = br#"{"target_url":"http://127.0.0.1:7233","identity":""}"#;
    let message = consume(connect(runtime, empty_identity), STATUS_CONFIGURATION);
    assert!(message.contains("identity must not be empty"), "{message}");

    // SAFETY: The runtime remains live because both failures occurred before
    // graph mutation, and this test exclusively owns its pointer slot.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

/// A refused endpoint is an expected connection failure, leaves no partially
/// published client, and can be retried without an invalid-state error.
#[test]
fn connection_failure_is_structured_and_rolls_back_state() {
    let mut runtime = runtime();
    let config = br#"{"target_url":"http://127.0.0.1:1","identity":"worker"}"#;
    for _ in 0..2 {
        let message = consume(connect(runtime, config), STATUS_CONNECTION);
        // The closed cause and transport detail are covered in
        // `connection_diagnostics.rs`; this test owns the rollback contract.
        assert!(
            message.starts_with("Temporal client connection failed (cause=refused)"),
            "{message}"
        );
    }
    // SAFETY: Failed constructors retained no child resource, and the runtime
    // slot is exclusively owned by this test.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

/// Worker construction requires an already connected client and never
/// publishes a child merely because its JSON was valid.
#[test]
fn worker_requires_connected_client() {
    let mut runtime = runtime();
    let config = br#"{
        "namespace":"temporal-sdk-test",
        "task_queue":"lifecycle-test",
        "build_id":"test-build",
        "versioning":{"kind":"none"},
        "max_cached_workflows":100,
        "max_outstanding_workflow_tasks":100,
        "max_concurrent_workflow_task_polls":5,
        "graceful_shutdown_timeout_ms":1000
    }"#;
    let message = consume(start_worker(runtime, config), STATUS_INVALID_STATE);
    assert!(message.contains("client is not connected"), "{message}");
    // SAFETY: No child handle was constructed; the runtime slot is exclusive.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

/// Closing absent children is deliberately idempotent so explicit shutdown,
/// defensive cleanup, and finalization cannot double-free native state.
#[test]
fn repeated_child_close_is_idempotent() {
    let mut runtime = runtime();
    for _ in 0..2 {
        let mut result = empty_result();
        assert_eq!(
            // SAFETY: The runtime and result are exclusively owned for the
            // duration of this synchronous operation.
            unsafe { ocaml_temporal_core_v3_worker_shutdown(runtime, &mut result) },
            STATUS_OK
        );
        consume(result, STATUS_OK);
        let mut result = empty_result();
        assert_eq!(
            // SAFETY: The same ownership reasoning applies to client close.
            unsafe { ocaml_temporal_core_v3_client_disconnect(runtime, &mut result) },
            STATUS_OK
        );
        consume(result, STATUS_OK);
    }
    // SAFETY: The runtime slot is valid and uniquely owned.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

/// Null runtime/input/output pointers are contained at the ABI boundary and
/// never reach JSON parsing or Core.
#[test]
fn lifecycle_operations_reject_null_pointers() {
    let config = br#"{"target_url":"http://127.0.0.1:7233","identity":"worker"}"#;
    let mut result = empty_result();
    assert_eq!(
        // SAFETY: This intentionally supplies a null runtime to exercise the
        // documented defensive branch.
        unsafe {
            ocaml_temporal_core_v3_client_connect_json(
                ptr::null_mut(),
                config.as_ptr(),
                config.len(),
                &mut result,
            )
        },
        STATUS_INVALID_ARGUMENT
    );
    consume(result, STATUS_INVALID_ARGUMENT);
}

/// A live server proves the bridge constructs and validates the official Core
/// workflow worker, then destroys worker, client, and runtime repeatedly.
#[test]
#[ignore = "requires the Docker Compose Temporal server"]
fn real_client_worker_lifecycle() {
    let address = std::env::var("TEMPORAL_ADDRESS")
        .expect("TEMPORAL_ADDRESS is required for the ignored integration test");
    let namespace =
        std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "temporal-sdk-test".to_owned());
    let mut runtime = runtime();
    let client =
        format!(r#"{{"target_url":"{address}","identity":"ocaml-temporal-lifecycle-test"}}"#);
    consume(connect(runtime, client.as_bytes()), STATUS_OK);
    let worker = format!(
        r#"{{"namespace":"{namespace}","task_queue":"ocaml-temporal-lifecycle-test","build_id":"lifecycle-test-build","versioning":{{"kind":"none"}},"max_cached_workflows":100,"max_outstanding_workflow_tasks":100,"max_concurrent_workflow_task_polls":5,"graceful_shutdown_timeout_ms":1000}}"#
    );
    consume(start_worker(runtime, worker.as_bytes()), STATUS_OK);

    for _ in 0..2 {
        let mut result = empty_result();
        // SAFETY: The graph owner and result are exclusive to this test.
        unsafe { ocaml_temporal_core_v3_worker_shutdown(runtime, &mut result) };
        consume(result, STATUS_OK);
    }
    for _ in 0..2 {
        let mut result = empty_result();
        // SAFETY: The worker has closed and graph ownership remains exclusive.
        unsafe { ocaml_temporal_core_v3_client_disconnect(runtime, &mut result) };
        consume(result, STATUS_OK);
    }
    // SAFETY: Children are closed, though runtime close also defensively
    // guarantees reverse-order cleanup itself.
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v3_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

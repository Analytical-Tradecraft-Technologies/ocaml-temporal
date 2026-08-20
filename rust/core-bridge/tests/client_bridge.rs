//! ABI-level tests for the private dynamic client start/wait slice.

use std::ptr;

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, STATUS_INVALID_ARGUMENT, STATUS_INVALID_STATE, STATUS_OK,
    STATUS_PROTOCOL, ocaml_temporal_core_v2_client_begin_start_workflow_json,
    ocaml_temporal_core_v2_client_cancel_workflow_json,
    ocaml_temporal_core_v2_client_list_visibility_json,
    ocaml_temporal_core_v2_client_poll_start_workflow_json,
    ocaml_temporal_core_v2_client_query_workflow_json,
    ocaml_temporal_core_v2_client_signal_workflow_json,
    ocaml_temporal_core_v2_client_start_workflow_json,
    ocaml_temporal_core_v2_client_wait_start_workflow_json,
    ocaml_temporal_core_v2_client_wait_workflow_json, ocaml_temporal_core_v2_result_free,
    ocaml_temporal_core_v2_runtime_free, ocaml_temporal_core_v2_runtime_new,
};

/// Produces writable initialized storage matching the ABI result contract.
fn empty_result() -> AbiResult {
    AbiResult::default()
}

/// Copies the Rust-owned diagnostic before the result is freed.
fn error_bytes(result: &AbiResult) -> Vec<u8> {
    if result.error.ptr.is_null() {
        assert_eq!(result.error.len, 0);
        Vec::new()
    } else {
        // SAFETY: The test reads the result before invoking result_free.
        unsafe { std::slice::from_raw_parts(result.error.ptr, result.error.len).to_vec() }
    }
}

/// Minimal valid start document used when testing ABI state handling.
const START_REQUEST: &[u8] = br#"{"request_id":"request-1","namespace":"default","workflow_id":"workflow-1","workflow_type":"smoke","task_queue":"queue","input":[]}"#;
/// Minimal valid exact-run wait document used when testing ABI state handling.
const WAIT_REQUEST: &[u8] =
    br#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"}"#;
/// Minimal valid exact-run signal document used for ABI state tests.
const SIGNAL_REQUEST: &[u8] = br#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","signal_name":"add_document","request_id":"signal-1","input":[]}"#;
/// Minimal valid output-only query document used when testing ABI state handling.
const QUERY_REQUEST: &[u8] = br#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","query_type":"current_state","input":[]}"#;
/// Syntactically valid opaque ticket used for unknown-ticket state tests.
const START_TICKET: &[u8] = br#"{"ticket":"ticket-1"}"#;
/// Minimal valid visibility request used for ABI state tests.
const VISIBILITY_REQUEST: &[u8] =
    br#"{"namespace":"default","query":"","page_size":10,"next_page_token":null}"#;

#[test]
/// Null runtime handles fail without dereferencing the client operations.
fn client_operations_reject_null_runtime() {
    let mut result = empty_result();
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_start_workflow_json(
                ptr::null_mut(),
                START_REQUEST.as_ptr(),
                START_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_wait_workflow_json(
                ptr::null_mut(),
                WAIT_REQUEST.as_ptr(),
                WAIT_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_list_visibility_json(
                ptr::null_mut(),
                VISIBILITY_REQUEST.as_ptr(),
                VISIBILITY_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// A live runtime without a connection rejects start and wait as invalid
/// state, while still returning owned diagnostics that can be freed.
fn client_operations_require_connected_client() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_start_workflow_json(
                runtime,
                START_REQUEST.as_ptr(),
                START_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert!(!error_bytes(&result).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_wait_workflow_json(
                runtime,
                WAIT_REQUEST.as_ptr(),
                WAIT_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert!(!error_bytes(&result).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

#[test]
/// Async start admission and ticket reads require a connected client. Once the
/// lifecycle guard passes, ticket reads still reject tickets that were not
/// created by this runtime owner.
fn async_start_operations_require_connected_client() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_begin_start_workflow_json(
                runtime,
                START_REQUEST.as_ptr(),
                START_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_poll_start_workflow_json(
                runtime,
                START_TICKET.as_ptr(),
                START_TICKET.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_wait_start_workflow_json(
                runtime,
                START_TICKET.as_ptr(),
                START_TICKET.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Malformed JSON is rejected before attempting to access the client graph.
fn client_operations_validate_json_before_state_use() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    let malformed = b"{}";
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_start_workflow_json(
                runtime,
                malformed.as_ptr(),
                malformed.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    let malformed_wait = br#"{}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_wait_workflow_json(
                runtime,
                malformed_wait.as_ptr(),
                malformed_wait.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Cancellation validates its closed JSON request before checking whether the
/// runtime has a Temporal connection. This keeps malformed caller data from
/// being reported as a misleading lifecycle error and proves the cancel ABI
/// uses the same fail-closed boundary as start and wait.
fn client_cancel_validates_json_before_state_use() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    let malformed_cancel = br#"{}"#;
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    // An unconnected runtime would otherwise return STATUS_INVALID_STATE; a
    // protocol status proves validation happened before state lookup.
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_cancel_workflow_json(
                runtime,
                malformed_cancel.as_ptr(),
                malformed_cancel.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

#[test]
/// Signal validation runs before connection-state lookup and returns an owned
/// diagnostic that can be freed without retaining runtime or JSON memory.
fn client_signal_validates_json_before_state_use() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_signal_workflow_json(
                runtime,
                br#"{}"#.as_ptr(),
                2,
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    // A complete request reaches the lifecycle guard and is rejected because
    // this test deliberately has not connected a Temporal client.
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_signal_workflow_json(
                runtime,
                SIGNAL_REQUEST.as_ptr(),
                SIGNAL_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Query validation runs before connection-state lookup, and a complete
/// request reaches the lifecycle guard only after the closed JSON shape has
/// been accepted.
fn client_query_validates_json_before_state_use() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_query_workflow_json(
                runtime,
                br#"{}"#.as_ptr(),
                2,
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_query_workflow_json(
                runtime,
                QUERY_REQUEST.as_ptr(),
                QUERY_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Null input spans are rejected before a client-state lookup or byte read.
fn client_operations_reject_null_input_spans() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_start_workflow_json(runtime, ptr::null(), 1, &mut result)
        },
        STATUS_INVALID_ARGUMENT
    );
    assert!(!error_bytes(&result).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_wait_workflow_json(runtime, ptr::null(), 1, &mut result)
        },
        STATUS_INVALID_ARGUMENT
    );
    assert!(!error_bytes(&result).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Oversized documents fail at the bounded span check without dereferencing a
/// null pointer or entering the connected-client state machine.
fn client_operations_reject_oversized_documents() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    let oversized_len = ocaml_temporal_core_bridge::protocol::MAX_DOCUMENT_BYTES + 1;

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_start_workflow_json(
                runtime,
                ptr::null(),
                oversized_len,
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert!(!error_bytes(&result).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_wait_workflow_json(
                runtime,
                ptr::null(),
                oversized_len,
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert!(!error_bytes(&result).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Clearing a runtime slot makes every later client call fail safely instead
/// of allowing a stale pointer to reach the Rust client graph.
fn client_operations_reject_a_freed_runtime_slot() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_start_workflow_json(
                runtime,
                START_REQUEST.as_ptr(),
                START_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_wait_workflow_json(
                runtime,
                WAIT_REQUEST.as_ptr(),
                WAIT_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// Rust mirrors the public OCaml identifier rule and rejects embedded NULs.
fn client_operations_reject_nul_identifiers() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    let nul_start = br#"{"request_id":"request-1","namespace":"default\u0000","workflow_id":"workflow-1","workflow_type":"smoke","task_queue":"queue","input":[]}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_start_workflow_json(
                runtime,
                nul_start.as_ptr(),
                nul_start.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    let nul_wait = br#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-\u00001"}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v2_client_wait_workflow_json(
                runtime,
                nul_wait.as_ptr(),
                nul_wait.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

// Keep Buffer referenced so this integration test also verifies the public
// result layout remains available to C callers without a private helper.
const _: Option<Buffer> = None;

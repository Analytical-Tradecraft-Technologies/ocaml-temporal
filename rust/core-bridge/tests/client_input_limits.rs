//! ABI regressions for client signal, query, and update input size bounds.
//!
//! Issue #771: these three operations once decoded their request with the
//! generic object parser, which applies the 65,536-byte text limit to base64
//! payload data and therefore rejected any input over 49,152 raw bytes before
//! an RPC was attempted. They must now accept payloads on the same terms as
//! workflow start input (128 MiB per decoded byte field, 192 MiB per document)
//! while still bounding identifiers and metadata keys as ordinary text.
//!
//! Every test uses a runtime without a connected client. A request that passes
//! strict decoding and semantic validation therefore reaches the lifecycle
//! guard and returns `STATUS_INVALID_STATE`; a request rejected by the bridge's
//! size or shape checks returns `STATUS_PROTOCOL`. That distinction proves the
//! bound is enforced before any connection lookup or Temporal RPC.

use std::ptr;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ocaml_temporal_core_bridge::{
    Result as AbiResult, Runtime, STATUS_INVALID_STATE, STATUS_OK, STATUS_PROTOCOL, Status,
    ocaml_temporal_core_v2_client_query_workflow_json,
    ocaml_temporal_core_v2_client_signal_workflow_json,
    ocaml_temporal_core_v2_client_update_workflow_json, ocaml_temporal_core_v2_result_free,
    ocaml_temporal_core_v2_runtime_free, ocaml_temporal_core_v2_runtime_new,
    protocol::{MAX_PAYLOAD_BYTES, MAX_STRING_BYTES},
};

/// Raw payload size whose canonical base64 text is exactly the old
/// 65,536-character string ceiling.
const OLD_LIMIT_RAW_BYTES: usize = MAX_STRING_BYTES / 4 * 3;

/// Canonical padded base64 length of the largest accepted payload field.
const MAX_PAYLOAD_BASE64_BYTES: usize = MAX_PAYLOAD_BYTES.div_ceil(3) * 4;

/// The client operations whose payload-carrying requests share this bound.
#[derive(Clone, Copy, Debug)]
enum Operation {
    /// `ocaml_temporal_core_v2_client_signal_workflow_json`.
    Signal,
    /// `ocaml_temporal_core_v2_client_query_workflow_json`.
    Query,
    /// `ocaml_temporal_core_v2_client_update_workflow_json`.
    Update,
}

/// Every operation covered by issue #771, in a stable test order.
const OPERATIONS: [Operation; 3] = [Operation::Signal, Operation::Query, Operation::Update];

/// Owns one unconnected native runtime for the duration of a test and frees it
/// on drop, so assertion failures cannot leak the runtime slot.
struct UnconnectedRuntime(*mut Runtime);

impl UnconnectedRuntime {
    /// Creates a runtime and releases the creation result's diagnostics.
    fn new() -> Self {
        let mut runtime = ptr::null_mut();
        let mut result = AbiResult::default();
        assert_eq!(
            unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
            STATUS_OK
        );
        assert_eq!(
            unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
            STATUS_OK
        );
        Self(runtime)
    }

    /// Submits one request document and returns its status after freeing the
    /// Rust-owned result buffers.
    fn submit(&mut self, operation: Operation, document: &[u8]) -> Status {
        let mut result = AbiResult::default();
        // SAFETY: `document` outlives the synchronous call, the runtime is live,
        // and `result` is initialized writable storage freed immediately below.
        let status = unsafe {
            match operation {
                Operation::Signal => ocaml_temporal_core_v2_client_signal_workflow_json(
                    self.0,
                    document.as_ptr(),
                    document.len(),
                    &mut result,
                ),
                Operation::Query => ocaml_temporal_core_v2_client_query_workflow_json(
                    self.0,
                    document.as_ptr(),
                    document.len(),
                    &mut result,
                ),
                Operation::Update => ocaml_temporal_core_v2_client_update_workflow_json(
                    self.0,
                    document.as_ptr(),
                    document.len(),
                    &mut result,
                ),
            }
        };
        assert_eq!(
            unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
            STATUS_OK
        );
        status
    }
}

impl Drop for UnconnectedRuntime {
    /// Frees the runtime slot exactly once.
    fn drop(&mut self) {
        assert_eq!(
            unsafe { ocaml_temporal_core_v2_runtime_free(&mut self.0) },
            STATUS_OK
        );
    }
}

/// Builds one closed request document for `operation` whose `input` list holds
/// exactly `payload` (already-rendered payload JSON). `name` replaces the
/// operation's handler name so identifier bounds can be exercised too.
fn request(operation: Operation, name: &str, payload: &str) -> Vec<u8> {
    let execution = r#""namespace":"default","workflow_id":"workflow-1","run_id":"run-1""#;
    match operation {
        Operation::Signal => format!(
            r#"{{{execution},"signal_name":"{name}","request_id":"signal-1","input":[{payload}]}}"#
        ),
        Operation::Query => {
            format!(r#"{{{execution},"query_type":"{name}","input":[{payload}]}}"#)
        }
        Operation::Update => format!(
            r#"{{{execution},"update_id":"update-1","update_name":"{name}","input":[{payload}]}}"#
        ),
    }
    .into_bytes()
}

/// Renders one payload whose metadata is empty and whose data field carries the
/// given canonical base64 text.
fn payload_with_base64(data: &str) -> String {
    format!(r#"{{"metadata":{{}},"data":{{"encoding":"base64","data":"{data}"}}}}"#)
}

/// Renders one payload holding `len` raw bytes of deterministic data.
fn payload_of_len(len: usize) -> String {
    let bytes: Vec<u8> = (0..len).map(|index| (index % 251) as u8).collect();
    payload_with_base64(&STANDARD.encode(bytes))
}

#[test]
/// Payloads at, just above, and well beyond the old 49,152-byte ceiling are
/// accepted by signal, query, and update decoding, matching start input.
fn payloads_above_the_old_text_limit_pass_validation() {
    let mut runtime = UnconnectedRuntime::new();
    assert_eq!(
        STANDARD.encode(vec![0_u8; OLD_LIMIT_RAW_BYTES + 1]).len(),
        MAX_STRING_BYTES + 4,
        "the regression must cross the old base64 string ceiling"
    );
    for raw_len in [
        OLD_LIMIT_RAW_BYTES,
        OLD_LIMIT_RAW_BYTES + 1,
        60_000,
        256 * 1024,
    ] {
        let payload = payload_of_len(raw_len);
        for operation in OPERATIONS {
            assert_eq!(
                runtime.submit(operation, &request(operation, "handler", &payload)),
                STATUS_INVALID_STATE,
                "{operation:?} rejected a {raw_len}-byte payload before the lifecycle guard"
            );
        }
    }
}

#[test]
/// Relaxing the payload-data ceiling must not relax ordinary text: handler
/// names and payload metadata keys remain bounded at 65,536 bytes.
fn identifiers_and_metadata_keys_keep_the_text_limit() {
    let mut runtime = UnconnectedRuntime::new();
    let small = payload_of_len(1);
    let at_limit = "n".repeat(MAX_STRING_BYTES);
    let above_limit = "n".repeat(MAX_STRING_BYTES + 1);
    for operation in OPERATIONS {
        assert_eq!(
            runtime.submit(operation, &request(operation, &at_limit, &small)),
            STATUS_INVALID_STATE,
            "{operation:?} rejected a handler name at the text limit"
        );
        assert_eq!(
            runtime.submit(operation, &request(operation, &above_limit, &small)),
            STATUS_PROTOCOL,
            "{operation:?} accepted a handler name above the text limit"
        );
    }

    let metadata_payload = |key: &str| {
        format!(
            r#"{{"metadata":{{"{key}":{{"encoding":"base64","data":"AA=="}}}},"data":{{"encoding":"base64","data":""}}}}"#
        )
    };
    let key_at_limit = metadata_payload(&"k".repeat(MAX_STRING_BYTES));
    let key_above_limit = metadata_payload(&"k".repeat(MAX_STRING_BYTES + 1));
    for operation in OPERATIONS {
        assert_eq!(
            runtime.submit(operation, &request(operation, "handler", &key_at_limit)),
            STATUS_INVALID_STATE,
            "{operation:?} rejected a metadata key at the text limit"
        );
        assert_eq!(
            runtime.submit(operation, &request(operation, "handler", &key_above_limit)),
            STATUS_PROTOCOL,
            "{operation:?} accepted a metadata key above the text limit"
        );
    }
}

#[test]
/// The new upper bound is explicit: one payload field of exactly 128 MiB is
/// accepted, and base64 text one quantum longer than the largest canonical
/// field is rejected before the lifecycle guard for every operation.
///
/// This test allocates several hundred MiB, so it lives in its own test
/// binary and builds one large document at a time.
fn payload_field_bound_matches_start_input() {
    let mut runtime = UnconnectedRuntime::new();

    // Exactly MAX_PAYLOAD_BYTES zero bytes: "AAAA" encodes three zero bytes,
    // and the 2-byte remainder is the canonical padded "AA==" quantum.
    assert_eq!(MAX_PAYLOAD_BYTES % 3, 2);
    let mut at_bound = "AAAA".repeat(MAX_PAYLOAD_BYTES / 3);
    at_bound.push_str("AA==");
    assert_eq!(at_bound.len(), MAX_PAYLOAD_BASE64_BYTES);
    let document = request(
        Operation::Signal,
        "handler",
        &payload_with_base64(&at_bound),
    );
    drop(at_bound);
    assert_eq!(
        runtime.submit(Operation::Signal, &document),
        STATUS_INVALID_STATE,
        "a 128 MiB signal payload was rejected"
    );
    drop(document);

    let above_bound = "A".repeat(MAX_PAYLOAD_BASE64_BYTES + 4);
    let payload = payload_with_base64(&above_bound);
    drop(above_bound);
    for operation in OPERATIONS {
        let document = request(operation, "handler", &payload);
        assert!(document.len() <= ocaml_temporal_core_bridge::protocol::MAX_DOCUMENT_BYTES);
        assert_eq!(
            runtime.submit(operation, &document),
            STATUS_PROTOCOL,
            "{operation:?} accepted a payload field above the 128 MiB bound"
        );
    }
}

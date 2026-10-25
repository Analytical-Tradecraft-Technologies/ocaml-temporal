use std::{fs, path::PathBuf};

use ocaml_temporal_core_bridge::protocol::{
    self, COMPATIBILITY_VERSION, Envelope, MAX_COLLECTION_ITEMS, MAX_DEPTH, MAX_DOCUMENT_BYTES,
    MAX_NODES, MAX_PAYLOAD_BYTES, MAX_STRING_BYTES, Request,
};

/// Locates a shared fixture from the Rust crate within the repository tree.
fn fixture_path(parts: &[&str]) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.extend(["..", "..", "test", "bridge", "fixtures", "protocol"]);
    path.extend(parts);
    path
}

/// Reads one shared fixture without embedding its potentially malformed bytes
/// in assertion output.
fn fixture(parts: &[&str]) -> String {
    fs::read_to_string(fixture_path(parts)).expect("shared protocol fixture must be readable")
}

/// Proves Rust produces exactly the same normalized envelope bytes as OCaml.
#[test]
fn accepts_and_normalizes_valid_envelopes() {
    for name in ["request", "response", "error", "unicode"] {
        let input = fixture(&["valid", &format!("{name}.input.json")]);
        let expected = fixture(&["valid", &format!("{name}.normalized.json")]);
        let decoded = protocol::decode(&input).expect("valid envelope should decode");
        assert_eq!(protocol::encode(&decoded).unwrap(), expected.trim());
        protocol::decode(expected.trim()).expect("normalized envelope should decode");
    }
}

/// Proves malformed shared envelopes fail, particularly duplicate object keys.
#[test]
fn rejects_invalid_envelopes() {
    for name in [
        "duplicate-envelope",
        "duplicate-body",
        "missing-field",
        "unknown-field",
        "wrong-type",
        "invalid-correlation",
        "unknown-kind",
        "non-integral-number",
        "integer-out-of-range",
        "error-unknown-field",
    ] {
        let input = fixture(&["invalid", &format!("{name}.json")]);
        assert!(protocol::decode(&input).is_err(), "{name} was accepted");
    }
}

/// Exercises canonical padded base64 and closed opaque-payload objects.
#[test]
fn validates_and_normalizes_payloads() {
    let input = fixture(&["valid", "payload.input.json"]);
    let expected = fixture(&["valid", "payload.normalized.json"]);
    let bytes = protocol::decode_payload(&input).expect("payload should decode");
    assert_eq!(bytes, [0, 1, 2, 254, 255]);
    assert_eq!(protocol::encode_payload(&bytes).unwrap(), expected.trim());
    let all_bytes = (0..=u8::MAX).collect::<Vec<_>>();
    assert_eq!(
        protocol::decode_payload(&protocol::encode_payload(&all_bytes).unwrap()).unwrap(),
        all_bytes
    );
    // Exercise the server-default blob-limit scale without forcing every CI
    // matrix cell to allocate the bridge's 128 MiB transport safety maximum.
    let maximum = (0..2 * 1024 * 1024)
        .map(|index| (index & usize::from(u8::MAX)) as u8)
        .collect::<Vec<_>>();
    assert_eq!(
        protocol::decode_payload(&protocol::encode_payload(&maximum).unwrap()).unwrap(),
        maximum
    );
    let oversized_encoding = format!(r#"{{"encoding":"{}","data":""}}"#, "a".repeat(65_537));
    assert!(protocol::decode_payload(&oversized_encoding).is_err());
    let oversized_unknown_field = format!(
        r#"{{"encoding":"base64","data":"","extra":"{}"}}"#,
        "a".repeat(65_537)
    );
    assert!(protocol::decode_payload(&oversized_unknown_field).is_err());
    for name in ["payload-invalid-base64", "payload-unknown-field"] {
        assert!(protocol::decode_payload(&fixture(&["invalid", &format!("{name}.json")])).is_err());
    }
}

/// Ensures hostile document, nesting, and decoded-payload sizes fail before use.
#[test]
fn enforces_resource_limits() {
    let prefix = r#"{"kind":"request","correlation_id":"0123456789abcdef0123456789abcdef","operation":"worker.poll","body":"#;
    let deep = format!("{prefix}{}{}{}", "[".repeat(129), "]".repeat(129), "}");
    let long_string = format!("{prefix}{{\"value\":\"{}\"}}}}", "a".repeat(65_537));
    let escaped_long_string = format!("{prefix}{{\"value\":\"{}\"}}}}", "\\\"".repeat(65_537));
    let long_array = format!(
        "{prefix}{{\"values\":[{}]}}}}",
        std::iter::repeat_n("null", 257)
            .collect::<Vec<_>>()
            .join(",")
    );
    assert!(protocol::decode(&deep).is_err());
    assert!(protocol::decode(&long_string).is_err());
    assert!(protocol::decode(&escaped_long_string).is_err());
    assert!(protocol::decode(&long_array).is_ok());
    assert_eq!(MAX_DEPTH, 128);
    assert_eq!(MAX_COLLECTION_ITEMS, MAX_DOCUMENT_BYTES);
    assert_eq!(MAX_NODES, MAX_DOCUMENT_BYTES);
    assert_eq!(MAX_DOCUMENT_BYTES, 192 * 1024 * 1024);
    assert_eq!(MAX_PAYLOAD_BYTES, 128 * 1024 * 1024);
    let maximum_base64_bytes = MAX_PAYLOAD_BYTES.div_ceil(3) * 4;
    assert_eq!(maximum_base64_bytes, 178_956_972);
    assert!(MAX_DOCUMENT_BYTES > maximum_base64_bytes);
    assert!(MAX_DOCUMENT_BYTES < maximum_base64_bytes * 2);
}

/// Checks the once-per-runtime compatibility number and sender-side validation.
#[test]
fn checks_compatibility_and_outgoing_values() {
    protocol::check_compatibility(COMPATIBILITY_VERSION).unwrap();
    assert!(protocol::check_compatibility(u32::MAX).is_err());
    let invalid = Envelope::Request(Request {
        correlation_id: "not-a-correlation-id".to_owned(),
        operation: "worker.poll".to_owned(),
        body: protocol::JsonValue::Object(Vec::new()),
    });
    assert!(protocol::encode(&invalid).is_err());
}

/// Proves semantic protocol modules can reuse the strict object-only boundary.
#[test]
fn validates_operation_specific_objects() {
    assert!(protocol::decode_object(r#"{"outer":{"value":1,"value":2}}"#).is_err());
    let value = protocol::decode_object(r#" {"z":2,"a":{"y":1}} "#).unwrap();
    assert_eq!(
        protocol::encode_object(&value).unwrap(),
        r#"{"a":{"y":1},"z":2}"#
    );
    assert!(protocol::decode_object("[]").is_err());
    assert!(protocol::encode_object(&protocol::JsonValue::Array(Vec::new())).is_err());
}

/// The string limit applies to decoded bytes, as `validate_tree` and the OCaml
/// validators measure it (#1226). A string of escapes is measured by what it
/// decodes to: `\u0001` is 1 byte, a surrogate pair 4, `\n` 1. The preflight
/// counter used to count the hex digits, so 16,385 `\u00XX` escapes (16,385
/// decoded bytes) were refused as over the 65,536-byte limit.
#[test]
fn string_limit_counts_decoded_escape_bytes() {
    let document =
        |escape: &str, count: usize| format!(r#"{{"reason":"{}"}}"#, escape.repeat(count));
    for (escape, bytes) in [
        ("\\u0001", 1),
        ("\\u00e9", 2),
        ("\\u20ac", 3),
        ("\\ud83d\\ude00", 4),
        ("\\n", 1),
    ] {
        let at_limit = MAX_STRING_BYTES / bytes;
        assert!(
            protocol::decode_object(&document(escape, at_limit)).is_ok(),
            "{at_limit} copies of {escape} decode to at most the limit"
        );
        assert!(
            protocol::decode_object(&document(escape, at_limit + 1)).is_err(),
            "{} copies of {escape} decode past the limit",
            at_limit + 1
        );
    }
    assert!(protocol::decode_object(&document("\\u0001", 16_385)).is_ok());
}

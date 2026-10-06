//! Connection-failure causes and Core log formatting (#833).
//!
//! The ABI tests connect to endpoints that fail locally (a closed loopback
//! port and a reserved `.invalid` host name), so they need no Temporal Server.
//! The remaining tests exercise the bounded formatting helpers directly.

use std::collections::HashMap;
use std::ptr;

use ocaml_temporal_core_bridge::diagnostics::{
    ConnectionCause, CoreLogLevel, MAX_CONNECTION_DETAIL_BYTES, MAX_CORE_LOG_LINE_BYTES,
    classify_chain, core_log_filter, format_core_log_line, parse_core_log_level,
};
use ocaml_temporal_core_bridge::{
    Result as AbiResult, STATUS_CONNECTION, STATUS_OK, ocaml_temporal_core_v2_client_connect_json,
    ocaml_temporal_core_v2_result_free, ocaml_temporal_core_v2_runtime_free,
    ocaml_temporal_core_v2_runtime_new,
};

/// Length of the constant message prefix plus the longest cause spelling and
/// separator; the detail bound is added to this to bound the full message.
const MESSAGE_OVERHEAD_BYTES: usize = 128;

/// Creates one live runtime, releasing its empty success result.
fn runtime() -> *mut ocaml_temporal_core_bridge::Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    let status = unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) };
    assert_eq!(status, STATUS_OK);
    // SAFETY: The bridge initialized `result` and this test owns it uniquely.
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    runtime
}

/// Connects a fresh runtime to `target_url`, returns the failure status and
/// copied diagnostic, and releases every native resource.
fn connect_failure(target_url: &str) -> (i32, String) {
    let mut runtime = runtime();
    let config = format!(r#"{{"target_url":"{target_url}","identity":"worker"}}"#);
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live, the input span stays readable for the
    // blocking call, and the result is uniquely writable.
    let status = unsafe {
        ocaml_temporal_core_v2_client_connect_json(
            runtime,
            config.as_ptr(),
            config.len(),
            &mut result,
        )
    };
    assert_eq!(status, result.status);
    // SAFETY: The bridge owns this readable allocation until it is freed below.
    let message = unsafe { std::slice::from_raw_parts(result.error.ptr, result.error.len) };
    let message = String::from_utf8(message.to_vec()).expect("diagnostic is UTF-8");
    // SAFETY: The result and runtime are released exactly once by their owner.
    unsafe {
        assert_eq!(ocaml_temporal_core_v2_result_free(&mut result), STATUS_OK);
        assert_eq!(ocaml_temporal_core_v2_runtime_free(&mut runtime), STATUS_OK);
    }
    (status, message)
}

/// Asserts the shared shape of every connection-failure diagnostic: constant
/// prefix, single line, and bounded length.
fn assert_bounded_connection_message(message: &str) {
    assert!(
        message.starts_with("Temporal client connection failed (cause="),
        "{message}"
    );
    assert!(!message.contains('\n'), "{message}");
    assert!(
        message.len() <= MAX_CONNECTION_DETAIL_BYTES + MESSAGE_OVERHEAD_BYTES,
        "{} bytes: {message}",
        message.len()
    );
}

/// A closed loopback port is reported as a refused connection, and the
/// transport cause text (not only the category) reaches the caller.
#[test]
fn refused_connection_reports_cause_and_transport_detail() {
    let (status, message) = connect_failure("http://127.0.0.1:1");
    assert_eq!(status, STATUS_CONNECTION, "{message}");
    assert_bounded_connection_message(&message);
    assert!(message.contains("(cause=refused): "), "{message}");
    assert!(
        message.to_ascii_lowercase().contains("refused"),
        "detail should carry the OS error text: {message}"
    );
}

/// A reserved, never-resolvable host name is reported as a DNS failure.
#[test]
fn unresolvable_host_reports_dns_cause() {
    let (status, message) = connect_failure("http://ocaml-temporal-issue-833.invalid:7233");
    assert_eq!(status, STATUS_CONNECTION, "{message}");
    assert_bounded_connection_message(&message);
    assert!(message.contains("(cause=dns)"), "{message}");
}

/// Structured I/O error kinds classify without relying on platform wording,
/// and an unrecognized error falls back to no cause.
#[test]
fn io_error_kinds_classify_connection_causes() {
    let cases = [
        (
            std::io::ErrorKind::ConnectionRefused,
            Some(ConnectionCause::Refused),
        ),
        (
            std::io::ErrorKind::ConnectionReset,
            Some(ConnectionCause::Reset),
        ),
        (std::io::ErrorKind::TimedOut, Some(ConnectionCause::Timeout)),
        (std::io::ErrorKind::PermissionDenied, None),
    ];
    for (kind, expected) in cases {
        let error = std::io::Error::new(kind, "opaque");
        assert_eq!(classify_chain(&error), expected, "{kind:?}");
    }
    let tls = std::io::Error::other("invalid peer certificate: UnknownIssuer");
    assert_eq!(classify_chain(&tls), Some(ConnectionCause::Tls));
    let dns = std::io::Error::other("dns error: failed to lookup address information");
    assert_eq!(classify_chain(&dns), Some(ConnectionCause::Dns));
}

/// The environment value selects a level, `off` disables forwarding, unset
/// or blank defaults to `warn`, and invalid text is rejected without echoing.
#[test]
fn core_log_level_parsing() {
    assert_eq!(parse_core_log_level(None), Ok(Some(CoreLogLevel::Warn)));
    assert_eq!(
        parse_core_log_level(Some("  ")),
        Ok(Some(CoreLogLevel::Warn))
    );
    assert_eq!(parse_core_log_level(Some("OFF")), Ok(None));
    assert_eq!(
        parse_core_log_level(Some("Debug")),
        Ok(Some(CoreLogLevel::Debug))
    );
    let error = parse_core_log_level(Some("verbose\nsecret")).unwrap_err();
    assert!(error.contains("OCAML_TEMPORAL_CORE_LOG"), "{error}");
    assert!(!error.contains("secret"), "{error}");
}

/// Temporal crates follow the selected level while third-party transport
/// crates are capped at `warn`.
#[test]
fn core_log_filter_caps_third_party_crates() {
    assert_eq!(
        core_log_filter(CoreLogLevel::Debug),
        "warn,temporalio_common=debug,temporalio_sdk_core=debug,\
         temporalio_client=debug,temporalio_sdk=debug"
    );
    assert!(core_log_filter(CoreLogLevel::Error).starts_with("error,"));
}

/// A Core record becomes exactly one line: control characters are escaped,
/// fields are sorted, and oversized records are cut with a visible marker.
#[test]
fn core_log_lines_are_single_bounded_lines() {
    let mut fields = HashMap::new();
    fields.insert(
        "task_queue".to_owned(),
        serde_json::json!("queue\r\nforged"),
    );
    fields.insert("attempt".to_owned(), serde_json::json!(3));
    let line = format_core_log_line(
        "WARN",
        "temporalio_sdk_core::worker",
        "poll\nfailed",
        &fields,
    );
    assert_eq!(
        line,
        "ocaml-temporal core WARN temporalio_sdk_core::worker: poll\\nfailed \
         attempt=3 task_queue=queue\\r\\nforged\n"
    );

    let huge = "x".repeat(MAX_CORE_LOG_LINE_BYTES * 2);
    let line = format_core_log_line("ERROR", "target", &huge, &HashMap::new());
    assert!(line.ends_with("...[truncated]\n"), "{line}");
    assert_eq!(line.matches('\n').count(), 1);
    assert!(line.len() <= MAX_CORE_LOG_LINE_BYTES + 1);
}

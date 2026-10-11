//! Opt-in Prometheus export on a shared runtime (#500).
//!
//! These tests drive `shared_runtime_new_with_telemetry_json` through the C
//! ABI only. They prove that a configured exporter is really created (its
//! endpoint answers scrapes and attached graphs receive Core's meter), that
//! every rejected document leaves no handle or socket behind, and that the
//! scrape socket is closed by the time the last Core reference is released.
//! Every endpoint binds an ephemeral loopback port, so tests never collide.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ptr;
use std::time::Duration;

use ocaml_temporal_core_bridge::{
    Result as AbiResult, Runtime, STATUS_CONFIGURATION, STATUS_OK, SharedRuntime,
    ocaml_temporal_core_v4_result_free, ocaml_temporal_core_v4_runtime_free,
    ocaml_temporal_core_v4_runtime_new_attached, ocaml_temporal_core_v4_shared_runtime_free,
    ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json, test_runtime_has_metrics_meter,
};

/// Copies one bridge buffer into owned bytes.
fn buffer_bytes(ptr: *const u8, len: usize) -> Vec<u8> {
    if len == 0 {
        return Vec::new();
    }
    // SAFETY: A live result buffer is readable for `len` bytes.
    unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
}

/// Creates a shared runtime from one telemetry document. Returns the status,
/// the (possibly null) handle, and the success value or error message text.
fn create(document: &[u8]) -> (i32, *mut SharedRuntime, String) {
    let mut shared: *mut SharedRuntime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: The document outlives the call and both outputs are writable.
    let status = unsafe {
        ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json(
            1,
            document.as_ptr(),
            document.len(),
            &mut shared,
            &mut result,
        )
    };
    let text = if status == STATUS_OK {
        buffer_bytes(result.value.ptr, result.value.len)
    } else {
        buffer_bytes(result.error.ptr, result.error.len)
    };
    // SAFETY: The result was initialized by the bridge and is freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    (
        status,
        shared,
        String::from_utf8(text).expect("bridge text is UTF-8"),
    )
}

/// Releases a shared handle through its slot, waiting for Core when it holds
/// the last reference.
fn free_shared(shared: &mut *mut SharedRuntime) {
    // SAFETY: The slot holds a live handle or null and is used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(shared) },
        STATUS_OK
    );
    assert!(shared.is_null());
}

/// Sends one HTTP/1.1 GET and returns the raw response text.
fn http_get(address: SocketAddr, path: &str) -> String {
    let mut stream =
        TcpStream::connect_timeout(&address, Duration::from_secs(5)).expect("scrape connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .expect("scrape request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("scrape response");
    response
}

/// A Prometheus document creates a live scrape endpoint on an ephemeral
/// port, reports the resolved address, and gives attached graphs Core's
/// meter. Releasing the last reference closes the socket.
#[test]
fn prometheus_exporter_serves_scrapes_until_core_is_released() {
    let (status, mut shared, address) = create(
        br#"{"prometheus":{"bind_address":"127.0.0.1:0","global_tags":[{"name":"deployment","value":"test"}],"counters_total_suffix":true}}"#,
    );
    assert_eq!(status, STATUS_OK, "{address}");
    assert!(!shared.is_null());
    let address: SocketAddr = address.parse().expect("bound socket address");
    assert!(address.ip().is_loopback());
    assert_ne!(address.port(), 0, "the ephemeral port must be resolved");

    let scrape = http_get(address, "/metrics");
    assert!(scrape.starts_with("HTTP/1.1 200"), "{scrape}");
    assert!(
        scrape
            .to_ascii_lowercase()
            .contains("content-type: text/plain"),
        "{scrape}"
    );
    let missing = http_get(address, "/other");
    assert!(missing.starts_with("HTTP/1.1 404"), "{missing}");

    let mut runtime: *mut Runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: `shared` is live; both outputs are writable.
    let attached =
        unsafe { ocaml_temporal_core_v4_runtime_new_attached(shared, &mut runtime, &mut result) };
    // SAFETY: Initialized by the bridge, freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(attached, STATUS_OK);
    // SAFETY: `runtime` is live and used only here.
    assert_eq!(
        unsafe { test_runtime_has_metrics_meter(runtime) },
        Some(true)
    );

    // Releasing the handle first keeps Core (and the endpoint) alive for the
    // attached graph; the graph's release then destroys Core.
    free_shared(&mut shared);
    assert!(http_get(address, "/metrics").starts_with("HTTP/1.1 200"));
    // SAFETY: The slot holds the live graph and is used by this thread only.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(
        TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_err(),
        "the scrape socket must be closed once Core is destroyed"
    );
}

/// `{}` configures no exporter: the call behaves like `shared_runtime_new`
/// and graphs carry no meter.
#[test]
fn empty_document_creates_no_exporter() {
    let (status, mut shared, address) = create(b"{}");
    assert_eq!(status, STATUS_OK);
    assert!(address.is_empty());
    let mut runtime: *mut Runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: As in the exporter test.
    let attached =
        unsafe { ocaml_temporal_core_v4_runtime_new_attached(shared, &mut runtime, &mut result) };
    // SAFETY: Initialized by the bridge, freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(attached, STATUS_OK);
    // SAFETY: `runtime` is live and used only here.
    assert_eq!(
        unsafe { test_runtime_has_metrics_meter(runtime) },
        Some(false)
    );
    // SAFETY: As above.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
    free_shared(&mut shared);
}

/// Every invalid document is a configuration error that publishes nothing.
#[test]
fn invalid_documents_are_rejected_before_anything_is_published() {
    let long_value = "v".repeat(257);
    let too_many: Vec<String> = (0..17)
        .map(|index| format!(r#"{{"name":"t{index}","value":"x"}}"#))
        .collect();
    let documents = [
        String::new(),
        "not json".to_owned(),
        r#"{"unknown":1}"#.to_owned(),
        r#"{"prometheus":{}}"#.to_owned(),
        r#"{"prometheus":{"bind_address":"localhost:9464"}}"#.to_owned(),
        r#"{"prometheus":{"bind_address":"127.0.0.1"}}"#.to_owned(),
        r#"{"prometheus":{"bind_address":"127.0.0.1:0","extra":true}}"#.to_owned(),
        r#"{"prometheus":{"bind_address":"127.0.0.1:0","global_tags":[{"name":"task_queue","value":"x"}]}}"#.to_owned(),
        r#"{"prometheus":{"bind_address":"127.0.0.1:0","global_tags":[{"name":"__x","value":"x"}]}}"#.to_owned(),
        r#"{"prometheus":{"bind_address":"127.0.0.1:0","global_tags":[{"name":"9x","value":"x"}]}}"#.to_owned(),
        r#"{"prometheus":{"bind_address":"127.0.0.1:0","global_tags":[{"name":"a","value":""}]}}"#.to_owned(),
        r#"{"prometheus":{"bind_address":"127.0.0.1:0","global_tags":[{"name":"a","value":"x\ny"}]}}"#.to_owned(),
        format!(
            r#"{{"prometheus":{{"bind_address":"127.0.0.1:0","global_tags":[{{"name":"a","value":"{long_value}"}}]}}}}"#
        ),
        r#"{"prometheus":{"bind_address":"127.0.0.1:0","global_tags":[{"name":"a","value":"x"},{"name":"a","value":"y"}]}}"#.to_owned(),
        format!(
            r#"{{"prometheus":{{"bind_address":"127.0.0.1:0","global_tags":[{}]}}}}"#,
            too_many.join(",")
        ),
    ];
    for document in documents {
        let (status, shared, message) = create(document.as_bytes());
        assert_eq!(status, STATUS_CONFIGURATION, "{document}: {message}");
        assert!(shared.is_null(), "{document}");
        assert!(
            !message.contains("localhost"),
            "no input is echoed: {message}"
        );
    }
}

/// An address that is already in use fails with a configuration error that
/// names the configured address and the I/O category, and leaves no handle.
#[test]
fn busy_port_is_a_configuration_error() {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("occupy a port");
    let address = occupied.local_addr().expect("occupied address");
    let document = format!(r#"{{"prometheus":{{"bind_address":"{address}"}}}}"#);
    let (status, shared, message) = create(document.as_bytes());
    assert_eq!(status, STATUS_CONFIGURATION, "{message}");
    assert!(shared.is_null());
    assert!(message.contains(&address.to_string()), "{message}");
    assert!(message.contains("AddrInUse"), "{message}");
}

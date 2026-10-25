//! A scraper that sends an incomplete request head cannot stall other
//! scrapes (#1212).
//!
//! The relay used to forward whatever arrived within its 5 s head timeout
//! to Core and then wait up to 5 s for Core's first bytes while holding the
//! shared descriptor reserve. Core's scrape server has no header-read
//! timeout, so it never answers an unterminated head, and every other relay
//! queued behind the reserve: two such clients delayed a real scrape by
//! about 10 s. An incomplete head is now answered `400` without touching
//! Core or the reserve.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Result as AbiResult, STATUS_OK, SharedRuntime, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_shared_runtime_free,
    ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json,
};

/// Creates a shared runtime with a loopback Prometheus endpoint and returns
/// it with the endpoint's address.
fn start_endpoint() -> (*mut SharedRuntime, SocketAddr) {
    let document = br#"{"prometheus":{"bind_address":"127.0.0.1:0"}}"#;
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
    assert_eq!(status, STATUS_OK);
    // SAFETY: A successful result's value buffer is readable for its length.
    let text = unsafe { std::slice::from_raw_parts(result.value.ptr, result.value.len) };
    let address = std::str::from_utf8(text)
        .expect("UTF-8 address")
        .parse()
        .expect("socket address");
    // SAFETY: Initialized by the bridge and freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    (shared, address)
}

/// Performs one complete scrape on a fresh connection and returns the
/// response text.
fn scrape(address: SocketAddr) -> String {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .expect("read timeout");
    write!(
        stream,
        "GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .expect("request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    response
}

/// Two clients send a head without its blank line and then wait. Once their
/// head timeout has passed, a real scrape must still be answered promptly,
/// and each incomplete client must receive a `400`.
#[test]
fn incomplete_request_heads_do_not_stall_scrapes() {
    let (mut shared, address) = start_endpoint();
    assert!(scrape(address).starts_with("HTTP/1.1 200"));

    let partial: Vec<_> = (0..2)
        .map(|_| {
            thread::spawn(move || {
                let mut stream =
                    TcpStream::connect_timeout(&address, Duration::from_secs(5)).expect("connect");
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .expect("read timeout");
                write!(stream, "GET /metrics HTTP/1.1\r\nHost: {address}\r\n")
                    .expect("partial head");
                let mut response = String::new();
                let _ = stream.read_to_string(&mut response);
                response
            })
        })
        .collect();
    // Past the 5 s head timeout, both incomplete relays act at once.
    thread::sleep(Duration::from_millis(5_500));
    let started = Instant::now();
    let response = scrape(address);
    let elapsed = started.elapsed();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        elapsed < Duration::from_secs(3),
        "a scrape waited {elapsed:?} behind incomplete request heads"
    );
    for client in partial {
        let response = client.join().expect("partial client");
        assert!(
            response.starts_with("HTTP/1.1 400"),
            "an incomplete head was not answered 400: {response:?}"
        );
    }

    // SAFETY: The handle is live and released exactly once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(&mut shared) },
        STATUS_OK
    );
}

//! The metrics endpoint survives `accept()` errors (#1030).
//!
//! Core's own scrape server returns from its accept loop on the first error,
//! closing the port for good. The bridge's accept loop must instead retry.
//! This binary provokes a real `EMFILE` from `accept()`: it lowers the
//! process descriptor limit, fills every remaining descriptor, and connects a
//! scraper with the last one, so the server cannot accept it. Descriptor
//! limits are process-wide, so this file holds exactly one test and no other
//! test can run beside it.

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::ptr;
use std::time::Duration;

use ocaml_temporal_core_bridge::{
    Result as AbiResult, STATUS_OK, SharedRuntime, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_shared_runtime_free,
    ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json,
};

/// Descriptor limit used while the endpoint is starved. Small enough to fill
/// quickly, large enough for the runtime's own descriptors.
const STARVED_LIMIT: libc::rlim_t = 256;

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

/// Completes one scrape on an already connected stream and returns the
/// response text.
fn finish_scrape(mut stream: TcpStream, address: SocketAddr) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    write!(
        stream,
        "GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .expect("scrape request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("scrape response");
    response
}

/// Lowers the soft descriptor limit, returning the previous limits.
fn lower_descriptor_limit() -> libc::rlimit {
    let mut previous = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `previous` is a writable `rlimit`.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut previous) },
        0
    );
    let lowered = libc::rlimit {
        rlim_cur: previous.rlim_cur.min(STARVED_LIMIT),
        rlim_max: previous.rlim_max,
    };
    // SAFETY: Lowering the soft limit within the hard limit is permitted.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lowered) }, 0);
    previous
}

/// Duplicates standard input until the process has no free descriptor and
/// returns the duplicates.
fn fill_descriptors() -> Vec<libc::c_int> {
    let mut filler = Vec::new();
    loop {
        // SAFETY: Duplicating a descriptor has no memory-safety effects.
        let fd = unsafe { libc::dup(0) };
        if fd < 0 {
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::EMFILE),
                "descriptor filling stopped for an unexpected reason"
            );
            return filler;
        }
        filler.push(fd);
        assert!(filler.len() < 1_000_000, "descriptor limit was not applied");
    }
}

/// Closes every descriptor in `filler`.
fn release_descriptors(filler: Vec<libc::c_int>) {
    for fd in filler {
        // SAFETY: Each descriptor came from `dup` and is closed once.
        unsafe { libc::close(fd) };
    }
}

/// A scraper connects while the process has no free descriptor, so the
/// endpoint's `accept()` fails with `EMFILE`. Once descriptors are available
/// again, that scraper and a new one are both served: the error did not stop
/// the endpoint.
#[test]
fn the_endpoint_keeps_serving_after_accept_fails_with_emfile() {
    let (mut shared, address) = start_endpoint();
    let warm_up = TcpStream::connect(address).expect("warm-up connect");
    assert!(finish_scrape(warm_up, address).starts_with("HTTP/1.1 200"));

    let previous = lower_descriptor_limit();
    let mut filler = fill_descriptors();
    // Free exactly one descriptor for the scraper's socket. The server's
    // `accept()` then has none and fails with EMFILE (the kernel completes
    // the handshake from the listen backlog regardless).
    let last = filler.pop().expect("at least one filler descriptor");
    // SAFETY: `last` came from `dup` and is closed once.
    unsafe { libc::close(last) };
    let starved = TcpStream::connect(address).expect("connect while starved");
    // Give the accept loop time to hit the error, and to retry several times.
    std::thread::sleep(Duration::from_millis(300));

    release_descriptors(filler);
    // SAFETY: Restoring the limits read above.
    assert_eq!(
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &previous) },
        0
    );

    // Linux leaves the connection in the backlog when accept() fails with
    // EMFILE, so it is served now. macOS dequeues and closes it inside the
    // failing accept(), so the scraper sees an empty, closed response; no
    // server can recover that connection.
    let pending = finish_scrape(starved, address);
    if cfg!(target_os = "linux") {
        assert!(
            pending.starts_with("HTTP/1.1 200"),
            "the connection pending during EMFILE must be served after recovery: {pending:?}"
        );
    } else {
        assert!(
            pending.is_empty() || pending.starts_with("HTTP/1.1 200"),
            "{pending:?}"
        );
    }
    // The property that matters on every platform: the endpoint itself
    // survived the error and serves new scrapes.
    let fresh = TcpStream::connect(address).expect("connect after recovery");
    assert!(finish_scrape(fresh, address).starts_with("HTTP/1.1 200"));

    // SAFETY: The slot holds the live handle and is used by this thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(&mut shared) },
        STATUS_OK
    );
}

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

#[path = "support/descriptor_pressure.rs"]
mod descriptor_pressure;

use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use descriptor_pressure::{
    fill_descriptors, finish_scrape, lower_descriptor_limit, release_descriptors, start_endpoint,
};
use ocaml_temporal_core_bridge::{STATUS_OK, ocaml_temporal_core_v4_shared_runtime_free};

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
    // failing accept(), so the scraper sees the connection closed: either an
    // empty response or, if the close arrives before the request is written,
    // a broken pipe or reset. No server can recover that connection.
    let pending = if cfg!(target_os = "linux") {
        finish_scrape(starved, address)
    } else {
        scrape_closed_or_served(starved, address)
    };
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

/// Completes a scrape on a connection the kernel may already have closed.
/// A broken pipe or reset while writing the request or reading the response
/// means the connection was closed before it could be served, which is
/// reported as an empty response, like a clean close.
fn scrape_closed_or_served(mut stream: TcpStream, address: SocketAddr) -> String {
    use std::io::{ErrorKind, Read, Write};
    let closed = |error: &std::io::Error| {
        matches!(
            error.kind(),
            ErrorKind::BrokenPipe | ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted
        )
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    if let Err(error) = write!(
        stream,
        "GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    ) {
        assert!(closed(&error), "scrape request: {error:?}");
        return String::new();
    }
    let mut response = String::new();
    match stream.read_to_string(&mut response) {
        Ok(_) => response,
        Err(error) if closed(&error) => String::new(),
        Err(error) => panic!("scrape response: {error:?}"),
    }
}

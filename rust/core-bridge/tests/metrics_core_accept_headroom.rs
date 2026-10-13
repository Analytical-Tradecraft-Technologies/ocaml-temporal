//! Descriptor pressure on the public side never starves Core's private
//! `accept()` (#1030).
//!
//! Each relayed scrape needs the public `accept()`, the relay's private
//! `connect()`, and Core's `accept()` of that connection. Core's accept loop
//! ends for good on `EMFILE`, so if only the first two descriptors were
//! available, a naive relay would kill Core's server. This binary keeps the
//! process at its descriptor limit and frees only two or three descriptors
//! per scrape (the in-process scraper's own socket takes one), checking
//! after every round that Core's server is still running. Descriptor limits
//! are process-wide, so this file holds exactly one test.

#![cfg(unix)]

#[path = "support/descriptor_pressure.rs"]
mod descriptor_pressure;

use std::net::TcpStream;
use std::time::Duration;

use descriptor_pressure::{
    fill_descriptors, finish_scrape, lower_descriptor_limit, release_descriptors, start_endpoint,
};
use ocaml_temporal_core_bridge::{
    STATUS_OK, ocaml_temporal_core_v4_shared_runtime_free,
    test_shared_runtime_core_metrics_server_stopped,
};

/// Descriptors freed before each starved scrape. Three leave exactly the
/// scraper's socket, the public `accept()` and the relay's `connect()`, so
/// Core's `accept()` has only what the relay's reserve gives back; two leave
/// the relay nothing to connect with.
const ROUNDS: [usize; 6] = [3, 2, 3, 3, 2, 3];

/// Scrapes while descriptors trickle back in small steps: every answer is a
/// metrics response or the transient "too few descriptors" 503, Core's
/// server never stops, and full service returns once pressure ends.
#[test]
fn starved_scrapes_never_stop_core_metrics_server() {
    let (mut shared, address) = start_endpoint();
    let warm_up = TcpStream::connect(address).expect("warm-up connect");
    assert!(finish_scrape(warm_up, address).starts_with("HTTP/1.1 200"));

    let previous = lower_descriptor_limit();
    let mut filler = fill_descriptors();
    for (round, freed) in ROUNDS.into_iter().enumerate() {
        for _ in 0..freed {
            let fd = filler.pop().expect("a filler descriptor to free");
            // SAFETY: `fd` came from `dup` and is closed once.
            unsafe { libc::close(fd) };
        }
        let stream = TcpStream::connect(address).expect("connect while starved");
        let response = finish_scrape(stream, address);
        if round == 0 {
            // The reserve is full from startup, so the first starved scrape
            // is served: the reserve, not luck, supplied Core's accept().
            assert!(response.starts_with("HTTP/1.1 200"), "{response:?}");
        } else {
            assert!(
                response.starts_with("HTTP/1.1 200")
                    || (response.starts_with("HTTP/1.1 503")
                        && response.contains("Too few file descriptors")),
                "round {round}: {response:?}"
            );
        }
        // SAFETY: `shared` is live and released only below.
        assert_eq!(
            unsafe { test_shared_runtime_core_metrics_server_stopped(shared) },
            Some(false),
            "round {round} stopped Core's metrics server"
        );
        // Let the finished scrape close its sockets, then take every
        // descriptor back so the next round starts at the limit again.
        std::thread::sleep(Duration::from_millis(100));
        filler.extend(fill_descriptors());
    }

    release_descriptors(filler);
    // SAFETY: Restoring the limits read above.
    assert_eq!(
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &previous) },
        0
    );
    let fresh = TcpStream::connect(address).expect("connect after recovery");
    assert!(finish_scrape(fresh, address).starts_with("HTTP/1.1 200"));
    // SAFETY: As above.
    assert_eq!(
        unsafe { test_shared_runtime_core_metrics_server_stopped(shared) },
        Some(false)
    );

    // SAFETY: The slot holds the live handle and is used by this thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(&mut shared) },
        STATUS_OK
    );
}

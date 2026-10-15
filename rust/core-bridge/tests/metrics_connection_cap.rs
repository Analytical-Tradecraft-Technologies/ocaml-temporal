//! The metrics endpoint relays at most `MAX_CONCURRENT_SCRAPES` connections
//! at once (#500).
//!
//! The test fills every relay slot with clients that never finish (half send
//! nothing, half keep an HTTP/1.1 connection alive after one scrape), then
//! checks that a burst of idle connections beyond the cap is closed within
//! the rejection timeout, that further scrapes are answered quickly with the
//! "too many concurrent scrapes" `503` (or reset) and never with metrics, that the
//! bridge closes the idle holders on its own, that real scrapes are served
//! again afterwards, and (on Unix) that the process ends with the descriptor
//! count it started with. The descriptor count is process-wide, so this file
//! holds exactly one test.

use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::ptr;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    MAX_CONCURRENT_SCRAPES, Result as AbiResult, STATUS_OK, SharedRuntime,
    ocaml_temporal_core_v4_result_free, ocaml_temporal_core_v4_shared_runtime_free,
    ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json,
};

/// Connections opened beyond the cap while every slot is held.
const OVER_CAP_ATTEMPTS: usize = 3 * MAX_CONCURRENT_SCRAPES;

/// Longest an over-cap connection may take to be answered. The rejection
/// does not wait for anything, so this only absorbs scheduling noise.
const REJECTION_BUDGET: Duration = Duration::from_secs(2);

/// Longest the bridge may take to close a slot holder on its own: the 10 s
/// relay idle timeout plus slack (an idle client is closed after 5 s).
const RELEASE_BUDGET: Duration = Duration::from_secs(20);

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

/// Writes one GET for `/metrics` with the given `Connection` header value.
fn send_request(stream: &mut TcpStream, address: SocketAddr, connection: &str) -> bool {
    write!(
        stream,
        "GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: {connection}\r\n\r\n"
    )
    .is_ok()
}

/// Performs one complete scrape on a fresh connection and returns the
/// response text, or the error that interrupted it.
fn scrape(address: SocketAddr) -> std::io::Result<String> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    if !send_request(&mut stream, address, "close") {
        return Err(ErrorKind::BrokenPipe.into());
    }
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

/// Whether an I/O error is the peer closing or resetting the connection, as
/// opposed to a timeout.
fn closed_by_peer(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted | ErrorKind::BrokenPipe
    )
}

/// Reads until the bridge closes `stream`, failing if that takes longer than
/// `budget` from `started`.
fn await_close(mut stream: TcpStream, started: Instant, budget: Duration) {
    let mut sink = [0_u8; 4096];
    loop {
        let remaining = budget
            .checked_sub(started.elapsed())
            .expect("the bridge did not close a slot holder in time");
        stream
            .set_read_timeout(Some(remaining.max(Duration::from_millis(1))))
            .expect("read timeout");
        match stream.read(&mut sink) {
            Ok(0) => return,
            Ok(_) => {}
            Err(error) if closed_by_peer(error.kind()) => return,
            Err(error) => panic!("slot holder still open: {error}"),
        }
    }
}

/// Number of descriptors this process has open, or `None` where the count
/// is not observable.
fn open_descriptors() -> Option<usize> {
    #[cfg(unix)]
    {
        Some(
            std::fs::read_dir("/dev/fd")
                .expect("list open descriptors")
                .count(),
        )
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// Fills the cap with idle and kept-alive clients, checks that extra
/// connections are refused promptly without metrics, that the holders are
/// closed by the bridge, and that service and the descriptor count recover.
#[test]
fn scrape_connections_are_capped_and_released() {
    let (mut shared, address) = start_endpoint();
    let warm_up = scrape(address).expect("warm-up scrape");
    assert!(warm_up.starts_with("HTTP/1.1 200"), "{warm_up:?}");
    std::thread::sleep(Duration::from_millis(300));
    let baseline = open_descriptors();

    // Fill every slot. Idle holders never send a request; kept-alive holders
    // finish one real scrape and then keep the connection open.
    let idle = MAX_CONCURRENT_SCRAPES / 2;
    let mut holders = Vec::new();
    for _ in 0..idle {
        holders.push(TcpStream::connect(address).expect("idle holder connect"));
    }
    for _ in idle..MAX_CONCURRENT_SCRAPES {
        let mut stream = TcpStream::connect(address).expect("keep-alive holder connect");
        assert!(send_request(&mut stream, address, "keep-alive"));
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        let mut first = [0_u8; 12];
        stream
            .read_exact(&mut first)
            .expect("keep-alive scrape answered");
        assert_eq!(&first, b"HTTP/1.1 200");
        holders.push(stream);
    }
    let holders_started = Instant::now();
    // The idle holders were accepted (and given slots) without sending
    // anything; give the accept loop a moment to reach all of them.
    std::thread::sleep(Duration::from_millis(300));

    // A burst of idle connections beyond the cap, more than the bridge keeps
    // pending for a 503: every one is closed by the bridge within the
    // one-second rejection timeout (or at once), never relayed.
    let burst_started = Instant::now();
    let burst: Vec<TcpStream> = (0..OVER_CAP_ATTEMPTS)
        .map(|_| TcpStream::connect(address).expect("idle over-cap connect"))
        .collect();
    for stream in burst {
        await_close(
            stream,
            burst_started,
            REJECTION_BUDGET + Duration::from_secs(1),
        );
    }
    // Let the rejection tasks finish so their permits are free again.
    std::thread::sleep(Duration::from_millis(200));

    let mut busy = 0;
    for attempt in 0..OVER_CAP_ATTEMPTS {
        let started = Instant::now();
        match scrape(address) {
            Ok(response) => {
                assert!(
                    !response.starts_with("HTTP/1.1 200"),
                    "attempt {attempt} was relayed beyond the cap"
                );
                if !response.is_empty() {
                    assert!(
                        response.starts_with("HTTP/1.1 503")
                            && response.contains("Too many concurrent metrics scrapes"),
                        "attempt {attempt}: {response:?}"
                    );
                    busy += 1;
                }
            }
            Err(error) => assert!(
                closed_by_peer(error.kind()),
                "attempt {attempt} failed unexpectedly: {error}"
            ),
        }
        assert!(
            started.elapsed() < REJECTION_BUDGET,
            "attempt {attempt} waited {:?} for its rejection",
            started.elapsed()
        );
    }
    assert!(busy > 0, "no over-cap connection received the busy 503");

    // Every holder is closed by the bridge, not by the test.
    for holder in holders {
        await_close(holder, holders_started, RELEASE_BUDGET);
    }

    // The permits return as the relay tasks end, a moment after the close.
    let recovered = Instant::now();
    loop {
        let response = scrape(address).expect("scrape after release");
        if response.starts_with("HTTP/1.1 200") {
            break;
        }
        assert!(
            response.starts_with("HTTP/1.1 503") && recovered.elapsed() < Duration::from_secs(2),
            "endpoint did not recover: {response:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    for _ in 0..MAX_CONCURRENT_SCRAPES {
        let response = scrape(address).expect("sequential scrape");
        assert!(response.starts_with("HTTP/1.1 200"), "{response:?}");
    }

    // Every socket of the holders, the rejected connections and their relays
    // (including Core's side) is closed again.
    if let Some(baseline) = baseline {
        let settle = Instant::now();
        loop {
            let now = open_descriptors().expect("descriptor count");
            if now <= baseline {
                break;
            }
            assert!(
                settle.elapsed() < Duration::from_secs(5),
                "descriptors leaked: {now} open, {baseline} before"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    // SAFETY: The slot holds the live handle and is used by this thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(&mut shared) },
        STATUS_OK
    );
}

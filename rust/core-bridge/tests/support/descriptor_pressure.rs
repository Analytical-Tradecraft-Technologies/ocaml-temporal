//! Descriptor-pressure helpers shared by the metrics endpoint tests (#1030).
//!
//! They start a loopback Prometheus endpoint through the C ABI, lower the
//! process descriptor limit, and fill or release descriptors. Descriptor
//! limits are process-wide, so each test binary that includes this module
//! holds exactly one test.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::ptr;
use std::time::Duration;

use ocaml_temporal_core_bridge::{
    Result as AbiResult, STATUS_OK, SharedRuntime, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json,
};

/// Descriptor limit used while the endpoint is starved. Small enough to fill
/// quickly, large enough for the runtime's own descriptors.
const STARVED_LIMIT: libc::rlim_t = 256;

/// Creates a shared runtime with a loopback Prometheus endpoint and returns
/// it with the endpoint's address.
pub fn start_endpoint() -> (*mut SharedRuntime, SocketAddr) {
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
pub fn finish_scrape(mut stream: TcpStream, address: SocketAddr) -> String {
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
pub fn lower_descriptor_limit() -> libc::rlimit {
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
pub fn fill_descriptors() -> Vec<libc::c_int> {
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
pub fn release_descriptors(filler: Vec<libc::c_int>) {
    for fd in filler {
        // SAFETY: Each descriptor came from `dup` and is closed once.
        unsafe { libc::close(fd) };
    }
}

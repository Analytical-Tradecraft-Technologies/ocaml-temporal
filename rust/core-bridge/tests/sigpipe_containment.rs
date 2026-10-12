//! SIGPIPE from a peer-closed connection must not terminate the host (#1041).
//!
//! The bridge links into an OCaml executable, where SIGPIPE keeps its default
//! (terminating) action. A Rust test binary instead starts with SIGPIPE
//! ignored, which would hide the bug, so every test here first restores the
//! default disposition for this whole test process, as an OCaml host has it.
//! All tests in this binary want that same disposition, so they can share it.
//!
//! The tests check the two halves of the containment in [`sigpipe`]: bridge
//! threads (Core's Tokio pool) block the signal permanently, and an ABI call
//! on an application thread blocks it only for the call and restores the
//! caller's mask. The Linux-only tests drive real `writev` calls into reset
//! connections, which is the path that killed workers before the fix.

#![cfg(unix)]

use std::ptr;
use std::sync::Once;

use ocaml_temporal_core_bridge::{
    Result as AbiResult, Runtime, STATUS_OK, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new_with_worker_threads,
    sigpipe, test_runtime_threads_block_sigpipe,
};

/// Restores SIGPIPE's default, terminating action for this test process, once.
fn default_sigpipe_disposition() {
    static RESTORE: Once = Once::new();
    RESTORE.call_once(|| {
        // SAFETY: Setting a standard disposition for a valid signal number.
        let previous = unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
        assert_ne!(previous, libc::SIG_ERR, "could not reset SIGPIPE");
    });
}

/// Runs `body` on a fresh thread whose SIGPIPE mask starts unblocked, as an
/// application thread's normally is, and propagates its panic.
fn on_unblocked_thread(body: impl FnOnce() + Send + 'static) {
    std::thread::spawn(move || {
        let mut unblock = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
        // SAFETY: The set is initialized before use; the mask call only
        // removes SIGPIPE from this new thread's own mask.
        unsafe {
            libc::sigemptyset(unblock.as_mut_ptr());
            libc::sigaddset(unblock.as_mut_ptr(), libc::SIGPIPE);
            libc::pthread_sigmask(libc::SIG_UNBLOCK, unblock.as_ptr(), ptr::null_mut());
        }
        assert!(!sigpipe::blocked_in_current_thread());
        body();
    })
    .join()
    .expect("test thread");
}

/// Creates a one-worker runtime through the C ABI and returns its handle.
fn new_runtime() -> *mut Runtime {
    let mut runtime: *mut Runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both outputs are writable and exclusively owned.
    let status = unsafe {
        ocaml_temporal_core_v4_runtime_new_with_worker_threads(1, &mut runtime, &mut result)
    };
    // SAFETY: The result was initialized by the bridge and is freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(status, STATUS_OK);
    runtime
}

/// Releases a handle from [`new_runtime`] through its slot.
fn free_runtime(mut runtime: *mut Runtime) {
    // SAFETY: The slot holds a live handle used only by this thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

/// Every Tokio worker and blocking-pool thread of Core's runtime has SIGPIPE
/// blocked, so a vectored socket write there fails with `EPIPE` instead.
#[test]
fn core_tokio_threads_block_sigpipe() {
    default_sigpipe_disposition();
    let runtime = new_runtime();
    // SAFETY: The handle is live and used only by this thread.
    let observed = unsafe { test_runtime_threads_block_sigpipe(runtime) };
    free_runtime(runtime);
    assert_eq!(observed, Some((true, true)));
}

/// An ABI call leaves the application thread's mask exactly as it found it,
/// both when SIGPIPE was unblocked and when the caller had blocked it.
#[test]
fn abi_calls_restore_the_callers_signal_mask() {
    default_sigpipe_disposition();
    on_unblocked_thread(|| {
        let runtime = new_runtime();
        assert!(!sigpipe::blocked_in_current_thread());
        free_runtime(runtime);

        sigpipe::block_current_thread();
        let runtime = new_runtime();
        assert!(
            sigpipe::blocked_in_current_thread(),
            "a caller's own block must survive the call"
        );
        free_runtime(runtime);
    });
}

/// A SIGPIPE directed at the calling thread during a guarded call is
/// consumed, not delivered when the guard restores the mask; with the default
/// disposition, delivery would end this process. The guard only acts on
/// Linux; elsewhere bridge writes cannot raise SIGPIPE.
#[cfg(target_os = "linux")]
#[test]
fn guard_consumes_sigpipe_raised_during_the_call() {
    default_sigpipe_disposition();
    on_unblocked_thread(|| {
        sigpipe::contain(|| {
            assert!(sigpipe::blocked_in_current_thread());
            // SAFETY: Signalling the current thread with a valid signal.
            assert_eq!(
                unsafe { libc::pthread_kill(libc::pthread_self(), libc::SIGPIPE) },
                0
            );
            assert!(sigpipe::pending(), "the blocked signal stays pending");
        });
        assert!(!sigpipe::pending());
        assert!(!sigpipe::blocked_in_current_thread());
    });
}

/// Linux regressions that write into a connection the peer has closed. On
/// macOS std and mio sockets set `SO_NOSIGPIPE`, so the kernel never raises
/// the signal and these would pass vacuously.
#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::io::{ErrorKind, IoSlice, Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::time::Duration;

    use ocaml_temporal_core_bridge::{
        SharedRuntime, ocaml_temporal_core_v4_shared_runtime_free,
        ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json,
    };

    /// Closes `stream` with an RST rather than a FIN by setting a zero
    /// linger time first.
    fn reset(stream: TcpStream) {
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        // SAFETY: The descriptor is live and the option value is a `linger`
        // of the size passed.
        let rc = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                (&raw const linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "SO_LINGER");
        drop(stream);
    }

    /// Writes with `writev` (std's vectored write, which passes no
    /// `MSG_NOSIGNAL`) until the reset connection reports `EPIPE`.
    fn writev_until_epipe(stream: &mut TcpStream) {
        let chunk = [0_u8; 4096];
        for _ in 0..1000 {
            match stream.write_vectored(&[IoSlice::new(&chunk), IoSlice::new(&chunk)]) {
                Ok(_) => std::thread::sleep(Duration::from_millis(1)),
                Err(error) if error.kind() == ErrorKind::BrokenPipe => return,
                Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
                Err(error) => panic!("unexpected write error: {error}"),
            }
        }
        panic!("the reset connection never reported EPIPE");
    }

    /// Returns a connected client whose peer has already been reset.
    fn reset_connection() -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let client = TcpStream::connect(listener.local_addr().expect("address")).expect("connect");
        let (server, _) = listener.accept().expect("accept");
        reset(server);
        client
    }

    /// The mechanism itself: `writev` into a reset connection on a thread
    /// that blocked SIGPIPE (as every Core Tokio thread does) returns `EPIPE`
    /// and the process survives.
    #[test]
    fn writev_on_a_blocked_bridge_thread_returns_epipe() {
        default_sigpipe_disposition();
        on_unblocked_thread(|| {
            sigpipe::block_current_thread();
            let mut client = reset_connection();
            writev_until_epipe(&mut client);
        });
    }

    /// The same write on an application thread inside a guarded ABI call
    /// (for example a TLS handshake under `block_on`) returns `EPIPE`, and
    /// the guard leaves no signal behind when it restores the mask.
    #[test]
    fn writev_inside_a_guarded_call_returns_epipe() {
        default_sigpipe_disposition();
        on_unblocked_thread(|| {
            let mut client = reset_connection();
            sigpipe::contain(|| writev_until_epipe(&mut client));
            assert!(!sigpipe::pending());
            assert!(!sigpipe::blocked_in_current_thread());
        });
    }

    /// Scrapers that send a request and then reset the connection make the
    /// Prometheus server (hyper, vectored writes, Core's Tokio threads) write
    /// into dead sockets. The process survives and the endpoint keeps
    /// serving.
    #[test]
    fn aborted_metrics_scrapes_do_not_kill_the_process() {
        default_sigpipe_disposition();
        let document = br#"{"prometheus":{"bind_address":"127.0.0.1:0"}}"#;
        let mut shared: *mut SharedRuntime = ptr::null_mut();
        let mut result = AbiResult::default();
        // SAFETY: The document outlives the call and both outputs are
        // writable.
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
        // SAFETY: A live result buffer is readable for its length.
        let address = unsafe { std::slice::from_raw_parts(result.value.ptr, result.value.len) };
        let address: SocketAddr = std::str::from_utf8(address)
            .expect("UTF-8 address")
            .parse()
            .expect("socket address");
        // SAFETY: Initialized by the bridge and freed once.
        assert_eq!(
            unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
            STATUS_OK
        );

        let request = format!("GET /metrics HTTP/1.1\r\nHost: {address}\r\n\r\n");
        for round in 0..64 {
            let mut stream = TcpStream::connect(address).expect("scrape connect");
            stream.write_all(request.as_bytes()).expect("request");
            if round % 2 == 1 {
                // Let the response start before the reset on half the rounds.
                std::thread::sleep(Duration::from_millis(1));
            }
            reset(stream);
        }

        let mut stream = TcpStream::connect(address).expect("final scrape");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        write!(
            stream,
            "GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
        )
        .expect("final request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("final response");
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");

        // SAFETY: The slot holds the live handle and is used by this thread.
        assert_eq!(
            unsafe { ocaml_temporal_core_v4_shared_runtime_free(&mut shared) },
            STATUS_OK
        );
    }
}

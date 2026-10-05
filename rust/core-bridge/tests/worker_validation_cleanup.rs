//! Regression coverage for failed worker validation cleanup (issue #770).
//!
//! Core validates a worker by calling `DescribeNamespace`. When that RPC fails
//! the bridge must release the partially constructed Core worker and return a
//! typed worker failure promptly. Awaiting Core's `finalize_shutdown` without
//! driving both poll APIs to `ShutDown` previously hung forever, because
//! Core's activity manager only completes shutdown after its poll stream has
//! reported `ShutDown`.
//!
//! The tests use a minimal plaintext HTTP/2 gRPC double instead of a Temporal
//! server, so they are deterministic and run in every Rust test job.

use std::net::SocketAddr;
use std::ptr;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, STATUS_OK, STATUS_WORKER,
    ocaml_temporal_core_v2_client_connect_json, ocaml_temporal_core_v2_client_disconnect,
    ocaml_temporal_core_v2_result_free, ocaml_temporal_core_v2_runtime_free,
    ocaml_temporal_core_v2_runtime_new, ocaml_temporal_core_v2_worker_shutdown,
    ocaml_temporal_core_v2_worker_start_json,
};

/// Upper bound for one failed `Worker.create` round trip. The fixed path takes
/// milliseconds; the bound only exists so a regression fails instead of
/// hanging CI.
const START_DEADLINE: Duration = Duration::from_secs(30);

/// gRPC status code `NOT_FOUND`, returned by the double for
/// `DescribeNamespace` exactly as a server does for a missing namespace.
const GRPC_NOT_FOUND: &str = "5";

/// gRPC status code `UNIMPLEMENTED`. Core's connection probe accepts this for
/// `GetSystemInfo` when the message names an unknown method.
const GRPC_UNIMPLEMENTED: &str = "12";

/// Starts a background gRPC double and returns its loopback address.
///
/// Every request receives a trailers-only response: `DescribeNamespace`
/// fails with `NOT_FOUND`; every other method, including the connection-time
/// `GetSystemInfo` probe and any poll or shutdown RPC, fails as an unknown
/// method. The server runs on its own detached OS thread with a private
/// current-thread Tokio runtime, so it never shares an executor with the
/// bridge under test and simply ends with the test process.
fn spawn_missing_namespace_server() -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-missing-namespace-double".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test server runtime");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind loopback test server");
                address_sender
                    .send(listener.local_addr().expect("test server address"))
                    .expect("publish test server address");
                loop {
                    let Ok((socket, _)) = listener.accept().await else {
                        continue;
                    };
                    tokio::spawn(serve_connection(socket));
                }
            });
        })
        .expect("spawn test server thread");
    address_receiver
        .recv_timeout(START_DEADLINE)
        .expect("test server publishes its address")
}

/// Serves one HTTP/2 connection until the client closes it.
///
/// Request bodies are ignored: a trailers-only gRPC response is valid before
/// the request stream ends, and dropping the receive half resets it.
async fn serve_connection(socket: tokio::net::TcpStream) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((request, mut respond))) = connection.accept().await {
        let (code, message) = if request.uri().path().ends_with("/DescribeNamespace") {
            (GRPC_NOT_FOUND, "Namespace missing-namespace is not found.")
        } else {
            (GRPC_UNIMPLEMENTED, "unknown method")
        };
        let response = http::Response::builder()
            .status(200)
            .header("content-type", "application/grpc")
            .header("grpc-status", code)
            .header("grpc-message", message)
            .body(())
            .expect("static gRPC response");
        let _ = respond.send_response(response, true);
    }
}

/// Copies one live ABI buffer without taking ownership from its result.
fn bytes(buffer: &Buffer) -> Vec<u8> {
    if buffer.ptr.is_null() {
        assert_eq!(buffer.len, 0);
        Vec::new()
    } else {
        // SAFETY: The bridge owns this readable allocation until the
        // containing result is released by `consume`.
        unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len).to_vec() }
    }
}

/// Frees a live result after checking its status and returns its diagnostic.
fn consume(mut result: AbiResult, expected_status: i32) -> String {
    let message = String::from_utf8(bytes(&result.error)).expect("diagnostic is UTF-8");
    assert_eq!(result.status, expected_status, "{message}");
    // SAFETY: This helper has exclusive ownership of the initialized result.
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    message
}

/// Raw runtime pointer that may move to the bounded worker-start thread.
///
/// The test thread never touches the runtime while the helper thread owns the
/// call, matching the ABI rule that one caller owns a runtime at a time.
struct RuntimePtr(*mut ocaml_temporal_core_bridge::Runtime);

// SAFETY: Ownership of the runtime pointer is transferred to exactly one
// thread at a time and returned through a channel before reuse.
unsafe impl Send for RuntimePtr {}

/// Calls worker start on a helper thread and fails the test if it does not
/// return within [`START_DEADLINE`]. The helper checks the status and releases
/// the result itself, returning only the runtime and the diagnostic text.
///
/// On timeout the helper thread is intentionally leaked with the runtime: the
/// bridge is wedged inside Core, so neither releasing nor joining it is safe.
fn start_worker_bounded(
    runtime: *mut ocaml_temporal_core_bridge::Runtime,
    config: &'static [u8],
    expected_status: i32,
) -> (*mut ocaml_temporal_core_bridge::Runtime, String) {
    let (sender, receiver) = mpsc::channel();
    let owned = RuntimePtr(runtime);
    thread::spawn(move || {
        let owned = owned;
        let mut result = AbiResult::default();
        // SAFETY: This thread exclusively owns the live runtime for the call,
        // the static configuration stays readable, and the output is unique.
        unsafe {
            ocaml_temporal_core_v2_worker_start_json(
                owned.0,
                config.as_ptr(),
                config.len(),
                &mut result,
            );
        }
        let message = consume(result, expected_status);
        let _ = sender.send((owned, message));
    });
    let (owned, message) = receiver
        .recv_timeout(START_DEADLINE)
        .expect("worker start must return after failed namespace validation (issue #770)");
    (owned.0, message)
}

/// Worker configuration for a namespace the gRPC double reports as missing.
const MISSING_NAMESPACE_WORKER: &[u8] = br#"{
    "namespace":"missing-namespace",
    "task_queue":"validation-cleanup-test",
    "build_id":"validation-cleanup-build",
    "versioning":{"kind":"none"},
    "max_cached_workflows":10,
    "max_outstanding_workflow_tasks":10,
    "max_concurrent_workflow_task_polls":2,
    "graceful_shutdown_timeout_ms":1000
}"#;

/// A missing namespace fails `Worker.create` promptly with the closed worker
/// diagnostic, publishes no worker, and leaves the client usable so a
/// corrected retry and normal runtime teardown both succeed.
#[test]
fn failed_namespace_validation_releases_worker_without_hanging() {
    let address = spawn_missing_namespace_server();
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) };
    consume(result, STATUS_OK);

    let client = format!(r#"{{"target_url":"http://{address}","identity":"validation-test"}}"#);
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned; the input remains
    // readable for the full blocking call.
    unsafe {
        ocaml_temporal_core_v2_client_connect_json(
            runtime,
            client.as_ptr(),
            client.len(),
            &mut result,
        );
    }
    consume(result, STATUS_OK);

    // Two attempts prove the failed worker was fully released: a leaked
    // worker graph would make the retry report an already-running worker.
    for _ in 0..2 {
        let (returned, message) =
            start_worker_bounded(runtime, MISSING_NAMESPACE_WORKER, STATUS_WORKER);
        runtime = returned;
        assert_eq!(message, "Temporal workflow worker validation failed");
    }

    // No worker was published, so shutdown is the idempotent absent case.
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread again.
    unsafe { ocaml_temporal_core_v2_worker_shutdown(runtime, &mut result) };
    consume(result, STATUS_OK);

    // The client survives validation failure, so it is still disconnectable.
    let mut result = AbiResult::default();
    // SAFETY: Same exclusive runtime ownership as above.
    unsafe { ocaml_temporal_core_v2_client_disconnect(runtime, &mut result) };
    consume(result, STATUS_OK);

    // SAFETY: Children are closed and the slot is exclusively owned.
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

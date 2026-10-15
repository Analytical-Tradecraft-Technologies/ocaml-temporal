//! Regression coverage for issue #1025: asynchronous activity completions
//! and heartbeats as submitted client calls.
//!
//! The OCaml worker used to send each `Async_handle` operation as a blocking
//! RPC on its single owner Domain, so every other worker operation queued
//! behind the server round trip. The operations are now submitted like the
//! other client calls (#807): the owner only validates the request and spawns
//! the RPC, and the caller awaits the outcome from its own thread.
//!
//! A minimal plaintext HTTP/2 gRPC double stands in for the Temporal server,
//! so the tests are deterministic and run in every Rust test job. It holds
//! every poll open, answers `RecordActivityTaskHeartbeat` for one token with
//! `NOT_FOUND`, holds heartbeats for a second token until the test releases
//! them, and succeeds everything else.

use std::io::Read as _;
use std::net::SocketAddr;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_INVALID_STATE, STATUS_NOT_READY, STATUS_OK,
    STATUS_PROTOCOL, Status, ocaml_temporal_core_v4_client_await_call,
    ocaml_temporal_core_v4_client_connect_json, ocaml_temporal_core_v4_client_disconnect,
    ocaml_temporal_core_v4_client_record_async_activity_heartbeat_json,
    ocaml_temporal_core_v4_client_submit_json, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new,
    ocaml_temporal_core_v4_worker_shutdown, ocaml_temporal_core_v4_worker_start_json,
};
use prost::bytes::Bytes;

/// Selector of a submitted async activity completion, as in the C header.
const CALL_ASYNC_ACTIVITY_COMPLETION: u32 = 11;
/// Selector of a submitted async activity heartbeat, as in the C header.
const CALL_ASYNC_ACTIVITY_HEARTBEAT: u32 = 12;

/// Token the double answers with `NOT_FOUND`; base64 of
/// `async-missing-token`.
const MISSING_TOKEN: &[u8] = b"async-missing-token";
/// Token whose heartbeat the double holds until released; base64 of
/// `async-slow-token`.
const SLOW_TOKEN: &[u8] = b"async-slow-token";

/// Heartbeat document for a token the double accepts (base64 of
/// `async-known-token`).
const KNOWN_HEARTBEAT: &str = r#"{"task_token":"YXN5bmMta25vd24tdG9rZW4=","details":[]}"#;
/// Heartbeat document for [`MISSING_TOKEN`].
const MISSING_HEARTBEAT: &str = r#"{"task_token":"YXN5bmMtbWlzc2luZy10b2tlbg==","details":[]}"#;
/// Heartbeat document for [`SLOW_TOKEN`].
const SLOW_HEARTBEAT: &str = r#"{"task_token":"YXN5bmMtc2xvdy10b2tlbg==","details":[]}"#;
/// Successful completion of the same accepted token with an empty payload.
const KNOWN_COMPLETION: &str = r#"{"task_token":"YXN5bmMta25vd24tdG9rZW4=","result":{"kind":"completed","result":{"metadata":{},"data":{"encoding":"base64","data":""}}}}"#;

/// Upper bound for one bridge call under test; a regression fails instead of
/// hanging CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// Requests observed by the gRPC double, shared with the test thread.
#[derive(Default)]
struct Observed {
    /// Accepted `RecordActivityTaskHeartbeat` requests.
    heartbeats: AtomicUsize,
    /// Accepted `RespondActivityTaskCompleted` requests.
    completions: AtomicUsize,
    /// Set while a heartbeat for [`SLOW_TOKEN`] is held by the double.
    slow_heartbeat_held: AtomicBool,
    /// Releases a held [`SLOW_TOKEN`] heartbeat when set by the test.
    release_slow_heartbeat: AtomicBool,
}

/// Whether `haystack` contains `needle`; protobuf carries the token bytes
/// verbatim, so a byte search identifies the request's token.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Returns the protobuf message of one gRPC request body, decompressing it
/// when its length-prefix flag says Core sent it gzip-compressed.
fn grpc_message(buffer: &[u8]) -> Vec<u8> {
    let compressed = buffer.first() == Some(&1);
    let message = buffer.get(5..).unwrap_or_default();
    if compressed {
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(message)
            .read_to_end(&mut decoded)
            .expect("Core sends valid gzip request messages");
        decoded
    } else {
        message.to_vec()
    }
}

/// Starts the gRPC double on a detached OS thread with its own
/// current-thread Tokio runtime and returns its loopback address. The thread
/// ends with the test process.
fn spawn_server(observed: Arc<Observed>) -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-async-activity-double".to_owned())
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
                    .expect("report test server address");
                loop {
                    let Ok((socket, _)) = listener.accept().await else {
                        continue;
                    };
                    tokio::spawn(serve_connection(socket, Arc::clone(&observed)));
                }
            });
        })
        .expect("spawn test server thread");
    address_receiver
        .recv_timeout(CALL_DEADLINE)
        .expect("test server started")
}

/// Serves one HTTP/2 connection until the client closes it. Each request is
/// answered on its own task, so a held heartbeat never delays another call.
async fn serve_connection(socket: tokio::net::TcpStream, observed: Arc<Observed>) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    let mut held = Vec::new();
    while let Some(Ok((request, respond))) = connection.accept().await {
        let path = request.uri().path().to_owned();
        if path.ends_with("/PollActivityTaskQueue") || path.ends_with("/PollWorkflowTaskQueue") {
            held.push(respond);
            continue;
        }
        tokio::spawn(answer(path, request, respond, Arc::clone(&observed)));
    }
}

/// Reads one request body and answers it as described in the module docs.
async fn answer(
    path: String,
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    observed: Arc<Observed>,
) {
    let mut body = request.into_body();
    let mut received = Vec::new();
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
        received.extend_from_slice(&chunk);
    }
    let received = grpc_message(&received);
    let mut grpc_status = "0";
    if path.ends_with("/RecordActivityTaskHeartbeat") {
        if contains(&received, MISSING_TOKEN) {
            grpc_status = "5";
        } else {
            if contains(&received, SLOW_TOKEN) {
                observed.slow_heartbeat_held.store(true, Ordering::SeqCst);
                while !observed.release_slow_heartbeat.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            observed.heartbeats.fetch_add(1, Ordering::SeqCst);
        }
    } else if path.ends_with("/RespondActivityTaskCompleted") {
        observed.completions.fetch_add(1, Ordering::SeqCst);
    }
    let response = http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(())
        .expect("static gRPC response");
    let Ok(mut stream) = respond.send_response(response, false) else {
        return;
    };
    if grpc_status == "0" {
        // The default instance of every response message encodes to zero
        // bytes, so one empty length-prefixed frame is a valid success.
        if stream
            .send_data(Bytes::from_static(&[0, 0, 0, 0, 0]), false)
            .is_err()
        {
            return;
        }
    }
    let mut trailers = http::HeaderMap::new();
    trailers.insert(
        "grpc-status",
        http::HeaderValue::from_str(grpc_status).expect("static status"),
    );
    let _ = stream.send_trailers(trailers);
}

/// Copies one live ABI buffer without taking ownership from its result.
fn bytes(buffer: &Buffer) -> Vec<u8> {
    if buffer.ptr.is_null() {
        assert_eq!(buffer.len, 0);
        Vec::new()
    } else {
        // SAFETY: The bridge owns this readable allocation until the
        // containing result is released by `release`.
        unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len).to_vec() }
    }
}

/// Frees a live result and returns its status, value, and diagnostic text.
fn release(mut result: AbiResult) -> (Status, Vec<u8>, String) {
    let status = result.status;
    let value = bytes(&result.value);
    let message = String::from_utf8(bytes(&result.error)).expect("diagnostic is UTF-8");
    // SAFETY: This helper has exclusive ownership of the initialized result.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    (status, value, message)
}

/// Activity-only worker document; the worker supplies the namespace the
/// async activity client addresses.
const ACTIVITY_WORKER: &[u8] = br#"{
    "namespace":"async-activity-test",
    "task_queue":"async-activity-submitted-test",
    "build_id":"async-activity-submitted-build",
    "versioning":{"kind":"none"},
    "max_cached_workflows":10,
    "max_outstanding_workflow_tasks":10,
    "max_concurrent_workflow_task_polls":2,
    "graceful_shutdown_timeout_ms":1000,
    "task_types":{"workflows":false,"activities":true}
}"#;

/// Creates a runtime and connects it to the double; with `worker`, also
/// starts the activity worker. Returns the exclusively owned runtime.
fn start_runtime(address: SocketAddr, worker: bool) -> *mut Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);

    let client = format!(r#"{{"target_url":"http://{address}","identity":"async-test"}}"#);
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned; the input remains
    // readable for the full blocking call.
    unsafe {
        ocaml_temporal_core_v4_client_connect_json(
            runtime,
            client.as_ptr(),
            client.len(),
            &mut result,
        );
    }
    let (status, _, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");

    if worker {
        let mut result = AbiResult::default();
        // SAFETY: Same exclusive runtime ownership; the static document stays
        // readable for the call.
        unsafe {
            ocaml_temporal_core_v4_worker_start_json(
                runtime,
                ACTIVITY_WORKER.as_ptr(),
                ACTIVITY_WORKER.len(),
                &mut result,
            );
        }
        let (status, _, message) = release(result);
        assert_eq!(status, STATUS_OK, "{message}");
    }
    runtime
}

/// Submits one call and returns its handle, or the submission's failure.
fn submit(
    runtime: *mut Runtime,
    kind: u32,
    document: &str,
) -> Result<(u64, u64), (Status, String)> {
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread; the
    // document stays readable for the call.
    unsafe {
        ocaml_temporal_core_v4_client_submit_json(
            runtime,
            kind,
            document.as_ptr(),
            document.len(),
            &mut result,
        );
    }
    let (status, value, message) = release(result);
    if status != STATUS_OK {
        return Err((status, message));
    }
    let handle = String::from_utf8(value).expect("ASCII call handle");
    let (owner, call) = handle.split_once('.').expect("<owner>.<call> handle");
    Ok((
        owner.parse().expect("decimal owner"),
        call.parse().expect("decimal call"),
    ))
}

/// Awaits one submitted call from the calling thread and returns its
/// terminal status, value, and diagnostic.
fn await_call((owner, call): (u64, u64)) -> (Status, Vec<u8>, String) {
    let deadline = Instant::now() + CALL_DEADLINE;
    loop {
        let mut result = AbiResult::default();
        // SAFETY: The output location is writable and exclusively owned; the
        // await borrows no runtime.
        unsafe { ocaml_temporal_core_v4_client_await_call(owner, call, 1_000, &mut result) };
        let outcome = release(result);
        if outcome.0 != STATUS_NOT_READY {
            return outcome;
        }
        assert!(Instant::now() < deadline, "submitted call never completed");
    }
}

/// Shuts the worker down, disconnects the client, and frees the runtime.
fn close_runtime(mut runtime: *mut Runtime) {
    for close in [
        ocaml_temporal_core_v4_worker_shutdown,
        ocaml_temporal_core_v4_client_disconnect,
    ] {
        let mut result = AbiResult::default();
        // SAFETY: The runtime is live and exclusively owned by this thread.
        unsafe { close(runtime, &mut result) };
        let (status, _, message) = release(result);
        assert_eq!(status, STATUS_OK, "{message}");
    }
    // SAFETY: Children are closed and the slot is exclusively owned.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

/// Accepted heartbeats and completions are an empty outcome, and a token
/// the server no longer knows is the fixed `not_found` value rather than
/// `STATUS_INVALID_STATE`, which an await reserves for a closed call. The
/// synchronous symbol keeps reporting `STATUS_INVALID_STATE` for it.
#[test]
fn submitted_async_activity_calls_report_server_outcomes() {
    let observed = Arc::new(Observed::default());
    let address = spawn_server(Arc::clone(&observed));
    let runtime = start_runtime(address, true);

    let heartbeat = submit(runtime, CALL_ASYNC_ACTIVITY_HEARTBEAT, KNOWN_HEARTBEAT)
        .expect("heartbeat submission");
    let (status, value, message) = await_call(heartbeat);
    assert_eq!(status, STATUS_OK, "{message}");
    assert!(value.is_empty());
    assert_eq!(observed.heartbeats.load(Ordering::SeqCst), 1);

    let completion = submit(runtime, CALL_ASYNC_ACTIVITY_COMPLETION, KNOWN_COMPLETION)
        .expect("completion submission");
    let (status, value, message) = await_call(completion);
    assert_eq!(status, STATUS_OK, "{message}");
    assert!(value.is_empty());
    assert_eq!(observed.completions.load(Ordering::SeqCst), 1);

    let missing = submit(runtime, CALL_ASYNC_ACTIVITY_HEARTBEAT, MISSING_HEARTBEAT)
        .expect("missing-token submission");
    let (status, value, message) = await_call(missing);
    assert_eq!(status, STATUS_OK, "{message}");
    assert_eq!(value, b"not_found");

    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread; the
    // static document stays readable for the call.
    unsafe {
        ocaml_temporal_core_v4_client_record_async_activity_heartbeat_json(
            runtime,
            MISSING_HEARTBEAT.as_ptr(),
            MISSING_HEARTBEAT.len(),
            &mut result,
        );
    }
    let (status, _, _) = release(result);
    assert_eq!(status, STATUS_INVALID_STATE);
    close_runtime(runtime);
}

/// A malformed request fails its submission synchronously, before any task
/// or RPC exists, and a runtime without a worker has no namespace to address.
#[test]
fn submitted_async_activity_calls_validate_before_the_rpc() {
    let observed = Arc::new(Observed::default());
    let address = spawn_server(Arc::clone(&observed));

    let without_worker = start_runtime(address, false);
    let refused = submit(
        without_worker,
        CALL_ASYNC_ACTIVITY_HEARTBEAT,
        KNOWN_HEARTBEAT,
    )
    .expect_err("a runtime without a worker accepted an async heartbeat");
    assert_eq!(refused.0, STATUS_INVALID_STATE, "{}", refused.1);
    close_runtime(without_worker);

    let runtime = start_runtime(address, true);
    let deferred_again =
        r#"{"task_token":"YXN5bmMta25vd24tdG9rZW4=","result":{"kind":"will_complete_async"}}"#;
    let refused = submit(runtime, CALL_ASYNC_ACTIVITY_COMPLETION, deferred_again)
        .expect_err("a deferred async completion was submitted");
    assert_eq!(refused.0, STATUS_PROTOCOL, "{}", refused.1);
    assert_eq!(observed.completions.load(Ordering::SeqCst), 0);
    close_runtime(runtime);
}

/// While one heartbeat waits for the server, the owner thread is free: it
/// submits and completes another call at once. Before #1025 the owner was
/// blocked in the first RPC and every other operation queued behind it.
#[test]
fn slow_async_heartbeat_does_not_hold_the_owner() {
    let observed = Arc::new(Observed::default());
    let address = spawn_server(Arc::clone(&observed));
    let runtime = start_runtime(address, true);

    let slow = submit(runtime, CALL_ASYNC_ACTIVITY_HEARTBEAT, SLOW_HEARTBEAT)
        .expect("slow heartbeat submission");
    let deadline = Instant::now() + CALL_DEADLINE;
    while !observed.slow_heartbeat_held.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "the slow heartbeat never arrived"
        );
        thread::sleep(Duration::from_millis(5));
    }

    let fast = submit(runtime, CALL_ASYNC_ACTIVITY_HEARTBEAT, KNOWN_HEARTBEAT)
        .expect("heartbeat submission while another is in flight");
    let (status, value, message) = await_call(fast);
    assert_eq!(status, STATUS_OK, "{message}");
    assert!(value.is_empty());
    assert!(
        !observed.release_slow_heartbeat.load(Ordering::SeqCst),
        "the fast heartbeat completed only after the slow one"
    );

    observed
        .release_slow_heartbeat
        .store(true, Ordering::SeqCst);
    let (status, value, message) = await_call(slow);
    assert_eq!(status, STATUS_OK, "{message}");
    assert!(value.is_empty());
    assert_eq!(observed.heartbeats.load(Ordering::SeqCst), 2);
    close_runtime(runtime);
}

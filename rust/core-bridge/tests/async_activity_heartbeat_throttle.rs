//! Regression coverage for issue #1025: async activity heartbeats are
//! throttled per task token the way Core throttles a running activity's
//! heartbeats.
//!
//! The first heartbeat for a token is sent at once; later ones within the
//! throttle interval only replace the pending details, and a trailing send
//! carries the latest details when the interval expires. The interval is 80%
//! of the heartbeat timeout carried by the request (or the worker's default
//! without one), capped by the worker's maximum. The tests also cover the
//! server flags reported to callers, the window ending on a failed send,
//! eviction by terminal requests, cleanup on disconnect and close, and the
//! bound on tracked tokens.
//!
//! A minimal plaintext HTTP/2 gRPC double stands in for the Temporal server.
//! It holds every poll open, records every `RecordActivityTaskHeartbeat` and
//! terminal request with its arrival time, and answers according to the
//! token's name (see [`answer`]).

use std::io::Read as _;
use std::net::SocketAddr;
use std::ptr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_NOT_READY, STATUS_OK, STATUS_PROTOCOL, Status,
    ocaml_temporal_core_v4_client_await_call, ocaml_temporal_core_v4_client_connect_json,
    ocaml_temporal_core_v4_client_disconnect, ocaml_temporal_core_v4_client_submit_json,
    ocaml_temporal_core_v4_result_free, ocaml_temporal_core_v4_runtime_free,
    ocaml_temporal_core_v4_runtime_new, ocaml_temporal_core_v4_worker_shutdown,
    ocaml_temporal_core_v4_worker_start_json, test_heartbeat_throttle_counts,
};
use prost::bytes::Bytes;

/// Selector of a submitted async activity completion, as in the C header.
const CALL_ASYNC_ACTIVITY_COMPLETION: u32 = 11;
/// Selector of a submitted async activity heartbeat, as in the C header.
const CALL_ASYNC_ACTIVITY_HEARTBEAT: u32 = 12;

/// Upper bound for one wait in these tests; a regression fails instead of
/// hanging CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// One request observed by the gRPC double.
#[derive(Clone)]
struct Request {
    /// Arrival time at the double.
    at: Instant,
    /// Final path segment, for example `RecordActivityTaskHeartbeat`.
    method: String,
    /// Decoded protobuf request message.
    body: Vec<u8>,
}

/// Requests observed by the gRPC double, in arrival order.
#[derive(Default)]
struct Observed {
    requests: Mutex<Vec<Request>>,
}

impl Observed {
    /// Heartbeats received so far whose request carries `token`.
    fn heartbeats(&self, token: &str) -> Vec<Request> {
        self.matching(token, "RecordActivityTaskHeartbeat")
    }

    /// Requests of `method` received so far whose message carries `token`.
    fn matching(&self, token: &str, method: &str) -> Vec<Request> {
        self.requests
            .lock()
            .expect("observed requests")
            .iter()
            .filter(|request| request.method == method && contains(&request.body, token.as_bytes()))
            .cloned()
            .collect()
    }

    /// Every request carrying `token`, in arrival order, as method names.
    fn methods(&self, token: &str) -> Vec<String> {
        self.requests
            .lock()
            .expect("observed requests")
            .iter()
            .filter(|request| contains(&request.body, token.as_bytes()))
            .map(|request| request.method.clone())
            .collect()
    }

    /// Waits until at least `count` heartbeats for `token` have arrived and
    /// returns them.
    fn await_heartbeats(&self, token: &str, count: usize) -> Vec<Request> {
        let deadline = Instant::now() + CALL_DEADLINE;
        loop {
            let heartbeats = self.heartbeats(token);
            if heartbeats.len() >= count {
                return heartbeats;
            }
            assert!(
                Instant::now() < deadline,
                "heartbeat {count} for {token} never arrived"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }
}

/// Whether `haystack` contains `needle`; protobuf carries token and payload
/// bytes verbatim, so a byte search identifies them.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Returns the protobuf message of one gRPC request body, decompressing it
/// when its length-prefix flag says it was sent gzip-compressed.
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
/// current-thread Tokio runtime and returns its loopback address.
fn spawn_server(observed: Arc<Observed>) -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-heartbeat-throttle-double".to_owned())
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

/// Serves one HTTP/2 connection, holding polls open and answering every
/// other request on its own task.
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

/// Records one request and answers it. A heartbeat whose token contains
/// `cancel-now` is answered with `cancel_requested`; one containing
/// `cancel-later` or `missing-later` is answered with `cancel_requested` or
/// `NOT_FOUND` from its second heartbeat on. Everything else succeeds with
/// an empty response.
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
    let body = grpc_message(&received);
    let method = path.rsplit('/').next().unwrap_or_default().to_owned();
    // Earlier requests of the same method for the same token.
    let earlier = {
        let mut requests = observed.requests.lock().expect("observed requests");
        let earlier = requests
            .iter()
            .filter(|request| {
                request.method == method && token_of(&request.body) == token_of(&body)
            })
            .count();
        requests.push(Request {
            at: Instant::now(),
            method: method.clone(),
            body: body.clone(),
        });
        earlier
    };
    let mut grpc_status = "0";
    let mut message: &[u8] = &[];
    if method == "RecordActivityTaskHeartbeat" {
        // `cancel_requested = true` is field 1 with varint value 1.
        if contains(&body, b"cancel-now") || (contains(&body, b"cancel-later") && earlier >= 1) {
            message = &[0x08, 0x01];
        } else if contains(&body, b"missing-later") && earlier >= 1 {
            grpc_status = "5";
        }
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
        let mut frame = vec![
            0,
            0,
            0,
            0,
            u8::try_from(message.len()).expect("short message"),
        ];
        frame.extend_from_slice(message);
        if stream.send_data(Bytes::from(frame), false).is_err() {
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

/// The task token of a request message: every test token starts with
/// `tok-` and ends with `!`, so it can be cut out of the protobuf bytes.
fn token_of(body: &[u8]) -> Option<Vec<u8>> {
    let start = body.windows(4).position(|window| window == b"tok-")?;
    let length = body[start..].iter().position(|byte| *byte == b'!')?;
    Some(body[start..=start + length].to_vec())
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

/// Activity-only worker document with the given heartbeat throttle tuning
/// (a JSON object body, possibly empty).
fn worker_document(tuning: &str) -> String {
    format!(
        r#"{{
    "namespace":"heartbeat-throttle-test",
    "task_queue":"heartbeat-throttle-test",
    "build_id":"heartbeat-throttle-build",
    "versioning":{{"kind":"none"}},
    "max_cached_workflows":10,
    "max_outstanding_workflow_tasks":10,
    "max_concurrent_workflow_task_polls":2,
    "graceful_shutdown_timeout_ms":1000,
    "task_types":{{"workflows":false,"activities":true}},
    "tuning":{{{tuning}}}
}}"#
    )
}

/// Creates a runtime, connects it to the double with `client_extra` added
/// to the client document, and starts an activity worker with `tuning`.
fn start_runtime(address: SocketAddr, client_extra: &str, tuning: &str) -> *mut Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);

    let client =
        format!(r#"{{"target_url":"http://{address}","identity":"throttle-test"{client_extra}}}"#);
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

    let worker = worker_document(tuning);
    let mut result = AbiResult::default();
    // SAFETY: Same exclusive runtime ownership; the document stays readable.
    unsafe {
        ocaml_temporal_core_v4_worker_start_json(
            runtime,
            worker.as_ptr(),
            worker.len(),
            &mut result,
        );
    }
    let (status, _, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
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

/// Awaits one submitted call and returns its status, value, and diagnostic.
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

/// Base64 of a test token; every token is `tok-<name>!` (see [`token_of`]).
fn token(name: &str) -> String {
    format!("tok-{name}!")
}

/// One base64 payload document carrying `marker` as its data.
fn payload(marker: &str) -> String {
    format!(
        r#"{{"metadata":{{}},"data":{{"encoding":"base64","data":"{}"}}}}"#,
        STANDARD.encode(marker)
    )
}

/// An async heartbeat document for `token` with one detail `marker` and the
/// given heartbeat timeout in milliseconds (`None` for unset).
fn heartbeat(token: &str, marker: &str, timeout_ms: Option<u64>) -> String {
    let timeout = match timeout_ms {
        None => "null".to_owned(),
        Some(ms) => format!(
            r#"{{"seconds":{},"nanoseconds":{}}}"#,
            ms / 1_000,
            (ms % 1_000) * 1_000_000
        ),
    };
    format!(
        r#"{{"task_token":"{}","details":[{}],"heartbeat_timeout":{timeout}}}"#,
        STANDARD.encode(token),
        payload(marker)
    )
}

/// Submits a heartbeat and returns its outcome body, asserting success.
fn heartbeat_now(
    runtime: *mut Runtime,
    token: &str,
    marker: &str,
    timeout_ms: Option<u64>,
) -> Vec<u8> {
    let call = submit(
        runtime,
        CALL_ASYNC_ACTIVITY_HEARTBEAT,
        &heartbeat(token, marker, timeout_ms),
    )
    .expect("heartbeat submission");
    let (status, value, message) = await_call(call);
    assert_eq!(status, STATUS_OK, "{message}");
    value
}

/// Submits a terminal request (`completed` or `cancelled`) for `token`.
fn terminal(runtime: *mut Runtime, token: &str, cancelled: bool) {
    let result = if cancelled {
        r#"{"kind":"cancelled","failure":{"message":"cancelled","source":"","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"canceled","details":[],"identity":""}}}"#.to_owned()
    } else {
        format!(r#"{{"kind":"completed","result":{}}}"#, payload(""))
    };
    let document = format!(
        r#"{{"task_token":"{}","result":{result}}}"#,
        STANDARD.encode(token)
    );
    let call =
        submit(runtime, CALL_ASYNC_ACTIVITY_COMPLETION, &document).expect("terminal submission");
    let (status, _, message) = await_call(call);
    assert_eq!(status, STATUS_OK, "{message}");
}

/// The throttle's tracked tokens and retained drivers.
fn counts(runtime: *mut Runtime) -> (usize, usize) {
    // SAFETY: The runtime is live and only used by this thread.
    unsafe { test_heartbeat_throttle_counts(runtime) }.expect("live runtime")
}

/// Waits until the throttle tracks no token and retains no driver.
fn await_empty(runtime: *mut Runtime) {
    let deadline = Instant::now() + CALL_DEADLINE;
    while counts(runtime) != (0, 0) {
        assert!(
            Instant::now() < deadline,
            "throttle state was never released"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

/// Runs `call` on the owned runtime with no extra input.
fn lifecycle(
    runtime: *mut Runtime,
    call: unsafe extern "C" fn(*mut Runtime, *mut AbiResult) -> Status,
) {
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread.
    unsafe { call(runtime, &mut result) };
    let (status, _, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
}

/// Frees the runtime, which closes any remaining children.
fn free(mut runtime: *mut Runtime) {
    // SAFETY: The slot is exclusively owned and not used afterwards.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

/// Shuts the worker down, disconnects, and frees the runtime.
fn close_runtime(runtime: *mut Runtime) {
    lifecycle(runtime, ocaml_temporal_core_v4_worker_shutdown);
    lifecycle(runtime, ocaml_temporal_core_v4_client_disconnect);
    free(runtime);
}

/// The first heartbeat is sent at once; heartbeats within the interval
/// return at once without a send and only replace the pending details; one
/// trailing send carries the latest details when the interval (80% of the
/// 2 s heartbeat timeout) expires, well before the timeout. With nothing
/// pending afterwards the token's state is released, and the next heartbeat
/// is again sent at once.
#[test]
fn first_heartbeat_is_immediate_and_later_ones_coalesce_into_a_trailing_send() {
    let observed = Arc::new(Observed::default());
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)), "", "");
    let token = token("coalesce");

    assert!(heartbeat_now(runtime, &token, "detail-one", Some(2_000)).is_empty());
    let first = observed.heartbeats(&token);
    assert_eq!(first.len(), 1, "the first heartbeat was not sent at once");
    assert!(contains(&first[0].body, b"detail-one"));

    for marker in ["detail-two", "detail-three", "detail-four"] {
        assert!(heartbeat_now(runtime, &token, marker, Some(2_000)).is_empty());
    }
    assert_eq!(
        observed.heartbeats(&token).len(),
        1,
        "a heartbeat within the interval was sent"
    );
    assert_eq!(counts(runtime), (1, 1));

    let sends = observed.await_heartbeats(&token, 2);
    let trailing = &sends[1];
    assert!(contains(&trailing.body, b"detail-four"));
    assert!(!contains(&trailing.body, b"detail-two"));
    let gap = trailing.at.duration_since(sends[0].at);
    assert!(
        gap >= Duration::from_millis(1_500),
        "trailing send after {gap:?}, before the 1.6 s interval"
    );
    assert!(
        gap < Duration::from_millis(2_000),
        "trailing send after {gap:?}, past the 2 s heartbeat timeout"
    );

    // Nothing is pending, so the next interval ends without a send and the
    // state is released.
    await_empty(runtime);
    assert_eq!(observed.heartbeats(&token).len(), 2);

    assert!(heartbeat_now(runtime, &token, "detail-five", Some(2_000)).is_empty());
    assert_eq!(
        observed.heartbeats(&token).len(),
        3,
        "a heartbeat after a quiet interval was not sent at once"
    );
    close_runtime(runtime);
}

/// The worker's throttle settings apply: a long heartbeat timeout is capped
/// by `max_heartbeat_throttle_interval_ms`, and an unset timeout uses
/// `default_heartbeat_throttle_interval_ms`.
#[test]
fn worker_throttle_settings_bound_the_interval() {
    let observed = Arc::new(Observed::default());
    let runtime = start_runtime(
        spawn_server(Arc::clone(&observed)),
        "",
        r#""max_heartbeat_throttle_interval_ms":300,"default_heartbeat_throttle_interval_ms":200"#,
    );

    for (name, timeout_ms, at_least) in [
        ("capped", Some(600_000), Duration::from_millis(250)),
        ("default", None, Duration::from_millis(150)),
    ] {
        let token = token(name);
        heartbeat_now(runtime, &token, "first", timeout_ms);
        heartbeat_now(runtime, &token, "second", timeout_ms);
        let sends = observed.await_heartbeats(&token, 2);
        let gap = sends[1].at.duration_since(sends[0].at);
        assert!(gap >= at_least, "{name}: trailing send after only {gap:?}");
        assert!(
            gap < Duration::from_secs(5),
            "{name}: trailing send after {gap:?}"
        );
        assert!(contains(&sends[1].body, b"second"));
    }
    close_runtime(runtime);
}

/// A cancellation request is the outcome of the send that observed it and
/// of every later coalesced heartbeat, including one seen only by a trailing
/// send.
#[test]
fn cancellation_reaches_the_caller_on_its_next_heartbeat() {
    let observed = Arc::new(Observed::default());
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)), "", "");

    let now = token("cancel-now");
    assert_eq!(
        heartbeat_now(runtime, &now, "a", Some(10_000)),
        b"cancel_requested"
    );
    assert_eq!(
        heartbeat_now(runtime, &now, "b", Some(10_000)),
        b"cancel_requested"
    );
    assert_eq!(observed.heartbeats(&now).len(), 1);

    let later = token("cancel-later");
    assert!(heartbeat_now(runtime, &later, "a", Some(1_000)).is_empty());
    assert!(heartbeat_now(runtime, &later, "b", Some(1_000)).is_empty());
    observed.await_heartbeats(&later, 2);
    // The trailing send's flags are recorded just after it is answered.
    let deadline = Instant::now() + CALL_DEADLINE;
    loop {
        let body = heartbeat_now(runtime, &later, "c", Some(1_000));
        if body == b"cancel_requested" {
            break;
        }
        assert!(body.is_empty());
        assert!(
            Instant::now() < deadline,
            "the trailing cancellation was lost"
        );
        thread::sleep(Duration::from_millis(5));
    }
    close_runtime(runtime);
}

/// A failed trailing send ends the window: the token's state is released,
/// so the next heartbeat is sent at once and reports the server's answer
/// (`not_found`) itself instead of a coalesced success.
#[test]
fn failed_trailing_send_ends_the_window() {
    let observed = Arc::new(Observed::default());
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)), "", "");
    let token = token("missing-later");

    assert!(heartbeat_now(runtime, &token, "a", Some(500)).is_empty());
    assert!(heartbeat_now(runtime, &token, "b", Some(500)).is_empty());
    observed.await_heartbeats(&token, 2);
    await_empty(runtime);
    assert_eq!(heartbeat_now(runtime, &token, "c", Some(500)), b"not_found");
    assert_eq!(observed.heartbeats(&token).len(), 3);
    close_runtime(runtime);
}

/// A terminal request evicts its token: a sleeping driver is aborted, so no
/// trailing heartbeat follows a completion and the state is released at
/// once. Before a cancellation the pending details are flushed first, as
/// Core does.
#[test]
fn terminal_requests_evict_the_token() {
    let observed = Arc::new(Observed::default());
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)), "", "");

    let completed = token("completed");
    heartbeat_now(runtime, &completed, "a", Some(1_000));
    heartbeat_now(runtime, &completed, "pending", Some(1_000));
    assert_eq!(counts(runtime), (1, 1));
    terminal(runtime, &completed, false);
    await_empty(runtime);
    thread::sleep(Duration::from_millis(1_200));
    assert_eq!(
        observed.methods(&completed),
        [
            "RecordActivityTaskHeartbeat",
            "RespondActivityTaskCompleted"
        ],
        "a heartbeat followed the completion"
    );

    let cancelled = token("cancelled");
    heartbeat_now(runtime, &cancelled, "a", Some(1_000));
    heartbeat_now(runtime, &cancelled, "flushed", Some(1_000));
    terminal(runtime, &cancelled, true);
    await_empty(runtime);
    thread::sleep(Duration::from_millis(1_200));
    assert_eq!(
        observed.methods(&cancelled),
        [
            "RecordActivityTaskHeartbeat",
            "RecordActivityTaskHeartbeat",
            "RespondActivityTaskCanceled"
        ]
    );
    assert!(contains(
        &observed.heartbeats(&cancelled)[1].body,
        b"flushed"
    ));
    close_runtime(runtime);
}

/// Disconnect aborts and joins every driver and releases every token, and so
/// does closing a runtime that still has its client: no trailing heartbeat
/// is sent afterwards.
#[test]
fn disconnect_and_close_release_throttle_state() {
    let observed = Arc::new(Observed::default());
    let address = spawn_server(Arc::clone(&observed));

    let runtime = start_runtime(address, "", "");
    let disconnected = token("disconnected");
    heartbeat_now(runtime, &disconnected, "a", Some(1_000));
    heartbeat_now(runtime, &disconnected, "pending", Some(1_000));
    assert_eq!(counts(runtime), (1, 1));
    lifecycle(runtime, ocaml_temporal_core_v4_worker_shutdown);
    lifecycle(runtime, ocaml_temporal_core_v4_client_disconnect);
    assert_eq!(counts(runtime), (0, 0));
    free(runtime);

    let runtime = start_runtime(address, "", "");
    let closed = token("closed");
    heartbeat_now(runtime, &closed, "a", Some(1_000));
    heartbeat_now(runtime, &closed, "pending", Some(1_000));
    lifecycle(runtime, ocaml_temporal_core_v4_worker_shutdown);
    free(runtime);

    thread::sleep(Duration::from_millis(1_200));
    assert_eq!(observed.heartbeats(&disconnected).len(), 1);
    assert_eq!(observed.heartbeats(&closed).len(), 1);
}

/// Throttle state does not grow with the number of tokens: state for many
/// tokens is released once their intervals pass, and at most the
/// connection's in-flight call limit of tokens is tracked at once; beyond
/// it, heartbeats are sent unthrottled.
#[test]
fn throttle_state_stays_bounded_across_many_tokens() {
    let observed = Arc::new(Observed::default());
    let runtime = start_runtime(
        spawn_server(Arc::clone(&observed)),
        r#","max_in_flight_calls":8"#,
        "",
    );

    for index in 0..40 {
        let token = token(&format!("bounded-{index}"));
        heartbeat_now(runtime, &token, "a", Some(60_000));
        let (tokens, drivers) = counts(runtime);
        assert!(tokens <= 8, "{tokens} tokens tracked");
        assert!(drivers <= 8, "{drivers} drivers retained");
    }
    // Every token beyond the limit was sent, not dropped.
    for index in 0..40 {
        assert_eq!(
            observed
                .heartbeats(&token(&format!("bounded-{index}")))
                .len(),
            1
        );
    }
    // An untracked token's heartbeats are each sent at once.
    let untracked = token("bounded-39");
    heartbeat_now(runtime, &untracked, "b", Some(60_000));
    assert_eq!(observed.heartbeats(&untracked).len(), 2);
    close_runtime(runtime);

    let runtime = start_runtime(spawn_server(Arc::clone(&observed)), "", "");
    for round in 0..3 {
        for index in 0..200 {
            let token = token(&format!("churn-{index}"));
            heartbeat_now(runtime, &token, &format!("round-{round}"), Some(100));
        }
        await_empty(runtime);
    }
    close_runtime(runtime);
}

/// The submitted heartbeat document must carry the heartbeat timeout; the
/// synchronous heartbeat document is refused before any RPC.
#[test]
fn submitted_heartbeat_requires_the_timeout_field() {
    let observed = Arc::new(Observed::default());
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)), "", "");
    let token = token("no-timeout-field");
    let document = format!(
        r#"{{"task_token":"{}","details":[]}}"#,
        STANDARD.encode(&token)
    );
    let refused = submit(runtime, CALL_ASYNC_ACTIVITY_HEARTBEAT, &document)
        .expect_err("a heartbeat without its timeout field was submitted");
    assert_eq!(refused.0, STATUS_PROTOCOL, "{}", refused.1);
    assert!(observed.heartbeats(&token).is_empty());
    close_runtime(runtime);
}

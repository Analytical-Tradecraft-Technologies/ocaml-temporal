//! Regression coverage for issue #1137: the submitted `HISTORY` client call
//! that fetches a run's complete history for `Temporal.Replay`.
//!
//! A minimal plaintext HTTP/2 gRPC double stands in for the Temporal server,
//! so the tests are deterministic and run in every Rust test job. It serves
//! `GetWorkflowExecutionHistory` for three workflow IDs: [`PAGED`] returns
//! one event per page and a continuation token until the last event,
//! [`MISSING`] fails with `NOT_FOUND`, and any other ID (the tests use
//! `empty`) returns a page with no events. Every other RPC succeeds with an empty message.

use std::io::Read as _;
use std::net::SocketAddr;
use std::ptr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, Runtime, STATUS_CONNECTION, STATUS_NOT_READY, STATUS_OK,
    STATUS_PROTOCOL, Status, ocaml_temporal_core_v4_client_await_call,
    ocaml_temporal_core_v4_client_connect_json, ocaml_temporal_core_v4_client_disconnect,
    ocaml_temporal_core_v4_client_submit_json, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new,
};
use prost::Message as _;
use prost::bytes::Bytes;
use temporalio_common::protos::temporal::api::{
    history::v1::{History, HistoryEvent},
    workflowservice::v1::{
        GetWorkflowExecutionHistoryRequest, GetWorkflowExecutionHistoryResponse,
    },
};

/// Selector of a submitted history fetch, as in the C header.
const CALL_HISTORY: u32 = 13;

/// Workflow ID whose history the double serves one event per page.
const PAGED: &str = "paged";
/// Workflow ID the double answers with `NOT_FOUND`.
const MISSING: &str = "missing";
/// The closed failure of a Core conversion; its text never crosses the ABI.
const CORE_INVALID: &str = r#"{"kind":"protocol","code":"core_invalid"}"#;

/// Number of events in [`PAGED`]'s history.
const PAGED_EVENTS: i64 = 3;

/// Upper bound for one bridge call under test; a regression fails instead of
/// hanging CI.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// History requests the double received, in arrival order.
type Observed = Arc<Mutex<Vec<GetWorkflowExecutionHistoryRequest>>>;

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

/// The event with identifier `id` of [`PAGED`]'s history. Only the
/// identifier matters: the bridge re-encodes pages without interpreting them.
fn event(id: i64) -> HistoryEvent {
    HistoryEvent {
        event_id: id,
        ..Default::default()
    }
}

/// Answers one history request: the page after the event named by its
/// token for [`PAGED`], `NOT_FOUND` for [`MISSING`], and an empty page for
/// anything else. Returns the response message or a gRPC status code.
fn history_page(request: &GetWorkflowExecutionHistoryRequest) -> Result<Vec<u8>, &'static str> {
    let workflow_id = request
        .execution
        .as_ref()
        .map(|execution| execution.workflow_id.as_str())
        .unwrap_or_default();
    match workflow_id {
        MISSING => Err("5"),
        PAGED => {
            let next: i64 = if request.next_page_token.is_empty() {
                1
            } else {
                String::from_utf8(request.next_page_token.clone())
                    .expect("token is the decimal event ID")
                    .parse()
                    .expect("token is the decimal event ID")
            };
            let response = GetWorkflowExecutionHistoryResponse {
                history: Some(History {
                    events: vec![event(next)],
                }),
                next_page_token: if next < PAGED_EVENTS {
                    (next + 1).to_string().into_bytes()
                } else {
                    Vec::new()
                },
                ..Default::default()
            };
            Ok(response.encode_to_vec())
        }
        _ => Ok(GetWorkflowExecutionHistoryResponse::default().encode_to_vec()),
    }
}

/// Starts the gRPC double on a detached OS thread with its own
/// current-thread Tokio runtime and returns its loopback address. The thread
/// ends with the test process.
fn spawn_server(observed: Observed) -> SocketAddr {
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-history-double".to_owned())
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

/// Serves one HTTP/2 connection until the client closes it, answering each
/// request on its own task.
async fn serve_connection(socket: tokio::net::TcpStream, observed: Observed) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((request, respond))) = connection.accept().await {
        tokio::spawn(answer(request, respond, Arc::clone(&observed)));
    }
}

/// Reads one request body and answers it as described in the module docs.
async fn answer(
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    observed: Observed,
) {
    let path = request.uri().path().to_owned();
    let mut body = request.into_body();
    let mut received = Vec::new();
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
        received.extend_from_slice(&chunk);
    }
    let received = grpc_message(&received);
    // The default instance of every other response message encodes to zero
    // bytes, so an empty message is a valid success.
    let outcome = if path.ends_with("/GetWorkflowExecutionHistory") {
        let history_request = GetWorkflowExecutionHistoryRequest::decode(received.as_slice())
            .expect("Core sends a valid history request");
        let outcome = history_page(&history_request);
        observed
            .lock()
            .expect("observed lock")
            .push(history_request);
        outcome
    } else {
        Ok(Vec::new())
    };
    let response = http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(())
        .expect("static gRPC response");
    let Ok(mut stream) = respond.send_response(response, false) else {
        return;
    };
    let grpc_status = match outcome {
        Ok(message) => {
            let length = u32::try_from(message.len()).expect("small test message");
            let mut frame = vec![0];
            frame.extend_from_slice(&length.to_be_bytes());
            frame.extend_from_slice(&message);
            if stream.send_data(Bytes::from(frame), false).is_err() {
                return;
            }
            "0"
        }
        Err(code) => code,
    };
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

/// Creates a runtime connected to the double and returns it, exclusively
/// owned by the caller.
fn start_runtime(address: SocketAddr) -> *mut Runtime {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) };
    assert_eq!(release(result).0, STATUS_OK);
    let client = format!(r#"{{"target_url":"http://{address}","identity":"history-test"}}"#);
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
    runtime
}

/// Submits one history fetch and returns its handle, or the submission's
/// failure.
fn submit(runtime: *mut Runtime, document: &str) -> Result<(u64, u64), (Status, String)> {
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread; the
    // document stays readable for the call.
    unsafe {
        ocaml_temporal_core_v4_client_submit_json(
            runtime,
            CALL_HISTORY,
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

/// Submits one history fetch, awaits it from this thread, and returns its
/// terminal status, value, and diagnostic.
fn fetch(runtime: *mut Runtime, document: &str) -> (Status, Vec<u8>, String) {
    let (owner, call) = submit(runtime, document).expect("history submission");
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

/// Disconnects the client and frees the runtime.
fn close_runtime(mut runtime: *mut Runtime) {
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and exclusively owned by this thread.
    unsafe { ocaml_temporal_core_v4_client_disconnect(runtime, &mut result) };
    let (status, _, message) = release(result);
    assert_eq!(status, STATUS_OK, "{message}");
    // SAFETY: The client is closed and the slot is exclusively owned.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

/// The bridge follows every continuation token and returns one binary
/// `History` whose events are all pages' events in order: exactly the
/// message a single page would have carried. Each page is requested for the
/// same run with every event, without waiting, and with the caller's page
/// size.
#[test]
fn history_fetch_concatenates_every_page() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let (status, value, message) = fetch(
        runtime,
        r#"{"namespace":"default","workflow_id":"paged","run_id":"run","page_size":1}"#,
    );
    assert_eq!(status, STATUS_OK, "{message}");
    let expected = History {
        events: (1..=PAGED_EVENTS).map(event).collect(),
    };
    assert_eq!(value, expected.encode_to_vec());
    let requests = observed.lock().expect("observed lock");
    assert_eq!(requests.len(), 3);
    for (index, request) in requests.iter().enumerate() {
        let execution = request.execution.as_ref().expect("execution");
        assert_eq!(request.namespace, "default");
        assert_eq!(execution.workflow_id, PAGED);
        assert_eq!(execution.run_id, "run");
        assert_eq!(request.maximum_page_size, 1);
        assert!(!request.wait_new_event);
        assert_eq!(request.history_event_filter_type, 1, "all events");
        let token = if index == 0 {
            Vec::new()
        } else {
            (index + 1).to_string().into_bytes()
        };
        assert_eq!(request.next_page_token, token);
    }
    drop(requests);
    close_runtime(runtime);
}

/// A run the server does not know is the closed `rpc` failure document with
/// the connection status, and a history with no events is a protocol error
/// rather than an empty success that replay would reject later.
#[test]
fn history_fetch_reports_missing_and_empty_histories() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    let (status, _, message) = fetch(
        runtime,
        r#"{"namespace":"default","workflow_id":"missing","run_id":""}"#,
    );
    assert_eq!(status, STATUS_CONNECTION, "{message}");
    assert_eq!(message, r#"{"kind":"rpc","code":"not_found"}"#);
    let (status, _, message) = fetch(
        runtime,
        r#"{"namespace":"default","workflow_id":"empty","run_id":""}"#,
    );
    assert_eq!(status, STATUS_PROTOCOL, "{message}");
    assert_eq!(message, CORE_INVALID);
    assert_eq!(
        observed.lock().expect("observed lock")[1].maximum_page_size,
        0,
        "an absent page size asks for the server default"
    );
    close_runtime(runtime);
}

/// A history request names one run, exact or current, takes an optional
/// page size between 1 and 1000 and RPC timeout between 1 ms and one minute,
/// and is closed: anything else fails its submission synchronously, before
/// any task or RPC exists.
#[test]
fn history_fetch_validates_before_the_rpc() {
    let observed = Observed::default();
    let runtime = start_runtime(spawn_server(Arc::clone(&observed)));
    for document in [
        r#"{"namespace":"default","workflow_id":"","run_id":""}"#,
        r#"{"namespace":"","workflow_id":"w","run_id":""}"#,
        "{\"namespace\":\"default\",\"workflow_id\":\"w\\u0000\",\"run_id\":\"\"}",
        r#"{"namespace":"default","workflow_id":"w","run_id":"","page_size":0}"#,
        r#"{"namespace":"default","workflow_id":"w","run_id":"","page_size":1001}"#,
        r#"{"namespace":"default","workflow_id":"w","run_id":"","page_size":"10"}"#,
        r#"{"namespace":"default","workflow_id":"w","run_id":"","rpc_timeout_ms":0}"#,
        r#"{"namespace":"default","workflow_id":"w","run_id":"","rpc_timeout_ms":60001}"#,
        r#"{"namespace":"default","workflow_id":"w"}"#,
        r#"{"namespace":"default","workflow_id":"w","run_id":"","next_page_token":null}"#,
    ] {
        let refused = submit(runtime, document).expect_err(document);
        assert_eq!(refused.0, STATUS_PROTOCOL, "{document}: {}", refused.1);
    }
    assert!(observed.lock().expect("observed lock").is_empty());
    // The bounds themselves are accepted. The one-minute fetch reaches the
    // double's empty page; the 1 ms one may instead expire first.
    let (status, _, message) = fetch(
        runtime,
        r#"{"namespace":"default","workflow_id":"empty","run_id":"r","page_size":1000,"rpc_timeout_ms":60000}"#,
    );
    assert_eq!((status, message.as_str()), (STATUS_PROTOCOL, CORE_INVALID));
    let (status, _, message) = fetch(
        runtime,
        r#"{"namespace":"default","workflow_id":"empty","run_id":"r","page_size":1,"rpc_timeout_ms":1}"#,
    );
    assert!(
        (status, message.as_str()) == (STATUS_PROTOCOL, CORE_INVALID)
            || (status == STATUS_CONNECTION && message.contains("deadline_exceeded")),
        "{message}"
    );
    close_runtime(runtime);
}

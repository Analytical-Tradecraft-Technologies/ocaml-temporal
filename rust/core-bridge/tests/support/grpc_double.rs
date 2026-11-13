//! Minimal plaintext HTTP/2 gRPC double for in-crate live-worker tests.
//!
//! In-crate tests (child modules of `src/abi.rs`) can reach private bridge
//! state, such as a worker's shutdown bounds, while still calling the real C
//! ABI entry points. They cannot share the doubles of the integration-test
//! binaries, so this module provides one whose answer to each request is
//! chosen by a test-supplied [`Responder`].
//!
//! The double runs on its own detached OS thread with a private
//! current-thread Tokio runtime, so it never shares an executor with the
//! bridge under test and simply ends with the test process. A held request
//! keeps its stream open until the client resets it or closes the
//! connection, like an idle server long poll.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use prost::bytes::Bytes;

/// How long [`spawn`] waits for the server thread to publish its address.
const STARTUP_DEADLINE: Duration = Duration::from_secs(30);

/// The double's answer to one request.
pub(super) enum Reply {
    /// A successful response carrying this encoded protobuf message. The
    /// default instance of every response message encodes to zero bytes, so
    /// an empty vector is a valid success for any method.
    Message(Vec<u8>),
    /// No response: the stream stays open, like a server long poll.
    Hold,
}

/// Chooses the [`Reply`] for a request, given the gRPC method name (the last
/// path segment, such as `PollWorkflowTaskQueue`) and how many earlier
/// requests for that method the double had already received.
pub(super) type Responder = dyn Fn(&str, usize) -> Reply + Send + Sync;

/// A running double: its loopback address and the requests it received.
pub(super) struct Double {
    /// Address to put in a client's `target_url`.
    pub(super) address: SocketAddr,
    /// Requests received so far, per method name, including held ones.
    received: Arc<Mutex<HashMap<String, usize>>>,
}

impl Double {
    /// Number of requests for `method` received so far, held ones included.
    pub(super) fn received(&self, method: &str) -> usize {
        self.received
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(method)
            .copied()
            .unwrap_or(0)
    }
}

/// Starts a double that answers every request through `responder`.
pub(super) fn spawn(responder: Arc<Responder>) -> Double {
    let received = Arc::new(Mutex::new(HashMap::new()));
    let counts = Arc::clone(&received);
    let (address_sender, address_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("grpc-in-crate-double".to_owned())
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
                    tokio::spawn(serve_connection(
                        socket,
                        Arc::clone(&responder),
                        Arc::clone(&counts),
                    ));
                }
            });
        })
        .expect("spawn test server thread");
    let address = address_receiver
        .recv_timeout(STARTUP_DEADLINE)
        .expect("test server publishes its address");
    Double { address, received }
}

/// Serves one HTTP/2 connection until the client closes it. Held requests
/// keep their response handles in `held` for the life of the connection.
async fn serve_connection(
    socket: tokio::net::TcpStream,
    responder: Arc<Responder>,
    received: Arc<Mutex<HashMap<String, usize>>>,
) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    let mut held = Vec::new();
    while let Some(Ok((request, mut respond))) = connection.accept().await {
        let method = request
            .uri()
            .path()
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        let earlier = {
            let mut counts = received.lock().unwrap_or_else(|error| error.into_inner());
            let count = counts.entry(method.clone()).or_insert(0);
            let earlier = *count;
            *count += 1;
            earlier
        };
        let message = match responder(&method, earlier) {
            Reply::Message(message) => message,
            Reply::Hold => {
                held.push(respond);
                continue;
            }
        };
        let response = http::Response::builder()
            .status(200)
            .header("content-type", "application/grpc")
            .body(())
            .expect("static gRPC response");
        let Ok(mut stream) = respond.send_response(response, false) else {
            continue;
        };
        // gRPC length-prefixed message: uncompressed flag, big-endian length.
        let length = u32::try_from(message.len()).expect("small test message");
        let mut frame = Vec::with_capacity(5 + message.len());
        frame.push(0);
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&message);
        if stream.send_data(Bytes::from(frame), false).is_err() {
            continue;
        }
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        let _ = stream.send_trailers(trailers);
    }
}

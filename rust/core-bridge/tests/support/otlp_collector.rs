//! Loopback OTLP collector doubles for the OTLP exporter tests (#1161).
//!
//! Two minimal collectors accept `ExportMetricsServiceRequest` messages the
//! way a real OpenTelemetry Collector does: [`Collector::http`] serves
//! OTLP/HTTP (`POST` with a protobuf body over HTTP/1.1) and
//! [`Collector::grpc`] serves OTLP/gRPC over plaintext HTTP/2 with the `h2`
//! crate the other gRPC doubles use. Each records the request path, every
//! request header, and the decoded message, and answers either success or an
//! authentication failure ([`Answer`]). They bind ephemeral loopback ports,
//! never reach the network, and run on detached threads that end with the
//! test process.
//!
//! The protobuf messages below are the subset of
//! `opentelemetry/proto/collector/metrics/v1` and
//! `opentelemetry/proto/metrics/v1` the assertions read, declared with
//! `prost` derives (prost skips the fields they omit). Field numbers follow
//! the OTLP specification.

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use prost::Message as _;
use prost::bytes::Bytes;

/// `opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ExportMetricsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_metrics: Vec<ResourceMetrics>,
}

/// `opentelemetry.proto.metrics.v1.ResourceMetrics`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ResourceMetrics {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_metrics: Vec<ScopeMetrics>,
}

/// `opentelemetry.proto.resource.v1.Resource`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Resource {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
}

/// `opentelemetry.proto.common.v1.KeyValue`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<AnyValue>,
}

/// `opentelemetry.proto.common.v1.AnyValue`, string member only.
#[derive(Clone, PartialEq, prost::Message)]
pub struct AnyValue {
    #[prost(string, optional, tag = "1")]
    pub string_value: Option<String>,
}

/// `opentelemetry.proto.metrics.v1.ScopeMetrics`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ScopeMetrics {
    #[prost(message, repeated, tag = "2")]
    pub metrics: Vec<Metric>,
}

/// `opentelemetry.proto.metrics.v1.Metric`, with the `sum` and `histogram`
/// members of its `data` one-of.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Metric {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub unit: String,
    #[prost(message, optional, tag = "7")]
    pub sum: Option<Sum>,
    #[prost(message, optional, tag = "9")]
    pub histogram: Option<Histogram>,
}

/// `opentelemetry.proto.metrics.v1.Sum`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Sum {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<NumberDataPoint>,
    /// 1 is delta, 2 is cumulative.
    #[prost(int32, tag = "2")]
    pub aggregation_temporality: i32,
}

/// `opentelemetry.proto.metrics.v1.NumberDataPoint`; the `value` one-of is
/// read as its two optional members.
#[derive(Clone, PartialEq, prost::Message)]
pub struct NumberDataPoint {
    #[prost(double, optional, tag = "4")]
    pub as_double: Option<f64>,
    #[prost(sfixed64, optional, tag = "6")]
    pub as_int: Option<i64>,
    #[prost(message, repeated, tag = "7")]
    pub attributes: Vec<KeyValue>,
}

/// `opentelemetry.proto.metrics.v1.Histogram`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Histogram {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<HistogramDataPoint>,
}

/// `opentelemetry.proto.metrics.v1.HistogramDataPoint`, count only.
#[derive(Clone, PartialEq, prost::Message)]
pub struct HistogramDataPoint {
    #[prost(fixed64, tag = "4")]
    pub count: u64,
}

/// How a collector double answers every export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// HTTP `200` with an empty response message, or gRPC status `OK`.
    Accept,
    /// HTTP `401`, or gRPC status `UNAUTHENTICATED` (16).
    Reject,
}

/// One export request as the collector received it.
#[derive(Clone, Debug)]
pub struct Export {
    /// Request path, for example `/v1/metrics`.
    pub path: String,
    /// Every request header, names lowercased, in arrival order.
    pub headers: Vec<(String, String)>,
    /// The decoded request message.
    pub request: ExportMetricsServiceRequest,
}

impl Export {
    /// The value of the first header named `name` (lowercase).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// Every metric of the request, across resources and scopes.
    pub fn metrics(&self) -> impl Iterator<Item = &Metric> {
        self.request
            .resource_metrics
            .iter()
            .flat_map(|resource| resource.scope_metrics.iter())
            .flat_map(|scope| scope.metrics.iter())
    }

    /// The string resource attribute `key`, if any resource carries it.
    pub fn resource_attribute(&self, key: &str) -> Option<String> {
        self.request
            .resource_metrics
            .iter()
            .filter_map(|resource| resource.resource.as_ref())
            .flat_map(|resource| resource.attributes.iter())
            .find(|attribute| attribute.key == key)
            .and_then(|attribute| attribute.value.as_ref())
            .and_then(|value| value.string_value.clone())
    }
}

/// The names of every metric in `exports`.
pub fn metric_names(exports: &[Export]) -> BTreeSet<String> {
    exports
        .iter()
        .flat_map(|export| export.metrics().map(|metric| metric.name.clone()))
        .collect()
}

/// The largest value of sum `name`'s first data point in any export, read
/// from whichever numeric member the exporter used.
pub fn sum_value(exports: &[Export], name: &str) -> Option<f64> {
    exports
        .iter()
        .flat_map(|export| export.metrics())
        .filter(|metric| metric.name == name)
        .filter_map(|metric| metric.sum.as_ref())
        .filter_map(|sum| sum.data_points.first())
        .filter_map(|point| {
            // The probes record small values, so an integer point always
            // fits `i32` and converts to `f64` exactly.
            point.as_double.or_else(|| {
                point
                    .as_int
                    .and_then(|value| i32::try_from(value).ok())
                    .map(f64::from)
            })
        })
        .reduce(f64::max)
}

/// A running collector double.
pub struct Collector {
    /// The loopback address it listens on.
    pub address: SocketAddr,
    exports: Arc<Mutex<Vec<Export>>>,
}

impl Collector {
    /// Starts an OTLP/HTTP collector.
    pub fn http(answer: Answer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP collector");
        let address = listener.local_addr().expect("HTTP collector address");
        let exports = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&exports);
        thread::Builder::new()
            .name("otlp-http-collector".to_owned())
            .spawn(move || {
                for stream in listener.incoming().flatten() {
                    let recorded = Arc::clone(&recorded);
                    thread::spawn(move || serve_http(stream, answer, &recorded));
                }
            })
            .expect("spawn HTTP collector");
        Self { address, exports }
    }

    /// Starts an OTLP/gRPC collector on its own current-thread Tokio runtime.
    pub fn grpc(answer: Answer) -> Self {
        let exports = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&exports);
        let (address_sender, address_receiver) = mpsc::channel();
        thread::Builder::new()
            .name("otlp-grpc-collector".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("collector runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind gRPC collector");
                    address_sender
                        .send(listener.local_addr().expect("gRPC collector address"))
                        .expect("report collector address");
                    loop {
                        let Ok((socket, _)) = listener.accept().await else {
                            continue;
                        };
                        tokio::spawn(serve_grpc(socket, answer, Arc::clone(&recorded)));
                    }
                });
            })
            .expect("spawn gRPC collector");
        let address = address_receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("gRPC collector started");
        Self { address, exports }
    }

    /// A snapshot of every export received so far.
    pub fn exports(&self) -> Vec<Export> {
        self.exports.lock().expect("exports lock").clone()
    }

    /// Waits up to `bound` for `ready` to hold over the received exports and
    /// returns them; panics with the observed metric names otherwise.
    pub fn wait_for(&self, bound: Duration, ready: impl Fn(&[Export]) -> bool) -> Vec<Export> {
        let deadline = Instant::now() + bound;
        loop {
            let exports = self.exports();
            if ready(&exports) {
                return exports;
            }
            assert!(
                Instant::now() < deadline,
                "collector condition not met within {bound:?}; {} exports, metrics {:?}",
                exports.len(),
                metric_names(&exports)
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Serves HTTP/1.1 requests on one connection until the client closes it.
fn serve_http(mut stream: TcpStream, answer: Answer, recorded: &Mutex<Vec<Export>>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let mut pending = Vec::new();
    let mut chunk = [0_u8; 16 * 1024];
    loop {
        let head_end = loop {
            if let Some(index) = pending.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(count) => pending.extend_from_slice(&chunk[..count]),
            }
        };
        let head = String::from_utf8_lossy(&pending[..head_end]).into_owned();
        let mut lines = head.split("\r\n");
        let path = lines
            .next()
            .and_then(|line| line.split(' ').nth(1))
            .unwrap_or_default()
            .to_owned();
        let headers: Vec<(String, String)> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            .collect();
        let length = headers
            .iter()
            .find(|(name, _)| name == "content-length")
            .and_then(|(_, value)| value.parse::<usize>().ok())
            .unwrap_or(0);
        while pending.len() < head_end + length {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(count) => pending.extend_from_slice(&chunk[..count]),
            }
        }
        let body: Vec<u8> = pending.drain(..head_end + length).skip(head_end).collect();
        let request =
            ExportMetricsServiceRequest::decode(body.as_slice()).expect("OTLP/HTTP protobuf body");
        recorded.lock().expect("exports lock").push(Export {
            path,
            headers,
            request,
        });
        let response: &[u8] = match answer {
            Answer::Accept => {
                b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\n\r\n"
            }
            Answer::Reject => b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n",
        };
        if stream.write_all(response).is_err() {
            return;
        }
    }
}

/// Serves one HTTP/2 connection, answering each export on its own task.
async fn serve_grpc(
    socket: tokio::net::TcpStream,
    answer: Answer,
    recorded: Arc<Mutex<Vec<Export>>>,
) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((request, respond))) = connection.accept().await {
        tokio::spawn(answer_grpc(request, respond, answer, Arc::clone(&recorded)));
    }
}

/// Records one gRPC export and answers it as `answer` says.
async fn answer_grpc(
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    answer: Answer,
    recorded: Arc<Mutex<Vec<Export>>>,
) {
    let path = request.uri().path().to_owned();
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    let mut body = request.into_body();
    let mut received = Vec::new();
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
        received.extend_from_slice(&chunk);
    }
    // One length-prefixed gRPC message: a compression flag (the exporter
    // sends uncompressed messages by default), a big-endian length, the body.
    assert!(received.len() >= 5, "gRPC message frame");
    assert_eq!(received[0], 0, "uncompressed gRPC message");
    let request =
        ExportMetricsServiceRequest::decode(&received[5..]).expect("OTLP/gRPC protobuf message");
    recorded.lock().expect("exports lock").push(Export {
        path,
        headers,
        request,
    });
    let response = http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(())
        .expect("static gRPC response");
    let Ok(mut stream) = respond.send_response(response, false) else {
        return;
    };
    let status = match answer {
        Answer::Accept => {
            // An empty ExportMetricsServiceResponse.
            if stream
                .send_data(Bytes::from_static(&[0, 0, 0, 0, 0]), false)
                .is_err()
            {
                return;
            }
            "0"
        }
        Answer::Reject => "16",
    };
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static(status));
    let _ = stream.send_trailers(trailers);
}

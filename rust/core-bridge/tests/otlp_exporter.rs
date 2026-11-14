//! Opt-in OTLP metrics push export on a shared runtime (#1161).
//!
//! These tests drive `shared_runtime_new_with_telemetry_json` and the SDK
//! metric meter through the C ABI against the loopback collector doubles in
//! `support/otlp_collector.rs`, over both OTLP/gRPC and OTLP/HTTP. They prove
//! that exports reach the collector with the configured headers, resource
//! attributes and series; that the exported names are exactly the families
//! the Prometheus path serves for the same series; that releasing the runtime
//! makes one final export and nothing afterwards; that a rejecting or
//! unreachable collector neither fails the runtime nor puts a header value
//! into Core's log; and that invalid documents publish nothing. Nothing
//! leaves the loopback interface.

#[path = "support/otlp_collector.rs"]
mod otlp_collector;

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ptr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::diagnostics::CoreLogLevel;
use ocaml_temporal_core_bridge::{
    MetricMeter, Result as AbiResult, Runtime, STATUS_CONFIGURATION, STATUS_OK, SharedRuntime,
    ocaml_temporal_core_v4_metric_meter_free, ocaml_temporal_core_v4_metric_meter_record,
    ocaml_temporal_core_v4_metric_meter_register_json, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new_attached,
    ocaml_temporal_core_v4_shared_runtime_free, ocaml_temporal_core_v4_shared_runtime_metric_meter,
    ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json, test_runtime_has_metrics_meter,
    test_shared_runtime_new_with_telemetry_and_log_sink,
};
use otlp_collector::{Answer, Collector, Export, metric_names, sum_value};

/// A credential-shaped header value that must reach the collector and never
/// the log or an error message.
const SECRET: &str = "Bearer s3cr3t-otlp-token";

/// Longest wait for a periodic export at the 100 ms period the tests use.
const EXPORT_WAIT: Duration = Duration::from_secs(20);

/// The two OTLP transports, by their document spelling.
const PROTOCOLS: [&str; 2] = ["grpc", "http_protobuf"];

/// Copies one bridge buffer into owned text.
fn buffer_text(ptr: *const u8, len: usize) -> String {
    if len == 0 {
        return String::new();
    }
    // SAFETY: A live result buffer is readable for `len` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    String::from_utf8(bytes).expect("bridge text is UTF-8")
}

/// Releases one initialized result, asserting the release succeeded.
fn free_result(result: &mut AbiResult) {
    // SAFETY: The result was initialized by the bridge and is freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(result) },
        STATUS_OK
    );
}

/// Creates a shared runtime from `document`; returns the status, the
/// (possibly null) handle, and the success value or error text.
fn create(document: &str) -> (i32, *mut SharedRuntime, String) {
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
    let text = if status == STATUS_OK {
        buffer_text(result.value.ptr, result.value.len)
    } else {
        buffer_text(result.error.ptr, result.error.len)
    };
    free_result(&mut result);
    (status, shared, text)
}

/// Creates a shared runtime that must be accepted and returns it with its
/// reported address.
fn create_ok(document: &str) -> (*mut SharedRuntime, String) {
    let (status, shared, text) = create(document);
    assert_eq!(status, STATUS_OK, "{text}");
    assert!(!shared.is_null());
    (shared, text)
}

/// Releases a shared handle through its slot, waiting for Core when it holds
/// the last reference, and returns how long the release took.
fn free_shared(shared: &mut *mut SharedRuntime) -> Duration {
    let started = Instant::now();
    // SAFETY: The slot holds a live handle or null and is used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(shared) },
        STATUS_OK
    );
    assert!(shared.is_null());
    started.elapsed()
}

/// The runtime's SDK metric meter, which must exist.
fn meter_of(shared: *const SharedRuntime) -> *mut MetricMeter {
    let mut meter: *mut MetricMeter = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: `shared` is live; both outputs are writable.
    let status = unsafe {
        ocaml_temporal_core_v4_shared_runtime_metric_meter(shared, &mut meter, &mut result)
    };
    free_result(&mut result);
    assert_eq!(status, STATUS_OK);
    assert!(!meter.is_null(), "an exporting runtime has an SDK meter");
    meter
}

/// Releases a meter through its slot.
fn free_meter(meter: &mut *mut MetricMeter) {
    // SAFETY: The slot holds a live meter or null and is used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_metric_meter_free(meter) },
        STATUS_OK
    );
}

/// Registers one series that must be accepted and returns its identifier.
fn register(meter: *const MetricMeter, document: &str) -> u32 {
    let mut id = 0_u32;
    let mut result = AbiResult::default();
    // SAFETY: The meter is live, the document outlives the call, and both
    // outputs are writable.
    let status = unsafe {
        ocaml_temporal_core_v4_metric_meter_register_json(
            meter,
            document.as_ptr(),
            document.len(),
            &mut id,
            &mut result,
        )
    };
    let error = buffer_text(result.error.ptr, result.error.len);
    free_result(&mut result);
    assert_eq!(status, STATUS_OK, "{error}");
    id
}

/// Records one value that must be accepted.
fn record(meter: *const MetricMeter, id: u32, value: i64) {
    // SAFETY: The meter is live.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_metric_meter_record(meter, id, value) },
        STATUS_OK
    );
}

/// An OTLP document for `protocol` aimed at `collector`, with the secret
/// header, a tenant header, one global tag and the given period.
fn otlp_document(protocol: &str, collector: SocketAddr, period_ms: u64) -> String {
    let endpoint = match protocol {
        "grpc" => format!("http://{collector}"),
        _ => format!("http://{collector}/v1/metrics"),
    };
    format!(
        r#"{{"otlp":{{"endpoint":"{endpoint}","protocol":"{protocol}",
            "headers":[{{"name":"authorization","value":"{SECRET}"}},
                       {{"name":"x-scope-orgid","value":"tenant-a"}}],
            "period_ms":{period_ms},"temporality":"cumulative",
            "global_tags":[{{"name":"deployment","value":"blue"}}],
            "durations_as_seconds":false}}}}"#
    )
}

/// A collector double for `protocol`.
fn collector(protocol: &str, answer: Answer) -> Collector {
    match protocol {
        "grpc" => Collector::grpc(answer),
        _ => Collector::http(answer),
    }
}

/// One series of every SDK instrument kind, as `(kind, name)`.
const PROBES: [(&str, &str); 5] = [
    ("counter", "otlp_probe_counter"),
    ("up_down_counter", "otlp_probe_occupancy"),
    ("gauge", "otlp_probe_gauge"),
    ("histogram", "otlp_probe_histogram"),
    ("duration_histogram", "otlp_probe_duration"),
];

/// Registers every [`PROBES`] series with one `queue` label and records
/// `value` into each.
fn record_probes(meter: *const MetricMeter, value: i64) {
    for (kind, name) in PROBES {
        let id = register(
            meter,
            &format!(
                r#"{{"kind":"{kind}","name":"{name}","description":"OTLP probe",
                    "labels":[{{"name":"queue","value":"probe-queue"}}]}}"#
            ),
        );
        record(meter, id, value);
    }
}

#[test]
fn an_otlp_runtime_listens_on_nothing_and_hands_attached_graphs_its_meter() {
    let collector = Collector::grpc(Answer::Accept);
    let (mut shared, address) = create_ok(&otlp_document("grpc", collector.address, 1_000));
    assert_eq!(address, "", "an OTLP exporter reports no listening address");
    let mut runtime: *mut Runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: `shared` is live; both outputs are writable.
    let status =
        unsafe { ocaml_temporal_core_v4_runtime_new_attached(shared, &mut runtime, &mut result) };
    free_result(&mut result);
    assert_eq!(status, STATUS_OK);
    // SAFETY: The graph is live and used only by this thread.
    assert_eq!(
        unsafe { test_runtime_has_metrics_meter(runtime) },
        Some(true)
    );
    let mut meter = meter_of(shared);
    free_meter(&mut meter);
    // SAFETY: The slot holds the live graph and is used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(&mut runtime) },
        STATUS_OK
    );
    free_shared(&mut shared);
}

#[test]
fn exports_carry_the_headers_resource_and_series_over_both_transports() {
    for protocol in PROTOCOLS {
        let collector = collector(protocol, Answer::Accept);
        let (mut shared, _) = create_ok(&otlp_document(protocol, collector.address, 100));
        let mut meter = meter_of(shared);
        record_probes(meter, 3);
        let exports = collector.wait_for(EXPORT_WAIT, |exports| {
            sum_value(exports, "temporal_otlp_probe_counter") == Some(3.0)
        });
        let export: &Export = exports
            .iter()
            .find(|export| {
                export
                    .metrics()
                    .any(|metric| metric.name == "temporal_otlp_probe_counter")
            })
            .expect("an export with the probe");
        assert_eq!(export.header("authorization"), Some(SECRET), "{protocol}");
        assert_eq!(
            export.header("x-scope-orgid"),
            Some("tenant-a"),
            "{protocol}"
        );
        match protocol {
            "grpc" => {
                assert_eq!(
                    export.path,
                    "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"
                );
                assert_eq!(export.header("content-type"), Some("application/grpc"));
            }
            _ => {
                assert_eq!(export.path, "/v1/metrics");
                assert_eq!(
                    export.header("content-type"),
                    Some("application/x-protobuf")
                );
            }
        }
        // Global tags are resource attributes; Core names the service.
        assert_eq!(
            export.resource_attribute("deployment").as_deref(),
            Some("blue")
        );
        assert_eq!(
            export.resource_attribute("service.name").as_deref(),
            Some("temporal-core-sdk")
        );
        let counter = export
            .metrics()
            .find(|metric| metric.name == "temporal_otlp_probe_counter")
            .and_then(|metric| metric.sum.as_ref())
            .expect("the counter is an OTLP sum");
        assert_eq!(counter.aggregation_temporality, 2, "cumulative");
        let labels: BTreeSet<&str> = counter.data_points[0]
            .attributes
            .iter()
            .map(|attribute| attribute.key.as_str())
            .collect();
        assert_eq!(
            labels,
            BTreeSet::from(["queue", "service_name"]),
            "{protocol}"
        );
        let duration = export
            .metrics()
            .find(|metric| metric.name == "temporal_otlp_probe_duration")
            .expect("the duration histogram is exported");
        assert_eq!(duration.unit, "ms");
        assert_eq!(
            duration
                .histogram
                .as_ref()
                .map(|histogram| histogram.data_points[0].count),
            Some(1)
        );
        free_meter(&mut meter);
        free_shared(&mut shared);
    }
}

/// Fetches `/metrics` from a Prometheus endpoint and returns the response.
fn scrape(address: &str) -> String {
    let address: SocketAddr = address.parse().expect("bound address");
    let mut stream =
        TcpStream::connect_timeout(&address, Duration::from_secs(5)).expect("scrape connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
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

#[test]
fn the_otlp_series_are_the_prometheus_families() {
    let (mut prometheus, address) = create_ok(r#"{"prometheus":{"bind_address":"127.0.0.1:0"}}"#);
    let mut prometheus_meter = meter_of(prometheus);
    record_probes(prometheus_meter, 2);
    let families: BTreeSet<String> = scrape(&address)
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|rest| rest.split(' ').next())
        .map(str::to_owned)
        .collect();
    free_meter(&mut prometheus_meter);
    free_shared(&mut prometheus);

    for protocol in PROTOCOLS {
        let collector = collector(protocol, Answer::Accept);
        let (mut shared, _) = create_ok(&otlp_document(protocol, collector.address, 100));
        let mut meter = meter_of(shared);
        record_probes(meter, 2);
        let exports = collector.wait_for(EXPORT_WAIT, |exports| {
            metric_names(exports).len() >= PROBES.len()
        });
        assert_eq!(metric_names(&exports), families, "{protocol}");
        free_meter(&mut meter);
        free_shared(&mut shared);
    }
}

#[test]
fn release_makes_one_final_export() {
    for protocol in PROTOCOLS {
        let collector = collector(protocol, Answer::Accept);
        // A one-hour period: the only export is the one at release.
        let (mut shared, _) = create_ok(&otlp_document(protocol, collector.address, 3_600_000));
        let mut meter = meter_of(shared);
        record_probes(meter, 7);
        thread::sleep(Duration::from_millis(300));
        assert!(
            collector.exports().is_empty(),
            "{protocol}: no periodic export yet"
        );
        let elapsed = free_shared(&mut shared);
        assert!(
            elapsed < Duration::from_secs(6),
            "{protocol}: release took {elapsed:?}"
        );
        let exports = collector.exports();
        assert_eq!(exports.len(), 1, "{protocol}: exactly the final export");
        assert_eq!(
            sum_value(&exports, "temporal_otlp_probe_counter"),
            Some(7.0)
        );
        // The meter outlives the runtime: recording and late registration
        // are harmless and nothing more is exported.
        record_probes(meter, 1);
        thread::sleep(Duration::from_millis(300));
        assert_eq!(
            collector.exports().len(),
            1,
            "{protocol}: nothing after release"
        );
        free_meter(&mut meter);
    }
}

/// A `Write` sink that appends to a shared buffer the test can read.
#[derive(Clone, Default)]
struct SharedLog(Arc<Mutex<Vec<u8>>>);

impl Write for SharedLog {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SharedLog {
    /// Everything written so far, as text.
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log lock")).into_owned()
    }
}

#[test]
fn failed_exports_are_logged_without_header_values() {
    // A port with nothing listening: bind, read the port, close.
    let unreachable = TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("ephemeral port");
    let cases: Vec<(&str, Option<Collector>)> = vec![
        ("grpc", Some(Collector::grpc(Answer::Reject))),
        ("http_protobuf", Some(Collector::http(Answer::Reject))),
        ("grpc", None),
        ("http_protobuf", None),
    ];
    for (protocol, rejecting) in cases {
        let target = rejecting
            .as_ref()
            .map_or(unreachable, |collector| collector.address);
        let log = SharedLog::default();
        let mut shared = test_shared_runtime_new_with_telemetry_and_log_sink(
            otlp_document(protocol, target, 100).as_bytes(),
            CoreLogLevel::Trace,
            Box::new(log.clone()),
        )
        .expect("an unreachable or rejecting collector does not fail creation");
        let mut meter = meter_of(shared);
        record_probes(meter, 1);
        match &rejecting {
            Some(collector) => {
                collector.wait_for(EXPORT_WAIT, |exports| exports.len() >= 2);
            }
            None => thread::sleep(Duration::from_millis(1_500)),
        }
        let elapsed = free_shared(&mut shared);
        assert!(
            elapsed < Duration::from_secs(16),
            "{protocol}: release took {elapsed:?}"
        );
        let text = log.text();
        assert!(
            text.contains("OTLP metric export failed"),
            "{protocol}: failed exports are logged: {text}"
        );
        assert!(
            !text.contains("s3cr3t"),
            "{protocol}: a header value reached the log"
        );
        assert!(
            !text.contains("tenant-a"),
            "{protocol}: a header value reached the log"
        );
        free_meter(&mut meter);
    }
}

#[test]
fn invalid_otlp_documents_publish_nothing_and_echo_no_secret() {
    let endpoint = r#""endpoint":"http://127.0.0.1:4317""#;
    for document in [
        format!(r#"{{"otlp":{{"endpoint":"http://user:{SECRET}@127.0.0.1:4317"}}}}"#),
        format!(
            r#"{{"otlp":{{{endpoint},"headers":[{{"name":"Authorization","value":"{SECRET}"}}]}}}}"#
        ),
        format!(
            r#"{{"otlp":{{{endpoint},"headers":[{{"name":"authorization","value":"{SECRET}\n"}}]}}}}"#
        ),
        format!(
            r#"{{"otlp":{{{endpoint},"period_ms":0,"headers":[{{"name":"a","value":"{SECRET}"}}]}}}}"#
        ),
        format!(
            r#"{{"prometheus":{{"bind_address":"127.0.0.1:0"}},"otlp":{{{endpoint},"headers":[{{"name":"a","value":"{SECRET}"}}]}}}}"#
        ),
    ] {
        let (status, shared, message) = create(&document);
        assert_eq!(status, STATUS_CONFIGURATION, "{message}");
        assert!(shared.is_null());
        assert!(!message.contains("s3cr3t"), "{message}");
    }
}

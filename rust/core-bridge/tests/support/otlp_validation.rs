//! Unit regressions for the OTLP telemetry options (#1161). They exercise
//! the private validators of `src/otlp.rs` (endpoint grammar, header and
//! period rules, redaction of header values) and the detachable meter's
//! behavior after its exporter is released. The module is compiled into the
//! crate through a `#[path]` declaration so production code and tests stay
//! in separate files. The OCaml validator in
//! `lib/base/metrics_config.ml` applies the same grammar; the shared cases
//! are mirrored in `test/unit/test_metrics_config.ml`.

use super::*;
use temporalio_common::telemetry::metrics::{CounterBase, MetricAttributable};

/// A credential-shaped header value that no message may ever contain.
const SECRET: &str = "Bearer s3cr3t-token-value";

/// Parses a complete OTLP document through the crate's telemetry parser.
fn parse(otlp: &str) -> Result<crate::telemetry::TelemetryConfig, Failure> {
    crate::telemetry::parse_telemetry_config(format!(r#"{{"otlp":{otlp}}}"#).as_bytes())
}

/// The message of a rejected document, which must also omit [`SECRET`].
fn rejection(otlp: &str) -> String {
    match parse(otlp) {
        Ok(_) => panic!("document should be rejected: {otlp}"),
        Err(failure) => {
            assert_eq!(failure.status, STATUS_CONFIGURATION);
            assert!(
                !failure.message.contains("s3cr3t"),
                "a rejection echoed a header value: {}",
                failure.message
            );
            failure.message
        }
    }
}

#[test]
fn accepted_endpoints_follow_the_documented_grammar() {
    for endpoint in [
        "http://localhost:4317",
        "https://otlp.example.com",
        "https://otlp.example.com:443/v1/metrics",
        "http://127.0.0.1:4318/v1/metrics",
        "http://[::1]:4317",
        "http://[2001:db8::1]",
        "https://collector-1.internal.example:4317/",
        "http://collector/v1/metrics?tenant=a",
        "http://Collector.Example.COM:4317",
    ] {
        assert!(
            validate_endpoint(endpoint).is_ok(),
            "endpoint should be accepted: {endpoint}"
        );
    }
}

#[test]
fn rejected_endpoints_name_only_the_rule() {
    for endpoint in [
        "",
        "localhost:4317",
        "grpc://localhost:4317",
        "HTTP://localhost:4317",
        "ftp://localhost",
        "http://",
        "http://:4317",
        "http://user:pass@localhost:4317",
        "http://token@localhost",
        "http://localhost:0",
        "http://localhost:65536",
        "http://localhost:04317",
        "http://localhost:",
        "http://localhost:43a7",
        "http://local host:4317",
        "http://localhost:4317/v1/metrics#frag",
        "http://localhost:4317/\tpath",
        "http://127.1:4317",
        "http://1.2.3.4.5",
        "http://example.123",
        "http://0x7f.0.0.1",
        "http://010.0.0.1",
        "http://-bad.example",
        "http://bad-.example",
        "http://a..b",
        "http://[::1%25eth0]:4317",
        "http://[::zz]",
        "http://[::1",
        "http://exa_mple.com",
    ] {
        let failure = validate_endpoint(endpoint).expect_err(endpoint);
        assert_eq!(failure.status, STATUS_CONFIGURATION, "{endpoint}");
        assert_eq!(failure.message, ENDPOINT_RULE, "{endpoint}");
    }
    let long = format!("http://localhost/{}", "a".repeat(MAX_OTLP_ENDPOINT_BYTES));
    assert!(validate_endpoint(&long).is_err());
    let longest = format!(
        "http://localhost/{}",
        "a".repeat(MAX_OTLP_ENDPOINT_BYTES - "http://localhost/".len())
    );
    assert!(validate_endpoint(&longest).is_ok());
}

#[test]
fn a_complete_document_is_accepted_with_core_defaults() {
    parse(r#"{"endpoint":"http://localhost:4317"}"#).expect("minimal document");
    parse(&format!(
        r#"{{"endpoint":"https://otlp.example.com/v1/metrics","protocol":"http_protobuf",
            "headers":[{{"name":"authorization","value":"{SECRET}"}},
                       {{"name":"x-tenant.id_2","value":"a b"}}],
            "period_ms":60000,"temporality":"delta",
            "global_tags":[{{"name":"deployment","value":"blue"}}],
            "durations_as_seconds":true}}"#
    ))
    .expect("full document");
    for period in [MIN_OTLP_PERIOD_MS, MAX_OTLP_PERIOD_MS] {
        parse(&format!(
            r#"{{"endpoint":"http://localhost:4317","period_ms":{period}}}"#
        ))
        .expect("period bound is inclusive");
    }
}

#[test]
fn rejected_documents_never_echo_header_values() {
    let endpoint = r#""endpoint":"http://localhost:4317""#;
    let header = |name: &str, value: &str| {
        format!(r#"{{{endpoint},"headers":[{{"name":"{name}","value":"{value}"}}]}}"#)
    };
    for name in [
        "",
        "Authorization",
        "-leading",
        "x key",
        "x:key",
        "content-type",
        "user-agent",
        "te",
        "grpc-timeout",
        "trace-bin",
        &"a".repeat(MAX_OTLP_HEADER_NAME_BYTES + 1),
    ] {
        let message = rejection(&header(name, SECRET));
        assert!(message.contains("header names"), "{name}: {message}");
    }
    for value in [
        String::new(),
        format!(" {SECRET}"),
        format!("{SECRET} "),
        format!("{SECRET}\\n"),
        format!("{SECRET}\\u00e9"),
        format!("{SECRET}\\u007f"),
        "s3cr3t".repeat(MAX_OTLP_HEADER_VALUE_BYTES),
    ] {
        let message = rejection(&header("authorization", &value));
        assert!(message.contains("header values"), "{message}");
    }
    let duplicate = format!(
        r#"{{{endpoint},"headers":[{{"name":"a","value":"{SECRET}"}},{{"name":"a","value":"{SECRET}"}}]}}"#
    );
    assert!(rejection(&duplicate).contains("unique"));
    let too_many: Vec<String> = (0..=MAX_OTLP_HEADERS)
        .map(|index| format!(r#"{{"name":"h{index}","value":"{SECRET}"}}"#))
        .collect();
    let message = rejection(&format!(
        r#"{{{endpoint},"headers":[{}]}}"#,
        too_many.join(",")
    ));
    assert!(message.contains("at most"), "{message}");
    // The longest value is accepted.
    parse(&header(
        "authorization",
        &"v".repeat(MAX_OTLP_HEADER_VALUE_BYTES),
    ))
    .expect("longest header value");
}

#[test]
fn other_members_are_validated_with_constant_messages() {
    for period in [0, MIN_OTLP_PERIOD_MS - 1, MAX_OTLP_PERIOD_MS + 1] {
        let message = rejection(&format!(
            r#"{{"endpoint":"http://localhost:4317","period_ms":{period}}}"#
        ));
        assert!(message.contains("period"), "{message}");
    }
    for document in [
        r#"{"endpoint":"http://localhost:4317","protocol":"http"}"#,
        r#"{"endpoint":"http://localhost:4317","temporality":"Delta"}"#,
        r#"{"endpoint":"http://localhost:4317","unknown":true}"#,
        r#"{"endpoint":"http://localhost:4317","period_ms":-1}"#,
        r#"{"headers":[]}"#,
    ] {
        assert_eq!(
            rejection(document),
            "invalid runtime telemetry configuration JSON"
        );
    }
    let message = rejection(
        r#"{"endpoint":"http://localhost:4317","global_tags":[{"name":"task_queue","value":"x"}]}"#,
    );
    assert!(message.contains("global tag names"), "{message}");
    let both = crate::telemetry::parse_telemetry_config(
        br#"{"prometheus":{"bind_address":"127.0.0.1:0"},"otlp":{"endpoint":"http://localhost:4317"}}"#,
    )
    .expect_err("two exporters");
    assert_eq!(
        both.message,
        "runtime telemetry configures at most one metrics exporter"
    );
}

#[test]
fn debug_output_redacts_header_values() {
    let config = match parse(&format!(
        r#"{{"endpoint":"http://localhost:4317","headers":[{{"name":"authorization","value":"{SECRET}"}}]}}"#
    )) {
        Ok(config) => config,
        Err(failure) => panic!("{}", failure.message),
    };
    let rendered = format!("{config:?}");
    assert!(rendered.contains("authorization"), "{rendered}");
    assert!(rendered.contains("<redacted>"), "{rendered}");
    assert!(!rendered.contains("s3cr3t"), "{rendered}");
}

/// After its exporter is released, the detachable meter hands out no-op
/// instruments and accepts attributes created before the release without
/// the no-op meter's debug assertion firing (which would panic in a debug
/// build of a late SDK registration).
#[test]
fn a_detached_meter_creates_no_op_instruments() {
    let options = OtelCollectorOptions::builder()
        .url(validate_endpoint("http://127.0.0.1:9/v1/metrics").expect("endpoint"))
        .protocol(OtlpProtocol::Http)
        .metric_periodicity(Duration::from_secs(3600))
        .build();
    let meter: Arc<dyn CoreMeter> = Arc::new(build_otlp_metric_exporter(options).expect("build"));
    let slot: MeterSlot = Arc::new(RwLock::new(Some(meter)));
    let detachable = DetachableMeter {
        slot: Arc::clone(&slot),
    };
    let attributes = detachable.new_attributes(NewAttributes::default());
    assert!(matches!(attributes, MetricAttributes::OTel { .. }));
    // Bound to its attributes at creation, as `MetricMeter::bind` does.
    let live_counter = detachable
        .counter(MetricParameters::builder().name("live").build())
        .with_attributes(&attributes)
        .expect("OpenTelemetry counters bind infallibly");
    let exporter = OtlpExporter {
        slot,
        global_tag_names: Vec::new(),
        dispatch: None,
    };
    drop(exporter);
    let extended = detachable.extend_attributes(attributes, NewAttributes::default());
    assert!(matches!(extended, MetricAttributes::OTel { .. }));
    assert!(matches!(
        detachable.new_attributes(NewAttributes::default()),
        MetricAttributes::NoOp(_)
    ));
    // A late registration binds the pre-release attributes, exactly as
    // `MetricMeter::bind` would after `Runtime.shutdown`.
    let late = detachable
        .counter(MetricParameters::builder().name("late").build())
        .with_attributes(&extended)
        .expect("no-op counters bind infallibly");
    late.adds(1);
    // An instrument created before the release keeps accepting recordings.
    live_counter.adds(1);
    detachable.histogram_duration(MetricParameters::builder().name("late_duration").build());
    detachable.up_down_counter(MetricParameters::builder().name("late_gauge").build());
}

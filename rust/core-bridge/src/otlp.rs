//! Opt-in OTLP metrics push export for a shared Core runtime (#1161).
//!
//! Temporal Core's `otel` feature builds an OpenTelemetry meter whose
//! periodic reader pushes Core's instruments to an OTLP collector over gRPC
//! or HTTP/protobuf ([`build_otlp_metric_exporter`]). The official SDKs feed
//! the same builder from their runtime telemetry options, so metric names,
//! units and labels match the Prometheus path and the official SDKs.
//!
//! Ownership (#1161): Core's meter owns its OpenTelemetry meter provider, and
//! the provider's periodic-reader thread keeps exporting until the provider
//! is dropped, which flushes once more and stops the thread. Core, every
//! client and worker, and the SDK's own [`crate::telemetry::MetricMeter`]
//! (released only by OCaml's collector) all clone the meter they are handed,
//! so handing them Core's meter directly would tie the exporter's lifetime to
//! the last of those clones. The bridge therefore keeps the one strong
//! reference to Core's meter in [`OtlpExporter`], stored in the runtime's
//! `SharedCore` ahead of Core itself, and hands everyone else a
//! [`DetachableMeter`] that borrows it under a lock. Dropping the exporter
//! detaches the meter and drops the strong reference on the releasing
//! thread while Core's Tokio runtime is still alive, so the final export
//! (which the gRPC transport drives on that runtime) happens exactly once,
//! at runtime release, never from a GC finalizer and never after Core is
//! gone. Instruments created before the release keep recording into the
//! detached pipeline, which nobody exports any longer; instruments created
//! afterwards are Core's no-op instruments.
//!
//! Redaction: header values are credentials (API keys, bearer tokens). They
//! are never logged, never echoed in an error, and never formatted by a
//! `Debug` implementation in this module. Validation messages name only the
//! violated rule, and failures from Core's exporter builder are reduced to a
//! constant message because the `http` and `tonic` errors behind them are
//! not guaranteed to omit the offending value.
//!
//! Validation mirrors `Temporal_base.Metrics_config.otlp` exactly, so a
//! document OCaml accepts is never rejected here, while a caller that
//! bypasses OCaml still cannot configure a header or endpoint outside the
//! documented grammar.

use crate::abi::{Failure, STATUS_CONFIGURATION};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;
use temporalio_common::telemetry::metrics::{
    CoreMeter, Counter, Gauge, GaugeF64, Histogram, HistogramDuration, HistogramF64,
    MetricAttributes, MetricParameters, NewAttributes, NoOpCoreMeter, UpDownCounter,
};
use temporalio_common::telemetry::{
    MetricTemporality, OtelCollectorOptions, OtlpProtocol, build_otlp_metric_exporter,
};
use temporalio_sdk_core::{CoreRuntime, Url};

/// Longest accepted collector endpoint URL, in bytes.
pub(crate) const MAX_OTLP_ENDPOINT_BYTES: usize = 2048;

/// Most request headers one exporter sends.
pub(crate) const MAX_OTLP_HEADERS: usize = 16;

/// Longest accepted header name, in bytes.
pub(crate) const MAX_OTLP_HEADER_NAME_BYTES: usize = 64;

/// Longest accepted header value, in bytes. Large enough for a bearer JWT.
pub(crate) const MAX_OTLP_HEADER_VALUE_BYTES: usize = 4096;

/// Shortest accepted export period, in milliseconds. Each export collects
/// every series and makes one request, so a shorter period mostly adds load
/// on the process and the collector.
pub(crate) const MIN_OTLP_PERIOD_MS: u64 = 100;

/// Longest accepted export period, in milliseconds (one hour).
pub(crate) const MAX_OTLP_PERIOD_MS: u64 = 3_600_000;

/// Core's default export period, used when the document omits `period_ms`.
const DEFAULT_OTLP_PERIOD_MS: u64 = 1_000;

/// Header names the transport sets itself or that HTTP/2 forbids. A
/// user-supplied copy would conflict with the exporter's own value or be
/// refused by the gRPC stack, so it is rejected up front. Every name with
/// the `grpc-` prefix is reserved too (see [`valid_header_name`]).
pub(crate) const RESERVED_OTLP_HEADER_NAMES: &[&str] = &[
    "connection",
    "content-encoding",
    "content-length",
    "content-type",
    "host",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "user-agent",
];

/// Transport for OTLP export, Core's [`OtlpProtocol`] by name.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OtlpTransport {
    /// OTLP/gRPC, Core's default; `endpoint` is the collector's gRPC address.
    #[default]
    Grpc,
    /// OTLP/HTTP with protobuf bodies; `endpoint` is the full metrics URL,
    /// conventionally ending in `/v1/metrics`.
    HttpProtobuf,
}

/// Aggregation temporality of exported sums and histograms, Core's
/// [`MetricTemporality`] by name.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OtlpTemporality {
    /// Every data point covers the time since the exporter started (Core's
    /// default, and what Prometheus-style backends expect).
    #[default]
    Cumulative,
    /// Every data point covers the time since the previous export.
    Delta,
}

/// One request header. `Debug` never prints the value.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OtlpHeader {
    name: String,
    value: String,
}

impl std::fmt::Debug for OtlpHeader {
    /// Prints the header name only; values are credentials.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OtlpHeader")
            .field("name", &self.name)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// One constant metric attribute, as in the Prometheus configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OtlpGlobalTag {
    name: String,
    value: String,
}

/// OTLP push-export options, a strict subset of Core's
/// [`OtelCollectorOptions`] with the same defaults. The OCaml encoder writes
/// every member explicitly.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OtlpConfig {
    /// Collector URL; see [`validate_endpoint`] for the grammar.
    endpoint: String,
    /// gRPC (default) or HTTP/protobuf.
    #[serde(default)]
    protocol: OtlpTransport,
    /// Request headers, for example an API key; never logged.
    #[serde(default)]
    headers: Vec<OtlpHeader>,
    /// Interval between exports, in milliseconds.
    #[serde(default = "default_period_ms")]
    period_ms: u64,
    /// Aggregation temporality of sums and histograms.
    #[serde(default)]
    temporality: OtlpTemporality,
    /// Constant attributes attached to every series, in caller order. Core
    /// records them as resource attributes of the export.
    #[serde(default)]
    global_tags: Vec<OtlpGlobalTag>,
    /// Record durations as floating-point seconds instead of integer
    /// milliseconds.
    #[serde(default)]
    durations_as_seconds: bool,
}

/// Serde default for [`OtlpConfig::period_ms`].
fn default_period_ms() -> u64 {
    DEFAULT_OTLP_PERIOD_MS
}

impl OtlpConfig {
    /// Names of the configured global tags, in configuration order.
    pub(crate) fn global_tag_names(&self) -> Vec<String> {
        self.global_tags
            .iter()
            .map(|tag| tag.name.clone())
            .collect()
    }

    /// Global tags as `(name, value)` pairs for the shared tag validator.
    pub(crate) fn global_tag_pairs(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.global_tags
            .iter()
            .map(|tag| (tag.name.as_str(), tag.value.as_str()))
    }
}

/// Builds a configuration failure with a constant-shaped message.
fn invalid(message: String) -> Failure {
    Failure {
        status: STATUS_CONFIGURATION,
        message,
    }
}

/// Whether `text` is a canonical decimal number of at most `max_digits`
/// digits: ASCII digits only, without a leading zero unless it is `0`.
fn canonical_decimal(text: &str, max_digits: usize) -> bool {
    !text.is_empty()
        && text.len() <= max_digits
        && text.bytes().all(|byte| byte.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'))
}

/// Whether `host` is four canonical decimal octets 0..=255. Rust's parser
/// already rejects shorthand, octal and hexadecimal forms; the explicit digit
/// rule keeps the grammar identical to the OCaml validator.
fn valid_ipv4(host: &str) -> bool {
    let octets: Vec<&str> = host.split('.').collect();
    octets.len() == 4
        && octets.iter().all(|octet| canonical_decimal(octet, 3))
        && Ipv4Addr::from_str(host).is_ok()
}

/// Whether `label` is one DNS label: 1 to 63 ASCII letters, digits or
/// hyphens, not starting or ending with a hyphen.
fn valid_dns_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 63
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        && !label.starts_with('-')
        && !label.ends_with('-')
}

/// Whether `host` is an accepted endpoint host: a bracketed IPv6 address
/// without a zone identifier, a dotted-quad IPv4 address, or a DNS name of at
/// most 253 bytes. A name whose last label is all digits is read as IPv4 by
/// URL parsers (so `127.1` would mean `127.0.0.1`), and must therefore be a
/// canonical dotted quad.
fn valid_host(host: &str) -> bool {
    if let Some(inner) = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        return !inner.contains('%') && Ipv6Addr::from_str(inner).is_ok();
    }
    let labels: Vec<&str> = host.split('.').collect();
    let numeric_tail = labels
        .last()
        .is_some_and(|last| !last.is_empty() && last.bytes().all(|byte| byte.is_ascii_digit()));
    if numeric_tail {
        return valid_ipv4(host);
    }
    host.len() <= 253 && labels.iter().all(|label| valid_dns_label(label))
}

/// The message for every endpoint rejection. It never echoes the endpoint,
/// which might carry a credential in a query string.
const ENDPOINT_RULE: &str = "metrics OTLP endpoint must be an http:// or https:// URL of at most \
     2048 bytes with a host name, IPv4 or bracketed IPv6 host, an optional port 1 to 65535, \
     no user information, no fragment, and no spaces or control characters";

/// Validates the collector endpoint with the grammar
/// `Temporal_base.Metrics_config` documents, then parses it with the URL
/// type Core takes.
///
/// Grammar: the scheme is exactly `http://` or `https://` (lowercase); the
/// authority, up to the first `/` or `?`, is a host (see [`valid_host`])
/// optionally followed by `:` and a canonical decimal port 1..=65535, with
/// no `@` (user information would put a credential in every log of the
/// URL); the rest is empty or starts with `/` or `?`; no byte is outside
/// printable ASCII `!`..=`~` and there is no `#`. Rejecting whitespace up
/// front matters because URL parsers silently strip some of it.
pub(crate) fn validate_endpoint(endpoint: &str) -> Result<Url, Failure> {
    let rule = || invalid(ENDPOINT_RULE.to_owned());
    if endpoint.is_empty()
        || endpoint.len() > MAX_OTLP_ENDPOINT_BYTES
        || !endpoint.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
        || endpoint.contains('#')
    {
        return Err(rule());
    }
    let rest = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .ok_or_else(rule)?;
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.contains('@') {
        return Err(rule());
    }
    // A bracketed IPv6 host contains colons, so the port separator is the
    // last colon after the closing bracket, if any.
    let host_end = match authority.rfind(':') {
        Some(colon) if !authority[colon..].contains(']') => colon,
        _ => authority.len(),
    };
    let host = &authority[..host_end];
    if host_end < authority.len() {
        let port = &authority[host_end + 1..];
        let in_range = canonical_decimal(port, 5)
            && port
                .parse::<u32>()
                .is_ok_and(|value| (1..=65_535).contains(&value));
        if !in_range {
            return Err(rule());
        }
    }
    if !valid_host(host) {
        return Err(rule());
    }
    let url = Url::parse(endpoint).map_err(|_error| rule())?;
    // The grammar above already excludes these; the checks keep the contract
    // explicit should the URL parser ever disagree.
    if url.username() != "" || url.password().is_some() || url.host_str().is_none() {
        return Err(rule());
    }
    Ok(url)
}

/// Whether `name` is an accepted header name: 1 to
/// [`MAX_OTLP_HEADER_NAME_BYTES`] bytes of lowercase letters, digits, `-`,
/// `_` and `.`, starting with a letter or digit, not ending in `-bin` (gRPC
/// would require base64 binary values), not starting with `grpc-`, and not in
/// [`RESERVED_OTLP_HEADER_NAMES`]. Lowercase is required because gRPC
/// metadata keys are lowercase and HTTP names are case-insensitive, so
/// uniqueness is exact.
fn valid_header_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let first_ok = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit());
    first_ok
        && name.len() <= MAX_OTLP_HEADER_NAME_BYTES
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
        && !name.ends_with("-bin")
        && !name.starts_with("grpc-")
        && !RESERVED_OTLP_HEADER_NAMES.contains(&name)
}

/// Whether `value` is an accepted header value: 1 to
/// [`MAX_OTLP_HEADER_VALUE_BYTES`] bytes of printable ASCII (space through
/// `~`) without a leading or trailing space, which both HTTP and gRPC ASCII
/// metadata carry unchanged.
fn valid_header_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_OTLP_HEADER_VALUE_BYTES
        && value.bytes().all(|byte| (b' '..=b'~').contains(&byte))
        && !value.starts_with(' ')
        && !value.ends_with(' ')
}

/// Applies the documented bounds to one OTLP configuration, except the
/// global tags, which [`crate::telemetry`] checks with the Prometheus rules.
/// No message names a header value or the endpoint.
pub(crate) fn validate_otlp(config: &OtlpConfig) -> Result<(), Failure> {
    validate_endpoint(&config.endpoint)?;
    if !(MIN_OTLP_PERIOD_MS..=MAX_OTLP_PERIOD_MS).contains(&config.period_ms) {
        return Err(invalid(format!(
            "metrics OTLP period must be {MIN_OTLP_PERIOD_MS} ms to {MAX_OTLP_PERIOD_MS} ms"
        )));
    }
    if config.headers.len() > MAX_OTLP_HEADERS {
        return Err(invalid(format!(
            "metrics OTLP headers accepts at most {MAX_OTLP_HEADERS} headers"
        )));
    }
    let mut seen = HashSet::new();
    for header in &config.headers {
        if !valid_header_name(&header.name) {
            return Err(invalid(format!(
                "metrics OTLP header names must be 1 to {MAX_OTLP_HEADER_NAME_BYTES} bytes of \
                 [a-z0-9_.-] starting with a letter or digit, must not end in -bin or start \
                 with grpc-, and must not be a header the transport sets"
            )));
        }
        if !valid_header_value(&header.value) {
            return Err(invalid(format!(
                "metrics OTLP header values must be 1 to {MAX_OTLP_HEADER_VALUE_BYTES} bytes of \
                 printable ASCII without leading or trailing spaces"
            )));
        }
        if !seen.insert(header.name.as_str()) {
            return Err(invalid(
                "metrics OTLP header names must be unique".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Core's meter behind a lock that the [`OtlpExporter`] empties on release.
type MeterSlot = Arc<RwLock<Option<Arc<dyn CoreMeter>>>>;

/// The meter handed to Core, its clients and workers, and the SDK recorder.
///
/// It borrows Core's OTLP meter from the shared slot for the duration of each
/// call and never clones the strong reference out, so the [`OtlpExporter`]
/// alone decides when the meter provider shuts down. Once the slot is empty
/// every instrument it creates is Core's no-op instrument. Instrument
/// creation is rare (per worker and per SDK series); recording goes straight
/// to the instruments and never takes this lock.
#[derive(Debug)]
pub(crate) struct DetachableMeter {
    slot: MeterSlot,
}

impl DetachableMeter {
    /// Runs `live` against Core's meter while it is attached, `detached`
    /// otherwise. The read lock is held only for the call, never across an
    /// await or an export.
    fn with_meter<T>(
        &self,
        live: impl FnOnce(&dyn CoreMeter) -> T,
        detached: impl FnOnce() -> T,
    ) -> T {
        let guard = self.slot.read().unwrap_or_else(PoisonError::into_inner);
        match guard.as_ref() {
            Some(meter) => live(meter.as_ref()),
            None => detached(),
        }
    }
}

impl CoreMeter for DetachableMeter {
    /// Core's OpenTelemetry attributes while attached, no-op ones afterwards.
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        let fallback = attribs.clone();
        self.with_meter(
            |meter| meter.new_attributes(attribs),
            || NoOpCoreMeter.new_attributes(fallback),
        )
    }

    /// After release the existing attributes are returned unchanged: they
    /// may be OpenTelemetry attributes from before the release, which the
    /// no-op meter would report as a defect, and no-op instruments ignore
    /// attributes anyway.
    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        let fallback = existing.clone();
        self.with_meter(
            |meter| meter.extend_attributes(existing, attribs),
            || fallback,
        )
    }

    fn counter(&self, params: MetricParameters) -> Counter {
        let fallback = params.clone();
        self.with_meter(
            |meter| meter.counter(params),
            || NoOpCoreMeter.counter(fallback),
        )
    }

    fn histogram(&self, params: MetricParameters) -> Histogram {
        let fallback = params.clone();
        self.with_meter(
            |meter| meter.histogram(params),
            || NoOpCoreMeter.histogram(fallback),
        )
    }

    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64 {
        let fallback = params.clone();
        self.with_meter(
            |meter| meter.histogram_f64(params),
            || NoOpCoreMeter.histogram_f64(fallback),
        )
    }

    fn histogram_duration(&self, params: MetricParameters) -> HistogramDuration {
        let fallback = params.clone();
        self.with_meter(
            |meter| meter.histogram_duration(params),
            || NoOpCoreMeter.histogram_duration(fallback),
        )
    }

    fn gauge(&self, params: MetricParameters) -> Gauge {
        let fallback = params.clone();
        self.with_meter(
            |meter| meter.gauge(params),
            || NoOpCoreMeter.gauge(fallback),
        )
    }

    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64 {
        let fallback = params.clone();
        self.with_meter(
            |meter| meter.gauge_f64(params),
            || NoOpCoreMeter.gauge_f64(fallback),
        )
    }

    fn up_down_counter(&self, params: MetricParameters) -> UpDownCounter {
        let fallback = params.clone();
        self.with_meter(
            |meter| meter.up_down_counter(params),
            || NoOpCoreMeter.up_down_counter(fallback),
        )
    }
}

/// Owns the running OTLP exporter of one Core runtime: the only strong
/// reference to Core's OpenTelemetry meter, and with it the meter provider
/// and its periodic-reader thread.
///
/// Stored in `SharedCore` ahead of Core, so it is dropped first, on the
/// thread that releases the last Core reference (never a Tokio worker; see
/// `SharedCore`). Dropping it empties the slot, so later instrument creation
/// gets no-op instruments, and drops the meter outside the lock. That shuts
/// the provider down: the reader collects and exports one final time and its
/// thread exits. OpenTelemetry waits at most five seconds for that final
/// export; an export still running then (a collector that does not answer)
/// ends with the exporter's own request timeout, ten seconds by default, or
/// for gRPC as soon as Core's Tokio runtime is dropped next. The reader
/// thread runs no OCaml code and holds no OCaml value.
#[derive(Debug)]
pub(crate) struct OtlpExporter {
    slot: MeterSlot,
    /// Names of the configured global tags; see
    /// [`crate::telemetry::MetricsExporter::global_tag_names`].
    global_tag_names: Vec<String>,
    /// Core's log pipeline, so a panic while flushing is reported where
    /// Core's own records go even though the releasing thread is not a Core
    /// thread.
    dispatch: Option<tracing::Dispatch>,
}

impl OtlpExporter {
    /// Names of the configured global tags, in configuration order.
    pub(crate) fn global_tag_names(&self) -> &[String] {
        &self.global_tag_names
    }
}

impl Drop for OtlpExporter {
    /// Detaches the meter and shuts the provider down with a final export;
    /// see the type documentation. A panic inside OpenTelemetry's shutdown is
    /// contained here and logged, so the rest of Core's release still runs.
    fn drop(&mut self) {
        let meter = self
            .slot
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if catch_unwind(AssertUnwindSafe(move || drop(meter))).is_err() {
            let report = || {
                tracing::error!(
                    "OTLP metrics exporter panicked during its final export; \
                     the last interval's metrics may be lost"
                );
            };
            match &self.dispatch {
                Some(dispatch) => tracing::dispatcher::with_default(dispatch, report),
                None => report(),
            }
        }
    }
}

/// Starts the OTLP exporter configured by `config` and attaches its meter
/// to `core`.
///
/// Must run before any client or worker is created on `core`, because Core
/// hands each of them the meter attached at that moment. Core's builder runs
/// inside Core's Tokio context (the gRPC transport spawns its connection
/// worker there) and with Core's log subscriber as the default, which Core
/// captures to report failed exports. No connection is made here: the
/// collector is contacted at the first export, so an unreachable collector
/// is a logged export failure, not a creation error. Any builder failure is
/// reported with a constant message (see the module documentation).
pub(crate) fn start_otlp(
    core: &mut CoreRuntime,
    config: &OtlpConfig,
) -> Result<OtlpExporter, Failure> {
    let url = validate_endpoint(&config.endpoint)?;
    let headers: HashMap<String, String> = config
        .headers
        .iter()
        .map(|header| (header.name.clone(), header.value.clone()))
        .collect();
    let global_tags: HashMap<String, String> = config
        .global_tags
        .iter()
        .map(|tag| (tag.name.clone(), tag.value.clone()))
        .collect();
    let options = OtelCollectorOptions::builder()
        .url(url)
        .headers(headers)
        .metric_periodicity(Duration::from_millis(config.period_ms))
        .metric_temporality(match config.temporality {
            OtlpTemporality::Cumulative => MetricTemporality::Cumulative,
            OtlpTemporality::Delta => MetricTemporality::Delta,
        })
        .global_tags(global_tags)
        .use_seconds_for_durations(config.durations_as_seconds)
        .protocol(match config.protocol {
            OtlpTransport::Grpc => OtlpProtocol::Grpc,
            OtlpTransport::HttpProtobuf => OtlpProtocol::Http,
        })
        .build();
    let dispatch = core
        .telemetry()
        .trace_subscriber()
        .map(tracing::Dispatch::new);
    let handle = core.tokio_handle().clone();
    let build = || {
        let _context = handle.enter();
        build_otlp_metric_exporter(options)
    };
    // Contain a panic in Core's or OpenTelemetry's builder as a configuration
    // failure; nothing has been attached to Core yet.
    let built = catch_unwind(AssertUnwindSafe(|| match &dispatch {
        Some(dispatch) => tracing::dispatcher::with_default(dispatch, build),
        None => build(),
    }))
    .map_err(|_panic| invalid("could not start the OTLP metrics exporter".to_owned()))?
    .map_err(|_error| invalid("could not start the OTLP metrics exporter".to_owned()))?;
    let meter: Arc<dyn CoreMeter> = Arc::new(built);
    let slot: MeterSlot = Arc::new(RwLock::new(Some(meter)));
    core.telemetry_mut()
        .attach_late_init_metrics(Arc::new(DetachableMeter {
            slot: Arc::clone(&slot),
        }));
    Ok(OtlpExporter {
        slot,
        global_tag_names: config.global_tag_names(),
        dispatch,
    })
}

#[cfg(test)]
#[path = "../tests/support/otlp_validation.rs"]
mod otlp_validation;

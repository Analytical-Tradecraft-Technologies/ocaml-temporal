//! Opt-in metrics export for a shared Core runtime (#500).
//!
//! Temporal Core already instruments its workers and client (poll results,
//! slot use, sticky cache, schedule-to-start and execution latency, RPC
//! counts and latency). The official SDKs expose those instruments by giving
//! Core a meter when the runtime is built; this module does the same for a
//! Prometheus scrape endpoint, so the metric names match the official SDKs.
//!
//! Ownership: [`start_metrics`] spawns Core's scrape server as one task on the
//! runtime's own Tokio pool and returns a [`MetricsExporter`] guard. The guard
//! is stored in the [`crate::abi`] `SharedCore` ahead of Core itself, so
//! releasing the last Core reference first aborts the server task and then
//! drops the Tokio runtime, which waits for its worker threads and therefore
//! closes the listening socket before the release returns. No OCaml code is
//! ever called: the server answers scrapes entirely on Tokio threads.
//!
//! Validation mirrors the OCaml `Temporal.Runtime.Metrics` constructor so a
//! value that OCaml accepts is never rejected here, while a caller that
//! bypasses OCaml still cannot register a label that collides with Core's own
//! labels or exceeds the documented bounds.

use crate::abi::{Failure, STATUS_CONFIGURATION};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;
use temporalio_common::telemetry::{PrometheusExporterOptions, start_prometheus_metric_exporter};
use temporalio_sdk_core::CoreRuntime;
use tokio::task::AbortHandle;

/// Longest accepted `bind_address`, in bytes. Any IPv6 socket address with a
/// scope identifier fits comfortably.
pub(crate) const MAX_METRICS_BIND_ADDRESS_BYTES: usize = 128;

/// Most global tags one exporter accepts. Each tag is attached to every
/// series, so the bound keeps per-series memory and scrape size predictable.
pub(crate) const MAX_METRICS_GLOBAL_TAGS: usize = 16;

/// Longest accepted global tag name, in bytes.
pub(crate) const MAX_METRICS_TAG_NAME_BYTES: usize = 64;

/// Longest accepted global tag value, in bytes.
pub(crate) const MAX_METRICS_TAG_VALUE_BYTES: usize = 256;

/// Label names that Core (or the Prometheus exposition format) already uses.
/// A global tag with one of these names would collide with a per-series label
/// and make Core's registry reject the affected instruments, so it is refused
/// up front instead of silently losing metrics.
pub(crate) const RESERVED_METRICS_TAG_NAMES: &[&str] = &[
    "activity_type",
    "eager",
    "failure_reason",
    "le",
    "namespace",
    "operation",
    "poller_type",
    "quantile",
    "service_name",
    "status_code",
    "task_queue",
    "worker_type",
    "workflow_type",
];

/// Strict telemetry document accepted by
/// `ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json`.
///
/// Every member is optional so later exporters can be added without an ABI
/// version change; an empty object configures no exporter.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TelemetryConfig {
    /// Serve Core's metrics on a Prometheus scrape endpoint.
    #[serde(default)]
    prometheus: Option<PrometheusConfig>,
}

/// Prometheus scrape-endpoint options, a strict subset of Core's
/// [`PrometheusExporterOptions`] with the same defaults.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrometheusConfig {
    /// IP socket address to listen on, for example `0.0.0.0:9464`. Port `0`
    /// selects an ephemeral port, which is reported back to the caller.
    bind_address: String,
    /// Constant labels attached to every series, in caller order.
    #[serde(default)]
    global_tags: Vec<GlobalTag>,
    /// Append `_total` to counter names (OpenMetrics style).
    #[serde(default)]
    counters_total_suffix: bool,
    /// Append the unit to histogram names, for example `_milliseconds`.
    #[serde(default)]
    unit_suffix: bool,
    /// Record durations as floating-point seconds instead of integer
    /// milliseconds.
    #[serde(default)]
    durations_as_seconds: bool,
}

/// One constant metric label.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalTag {
    name: String,
    value: String,
}

/// Owns the running Prometheus scrape server of one Core runtime.
///
/// Dropping the guard aborts the server task. The listening socket itself is
/// closed when the task is dropped, which happens no later than the owning
/// Tokio runtime's shutdown.
#[derive(Debug)]
pub(crate) struct MetricsExporter {
    bound_address: SocketAddr,
    abort: AbortHandle,
}

impl MetricsExporter {
    /// The address the endpoint actually listens on, with any ephemeral port
    /// resolved.
    pub(crate) fn bound_address(&self) -> SocketAddr {
        self.bound_address
    }
}

impl Drop for MetricsExporter {
    /// Stops accepting scrapes before Core is torn down.
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// Builds a configuration failure with a constant-shaped message.
fn invalid(message: String) -> Failure {
    Failure {
        status: STATUS_CONFIGURATION,
        message,
    }
}

/// Whether `name` is a Prometheus label name that this SDK accepts as a
/// global tag: `[A-Za-z_][A-Za-z0-9_]*`, not reserved by Prometheus (`__`
/// prefix), and not one of [`RESERVED_METRICS_TAG_NAMES`].
fn valid_tag_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let first_ok = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    first_ok
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && !name.starts_with("__")
        && !RESERVED_METRICS_TAG_NAMES.contains(&name)
}

/// Parses a bind address with the grammar `Temporal_base.Metrics_config`
/// documents, identically on every platform: a dotted-quad IPv4 address of
/// four canonical decimal octets, or a bracketed IPv6 address without a zone
/// identifier, then a canonical decimal port 0..=65535 (no leading zeros).
///
/// Rust's `Ipv4Addr` parser already rejects shorthand (`1`, `127.1`), octal
/// (`0177.0.0.1`), and hexadecimal forms. The explicit checks pin the
/// remaining spellings the OCaml validator also rejects (zone identifiers,
/// zero-padded ports), so both sides accept exactly the same text.
fn parse_bind_address(text: &str) -> Option<SocketAddr> {
    let (host, port) = text.rsplit_once(':')?;
    let canonical_port = !port.is_empty()
        && port.len() <= 5
        && port.bytes().all(|byte| byte.is_ascii_digit())
        && (port == "0" || !port.starts_with('0'));
    if !canonical_port || host.contains('%') {
        return None;
    }
    let address = SocketAddr::from_str(text).ok()?;
    // A bracketed host must be IPv6 and a bare one IPv4. The standard parser
    // already enforces this; the check keeps the contract explicit.
    let bracketed = host.starts_with('[') && host.ends_with(']');
    (bracketed == address.is_ipv6()).then_some(address)
}

/// Decodes and validates one telemetry document.
///
/// Parser diagnostics are reduced to one constant message for the same reason
/// as the lifecycle configuration decoder: serde can echo input-controlled
/// member names. Validation messages name only the offending field.
pub(crate) fn parse_telemetry_config(bytes: &[u8]) -> Result<TelemetryConfig, Failure> {
    let config: TelemetryConfig = serde_json::from_slice(bytes)
        .map_err(|_error| invalid("invalid runtime telemetry configuration JSON".to_owned()))?;
    if let Some(prometheus) = &config.prometheus {
        validate_prometheus(prometheus)?;
    }
    Ok(config)
}

/// Applies the documented bounds to one Prometheus configuration.
fn validate_prometheus(config: &PrometheusConfig) -> Result<(), Failure> {
    if config.bind_address.len() > MAX_METRICS_BIND_ADDRESS_BYTES
        || parse_bind_address(&config.bind_address).is_none()
    {
        return Err(invalid(
            "metrics bind_address must be an IP socket address such as 127.0.0.1:9464".to_owned(),
        ));
    }
    if config.global_tags.len() > MAX_METRICS_GLOBAL_TAGS {
        return Err(invalid(format!(
            "metrics global_tags accepts at most {MAX_METRICS_GLOBAL_TAGS} tags"
        )));
    }
    let mut seen = HashSet::new();
    for tag in &config.global_tags {
        if tag.name.len() > MAX_METRICS_TAG_NAME_BYTES || !valid_tag_name(&tag.name) {
            return Err(invalid(format!(
                "metrics global tag names must match [A-Za-z_][A-Za-z0-9_]*, be at most \
                 {MAX_METRICS_TAG_NAME_BYTES} bytes, not start with __, and not be a \
                 label Temporal Core uses"
            )));
        }
        if tag.value.is_empty()
            || tag.value.len() > MAX_METRICS_TAG_VALUE_BYTES
            || tag.value.chars().any(char::is_control)
        {
            return Err(invalid(format!(
                "metrics global tag values must be 1 to {MAX_METRICS_TAG_VALUE_BYTES} bytes \
                 without control characters"
            )));
        }
        if !seen.insert(tag.name.as_str()) {
            return Err(invalid(
                "metrics global tag names must be unique".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Starts the configured exporter, if any, and attaches its meter to `core`.
///
/// Must run before any client or worker is created on `core`, because Core
/// hands each of them the meter that is attached at that moment. The scrape
/// server binds synchronously (so a busy port fails here, before the runtime
/// is published) and then runs on `core`'s Tokio pool.
pub(crate) fn start_metrics(
    core: &mut CoreRuntime,
    config: &TelemetryConfig,
) -> Result<Option<MetricsExporter>, Failure> {
    let Some(prometheus) = &config.prometheus else {
        return Ok(None);
    };
    let socket_addr = parse_bind_address(&prometheus.bind_address)
        .ok_or_else(|| invalid("metrics bind_address must be an IP socket address".to_owned()))?;
    let global_tags: HashMap<String, String> = prometheus
        .global_tags
        .iter()
        .map(|tag| (tag.name.clone(), tag.value.clone()))
        .collect();
    let options = PrometheusExporterOptions::builder()
        .socket_addr(socket_addr)
        .global_tags(global_tags)
        .counters_total_suffix(prometheus.counters_total_suffix)
        .unit_suffix(prometheus.unit_suffix)
        .use_seconds_for_durations(prometheus.durations_as_seconds)
        .build();
    // Core's exporter spawns its accept loop with `tokio::spawn`, so it needs
    // this runtime's context; the guard is dropped before returning.
    let started = {
        let _context = core.tokio_handle().enter();
        start_prometheus_metric_exporter(options)
    }
    .map_err(|error| {
        // Only the I/O error kind is reported: it is stable and cannot carry
        // remote or input-controlled text. The address is the caller's own
        // configuration.
        let kind = error
            .downcast_ref::<std::io::Error>()
            .map_or_else(|| "other".to_owned(), |io| format!("{:?}", io.kind()));
        invalid(format!(
            "could not bind the Prometheus metrics endpoint {socket_addr}: {kind}"
        ))
    })?;
    core.telemetry_mut()
        .attach_late_init_metrics(started.meter.clone());
    Ok(Some(MetricsExporter {
        bound_address: started.bound_addr,
        abort: started.abort_handle,
    }))
}

//! Opt-in metrics export for a shared Core runtime (#500).
//!
//! Temporal Core already instruments its workers and client (poll results,
//! slot use, sticky cache, schedule-to-start and execution latency, RPC
//! counts and latency). The official SDKs expose those instruments by giving
//! Core a meter when the runtime is built; this module does the same for a
//! Prometheus scrape endpoint, so the metric names match the official SDKs.
//!
//! Accept resilience (#1030): Core's scrape server returns from its accept
//! loop on the first `accept()` error, which closes its listener for good,
//! and Core exposes neither its registry nor a way to restart the server with
//! the same meter. The bridge therefore owns the public listener itself and
//! runs a retrying accept loop on it ([`serve_scrapes`]): per-connection
//! errors (a peer that reset before `accept`) are skipped, and every other
//! error (for example `EMFILE` under descriptor pressure) is logged and
//! retried with a bounded backoff. Core's server listens only on an
//! ephemeral loopback port, and each accepted scrape connection is relayed
//! to it byte for byte. Core's accept loop then sees only the bridge's own
//! connections, so the remaining way it can stop is a resource error on that
//! private listener; scrapes then receive `503 Service Unavailable` and one
//! error record is logged instead of the port going silent.
//!
//! Ownership: [`start_metrics`] binds the public listener, starts Core's
//! scrape server, and spawns the accept loop, all on the runtime's own Tokio
//! pool, and returns a [`MetricsExporter`] guard. The guard is stored in the
//! [`crate::abi`] `SharedCore` ahead of Core itself, so releasing the last
//! Core reference first aborts both tasks and then drops the Tokio runtime,
//! which waits for its worker threads and therefore closes both listening
//! sockets (and any relayed connection) before the release returns. No OCaml
//! code is ever called: scrapes are answered entirely on Tokio threads.
//!
//! Validation mirrors the OCaml `Temporal.Runtime.Metrics` constructor so a
//! value that OCaml accepts is never rejected here, while a caller that
//! bypasses OCaml still cannot register a label that collides with Core's own
//! labels or exceeds the documented bounds.

use crate::abi::{Failure, STATUS_CONFIGURATION};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use temporalio_common::telemetry::{PrometheusExporterOptions, start_prometheus_metric_exporter};
use temporalio_sdk_core::CoreRuntime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
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

/// First delay before retrying `accept()` after an error that is not scoped
/// to one connection. Doubles on each consecutive error up to
/// [`ACCEPT_RETRY_MAX`] and resets after the next successful accept.
const ACCEPT_RETRY_MIN: Duration = Duration::from_millis(10);

/// Longest delay between `accept()` retries. Bounds both how long a
/// recovered endpoint can stay unresponsive and how often a persistent error
/// is logged (at most once per interval).
const ACCEPT_RETRY_MAX: Duration = Duration::from_secs(1);

/// Longest time the `503` fallback waits for a scraper's request head before
/// answering anyway.
const UNAVAILABLE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Largest request head the `503` fallback reads before answering; the
/// endpoint serves only bodyless `GET` requests.
const UNAVAILABLE_REQUEST_MAX_BYTES: usize = 8 * 1024;

/// Body of the `503` fallback, naming the cause and the recovery.
const UNAVAILABLE_BODY: &str = "Temporal Core metrics server stopped; recreate the runtime.\n";

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

/// Owns the running Prometheus scrape endpoint of one Core runtime: the
/// bridge's accept loop on the public address and Core's scrape server on a
/// private loopback port.
///
/// Dropping the guard aborts both tasks. Each listening socket is closed when
/// its task is dropped, which happens no later than the owning Tokio
/// runtime's shutdown.
#[derive(Debug)]
pub(crate) struct MetricsExporter {
    bound_address: SocketAddr,
    /// The bridge's retrying accept loop on `bound_address`.
    accept_loop: AbortHandle,
    /// Core's scrape server; shared with relays, which consult
    /// [`AbortHandle::is_finished`] to tell a stopped server from a transient
    /// connect failure.
    core_server: Arc<AbortHandle>,
}

impl MetricsExporter {
    /// The address the endpoint actually listens on, with any ephemeral port
    /// resolved.
    pub(crate) fn bound_address(&self) -> SocketAddr {
        self.bound_address
    }

    /// Stops Core's scrape server while leaving the public accept loop
    /// running, as an accept error on Core's private listener would. Test
    /// support only.
    pub(crate) fn stop_core_server(&self) {
        self.core_server.abort();
    }
}

impl Drop for MetricsExporter {
    /// Stops accepting scrapes before Core is torn down.
    fn drop(&mut self) {
        self.accept_loop.abort();
        self.core_server.abort();
    }
}

/// Whether an `accept()` error concerns only the connection being accepted
/// (it was reset or aborted before it could be accepted, or the call was
/// interrupted), so the listener is healthy and accepting again at once is
/// correct.
fn connection_scoped(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset | ErrorKind::Interrupted
    )
}

/// Accepts scrape connections on the public listener until aborted, relaying
/// each one to Core's scrape server at `core_address`.
///
/// No `accept()` error ends the loop. Connection-scoped errors are skipped;
/// any other error (descriptor exhaustion, a network that went down) is
/// logged with its kind only and retried after a backoff that doubles from
/// [`ACCEPT_RETRY_MIN`] to [`ACCEPT_RETRY_MAX`], so a persistent error
/// neither spins a Tokio worker nor floods the log.
async fn serve_scrapes(
    listener: TcpListener,
    core_address: SocketAddr,
    core_server: Arc<AbortHandle>,
) {
    let stop_reported = Arc::new(AtomicBool::new(false));
    let mut retry = ACCEPT_RETRY_MIN;
    loop {
        match listener.accept().await {
            Ok((client, _)) => {
                retry = ACCEPT_RETRY_MIN;
                tokio::spawn(relay_scrape(
                    client,
                    core_address,
                    Arc::clone(&core_server),
                    Arc::clone(&stop_reported),
                ));
            }
            Err(error) if connection_scoped(error.kind()) => {
                tracing::debug!(kind = ?error.kind(), "metrics endpoint skipped a failed connection");
            }
            Err(error) => {
                tracing::warn!(
                    kind = ?error.kind(),
                    os_error = error.raw_os_error(),
                    retry_ms = u64::try_from(retry.as_millis()).unwrap_or(u64::MAX),
                    "metrics endpoint accept failed; retrying"
                );
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(ACCEPT_RETRY_MAX);
            }
        }
    }
}

/// Relays one scrape connection to Core's scrape server, byte for byte, until
/// either side closes.
///
/// When Core's server cannot be reached because it has stopped, the scraper
/// receives a `503` (see [`answer_unavailable`]) and the first such relay
/// logs one error record (`stop_reported` makes it once per exporter). A
/// connect failure while Core's server is still running is transient and
/// only closes this connection.
async fn relay_scrape(
    mut client: TcpStream,
    core_address: SocketAddr,
    core_server: Arc<AbortHandle>,
    stop_reported: Arc<AtomicBool>,
) {
    match TcpStream::connect(core_address).await {
        Ok(mut upstream) => {
            // Errors here are a scraper or Core closing its side; there is
            // nothing to report beyond what Core logs for its connection.
            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
        }
        Err(error) if core_server.is_finished() => {
            if !stop_reported.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    kind = ?error.kind(),
                    "Temporal Core's metrics server stopped; the endpoint answers 503 until the runtime is recreated"
                );
            }
            answer_unavailable(&mut client).await;
        }
        Err(error) => {
            tracing::warn!(
                kind = ?error.kind(),
                "could not reach Temporal Core's metrics server; closing one scrape connection"
            );
        }
    }
}

/// Reads the scraper's request head (bounded in size and time) and answers
/// `503 Service Unavailable` with [`UNAVAILABLE_BODY`]. Reading first means
/// the close that follows is an orderly FIN, so the scraper sees the status
/// rather than a reset caused by unread request bytes.
async fn answer_unavailable(client: &mut TcpStream) {
    let read_head = async {
        let mut head = Vec::new();
        let mut chunk = [0_u8; 1024];
        while head.len() < UNAVAILABLE_REQUEST_MAX_BYTES
            && !head.windows(4).any(|window| window == b"\r\n\r\n")
        {
            match client.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(count) => head.extend_from_slice(&chunk[..count]),
            }
        }
    };
    let _ = tokio::time::timeout(UNAVAILABLE_REQUEST_TIMEOUT, read_head).await;
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain; charset=utf-8\r\n\
         Connection: close\r\nContent-Length: {}\r\n\r\n{UNAVAILABLE_BODY}",
        UNAVAILABLE_BODY.len()
    );
    let _ = client.write_all(response.as_bytes()).await;
    let _ = client.shutdown().await;
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
/// hands each of them the meter that is attached at that moment. The public
/// listener binds synchronously (so a busy port fails here, before the
/// runtime is published); Core's scrape server and the bridge's accept loop
/// then run on `core`'s Tokio pool. Any failure drops whatever was already
/// bound or spawned here, so nothing keeps running.
pub(crate) fn start_metrics(
    core: &mut CoreRuntime,
    config: &TelemetryConfig,
) -> Result<Option<MetricsExporter>, Failure> {
    let Some(prometheus) = &config.prometheus else {
        return Ok(None);
    };
    let socket_addr = parse_bind_address(&prometheus.bind_address)
        .ok_or_else(|| invalid("metrics bind_address must be an IP socket address".to_owned()))?;
    // Only the I/O error kind is reported: it is stable and cannot carry
    // remote or input-controlled text. The address is the caller's own
    // configuration.
    let bind_failure = |kind: String| {
        invalid(format!(
            "could not bind the Prometheus metrics endpoint {socket_addr}: {kind}"
        ))
    };
    let public = std::net::TcpListener::bind(socket_addr)
        .and_then(|listener| listener.set_nonblocking(true).map(|()| listener))
        .map_err(|error| bind_failure(format!("{:?}", error.kind())))?;
    let bound_address = public
        .local_addr()
        .map_err(|error| bind_failure(format!("{:?}", error.kind())))?;

    // Core's server is reachable only through the bridge's relay, on the
    // loopback address of the same family as the public endpoint.
    let core_bind = SocketAddr::new(
        if socket_addr.is_ipv6() {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        } else {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        },
        0,
    );
    let global_tags: HashMap<String, String> = prometheus
        .global_tags
        .iter()
        .map(|tag| (tag.name.clone(), tag.value.clone()))
        .collect();
    let options = PrometheusExporterOptions::builder()
        .socket_addr(core_bind)
        .global_tags(global_tags)
        .counters_total_suffix(prometheus.counters_total_suffix)
        .unit_suffix(prometheus.unit_suffix)
        .use_seconds_for_durations(prometheus.durations_as_seconds)
        .build();
    // Core's exporter spawns its accept loop with `tokio::spawn`, and Tokio
    // listeners register with the current reactor, so both need this
    // runtime's context; the guard is dropped before returning.
    let handle = core.tokio_handle().clone();
    let context = handle.enter();
    let started = start_prometheus_metric_exporter(options).map_err(|error| {
        let kind = error
            .downcast_ref::<std::io::Error>()
            .map_or_else(|| "other".to_owned(), |io| format!("{:?}", io.kind()));
        bind_failure(kind)
    })?;
    let core_server = Arc::new(started.abort_handle);
    let listener = match TcpListener::from_std(public) {
        Ok(listener) => listener,
        Err(error) => {
            core_server.abort();
            return Err(bind_failure(format!("{:?}", error.kind())));
        }
    };
    let accept_loop = handle
        .spawn(serve_scrapes(
            listener,
            started.bound_addr,
            Arc::clone(&core_server),
        ))
        .abort_handle();
    drop(context);
    core.telemetry_mut()
        .attach_late_init_metrics(started.meter.clone());
    Ok(Some(MetricsExporter {
        bound_address,
        accept_loop,
        core_server,
    }))
}

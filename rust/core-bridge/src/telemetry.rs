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
//! error record is logged instead of the port going silent. Relays keep a
//! small reserve of spare descriptors ([`DescriptorReserve`]) and release it
//! just before each private `connect()`, so descriptor pressure on the public
//! side cannot leave Core's `accept()` without a descriptor.
//!
//! Connection cap (#500): at most [`MAX_CONCURRENT_SCRAPES`] connections are
//! relayed at once. The accept loop takes a semaphore permit before spawning
//! a relay. A connection accepted while every permit is held never connects
//! to Core and never touches the descriptor reserve: a short rejection task
//! (at most [`MAX_PENDING_REJECTIONS`] at once, each bounded by
//! [`REJECTION_TIMEOUT`]) reads its request head and answers `503`, and once
//! those are exhausted too the connection is closed straight away. Every
//! relay is bounded in time (request head, idle and lifetime timeouts), so
//! slow or idle clients release their permit after at most
//! [`RELAY_MAX_LIFETIME`].
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

use crate::abi::{Failure, STATUS_CONFIGURATION, STATUS_INVALID_ARGUMENT, STATUS_OK, Status};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use temporalio_common::telemetry::metrics::{
    Counter, CounterBase, Gauge, GaugeBase, Histogram, HistogramBase, HistogramDuration,
    HistogramDurationBase, MetricAttributable, MetricKeyValue, MetricParameters, NewAttributes,
    TemporalMeter, UpDownCounter, UpDownCounterBase,
};
use temporalio_common::telemetry::{PrometheusExporterOptions, start_prometheus_metric_exporter};
use temporalio_sdk_core::CoreRuntime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::AbortHandle;
use tokio::time::Instant;

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

/// Longest time a relay waits for a scraper's request head before handling
/// what it has (or dropping a connection that sent nothing).
const REQUEST_HEAD_TIMEOUT: Duration = Duration::from_secs(5);

/// Largest request head a relay buffers before connecting to Core; the
/// endpoint serves only bodyless `GET` requests.
const REQUEST_HEAD_MAX_BYTES: usize = 8 * 1024;

/// Longest time a relay holds the descriptor reserve open waiting for Core's
/// first response bytes, which prove Core has accepted the private
/// connection.
const FIRST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Most scrape connections relayed at once (#500). A Prometheus server keeps
/// one connection per target, so a handful of scrapers (an HA pair, an
/// agent, an operator's `curl`) fit with room to spare, while a flood of
/// connections holds at most this many relays, each with two sockets of its
/// own and one on Core's side. Connections beyond the cap receive a `503`
/// with [`BUSY_BODY`] (see [`MAX_PENDING_REJECTIONS`]).
pub const MAX_CONCURRENT_SCRAPES: usize = 8;

/// Most over-cap connections answered with a `503` at once. Each holds one
/// socket for at most [`REJECTION_TIMEOUT`]; a connection accepted while
/// this many are pending is closed without an answer, so a flood costs one
/// `accept()` and one `close()` per connection beyond both bounds.
const MAX_PENDING_REJECTIONS: usize = 16;

/// Longest time an over-cap connection is kept to read its request head and
/// receive the `503`. Reading the head first lets the close be an orderly
/// FIN: closing with unread request bytes makes the kernel send a reset,
/// which on some platforms discards the response before the scraper reads it.
const REJECTION_TIMEOUT: Duration = Duration::from_secs(1);

/// Longest time a relayed connection may go without a byte moving in either
/// direction before the relay closes it. Releases the permit of a keep-alive
/// scraper between scrapes and of a client that stopped reading or writing.
const RELAY_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest time a relayed connection may stay open in total, measured from
/// `accept()`. Bounds a client that trickles bytes just often enough to
/// defeat [`RELAY_IDLE_TIMEOUT`]. A keep-alive scraper whose connection is
/// closed here simply reconnects for its next scrape.
const RELAY_MAX_LIFETIME: Duration = Duration::from_secs(60);

/// Buffer size for each direction of a relayed connection.
const RELAY_BUFFER_BYTES: usize = 8 * 1024;

/// Spare descriptors the relays keep open (#1030). Releasing them immediately
/// before a private `connect` leaves, even when the process is otherwise at
/// its descriptor limit, one descriptor for the relay's own socket, one for
/// Core's `accept()` of it, and one for the `accept()` Core's loop makes
/// straight afterwards. Tokio keeps calling `accept()` until it returns
/// `WouldBlock`, and Linux allocates the new descriptor before it looks at
/// the queue, so that follow-up call fails with `EMFILE` (ending Core's loop)
/// rather than `EAGAIN` when no descriptor is free. Windows keeps no reserve,
/// so the constant is Unix-only.
#[cfg(unix)]
const RESERVED_DESCRIPTORS: usize = 3;

/// Body of the `503` sent once Core's scrape server has stopped, naming the
/// cause and the recovery.
const STOPPED_BODY: &str = "Temporal Core metrics server stopped; recreate the runtime.\n";

/// Body of the `503` sent when the descriptor reserve cannot be refilled, so
/// relaying would risk starving Core's `accept()`. Transient: the next scrape
/// after descriptor pressure eases is served normally.
const STARVED_BODY: &str = "Too few file descriptors to serve metrics safely; retry later.\n";

/// Body of the `503` sent to a connection accepted while
/// [`MAX_CONCURRENT_SCRAPES`] relays are running. Transient: a later scrape
/// is served once a relay finishes.
const BUSY_BODY: &str = "Too many concurrent metrics scrapes; retry later.\n";

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
    /// Names of the configured global tags. Every series already carries
    /// them, so an SDK instrument label with one of these names would
    /// duplicate a label; [`MetricMeter::register`] refuses it.
    global_tag_names: Vec<String>,
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

    /// Whether Core's scrape server task has ended. Test support only.
    pub(crate) fn core_server_stopped(&self) -> bool {
        self.core_server.is_finished()
    }

    /// Names of the configured global tags, in configuration order.
    pub(crate) fn global_tag_names(&self) -> &[String] {
        &self.global_tag_names
    }
}

impl Drop for MetricsExporter {
    /// Stops accepting scrapes before Core is torn down.
    fn drop(&mut self) {
        self.accept_loop.abort();
        self.core_server.abort();
    }
}

/// Spare descriptors (open handles to the null device) that guarantee
/// headroom for Core's private `accept()`.
///
/// Core's accept loop ends for good on `EMFILE`, and every relayed scrape
/// makes Core accept one connection and then call `accept()` once more,
/// which on Linux needs a free descriptor even though nothing is queued (see
/// [`RESERVED_DESCRIPTORS`]). Without a reserve, a process at its descriptor
/// limit that frees exactly the descriptors the relay needs (the public
/// `accept()` and the private `connect()`) would leave Core's `accept()`
/// calls with none. A relay therefore refills the reserve first, releases it
/// immediately before connecting, and refills it only after Core has
/// answered. Core makes both `accept()` calls in the same poll of its accept
/// loop, before the spawned connection task can answer, so they draw on the
/// released descriptors. Relays use the reserve one at a time, under the
/// mutex in [`RelayShared`].
///
/// The descriptor table is process-wide, so another thread that opens a file
/// in the instant between release and Core's `accept()` calls can still take
/// a released descriptor. With several Tokio workers, Core's connection task
/// could in principle answer on another worker before its accept loop's
/// follow-up call; that window is a few instructions against a full HTTP
/// exchange. The reserve removes the deterministic starvation, not these
/// races. Windows has no comparable per-process descriptor limit for
/// sockets, so the reserve holds nothing there.
#[derive(Debug, Default)]
struct DescriptorReserve {
    /// Open spare descriptors, at most [`RESERVED_DESCRIPTORS`].
    #[cfg(unix)]
    held: Vec<std::fs::File>,
}

impl DescriptorReserve {
    /// Opens spares until [`RESERVED_DESCRIPTORS`] are held. Returns whether
    /// the reserve is full; a failure (normally `EMFILE`) keeps what was
    /// opened so a later call can finish the job.
    fn refill(&mut self) -> bool {
        #[cfg(unix)]
        while self.held.len() < RESERVED_DESCRIPTORS {
            match std::fs::File::open("/dev/null") {
                Ok(file) => self.held.push(file),
                Err(_) => return false,
            }
        }
        true
    }

    /// Closes every spare so the descriptors are free for the next
    /// `connect()` and Core's `accept()`.
    fn release(&mut self) {
        #[cfg(unix)]
        self.held.clear();
    }
}

/// State every relay of one exporter shares.
struct RelayShared {
    /// Core's private scrape server address.
    core_address: SocketAddr,
    /// Core's scrape server task; see [`MetricsExporter::core_server`].
    core_server: Arc<AbortHandle>,
    /// Makes the "server stopped" error record once per exporter.
    stop_reported: AtomicBool,
    /// Descriptor headroom for Core's `accept()`. The async mutex also
    /// serializes the window between releasing it and Core's first
    /// response, which is what makes the released descriptors available to
    /// Core rather than to a concurrent relay.
    reserve: tokio::sync::Mutex<DescriptorReserve>,
    /// One permit per relay that may run, [`MAX_CONCURRENT_SCRAPES`] in all.
    /// The accept loop takes a permit before spawning a relay, and the relay
    /// task owns it until the task ends, so every exit path releases it.
    relay_slots: Arc<Semaphore>,
    /// One permit per pending over-cap rejection, [`MAX_PENDING_REJECTIONS`]
    /// in all, owned by the rejection task like `relay_slots`.
    rejection_slots: Arc<Semaphore>,
    /// Makes the first "too many concurrent scrapes" record a warning; later
    /// rejections are logged at debug level so a flood cannot flood the log.
    busy_reported: AtomicBool,
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
/// each one to Core's scrape server.
///
/// Each relay needs one of [`MAX_CONCURRENT_SCRAPES`] permits. A connection
/// accepted when none is free is answered by [`reject_busy`] if one of
/// [`MAX_PENDING_REJECTIONS`] rejection permits is free, and closed at once
/// otherwise. Accepting continues while every permit is held, so the
/// kernel's listen backlog does not fill with connections that would
/// otherwise wait unanswered.
///
/// No `accept()` error ends the loop. Connection-scoped errors are skipped;
/// any other error (descriptor exhaustion, a network that went down) is
/// logged with its kind only and retried after a backoff that doubles from
/// [`ACCEPT_RETRY_MIN`] to [`ACCEPT_RETRY_MAX`], so a persistent error
/// neither spins a Tokio worker nor floods the log.
async fn serve_scrapes(listener: TcpListener, shared: Arc<RelayShared>) {
    let mut retry = ACCEPT_RETRY_MIN;
    loop {
        match listener.accept().await {
            Ok((client, _)) => {
                retry = ACCEPT_RETRY_MIN;
                match Arc::clone(&shared.relay_slots).try_acquire_owned() {
                    Ok(permit) => {
                        tokio::spawn(relay_scrape(client, Arc::clone(&shared), permit));
                    }
                    Err(_) => {
                        report_busy(&shared);
                        if let Ok(permit) = Arc::clone(&shared.rejection_slots).try_acquire_owned()
                        {
                            tokio::spawn(reject_busy(client, permit));
                        }
                        // Otherwise `client` is dropped here, closing it
                        // without an answer.
                    }
                }
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

/// Logs that a connection arrived while every relay permit was held. The
/// first rejection per exporter is a `warn` record and later ones are
/// `debug`, so a connection flood cannot flood the log.
fn report_busy(shared: &RelayShared) {
    if shared.busy_reported.swap(true, Ordering::Relaxed) {
        tracing::debug!("metrics endpoint at its connection cap; answering 503");
    } else {
        tracing::warn!(
            limit = MAX_CONCURRENT_SCRAPES,
            "metrics endpoint at its concurrent connection cap; answering 503 (further rejections are logged at debug level)"
        );
    }
}

/// Answers an over-cap connection with a `503` carrying [`BUSY_BODY`] and
/// closes it, within [`REJECTION_TIMEOUT`] in total. The request head is read
/// first so the close is an orderly FIN; a connection that sends nothing in
/// time is closed without an answer. Never connects to Core and never touches
/// the descriptor reserve. `_permit` is one of the
/// [`MAX_PENDING_REJECTIONS`] slots, released when the task ends.
async fn reject_busy(mut client: TcpStream, _permit: OwnedSemaphorePermit) {
    let _ = tokio::time::timeout(REJECTION_TIMEOUT, async {
        if read_request_head(&mut client).await.is_some() {
            answer_unavailable(&mut client, BUSY_BODY).await;
        }
    })
    .await;
}

/// Relays one scrape connection to Core's scrape server, byte for byte, until
/// either side closes, the connection idles for [`RELAY_IDLE_TIMEOUT`], or
/// [`RELAY_MAX_LIFETIME`] has passed since it was accepted. `_permit` is one
/// of the [`MAX_CONCURRENT_SCRAPES`] relay slots; it is released when this
/// task ends, whichever way it ends (including an abort at shutdown).
///
/// The request head is read before Core is involved, so a connection that
/// sends nothing never occupies the reserve. Then, holding the reserve:
///
/// - if Core's server has stopped, the scraper receives a `503` with
///   [`STOPPED_BODY`] and the first such relay logs one error record;
/// - if the reserve cannot be refilled, connecting could starve Core's
///   `accept()`, so the scraper receives a `503` with [`STARVED_BODY`];
/// - otherwise the reserve is released, the relay connects and forwards the
///   head, and refills the reserve only once Core's first response bytes
///   arrive (or [`FIRST_RESPONSE_TIMEOUT`] passes), before relaying the rest.
async fn relay_scrape(
    mut client: TcpStream,
    shared: Arc<RelayShared>,
    _permit: OwnedSemaphorePermit,
) {
    let deadline = Instant::now() + RELAY_MAX_LIFETIME;
    let Some(head) = read_request_head(&mut client).await else {
        return;
    };
    let mut reserve = shared.reserve.lock().await;
    if shared.core_server.is_finished() {
        drop(reserve);
        report_stopped(&shared);
        answer_unavailable(&mut client, STOPPED_BODY).await;
        return;
    }
    if !reserve.refill() {
        drop(reserve);
        tracing::warn!("too few file descriptors to relay a metrics scrape safely; answering 503");
        answer_unavailable(&mut client, STARVED_BODY).await;
        return;
    }
    reserve.release();
    let mut upstream = match TcpStream::connect(shared.core_address).await {
        Ok(upstream) => upstream,
        Err(error) => {
            reserve.refill();
            drop(reserve);
            if shared.core_server.is_finished() {
                report_stopped(&shared);
                answer_unavailable(&mut client, STOPPED_BODY).await;
            } else {
                tracing::warn!(
                    kind = ?error.kind(),
                    "could not reach Temporal Core's metrics server; answering 503"
                );
                answer_unavailable(&mut client, STARVED_BODY).await;
            }
            return;
        }
    };
    // Core answers only after it has accepted, so its first bytes prove the
    // released descriptor was used by Core's `accept()` and refilling cannot
    // take it away. A timeout leaves only a slow or idle exchange to relay.
    let mut first = vec![0_u8; 8 * 1024];
    let first_read = if upstream.write_all(&head).await.is_ok() {
        tokio::time::timeout(FIRST_RESPONSE_TIMEOUT, upstream.read(&mut first)).await
    } else {
        Ok(Ok(0))
    };
    reserve.refill();
    drop(reserve);
    match first_read {
        Ok(Ok(0) | Err(_)) => return,
        Ok(Ok(count)) => {
            if client.write_all(&first[..count]).await.is_err() {
                return;
            }
        }
        Err(_elapsed) => {}
    }
    // Ending at the deadline drops both streams, which closes them; a slow
    // write stuck inside `relay_until_idle` is cancelled with it.
    let _ = tokio::time::timeout_at(deadline, relay_until_idle(&mut client, &mut upstream)).await;
}

/// Copies bytes in both directions until both sides have closed, either side
/// fails, or no byte moves for [`RELAY_IDLE_TIMEOUT`].
///
/// A side's end of input is forwarded as a write shutdown to the other side,
/// as `tokio::io::copy_bidirectional` does, so HTTP half-close keeps working.
/// A write that cannot complete within the idle timeout (a scraper that
/// stopped reading) also ends the relay. Errors are a scraper or Core closing
/// its side; there is nothing to report beyond what Core logs for its
/// connection.
async fn relay_until_idle(client: &mut TcpStream, upstream: &mut TcpStream) {
    let (mut client_read, mut client_write) = client.split();
    let (mut core_read, mut core_write) = upstream.split();
    let mut to_core = vec![0_u8; RELAY_BUFFER_BYTES];
    let mut to_client = vec![0_u8; RELAY_BUFFER_BYTES];
    let mut client_open = true;
    let mut core_open = true;
    while client_open || core_open {
        // One step: wait for either side's next read and forward it in full.
        // Both branches borrow disjoint halves, and a branch that is not
        // chosen is dropped before any bytes are consumed from its socket.
        let step = tokio::time::timeout(RELAY_IDLE_TIMEOUT, async {
            tokio::select! {
                read = client_read.read(&mut to_core), if client_open => match read {
                    Ok(0) => {
                        client_open = false;
                        core_write.shutdown().await.is_ok()
                    }
                    Ok(count) => core_write.write_all(&to_core[..count]).await.is_ok(),
                    Err(_) => false,
                },
                read = core_read.read(&mut to_client), if core_open => match read {
                    Ok(0) => {
                        core_open = false;
                        client_write.shutdown().await.is_ok()
                    }
                    Ok(count) => client_write.write_all(&to_client[..count]).await.is_ok(),
                    Err(_) => false,
                },
            }
        })
        .await;
        if !matches!(step, Ok(true)) {
            return;
        }
    }
}

/// Logs that Core's scrape server has stopped, once per exporter.
fn report_stopped(shared: &RelayShared) {
    if !shared.stop_reported.swap(true, Ordering::Relaxed) {
        tracing::error!(
            "Temporal Core's metrics server stopped; the endpoint answers 503 until the runtime is recreated"
        );
    }
}

/// Reads the scraper's request head, bounded by [`REQUEST_HEAD_MAX_BYTES`]
/// and [`REQUEST_HEAD_TIMEOUT`]. Returns whatever arrived (which may run
/// past the head), or `None` when the scraper sent nothing.
async fn read_request_head(client: &mut TcpStream) -> Option<Vec<u8>> {
    let mut head = Vec::new();
    let read = async {
        let mut chunk = [0_u8; 1024];
        while head.len() < REQUEST_HEAD_MAX_BYTES
            && !head.windows(4).any(|window| window == b"\r\n\r\n")
        {
            match client.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(count) => head.extend_from_slice(&chunk[..count]),
            }
        }
    };
    let _ = tokio::time::timeout(REQUEST_HEAD_TIMEOUT, read).await;
    (!head.is_empty()).then_some(head)
}

/// Answers `503 Service Unavailable` with `body` and closes. The request head
/// was already read, so the close is an orderly FIN and the scraper sees the
/// status rather than a reset caused by unread request bytes.
async fn answer_unavailable(client: &mut TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain; charset=utf-8\r\n\
         Connection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
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
    // A reserve that cannot be filled yet is refilled by the first relay.
    let mut reserve = DescriptorReserve::default();
    reserve.refill();
    let shared = Arc::new(RelayShared {
        core_address: started.bound_addr,
        core_server: Arc::clone(&core_server),
        stop_reported: AtomicBool::new(false),
        reserve: tokio::sync::Mutex::new(reserve),
        relay_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_SCRAPES)),
        rejection_slots: Arc::new(Semaphore::new(MAX_PENDING_REJECTIONS)),
        busy_reported: AtomicBool::new(false),
    });
    let accept_loop = handle.spawn(serve_scrapes(listener, shared)).abort_handle();
    drop(context);
    core.telemetry_mut()
        .attach_late_init_metrics(started.meter.clone());
    Ok(Some(MetricsExporter {
        bound_address,
        accept_loop,
        core_server,
        global_tag_names: prometheus
            .global_tags
            .iter()
            .map(|tag| tag.name.clone())
            .collect(),
    }))
}

/// Most series one [`MetricMeter`] registers. The SDK registers a fixed set
/// per worker (a few dozen), so the bound only stops a defect from growing
/// the registry, and with it every scrape, without limit.
pub(crate) const MAX_SDK_INSTRUMENTS: usize = 1024;

/// Most labels one SDK series carries, besides Core's `service_name` and the
/// runtime's global tags.
pub(crate) const MAX_SDK_INSTRUMENT_LABELS: usize = 8;

/// Longest accepted SDK metric name, in bytes, before Core's `temporal_`
/// prefix.
pub(crate) const MAX_SDK_INSTRUMENT_NAME_BYTES: usize = 128;

/// Longest accepted SDK metric description, in bytes.
pub(crate) const MAX_SDK_INSTRUMENT_DESCRIPTION_BYTES: usize = 512;

/// Upper bound on one series registration document.
pub(crate) const MAX_SDK_INSTRUMENT_DOCUMENT_BYTES: usize = 8 * 1024;

/// Label names an SDK series may not use: Core attaches `service_name` to
/// every series, and Prometheus reserves `le` and `quantile` for histogram
/// and summary samples.
const RESERVED_SDK_LABEL_NAMES: &[&str] = &["le", "quantile", "service_name"];

/// The kind of one SDK metric, fixed per metric name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InstrumentKind {
    /// Monotonic counter; a recorded value is added.
    Counter,
    /// Signed gauge driven by deltas; a recorded value, which may be
    /// negative, is added. Exported as a Prometheus gauge.
    UpDownCounter,
    /// Gauge; a recorded value replaces the current one.
    Gauge,
    /// Histogram of plain numbers; a recorded value is one observation.
    Histogram,
    /// Duration histogram; a recorded value is one observation in
    /// microseconds. Core exports it in milliseconds or, with
    /// `durations_as_seconds`, in seconds, like its own latency histograms.
    DurationHistogram,
}

/// Strict registration document accepted by
/// `ocaml_temporal_core_v4_metric_meter_register_json`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentSpec {
    kind: InstrumentKind,
    /// Name without Core's `temporal_` prefix, which Core adds.
    name: String,
    /// Prometheus `HELP` text; identical for every series of one name.
    #[serde(default)]
    description: String,
    /// Constant labels of this series, in caller order.
    #[serde(default)]
    labels: Vec<GlobalTag>,
}

/// One registered series, bound to its label set at registration so that
/// recording neither allocates nor looks labels up: Core caches the bound
/// Prometheus child on first use.
enum Instrument {
    Counter(Counter),
    UpDownCounter(UpDownCounter),
    Gauge(Gauge),
    Histogram(Histogram),
    DurationHistogram(HistogramDuration),
}

/// Registration state behind [`MetricMeter`]'s lock.
#[derive(Default)]
struct InstrumentRegistry {
    /// Bound series, indexed by the identifier returned to the caller.
    instruments: Vec<Instrument>,
    /// Canonical registration key (kind, name and labels) to identifier, so
    /// registering the same series twice returns the same identifier.
    ids: HashMap<String, u32>,
    /// Kind and description of every registered name. One Prometheus family
    /// has one type and one help text, so a conflicting registration is
    /// refused rather than producing a malformed scrape.
    families: HashMap<String, (InstrumentKind, String)>,
}

/// SDK-side metrics recorder over a runtime's Core meter (#500).
///
/// Core records its own worker and client metrics, but some worker states
/// exist only in the OCaml SDK (activity-lane occupancy, refused tasks,
/// retained-completion retries, shutdown outcomes, expired asynchronous
/// leases). The SDK registers each series once with [`MetricMeter::register`]
/// and then records into it by identifier with [`MetricMeter::record`]. The
/// series land in the same Prometheus registry as Core's own instruments,
/// with Core's `temporal_` prefix, its `service_name` label, and the
/// runtime's global tags.
///
/// Ownership: the meter holds a clone of Core's meter (the Prometheus
/// registry), not of Core itself, so it never keeps Core's Tokio runtime
/// alive and may be dropped on any thread, including from a GC finalizer.
/// Recording after Core was destroyed updates a registry that nobody scrapes
/// any longer, which is harmless. Registration takes the write lock and
/// recording only the read lock, so concurrent recorders on several threads
/// never wait for each other.
pub struct MetricMeter {
    meter: TemporalMeter,
    /// Label names the runtime's global tags already use.
    global_tag_names: Vec<String>,
    registry: RwLock<InstrumentRegistry>,
}

impl std::fmt::Debug for MetricMeter {
    /// Bound instruments are opaque Core values; only their count is useful.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self
            .registry
            .read()
            .map_or(0, |registry| registry.instruments.len());
        formatter
            .debug_struct("MetricMeter")
            .field("instruments", &count)
            .finish_non_exhaustive()
    }
}

/// Whether `name` is an acceptable SDK metric or label name:
/// `[a-z_][a-z0-9_]*`, without the Prometheus-reserved `__` prefix.
fn valid_sdk_identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        && !name.starts_with("__")
}

impl MetricMeter {
    /// Wraps a runtime's prefixed Core meter. `global_tag_names` are the
    /// exporter's constant labels, which series labels may not repeat.
    pub(crate) fn new(meter: TemporalMeter, global_tag_names: Vec<String>) -> Self {
        Self {
            meter,
            global_tag_names,
            registry: RwLock::new(InstrumentRegistry::default()),
        }
    }

    /// Registers one series from a strict JSON document and returns its
    /// identifier. Registering an identical series again returns the first
    /// identifier. Every validation failure is a configuration error whose
    /// message names only the violated rule, never the rejected text.
    pub(crate) fn register(&self, bytes: &[u8]) -> Result<u32, Failure> {
        let spec: InstrumentSpec = serde_json::from_slice(bytes)
            .map_err(|_error| invalid("invalid SDK metric registration JSON".to_owned()))?;
        self.validate(&spec)?;
        // The key is unambiguous: names exclude both separators, and label
        // values may not contain control characters.
        let mut key = format!("{:?}\u{1}{}", spec.kind, spec.name);
        for label in &spec.labels {
            key.push('\u{1}');
            key.push_str(&label.name);
            key.push('\u{2}');
            key.push_str(&label.value);
        }
        let mut registry = self
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((kind, description)) = registry.families.get(&spec.name)
            && (*kind != spec.kind || *description != spec.description)
        {
            return Err(invalid(
                "an SDK metric name must keep one kind and one description".to_owned(),
            ));
        }
        if let Some(id) = registry.ids.get(&key) {
            return Ok(*id);
        }
        if registry.instruments.len() >= MAX_SDK_INSTRUMENTS {
            return Err(invalid(format!(
                "at most {MAX_SDK_INSTRUMENTS} SDK metric series may be registered"
            )));
        }
        let id = u32::try_from(registry.instruments.len())
            .map_err(|_error| invalid("SDK metric identifier overflow".to_owned()))?;
        let instrument = self.bind(&spec);
        registry.instruments.push(instrument);
        registry.ids.insert(key, id);
        registry
            .families
            .insert(spec.name, (spec.kind, spec.description));
        Ok(id)
    }

    /// Applies the documented bounds to one registration.
    fn validate(&self, spec: &InstrumentSpec) -> Result<(), Failure> {
        if spec.name.len() > MAX_SDK_INSTRUMENT_NAME_BYTES
            || !valid_sdk_identifier(&spec.name)
            || spec.name.starts_with("temporal_")
        {
            return Err(invalid(format!(
                "SDK metric names must match [a-z_][a-z0-9_]*, be at most \
                 {MAX_SDK_INSTRUMENT_NAME_BYTES} bytes, and omit the temporal_ prefix"
            )));
        }
        if spec.description.len() > MAX_SDK_INSTRUMENT_DESCRIPTION_BYTES
            || spec.description.chars().any(char::is_control)
        {
            return Err(invalid(format!(
                "SDK metric descriptions must be at most \
                 {MAX_SDK_INSTRUMENT_DESCRIPTION_BYTES} bytes without control characters"
            )));
        }
        if spec.labels.len() > MAX_SDK_INSTRUMENT_LABELS {
            return Err(invalid(format!(
                "an SDK metric series accepts at most {MAX_SDK_INSTRUMENT_LABELS} labels"
            )));
        }
        let mut seen = HashSet::new();
        for label in &spec.labels {
            if label.name.len() > MAX_METRICS_TAG_NAME_BYTES
                || !valid_sdk_identifier(&label.name)
                || RESERVED_SDK_LABEL_NAMES.contains(&label.name.as_str())
                || self.global_tag_names.contains(&label.name)
                || !seen.insert(label.name.as_str())
            {
                return Err(invalid(format!(
                    "SDK metric label names must match [a-z_][a-z0-9_]*, be at most \
                     {MAX_METRICS_TAG_NAME_BYTES} bytes, be unique, and not repeat a \
                     reserved label or a global tag"
                )));
            }
            if label.value.is_empty()
                || label.value.len() > MAX_METRICS_TAG_VALUE_BYTES
                || label.value.chars().any(char::is_control)
            {
                return Err(invalid(format!(
                    "SDK metric label values must be 1 to {MAX_METRICS_TAG_VALUE_BYTES} \
                     bytes without control characters"
                )));
            }
        }
        Ok(())
    }

    /// Creates the Core instrument for `spec` and binds it to its labels plus
    /// the meter's default `service_name`.
    ///
    /// Core's wrappers bind infallibly (they defer the Prometheus lookup to
    /// the first recording and cache it), so the unbound fallback below is
    /// unreachable in practice; it only keeps a future Core change from
    /// turning a metrics defect into a worker failure.
    fn bind(&self, spec: &InstrumentSpec) -> Instrument {
        let attributes = self.meter.extend_attributes(
            self.meter.get_default_attributes().clone(),
            NewAttributes::from(
                spec.labels
                    .iter()
                    .map(|label| MetricKeyValue::new(label.name.clone(), label.value.clone())),
            ),
        );
        let parameters = MetricParameters::builder()
            .name(spec.name.clone())
            .description(spec.description.clone())
            .build();
        match spec.kind {
            InstrumentKind::Counter => {
                let unbound = self.meter.counter(parameters);
                Instrument::Counter(unbound.with_attributes(&attributes).unwrap_or(unbound))
            }
            InstrumentKind::UpDownCounter => {
                let unbound = self.meter.up_down_counter(parameters);
                Instrument::UpDownCounter(unbound.with_attributes(&attributes).unwrap_or(unbound))
            }
            InstrumentKind::Gauge => {
                let unbound = self.meter.gauge(parameters);
                Instrument::Gauge(unbound.with_attributes(&attributes).unwrap_or(unbound))
            }
            InstrumentKind::Histogram => {
                let unbound = self.meter.histogram(parameters);
                Instrument::Histogram(unbound.with_attributes(&attributes).unwrap_or(unbound))
            }
            InstrumentKind::DurationHistogram => {
                let unbound = self.meter.histogram_duration(parameters);
                Instrument::DurationHistogram(
                    unbound.with_attributes(&attributes).unwrap_or(unbound),
                )
            }
        }
    }

    /// Records `value` into the series registered as `id`.
    ///
    /// A counter, gauge or histogram requires a non-negative value; an
    /// up-down counter adds it as a signed delta; a duration histogram reads
    /// it as microseconds. An unknown identifier, or a negative value for an
    /// unsigned instrument, returns `STATUS_INVALID_ARGUMENT` and records
    /// nothing. Only the read lock is taken, and nothing is allocated once
    /// the series has been recorded into for the first time.
    pub(crate) fn record(&self, id: u32, value: i64) -> Status {
        let registry = self
            .registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(instrument) = usize::try_from(id)
            .ok()
            .and_then(|index| registry.instruments.get(index))
        else {
            return STATUS_INVALID_ARGUMENT;
        };
        if let Instrument::UpDownCounter(counter) = instrument {
            counter.adds(value);
            return STATUS_OK;
        }
        let Ok(unsigned) = u64::try_from(value) else {
            return STATUS_INVALID_ARGUMENT;
        };
        match instrument {
            Instrument::Counter(counter) => counter.adds(unsigned),
            Instrument::Gauge(gauge) => gauge.records(unsigned),
            Instrument::Histogram(histogram) => histogram.records(unsigned),
            Instrument::DurationHistogram(histogram) => {
                histogram.records(Duration::from_micros(unsigned));
            }
            // Handled above, before the sign check.
            Instrument::UpDownCounter(_) => {}
        }
        STATUS_OK
    }
}

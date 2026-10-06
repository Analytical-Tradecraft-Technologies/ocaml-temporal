//! Bounded operator diagnostics for connection establishment and Temporal
//! Core's own log records (#833).
//!
//! Two independent concerns live here because both turn Rust-side error or
//! log text into bytes that leave the bridge:
//!
//! * [`describe_connect_error`] reduces a Core [`ClientConnectError`] to a
//!   closed [`ConnectionCause`] plus a bounded, sanitized chain of *local*
//!   transport error text (DNS, TCP, TLS). Server-provided gRPC status
//!   messages are never copied; only the status code name is.
//! * [`core_logger`] routes Core's `tracing` records to process stderr. Core
//!   invokes the consumer ([`CoreLogQueue`]) synchronously on whichever Tokio
//!   or caller thread emitted the record, so it never touches OCaml or
//!   performs I/O: it formats one bounded line and offers it to a bounded
//!   queue without blocking, dropping and counting the line when the queue is
//!   full. A per-runtime [`CoreLogWriter`] thread drains the queue to stderr,
//!   so a stalled stderr reader can never stall Core.
//!
//! The stderr level is selected per runtime by [`CORE_LOG_ENV`]; see
//! `docs/reference/observability.md` for the operator-facing contract.

use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;
use temporalio_client::errors::ClientConnectError;
use temporalio_client::tonic::Code;
use temporalio_common::telemetry::{CoreLog, CoreLogConsumer, Logger};

/// Environment variable selecting the most verbose Temporal Core log level
/// forwarded to stderr. Read once when each native runtime is created.
pub const CORE_LOG_ENV: &str = "OCAML_TEMPORAL_CORE_LOG";

/// Upper bound, in bytes, of the transport detail appended to a connection
/// failure message. The complete message stays well below the C result's
/// diagnostic budget and cannot grow with nested error depth.
pub const MAX_CONNECTION_DETAIL_BYTES: usize = 512;

/// Upper bound, in bytes, of one forwarded Core log line, excluding the
/// terminating newline. Fields that do not fit are dropped and the line is
/// marked as truncated.
pub const MAX_CORE_LOG_LINE_BYTES: usize = 2048;

/// Maximum number of `source()` links followed when describing an error. A
/// cyclic or pathologically deep chain therefore terminates.
const MAX_SOURCE_DEPTH: usize = 8;

/// Visible suffix added when bounded text had to be cut.
const TRUNCATED_MARKER: &str = "...[truncated]";

/// Verbosity threshold for Core records written to stderr, ordered from least
/// to most verbose so `a <= b` means `a` is at least as severe as `b`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CoreLogLevel {
    /// Only Core errors.
    Error,
    /// Errors and warnings; the default when [`CORE_LOG_ENV`] is unset.
    Warn,
    /// Adds informational lifecycle records.
    Info,
    /// Adds debug records, including per-poll detail.
    Debug,
    /// Everything Core emits.
    Trace,
}

impl CoreLogLevel {
    /// Lowercase `tracing` directive spelling used in an `EnvFilter`.
    fn directive(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

/// Parses the value of [`CORE_LOG_ENV`].
///
/// `None` (unset) and an empty or whitespace-only value select the default
/// `warn`. `off` disables forwarding and yields `Ok(None)`. Accepted level
/// names are case-insensitive. Any other value is a configuration error whose
/// message lists the accepted spellings without echoing the rejected value.
pub fn parse_core_log_level(value: Option<&str>) -> Result<Option<CoreLogLevel>, String> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(Some(CoreLogLevel::Warn));
    };
    match value.to_ascii_lowercase().as_str() {
        "off" | "none" => Ok(None),
        "error" => Ok(Some(CoreLogLevel::Error)),
        "warn" | "warning" => Ok(Some(CoreLogLevel::Warn)),
        "info" => Ok(Some(CoreLogLevel::Info)),
        "debug" => Ok(Some(CoreLogLevel::Debug)),
        "trace" => Ok(Some(CoreLogLevel::Trace)),
        _ => Err(invalid_core_log_level()),
    }
}

/// Constant configuration error for an unrecognized [`CORE_LOG_ENV`] value.
fn invalid_core_log_level() -> String {
    format!("{CORE_LOG_ENV} must be one of off, error, warn, info, debug, or trace")
}

/// Reads [`CORE_LOG_ENV`] from the process environment. A value that is not
/// valid Unicode is treated like any other unrecognized value.
pub(crate) fn core_log_level_from_env() -> Result<Option<CoreLogLevel>, String> {
    match std::env::var(CORE_LOG_ENV) {
        Ok(value) => parse_core_log_level(Some(&value)),
        Err(std::env::VarError::NotPresent) => parse_core_log_level(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(invalid_core_log_level()),
    }
}

/// Builds the `tracing` `EnvFilter` directive for `level`.
///
/// Temporal's own crates are filtered at `level`. Third-party crates in Core's
/// transport stack (tonic, hyper, h2, rustls) are capped at `warn` so that
/// `debug`/`trace` show Core's decisions without flooding stderr with
/// per-frame HTTP/2 records.
pub fn core_log_filter(level: CoreLogLevel) -> String {
    let core = level.directive();
    let other = level.min(CoreLogLevel::Warn).directive();
    format!(
        "{other},temporalio_common={core},temporalio_sdk_core={core},\
         temporalio_client={core},temporalio_sdk={core}"
    )
}

/// Core `Logger` that pushes every record admitted by [`core_log_filter`] to
/// `queue`, the producer side of one runtime's [`CoreLogWriter`].
pub(crate) fn core_logger(level: CoreLogLevel, queue: CoreLogQueue) -> Logger {
    Logger::Push {
        filter: core_log_filter(level),
        consumer: Arc::new(queue),
    }
}

/// Number of formatted Core log lines one runtime buffers between Core's
/// emitting threads and its stderr writer thread. At the 2,048-byte line
/// bound this caps the queue at roughly 2 MiB per runtime. Records arriving
/// while the queue is full are dropped and counted rather than waited for.
pub const CORE_LOG_QUEUE_CAPACITY: usize = 1024;

/// Longest time runtime close waits for the Core log writer thread to flush
/// queued lines and exit. A writer still blocked on stderr after this bound
/// is detached (see [`CoreLogWriter`]), so a stalled stderr reader can delay
/// runtime close by at most this duration.
pub const CORE_LOG_CLOSE_TIMEOUT: Duration = Duration::from_millis(500);

/// Longest time a drop count waits to be reported while no new record
/// arrives. The writer also reports pending drops before each written line
/// and once more when it shuts down.
const CORE_LOG_DROP_REPORT_INTERVAL: Duration = Duration::from_secs(1);

/// State shared by every [`CoreLogQueue`] producer and the writer thread.
struct CoreLogShared {
    /// Producer end of the bounded line queue. [`CoreLogWriter`] takes and
    /// drops it on close, which disconnects the queue so the writer exits
    /// after draining. Core may keep its subscriber, and therefore a queue
    /// clone, alive after the runtime is gone (it stays installed as the
    /// creating thread's default), so disconnecting cannot rely on Core
    /// dropping its consumer. The lock is held only for a non-blocking
    /// `try_send` or for `take`, never across I/O.
    sender: Mutex<Option<SyncSender<String>>>,
    /// Records rejected because the queue was full since the writer last
    /// reported drops.
    dropped: AtomicU64,
}

impl CoreLogShared {
    /// Locks the sender slot, recovering from poisoning: the slot is a plain
    /// `Option` that no panic can leave logically inconsistent.
    fn sender(&self) -> MutexGuard<'_, Option<SyncSender<String>>> {
        self.sender.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Non-blocking producer side of a runtime's Core log queue, installed as
/// Core's push-log consumer.
///
/// Core calls [`CoreLogConsumer::on_log`] synchronously on the emitting
/// thread, which may be a Tokio worker driving network progress or the OCaml
/// supervisor thread inside a blocking bridge call. The consumer therefore
/// only formats one bounded line and offers it to the queue with `try_send`;
/// it never performs I/O, never waits for queue space, holds no OCaml values,
/// and contains any formatting panic so it cannot unwind into Core's runtime.
#[derive(Clone)]
pub struct CoreLogQueue {
    shared: Arc<CoreLogShared>,
}

impl CoreLogQueue {
    /// Offers one already formatted line to the writer without blocking.
    ///
    /// Returns `true` when the line was queued. A full queue drops the line
    /// and counts it for the writer's next drop report; a closed queue (the
    /// runtime has been released) drops it silently. Either way the caller
    /// returns immediately.
    pub fn submit(&self, line: String) -> bool {
        let sender = self.shared.sender();
        let Some(sender) = sender.as_ref() else {
            return false;
        };
        match sender.try_send(line) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                self.shared.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
            Err(TrySendError::Disconnected(_)) => false,
        }
    }
}

/// Opaque `Debug` output; Core requires it of consumers, and the queue
/// contents are not useful diagnostic text.
impl std::fmt::Debug for CoreLogQueue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CoreLogQueue")
    }
}

impl CoreLogConsumer for CoreLogQueue {
    fn on_log(&self, log: CoreLog) {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            self.submit(format_core_log_line(
                log.level.as_str(),
                &log.target,
                &log.message,
                &log.fields,
            ))
        }));
    }
}

/// How [`CoreLogWriter::close`] ended the writer thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreLogWriterClose {
    /// The writer drained the queue, exited, and was joined.
    Joined,
    /// The writer was still blocked in its sink when the close bound expired
    /// and was detached; it exits on its own once that write returns.
    Detached,
}

/// Owner of one runtime's Core log writer thread and queue.
///
/// Ownership and lifecycle: runtime creation spawns exactly one writer per
/// runtime whose Core logging is enabled, and the native `Runtime` handle
/// owns this value. Closing the runtime moves it to the runtime cleanup
/// thread, which closes it after Core itself has been dropped so shutdown
/// records are still written. [`CoreLogWriter::close`] (also run by `Drop`,
/// for construction failure paths) is the single release path:
///
/// 1. It takes the queue sender, so the queue disconnects. Records emitted
///    afterwards, including through a subscriber Core leaves installed on the
///    creating thread, are discarded without blocking.
/// 2. The writer drains the remaining lines, reports pending drops, signals
///    completion, and returns.
/// 3. The closer waits at most the given bound for that signal. On success
///    it joins the (already finished) thread. On timeout the writer must be
///    blocked in a stderr write whose reader has stalled; waiting longer could
///    hang runtime close indefinitely, so the join handle is dropped and the
///    thread is detached. A detached writer owns only the queue receiver,
///    the shared drop counter, and its sink: no Core, Tokio, or OCaml state.
///    It finishes the remaining bounded queue once the write returns, or ends
///    with the process.
///
/// The writer thread never calls OCaml and contains panics from its sink.
pub struct CoreLogWriter {
    shared: Arc<CoreLogShared>,
    /// Completion signal sent by the writer immediately before it returns.
    done: Receiver<()>,
    /// `None` once closed.
    thread: Option<JoinHandle<()>>,
}

impl CoreLogWriter {
    /// Spawns the writer thread for the process stderr stream.
    pub(crate) fn spawn_stderr() -> std::io::Result<Self> {
        Self::spawn(Box::new(std::io::stderr()), CORE_LOG_QUEUE_CAPACITY)
    }

    /// Spawns a writer thread draining a queue of `capacity` lines (at least
    /// one) into `sink`. Write errors are ignored: a closed stderr must not
    /// affect Temporal progress. Exposed so tests can substitute a sink.
    pub fn spawn(sink: Box<dyn Write + Send>, capacity: usize) -> std::io::Result<Self> {
        let (sender, receiver) = sync_channel(capacity.max(1));
        let shared = Arc::new(CoreLogShared {
            sender: Mutex::new(Some(sender)),
            dropped: AtomicU64::new(0),
        });
        let (done_sender, done) = sync_channel(1);
        let writer_shared = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("ocaml-temporal-core-log".to_owned())
            .spawn(move || {
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    run_core_log_writer(&receiver, &writer_shared, sink)
                }));
                let _ = done_sender.send(());
            })?;
        Ok(Self {
            shared,
            done,
            thread: Some(thread),
        })
    }

    /// Returns a producer handle feeding this writer.
    pub fn queue(&self) -> CoreLogQueue {
        CoreLogQueue {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Disconnects the queue and waits at most `timeout` for the writer to
    /// flush and exit, detaching it if its sink is still blocked. See the
    /// type documentation for the complete protocol.
    pub fn close(mut self, timeout: Duration) -> CoreLogWriterClose {
        self.shutdown(timeout)
    }

    /// Idempotent implementation of [`CoreLogWriter::close`].
    fn shutdown(&mut self, timeout: Duration) -> CoreLogWriterClose {
        drop(self.shared.sender().take());
        let Some(thread) = self.thread.take() else {
            return CoreLogWriterClose::Joined;
        };
        match self.done.recv_timeout(timeout) {
            // A disconnected signal also means the thread has already exited.
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                let _ = thread.join();
                CoreLogWriterClose::Joined
            }
            Err(RecvTimeoutError::Timeout) => {
                drop(thread);
                CoreLogWriterClose::Detached
            }
        }
    }
}

/// Runs the bounded close protocol for owners that did not close explicitly,
/// such as a runtime constructor that fails after spawning the writer.
impl Drop for CoreLogWriter {
    fn drop(&mut self) {
        let _ = self.shutdown(CORE_LOG_CLOSE_TIMEOUT);
    }
}

/// Writer-thread loop: writes queued lines to `sink` until the queue is
/// disconnected and drained, reporting dropped records along the way.
fn run_core_log_writer(
    receiver: &Receiver<String>,
    shared: &CoreLogShared,
    mut sink: Box<dyn Write + Send>,
) {
    loop {
        match receiver.recv_timeout(CORE_LOG_DROP_REPORT_INTERVAL) {
            Ok(line) => {
                report_core_log_drops(shared, &mut sink);
                let _ = sink.write_all(line.as_bytes());
            }
            Err(RecvTimeoutError::Timeout) => report_core_log_drops(shared, &mut sink),
            Err(RecvTimeoutError::Disconnected) => {
                report_core_log_drops(shared, &mut sink);
                return;
            }
        }
    }
}

/// Writes one summary line for records dropped since the previous report,
/// if any. The line is emitted when the writer next has work, is idle, or
/// shuts down, so its position relative to surviving lines is approximate.
fn report_core_log_drops(shared: &CoreLogShared, sink: &mut Box<dyn Write + Send>) {
    let dropped = shared.dropped.swap(0, Ordering::Relaxed);
    if dropped > 0 {
        let _ = sink.write_all(core_log_drop_line(dropped).as_bytes());
    }
}

/// Formats the stderr line reporting `dropped` discarded Core records.
pub fn core_log_drop_line(dropped: u64) -> String {
    format!(
        "ocaml-temporal core WARN ocaml_temporal_core_bridge: {dropped} Core log records \
         dropped because the stderr writer fell behind\n"
    )
}

/// Formats one Core record as a single bounded stderr line.
///
/// The layout is `ocaml-temporal core LEVEL target: message key=value ...\n`
/// with fields sorted by key for stable output. Control characters, including
/// newlines, are escaped so a record can never forge additional log lines.
/// The text before the newline is at most [`MAX_CORE_LOG_LINE_BYTES`] bytes;
/// longer records end with a visible truncation marker.
pub fn format_core_log_line(
    level: &str,
    target: &str,
    message: &str,
    fields: &HashMap<String, serde_json::Value>,
) -> String {
    let mut line = BoundedText::new(MAX_CORE_LOG_LINE_BYTES);
    line.push("ocaml-temporal core ");
    line.push(level);
    line.push(" ");
    line.push(target);
    line.push(": ");
    line.push(message);
    let mut keys: Vec<&String> = fields.keys().collect();
    keys.sort();
    for key in keys {
        line.push(" ");
        line.push(key);
        line.push("=");
        match &fields[key] {
            serde_json::Value::String(value) => line.push(value),
            value => line.push(&value.to_string()),
        }
    }
    let mut text = line.finish();
    text.push('\n');
    text
}

/// Closed category of a failed client connection. The stable spelling from
/// [`ConnectionCause::as_str`] is part of the connection-failure message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionCause {
    /// The host name could not be resolved.
    Dns,
    /// The TCP connection was actively refused.
    Refused,
    /// The connection was reset or aborted by the peer.
    Reset,
    /// Connecting or the initial `GetSystemInfo` call timed out.
    Timeout,
    /// TLS negotiation or certificate validation failed.
    Tls,
    /// The server rejected the client's credentials.
    Unauthenticated,
    /// The server denied the authenticated client.
    PermissionDenied,
    /// The server or an intermediary reported itself unavailable.
    Unavailable,
    /// Any other transport or server failure.
    Other,
}

impl ConnectionCause {
    /// Stable lowercase spelling used in connection-failure messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dns => "dns",
            Self::Refused => "refused",
            Self::Reset => "reset",
            Self::Timeout => "timeout",
            Self::Tls => "tls",
            Self::Unauthenticated => "unauthenticated",
            Self::PermissionDenied => "permission_denied",
            Self::Unavailable => "unavailable",
            Self::Other => "other",
        }
    }
}

/// Bridge-level classification of a Core connection error.
pub(crate) enum ConnectFailure {
    /// Core rejected the connection options themselves (URI, headers, TLS
    /// settings). The message is constant because Core's text may echo
    /// configured header names or values.
    Configuration,
    /// The endpoint could not be reached or did not accept the connection.
    Connection {
        /// Closed cause category.
        cause: ConnectionCause,
        /// Bounded, sanitized local transport detail; may be empty.
        detail: String,
    },
}

/// Classifies a Core connection error without copying server-provided text.
///
/// Transport errors are described by walking their `source()` chain, whose
/// entries come from the local resolver, socket, hyper, and rustls layers.
/// A failed `GetSystemInfo` call is described by its gRPC code name and its
/// local transport sources only; `Status::message` may contain arbitrary
/// server text and is deliberately omitted.
pub(crate) fn describe_connect_error(error: &ClientConnectError) -> ConnectFailure {
    match error {
        ClientConnectError::TonicTransportError(error) => connection_failure(error, None),
        ClientConnectError::SystemInfoCallError(status) => {
            let code_cause = match status.code() {
                Code::Unauthenticated => Some(ConnectionCause::Unauthenticated),
                Code::PermissionDenied => Some(ConnectionCause::PermissionDenied),
                Code::DeadlineExceeded => Some(ConnectionCause::Timeout),
                _ => None,
            };
            let mut detail = BoundedText::new(MAX_CONNECTION_DETAIL_BYTES);
            detail.push("GetSystemInfo returned ");
            detail.push(grpc_code_name(status.code()));
            let mut cause = code_cause;
            if let Some(source) = status.source() {
                detail.push(": ");
                push_error_chain(&mut detail, source);
                cause = cause.or_else(|| classify_chain(source));
            }
            let cause = cause.unwrap_or(if status.code() == Code::Unavailable {
                ConnectionCause::Unavailable
            } else {
                ConnectionCause::Other
            });
            ConnectFailure::Connection {
                cause,
                detail: detail.finish(),
            }
        }
        // The host is application configuration, not a transport detail, so
        // only the resolver's own error chain is reported.
        ClientConnectError::DnsResolutionError { source, .. } => {
            connection_failure(source, Some(ConnectionCause::Dns))
        }
        ClientConnectError::InvalidUri(_)
        | ClientConnectError::InvalidHeaders(_)
        | ClientConnectError::InvalidConfig(_) => ConnectFailure::Configuration,
        // `ClientConnectError` is non-exhaustive; a future Core variant is
        // still a connection failure but contributes no unvetted text.
        _ => ConnectFailure::Connection {
            cause: ConnectionCause::Other,
            detail: String::new(),
        },
    }
}

/// Builds a connection failure from one local error chain, using `forced`
/// when the Core variant already determines the cause.
fn connection_failure(
    error: &(dyn Error + 'static),
    forced: Option<ConnectionCause>,
) -> ConnectFailure {
    let mut detail = BoundedText::new(MAX_CONNECTION_DETAIL_BYTES);
    push_error_chain(&mut detail, error);
    ConnectFailure::Connection {
        cause: forced
            .or_else(|| classify_chain(error))
            .unwrap_or(ConnectionCause::Other),
        detail: detail.finish(),
    }
}

/// Renders the stable connection-failure message carried by
/// `STATUS_CONNECTION`: a constant prefix, the closed cause, and, when
/// available, the bounded transport detail.
pub(crate) fn connection_failure_message(cause: ConnectionCause, detail: &str) -> String {
    if detail.is_empty() {
        format!(
            "Temporal client connection failed (cause={})",
            cause.as_str()
        )
    } else {
        format!(
            "Temporal client connection failed (cause={}): {detail}",
            cause.as_str()
        )
    }
}

/// Appends the `Display` text of `error` and its sources, separated by `: `.
/// Consecutive links whose text is already contained in the previous link are
/// skipped, because hyper and tonic frequently repeat their cause inline.
fn push_error_chain(output: &mut BoundedText, error: &(dyn Error + 'static)) {
    let mut previous = String::new();
    let mut first = true;
    for link in error_chain(error) {
        let text = link.to_string();
        if text.is_empty() || previous.contains(&text) {
            continue;
        }
        if !first {
            output.push(": ");
        }
        output.push(&text);
        first = false;
        previous = text;
    }
}

/// Iterates over `error` and at most [`MAX_SOURCE_DEPTH`] - 1 sources.
fn error_chain<'a>(
    error: &'a (dyn Error + 'static),
) -> impl Iterator<Item = &'a (dyn Error + 'static)> {
    std::iter::successors(Some(error), |error: &&'a (dyn Error + 'static)| {
        (*error).source()
    })
    .take(MAX_SOURCE_DEPTH)
}

/// Determines a cause from a local error chain.
///
/// Structured `std::io::Error` kinds are preferred. Resolver and TLS failures
/// reach the bridge as opaque hyper/rustls errors, so their well-known
/// lowercase phrases are matched as a fallback. The innermost recognizable
/// link wins, because outer links are generic wrappers such as
/// `transport error`.
pub fn classify_chain(error: &(dyn Error + 'static)) -> Option<ConnectionCause> {
    let mut cause = None;
    for link in error_chain(error) {
        if let Some(io) = link.downcast_ref::<std::io::Error>() {
            match io.kind() {
                std::io::ErrorKind::ConnectionRefused => cause = Some(ConnectionCause::Refused),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted => {
                    cause = Some(ConnectionCause::Reset)
                }
                std::io::ErrorKind::TimedOut => cause = Some(ConnectionCause::Timeout),
                _ => {}
            }
        }
        let text = link.to_string().to_ascii_lowercase();
        if text.contains("dns error")
            || text.contains("failed to lookup address")
            || text.contains("name or service not known")
            || text.contains("no such host")
        {
            cause = Some(ConnectionCause::Dns);
        } else if text.contains("certificate") || text.contains("tls") || text.contains("handshake")
        {
            cause = Some(ConnectionCause::Tls);
        } else if cause.is_none() && (text.contains("timed out") || text.contains("deadline")) {
            cause = Some(ConnectionCause::Timeout);
        } else if cause.is_none() && text.contains("connection refused") {
            cause = Some(ConnectionCause::Refused);
        }
    }
    cause
}

/// Stable snake-case gRPC code name, independent of tonic's `Debug` text.
fn grpc_code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

/// Append-only text buffer with a byte limit and control-character escaping.
///
/// Every pushed fragment is sanitized: control characters become Rust escape
/// sequences (for example `\n`), so the result is always a single line of
/// valid UTF-8. Once the limit would be exceeded, the buffer is cut on a
/// character boundary and [`TRUNCATED_MARKER`] is appended by [`finish`].
///
/// [`finish`]: BoundedText::finish
struct BoundedText {
    text: String,
    limit: usize,
    truncated: bool,
}

impl BoundedText {
    /// Creates an empty buffer whose finished text is at most `limit` bytes.
    fn new(limit: usize) -> Self {
        Self {
            text: String::new(),
            limit,
            truncated: false,
        }
    }

    /// Appends `fragment` after escaping control characters, stopping at the
    /// limit reserved for the truncation marker.
    fn push(&mut self, fragment: &str) {
        let budget = self.limit.saturating_sub(TRUNCATED_MARKER.len());
        for character in fragment.chars() {
            if self.truncated {
                return;
            }
            if character.is_control() {
                // The escape is appended as one unit so truncation never
                // leaves a dangling backslash.
                self.push_unit(&character.escape_default().to_string(), budget);
            } else {
                let mut encoded = [0; 4];
                self.push_unit(character.encode_utf8(&mut encoded), budget);
            }
        }
    }

    /// Appends one already-sanitized unit if it fits entirely in `budget`;
    /// otherwise marks the buffer truncated and drops the rest of the input.
    fn push_unit(&mut self, unit: &str, budget: usize) {
        if self.text.len() + unit.len() > budget {
            self.truncated = true;
        } else {
            self.text.push_str(unit);
        }
    }

    /// Returns the bounded text, adding the marker if anything was dropped.
    fn finish(mut self) -> String {
        if self.truncated {
            self.text.push_str(TRUNCATED_MARKER);
        }
        self.text
    }
}

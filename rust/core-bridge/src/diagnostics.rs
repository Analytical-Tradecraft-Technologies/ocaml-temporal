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
//! * [`core_logger`] routes Core's `tracing` records to process stderr through
//!   [`StderrCoreLogConsumer`]. Core invokes the consumer synchronously on
//!   whichever Tokio or caller thread emitted the record, so the consumer
//!   never touches OCaml: it formats one bounded line and performs one
//!   best-effort `write_all` on the locked stderr handle.
//!
//! The stderr level is selected per runtime by [`CORE_LOG_ENV`]; see
//! `docs/reference/observability.md` for the operator-facing contract.

use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
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
/// [`StderrCoreLogConsumer`].
pub(crate) fn core_logger(level: CoreLogLevel) -> Logger {
    Logger::Push {
        filter: core_log_filter(level),
        consumer: Arc::new(StderrCoreLogConsumer),
    }
}

/// Stateless Core log consumer writing one bounded line per record to stderr.
///
/// Core calls [`CoreLogConsumer::on_log`] synchronously on the emitting
/// thread, which may be a Tokio worker or the OCaml supervisor thread inside
/// a blocking bridge call. The consumer therefore holds no OCaml values, takes
/// only the standard library's stderr lock for one write, ignores write
/// failures (a closed stderr must not affect Temporal progress), and contains
/// any formatting panic so it cannot unwind into Core's runtime.
#[derive(Debug)]
pub(crate) struct StderrCoreLogConsumer;

impl CoreLogConsumer for StderrCoreLogConsumer {
    fn on_log(&self, log: CoreLog) {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let line =
                format_core_log_line(log.level.as_str(), &log.target, &log.message, &log.fields);
            let _ = std::io::stderr().lock().write_all(line.as_bytes());
        }));
    }
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

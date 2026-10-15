//! Private native bridge used by the public OCaml library.
//!
//! The crate is a static library so the final process remains an OCaml
//! executable which links this implementation detail.

mod abi;
pub mod activity_protocol;
mod client_calls;
mod client_protocol;
#[doc(hidden)]
pub mod diagnostics;
mod heartbeat_throttle;
pub mod protocol;
mod replay_bridge;
#[doc(hidden)]
pub mod sigpipe;
mod telemetry;
#[doc(hidden)]
pub mod worker_bridge;
pub mod workflow_protocol;

pub use abi::*;
/// Concurrent metrics scrape connections the bridge relays before answering
/// `503`; exported so the integration tests can saturate exactly the cap.
#[doc(hidden)]
pub use telemetry::MAX_CONCURRENT_SCRAPES;

/// Immutable Temporal Core revision linked by this bridge.
pub const TEMPORAL_CORE_REVISION: &str = "8fe478ee179ed754614d230f39586f3edb57da71";

/// Identifies the Core crate pinned in the locked Cargo build graph.
pub fn temporal_core_revision() -> &'static str {
    TEMPORAL_CORE_REVISION
}

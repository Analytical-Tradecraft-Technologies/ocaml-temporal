//! Private native bridge used by the public OCaml library.
//!
//! The crate is a static library so the final process remains an OCaml
//! executable which links this implementation detail.

mod abi;
pub mod activity_protocol;
mod client_protocol;
pub mod protocol;
mod replay_bridge;
#[doc(hidden)]
pub mod worker_bridge;
pub mod workflow_protocol;

pub use abi::*;

/// Immutable Temporal Core revision linked by this bridge.
pub const TEMPORAL_CORE_REVISION: &str = "95e97686a079dcfe6c42e3254b2f3f5e3d97408f";

/// Identifies the Core crate pinned in the locked Cargo build graph.
pub fn temporal_core_revision() -> &'static str {
    TEMPORAL_CORE_REVISION
}

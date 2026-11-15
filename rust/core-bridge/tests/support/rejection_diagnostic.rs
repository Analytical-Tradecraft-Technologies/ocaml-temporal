//! Best-effort stderr diagnostics written when a workflow or activity
//! task is rejected.
//!
//! Mounted from `abi.rs` with `#[path]` rather than built as an
//! integration test because the diagnostic writers are private to the
//! ABI module.

use super::{write_rejected_activity_diagnostic, write_rejected_workflow_diagnostic};
use std::io::{self, Write};

/// Models a closed stderr stream without changing process-wide descriptors.
struct BrokenStderr(usize);

impl Write for BrokenStderr {
    fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
        self.0 += 1;
        Err(io::ErrorKind::BrokenPipe.into())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A diagnostic write error must be observed without unwinding the worker.
#[test]
fn closed_stderr_does_not_panic_after_workflow_rejection() {
    let mut stderr = BrokenStderr(0);
    write_rejected_workflow_diagnostic(&mut stderr, "unsupported Core failure category");
    assert_eq!(stderr.0, 1);
}

/// The activity rejection diagnostic is equally best effort (issue #801).
#[test]
fn closed_stderr_does_not_panic_after_activity_rejection() {
    let mut stderr = BrokenStderr(0);
    write_rejected_activity_diagnostic(&mut stderr, "activity task header key is empty");
    assert_eq!(stderr.0, 1);
}

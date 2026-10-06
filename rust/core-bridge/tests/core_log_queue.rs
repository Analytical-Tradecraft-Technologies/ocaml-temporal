//! Core log forwarding never blocks Core and never blocks runtime close.
//!
//! Core invokes the bridge's log consumer synchronously on its Tokio and
//! supervisor threads, so the consumer only enqueues into a bounded queue and
//! a dedicated writer thread performs the stderr I/O. These tests substitute a
//! sink whose writes block until the test opens a gate, which models a stderr
//! pipe whose reader has stalled.

use std::io::Write;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::diagnostics::{
    CORE_LOG_CLOSE_TIMEOUT, CoreLogWriter, CoreLogWriterClose, core_log_drop_line,
};
use ocaml_temporal_core_bridge::{
    STATUS_OK, ocaml_temporal_core_v2_runtime_free, test_runtime_new_with_core_log_sink,
};

/// Generous bound for operations that must not block; far below "forever"
/// yet tolerant of a loaded CI machine.
const PROMPT: Duration = Duration::from_secs(10);

/// Observable state of a [`GateSink`].
#[derive(Default)]
struct GateState {
    /// Whether writes may complete.
    open: bool,
    /// Number of `write` calls that have started, including blocked ones.
    started: usize,
    /// Bytes written after the gate opened.
    output: Vec<u8>,
}

/// Test sink whose writes block until [`GateSink::open`] is called.
#[derive(Clone, Default)]
struct GateSink {
    state: Arc<(Mutex<GateState>, Condvar)>,
}

impl GateSink {
    /// Waits until at least `count` writes have started, failing the test if
    /// that does not happen within [`PROMPT`].
    fn wait_for_writes(&self, count: usize) {
        let (lock, changed) = &*self.state;
        let state = lock.lock().unwrap();
        let (state, timeout) = changed
            .wait_timeout_while(state, PROMPT, |state| state.started < count)
            .unwrap();
        assert!(
            !timeout.timed_out(),
            "only {} writes started",
            state.started
        );
    }

    /// Lets blocked and future writes complete.
    fn open(&self) {
        let (lock, changed) = &*self.state;
        lock.lock().unwrap().open = true;
        changed.notify_all();
    }

    /// Returns everything written so far as UTF-8 text.
    fn output(&self) -> String {
        String::from_utf8(self.state.0.lock().unwrap().output.clone()).unwrap()
    }
}

impl Write for GateSink {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.started += 1;
        changed.notify_all();
        let mut state = changed.wait_while(state, |state| !state.open).unwrap();
        state.output.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// With the writer blocked and the queue full, producers return immediately
/// and every surplus record is rejected instead of waited for. Closing then
/// detaches the stuck writer within its bound.
#[test]
fn submit_never_blocks_when_queue_is_full() {
    let sink = GateSink::default();
    let writer = CoreLogWriter::spawn(Box::new(sink.clone()), 4).unwrap();
    let queue = writer.queue();
    assert!(queue.submit("first\n".to_owned()));
    sink.wait_for_writes(1);

    let started = Instant::now();
    let accepted = (0..10_000)
        .filter(|index| queue.submit(format!("line {index}\n")))
        .count();
    assert!(started.elapsed() < PROMPT, "{:?}", started.elapsed());
    assert_eq!(accepted, 4);

    let started = Instant::now();
    assert_eq!(
        writer.close(Duration::from_millis(50)),
        CoreLogWriterClose::Detached
    );
    assert!(started.elapsed() < PROMPT, "{:?}", started.elapsed());
    // The queue is disconnected, so late records are discarded immediately.
    assert!(!queue.submit("late\n".to_owned()));
    // Release the detached writer so it can drain and exit.
    sink.open();
}

/// Dropped records are counted and reported in one summary line once the
/// writer catches up; queued records survive, and close joins the writer.
#[test]
fn dropped_records_are_reported_once_writer_catches_up() {
    let sink = GateSink::default();
    let writer = CoreLogWriter::spawn(Box::new(sink.clone()), 2).unwrap();
    let queue = writer.queue();
    assert!(queue.submit("first\n".to_owned()));
    sink.wait_for_writes(1);

    let accepted = (0..10)
        .filter(|index| queue.submit(format!("line {index}\n")))
        .count();
    assert_eq!(accepted, 2);

    sink.open();
    assert_eq!(writer.close(PROMPT), CoreLogWriterClose::Joined);
    let expected = format!("first\n{}line 0\nline 1\n", core_log_drop_line(8));
    assert_eq!(sink.output(), expected);
    assert!(expected.contains("8 Core log records dropped"));
    assert!(!queue.submit("late\n".to_owned()));
}

/// Runtime close completes, within the documented writer bound, even though
/// the runtime's Core log writer is blocked in its sink indefinitely.
#[test]
fn runtime_close_completes_while_log_writer_is_blocked() {
    let sink = GateSink::default();
    let (mut runtime, queue) =
        test_runtime_new_with_core_log_sink(Box::new(sink.clone()), 4).unwrap();
    assert!(queue.submit("first\n".to_owned()));
    sink.wait_for_writes(1);
    for index in 0..16 {
        let _ = queue.submit(format!("line {index}\n"));
    }

    let started = Instant::now();
    // SAFETY: The runtime slot holds the uniquely owned handle created above.
    let status = unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) };
    let elapsed = started.elapsed();
    assert_eq!(status, STATUS_OK);
    assert!(runtime.is_null());
    assert!(elapsed < CORE_LOG_CLOSE_TIMEOUT + PROMPT, "{elapsed:?}");
    assert!(!queue.submit("late\n".to_owned()));
    // Release the detached writer; it holds no runtime state.
    sink.open();
}

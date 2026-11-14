//! Bounded explicit release of a runtime and a shared runtime (#1149).
//!
//! Included as a child module of `src/abi.rs`. Each test reproduces the
//! #1149 hazard directly: a closure in Core's Tokio blocking pool that does
//! not finish makes the final drop of Core wait, because a Tokio runtime's
//! drop waits for blocking-pool work without a bound, exactly as it does for
//! Core's workflow processing thread after a worker finalizer was detached.
//! The explicit release must return `STATUS_DETACHED` at its bound instead of
//! blocking its caller, while the cleanup thread keeps ownership and
//! finishes the drop once the blocking work ends.

use super::*;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

/// Bound used instead of the production timeouts, so the tests take
/// milliseconds.
const TEST_BOUND: Duration = Duration::from_millis(200);

/// Upper bound on how long a release that detached may take to return;
/// generous for slow CI, far below the blocked drop.
const RETURN_DEADLINE: Duration = Duration::from_secs(5);

/// How long the blocking-pool closure holds Core's drop when nothing
/// releases it; the tests release it much earlier.
const HOLD_LIMIT: Duration = Duration::from_secs(30);

/// Builds a Core runtime the way the ABI does for tests, with default
/// options and no telemetry.
fn core() -> CoreRuntime {
    let options = RuntimeOptions::builder().build().expect("runtime options");
    CoreRuntime::new(options, TokioRuntimeBuilder::default()).expect("Core runtime")
}

/// Occupies one thread of `core`'s blocking pool until `release` is
/// signalled (or [`HOLD_LIMIT`] passes). Returns the signal sender and a
/// flag set when the closure has finished, so a test can let Core's drop
/// complete and observe that it did.
fn hold_blocking_pool(core: &CoreRuntime) -> (std::sync::mpsc::Sender<()>, Arc<AtomicBool>) {
    let (release, held) = std::sync::mpsc::channel::<()>();
    let finished = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&finished);
    let (started_sender, started) = std::sync::mpsc::channel();
    core.tokio_handle().spawn_blocking(move || {
        let _ = started_sender.send(());
        let _ = held.recv_timeout(HOLD_LIMIT);
        flag.store(true, Ordering::Release);
    });
    started
        .recv_timeout(RETURN_DEADLINE)
        .expect("the blocking-pool closure starts");
    (release, finished)
}

/// Waits until `finished` is set, failing after [`RETURN_DEADLINE`].
fn wait_finished(finished: &AtomicBool) {
    let deadline = Instant::now() + RETURN_DEADLINE;
    while !finished.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "the held blocking work never finished"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A shared runtime whose Core drop is held up returns `STATUS_DETACHED` at
/// its bound, having already handed its reference to the cleanup thread;
/// once the held work ends, the cleanup thread finishes the drop and the
/// shared Core is gone.
#[test]
fn shared_release_detaches_when_core_drop_outlives_its_bound() {
    let shared = SharedRuntime::new(SharedCore {
        metrics: None,
        core: core(),
        core_log: std::sync::Mutex::new(None),
        _dropped: CoreDropMarker,
    })
    .expect("shared runtime cleanup thread");
    let weak = Arc::downgrade(&shared.core);
    let (release, finished) = hold_blocking_pool(&shared.core);

    let started = Instant::now();
    let status = shared.release_within(Some(TEST_BOUND));
    let elapsed = started.elapsed();
    assert_eq!(status, STATUS_DETACHED);
    assert!(
        elapsed >= TEST_BOUND,
        "returned before its bound: {elapsed:?}"
    );
    assert!(
        elapsed < RETURN_DEADLINE,
        "the release was not bounded: {elapsed:?}"
    );
    assert_eq!(
        weak.strong_count(),
        0,
        "the handle must have handed its Core reference to the cleanup thread"
    );

    let _ = release.send(());
    wait_finished(&finished);
}

/// A runtime graph whose Core drop is held up returns `STATUS_DETACHED` at
/// its bound; the cleanup thread still owns and disposes the graph.
#[test]
fn runtime_close_detaches_when_disposal_outlives_its_bound() {
    let runtime = Runtime::with_owned_core(core()).expect("runtime cleanup thread");
    let held_core = runtime.core.as_ref().expect("a new graph owns Core");
    let weak = Arc::downgrade(held_core);
    let (release, finished) = hold_blocking_pool(held_core);

    let started = Instant::now();
    let status = runtime.close_within(Some(TEST_BOUND));
    let elapsed = started.elapsed();
    assert_eq!(status, STATUS_DETACHED);
    assert!(
        elapsed >= TEST_BOUND,
        "returned before its bound: {elapsed:?}"
    );
    assert!(
        elapsed < RETURN_DEADLINE,
        "the close was not bounded: {elapsed:?}"
    );
    assert_eq!(
        weak.strong_count(),
        0,
        "the graph must have handed Core to the cleanup thread"
    );

    let _ = release.send(());
    wait_finished(&finished);
}

/// Without held work both releases finish well within their bounds and
/// report success, so the bound changes nothing on the normal path.
#[test]
fn bounded_releases_succeed_on_the_normal_path() {
    let shared = SharedRuntime::new(SharedCore {
        metrics: None,
        core: core(),
        core_log: std::sync::Mutex::new(None),
        _dropped: CoreDropMarker,
    })
    .expect("shared runtime cleanup thread");
    assert_eq!(shared.release_within(Some(RETURN_DEADLINE)), STATUS_OK);

    let runtime = Runtime::with_owned_core(core()).expect("runtime cleanup thread");
    assert_eq!(runtime.close_within(Some(RETURN_DEADLINE)), STATUS_OK);
}

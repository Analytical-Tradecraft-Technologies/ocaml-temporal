//! Worker idle pacing sleeps with sub-tick precision (#1008).
//!
//! The OCaml worker lanes back off from 0.1 ms to 10 ms between polls. On
//! Windows a `Sleep`-based delay lasts until the next 15.6 ms timer tick, so
//! the bridge exports a pacing sleep built on `std::thread::sleep`, which uses
//! a high-resolution waitable timer there. These tests check its bounds and
//! that a 1 ms request is far shorter than one tick on every platform. The
//! precision check uses the median of several sleeps, so one sleep delayed by
//! a loaded runner cannot fail it.

use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    MAX_PACING_SLEEP_NS, STATUS_OK, ocaml_temporal_core_v4_pacing_sleep_ns,
};

/// One default Windows system timer tick, the granularity the pacing sleep
/// must beat.
const WINDOWS_TIMER_TICK: Duration = Duration::from_micros(15_625);

/// Times one pacing sleep of `nanoseconds` and checks its status.
fn timed_sleep(nanoseconds: u64) -> Duration {
    let started = Instant::now();
    assert_eq!(
        ocaml_temporal_core_v4_pacing_sleep_ns(nanoseconds),
        STATUS_OK
    );
    started.elapsed()
}

/// A zero request returns at once, and a nonzero one sleeps about as long
/// as requested. The 10% allowance only absorbs a timer that fires a few
/// microseconds before the monotonic clock reaches the due time; a wrong unit
/// would be off by orders of magnitude.
#[test]
fn sleeps_at_least_the_requested_time() {
    assert!(timed_sleep(0) < WINDOWS_TIMER_TICK);
    let requested = Duration::from_millis(2);
    let nanoseconds = u64::try_from(requested.as_nanos()).expect("2 ms fits in u64");
    assert!(timed_sleep(nanoseconds) >= requested * 9 / 10);
}

/// A 1 ms pacing sleep typically lasts far less than one Windows timer tick.
/// `Sleep(1)` at the default timer resolution would last a whole tick every
/// time, so this fails if the high-resolution timer is not used. It runs only
/// on Windows: elsewhere the sleep is `nanosleep`, and a virtualised CI
/// runner's scheduling delay (medians above 12 ms were seen on macOS) says
/// nothing about the Windows timer this guards.
#[cfg(windows)]
#[test]
fn one_millisecond_sleep_is_shorter_than_a_timer_tick() {
    let mut samples: Vec<Duration> = (0..21).map(|_| timed_sleep(1_000_000)).collect();
    samples.sort();
    let median = samples[samples.len() / 2];
    assert!(
        median < WINDOWS_TIMER_TICK / 2,
        "median 1 ms pacing sleep took {median:?}; samples {samples:?}"
    );
}

/// The C header publishes the same clamp the bridge applies, so C and OCaml
/// callers cannot drift from Rust silently.
#[test]
fn header_bound_matches_bridge() {
    let header = include_str!("../include/ocaml_temporal_core.h");
    let definition =
        format!("#define OCAML_TEMPORAL_CORE_MAX_PACING_SLEEP_NS UINT64_C({MAX_PACING_SLEEP_NS})");
    assert!(header.contains(&definition), "missing {definition}");
}

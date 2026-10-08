//! Seeded operation-sequence stress for the native lifecycle (issue #522).
//!
//! Each generated case is a reproducible sequence of abstract operations over
//! two runtime slots: runtime create/free/GC-fallback dispose, replay-worker
//! start/feed/poll/complete/reject/finalize/dispose, live client
//! connect/disconnect, live activity-worker start/poll/complete/reject/
//! shutdown, stale and repeated completions, null-handle calls after release,
//! and a panic-containment probe. Every operation is valid input in every
//! model state: an operation that does not apply to the current state is
//! still executed and must fail with the status the ABI documents for it.
//! Calls that the ABI contract forbids (overwriting a live runtime slot,
//! dangling pointers) are never made; after release a slot holds the null
//! value the ABI wrote into it, and every later call uses that value.
//!
//! An explicit model and two ledgers (runtime cleanup counters and the gRPC
//! double's completions) check, after every operation and again after
//! teardown, that:
//!
//! - every runtime handle is created once and cleaned up exactly once
//!   (process-local creation and cleanup counters never exceed the model's
//!   releases and converge on them after teardown);
//! - a released slot is null and every use of it is rejected with
//!   `STATUS_INVALID_ARGUMENT`;
//! - a lease (replay activation or activity task) is delivered once, a
//!   second completion or rejection of a retired lease is refused, worker
//!   shutdown reports abandoned leases with `STATUS_OUTSTANDING_TASKS`, and
//!   the gRPC double observes at most one completion for any task token and
//!   exactly one for every token the bridge leased, whichever path (explicit
//!   completion, rejection, shutdown, free, or GC-fallback dispose) retired
//!   it;
//! - a replay finalizes only after its input is closed and every fed history
//!   was evicted, and a drained replay always finalizes;
//! - every result follows the single-owner buffer contract and can be freed
//!   twice, and only the panic probe reports `STATUS_PANIC`.
//!
//! No Temporal server is used. Replay workers need none, and live workers
//! connect to a minimal plaintext HTTP/2 gRPC double (as in
//! `worker_shutdown_outstanding.rs`) that hands one uniquely tokenized
//! activity task to every activity poll and records every completion.
//!
//! The operation sequence is a pure function of the seed and case number.
//! Native scheduling is not: whether a poll finds a task depends on Core's
//! timing, so the model accepts every documented outcome of a racy call and
//! updates itself from the observed one. A failing case is minimized by
//! deleting operations while the failure persists, then reported with its
//! seed, case number, reproduction command, and minimized trace. See
//! `docs/reference/bridge-lifecycle-stress.md` for budgets and tool scope.
//!
//! To add an operation (for example the shared-runtime symbols), add an [`Op`]
//! variant, map it to its slot in [`Op::slot`], give it a weight in
//! [`OP_TABLE`] (and, if it advances a state, a place in
//! [`GenSlot::progress`]), describe its documented statuses and model
//! transition in [`Case::execute`], and extend [`SlotModel`] with any new
//! owned state. A handle shared between slots would also extend the cleanup
//! ledger in [`Case::check_ledgers`] to count the shared owner once.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::fs;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ocaml_temporal_core_bridge::{
    Result as AbiResult, Runtime, STATUS_ABI_MISMATCH, STATUS_ALREADY_STARTED,
    STATUS_ASYNC_HEARTBEAT_REJECTED, STATUS_CONFIGURATION, STATUS_CONNECTION, STATUS_INTERNAL,
    STATUS_INVALID_ARGUMENT, STATUS_INVALID_STATE, STATUS_NOT_READY, STATUS_OK,
    STATUS_OUTSTANDING_TASKS, STATUS_PANIC, STATUS_PROTOCOL, STATUS_RESOURCE_EXHAUSTED,
    STATUS_RETRYABLE, STATUS_WORKER, Status,
    activity_protocol::{self, ActivityCompletion, ActivityCompletionResult},
    ocaml_temporal_core_v4_client_connect_json, ocaml_temporal_core_v4_client_disconnect,
    ocaml_temporal_core_v4_replay_worker_complete_workflow_json,
    ocaml_temporal_core_v4_replay_worker_dispose,
    ocaml_temporal_core_v4_replay_worker_feed_history_json,
    ocaml_temporal_core_v4_replay_worker_finalize,
    ocaml_temporal_core_v4_replay_worker_finish_input,
    ocaml_temporal_core_v4_replay_worker_reject_workflow_json,
    ocaml_temporal_core_v4_replay_worker_start_json,
    ocaml_temporal_core_v4_replay_worker_try_poll_workflow,
    ocaml_temporal_core_v4_replay_worker_wait_workflow, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_dispose, ocaml_temporal_core_v4_runtime_free,
    ocaml_temporal_core_v4_runtime_new, ocaml_temporal_core_v4_worker_complete_activity_json,
    ocaml_temporal_core_v4_worker_reject_activity_json, ocaml_temporal_core_v4_worker_shutdown,
    ocaml_temporal_core_v4_worker_start_json, ocaml_temporal_core_v4_worker_try_poll_activity,
    ocaml_temporal_core_v4_worker_wait_activity, test_invoke_panic, test_runtime_cleanup_counts,
    workflow_protocol::{self, ActivationJob, Completion, CompletionCommand, Payload},
};
use prost::Message;
use prost::bytes::{Bytes, BytesMut};
use temporalio_common::protos::temporal::api::{
    common::v1::{ActivityType, WorkflowExecution, WorkflowType},
    history::v1::{History, history_event::Attributes},
    workflowservice::v1::{
        PollActivityTaskQueueResponse, RespondActivityTaskCanceledRequest,
        RespondActivityTaskCompletedRequest, RespondActivityTaskFailedRequest,
    },
};

#[path = "support/replay_fixture.rs"]
mod replay_fixture;

/// Default generated cases for the ordinary test suite: a few seconds on a
/// CI runner. Longer runs raise `LIFECYCLE_STRESS_CASES`.
const DEFAULT_CASES: u64 = 24;
/// Default operations per generated case.
const DEFAULT_STEPS: usize = 48;
/// Largest accepted case count, so a typo cannot start an unbounded run.
const MAX_CASES: u64 = 1_000_000;
/// Largest accepted sequence length per case.
const MAX_STEPS: usize = 2_000;
/// Stable default seed; changing it requires recording the new value.
const DEFAULT_SEED: u64 = 0x0522_2026;
/// Number of independent runtime slots each case interleaves.
const SLOTS: usize = 2;
/// Upper bound for one native call. Every documented call returns in well
/// under a second here; the bound only turns a hang into a reported failure.
const CALL_DEADLINE: Duration = Duration::from_secs(60);
/// Upper bound for asynchronous cleanup and server-side completion evidence
/// to converge after a case releases its runtimes.
const SETTLE_DEADLINE: Duration = Duration::from_secs(30);
/// Wait/poll rounds a replay drain may take before its liveness check fails.
const DRAIN_ROUNDS: usize = 200;
/// Maximum candidate executions spent minimizing one failing case.
const MAX_SHRINK_RUNS: usize = 256;
/// Replay histories that may be fed but not yet evicted before another feed.
/// The feeder holds one history; feeding while two are unretired could block
/// on Core's backpressure until a lease the model still holds is completed,
/// which is correct native behavior rather than a lifecycle defect.
const MAX_UNRETIRED_HISTORIES: usize = 1;

/// Replay worker settings shared with `replay_abi.rs`: no cache, so every
/// history ends with an eviction, and one outstanding workflow task.
const REPLAY_WORKER: &[u8] = br#"{"namespace":"default","task_queue":"replay","build_id":"lifecycle-stress","versioning":{"kind":"none"},"max_cached_workflows":0,"max_outstanding_workflow_tasks":1,"max_concurrent_workflow_task_polls":1,"graceful_shutdown_timeout_ms":1000}"#;

/// Activity-only live worker settings. The bridge enables one activity slot,
/// so Core leases at most one task per worker at a time.
const ACTIVITY_WORKER: &[u8] = br#"{"namespace":"lifecycle-stress","task_queue":"lifecycle-stress","build_id":"lifecycle-stress","versioning":{"kind":"none"},"max_cached_workflows":10,"max_outstanding_workflow_tasks":10,"max_concurrent_workflow_task_polls":2,"graceful_shutdown_timeout_ms":1000,"task_types":{"workflows":false,"activities":true}}"#;

/// Client document for an endpoint that refuses connections.
const REFUSED_CLIENT: &[u8] =
    br#"{"target_url":"http://127.0.0.1:1","identity":"lifecycle-stress"}"#;

/// Serializes the tests in this binary: the runtime cleanup counters are
/// process-wide, so concurrent cases would make the ledger ambiguous.
static SERIAL: Mutex<()> = Mutex::new(());

/// Process-wide counter that makes every fed replay history unique.
static NEXT_HISTORY: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Configuration and deterministic randomness
// ---------------------------------------------------------------------------

/// Run settings read once from the environment.
#[derive(Clone, Copy)]
struct Config {
    /// Base seed for every generated case.
    seed: u64,
    /// Number of generated cases.
    cases: u64,
    /// Operations generated per case.
    steps: usize,
    /// When set, only this case number runs (reproduction mode).
    only_case: Option<u64>,
}

/// Parses a decimal or `0x` hexadecimal unsigned integer setting.
fn parse_u64(name: &str, text: &str) -> u64 {
    let parsed = match text.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => text.parse(),
    };
    parsed.unwrap_or_else(|_| panic!("{name} must be a decimal or 0x-hexadecimal integer"))
}

/// Reads one optional integer environment setting.
fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .filter(|text| !text.is_empty())
        .map(|text| parse_u64(name, &text))
}

impl Config {
    /// Reads `LIFECYCLE_STRESS_{SEED,CASES,STEPS,CASE}` and rejects budgets
    /// that would make the run unbounded or empty.
    fn from_env() -> Self {
        let seed = env_u64("LIFECYCLE_STRESS_SEED").unwrap_or(DEFAULT_SEED);
        let cases = env_u64("LIFECYCLE_STRESS_CASES").unwrap_or(DEFAULT_CASES);
        assert!(
            (1..=MAX_CASES).contains(&cases),
            "LIFECYCLE_STRESS_CASES must be 1..={MAX_CASES}"
        );
        let steps = env_u64("LIFECYCLE_STRESS_STEPS").map_or(DEFAULT_STEPS, |steps| {
            usize::try_from(steps).expect("LIFECYCLE_STRESS_STEPS fits usize")
        });
        assert!(
            (1..=MAX_STEPS).contains(&steps),
            "LIFECYCLE_STRESS_STEPS must be 1..={MAX_STEPS}"
        );
        let only_case = env_u64("LIFECYCLE_STRESS_CASE");
        Self {
            seed,
            cases,
            steps,
            only_case,
        }
    }
}

/// SplitMix64: a tiny dependency-free generator with no absorbing state, so
/// every 64-bit seed (including zero) yields a full-period stream.
struct Rng(u64);

impl Rng {
    /// Derives an independent stream for one case, so a single case can be
    /// regenerated without generating the cases before it.
    fn for_case(seed: u64, case: u64) -> Self {
        let mut mixer = Rng(seed ^ case.wrapping_mul(0xD1B5_4A32_D192_ED03));
        Rng(mixer.next())
    }

    /// Returns the next 64 pseudo-random bits.
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    /// Returns a value in `0..bound`; `bound` must be non-zero. The modulo
    /// bias is irrelevant for operation selection.
    fn below(&mut self, bound: usize) -> usize {
        let bound = u64::try_from(bound).expect("bound fits u64");
        usize::try_from(self.next() % bound).expect("value below a usize bound fits usize")
    }

    /// Returns a small lease selector; the executor reduces it modulo the
    /// number of leases held when the operation runs.
    fn pick(&mut self) -> u8 {
        u8::try_from(self.next() & 0xff).expect("masked byte fits u8")
    }
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// Replay history shape fed to Core.
#[derive(Clone, Copy, Debug)]
enum HistoryKind {
    /// Terminal history: one workflow task, then workflow completion.
    Complete,
    /// History whose first workflow task is still open.
    Open,
}

/// One abstract lifecycle operation. `usize` fields select a runtime slot;
/// `u8` fields select a held lease modulo the number held at run time.
///
/// The derived `Debug` form of a `Vec<Op>` is valid Rust with `use Op::*`,
/// so a minimized trace can be pasted into [`scripted_regressions`].
#[derive(Clone, Copy, Debug)]
enum Op {
    /// `runtime_new` into the slot (skipped while the slot is live, because
    /// overwriting a live slot violates the ABI contract).
    RuntimeNew(usize),
    /// Explicit `runtime_free`; a released slot exercises double free.
    RuntimeFree(usize),
    /// GC-fallback `runtime_dispose`; a released slot exercises double close.
    RuntimeDispose(usize),
    /// `runtime_free` and `runtime_dispose` with a null slot pointer.
    RuntimeNullSlot,
    /// Synthetic panic inside the shared ABI wrapper.
    PanicProbe,
    /// Start the replay worker.
    ReplayStart(usize),
    /// Feed one uniquely identified replay history.
    ReplayFeed(usize, HistoryKind),
    /// Feed an invalid replay history document.
    ReplayFeedMalformed(usize),
    /// Close the replay history feeder.
    ReplayFinish(usize),
    /// Bounded replay readiness wait.
    ReplayWait(usize),
    /// Non-blocking replay poll; success leases one activation.
    ReplayPoll(usize),
    /// Complete a held replay lease (or a never-leased run without one).
    ReplayComplete(usize, u8),
    /// Re-send a completion whose lease was already retired.
    ReplayCompleteStale(usize),
    /// Reject a held replay lease (or an unleased document without one).
    ReplayReject(usize, u8),
    /// Re-send a rejection whose lease was already retired.
    ReplayRejectStale(usize),
    /// Attempt natural replay finalization.
    ReplayFinalize(usize),
    /// Close input, complete every activation, and require finalization.
    ReplayDrain(usize),
    /// Explicitly abandon the replay worker.
    ReplayDispose(usize),
    /// Connect the client to the gRPC double.
    Connect(usize),
    /// Connect to a refusing endpoint; must roll back.
    ConnectRefused(usize),
    /// Start the live activity worker.
    WorkerStart(usize),
    /// Bounded activity readiness wait.
    ActivityWait(usize),
    /// Non-blocking activity poll; success leases one task.
    ActivityPoll(usize),
    /// Complete a held activity lease (or an unleased token without one).
    ActivityComplete(usize, u8),
    /// Re-send a completion whose lease was already retired.
    ActivityCompleteStale(usize),
    /// Reject a held activity lease (or an unleased document without one).
    ActivityReject(usize, u8),
    /// Re-send a rejection whose lease was already retired.
    ActivityRejectStale(usize),
    /// Shut the live worker down, force-completing abandoned leases.
    WorkerShutdown(usize),
    /// Disconnect the client.
    ClientDisconnect(usize),
}

use HistoryKind::{Complete, Open};
use Op::*;

impl Op {
    /// Returns the runtime slot the operation targets, if any.
    fn slot(&self) -> Option<usize> {
        match *self {
            RuntimeNullSlot | PanicProbe => None,
            RuntimeNew(slot)
            | RuntimeFree(slot)
            | RuntimeDispose(slot)
            | ReplayStart(slot)
            | ReplayFeed(slot, _)
            | ReplayFeedMalformed(slot)
            | ReplayFinish(slot)
            | ReplayWait(slot)
            | ReplayPoll(slot)
            | ReplayComplete(slot, _)
            | ReplayCompleteStale(slot)
            | ReplayReject(slot, _)
            | ReplayRejectStale(slot)
            | ReplayFinalize(slot)
            | ReplayDrain(slot)
            | ReplayDispose(slot)
            | Connect(slot)
            | ConnectRefused(slot)
            | WorkerStart(slot)
            | ActivityWait(slot)
            | ActivityPoll(slot)
            | ActivityComplete(slot, _)
            | ActivityCompleteStale(slot)
            | ActivityReject(slot, _)
            | ActivityRejectStale(slot)
            | WorkerShutdown(slot)
            | ClientDisconnect(slot) => Some(slot),
        }
    }

    /// Returns the variant name, used as the coverage key.
    fn name(&self) -> String {
        let text = format!("{self:?}");
        match text.split_once('(') {
            Some((name, _)) => name.to_owned(),
            None => text,
        }
    }
}

/// Observed `(operation, outcome)` counts. Each [`run_ops`] call merges its
/// case's counts into a collector owned by the caller, never into process
/// state: the default generated run asserts that the interesting outcomes in
/// [`REQUIRED_COVERAGE`] were reached, so a generator change cannot silently
/// degrade the stress into misuse-only calls, and hand-written regressions or
/// minimization reruns must not be able to satisfy that assertion for it.
type Coverage = BTreeMap<(String, String), usize>;

/// Outcomes the default seed and budget must reach at least once.
const REQUIRED_COVERAGE: &[(&str, &str)] = &[
    ("ReplayPoll", "OK"),
    ("ReplayComplete", "OK"),
    ("ReplayReject", "OK"),
    ("ReplayFinalize", "OK"),
    ("ReplayFinalize", "OUTSTANDING_TASKS"),
    ("ReplayCompleteStale", "PROTOCOL"),
    ("ReplayDispose", "OK"),
    ("ReplayDrain", "drained and finalized"),
    ("ActivityPoll", "OK"),
    ("ActivityComplete", "OK"),
    ("ActivityReject", "OK"),
    ("ActivityCompleteStale", "WORKER"),
    ("WorkerShutdown", "OUTSTANDING_TASKS"),
    ("ClientDisconnect", "INVALID_STATE"),
    ("RuntimeFree", "released with a held lease"),
    ("RuntimeDispose", "released with a held lease"),
    ("WorkerStart", "INVALID_ARGUMENT"),
];

/// Counts one observed outcome in `coverage`.
fn record_coverage(coverage: &mut Coverage, name: String, detail: &str) {
    *coverage.entry((name, detail.to_owned())).or_default() += 1;
}

/// Adds every count in `from` to `into`.
fn merge_coverage(into: &mut Coverage, from: Coverage) {
    for (key, count) in from {
        *into.entry(key).or_default() += count;
    }
}

/// Builds one operation, drawing its slot and lease selector from the stream.
type MakeOp = fn(&mut Rng) -> Op;

/// Operation constructors with relative weights for the "any operation"
/// share of generation. Drawing from the whole table keeps misuse (calls in
/// the wrong state, after release, or on stale leases) interleaved with the
/// state-directed progress operations of [`GenSlot::progress`].
const OP_TABLE: &[(usize, MakeOp)] = &[
    (9, |rng| RuntimeNew(rng.below(SLOTS))),
    (2, |rng| RuntimeFree(rng.below(SLOTS))),
    (2, |rng| RuntimeDispose(rng.below(SLOTS))),
    (1, |_| RuntimeNullSlot),
    (1, |_| PanicProbe),
    (5, |rng| ReplayStart(rng.below(SLOTS))),
    (4, |rng| ReplayFeed(rng.below(SLOTS), Complete)),
    (2, |rng| ReplayFeed(rng.below(SLOTS), Open)),
    (1, |rng| ReplayFeedMalformed(rng.below(SLOTS))),
    (2, |rng| ReplayFinish(rng.below(SLOTS))),
    (2, |rng| ReplayWait(rng.below(SLOTS))),
    (6, |rng| ReplayPoll(rng.below(SLOTS))),
    (5, |rng| ReplayComplete(rng.below(SLOTS), rng.pick())),
    (2, |rng| ReplayCompleteStale(rng.below(SLOTS))),
    (2, |rng| ReplayReject(rng.below(SLOTS), rng.pick())),
    (1, |rng| ReplayRejectStale(rng.below(SLOTS))),
    (2, |rng| ReplayFinalize(rng.below(SLOTS))),
    (2, |rng| ReplayDrain(rng.below(SLOTS))),
    (2, |rng| ReplayDispose(rng.below(SLOTS))),
    (5, |rng| Connect(rng.below(SLOTS))),
    (1, |rng| ConnectRefused(rng.below(SLOTS))),
    (5, |rng| WorkerStart(rng.below(SLOTS))),
    (2, |rng| ActivityWait(rng.below(SLOTS))),
    (6, |rng| ActivityPoll(rng.below(SLOTS))),
    (4, |rng| ActivityComplete(rng.below(SLOTS), rng.pick())),
    (2, |rng| ActivityCompleteStale(rng.below(SLOTS))),
    (2, |rng| ActivityReject(rng.below(SLOTS), rng.pick())),
    (1, |rng| ActivityRejectStale(rng.below(SLOTS))),
    (3, |rng| WorkerShutdown(rng.below(SLOTS))),
    (2, |rng| ClientDisconnect(rng.below(SLOTS))),
];

/// Percentage of generated operations drawn from [`OP_TABLE`] rather than
/// directed by the generator's abstract state.
const ANY_OPERATION_PERCENT: usize = 25;

/// The generator's own abstract view of one slot. It evolves only from the
/// generated operations (never from native outcomes), so generation stays a
/// pure function of the seed. It merely biases selection toward operations
/// that make progress from the intended state; the executor's model, not
/// this view, decides what each call must return.
#[derive(Clone, Copy, Default)]
struct GenSlot {
    /// A runtime is intended to exist in the slot.
    live: bool,
    /// A client is intended to be connected.
    client: bool,
    /// A live activity worker is intended to run.
    worker: bool,
    /// `Some(feeder_open)` while a replay worker is intended to run.
    replay: Option<bool>,
}

impl GenSlot {
    /// Applies the intended effect of a generated operation on this slot.
    fn apply(&mut self, op: Op) {
        match op {
            RuntimeNew(_) => self.live = true,
            RuntimeFree(_) | RuntimeDispose(_) => *self = GenSlot::default(),
            ReplayStart(_) if self.live && !self.worker && self.replay.is_none() => {
                self.replay = Some(true);
            }
            ReplayFinish(_) if self.replay.is_some() => self.replay = Some(false),
            ReplayDrain(_) | ReplayDispose(_) => self.replay = None,
            Connect(_) if self.live => self.client = true,
            WorkerStart(_) if self.client && self.replay.is_none() => self.worker = true,
            WorkerShutdown(_) => self.worker = false,
            ClientDisconnect(_) if !self.worker => self.client = false,
            _ => {}
        }
    }

    /// Chooses operations that advance `slot` from its intended state:
    /// create, start, feed, wait-then-poll pairs, complete or reject leases,
    /// and occasionally shut down or release with leases still held.
    fn progress(&self, rng: &mut Rng, slot: usize) -> Vec<Op> {
        let roll = rng.below(100);
        if !self.live {
            return vec![RuntimeNew(slot)];
        }
        if roll < 4 {
            return vec![RuntimeFree(slot)];
        }
        if roll < 8 {
            return vec![RuntimeDispose(slot)];
        }
        if let Some(feeder_open) = self.replay {
            return match roll {
                8..=27 if feeder_open => vec![ReplayFeed(slot, Complete)],
                28..=34 if feeder_open => vec![ReplayFeed(slot, Open)],
                8..=34 => vec![ReplayFinalize(slot)],
                35..=59 => vec![ReplayWait(slot), ReplayPoll(slot)],
                60..=74 => vec![ReplayComplete(slot, rng.pick())],
                75..=81 => vec![ReplayReject(slot, rng.pick())],
                82..=86 => vec![ReplayFinish(slot)],
                87..=93 => vec![ReplayDrain(slot)],
                _ => vec![ReplayDispose(slot)],
            };
        }
        if self.worker {
            return match roll {
                8..=49 => vec![ActivityWait(slot), ActivityPoll(slot)],
                50..=71 => vec![ActivityComplete(slot, rng.pick())],
                72..=81 => vec![ActivityReject(slot, rng.pick())],
                82..=86 => vec![ActivityCompleteStale(slot)],
                // Disconnect is refused while the worker still runs.
                87..=89 => vec![ClientDisconnect(slot)],
                _ => vec![WorkerShutdown(slot)],
            };
        }
        // A refused connection is left to the uniform table: on Windows each
        // refusal costs about two seconds of TCP retransmission.
        match roll {
            8..=49 => vec![ReplayStart(slot)],
            50..=94 if self.client => vec![WorkerStart(slot)],
            _ if self.client => vec![ClientDisconnect(slot)],
            _ => vec![Connect(slot)],
        }
    }
}

/// Generates the operation sequence for one case: mostly state-directed
/// progress on a random slot, interleaved with arbitrary operations.
fn generate(seed: u64, case: u64, steps: usize) -> Vec<Op> {
    let total: usize = OP_TABLE.iter().map(|(weight, _)| weight).sum();
    let mut rng = Rng::for_case(seed, case);
    let mut intended = [GenSlot::default(); SLOTS];
    let mut ops = Vec::with_capacity(steps + 1);
    while ops.len() < steps {
        let chosen = if rng.below(100) < ANY_OPERATION_PERCENT {
            let mut roll = rng.below(total);
            let mut chosen = None;
            for (weight, make) in OP_TABLE {
                if roll < *weight {
                    chosen = Some(make(&mut rng));
                    break;
                }
                roll -= weight;
            }
            vec![chosen.expect("roll is below the total weight")]
        } else {
            let slot = rng.below(SLOTS);
            intended[slot].progress(&mut rng, slot)
        };
        for op in chosen {
            if let Some(slot) = op.slot() {
                intended[slot].apply(op);
            }
            ops.push(op);
        }
    }
    ops.truncate(steps);
    ops
}

// ---------------------------------------------------------------------------
// gRPC double with a server-side completion ledger
// ---------------------------------------------------------------------------

/// Server-side evidence shared between the double and the checker.
#[derive(Default)]
struct ServerLedger {
    /// Next task-token number handed to an activity poll.
    next_token: AtomicU64,
    /// Completion RPCs (completed, failed, or cancelled) per task token.
    completions: Mutex<HashMap<Vec<u8>, usize>>,
}

impl ServerLedger {
    /// Returns the completion count recorded for one token.
    fn completions_of(&self, token: &[u8]) -> usize {
        self.completions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(token)
            .copied()
            .unwrap_or(0)
    }

    /// Returns any token completed more than once.
    fn double_completed(&self) -> Option<(Vec<u8>, usize)> {
        self.completions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .find(|(_, count)| **count > 1)
            .map(|(token, count)| (token.clone(), *count))
    }
}

/// The double shared by every case in the process: its address and ledger.
struct Double {
    /// Loopback address clients connect to.
    address: SocketAddr,
    /// Completion evidence recorded by the server.
    ledger: Arc<ServerLedger>,
}

/// Starts the shared gRPC double on first use.
///
/// The server runs on its own detached OS thread with a private
/// current-thread Tokio runtime, so it never shares an executor with the
/// bridge under test, and ends with the test process.
fn double() -> &'static Double {
    static DOUBLE: OnceLock<Double> = OnceLock::new();
    DOUBLE.get_or_init(|| {
        let ledger = Arc::new(ServerLedger::default());
        let (address_sender, address_receiver) = mpsc::channel();
        let server_ledger = Arc::clone(&ledger);
        thread::Builder::new()
            .name("grpc-lifecycle-stress-double".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test server runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind loopback test server");
                    address_sender
                        .send(listener.local_addr().expect("test server address"))
                        .expect("publish test server address");
                    loop {
                        let Ok((socket, _)) = listener.accept().await else {
                            continue;
                        };
                        tokio::spawn(serve_connection(socket, Arc::clone(&server_ledger)));
                    }
                });
            })
            .expect("spawn test server thread");
        let address = address_receiver
            .recv_timeout(CALL_DEADLINE)
            .expect("test server publishes its address");
        Double { address, ledger }
    })
}

/// Serves one HTTP/2 connection; each request runs on its own task because
/// reading a request body needs this accept loop to keep polling.
async fn serve_connection(socket: tokio::net::TcpStream, ledger: Arc<ServerLedger>) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((request, respond))) = connection.accept().await {
        tokio::spawn(serve_request(request, respond, Arc::clone(&ledger)));
    }
}

/// Reads one complete unary gRPC request message, decompressing Core's
/// default gzip encoding when the length prefix says so.
async fn read_unary_message(mut body: h2::RecvStream) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
        buffer.extend_from_slice(&chunk);
    }
    let compressed = buffer.first() == Some(&1);
    let message = buffer.get(5..).unwrap_or_default();
    if compressed {
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(message)
            .read_to_end(&mut decoded)
            .expect("Core sends valid gzip request messages");
        decoded
    } else {
        message.to_vec()
    }
}

/// Encodes a uniquely tokenized activity task the bridge can represent. No
/// timeouts are set, so Core never completes the task on its own.
fn activity_task(token: &[u8]) -> Vec<u8> {
    PollActivityTaskQueueResponse {
        task_token: token.to_vec(),
        workflow_namespace: "lifecycle-stress".to_owned(),
        workflow_type: Some(WorkflowType {
            name: "lifecycle-stress-workflow".to_owned(),
        }),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: "lifecycle-stress-workflow-id".to_owned(),
            run_id: "lifecycle-stress-run-id".to_owned(),
        }),
        activity_type: Some(ActivityType {
            name: "lifecycle-stress-activity".to_owned(),
        }),
        activity_id: "lifecycle-stress-activity-id".to_owned(),
        attempt: 1,
        ..Default::default()
    }
    .encode_to_vec()
}

/// Answers one gRPC request.
///
/// Every activity poll receives a new task; workflow polls are held open like
/// an idle long poll until Core cancels them. Activity completions of every
/// kind are recorded per token. Every other method succeeds with an empty
/// message, whose default encoding is valid for any response type.
async fn serve_request(
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    ledger: Arc<ServerLedger>,
) {
    let path = request.uri().path().to_owned();
    let message = if path.ends_with("/PollActivityTaskQueue") {
        let number = ledger.next_token.fetch_add(1, Ordering::SeqCst);
        activity_task(format!("lifecycle-stress-token-{number}").as_bytes())
    } else if path.ends_with("/PollWorkflowTaskQueue") {
        std::future::pending::<()>().await;
        return;
    } else {
        let token = if path.ends_with("/RespondActivityTaskCompleted") {
            let bytes = read_unary_message(request.into_body()).await;
            RespondActivityTaskCompletedRequest::decode(bytes.as_slice())
                .ok()
                .map(|request| request.task_token)
        } else if path.ends_with("/RespondActivityTaskFailed") {
            let bytes = read_unary_message(request.into_body()).await;
            RespondActivityTaskFailedRequest::decode(bytes.as_slice())
                .ok()
                .map(|request| request.task_token)
        } else if path.ends_with("/RespondActivityTaskCanceled") {
            let bytes = read_unary_message(request.into_body()).await;
            RespondActivityTaskCanceledRequest::decode(bytes.as_slice())
                .ok()
                .map(|request| request.task_token)
        } else {
            None
        };
        if let Some(token) = token {
            *ledger
                .completions
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .entry(token)
                .or_default() += 1;
        }
        Vec::new()
    };
    let response = http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(())
        .expect("static gRPC response");
    let Ok(mut stream) = respond.send_response(response, false) else {
        return;
    };
    // gRPC length-prefixed message: uncompressed flag, big-endian length.
    let length = u32::try_from(message.len()).expect("small test message");
    let mut frame = Vec::with_capacity(5 + message.len());
    frame.push(0);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&message);
    if stream.send_data(Bytes::from(frame), false).is_err() {
        return;
    }
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    let _ = stream.send_trailers(trailers);
}

// ---------------------------------------------------------------------------
// Bounded ABI calls
// ---------------------------------------------------------------------------

/// ABI entry point taking only a runtime and a result.
type PlainCall = unsafe extern "C" fn(*mut Runtime, *mut AbiResult) -> Status;
/// ABI entry point taking a runtime, a borrowed input span, and a result.
type InputCall = unsafe extern "C" fn(*mut Runtime, *const u8, usize, *mut AbiResult) -> Status;
/// ABI entry point taking a runtime slot (pointer to the handle pointer).
type SlotCall = unsafe extern "C" fn(*mut *mut Runtime) -> Status;

/// One runtime-scoped ABI call and the input it borrows.
enum Entry {
    /// A call with no input document.
    Plain(PlainCall),
    /// A call borrowing the owned input bytes for its duration.
    Input(InputCall, Vec<u8>),
}

/// Copied result of one runtime-scoped call. The native result has already
/// been released (twice, proving idempotent result release).
struct Outcome {
    /// Status returned by the call (and stored in its result).
    status: Status,
    /// Copied success value; empty on every failure.
    value: Vec<u8>,
    /// Copied UTF-8 diagnostic; empty on success.
    error: String,
}

/// Raw runtime pointer moved to the bounded call thread. Exactly one thread
/// uses it at a time, matching the ABI's single-owner rule; the test thread
/// waits for the call before touching the runtime again.
struct SendRuntime(*mut Runtime);
// SAFETY: Ownership of the pointer is handed to one helper thread for one
// call and observed again only after that call returns through a channel.
unsafe impl Send for SendRuntime {}

/// A call that did not return within [`CALL_DEADLINE`]. The runtime it used
/// is wedged inside the bridge, so it is leaked rather than released.
struct Hang;

/// Runs `body` on a helper thread and waits at most [`CALL_DEADLINE`].
fn bounded<T: Send + 'static>(body: impl FnOnce() -> T + Send + 'static) -> Result<T, Hang> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(body());
    });
    receiver.recv_timeout(CALL_DEADLINE).map_err(|_| Hang)
}

/// Copies one live buffer without taking ownership from its result.
fn buffer_bytes(buffer: &ocaml_temporal_core_bridge::Buffer) -> Vec<u8> {
    if buffer.ptr.is_null() {
        assert_eq!(buffer.len, 0, "an empty buffer has zero length");
        Vec::new()
    } else {
        // SAFETY: The bridge owns this readable allocation until the
        // containing result is released by the caller.
        unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len).to_vec() }
    }
}

/// Performs one runtime-scoped call and checks the result contract: the
/// returned status equals the stored one, at most one buffer owns bytes,
/// and releasing the result twice succeeds and leaves it empty.
fn call(runtime: *mut Runtime, entry: Entry) -> Result<Result<Outcome, String>, Hang> {
    let owned = SendRuntime(runtime);
    bounded(move || {
        let owned = owned;
        let mut result = AbiResult::default();
        // SAFETY: The runtime is null or live and exclusively used by this
        // thread for the call; the input vector outlives the call; the result
        // is initialized and uniquely writable.
        let status = unsafe {
            match &entry {
                Entry::Plain(function) => function(owned.0, &mut result),
                Entry::Input(function, input) => {
                    function(owned.0, input.as_ptr(), input.len(), &mut result)
                }
            }
        };
        release_result(status, result)
    })
}

/// Checks and releases one result produced by an ABI call.
fn release_result(status: Status, mut result: AbiResult) -> Result<Outcome, String> {
    let value = buffer_bytes(&result.value);
    let error = buffer_bytes(&result.error);
    let stored = result.status;
    for _ in 0..2 {
        // SAFETY: This function exclusively owns the initialized result; the
        // second release exercises the documented idempotent path.
        let freed = unsafe { ocaml_temporal_core_v4_result_free(&mut result) };
        if freed != STATUS_OK {
            return Err(format!("result_free returned {}", status_name(freed)));
        }
    }
    if !result.value.ptr.is_null() || !result.error.ptr.is_null() {
        return Err("result_free left a buffer owned".to_owned());
    }
    if stored != status {
        return Err(format!(
            "returned {} but stored {}",
            status_name(status),
            status_name(stored)
        ));
    }
    if status == STATUS_OK && !error.is_empty() {
        return Err("a successful result carried a diagnostic".to_owned());
    }
    if status != STATUS_OK && !value.is_empty() {
        return Err("a failed result carried a value".to_owned());
    }
    let error = String::from_utf8(error).map_err(|_| "diagnostic is not UTF-8".to_owned())?;
    Ok(Outcome {
        status,
        value,
        error,
    })
}

/// Runs a slot-consuming call (`runtime_free` or `runtime_dispose`) and
/// returns its status and the pointer left in the slot.
fn slot_call(runtime: *mut Runtime, function: SlotCall) -> Result<(Status, bool), Hang> {
    let owned = SendRuntime(runtime);
    bounded(move || {
        let owned = owned;
        let mut slot = owned.0;
        // SAFETY: The slot holds null or a live handle exclusively owned by
        // this call; the slot itself is a valid local.
        let status = unsafe { function(&mut slot) };
        (status, slot.is_null())
    })
}

/// Returns the symbolic name of an ABI status for traces.
fn status_name(status: Status) -> &'static str {
    match status {
        STATUS_OK => "OK",
        STATUS_INVALID_ARGUMENT => "INVALID_ARGUMENT",
        STATUS_ABI_MISMATCH => "ABI_MISMATCH",
        STATUS_PANIC => "PANIC",
        STATUS_INTERNAL => "INTERNAL",
        STATUS_INVALID_STATE => "INVALID_STATE",
        STATUS_CONFIGURATION => "CONFIGURATION",
        STATUS_CONNECTION => "CONNECTION",
        STATUS_WORKER => "WORKER",
        STATUS_OUTSTANDING_TASKS => "OUTSTANDING_TASKS",
        STATUS_NOT_READY => "NOT_READY",
        STATUS_PROTOCOL => "PROTOCOL",
        STATUS_ALREADY_STARTED => "ALREADY_STARTED",
        STATUS_RETRYABLE => "RETRYABLE",
        STATUS_ASYNC_HEARTBEAT_REJECTED => "ASYNC_HEARTBEAT_REJECTED",
        STATUS_RESOURCE_EXHAUSTED => "RESOURCE_EXHAUSTED",
        _ => "UNKNOWN",
    }
}

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/// One replay activation leased to the test.
struct ReplayLease {
    /// Run whose single outstanding activation this lease owns.
    run_id: String,
    /// Exact JSON document returned by the poll, used for rejection.
    document: Vec<u8>,
    /// Whether the activation is a pure cache eviction.
    eviction: bool,
}

/// Model of a started replay worker.
struct ReplayModel {
    /// Whether `finish_input` has not yet closed the feeder.
    feeder_open: bool,
    /// Activations polled and not yet completed or rejected.
    leases: Vec<ReplayLease>,
    /// Run IDs fed whose eviction has not yet been acknowledged.
    unretired: BTreeSet<String>,
}

/// One activity task leased to the test.
struct ActivityLease {
    /// Canonical base64 token used in semantic documents.
    token_text: String,
    /// Exact task JSON returned by the poll, used for rejection.
    document: Vec<u8>,
}

/// Model of one runtime slot's native graph.
#[derive(Default)]
struct SlotModel {
    /// Live handle, or null after release (the value the ABI wrote back).
    runtime: Option<SendRuntime>,
    /// Whether a client is connected.
    client: bool,
    /// Live worker leases, when a live worker is running.
    worker: Option<Vec<ActivityLease>>,
    /// Replay worker state, when one is running.
    replay: Option<ReplayModel>,
}

impl SlotModel {
    /// Returns the handle to pass to the ABI: live, or null after release.
    fn pointer(&self) -> *mut Runtime {
        self.runtime
            .as_ref()
            .map_or(ptr::null_mut(), |owned| owned.0)
    }
}

/// Documents whose lease has been retired, kept to exercise stale and
/// repeated completion. They are never reused while the same identity is
/// leased again.
#[derive(Default)]
struct Stale {
    /// Accepted replay completions with the run each one retired.
    replay_completions: Vec<(String, Vec<u8>)>,
    /// Accepted replay rejections with the run each one retired.
    replay_rejections: Vec<(String, Vec<u8>)>,
    /// Accepted activity completions; server tokens are never reissued.
    activity_completions: Vec<Vec<u8>>,
    /// Accepted activity rejections (exact task documents).
    activity_rejections: Vec<Vec<u8>>,
}

/// Failure of one case, with enough context to reproduce it.
#[derive(Clone)]
struct Failure {
    /// The first contract or ledger violation observed.
    message: String,
    /// Every executed operation with its observed outcome, in order.
    trace: Vec<String>,
    /// The failure was a native hang; the runtime was leaked and the
    /// process is no longer a clean base for minimization.
    wedged: bool,
}

/// Executor state for one case.
struct Case {
    /// Model and live handle of each runtime slot.
    slots: Vec<SlotModel>,
    /// Retired documents available for repeated-completion operations.
    stale: Stale,
    /// Executed operations with their observed outcomes.
    trace: Vec<String>,
    /// Runtimes this case created.
    created: u64,
    /// Runtimes released through the waiting `runtime_free`.
    released_sync: u64,
    /// Runtimes released through the non-waiting GC-fallback dispose.
    released_async: u64,
    /// Process-wide runtime creation count when the case began.
    base_created: u64,
    /// Process-wide runtime cleanup count when the case began. The gap to
    /// `base_created` covers earlier asynchronous cleanup still in flight.
    base_cleaned: u64,
    /// Task tokens leased to this case; each must be completed exactly once.
    leased_tokens: Vec<Vec<u8>>,
    /// Outcomes observed by this case only; [`run_ops`] hands them to the
    /// caller's collector when the case ends.
    coverage: Coverage,
}

impl Case {
    /// Creates an empty case and records the counter baseline.
    fn new() -> Self {
        let (base_created, base_cleaned) = test_runtime_cleanup_counts();
        Self {
            slots: (0..SLOTS).map(|_| SlotModel::default()).collect(),
            stale: Stale::default(),
            trace: Vec::new(),
            created: 0,
            released_sync: 0,
            released_async: 0,
            base_created,
            base_cleaned,
            leased_tokens: Vec::new(),
            coverage: Coverage::new(),
        }
    }

    /// Records one executed operation and its observed status.
    fn note(&mut self, op: &Op, detail: &str) {
        let index = self.trace.len();
        self.trace.push(format!("#{index:03} {op:?} -> {detail}"));
        record_coverage(&mut self.coverage, op.name(), detail);
    }

    /// Calls a runtime-scoped entry point on `slot` and requires one of the
    /// `allowed` statuses. A released slot passes null, which every
    /// runtime-scoped call must reject with `STATUS_INVALID_ARGUMENT`.
    fn expect(
        &mut self,
        op: &Op,
        slot: usize,
        entry: Entry,
        allowed: &[Status],
    ) -> Result<Outcome, CaseError> {
        let runtime = self.slots[slot].pointer();
        let allowed: &[Status] = if runtime.is_null() {
            &[STATUS_INVALID_ARGUMENT]
        } else {
            allowed
        };
        let outcome = call(runtime, entry)
            .map_err(|Hang| CaseError::Hang(format!("{op:?} did not return")))?
            .map_err(|message| CaseError::Check(format!("{op:?}: {message}")))?;
        self.note(op, status_name(outcome.status));
        if !allowed.contains(&outcome.status) {
            let names: Vec<_> = allowed.iter().map(|status| status_name(*status)).collect();
            return Err(CaseError::Check(format!(
                "{op:?} returned {} ({}), expected one of {names:?}",
                status_name(outcome.status),
                outcome.error
            )));
        }
        Ok(outcome)
    }

    /// Releases the slot's runtime through `function` and updates the
    /// handle ledger. A released slot exercises double release.
    fn release(&mut self, op: &Op, slot: usize, function: SlotCall, waits: bool) -> Step {
        let runtime = self.slots[slot].pointer();
        let (status, cleared) =
            slot_call(runtime, function).map_err(|Hang| CaseError::Hang(format!("{op:?}")))?;
        self.note(op, status_name(status));
        if status != STATUS_OK || !cleared {
            return Err(CaseError::Check(format!(
                "{op:?} returned {} with cleared slot {cleared}",
                status_name(status)
            )));
        }
        if !runtime.is_null() {
            if waits {
                self.released_sync += 1;
            } else {
                self.released_async += 1;
            }
            let model = &self.slots[slot];
            if model
                .worker
                .as_ref()
                .is_some_and(|leases| !leases.is_empty())
                || model
                    .replay
                    .as_ref()
                    .is_some_and(|replay| !replay.leases.is_empty())
            {
                record_coverage(&mut self.coverage, op.name(), "released with a held lease");
            }
            // Every lease the graph still held is force-completed by close;
            // its server completion is checked after teardown.
            self.slots[slot] = SlotModel::default();
        }
        Ok(())
    }

    /// Executes one operation against the bridge and the model.
    fn execute(&mut self, op: Op) -> Step {
        match op {
            RuntimeNew(slot) => {
                if self.slots[slot].runtime.is_some() {
                    self.note(&op, "skipped: slot is live (ABI contract)");
                    return Ok(());
                }
                let created = bounded(|| {
                    let mut runtime = ptr::null_mut();
                    let mut result = AbiResult::default();
                    // SAFETY: Both output locations are writable locals.
                    let status =
                        unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) };
                    (release_result(status, result), SendRuntime(runtime))
                })
                .map_err(|Hang| CaseError::Hang(format!("{op:?}")))?;
                let (outcome, runtime) = created;
                let outcome = outcome.map_err(CaseError::Check)?;
                self.note(&op, status_name(outcome.status));
                if outcome.status != STATUS_OK || runtime.0.is_null() {
                    return Err(CaseError::Check(format!(
                        "runtime_new returned {} ({})",
                        status_name(outcome.status),
                        outcome.error
                    )));
                }
                self.created += 1;
                self.slots[slot].runtime = Some(runtime);
                Ok(())
            }
            RuntimeFree(slot) => self.release(&op, slot, ocaml_temporal_core_v4_runtime_free, true),
            RuntimeDispose(slot) => {
                self.release(&op, slot, ocaml_temporal_core_v4_runtime_dispose, false)
            }
            RuntimeNullSlot => {
                // SAFETY: A null slot pointer is the documented defensive case.
                let statuses = unsafe {
                    [
                        ocaml_temporal_core_v4_runtime_free(ptr::null_mut()),
                        ocaml_temporal_core_v4_runtime_dispose(ptr::null_mut()),
                    ]
                };
                self.note(&op, status_name(statuses[0]));
                check(
                    statuses == [STATUS_INVALID_ARGUMENT; 2],
                    "null slot pointers must be rejected",
                )
            }
            PanicProbe => {
                let outcome = bounded(|| {
                    let mut result = AbiResult::default();
                    // SAFETY: The result is an initialized writable local.
                    let status = unsafe { test_invoke_panic(&mut result) };
                    release_result(status, result)
                })
                .map_err(|Hang| CaseError::Hang(format!("{op:?}")))?
                .map_err(CaseError::Check)?;
                self.note(&op, status_name(outcome.status));
                check(
                    outcome.status == STATUS_PANIC && !outcome.error.is_empty(),
                    "the panic probe must return a contained PANIC diagnostic",
                )
            }
            ReplayStart(slot) => {
                let model = &self.slots[slot];
                let allowed = if model.worker.is_some() || model.replay.is_some() {
                    STATUS_INVALID_STATE
                } else {
                    STATUS_OK
                };
                let outcome = self.expect(
                    &op,
                    slot,
                    Entry::Input(
                        ocaml_temporal_core_v4_replay_worker_start_json,
                        REPLAY_WORKER.to_vec(),
                    ),
                    &[allowed],
                )?;
                if outcome.status == STATUS_OK && self.slots[slot].runtime.is_some() {
                    self.slots[slot].replay = Some(ReplayModel {
                        feeder_open: true,
                        leases: Vec::new(),
                        unretired: BTreeSet::new(),
                    });
                }
                Ok(())
            }
            ReplayFeed(slot, kind) => {
                let allowed = match &self.slots[slot].replay {
                    None => Some(STATUS_INVALID_STATE),
                    Some(replay) if !replay.feeder_open => Some(STATUS_INVALID_STATE),
                    Some(replay) if replay.unretired.len() > MAX_UNRETIRED_HISTORIES => None,
                    Some(_) => Some(STATUS_OK),
                };
                let Some(allowed) = allowed else {
                    self.note(&op, "skipped: replay backpressure guard");
                    return Ok(());
                };
                let (run_id, document) = replay_history(kind);
                let outcome = self.expect(
                    &op,
                    slot,
                    Entry::Input(
                        ocaml_temporal_core_v4_replay_worker_feed_history_json,
                        document.into_bytes(),
                    ),
                    &[allowed],
                )?;
                if outcome.status == STATUS_OK
                    && let Some(replay) = self.slots[slot].replay.as_mut()
                {
                    replay.unretired.insert(run_id);
                }
                Ok(())
            }
            ReplayFeedMalformed(slot) => {
                // Strict history decoding runs before the feeder state check.
                let allowed = if self.slots[slot].replay.is_some() {
                    STATUS_PROTOCOL
                } else {
                    STATUS_INVALID_STATE
                };
                let document = br#"{"workflow_id":"stress","history":{"encoding":"base64","data":"not canonical"}}"#;
                self.expect(
                    &op,
                    slot,
                    Entry::Input(
                        ocaml_temporal_core_v4_replay_worker_feed_history_json,
                        document.to_vec(),
                    ),
                    &[allowed],
                )
                .map(drop)
            }
            ReplayFinish(slot) => {
                self.expect(
                    &op,
                    slot,
                    Entry::Plain(ocaml_temporal_core_v4_replay_worker_finish_input),
                    &[STATUS_OK],
                )?;
                if let Some(replay) = self.slots[slot].replay.as_mut() {
                    replay.feeder_open = false;
                }
                Ok(())
            }
            ReplayWait(slot) => {
                let allowed: &[Status] = if self.slots[slot].replay.is_some() {
                    &[STATUS_OK, STATUS_NOT_READY]
                } else {
                    &[STATUS_INVALID_STATE]
                };
                self.expect(
                    &op,
                    slot,
                    Entry::Plain(ocaml_temporal_core_v4_replay_worker_wait_workflow),
                    allowed,
                )
                .map(drop)
            }
            ReplayPoll(slot) => self.replay_poll(&op, slot).map(drop),
            ReplayComplete(slot, pick) => {
                let index = self.slots[slot]
                    .replay
                    .as_ref()
                    .filter(|replay| !replay.leases.is_empty())
                    .map(|replay| usize::from(pick) % replay.leases.len());
                match index {
                    Some(index) => self.replay_complete(&op, slot, index),
                    None => self.replay_complete_unleased(&op, slot, false),
                }
            }
            ReplayCompleteStale(slot) => self.replay_complete_unleased(&op, slot, true),
            ReplayReject(slot, pick) => {
                let index = self.slots[slot]
                    .replay
                    .as_ref()
                    .filter(|replay| !replay.leases.is_empty())
                    .map(|replay| usize::from(pick) % replay.leases.len());
                match index {
                    Some(index) => self.replay_reject(&op, slot, index),
                    None => self.replay_reject_unleased(&op, slot, false),
                }
            }
            ReplayRejectStale(slot) => self.replay_reject_unleased(&op, slot, true),
            ReplayFinalize(slot) => self.replay_finalize(&op, slot).map(drop),
            ReplayDrain(slot) => self.replay_drain(&op, slot),
            ReplayDispose(slot) => {
                self.expect(
                    &op,
                    slot,
                    Entry::Plain(ocaml_temporal_core_v4_replay_worker_dispose),
                    &[STATUS_OK],
                )?;
                // Disposal acknowledges every lease and abandons queued
                // histories; the bridge drops their semantic handoffs.
                self.slots[slot].replay = None;
                Ok(())
            }
            Connect(slot) => {
                let allowed = if self.slots[slot].client {
                    STATUS_INVALID_STATE
                } else {
                    STATUS_OK
                };
                let config = format!(
                    r#"{{"target_url":"http://{}","identity":"lifecycle-stress"}}"#,
                    double().address
                );
                let outcome = self.expect(
                    &op,
                    slot,
                    Entry::Input(
                        ocaml_temporal_core_v4_client_connect_json,
                        config.into_bytes(),
                    ),
                    &[allowed],
                )?;
                if outcome.status == STATUS_OK && self.slots[slot].runtime.is_some() {
                    self.slots[slot].client = true;
                }
                Ok(())
            }
            ConnectRefused(slot) => {
                // A refused connection publishes no client, so the model is
                // unchanged and a later connect must still succeed.
                let allowed = if self.slots[slot].client {
                    STATUS_INVALID_STATE
                } else {
                    STATUS_CONNECTION
                };
                self.expect(
                    &op,
                    slot,
                    Entry::Input(
                        ocaml_temporal_core_v4_client_connect_json,
                        REFUSED_CLIENT.to_vec(),
                    ),
                    &[allowed],
                )
                .map(drop)
            }
            WorkerStart(slot) => {
                let model = &self.slots[slot];
                let allowed = if model.worker.is_some() || model.replay.is_some() || !model.client {
                    STATUS_INVALID_STATE
                } else {
                    STATUS_OK
                };
                let outcome = self.expect(
                    &op,
                    slot,
                    Entry::Input(
                        ocaml_temporal_core_v4_worker_start_json,
                        ACTIVITY_WORKER.to_vec(),
                    ),
                    &[allowed],
                )?;
                if outcome.status == STATUS_OK && self.slots[slot].runtime.is_some() {
                    self.slots[slot].worker = Some(Vec::new());
                }
                Ok(())
            }
            ActivityWait(slot) => {
                let allowed: &[Status] = if self.slots[slot].worker.is_some() {
                    &[STATUS_OK, STATUS_NOT_READY]
                } else {
                    &[STATUS_INVALID_STATE]
                };
                self.expect(
                    &op,
                    slot,
                    Entry::Plain(ocaml_temporal_core_v4_worker_wait_activity),
                    allowed,
                )
                .map(drop)
            }
            ActivityPoll(slot) => self.activity_poll(&op, slot),
            ActivityComplete(slot, pick) => {
                let index = self.slots[slot]
                    .worker
                    .as_ref()
                    .filter(|leases| !leases.is_empty())
                    .map(|leases| usize::from(pick) % leases.len());
                match index {
                    Some(index) => self.activity_complete(&op, slot, index),
                    None => self.activity_complete_unleased(&op, slot, false),
                }
            }
            ActivityCompleteStale(slot) => self.activity_complete_unleased(&op, slot, true),
            ActivityReject(slot, pick) => {
                let index = self.slots[slot]
                    .worker
                    .as_ref()
                    .filter(|leases| !leases.is_empty())
                    .map(|leases| usize::from(pick) % leases.len());
                match index {
                    Some(index) => self.activity_reject(&op, slot, index),
                    None => self.activity_reject_unleased(&op, slot, false),
                }
            }
            ActivityRejectStale(slot) => self.activity_reject_unleased(&op, slot, true),
            WorkerShutdown(slot) => {
                // Shutdown retires undelivered tasks silently and reports
                // force-completed leases with the typed outstanding status.
                let allowed = match &self.slots[slot].worker {
                    Some(leases) if !leases.is_empty() => STATUS_OUTSTANDING_TASKS,
                    _ => STATUS_OK,
                };
                self.expect(
                    &op,
                    slot,
                    Entry::Plain(ocaml_temporal_core_v4_worker_shutdown),
                    &[allowed],
                )?;
                self.slots[slot].worker = None;
                Ok(())
            }
            ClientDisconnect(slot) => {
                let allowed = if self.slots[slot].worker.is_some() {
                    STATUS_INVALID_STATE
                } else {
                    STATUS_OK
                };
                let outcome = self.expect(
                    &op,
                    slot,
                    Entry::Plain(ocaml_temporal_core_v4_client_disconnect),
                    &[allowed],
                )?;
                if outcome.status == STATUS_OK {
                    self.slots[slot].client = false;
                }
                Ok(())
            }
        }
    }

    /// Polls one replay activation and leases it in the model.
    ///
    /// Returns whether an activation was leased. A delivered activation must
    /// belong to a fed, unretired history, and its run must not already be
    /// leased: a duplicate would mean one Core debt had two owners.
    fn replay_poll(&mut self, op: &Op, slot: usize) -> Result<bool, CaseError> {
        let allowed: &[Status] = if self.slots[slot].replay.is_some() {
            &[STATUS_OK, STATUS_NOT_READY]
        } else {
            &[STATUS_INVALID_STATE]
        };
        let outcome = self.expect(
            op,
            slot,
            Entry::Plain(ocaml_temporal_core_v4_replay_worker_try_poll_workflow),
            allowed,
        )?;
        if outcome.status != STATUS_OK {
            return Ok(false);
        }
        let text = std::str::from_utf8(&outcome.value)
            .map_err(|_| CaseError::Check("replay activation is not UTF-8".to_owned()))?;
        let activation = workflow_protocol::decode_activation(text)
            .map_err(|error| CaseError::Check(format!("replay activation: {error:?}")))?;
        let eviction = activation
            .jobs
            .iter()
            .any(|job| matches!(job, ActivationJob::RemoveFromCache { .. }));
        let replay = self.slots[slot]
            .replay
            .as_mut()
            .expect("a successful replay poll has a replay worker");
        if !replay.unretired.contains(&activation.run_id) {
            return Err(CaseError::Check(format!(
                "replay delivered an activation for unknown or retired run {}",
                activation.run_id
            )));
        }
        if replay
            .leases
            .iter()
            .any(|lease| lease.run_id == activation.run_id)
        {
            return Err(CaseError::Check(format!(
                "replay leased run {} twice",
                activation.run_id
            )));
        }
        replay.leases.push(ReplayLease {
            run_id: activation.run_id,
            document: outcome.value,
            eviction,
        });
        Ok(true)
    }

    /// Completes held replay lease `index` with the completion matching its
    /// activation and retires it in the model.
    fn replay_complete(&mut self, op: &Op, slot: usize, index: usize) -> Step {
        let (run_id, eviction) = {
            let lease = &self.slots[slot]
                .replay
                .as_ref()
                .expect("lease owner")
                .leases[index];
            (lease.run_id.clone(), lease.eviction)
        };
        // An eviction is acknowledged empty; a workflow task completes the
        // replayed workflow, matching the fixture's terminal event.
        let commands = if eviction {
            Vec::new()
        } else {
            vec![CompletionCommand::CompleteWorkflow { result: None }]
        };
        let document = workflow_protocol::encode_completion(&Completion {
            run_id: run_id.clone(),
            commands,
            task_failure: None,
        })
        .map_err(|error| CaseError::Check(format!("completion encoding: {error:?}")))?;
        self.expect(
            op,
            slot,
            Entry::Input(
                ocaml_temporal_core_v4_replay_worker_complete_workflow_json,
                document.clone().into_bytes(),
            ),
            &[STATUS_OK],
        )?;
        self.retire_replay_lease(slot, index);
        self.stale
            .replay_completions
            .push((run_id, document.into_bytes()));
        Ok(())
    }

    /// Removes replay lease `index`; an eviction also retires its history.
    fn retire_replay_lease(&mut self, slot: usize, index: usize) -> ReplayLease {
        let replay = self.slots[slot].replay.as_mut().expect("lease owner");
        let lease = replay.leases.remove(index);
        if lease.eviction {
            replay.unretired.remove(&lease.run_id);
        }
        lease
    }

    /// Returns true when `run_id` is currently leased on any slot, so a
    /// stale document for it would be a fresh (if wrong) completion.
    fn run_is_leased(&self, run_id: &str) -> bool {
        self.slots.iter().any(|slot| {
            slot.replay
                .as_ref()
                .is_some_and(|replay| replay.leases.iter().any(|lease| lease.run_id == run_id))
        })
    }

    /// Sends a replay completion that matches no lease: a retired one when
    /// `stale` and one exists, otherwise a never-leased run. The lease map is
    /// checked before the worker, so this is `STATUS_PROTOCOL` whenever the
    /// runtime is live.
    fn replay_complete_unleased(&mut self, op: &Op, slot: usize, stale: bool) -> Step {
        let reusable = self
            .stale
            .replay_completions
            .iter()
            .rev()
            .find(|(run_id, _)| !self.run_is_leased(run_id))
            .map(|(_, document)| document.clone());
        let document = match reusable.filter(|_| stale) {
            Some(document) => document,
            None => workflow_protocol::encode_completion(&Completion {
                run_id: "lifecycle-stress-never-leased".to_owned(),
                commands: Vec::new(),
                task_failure: None,
            })
            .expect("static completion encodes")
            .into_bytes(),
        };
        self.expect(
            op,
            slot,
            Entry::Input(
                ocaml_temporal_core_v4_replay_worker_complete_workflow_json,
                document,
            ),
            &[STATUS_PROTOCOL],
        )
        .map(drop)
    }

    /// Rejects held replay lease `index` with its exact poll document.
    fn replay_reject(&mut self, op: &Op, slot: usize, index: usize) -> Step {
        let document = self.slots[slot]
            .replay
            .as_ref()
            .expect("lease owner")
            .leases[index]
            .document
            .clone();
        self.expect(
            op,
            slot,
            Entry::Input(
                ocaml_temporal_core_v4_replay_worker_reject_workflow_json,
                document.clone(),
            ),
            &[STATUS_OK],
        )?;
        // A rejected workflow task is followed by a Core eviction for the
        // same run, which keeps the history unretired until acknowledged.
        let lease = self.retire_replay_lease(slot, index);
        self.stale.replay_rejections.push((lease.run_id, document));
        Ok(())
    }

    /// Sends a replay rejection that matches no lease; always
    /// `STATUS_PROTOCOL` on a live runtime.
    fn replay_reject_unleased(&mut self, op: &Op, slot: usize, stale: bool) -> Step {
        let reusable = self
            .stale
            .replay_rejections
            .iter()
            .rev()
            .find(|(run_id, _)| !self.run_is_leased(run_id))
            .map(|(_, document)| document.clone());
        let document = reusable.filter(|_| stale).unwrap_or_else(|| b"{}".to_vec());
        self.expect(
            op,
            slot,
            Entry::Input(
                ocaml_temporal_core_v4_replay_worker_reject_workflow_json,
                document,
            ),
            &[STATUS_PROTOCOL],
        )
        .map(drop)
    }

    /// Attempts natural finalization and returns whether it succeeded.
    ///
    /// Success is only valid once input is closed, no lease is held, and
    /// every fed history was evicted; anything else means Core or the bridge
    /// dropped a completion debt.
    fn replay_finalize(&mut self, op: &Op, slot: usize) -> Result<bool, CaseError> {
        let allowed: &[Status] = if self.slots[slot].replay.is_some() {
            &[STATUS_OK, STATUS_OUTSTANDING_TASKS]
        } else {
            &[STATUS_INVALID_STATE]
        };
        let outcome = self.expect(
            op,
            slot,
            Entry::Plain(ocaml_temporal_core_v4_replay_worker_finalize),
            allowed,
        )?;
        if outcome.status != STATUS_OK || self.slots[slot].runtime.is_none() {
            return Ok(false);
        }
        let replay = self.slots[slot]
            .replay
            .take()
            .expect("successful finalize had a replay worker");
        if replay.feeder_open || !replay.leases.is_empty() || !replay.unretired.is_empty() {
            return Err(CaseError::Check(format!(
                "replay finalized with feeder open {}, {} leases held, unretired runs {:?}",
                replay.feeder_open,
                replay.leases.len(),
                replay.unretired
            )));
        }
        Ok(true)
    }

    /// Closes input, completes every held and newly delivered activation,
    /// and requires natural finalization within [`DRAIN_ROUNDS`].
    fn replay_drain(&mut self, op: &Op, slot: usize) -> Step {
        if self.slots[slot].replay.is_none() || self.slots[slot].runtime.is_none() {
            // Without a replay worker the drain degenerates to its first
            // call, whose documented failure is still checked.
            return self.execute(ReplayFinalize(slot));
        }
        // The inner calls are traced (and counted) as the operations they
        // are, between this marker and the drain's final outcome.
        self.note(op, "drain begins");
        self.execute(ReplayFinish(slot))?;
        for _ in 0..DRAIN_ROUNDS {
            while self.slots[slot]
                .replay
                .as_ref()
                .is_some_and(|replay| !replay.leases.is_empty())
            {
                self.replay_complete(&ReplayComplete(slot, 0), slot, 0)?;
            }
            if self.replay_finalize(&ReplayFinalize(slot), slot)? {
                self.note(op, "drained and finalized");
                return Ok(());
            }
            self.execute(ReplayWait(slot))?;
            self.replay_poll(&ReplayPoll(slot), slot)?;
        }
        Err(CaseError::Check(format!(
            "{op:?}: replay did not finalize after {DRAIN_ROUNDS} drain rounds"
        )))
    }

    /// Polls one activity task and leases it in the model. Every server
    /// token is unique, so a token that is already leased or retired here
    /// would be a duplicate delivery.
    fn activity_poll(&mut self, op: &Op, slot: usize) -> Step {
        let allowed: &[Status] = if self.slots[slot].worker.is_some() {
            &[STATUS_OK, STATUS_NOT_READY]
        } else {
            &[STATUS_INVALID_STATE]
        };
        let outcome = self.expect(
            op,
            slot,
            Entry::Plain(ocaml_temporal_core_v4_worker_try_poll_activity),
            allowed,
        )?;
        if outcome.status != STATUS_OK {
            return Ok(());
        }
        let text = std::str::from_utf8(&outcome.value)
            .map_err(|_| CaseError::Check("activity task is not UTF-8".to_owned()))?;
        let task = activity_protocol::decode_task(text)
            .map_err(|error| CaseError::Check(format!("activity task: {error:?}")))?;
        let token = STANDARD
            .decode(&task.task_token)
            .map_err(|_| CaseError::Check("activity token is not base64".to_owned()))?;
        if self.leased_tokens.contains(&token) {
            return Err(CaseError::Check(format!(
                "activity token {} was delivered twice",
                String::from_utf8_lossy(&token)
            )));
        }
        self.leased_tokens.push(token);
        self.slots[slot]
            .worker
            .as_mut()
            .expect("a successful activity poll has a worker")
            .push(ActivityLease {
                token_text: task.task_token,
                document: outcome.value,
            });
        Ok(())
    }

    /// Completes held activity lease `index` and retires it.
    fn activity_complete(&mut self, op: &Op, slot: usize, index: usize) -> Step {
        let token_text = self.slots[slot].worker.as_ref().expect("lease owner")[index]
            .token_text
            .clone();
        let document = activity_completion(&token_text);
        self.expect(
            op,
            slot,
            Entry::Input(
                ocaml_temporal_core_v4_worker_complete_activity_json,
                document.clone(),
            ),
            &[STATUS_OK],
        )?;
        self.slots[slot]
            .worker
            .as_mut()
            .expect("lease owner")
            .remove(index);
        self.stale.activity_completions.push(document);
        Ok(())
    }

    /// Sends an activity completion that matches no lease. Without a worker
    /// this is `STATUS_INVALID_STATE`; with one, the bridge ledger refuses
    /// the unknown or retired token with `STATUS_WORKER`.
    fn activity_complete_unleased(&mut self, op: &Op, slot: usize, stale: bool) -> Step {
        let document = self
            .stale
            .activity_completions
            .last()
            .filter(|_| stale)
            .cloned()
            .unwrap_or_else(|| activity_completion(&STANDARD.encode("never-leased")));
        let allowed = if self.slots[slot].worker.is_some() {
            STATUS_WORKER
        } else {
            STATUS_INVALID_STATE
        };
        self.expect(
            op,
            slot,
            Entry::Input(
                ocaml_temporal_core_v4_worker_complete_activity_json,
                document,
            ),
            &[allowed],
        )
        .map(drop)
    }

    /// Rejects held activity lease `index` with its exact poll document;
    /// the bridge fails the task back to the server and retires the lease.
    fn activity_reject(&mut self, op: &Op, slot: usize, index: usize) -> Step {
        let document = self.slots[slot].worker.as_ref().expect("lease owner")[index]
            .document
            .clone();
        self.expect(
            op,
            slot,
            Entry::Input(
                ocaml_temporal_core_v4_worker_reject_activity_json,
                document.clone(),
            ),
            &[STATUS_OK],
        )?;
        self.slots[slot]
            .worker
            .as_mut()
            .expect("lease owner")
            .remove(index);
        self.stale.activity_rejections.push(document);
        Ok(())
    }

    /// Sends an activity rejection that matches no lease. The handoff map is
    /// checked before the worker, so this is `STATUS_PROTOCOL` on a live
    /// runtime.
    fn activity_reject_unleased(&mut self, op: &Op, slot: usize, stale: bool) -> Step {
        let document = self
            .stale
            .activity_rejections
            .last()
            .filter(|_| stale)
            .cloned()
            .unwrap_or_else(|| b"{}".to_vec());
        self.expect(
            op,
            slot,
            Entry::Input(ocaml_temporal_core_v4_worker_reject_activity_json, document),
            &[STATUS_PROTOCOL],
        )
        .map(drop)
    }

    /// Checks the handle and server ledgers between operations: creations
    /// match the model exactly, cleanups never exceed releases (a double
    /// release would), and no task token was completed twice.
    fn check_ledgers(&self) -> Step {
        let (created, cleaned) = test_runtime_cleanup_counts();
        let created = created - self.base_created;
        let cleaned = cleaned - self.base_cleaned;
        let in_flight_before = self.base_created - self.base_cleaned;
        if created != self.created {
            return Err(CaseError::Check(format!(
                "runtime creations {created} differ from the model's {}",
                self.created
            )));
        }
        let released = self.released_sync + self.released_async;
        if cleaned > released + in_flight_before || cleaned < self.released_sync {
            return Err(CaseError::Check(format!(
                "runtime cleanups {cleaned} outside [{}, {}]",
                self.released_sync,
                released + in_flight_before
            )));
        }
        if let Some((token, count)) = double().ledger.double_completed() {
            return Err(CaseError::Check(format!(
                "task token {} was completed {count} times",
                String::from_utf8_lossy(&token)
            )));
        }
        Ok(())
    }

    /// Releases every live runtime, then waits for asynchronous cleanup and
    /// server completions to converge: every runtime cleaned exactly once and
    /// every token leased in this case completed exactly once.
    fn teardown(&mut self) -> Step {
        for slot in 0..SLOTS {
            if self.slots[slot].runtime.is_some() {
                self.execute(RuntimeFree(slot))?;
            }
        }
        let released = self.released_sync + self.released_async;
        let deadline = Instant::now() + SETTLE_DEADLINE;
        loop {
            let (created, cleaned) = test_runtime_cleanup_counts();
            let pending_before = self.base_created - self.base_cleaned;
            let created = created - self.base_created;
            let cleaned = cleaned - self.base_cleaned;
            if created == released && cleaned == released + pending_before {
                break;
            }
            if Instant::now() >= deadline {
                return Err(CaseError::Check(format!(
                    "after teardown {created} runtimes were created and {cleaned} cleaned, \
                     expected {released} and {}",
                    released + pending_before
                )));
            }
            thread::sleep(Duration::from_millis(5));
        }
        let ledger = &double().ledger;
        for token in &self.leased_tokens {
            loop {
                let count = ledger.completions_of(token);
                if count == 1 {
                    break;
                }
                if count > 1 || Instant::now() >= deadline {
                    return Err(CaseError::Check(format!(
                        "leased task token {} was completed {count} times",
                        String::from_utf8_lossy(token)
                    )));
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        self.check_ledgers()
    }
}

/// Error raised while executing a case.
enum CaseError {
    /// A model or contract check failed; the process is still clean.
    Check(String),
    /// A native call did not return; its runtime was leaked.
    Hang(String),
}

/// Result type of executor steps.
type Step = Result<(), CaseError>;

/// Converts a boolean contract check into a step result.
fn check(condition: bool, message: &str) -> Step {
    if condition {
        Ok(())
    } else {
        Err(CaseError::Check(message.to_owned()))
    }
}

/// Builds one uniquely identified replay history document. Distinct run IDs
/// keep Core from treating a later history as another task of an earlier one.
fn replay_history(kind: HistoryKind) -> (String, String) {
    let number = NEXT_HISTORY.fetch_add(1, Ordering::SeqCst);
    let workflow_id = format!("lifecycle-stress-workflow-{number}");
    let run_id = format!("lifecycle-stress-run-{number}");
    let template = match kind {
        Complete => replay_fixture::complete_history_document(&workflow_id),
        Open => replay_fixture::open_workflow_task_document(&workflow_id),
    };
    let template: serde_json::Value =
        serde_json::from_str(&template).expect("fixture document is JSON");
    let data = template["history"]["data"]
        .as_str()
        .expect("fixture carries base64 history");
    let mut history = History::decode(STANDARD.decode(data).expect("fixture base64").as_slice())
        .expect("fixture history decodes");
    let Some(Attributes::WorkflowExecutionStartedEventAttributes(started)) =
        history.events[0].attributes.as_mut()
    else {
        panic!("replay fixture must begin with a workflow start event");
    };
    started.original_execution_run_id = run_id.clone();
    started.first_execution_run_id = run_id.clone();
    let document = serde_json::json!({
        "workflow_id": workflow_id,
        "history": {"encoding": "base64", "data": STANDARD.encode(history.encode_to_vec())},
    })
    .to_string();
    (run_id, document)
}

/// Encodes a successful completion for one activity token. Core requires a
/// result payload (possibly empty) on every successful completion, as the
/// OCaml executor always supplies.
fn activity_completion(token_text: &str) -> Vec<u8> {
    activity_protocol::encode_completion(&ActivityCompletion {
        task_token: token_text.to_owned(),
        result: ActivityCompletionResult::Completed {
            result: Some(Payload {
                metadata: BTreeMap::new(),
                data: Vec::new(),
            }),
        },
    })
    .expect("activity completion encodes")
    .into_bytes()
}

// ---------------------------------------------------------------------------
// Case driver, minimization, and reporting
// ---------------------------------------------------------------------------

/// Runs one operation sequence from a fresh model, checking the ledgers
/// after every step and once more after teardown. The outcomes the case
/// observed, including those of a failing case, are added to `coverage`.
fn run_ops(ops: &[Op], coverage: &mut Coverage) -> Result<(), Failure> {
    let mut case = Case::new();
    let mut outcome = Ok(());
    for op in ops {
        outcome = case.execute(*op).and_then(|()| case.check_ledgers());
        if outcome.is_err() {
            break;
        }
    }
    let outcome = match outcome {
        // A hung runtime is wedged; releasing it could hang again.
        Err(CaseError::Hang(message)) => Err(CaseError::Hang(message)),
        Ok(()) => case.teardown(),
        Err(CaseError::Check(message)) => {
            // Release what the failed case still owns so later cases and
            // minimization runs start from a clean ledger.
            let _ = case.teardown();
            Err(CaseError::Check(message))
        }
    };
    merge_coverage(coverage, std::mem::take(&mut case.coverage));
    outcome.map_err(|error| {
        let (message, wedged) = match error {
            CaseError::Check(message) => (message, false),
            CaseError::Hang(message) => (format!("native call hang: {message}"), true),
        };
        Failure {
            message,
            trace: std::mem::take(&mut case.trace),
            wedged,
        }
    })
}

/// Coarse failure category used to keep minimization on the same defect:
/// the leading word of the message, which is the failing operation's name
/// for contract checks and a fixed word for ledger checks.
fn failure_kind(message: &str) -> &str {
    let end = message
        .find(|character: char| !character.is_ascii_alphabetic())
        .unwrap_or(message.len());
    &message[..end]
}

/// Deletes chunks of operations while the sequence still fails in the same
/// category, bounded by [`MAX_SHRINK_RUNS`]. Native timing can still change
/// the exact failure; the result is a shorter sequence with its own trace,
/// not a proof of minimality. Reruns record into a discarded collector so
/// shrinking cannot add coverage the generated cases did not reach.
fn minimize(mut ops: Vec<Op>, mut failure: Failure) -> (Vec<Op>, Failure) {
    if failure.wedged {
        return (ops, failure);
    }
    let kind = failure_kind(&failure.message).to_owned();
    let mut chunk = ops.len() / 2;
    let mut runs = 0;
    while chunk > 0 && runs < MAX_SHRINK_RUNS {
        let mut start = 0;
        let mut reduced = false;
        while start < ops.len() && runs < MAX_SHRINK_RUNS {
            let end = (start + chunk).min(ops.len());
            let mut candidate = ops[..start].to_vec();
            candidate.extend_from_slice(&ops[end..]);
            runs += 1;
            match run_ops(&candidate, &mut Coverage::new()) {
                Err(smaller) if smaller.wedged => {
                    // A hang leaves a leaked runtime; stop reducing.
                    return (candidate, smaller);
                }
                Err(smaller) if failure_kind(&smaller.message) == kind => {
                    ops = candidate;
                    failure = smaller;
                    reduced = true;
                }
                Err(_) | Ok(()) => start = end,
            }
        }
        if !reduced {
            chunk /= 2;
        }
    }
    (ops, failure)
}

/// Directory for failure reports, retained by CI as an artifact.
fn artifact_dir() -> PathBuf {
    std::env::var_os("LIFECYCLE_STRESS_OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../_build/lifecycle-stress")
        })
}

/// Formats a failure report: seed, case, reproduction command, the original
/// failure, and the minimized operation list and trace.
fn report(
    config: &Config,
    case: u64,
    original: &[Op],
    first: &Failure,
    minimized: &[Op],
    last: &Failure,
) -> String {
    let mut text = String::new();
    let _ = writeln!(
        text,
        "lifecycle stress failure: seed 0x{:x}, case {case}, {} steps",
        config.seed, config.steps
    );
    let _ = writeln!(
        text,
        "reproduce: LIFECYCLE_STRESS_SEED=0x{:x} LIFECYCLE_STRESS_CASE={case} \
         LIFECYCLE_STRESS_STEPS={} make native-test-lifecycle-stress",
        config.seed, config.steps
    );
    let _ = writeln!(text, "original failure: {}", first.message);
    let _ = writeln!(
        text,
        "original sequence ({} ops): {original:?}",
        original.len()
    );
    let _ = writeln!(text, "minimized failure: {}", last.message);
    let _ = writeln!(
        text,
        "minimized sequence ({} ops): {minimized:?}",
        minimized.len()
    );
    let _ = writeln!(text, "minimized trace:");
    for line in &last.trace {
        let _ = writeln!(text, "  {line}");
    }
    text
}

/// Seeded operation sequences over replay and live workers: every case's
/// handles, leases, and completions are accounted for exactly once.
#[test]
fn seeded_lifecycle_operation_sequences() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let config = Config::from_env();
    let cases: Vec<u64> = match config.only_case {
        Some(case) => vec![case],
        None => (0..config.cases).collect(),
    };
    let started = Instant::now();
    let mut operations = 0;
    // Local to this test: only generated cases count towards the required
    // coverage, whatever other tests in the binary ran first.
    let mut coverage = Coverage::new();
    for case in cases {
        let ops = generate(config.seed, case, config.steps);
        operations += ops.len();
        if let Err(first) = run_ops(&ops, &mut coverage) {
            let (minimized, last) = minimize(ops.clone(), first.clone());
            let text = report(&config, case, &ops, &first, &minimized, &last);
            let directory = artifact_dir();
            let path = directory.join(format!("{:016x}-{case:06}.txt", config.seed));
            if fs::create_dir_all(&directory).is_ok() && fs::write(&path, &text).is_ok() {
                eprintln!("lifecycle stress report written to {}", path.display());
            }
            panic!("{text}");
        }
    }
    eprintln!(
        "lifecycle stress: seed 0x{:x}, {} cases, {operations} operations in {:?}",
        config.seed,
        config.only_case.map_or(config.cases, |_| 1),
        started.elapsed()
    );
    for ((name, detail), count) in coverage.iter() {
        eprintln!("  {count:6} {name} -> {detail}");
    }
    // Only the default stream is required to reach every outcome; custom
    // seeds and budgets report their coverage without asserting it.
    if config.seed == DEFAULT_SEED && config.cases >= DEFAULT_CASES && config.only_case.is_none() {
        for (name, detail) in REQUIRED_COVERAGE {
            assert!(
                coverage.contains_key(&((*name).to_owned(), (*detail).to_owned())),
                "the default stress run never observed {name} -> {detail}"
            );
        }
    }
}

/// Hand-written sequences for interleavings worth pinning regardless of the
/// seed: abandoned leases at GC-fallback dispose and explicit free, double
/// release, shutdown with an outstanding lease followed by stale
/// completions, and a replay drained after a rejection. A minimized failing
/// trace can be pasted here verbatim as a regression.
#[test]
fn scripted_regressions() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let sequences: &[&[Op]] = &[
        // Activity lease abandoned at GC-fallback dispose, then double close.
        &[
            RuntimeNew(0),
            Connect(0),
            WorkerStart(0),
            ActivityWait(0),
            ActivityPoll(0),
            ActivityWait(0),
            ActivityPoll(0),
            RuntimeDispose(0),
            RuntimeDispose(0),
            RuntimeFree(0),
            ActivityPoll(0),
        ],
        // Lease reported by shutdown, then stale completion and rejection on
        // a restarted worker, then disconnect ordering.
        &[
            RuntimeNew(1),
            Connect(1),
            WorkerStart(1),
            ActivityWait(1),
            ActivityPoll(1),
            ActivityComplete(1, 0),
            ActivityWait(1),
            ActivityPoll(1),
            ClientDisconnect(1),
            WorkerShutdown(1),
            WorkerShutdown(1),
            ActivityCompleteStale(1),
            WorkerStart(1),
            ActivityCompleteStale(1),
            ActivityRejectStale(1),
            ActivityWait(1),
            ActivityPoll(1),
            ActivityReject(1, 0),
            ActivityRejectStale(1),
            WorkerShutdown(1),
            ClientDisconnect(1),
            RuntimeFree(1),
            RuntimeFree(1),
        ],
        // Replay lease held at explicit free and at replay dispose; replay
        // drained after a rejection; finalize before and after drain.
        &[
            RuntimeNew(0),
            RuntimeNew(1),
            ReplayStart(0),
            ReplayStart(1),
            ReplayFeed(0, Open),
            ReplayFeed(1, Complete),
            ReplayWait(0),
            ReplayPoll(0),
            ReplayReject(0, 0),
            ReplayRejectStale(0),
            ReplayFinalize(0),
            ReplayDrain(0),
            ReplayFinalize(0),
            ReplayWait(1),
            ReplayPoll(1),
            ReplayDispose(1),
            ReplayStart(1),
            ReplayFeed(1, Complete),
            ReplayWait(1),
            ReplayPoll(1),
            RuntimeFree(1),
            ReplayStart(0),
            WorkerStart(0),
            PanicProbe,
            RuntimeNullSlot,
            RuntimeDispose(0),
        ],
    ];
    let mut coverage = Coverage::new();
    for (index, ops) in sequences.iter().enumerate() {
        if let Err(failure) = run_ops(ops, &mut coverage) {
            let mut trace = String::new();
            for line in &failure.trace {
                let _ = writeln!(trace, "  {line}");
            }
            panic!(
                "scripted sequence {index} failed: {}\n{trace}",
                failure.message
            );
        }
    }
    // The scripted outcomes land in this test's own collector, which the
    // generated-coverage assertion never reads; check that they were
    // collected here rather than leaking into shared state.
    assert!(
        coverage.contains_key(&(
            "RuntimeDispose".to_owned(),
            "released with a held lease".to_owned()
        )),
        "scripted regressions did not record their own coverage"
    );
}

//! Seeded multi-thread lifecycle stress across independent and shared
//! runtimes (issue #506).
//!
//! `lifecycle_stress.rs` interleaves two runtime slots on one thread, which
//! matches the single-owner rule but never lets two native graphs change
//! state at the same time. This test runs several OS threads at once. Each
//! thread owns its own graphs, as the ABI requires, but the graphs share
//! process-wide state: Core's Tokio pools (through shared runtimes), the
//! cleanup threads, the replay machinery, and the cleanup counters.
//!
//! Each round:
//!
//! 1. The coordinator creates two shared runtimes.
//! 2. Every worker thread concurrently attaches graphs to those shared
//!    handles (the ABI permits concurrent attaches) and may also create a
//!    graph with its own Core.
//! 3. A start gate releases all threads together. The coordinator then frees or
//!    GC-disposes the shared handles at a seeded moment while the threads
//!    start replay workers, feed histories, lease and complete activations,
//!    finish input, finalize, dispose replay workers, and release their
//!    graphs through `runtime_free` or the GC-fallback `runtime_dispose`,
//!    sometimes with a lease still held.
//!
//! Every call must return a documented status and never `STATUS_PANIC`,
//! `STATUS_INTERNAL`, or `STATUS_INVALID_ARGUMENT` on a live handle. The
//! round deadline starts before any thread is spawned and bounds setup, the
//! start gate, and the run: a thread that hangs or panics during setup aborts
//! the round promptly instead of stalling the others, and a round that does
//! not finish in time is reported as a hang. A second test injects a setup
//! hang and a setup panic to prove that path. After
//! the last round the process-wide counters must converge: every runtime
//! graph cleaned exactly once and every Core runtime (owned or shared)
//! dropped exactly once, whichever holder released it last.
//!
//! The thread schedule is not seeded, so a failure may not reproduce
//! exactly; the report names the seed, round, thread, and per-thread trace.
//! See `docs/reference/bridge-lifecycle-stress.md` for budgets.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    ptr,
    sync::{Arc, Condvar, Mutex, MutexGuard, mpsc},
    thread,
    time::{Duration, Instant},
};

use ocaml_temporal_core_bridge::{
    Result as AbiResult, Runtime, STATUS_INTERNAL, STATUS_INVALID_ARGUMENT, STATUS_NOT_READY,
    STATUS_OK, STATUS_OUTSTANDING_TASKS, STATUS_PANIC, STATUS_WORKER, SharedRuntime, Status,
    ocaml_temporal_core_v4_replay_worker_complete_workflow_json,
    ocaml_temporal_core_v4_replay_worker_dispose,
    ocaml_temporal_core_v4_replay_worker_feed_history_json,
    ocaml_temporal_core_v4_replay_worker_finalize,
    ocaml_temporal_core_v4_replay_worker_finish_input,
    ocaml_temporal_core_v4_replay_worker_start_json,
    ocaml_temporal_core_v4_replay_worker_try_poll_workflow,
    ocaml_temporal_core_v4_replay_worker_wait_workflow, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_dispose, ocaml_temporal_core_v4_runtime_free,
    ocaml_temporal_core_v4_runtime_new, ocaml_temporal_core_v4_runtime_new_attached,
    ocaml_temporal_core_v4_shared_runtime_dispose, ocaml_temporal_core_v4_shared_runtime_free,
    ocaml_temporal_core_v4_shared_runtime_new, test_core_runtime_counts,
    test_runtime_cleanup_counts,
    workflow_protocol::{self, ActivationJob, Completion, CompletionCommand},
};

#[path = "support/replay_fixture.rs"]
#[allow(dead_code)] // Only the completed-history fixture is used here.
mod replay_fixture;

/// Default base seed; record a new one in the docs if it changes.
const DEFAULT_SEED: u64 = 0x0506_c0c0;
/// Default rounds per run.
const DEFAULT_ROUNDS: usize = 3;
/// Default concurrent worker threads per round.
const DEFAULT_THREADS: usize = 4;
/// Default graphs per thread per round.
const DEFAULT_GRAPHS: usize = 3;
/// Shared runtimes the coordinator creates per round.
const SHARED_PER_ROUND: usize = 2;
/// Tokio worker threads of each shared runtime.
const SHARED_WORKER_THREADS: u32 = 2;
/// Longest a round, from before its threads are spawned to the last report,
/// may take before it is reported as a hang.
const ROUND_DEADLINE: Duration = Duration::from_secs(180);
/// Time an aborted round allows the other threads to release their graphs.
const ABORT_GRACE: Duration = Duration::from_secs(30);

/// Serializes the two tests in this binary: the cleanup counters are
/// process-wide, so the fault-injection round must not overlap the stress.
static SERIAL: Mutex<()> = Mutex::new(());

/// Acquires [`SERIAL`] even after the other test panicked.
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|poison| poison.into_inner())
}
/// Longest the cleanup counters may take to converge after the last round.
const CLEANUP_DEADLINE: Duration = Duration::from_secs(60);
/// Pump iterations a full drain may use before it is reported as stuck.
const DRAIN_PUMPS: usize = 200;

/// Replay worker settings used by every graph.
const REPLAY_CONFIG: &[u8] = br#"{"namespace":"concurrent-stress","task_queue":"concurrent-stress","build_id":"concurrent-stress","versioning":{"kind":"none"},"max_cached_workflows":0,"max_outstanding_workflow_tasks":1,"max_concurrent_workflow_task_polls":1,"graceful_shutdown_timeout_ms":1000}"#;

/// Deterministic xorshift stream.
struct Rng(u64);

impl Rng {
    /// Derives an independent stream from the base seed and a label.
    fn derive(seed: u64, label: u64) -> Self {
        let state = (seed ^ label.wrapping_mul(0x9e37_79b9_7f4a_7c15)).rotate_left(17);
        Self(if state == 0 { DEFAULT_SEED } else { state })
    }

    /// Next raw value.
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// Index in `0..bound`; `bound` must be nonzero.
    fn below(&mut self, bound: usize) -> usize {
        let bound64 = u64::try_from(bound).expect("bound fits u64");
        usize::try_from(self.next_u64() % bound64).expect("index fits usize")
    }
}

/// Reads a bounded integer setting.
fn setting(name: &str, default: usize, range: std::ops::RangeInclusive<usize>) -> usize {
    let value = std::env::var(name)
        .ok()
        .filter(|text| !text.is_empty())
        .map(|text| {
            text.parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be an integer"))
        })
        .unwrap_or(default);
    assert!(range.contains(&value), "{name} must be in {range:?}");
    value
}

/// Decimal or `0x` hexadecimal base seed.
fn base_seed() -> u64 {
    let Some(text) = std::env::var("CONCURRENT_STRESS_SEED")
        .ok()
        .filter(|text| !text.is_empty())
    else {
        return DEFAULT_SEED;
    };
    match text.strip_prefix("0x") {
        Some(hex) => {
            u64::from_str_radix(hex, 16).expect("invalid hexadecimal CONCURRENT_STRESS_SEED")
        }
        None => text
            .parse()
            .expect("invalid decimal CONCURRENT_STRESS_SEED"),
    }
}

/// How a graph ends, chosen per graph from the seed.
#[derive(Clone, Copy, Debug)]
enum Script {
    /// Release the graph without starting anything.
    FreeIdle,
    /// Start replay, feed, pump a few times, `runtime_free` (lease may be held).
    FreeWithWork,
    /// As above, but release through the GC-fallback `runtime_dispose`.
    DisposeWithWork,
    /// Feed, finish input, drain every activation, finalize, then free.
    DrainAndFinalize,
    /// Start, finish input, dispose the replay worker, restart, feed, free.
    RestartReplay,
    /// Feed, finish input, pump a little, dispose the replay worker, free.
    DisposeReplayEarly,
}

impl Script {
    /// Every script, for seeded selection.
    const ALL: [Script; 6] = [
        Script::FreeIdle,
        Script::FreeWithWork,
        Script::DisposeWithWork,
        Script::DrainAndFinalize,
        Script::RestartReplay,
        Script::DisposeReplayEarly,
    ];
}

/// One step of a graph script.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// `replay_worker_start_json`.
    Start,
    /// `replay_worker_feed_history_json` with the terminal fixture.
    Feed,
    /// `replay_worker_finish_input`.
    Finish,
    /// One bounded wait, one poll, and completion of any leased activation.
    Pump,
    /// Pump and finalize until finalization succeeds.
    Drain,
    /// `replay_worker_dispose`.
    DisposeReplay,
    /// `runtime_free`; always the last step.
    Free,
    /// `runtime_dispose`; always the last step.
    DisposeRuntime,
}

impl Script {
    /// Expands the script into steps; `pumps` is the seeded pump count.
    fn steps(self, pumps: usize) -> Vec<Step> {
        use Step::*;
        let pumping = std::iter::repeat_n(Pump, pumps);
        match self {
            Script::FreeIdle => vec![Free],
            Script::FreeWithWork => [Start, Feed]
                .into_iter()
                .chain(pumping)
                .chain([Free])
                .collect(),
            Script::DisposeWithWork => [Start, Feed]
                .into_iter()
                .chain(pumping)
                .chain([DisposeRuntime])
                .collect(),
            Script::DrainAndFinalize => vec![Start, Feed, Finish, Drain, Free],
            Script::RestartReplay => vec![Start, Finish, DisposeReplay, Start, Feed, Pump, Free],
            Script::DisposeReplayEarly => [Start, Feed, Finish]
                .into_iter()
                .chain(pumping)
                .chain([DisposeReplay, Free])
                .collect(),
        }
    }
}

/// Raw shared-runtime handle; the coordinator alone releases it, and worker
/// threads only attach before the start gate opens.
#[derive(Clone, Copy)]
struct SharedHandle(*const SharedRuntime);

// SAFETY: concurrent attaches on one live shared handle are permitted by the
// ABI, and the start gate orders every attach before the coordinator's release.
unsafe impl Send for SharedHandle {}
// SAFETY: as above; only attaches read through shared references.
unsafe impl Sync for SharedHandle {}

/// Statuses that indicate a bridge defect wherever they appear.
const FORBIDDEN: [Status; 3] = [STATUS_PANIC, STATUS_INTERNAL, STATUS_INVALID_ARGUMENT];

/// One graph owned by a worker thread.
struct Graph {
    runtime: *mut Runtime,
    steps: Vec<Step>,
    next: usize,
    /// Whether a history was fed to the current replay worker.
    fed: bool,
}

/// Per-thread record of every call and its status.
#[derive(Default)]
struct Trace {
    lines: Vec<String>,
    outcomes: BTreeMap<String, usize>,
}

impl Trace {
    /// Records one call; returns the status for chaining.
    fn record(&mut self, graph: usize, name: &str, status: Status) -> Status {
        self.lines.push(format!("g{graph} {name} -> {status}"));
        *self.outcomes.entry(format!("{name}={status}")).or_default() += 1;
        status
    }
}

/// Calls a no-input runtime entry point and returns its status and value.
fn call_plain(
    runtime: *mut Runtime,
    call: unsafe extern "C" fn(*mut Runtime, *mut AbiResult) -> Status,
) -> (Status, Vec<u8>) {
    let mut result = AbiResult::default();
    // SAFETY: `runtime` is a live handle owned by the calling thread and
    // `result` is initialized storage released below.
    let status = unsafe { call(runtime, &mut result) };
    let value = if result.value.len == 0 {
        Vec::new()
    } else {
        // SAFETY: a nonempty result buffer owns `len` bytes until freed.
        unsafe { std::slice::from_raw_parts(result.value.ptr, result.value.len) }.to_vec()
    };
    free_result(&mut result);
    (status, value)
}

/// Calls a JSON-input runtime entry point and returns its status.
fn call_json(
    runtime: *mut Runtime,
    call: unsafe extern "C" fn(*mut Runtime, *const u8, usize, *mut AbiResult) -> Status,
    input: &[u8],
) -> Status {
    let mut result = AbiResult::default();
    // SAFETY: as in `call_plain`; `input` outlives the synchronous call.
    let status = unsafe { call(runtime, input.as_ptr(), input.len(), &mut result) };
    free_result(&mut result);
    status
}

/// Releases one result and asserts the release succeeded.
fn free_result(result: &mut AbiResult) {
    // SAFETY: the result was initialized by the bridge and is freed once.
    let status = unsafe { ocaml_temporal_core_v4_result_free(result) };
    assert_eq!(status, STATUS_OK, "result_free");
}

/// Fails the step when `status` is outside `allowed` or forbidden anywhere.
fn require(name: &str, status: Status, allowed: &[Status]) -> Result<(), String> {
    if FORBIDDEN.contains(&status) || !allowed.contains(&status) {
        return Err(format!(
            "{name} returned {status}, expected one of {allowed:?}"
        ));
    }
    Ok(())
}

impl Graph {
    /// Waits once, polls once, and completes a leased activation.
    fn pump(&mut self, index: usize, trace: &mut Trace) -> Result<(), String> {
        let wait = trace.record(
            index,
            "wait",
            call_plain(
                self.runtime,
                ocaml_temporal_core_v4_replay_worker_wait_workflow,
            )
            .0,
        );
        require("wait", wait, &[STATUS_OK, STATUS_NOT_READY, STATUS_WORKER])?;
        let (status, value) = call_plain(
            self.runtime,
            ocaml_temporal_core_v4_replay_worker_try_poll_workflow,
        );
        trace.record(index, "poll", status);
        require(
            "poll",
            status,
            &[STATUS_OK, STATUS_NOT_READY, STATUS_WORKER],
        )?;
        if status != STATUS_OK || value.is_empty() {
            return Ok(());
        }
        let text = std::str::from_utf8(&value).map_err(|_| "activation is not UTF-8")?;
        let activation = workflow_protocol::decode_activation(text)
            .map_err(|error| format!("activation does not decode: {error:?}"))?;
        let eviction = activation
            .jobs
            .iter()
            .any(|job| matches!(job, ActivationJob::RemoveFromCache { .. }));
        // An eviction is acknowledged empty; a workflow task completes the
        // replayed workflow, matching the fixture's terminal event.
        let commands = if eviction {
            Vec::new()
        } else {
            vec![CompletionCommand::CompleteWorkflow { result: None }]
        };
        let document = workflow_protocol::encode_completion(&Completion {
            run_id: activation.run_id,
            commands,
            task_failure: None,
        })
        .map_err(|error| format!("completion encoding: {error:?}"))?;
        let status = trace.record(
            index,
            "complete",
            call_json(
                self.runtime,
                ocaml_temporal_core_v4_replay_worker_complete_workflow_json,
                document.as_bytes(),
            ),
        );
        require("complete", status, &[STATUS_OK])
    }

    /// Runs the next step. Returns `true` once the graph has been released.
    fn step(&mut self, index: usize, trace: &mut Trace) -> Result<bool, String> {
        let step = self.steps[self.next];
        self.next += 1;
        match step {
            Step::Start => {
                let status = trace.record(
                    index,
                    "start",
                    call_json(
                        self.runtime,
                        ocaml_temporal_core_v4_replay_worker_start_json,
                        REPLAY_CONFIG,
                    ),
                );
                require("start", status, &[STATUS_OK])?;
                self.fed = false;
            }
            Step::Feed => {
                let history = replay_fixture::complete_history_document(&format!(
                    "concurrent-stress-{index}"
                ));
                let status = trace.record(
                    index,
                    "feed",
                    call_json(
                        self.runtime,
                        ocaml_temporal_core_v4_replay_worker_feed_history_json,
                        history.as_bytes(),
                    ),
                );
                require("feed", status, &[STATUS_OK])?;
                self.fed = true;
            }
            Step::Finish => {
                let status = trace.record(
                    index,
                    "finish",
                    call_plain(
                        self.runtime,
                        ocaml_temporal_core_v4_replay_worker_finish_input,
                    )
                    .0,
                );
                require("finish", status, &[STATUS_OK])?;
            }
            Step::Pump => self.pump(index, trace)?,
            Step::Drain => {
                for _ in 0..DRAIN_PUMPS {
                    self.pump(index, trace)?;
                    let status = trace.record(
                        index,
                        "finalize",
                        call_plain(self.runtime, ocaml_temporal_core_v4_replay_worker_finalize).0,
                    );
                    require("finalize", status, &[STATUS_OK, STATUS_OUTSTANDING_TASKS])?;
                    if status == STATUS_OK {
                        return Ok(false);
                    }
                }
                return Err(format!(
                    "replay did not finalize after {DRAIN_PUMPS} pumps (fed={})",
                    self.fed
                ));
            }
            Step::DisposeReplay => {
                let status = trace.record(
                    index,
                    "dispose_replay",
                    call_plain(self.runtime, ocaml_temporal_core_v4_replay_worker_dispose).0,
                );
                // Disposal retains the worker if Core cannot finish yet.
                require(
                    "dispose_replay",
                    status,
                    &[STATUS_OK, STATUS_OUTSTANDING_TASKS, STATUS_WORKER],
                )?;
                if status != STATUS_OK {
                    // The restart script needs an empty slot; skip its
                    // second start rather than expect a refusal.
                    while self.next < self.steps.len()
                        && matches!(self.steps[self.next], Step::Start | Step::Feed | Step::Pump)
                    {
                        self.next += 1;
                    }
                }
            }
            Step::Free => {
                // SAFETY: the slot holds this thread's live handle.
                let status = unsafe { ocaml_temporal_core_v4_runtime_free(&mut self.runtime) };
                trace.record(index, "runtime_free", status);
                require("runtime_free", status, &[STATUS_OK])?;
                if !self.runtime.is_null() {
                    return Err("runtime_free left the slot non-null".to_owned());
                }
                return Ok(true);
            }
            Step::DisposeRuntime => {
                // SAFETY: the slot holds this thread's live handle.
                let status = unsafe { ocaml_temporal_core_v4_runtime_dispose(&mut self.runtime) };
                trace.record(index, "runtime_dispose", status);
                require("runtime_dispose", status, &[STATUS_OK])?;
                if !self.runtime.is_null() {
                    return Err("runtime_dispose left the slot non-null".to_owned());
                }
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// What a worker thread reports back to the coordinator.
struct ThreadReport {
    thread: usize,
    error: Option<String>,
    trace: Trace,
}

/// Messages a worker thread sends to the coordinator. Every send ignores a
/// closed channel, because the coordinator may already have given up.
enum Event {
    /// Setup finished; `owned` of the thread's graphs built their own Core.
    Ready { owned: usize },
    /// The thread finished, failed, or panicked. A thread that never became
    /// ready sends only this.
    Done(ThreadReport),
}

/// State of the start gate that replaces a blocking barrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GateState {
    /// The coordinator is still collecting `Ready` events.
    Waiting,
    /// Every thread is ready and no attach can still be running.
    Go,
    /// Setup failed or timed out; threads stop creating graphs, release
    /// what they hold, and report.
    Abort,
}

/// Start gate shared by the coordinator and its worker threads. Unlike
/// `std::sync::Barrier`, every wait is bounded by the round deadline and the
/// coordinator can abort it, so a thread that hangs or panics during setup
/// cannot stall the others.
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl Gate {
    /// Creates a closed gate.
    fn new() -> Self {
        Self {
            state: Mutex::new(GateState::Waiting),
            changed: Condvar::new(),
        }
    }

    /// Opens (`Go`) or aborts the gate and wakes every waiter. An abort is
    /// final; a later `Go` cannot reopen it.
    fn set(&self, next: GateState) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if *state != GateState::Abort {
            *state = next;
        }
        self.changed.notify_all();
    }

    /// Current state without waiting.
    fn get(&self) -> GateState {
        *self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Waits until the gate leaves `Waiting` or `deadline` passes; a
    /// timeout is reported as `Abort`.
    fn wait(&self, deadline: Instant) -> GateState {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while *state == GateState::Waiting {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return GateState::Abort;
            }
            state = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poison| poison.into_inner())
                .0;
        }
        *state
    }
}

/// Test-only fault injected into one worker's setup, so the deadline
/// handling itself is tested. The ordinary stress never injects anything.
#[derive(Clone, Copy, Debug)]
enum SetupFault {
    /// The thread sleeps for this long before creating any graph.
    Hang(usize, Duration),
    /// The thread panics before creating any graph.
    Panic(usize),
}

/// Fixed inputs of one round.
#[derive(Clone, Copy)]
struct RoundPlan {
    seed: u64,
    round: usize,
    threads: usize,
    graphs: usize,
    /// Whole-round budget, measured from before the threads are spawned.
    deadline: Duration,
    /// Extra time after an abort for the remaining threads to release their
    /// graphs and report, so the shared handles can be released safely.
    grace: Duration,
    fault: Option<SetupFault>,
    /// Whether a failure writes a report file; the fault-injection test
    /// keeps its expected failures out of the CI artifact directory.
    write_reports: bool,
}

/// Creates the thread's graphs (or stops early on an abort), reports
/// readiness, waits at the gate, then runs every graph's script with seeded
/// interleaving.
fn worker(
    thread: usize,
    mut rng: Rng,
    shared: Vec<SharedHandle>,
    plan: RoundPlan,
    gate: &Gate,
    deadline: Instant,
    events: &mpsc::Sender<Event>,
) -> ThreadReport {
    match plan.fault {
        Some(SetupFault::Hang(target, duration)) if target == thread => thread::sleep(duration),
        Some(SetupFault::Panic(target)) if target == thread => {
            panic!("injected setup panic in thread {thread}")
        }
        _ => {}
    }
    let mut trace = Trace::default();
    let mut live: Vec<Graph> = Vec::new();
    let mut owned = 0;
    let mut setup_error = None;
    for _ in 0..plan.graphs {
        // An aborted round must not attach any more: the coordinator may
        // release the shared handles as soon as every thread has reported.
        if gate.get() == GateState::Abort {
            setup_error = Some("setup aborted by the coordinator".to_owned());
            break;
        }
        let mut runtime: *mut Runtime = ptr::null_mut();
        let mut result = AbiResult::default();
        // One graph in four builds its own Core; the rest attach.
        let status = if rng.below(4) == 0 {
            owned += 1;
            // SAFETY: both outputs are writable and owned by this thread.
            unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) }
        } else {
            let handle = shared[rng.below(shared.len())];
            // SAFETY: the coordinator releases a shared handle only after
            // every thread is ready or has reported, so the handle is live;
            // concurrent attaches on one handle are permitted.
            unsafe {
                ocaml_temporal_core_v4_runtime_new_attached(handle.0, &mut runtime, &mut result)
            }
        };
        free_result(&mut result);
        trace.record(live.len(), "create", status);
        if status != STATUS_OK || runtime.is_null() {
            setup_error = Some(format!("graph creation returned {status}"));
            break;
        }
        let script = Script::ALL[rng.below(Script::ALL.len())];
        let pumps = rng.below(4);
        trace
            .lines
            .push(format!("g{} script {script:?} pumps {pumps}", live.len()));
        live.push(Graph {
            runtime,
            steps: script.steps(pumps),
            next: 0,
            fed: false,
        });
    }
    if let Some(error) = setup_error {
        release_all(&mut live, &mut trace);
        return ThreadReport {
            thread,
            error: Some(error),
            trace,
        };
    }
    let _ = events.send(Event::Ready { owned });
    // The coordinator always opens or aborts the gate by the deadline; the
    // extra grace keeps this thread's own timeout from racing that decision,
    // so it fires only if the coordinator itself is gone.
    if gate.wait(deadline + plan.grace) != GateState::Go {
        release_all(&mut live, &mut trace);
        return ThreadReport {
            thread,
            error: Some("start gate aborted or timed out".to_owned()),
            trace,
        };
    }
    let mut pending: Vec<usize> = (0..live.len()).collect();
    while !pending.is_empty() {
        let choice = rng.below(pending.len());
        let index = pending[choice];
        match live[index].step(index, &mut trace) {
            Ok(true) => {
                pending.swap_remove(choice);
            }
            Ok(false) => {}
            Err(error) => {
                release_all(&mut live, &mut trace);
                return ThreadReport {
                    thread,
                    error: Some(format!("g{index}: {error}")),
                    trace,
                };
            }
        }
    }
    ThreadReport {
        thread,
        error: None,
        trace,
    }
}

/// Releases every still-live graph after a failure so the process can
/// report instead of leaking native threads.
fn release_all(graphs: &mut [Graph], trace: &mut Trace) {
    for (index, graph) in graphs.iter_mut().enumerate() {
        if !graph.runtime.is_null() {
            // SAFETY: the slot holds this thread's live handle.
            let status = unsafe { ocaml_temporal_core_v4_runtime_free(&mut graph.runtime) };
            trace.record(index, "cleanup_free", status);
        }
    }
}

/// Writes a failure report and returns its path.
fn write_report(seed: u64, round: usize, text: &str) -> PathBuf {
    let directory = std::env::var_os("CONCURRENT_STRESS_OUTPUT_DIR")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../_build/concurrent-stress")
        });
    fs::create_dir_all(&directory).expect("create concurrent stress report directory");
    let path = directory.join(format!("{seed:016x}-round{round:03}.txt"));
    fs::write(&path, text).expect("write concurrent stress report");
    path
}

/// Adds one thread's outcome counts to the round totals and its failure,
/// if any, to `failures`.
fn absorb(report: ThreadReport, failures: &mut String, outcomes: &mut BTreeMap<String, usize>) {
    for (outcome, count) in &report.trace.outcomes {
        *outcomes.entry(outcome.clone()).or_default() += count;
    }
    describe(failures, &report);
}

/// Appends one thread's failure and trace to `failures`.
fn describe(failures: &mut String, report: &ThreadReport) {
    if let Some(error) = &report.error {
        let _ = writeln!(failures, "thread {}: {error}", report.thread);
        for line in &report.trace.lines {
            let _ = writeln!(failures, "  {line}");
        }
    }
}

/// Releases the round's shared handles at the coordinator.
fn release_shared(shared: &mut [*mut SharedRuntime], rng: &mut Rng) {
    for handle in shared {
        let dispose = rng.below(2) == 0;
        // SAFETY: the coordinator owns the slot, and the caller guarantees no
        // attach can still be running on it.
        let status = unsafe {
            if dispose {
                ocaml_temporal_core_v4_shared_runtime_dispose(handle)
            } else {
                ocaml_temporal_core_v4_shared_runtime_free(handle)
            }
        };
        assert_eq!(status, STATUS_OK, "shared release (dispose={dispose})");
        assert!(handle.is_null(), "shared release clears the slot");
    }
}

/// Runs one round. Returns the number of Core runtimes it created, or a
/// report (also written to disk) when a thread failed, panicked, or missed
/// the deadline.
///
/// The deadline starts before any thread is spawned and bounds setup, the
/// start gate, and the run. If setup fails or times out, the gate is
/// aborted and the coordinator waits at most `grace` longer for the other
/// threads to release their graphs. It releases the shared handles only
/// when every thread has reported, because a hung thread might still be
/// inside `runtime_new_attached` on them; otherwise the handles are leaked
/// and the report says so.
fn run_round(plan: RoundPlan, outcomes: &mut BTreeMap<String, usize>) -> Result<u64, String> {
    let RoundPlan {
        seed,
        round,
        threads,
        ..
    } = plan;
    let started = Instant::now();
    let deadline = started + plan.deadline;
    let mut rng = Rng::derive(seed, u64::try_from(round).expect("round fits u64"));
    let mut shared: Vec<*mut SharedRuntime> = Vec::new();
    for _ in 0..SHARED_PER_ROUND {
        let mut handle: *mut SharedRuntime = ptr::null_mut();
        let mut result = AbiResult::default();
        // SAFETY: both outputs are writable and owned by the coordinator.
        let status = unsafe {
            ocaml_temporal_core_v4_shared_runtime_new(
                SHARED_WORKER_THREADS,
                &mut handle,
                &mut result,
            )
        };
        free_result(&mut result);
        assert_eq!(status, STATUS_OK, "shared_runtime_new");
        shared.push(handle);
    }
    let handles: Vec<SharedHandle> = shared.iter().map(|handle| SharedHandle(*handle)).collect();
    let gate = Arc::new(Gate::new());
    let (events_tx, events_rx) = mpsc::channel();
    for thread in 0..threads {
        let thread_rng = Rng::derive(rng.next_u64(), u64::try_from(thread).expect("fits"));
        let handles = handles.clone();
        let gate = Arc::clone(&gate);
        let events = events_tx.clone();
        thread::Builder::new()
            .name(format!("concurrent-stress-{round}-{thread}"))
            .spawn(move || {
                // A panic anywhere in the worker becomes a prompt failure
                // report instead of a silent missing thread.
                let report = catch_unwind(AssertUnwindSafe(|| {
                    worker(thread, thread_rng, handles, plan, &gate, deadline, &events)
                }))
                .unwrap_or_else(|panic| ThreadReport {
                    thread,
                    error: Some(format!("thread panicked: {}", panic_text(panic.as_ref()))),
                    trace: Trace::default(),
                });
                let _ = events.send(Event::Done(report));
            })
            .expect("spawn stress thread");
    }
    drop(events_tx);

    // Setup: wait, bounded by the deadline, until every thread is ready or
    // has reported. Any early report is a failure and aborts the round.
    let mut ready = 0;
    let mut owned = 0;
    let mut reported = 0;
    let mut failures = String::new();
    while ready + reported < threads {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match events_rx.recv_timeout(remaining) {
            Ok(Event::Ready { owned: count }) => {
                ready += 1;
                owned += count;
            }
            Ok(Event::Done(report)) => {
                reported += 1;
                if report.error.is_none() {
                    let _ = writeln!(
                        failures,
                        "thread {} finished before the gate",
                        report.thread
                    );
                }
                absorb(report, &mut failures, outcomes);
                break;
            }
            Err(_) => {
                let _ = writeln!(
                    failures,
                    "setup did not finish within {:?}: {ready} of {threads} threads ready",
                    plan.deadline
                );
                break;
            }
        }
    }

    if !failures.is_empty() {
        // Abort: ready threads release their graphs; a thread still in setup
        // stops before its next attach. Collect reports within the grace.
        gate.set(GateState::Abort);
        let grace = Instant::now() + plan.grace;
        while reported < threads {
            let remaining = grace.saturating_duration_since(Instant::now());
            match events_rx.recv_timeout(remaining) {
                Ok(Event::Ready { .. }) => {}
                Ok(Event::Done(report)) => {
                    reported += 1;
                    absorb(report, &mut failures, outcomes);
                }
                Err(_) => break,
            }
        }
        if reported == threads {
            release_shared(&mut shared, &mut rng);
        } else {
            let _ = writeln!(
                failures,
                "{} thread(s) never reported; the shared runtimes are leaked",
                threads - reported
            );
        }
        return Err(fail(plan, started, &failures));
    }

    gate.set(GateState::Go);
    // Release the shared handles at a seeded moment while the graphs run.
    // Every attach finished before its thread became ready.
    thread::sleep(Duration::from_millis(
        u64::try_from(rng.below(20)).expect("fits"),
    ));
    release_shared(&mut shared, &mut rng);

    while reported < threads {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match events_rx.recv_timeout(remaining) {
            Ok(Event::Ready { .. }) => {}
            Ok(Event::Done(report)) => {
                reported += 1;
                absorb(report, &mut failures, outcomes);
            }
            Err(_) => {
                let _ = writeln!(
                    failures,
                    "round exceeded its {:?} deadline with {} thread(s) still running",
                    plan.deadline,
                    threads - reported
                );
                return Err(fail(plan, started, &failures));
            }
        }
    }
    if !failures.is_empty() {
        return Err(fail(plan, started, &failures));
    }
    Ok(u64::try_from(owned + SHARED_PER_ROUND).expect("fits"))
}

/// Formats and writes a round failure report, returning its text.
fn fail(plan: RoundPlan, started: Instant, failures: &str) -> String {
    let RoundPlan {
        seed,
        round,
        threads,
        graphs,
        ..
    } = plan;
    let text = format!(
        "seed {seed:#x} round {round} threads {threads} graphs {graphs} after {:?}\n\
         rerun: make native-test-concurrent-stress CONCURRENT_STRESS_SEED={seed:#x}\n{failures}",
        started.elapsed()
    );
    if !plan.write_reports {
        return text;
    }
    let path = write_report(seed, round, &text);
    format!("{text}report {}", path.display())
}

/// Extracts the message of a caught panic payload.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

/// Source locations of every panic observed on any thread during the run,
/// including Tokio worker threads whose panics never reach the ABI.
static PANICS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Installs a panic hook that records each panic's location before running
/// the default hook, so contained panics on Core's Tokio threads are counted
/// rather than lost in the log.
fn record_panics() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info.location().map_or_else(
            || "<unknown>".to_owned(),
            |at| format!("{}:{}", at.file(), at.line()),
        );
        PANICS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(location);
        default(info);
    }));
}

/// Whether a panic location is inside this bridge's own sources.
fn is_bridge_source(location: &str) -> bool {
    let normalized = location.replace('\\', "/");
    normalized.contains("core-bridge/src/")
}

/// Polls until `done` holds or the cleanup deadline passes.
fn eventually(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + CLEANUP_DEADLINE;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    done()
}

/// The seeded concurrent rounds, followed by the process-wide convergence
/// checks. The only other test in this binary holds [`SERIAL`] too, so
/// nothing else moves the counters while they are measured.
#[test]
fn concurrent_lifecycle_rounds_converge() {
    let _serial = serial();
    record_panics();
    let seed = base_seed();
    let rounds = setting("CONCURRENT_STRESS_ROUNDS", DEFAULT_ROUNDS, 1..=10_000);
    let threads = setting("CONCURRENT_STRESS_THREADS", DEFAULT_THREADS, 2..=64);
    let graphs = setting("CONCURRENT_STRESS_GRAPHS", DEFAULT_GRAPHS, 1..=32);
    let (runtimes_created_before, runtimes_cleaned_before) = test_runtime_cleanup_counts();
    let (_, cores_dropped_before) = test_core_runtime_counts();
    let mut outcomes = BTreeMap::new();
    let mut cores = 0;
    let started = Instant::now();
    for round in 0..rounds {
        let plan = RoundPlan {
            seed,
            round,
            threads,
            graphs,
            deadline: ROUND_DEADLINE,
            grace: ABORT_GRACE,
            fault: None,
            write_reports: true,
        };
        match run_round(plan, &mut outcomes) {
            Ok(created) => cores += created,
            Err(report) => panic!("{report}"),
        }
    }

    // Every graph was released, so every graph's cleanup and every Core
    // drop must eventually complete exactly once.
    let converged = eventually(|| {
        let (created, cleaned) = test_runtime_cleanup_counts();
        created - runtimes_created_before == cleaned - runtimes_cleaned_before
    });
    let (created, cleaned) = test_runtime_cleanup_counts();
    assert!(
        converged,
        "runtime cleanup did not converge: created {} cleaned {}",
        created - runtimes_created_before,
        cleaned - runtimes_cleaned_before
    );
    let graph_count = u64::try_from(rounds * threads * graphs).expect("fits");
    assert_eq!(
        created - runtimes_created_before,
        graph_count,
        "every graph was created through runtime_new or runtime_new_attached"
    );
    let cores_converged =
        eventually(|| test_core_runtime_counts().1 - cores_dropped_before == cores);
    assert!(
        cores_converged,
        "Core drops did not converge: expected {cores}, observed {}",
        test_core_runtime_counts().1 - cores_dropped_before
    );
    // Drops past the expected count would mean a double destruction.
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        test_core_runtime_counts().1 - cores_dropped_before,
        cores,
        "a Core runtime was dropped more than once"
    );

    // A panic in bridge code is a defect even on a Tokio thread where the
    // ABI's catch_unwind cannot see it. Panics inside Temporal Core during
    // runtime teardown are contained by Tokio and reported, not failed; see
    // the "Findings" section of docs/reference/bridge-lifecycle-stress.md.
    let panics = PANICS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let bridge: Vec<&String> = panics.iter().filter(|at| is_bridge_source(at)).collect();
    assert!(bridge.is_empty(), "bridge code panicked: {bridge:?}");
    let mut contained: BTreeMap<&str, usize> = BTreeMap::new();
    for location in &panics {
        *contained.entry(location.as_str()).or_default() += 1;
    }

    let mut summary = String::new();
    for (outcome, count) in &outcomes {
        let _ = write!(summary, " {outcome}:{count}");
    }
    eprintln!(
        "concurrent stress: seed={seed:#x} rounds={rounds} threads={threads} graphs={graphs} \
         cores={cores} elapsed={:?} contained_upstream_panics={contained:?}\n {summary}",
        started.elapsed()
    );
    // With the default budget the important paths must appear, so a change
    // to the generator cannot reduce the stress to idle graphs.
    if seed == DEFAULT_SEED && rounds >= DEFAULT_ROUNDS && threads >= DEFAULT_THREADS {
        for required in [
            "complete=0",
            "finalize=0",
            "runtime_dispose=0",
            "runtime_free=0",
        ] {
            assert!(
                outcomes.contains_key(required),
                "default run never observed {required}"
            );
        }
    }
}

/// A worker that hangs or panics during setup is reported within the round
/// deadline instead of stalling the coordinator and the other threads at
/// the start gate. The faults are injected only here; the ordinary stress
/// never sets [`RoundPlan::fault`]. Every graph the healthy threads created
/// is released and both shared runtimes are freed, so the round leaks
/// nothing.
#[test]
fn setup_faults_are_reported_within_the_deadline() {
    let _serial = serial();
    let deadline = Duration::from_secs(1);
    let hang = Duration::from_secs(3);
    for (fault, expected, bound) in [
        (
            SetupFault::Hang(0, hang),
            "setup did not finish within",
            deadline + hang + Duration::from_secs(10),
        ),
        (
            SetupFault::Panic(1),
            "injected setup panic",
            deadline + Duration::from_secs(10),
        ),
    ] {
        let (created_before, cleaned_before) = test_runtime_cleanup_counts();
        let plan = RoundPlan {
            seed: DEFAULT_SEED,
            round: 0,
            threads: 3,
            graphs: 2,
            deadline,
            grace: Duration::from_secs(30),
            fault: Some(fault),
            write_reports: false,
        };
        let started = Instant::now();
        let report = run_round(plan, &mut BTreeMap::new())
            .expect_err("an injected setup fault must fail the round");
        let elapsed = started.elapsed();
        assert!(report.contains(expected), "{fault:?} report: {report}");
        assert!(!report.contains("leaked"), "{fault:?} leaked: {report}");
        assert!(
            elapsed < bound,
            "{fault:?} took {elapsed:?}, bound {bound:?}"
        );
        let (created, cleaned) = test_runtime_cleanup_counts();
        assert_eq!(
            created - created_before,
            cleaned - cleaned_before,
            "{fault:?}: every graph created before the abort was released"
        );
    }
}

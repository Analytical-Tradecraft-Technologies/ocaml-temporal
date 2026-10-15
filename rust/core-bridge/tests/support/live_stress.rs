//! Concurrent live workers and clients (#506), part of `lifecycle_stress.rs`.
//!
//! The generated cases in the parent module drive live workers from one
//! thread, and `concurrent_lifecycle_stress.rs` runs replay workers only. This
//! module runs live activity workers and submitted client calls on several
//! threads at once against the parent's gRPC double:
//!
//! - activity polling, worker completions and rejections, and worker
//!   heartbeats that the double answers with `cancel_requested` for every
//!   third token, after which Core delivers a Cancel task that the test
//!   completes as cancelled;
//! - handoff to asynchronous completion (`will_complete_async`), then
//!   throttled async heartbeats and async completions submitted as client
//!   calls (#1025), through any graph of the thread that runs a worker;
//! - signals, workflow cancellation requests, and exact-run waits the double
//!   holds open forever, awaited from the submitting thread and sometimes
//!   released before their outcome (#1019);
//! - worker initiate-shutdown (#1028), shutdown with and without a held
//!   lease, and client disconnect;
//! - graphs that build their own Core runtime or attach to the round's
//!   shared runtime, which the coordinator frees or disposes at a seeded
//!   moment while the graphs run, and graphs released on their own thread
//!   through `runtime_free` or the GC-fallback `runtime_dispose`, or handed
//!   to a reaper thread that releases them while their owner still awaits
//!   their calls.
//!
//! Every thread owns its graphs, as the ABI's single-owner rule requires;
//! ownership moves to the reaper as a whole. Calls are checked against a
//! model of each graph, and the round's evidence against process-wide
//! ledgers: runtime cleanup and Core drop counters, the client call
//! registry, the double's completion, heartbeat, and long-poll counters, and
//! a panic hook that sees panics on every thread, including Core's Tokio
//! threads.

use super::*;
use ocaml_temporal_core_bridge::{
    SharedRuntime, ocaml_temporal_core_v4_client_await_call,
    ocaml_temporal_core_v4_client_release_call, ocaml_temporal_core_v4_client_submit_json,
    ocaml_temporal_core_v4_runtime_new_attached, ocaml_temporal_core_v4_shared_runtime_dispose,
    ocaml_temporal_core_v4_shared_runtime_free, ocaml_temporal_core_v4_shared_runtime_new,
    ocaml_temporal_core_v4_worker_initiate_shutdown,
    ocaml_temporal_core_v4_worker_record_activity_heartbeat_json, test_core_runtime_counts,
    test_heartbeat_throttle_counts, test_registered_client_calls,
};
use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Selector of a submitted exact-run wait (`OCAML_TEMPORAL_CORE_CLIENT_CALL_WAIT`).
const CALL_WAIT: u32 = 2;
/// Selector of a submitted workflow cancellation request.
const CALL_CANCEL: u32 = 3;
/// Selector of a submitted signal.
const CALL_SIGNAL: u32 = 6;
/// Selector of a submitted async activity completion.
const CALL_ASYNC_COMPLETION: u32 = 11;
/// Selector of a submitted async activity heartbeat.
const CALL_ASYNC_HEARTBEAT: u32 = 12;

/// Default rounds in the ordinary test run.
const DEFAULT_LIVE_ROUNDS: usize = 3;
/// Default worker threads per round.
const DEFAULT_LIVE_THREADS: usize = 4;
/// Default upper bound of operations per graph; each graph draws its own
/// count between half of this and this.
const DEFAULT_LIVE_STEPS: usize = 48;
/// Graphs each worker thread owns.
const LIVE_GRAPHS: usize = 2;
/// Salt that separates this stress's random streams from the generated
/// cases' streams under the same `LIFECYCLE_STRESS_SEED`.
const LIVE_SEED_SALT: u64 = 0x0506_11fe_5eed_0001;
/// Longest one round may take, from before its threads are spawned to the
/// last report, before it is reported as a hang.
const LIVE_ROUND_DEADLINE: Duration = Duration::from_secs(180);
/// Bound on one worker shutdown or waiting `runtime_free`. The bridge's own
/// drain and finalization bounds are well below it, so this fails a
/// shutdown that is merely slow, not only one that hangs.
const SHUTDOWN_BOUND: Duration = Duration::from_secs(20);
/// Bound for every call of a released graph to reach a terminal outcome,
/// and for the round's server-side evidence and counters to converge.
const CALL_SETTLE: Duration = Duration::from_secs(30);
/// Interval of one bounded `client_await_call` while the graphs run.
const AWAIT_SLICE_MS: u32 = 20;
/// Tokio worker threads of the round's shared Core runtime.
const SHARED_CORE_THREADS: u32 = 2;
/// Statuses that are a bridge defect wherever they appear here: every call
/// passes a live handle, so even `STATUS_INVALID_ARGUMENT` would be one.
const LIVE_FORBIDDEN: [Status; 3] = [STATUS_PANIC, STATUS_INTERNAL, STATUS_INVALID_ARGUMENT];
/// Cancelled activity result, as the OCaml executor reports a cooperative
/// cancellation.
const CANCELLED_RESULT: &str = r#"{"kind":"cancelled","failure":{"message":"cancelled","source":"","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"canceled","details":[],"identity":""}}}"#;
/// Successful async completion result with an empty payload.
const COMPLETED_RESULT: &str =
    r#"{"kind":"completed","result":{"metadata":{},"data":{"encoding":"base64","data":""}}}"#;
/// Outcomes the default seed and budget must reach at least once, so a
/// change to the operation mix cannot quietly drop a path.
const LIVE_REQUIRED_COVERAGE: &[(&str, &str)] = &[
    ("poll_activity", "start"),
    ("poll_activity", "cancel for the held lease"),
    ("complete_activity", "OK"),
    ("complete_cancelled", "OK"),
    ("reject_activity", "OK"),
    ("record_heartbeat", "OK"),
    ("hand_off_async", "OK"),
    ("submit_async_heartbeat", "OK"),
    ("submit_async_heartbeat", "INVALID_STATE"),
    ("submit_async_completion", "OK"),
    ("await_async_heartbeat", "cancel_requested"),
    ("await_async_completion", "OK"),
    ("await_signal", "OK"),
    ("await_cancel_workflow", "OK"),
    ("release_call", "OK"),
    ("await_released_wait", "INVALID_STATE"),
    ("initiate_shutdown", "OK"),
    ("worker_shutdown", "OK"),
    ("worker_shutdown", "OUTSTANDING_TASKS"),
    ("disconnect", "OK"),
    ("disconnect", "INVALID_STATE"),
    ("runtime_free", "OK"),
    ("runtime_dispose", "OK"),
    ("reaper_free", "OK"),
    ("reaper_dispose", "OK"),
];

/// Budget of the stress, read once from the environment.
struct LiveConfig {
    /// Base seed, shared with the generated cases.
    seed: u64,
    /// Rounds to run.
    rounds: usize,
    /// Worker threads per round.
    threads: usize,
    /// Upper bound of operations per graph.
    steps: usize,
    /// When set, only this round runs (reproduction mode).
    only_round: Option<usize>,
}

/// Reads one bounded `usize` setting.
fn live_setting(name: &str, default: usize, range: std::ops::RangeInclusive<usize>) -> usize {
    let value = env_u64(name).map_or(default, |value| {
        usize::try_from(value).unwrap_or_else(|_| panic!("{name} must fit usize"))
    });
    assert!(range.contains(&value), "{name} must be in {range:?}");
    value
}

impl LiveConfig {
    /// Reads `LIFECYCLE_STRESS_SEED` and
    /// `LIFECYCLE_STRESS_LIVE_{ROUNDS,THREADS,STEPS,ROUND}`.
    fn from_env() -> Self {
        Self {
            seed: env_u64("LIFECYCLE_STRESS_SEED").unwrap_or(DEFAULT_SEED),
            rounds: live_setting(
                "LIFECYCLE_STRESS_LIVE_ROUNDS",
                DEFAULT_LIVE_ROUNDS,
                1..=10_000,
            ),
            threads: live_setting(
                "LIFECYCLE_STRESS_LIVE_THREADS",
                DEFAULT_LIVE_THREADS,
                2..=32,
            ),
            steps: live_setting("LIFECYCLE_STRESS_LIVE_STEPS", DEFAULT_LIVE_STEPS, 2..=2_000),
            only_round: env_u64("LIFECYCLE_STRESS_LIVE_ROUND").map(|round| {
                usize::try_from(round).expect("LIFECYCLE_STRESS_LIVE_ROUND must fit usize")
            }),
        }
    }

    /// Whether this is at least the default budget with the default seed,
    /// which must reach every outcome in [`LIVE_REQUIRED_COVERAGE`].
    fn is_default(&self) -> bool {
        self.seed == DEFAULT_SEED
            && self.rounds >= DEFAULT_LIVE_ROUNDS
            && self.threads >= DEFAULT_LIVE_THREADS
            && self.steps >= DEFAULT_LIVE_STEPS
            && self.only_round.is_none()
    }
}

/// What the server must eventually have seen for one leased task token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fate {
    /// Still leased to a graph; never left once its thread has finished.
    Held,
    /// Retired through a path that owes the server exactly one completion:
    /// worker completion or rejection, force-completion at shutdown or
    /// release, or an async completion whose outcome was read as accepted.
    Once,
    /// Handed off to async completion without a confirmed completion: the
    /// call was never made, was released, or was closed with its runtime,
    /// so the server may see one completion or none, never two.
    AtMostOnce,
}

/// One activity task leased to a graph.
struct LiveLease {
    /// Raw token bytes, the server ledger's key.
    token: Vec<u8>,
    /// Canonical base64 token used in semantic documents.
    text: String,
    /// Exact task document, used for rejection.
    document: Vec<u8>,
    /// Whether Core delivered a Cancel task for this lease.
    cancel_requested: bool,
    /// Whether a worker heartbeat was recorded and the double will answer it
    /// with `cancel_requested`, so Core is about to deliver a Cancel task.
    cancel_expected: bool,
}

/// A live worker started on a graph.
struct LiveWorker {
    /// The single lease the worker's one activity slot allows.
    lease: Option<LiveLease>,
    /// Whether `worker_initiate_shutdown` was called.
    initiated: bool,
}

/// One runtime graph owned by a worker thread.
struct LiveGraph {
    /// Live handle, or null after release (by this thread or the reaper).
    runtime: *mut Runtime,
    /// Whether a client is connected.
    client: bool,
    /// The live worker, when one runs.
    worker: Option<LiveWorker>,
    /// Every token this graph ever leased, for the failure report of a
    /// misdirected Cancel task.
    leased: HashSet<Vec<u8>>,
    /// Operations left before the graph is released.
    steps_left: usize,
}

/// A token handed off with `will_complete_async`. It can be completed
/// through any graph of the thread that runs a worker, since every worker
/// shares the namespace, as an external async handle would be.
struct AsyncToken {
    /// Raw token bytes.
    token: Vec<u8>,
    /// Canonical base64 token.
    text: String,
    /// Whether Core delivered a Cancel before the handoff, or an async
    /// heartbeat reported `cancel_requested`; the completion then reports a
    /// cancellation, as the OCaml async handle does.
    cancel_requested: bool,
}

/// Kind of one submitted call, which decides what its outcome may be.
#[derive(Clone, Debug)]
enum CallKind {
    /// Signal: the double acknowledges it at once.
    Signal,
    /// Workflow cancellation request: acknowledged at once.
    CancelWorkflow,
    /// Exact-run wait: the double holds the history long poll open, so it
    /// can end only through release or its runtime closing.
    Wait,
    /// Async heartbeat for a handed-off token.
    AsyncHeartbeat(Vec<u8>),
    /// Async completion for a handed-off token.
    AsyncCompletion(Vec<u8>),
}

impl CallKind {
    /// Coverage name of an await of this kind.
    fn await_name(&self) -> &'static str {
        match self {
            CallKind::Signal => "await_signal",
            CallKind::CancelWorkflow => "await_cancel_workflow",
            CallKind::Wait => "await_wait",
            CallKind::AsyncHeartbeat(_) => "await_async_heartbeat",
            CallKind::AsyncCompletion(_) => "await_async_completion",
        }
    }
}

/// What an await of a pending call may return.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CallState {
    /// Its runtime still serves it: an outcome or `STATUS_NOT_READY`.
    Live,
    /// Released, disconnected, or freed by this thread. The registry entry
    /// is gone before those calls return, so an await must report the
    /// closed failure (`STATUS_INVALID_STATE`) at once.
    Closed,
    /// Its runtime was handed to the reaper and is being released
    /// concurrently: an outcome, `STATUS_NOT_READY`, or the closed failure,
    /// and it must stop being `STATUS_NOT_READY` within [`CALL_SETTLE`].
    Closing,
}

/// One submitted call awaiting its outcome.
struct PendingCall {
    /// Submitting graph's identity, from the handle.
    owner: u64,
    /// Call identifier, from the handle.
    call: u64,
    /// Index of the submitting graph in its thread.
    graph: usize,
    /// What was submitted.
    kind: CallKind,
    /// What an await may return.
    state: CallState,
}

/// The round's shared Core runtime handle, which worker threads attach to
/// before the coordinator releases it.
#[derive(Clone, Copy)]
struct SharedHandle(*const SharedRuntime);
// SAFETY: Concurrent attaches on one live shared handle are permitted by the
// ABI, and the coordinator releases it only after every thread reported that
// its attaches finished.
unsafe impl Send for SharedHandle {}

/// A graph whose ownership moves to the reaper thread for release.
struct ReaperJob {
    /// The graph, now owned by the reaper alone.
    runtime: SendRuntime,
    /// Release through the GC-fallback dispose instead of `runtime_free`.
    dispose: bool,
    /// Owner thread and graph, for the trace.
    label: String,
}

/// Messages to the round's coordinator. Sends ignore a closed channel,
/// because the coordinator may have given up already.
enum LiveEvent {
    /// The thread finished attaching; the shared runtime may be released.
    Attached(usize),
    /// A worker thread or the reaper finished, failed, or panicked.
    Done(LiveReport),
}

/// Everything one worker thread (or the reaper) reports back.
#[derive(Default)]
struct LiveReport {
    /// Participant index; the reaper is index `threads`.
    thread: usize,
    /// First contract violation, if any.
    error: Option<String>,
    /// Every call with its observed outcome.
    trace: Vec<String>,
    /// Observed `(operation, outcome)` counts.
    coverage: Coverage,
    /// Required server outcome per leased token.
    fates: HashMap<Vec<u8>, Fate>,
    /// Heartbeats the test recorded or submitted per token.
    heartbeats: HashMap<Vec<u8>, usize>,
    /// Graphs created.
    graphs: u64,
    /// Graphs that built their own Core runtime.
    owned_cores: u64,
}

/// Inputs of one worker thread.
struct LiveThread {
    /// Participant index.
    index: usize,
    /// The thread's random stream.
    rng: Rng,
    /// Upper bound of operations per graph.
    steps: usize,
    /// The round's shared Core runtime.
    shared: SharedHandle,
    /// Hands graphs to the reaper.
    reaper: mpsc::Sender<ReaperJob>,
    /// Reports to the coordinator.
    events: mpsc::Sender<LiveEvent>,
    /// Tokens delivered as Start tasks to any graph this round; a second
    /// delivery anywhere is a defect.
    delivered: Arc<Mutex<HashSet<Vec<u8>>>>,
}

/// Result type of the stress's steps, with plain failure messages.
type LiveStep = Result<(), String>;

/// Performs one runtime-scoped call on the calling thread, which owns the
/// runtime, and checks the result contract (see [`release_result`]).
fn direct(runtime: *mut Runtime, entry: &Entry) -> Result<Outcome, String> {
    let mut result = AbiResult::default();
    // SAFETY: The runtime is live and owned by the calling thread; the input
    // outlives the call; the result is initialized and uniquely writable.
    let status = unsafe {
        match entry {
            Entry::Plain(function) => function(runtime, &mut result),
            Entry::Input(function, input) => {
                function(runtime, input.as_ptr(), input.len(), &mut result)
            }
        }
    };
    release_result(status, result)
}

/// Fails when `outcome` has a forbidden status or one outside `allowed`.
fn expect_status(graph: usize, name: &str, outcome: &Outcome, allowed: &[Status]) -> LiveStep {
    if LIVE_FORBIDDEN.contains(&outcome.status) || !allowed.contains(&outcome.status) {
        let names: Vec<_> = allowed.iter().map(|status| status_name(*status)).collect();
        return Err(format!(
            "g{graph} {name} returned {} ({}), expected one of {names:?}",
            status_name(outcome.status),
            outcome.error
        ));
    }
    Ok(())
}

/// Fails when a shutdown or waiting release started at `started` took
/// longer than [`SHUTDOWN_BOUND`].
fn check_bound(graph: usize, name: &str, started: Instant) -> LiveStep {
    let elapsed = started.elapsed();
    if elapsed > SHUTDOWN_BOUND {
        return Err(format!(
            "g{graph} {name} took {elapsed:?}, beyond the {SHUTDOWN_BOUND:?} bound"
        ));
    }
    Ok(())
}

/// Parses an `<owner>.<call>` submission handle.
fn parse_handle(value: &[u8]) -> Result<(u64, u64), String> {
    let text = std::str::from_utf8(value).map_err(|_| "call handle is not ASCII".to_owned())?;
    let (owner, call) = text
        .split_once('.')
        .ok_or_else(|| format!("call handle {text:?} is not <owner>.<call>"))?;
    Ok((
        owner.parse().map_err(|_| format!("owner in {text:?}"))?,
        call.parse().map_err(|_| format!("call in {text:?}"))?,
    ))
}

/// Builds an async heartbeat document. Its 500 ms heartbeat timeout gives a
/// 400 ms throttle window, so coalesced and trailing sends happen within a
/// round.
fn async_heartbeat_document(text: &str) -> Vec<u8> {
    format!(
        r#"{{"task_token":"{text}","details":[],"heartbeat_timeout":{{"seconds":0,"nanoseconds":500000000}}}}"#
    )
    .into_bytes()
}

/// Releases one owned runtime through `runtime_free` or the GC-fallback
/// `runtime_dispose`. Returns the operation name, the status, whether the
/// slot was cleared, and when the release started.
fn release_runtime(runtime: *mut Runtime, dispose: bool) -> (&'static str, Status, bool, Instant) {
    let mut slot = runtime;
    let started = Instant::now();
    // SAFETY: The caller owns the live handle exclusively and never uses it
    // again; the slot is a writable local.
    let status = unsafe {
        if dispose {
            ocaml_temporal_core_v4_runtime_dispose(&mut slot)
        } else {
            ocaml_temporal_core_v4_runtime_free(&mut slot)
        }
    };
    let name = if dispose {
        "runtime_dispose"
    } else {
        "runtime_free"
    };
    (name, status, slot.is_null(), started)
}

/// Mutable state of one worker thread.
struct LiveState {
    /// Participant index.
    thread: usize,
    /// The thread's graphs; released graphs stay with a null handle.
    graphs: Vec<LiveGraph>,
    /// Submitted calls not yet terminally awaited.
    calls: Vec<PendingCall>,
    /// Handed-off tokens not yet given an async completion.
    async_tokens: Vec<AsyncToken>,
    /// See [`LiveThread::delivered`].
    delivered: Arc<Mutex<HashSet<Vec<u8>>>>,
    /// Sequence for unique workflow and request identifiers.
    next_request: u64,
    /// The report built up as the thread runs.
    report: LiveReport,
}

impl LiveState {
    /// Records one observed outcome.
    fn note(&mut self, graph: usize, name: &str, detail: &str) {
        self.report
            .trace
            .push(format!("g{graph} {name} -> {detail}"));
        record_coverage(&mut self.report.coverage, name.to_owned(), detail);
    }

    /// Calls a runtime-scoped entry point on `graph` and requires one of the
    /// `allowed` statuses (never a [`LIVE_FORBIDDEN`] one).
    fn invoke(
        &mut self,
        graph: usize,
        name: &str,
        entry: Entry,
        allowed: &[Status],
    ) -> Result<Outcome, String> {
        let outcome = direct(self.graphs[graph].runtime, &entry)
            .map_err(|message| format!("g{graph} {name}: {message}"))?;
        self.note(graph, name, status_name(outcome.status));
        expect_status(graph, name, &outcome, allowed)?;
        Ok(outcome)
    }

    /// Creates the thread's graphs: one in three builds its own Core, the
    /// rest attach to the round's shared runtime.
    fn create_graphs(&mut self, rng: &mut Rng, shared: SharedHandle, steps: usize) -> LiveStep {
        for graph in 0..LIVE_GRAPHS {
            let own = rng.below(3) == 0;
            let mut runtime = ptr::null_mut();
            let mut result = AbiResult::default();
            // SAFETY: Both outputs are writable locals; the coordinator keeps
            // the shared handle live until this thread reports `Attached`.
            let status = unsafe {
                if own {
                    ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result)
                } else {
                    ocaml_temporal_core_v4_runtime_new_attached(shared.0, &mut runtime, &mut result)
                }
            };
            let outcome = release_result(status, result)?;
            let name = if own {
                "runtime_new"
            } else {
                "runtime_new_attached"
            };
            self.note(graph, name, status_name(outcome.status));
            if outcome.status != STATUS_OK || runtime.is_null() {
                return Err(format!("g{graph} {name} returned {}", status_name(status)));
            }
            self.report.graphs += 1;
            if own {
                self.report.owned_cores += 1;
            }
            self.graphs.push(LiveGraph {
                runtime,
                client: false,
                worker: None,
                leased: HashSet::new(),
                steps_left: steps / 2 + rng.below(steps / 2 + 1),
            });
        }
        Ok(())
    }

    /// Runs seeded operations on random graphs until every graph has been
    /// released, interleaving awaits and releases of submitted calls.
    fn run(&mut self, rng: &mut Rng, reaper: &mpsc::Sender<ReaperJob>) -> LiveStep {
        loop {
            let live: Vec<usize> = (0..self.graphs.len())
                .filter(|graph| !self.graphs[*graph].runtime.is_null())
                .collect();
            if live.is_empty() {
                return Ok(());
            }
            if !self.calls.is_empty() && rng.below(100) < 20 {
                self.call_op(rng)?;
                continue;
            }
            let graph = live[rng.below(live.len())];
            if self.graphs[graph].steps_left == 0 {
                self.release(graph, rng, reaper)?;
            } else {
                self.graphs[graph].steps_left -= 1;
                self.graph_op(graph, rng)?;
            }
        }
    }

    /// Chooses and runs one operation on `graph` from its modelled state.
    fn graph_op(&mut self, graph: usize, rng: &mut Rng) -> LiveStep {
        let roll = rng.below(100);
        let model = &self.graphs[graph];
        if !model.client {
            return self.connect(graph);
        }
        let Some(worker) = &model.worker else {
            return match roll {
                0..=54 => self.worker_start(graph),
                55..=74 => self.client_call(graph, rng),
                // Async calls need the worker's namespace: refused here.
                75..=92 => self.async_op(graph, rng),
                _ => self.disconnect(graph),
            };
        };
        let initiated = worker.initiated;
        let (cancel_expected, cancelled) = worker.lease.as_ref().map_or((false, false), |lease| {
            (
                lease.cancel_expected && !lease.cancel_requested,
                lease.cancel_requested,
            )
        });
        if cancel_expected && roll < 60 {
            // Core sends the heartbeat and delivers the Cancel
            // asynchronously; poll for it before the lease is retired.
            return self.wait_poll(graph);
        }
        if cancelled && roll < 40 {
            // Acknowledge the cancellation, as the OCaml executor does once
            // the activity observes it.
            return self.complete(graph);
        }
        if worker.lease.is_some() {
            match roll {
                0..=16 => self.complete(graph),
                17..=24 => self.reject(graph),
                25..=39 => self.heartbeat(graph),
                40..=52 => self.hand_off(graph),
                // With the slot held, a wait can only find a Cancel task.
                53..=62 => self.wait_poll(graph),
                63..=70 => self.async_op(graph, rng),
                71..=78 => self.client_call(graph, rng),
                79..=84 if !initiated => self.initiate(graph),
                // Refused while the worker runs.
                85..=88 => self.disconnect(graph),
                _ => self.worker_shutdown(graph),
            }
        } else {
            match roll {
                0..=44 => self.wait_poll(graph),
                45..=58 => self.async_op(graph, rng),
                59..=70 => self.client_call(graph, rng),
                71..=78 if !initiated => self.initiate(graph),
                71..=78 => self.wait_poll(graph),
                79..=93 => self.worker_shutdown(graph),
                _ => self.disconnect(graph),
            }
        }
    }

    /// Connects the graph's client to the double.
    fn connect(&mut self, graph: usize) -> LiveStep {
        let config = format!(
            r#"{{"target_url":"http://{}","identity":"lifecycle-stress-live"}}"#,
            double().address
        );
        let entry = Entry::Input(
            ocaml_temporal_core_v4_client_connect_json,
            config.into_bytes(),
        );
        self.invoke(graph, "connect", entry, &[STATUS_OK])?;
        self.graphs[graph].client = true;
        Ok(())
    }

    /// Starts the activity worker; its namespace validation reaches the
    /// double.
    fn worker_start(&mut self, graph: usize) -> LiveStep {
        let entry = Entry::Input(
            ocaml_temporal_core_v4_worker_start_json,
            ACTIVITY_WORKER.to_vec(),
        );
        self.invoke(graph, "worker_start", entry, &[STATUS_OK])?;
        self.graphs[graph].worker = Some(LiveWorker {
            lease: None,
            initiated: false,
        });
        Ok(())
    }

    /// Returns the graph's worker, which the caller's state check ensured.
    fn worker(&mut self, graph: usize) -> &mut LiveWorker {
        self.graphs[graph]
            .worker
            .as_mut()
            .expect("the operation was chosen for a running worker")
    }

    /// One bounded readiness wait and one poll. A Start task leases the
    /// worker's single slot; a Cancel task must update the held lease.
    fn wait_poll(&mut self, graph: usize) -> LiveStep {
        let initiated = self.worker(graph).initiated;
        // After initiate-shutdown the readiness signal is closed: a wait
        // drains what is queued and then reports the shutdown.
        let wait_allowed: &[Status] = if initiated {
            &[STATUS_OK, STATUS_NOT_READY, STATUS_INVALID_STATE]
        } else {
            &[STATUS_OK, STATUS_NOT_READY]
        };
        self.invoke(
            graph,
            "wait_activity",
            Entry::Plain(ocaml_temporal_core_v4_worker_wait_activity),
            wait_allowed,
        )?;
        let outcome = direct(
            self.graphs[graph].runtime,
            &Entry::Plain(ocaml_temporal_core_v4_worker_try_poll_activity),
        )
        .map_err(|message| format!("g{graph} poll_activity: {message}"))?;
        if outcome.status != STATUS_OK {
            self.note(graph, "poll_activity", status_name(outcome.status));
        }
        expect_status(
            graph,
            "poll_activity",
            &outcome,
            &[STATUS_OK, STATUS_NOT_READY],
        )?;
        if outcome.status != STATUS_OK {
            return Ok(());
        }
        let text = std::str::from_utf8(&outcome.value)
            .map_err(|_| format!("g{graph} activity task is not UTF-8"))?;
        let task = activity_protocol::decode_task(text)
            .map_err(|error| format!("g{graph} activity task: {error:?}"))?;
        let token = STANDARD
            .decode(&task.task_token)
            .map_err(|_| format!("g{graph} activity token is not base64"))?;
        let label = String::from_utf8_lossy(&token).into_owned();
        match task.variant {
            activity_protocol::ActivityTaskVariant::Start(_) => {
                self.note(graph, "poll_activity", "start");
                let fresh = self
                    .delivered
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .insert(token.clone());
                if !fresh {
                    return Err(format!("g{graph} token {label} was delivered twice"));
                }
                if self.worker(graph).lease.is_some() {
                    return Err(format!(
                        "g{graph} start {label} was delivered while the only slot is leased"
                    ));
                }
                self.graphs[graph].leased.insert(token.clone());
                self.report.fates.insert(token.clone(), Fate::Held);
                self.worker(graph).lease = Some(LiveLease {
                    token,
                    text: task.task_token,
                    document: outcome.value,
                    cancel_requested: false,
                    cancel_expected: false,
                });
            }
            activity_protocol::ActivityTaskVariant::Cancel(_) => {
                match self.worker(graph).lease.as_mut() {
                    Some(lease) if lease.token == token && !lease.cancel_requested => {
                        lease.cancel_requested = true;
                        self.note(graph, "poll_activity", "cancel for the held lease");
                    }
                    // The bridge drops a Cancel whose Start was retired or
                    // already cancelled; delivering one would invite a
                    // second completion for the token.
                    _ => {
                        self.note(graph, "poll_activity", "cancel for no held lease");
                        return Err(format!(
                            "g{graph} Cancel for {label}, which is not an uncancelled held \
                             lease (ever leased here: {})",
                            self.graphs[graph].leased.contains(&token)
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Takes the held lease out of the model.
    fn take_lease(&mut self, graph: usize) -> LiveLease {
        self.worker(graph)
            .lease
            .take()
            .expect("the operation was chosen for a held lease")
    }

    /// Completes the held lease through the worker: successfully, or as
    /// cancelled once Core delivered a Cancel for it.
    fn complete(&mut self, graph: usize) -> LiveStep {
        let lease = self.take_lease(graph);
        let (name, document) = if lease.cancel_requested {
            let document = format!(
                r#"{{"task_token":"{}","result":{CANCELLED_RESULT}}}"#,
                lease.text
            );
            ("complete_cancelled", document.into_bytes())
        } else {
            ("complete_activity", activity_completion(&lease.text))
        };
        let entry = Entry::Input(
            ocaml_temporal_core_v4_worker_complete_activity_json,
            document,
        );
        self.invoke(graph, name, entry, &[STATUS_OK])?;
        self.report.fates.insert(lease.token, Fate::Once);
        Ok(())
    }

    /// Rejects the held lease with its exact task document; the bridge fails
    /// the task back to the server.
    fn reject(&mut self, graph: usize) -> LiveStep {
        let lease = self.take_lease(graph);
        let entry = Entry::Input(
            ocaml_temporal_core_v4_worker_reject_activity_json,
            lease.document,
        );
        self.invoke(graph, "reject_activity", entry, &[STATUS_OK])?;
        self.report.fates.insert(lease.token, Fate::Once);
        Ok(())
    }

    /// Records a worker heartbeat for the held lease. Core sends it; for
    /// every third token the server answers `cancel_requested`, and Core
    /// then delivers a Cancel task through the poll.
    fn heartbeat(&mut self, graph: usize) -> LiveStep {
        let lease = self.worker(graph).lease.as_ref().expect("held lease");
        let token = lease.token.clone();
        let document = format!(r#"{{"task_token":"{}","details":[]}}"#, lease.text);
        let entry = Entry::Input(
            ocaml_temporal_core_v4_worker_record_activity_heartbeat_json,
            document.into_bytes(),
        );
        self.invoke(graph, "record_heartbeat", entry, &[STATUS_OK])?;
        if let Some(lease) = self.worker(graph).lease.as_mut() {
            lease.cancel_expected = cancels_on_heartbeat(&lease.token);
        }
        *self.report.heartbeats.entry(token).or_default() += 1;
        Ok(())
    }

    /// Completes the held lease with `will_complete_async`: the worker slot
    /// is freed and the token moves to the thread's async pool.
    fn hand_off(&mut self, graph: usize) -> LiveStep {
        let lease = self.take_lease(graph);
        let document = format!(
            r#"{{"task_token":"{}","result":{{"kind":"will_complete_async"}}}}"#,
            lease.text
        );
        let entry = Entry::Input(
            ocaml_temporal_core_v4_worker_complete_activity_json,
            document.into_bytes(),
        );
        self.invoke(graph, "hand_off_async", entry, &[STATUS_OK])?;
        self.report
            .fates
            .insert(lease.token.clone(), Fate::AtMostOnce);
        self.async_tokens.push(AsyncToken {
            token: lease.token,
            text: lease.text,
            cancel_requested: lease.cancel_requested,
        });
        Ok(())
    }

    /// Submits an async heartbeat or completion for a pooled token through
    /// `graph`. Without a running worker the graph has no namespace, so the
    /// submission is refused before any task exists.
    fn async_op(&mut self, graph: usize, rng: &mut Rng) -> LiveStep {
        if self.async_tokens.is_empty() {
            return Ok(());
        }
        let index = rng.below(self.async_tokens.len());
        let bound = self.graphs[graph].client && self.graphs[graph].worker.is_some();
        let allowed: &[Status] = if bound {
            &[STATUS_OK, STATUS_RESOURCE_EXHAUSTED]
        } else {
            &[STATUS_INVALID_STATE]
        };
        let pooled = &self.async_tokens[index];
        // A token the double will cancel is heartbeated until the cancel is
        // seen, which is when an async activity learns of it.
        let completion = if pooled.cancel_requested {
            rng.below(2) == 0
        } else if cancels_on_heartbeat(&pooled.token) {
            rng.below(6) == 0
        } else {
            rng.below(3) == 0
        };
        let token = pooled.token.clone();
        let (name, selector, document) = if completion {
            let result = if pooled.cancel_requested {
                CANCELLED_RESULT
            } else {
                COMPLETED_RESULT
            };
            let document = format!(r#"{{"task_token":"{}","result":{result}}}"#, pooled.text);
            (
                "submit_async_completion",
                CALL_ASYNC_COMPLETION,
                document.into_bytes(),
            )
        } else {
            (
                "submit_async_heartbeat",
                CALL_ASYNC_HEARTBEAT,
                async_heartbeat_document(&pooled.text),
            )
        };
        let Some((owner, call)) = self.submit(graph, name, selector, document, allowed)? else {
            return Ok(());
        };
        let kind = if completion {
            self.async_tokens.swap_remove(index);
            CallKind::AsyncCompletion(token)
        } else {
            *self.report.heartbeats.entry(token.clone()).or_default() += 1;
            CallKind::AsyncHeartbeat(token)
        };
        self.calls.push(PendingCall {
            owner,
            call,
            graph,
            kind,
            state: CallState::Live,
        });
        if rng.below(2) == 0 {
            // Half the time the submitting caller awaits at once, as the
            // OCaml async handle does; otherwise the call stays pending while
            // the thread works on, and may be released or closed first.
            let last = self.calls.len() - 1;
            self.await_call(last, 1_000)?;
        }
        Ok(())
    }

    /// Submits a signal, a workflow cancellation request, or an exact-run
    /// wait that the double never answers.
    fn client_call(&mut self, graph: usize, rng: &mut Rng) -> LiveStep {
        let request = self.next_request;
        self.next_request += 1;
        let id = format!("live-{}-{request}", self.thread);
        let (name, selector, kind, document) = match rng.below(3) {
            0 => (
                "submit_signal",
                CALL_SIGNAL,
                CallKind::Signal,
                format!(
                    r#"{{"namespace":"lifecycle-stress","workflow_id":"{id}","run_id":"run","signal_name":"stress","request_id":"{id}","input":[]}}"#
                ),
            ),
            1 => (
                "submit_cancel_workflow",
                CALL_CANCEL,
                CallKind::CancelWorkflow,
                format!(
                    r#"{{"namespace":"lifecycle-stress","workflow_id":"{id}","run_id":"run","request_id":"{id}","reason":"stress"}}"#
                ),
            ),
            _ => (
                "submit_wait",
                CALL_WAIT,
                CallKind::Wait,
                format!(
                    r#"{{"namespace":"lifecycle-stress","workflow_id":"{id}","run_id":"run"}}"#
                ),
            ),
        };
        let allowed = [STATUS_OK, STATUS_RESOURCE_EXHAUSTED];
        if let Some((owner, call)) =
            self.submit(graph, name, selector, document.into_bytes(), &allowed)?
        {
            self.calls.push(PendingCall {
                owner,
                call,
                graph,
                kind,
                state: CallState::Live,
            });
        }
        Ok(())
    }

    /// Submits one client call and returns its handle on success.
    fn submit(
        &mut self,
        graph: usize,
        name: &str,
        selector: u32,
        document: Vec<u8>,
        allowed: &[Status],
    ) -> Result<Option<(u64, u64)>, String> {
        let mut result = AbiResult::default();
        // SAFETY: The runtime is live and owned by this thread; the document
        // outlives the call; the result is initialized and uniquely writable.
        let status = unsafe {
            ocaml_temporal_core_v4_client_submit_json(
                self.graphs[graph].runtime,
                selector,
                document.as_ptr(),
                document.len(),
                &mut result,
            )
        };
        let outcome = release_result(status, result)
            .map_err(|message| format!("g{graph} {name}: {message}"))?;
        self.note(graph, name, status_name(outcome.status));
        expect_status(graph, name, &outcome, allowed)?;
        if outcome.status != STATUS_OK {
            return Ok(None);
        }
        parse_handle(&outcome.value)
            .map(Some)
            .map_err(|message| format!("g{graph} {name}: {message}"))
    }

    /// Stops the worker from taking new tasks without releasing it (#1028).
    fn initiate(&mut self, graph: usize) -> LiveStep {
        self.invoke(
            graph,
            "initiate_shutdown",
            Entry::Plain(ocaml_temporal_core_v4_worker_initiate_shutdown),
            &[STATUS_OK],
        )?;
        self.worker(graph).initiated = true;
        Ok(())
    }

    /// Shuts the worker down within [`SHUTDOWN_BOUND`]. A held lease is
    /// force-completed and reported with `STATUS_OUTSTANDING_TASKS`.
    fn worker_shutdown(&mut self, graph: usize) -> LiveStep {
        let lease = self.worker(graph).lease.take();
        let allowed = if lease.is_some() {
            STATUS_OUTSTANDING_TASKS
        } else {
            STATUS_OK
        };
        let started = Instant::now();
        self.invoke(
            graph,
            "worker_shutdown",
            Entry::Plain(ocaml_temporal_core_v4_worker_shutdown),
            &[allowed],
        )?;
        check_bound(graph, "worker_shutdown", started)?;
        if let Some(lease) = lease {
            self.report.fates.insert(lease.token, Fate::Once);
        }
        self.graphs[graph].worker = None;
        Ok(())
    }

    /// Disconnects the client, which is refused while a worker runs. A
    /// successful disconnect closes every call the graph submitted and
    /// releases its heartbeat throttle state.
    fn disconnect(&mut self, graph: usize) -> LiveStep {
        let allowed = if self.graphs[graph].worker.is_some() {
            STATUS_INVALID_STATE
        } else {
            STATUS_OK
        };
        let outcome = self.invoke(
            graph,
            "disconnect",
            Entry::Plain(ocaml_temporal_core_v4_client_disconnect),
            &[allowed],
        )?;
        if outcome.status != STATUS_OK {
            return Ok(());
        }
        self.graphs[graph].client = false;
        // SAFETY: The runtime is live and used only by this thread.
        let throttle = unsafe { test_heartbeat_throttle_counts(self.graphs[graph].runtime) };
        if throttle != Some((0, 0)) {
            return Err(format!(
                "g{graph} disconnect left heartbeat throttle state {throttle:?}"
            ));
        }
        self.close_calls(graph, CallState::Closed);
        Ok(())
    }

    /// Marks every live call of `graph` with `state`.
    fn close_calls(&mut self, graph: usize, state: CallState) {
        for call in &mut self.calls {
            if call.graph == graph && call.state == CallState::Live {
                call.state = state;
            }
        }
    }

    /// Releases `graph`: `runtime_free` or `runtime_dispose` on this thread,
    /// or by the reaper thread while this thread keeps awaiting the graph's
    /// calls. The release retires whatever the graph still holds.
    fn release(
        &mut self,
        graph: usize,
        rng: &mut Rng,
        reaper: &mpsc::Sender<ReaperJob>,
    ) -> LiveStep {
        let held = self.graphs[graph]
            .worker
            .as_mut()
            .and_then(|worker| worker.lease.take());
        let detail = if held.is_some() {
            "released with a held lease"
        } else {
            "OK"
        };
        if let Some(lease) = held {
            self.report.fates.insert(lease.token, Fate::Once);
        }
        let runtime = std::mem::replace(&mut self.graphs[graph].runtime, ptr::null_mut());
        self.graphs[graph].worker = None;
        self.graphs[graph].client = false;
        let mode = rng.below(4);
        let dispose = mode % 2 == 1;
        if mode >= 2 {
            let job = ReaperJob {
                runtime: SendRuntime(runtime),
                dispose,
                label: format!("t{}g{graph}", self.thread),
            };
            match reaper.send(job) {
                Ok(()) => {
                    self.note(graph, "hand_to_reaper", detail);
                    self.close_calls(graph, CallState::Closing);
                    return Ok(());
                }
                // The reaper has stopped after a failure; release here.
                Err(mpsc::SendError(job)) => {
                    return self.release_here(graph, job.runtime.0, dispose, detail);
                }
            }
        }
        self.release_here(graph, runtime, dispose, detail)
    }

    /// Releases `runtime` on this thread and checks the slot, the status,
    /// and (for the waiting free) the shutdown bound. Both paths unregister
    /// the graph's calls before returning, so later awaits report closed.
    fn release_here(
        &mut self,
        graph: usize,
        runtime: *mut Runtime,
        dispose: bool,
        detail: &str,
    ) -> LiveStep {
        let (name, status, cleared, started) = release_runtime(runtime, dispose);
        let observed = if status == STATUS_OK {
            detail
        } else {
            status_name(status)
        };
        self.note(graph, name, observed);
        if status != STATUS_OK || !cleared {
            return Err(format!(
                "g{graph} {name} returned {} with cleared slot {cleared}",
                status_name(status)
            ));
        }
        if !dispose {
            check_bound(graph, name, started)?;
        }
        self.close_calls(graph, CallState::Closed);
        Ok(())
    }

    /// Awaits one pending call for [`AWAIT_SLICE_MS`], sometimes releasing
    /// it first (#1019).
    fn call_op(&mut self, rng: &mut Rng) -> LiveStep {
        let index = rng.below(self.calls.len());
        if self.calls[index].state == CallState::Live && rng.below(4) == 0 {
            let PendingCall {
                owner, call, graph, ..
            } = self.calls[index];
            let mut result = AbiResult::default();
            // SAFETY: The result is an initialized writable local; release
            // borrows no runtime.
            let status =
                unsafe { ocaml_temporal_core_v4_client_release_call(owner, call, &mut result) };
            let outcome = release_result(status, result)?;
            self.note(graph, "release_call", status_name(outcome.status));
            expect_status(graph, "release_call", &outcome, &[STATUS_OK])?;
            self.calls[index].state = CallState::Closed;
        }
        self.await_call(index, AWAIT_SLICE_MS).map(drop)
    }

    /// Awaits call `index` once and checks the outcome against the call's
    /// kind and state. Returns whether the call reached a terminal outcome,
    /// in which case it is removed.
    fn await_call(&mut self, index: usize, slice_ms: u32) -> Result<bool, String> {
        let PendingCall {
            owner,
            call,
            graph,
            state,
            ..
        } = self.calls[index];
        let kind = self.calls[index].kind.clone();
        let mut result = AbiResult::default();
        // SAFETY: The result is an initialized writable local; an await
        // borrows no runtime, so it is safe while the reaper frees the graph.
        let status =
            unsafe { ocaml_temporal_core_v4_client_await_call(owner, call, slice_ms, &mut result) };
        let outcome = release_result(status, result)?;
        let name = match (&kind, state) {
            (CallKind::Wait, CallState::Closed) => "await_released_wait",
            _ => kind.await_name(),
        };
        let allowed: &[Status] = match state {
            CallState::Live => &[STATUS_OK, STATUS_NOT_READY],
            CallState::Closed => &[STATUS_INVALID_STATE],
            CallState::Closing => &[STATUS_OK, STATUS_NOT_READY, STATUS_INVALID_STATE],
        };
        if outcome.status == STATUS_NOT_READY {
            // Not counted per slice: the count would only measure timing.
            expect_status(graph, name, &outcome, allowed)?;
            return Ok(false);
        }
        self.calls.swap_remove(index);
        let detail = match (&kind, outcome.status) {
            (CallKind::AsyncHeartbeat(token), STATUS_OK) => {
                let body = String::from_utf8_lossy(&outcome.value).into_owned();
                let valid = match body.as_str() {
                    "" => true,
                    "cancel_requested" => cancels_on_heartbeat(token),
                    _ => false,
                };
                if !valid {
                    return Err(format!(
                        "g{graph} async heartbeat for {} answered {body:?}",
                        String::from_utf8_lossy(token)
                    ));
                }
                if !body.is_empty()
                    && let Some(pooled) = self
                        .async_tokens
                        .iter_mut()
                        .find(|pooled| pooled.token == *token)
                {
                    pooled.cancel_requested = true;
                }
                if body.is_empty() {
                    "OK".to_owned()
                } else {
                    body
                }
            }
            (CallKind::AsyncCompletion(token), STATUS_OK) => {
                if !outcome.value.is_empty() {
                    return Err(format!(
                        "g{graph} async completion for {} answered {:?}",
                        String::from_utf8_lossy(token),
                        String::from_utf8_lossy(&outcome.value)
                    ));
                }
                self.report.fates.insert(token.clone(), Fate::Once);
                "OK".to_owned()
            }
            (CallKind::Wait, STATUS_OK) => {
                return Err(format!(
                    "g{graph} a wait that the double never answers completed"
                ));
            }
            (_, status) => status_name(status).to_owned(),
        };
        self.note(graph, name, &detail);
        expect_status(graph, name, &outcome, allowed)?;
        Ok(true)
    }

    /// Releases every graph still live on this thread, which only happens
    /// after a failure, so the round leaks nothing.
    fn release_remaining(&mut self) {
        for graph in 0..self.graphs.len() {
            if self.graphs[graph].runtime.is_null() {
                continue;
            }
            if let Some(lease) = self.graphs[graph]
                .worker
                .as_mut()
                .and_then(|worker| worker.lease.take())
            {
                self.report.fates.insert(lease.token, Fate::Once);
            }
            let runtime = std::mem::replace(&mut self.graphs[graph].runtime, ptr::null_mut());
            let (name, status, _, _) = release_runtime(runtime, false);
            self.note(graph, name, status_name(status));
            self.close_calls(graph, CallState::Closed);
        }
    }

    /// Awaits every remaining call until it reaches a terminal outcome. All
    /// graphs are released by now, so each call must end within
    /// [`CALL_SETTLE`], and one closed by this thread must end at once.
    fn settle_calls(&mut self) -> LiveStep {
        let deadline = Instant::now() + CALL_SETTLE;
        while let Some(last) = self.calls.len().checked_sub(1) {
            if !self.await_call(last, 100)? && Instant::now() >= deadline {
                let call = &self.calls[last];
                return Err(format!(
                    "g{} {:?} call ({:?}) did not end within {CALL_SETTLE:?} of its release",
                    call.graph, call.kind, call.state
                ));
            }
        }
        Ok(())
    }
}

/// Body of one worker thread: attach, report `Attached`, run, release, and
/// settle every call. A failure still releases what the thread owns.
fn live_thread(context: LiveThread) -> LiveReport {
    let LiveThread {
        index,
        mut rng,
        steps,
        shared,
        reaper,
        events,
        delivered,
    } = context;
    let mut state = LiveState {
        thread: index,
        graphs: Vec::new(),
        calls: Vec::new(),
        async_tokens: Vec::new(),
        delivered,
        next_request: 0,
        report: LiveReport {
            thread: index,
            ..LiveReport::default()
        },
    };
    let setup = state.create_graphs(&mut rng, shared, steps);
    let _ = events.send(LiveEvent::Attached(index));
    let mut outcome = setup.and_then(|()| state.run(&mut rng, &reaper));
    // Drop this thread's sender before settling, so the reaper finishes as
    // soon as every thread is done handing graphs over.
    drop(reaper);
    state.release_remaining();
    let settled = state.settle_calls();
    if outcome.is_ok() {
        outcome = settled;
    }
    // Tokens still pooled were never given an async completion.
    for pooled in std::mem::take(&mut state.async_tokens) {
        state.report.fates.insert(pooled.token, Fate::AtMostOnce);
    }
    if outcome.is_ok()
        && let Some(token) = state
            .report
            .fates
            .iter()
            .find(|(_, fate)| **fate == Fate::Held)
            .map(|(token, _)| String::from_utf8_lossy(token).into_owned())
    {
        outcome = Err(format!(
            "token {token} is still modelled as leased after release"
        ));
    }
    state.report.error = outcome.err();
    state.report
}

/// Body of the reaper thread: releases every graph handed to it until all
/// worker threads have dropped their senders.
fn reaper_thread(index: usize, jobs: mpsc::Receiver<ReaperJob>) -> LiveReport {
    let mut report = LiveReport {
        thread: index,
        ..LiveReport::default()
    };
    for job in jobs {
        let (_, status, cleared, started) = release_runtime(job.runtime.0, job.dispose);
        let name = if job.dispose {
            "reaper_dispose"
        } else {
            "reaper_free"
        };
        report
            .trace
            .push(format!("{} {name} -> {}", job.label, status_name(status)));
        record_coverage(&mut report.coverage, name.to_owned(), status_name(status));
        if status != STATUS_OK || !cleared {
            report.error = Some(format!(
                "{} {name} returned {} with cleared slot {cleared}",
                job.label,
                status_name(status)
            ));
            break;
        }
        let elapsed = started.elapsed();
        if !job.dispose && elapsed > SHUTDOWN_BOUND {
            report.error = Some(format!("{} {name} took {elapsed:?}", job.label));
            break;
        }
    }
    report
}

/// Report of a participant whose body panicked; the panic hook has already
/// recorded the location.
fn panicked(index: usize, payload: &(dyn std::any::Any + Send)) -> LiveReport {
    let message = payload
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned());
    LiveReport {
        thread: index,
        error: Some(format!("thread panicked: {message}")),
        ..LiveReport::default()
    }
}

/// Spawns one participant whose body's report, or a panic report, is sent
/// to the coordinator.
fn spawn_participant(
    name: String,
    index: usize,
    events: mpsc::Sender<LiveEvent>,
    body: impl FnOnce() -> LiveReport + Send + 'static,
) {
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            let report = catch_unwind(AssertUnwindSafe(body))
                .unwrap_or_else(|panic| panicked(index, panic.as_ref()));
            let _ = events.send(LiveEvent::Done(report));
        })
        .expect("spawn live stress participant");
}

/// What one round hands back to the test.
#[derive(Default)]
struct LiveRound {
    /// Merged coverage of every participant.
    coverage: Coverage,
    /// Required server outcome per leased token.
    fates: HashMap<Vec<u8>, Fate>,
    /// Heartbeats the test recorded or submitted per token.
    heartbeats: HashMap<Vec<u8>, usize>,
    /// Graphs created.
    graphs: u64,
    /// Core runtimes created: the shared one plus every owned one.
    cores: u64,
}

impl LiveRound {
    /// Adds another round's (or participant's) evidence to this one.
    fn absorb(
        &mut self,
        coverage: Coverage,
        fates: HashMap<Vec<u8>, Fate>,
        heartbeats: HashMap<Vec<u8>, usize>,
    ) {
        merge_coverage(&mut self.coverage, coverage);
        self.fates.extend(fates);
        for (token, count) in heartbeats {
            *self.heartbeats.entry(token).or_default() += count;
        }
    }
}

/// Runs one round and returns its evidence, or a failure report.
///
/// The coordinator creates the shared Core runtime, spawns the reaper and
/// the worker threads, and releases the shared runtime at a seeded moment
/// once every thread has finished attaching. The deadline bounds the whole
/// round. If setup does not finish in time, the shared runtime is leaked
/// rather than released under a possible attach.
fn run_live_round(config: &LiveConfig, round: usize) -> Result<LiveRound, String> {
    let started = Instant::now();
    let deadline = started + LIVE_ROUND_DEADLINE;
    let mut rng = Rng::for_case(
        config.seed ^ LIVE_SEED_SALT,
        u64::try_from(round).expect("round fits u64"),
    );
    let mut shared: *mut SharedRuntime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both outputs are writable locals owned by the coordinator.
    let status = unsafe {
        ocaml_temporal_core_v4_shared_runtime_new(SHARED_CORE_THREADS, &mut shared, &mut result)
    };
    release_result(status, result)?;
    if status != STATUS_OK || shared.is_null() {
        return Err(format!(
            "shared_runtime_new returned {}",
            status_name(status)
        ));
    }
    let handle = SharedHandle(shared);
    let delivered = Arc::new(Mutex::new(HashSet::new()));
    let (events_tx, events_rx) = mpsc::channel();
    let (reaper_tx, reaper_rx) = mpsc::channel();
    let reaper_index = config.threads;
    spawn_participant(
        format!("live-stress-reaper-{round}"),
        reaper_index,
        events_tx.clone(),
        move || reaper_thread(reaper_index, reaper_rx),
    );
    for index in 0..config.threads {
        let context = LiveThread {
            index,
            rng: Rng::for_case(rng.next(), u64::try_from(index).expect("index fits u64")),
            steps: config.steps,
            shared: handle,
            reaper: reaper_tx.clone(),
            events: events_tx.clone(),
            delivered: Arc::clone(&delivered),
        };
        spawn_participant(
            format!("live-stress-{round}-{index}"),
            index,
            events_tx.clone(),
            move || live_thread(context),
        );
    }
    drop(reaper_tx);
    drop(events_tx);

    let participants = config.threads + 1;
    let mut attached = vec![false; config.threads];
    let mut reports: Vec<LiveReport> = Vec::new();
    let mut failures = String::new();
    // A thread that panicked before reporting `Attached` reports `Done`,
    // which also ends its use of the shared handle.
    while attached.iter().any(|done| !done) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match events_rx.recv_timeout(remaining) {
            Ok(LiveEvent::Attached(index)) => attached[index] = true,
            Ok(LiveEvent::Done(report)) => {
                if let Some(done) = attached.get_mut(report.thread) {
                    *done = true;
                }
                reports.push(report);
            }
            Err(_) => {
                let _ = writeln!(
                    failures,
                    "setup did not finish within {LIVE_ROUND_DEADLINE:?}; the shared runtime is leaked"
                );
                return Err(live_failure(config, round, started, &failures, &reports));
            }
        }
    }
    thread::sleep(Duration::from_millis(
        u64::try_from(rng.below(25)).expect("small delay fits u64"),
    ));
    let dispose = rng.below(2) == 0;
    // SAFETY: The coordinator owns the slot and every attach has finished.
    let status = unsafe {
        if dispose {
            ocaml_temporal_core_v4_shared_runtime_dispose(&mut shared)
        } else {
            ocaml_temporal_core_v4_shared_runtime_free(&mut shared)
        }
    };
    if status != STATUS_OK || !shared.is_null() {
        let _ = writeln!(
            failures,
            "shared runtime release (dispose {dispose}) returned {}",
            status_name(status)
        );
    }
    while reports.len() < participants {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match events_rx.recv_timeout(remaining) {
            Ok(LiveEvent::Attached(_)) => {}
            Ok(LiveEvent::Done(report)) => reports.push(report),
            Err(_) => {
                let _ = writeln!(
                    failures,
                    "round exceeded its {LIVE_ROUND_DEADLINE:?} deadline with {} participant(s) running",
                    participants - reports.len()
                );
                break;
            }
        }
    }
    if !failures.is_empty() || reports.iter().any(|report| report.error.is_some()) {
        return Err(live_failure(config, round, started, &failures, &reports));
    }
    let mut merged = LiveRound {
        cores: 1,
        ..LiveRound::default()
    };
    for report in reports {
        merged.absorb(report.coverage, report.fates, report.heartbeats);
        merged.graphs += report.graphs;
        merged.cores += report.owned_cores;
    }
    Ok(merged)
}

/// Formats a round failure with every failing participant's trace, writes it
/// to the report directory, and returns it.
fn live_failure(
    config: &LiveConfig,
    round: usize,
    started: Instant,
    failures: &str,
    reports: &[LiveReport],
) -> String {
    let mut text = String::new();
    let _ = writeln!(
        text,
        "concurrent live stress failure: seed 0x{:x}, round {round}, {} threads, {} steps, after {:?}",
        config.seed,
        config.threads,
        config.steps,
        started.elapsed()
    );
    let _ = writeln!(
        text,
        "rerun (thread scheduling is not seeded): LIFECYCLE_STRESS_SEED=0x{:x} \
         LIFECYCLE_STRESS_LIVE_ROUND={round} LIFECYCLE_STRESS_LIVE_THREADS={} \
         LIFECYCLE_STRESS_LIVE_STEPS={} make native-test-lifecycle-stress",
        config.seed, config.threads, config.steps
    );
    text.push_str(failures);
    for report in reports {
        if let Some(error) = &report.error {
            let _ = writeln!(text, "participant {}: {error}", report.thread);
            for line in &report.trace {
                let _ = writeln!(text, "  {line}");
            }
        }
    }
    let directory = artifact_dir();
    let path = directory.join(format!("live-{:016x}-round{round:04}.txt", config.seed));
    if fs::create_dir_all(&directory).is_ok() && fs::write(&path, &text).is_ok() {
        let _ = writeln!(text, "report written to {}", path.display());
    }
    text
}

/// Source locations of every panic observed on any thread while the panic
/// recorder is installed, including Tokio worker threads whose panics never
/// reach the ABI's `catch_unwind`.
static PANICS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Installs, once per process, a panic hook that records each panic's
/// location before running the previous hook.
fn record_panics() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let location = info.location().map_or_else(
                || "<unknown>".to_owned(),
                |at| format!("{}:{}", at.file(), at.line()),
            );
            PANICS
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(location);
            previous(info);
        }));
    });
}

/// Polls until `done` holds or [`CALL_SETTLE`] passes.
fn eventually(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + CALL_SETTLE;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    done()
}

/// Checks the round evidence against the process-wide ledgers once every
/// round has finished: handles, call registrations, held long polls, server
/// completions and heartbeats per token. Panics on the first violation.
fn check_live_ledgers(total: &LiveRound, before: &LiveBaseline) {
    assert!(
        eventually(|| {
            let (created, cleaned) = test_runtime_cleanup_counts();
            created - before.created == cleaned - before.cleaned
        }),
        "runtime cleanup did not converge: {:?} against a baseline of ({}, {})",
        test_runtime_cleanup_counts(),
        before.created,
        before.cleaned
    );
    assert_eq!(
        test_runtime_cleanup_counts().0 - before.created,
        total.graphs,
        "every graph was created through runtime_new or runtime_new_attached"
    );
    assert!(
        eventually(|| test_core_runtime_counts().1 - before.drops == total.cores),
        "Core drops did not converge: expected {}, observed {}",
        total.cores,
        test_core_runtime_counts().1 - before.drops
    );
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        test_core_runtime_counts().1 - before.drops,
        total.cores,
        "a Core runtime was dropped more than once"
    );
    // No call registration outlives its runtime, and every held history
    // long poll was ended by release or close rather than left detached.
    assert!(
        eventually(|| test_registered_client_calls() == before.calls),
        "client call registrations leaked: {} before, {} after",
        before.calls,
        test_registered_client_calls()
    );
    let ledger = &double().ledger;
    assert!(
        eventually(|| {
            let (held, ended) = ledger.long_polls();
            held - before.held == ended - before.ended
        }),
        "held long polls did not all end: {:?} against a baseline of ({}, {})",
        ledger.long_polls(),
        before.held,
        before.ended
    );
    // Exactly one completion where one is owed, at most one otherwise, and
    // never more heartbeats than were recorded or submitted: throttling may
    // coalesce heartbeats but must not invent them.
    for (token, fate) in &total.fates {
        let label = String::from_utf8_lossy(token);
        match fate {
            Fate::Once => assert!(
                eventually(|| ledger.completions_of(token) == 1),
                "token {label} was completed {} times, expected exactly once",
                ledger.completions_of(token)
            ),
            Fate::AtMostOnce => assert!(
                ledger.completions_of(token) <= 1,
                "async token {label} was completed {} times",
                ledger.completions_of(token)
            ),
            Fate::Held => panic!("token {label} is still modelled as leased"),
        }
        let sent = ledger.heartbeats_of(token);
        let recorded = total.heartbeats.get(token).copied().unwrap_or(0);
        assert!(
            sent <= recorded,
            "token {label}: the server saw {sent} heartbeats for {recorded} recorded"
        );
    }
    if let Some((token, count)) = ledger.double_completed() {
        panic!(
            "task token {} was completed {count} times",
            String::from_utf8_lossy(&token)
        );
    }
}

/// Process-wide counters recorded before the first round.
struct LiveBaseline {
    /// Runtimes created.
    created: u64,
    /// Runtimes cleaned.
    cleaned: u64,
    /// Core runtimes dropped.
    drops: u64,
    /// Registered client calls.
    calls: usize,
    /// History long polls the double held.
    held: u64,
    /// Held history long polls that ended.
    ended: u64,
}

impl LiveBaseline {
    /// Reads every counter now.
    fn now() -> Self {
        let (created, cleaned) = test_runtime_cleanup_counts();
        let (held, ended) = double().ledger.long_polls();
        Self {
            created,
            cleaned,
            drops: test_core_runtime_counts().1,
            calls: test_registered_client_calls(),
            held,
            ended,
        }
    }
}

/// Concurrent live activity workers and submitted client calls against the
/// gRPC double, with shared and cross-thread runtime release. After the
/// rounds every graph has been cleaned once and every Core runtime dropped
/// once, no client call registration or held long poll survives, each
/// leased token has the server completions its fate requires, throttling
/// never sent more heartbeats than were recorded, and no panic came from
/// bridge code on any thread.
#[test]
fn concurrent_live_workers_and_clients() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    record_panics();
    let panics_before = PANICS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .len();
    let config = LiveConfig::from_env();
    let before = LiveBaseline::now();
    let started = Instant::now();
    let rounds: Vec<usize> = match config.only_round {
        Some(round) => vec![round],
        None => (0..config.rounds).collect(),
    };
    let mut total = LiveRound::default();
    for round in rounds {
        let result = run_live_round(&config, round).unwrap_or_else(|report| panic!("{report}"));
        total.absorb(result.coverage, result.fates, result.heartbeats);
        total.graphs += result.graphs;
        total.cores += result.cores;
    }
    check_live_ledgers(&total, &before);

    // No panic from bridge code on any thread. Panics inside Temporal Core
    // are contained by Tokio and reported, not failed; see the Findings
    // section of docs/reference/bridge-lifecycle-stress.md.
    let panics: Vec<String> =
        PANICS.lock().unwrap_or_else(|error| error.into_inner())[panics_before..].to_vec();
    let bridge: Vec<&String> = panics
        .iter()
        .filter(|location| location.replace('\\', "/").contains("core-bridge/src/"))
        .collect();
    assert!(bridge.is_empty(), "bridge code panicked: {bridge:?}");
    let mut contained: BTreeMap<&str, usize> = BTreeMap::new();
    for location in &panics {
        *contained.entry(location.as_str()).or_default() += 1;
    }
    eprintln!(
        "concurrent live stress: seed 0x{:x}, {} rounds, {} threads, {} steps, {} graphs, \
         {} cores, {} tokens in {:?}; contained upstream panics: {contained:?}",
        config.seed,
        config.only_round.map_or(config.rounds, |_| 1),
        config.threads,
        config.steps,
        total.graphs,
        total.cores,
        total.fates.len(),
        started.elapsed()
    );
    for ((name, detail), count) in &total.coverage {
        eprintln!("  {count:6} {name} -> {detail}");
    }
    if config.is_default() {
        for (name, detail) in LIVE_REQUIRED_COVERAGE {
            assert!(
                total
                    .coverage
                    .contains_key(&((*name).to_owned(), (*detail).to_owned())),
                "the default live stress never observed {name} -> {detail}"
            );
        }
    }
}

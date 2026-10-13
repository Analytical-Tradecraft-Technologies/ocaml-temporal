//! Submit-and-complete client calls (#807) through real Core connections.
//!
//! A callback transport stands in for Temporal: history long polls and
//! queries never answer, while signals and starts answer at once. The tests
//! show that a pending long poll occupies neither the owner nor any other
//! call, that every call's outcome reaches only its own completion cell, and
//! that disconnect and close release pending calls deterministically.

use super::*;
use crate::client_calls::registered_calls;

/// A submission handle: the owning graph's identity and the call identifier.
type Handle = (u64, u64);

/// Awaits the call `handle` names, as its submitting caller would.
fn await_call(handle: Handle, timeout: Duration) -> Operation {
    crate::client_calls::await_call(handle.0, handle.1, timeout)
}
use prost::Message;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use temporalio_client::callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse};
use temporalio_client::tonic::Status as RpcStatus;
use temporalio_common::protos::temporal::api::workflowservice::v1::{
    SignalWorkflowExecutionResponse, StartWorkflowExecutionResponse,
};

/// Counts in-flight transport futures that were started and later dropped.
#[derive(Default)]
struct Probe {
    /// Long polls (history and query) that reached the transport.
    long_polls: AtomicUsize,
    /// Long polls whose transport future was dropped (cancelled).
    released: AtomicUsize,
    /// Signals the transport answered.
    signals: AtomicUsize,
}

/// Records that a never-answering transport future was dropped.
struct Released(Arc<Probe>);

impl Drop for Released {
    /// Dropping the future proves the RPC was cancelled, not detached.
    fn drop(&mut self) {
        self.0.released.fetch_add(1, Ordering::SeqCst);
    }
}

/// Connects a runtime to a transport whose history and query RPCs hang
/// forever and whose signal and start RPCs succeed immediately.
fn connected_runtime() -> (Runtime, Arc<Probe>) {
    let options = RuntimeOptions::builder().build().expect("runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default()).expect("Core runtime");
    let mut runtime = Runtime::with_owned_core(core).expect("runtime cleanup thread");
    let probe = Arc::new(Probe::default());
    let observed = Arc::clone(&probe);
    let service = CallbackBasedGrpcService {
        callback: Arc::new(move |request| {
            let probe = Arc::clone(&observed);
            Box::pin(async move {
                match request.rpc.as_str() {
                    "GetWorkflowExecutionHistory" | "QueryWorkflow" => {
                        probe.long_polls.fetch_add(1, Ordering::SeqCst);
                        let _released = Released(Arc::clone(&probe));
                        std::future::pending::<()>().await;
                        unreachable!("a pending future never resolves")
                    }
                    "SignalWorkflowExecution" => {
                        probe.signals.fetch_add(1, Ordering::SeqCst);
                        Ok(GrpcSuccessResponse {
                            headers: Default::default(),
                            proto: SignalWorkflowExecutionResponse::default().encode_to_vec(),
                        })
                    }
                    "StartWorkflowExecution" => Ok(GrpcSuccessResponse {
                        headers: Default::default(),
                        proto: StartWorkflowExecutionResponse {
                            run_id: "run-1".to_owned(),
                            started: true,
                            ..Default::default()
                        }
                        .encode_to_vec(),
                    }),
                    other => Err(RpcStatus::unimplemented(format!("{other} is not scripted"))),
                }
            })
        }),
    };
    let options = ConnectionOptions::new(
        temporalio_sdk_core::Url::parse("http://localhost:7233").expect("test URL"),
    )
    .skip_get_system_info(true)
    .service_override(service)
    .dns_load_balancing(None)
    .build();
    runtime.client = Some(
        runtime
            .core
            .as_ref()
            .unwrap()
            .tokio_handle()
            .block_on(Connection::connect(options))
            .expect("callback connection"),
    );
    (runtime, probe)
}

/// The wait document for one exact run.
const WAIT: &[u8] = br#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"}"#;
/// A query that the transport never answers.
const QUERY: &[u8] = br#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","query_type":"state","input":[]}"#;
/// A signal that the transport acknowledges at once.
const SIGNAL: &[u8] = br#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","signal_name":"add","request_id":"signal-1","input":[]}"#;
/// A start that the transport accepts at once.
const START: &[u8] = br#"{"request_id":"start-1","namespace":"default","workflow_id":"workflow-2","workflow_type":"Workflow","task_queue":"queue","input":[]}"#;

/// Submits one call and parses the `<owner>.<call>` handle the ABI returns.
fn submit(runtime: &mut Runtime, kind: ClientCallKind, input: &[u8]) -> Handle {
    let selector = match kind {
        ClientCallKind::Start => 1,
        ClientCallKind::Wait => 2,
        ClientCallKind::Query => 7,
        ClientCallKind::Signal => 6,
        _ => unreachable!("only scripted kinds are submitted"),
    };
    let bytes = runtime
        .submit_client_call(selector, input)
        .expect("call admitted");
    let text = std::str::from_utf8(&bytes).expect("ASCII handle");
    let (owner, call) = text.split_once('.').expect("owner.call handle");
    let handle = (
        owner.parse().expect("decimal owner"),
        call.parse().expect("decimal call"),
    );
    assert_eq!(handle.0, runtime.call_owner);
    handle
}

/// Awaits one call within `bound`, retrying the bounded native wait.
fn outcome(call: Handle, bound: Duration) -> Operation {
    let deadline = Instant::now() + bound;
    loop {
        match await_call(call, Duration::from_millis(50)) {
            Err(error) if error.status == STATUS_NOT_READY && Instant::now() < deadline => {}
            result => return result,
        }
    }
}

/// A pending wait and a pending query hold no owner time: a signal and a
/// start submitted afterwards complete at once, each through its own cell,
/// while the long polls stay pending. Submission itself returns immediately.
#[test]
fn pending_long_polls_do_not_delay_other_calls() {
    let (mut runtime, probe) = connected_runtime();
    let started = Instant::now();
    let wait = submit(&mut runtime, ClientCallKind::Wait, WAIT);
    let query = submit(&mut runtime, ClientCallKind::Query, QUERY);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "submission must not wait for the RPC"
    );

    let signalled = Instant::now();
    let signal = submit(&mut runtime, ClientCallKind::Signal, SIGNAL);
    let response = outcome(signal, Duration::from_secs(5)).expect("signal acknowledged");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response).unwrap(),
        serde_json::json!({"acknowledged": true})
    );
    let start = submit(&mut runtime, ClientCallKind::Start, START);
    let response = outcome(start, Duration::from_secs(5)).expect("start outcome");
    let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(response["kind"], "accepted");
    assert_eq!(response["execution"]["run_id"], "run-1");
    assert!(
        signalled.elapsed() < Duration::from_secs(5),
        "short calls must not queue behind pending long polls"
    );
    assert_eq!(probe.signals.load(Ordering::SeqCst), 1);

    // The long polls are still pending, and a retired identifier is closed.
    for call in [wait, query] {
        let pending = await_call(call, Duration::ZERO).expect_err("still pending");
        assert_eq!(pending.status, STATUS_NOT_READY);
    }
    let retired = await_call(signal, Duration::ZERO).expect_err("consumed call");
    assert_eq!(retired.status, STATUS_INVALID_STATE);

    assert!(runtime.disconnect_client().is_ok());
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Disconnect wakes a waiter blocked on a pending call with the closed
/// status, aborts and joins its task (the transport future is dropped), and
/// leaves no registered call behind.
#[test]
fn disconnect_closes_pending_calls() {
    let (mut runtime, probe) = connected_runtime();
    let owner = runtime.call_owner;
    let wait = submit(&mut runtime, ClientCallKind::Wait, WAIT);
    let query = submit(&mut runtime, ClientCallKind::Query, QUERY);
    let deadline = Instant::now() + Duration::from_secs(5);
    while probe.long_polls.load(Ordering::SeqCst) < 2 {
        assert!(
            Instant::now() < deadline,
            "long polls never reached the transport"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let waiter = std::thread::spawn(move || await_call(wait, Duration::from_secs(30)));
    std::thread::sleep(Duration::from_millis(50));

    assert!(runtime.disconnect_client().is_ok());
    let closed = waiter
        .join()
        .expect("waiter thread")
        .expect_err("closed call");
    assert_eq!(closed.status, STATUS_INVALID_STATE);
    let closed = await_call(query, Duration::ZERO).expect_err("closed call");
    assert_eq!(closed.status, STATUS_INVALID_STATE);
    assert_eq!(probe.released.load(Ordering::SeqCst), 2);
    assert!(runtime.pending_calls.is_empty());
    assert_eq!(registered_calls(owner), 0);
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// The garbage-collector close path releases pending calls too: the cells
/// close at once and the aborted tasks are joined by the cleanup thread.
#[test]
fn fallback_close_releases_pending_calls() {
    let (mut runtime, probe) = connected_runtime();
    let owner = runtime.call_owner;
    let wait = submit(&mut runtime, ClientCallKind::Wait, WAIT);
    assert_eq!(runtime.close(false), STATUS_OK);
    let closed = await_call(wait, Duration::ZERO).expect_err("closed call");
    assert_eq!(closed.status, STATUS_INVALID_STATE);
    assert_eq!(registered_calls(owner), 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    while probe.released.load(Ordering::SeqCst) < probe.long_polls.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "cleanup never released the long poll"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Malformed input, an unknown selector, and a missing connection all fail
/// synchronously, before any call is registered.
#[test]
fn submission_validates_before_registering() {
    let (mut runtime, _probe) = connected_runtime();
    let owner = runtime.call_owner;
    let unknown = runtime
        .submit_client_call(0, WAIT)
        .expect_err("unknown kind");
    assert_eq!(unknown.status, STATUS_INVALID_ARGUMENT);
    let malformed = runtime.submit_client_call(2, b"{}").expect_err("bad wait");
    assert_eq!(malformed.status, STATUS_PROTOCOL);
    assert_eq!(registered_calls(owner), 0);
    assert!(runtime.disconnect_client().is_ok());
    let disconnected = runtime
        .submit_client_call(6, SIGNAL)
        .expect_err("no client");
    assert_eq!(disconnected.status, STATUS_INVALID_STATE);
    assert_eq!(registered_calls(owner), 0);
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Waits on 64 distinct runs fill the wait class; a 65th distinct run is
/// refused with the retryable capacity status, while another wait on an
/// already waited run and a signal are still admitted.
#[test]
fn wait_capacity_counts_distinct_runs() {
    let (mut runtime, _probe) = connected_runtime();
    for index in 0..MAX_PENDING_WAITS {
        let request = format!(
            r#"{{"namespace":"default","workflow_id":"workflow-{index}","run_id":"run-1"}}"#
        );
        submit(&mut runtime, ClientCallKind::Wait, request.as_bytes());
    }
    let over = runtime
        .submit_client_call(
            2,
            br#"{"namespace":"default","workflow_id":"workflow-extra","run_id":"run-1"}"#,
        )
        .expect_err("distinct run beyond capacity");
    assert_eq!(over.status, STATUS_RESOURCE_EXHAUSTED);
    submit(
        &mut runtime,
        ClientCallKind::Wait,
        br#"{"namespace":"default","workflow_id":"workflow-0","run_id":"run-1"}"#,
    );
    let signal = submit(&mut runtime, ClientCallKind::Signal, SIGNAL);
    outcome(signal, Duration::from_secs(5)).expect("signal still admitted");
    assert!(runtime.disconnect_client().is_ok());
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// A task that panics settles its cell with the panic status instead of
/// stranding the waiter.
#[test]
fn panicking_task_settles_its_cell() {
    let owner = crate::client_calls::new_owner_id();
    let (call, guard) = crate::client_calls::register(owner).expect("registered call");
    let task = std::thread::spawn(move || {
        let _guard = guard;
        panic!("synthetic client task panic");
    });
    assert!(task.join().is_err());
    let failure = await_call((owner, call), Duration::ZERO).expect_err("panicked call");
    assert_eq!(failure.status, STATUS_PANIC);
}

/// A call can be awaited only through the owner that submitted it. Another
/// runtime's owner identity, or an arbitrary one, gets the same closed/unknown
/// status as an identifier that was never issued, and neither waits on,
/// consumes, nor settles the call: its submitter still receives the outcome.
#[test]
fn awaiting_requires_the_submitting_owner() {
    let (mut runtime, _probe) = connected_runtime();
    let (other, _other_probe) = connected_runtime();
    assert_ne!(runtime.call_owner, other.call_owner);
    let wait = submit(&mut runtime, ClientCallKind::Wait, WAIT);
    let signal = submit(&mut runtime, ClientCallKind::Signal, SIGNAL);
    let unknown = crate::client_calls::await_call(other.call_owner, u64::MAX, Duration::ZERO)
        .expect_err("unknown call");
    for foreign in [other.call_owner, 0, runtime.call_owner ^ 1] {
        for call in [wait.1, signal.1] {
            let started = Instant::now();
            let refused = crate::client_calls::await_call(foreign, call, Duration::from_secs(5))
                .expect_err("foreign owner refused");
            assert_eq!(refused.status, unknown.status);
            assert_eq!(refused.message, unknown.message);
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "a foreign owner must not wait on the call"
            );
        }
    }
    // The submitter still gets the signal's outcome and the pending wait.
    outcome(signal, Duration::from_secs(5)).expect("signal outcome kept for its owner");
    let pending = await_call(wait, Duration::ZERO).expect_err("still pending");
    assert_eq!(pending.status, STATUS_NOT_READY);
    assert!(runtime.disconnect_client().is_ok());
    assert_eq!(runtime.close(true), STATUS_OK);
    assert_eq!(other.close(true), STATUS_OK);
}

/// Completed calls keep counting against the runtime's ceiling until their
/// cells are retired (Codex review on #976). Abandoning every call without
/// reading it fills the ceiling even though every task has finished, so the
/// next submission is refused with the capacity status; releasing one call,
/// or reading one, frees exactly one slot.
#[test]
fn unread_completed_calls_hold_capacity_until_retired() {
    let (mut runtime, probe) = connected_runtime();
    let owner = runtime.call_owner;
    let mut handles = (0..MAX_PENDING_CALLS)
        .map(|_| submit(&mut runtime, ClientCallKind::Signal, SIGNAL))
        .collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_secs(30);
    while probe.signals.load(Ordering::SeqCst) < MAX_PENDING_CALLS {
        assert!(Instant::now() < deadline, "signals never completed");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(registered_calls(owner), MAX_PENDING_CALLS);
    let refused = runtime
        .submit_client_call(6, SIGNAL)
        .expect_err("ceiling reached by unread outcomes");
    assert_eq!(refused.status, STATUS_RESOURCE_EXHAUSTED);

    // An interrupted caller releases its call: one slot is freed and the
    // released call can no longer be read.
    let released = handles.pop().expect("a submitted call");
    crate::client_calls::release_call(released.0, released.1);
    let gone = await_call(released, Duration::ZERO).expect_err("released call");
    assert_eq!(gone.status, STATUS_INVALID_STATE);
    assert_eq!(registered_calls(owner), MAX_PENDING_CALLS - 1);
    let admitted = submit(&mut runtime, ClientCallKind::Signal, SIGNAL);
    assert!(runtime.submit_client_call(6, SIGNAL).is_err());

    // Reading a call retires it too; a foreign release changes nothing.
    crate::client_calls::release_call(owner ^ 1, admitted.1);
    outcome(admitted, Duration::from_secs(5)).expect("admitted signal");
    assert_eq!(registered_calls(owner), MAX_PENDING_CALLS - 1);
    submit(&mut runtime, ClientCallKind::Signal, SIGNAL);

    assert!(runtime.disconnect_client().is_ok());
    assert_eq!(registered_calls(owner), 0);
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Builds the wait document for the exact run of `workflow_id`.
fn wait_for(workflow_id: &str) -> Vec<u8> {
    format!(r#"{{"namespace":"default","workflow_id":"{workflow_id}","run_id":"run-1"}}"#)
        .into_bytes()
}

/// A configured wait limit of two (#796) replaces the default of 64: a third
/// distinct run is refused with the retryable capacity status naming the
/// configured limit, while another wait on an admitted run still shares its
/// slot.
#[test]
fn configured_wait_limit_is_enforced() {
    let (mut runtime, _probe) = connected_runtime();
    runtime.client_limits =
        crate::client_calls::ClientLimits::from_config(Some(2), None, None).expect("valid limits");
    submit(&mut runtime, ClientCallKind::Wait, &wait_for("workflow-a"));
    submit(&mut runtime, ClientCallKind::Wait, &wait_for("workflow-b"));
    let over = runtime
        .submit_client_call(2, &wait_for("workflow-c"))
        .expect_err("third distinct run beyond the configured limit");
    assert_eq!(over.status, STATUS_RESOURCE_EXHAUSTED);
    assert!(over.message.contains("limit 2"), "{}", over.message);
    submit(&mut runtime, ClientCallKind::Wait, &wait_for("workflow-a"));
    assert!(runtime.disconnect_client().is_ok());
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Disconnecting resets the connection's limits to the defaults (#796), so a
/// runtime without a connection never keeps an earlier client's limits.
#[test]
fn disconnect_resets_client_limits() {
    let (mut runtime, _probe) = connected_runtime();
    runtime.client_limits =
        crate::client_calls::ClientLimits::from_config(Some(2), Some(3), Some(5))
            .expect("valid limits");
    assert_ne!(
        runtime.client_limits,
        crate::client_calls::ClientLimits::default()
    );
    assert!(runtime.disconnect_client().is_ok());
    assert_eq!(
        runtime.client_limits,
        crate::client_calls::ClientLimits::default()
    );
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// A configured in-flight ceiling of three (#796) refuses a fourth call of
/// any kind, here a signal behind three pending queries.
#[test]
fn configured_in_flight_limit_is_enforced() {
    let (mut runtime, _probe) = connected_runtime();
    runtime.client_limits =
        crate::client_calls::ClientLimits::from_config(None, None, Some(3)).expect("valid limits");
    for _ in 0..3 {
        submit(&mut runtime, ClientCallKind::Query, QUERY);
    }
    let over = runtime
        .submit_client_call(6, SIGNAL)
        .expect_err("fourth call beyond the configured ceiling");
    assert_eq!(over.status, STATUS_RESOURCE_EXHAUSTED);
    assert!(over.message.contains("limit 3"), "{}", over.message);
    assert!(runtime.disconnect_client().is_ok());
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Omitted limits keep the pre-#796 values, both for a fresh runtime and for
/// a connect document without limit fields.
#[test]
fn omitted_client_limits_keep_the_defaults() {
    let defaults = crate::client_calls::ClientLimits::default();
    assert_eq!(defaults.pending_waits, 64);
    assert_eq!(defaults.pending_starts, 64);
    assert_eq!(defaults.in_flight_calls, 4_096);
    assert_eq!(
        crate::client_calls::ClientLimits::from_config(None, None, None).expect("defaults"),
        defaults
    );
    let config: ClientConfig =
        serde_json::from_str(r#"{"target_url":"http://localhost:7233","identity":"test"}"#)
            .expect("pre-#796 document");
    assert_eq!(
        (
            config.max_pending_waits,
            config.max_pending_starts,
            config.max_in_flight_calls
        ),
        (None, None, None)
    );
    let (runtime, _probe) = connected_runtime();
    assert_eq!(runtime.client_limits, defaults);
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Each limit accepts exactly `1..=maximum`; zero or a value above the
/// documented maximum is a configuration failure naming the field, and
/// connect rejects it before any network access, leaving no client.
#[test]
fn invalid_client_limits_are_rejected() {
    use crate::client_calls::{
        ClientLimits, MAX_CONFIGURABLE_IN_FLIGHT_CALLS, MAX_CONFIGURABLE_PENDING_STARTS,
        MAX_CONFIGURABLE_PENDING_WAITS,
    };
    let accepted = ClientLimits::from_config(
        Some(MAX_CONFIGURABLE_PENDING_WAITS),
        Some(1),
        Some(MAX_CONFIGURABLE_IN_FLIGHT_CALLS),
    )
    .expect("boundary values");
    assert_eq!(accepted.pending_waits, 4_096);
    assert_eq!(accepted.pending_starts, 1);
    assert_eq!(accepted.in_flight_calls, 65_536);
    for (limits, field) in [
        ((Some(0), None, None), "max_pending_waits"),
        (
            (Some(MAX_CONFIGURABLE_PENDING_WAITS + 1), None, None),
            "max_pending_waits",
        ),
        ((None, Some(0), None), "max_pending_starts"),
        (
            (None, Some(MAX_CONFIGURABLE_PENDING_STARTS + 1), None),
            "max_pending_starts",
        ),
        ((None, None, Some(0)), "max_in_flight_calls"),
        (
            (None, None, Some(MAX_CONFIGURABLE_IN_FLIGHT_CALLS + 1)),
            "max_in_flight_calls",
        ),
    ] {
        let error = ClientLimits::from_config(limits.0, limits.1, limits.2)
            .expect_err("out-of-range limit");
        assert_eq!(error.status, STATUS_CONFIGURATION);
        assert!(error.message.contains(field), "{}", error.message);
    }

    let options = RuntimeOptions::builder().build().expect("runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default()).expect("Core runtime");
    let mut runtime = Runtime::with_owned_core(core).expect("runtime cleanup thread");
    let config: ClientConfig = serde_json::from_str(
        r#"{"target_url":"http://localhost:7233","identity":"test","max_pending_waits":0}"#,
    )
    .expect("strictly typed document");
    let error = runtime
        .connect_client(config)
        .expect_err("zero limit rejected before connecting");
    assert_eq!(error.status, STATUS_CONFIGURATION);
    assert!(runtime.client.is_none());
    assert_eq!(runtime.client_limits, ClientLimits::default());
    for document in [
        r#"{"target_url":"http://localhost:7233","identity":"test","max_pending_waits":-1}"#,
        r#"{"target_url":"http://localhost:7233","identity":"test","max_pending_starts":"2"}"#,
        r#"{"target_url":"http://localhost:7233","identity":"test","max_in_flight_calls":4294967296}"#,
        r#"{"target_url":"http://localhost:7233","identity":"test","max_pending":2}"#,
    ] {
        assert!(
            serde_json::from_str::<ClientConfig>(document).is_err(),
            "{document}"
        );
    }
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// Polls `condition` every few milliseconds until it holds, failing with
/// `what` after five seconds.
fn eventually(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Releasing a pending wait aborts its task at once (#1019): the transport
/// long poll is dropped with no further submission, disconnect, or close.
/// Before the fix, the task kept long-polling until the run closed.
#[test]
fn released_wait_aborts_its_long_poll() {
    let (mut runtime, probe) = connected_runtime();
    let owner = runtime.call_owner;
    let wait = submit(&mut runtime, ClientCallKind::Wait, WAIT);
    eventually("the long poll never reached the transport", || {
        probe.long_polls.load(Ordering::SeqCst) == 1
    });
    crate::client_calls::release_call(wait.0, wait.1);
    eventually("releasing the wait did not cancel its long poll", || {
        probe.released.load(Ordering::SeqCst) == 1
    });
    assert_eq!(registered_calls(owner), 0);
    let gone = await_call(wait, Duration::ZERO).expect_err("released call");
    assert_eq!(gone.status, STATUS_INVALID_STATE);
    assert!(runtime.disconnect_client().is_ok());
    assert!(runtime.pending_calls.is_empty());
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// A released wait stops holding a wait slot and an in-flight slot at once
/// (#1019), even before its aborted task has finished: with the default
/// limits, `max_pending_waits` new distinct waits are admitted right after
/// one wait is released.
#[test]
fn released_wait_frees_its_wait_slot() {
    let (mut runtime, probe) = connected_runtime();
    let released = submit(
        &mut runtime,
        ClientCallKind::Wait,
        &wait_for("workflow-old"),
    );
    crate::client_calls::release_call(released.0, released.1);
    for index in 0..MAX_PENDING_WAITS {
        submit(
            &mut runtime,
            ClientCallKind::Wait,
            &wait_for(&format!("workflow-{index}")),
        );
    }
    let over = runtime
        .submit_client_call(2, &wait_for("workflow-extra"))
        .expect_err("live waits still fill the class");
    assert_eq!(over.status, STATUS_RESOURCE_EXHAUSTED);
    assert!(runtime.disconnect_client().is_ok());
    assert_eq!(
        probe.released.load(Ordering::SeqCst),
        probe.long_polls.load(Ordering::SeqCst)
    );
    assert_eq!(runtime.close(true), STATUS_OK);
}

/// With configured limits of two waits and two in-flight calls, releasing
/// both admitted waits lets two new distinct waits in immediately (#1019):
/// released calls count against neither limit, while a released call's task
/// is still joined by disconnect.
#[test]
fn released_calls_free_configured_in_flight_capacity() {
    let (mut runtime, probe) = connected_runtime();
    let owner = runtime.call_owner;
    runtime.client_limits = crate::client_calls::ClientLimits::from_config(Some(2), None, Some(2))
        .expect("valid limits");
    let first = submit(&mut runtime, ClientCallKind::Wait, &wait_for("workflow-a"));
    let second = submit(&mut runtime, ClientCallKind::Wait, &wait_for("workflow-b"));
    let full = runtime
        .submit_client_call(7, QUERY)
        .expect_err("in-flight ceiling reached");
    assert_eq!(full.status, STATUS_RESOURCE_EXHAUSTED);
    for call in [first, second] {
        crate::client_calls::release_call(call.0, call.1);
    }
    submit(&mut runtime, ClientCallKind::Wait, &wait_for("workflow-c"));
    submit(&mut runtime, ClientCallKind::Wait, &wait_for("workflow-d"));
    let over = runtime
        .submit_client_call(2, &wait_for("workflow-e"))
        .expect_err("the two live waits fill both limits");
    assert_eq!(over.status, STATUS_RESOURCE_EXHAUSTED);
    assert_eq!(registered_calls(owner), 2);
    assert!(runtime.disconnect_client().is_ok());
    assert!(runtime.pending_calls.is_empty());
    assert_eq!(
        probe.released.load(Ordering::SeqCst),
        probe.long_polls.load(Ordering::SeqCst)
    );
    assert_eq!(runtime.close(true), STATUS_OK);
}

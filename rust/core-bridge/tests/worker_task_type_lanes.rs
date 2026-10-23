//! Poll-lane regressions for registration-derived Core task types (#805).
//!
//! A live worker starts only the Core poll loops that its configured task
//! types enable. A disabled lane must look idle to the supervisor rather than
//! closed, and shutdown must still complete without a producer for that lane.

use ocaml_temporal_core_bridge::worker_bridge::{
    PollLanes, ReadinessWait, bridge_task_types, poll_lanes_for,
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, RuntimeOptions, TokioRuntimeBuilder, WorkerConfig,
    WorkerVersioningStrategy,
    replay::{HistoryFeeder, ReplayWorkerInput},
};

/// The lane plan follows Core's task types: the workflow lane exists only with
/// workflows, and the activity lane exists whenever Core can produce a local
/// or remote activity task.
#[test]
fn lane_plan_follows_registered_task_kinds() {
    let both = bridge_task_types(true, true).unwrap();
    let workflow_only = bridge_task_types(true, false).unwrap();
    let activity_only = bridge_task_types(false, true).unwrap();

    assert_eq!(poll_lanes_for(&both), (true, true));
    // Local activities still need the activity lane, but Core never polls the
    // server for remote activity tasks on this worker's behalf.
    assert!(!workflow_only.enable_remote_activities);
    assert_eq!(poll_lanes_for(&workflow_only), (true, true));
    // Polling workflows on an activity-only worker would make Core return
    // `ShutDown` and stop the whole worker, so that lane must not start.
    assert_eq!(poll_lanes_for(&activity_only), (false, true));
    assert_eq!(
        poll_lanes_for(&WorkerTaskTypes::workflow_only()),
        (true, false)
    );
    assert!(bridge_task_types(false, false).is_err());
}

/// Builds a real Core worker whose configuration enables workflows only.
///
/// Core's replay constructor is the only way to obtain a real worker without
/// a Temporal server; it forces `WorkerTaskTypes::workflow_only()`, which is
/// exactly the "no activity polling at all" shape under test. The returned
/// feeder must stay alive so the worker remains idle until shutdown.
fn workflow_only_worker(core: &CoreRuntime) -> (temporalio_sdk_core::Worker, HistoryFeeder) {
    let config = WorkerConfig::builder()
        .namespace("task-type-lanes")
        .task_queue("task-type-lanes")
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "task-type-lanes".to_owned(),
        })
        .task_types(WorkerTaskTypes::workflow_only())
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(1))
        .max_outstanding_workflow_tasks(1usize)
        .ignore_evicts_on_shutdown(true)
        .build()
        .expect("workflow-only worker configuration should be valid");
    let (feeder, stream) = HistoryFeeder::new(1);
    let _guard = core.tokio_handle().enter();
    let worker = temporalio_sdk_core::init_replay_worker(ReplayWorkerInput::new(config, stream))
        .expect("workflow-only Core worker should construct");
    (worker, feeder)
}

/// A worker whose Core config disables activities never starts an activity
/// poll. The absent lane stays open and idle — no task, timed-out waits —
/// until shutdown closes it, and the worker still joins and finalizes.
#[test]
fn disabled_activity_lane_is_idle_until_shutdown_and_finalizes() {
    let core = CoreRuntime::new(
        RuntimeOptions::builder().build().unwrap(),
        TokioRuntimeBuilder::default(),
    )
    .unwrap();
    let handle = core.tokio_handle();
    let (worker, feeder) = workflow_only_worker(&core);

    let mut lanes = PollLanes::start(worker, &handle);
    assert!(lanes.polls_workflow_tasks());
    assert!(!lanes.polls_activity_tasks());
    assert!(!lanes.task_types().enable_remote_activities);

    // An idle disabled lane is indistinguishable from a quiet one: the OCaml
    // supervisor must not observe a shutdown while the worker is running.
    assert!(lanes.try_take_activity(&handle).is_none());
    assert_eq!(lanes.wait_activity(), ReadinessWait::TimedOut);
    assert!(lanes.try_take_activity(&handle).is_none());

    {
        let _guard = handle.enter();
        lanes.initiate_shutdown();
    }
    assert_eq!(lanes.wait_activity(), ReadinessWait::Shutdown);
    drop(feeder);
    handle
        .block_on(lanes.join_poll_lanes())
        .expect("only the started workflow lane is joined");
    assert!(lanes.can_finalize());
    if let Err((_lanes, error)) = handle.block_on(lanes.finalize()) {
        panic!("worker without an activity lane must finalize: {error:?}");
    }
}

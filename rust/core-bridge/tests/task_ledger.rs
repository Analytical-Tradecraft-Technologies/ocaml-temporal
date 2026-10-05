use ocaml_temporal_core_bridge::worker_bridge::{
    ActivityAdmission, Admission, AdmitError, CompleteError, TaskLedger, bridge_task_types,
};

/// A workflow activation is admitted once and remains outstanding until its
/// matching run identifier is completed.
#[test]
fn workflow_run_ids_are_unique_and_completion_is_exact() {
    let mut ledger = TaskLedger::new();

    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::Duplicate));
    assert_eq!(
        ledger.complete_workflow("unknown"),
        Err(CompleteError::UnknownWorkflow)
    );
    assert_eq!(ledger.outstanding_workflows(), 1);
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
    assert_eq!(ledger.outstanding_workflows(), 0);
}

/// A cancellation admitted before the owner leases the start reuses the
/// original task token and therefore still creates only one completion
/// obligation.
#[test]
fn cancellation_before_start_lease_reuses_one_completion_debt() {
    let mut ledger = TaskLedger::new();
    let token = b"opaque-task-token";

    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::ExistingCancellation)
    );
    assert_eq!(ledger.outstanding_activities(), 1);
    assert_eq!(
        ledger.handoff_activity(token, ActivityAdmission::Cancel),
        Ok(())
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));
    assert_eq!(ledger.complete_activity(token), Ok(()));
    assert_eq!(ledger.outstanding_activities(), 0);
}

/// A cancellation queued after the owner completes its start is stale. Its
/// token no longer exists, so the handoff rejects it without creating a second
/// completion debt.
#[test]
fn cancellation_after_start_completion_has_no_second_completion_debt() {
    let mut ledger = TaskLedger::new();
    let token = b"opaque-task-token";

    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::ExistingCancellation)
    );
    assert_eq!(ledger.complete_activity(token), Ok(()));
    assert_eq!(ledger.outstanding_activities(), 0);
    assert_eq!(
        ledger.handoff_activity(token, ActivityAdmission::Cancel),
        Err(CompleteError::UnknownActivity)
    );
    assert_eq!(ledger.outstanding_activities(), 0);
}

/// A cancellation observed while the start is still leased is delivered to the
/// owner without acquiring a second lease. The start remains the sole
/// completion owner until it ends.
#[test]
fn cancellation_during_start_lease_is_delivered_without_second_debt() {
    let mut ledger = TaskLedger::new();
    let token = b"opaque-task-token";

    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::ExistingCancellation)
    );
    assert_eq!(
        ledger.handoff_activity(token, ActivityAdmission::Cancel),
        Ok(())
    );
    assert_eq!(
        ledger.lease_activity(token),
        Err(CompleteError::AlreadyLeased)
    );
    assert_eq!(ledger.outstanding_activities(), 1);
    assert_eq!(ledger.complete_activity(token), Ok(()));
    assert_eq!(ledger.outstanding_activities(), 0);
}

/// Repeated cancellation notifications do not poison shutdown. They update
/// the original start identity and must not be treated as dropped completion
/// leases merely because the same token appears more than once.
#[test]
fn repeated_cancellation_does_not_block_finalization() {
    let mut ledger = TaskLedger::new();
    let token = b"activity-token";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::ExistingCancellation)
    );
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::Duplicate)
    );
    assert_eq!(ledger.complete_activity(token), Ok(()));
    ledger.begin_draining();
    assert!(ledger.can_finalize());
}

/// Shutdown closes poll admission immediately, while preserving the ability to
/// finish work already leased to OCaml before final Core worker destruction.
#[test]
fn draining_rejects_new_tasks_until_existing_tasks_complete() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));

    ledger.begin_draining();
    assert!(ledger.admit_workflow("run-2").is_err());
    assert!(!ledger.can_finalize());
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
    assert!(ledger.can_finalize());
}

/// A poll lease dropped before safe completion makes ordinary outstanding
/// counts insufficient for finalization, even after known ledger debt drains.
#[test]
fn lost_poll_lease_blocks_finalization_after_debt_drains() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));

    ledger.mark_lost_poll_lease();
    ledger.begin_draining();
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));

    assert_eq!(ledger.outstanding(), 0);
    assert!(!ledger.can_finalize());
}

/// Core may issue cancellation for an already admitted activity while worker
/// shutdown is draining; that update must remain deliverable without admitting
/// any new completion obligation.
#[test]
fn draining_accepts_cancellation_for_an_existing_activity() {
    let mut ledger = TaskLedger::new();
    let token = b"activity";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    ledger.begin_draining();

    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::ExistingCancellation)
    );
    assert_eq!(
        ledger.handoff_activity(token, ActivityAdmission::Cancel),
        Ok(())
    );
    assert!(
        ledger
            .admit_activity(b"new", ActivityAdmission::Start)
            .is_err()
    );
}

/// Dispose cleanup must be able to harvest every outstanding identity once so
/// force-fail completions cannot miss a leased or unleased residual entry.
#[test]
fn take_all_outstanding_drains_workflow_and_activity_debt() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));
    let token = b"activity-token";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    let (workflows, activities) = ledger.take_all_outstanding();
    assert_eq!(workflows, vec![("run-1".to_owned(), false)]);
    assert_eq!(activities, vec![token.to_vec()]);
    assert_eq!(ledger.outstanding(), 0);
    assert!(!ledger.can_finalize()); // still Open, not Draining
    ledger.begin_draining();
    assert!(ledger.can_finalize());
}

/// The disposal snapshot reports which leased runs are pure cache evictions,
/// so dispose acknowledges them empty instead of failing them (issue #775).
/// An unleased entry reports `false`; dispose takes its bit from the queued
/// activation instead.
#[test]
fn take_all_outstanding_reports_leased_eviction_bits() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("evicted-run"), Ok(Admission::New));
    assert_eq!(
        ledger.lease_workflow_activation("evicted-run", true),
        Ok(())
    );
    assert_eq!(ledger.admit_workflow("task-run"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow_activation("task-run", false), Ok(()));
    assert_eq!(ledger.admit_workflow("queued-run"), Ok(Admission::New));

    let (mut workflows, activities) = ledger.take_all_outstanding();
    workflows.sort();
    assert_eq!(
        workflows,
        vec![
            ("evicted-run".to_owned(), true),
            ("queued-run".to_owned(), false),
            ("task-run".to_owned(), false),
        ]
    );
    assert!(activities.is_empty());
    assert!(!ledger.is_eviction_lease("evicted-run"));
    assert_eq!(
        ledger.admit_polled_workflow("evicted-run"),
        Err(AdmitError::Retired)
    );
}

/// A poll already in Core can finish after the first dispose drain has emptied
/// the ledger. The second drain, performed after both poll lanes join, must
/// still harvest that late identity rather than leaving completion debt behind.
#[test]
fn post_join_dispose_pass_harvests_late_poll_admissions() {
    let mut ledger = TaskLedger::new();
    let initial_token = b"initial-activity-token";
    assert_eq!(ledger.admit_workflow("initial-run"), Ok(Admission::New));
    assert_eq!(
        ledger.admit_activity(initial_token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    ledger.begin_draining();

    // This models the first force-complete pass, which runs before the poll
    // lanes join so Core can observe shutdown without waiting for OCaml.
    let (initial_workflows, initial_activities) = ledger.take_all_outstanding();
    assert_eq!(initial_workflows, vec![("initial-run".to_owned(), false)]);
    assert_eq!(initial_activities, vec![initial_token.to_vec()]);

    // A same-identity poll racing after the snapshot is rejected before the
    // lane can enqueue an `Ok` activation. This is why clearing retired
    // tombstones at the completed join barrier cannot expose a queued
    // duplicate.
    assert_eq!(
        ledger.admit_polled_workflow("initial-run"),
        Err(AdmitError::Retired)
    );
    assert_eq!(
        ledger.admit_polled_activity(initial_token, ActivityAdmission::Start),
        Err(AdmitError::Retired)
    );

    // A Core poll that was already in flight bypasses the first disposal
    // snapshot. The post-join pass must collect both new task shapes once
    // producers are known to have stopped.
    assert_eq!(ledger.admit_polled_workflow("late-run"), Ok(Admission::New));
    let late_token = b"late-activity-token";
    assert_eq!(
        ledger.admit_polled_activity(late_token, ActivityAdmission::Start),
        Ok(Admission::New)
    );

    let (late_workflows, late_activities) = ledger.take_all_outstanding();
    assert_eq!(late_workflows, vec![("late-run".to_owned(), false)]);
    assert_eq!(late_activities, vec![late_token.to_vec()]);
    assert_eq!(ledger.outstanding(), 0);
    assert_eq!(
        ledger.admit_polled_workflow("late-run"),
        Err(AdmitError::Retired)
    );
    assert_eq!(
        ledger.admit_polled_activity(late_token, ActivityAdmission::Start),
        Err(AdmitError::Retired)
    );
    assert!(ledger.can_finalize());
}

/// A late poll can interleave after the first disposal snapshot but before its
/// ready queue is drained. The queue reconciliation must retire that identity
/// before awaiting Core, so a duplicate poll cannot be admitted as a new task
/// and completed again by the post-join pass.
#[test]
fn disposal_snapshot_queue_boundary_retires_workflow_and_activity_once() {
    let mut ledger = TaskLedger::new();
    let initial_token = b"initial-dispose-token";
    assert_eq!(
        ledger.admit_workflow("initial-dispose-run"),
        Ok(Admission::New)
    );
    assert_eq!(
        ledger.admit_activity(initial_token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    ledger.begin_draining();

    // Snapshot phase: both initial identities become retired before any late
    // poll result can be reconciled.
    let (initial_workflows, initial_activities) = ledger.take_all_outstanding();
    assert_eq!(
        initial_workflows,
        vec![("initial-dispose-run".to_owned(), false)]
    );
    assert_eq!(initial_activities, vec![initial_token.to_vec()]);
    assert_eq!(
        ledger.admit_polled_workflow("initial-dispose-run"),
        Err(AdmitError::Retired)
    );
    assert_eq!(
        ledger.admit_polled_activity(initial_token, ActivityAdmission::Start),
        Err(AdmitError::Retired)
    );

    // Deterministic boundary injection: these represent poll results that
    // were admitted and queued after the snapshot, but before queue drain.
    let late_token = b"late-dispose-token";
    assert_eq!(
        ledger.admit_polled_workflow("late-dispose-run"),
        Ok(Admission::New)
    );
    assert_eq!(
        ledger.admit_polled_activity(late_token, ActivityAdmission::Start),
        Ok(Admission::New)
    );

    // Queue reconciliation retires each late identity before its asynchronous
    // force-failure. Calling it a second time models a duplicate queue entry;
    // the next poll is still rejected by the tombstone rather than creating a
    // second Core completion obligation.
    ledger.retire_workflow_for_dispose("late-dispose-run");
    ledger.retire_activity_for_dispose(late_token);
    ledger.retire_workflow_for_dispose("late-dispose-run");
    ledger.retire_activity_for_dispose(late_token);

    assert_eq!(
        ledger.admit_polled_workflow("late-dispose-run"),
        Err(AdmitError::Retired)
    );
    assert_eq!(
        ledger.admit_polled_activity(late_token, ActivityAdmission::Start),
        Err(AdmitError::Retired)
    );
    assert_eq!(ledger.outstanding(), 0);
}

/// A second lease for the same identity is rejected rather than silently
/// overwriting ownership.
#[test]
fn double_lease_is_rejected() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));
    assert_eq!(
        ledger.lease_workflow("run-1"),
        Err(CompleteError::AlreadyLeased)
    );
    let token = b"activity-token";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));
    assert_eq!(
        ledger.lease_activity(token),
        Err(CompleteError::AlreadyLeased)
    );
}

/// A dequeued activation that fails lease handoff must be removable from the
/// ledger while still unleased so force-fail cleanup cannot leave phantom debt.
#[test]
fn abandon_admission_removes_only_unleased_entries() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    ledger.abandon_workflow_admission("run-1");
    assert_eq!(ledger.outstanding_workflows(), 0);

    assert_eq!(ledger.admit_workflow("run-2"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-2"), Ok(()));
    ledger.abandon_workflow_admission("run-2");
    assert_eq!(ledger.outstanding_workflows(), 1);
    assert_eq!(ledger.complete_workflow("run-2"), Ok(()));

    let token = b"activity-token";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    ledger.abandon_activity_admission(token);
    assert_eq!(ledger.outstanding_activities(), 0);
}

/// Empty task identities are rejected before they can become keys whose
/// ownership or completion cannot be explained at the native boundary.
#[test]
fn task_identity_validation_rejects_empty_values() {
    let mut ledger = TaskLedger::new();

    assert!(ledger.admit_workflow("").is_err());
    assert!(
        ledger
            .admit_activity(&[], ActivityAdmission::Start)
            .is_err()
    );
    assert_eq!(ledger.outstanding(), 0);
}

/// A completion cannot consume a task that remains in Rust's ready queue and
/// has never crossed the lease handoff to the OCaml supervisor.
#[test]
fn completion_requires_a_prior_lease_handoff() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));

    assert_eq!(
        ledger.complete_workflow("run-1"),
        Err(CompleteError::NotLeased)
    );
    assert_eq!(ledger.outstanding(), 1);
}

/// A duplicate workflow poll cannot revoke the existing OCaml lease and make
/// a legitimate completion appear forged.
#[test]
fn duplicate_workflow_admission_preserves_lease_state() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));

    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::Duplicate));
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
}

/// A workflow activation rejected during Rust-to-semantic conversion was never
/// visible to OCaml. Retiring that exact lease prevents shutdown from waiting
/// for a completion the language runtime cannot possibly construct.
#[test]
fn rejected_workflow_conversion_retires_the_inaccessible_lease() {
    let mut ledger = TaskLedger::new();
    assert_eq!(
        ledger.admit_workflow("unrepresentable-run"),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_workflow("unrepresentable-run"), Ok(()));

    assert_eq!(
        ledger.retire_rejected_workflow("unrepresentable-run"),
        Ok(())
    );
    ledger.begin_draining();
    assert!(ledger.can_finalize());
    assert_eq!(
        ledger.retire_rejected_workflow("unrepresentable-run"),
        Err(CompleteError::UnknownWorkflow)
    );
}

/// Remote activity conversion failure follows the same one-shot ownership
/// rule, using the opaque Core token rather than a workflow run identifier.
#[test]
fn rejected_activity_conversion_retires_the_inaccessible_token() {
    let mut ledger = TaskLedger::new();
    let token = b"unrepresentable-activity";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));

    assert_eq!(ledger.retire_rejected_activity(token), Ok(()));
    ledger.begin_draining();
    assert!(ledger.can_finalize());
    assert_eq!(
        ledger.retire_rejected_activity(token),
        Err(CompleteError::UnknownActivity)
    );
}

/// Decode drift may reject workflow and activity deliveries concurrently with
/// shutdown. Retiring both exact leased identities must make the draining
/// ledger finalizable without fabricating a completion from OCaml.
#[test]
fn rejected_language_deliveries_unblock_shutdown_drainage() {
    let mut ledger = TaskLedger::new();
    let token = b"decode-drift-activity";
    assert_eq!(
        ledger.admit_workflow("decode-drift-run"),
        Ok(Admission::New)
    );
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_workflow("decode-drift-run"), Ok(()));
    assert_eq!(ledger.lease_activity(token), Ok(()));
    ledger.begin_draining();
    assert!(!ledger.can_finalize());

    assert_eq!(ledger.retire_rejected_workflow("decode-drift-run"), Ok(()));
    assert_eq!(ledger.retire_rejected_activity(token), Ok(()));
    assert!(ledger.can_finalize());
}

/// Rejection is correlated by exact native identity. A changed run ID or task
/// token must not consume the genuine lease that remains owed during shutdown.
#[test]
fn changed_rejection_identities_do_not_retire_real_leases() {
    let mut ledger = TaskLedger::new();
    let token = b"real-activity-token";
    assert_eq!(ledger.admit_workflow("real-run"), Ok(Admission::New));
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_workflow("real-run"), Ok(()));
    assert_eq!(ledger.lease_activity(token), Ok(()));

    assert_eq!(
        ledger.retire_rejected_workflow("changed-run"),
        Err(CompleteError::UnknownWorkflow)
    );
    assert_eq!(
        ledger.retire_rejected_activity(b"changed-activity-token"),
        Err(CompleteError::UnknownActivity)
    );
    assert_eq!(ledger.outstanding(), 2);

    assert_eq!(ledger.retire_rejected_workflow("real-run"), Ok(()));
    assert_eq!(ledger.retire_rejected_activity(token), Ok(()));
    ledger.begin_draining();
    assert!(ledger.can_finalize());
}

/// Regression test for the completion/eviction retirement race: a terminal
/// workflow completion must retire its ledger lease before Core is asked to
/// accept it, exactly like [`retire_rejected_workflow`]. A successful
/// completion causes Core to schedule a follow-up cache-eviction activation
/// for the same run_id, which the background poll lane can observe while the
/// completion await is still in flight. If the run were still recorded at
/// that moment, the eviction would be misclassified as `Admission::Duplicate`
/// and raise a fatal `PollLaneError::DuplicateIdentity` on an otherwise
/// healthy worker, and the eviction's own completion debt would be dropped
/// rather than acknowledged. This models `PollLanes::complete_workflow`'s
/// retire-before-await ordering directly against the ledger.
#[test]
fn completed_workflow_lease_retires_before_ack_allowing_eviction_readmission() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));

    // Models `PollLanes::complete_workflow` retiring the ledger lease before
    // the Core completion await resolves.
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));

    // The follow-up cache eviction for the same run_id must be admitted as a
    // fresh obligation rather than misclassified as a duplicate of the
    // already-completed run.
    assert_eq!(ledger.admit_polled_workflow("run-1"), Ok(Admission::New));
}

/// When Core rejects a terminal completion, the run never actually completed
/// from Core's point of view, so the language side must be able to retry with
/// a corrected completion for the same run_id. Retiring the lease before the
/// Core await (previous test) would otherwise permanently strand that retry
/// behind an `UnknownWorkflow` error; restoring the lease on rejection keeps
/// the original retry contract intact.
#[test]
fn rejected_completion_restores_the_retired_lease_for_retry() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));

    // Models the optimistic retirement `PollLanes::complete_workflow` performs
    // before the Core completion await.
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
    assert_eq!(
        ledger.ensure_workflow_leased("run-1"),
        Err(CompleteError::UnknownWorkflow)
    );

    // Core rejected the completion: restore the lease so a corrected retry
    // can be sent for the same run.
    ledger.restore_rejected_workflow_completion("run-1");
    assert_eq!(ledger.ensure_workflow_leased("run-1"), Ok(()));
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
}

/// A rejected completion cannot have caused Core to schedule a follow-up
/// eviction, so no other producer should legitimately re-admit the same
/// run_id while the rejection's restore is pending. If that invariant is
/// ever violated, restoring must not silently stamp a fresh, still-unleased
/// admission as leased behind its real owner's back.
#[test]
fn restore_after_rejection_does_not_clobber_a_legitimately_readmitted_run() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));

    assert_eq!(ledger.admit_polled_workflow("run-1"), Ok(Admission::New));

    ledger.restore_rejected_workflow_completion("run-1");
    assert_eq!(
        ledger.ensure_workflow_leased("run-1"),
        Err(CompleteError::NotLeased)
    );
}

/// A worker registering both kinds polls workflows, remote activities, and
/// the local activity lane; Nexus remains outside this bridge's scope.
#[test]
fn bridge_enables_only_supported_core_task_types() {
    let task_types = bridge_task_types(true, true).expect("both kinds are valid");

    assert!(task_types.enable_workflows);
    assert!(task_types.enable_remote_activities);
    assert!(task_types.enable_local_activities);
    assert!(!task_types.enable_nexus);
}

/// Worker shutdown takes exactly the leases OCaml abandoned (issue #769).
/// Unleased handoffs stay in the ledger because shutdown retires each one
/// from its own ready-queue message, and no tombstone is written, so Core's
/// follow-up eviction for a failed workflow task is admitted as new debt.
#[test]
fn shutdown_takes_only_leased_tasks_and_keeps_eviction_kind() {
    let mut ledger = TaskLedger::new();
    let leased_token: &[u8] = b"leased-activity";
    let queued_token: &[u8] = b"queued-activity";
    for run_id in ["leased-task", "leased-eviction", "queued-task"] {
        assert_eq!(ledger.admit_polled_workflow(run_id), Ok(Admission::New));
    }
    assert_eq!(
        ledger.lease_workflow_activation("leased-task", false),
        Ok(())
    );
    assert_eq!(
        ledger.lease_workflow_activation("leased-eviction", true),
        Ok(())
    );
    for token in [leased_token, queued_token] {
        assert_eq!(
            ledger.admit_polled_activity(token, ActivityAdmission::Start),
            Ok(Admission::New)
        );
    }
    assert_eq!(
        ledger.handoff_activity(leased_token, ActivityAdmission::Start),
        Ok(())
    );
    ledger.begin_draining();

    let (mut workflows, activities) = ledger.take_leased_for_shutdown();
    workflows.sort();
    assert_eq!(
        workflows,
        vec![
            ("leased-eviction".to_owned(), true),
            ("leased-task".to_owned(), false)
        ]
    );
    assert_eq!(activities, vec![leased_token.to_vec()]);
    // Only the queued handoffs remain; a second pass takes nothing.
    assert_eq!(ledger.outstanding(), 2);
    assert_eq!(ledger.take_leased_for_shutdown(), (Vec::new(), Vec::new()));
    // Core's eviction after the failed task is a fresh obligation.
    assert_eq!(
        ledger.admit_polled_workflow("leased-task"),
        Ok(Admission::New)
    );
}

/// The eviction bit follows the lease it describes: a completed eviction
/// leaves no stale bit, a rejected completion restores it, and a later real
/// workflow task for the same run is not mistaken for an eviction.
#[test]
fn eviction_lease_bit_tracks_the_current_lease() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_polled_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow_activation("run-1", true), Ok(()));
    assert!(ledger.is_eviction_lease("run-1"));

    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
    assert!(!ledger.is_eviction_lease("run-1"));
    assert!(ledger.restore_rejected_workflow_completion("run-1"));
    ledger.restore_eviction_lease("run-1");
    assert!(ledger.is_eviction_lease("run-1"));

    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
    assert_eq!(ledger.admit_polled_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow_activation("run-1", false), Ok(()));
    assert!(!ledger.is_eviction_lease("run-1"));
}

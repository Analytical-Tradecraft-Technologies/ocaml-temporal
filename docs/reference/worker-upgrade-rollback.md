# Worker upgrade and rollback rehearsal

**Status: proposed operator procedure, not an accepted upgrade.** This is the
rehearsal plan for [#508]. No authenticated old-to-new deployment, mixed-version
routing, drain, or rollback has been demonstrated by this document. The proposed
[MVP support policy in PR #557] defers cross-version history compatibility and
worker routing; it is not effective until approved and merged. The successful
[same-version restart] and [patch lifecycle] fixtures are useful prechecks, not
an upgrade rehearsal. Keep #508 open until its recorded live acceptance is
complete.

## Routing decision for the first rehearsal

Use a separate task queue for each incompatible application worker generation,
with `Temporal.Worker.Options.No_versioning`. Keep the old worker polling the
old queue while new starts are directed to a new queue through
`Temporal.Client.start ~task_queue`. `No_versioning` is the SDK default. The
application deployment, not this SDK, owns the queue-name change, worker
processes, traffic controls, and rollback decision. This strategy avoids
claiming that the [legacy build-ID or deployment-based options] register builds
or have been proved to route live tasks; it does **not** make histories or
payloads compatible across versions.

Before choosing queue names, inventory every workflow type and every explicit
`~task_queue` on remote activities and child workflows. An activity without an
override uses its workflow context's queue; an explicit override can send work
elsewhere. A child workflow can also name another queue. Record the actual
queue for each run and task from server evidence rather than inferring routing
from the name of the worker that happened to log it. Keep workers for all
queues needed by open parent, child, and activity work. Do not move an existing
run to a new queue by merely changing the application's start configuration.
`Workflow.continue_as_new` exposes no task-queue parameter; record its
successor run ID and verify its actual queue on the server before deciding that
either worker can be retired.

| Owner | Decision or record |
| --- | --- |
| Release owner | Approves the exact old/new source commits, SDK/Core and server versions, candidate behavior change, stop conditions, and final evidence. |
| Application owner | Inventories workflow and payload schemas, patch IDs, explicit child/activity queues, external side effects, and each open run's exact identity. |
| Deployment operator | Owns authenticated endpoint configuration, queue routing for new starts, worker process supervision, visibility, backups, rollback controls, and retained logs. |

The [current live fixture] uses a controlled plaintext Temporal/PostgreSQL
stack. An authenticated endpoint, credentials, backup/restore, and production
worker deployment belong to the application operator and need separate
qualification. Do not treat a green fixture job as that qualification.

## Gates before starting a live rehearsal

1. Pin the old and new source commits, package versions, Core revision, server
   version, worker image digests, namespace, old/new task queues, and the
   application release that chooses `Client.start`'s queue. Record the rollback
   decision-maker and a finite observation window. Preserve the old executable
   and its configuration until every old-queue run can finish or has an approved
   migration plan.
2. Replay retained histories with the old and candidate code using the
   compatibility corpus required by [#503]. Review any changed command order,
   patch marker, payload codec/schema, error type, activity name, child type,
   or task-queue override. Run the [patch lifecycle] gate if a patch changes.
   A green same-version restart does not establish cross-version replay.
3. Pass the exact candidate's build, installed-consumer, dependency, and
   [live acceptance] checks. Include the worker resource/shutdown evidence
   required by [#498], supported-feature conformance from [#505], and
   transport uncertainty cases from [#504]. An incomplete prerequisite is a
   failed upgrade gate, even if this runbook is otherwise usable.
4. Define workload-specific limits for pending workflows, activities, timers,
   and interactions; a maximum acceptable completion latency; observability
   for both queues; a process termination bound; and a response for an
   indefinitely blocked callback. Temporal timeouts do not interrupt an OCaml
   callback, and a transport timeout may leave a completion outcome uncertain.
   Reconcile such an outcome against both the exact run's server history and
   the external system's application-owned idempotency key, ledger, or provider
   lookup before retrying a side effect. History alone cannot prove whether an
   external payment or write occurred.
5. Capture a recoverable server backup or an operator-approved equivalent,
   verify access to exact-run histories and worker logs, and record how the
   application stops **new starts** without deleting or terminating open runs.

Useful repository prechecks are `make test-temporal-worker-restart`,
`make test-temporal-workflow-patching`, and `make test-temporal-live-ci` on a
disposable stack. They cannot replace the authenticated deployment rehearsal
or the retained-history corpus. Record the command, commit, run URL, result,
and retrieved artifacts; do not cite the presence of a test target as a pass.

## Rehearsal sequence and stop conditions

1. **Baseline:** With the old worker on the old queue, start representative
   long-lived exact runs. Include a durable timer, a retryable remote activity,
   and each interaction included in the approved workload. Record workflow ID,
   run ID, queue, initial history, worker build, and expected terminal outcome.
   Confirm the runs are still open before cutover.
2. **Introduce the new worker:** Start the new executable on the new queue
   without stopping the old worker. Prove both workers are polling their
   intended queues. Switch only *new starts* to the new queue; retain their
   returned run IDs. Observe one exact new run through completion and query
   its server history and queue. Abort the start switch if routing, health, or
   an expected outcome differs.
3. **Mixed window:** Keep the old and new runs open together. Exercise timer
   firing, activity retry, and the supported interactions. Capture per-run
   histories and generation-labelled worker diagnostics. Confirm old runs
   progress on the old queue and new runs on the new queue, including any
   explicit child or activity queue. Stop further new starts if a run stalls,
   replays nondeterministically, produces an incompatible payload, or receives
   work on an unexpected queue; keep the workers required by open runs alive.
4. **Drain:** Continue monitoring exact open runs and pending tasks on every
   old queue. Stop an old worker only when the operator proves no open run or
   pending task still needs it, and verifies that a fresh start cannot target
   its queue. A long-lived run that does not drain is a reason to retain its
   old worker or design and separately prove a migration; elapsed time alone
   is not evidence of drain. Rehearse same-version restart and bounded stop
   separately before relying on either during the cutover.
5. **Close the observation window:** Compare every retained workflow/run ID
   with its expected terminal class and decoded result, server history,
   activity/child outcome, and any remaining open work. Keep old images and
   logs until the release owner accepts the evidence and rollback window.

At every stage, stop new starts and escalate if either queue is unobservable,
a run cannot be matched to its exact history, a worker cannot stop within the
agreed process bound, an external side effect has an uncertain outcome, or a
replay/determinism error appears. Do not delete queues, histories, or old
workers as a diagnostic shortcut.

## Rollback decision by stage

| Stage and evidence | Safe operator action | Action requiring separate proof |
| --- | --- | --- |
| Before any new-queue run is accepted | Keep the old worker and revert the application starter and its *new starts* to the old queue, after checking that the old worker accepts their workflow type and input. Stop the unused new worker after checking it owns no task. | None of this proves a future mixed-version downgrade. |
| New-queue runs exist, but no incompatible history is known | Route **future** starts back to the old queue only after checking that the old worker accepts their workflow type and input; keep the new worker for its existing runs. Inspect exact histories and payloads before any attempted migration. | Sending an existing new-queue run to old code, or stopping its worker, requires a proved compatible replay/migration path. |
| New code has written a history, patch marker, or payload that old code cannot replay | Stop further new starts, retain the new worker for affected runs, and deploy a forward fix or a separately validated history-compatible replacement. | Never point those runs at the old worker simply because the old application binary is still available. |
| Worker is unavailable or a completion outcome is uncertain | Preserve server histories and both workers' diagnostics, restore the same history-compatible worker if possible, and reconcile the exact run plus the external idempotency record before any side-effect retry. | Server/database restore or rollback follows the server operator's separate recovery plan; it is not an SDK rollback operation. |

## Evidence record and completion boundary

For each rehearsal, retain a manifest with: date and environment; old/new
commit and image digest; SDK/Core/server versions; namespace and queue names;
start-routing change and rollback timestamps; exact workflow/run IDs; parent,
child, activity, timer and interaction observations; normalized initial and
terminal histories; worker logs and process exit status; replay-corpus result;
backup reference; metrics/alerts; stop-condition decisions; and the operator's
signed acceptance or rejection. Link the CI run and downloadable artifacts.
Redact payloads and credentials from diagnostics.

The runbook only supplies a reviewable procedure. [#508] is complete after a
reproducible authenticated rehearsal records old-to-new routing, drain,
restart, rollback at the stages above, and at least one incompatible rollback
case with a demonstrated forward recovery. Until then, report upgrade and
rollback as unqualified in the support policy and release notes.

[#508]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/508
[#498]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/498
[#503]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/503
[#504]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/504
[#505]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/505
[MVP support policy in PR #557]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/557
[same-version restart]: worker-restart-replay-acceptance.md
[patch lifecycle]: workflow-patching.md
[legacy build-ID or deployment-based options]: worker-versioning.md
[current live fixture]: local-temporal-stack.md
[live acceptance]: live-acceptance-coverage.md

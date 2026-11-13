# Live acceptance coverage and evidence

This reference records the live evidence re-audited for issue #1142 on
2026-11-13 at master commit [`7963a9a68e3c`][baseline]. It replaces the #491
audit of `beae10d0a58e` (2026-09-20), which qualified the earlier Core pin
`95e97686` only. **Implemented**, **focused-tested**, **live-tested**, and
**supported** are different claims. A source definition or Docker-free contract
proves neither a successful live run nor production support. The package
remains experimental; [#489][policy] tracks the v1 support decision. This is a
master snapshot, not release-candidate qualification: the
[support policy](v1-support-policy.md) still requires evidence for the exact
candidate commit.

## Verified CI boundary

The [November 10 Build run][build] (a push to `master`) completed successfully
at the audited commit. Its three Temporal/PostgreSQL shards, [baseline][live-baseline],
[recovery][live-recovery] and [replay][live-replay], and the required
[integration result][live-result] that passes only when every shard did,
each succeeded. Every controller in the tables below ran in one of those shard
logs, and each one fails its job on any assertion failure. The baseline shard
prints the driver's final `two-binary driver assertions passed` marker and a
`live regression <suite>: ok` line for each of the 24 regression suites. The
recovery shard prints `cache eviction driver assertions passed` and the
start-metadata `acceptance passed` line; the replay shard runs the patch and
parent/child controllers. A queued, cancelled or failed run is not evidence.

| Evidence | Verified configuration | Limit |
| --- | --- | --- |
| Live server job | Linux `ubuntu-24.04`, OCaml 5.5, independent OCaml worker/client processes, plaintext Temporal Server 1.32.2 and PostgreSQL 18.6 in Compose | One server/compiler/platform/topology combination; no TLS/authentication, Temporal Cloud, upgrade, or production qualification |
| Linux build/test matrix | OCaml 5.2, 5.3, 5.4, and 5.5 on amd64 and arm64 | Build, lint, focused tests, and source contracts; these are not additional live-server lanes |
| Native build/test matrix | OCaml 5.5 on Windows x64 and macOS ARM64 | Native package/bridge/runtime validation, without the Compose server suite |
| Pins | [Compose images][compose] include immutable image digests; [Cargo manifest][cargo] pins Temporal Core to `8fe478ee179ed754614d230f39586f3edb57da71`<!-- pin:core --> (v1.1.0, #989); [`rust-toolchain.toml`][toolchain] selects Rust `1.99.1`<!-- pin:rust -->, which the Makefile, CI and Rust images read through `scripts/rust-version.sh`; the [workflow][workflow] identifies the executed lanes | A successful snapshot does not qualify later image, Core, toolchain, or source changes |

`make check-doc-pins` (part of `make test-quality-contract`) fails when the Core
pin or Rust toolchain quoted in this row, or marked in the other reference
documents, differs from `rust/Cargo.toml` or `rust/rust-toolchain.toml`. A pin
change therefore forces this record to be revisited. Re-audit it against a new
successful run rather than only editing the quoted value.

The workflow and Makefile must be read at the tested commit. The earlier
records remain historical evidence for their own commits: the #491 audit
([September 19 run][previous-build] at [`beae10d0a58e`][previous-baseline])
and the first run on Core v1.1.0 ([October 12 run][core-upgrade-build] at
[`acbda8dad9a3`][core-upgrade-commit]).

## Live controllers

These are the public Makefile entry points invoked by the [audited workflow][workflow].
Each specialized wrapper runs its source contract before its live controller.
All live controllers use a real Temporal Server and own fixture cleanup.

| Make target | Assertion boundary at the tested commit | Source and contract |
| --- | --- | --- |
| `test-temporal-integration` | Core lifecycle plus the independent baseline worker/driver; exact outcomes, readiness barriers, client shutdown and worker stop marker | [Makefile][makefile], [driver][driver], [definitions][definitions], [lifecycle test][lifecycle] |
| `test-temporal-worker-restart` | Graceful worker replacement preserves the exact run; generation two replays and produces `SMOKE:AFTER-REPLAY:ATTEMPT:2` | [Makefile][makefile], [restart driver][restart-driver], [restart contract][restart-contract] |
| `test-temporal-worker-crash-recovery` | Forced generation-one exit 137 without a graceful-stop marker; replacement replay and exact terminal result | [Makefile][makefile], [crash contract][crash-contract] |
| `test-temporal-worker-cache-eviction` | One-slot cache pressure, Core `RemoveFromCache` with `cache_full`, empty eviction acknowledgement, continued progress and exact-run cancellation | [Makefile][makefile], [eviction driver][eviction-driver], [eviction contract][eviction-contract] |
| `test-temporal-workflow-patching` | Separately compiled sources exercise marker-free to active, active to deprecated, and deprecated to removed patch calls; normalized history and marker assertions | [patch controller][patch-controller], [patch contract][patch-contract] |
| `test-temporal-parent-child-restart` | Exact parent and child histories, linkage, nonterminal prefixes after removal, both generation-two replay observations, then successful completion | [parent/child controller][parent-controller], [parent/child contract][parent-contract] |
| `test-temporal-parent-child-failure-replay` | Both runs replay before the child fails non-retryably; child and parent failure events retain exact linkage; parent returns `SMOKE:PARENT:CHILD:FAILURE_RECOVERED` | [failure controller][failure-controller], [failure contract][failure-contract], [definitions][definitions] |
| `test-temporal-start-metadata-live` | Five exact active runs carry memo, search-attribute, combined, continued and official-CLI start metadata; workflow snapshots match before and after worker replacement. Three more runs on a one-slot-cache worker are each evicted with reason `cache_full` and replayed; snapshots, exact expirations and results match | [start metadata controller][metadata-controller], [start metadata](workflow-start-metadata.md) |
| `test-temporal-task-failure-live` | Body, encoder and missing-registration defects leave exact runs with durable workflow-task failures; a corrected worker replays and completes them, and typed business failures keep their retryability flags | [task-failure controller][task-failure-controller], [workflow failures](workflow-failures.md) |
| `test-temporal-examples-live` | The shipped example's split activity and workflow workers serve a valid name with the documented two-line result and fail a blank name with the typed business failure; both workers stop cleanly on `SIGTERM`; the cooperative cancellation example and the idempotent side-effect example (#504) print their exact documented reports | [examples controller][examples-controller], [examples](../../examples/README.md) |
| `test-temporal-live-regressions` | Each of the 24 regression executables in `LIVE_REGRESSION_EXECUTABLES` runs once, bounded, against a fresh stack: cooperative activity cancellation and the worker-shutdown signal (#494); abrupt worker death during a running activity, recovered on the server's heartbeat timeout (#519); worker death after an activity's external side effect commits and before its completion, with the redelivered attempt answered as a duplicate by the example ledger's atomic idempotency key (#504); bounded shutdown of a worker whose activity never heartbeats, with the attempt retried on a second worker (#495); child start options (#1135); client start policies and RPC deadlines (#499); client request IDs (#545); completed-workflow queries (#548); concurrent calls on one client (#807); `Client.describe` and `Client.count` (#1152); `Client.fetch_history` replay (#1137); query and suspended-update recovery after sticky-cache eviction and worker replacement (#530); local activity retry cancellation (#567); detection, task failure and bounded shutdown of a worker whose workflow code stops yielding, with the run recovered on a corrected worker (#493); official-SDK interoperability (#517), where the pinned official Python SDK client and activity worker drive an OCaml workflow and both clients decode the same exact runs, and signal-with-start and update-with-start run in both directions against a workflow of each SDK (#1151); split workflow/activity workers (#805, #1211); durable timer cancellation (#1134); transport interruption and uncertain outcomes (#504); update admission outcomes (#546); update callers when the run closes (#520); `Client.signal_with_start` and `Client.update_with_start` (#1151); Prometheus worker metrics and log correlation (#500); worker resource and shutdown options under saturation (#498); per-workflow worker versioning (#1159); and workflow start info (#512). The run fails if any suite fails; a Docker-free contract requires every `test/integration/*/regression.exe` to be in the smoke artifact and run by CI (#795) | [regression runner][regressions-controller]; each suite's `README.md` or module comment under `test/integration/` |

The [generated inventory](live-acceptance-inventory.md) tracks the membership of
these targets.

The [child-failure acceptance reference](child-failure-replay-acceptance.md)
records the original PR #361 evidence. Its status is live-tested, not pending a
first run. Broader child failure, cache-pressure, repeated restart, and recovery
combinations still need dedicated cases.

## Worker crash, restart, shutdown and eviction inventory

Issue [#519][issue-519] names three boundaries. The existing suites below
already cover part of each; this table records which part, so new cases extend
them rather than duplicate them. Every suite asserts a durable server outcome
for exact runs, not just a process exit, and each runs in the Build workflow's
Temporal/PostgreSQL job through `make test-temporal-live-ci`.

| Boundary | Suite | What it removes or forces, and when | Durable outcome asserted |
| --- | --- | --- | --- |
| Worker removal during pending work | `test-temporal-worker-crash-recovery` | `docker kill --signal KILL` on generation one once the exact run's history shows its pending timer; exit 137, no graceful-stop marker | Generation two replays (`is_replaying=true`) and the same run completes |
| Worker removal during pending work | `test-temporal-parent-child-restart` | Graceful `Worker.shutdown` of generation one while a parent and its long-running child are both suspended | Both exact runs replay on generation two and complete with exact parent/child linkage |
| Worker removal during a running activity callback | `activity_worker_crash` (#519) | `SIGKILL` to the worker process (hosting the workflow and the activity) after the callback's marker file and `workflow describe` both show attempt 1 started and heartbeating on it; the process must die of that signal | With no worker polling, the server alone fails attempt 1 on its three-second heartbeat timeout and schedules attempt 2. A replacement process completes attempt 2 (`Activity.Info.attempt`), reads attempt 1's last heartbeat detail, and the run returns the exact expected result; history has one scheduled, started (attempt 2, replacement identity, heartbeat-timeout last failure) and completed activity event and no activity failure or timeout |
| Worker removal after an external side effect, before its completion | `activity_idempotency` (#504) | The activity callback posts line 1 to the example's file-backed ledger, whose hard-link publication deduplicates by idempotency key atomically, then sends `SIGKILL` to its own worker process before returning; the process must die of that signal with exactly that record in the ledger | Temporal times attempt 1 out and schedules attempt 2 (`Client.describe`). A replacement process's attempt 2 sends the same key and is answered as a duplicate; line 2, with an identical posting, is posted under its own key. The run returns both exact receipts; the ledger holds two records and a three-request journal correlated to the exact workflow, run, activity, attempt and generation; history has two scheduled, started and completed activities (line 1 started as attempt 2 on the replacement, timeout last failure) and no activity failure or timeout |
| Worker removal during pending work | `interaction_recovery` (#530) | `SIGTERM` to the worker process that accepted a suspended update | The replacement answers queries from replayed state; history has one acceptance and one completion of the original update ID, attributed to the original and replacement workers |
| Shutdown with outstanding work | `bounded_shutdown` (#495) | `Worker.shutdown_with_report` with a two-second grace period while an activity callback ignores cancellation and the shutdown flag | Shutdown returns within its bound and reports one abandoned callback; Temporal retries the attempt and a second worker completes attempt 2 |
| Eviction followed by continued progress | `test-temporal-worker-cache-eviction` | One-slot cache; a second parked run forces Core `RemoveFromCache` (`cache_full`) of the first | Empty eviction acknowledgement; evicted run A resumes on its signal and completes, then B is cancelled with a typed terminal outcome (see the [#519 matrix](#issue-519-worker-recovery-regression-matrix)) |
| Eviction followed by continued progress | `interaction_recovery` (#530) | One-entry sticky cache; a filler run evicts the target while an update handler is suspended | A query answers from replayed state, the release signal resumes the handler from history, and the run's durable events equal the control run's |

Reproduce each against the pinned Compose stack (Docker required). The first
three targets each own a fresh Temporal/PostgreSQL lifecycle:

```sh
make test-temporal-worker-crash-recovery
make test-temporal-worker-cache-eviction
make test-temporal-parent-child-restart
# All LIVE_REGRESSION_EXECUTABLES, including interaction_recovery,
# bounded_shutdown, activity_worker_crash and activity_idempotency, against
# one fresh stack; logs
# land in _build/live-regressions/.
make test-temporal-live-regressions
```

To run one regression suite against an already running disposable server, use
its focused target. `interaction_recovery`, `activity_worker_crash` and
`activity_idempotency` need a
`default` namespace and the official Temporal CLI; `bounded_shutdown` defaults
to `temporal-sdk-test`:

```sh
make test-interaction-recovery-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/absolute/path/to/temporal
make test-activity-worker-crash-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/absolute/path/to/temporal \
  ACTIVITY_WORKER_CRASH_EVIDENCE=$PWD/_build/activity-worker-crash
make test-activity-idempotency-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/absolute/path/to/temporal \
  ACTIVITY_IDEMPOTENCY_EVIDENCE=$PWD/_build/activity-idempotency
make test-bounded-shutdown-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233
```

In CI, the controllers' bundles are in the `live-diagnostics-<shard>-<run>-<attempt>`
artifact of the shard that ran them ([live diagnostic artifacts](live-diagnostic-artifacts.md)), and each
regression suite's log, plus the official-SDK interoperability evidence, is in
`live-regressions-<run>-<attempt>`. That artifact also holds
`activity_worker_crash/`: the suite's `describe` snapshots before and after the
kill, the raw exact-run history, and each worker generation's log (the logs are
copied into the suite log too), and `activity_idempotency/`: the suite's
ledger directory, its `correlation.json` summary, `describe` snapshots, the raw
exact-run history and both worker generations' logs. Both artifacts are
uploaded even when the step fails and kept for seven days.

The `activity_worker_crash` suite covers the case the other suites leave open:
an abrupt process death while an activity callback is running. The crash
controller kills its worker while the run is parked on a timer, and
`bounded_shutdown` abandons a running callback through the graceful path in a
process that stays alive. Here recovery rests on Temporal's heartbeat timeout
and retry policy alone, the documented at-least-once contract.

## Issue #519 worker recovery regression matrix

For [issue #519](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/519),
this matrix separates the existing live-tested restart and crash cases from
the stronger cache-eviction assertion. The revised cache controller passed in
the audited run's recovery shard, which prints `cache eviction driver
assertions passed` and `cache eviction progress histories: ok`. The
[Makefile](../../Makefile), [restart driver](../../test/integration/temporal/driver/restart_driver.ml),
[cache driver](../../test/integration/temporal/driver/cache_eviction_driver.ml),
and [workflow definitions](../../test/integration/temporal/common/smoke_definitions.ml)
are the current source for these assertions.

| Case and command | Fault barrier and required durable outcome | Evidence and limit |
| --- | --- | --- |
| Graceful shutdown with pending work: `make test-temporal-worker-restart` | The controller records the exact run's initial server history with `TimerStarted` and no `TimerFired` before stopping generation one. `temporal-stop-worker` gives Compose 30 seconds and requires the worker's post-shutdown marker. A fresh generation-two container must replay that same run and complete with `SMOKE:AFTER-REPLAY:ATTEMPT:2`; process exit alone does not pass. | Exact initial/terminal histories, replay diagnostics, controller phases, and worker logs. This covers a parked timer, not a long-running OCaml activity callback or an orchestrator termination deadline. |
| Forced removal with pending work: `make test-temporal-worker-crash-recovery` | The same pending-timer history precedes `SIGKILL`; generation one must exit 137 without a graceful-stop marker and be removed. A new container must replay the exact run and reach the same successful terminal result. | The same exact-run evidence plus crash mode and exit code in the controller. One kill/replacement cycle does not cover repeated churn or every in-flight command. |
| Cache eviction followed by normal progress: `make test-temporal-worker-cache-eviction` | A parks on a replay-safe signal condition. Its initial activation completion is acknowledged before timer-backed B starts; the observer publishes A's exact `RemoveFromCache(CacheFull)` marker only after Core acknowledges removal. The driver then waits for B's initial activation acknowledgement, signals exact run A while B is outstanding, requires `SMOKE:CACHE:FIRST:RESUME` and terminal `WorkflowExecutionCompleted`, and cancels B only afterward. B's exact history must contain `TimerStarted`, `WorkflowExecutionCancelRequested`, and terminal `WorkflowExecutionCanceled`. | The [cache history validator](../../test/integration/temporal/scripts/validate-cache-eviction-progress.sh) checks the marker and payload-free histories for both run IDs; the driver log and worker logs retain fault phases. This is a one-slot, two-run case, live-verified for #519 at the audited commit. A has no timer; B supplies the pending timer boundary. |

For a reproducible run that retains filtered worker logs, available normalized
exact-run histories, and failure phases under `_build/live-diagnostics/`, use the same
[diagnostic wrapper](../../test/integration/temporal/scripts/run-with-live-diagnostics.sh)
as CI:

```sh
bash test/integration/temporal/scripts/run-with-live-diagnostics.sh restart make test-temporal-worker-restart
bash test/integration/temporal/scripts/run-with-live-diagnostics.sh crash make test-temporal-worker-crash-recovery
bash test/integration/temporal/scripts/run-with-live-diagnostics.sh cache-eviction make test-temporal-worker-cache-eviction
```

Each command owns a disposable Temporal/PostgreSQL Compose stack. The cache
driver has a 900-second process watchdog; its marker waits share the same
deadline, and each CLI identity/history query has a 60-second bound. The
controller checks each `workflow describe` run identity before accepting its
normalized history. Failure output identifies the exact run IDs and last
completed phases. The [diagnostic collector](../../test/integration/temporal/scripts/collect-live-diagnostics.sh)
retains bounded, filtered artifacts rather than raw history payloads. A failed
or timed-out run is evidence of an unresolved boundary, not passing qualification.
The cache controller fetches both histories on success and makes a best-effort
exact-run query during failure cleanup once a run ID is known. A pre-start
failure, unavailable Temporal service, or failed history query can still leave
a normalized history absent; the collector records the available phases and logs.

## Baseline scenario assertions

The following names refer to the [exact workflow definitions][definitions] and
[driver assertions][driver] exercised in the successful job. Workflow types,
client starts, child runs, continue-as-new successors, and individual assertions
have different totals; none is a substitute for the named evidence below.

| Scenario | Verified result or boundary | Remaining limit |
| --- | --- | --- |
| `smoke.fan_out`, `smoke.timer_then_activity` | Two scheduled activities return `SMOKE:LEFT\|SMOKE:RIGHT`; timer then activity returns `SMOKE:TIMER` | Does not exhaust future ordering, timer cancellation, or duration edge cases |
| `smoke.local_activity` | `Activity.execute_local` runs the typed local callback and returns `LOCAL` | Local retry/backoff, cancellation, marker replay and restart combinations remain focused-test or unqualified paths |
| `smoke.continue_as_new` | Client observes continuation, follows the explicit successor identity and receives `SMOKE:CONTINUED:SECOND` | No arbitrary chain or upgrade compatibility claim |
| `smoke.activity_retry`, `smoke.activity_heartbeat_retry` | Attempt two is required; the heartbeat retry consumes the prior attempt's copied detail and timeout | No complete retry-policy or jitter conformance claim |
| `smoke.activity_long_backoff_retry` | Configured two-second backoff; second callback rejects an elapsed delay under one second and returns `SMOKE:BACKOFF:RETRIED:SMOKE` | Proves non-immediate delivery, not that the full configured delay elapsed |
| `smoke.activity_timeout_retry`, `smoke.activity_heartbeat_timeout_retry` | Deliberately slow first callbacks cause server-owned retries with exact attempt-two terminal markers | Does not prove callback interruption or a worker shutdown deadline |
| `smoke.activity_non_retryable_failure`, `smoke.async_activity_completion` | Error-type policy prevents an unwanted retry; retained asynchronous handle completes later with `SMOKE:ASYNC:COMPLETED:SMOKE` | Heartbeat response flags and full deferred-completion recovery remain unqualified |
| `smoke.parent_awaits_child`, `smoke.parent_awaits_failed_child`, `smoke.parent_cancels_child` | Child success, propagated typed non-retryable failure, explicit child-handle cancellation using `Wait_cancellation_requested` | This cancellation scenario does not invoke `Scope.cancel` or prove all cancellation policies |
| `smoke.parent_retries_child`, `smoke.parent_observes_child_start_failure` | Server-owned second child attempt and duplicate-ID child-start failure | Retry and cancellation races require separate scenarios |
| `smoke.non_retryable_failure`, `smoke.long_running_cancellation` | Typed workflow failure; separate readiness-marked exact-run cancellation and termination targets, with exact terminal metadata | A cancellation acknowledgement alone is not terminal completion; termination reason/race coverage remains incomplete |
| `smoke.signal_condition` | Separate direct-client signal, external signal, and update targets; output-only and typed-input queries; missing-handler and locally invalid-input rejection | Readiness does not prove that a condition was already parked; FIFO wake-up and teardown rely on focused runtime tests |
| `smoke.external_signal_parent`, `smoke.external_signal_completed_parent` | Delivery to the retained exact target; rejection when signaling a confirmed completed run | Missing targets and external-operation replay/retry combinations remain unqualified |
| `smoke.external_cancellation_parent`, `smoke.external_cancellation_wrong_run_parent` | Exact-run external cancellation and rejection of mismatched run identity before acknowledgement | Wrong-run rejection is a retryable workflow error (`non_retryable=false`); this does not cover cancellation of every missing/completed target |
| Typed update on `smoke.signal_condition` | Unknown handler is rejected; registered update is admitted, polled to typed completion, and changes workflow-local state | Suspended-update recovery across eviction and worker replacement, and validator rejection, are covered by the `interaction_recovery` (#530) and `update_outcomes` (#546) regressions, and the typed caller result when the run closes before an update finishes by `update_run_close` (#520); retry and deadline behavior need additional live cases |

## Cancellation and non-live evidence

[Scope hooks][scope-source], [activity handles][activity-source], and
[child handles][child-source] implement deterministic cancellation commands.
[Scope tests][scope-tests] cover owner checks, waiter wake-up, idempotence,
registration ordering and cleanup errors; [activation tests][activation-tests]
cover activity/child command policies and post-completion no-op cancellation.
The live suite does not directly qualify the `~scope` path. See
[workflow scopes](workflow-scopes.md) for the public contract.

Cancelling observation, buffering a Core cancellation command, receiving an
activity cancellation task, stopping an OCaml callback, and shutting down a
worker are distinct events. A Core cancellation task cannot preempt an
already running OCaml callback; it reaches the callback cooperatively at its
next heartbeat, and the `activity_cancellation` regression (#494) is the live
evidence for that path and for the worker-shutdown flag. Worker shutdown
drains owned work through its lifecycle path, and the `bounded_shutdown`
regression (#495) is the live evidence that shutdown returns within its bound
while a callback ignores it. That bound applies to `Worker.shutdown`, not to
the abandoned callback, which keeps running; the stop-marker checks and
focused lifecycle tests do not prove Kubernetes termination behavior.
The `non_yielding_workflow` regression (#493) is the matching evidence for
workflow code: the watchdog fails the stuck task within the deadline plus one
tick, shutdown reports the abandoned activation within its bound, and
Temporal's retry completes the same run on a corrected worker.

Reset, bounded visibility listing, search-attribute upserts and priority
metadata have implemented/focused-tested surfaces but no dedicated live
qualification. Worker deployment routing has the per-workflow
`worker_versioning` regression (#1159), and start memo/search attributes have
the start-metadata controller described above. The private replay feeder
is not a public history replay tool. The seeded
[history corpus](history-corpus.md) is replayed offline in every `dune runtest`;
it is not live acceptance. Authentication configuration, running that corpus
across versions (#524), release delivery and operational/load qualification remain
tracked work; consult [feature coverage](feature-coverage.md) and the
[roadmap](../implementation-roadmap.md).

## Keeping evidence current

The [generated source inventory](live-acceptance-inventory.md) lists Build's
live targets and the baseline driver's referenced workflow definitions.
`make check-live-acceptance-inventory` compares it with source without Docker;
`make update-live-acceptance-inventory` regenerates it after a scenario change.
The check is part of `make test-quality-contract`, hence the existing broad
Linux/native test gates. It checks membership rather than arbitrary start
counts, line numbers, scheduling order, or result values. The
`make test-live-acceptance-inventory-contract` regression checks equivalent LF
and Windows CRLF checkouts, canonical generation and rejected membership drift.

When changing a scenario, regenerate that inventory, review this matrix against
the definition and terminal assertions, and run the affected source contract.
After the live job succeeds, update the tested commit, job link, pins, and exact
outcomes here. A generated list cannot establish that an assertion executed;
keep the successful CI link and source snapshot together. Failed or incomplete
runs must not replace the successful evidence with a broader support claim.

`make check-doc-pins` reads every Markdown file under `docs/` and `README.md`.
A current-pin statement marks its quoted value with an invisible HTML comment
naming `pin:core` or `pin:rust`, placed directly after the value's closing
backtick (see `scripts/check-doc-pins.sh` for the exact form). Each
marked Core value must be a prefix of at least eight hex digits of the `rev`
in `rust/Cargo.toml`, and each Rust value must equal the channel in
`rust/rust-toolchain.toml`. This record, the support policy, the Core bridge
reference and the dependency inventory must each keep their marker. Historical
mentions, such as upgrade notes and capture records, stay unmarked.
`make test-doc-pins-contract` checks the checker against a fixture tree.

```sh
make check-live-acceptance-inventory
make check-doc-pins
make test-quality-contract
make test-unit
make test-runtime
make test-bridge
make verify
# Run the applicable live controller from the table above separately.
```

[baseline]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/commit/7963a9a68e3c31c876a4c883b290064d40ff9ddc
[build]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/42926120004
[live-baseline]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/42926120004/job/130308705520
[live-recovery]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/42926120004/job/130308705505
[live-replay]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/42926120004/job/130308705515
[live-result]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/42926120004/job/130313438378
[core-upgrade-build]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/38186805228
[core-upgrade-commit]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/commit/acbda8dad9a356cb2cc3ba47f9c4adeb146a5bee
[previous-build]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/35469570419
[previous-baseline]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/commit/beae10d0a58e58fb8e076734cd38113d7a2b4466
[toolchain]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/rust/rust-toolchain.toml
[metadata-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/run-start-metadata-live.sh
[task-failure-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/run-task-failure-live.py
[examples-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/run-examples-live.sh
[regressions-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/run-live-regressions.sh
[policy]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/489
[compose]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/compose.yaml
[cargo]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/rust/Cargo.toml
[workflow]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/.github/workflows/build-pr.yml
[makefile]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/Makefile
[driver]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/driver/smoke_driver.ml
[definitions]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/common/smoke_definitions.ml
[lifecycle]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/test_core_lifecycle.ml
[restart-driver]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/driver/restart_driver.ml
[restart-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/test-restart-replay-contract.sh
[crash-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/smoke/test_temporal_worker_crash_recovery_contract.sh
[eviction-driver]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/driver/cache_eviction_driver.ml
[eviction-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/smoke/test_temporal_worker_cache_eviction_contract.sh
[patch-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/run-patch-replay-live.sh
[patch-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/test-patch-replay-contract.sh
[parent-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/run-parent-child-restart-replay-live.sh
[parent-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/test-parent-child-restart-replay-contract.sh
[failure-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/run-child-failure-replay-live.sh
[failure-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/integration/temporal/scripts/test-child-failure-replay-contract.sh
[scope-source]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/lib/public/scope.ml
[activity-source]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/lib/public/activity.ml
[child-source]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/lib/public/child_workflow.ml
[scope-tests]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/runtime/test_scope.ml
[activation-tests]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/7963a9a68e3c31c876a4c883b290064d40ff9ddc/test/runtime/test_activation.ml
[issue-519]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/519

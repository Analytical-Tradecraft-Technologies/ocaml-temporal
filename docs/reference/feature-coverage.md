# Feature coverage and implementation status

This reference describes the source audited at
[`beae10d0a58e`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/commit/beae10d0a58e58fb8e076734cd38113d7a2b4466)
on 2026-09-20. The SDK is experimental; the current package version is the `0.1.0~rc.1`
release candidate, and no final `0.1.0` has been released. A feature's presence,
focused tests, successful live acceptance, and a release support commitment are
separate evidence levels. [#489](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/489)
tracks the support decision; this audit does not decide that policy.

## Evidence levels

- **Implemented**: the source provides the public API or private mechanism.
- **Focused-tested**: deterministic OCaml/Rust/C tests or source contracts
  exercise the mechanism without a server. Mock client tests do not run workflows;
  `Temporal.Testing` runs them in-process against a server simulator.
- **Live-tested**: a named assertion passed against real Temporal Server and
  PostgreSQL. This applies only to the tested source, pins, topology and scenarios.
- **Supported**: an explicit release policy commits to a scope and compatibility
  matrix. The experimental package has no v1 production support promise yet.

The [live evidence reference](live-acceptance-coverage.md) contains the
commit-pinned test links and verified [September 19 CI run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/35469570419).
Its server lane used Linux OCaml 5.5, Temporal 1.32.0 and PostgreSQL 18.6. The
other Linux/compiler and native build lanes are not additional live-server tests.
The [generated inventory](live-acceptance-inventory.md) lists current fixture
membership, including the start-metadata and workflow-task-failure controllers
added after the audited baseline; historical milestone evidence remains in
[progress](../progress.md).

## Runtime and public APIs

| Capability | Implemented and focused-tested boundary | Live evidence and remaining gap |
| --- | --- | --- |
| Definitions, codecs and typed errors | Ordinary OCaml workflow/activity functions; codecs remain paired with definitions; expected failures use `result`. Unit definition/codec/error and installed-consumer tests cover construction and ownership. | Live typed payload/result paths exist. The [official-SDK interoperability regression](../../test/integration/sdk_interop/README.md) (#517) runs an official Temporal Python SDK 1.33.0 client and activity worker against an OCaml workflow: `json/plain` start input, activity input and result, query, signal and update payloads, update rejections for a validator, a `binary/plain` input and a wrongly shaped object, fail-closed undecodable workflow input, and application failure types and details in both directions. Other official SDKs, child workflows across SDKs, custom codec interoperability and every malformed payload are not live-qualified. |
| Scheduler, futures and conditions | Direct-style OCaml 5 effect suspension; workflow-owned `await`, `map`, `both`, `all`, `race`, `first`; deterministic FIFO conditions and cleanup. | Live timer/activity/child waits and signal-conditioned completion. Detailed ordering, parked-condition wake-up and teardown are focused-test evidence. Losing futures are not implicitly cancelled; wrap the race in `Scope.with_scope` and start scoped operations to cancel the losers. |
| Time, randomness and timers | `Workflow.now` uses activation time; `Workflow.random_int` uses execution-local deterministic state seeded by Core; timer commands and zero-duration behavior are tested. | Live durable timer and selected replay paths; no general wall-clock/random I/O allowance or complete timer-edge qualification. |
| Cancellation scopes | `Scope` owns a cancellation signal, checks scheduler ownership and runs registered hooks once. Remote activity and child starts and `Workflow.start_sleep`/`sleep` with `~scope` attach their cancellation commands (`Cancel_timer` for timers). | Scope hook behavior, including timer cancel-before/after-fire, is focused-tested; the [timer cancellation regression](../../test/integration/timer_cancellation/README.md) checks scoped-sleep and bounded-condition cancellation (`TIMER_CANCELED`) and a fired bound live, across worker replacement. Unscoped operations remain observation-only. See [scope contract](workflow-scopes.md). |
| Remote activities | Typed dispatch, timeout/retry policies, context heartbeat details, explicit handle cancellation, priority/fairness metadata, retained asynchronous completion, strict token lifecycle and completion retry. | Live ordinary/heartbeat/timeout retries, non-retryable policy and delayed completion. Callback concurrency, cooperative cancellation observation, heartbeat response flags and broader recovery remain incomplete. |
| Local activities | `Activity.start_local`/`execute_local`, Core local task lane, marker result resolution, and Core-directed `DoBackoff` timer/rescheduling have focused coverage. | `smoke.local_activity` passed live and returns `LOCAL`. Local retry/backoff, cancellation and replay/restart combinations need dedicated live cases. Local starts accept `~scope` and `start_local_handle` exposes `cancel` (#1156); the [local activity cancellation regression](../../test/integration/local_activity_cancellation/README.md) drives both public paths live under every policy and replays them on a fresh worker. |
| Child workflows | Two-stage start acknowledgement/terminal resolution, exact sequence ownership, rejection paths, retry-policy conversion, explicit handle and scope cancellation. Execution/run/task timeouts and the ID reuse policy (#1135) are validated like `Client.start` and pinned by bilateral OCaml/Rust protocol tests. | Live success, propagated failure, cancellation, retry and duplicate-ID failure; separate live controllers verify successful replay and child failure after replay with parent recovery. The [child start options regression](../../test/integration/child_start_options/README.md) checks child timeouts read back through `Workflow.info` across worker replacement, a run timeout ending a child as `Timed_out`, and both ID reuse outcomes; child memo and search attributes are not supported. Broader policy/race/recovery combinations remain open. |
| Signals, queries and updates | Typed definitions, exact-run client operations, deterministic handlers, both query forms, validators and immediate/suspended update continuations have focused tests. | Live direct/external signal delivery, output-only and typed-input queries, typed update admission/completion, unknown-handler and validator rejection, and query and suspended-update recovery after sticky-cache eviction and worker replacement (#530). The [update run-close regression](../../test/integration/update_run_close/README.md) (#520) shows every caller of an accepted, unfinished update receiving a typed non-retryable `Update` failure when the run completes, is cancelled, or is terminated, and the caller of an admitted update receiving `Not_found` when the run is terminated, each within 10 s. The deterministic offline race matrix pins handler orderings; the two #962 orderings that drop handler work are not produced live. Deadlines and update retry need more live tests. |
| Client operations | Start/wait/follow/cancel/terminate/reset, signals, queries, update handles, handles by workflow ID with or without a run ID (`get_handle`, current-run `wait` following the run chain), bounded visibility listing and shutdown are implemented. `start` takes workflow execution/run/task timeouts, a workflow retry policy, and ID reuse and conflict policies; every bounded RPC takes a per-call `?rpc_timeout` (#499). Calls on one client run concurrently: a pending `wait`, query, or update poll does not delay other calls (#807). `Client.create` sets the per-client admission limits on distinct waited runs, in-flight starts, and calls in total (defaults 64, 64, and 4,096); a call beyond one is a retryable error recognized by `Client.is_at_capacity` (#796). | Start/wait/follow/cancel/terminate and named interactions are live-tested, including signal/update/query by workflow ID and a current-run `wait` across continue-as-new. The [policy regression](../../test/integration/client_policies/README.md) shows execution and run timeouts ending runs as `Timed_out`, the recorded task timeout, reuse-policy refusals, a workflow retry of a failed and of a timed-out run each reported with its successor rather than as continue-as-new (#971), an explicit continue-as-new, an expired query deadline leaving the workflow running, and an uncertain start reconciled by request ID. The [concurrent client regression](../../test/integration/concurrent_client/README.md) keeps 24 waits and a stuck query pending while signals and starts on the same client each finish within 1.5 s, and shows shutdown ending an in-flight wait (#807); it also refuses a third wait on a client created with `~max_pending_waits:2` (#796). Reset/visibility conformance and wider reconciliation races remain unqualified. A control RPC acknowledgement is not worker execution or terminal completion. |
| Continue-as-new and patching | Explicit successor handles; `patched` decisions and `deprecate_patch` markers with mixed-mode protection. | Live successor following and marker-free to active, active to deprecated, deprecated to removed patch histories. The [history corpus](history-corpus.md) replays every lifecycle stage against every other for multiple patches, a patch in a loop, patches in signal and update handlers and a mid-run upgrade, with negative controls for premature deprecation, removal without deprecation and rollback ([#531](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/531)). Concurrent mixed-version workers, broader history compatibility and migration automation remain open. |
| External operations | Workflow signal/cancellation commands preserve exact workflow/run identity and typed acknowledgement/failure. | Live signal delivery, signal rejection for a confirmed completed target, external cancellation and wrong-run cancellation rejection. Missing targets, completed-target cancellation and retry/replay combinations remain unqualified. |
| In-process test environment | `Temporal.Testing` drives the real execution runtime with a deterministic simulator: zero-time activities with retry policies and heartbeat-detail hand-off, virtual-time timers, child workflows with parent-close and retry policies, Core-ordered activation jobs, native-shaped activity and child errors, external signals/cancellation, queries, validated updates, cancellation and continue-as-new. | Not a live feature. Activity timeouts, task retries, replay, child cancellation types, asynchronous activities, Nexus and visibility are not simulated. |
| Worker versioning and metadata | Legacy build-ID and deployment-based Core options, task-local deployment identity, start memo/search attributes, search-attribute upserts and activity priority/fairness fields are implemented and focused-tested. Workflow code reads the start memo/search attributes (`Workflow.start_metadata`) and the recorded timeouts, retry policy, and retry or continue-as-new predecessor (`Workflow.Info`, #512). | No dedicated live routing, deployment rollout or fairness qualification. `make test-temporal-start-metadata-live`, added after the audited baseline, checks start memo/search attributes, continue-as-new and an official-CLI start across worker replacement, and memo, search attributes and exact expiration across cache-full eviction and replay on a one-slot-cache worker; see [start metadata](workflow-start-metadata.md). The [start info regression](../../test/integration/workflow_start_info/README.md) reads timeouts, retry policy, memo and continuation provenance back on a root run, a retry run and a continue-as-new successor across worker replacement. Search-attribute upserts and priority metadata are not live-qualified. |

Focused tests are under [`test/unit/`](../../test/unit/),
[`test/runtime/`](../../test/runtime/), [`test/bridge/`](../../test/bridge/),
[`test/sdk_supervisor/`](../../test/sdk_supervisor/) and
[`rust/core-bridge/tests/`](../../rust/core-bridge/tests/). These locations point
to the current checkout; the evidence reference pins the audited source and
specific regression/controller files to the successful run.

## Native ownership and recovery

| Mechanism | Evidence boundary |
| --- | --- |
| Private JSON and C/Rust boundary | Bilateral closed-record validation, copied payloads, opaque handles and exact lease correlation have focused tests. Every pinned Core failure-info kind has a variant: application, canceled, terminated, activity, child-workflow, timeout, server, reset-workflow, Nexus-operation and Nexus-handler. A missing failure info is `absent`. A failure kind therefore never rejects an activation, client wait or update outcome. Server, reset and Nexus failures are covered by Core-conversion and shared-fixture tests only; no live controller produces them. The live controllers exercise ordinary and selected failure records; malformed-input/fault-injection coverage remains focused. |
| One-owner supervisor and completion drainage | Mailbox, owner Domain, bounded readiness waits, protocol rejection, retryable completion retention and shutdown tests protect lifecycle rules. A retry does not rerun user code. The baseline also requires client and worker stop markers. |
| Serialized activity execution | One callback is decoded, invoked and completed before another task is admitted. Core is granted one activity slot and one local-activity slot to match, so the server keeps undelivered tasks schedulable instead of starting their timeouts in the bridge queue; at most one remote task can wait behind one local callback. The pools stay separate because Core pre-reserves an idle local-activity permit, which would starve a shared pool. A cancellation task updates the original token and does not create a second completion lease. It cannot preempt a callback already executing under the adapter lock; instead each heartbeat of a running synchronous callback sweeps the ready tasks, delivers that attempt's cancellation to its context, and defers other tasks in order (#494). |
| Worker resource and shutdown options | `Worker.Options.make` validates the sticky cache bound, workflow-task limit, fixed or autoscaling workflow pollers, sticky-queue timeout, shutdown grace period, heartbeat throttle intervals and per-worker/task-queue activity rates (#498). Invalid, overflowing and contradictory values are typed defects returned before any native allocation, and Rust validates the same rules again. `Worker.options` reports the effective values. Focused tests pin the exact bridge document and the Core `WorkerConfig` it produces. The `worker_resource_options` live regression runs a one-workflow-cache, two-slot, one-activity-per-second worker through a saturating burst and a recovery execution. Activity slot counts are not configurable: they stay pinned to the serial executor (#777). Shutdown is bounded by the grace period plus `shutdown_teardown_timeout` (#495): work still running when the grace period ends is abandoned and reported by `Worker.shutdown_with_report`, and the `bounded_shutdown` live regression shows a never-heartbeating activity retried and completed on a second worker. |
| Cooperative activity cancellation and shutdown signal | `Activity.Context.cancellation` exposes a delivered cancellation with Core's typed reasons, heartbeats return a `` `Cancelled `` error after it, and a `` `Cancelled `` result completes the attempt as cancelled only when a cancellation was delivered. A start cancelled before admission completes as cancelled without running; shutdown drain fails an undispatched deferred start retryably. `Context.is_worker_shutting_down` reads the worker's stop flags. Focused adapter tests cover delivery, precedence, before/after cases, drain and deferred poll errors; the `activity_cancellation` live regression covers a workflow-requested cancellation completing as cancelled, a heartbeat-timeout cancellation, and the shutdown signal. `Activity.Async_handle.heartbeat` reports a cancellation request as a non-retryable `` `Cancelled `` error and a pause or reset as a retryable one, while keeping the handle live (#1103); Rust throttle and OCaml adapter tests cover it. The [cooperative cancellation example](../../examples/README.md#cancel-activities-cooperatively), run live by `make test-temporal-examples-live`, cancels a synchronous and an asynchronous activity that each release an idempotent reservation before acknowledging, and shuts a worker down during a synchronous activity so the attempt releases its reservation and a replacement worker commits the retry. Reservation keys include the workflow run ID, and a new run reusing a closed run's workflow ID is shown doing its own work. Asynchronous handles are not told about worker shutdown (shutdown closes an unfinished handle as lost after the grace period, an unfinished handle expires per #1043, and Temporal retries per the attempt's timeouts), and a callback that never heartbeats never observes a cancellation. Re-qualification after the isolated activity dispatch of #492 remains. |
| Worker recovery | Separate successful live controllers cover graceful restart, forced crash, one-slot sticky-cache eviction, exact parent/child restart and child failure after replay. This is a bounded corpus, not arbitrary crash/failure history qualification. |
| Replay feeder and offline replay | Strict history JSON/protobuf validation and bounded Core replay-worker plumbing exist. `Temporal.Replay` exposes them as an offline API with typed nondeterminism, task-failure, invalid/unsupported-history and replay-error results; its public tests replay retained live histories, detect removed or replaced commands, reject malformed input and repeat 140 isolated replays. Nondeterminism results carry a structured mismatch (workflow ID and type, recorded event, matched command, Core's reason), asserted against the corpus negative controls. Live worker replacement separately proves server-delivered replay. The versioned [history corpus](history-corpus.md) replays 30 recorded histories as 78 cases through the public API in every `dune runtest`, including 25 nondeterminism negative controls and 5 malformed-history controls that must be refused as invalid histories; its [feature coverage table](history-corpus.md#feature-coverage) maps each replay-relevant feature to a case or to an explicit not-applicable or not-yet-captured reason (#503). The copyable [replay command example](../../examples/README.md#replay-recorded-histories-before-deploying) wraps the API in an offline CLI with one line per history and distinct exit statuses; `test/replay_cli` checks them against a recorded example history and deliberately changed workflow code. There is no protobuf-JSON history input. |
| Observability | Structured `logs` sources/tags and privacy-safe diagnostics have focused tests. Workflow lease records carry workflow ID, run ID and a replay flag (#500). A shared `Runtime` can export Temporal Core's worker and client metrics on a Prometheus endpoint with the official SDKs' names; validation, exporter lifecycle and a live scrape with label-cardinality checks are tested ([worker metrics](observability.md#worker-metrics)). Activity-scoped log correlation, OCaml-only executor/admission metrics, OTLP export, tracing, workload benchmarks and sustained-load evidence remain incomplete. |

See [runtime invariants](runtime-invariants.md), [Core ownership](core-bridge.md),
[native activities](native-activity-execution.md), and the
[live scenario matrix](live-acceptance-coverage.md) for the boundaries of each claim.

## Deferred and unqualified work

- Public TLS trust/mTLS/API-key configuration and secure connectivity acceptance.
  HTTP(S) URL handling alone is not authentication or Temporal Cloud support.
- Protobuf-JSON history input (an SDK-only route is
  `Client.fetch_history`, #1137; CLI JSON exports still need conversion),
  replaying the seeded
  [history corpus](history-corpus.md) across SDK/Core/server upgrades (#524),
  local-activity recovery, interaction recovery and deployment routing.
- Concurrency/load/resource qualification and operational guidance. Shutdown
  is bounded (#495), but abandoned callbacks keep running until the process
  exits.
- Cooperative workflow cancellation and durable cleanup (#514). A
  cancellation request closes the run as cancelled in the activation that
  delivers it; workflow code cannot observe it or run cleanup first. The
  proposed [v1 support policy](v1-support-policy.md) excludes it from MVP v1.
- Schedules, Nexus, interceptors, workflow side effects, workflow-level
  priority/fairness and remaining Temporal parity work.
- Client signal-with-start and describe-workflow operations are not
  implemented and are deferred past v1; a handle by workflow ID (#791) covers
  addressing an existing workflow without its run ID.
- A release support/compatibility decision, artifact publication/provenance,
  upgrade rehearsal and complete operational qualification. Existing package,
  installed-consumer, license and release-preflight checks are foundations,
  not evidence that a release was delivered.

The [roadmap](../implementation-roadmap.md) retains the long-term target.
Before release, reconcile the factual evidence here with the approved support
policy and retain a successful run at the exact candidate commit.

## Evidence commands

```sh
make check-live-acceptance-inventory # source/document membership, no server
make test-quality-contract          # includes inventory drift check
make test-unit
make test-runtime
make test-bridge
make verify                         # broad local tests, no live-server claim
make test-temporal-integration      # baseline real-server controller
```

Additional live targets are listed in [live acceptance coverage](live-acceptance-coverage.md).
Updating the generated inventory does not prove live success; inspect the
relevant assertions and CI outcome before changing an evidence status.

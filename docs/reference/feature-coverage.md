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
| Definitions, codecs and typed errors | Ordinary OCaml workflow/activity functions; codecs remain paired with definitions; expected failures use `result`. Unit definition/codec/error and installed-consumer tests cover construction and ownership. | Live typed payload/result paths exist; custom codec interoperability and every malformed payload are not live-qualified. |
| Scheduler, futures and conditions | Direct-style OCaml 5 effect suspension; workflow-owned `await`, `map`, `both`, `all`, `race`, `first`; deterministic FIFO conditions and cleanup. | Live timer/activity/child waits and signal-conditioned completion. Detailed ordering, parked-condition wake-up and teardown are focused-test evidence. Losing futures are not implicitly cancelled. |
| Time, randomness and timers | `Workflow.now` uses activation time; `Workflow.random_int` uses execution-local deterministic state seeded by Core; timer commands and zero-duration behavior are tested. | Live durable timer and selected replay paths; no general wall-clock/random I/O allowance or complete timer-edge qualification. |
| Cancellation scopes | `Scope` owns a cancellation signal, checks scheduler ownership and runs registered hooks once. Remote activity and child starts with `~scope` attach their cancellation commands. | Scope hook behavior is focused-tested, not directly exercised by the live driver. Timers and unscoped operations remain observation-only. See [scope contract](workflow-scopes.md). |
| Remote activities | Typed dispatch, timeout/retry policies, context heartbeat details, explicit handle cancellation, priority/fairness metadata, retained asynchronous completion, strict token lifecycle and completion retry. | Live ordinary/heartbeat/timeout retries, non-retryable policy and delayed completion. Callback concurrency, cooperative cancellation observation, heartbeat response flags and broader recovery remain incomplete. |
| Local activities | `Activity.start_local`/`execute_local`, Core local task lane, marker result resolution, and Core-directed `DoBackoff` timer/rescheduling have focused coverage. | `smoke.local_activity` passed live and returns `LOCAL`. Local retry/backoff, cancellation and replay/restart combinations need dedicated live cases. Local starts do not expose `~scope`. |
| Child workflows | Two-stage start acknowledgement/terminal resolution, exact sequence ownership, rejection paths, retry-policy conversion, explicit handle and scope cancellation. | Live success, propagated failure, cancellation, retry and duplicate-ID failure; separate live controllers verify successful replay and child failure after replay with parent recovery. Broader policy/race/recovery combinations remain open. |
| Signals, queries and updates | Typed definitions, exact-run client operations, deterministic handlers, both query forms, validators and immediate/suspended update continuations have focused tests. | Live direct/external signal delivery, output-only and typed-input queries, typed update admission/completion, unknown-handler and validator rejection, and query and suspended-update recovery after sticky-cache eviction and worker replacement (#530). Deadlines and update retry need more live tests. |
| Client operations | Start/wait/follow/cancel/terminate/reset, signals, queries, update handles, handles by workflow ID with or without a run ID (`get_handle`, current-run `wait` following the run chain), bounded visibility listing and shutdown are implemented. | Start/wait/follow/cancel/terminate and named interactions are live-tested, including signal/update/query by workflow ID and a current-run `wait` across continue-as-new. Reset/visibility conformance, deadlines and reconciliation races remain unqualified. A control RPC acknowledgement is not worker execution or terminal completion. |
| Continue-as-new and patching | Explicit successor handles; `patched` decisions and `deprecate_patch` markers with mixed-mode protection. | Live successor following and marker-free to active, active to deprecated, deprecated to removed patch histories. Broader history compatibility and migration automation remain open. |
| External operations | Workflow signal/cancellation commands preserve exact workflow/run identity and typed acknowledgement/failure. | Live signal delivery, signal rejection for a confirmed completed target, external cancellation and wrong-run cancellation rejection. Missing targets, completed-target cancellation and retry/replay combinations remain unqualified. |
| In-process test environment | `Temporal.Testing` drives the real execution runtime with a deterministic simulator: zero-time activities with retry policies and heartbeat-detail hand-off, virtual-time timers, child workflows with parent-close policies, external signals/cancellation, queries, validated updates, cancellation and continue-as-new. | Not a live feature. Activity timeouts, task retries, child cancellation types, asynchronous activities, Nexus and visibility are not simulated. |
| Worker versioning and metadata | Legacy build-ID and deployment-based Core options, task-local deployment identity, start memo/search attributes, search-attribute upserts and activity priority/fairness fields are implemented and focused-tested. | No dedicated live routing, deployment rollout or fairness qualification. `make test-temporal-start-metadata-live`, added after the audited baseline, checks start memo/search attributes, continue-as-new and an official-CLI start across worker replacement; see [start metadata](workflow-start-metadata.md). Search-attribute upserts and priority metadata are not live-qualified. |

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
| Serialized activity execution | One callback is decoded, invoked and completed before another task is admitted. Core is granted one activity slot and one local-activity slot to match, so the server keeps undelivered tasks schedulable instead of starting their timeouts in the bridge queue; at most one remote task can wait behind one local callback. The pools stay separate because Core pre-reserves an idle local-activity permit, which would starve a shared pool. A cancellation task updates the original token and does not create a second completion lease. It cannot preempt a callback already executing under the adapter lock. |
| Worker recovery | Separate successful live controllers cover graceful restart, forced crash, one-slot sticky-cache eviction, exact parent/child restart and child failure after replay. This is a bounded corpus, not arbitrary crash/failure history qualification. |
| Replay feeder and offline replay | Strict history JSON/protobuf validation and bounded Core replay-worker plumbing exist. `Temporal.Replay` exposes them as an offline API with typed nondeterminism, task-failure, invalid/unsupported-history and replay-error results; its public tests replay retained live histories, detect removed or replaced commands, reject malformed input and repeat 140 isolated replays. Nondeterminism results carry a structured mismatch (workflow ID and type, recorded event, matched command, Core's reason), asserted against the corpus negative controls. Live worker replacement separately proves server-delivered replay. The versioned [history corpus](history-corpus.md) replays 17 recorded histories through the private path in every `dune runtest`, including two nondeterminism negative controls. The copyable [replay command example](../../examples/README.md#replay-recorded-histories-before-deploying) wraps the API in an offline CLI with one line per history and distinct exit statuses; `test/replay_cli` checks them against a recorded example history and deliberately changed workflow code. There is no protobuf-JSON history input. |
| Observability | Structured `logs` sources/tags and privacy-safe diagnostics have focused tests. Operational metrics/tracing, workload benchmarks and sustained-load evidence remain incomplete. |

See [runtime invariants](runtime-invariants.md), [Core ownership](core-bridge.md),
[native activities](native-activity-execution.md), and the
[live scenario matrix](live-acceptance-coverage.md) for the boundaries of each claim.

## Deferred and unqualified work

- Public TLS trust/mTLS/API-key configuration and secure connectivity acceptance.
  HTTP(S) URL handling alone is not authentication or Temporal Cloud support.
- Protobuf-JSON history input, replaying the seeded
  [history corpus](history-corpus.md) across SDK/Core/server upgrades (#524),
  local-activity recovery, interaction recovery and deployment routing.
- Application-visible cooperative activity cancellation, bounded shutdown under
  slow callbacks, concurrency/load/resource qualification and operational guidance.
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

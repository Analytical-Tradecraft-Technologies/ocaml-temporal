# MVP v1 scope for the experimental prerelease

**Decision status:** This is the proposed scope for the MVP v1 milestone. It
takes effect only after a repository maintainer approves the matrix in a review
on [PR #557] and that PR merges. [#489] remains open until then. MVP v1 is a
product milestone, **not** a SemVer `1.0.0` compatibility or production-support
promise. The first candidate uses Git tag `v0.1.0-rc.1` and OPAM version
`0.1.0~rc.1`. Approval of this document does not qualify or publish that tag.

The SDK and the candidate remain experimental. [Feature coverage](feature-coverage.md)
records implemented and focused-tested paths; [live acceptance coverage](live-acceptance-coverage.md)
records bounded server evidence. The matrix below names the core paths to
qualify on the exact candidate commit. Public APIs outside that set may still
be useful for evaluation, but their presence in the installed package is not
an MVP behavior commitment.

## Environment and evidence boundary

| Area | MVP candidate boundary | Evidence limit |
| --- | --- | --- |
| Deployment | Self-hosted Temporal in the repository's [Compose fixture](../../test/integration/temporal/compose.yaml), with PostgreSQL and a configured namespace. The OCaml application owns its worker executable. | This is a controlled evaluation environment. The fixture uses plaintext; no production endpoint or deployment is qualified. |
| Server and Core | Temporal Server **1.32.0**, PostgreSQL **18.6**, and pinned Temporal Core **`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`**. The [Compose images](../../test/integration/temporal/compose.yaml) and [Cargo manifest](../../rust/Cargo.toml) identify the exact inputs. | No other Server/Core pair, database version, or topology follows from this evidence. A changed pin requires a fresh live run. |
| Live execution | Linux amd64, Ubuntu 24.04 CI runner, OCaml **5.5.1**, independent OCaml worker/client processes against that Server/PostgreSQL pair. | The [live job](../../.github/workflows/build-pr.yml) exercises selected scenarios. Its green result does not imply arbitrary workflow histories, sustained load, or production operation. |
| Build and installed package | The [release matrix](release-preflight.md#build-coverage-and-artifact-reuse) builds OCaml 5.2.1, 5.3.0, 5.4.1, and 5.5.1 on Linux amd64/arm64, macOS ARM64, and Windows x64 GNU/MinGW. The installed-consumer witness and private-module negative checks run with those builds. | These are build, unit, bridge, and package checks. Linux arm64, macOS, and Windows have **no** live-server qualification in this matrix. Published binary compatibility is limited to each asset's recorded compiler, dependency, and platform identity. |
| Authentication | Plaintext connection to the controlled self-hosted fixture only. | SDK-managed trust configuration, mTLS, Temporal Cloud, API keys, TLS proxy deployments, and authenticated endpoint acceptance are outside this candidate. [#496] tracks secure endpoint work. |

The `ocaml >= 5.2` OPAM constraint is a solver bound, not a claim about every
future compiler. Record exact toolchain versions, image digests, source commit,
Core revision, and artifact checksums for each release candidate. A green build
on one platform never substitutes for the live result on another.

## Feature matrix

**Core candidate** means the MVP release must pass the named behavior on the
exact candidate, subject to the limits below. **Experimental** means exported
for evaluation without a release behavior or history guarantee. **Deferred**
means no MVP capability claim, even if private machinery or partial APIs exist.

| Capability | Decision and application boundary | Evidence or qualification |
| --- | --- | --- |
| Workflow authoring | **Core candidate:** typed direct-style workflows, deterministic futures/conditions/time, durable timers, child workflows, and continue-as-new. Workflow code must yield and avoid nondeterministic I/O. | [Runtime tests](../../test/runtime/) and named [live scenarios](live-acceptance-coverage.md). Non-yielding code still blocks the workflow lane: the activation watchdog ([#493]) fails the stuck workflow task after the configurable deadline (default 2 s) and reports `Worker.health` as stuck, but cannot interrupt the code, so a supervisor must restart the process ([watchdog reference](native-worker-execution.md#non-yielding-workflow-watchdog), [watchdog tests](../../test/runtime/test_native_worker_watchdog.ml)). A live subprocess qualification of this path is still outstanding. |
| Remote activities | **Core candidate for bounded callbacks:** typed remote callbacks, retry/timeout policy, heartbeats, and result/failure handling. Application code must put finite deadlines on callback I/O and return within an application-defined budget. Activity delivery may repeat; callers must make external side effects idempotent. | [Native activity contract](native-activity-execution.md) and [live scenarios](live-acceptance-coverage.md). Callbacks currently execute serially under the polling lock: a blocked callback can indefinitely stop unrelated workflow progress ([#492]). Temporal activity timeouts do not interrupt the OCaml callback. The public callback context has no cooperative activity-cancellation or worker-shutdown observation; both are outside this core candidate ([#494]). |
| Client control | **Core candidate:** start, exact-run wait/follow, typed signals, and exact-run cancellation requests with typed outcomes. A request acknowledgement does not prove handler execution or terminal completion; a transport timeout can leave an outcome uncertain. | [Client interface](../../lib/public/client.mli), [client tests](../../test/unit/test_client_worker.ml), and [live driver](../../test/integration/temporal/driver/smoke_driver.ml). Workflow-level cooperative cancellation/cleanup is excluded ([#514]). |
| Worker and recovery | **Core candidate for supervised processes:** application-owned worker, same-version restart/replay, and selected crash recovery for the tested workload. The application operator must enforce a process-level deadline and force termination/replacement if callback execution or graceful shutdown stalls; outstanding work then depends on Temporal redelivery. | [Worker interface](../../lib/public/worker.mli) and the [live controller matrix](live-acceptance-coverage.md). The SDK has no end-to-end shutdown bound when a callback holds the run mutex ([#495]). [#501] closed after [PR #555] fixed a polling-capacity mismatch and repeated live checks passed. The sole cause of the earlier 900-second timeout remains unproven; cache eviction must pass again on the exact candidate. |
| Payloads and errors | **Core candidate:** built-in typed codecs, typed results/errors, and preservation of supported failure details. Application codecs own their schema and migration policy. | [Codec tests](../../test/unit/test_codec.ml), [error tests](../../test/unit/test_error.ml), installed-consumer tests, and selected live payload paths. Cross-SDK codec interoperability is not generally qualified. |
| Queries and updates | **Experimental:** output/typed-input queries and immediate/suspended updates. | Existing [interaction tests](../../test/unit/test_interactions.ml) and named live cases do not prove read-only enforcement, validator safety, replay/eviction recovery, or all deadline cases ([#513], [#505]). |
| Local and asynchronous activities | **Experimental:** local activity start/execution and retained asynchronous completion with task-token completion/heartbeat for remote activities. Deferred completion is unavailable for local activities: a local callback returning `Will_complete_async` fails that attempt. | Focused and limited live cases exist. [#691] closed after [PR #749] made the unsupported local/deferred combination an ordinary activity failure. [#692] closed after [PR #758]: an uncertain remote heartbeat RPC now retains the live handle and lease for an identical retry, so worker drain reports the outstanding lease. Before that fix, a transient heartbeat error could close the handle while the server activity remained live, and drain could falsely succeed. Live network-fault behavior and other retry/replay combinations remain unqualified. Do not depend on a process-local completion handle surviving replacement. |
| In-process workflow tests | **Experimental:** `Temporal.Testing` runs registered workflows and activities on the real workflow runtime against a deterministic, time-skipping server simulator, with activity/child stubs and signal, query, update, cancellation and continue-as-new drivers. It is a unit-test tool, not a server: activity timeouts, task retries, child cancellation types, asynchronous activities and visibility are not simulated. `mock://` remains a plumbing-only client/worker backend. | [Testing tests](../../test/unit/test_testing.ml) and the [example workflow test](../../examples/testing/example_workflow_test.ml). Passing in-process tests do not replace live qualification. |
| Additional exported client operations | **Experimental:** start memo/search attributes, exact-run reset and termination, and bounded visibility listing. | These appear in the [public client interface](../../lib/public/client.mli), but presence does not establish complete end-to-end semantics. [#512] tracks start-metadata alignment. |
| Unavailable client options | **Deferred:** public workflow execution/run/task timeout options, the workflow ID reuse policy for closed runs, per-call deadlines, cron schedules, and delayed starts. The workflow ID conflict policy for running executions is supported through `Client.start ?id_conflict_policy` (#933). | These are not exposed by `Client.start` or the public client operations. [#499] tracks the remaining policies and deadlines; applications must not infer support from server-side metadata or private protocol fields. |
| Offline history replay | **Experimental:** `Temporal.Replay` replays one binary `History` protobuf per call against registered workflow definitions, offline, with typed `Nondeterminism`, `Workflow_task_failed`, `Invalid_history`, `Unsupported_history`, and `Replay_error` results. It checks the recorded paths and command shapes only, not payload values or unrecorded branches. | [Replay interface](../../lib/public/replay.mli) and [public replay tests](../../test/bridge/test_public_replay.ml) over retained live histories ([#515]). The [replay command example](../../examples/README.md#replay-recorded-histories-before-deploying) is a copyable application-linked CLI with stable exit statuses, tested in [`test/replay_cli`](../../test/replay_cli/test_replay_cli.ml) ([#516]). There is no JSON-history input, persistent corpus, or richer mismatch context yet ([#503], [#529]). |
| Worker upgrades and replay tools | **Deferred:** cross-version workflow-history compatibility, deployment/build-ID routing, and an automated upgrade/rollback path. The application-linked replay command is an experimental example, not a cross-version promise. Patching APIs may be evaluated experimentally. | The [patching reference](workflow-patching.md) and the [replay bridge](replay-bridge.md) do not create a cross-version replay promise ([#497], [#503], [#508]). |
| Wider Temporal surface | **Deferred:** schedules, Nexus, interceptors, broad observability/load commitments, and other upstream SDK parity work. | [Feature coverage](feature-coverage.md) is the inventory; adding support requires a separate decision and qualification. |

Known defects that affect a core row must be fixed and retested before that
row is claimed on a candidate. A documented limit can narrow the workload;
it cannot turn a failing core test into a pass. The release notes must name
all residual limits, especially callbacks with application-owned deadlines,
external process supervision, non-yielding workflows, shutdown behavior,
repeat activity delivery, and cancellation semantics. Without bounded callback
I/O and a supervisor willing to force-terminate a stuck process, the core
worker liveness and shutdown paths are outside this candidate. [#492] and
[#495] must be fixed and qualified before widening that boundary.
Signals admitted before workflow completion need an application-defined drain
pattern; the SDK does not automatically wait for every handler. See the
[interaction reference](interactive-workflows.md).

## Prerelease compatibility

`0.1.0-rc.1` does **not** start a stable 1.x source, wire, behavior, platform,
or history compatibility promise. A later prerelease may change public
signatures or behavior; explain breaking changes and migration steps in its
release notes. Never reuse a published tag for changed contents.

The public installation boundary is the wrapped `Temporal` module listed in
[`lib/public/temporal.ml`](../../lib/public/temporal.ml). The [installed witness](../../test/fixtures/install-consumer/public_api.ml)
and [positive/negative install checks](../../test/bridge/test_install.sh)
must match it. Private OCaml modules, JSON protocol records, C symbols, Rust
types, and Temporal Core are not application APIs.

For this candidate, replay/restart qualification covers the *same* SDK/Core
version and the named histories only. Application workflow changes, SDK/Core
upgrades, and rolling mixed-version workers require retained-history replay
and a separate upgrade rehearsal before use. Built-in payload encodings include
`json/plain`, `binary/plain`, `binary/null`, and the OCaml-specific
`binary/x-ocaml-optional` envelope; this list is not a general cross-SDK
interoperability guarantee. Expected operational failures should remain typed;
diagnostic message text is not a parsing API. Unsupported or experimental
options are not guaranteed to be rejected before sending a request.

## Candidate qualification and responsibility

The repository maintainer owns scope approval on [PR #557]. The release owner
records the exact commit and tag, names the people reviewing API/runtime and
live evidence, and decides whether the experimental candidate may be
published. The application operator separately owns any deployment and its
server, namespace, authentication, backups, and workload limits. Publishing a
prerelease is not evidence that a deployment is fit for production.

For the **exact candidate commit**, retain:

1. A green configured build, unit/runtime/bridge/installed-consumer matrix,
   including `make test-api`, private-module negative checks, quality and
   dependency-license gates. Run `make release-preflight` from a clean tree
   and `make release-tag-check RELEASE_TAG=v0.1.0-rc.1` against matching OPAM
   metadata. [Release preflight](release-preflight.md) defines the inputs.
2. A green Linux amd64/OCaml 5.5.1 [Temporal/PostgreSQL live job](../../.github/workflows/build-pr.yml)
   against the exact Server/Core pins, including the integration driver,
   restart, crash recovery, cache eviction, parent/child replay, child failure
   after replay, and patching controllers. Retain run URL, logs, exact run IDs,
   histories, and residual limitations. Contract-only tests are not live
   evidence. The [#501 closeout] records ten fresh-stack local passes and
   scheduled [October 2] and [October 3] master diagnostics with accepted A/B
   runs and typed cancellation after [PR #555]. These runs support the
   polling-capacity explanation but do not prove it was the sole cause of the
   earlier 900-second timeout. They are prior-revision evidence: the exact
   candidate must pass its own cache-eviction controller, and any new timeout
   requires investigation. [#505] tracks broader conformance rather than
   silently making it a prerequisite for every excluded capability.
3. An explicit review of unresolved core-path defects and release notes that
   require bounded callback I/O and an external supervisor with a forced-exit
   deadline. A process kill is not graceful shutdown; document how Temporal
   retries outstanding tasks. Release approval cannot rely on a test from an
   older commit or on the presence of a fixture that did not finish.
4. A successful publish and retrieval of the immutable tag, source and binary
   assets, manifest, checksums, and Cargo SBOM for the tested commit. The
   [release procedure](release-preflight.md#publish-a-prerelease) documents a
   protected maintainer-created tag path. [#510] remains open until the exact
   candidate's publication and asset retrieval are rehearsed and evidenced.

The maintainer's policy approval closes [#489] only after this PR merges.
Release publication, secure endpoints, cross-version replay, and production
deployment remain separate decisions with their own evidence.

[#489]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/489
[#492]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/492
[#493]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/493
[#494]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/494
[#495]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/495
[#496]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/496
[#497]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/497
[#499]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/499
[#501]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/501
[#503]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/503
[#505]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/505
[#508]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/508
[#510]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/510
[#512]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/512
[#513]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/513
[#514]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/514
[#515]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/515
[#516]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/516
[#529]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/529
[#691]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/691
[#692]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/692
[#501 closeout]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/501#issuecomment-5978294089
[October 2]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/37073688475
[October 3]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/37156563875
[PR #555]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/555
[PR #749]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/749
[PR #758]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/758
[PR #557]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/557

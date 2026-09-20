# Proposed v1 support and compatibility policy

**Status: proposed for maintainer approval in [#489].** This document selects
a finite target for the first stable release; it does not declare today's
experimental `~dev` package production-ready. The existing
[pre-release API policy](api-stability.md) remains in force until maintainers
approve this policy and publish a qualified stable release. Approval must be
recorded on the implementing pull request before #489 is closed. Qualification
issues remain separate gates after that policy decision.

The source baseline for this proposal is
[`beae10d0a58e`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/commit/beae10d0a58e58fb8e076734cd38113d7a2b4466).
[Feature coverage](feature-coverage.md) and [live acceptance coverage](live-acceptance-coverage.md)
describe implementation evidence. The decisions below describe what a future
`1.0.0` must qualify, including capabilities that still require work.

## Deployment and platform decisions

| Decision | Proposed v1 contract | Evidence and remaining gate |
| --- | --- | --- |
| Deployment | Self-hosted Temporal with PostgreSQL, one configured namespace and application-owned OCaml worker executables. Temporal Cloud is deferred. | The [Compose fixture](../../test/integration/temporal/compose.yaml) exercises self-hosted Server/PostgreSQL. [#505] qualifies the selected SDK behavior; [#508] verifies the actual deployment and upgrade procedure. |
| Authentication | SDK-managed mTLS for both client and worker: client certificate/key, trusted server CA, and certificate hostname verification. No verification bypass or implicit plaintext fallback. API keys, OAuth/JWT configuration, TLS-terminating proxies as the authentication boundary, and live credential reload are deferred. | Constructors currently expose URL, namespace and identity, without public credential settings. HTTPS support is not authentication qualification. [#496] must implement and test mTLS success, bad/missing credentials, wrong roots/name, expired certificates, redaction, and rotation by rolling process replacement. |
| Server/Core | Candidate pair: Temporal Server **1.32.0** with Core **`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`**, the exact [fixture](../../test/integration/temporal/compose.yaml) and [Cargo workspace](../../rust/Cargo.toml) pins at this baseline. No older/newer server or Core range is inferred. | Existing plaintext live scenarios are evidence only for their recorded pair. The authenticated pair and all supported conformance cases must pass [#496]/[#505]. A changed pin requires a reviewed matrix update and fresh evidence before release. |
| Production execution | Linux amd64 and arm64, OCaml 5.5, using the qualified native dependencies and libc-based container environment. | Current live CI is Linux amd64/OCaml 5.5. [#505] must add live qualification on arm64; a successful arm64 build does not satisfy it. [#507] records bounded load/resource limits on both production targets. |
| Source/build compatibility | OCaml 5.2–5.5 on Linux amd64/arm64; OCaml 5.5 on macOS ARM64 and Windows x64 through the checked-in native build path. Other compilers, OS/CPU combinations and libc environments are unqualified. | The exhaustive [build workflow](../../.github/workflows/build.yml) covers compilation, unit/runtime, native bridge and installed-consumer checks. Native desktop jobs do not run the live server suite, so desktop production execution is outside v1. |

The package's `ocaml >= 5.2` dependency is a solver constraint, not proof of
support for every future compiler. Preserve the 5.2 compiler baseline; this
proposal does not request a compiler upgrade. For every release, record exact
compiler patch versions, Dune, Rust, protoc, OPAM/Cargo locks and container
digests from the jobs, rather than treating floating image tags as evidence.
The [dependency inventory](../dependencies.md) describes the licensing gate.

The deployment owner remains responsible for namespace authorization,
certificate provisioning, server/database operation and network policy.
mTLS authenticates a connection; it does not by itself define application
authorization. See [Temporal's self-hosted security model](https://docs.temporal.io/self-hosted-guide/security).
The mTLS rollout must be qualified against a representative endpoint before
production use; the unauthenticated development fixture is not that endpoint.
An existing deployment with a different server version needs its own approved
compatibility evidence or an approved server upgrade. This policy neither
changes an existing cluster nor asserts its version, authentication or health.

## Feature decisions

**V1 required** means release-blocking implementation and qualification.
**Experimental** means an existing public capability that applications may
evaluate without a stable behavioral/history promise. **Deferred** means no
v1 requirement or supported production path. An experimental API's presence
in the installed consumer does not promote its behavior to v1 support.

| Capability | Decision and application boundary | Existing evidence / required qualification |
| --- | --- | --- |
| Workflow execution | **V1 required:** typed direct-style workflows, deterministic futures/conditions/time, durable timers, remote activities, child workflows and continue-as-new. User code must yield and avoid nondeterministic I/O. Unexpected workflow/SDK defects must preserve the recoverable execution; intentional typed workflow failures may terminate it. | [Runtime tests](../../test/runtime/), [live driver](../../test/integration/temporal/driver/smoke_driver.ml); [#493], [#511] and [#505] qualify non-yielding code, repair/redeploy and failure/recovery boundaries. |
| Remote activities | **V1 required:** ordinary/context-aware callbacks, explicit retry and timeout policies, heartbeats, cancellation/shutdown observation and bounded execution independent of workflow polling. Delivery can repeat; external side effects need an application idempotency key and durable deduplication. | [Activity execution reference](native-activity-execution.md), [live coverage](live-acceptance-coverage.md); [#492], [#494], [#504] and [#505] qualify isolation, cancellation, redelivery and an external-side-effect deduplication strategy. No exactly-once side-effect promise. |
| Client operations and start metadata | **V1 required:** typed start, exact-run wait/follow, signal, query and cancellation, caller-owned start request IDs, memo/search attributes, explicit workflow execution/run/task timeouts, workflow ID reuse/conflict policy and per-call deadlines. | [Client interface](../../lib/public/client.mli), [client tests](../../test/unit/test_client_worker.ml); [#499] supplies the missing policies/deadlines and [#512]/[#505] qualify client-to-worker metadata across restart/replay. Timeout or transport failure is not proof an operation was rejected; [#504] qualifies uncertain outcomes. |
| Signals | **V1 required:** typed direct and workflow-to-workflow signals, with replay-safe handlers and the completion policy below. Server acknowledgement is not handler completion. | [Interaction reference](interactive-workflows.md), [live coverage](live-acceptance-coverage.md); [#505] qualifies missing/completed targets and terminal/handler races. |
| Queries | **V1 required:** output-only and typed-input read-only queries. Query callbacks may read state but cannot mutate SDK-managed state or emit commands; application code must also avoid mutating its own state. | [Query interface](../../lib/public/query.mli), [interaction tests](../../test/unit/test_interactions.ml); enforced SDK read-only behavior in [#513] and recovery/cache/deadline cases in [#505] are mandatory. A documented convention alone cannot satisfy the SDK gate. |
| Updates and validators | **Experimental:** both immediate and suspended updates, including admission, validation and terminal-handler behavior. Updates are not required by the v1 workload contract. | [Interaction reference](interactive-workflows.md) and [live coverage](live-acceptance-coverage.md) record current slices. Promotion requires [#513] validator enforcement plus [#505] acceptance/completion, replay and recovery qualification; an accepted update is not a completed update. |
| Cancellation | **V1 required, restricted:** exact-run cancellation requests and their terminal outcomes; cancellation of activities, children and timers follows explicit operation/scope policies. **Deferred:** workflow-level cooperative cancellation with durable cleanup or compensation after the cancellation request. | [Scope contract](workflow-scopes.md), [runtime cancellation tests](../../test/runtime/test_scope_server_cancel.ml); [#494]/[#505] qualify supported propagation and races. The explicit exclusion resolves the conditional implementation scope in [#514] only after maintainers approve it. |
| Local activities | **Experimental:** `Activity.start_local` and `execute_local`, including local retries and cancellation. They are excluded from the v1 production workload contract. | [Activity interface](../../lib/public/activity.mli) and [live coverage](live-acceptance-coverage.md) describe current implementation; broader semantics/recovery require [#505] before promotion. |
| Asynchronous activity completion | **Experimental:** retained completion handles and client task-token completion, heartbeat, failure and cancellation. Do not rely on a worker-local handle surviving process replacement. | [Activity interface](../../lib/public/activity.mli), [client interface](../../lib/public/client.mli) and [live coverage](live-acceptance-coverage.md); promotion requires [#504]/[#505] ownership, duplicate/late completion, lost-acknowledgement and replacement qualification. |
| Worker routing and workflow code changes | **V1 required:** unversioned workers (`No_versioning`) with compatible workflow code, `Workflow.patched`/`deprecate_patch` and a rehearsed upgrade procedure. **Experimental:** legacy build-ID and deployment-based routing. No automatic registration/migration or arbitrary code-change compatibility. | [Patching contract](workflow-patching.md), `make test-temporal-workflow-patching`, [worker versioning reference](worker-versioning.md); [#503]/[#508] require retained histories and upgrade/rollback evidence. |
| Application replay tooling | **V1 required:** a supported public runner for application-owned workflow registrations and retained histories, with typed diagnostics and failure exit status. | The [replay bridge](replay-bridge.md) is private and is not that API. [#497] supplies the public runner; [#503] retains the compatibility corpus and [#508] uses it before upgrades. |
| Worker operations | **V1 required:** validated resource/shutdown settings, readiness, bounded shutdown, diagnostic correlation and essential metrics. Resource limits must be reported as measured qualification results. | [Worker interface](../../lib/public/worker.mli), [logging reference](observability.md), existing restart/crash/cache controllers; [#495], [#498], [#500]–[#502], [#506] and [#507] complete lifecycle, cache reliability, instrumentation, stress and load gates. |
| Payloads and errors | **V1 required:** typed codecs, built-in encoding contracts, typed error/result categories and preserved detail payloads as described below. | [Codec tests](../../test/unit/test_codec.ml), [error tests](../../test/unit/test_error.ml), [installed witness](../../test/fixtures/install-consumer/public_api.ml); [#503]/[#505] add retained-history and pinned official-SDK interoperability evidence. |
| Other public or upstream features | **Experimental:** client reset, visibility and explicit termination operations. **Deferred:** schedules, Nexus, arbitrary interceptors and other upstream SDK features absent from this matrix. Public terminal-result types can still report server termination even when initiating it is outside the supported client subset. | [Public API map](public-api-map.md) and [feature coverage](feature-coverage.md) remain the factual inventory. Additional production promises require an explicit policy change and named qualification evidence. |

### Handler completion and cancellation limits

The supported signal pattern records in-flight handler count/state in
execution-local workflow state, prevents new business work after a closing
flag, and waits on a deterministic condition until admitted handlers finish
before returning or continuing as new. A suspended handler must decrement its
counter on every supported outcome. Applications must propagate a handler
failure deliberately rather than allowing a failed handler to strand the
wait. [#505] must deliver an executable example and qualify the drain pattern
across replacement/cache eviction and completion/continue-as-new races. This
is a release requirement, not a claim that an automatic drain helper exists.
[Temporal's handler guidance](https://docs.temporal.io/develop/python/workflows/message-passing#ensure-your-handlers-finish-before-the-workflow-completes)
explains why root completion and handler completion need separate treatment.

Normal completion/continue-as-new with unfinished admitted signal work is
outside the supported authoring contract. Intentional workflow failure,
server termination and the restricted cancellation path may abandon handlers;
they do not promise durable cleanup. [#505] must verify bounded diagnostics and
the durable outcome for those paths. If applications evaluate experimental
updates, that qualification must also record what an already accepted caller
observes; an unexplained indefinite wait is not an acceptable test outcome.
The SDK does not promise to drain every signal or update automatically.

For workloads needing compensation, model a stop request as a business signal,
complete and await idempotent cleanup in ordinary workflow execution, and only
then finish. A Temporal cancellation request, worker shutdown and server
termination are different events. The current workflow cancellation path can
seal execution immediately; it cannot be used as a graceful cleanup hook.
Workloads that require cleanup after an actual cancellation request need
[#514] implemented and qualified before they are included in this contract.

## Compatibility after the first stable release

The following promises start at `1.0.0`, for the approved matrix and supported
configuration. They do not retroactively make development snapshots stable.

| Boundary | Commitment |
| --- | --- |
| Public source API | Preserve the wrapped `Temporal` library's module names, abstract types, labelled arguments, return types and public records/variants through compatible 1.x upgrades. This source promise also protects exposed experimental signatures; their behavioral exclusion does not justify silently breaking compilation. An added mandatory field or variant that breaks exhaustive consumers is a breaking change. Private modules, C symbols, protocol records and Rust types remain excluded. |
| Installed boundary | `lib/public/temporal.ml` is the explicit export list. The [installed witness](../../test/fixtures/install-consumer/public_api.ml), [positive/negative install checks](../../test/bridge/test_install.sh) and [package-boundary reference](package-boundary.md) must agree with every candidate. Keep experimental modules in the witness while they remain exported. Public replay/authentication additions must extend it; never expose the private kernel to satisfy a new API. |
| Wire payloads | Preserve decoding and semantic meaning of built-in payloads produced by supported 1.x versions, including metadata/encoding names and option representation. Current encodings include `json/plain`, `binary/plain`, `binary/null` and the OCaml-specific `binary/x-ocaml-optional` envelope. [#505] must qualify each claimed cross-SDK encoding; the optional envelope is not a general upstream codec promise. Application codecs and schema migrations belong to the application. Adding a codec does not authorize changing old payload interpretation. |
| Errors | Expected operational failures remain typed `result`/terminal outcomes. Preserve documented categories, retryability and detail-payload semantics; message text is diagnostic, not a parsing API. Unexpected workflow/SDK defects fail recoverable task processing under [#511], not the workflow execution by default. Do not hide a changed retry/terminal outcome behind a source-compatible signature. |
| Workflow history | A compatible SDK/Core update must replay the retained corpus of all prior supported 1.x behaviors without changing durable command meaning. This does not make arbitrary edits to application workflow code safe: use replay qualification and the documented patch lifecycle. Application owners retain their own representative histories. Experimental histories are outside this guarantee and must be tested before any upgrade. |
| Core, dependency and server upgrades | Keep Core on an immutable revision and Cargo/OPAM locks reviewed. An upstream compatible-looking version does not establish SDK compatibility. Require license/provenance, ABI/ownership, installed API, replay corpus, authenticated live conformance and an application upgrade rehearsal for every changed Core/server pairing. Server operation and schema migration are deployment-owner responsibilities. |
| Versioning and maintenance | Patch releases fix defects/security issues while preserving the supported contract. Minor releases add compatible features or change documented experimental behavior, with explicit notes and history warnings. Removing/narrowing supported behavior, changing payload/history meaning, a breaking public signature, or dropping a promised platform requires a major release with migration guidance. Proposed maintenance covers the latest stable minor line only; no LTS/backport or response-time SLA is promised. [#509] must approve and publish the real reporting/patch procedure before release. |

When validating an unsupported option combination, fail with a typed error
before starting work or sending a request whenever local validation can decide
it. Do not silently ignore a requested policy, downgrade authentication, or
substitute different execution semantics. A known experimental option may
remain usable and labelled experimental; this proposal does not claim the
current SDK rejects every out-of-scope feature. Unknown/future server inputs
must produce a bounded diagnostic and recoverable task failure where required
by [#511], rather than a success claim or an unintended terminal workflow
failure. [#505] must qualify the promised rejection paths.

An intentionally breaking change needs a migration note, updated witness and
history evidence, and a major-version decision even when OCaml still compiles.
If a proposed bug fix changes durable behavior, use a replay-safe patch or
defer it to that breaking process. Reusing an old package tag for new contents
is prohibited. Rollback to an older worker is supported only for the exact
transition rehearsed under [#508]; forward compatibility of new histories is
not automatic.

## Release qualification and accountable roles

For each candidate, the release owner records named people for the roles
below in the release PR. A person may hold multiple roles; no external
contributor, security mailbox or service-level promise is assigned implicitly.
Maintainer approval of this proposal is the scope gate. Release approval is a
later decision based on evidence for the exact candidate commit and tag.

| Gate | Responsible role | Required retained evidence |
| --- | --- | --- |
| Scope and status | Repository maintainer / release owner | Approval of this matrix in [#489]; exact inclusions/exclusions in release notes and examples; [#491] documentation reconciliation; no unsupported capability described as stable. A scope amendment repeats review and updates linked qualification issues. |
| Public package | API maintainer | `make test-api` (same installed-consumer gate as `make test-install`), the public witness and every private-module negative fixture on the candidate; compiler/OS/architecture build results from the exhaustive workflow. Source review alone is not a passing install result. |
| Build and supply chain | Release owner | `make check OCAML_VERSION=5.2`, `make quality`, the exhaustive Linux/native matrix, and the independent Cargo license audit. Record exact resolved tools, locks and source inputs. `make release-preflight` must pass on the clean release commit; `make release-tag-check RELEASE_TAG=v1.0.0` runs only after matching manifests are prepared. [#510] supplies complete release-artifact provenance/SBOM and an installation rehearsal. |
| Authenticated live behavior | SDK qualification maintainer | [#496]/[#505] results for the chosen Server/Core and both production architectures, including rejection and recovery cases. Retain exact run IDs, histories, logs and controller outputs through [#490]; a build or mock pass is insufficient. |
| Replay and recovery | Runtime maintainer | [#497]/[#503] public replay/corpus results, [#504] uncertain-outcome/idempotency results, and the supported semantics from [#511]–[#514]. Retain old and candidate worker identities and histories; conditional exclusions in this policy must be explicitly recorded, not counted as implemented tests. |
| Operational limits | Runtime maintainer / application operator | [#492]–[#495], [#498], [#500]–[#502], [#506] and [#507] evidence for isolation, resource bounds, shutdown, cache reliability, privacy, stress and sustained load. Publish measured limits and unresolved exclusions. No unattended production-readiness claim from process health alone. |
| Maintenance and release | Repository maintainer / release owner | [#509] verified confidential reporting route, supported patch policy and incident tabletop; [#510] rehearsed release/withdrawal process, immutable artifact identities and explicit release approval. Green preflight/SBOM jobs alone do not publish or approve a release. |
| Deployment acceptance | Application deployment owner | [#508] application-specific upgrade/rollback rehearsal against the actual server, namespace, authentication, workload and retained histories; published package/artifact identity plus live connectivity, readiness, diagnostics and durable behavior after rollout. Release publication does not prove deployment. |

Existing live commands provide the starting evidence set, not a replacement
for the remaining qualification issues:

```sh
make test-temporal-integration
make test-temporal-worker-restart
make test-temporal-worker-crash-recovery
make test-temporal-worker-cache-eviction
make test-temporal-workflow-patching
make test-temporal-parent-child-restart
make test-temporal-parent-child-failure-replay
```

Run them using isolated Compose projects on the recorded candidate. The
`*-contract` targets validate fixtures/controllers without a live server and
must remain labelled separately from live results. Record command, exit
status, commit, exact SDK/Core/server/compiler/platform versions, artifact
links and residual limitations. Reusing a historical green run for a changed
candidate does not satisfy release qualification.

[#489]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/489
[#490]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/490
[#491]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/491
[#492]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/492
[#493]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/493
[#494]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/494
[#495]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/495
[#496]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/496
[#497]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/497
[#498]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/498
[#499]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/499
[#500]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/500
[#501]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/501
[#502]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/502
[#503]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/503
[#504]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/504
[#505]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/505
[#506]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/506
[#507]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/507
[#508]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/508
[#509]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/509
[#510]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/510
[#511]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/511
[#512]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/512
[#513]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/513
[#514]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/514

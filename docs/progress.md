# Progress

This document records verified implementation milestones. Planned work remains
in [the implementation roadmap](implementation-roadmap.md).

Each entry describes evidence that passed at the time of its commit. The most
recent entries supersede older package names, dependency counts, and build
details. For a concise statement of what users can run today, see the project
[README](../README.md).

Entries marked "Historical snapshot" preserve the status at an earlier
milestone. Their follow-up wording is not a claim about the current
implementation when a later entry documents that work as complete. The
latest entry that records a successful live run is the authoritative status
for the two-binary Temporal acceptance path.

## 2026-07-21: Stable and prerelease tag consistency gate (#444)

The complete [PR #444 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29827725596)
passed the release-preflight job and every applicable compatibility job. The
host-only `make release-tag-check RELEASE_TAG=...` gate accepts a stable
`vMAJOR.MINOR.PATCH` tag or an optional prerelease suffix. A SemVer-style tag
such as `v1.0.0-beta.1` is normalized to the OPAM version
`1.0.0~beta.1`; the equivalent `v1.0.0~beta.1` tag is accepted directly. The
checker then requires that normalized version in `.release-version`,
`temporal-sdk.opam`, and `temporal-sdk.opam.locked`.

Malformed tags, metadata disagreement, and the checked-in `~dev` version fail
before an artifact build. This gate does not create or publish a tag, establish
artifact provenance, or replace the complete release dry run and redistribution
audit that remain roadmap work.

## 2026-07-21: Repaired live sticky-cache eviction acceptance (#438)

The complete [PR #438 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29805397413)
passes the real Temporal/PostgreSQL integration smoke after isolating the
cache-eviction worker and configuring it with one Core cache slot. The current
`make test-temporal-worker-cache-eviction` gate uses a typed read-only query as
its synchronization barrier, requires the payload-free `cache_full` marker,
acknowledges Core's `RemoveFromCache` activation with an empty completion, and
then checks exact-run cancellation and teardown. The earlier [PR #322 run](https://github.com/mfow/ocaml-temporal/actions/runs/29402103748)
remains historical evidence for the original gate; broader cache/recovery and
child-failure scenarios remain planned.

## 2026-07-19: Live external workflow cancellation acceptance (#431)

The complete [PR #431 Build run](https://github.com/mfow/ocaml-temporal/actions/runs/29679213525)
passes the real Temporal/PostgreSQL integration smoke and all compatibility
jobs. Its two-binary scenario delivers an external signal, rejects a
mismatched run ID before acknowledging the exact-run request, then cancels the
separately started target and observes its cancelled result. This replaces the
earlier documentation boundary that treated live external workflow operations
as outstanding; missing or already-completed targets and replay interaction
remain separate live scenarios.

## 2026-07-21: Stable mismatched-run external cancellation diagnostic (#439)

The complete [PR #439 Build run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578)
keeps the live external-cancellation scenario green while tightening its
wrong-run assertion. A mismatched exact-run cancellation now has documented
bridge evidence as a retryable workflow error (`non_retryable=false`) whose
message begins `Unable to cancel external workflow because not found`. The
workflow-ID-plus-run-ID identity check remains the acceptance boundary; missing
or already-completed targets and replay interaction are not implied by this
diagnostic.

## 2026-07-19: Live typed queries, workflow updates, and termination

The complete [PR #434 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29684113836)
extends the parked-workflow query acceptance from output-only queries to
`Temporal.Client.query_with_input`. It also requires the exact missing-handler
rejection and preserves the local typed-input validation boundary. The [PR #428
Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29676120429)
verifies typed workflow-update admission, polling, handler state mutation, and
completion against the real Temporal/PostgreSQL stack; [PR #432](https://github.com/mfow/ocaml-temporal/actions/runs/29681119024)
adds the unregistered-handler rejection and proves the parked workflow remains
usable afterward. The [PR #433 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29683521094)
verifies exact-run termination and reconciliation of the uncertain
acknowledgement before observing the stable `Terminated` result.

Query deadlines and replay/cache-eviction behavior, suspended update
continuations and recovery, reset/visibility acceptance, and richer interaction
policies remain separate live scenarios.

## 2026-07-17: Live output-only query acceptance (#406)

The two-binary Temporal/PostgreSQL smoke now queries the exact
`smoke.signal_condition` run while the workflow is parked on its deterministic
condition. The driver waits for the worker-visible readiness marker, requires
the output-only query result `SMOKE:QUERY:PENDING`, then submits the typed
signal and checks the ordinary terminal result. This ordering proves that the
query response came from a live workflow execution rather than a local
dispatcher or a completed final state.

The complete [PR #406 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29557704643)
passed the live integration smoke and all compatibility jobs. It established
the output-only query slice; the newer 2026-07-19 entry above supersedes its
typed-input query, update, and termination status.

## 2026-07-17: Typed workflow query-input API

`Temporal.Query.define_with_input` now defines a synchronous, read-only query
that decodes exactly one typed argument and returns a typed result. The
source-compatible `Query.define` form remains available for output-only
queries. `Temporal.Interaction.query_with_input` exercises the same contract
in the deterministic local dispatcher, while `Temporal.Client.query_with_input`
encodes the argument through the validated native payload list for an exact
workflow/run handle.

Focused interaction tests cover successful input decoding, codec failures,
exception containment, and the output-only/typed-input distinction. The
install-consumer public API fixture also compiles the new query definition,
local dispatcher, and client entry points. Live typed-input query acceptance is
recorded by the newer 2026-07-19 entry above.

## 2026-07-16: Live workflow patch lifecycle gate verified

The dedicated real-Temporal patch target now adds active-to-deprecated and
deprecated-to-removed replay to its marker-free patch-in case. Four separately
compiled OCaml worker sources make the transition observable: active code is
replaced by deprecation-only code, and a fresh deprecated-marker history is
then replaced by code containing no patch API. Strict normalized histories
preserve the exact initial prefix and observed marker deprecation state, while
controller evidence requires distinct containers and generation-two replay.

The Docker-free contract passes, and the complete [PR #356 GitHub Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29469232271) verifies the
expanded cases against Temporal Server and PostgreSQL. It covers all three
transitions with separately compiled workers, exact normalized history and
marker assertions, worker handoff evidence, and cleanup.

## 2026-07-16: Legacy build-ID worker routing boundary

`Temporal.Worker.Options` now provides an immutable, typed construction
surface for selecting `No_versioning` or legacy whole-worker build-ID routing.
The OCaml bridge emits a closed `versioning` JSON object, and Rust rejects
unknown modes, malformed nested fields, and mismatched repeated build IDs
before constructing Temporal Core's `LegacyBuildIdBased` strategy. The bridge
schema, raw ABI fixtures, OCaml option tests, and Rust mapping tests cover both
the default and legacy paths. A dedicated live routing/compatibility gate and
modern deployment-based versioning remain separate follow-up work.

## 2026-07-17: Deployment-based worker routing boundary

The worker options and private bridge now also accept a modern
`Deployment_based` routing mode. It carries a deployment name, a build ID that
must match the top-level worker build ID, a worker-versioning toggle, and an
optional `auto_upgrade` or `pinned` default behavior. OCaml validates the typed
option before crossing the ABI, while Rust validates the JSON again and maps
it to Temporal Core's `WorkerDeploymentBased` strategy. The schema and
focused OCaml/Rust mapping tests cover the contract. Deployment registration,
rollout automation, and live server routing evidence remain future work.

## 2026-07-15: Workflow patch deprecation surface

`Temporal.Workflow.deprecate_patch ~id` now records the lifecycle phase after
initial patch-in without exposing a meaningless branch decision. It shares the
per-execution replay decision with `patched`, emits a deprecated Core marker on
every permitted call, copies durable IDs, and rejects active/deprecated calls
for one ID before Core's first-command-wins behavior could make history depend
on call order. Focused tests cover live and replay emission, notification state,
same-mode repetition, mixed-mode rejection, execution isolation, mutable ID
aliases, shutdown, and native completion translation.

This milestone does not claim live deprecation or safe call removal. The next
acceptance slice must replay an active-marker history under deprecation-only
code, create and replay a fresh deprecated-marker history, and only then prove
removal against that deprecated history.

## 2026-07-15: Bilateral parent/child restart-replay gate

Status: the complete [PR #351 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013) passed all
nine jobs, including the real Temporal/PostgreSQL acceptance in 23m11s.

The dedicated gate starts a fixed parent execution from an independent OCaml
client process and runs both parent and child definitions in a separate OCaml
worker. The child parks on a durable 120-second Temporal timer. After the
controller captures exact parent/child run identities and strict initial
histories, it stops and removes generation one before starting a fresh worker.
Generation two must publish replay checkpoints for both roles and complete the
same exact child and parent runs. Checked JSON schemas, canonical history
normalization, relationship validation, strict controller chronology, and
positive bounded replay diagnostics make the evidence fail closed. The
checkpoint file is private to the acceptance worker and is atomically replaced;
the runtime validates UTF-8 identifier bytes, canonical decimal history
lengths, record order, and the full document size before publishing it.
This proves one exact parent/child restart-replay path; broader cache pressure,
crash timing, and child-failure recovery remain separate scenarios.

## 2026-07-15: Live old/new-history workflow patch replay (#348)

Status: the complete [PR #348 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29411260374) passed all
nine jobs, including the real Temporal/PostgreSQL acceptance in 18m15s.

The live controller created a marker-free history under a legacy OCaml worker
whose workflow source contains no patch call, replaced it with a fresh
patch-aware worker, observed replay, and required the legacy activity and
result. It separately created a marker-bearing history under the patch-aware
definition, replaced that worker, observed replay, and required the new
activity and result. Exact workflow/run identity, history-prefix preservation,
Core marker encoding, controller chronology, and PostgreSQL-volume removal were
validated. This proves the initial non-deprecated patch-in path only; patch
deprecation/removal, deployment versioning, and broader history migrations
remain pending.

## 2026-07-15: Non-deprecated workflow patch-in primitive

Status at the time of this entry: focused OCaml and Rust tests passed locally.
The Docker-free contract and real Temporal Server target were implemented, but
no successful live replay run had yet been recorded. The newer entry above
supersedes that live-evidence status.

`Temporal.Workflow.patched ~id` now lets direct-style workflow code introduce
a new branch while older histories without the marker select the old branch.
Core's `NotifyHasPatch` activation is validated and applied before workflow
fibers run. Each call emits a non-deprecated `SetPatchMarker` command, including
repeated calls, and decisions remain isolated to one workflow execution.

The local OCaml unit/runtime/bridge suites pass, including new-execution,
replay-present, replay-absent, repeated-call, run-isolation, mutable-source
copying, malformed JSON, and strict query-only cases. The Rust workflow
protocol suite passes all 39 tests, including closed JSON fixtures, duplicate
marker preservation, and round trips through the pinned Core protobuf types.

This evidence does not claim patch deprecation/removal, worker deployment
versioning, side effects, or arbitrary historical compatibility. The dedicated
`make test-temporal-workflow-patching` target now creates two histories: a
legacy definition with no patch call is replaced by patch-aware code and must
finish through the old branch, while a new patch-aware definition is replaced
by a fresh patch-aware worker and must finish through the new branch. The
controller requires `is_replaying=true` after each replacement, zero patch
markers in both normalized legacy snapshots, and one non-deprecated marker in
both normalized new snapshots. This is an implemented acceptance design, not a
claim that the real-server target has already passed.

## 2026-07-14: Live non-immediate activity retry (#302)

The complete [PR #302 Actions
run](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638) passed the
Temporal/PostgreSQL integration job. Its public driver started
`smoke.activity_long_backoff_retry`, retained the exact workflow/run handle,
observed the run complete, required
`SMOKE:BACKOFF:RETRIED:SMOKE`, and finished with the complete driver assertion
marker. The worker-side fixture rejects a second callback delivered in under
one second, so the result is evidence of a server-delivered, non-immediate retry
under a policy configured with a two-second initial and maximum backoff. It does
not measure or prove that the full configured delay elapsed.

This was the first live evidence for the eighteenth baseline result. The
complete [PR #439 run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578)
retains the scenario in the later 26-start baseline; PR #289 remains historical
evidence for the preceding seventeen-result slice.

## 2026-07-14: Retry-after-restart acceptance extension

Status: verified in the Temporal/PostgreSQL integration job of the complete
[PR #298 GitHub Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291).

The two-generation restart/replay fixture now uses the existing bounded
two-attempt activity policy after generation 2 replays the pending timer. The
first activity task must fail with a retryable typed error, the replacement
worker must complete the second attempt, and the driver must receive the exact
`SMOKE:AFTER-REPLAY:ATTEMPT:2` result. Temporal compacts intermediate activity
retry events out of workflow history, so the normalized history contract
checks the logical activity path and completion while the exact result marker
proves that the retry reached attempt two. The checked-in fixture and
Docker-free negative tests keep the validation deterministic.

This extends the original live restart/replay evidence without claiming
sticky-cache eviction or crash recovery. The PR #298 job passed the dedicated
live acceptance target, including the replacement worker's replay marker,
attempt-two result, terminal result, normalized history, and cleanup record.

## 2026-07-14: Live child-workflow retry acceptance (#279)

Status: verified in the complete [PR #279 GitHub Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29329420364).

The two-binary Compose smoke now starts a parent whose child has an explicit
two-attempt retry policy. The child deliberately returns a retryable workflow
failure on its first attempt and the second attempt returns the exact
`SMOKE:CHILD_RETRY:ATTEMPT:2` marker. This is a useful live assertion because
the parent, child, retry policy, and second activation all cross the public
OCaml worker boundary and real Temporal Server rather than being simulated by
the driver.

The live run exposed a real bridge gap: Core includes the inherited workflow
retry policy in the initial child activation, but the private Rust/OCaml
protocol previously rejected that field as unrepresentable. The fix carries
the validated policy through `InitializeContext`, with bilateral JSON/schema
fixtures and focused Rust, OCaml bridge, and native execution tests. The same
PR also live-verifies the activity-level non-retryable policy and heartbeat
timeout retry scenarios. The acceptance now starts fourteen workflows before
its first wait and asserts sixteen top-level outcomes in total.

The full run passed the dependency-license, quality/security, OCaml 5.2 through
5.5 Linux, Windows x64, macOS ARM, and Temporal/PostgreSQL integration gates.

## 2026-07-14: Activity non-retryable policy acceptance fixture

Status: focused fixture and Docker-free contracts verified locally; live
Temporal Server and GitHub Actions verification are pending for this change.

The two-binary Compose acceptance now includes
`smoke.activity_non_retryable_failure`. Its activity returns a retryable typed
`Activity` error on the first attempt, while the workflow retry policy names
the public `activity` error type in `non_retryable_error_types`. The workflow
observes the activity future directly and requires the runtime to preserve its
`Activity` category and non-retryable decision. A second-attempt success branch
returns a different marker, so an incorrect server retry becomes an explicit
workflow defect rather than a false passing result.

The driver starts this scenario before its first terminal wait, and the worker
registers both the workflow and activity in the separate process. The
dedicated source contract and the shared Compose contract check the policy,
registration, two-process boundary, and exact marker without claiming that a
Docker-free run has observed Temporal's retry state machine.

## 2026-07-14: Heartbeat-timeout retry acceptance fixture

Status: focused fixture and Docker-free contracts verified locally; live
Temporal Server and GitHub Actions verification are pending for this change.

The two-binary Compose acceptance now includes
`smoke.activity_heartbeat_timeout_retry`. Its first activity callback stops
sending heartbeats and remains active for six seconds, while its start-to-close
lease is ten seconds and its heartbeat lease is the shared 500 ms timeout. The
callback returns a deliberately late first-attempt marker; the second attempt
requires empty heartbeat details and returns
`SMOKE:HEARTBEAT_TIMEOUT:RETRIED:SMOKE`. This distinguishes a server-managed
heartbeat timeout from the existing application-error heartbeat retry and the
existing start-to-close timeout retry.

The driver starts this scenario only after the start-to-close timeout scenario
has completed because the activity adapter serializes callback ownership. The
worker registers the workflow and context-aware activity in the separate
process, and both the dedicated heartbeat-timeout contract and the shared
Compose source contract check that boundary without claiming live behavior.
Focused binaries, `dune build @all`, `dune runtest test/smoke`, formatting, and
the source contracts pass locally.

## 2026-07-14: Live worker restart/replay acceptance (#253)

Status: verified in the complete [PR #253 GitHub Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
for head `017c58e00ad64458f0ce2b41a7d29bad3404ded5`.

The real Temporal/PostgreSQL integration job passed the twelve-result baseline
and the two-generation restart/replay controller. Generation 1 received the
workflow and committed its durable timer; the controller stopped and removed
that worker container, then started a fresh generation 2. The replacement
worker emitted the required replay diagnostic for the exact workflow/run pair,
completed the follow-up activity, and the driver asserted the exact
`SMOKE:AFTER-REPLAY` result. The controller also validated the normalized
history ordering, accepted its thirteen lifecycle steps, and removed the
PostgreSQL volume during cleanup.

The same Actions run passed the independent dependency-license and quality
jobs, Linux OCaml 5.2 through 5.5 amd64/arm64 jobs, and the OCaml 5.5 Windows
x64 and macOS ARM jobs. This establishes live restart/replay evidence for the
current acceptance controller; sticky-cache eviction, crash recovery, and
broader child lifecycle scenarios remain separate work.

## 2026-07-13: Native immediate workflow update activation slice

Status: locally verified with focused OCaml and Rust tests; no live Temporal
Server or GitHub Actions success is claimed for this slice.

The semantic bridge now carries Core `DoUpdate` jobs and `UpdateResponse`
commands through strict Rust and OCaml JSON validation, JSON Schema
documentation, and pinned Temporal Core conversion. Both sides retain the
workflow update ID, Core protocol-instance ID, requester metadata, headers,
ordered inputs, and replay-validation flag. The native worker can register
typed `Temporal.Update.Handler.t` values; a one-input, non-suspending handler
runs its validator when requested, returns a typed result, and emits the
Core-required accepted/completed pair. Validator, codec, missing-handler, and
callback failures become structured rejections, and replay skips validation.
Response-phase checks reject duplicate decisions while allowing completion-only
later responses and accepted-then-rejected terminal responses.

The focused Rust workflow-protocol suite (37 tests), OCaml bridge protocol
suite, and native execution suite pass locally. Suspended update continuations,
metadata-aware public callbacks, and live Compose update acceptance remain
future work; the bridge deliberately fails closed for malformed or
unrepresentable Core fields.

## 2026-07-13: Two-generation worker restart/replay acceptance controller

Status: implementation and Docker-free contract verified locally. The
successful live result is recorded in the newer [2026-07-14 entry](#2026-07-14-live-worker-restartreplay-acceptance-253).

The restart fixture now contains the deterministic
`smoke.worker_restart_replay` workflow. Its timer is deliberately long enough
for the controller to observe `TimerStarted`, stop and remove generation 1,
and start a fresh generation 2 on the same task queue. The private worker
activation hook records only run identity, replay status, generation, and
history length; it never writes payloads or native handles. The driver remains a
separate OCaml assertion binary and waits for the exact run and
`SMOKE:AFTER-REPLAY` result.

The controller normalizes machine-readable Temporal CLI history, separately
checks the exact identity returned by `workflow describe`, validates the
ordered initial/terminal event sequences, and emits a closed thirteen-step
lifecycle record. The offline contract, negative parser cases, and controller
fixture pass locally. The standalone CI integration job runs the baseline
two-binary smoke followed by `make test-temporal-worker-restart`; its successful
run is recorded above. The earlier local cold ARM64 attempt stopped before
readiness because the Docker daemon exhausted storage during the native build,
which was an infrastructure failure and not replay evidence.

## 2026-07-13: Live asynchronous activity completion acceptance

Status: locally verified against Temporal Server 1.31 and PostgreSQL; CI for
this checkout is the remaining gate.

The two-binary Compose acceptance now starts eleven workflows before its first
terminal wait, including a delayed asynchronous activity-completion workflow
and a continue-as-new successor workflow. After the heartbeat retry is
terminal, the driver starts the start-to-close timeout-retry workflow and waits
for its second-attempt marker. The local command
`OCAML_VERSION=5.5 DUNE_JOBS=1 make test-temporal-integration` passed all twelve
top-level assertions, including the exact
`SMOKE:ASYNC:COMPLETED:SMOKE` result, timeout retry, child success/failure/
cancellation, continue-as-new following, exact-run cancellation, and both
graceful shutdown markers. Compose cleanup removed the PostgreSQL volume after
the run. Restart/replay/cache recovery and heartbeat-timeout-triggered retry
remain separate acceptance work.

## 2026-07-13: Async handoff and native finalizer hardening

Status: locally verified; PR CI is the remaining gate.

The asynchronous activity handoff now checks that the returned completion
handle is the one created for the current activity attempt. A handle retained
from an earlier attempt remains dormant and is rejected as a typed activity
failure instead of being attached to a new task token whose later completion
could never reach the right lease. The adapter also classifies an admitted
asynchronous lease found during shutdown as retryable. Normal shutdown now
leaves the worker graph and handle usable so the caller can finish the lease
and retry; only terminal native cleanup closes outstanding handles.

The OCaml custom-block finalizer no longer enters or leaves an OCaml blocking
section. Finalizers perform only C-side borrow accounting and transfer native
destruction to the Rust cleanup path; regular bridge calls release their
borrow before reacquiring the OCaml runtime lock. This removes unsupported
runtime operations from the garbage-collector thread while preserving the
defensive active-call barrier.

Focused verification passed for the async activity suite, all runtime and
supervisor OCaml tests, and the locked Rust test suite (including the native
ABI and lifecycle tests). Temporary Dune and Cargo output was kept outside the
repository and removed after verification.

## 2026-07-13: Typed asynchronous activity completion bridge

Status: locally implemented, focused-tested, and exercised by the local
Temporal/PostgreSQL acceptance run; CI for this checkout is still pending.

The activity API now has an explicit `Temporal.Activity.define_async` form.
Its callback can finish immediately, fail with a typed activity error, or
return a retained opaque handle for work that completes after the worker
callback has returned. A per-activity state machine copies task-token and
payload bytes, rejects conflicting or repeated operations, and keeps the
asynchronous lease separate from the ordinary worker lease. The supervisor
serializes completion, failure, cancellation, and heartbeat submissions
through the Rust bridge, and shutdown accounts for both lease registries.

The focused OCaml async-activity suite, native async bridge build, delayed
completion workflow, and terminal `NotFound` lease-retirement test pass
locally. The remaining work is Core heartbeat response flags, heartbeat-timeout
retry, bounded native-wait expansion, and broader conformance/error coverage;
the local Compose run already covers the normal asynchronous completion and
shutdown paths.

## 2026-07-13: Deterministic replay disposal lane-failure regression

Status: locally verified; this is a test-harness reliability fix and makes no
claim about a new live Temporal Server capability.

The replay disposal regression test previously aborted Core's real poll handle
without first taking ownership of it. On a slow runner that lane could reach
natural shutdown before the abort, making the test incorrectly expect a join
failure. The test-only hook now awaits the original handle and installs a
deliberately aborted pending Tokio handle, so the expected `PollLane` error is
deterministic and no producer task is detached. The focused replay test passed
20 repetitions, all ten replay-bridge tests passed, Rust formatting and the
diff check passed, and the temporary Cargo target was removed afterward.

## 2026-07-13: Timeout-retry smoke ordering and nested diagnostics

Status: focused fixture change; live Temporal Server and GitHub Actions
verification are pending for this branch.

The first attempt in the timeout-retry scenario now waits six seconds before
returning. The activity has a 500ms server start-to-close lease, while Temporal
Core adds a five-second local timeout buffer. Waiting beyond that combined
local deadline means Core classifies the expired task token before the late
callback completes, so the single-threaded activity adapter can discard the
late result and continue polling the retry instead of waiting on a completion
RPC for an already-expired task. The delay is activity code, not workflow code,
and therefore does not affect deterministic replay.

Public terminal workflow errors now use the same bounded outer-to-inner
diagnostic traversal as the workflow runtime. A nested timeout or application
cause is therefore retained in the public error message instead of being
hidden by the outer activity/child wrapper. The focused protocol regression
test covers the nested timeout shape and the depth guard; live verification of
the smoke timing remains pending.

## 2026-07-13: Temporal Core timeout failure information

Status: locally verified bridge capability; no new live Temporal Server or
GitHub Actions success is claimed for this slice. The live smoke path has
reported timeout failures intermittently, so this change is deliberately
covered by typed conversion and protocol tests rather than inferred from a
single run.

The Rust/Core adapter now maps `TimeoutFailureInfo` into a closed semantic
timeout record containing Core's exact timeout policy and ordered heartbeat
payloads. OCaml and Rust both reject unknown timeout enum values and validate
heartbeat payload limits. An absent or empty Core heartbeat collection has the
documented empty-list semantic representation and is normalized back to the
absent protobuf field. The focused Rust round-trip/unknown-value test and the
OCaml bridge round-trip/unknown-value test passed; generated build and target
directories were removed after verification.

## 2026-07-13: Scheduler-owned native workflow signal handlers

Status: focused OCaml runtime, worker-adapter, and public registration tests
pass locally; no live Temporal Server or GitHub Actions success is claimed for
this slice.

`Temporal.Worker.workflow` can now attach typed `Temporal.Signal.Handler.t`
values to a workflow registration. The native semantic `SignalWorkflow` job
retains its ordered payloads, sender identity, and headers, then queues the
matching handler on that execution's deterministic scheduler. The public
handler deliberately accepts exactly one payload; zero or multiple payloads,
missing registrations, codec failures, and callback errors fail the workflow
through the typed non-retryable path instead of silently acknowledging an
event. Rust never calls an OCaml closure. Focused adapter tests prove scheduler
delivery, deterministic `Workflow.now`, metadata retention, and unhandled
signal cleanup. Native output-only query delivery is documented and verified in
the following entry; native updates and a live Compose signal/query round trip
remain roadmap work.

## 2026-07-13: Native output-only query activation slice

Status: focused OCaml protocol, runtime, and public-registration tests and the
focused Rust/Core conversion suite pass locally. No live Temporal Server or
GitHub Actions success is claimed for this slice.

The semantic bridge now carries Core `QueryWorkflow` jobs and `QueryResult`
commands without collapsing query IDs, repeated argument payloads, or headers.
The pinned Core conversion preserves the reserved `legacy_query` identifier and
routes successful and failed answers through Core's `RespondToQuery` oneof. A
query activation is required to contain only query jobs, and its completion
must contain exactly one result for every query ID. Conversely, a stray query
result on an ordinary activation is rejected rather than silently dropped.

The OCaml runtime registers output-only `Temporal.Query.Handler.t` callbacks in
the existing worker registration. They run synchronously on the execution
owner Domain, never enter the workflow scheduler, and return a typed
non-retryable query failure when Core supplies arguments that the current
public API cannot decode. Missing handlers and callback errors become failed
query answers without failing the workflow itself. Public registration tests,
private worker-adapter tests, semantic JSON round trips, malformed mixed-query
cases, and bilateral Rust/Core conversion tests cover this behavior. A typed
query-input API, updates, and live query acceptance remain future work.

## 2026-07-13: Typed continue-as-new successor handles

Status: locally verified and exercised by the local OCaml 5.5 Compose
acceptance; CI for the newer twelve-result path is still pending.

`Temporal.Client.wait` already returns a validated successor identity when an
exact run continues as new. The public `Temporal.Client.execution` record and
`Temporal.Client.follow` helper now let an application explicitly combine that
identity with the existing client and the original typed workflow definition.
The helper performs no implicit start or latest-run lookup, retains the
workflow codecs and client ownership, rejects malformed identifiers as typed
defects, and refuses to construct a handle after client shutdown. Unit tests
cover codec-preserving exact-run handle reconstruction, malformed successor
identities, cross-namespace successor rejection, and the closed-client
lifecycle boundary. The local acceptance now follows the continued-as-new
successor explicitly; the private activation protocol also preserves Core
continuation provenance, prior-run failure, and last-completion payload
metadata through both Rust and OCaml focused tests; no live server claim is
made for those fields.

## 2026-07-13: Complete nine-scenario Temporal smoke evidence (#210)

Status: historical CI evidence from the full [PR #210 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859),
then squash-merged to `master` as `f877fbf`. The newer local twelve-result
acceptance entry above supersedes this as the current fixture description.

The green run passed every required CI job, including the Temporal/PostgreSQL
integration job. Its independent OCaml driver started all nine workflows before
waiting for any result, and its separate OCaml worker executed them against the
real server. The assertions cover fan-out, a durable timer followed by an
activity, ordinary activity retry, heartbeat-detail retry, parent/child success,
propagated non-retryable child failure, child cancellation, a typed
non-retryable top-level workflow failure, and marker-guarded exact-run
cancellation. The driver and worker shutdown markers were also checked before
the Compose project and PostgreSQL volume were removed.

At that historical commit this evidence did not claim restart/replay/cache-eviction recovery,
timeout-triggered activity retry, asynchronous activity completion, child start
failure, or continued-as-new coverage; those remain roadmap work.

## 2026-07-13: Typed interaction definitions and deterministic dispatch

Status: locally verified only; no live Temporal Server or GitHub Actions
success claim is made. GitHub Actions may remain pending while the repository
quota is exhausted.

The public API now contains experimental typed signal, query, and update
definitions with existentially paired handlers. `Temporal.Update.Handler`
executes an optional validator before the implementation and prevents the
implementation from running after rejection. `Temporal.Interaction` builds
immutable per-kind registries, rejects duplicate names, preserves synchronous
submission order, validates both codec boundaries, and converts unexpected
handler exceptions into typed defects. The native activation protocol still
rejects interaction jobs, so this is a local semantic slice rather than live
Temporal delivery. `test/unit/test_interactions.ml` covers successful
dispatch, ordering, duplicate registrations, unknown names, codec mismatch,
validator short-circuiting, and exception containment.

Local evidence for this entry: `opam exec -- dune build @install
test/unit/test_interactions.exe`, `opam exec -- dune exec
./test/unit/test_interactions.exe`, and `git diff --check`.

## 2026-07-13: Local verification and queued-Actions guidance

Status: documentation-only update. No live Temporal Server result or GitHub
Actions success is claimed; queued checks remain unexecuted evidence while the
repository quota is exhausted.

The README, documentation guide, quality-gate reference, and live-acceptance
command reference now map every CI job to its Makefile command and distinguish
the local baseline from CI-only checks. `make check OCAML_VERSION=5.2` is the
representative Docker-backed build, test, and package-license baseline;
`make quality` covers the pinned host scanners; `make native-verify` covers a
matching Windows/macOS native host; and `make test-temporal-integration` is the
optional real Temporal Server/PostgreSQL gate. The locked Cargo license scan
remains a single isolated CI job, and local results are explicitly not treated
as a substitute for an unexecuted matrix, platform, or live-server job.

## 2026-07-13: Docker-free heartbeat acceptance contract

Status: locally verified; no live Temporal Server acceptance is claimed. The
dedicated Actions integration job may remain pending while the repository
quota is exhausted.

The two-binary heartbeat slice now has a focused Docker-free contract in
addition to the existing native protocol tests. The source contract protects
the separate driver/worker roles, worker registration, start-before-wait
ordering, exact heartbeat retry result, and marker cleanup. A Dune test invokes
the exact shared context-aware activity twice with in-memory contexts: the
first attempt must encode one progress detail and return a retryable error,
the second must receive that copied detail and the 500 ms timeout before it can
return, and an invalidated first context must reject a later heartbeat. The
test deliberately does not claim that the fake context observed server-managed
retry delivery. Focused Dune tests, the heartbeat contract script, shell
syntax, and `git diff --check` pass locally. A real PostgreSQL/Temporal Compose
run remains required for live heartbeat evidence.

## 2026-07-13: Replay/Core disposal lifecycle hardening

Status: locally verified; no live Temporal Server acceptance is claimed, and
GitHub Actions may remain pending while the repository quota is exhausted.

Replay disposal now acknowledges abandoned activations with Core's empty
completion, drains the cache-eviction activation that Core can publish after
that acknowledgement, and retains the native owner on join or finalization
failure. The replay-aware path avoids the live-worker failure completion that
triggered Core's `A non-empty completion was not processed` panic in the OCaml
5.2 replay lifecycle CI baseline. Focused replay ABI tests, the complete
`ocaml-temporal-core-bridge` Rust test suite, offline clippy with warnings
denied, formatting, and `git diff --check` pass locally.

## 2026-07-13: Typed activity cancellation handles

Status: locally verified in the public activity API, activation runtime, and
workflow-authoring tests. No live Temporal acceptance is claimed. GitHub
Actions checks for this milestone may remain pending because of the repository
quota.

The squash-merged [PR #191](https://github.com/mfow/ocaml-temporal/pull/191),
commit `cb07df2`, adds an opaque `Activity.start_handle` API alongside the
existing future-only `Activity.start` and `Activity.execute` helpers. The
handle keeps the typed result future with an owner-checked, parameterless
`Activity.cancel` operation. The runtime emits at most one deterministic
`Request_cancel_activity` command for the private activity sequence, rejects
calls from another workflow context, and treats repeated or post-terminal
cancellation as typed idempotent no-ops. Invalid options and input encoding
produce a ready failed handle without scheduling a command. Focused tests cover
command ordering, typed cancellation resolution, invalid and detached calls,
ownership checks, and natural or failed terminal races.

## 2026-07-13: Two-binary child failure and cancellation acceptance coverage

Status: locally contract-checked after the squash-merged [PR #193](https://github.com/mfow/ocaml-temporal/pull/193), commit `48ed97f`. No live Temporal Server or GitHub Actions success claim is made here. The expanded Actions run was cancelled, and subsequent checks may remain queued while the repository quota is exhausted.

The two-binary fixture now starts nine top-level workflows before awaiting any
result. In addition to the historical success and retry scenarios, the driver
asserts a parent that propagates a deterministic non-retryable child failure
and a parent that cancels a long-running child through its typed child handle,
waiting for `Wait_cancellation_requested` before returning an exact marker.
The worker registers both workflows and the driver checks their typed results.
Docker-free fixture/role/readiness/stop/quality contracts, focused Dune builds,
format checks, and `git diff --check` passed locally. The Docker-backed
PostgreSQL/Temporal run was not available in this environment.

## 2026-07-13: Replay worker ABI and supervisor operation

The bounded replay worker is now reachable through the private C ABI and the
single-domain OCaml supervisor. The supervisor validates each closed replay
history document before sending it to Rust, repeats the validation at the
native boundary, and serializes start, feed, poll, completion, rejection,
finalization, and disposal operations with the other runtime lifecycle calls.
Malformed histories and completions are rejected without retaining a feeder or
workflow lease. Focused Rust ABI tests cover null-handle and missing-worker
status paths, malformed input, and idempotent cleanup. The OCaml bridge suite
covers sender-side canonical-payload validation and replay disposal. This is
still library-level replay plumbing: live two-generation restart/replay
Compose acceptance remains planned. Local targeted Rust and Dune builds passed;
queued GitHub Actions are not treated as evidence for this milestone.

## 2026-07-13: Bounded native replay worker plumbing

The private Rust bridge now accepts a closed replay-history JSON document,
validates duplicate/unknown fields, canonical base64, payload bounds, and
Temporal Core history invariants, then feeds validated histories through a
one-slot `HistoryFeeder` into a workflow-only Core replay worker. Finalization
now requires the feeder to be closed, every activation to be completed, and
the workflow lane's natural `Shutdown` to be observed; it does not cancel
queued history. An explicit destructive `dispose` path owns force-completion
for abandoned work, and any terminal poll-lane failure retains the owner and
the typed error after best-effort ledger cleanup. Disposal retries Core's
terminal finalization once and reports the second failure rather than dropping
the still-owned native graph, so a caller can release a competing owner and
retry safely. The disposal ledger now retires every force-completed workflow
run ID and activity token before awaiting Core, preventing a late poll from
being admitted as a new identity between the initial snapshot and ready-queue
drain. Retired identities remain bounded tombstones until both poll lanes join,
then are cleared. The replay worker owns no OCaml pointer or callback. Focused
Rust tests cover round trips, rejection paths, construction, clean shutdown,
one-history admission/completion, the typed precondition for
finalize after feeder close but before draining, and retained-owner disposal
recovery for both shared-Core and poll-lane failures. The document format is
specified by
[`docs/reference/replay-bridge.md`](reference/replay-bridge.md) and its JSON
Schema. This entry records the native portion of the implementation; the
follow-up ABI and supervisor entry above records the OCaml operation layer.
Live two-generation restart/replay Compose acceptance remains planned. Local
Rust tests passed; queued GitHub Actions are not treated as evidence for this
milestone.

## 2026-07-13: Retained activity completion worker-loop resilience

The native activity adapter now carries an explicit retryability classification
from the supervisor for completion rejections and separately classifies private
transient completion exceptions. Only the explicit bilateral `Retryable` status
is eligible for a completion retry in production. Generic `Connection` and
`Not_ready` statuses are fail-closed because this Core revision may already have
consumed the lease; protocol, configuration, worker-state, and supervisor-defect
failures remain fatal. A new private
`Temporal_runtime.Native_worker_loop` applies a bounded activity-lane readiness
wait before retrying a retained completion, then allows the next activity task
to run without invoking the original OCaml implementation again. The wait is
performed by the blocking worker Domain through the existing native readiness
operation, so workflow effect schedulers and adapter mutexes are not blocked.

Focused local regressions cover one transient rejection followed by completion
acceptance and a subsequent task, a permanent protocol error that stops the
loop, and a specifically classified transient completion exception. The
runtime suite and focused native build pass locally; GitHub Actions remains
queued under the repository quota, so this entry makes no CI-success claim.

## 2026-07-13: Per-run worker shutdown evidence

The two-binary Compose teardown now removes a per-run marker before stopping
the worker and accepts shutdown only after the current worker publishes the
exact `worker-stopped` value. This avoids a false positive caused by
`docker compose logs`, whose aggregate output can retain a successful marker
line from an earlier container instance. The marker writer uses the same
temporary-file-and-rename rule as readiness publication, and a Docker-free
contract test seeds a stale log before proving that validation fails until the
current marker exists. Local validation passed with `dash -n`, the Compose
configuration contract, the worker-stop contract, and the restart/replay
contract; GitHub Actions is treated as pending infrastructure evidence rather
than a local correctness signal.

## 2026-07-13: Readiness-marker stale-state protection

The two-binary worker now removes its configured readiness marker immediately
after validating the marker environment and before `Worker.create` begins. Its
finalizer still removes the marker after normal shutdown and runtime errors.
This prevents a reused or interrupted Compose container from satisfying the
health check with a previous run's `worker-ready` file while the current
worker is still starting or has failed. The Docker-free readiness contract
seeds that stale marker and checks the source ordering, while the Compose
configuration contract requires the Makefile target. GitHub Actions may remain
queued because of repository quota; this milestone records local verification
only and makes no CI-success claim.

## 2026-07-13: Readiness lifecycle ordering and container recreation

The worker now clears its readiness marker immediately after validating the
readiness path, before later marker-environment validation can fail. The
Docker-free contract models a missing cancellation-marker setting and checks
that ordering. `temporal-start-worker` also force-recreates the Compose worker
container: readiness is stored in that container's `/tmp`, so reusing a stopped
container could otherwise satisfy `--wait` before the new process starts.
Local shell, Compose-model, formatting, native-build, and stale-marker runtime
checks provide the evidence for this milestone; queued Actions are not treated
as a result.

## 2026-07-13: Acceptance validator and client-boundary hardening (#164–#165)

The merged tip is `1fa679c`: [#164](https://github.com/mfow/ocaml-temporal/pull/164)
(`4724830`) preserves configurable `JQ_BIN` paths containing spaces and adds
a regression invocation; [#165](https://github.com/mfow/ocaml-temporal/pull/165)
(`1fa679c`) validates client protocol identifier sizes consistently and adds
boundary tests.

Focused local verification includes
`make test-temporal-worker-restart-contract`,
`HOST_UID=501 HOST_GID=20 make test-temporal-config`,
`make test-temporal-config`, `sh scripts/check-format.sh`,
`make test-quality-contract`, and `git diff --check`. GitHub Actions remain
queued while the repository quota is exhausted, so this entry makes no
CI-success claim and adds no new live Temporal evidence. The historical
five-execution live result in run
[`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073)
remains the latest successful two-binary acceptance evidence.

## 2026-07-13: Public client state, protocol evidence, and scheduler teardown (#159–#161)

The merged tip is `02f4627`: [#159](https://github.com/mfow/ocaml-temporal/pull/159)
(`980855f`) clarifies public client state and adds protocol coverage; [#160](https://github.com/mfow/ocaml-temporal/pull/160)
(`d5606ad`) separates local protocol evidence from live Temporal evidence; and
[#161](https://github.com/mfow/ocaml-temporal/pull/161) (`02f4627`) releases
settled futures during scheduler teardown with a weak-reference regression test.

PR #161 recorded this local verification: `DUNE_BUILD_DIR=/tmp/ocaml-temporal-dune-audit
CARGO_TARGET_DIR=/tmp/ocaml-temporal-cargo-target opam exec -- dune runtest --root .
test/runtime`, `sh scripts/check-format.sh`,
`sh test/smoke/test_quality_contract.sh .`, and `git diff --check`. The
repository's GitHub Actions checks remain queued while the Actions quota is
exhausted, so this entry makes no CI-success claim. The historical
five-execution live result in run
[`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073)
remains the latest successful two-binary acceptance evidence.

## 2026-07-13: Protocol, lifecycle, and two-binary acceptance contracts (#141–#152)

Status: the merged documentation and acceptance-contract milestones are now
present on `origin/master` at `c008c52`. The activity-protocol lifecycle and
evidence records are [#141](https://github.com/mfow/ocaml-temporal/pull/141)
(`cb2892c`), [#142](https://github.com/mfow/ocaml-temporal/pull/142)
(`d1d45c2`), and [#143](https://github.com/mfow/ocaml-temporal/pull/143)
(`ef3e171`); asynchronous completion, native worker ownership, and installed
package boundaries are [#144](https://github.com/mfow/ocaml-temporal/pull/144)
(`cfb760a`), [#145](https://github.com/mfow/ocaml-temporal/pull/145)
(`9a6992a`), and [#146](https://github.com/mfow/ocaml-temporal/pull/146)
(`127f3f6`). Native execution translation, dependency licensing, restart and
replay evidence, and the separation of control from operation JSON are
recorded by [#147](https://github.com/mfow/ocaml-temporal/pull/147)
(`6e860b2`), [#148](https://github.com/mfow/ocaml-temporal/pull/148)
(`b30f85f`), [#149](https://github.com/mfow/ocaml-temporal/pull/149)
(`54cf1a9`), and [#150](https://github.com/mfow/ocaml-temporal/pull/150)
(`3466ae9`). [#151](https://github.com/mfow/ocaml-temporal/pull/151)
(`ef0ed69`) adds assertions that the acceptance harness has separate
`smoke_driver` and `smoke_worker` OCaml binaries with distinct roles. [#152](https://github.com/mfow/ocaml-temporal/pull/152)
(`c008c52`) documents the native worker execution-state invariants and adds
regression coverage for them.

Representative local verification on the merged tip passed: `git diff
--check`, `sh scripts/check-format.sh`, `make test-quality-contract`,
`make test-temporal-config`,
`sh test/integration/temporal/scripts/test-restart-replay-contract.sh`, and
`cargo test --manifest-path rust/Cargo.toml --locked --test protocol` (six
protocol tests). The offline restart/replay contract reports
`restart/replay contract: ok`.

GitHub Actions for this series were observed queued or pending while the
repository was affected by its Actions quota, so this entry does not treat
those checks as passing evidence. The Docker Compose acceptance against a
live Temporal Server and PostgreSQL was not run for this milestone, and no
new live result is claimed at that milestone. The historical five-execution live result in run
[`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073)
was the latest successful two-binary acceptance evidence at that time; the
later PR #210 entry above supersedes it.

## 2026-07-13: Documentation evidence and navigation refresh (#129–#140)

Status: documentation-only updates were merged in PRs [#129](https://github.com/mfow/ocaml-temporal/pull/129)
(`d37f863`), [#130](https://github.com/mfow/ocaml-temporal/pull/130)
(`857862b`), and [#131](https://github.com/mfow/ocaml-temporal/pull/131)
(`404a7c5`) for runtime invariants, merged lifecycle evidence, and feature
coverage; [#132](https://github.com/mfow/ocaml-temporal/pull/132)
(`1dfc13e`), [#133](https://github.com/mfow/ocaml-temporal/pull/133)
(`515f723`), and [#135](https://github.com/mfow/ocaml-temporal/pull/135)
(`a656f92`) for the two-binary acceptance boundary, queued-CI fallback, and
live-acceptance evidence; [#134](https://github.com/mfow/ocaml-temporal/pull/134)
(`229b548`), [#137](https://github.com/mfow/ocaml-temporal/pull/137)
(`97924c7`), and [#138](https://github.com/mfow/ocaml-temporal/pull/138)
(`65b6441`) for native activity, client protocol, and Core-bridge contracts;
and [#136](https://github.com/mfow/ocaml-temporal/pull/136)
(`b487eaf`), [#139](https://github.com/mfow/ocaml-temporal/pull/139)
(`6709fce`), and [#140](https://github.com/mfow/ocaml-temporal/pull/140)
(`9104f3d`) for observability, workflow guidance, and documentation
navigation.

These changes clarify the current implementation and evidence boundaries but
do not add runtime behavior or new live Temporal results. Applicable local
format, quality-contract, configuration, and focused documentation checks were
used for the documentation PRs; host-only `make quality` remains dependent on
the pinned scanner binaries being installed. GitHub Actions checks may remain
queued because of the repository quota and are not treated as passing evidence.
The historical five-execution live result in run
[`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073)
was the latest successful two-binary acceptance evidence for this historical
entry. The expanded assertions were later live-verified by PR #210, as recorded
in the current entry above.

## 2026-07-13: Scope, child-lifecycle, and ABI cancellation-validation coverage

Status: locally verified on the merged `origin/master` tip with focused OCaml,
Rust, and ABI tests. The work was merged in [PR #125](https://github.com/mfow/ocaml-temporal/pull/125)
as `8e56c24`, [PR #126](https://github.com/mfow/ocaml-temporal/pull/126) as
`43beafb`, and [PR #127](https://github.com/mfow/ocaml-temporal/pull/127) as
`55d758c`. This entry makes no claim about a live Temporal Server run or
GitHub Actions success.

Scope operations now reject cancellation, status, checks, and awaits issued
from a scheduler that did not create the scope, without mutating the owner's
state. Native child-lifecycle tests cover terminal-before-start rejection,
duplicate start acknowledgements, duplicate terminal resolutions while an
unrelated parent timer remains pending, and retryable child-failure
classification. Bilateral protocol tests round-trip all four child-cancellation
policies. The client ABI test confirms malformed cancellation JSON is rejected
before lifecycle-state lookup, preventing invalid input from being reported as
an unrelated connection or state error.

## 2026-07-13: Activity-context lifetime regression coverage

Status: locally verified in the native activity execution tests; no live
Temporal Server claim is made by this test-only milestone. The change was
merged in [PR #120](https://github.com/mfow/ocaml-temporal/pull/120) as commit
`04a6bab`.

The activity-context tests now prove that previous-attempt details, heartbeat
arguments, callback-owned views, and heartbeat timeouts are copied at every
public boundary. They also verify that a heartbeat callback exception becomes a
non-retryable typed defect, that the callback is invoked only once, and that a
retained context rejects heartbeat calls after invalidation without entering
the callback. These checks complement the existing lease-retention and
post-completion invalidation tests; they do not add asynchronous completion or
live heartbeat-timeout behavior.

## 2026-07-13: Lifecycle and bridge ownership regression coverage

Status: locally verified in the focused OCaml and Rust tests; no live Temporal
Server or GitHub Actions success claim is made by these test-only milestones.
The lifecycle tests were merged in [PR #118](https://github.com/mfow/ocaml-temporal/pull/118)
as commit `8b62593`, and the bridge ownership tests were merged in [PR #119](https://github.com/mfow/ocaml-temporal/pull/119)
as commit `25eb755`.

The lifecycle corpus now checks that continue-as-new remains terminal when
later timer or cancellation jobs arrive, that child cancellation after a
failed start is an idempotent no-op, and that repeated scope cancellation does
not emit a Temporal command. Rust protocol tests reject a continue-as-new
completion with a follow-up command in both JSON and Core conversion. The ABI
and activity-protocol tests additionally verify malformed-heartbeat result
cleanup can be reused and that decoded activity bytes remain valid after the
source JSON buffer is dropped.

## 2026-07-13: Context-aware activity heartbeats

Status: locally verified in the bilateral OCaml/Rust protocol tests and the
private native activity execution adapter. This entry does not claim live
Temporal Server heartbeat or timeout coverage.

`Temporal.Activity.define_with_context` now gives an activity attempt a typed,
opaque context. The activity can read the ordered details saved by a previous
heartbeat, inspect the server-supplied heartbeat timeout, and send a typed
heartbeat through `Temporal.Activity.Context.heartbeat` (or already encoded
details through `heartbeat_payloads`). Public callbacks remain ordinary
direct-style OCaml functions and expected bridge failures remain
`(value, Error.t) result` values.

The context owns copied payloads and a copied task token. One adapter mutex and
the SDK supervisor mailbox serialize heartbeat, completion, polling, and
shutdown operations. Rust validates the strict closed JSON document, checks
the token against its outstanding activity ledger, converts payloads to the
official Core protobuf, and deliberately leaves the lease active for terminal
completion. The adapter invalidates the context on every activity exit path,
so retaining it cannot retain a native pointer or heartbeat a later task.

The new schema is
[`activity-heartbeat.schema.json`](schemas/bridge/activity-heartbeat.schema.json)
and the wire details are documented in
[activity protocol](reference/activity-protocol.md) and
[native activity execution](reference/native-activity-execution.md). Focused
tests cover binary details, malformed documents, context dispatch, heartbeat
lease retention, and post-completion invalidation. A dedicated Docker Compose
scenario is still required before this capability can be called live verified.

## 2026-07-13: Two-binary heartbeat-detail retry acceptance fixture

Status: implemented and locally contract-checked; no live Temporal Server or
GitHub Actions success claim is made here. The Docker backend is not available
in the current environment, so the fixture has not been run against its
PostgreSQL and Temporal containers.

The existing nested two-binary fixture now includes
`smoke.activity_heartbeat_retry`. The workflow starts this scenario alongside
the other smoke executions before awaiting any result. Its activity is defined
with `Temporal.Activity.define_with_context`, receives a 500 ms heartbeat
timeout, sends `SMOKE:HEARTBEAT:PROGRESS:1` on the first attempt, and returns a
retryable typed activity error. The driver requires the second attempt to read
that exact detail and timeout from Temporal through the opaque activity
context, returning `SMOKE:HEARTBEAT:RETRIED:SMOKE` only when the server-visible
heartbeat path worked.

The worker and driver remain separate OCaml binaries, and the Makefile's
failure-only workflow inspection lists the new workflow ID. The Docker-free
Compose contract checks both registrations and the exact driver assertion.
Heartbeat-timeout-triggered retries are intentionally not claimed: the
current synchronous activity adapter treats a stale completion after a server
timeout as a separate lifecycle capability requiring asynchronous completion
and recovery work.

## 2026-07-13: Typed child-workflow cancellation control

Status: locally verified in the OCaml activation, native translation, bridge
protocol, and Rust Core-conversion tests. Live Temporal acceptance is not
claimed by this entry.

Child workflows now expose an opaque `start_handle` that pairs the typed result
future with an idempotent `cancel` operation. The handle can select Core's
`Try_cancel`, `Wait_cancellation_completed`, `Wait_cancellation_requested`, or
`Abandon` policy; cancellation reasons are validated before becoming durable
history commands. The OCaml activation algebra, strict JSON protocol and
schema, Rust Core conversion, and native worker copy path all preserve the
child sequence, policy, and reason. Focused tests cover cancel-before-start,
duplicate cancel requests, typed child cancellation results, JSON round trips,
and Core command conversion.

## 2026-07-12: Public future shutdown guards

Status: locally verified in the focused runtime and package checks; this
milestone has no live Temporal or GitHub Actions success claim. The change was
merged in [PR #99](https://github.com/mfow/ocaml-temporal/pull/99) as commit
`efb02dd`.

Shutdown now makes queued public-future observers, derived-future mappers, and
ready continuations inert before they can run. A future awaited after its
owner has shut down returns the typed outside-owner error instead of resuming
workflow code. The regression tests cover both callback suppression and
re-entrant awaits, in addition to the existing root-future shutdown cases.
The PR recorded `DUNE_CACHE=disabled dune build --root . -j 1`, the focused
runtime tests, locked Rust tests, package-consumer smoke checks, formatting,
and `git diff --check` as passing locally.

## 2026-07-12: Correction to live cancellation evidence

Status: documentation correction merged in
[PR #100](https://github.com/mfow/ocaml-temporal/pull/100) as commit `9baa00a`;
no new live acceptance result was produced by that documentation change.

The acceptance references now distinguish the historical five-execution green
run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073)
from the current-at-that-time seven-run cancellation/heartbeat implementation and its local protocol,
client, worker, and supervisor checks. The one-shot OCaml assertion driver and
the long-lived worker are described separately. GitHub Actions run
[`29193818312`](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312)
was cancelled, so the seven-run cancellation and heartbeat scenarios were
unverified against a live Temporal Server at the time. PR #210 later supplied
the green nine-scenario run recorded in the current entry above.

## 2026-07-12: GitHub Actions capacity observation

Status: historical observation for PR #100; this entry does not claim a
completed CI result. The PR #100 Actions attempt
[`29194514765`](https://github.com/mfow/ocaml-temporal/actions/runs/29194514765)
did not produce a completed result in the observed window, and its push run for
merge commit `9baa00a`
[`29194534789`](https://github.com/mfow/ocaml-temporal/actions/runs/29194534789)
was later cancelled by workflow concurrency. Neither run is live acceptance
evidence. Repeated updates can cancel superseded runs because
`.github/workflows/build.yml` enables `cancel-in-progress`; the last successful
live acceptance evidence remains run `29191260073` above.

## 2026-07-12: Exact-run client cancellation control path

Status: focused OCaml, Rust, bridge, supervisor, and mock-client tests pass
locally; the live Compose cancellation scenario remains follow-up work.

`Temporal.Client.cancel` now requests cancellation for the exact workflow and
run retained by a typed client handle. The public API accepts an optional
caller-supplied request ID and reason, validates both before crossing the
native boundary, and generates a stable request ID when the caller omits one.
The operation returns only after Temporal acknowledges the control request;
the caller observes the eventual terminal state by waiting on the same handle.
Expected failures remain typed `result` values, and the mock backend models the
same exact-run and idempotent behavior for deterministic unit tests.

The private OCaml/Rust JSON protocol has closed request and acknowledgement
documents with bilateral unknown-field, duplicate-field, identifier, reason,
and positive-acknowledgement validation. Rust calls the official
`RequestCancelWorkflowExecution` RPC through the existing supervisor-owned
Core connection and bounds that RPC to one second, so a stalled server cannot
hold the owner Domain indefinitely. A timeout is reported as a typed bridge
failure and the caller can retry the same request ID. The C ABI, header, and
native OCaml wrapper preserve the existing ownership and panic-containment
rules; no Rust task calls OCaml.

Evidence: the OCaml client protocol executable, Rust cancellation protocol
tests, public mock-client cancellation tests, native worker-operation tests,
Rust formatting, and `git diff --check` passed. Schemas and wire semantics are
documented in [client protocol](reference/client-protocol.md). This milestone
does not claim live cancellation coverage; that scenario must keep a workflow
outstanding, issue the request through the two-binary fixture, observe the
server's cancelled terminal result, and then verify clean shutdown.

## 2026-07-12: Typed non-retryable workflow failure acceptance

Status: verified in GitHub Actions run
[`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073)
for merge commit `a4eaccc8`. The real Compose acceptance passed with
PostgreSQL and Temporal Server, the separate OCaml worker, and the one-shot
OCaml assertion driver.

The two-binary fixture now registers `smoke.non_retryable_failure`, a
deterministic workflow that returns `Temporal.Error.make
~category:\`Workflow ~non_retryable:true` with the stable message
`intentional terminal workflow failure`. The one-shot OCaml driver starts this
workflow as a fifth top-level execution before waiting for any result. It
requires the four existing exact success payloads and then inspects
`Temporal.Error.view` on the fifth `Client.Failed` outcome, rejecting any other
terminal class, category, retry policy, or message prefix. The worker remains
the only process that polls and executes workflow code.

The Makefile metadata inspection now includes the fifth workflow ID, and the
acceptance, feature-coverage, and local-stack references describe the new
typed-failure boundary. The CI command passed: PostgreSQL and Temporal became
healthy, the lifecycle check passed, the worker published its readiness
marker, four workflows returned exact payloads, and the fifth returned the
required typed non-retryable workflow failure. The PostgreSQL volume and
Compose project were removed by the target's cleanup trap.

## 2026-07-12: Live activity retry acceptance scenario

Status: verified in Linux CI run
[`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405)
for commit `b895d3c` by the `Temporal/PostgreSQL integration smoke (OCaml
5.5)` job. The existing retry-policy constructor, JSON protocol, and Temporal
Core conversion tests remain synthetic evidence; they prove that the policy is
validated and preserved, while the linked CI run proves one server schedules a
second activity attempt.

The two-binary fixture now includes `smoke.activity_retry`. Its
`smoke.retry_once` activity deliberately returns a retryable `Activity` error
on its first call and returns `SMOKE:ATTEMPT:2` on the next call. The one-shot
OCaml driver starts this workflow alongside the fan-out, timer, and
parent/child workflows before waiting for any result, then asserts that exact
attempt-2 payload. The long-lived `smoke-worker` registers the workflow and
activity; it remains the only process that polls and executes Temporal tasks.

Every run still starts from a fresh Compose project and removes the
PostgreSQL volume before and after the test. The activity's attempt counter is
process-local test state and is reset when that fresh worker process starts,
so the assertion does not depend on history from an earlier run.

Evidence: the linked CI run passed `make test-temporal-integration`, including
the exact `SMOKE:ATTEMPT:2` assertion, plus the focused OCaml/Rust policy and
protocol tests listed in the [activity retry decision](decisions/0007-activity-retry-policy.md).
This proves one short success-after-retry path only; retry timeouts,
non-retryable classification, cancellation, replay, and worker recovery remain
separate scenarios.

## 2026-07-12: Live two-OCaml-binary Compose acceptance

Status: verified in Linux CI for commit `d4456b7` by the
`Temporal/PostgreSQL integration smoke (OCaml 5.5)` job. The supported local
command is `make test-temporal-integration`.

The isolated Compose fixture now starts real PostgreSQL and Temporal Server,
runs the focused supervisor lifecycle check, then starts `smoke-worker` and
`smoke-driver` as separate OCaml processes. Both link the public
`temporal-sdk` library and each owns its own private Rust/Core graph. The
worker registers `smoke.fan_out`, `smoke.timer_then_activity`, and
`smoke.mock_transform`; the driver starts both workflows through
`Temporal.Client` before it waits for either exact workflow/run handle.

The driver asserted `SMOKE:LEFT|SMOKE:RIGHT` for the fan-out workflow and
`SMOKE:TIMER` for the timer-then-activity workflow. This provides live
success-path evidence for client start and exact-run wait, native workflow and
activity dispatch, a durable timer, and two activity commands scheduled before
the first workflow wait. The test also cleanly shuts down the client and worker
before Compose removes the isolated volume.

This is deliberately not a claim of full live parity. Child workflows,
non-success terminal outcomes, retry and cancellation behavior, worker restart
and replay, cache eviction, and shutdown with outstanding work still need
dedicated real-server scenarios. This entry supersedes earlier entries that
describe the two-binary live gate as pending.

Evidence: `make test-temporal-integration`; the passing CI log records both
driver starts, their exact-run waits, the two asserted results, and clean
worker/client teardown.

## 2026-07-12: Native child-workflow resolution lifecycle

Status: focused Rust protocol, OCaml runtime, worker-adapter, and bilateral
fixture tests pass locally; live Compose execution remains follow-up work.

The Core bridge now translates both child-resolution activation variants. A
successful `resolve_child_workflow_start` stores the server-assigned run ID
without completing the parent future. A failed or cancelled start resolves and
retires that future immediately. The later `resolve_child_workflow` job carries
the nullable payload or structured failure and is accepted only after the
successful start acknowledgment. The OCaml context store rejects final-before-
start, duplicate, and unknown sequences as typed bridge failures, while Rust
preserves child execution identity, event IDs, retry state, payload bytes, and
recursive failure causes. The temporary native-worker child-start rejection
gate has been removed.

Evidence: the shared `child-resolution` JSON fixture is accepted and
normalized by both Rust and OCaml; Rust Core-conversion tests cover completed
and failed child results; focused runtime tests cover ordered lifecycle,
start-failure cleanup, final-before-start, duplicate sequences, and lease
retirement. The live two-OCaml-binary Compose acceptance is still required to
prove the complete Temporal Server path.

## 2026-07-12: Native lifecycle retention regressions

Status: focused OCaml and Rust lifecycle tests pass locally; the live Compose
acceptance path remains separate.

The lifecycle coverage is now split into focused test files. The native
supervisor test checks repeated worker shutdown, client disconnect, and parent
runtime shutdown without a Temporal Server. Separate workflow and activity
adapter tests force a completion rejection during polling and another during a
drain, then verify that the next drain acknowledges the original copied
completion without rerunning user code. A separate Rust integration test
disposes one runtime twice and waits for exactly one cleanup-counter increment.
The existing private ABI regression also observes a dropped pending-start
future after nonblocking cleanup, proving Tokio handles are joined rather than
detached.

Evidence: `CARGO_TARGET_DIR="$PWD/rust/target" dune runtest --root .
test/runtime test/sdk_supervisor`, the three focused native OCaml executables,
`cargo test --manifest-path rust/Cargo.toml --locked --package
ocaml-temporal-core-bridge --test runtime_cleanup_idempotence`, and the ABI and
runtime-cleanup integration tests all pass on the representative macOS host.

## 2026-07-12: Complete native activity command translation

Status: focused runtime, worker-adapter, and native translation tests pass
locally; native public-worker wiring and live Compose acceptance remain
follow-up work.

The deterministic runtime now emits complete Temporal activity commands. A
workflow can choose an activity ID, task queue, all four Temporal timeout
fields, cancellation policy, and eager-execution preference. Omitted IDs are
derived from the deterministic command sequence; omitted queues inherit the
execution queue; and omitting both schedule-to-close and start-to-close uses a
60-second start-to-close default. The OCaml public API validates explicit
identifiers before allocating a command, while the native translator validates
the complete record again at the Rust/Core boundary and copies payload bytes.
Invalid identifiers, missing required timeout coverage, negative durations,
and malformed payloads are rejected before a completion can be emitted rather
than silently changing the command. Public negative durations are rejected by
`Temporal.Duration.of_ms`; malformed internal command records are reported as
typed bridge errors by the translator.

Evidence: `dune build --root . @install`, `dune runtest --root . test/runtime`,
and the focused activation, native-translation, and native-worker executables
pass on the representative host toolchain. Tests cover explicit options,
configured and default queues, UTF-8/identifier validation, payload copying,
cancellation and eager flags, timeout validation, and activity lease
retirement.

## 2026-07-12: Native child-workflow start command translation

Historical snapshot: focused OCaml and Rust protocol/translation tests passed
locally; child result resolution, native worker wiring, and live Compose
acceptance were follow-up work at this commit.

The private bilateral completion protocol now has a closed
`start_child_workflow` command. The OCaml runtime maps its deterministic
sequence, child workflow ID and type, copied input payload, and optional
validated retry policy into that record. Rust validates the identifiers and
policy, emits Temporal Core's `StartChildWorkflowExecution` command for
focused translation, and rejects non-default Core options that the current
OCaml API does not expose instead of silently discarding them. The native
worker gates this command before submission until child-resolution activations
are decoded, so no partially supported live path can strand a parent lease.
The JSON schema and both-language round-trip tests cover the semantic shape;
the live acceptance suite still has no child-retry scenario.

At that commit the activation side still lacked Core's child-resolution job,
so the milestone did not claim that a workflow could await a child result.
The later native child-workflow resolution entry and the live two-binary
acceptance entry above supersede that limitation; a live parent/child result
remains follow-up work.

Evidence: `dune runtest --force test/bridge`, the focused native execution
tests, and `cargo test --manifest-path rust/Cargo.toml --locked --test
workflow_protocol` pass on the representative host toolchain.

## 2026-07-12: Wakeable native worker readiness seam

Status: focused Rust readiness, C ABI, and OCaml private-wrapper tests are
implemented; live worker execution and the Docker Compose acceptance path
remain follow-up work.

The private bridge now has workflow and remote-activity readiness operations in
addition to its non-blocking drains. Each Rust poll lane uses a mutex-protected
pending count and condition variable. A producer holds the mutex while sending
the queue message and recording the count; the owner Domain holds it while
receiving and retiring the count. This makes queue publication and wakeup
linearizable, so notification-before-wait and send/receive races cannot lose a
task. Lane errors and shutdown close or fail the signal and wake a waiter; any
queued messages are drained before terminal state is reported.

The C stubs release the OCaml runtime lock for the native waits. They are
bounded at 100 ms and return `Not_ready` when Core is quiet, allowing a
supervisor mailbox to regain control and process shutdown instead of blocking
its reserved terminal message indefinitely. The OCaml wrappers keep the seam
private and return typed `result` values; they do not expose condition
variables, callbacks, Rust handles, or effect constructors.

Evidence:

- Rust unit tests cover notification before wait, concurrent queue publication
  and draining, shutdown wake, and persistent lane-error wakeups.
- Rust ABI tests and the C harness cover both wait symbols with null handles
  and no-worker lifecycle states, including normal result ownership cleanup.
- The OCaml bridge test exercises typed invalid-state results for both private
  waits and the existing two-Domain lock-release conformance probe.

## 2026-07-12: Private OCaml native workflow execution registry

Status: focused OCaml adapter and runtime tests pass locally; concrete native
supervisor wiring and the live Compose worker remain follow-up work.

`Temporal_runtime.Native_worker_execution` now provides a private functor over
typed workflow poll/complete operations. It registers heterogeneous executable
workflow definitions by name, keeps one existential `Execution.t` per Temporal
run ID, serializes calls with an OCaml mutex, applies validated activations in
deterministic order, and removes runs only after the supervisor confirms lease
retirement. Invalid initialization, unknown runs, malformed child
start/terminal resolution jobs, and codec failures become typed non-retryable
failure completions;
the adapter never fabricates missing Core fields or silently drops a lease. Its
constructor also validates the implicit activity queue (including empty, NUL,
oversized, and invalid UTF-8 values) before publishing any worker state, so a
configuration defect cannot fail a leased workflow at its first activation.

The functor intentionally does not depend on an unmerged readiness-wait API.
The future concrete `Sdk_supervisor.Native` instantiation can add wakeable
waiting without changing this execution registry. Malformed JSON is retired
by the lower typed supervisor protocol adapter before this functor sees an
activation, because only that layer still owns the raw lease token.

Evidence: `dune runtest --root . test/runtime` passes, including terminal
workflow, durable timer, cancellation, cache eviction, complete
activity-command translation, unknown-run, malformed source-error,
completion-exception cleanup, duplicate-registration, and remote-definition
tests, and task-queue configuration rejection. See the
[native worker execution reference](reference/native-worker-execution.md).

## 2026-07-12: Pure OCaml native execution translation

Status: focused native execution tests pass locally; the supervisor scheduling
loop and live Temporal worker remain follow-up work.

`Temporal_runtime.Native_execution` now translates the checked semantic
activation into the deterministic runtime's ordered jobs and translates its
ordered commands back into a checked completion. It reuses the protocol's
canonical validation for typed OCaml values, copies payload bytes at the
boundary, retains replay/initialization/cancellation/eviction metadata, and
reports malformed or duplicate sequences as typed bridge errors.

Commands are accepted only when the current runtime and protocol have an exact
lossless representation. Activity scheduling now carries Core's activity ID,
task queue, argument, timeout, cancellation, and eager-execution fields with
explicit validation and deterministic defaults. Child-workflow scheduling and
the two-stage start/terminal resolution lifecycle use the same strict semantic
protocol; the adapter never silently drops a command or fabricates an
undocumented default. See the
[translation reference](reference/native-execution-translation.md) for the
mapping and ownership rules.

## 2026-07-12: Typed supervisor worker operations

Historical snapshot: This milestone predates the wakeable readiness seam
documented above. The readiness follow-up described below records the state at
this commit; it does not mean that the current ABI lacks the readiness
operation.

Status: focused native-supervisor and protocol tests verified locally; a
wakeable readiness ABI and production worker loop remain follow-up work.

The private SDK supervisor now owns the complete OCaml-side worker poll and
completion boundary. `Try_poll_workflow` and `Try_poll_activity` run through
the same single-owner mailbox as lifecycle operations, strictly decode Rust's
semantic JSON into private protocol values, and represent an empty native lane
as `Ok None`. `Complete_workflow` and `Complete_activity` accept private typed
protocol values, canonically encode and reparse them, then submit copied bytes
through the existing C stubs. Protocol failures use the bridge's typed
`Protocol` status and diagnostics omit source JSON and payload bytes.

Decode failure after a successful native poll now has a one-shot rejection
path instead of leaking an inaccessible lease. OCaml returns the exact raw
document to Rust while preserving its original protocol error. Rust strictly
reparses it and requires the complete workflow activation or activity task to
equal retained handoff state before generating a failure for Core. Altered
identities or content are refused without consuming the real lease; repeated
activity cancellation documents sharing a token are retained without
overwriting one another. Ledger and semantic ownership are retired together so
worker shutdown cannot wait forever after decoder drift.

The pinned ABI does not yet provide a readiness event or wait symbol. This
slice therefore adds only the safe nonblocking try-poll seam and does not put a
timer, condition wait, or native blocking call in a workflow fiber. The owner
Domain continues to serialize native handle access; Rust/Tokio owns the two
poll lanes, and the existing C stubs release the OCaml runtime lock for every
native call. A later bridge slice must add a wakeable readiness wait before a
production worker loop can avoid bounded idle polling.

Focused tests cover canonical workflow/activity serialization, malformed
incoming and outgoing protocol values, `Not_ready` handling, worker-before-
start rejection, operation closure after shutdown, and the generic mailbox's
existing concurrent-producer and shutdown-race invariants.
Rust tests additionally cover exact-document correlation, changed run IDs and
activity tokens, changed same-identity content, duplicate poll preservation,
rejection cleanup, and shutdown drainage.

## 2026-07-12: Raw client start and exact-run wait adapter

Historical snapshot: This milestone predates the public client routing
milestone below. Its status and live-path follow-up describe what remained at
this commit, not the current public `Temporal.Client` implementation.

Status: Rust and OCaml protocol, ABI, formatting, and warnings-as-errors checks
pass locally; public client wiring and live Temporal integration remain
follow-up work.

The private Rust bridge now exposes strict JSON operations for starting a
workflow and waiting for one exact run. It uses Temporal Core's raw workflow
service trait, so dynamic OCaml workflow type names and payloads do not need a
Rust-side generated workflow registry. Start returns the server-assigned run
ID. Wait performs a close-event history long poll with a fixed
`follow_runs = false` policy for at most 100 ms per native call. An open run
returns `NOT_READY`, allowing the OCaml caller or a later orchestration loop to
regain the owner Domain and retry through the mailbox. Terminal
completed/failed/timed-out outcomes preserve successor metadata, while
continued-as-new is returned as a terminal result for the requested run rather
than silently switching to its successor.

Requests, responses, successor identities, terminal outcomes, and structured
AlreadyStarted/RPC failures use closed schemas under `docs/schemas/bridge/`.
Duplicate and unknown fields, identifier and payload limits, canonical payload
encoding, and output round trips are validated before bytes cross the ABI.
The ABI result owns diagnostics and reports AlreadyStarted distinctly while
discarding raw server status text that could contain user data.

The private OCaml codec now mirrors the same closed documents. It validates
identifiers before encoding, rejects NUL bytes on both directions, checks
successor namespace/workflow/run relationships, and accepts only the stable
RPC and Core conversion code vocabularies. The protocol test covers every
terminal outcome, malformed fields, duplicate members, structured errors, and
oversized or invalid request identifiers. The detailed message shapes and
ownership rules are in the [client protocol reference](reference/client-protocol.md).

Evidence:

- `cargo test --locked --all-targets` passes the complete Rust suite, including
  client protocol and C ABI tests for malformed JSON, exact-run semantics,
  successor retention, structured errors, null handles, lifecycle state, and
  owned result cleanup.
- `cargo clippy --locked --all-targets -- -D warnings` and `cargo fmt --all`
  pass locally.
- `dune build --root . @install` and `dune runtest --root .` pass locally on
  the representative host toolchain, including the new OCaml client protocol
  suite.
- The live Temporal Server path is intentionally not claimed here: it belongs
  to the Docker Compose acceptance test and will be wired after the OCaml
  supervisor can call these two operations.

## 2026-07-12: Public client routing through the native supervisor

Status: focused public-client and supervisor builds/tests pass locally; this
milestone does not claim a live Temporal Server worker acceptance run.

The public `Temporal.Client` now selects the deterministic `mock://` ledger only
for tests. HTTP(S) targets build a private supervisor graph, connect the Rust
Temporal Core client, and route typed start and exact-run wait requests through
the closed JSON protocol. Asynchronous start tickets are waited in bounded
steps, so the owner Domain can service lifecycle messages between attempts;
open exact runs use the same bounded `Not_ready` retry rather than blocking an
OCaml workflow scheduler. Protocol failures, duplicate workflow IDs, terminal
failure details, binary-safe payloads, and zero/multiple output payloads are
mapped to structured public `result` errors.

The supervisor now also exposes runtime-lock-free workflow/activity readiness
wait operations for the next worker slice. These operations remain private and
are serialized with all other native handles; no Rust thread calls an OCaml
closure. Internal mailbox and supervisor libraries use explicit internal OPAM
names so Dune can enforce the public dependency graph without exposing their
implementation modules in the `Temporal` API.

The public `Temporal.Client.start` surface now accepts an optional caller-owned
Temporal `request_id`. Applications can reuse that key when a start result is
uncertain, while omitted keys are generated once per logical call. The native
start request and every bounded ticket poll preserve the same key. Empty and
NUL-containing keys are rejected before the request reaches the backend.

Evidence:

- `dune build --root . test/unit/test_client_worker.exe` and its executable pass
  on the representative host toolchain.
- Unit coverage proves deterministic mock start/wait behavior, shutdown
  idempotence, malformed HTTP endpoint validation at the native boundary, and
  that public HTTP routing no longer returns the old "native adapter is not
  connected" path.
- At that time, the live two-binary Compose acceptance remained disabled until
  native worker polling, activity conversion, readiness signalling, and public
  dispatch were complete. The later live acceptance entry above records that
  initial success path as verified.

## 2026-07-12: Private OCaml/C poll and completion bindings

Status: focused C and OCaml boundary tests verified locally; the live worker
loop and native readiness wait remain separate follow-up work.

The OCaml bridge now wraps the Rust worker poll/completion ABI introduced by
the guarded Core poll lanes. The four operations are private and typed: two
non-blocking drains return semantic workflow or remote-activity JSON bytes, and
two completion functions accept semantic JSON bytes and return `unit`. Rust
status codes 9 through 11 are preserved as `Outstanding_tasks`, `Not_ready`,
and `Protocol` rather than being collapsed into a generic worker error.

The C stubs reuse the existing owned-response custom block, input-copy, and
runtime-lock release paths. Polls do not wait for Core, and completion input is
freed before returning. The OCaml wrapper always copies the Rust result before
deterministic `response_free`, with the custom-block finalizer retained as a
fallback. Focused tests exercise the new symbols before worker construction,
malformed completion handling, status conversion, and response cleanup.

This milestone does not claim that an OCaml worker can yet execute a live
activation. The next slices must add a native readiness wait, protocol records
on the OCaml side, and the per-run execution adapter before wiring these
operations into the supervisor.

## 2026-07-11: Direct-style workflow orchestration API

Status: full local OCaml build and test suite verified; live Temporal Core
translation and GitHub Actions verification follow this milestone.

The synthetic workflow runtime now supports explicit child-workflow starts,
non-blocking durable timers, wait-all aggregation, and deterministic
first-completed selection. Public workflow code remains ordinary direct-style
OCaml: private effects suspend only `Future.await`, while expected failures are
structured `result` values. Child IDs are supplied explicitly as durable
identity rather than invented from replay-local state.

Evidence:

- Focused tests first failed because the child module and activation variants
  did not exist, then passed for command emission, shared sequencing, typed
  input/output codecs, remote errors, and unknown or duplicate completion jobs.
- Scheduler tests cover ordered `all`, ready and pending `race`/`first`, error
  winners, retained losers, and typed cross-execution defects for every
  aggregator, including `both`.
- Timer tests cover multiple starts before waiting, zero-duration readiness,
  command order, and detached typed failure.
- A compile-checked unit fixture passes partially applied activity and child
  starters through ordinary higher-order helpers for homogeneous fan-out and
  heterogeneous racing.
- `dune build --root . @install`, the complete local OCaml and Rust test suites,
  Rust formatting and Clippy with warnings denied, the installed-package smoke
  test, repository formatting, OPAM lint, and diff checks passed. The host did
  not have the pinned one-shot scanner binaries, so GitHub Actions remains the
  scanner gate.

This evidence is for the synthetic interpreter. It does not claim that Core can
yet poll or complete these commands against a live Temporal Server.

## 2026-07-11: Official Core client and workflow-worker lifecycle

Status: native unit and boundary verification passed locally; the ignored live
lifecycle case and complete GitHub Actions verification follow this milestone.

One owner Domain now serializes the complete Rust runtime-client-worker graph.
The bridge uses the pinned official Core-based client, constructs a
workflow-only worker with explicit resource policy, and completes Core worker
validation before publishing it. Explicit and defensive shutdown both release
worker, client, then runtime, and repeated child or parent closure is safe.

Strict private JSON config documents are independently validated by OCaml and
Rust and described by closed schemas. Their 65,536-byte string limit is a
bridge transport safeguard, not a guessed Temporal Server identifier limit.
Connection and validation waits run in Rust while the C stub releases the
OCaml runtime lock. Polling and completion remain the next lifecycle slice.

Evidence:

- Rust lifecycle tests reject malformed config and invalid state, retry after
  connection failure without partial state, and verify repeated cleanup.
- The C ABI harness exercises worker-before-client rejection and idempotent
  child cleanup through the public header.
- OCaml bridge and supervisor tests exercise sender validation, typed lifecycle
  operations, and reverse terminal shutdown while retaining opaque handles.

## 2026-07-11: Bilateral workflow semantic protocol

Status: focused OCaml and Rust conformance and Core-conversion tests verified
locally; full native and GitHub Actions verification follows the milestone
commit.

The private boundary now has closed semantic JSON documents for the first
workflow activation/completion slice. Both languages implement typed payload,
time, initialization, activation metadata, activity resolution, failure,
eviction, activity/timer command, and terminal workflow command values. Rust
alone converts the official pinned Core protobuf types. Ordinary root and child
initialization, parent/root execution identity, priority, and top-level
activation metadata are preserved; unrepresented
non-default Core fields fail with a typed conversion error.

Evidence:

- Shared fixtures normalize identically in Rust and OCaml for all supported
  jobs and commands, eviction, failures, maximum unsigned randomness seeds, and
  realistic first-task initialization.
- A deliberately reversed payload/header metadata fixture proves canonical
  lexicographic map ordering in both implementations.
- Malformed fixtures prove duplicate/unknown/missing fields, numeric bounds,
  canonical base64, duration, activity timeout, terminal ordering, and eviction
  invariants fail closed.
- Bilateral regressions prove official Core eviction keeps its absent
  timestamp, initialization is unique and first, sequence zero remains valid,
  identifier validation does not invent a 255-byte server limit, structured
  failure fields remain schema-exact, and invalid header keys fail closed.
- Two 2 MiB opaque byte fields round-trip together in both implementations.
  Arithmetic tests verify the 128 MiB per-field and 192 MiB aggregate document
  safety ceilings without allocating either maximum in every CI matrix cell.
- Activations containing 300 small jobs prove collection accounting no longer
  imposes the former 256-item policy. Recursive failure tests accept 32 causes
  while rejecting input beyond the shared 128-level parser stack-safety bound.
- Required-nullable regressions cover activation timestamps, initialization
  context, metadata, activity results, recursive failures, schedule-activity
  timeouts, and workflow results so omission cannot be accepted as null.
- Rust tests convert realistic official Core root and child activations and semantic
  completions without loss, and reject unsupported fields, absent oneofs, and
  invalid eviction acknowledgements.
- Four Draft 2020-12 schemas document the closed activation, completion,
  payload, and recursive failure contracts; runtime validators remain
  authoritative for byte limits and duplicate-key evidence.
- Direct `temporalio-protos` and `prost-wkt-types` declarations reuse the
  already locked permissive Core dependency graph and add no package.

## 2026-07-11: One-shot quality and security scans

Status: focused contract and scanner checks verified locally; complete GitHub
Actions verification follows the milestone commit.

The repository now has a separate quality job for Cargo advisories and source
provenance, unused direct Rust dependencies, and cross-language spelling. Exact
tool versions are enforced locally and installed in CI from checksum-verified
release artifacts through an immutable action commit. The job is independent
of both the OCaml compiler matrix and the standalone dependency-license audit.

Evidence:

- The repository contract test first failed because `make quality` was absent,
  then passed after the Make targets, pinned workflow job, and Cargo source
  policy were added.
- `make quality` passed cargo-deny 0.20.2 advisory/source checks,
  cargo-machete 0.9.2 unused-dependency analysis, and typos 1.48.0.
- Cargo-deny does not fail on unmaintained transitive crates owned by the
  pinned Temporal Core graph; vulnerabilities and unapproved sources remain
  errors.
- No OCaml-specific dependency was added: maintained alternatives either
  duplicate compiler/Dune diagnostics or bring a prohibited copyleft tool
  closure. The language-neutral spelling scan covers OCaml source and docs.

## 2026-07-11: Strict JSON control-protocol foundation

Status: focused OCaml and Rust conformance tests verified locally; complete
native and GitHub Actions verification follows the milestone commit.

The private boundary now has a closed request/response/error envelope, a
once-per-runtime compatibility number, bounded strict JSON parsing, structured
privacy-safe errors, normalized output, and canonical padded base64 wrappers
for opaque payload bytes. Both implementations reject duplicate members before
converting objects into lookup structures. Future worker operations will add
closed body validators without exposing Temporal/Core protobuf to OCaml.

Evidence:

- Shared positive and malformed fixtures drive both language suites.
- Each suite passes five conformance groups covering normalized envelopes,
  missing/unknown/duplicate/wrong fields, correlation identifiers, fractional
  numbers, base64, oversized/deep input, compatibility, and outgoing
  self-validation.
- Draft 2020-12 schemas and the contributor reference document the tooling
  contract and the properties that schemas cannot enforce.
- Direct Rust serde, serde_json, and base64 declarations reuse already locked
  permissive packages and do not expand the dependency closure.

## 2026-07-11: Application-configurable OCaml logging

Status: verified locally; GitHub Actions verification follows the milestone
commit.

The SDK now emits bounded, structural events through the OCaml `logs` library
at lifecycle, native-bridge, workflow-state, and latency boundaries. Stable
sources and tags let applications filter without parsing message prose. The
library deliberately installs neither a reporter nor a global level, so the
OCaml application continues to own output format, destination, and verbosity.
Raw workflow payloads, arguments, and native diagnostics are excluded, and a
defective application reporter cannot change SDK result semantics.

Evidence:

- Focused tests first failed because the observability module did not exist,
  then passed for stable source and tag names, severity, latency, privacy, and
  reporter-exception containment after the implementation was added.
- A focused boundary test then failed for negative and non-finite metadata and
  passed after the common tag constructor normalized invalid durations and
  counts to zero.
- A reporter re-entry regression first produced an extra workflow command,
  then passed after runtime reports began masking the Domain-local workflow
  context around application callbacks.
- Repository smoke tests first failed because `logs` was absent from package
  metadata, then passed after Dune, OPAM, and the locked dependency closure
  declared it.
- License-policy fixtures first rejected the newly exposed `ocamlbuild`
  dependency, then passed after documenting and enforcing an exact build-only
  `0.16.1` OCaml linking-exception allowance. Adjacent versions remain rejected.

## 2026-07-11: Portable static foreign-archive build

Status: verified locally; GitHub Actions verification follows the corrective
commit.

The build now compiles the C binding into a static foreign archive and keeps it
separate from Rust's native system-library flags until the final executable is
linked. The workspace disables dynamically linked foreign archives, and the
internal bridge library disables OCaml native plugins with `no_dynlink`. The
SDK's supported artifact has always been an OCaml-owned native executable with
the Rust bridge linked into it, so constructing a separate loadable stub DLL
was unnecessary. On Windows, Dune's `foreign_stubs` path sent Rust's GNU native
library tokens through FlexDLL while creating that temporary DLL. FlexDLL
interpreted the tokens as filenames and rejected `-lwinapi_ntdll` after Rust
itself had compiled successfully.

The final Windows executable needs one additional piece of information that
`rustc --print=native-static-libs` does not include: Cargo's `winapi` package
ships its own MinGW import archives and exposes their directory through a
build-script link-search instruction. The bridge build now validates every
reported `-lwinapi_*` archive and carries that exact directory into Dune as a
quoted `-L` flag. It does not guess a Cargo registry location or duplicate the
archives in this repository.

Evidence:

- The repository regression test requires the static workspace policy, a
  dedicated `foreign_library`, and `no_dynlink`; it rejects reintroducing
  `foreign_stubs` at this boundary because that would recreate a temporary
  Windows DLL link.
- A platform-independent shell regression test constructs a fake Cargo build
  output and verifies that Windows receives the validated search directory,
  paths are encoded as a single Dune S-expression atom, and other platforms
  retain Rust's exact native-library sequence.
- The complete local native verification passes the Dune build and lint,
  Clippy with warnings denied, all Rust and OCaml tests, and a fresh installed-
  package consumer executable.
- macOS ARM64 and every Linux OCaml 5.2 through 5.5 amd64/arm64 job in the
  preceding runtime-ownership run passed; its Windows x64 job supplied the
  exact failing FlexDLL command addressed by this change.

## 2026-07-11: Plain-language documentation and maintained JSON codec

Status: verified locally and by GitHub Actions.

The repository documentation now begins with a guide and glossary, clearly
separates implemented behavior from target architecture, and explains public
APIs in terms of what callers provide and receive. Source comments cover the
public API, internal workflow runtime, OCaml/C/Rust ownership boundary, and
test helpers with emphasis on behavior and safety rather than restating code.

The optional `json/plain` string codec now uses Yojson 3.0.0 instead of a
project-owned JSON parser. Temporal still treats payloads as opaque bytes and
does not require JSON; the codec remains because it provides useful
cross-language interoperability. The locked license inventory records Yojson's
BSD-3-Clause license.

Evidence:

- Package smoke tests first failed because Yojson was not declared, then passed
  after Dune, OPAM, and the locked closure included it.
- Codec tests pass for escaping, Unicode surrogate pairs, invalid UTF-8,
  non-string JSON, trailing input, binary copies, and optional values.
- `make native-verify NATIVE_OCAML_VERSION=5.4 NATIVE_RUST_VERSION=1.96.0`
  passed the local OCaml/Rust build, Clippy, Rust tests, OCaml tests, and install
  smoke test.
- Docker-backed `make test-unit OCAML_VERSION=5.2`, `make test-runtime
  OCAML_VERSION=5.2`, and `make license-check OCAML_VERSION=5.2` passed.
- Both OPAM manifests pass `opam lint`, and `git diff --check` passes.

Next objective: the live Temporal/PostgreSQL Compose acceptance topology with
separate OCaml test-client and workflow/activity worker executables.

## 2026-07-11: Native Temporal Core runtime ownership

Status: verified locally; GitHub Actions verification follows the milestone
commit.

The OCaml-owned executable can now create and close a real Temporal Core/Tokio
runtime through the statically linked Rust bridge. The runtime remains an
abstract private OCaml value. Explicit shutdown waits for complete destruction
while the OCaml runtime lock is released; the garbage-collector fallback uses a
C-only borrow barrier and transfers destruction to a dedicated Rust cleanup
thread without calling OCaml runtime lock operations.

The C stub atomically detaches the sole native pointer, making explicit close,
repeated close, and finalization safe against one another. Blocking Rust calls
write only into C-stack storage while the OCaml runtime lock is released and
copy the completed result into a rooted custom block afterward.

The Core activation/completion adapter will use strict JSON rather than expose
Core protobuf to OCaml. The accepted design requires independent outgoing and
incoming validation in both languages, closed JSON Schema Draft 2020-12
schemas, duplicate-key rejection, bounded allocation, semantic validation, and
shared positive and malformed fixtures.

Evidence:

- The initial Rust ABI test failed because runtime lifecycle functions did not
  exist; the initial OCaml bridge test failed because `runtime_create` was not
  bound.
- Rust ABI tests pass runtime creation, explicit idempotent close, null-pointer
  rejection, and asynchronous pointer detachment.
- A separate Rust integration-test process waits until the asynchronously
  disposed Core destructor completes, preventing another parallel test from
  producing a false positive.
- The C11 ABI harness creates, explicitly closes, and asynchronously disposes
  real Core runtimes through the public header.
- The OCaml bridge suite creates and repeatedly closes the native runtime.
- `make native-verify NATIVE_OCAML_VERSION=5.4 NATIVE_RUST_VERSION=1.96.0
  NATIVE_ARCH=arm64 NATIVE_RUST_HOST=aarch64-apple-darwin` passed the complete
  local build, Clippy, Rust tests, OCaml tests, and install smoke test.

Next objective: implement the strict, bilaterally validated JSON activation and
completion adapter before connecting a worker to the Compose acceptance stack.

## 2026-07-11: Repository foundation

Status: verified.

The repository now has an Apache-2.0 package definition, a parameterized OCaml
5.2 through 5.5 development image, Docker Compose command runner, Dune
metadata, and a Makefile-first command contract.

Evidence:

- `make build` completed with Dune 3.24.0 in the compatibility image.
- `docker compose run --rm dev opam exec -- dune runtest test/smoke`
  passed 1 test with 0 failures.
- `make verify` completed successfully.
- `docker compose run --rm dev ocamlc -version` reported OCaml 5.2.1.
- `git diff --check` reported no whitespace errors.

The initial formatter experiment was removed during the dependency audit:
although `ocamlformat` itself is MIT licensed, its build closure contains GPL
tools. Repository-owned whitespace checks provide the current formatting gate
without adding prohibited dependencies.

## 2026-07-11: Executable dependency policy

Status: verified.

The locked project closure is intentionally small: OCaml 5.2.1, Dune 3.24.0,
compiler selection packages, and compiler virtual packages. `make
license-check` rejects missing, unknown, or prohibited licenses. It is kept
separate from the compiler build/test matrix; `make check` runs both locally.

Evidence:

- The policy fixture rejected GPL-3.0-only, missing metadata, an unreviewed
  OCaml linking exception, and a mixed MIT/GPL declaration.
- The same fixture accepted MIT.
- `make license-check` accepted every exact package in
  `temporal-sdk.opam.locked` and printed its decision.
- `make verify` completed the build, formatting gate, and test suite, and the
  separate `make license-check` audit completed successfully.
- `git diff --check` reported no whitespace errors.

Next phase: typed codecs and structured errors.

## 2026-07-11: Typed codecs and structured errors

Status: verified.

The first installable `temporal-sdk` library now provides typed payload codecs,
UTF-8 JSON string handling, byte and null encodings, abstract structured
errors, stable error views, and `result` binding syntax. Internal constructors
live in the explicitly unstable `temporal-sdk.internal_base` library.

Evidence:

- The initial focused test failed because the `temporal-sdk` library was absent.
- `make test-unit` passed codec, error, and repository tests on OCaml 5.2.1.
- Codec tests cover escaping, surrogate-pair decoding, invalid UTF-8, copied
  byte storage, encoding mismatch, and `None`/`Some` payload behavior.
- `make lint` and `make license-check` passed.
- The same unit and smoke suite passed with `OCAML_VERSION=5.5`.

Next phase: typed workflow and activity definitions.

## 2026-07-11: Typed workflow and activity definitions

Status: verified.

Local and remote workflows and activities now share an internal typed
definition representation while exposing separate abstract public types.
Definitions retain their input/output codecs and optional implementation;
public callers can inspect only the stable Temporal name.

Evidence:

- The initial focused test failed with unbound `Temporal.Activity` and
  `Temporal.Workflow` modules.
- `make verify` passed the full build, policy, and test gates on OCaml 5.2.1.
- The unit and smoke suites passed on the OCaml 5.5 Compose image.
- `dune build @install` and `opam lint temporal-sdk.opam` passed.
- Name tests cover local/remote definitions and reject empty or NUL-containing
  names during configuration.

Next phase: deterministic futures and effect scheduler.

## 2026-07-11: Deterministic futures and effect scheduler

Status: verified.

The runtime now has typed promises, a private OCaml 5 deep effect for
suspension, and a deterministic FIFO runnable queue. Public futures expose
`await`, `map`, `map_error`, `both`, `is_ready`, and `peek` without exposing
effect constructors or continuation values.

Scheduler invariants:

- Every scheduler and queued runnable receives a monotonic identity.
- Resolution is single-assignment; a second resolution raises
  `Invalid_argument` at the internal defect boundary.
- Waiters resume in registration order and resolution jobs enqueue in the
  caller-provided order.
- A continuation is captured only while its owning scheduler is running.
- `both` settles after both siblings and selects the left error first when both
  fail.
- Callback exceptions become scheduler failures rather than escaping the run
  loop.
- Shutdown discontinues captured continuations and drops queued work.

Evidence:

- The initial runtime test failed because `temporal-sdk.runtime` did not exist.
- `make test-runtime`, `make test-unit`, `make lint`, and `make license-check`
  passed on OCaml 5.2.1.
- The runtime, unit, and smoke suites passed on OCaml 5.5. The current compiler
  gate caught and removed one newly reserved identifier before commit.
- Tests cover FIFO resolution order, immediate waits, multiple waiters,
  double-resolution rejection, owner mismatch, mapping, mapped errors, pairing,
  sibling settlement after failure, callback defects, and shutdown disposal.
- A source scan found no `Obj.magic` or other `Obj` representation casts.
- `dune build @install` and `git diff --check` passed.

Next phase: synthetic activation interpreter and command API.

## 2026-07-11: Synthetic activation interpreter and command API

Status: verified.

The first end-to-end runtime slice schedules typed activities, decodes their
results, starts durable timers, resumes suspended OCaml code, and emits encoded
workflow completion commands. A domain-local context makes public operations
available only during activation execution. This is a synthetic proof and
does not yet poll Temporal Server.

Evidence:

- The initial focused test failed because the activation and execution modules
  did not exist.
- The schedule/activity-resolution/timer/completion sequence passed on OCaml
  5.2.1 and 5.5.
- Replaying identical job lists produced structurally identical payload bytes
  and command lists.
- Concurrent activity resolution tests proved that runnable order follows the
  explicit activation job order.
- Tests reject unknown and duplicate sequences as bridge defects, validate
  zero/negative durations, emit cancellation exactly once, and evict blocked
  executions without a command or leaked continuation warning.
- Terminal completion and failure tear down pending runtime state while
  retaining the terminal command.
- Full unit/runtime tests, lint, license audit, install build, OPAM lint,
  unsafe-cast scan, and `git diff --check` passed.

Next phase: Phase 1 documentation and clean-checkout handoff.

## 2026-07-11: Phase 1 deterministic runtime handoff

Historical snapshot: this handoff predates the native bridge and the typed
interaction slice documented above. Its known limitations describe the
repository at this commit and are not the current feature status.

Status: verified.

Phase 1 establishes the typed public kernel, effect scheduler, and synthetic
activation proof needed before binding to Temporal Core. Milestone commits are:

| Task | Commit | Outcome |
|---|---|---|
| Architecture | `5e80c6a` | Approved OCaml-over-Core design |
| Plan | `6d6d8b8` | Foundation/runtime implementation plan |
| 1 | `855d6b2` | Docker, Make, Dune, and package foundation |
| 2 | `174ad92` | Executable dependency-license gate |
| Repository metadata | `d1f84af` | GitHub location and `master` publication |
| 3 | `5c70b93` | Typed codecs and structured errors |
| 4 | `f4a49eb` | Typed workflows and activities |
| 5 | `fc352d1` | Deterministic effect scheduler |
| 6 | `a0e157d` | Synthetic activation interpreter |

The clean matrix executed from the repository root on 2026-07-11:

```sh
make clean
make build
make test-unit
make test-runtime
make license-check
make lint
make verify
docker compose run --rm dev opam exec -- ocamlc -version
git diff --check
```

Every command exited zero, and the compatibility image reported OCaml 5.2.1.
The complete runtime/unit/smoke suite also passed on OCaml 5.5.0 during the Task
6 compatibility gate. OPAM lint, the install target, and an explicit unsafe
`Obj` cast scan passed before handoff.

Known limitations:

- There is no live Temporal Core or Server connection yet.
- Compose does not yet include Temporal Server, PostgreSQL, UI, or a
  cross-language activity worker; those arrive with the first real bridge
  vertical slice.
- The synthetic protocol currently covers activities, timers, cancellation,
  completion, failure, and eviction only.
- Child workflows, structured cancellation, signals, queries, updates,
  continue-as-new, versioning, local activities, Nexus, replay-safe side
  effects, and the remaining parity surface are still planned.
- The current formatting gate checks repository whitespace because the
  formatter closure violated the all-dependencies license policy.

Next objective: pin and audit the Rust/Cargo closure, link the project-owned
Core static bridge into an OCaml-built worker, and run the same direct-style
workflow against Temporal Server and PostgreSQL in Docker Compose.

## 2026-07-11: Cross-version and cross-architecture CI

Status: verified.

GitHub Actions now runs every supported OCaml minor release from 5.2 through
5.5 on native amd64 and arm64 GitHub-hosted runners. The dependency-license
audit is one independent job rather than repeated for each compiler and
architecture. Compose commands run with the checkout owner's UID/GID, avoiding
host/container bind-mount ownership failures, and `version-check` proves each
matrix cell built the requested compiler image.

Evidence:

- The official OPAM images for OCaml 5.2, 5.3, 5.4, and 5.5 advertise both
  `amd64` and `arm64` manifests.
- Local `make verify OCAML_VERSION=<version>` passed for all four versions.
- Local `make license-check OCAML_VERSION=5.2` passed independently.
- [GitHub Actions run 29139710646](https://github.com/mfow/ocaml-temporal/actions/runs/29139710646)
  completed all eight compiler/architecture cells and the license job
  successfully.
- [GitHub Actions run 29139792049](https://github.com/mfow/ocaml-temporal/actions/runs/29139792049)
  repeated all nine jobs successfully after updating to the current official
  `actions/checkout` major version.

End-to-end Temporal/PostgreSQL Compose tests will be a separate Phase 2 job.
Their architecture matrix will be enabled only after every runtime image is
verified to publish the corresponding native manifest.

## 2026-07-11: Pinned Rust and Temporal Core build foundation

Status: verified locally and on the native CI matrix.

The development image now copies Rust 1.94.1 from a digest-pinned official
multi-architecture image and installs only Core's protobuf build tools. The
Apache-2.0 project bridge builds as a 21 MiB native static archive while the
final process architecture remains OCaml-owned. Temporal Core is a direct
Cargo dependency pinned to immutable commit
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`, with defaults disabled and the
`tls-ring` feature selected.

Local evidence:

- The toolchain smoke test first failed with no `rustc`, then passed with the
  pinned Rust 1.94.1 compiler, locked Cargo graph, and non-empty static archive.
- `cargo metadata --locked --offline` resolved 320 packages including the
  project bridge, and the fail-closed SPDX policy accepted the complete graph.
- Policy fixtures accepted compound permissive expressions and rejected GPL,
  LGPL, AGPL, MPL, missing, unknown, and malformed license metadata.
- The production Rust source is separate from its integration test under
  `rust/core-bridge/tests/`; the revision test passes.
- Action workflow lint, Rust format checking, repository formatting, Python
  syntax checking, and `git diff --check` pass.

The first native run exposed that the official Rust image does not preinstall
the optional Clippy and rustfmt components. The toolchain stage now installs
both explicitly, and the smoke test requires both commands before compiling
the archive.

The following run reached the separated Rust integration test and exposed that
a crate configured to emit only a `staticlib` cannot be imported by that test.
The bridge now also emits Rust's internal `rlib` artifact for integration tests;
the `staticlib` remains the artifact linked into the OCaml-owned executable.

GitHub Actions run 29140893276 then passed the standalone license audit and all
eight native build, lint, and test jobs for OCaml 5.2 through 5.5 on amd64 and
arm64. Cargo-only Dependabot updates are configured weekly against `master`;
OCaml and OPAM remain intentionally outside Dependabot.

The Cargo scanner is intentionally absent from the Makefile. The single
standalone GitHub Actions license job streams locked metadata from the build
container to a network-disabled, read-only, digest-pinned official Python
container. Every OCaml/compiler architecture cell runs the Rust build, Clippy,
and Rust tests through `make verify`; GitHub Actions is the compatibility gate
for OCaml 5.3 through 5.5 and native amd64/arm64.

## 2026-07-11: Versioned native ABI foundation

Status: verified.

The Rust bridge now exports a version-2 C ABI with explicitly numbered status
codes and one documented `repr(C)` result shape. Success and error bytes are
Rust-owned, empty buffers have the canonical null/zero representation, and one
idempotent disposal function clears both allocations. The C header reserves
opaque runtime, internal connection-client, and worker handles without exposing
Rust layouts.

Every fallible exported operation is panic-contained. A hidden Rust-only probe
deliberately panics through the shared wrapper and verifies that the caller
receives `STATUS_PANIC` plus an owned diagnostic instead of an unwind crossing
C. The public header contains no test-panic symbol.

Local evidence:

- The ABI integration test first failed because none of the new symbols or
  types existed, then all six ownership, negotiation, pointer, disposal, and
  panic-containment tests passed.
- Clippy with warnings denied, rustfmt checking, the complete locked Rust test
  suite, repository formatting, and `git diff --check` pass.
- A strict C11 harness compiles against the canonical header, links the actual
  Rust static archive, and passes under AddressSanitizer and
  UndefinedBehaviorSanitizer.
- [GitHub Actions run 29141377953](https://github.com/mfow/ocaml-temporal/actions/runs/29141377953)
  passed the standalone license audit and all eight OCaml/compiler and native
  architecture jobs.

This is an OCaml Temporal SDK, not only a Temporal service client. The future
Core client handle is an internal connection component; worker polling,
deterministic workflow execution, replay, and workflow command production are
first-class SDK responsibilities alongside start/result client operations.

The worker-versioning contract subsequently bumped this bridge to ABI v2. The
v2 header, Rust symbols, OCaml negotiation constant, and bilateral tests now
reject both the previous ABI and future unsupported numbers before any worker
JSON is accepted.

## 2026-07-11: OCaml-owned native static link

Status: verified locally and across the complete Linux and native desktop CI
matrix.

The public OCaml package now links the project Rust bridge through private C
stubs. `Temporal.Runtime_info.native_bridge_abi_version` negotiates ABI v2 from
an OCaml-built executable, while binary echo and bounded-wait conformance
operations exercise owned buffers and blocking calls in the internal test
surface.

The C boundary prioritizes leak safety. It allocates a finalizable OCaml custom
owner before calling Rust, deterministically disposes it through `Fun.protect`,
and retains the finalizer as a fallback for allocation failures and exceptions.
Returned bytes are copied once into OCaml before Rust frees them. Input bytes
are copied before releasing the OCaml runtime lock, so the unlocked stub never
inspects an OCaml heap value.

The staged package contains the native archive and compiled private stubs but
does not install the C header or Rust source. A fresh `ocamlfind ocamlopt`
consumer links only the installed `temporal-sdk` package and successfully calls
the Rust ABI. The stateful worker implementation will use one OCaml supervisor
actor per SDK instance to own the runtime/client/worker handle graph; it will
not create an actor for every individual handle.

Local evidence:

- The focused test first failed because `temporal-sdk.internal_core_bridge` did
  not exist, then passed through the real Rust archive.
- A second OCaml Domain progressed while the first waited in Rust, exercising
  the runtime-lock release/reacquire path.
- Rust ABI tests cover the bounded wait and all existing ownership and panic
  cases.
- `dune build @install` stages the native archive, compiled stubs, and public
  `Temporal.Runtime_info` API without the Rust source or C header.
- The install smoke test builds and runs a new native OCaml executable against
  that staged package.
- [GitHub Actions run 29142248581](https://github.com/mfow/ocaml-temporal/actions/runs/29142248581)
  passed the standalone dependency audit and all eight Linux OCaml 5.2 through
  5.5 amd64/arm64 jobs with the linked Rust bridge.

Native desktop evidence:

- Windows x64 builds the OCaml-owned executable with OCaml 5.5 and the pinned
  GNU Rust toolchain, then passes the Rust ABI tests, Clippy, rustfmt, OCaml
  tests, and the fresh installed-package consumer.
- macOS ARM64 performs the same native verification with OCaml 5.5 and the
  pinned Apple ARM Rust toolchain.
- [GitHub Actions run 29143621807](https://github.com/mfow/ocaml-temporal/actions/runs/29143621807)
  passed both native desktop jobs, all eight Linux OCaml 5.2 through 5.5
  amd64/arm64 jobs, and the standalone dependency audit.

The native jobs deliberately avoid Docker and validate the actual host
compiler, architecture, Rust target, OCaml tests, Rust tests, Clippy, rustfmt,
and fresh installed-package consumer.

## 2026-07-11: Private owner-Domain mailbox processor

Status: verified locally; cross-platform pull-request evidence pending.

A new Dune-private library provides the typed, bounded FIFO processor needed by
the future runtime/client/worker supervisor. A functor accepts a GADT request
language, `post` admits unit requests, and `call` preserves each request's
result type through an existential job and typed one-shot reply. The library
has no `public_name`, adds no dependency, and exposes no Eio, Temporal, Rust,
mutex, condition, continuation, or owner-Domain type.

One spawned Domain invokes the rank-2 handler sequentially. A mutex-protected
bounded queue establishes admission order and real producer backpressure.
Normal close rejects new work and drains admitted work. An unexpected handler
exception is contained, reported to the active call and join, and propagated to
all queued calls while queued posts are deterministically discarded. Blocking
operations are explicitly excluded from cooperative scheduler Domains; a
future adapter must offload them to a blocking bridge.

The handler contract also forbids re-entering `post`, `call`, or `join` on the
same processor because the sole owner cannot make the progress those operations
may require. Handler-initiated `close` remains safe and preserves orderly
draining after the handler returns.

Local evidence:

- The focused suite first failed because `temporal_mailbox_processor` did not
  exist, then passed FIFO, typed-call, eight-Domain exactly-once delivery and
  per-producer order, bounded backpressure, close/drain/rejection, terminal
  handler failure, queued-waiter release, and clean join scenarios.
- One hundred forced focused repetitions passed, including capacity waiters
  woken by both close and terminal handler failure.
- `make native-verify` passed on the locally available OCaml 5.4.1 and Rust
  1.96.0 toolchains, covering the complete OCaml suite, native install build,
  fresh installed-package consumer, Rust tests, rustfmt, and Clippy with
  warnings denied.
- Repository and install checks enforce the lack of `public_name` and reject
  mailbox artifacts in the staged `temporal-sdk` package.
- The design and synchronization evidence are recorded in
  [ADR 0003](decisions/0003-private-mailbox-processor.md).

## 2026-07-11: SDK instance owner-Domain supervisor

Status: representative native verification passed; complete cross-platform
pull-request evidence pending.

A new Dune-private supervisor layer now turns the generic mailbox into one
owner for an SDK instance's complete native resource graph. Backend creation,
typed use, and shutdown all execute on the same dedicated Domain. Backend state
does not appear in the operation API, so Rust runtime, future client, and future
worker handles cannot escape to producers.

The production specialization owns the real Rust runtime and supports an ABI
compatibility operation. This proves actual native creation/use/destruction
without claiming that live Temporal client or worker operations exist yet.
Expected operation errors preserve a running graph. Unexpected exceptions
record terminal state, attempt cleanup once, and use the mailbox failure path
to release active and queued callers with the same defect.

Focused evidence:

- The first test failed because `temporal_sdk_supervisor` did not exist.
- Creation, typed operations, and shutdown ran on one non-producer owner
  Domain; twelve concurrent producers never overlapped backend use.
- Expected create, operation, and shutdown errors retained explicit `result`
  values and exact idempotent shutdown outcomes.
- Sixteen concurrent shutdown callers shared one cached result while backend
  release ran once. Unexpected create/shutdown exceptions were contained, and
  the shutdown exception was cached for concurrent and later callers.
- Unexpected operation failure released callers contending during the defect
  and closed exactly once; the underlying mailbox suite separately proves
  admitted-queue failure propagation.
- Abandoning a live instance delegated its normal cleanup to a system thread;
  the garbage-collector finalizer did not block and backend shutdown ran once.
- Twenty-five forced repetitions of the complete supervisor suite passed.
- The real Rust runtime passed create, compatibility use, shutdown, and
  repeated-shutdown checks through the supervisor.
- An ARM CI scheduling failure exposed that the original saturation test used
  a fixed CPU-relax loop as a proxy for another Domain reaching the mailbox
  mutex. The mailbox now returns a typed pending reply after the terminal
  request and close transition linearize. Deterministic tests prove both the
  reserved terminal slot in a full FIFO and synchronous SDK admission closure
  while backend work remains blocked; sixteen concurrent public shutdown
  callers then share the cached terminal outcome and one backend close.
- One thousand forced repetitions of the mailbox and supervisor suites passed
  with the deterministic shutdown boundary.
- `make native-verify` passed on OCaml 5.4.1 and Rust 1.96.0, covering the
  install build, complete OCaml suite, Rust tests, Clippy with warnings denied,
  rustfmt, and the fresh installed-package consumer.
- [ADR 0004](decisions/0004-sdk-instance-supervisor.md) records ownership,
  cleanup, blocking, and future client/worker extension rules.

This milestone does not connect to Temporal Server, create a Core client or
worker, or poll activations. Those remain the next Phase 2 bridge tasks.

## 2026-07-11: Real Temporal Server and PostgreSQL Compose substrate

Status: verified locally; OCaml workflow connectivity remains pending.

The opt-in `temporal` Compose profile now runs the supported official Temporal
Server 1.31.0 image against PostgreSQL 16.13. A separate official admin-tools
container initializes the primary and visibility schemas. Exact OCI manifest
digests are pinned, and all selected indexes publish native Linux amd64 and
arm64 images.

The Make interface provides start, health, status, diagnostics, stop, clean,
and clean-volume integration-smoke targets. Health validation goes beyond a
port probe: it queries both Temporal schema-version tables, invokes the
frontend's gRPC cluster-health API, and verifies the test namespace.

GitHub Actions now runs that live smoke once in a standalone Ubuntu job with
`OCAML_VERSION=5.5`. It is deliberately outside the OCaml version and CPU
architecture matrix so every change proves the real server/database path
without starting eight equivalent clusters. The same lane will execute the
OCaml client and worker when those containers are implemented.

Local evidence:

- The configuration smoke first failed against the development-only Compose
  file, then passed against Compose's normalized model with the exact images,
  dependency conditions, health checks, named volume, and Make targets.
- `make test-temporal-integration` pulled the pinned ARM64 images, initialized
  empty PostgreSQL storage, observed both containers as healthy, received
  `SERVING` from `temporal operator cluster health`, registered and described
  `temporal-sdk-test`, and removed all test containers and data.
- Inspection inside the pinned server container found `nc` at `/usr/bin/nc`
  and the exact configured `postgres12`, port, seed host, user, and dynamic
  configuration path in its environment. Startup then reported `go-arch` as
  `arm64`, loaded both file-based dynamic settings, created the PostgreSQL
  `temporal_visibility` manager, and opened the frontend listener on port 7233.
- A separate `temporal-start`, `temporal-stop`, `temporal-start` sequence
  reused the retained PostgreSQL volume successfully. On the second start the
  schema job detected primary schema version 1.19 and visibility version 1.14,
  skipped the older 0.0 setup, found zero updates for both databases, and the
  repeated cluster-health and namespace checks passed. This verifies
  idempotent schema update and namespace handling against the pinned images.
- The stack exposes only Temporal's gRPC frontend; PostgreSQL remains private
  to the Compose network.

This milestone is the substrate for the later separate OCaml test-client,
workflow worker, and mock-activity containers. No live OCaml workflow path is
claimed yet. Operational details and Kubernetes correspondence are documented
in the [local stack reference](reference/local-temporal-stack.md) and
[ADR 0005](decisions/0005-temporal-postgres-compose-stack.md).

## 2026-07-12: Activity retry-policy command boundary

Status: focused OCaml and Rust policy, protocol, and Core-conversion tests pass
locally; live Temporal retry/failure scenarios remain deferred.

The public activity API now exposes an immutable
`Temporal.Activity.Retry_policy.t` constructor and accepts it through both
`Activity.start` and `Activity.execute`. Construction validates exact
millisecond intervals, a finite backoff coefficient at least 1.0, a signed
32-bit attempt count (`0` means unlimited), and non-retryable error type
names. Invalid configuration returns a typed defect instead of using exceptions
as control flow.

The private workflow completion protocol carries the coefficient as canonical
unsigned decimal IEEE-754 bits rather than a JSON float. Both OCaml and Rust
validate the closed retry object on decode and encode; `None` is serialized as
the required JSON `null`, while an explicit policy remains distinguishable.
Rust converts the validated representation to and from Temporal Core's retry
policy without changing coefficient bits. The JSON Schema, runtime invariants,
translation reference, and ADR document the ownership and replay rules.

Evidence: `dune runtest test/bridge/`, the focused OCaml unit/runtime retry
policy executables, and `cargo test -p ocaml-temporal-core-bridge --test
workflow_retry_policy` pass on the representative host. Existing workflow
protocol Rust tests also pass after the required nullable field was added.

## 2026-07-13: Fail-closed activity completion retry boundary

Status: focused OCaml and Rust bridge/policy tests are verified locally;
GitHub Actions results are not used as a blocker while the hosted queue is
quota-limited.

The worker loop now has a distinct retry-pending callback and a bounded native
backoff operation. A retained activity completion cannot spin merely because
an unrelated activity is ready: the supervisor owner Domain applies a fixed
10 ms delay while the C stub releases the OCaml runtime lock. The scheduler
tests distinguish that callback from ordinary readiness and keep permanent
protocol failures fatal.

The production adapter no longer treats generic `Connection` or `Not_ready`
statuses as safe completion retries. The pinned Temporal Core completion API
consumes/removes the activity task before internally logging and suppressing
network errors, so the bridge cannot prove that a second submission would be
safe. A reserved bilateral `Retryable` status is mapped through Rust, C, and
OCaml for a future Core-aware completion path that can prove the lease remains
pending; until then the OCaml policy fails closed. Shutdown reopens the worker
only for an explicitly retryable activity drain. Workflow drains and permanent
activity errors invoke `Native.shutdown`/`runtime_close` before becoming
terminal, so any outstanding native leases are force-retired while the
original adapter error remains the caller's result. A returned native `Error`
is still release-complete by contract and permits the OCaml adapter maps to be
discarded. If native shutdown raises before returning, the maps remain
retained, a terminal-cleanup-pending flag schedules a detached retry, and the
worker finalizer remains a last-resort path. A same-Domain shutdown admission
defect is kept retryable because it has not started teardown; the public wrapper
reopens admission so another Domain can retry after the active run loop exits.

The ownership and retry rationale is recorded in
[the native-worker reference](reference/native-worker-execution.md) and the
[runtime invariants](reference/runtime-invariants.md). Local evidence includes
the focused OCaml worker-loop/policy suites, Rust ABI status mapping and null
runtime tests, and these representative checks (with build directories outside
the repository so concurrent worktrees do not share generated files):

```text
DUNE_BUILD_DIR=/private/tmp/ocaml-temporal-dune-completion-resilience \
CARGO_TARGET_DIR=/private/tmp/ocaml-temporal-cargo-completion-resilience \
opam exec -- dune runtest --root .
CARGO_TARGET_DIR=/private/tmp/ocaml-temporal-cargo-completion-resilience \
cargo test --manifest-path rust/Cargo.toml -p ocaml-temporal-core-bridge
CARGO_TARGET_DIR=/private/tmp/ocaml-temporal-cargo-completion-resilience \
cargo clippy --manifest-path rust/Cargo.toml --locked --all-targets -- -D warnings
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
```

The live Temporal Compose scenario remains deferred because it requires a
running Temporal service and is intentionally not substituted by unit tests.

## 2026-07-13: Stable runtime owner allocation across blocking-section GC moves

Status: fixed and verified locally; the regression test reproducibly hangs
against the pre-fix code and passes against the fix.

`invoke_runtime_json`, `invoke_runtime`, and `ocaml_temporal_runtime_close` in
`lib/core_bridge/native_stubs.c` each captured the `owned_runtime *` interior
pointer of the `runtime` custom block once, then kept dereferencing it across
a `caml_enter_blocking_section` / `caml_leave_blocking_section` window during
which the OCaml runtime lock is released. A stop-the-world minor GC or
major-heap compaction on another Domain can run during that window and
relocate the custom block, leaving the cached pointer stale. `release_runtime`
would then decrement an `active_calls` counter at a stale address, and
`ocaml_temporal_runtime_close`'s `wait_for_runtime_calls` could spin forever
reading a count that would never reach zero at the address it actually holds
open.

The fix moves `owned_runtime` out of the OCaml-managed heap entirely: the
custom block payload now stores only a `malloc`-allocated pointer, nulled
immediately after allocation so an out-of-memory `malloc` leaves the finalizer
a safe no-op. `Runtime_val` dereferences that stored pointer once; because the
pointer's value never changes regardless of where the GC relocates the
enclosing custom block, every borrower can keep using it across a released
lock without re-fetching, including the `runtime_close` wait loop that
dereferences it for the loop's entire (possibly long) duration.
`finalize_runtime` is the sole `free` site, since a given custom block is
finalized at most once.

`test/bridge/test_ocaml_runtime_lifetime.ml` gained
`run_close_race_under_gc_pressure`, which races `runtime_close` against an
in-flight `replay_worker_wait_workflow` (a ~100ms bounded native wait) while a
third Domain forces `Gc.minor`/`Gc.compact` throughout the window. Reverting
the fix and running this test hangs indefinitely, confirming the test catches
the defect; with the fix it passes reliably across repeated runs.

```text
dune build --root . @install
dune build --root . @runtest --force
sh scripts/check-format.sh
```

## 2026-07-16: Child failure after worker-replay acceptance slice

The child-failure-after-replay slice adds a separate parent and child fixture,
an environment-selected controller path, and a source-only contract derived
from the existing successful parent/child snapshots. The history and
controller validators now bind the expected child workflow type and terminal
outcome, including `WorkflowExecutionFailed` in the child and
`ChildWorkflowExecutionFailed` in the parent. The live Compose controller is
intentionally marked pending until its complete CI run proves the real
Temporal replay and typed failure propagation.

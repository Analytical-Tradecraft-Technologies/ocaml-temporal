# Live acceptance coverage

This matrix records what the repository has actually proved against a real
Temporal Server and what still relies on deterministic local tests. It keeps
the first two-OCaml-binary success path separate from broader Temporal SDK
claims.

## How to read the statuses

- **Verified (live success path)** means `make test-temporal-integration` used
  real PostgreSQL and Temporal containers, a public OCaml worker, and a
  separate public OCaml driver. It proves only the listed successful scenario;
  it does not imply that every related failure or recovery path is live-tested.
- **Verified (synthetic only)** means the OCaml runtime, protocol adapter, or
  bridge behavior was tested without a workflow execution hosted by Temporal
  Server. It is useful evidence, but it is not live compatibility evidence.
- **Planned — later expansion** means a real-server assertion belongs in the
  existing two-binary Compose fixture after the success path is broadened.
- **Implemented — live verification pending** means the Compose controller,
  worker instrumentation, and strict assertions are present, but no successful
  real-server run has yet been recorded for that scenario.

The initial live gate passed in Linux CI for commit `d4456b7`, covering two
workflows. The current driver stages 26 top-level workflow starts. Its baseline
includes fan-out, timers, local and remote activities, four retry forms,
delayed asynchronous completion, continue-as-new, child success/failure/
cancellation/retry/start rejection, typed workflow failure, direct
cancellation and termination, direct and workflow-to-workflow signals,
queries, updates, and external cancellation with wrong-run rejection.
Worker-visible per-run markers guard signal, update, cancellation, termination,
and external-cancellation requests. Start-to-close and heartbeat-timeout retries
remain serialized after the shorter heartbeat path so the single activity
adapter does not turn local queueing into false timeout evidence. The complete
[PR #439 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578)
accepted all 26 starts and ended with the driver's complete assertion marker
against Temporal Server 1.31 and PostgreSQL.

The historical [PR #289 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719)
passed the earlier seventeen-result baseline. The [PR #302 Actions
run](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638) first
extended that baseline with a non-immediate activity retry under a configured
two-second-backoff policy and the exact `SMOKE:BACKOFF:RETRIED:SMOKE` result.
The timing guard rejects delivery in under one second and does not prove that
the full configured delay elapsed. The complete [PR #434 Actions
run](https://github.com/mfow/ocaml-temporal/actions/runs/29684113836) later
verified parent-to-workflow signal acknowledgement and target-side handler
delivery, and PR #439 retains those scenarios in the current baseline. The
[PR #253 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
passed the prior twelve assertions against Temporal Server 1.31 and PostgreSQL,
including the exact delayed asynchronous result, timeout retry, and
continue-as-new successor. The same run then passed the two-generation
continue-as-new successor. The [PR #266 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29310656994)
then passed all thirteen assertions, including the typed signal acknowledgement,
worker handler delivery, deterministic condition wake-up, and exact terminal
value. The PR #253 run also passed the two-generation restart/replay controller.
The historical [PR #210 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859)
and [PR #226 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29224854182)
remain useful CI evidence for the earlier nine- and ten-scenario slices.

The signal scenario is now **Verified (live success path)** by PR #266. The
driver waits for the signal workflow's exact readiness marker before sending
the signal, so the green integration result demonstrates server delivery to a
worker-accepted execution and a deterministic condition wake-up rather than a
local callback or a driver-side shortcut. The handler value is stored in
`Temporal.Workflow_context.Local`, which keeps repeated workflow executions
from sharing mutable module state.

The earlier run live-verified four exact successes, a second activity task delivered
by an ordinary retry policy, a heartbeat detail and timeout delivered to a
second activity attempt, successful parent/child completion, propagated
non-retryable child failure, child cancellation with
`Wait_cancellation_requested`, a typed non-retryable workflow failure, and
marker-guarded exact-run cancellation. The heartbeat scenario is server-visible:
the first attempt sends `SMOKE:HEARTBEAT:PROGRESS:1`, fails retryably, and the
second attempt can return `SMOKE:HEARTBEAT:RETRIED:SMOKE` only after receiving
that detail and timeout from Temporal. The child-failure driver checks the
public `Workflow` terminal category and retryability flag, while the worker
checks the in-workflow `Child_workflow` category.

The Docker-free contracts remain useful for fast feedback:
`test/smoke/test_temporal_heartbeat_contract.sh` protects the heartbeat path,
and `test/smoke/test_temporal_activity_timeout_contract.sh` protects the
timeout-retry registration, start-before-wait ordering, and exact marker. The
`test/integration/temporal/common/marker_test/test_smoke_definitions.ml` test
checks copied heartbeat details, timeout propagation, and invalidated-context
rejection. Retry-policy construction, JSON representation, and Core conversion
remain separately covered by synthetic tests. The current baseline also
live-verifies heartbeat-timeout retry and activity-level
non-retryable error-type classification. The restart/replay controller is
implemented as `make test-temporal-worker-restart`; its contract and
real-server execution passed in the [PR #253 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471).
The controller's thirteen-step record, exact run identity, replay marker,
normalized history, and volume cleanup are now live evidence. Retry after worker
replacement passed in [PR #298](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291).
The separate non-immediate retry under a two-second-backoff policy first passed
in [PR #302](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638);
its timing guard proves only that at least one second elapsed. Finally,
the separate one-slot sticky-cache eviction scenario passed in the complete
[PR #438 run](https://github.com/mfow/ocaml-temporal/actions/runs/29805397413);
the earlier [PR #322 run](https://github.com/mfow/ocaml-temporal/actions/runs/29402103748)
is historical evidence for the original gate, while crash recovery remains a
separate scenario.

The driver in this matrix is a one-shot OCaml assertion runner, not another
worker. It starts known workflows through `Temporal.Client`, waits for their
exact workflow/run results, and exits nonzero when an assertion fails. The
separate `smoke-worker` process registers and executes the workflows and mock
activity. Every integration run starts from a fresh Compose project and drops
the PostgreSQL data volume before and after the test, so its assertions never
depend on history left by an earlier run.

The public client also has an exact-run cancellation operation. Its request,
bounded native RPC, positive acknowledgement, idempotency key, and eventual
`Cancelled` result are covered by local mock, supervisor, OCaml protocol, and
Rust protocol tests. The current local driver implementation starts a
long-running durable-timer workflow, waits for its test-only marker activity to
publish the current run token after the timer and marker commands are issued
together, then sends `Temporal.Client.cancel` using the returned exact handle.
It waits on that same handle and checks the `Cancelled` category, retryability,
and stable message. The driver and worker also report successful shutdown
phases; the Makefile checks both markers before removing the Compose project.
The complete PR #210 run also checked the driver and worker shutdown phases
before Compose cleanup. The client cancellation request is still tested
locally for malformed input, idempotency, and bounded native-RPC behavior. The
complete PR #439 run is live evidence for the successful exact-run
cancellation path and its graceful-shutdown markers. Restart, replay, and
cache-eviction behavior require
separate runs.

## Coverage matrix

| Capability or scenario | Current local evidence | Real server evidence today | Remaining live boundary |
| --- | --- | --- | --- |
| Typed workflow/activity definitions, helper composition, and codecs | **Verified (synthetic only).** `make test-unit`; [`test/unit/test_definition.ml`](../../test/unit/test_definition.ml), [`test/unit/test_codec.ml`](../../test/unit/test_codec.ml), and [`test/unit/test_workflow_authoring.ml`](../../test/unit/test_workflow_authoring.ml). | **Verified (live success path).** The worker registers ordinary OCaml definitions and the driver decodes their typed results. | Failure and codec-rejection cases need dedicated live scenarios. |
| Direct-style workflow execution, suspension, futures, and deterministic replay | **Verified (synthetic).** `make test-runtime`; [`test/runtime/test_scheduler.ml`](../../test/runtime/test_scheduler.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), and native execution tests. | **Verified for live suspension and selected replay paths.** Timer, activity, and child waits suspend and later resume through real polling and completion. Dedicated controllers live-verify workflow replacement replay for the baseline in PR #253, patch histories in PR #348, and exact parent/child histories in [PR #351](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013). | Unusual scheduling paths and a broader replay corpus remain synthetic-only. |
| Workflow patch marker decisions | **Verified (synthetic).** Runtime tests cover patch-in and deprecation on new execution and replay, `NotifyHasPatch`, repeated same-mode calls, mixed-mode rejection, run isolation, ID copying, and native deprecated-marker completion. Shared fixtures plus Rust tests cover strict JSON and pinned Core round trips. | **Verified live in the complete [PR #356 run](https://github.com/mfow/ocaml-temporal/actions/runs/29469232271).** PR #348 created marker-free and active-marker histories through replacement. The expanded target replaces active code with deprecation-only code, then replaces a fresh deprecated-marker source with a source containing no patch API; exact normalized histories, marker states, worker topology, and cleanup are validated. | Legacy build-ID and deployment-based routing have bilateral bridge evidence; deployment registration/rollout, migration automation, a dedicated live routing gate, and broader historical compatibility remain separate. |
| OCaml/Rust JSON bridge validation and payload boundaries | **Verified (synthetic only).** `make test-bridge`; OCaml protocol tests under [`test/bridge/`](../../test/bridge/) and Rust protocol tests under [`rust/core-bridge/tests/`](../../rust/core-bridge/tests/) validate closed records, ownership, rejection, and normalization. | **Verified (live success path).** Happy-path client, workflow, and activity records cross both public processes. | Malformed and uncommon record variants need dedicated live fault injection if they become supported scenarios. |
| PostgreSQL, Temporal Server, namespace, and Core lifecycle | Focused supervisor/bridge lifecycle tests cover invalid and repeated transitions. | **Verified (live success path).** The fixture starts the stack, waits for health, runs [`test/integration/test_core_lifecycle.ml`](../../test/integration/test_core_lifecycle.ml), and cleans the project. | Upgrade, persistence, and production-topology coverage are separate concerns. |
| A workflow starts, runs, and returns a terminal result | Synthetic activation and native adapter tests cover command construction and terminal handling, including the public exact-run termination command. | **Verified (live success and termination paths).** The OCaml 5.5 Compose run starts the existing workflow corpus plus a readiness-marked long-running termination target, sends `Client.terminate` to that exact run, accepts the documented uncertain acknowledgement for reconciliation, and requires the same handle to report the `Terminated` category, non-retryable metadata, and stable message before completing the remaining exact-run assertions. | Operator reason detail shape and termination race/reconciliation semantics remain separately unverified. |
| Durable timers and wake-up | **Verified (synthetic only).** [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) covers zero-duration behavior, timer scheduling, and timer resolution. | **Verified (live success path).** `smoke.timer_then_activity` and the child in `smoke.parent_awaits_child` each wait for a short durable timer before returning. | Timer cancellation, unusual durations, and replay need separate live assertions. |
| Remote activity task polling and completion | **Verified (synthetic only).** [`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml), worker execution tests, and activity bridge tests cover typed task/completion conversion, lease retention, context-aware activity dispatch, prior heartbeat detail delivery, typed heartbeats, asynchronous lease retention, terminal lease retirement, and context invalidation. | **Verified live for ordinary, heartbeat-detail, start-to-close-timeout, heartbeat-timeout, non-immediate retry under a configured two-second-backoff policy, non-retryable, and delayed asynchronous completion paths.** PR #302 first verified the long-backoff result and that at least one second elapsed, not the full configured delay; the complete [PR #439 run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578) retains the full activity corpus. | Retry after worker replacement is separately live-verified in [PR #298](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291); broader recovery remains **Planned — later expansion**. |
| Activity retry policy and retry delivery | **Verified (synthetic only).** [`test/unit/test_activity_retry_policy.ml`](../../test/unit/test_activity_retry_policy.ml), [`test/runtime/test_activity_retry_policy.ml`](../../test/runtime/test_activity_retry_policy.ml), OCaml/Rust protocol tests, and Core conversion tests validate the immutable policy, exact coefficient bits, and malformed-input rejection without a server. | **Verified live for ordinary, heartbeat-detail, start-to-close-timeout, heartbeat-timeout, non-immediate delivery under a configured two-second-backoff policy, and non-retryable retry delivery.** The [PR #302 run](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638) first requires the exact long-backoff result, while its timing guard proves only that at least one second elapsed. The restart/replay contract additionally requires `SMOKE:AFTER-REPLAY:ATTEMPT:2`, and [PR #298](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291) verifies that live. | Broader backoff, jitter, and recovery combinations remain **Planned — later expansion**. |
| Multiple operations scheduled before awaiting | **Verified (synthetic only).** Scheduler and activation tests cover completion ordering, first-error behavior, and cancellation semantics. | **Verified live.** `smoke.fan_out` schedules two activities before its first wait; the cancellation workflow schedules its timer and marker activity before waiting; and the driver stages 26 top-level starts while preserving readiness barriers and serialized timeout retries. The complete PR #439 run checks every exact terminal outcome. | `race` and explicit server-history ordering assertions are **Planned — later expansion**. |
| Typed workflow failures and non-success terminal outcomes | **Verified (synthetic and live failure paths).** Error, protocol, client, worker, and runtime tests check typed rejection and terminal state handling without exceptions for expected failures. Focused client/bridge tests also cover exact-run termination. | **Verified (live success and failure paths).** The [PR #289 run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719) checks a typed non-retryable top-level workflow failure, propagated non-retryable child failure, duplicate-ID child start failure, child cancellation marker, and exact top-level cancellation metadata. | Live timeout remains **Planned — later expansion**; termination is live-verified by the same exact-run handle in the baseline smoke, and continued-as-new is live-verified separately. |
| Exact-run client cancellation and graceful shutdown with outstanding work | **Verified (synthetic only).** The public mock client, supervisor, OCaml bridge protocol, and Rust protocol tests validate exact run identity, bounded request handling, positive acknowledgement, stable request IDs, and typed `Cancelled` observation. The current driver also has live marker and result assertions. | **Verified live** in the complete [PR #439 run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578). The driver cancels `two-binary-long-running-cancellation`, waits for its exact run, checks category `Cancelled` and `non_retryable=false`, and checks the driver's and worker's graceful-shutdown markers. | Restart/replay is covered by the separate live controller; cache eviction remains **Planned — later expansion**. |
| Exact-run output-only client query while a workflow is suspended | **Verified (synthetic only).** Public query definitions, client request/response conversion, query-only activation handling, and read-only/non-suspending capability checks have focused OCaml and Rust coverage. The two-binary source contract also requires output-only and typed-input queries against the parked signal-condition execution, plus a missing-handler rejection and local invalid-input rejection. | **Verified live** in the complete [PR #434 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29684113836). The controller queried the exact suspended run for `SMOKE:QUERY:PENDING` and `SMOKE:TYPED_QUERY:PROBE:PENDING`, required an absent handler to return `bridge / Temporal client RPC failed: invalid_argument`, and kept codec rejection local before transport. | Query behavior across replay or cache eviction and query deadlines remain separately unverified. |
| Workflow-to-workflow external signal and cancellation | **Verified (synthetic only).** Public command construction, exact workflow/run identity, acknowledgement handling, and typed payload validation are covered by OCaml and Rust protocol tests. | **Verified live** in the complete [PR #439 run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578). The fixture exercises workflow-to-workflow signal delivery and exact-run external cancellation, including rejection of a mismatched run ID before acknowledgement. The pinned Core bridge exposes that rejection as a retryable workflow error (`non_retryable=false`) with a stable `Unable to cancel external workflow because not found` message prefix. | Failure of a missing/already-completed target and retry/replay interaction remain separate live scenarios. |
| Workflow update admission and typed completion | **Implemented in the two-binary fixture, with focused client, bridge, and runtime tests already passing.** The driver now starts a second parked workflow, admits a typed update, polls its typed result, and checks that the update changes workflow-local state. | **Verified live.** The two-binary Compose smoke first sends an unknown update to the parked run and requires either the exact current admission rejection (`Temporal client RPC failed: not_found`) or the exact typed non-retryable Core failure (`unhandled workflow update: smoke.update_not_registered`), then admits `smoke.set_value_update`, polls typed completion, and checks that the update changes workflow-local state. | Live rejected updates, suspended update handlers, replay/eviction during an update, and update retry/deadline behavior remain separate acceptance scenarios. |
| Activity heartbeat details and timeout propagation | **Verified (synthetic only).** Native activity execution, OCaml/Rust heartbeat protocol, lease retention, context lifetime, and bilateral validation are covered by focused local tests. The Docker-free [`test_temporal_heartbeat_contract.sh`](../../test/smoke/test_temporal_heartbeat_contract.sh) protects the two-process registration, start-before-wait, result assertion, and cleanup shape. [`test_smoke_definitions.ml`](../../test/integration/temporal/common/marker_test/test_smoke_definitions.ml) invokes the exact shared contextual activity twice and checks copied details, timeout propagation, and invalidated-context rejection. | **Verified live** in [PR #279](https://github.com/mfow/ocaml-temporal/actions/runs/29329420364). `smoke.activity_heartbeat_retry` preserves the first attempt's detail, while `smoke.activity_heartbeat_timeout_retry` requires the server-managed second attempt after heartbeats stop. | No heartbeat-specific live boundary remains in this fixture; worker restart and recovery remain separate. |
| Child-workflow start, acknowledgement, and terminal resolution | **Verified (synthetic only).** Focused Rust and OCaml tests cover ordered start/resolution, failures, duplicate sequences, and lease retirement. `test_child_replay_after_eviction_restarts_pending_child` additionally proves that an evicted parent does not revive its old child future: a replayed start creates a fresh execution, restarts the child sequence at one, and completes through the normal two-stage resolution. The [dedicated parent/child recovery contract](parent-child-restart-replay-acceptance.md) adds bilateral exact-run history and linkage validation. | **Verified live for success, propagated failure, cancellation, retry, duplicate-ID start failure, and exact parent/child replay.** The [PR #289 run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719) requires `smoke.parent_retries_child` to return `SMOKE:CHILD_RETRY:ATTEMPT:2` and `smoke.parent_observes_child_start_failure` to return `SMOKE:CHILD:START_FAILED`; the complete [PR #351 run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013) verifies exact parent and child histories plus both replacement-worker replay observations. | The new [child-failure-after-replay acceptance](child-failure-replay-acceptance.md) has a source-only contract and is pending its first complete live CI run. |
| Worker restart, replay, sticky-cache eviction, and continued execution | **Verified.** Runtime tests cover replay-stable commands and cache eviction; native activation diagnostics, strict history normalization, and the ordered controller contracts pass locally. The restart contract requires the replacement worker's exact attempt-two result and logical terminal activity path; the crash contract requires an exit-137 replacement without a graceful-stop marker; the eviction contract requires Core's real `RemoveFromCache`, an empty acknowledgement, and continued progress. The parent/child contract requires two exact initial histories, two post-removal nonterminal snapshots, and two generation-two replay checkpoints. | **Verified live.** Restart/replay passed in [PR #253](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471), retry after restart in [PR #298](https://github.com/mfow/ocaml-temporal/actions/runs/29346853291), the repaired isolated-worker sticky-cache eviction gate passed in the complete [PR #438 run](https://github.com/mfow/ocaml-temporal/actions/runs/29805397413), the earlier [PR #322 run](https://github.com/mfow/ocaml-temporal/actions/runs/29402103748) is historical evidence for the original gate, and exact parent/child replay passed in the complete [PR #351 run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013). | A broader cache/recovery and child-failure corpus remains planned. |

The sticky-cache eviction gate uses `make
test-temporal-worker-cache-eviction`: it configures one Core cache slot, starts
two runs whose first workflow task schedules a long deterministic timer, makes
a typed read-only cache-settling query of the first run, waits for the
worker's payload-free `cache_full` marker, and checks empty eviction
acknowledgement plus typed cancellation of both exact runs. The query is the
driver's synchronization barrier: a typed response proves that the worker has
accepted and executed an activation, without relying on a best-effort
filesystem callback that can be delayed while Core is processing the same
completion. Pinned Core ordering buffers the second run until the first run's
cache-full removal is acknowledged, so the driver's second-run marker is
timeout diagnostic information only and can never satisfy the required
`cache_full` observation. PR #322 is historical evidence for the original gate;
the repaired isolated-worker implementation passes the complete live
real-server contract in [PR #438](https://github.com/mfow/ocaml-temporal/actions/runs/29805397413),
so this row is green for the current topology. The broader cache/recovery
scenarios in the final column remain unverified.

## Stable evidence commands

These commands are the supported local gates for the corresponding matrix
rows:

```sh
make test-unit                 # definitions, codecs, public API, and errors
make test-runtime              # scheduler, activations, futures, replay
make test-bridge               # OCaml/Rust ABI and protocol fixtures
make verify                    # broad build, lint, Rust, bridge, and repository contracts
make test-temporal-integration # real PostgreSQL/Temporal + two OCaml binaries
make test-temporal-worker-restart # contract plus two-generation live restart/replay
make test-temporal-worker-cache-eviction # contract plus one-slot live eviction
make test-temporal-parent-child-restart # bilateral parent/child replay/recovery
```

All six live controllers start a real Temporal Server. The baseline integration
target owns the current 26-start two-binary gate. The graceful-restart
and forced-crash targets own distinct replacement-worker recovery sequences;
the cache-eviction target owns the one-slot `RemoveFromCache` sequence; the
patching target owns legacy-versus-patched replay selection; and the
parent/child target owns bilateral replay and recovery for two exact runs. Each
specialized target also runs its Docker-free contract before the live fixture.
Every controller owns its fixture lifecycle, starts independent worker and
one-shot assertion processes, and removes the PostgreSQL volume. A green `make
verify` alone is not live workflow evidence, and the baseline two-binary gate
must not be generalized to unlisted terminal, cache-eviction, patching, or
recovery scenarios. The [PR #253 run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
is the graceful restart/replay evidence, [PR #306](https://github.com/mfow/ocaml-temporal/actions/runs/29355426605)
is the forced-crash evidence, [PR #322](https://github.com/mfow/ocaml-temporal/actions/runs/29402103748)
is the sticky-eviction evidence, and [PR #348](https://github.com/mfow/ocaml-temporal/actions/runs/29411260374)
is the patch replay evidence. [PR #439](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578)
is the current complete CI evidence for the 26-start baseline; [PR
#302](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638) is the
first complete evidence for the long-backoff extension, and PR #289 remains
historical evidence for the preceding seventeen-result baseline. The
complete [PR #351 run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013)
is the exact parent/child restart-replay evidence. Broader recovery scenarios
remain unverified.

# Live acceptance coverage and evidence

This reference records the evidence audited for issue #491 on 2026-09-20.
The source baseline is [`beae10d0a58e`][baseline]. **Implemented**, **focused-tested**,
**live-tested**, and **supported** are different claims. A source definition or
Docker-free contract proves neither a successful live run nor production support.
The package remains experimental; [#489][policy] tracks the v1 support decision.

## Verified CI boundary

The [September 19 Build run][build] completed successfully at the baseline
commit. Its [Temporal/PostgreSQL integration job][live-job] ran every live
controller listed below, including the baseline driver's final
`two-binary driver assertions passed` marker. The final child-failure controller
also completed successfully; it was not merely present in the workflow YAML.
The [September 17 Build run][previous-build] at
[`a4581f7c97e8`][previous-baseline] is the earlier successful evidence cited by
issue #491 and also ran these seven controllers. Older PR runs document the
first introduction of smaller slices and must not be called the current suite.

| Evidence | Verified configuration | Limit |
| --- | --- | --- |
| Live server job | Linux `ubuntu-24.04`, OCaml 5.5, independent OCaml worker/client processes, plaintext Temporal Server 1.32.0 and PostgreSQL 18.6 in Compose | One server/compiler/platform/topology combination; no TLS/authentication, Temporal Cloud, upgrade, or production qualification |
| Linux build/test matrix | OCaml 5.2, 5.3, 5.4, and 5.5 on amd64 and arm64 | Build, lint, focused tests, and source contracts; these are not additional live-server lanes |
| Native build/test matrix | OCaml 5.5 on Windows x64 and macOS ARM64 | Native package/bridge/runtime validation, without the Compose server suite |
| Pins | [Compose images][compose] include immutable image digests; [Cargo manifest][cargo] pins Temporal Core to `95e97686a079dcfe6c42e3254b2f3f5e3d97408f`; the [workflow][workflow] identifies the executed lanes | A successful snapshot does not qualify later image, Core, toolchain, or source changes |

The workflow and Makefile must be read at the tested commit. In particular,
the audited Rust toolchain file selects 1.98 while the native workflow/Makefile
still request 1.94.1; the successful job is evidence for what actually ran, not
proof of a uniform release toolchain. Resolve the declared toolchain matrix
before claiming a supported release combination.

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

The [child-failure acceptance reference](child-failure-replay-acceptance.md)
records the original PR #361 evidence. Its status is live-tested, not pending a
first run. Broader child failure, cache-pressure, repeated restart, and recovery
combinations still need dedicated cases.

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
| Typed update on `smoke.signal_condition` | Unknown handler is rejected; registered update is admitted, polled to typed completion, and changes workflow-local state | Suspended updates, validator rejection, replay/eviction, retry and deadline behavior need additional live cases |

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
worker are distinct events. The serialized callback adapter has no public
cooperative cancellation probe; a Core cancellation task cannot preempt an
already running OCaml callback. Worker shutdown drains owned work through its
lifecycle path. The stop-marker checks and focused lifecycle tests do not prove
a bound on application callback duration or Kubernetes termination behavior.

Reset, bounded visibility listing, worker deployment routing, memo/search
attributes and priority metadata have implemented/focused-tested surfaces but
no dedicated live qualification in this inventory. The private replay feeder
is not a public history replay tool. Authentication configuration, cross-version
history corpora, release delivery and operational/load qualification remain
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

```sh
make check-live-acceptance-inventory
make test-quality-contract
make test-unit
make test-runtime
make test-bridge
make verify
# Run the applicable live controller from the table above separately.
```

[baseline]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/commit/beae10d0a58e58fb8e076734cd38113d7a2b4466
[build]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/35469570419
[live-job]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/35469570419/job/105967951226
[previous-build]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/35278812252
[previous-baseline]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/commit/a4581f7c97e83498d4910dadc3ad811389a80f82
[policy]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/489
[compose]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/compose.yaml
[cargo]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/rust/Cargo.toml
[workflow]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/.github/workflows/build.yml
[makefile]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/Makefile
[driver]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/driver/smoke_driver.ml
[definitions]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/common/smoke_definitions.ml
[lifecycle]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/test_core_lifecycle.ml
[restart-driver]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/driver/restart_driver.ml
[restart-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/scripts/test-restart-replay-contract.sh
[crash-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/smoke/test_temporal_worker_crash_recovery_contract.sh
[eviction-driver]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/driver/cache_eviction_driver.ml
[eviction-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/smoke/test_temporal_worker_cache_eviction_contract.sh
[patch-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/scripts/run-patch-replay-live.sh
[patch-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/scripts/test-patch-replay-contract.sh
[parent-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/scripts/run-parent-child-restart-replay-live.sh
[parent-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/scripts/test-parent-child-restart-replay-contract.sh
[failure-controller]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/scripts/run-child-failure-replay-live.sh
[failure-contract]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/integration/temporal/scripts/test-child-failure-replay-contract.sh
[scope-source]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/lib/public/scope.ml
[activity-source]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/lib/public/activity.ml
[child-source]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/lib/public/child_workflow.ml
[scope-tests]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/runtime/test_scope.ml
[activation-tests]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/blob/beae10d0a58e58fb8e076734cd38113d7a2b4466/test/runtime/test_activation.ml

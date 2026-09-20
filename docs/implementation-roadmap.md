# Implementation Roadmap

This roadmap decomposes the approved architecture into independently testable
subprojects. It does not redefine the final objective: the project is complete
only when the acceptance criteria in the architecture specification and every
capability in the parity matrix are verified.

“Complete” in the table means that the repository has passed the evidence for
that phase; it does not mean the whole SDK is production-ready. Core worker and
replay behavior is implemented before ergonomic features borrowed from other
Temporal SDKs. Those later features should preserve the useful behavior while
using idiomatic OCaml APIs. They may be implemented in OCaml even when another
SDK implements them in its host-language layer or in Rust, if that produces a
cleaner and more maintainable OCaml design.

## Delivery order

The status below is audited at
[`beae10d0a58e`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/commit/beae10d0a58e58fb8e076734cd38113d7a2b4466).
[Live acceptance coverage](reference/live-acceptance-coverage.md) retains the
successful CI job, commit-pinned scenarios and compatibility boundary. The
v1 support decision in [#489](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/489)
is a release gate, distinct from completing the long-term parity target.

| Phase | Deliverable and completion evidence | Current status |
| --- | --- | --- |
| 1 | Foundation, typed definitions, codecs, futures, scheduler and synthetic activations; broad local verification | Complete foundation; public API remains experimental. |
| 2 | Private Rust/C bridge, owner-Domain supervisor, native worker/client and Compose topology; separate OCaml worker and driver | Complete initial live slice. |
| 3 | Timers, activities, retries, cancellation and recovery; important success/failure/lifecycle paths run live | In progress. Named remote/local activity, terminal, restart, forced-crash and one-slot eviction scenarios pass live. Broader failures, load and recovery combinations remain. |
| 4 | Children and structured concurrency; fan-out, await and safe cancellation through the server | In progress. Child success/failure/cancellation/retry/start rejection, successful parent/child replay and child failure after replay pass live. Scope hooks emit activity/child cancellation commands and have focused tests; direct live scope qualification and broader policy/race cases remain. |
| 5 | Signals, queries, updates, validators, conditions and handler policies; interactive conformance including mode violations | In progress. Typed signal/query/update paths and unknown-handler rejection pass live. Suspended update recovery, validator rejection, query/update replay/eviction, deadlines and broader handler policies need live evidence. |
| 6 | Continue-as-new, patches, worker versioning, side effects, external operations and metadata; history compatibility and command integration | In progress. Continuation, patch lifecycle, external signal/cancellation, completed-target signal rejection and wrong-run cancellation rejection pass live. Routing/metadata have focused evidence; deployment rollout, side effects, missing-target/replay combinations and broader history compatibility remain. |
| 7 | OCaml/local activities, heartbeats, async completion, codecs, interceptors and shutdown; conformance and operational termination | In progress. Ordinary/local activity success, remote retries/heartbeats, delayed async completion and stop markers pass live. Local retry/recovery, callback concurrency/cooperative cancellation, heartbeat response flags, interceptors and bounded operational shutdown remain incomplete. |
| 8 | Client, schedules, visibility, reset/terminate/cancel, updates, Nexus and test-server controls; supported-version conformance | In progress. Core client controls/interactions are implemented and named paths pass live. Reset/visibility conformance, update recovery/options, schedules, Nexus and test-server controls remain. |
| 9 | Performance, observability, security, packaging, stability and release automation; reproducible artifacts and rehearsal | In progress. Logging, quality/license gates, installed-consumer/package checks and release preflight exist. Public authentication, performance/load evidence, support policy, provenance/publication, complete artifact audit and upgrade/release rehearsal remain release work. |
| 10 | Parity closure; every parity row links implementation, tests and documentation | Planned. A bounded v1 decision does not claim full parity. |

## Plan documents

1. [Foundation and deterministic runtime](superpowers/plans/2026-07-11-foundation-and-deterministic-runtime.md)
2. [Core bridge and first real workflow](superpowers/plans/2026-07-11-core-bridge-and-first-real-workflow.md)
   The private mailbox processor is a completed Phase 2 foundation described
   by [ADR 0003](decisions/0003-private-mailbox-processor.md). The one-Domain
   SDK graph supervisor now owns the real Rust runtime, client, and validated
   workflow/remote-activity worker as described by [ADR
   0004](decisions/0004-sdk-instance-supervisor.md). The bilateral first
   activation/completion semantic adapter is complete as described by [ADR
   0006](decisions/0006-first-workflow-semantic-protocol.md). Rust now owns one
   guarded workflow poll lane, one guarded remote-activity lane, their shared
   task ledger, and bounded owner-domain readiness waits. The pure-OCaml
   activation translation, execution command conversion, and private
   existential run registry are now covered by focused tests and exercised by
   the first public worker/driver live success path.
   Readiness waits intentionally return to that mailbox after 100 ms when Core
   is quiet. The current
   translation now preserves and validates every field needed by Core activity
   commands, including deterministic defaults for omitted queue and timeout
   options. Child commands now have closed semantic records and Core
   conversion; start acknowledgments and terminal child results are translated
   through the same JSON protocol and are covered by focused lifecycle tests.
   The basic live worker wiring and Compose acceptance path are complete.
   `Temporal.Scope` deterministically cancels observation and invokes registered
   remote activity/child cancellation hooks. Timers and unscoped operations
   remain observation-only, and hooks cannot preempt OCaml activity callbacks.
   Focused tests cover ownership, repeated cancellation, hook ordering and
   cleanup failures. The live controllers separately exercise explicit child
   cancellation, exact-run top-level cancellation, restart/crash/eviction,
   successful parent/child replay and child failure after replay with recovery.
   The [evidence reference](reference/live-acceptance-coverage.md) records the
   exact boundaries; broader lifecycle combinations remain unqualified.
   Poll decode failures use an exact-document rejection ABI: Rust retains
   semantic handoff state and will not retire a lease for a changed workflow
   activation or activity task.
3. Activities, timers, and replay (written after Phase 2 evidence is committed)
4. Child workflows and structured concurrency (written after Phase 3 evidence is committed)
5. Interactive and advanced features (split further at the preceding review gate)
6. Platform breadth and publication hardening (split further at the preceding review gate)

Each detailed plan is written immediately before its phase so it can use the
actual interfaces and upstream Core revision proven by prior phases. This
prevents later plans from pretending that unstable internal APIs are already
known while preserving the full target in this roadmap.

## End-to-end acceptance topology

The first live vertical slice creates the deployment shape used by all later
essential-feature tests:

- PostgreSQL stores Temporal Server state.
- Temporal Server uses PostgreSQL and exposes its normal frontend service.
- An OCaml worker container links this library and registers the workflow and
  deterministic mock activity implementations used by the suite.
- A separate OCaml test-client container links the same library, starts each
  test workflow, waits for its result, and checks the expected outcome.

The baseline driver stages the workflow definitions in the
[generated live inventory](reference/live-acceptance-inventory.md), preserves
worker-visible readiness barriers before control operations, and serializes the
timeout retries after the short heartbeat path. It asserts exact-run terminal
outcomes and both client/worker cleanup. Separate live controllers qualify
restart, crash, eviction, patching and both parent/child replay outcomes.

[Live acceptance coverage](reference/live-acceptance-coverage.md) names the
actual assertions, tested commit and successful CI job. The inventory check
runs in the existing broad test gates; update it and the evidence matrix when
adding or removing a scenario. A successful build or generated inventory alone
does not establish live compatibility. Every essential capability still needs
its applicable failure/lifecycle evidence before release.

## Dependency and licensing gate

Every phase must update the dependency inventory before it can be committed.
Project dependencies must use MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC,
Zlib, PostgreSQL, or another explicitly reviewed permissive license. The only
standing exception is `LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception` for
the OCaml compiler/runtime or an individually reviewed OCaml dependency. No
ordinary GPL, AGPL, LGPL, MPL, EPL, CDDL, SSPL, BUSL, Commons Clause,
source-available, non-commercial, missing, or unknown dependency is accepted.

The release artifacts and runtime container are audited independently from the
host operating system and ephemeral CI/build environment. Build tools are
recorded in the inventory; their licenses and whether they are redistributed
are made explicit rather than inferred from the final binary.

## Reset random-seed recovery (#570)

The bridge and runtime now preserve and apply Core's `UpdateRandomSeed` job.
The [timer-reset fixture](../test/integration/temporal/reset_random_seed/README.md)
records a successful native live reset and replays that exact history through
Core and the OCaml worker adapter in the ordinary offline test suite. This
qualifies the missing-event regression, including deterministic random draws;
reset remains experimental and outside production recovery guidance pending
the broader conformance and support-policy gates. No dependency was added.

## Workflow defect recovery before v1 (#511)

The private protocol now separates failed workflow tasks from intentional
terminal workflow failures. The [failure contract](reference/workflow-failures.md)
defines classification, cache/acknowledgement ownership, and the focused and
exact-run live recovery gates. This is a prerequisite for broad fault and
conformance qualification; passing the bounded recovery fixture does not claim
those later qualification gates complete.

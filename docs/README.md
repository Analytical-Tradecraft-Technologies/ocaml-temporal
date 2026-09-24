# Documentation guide

This directory describes an experimental OCaml Temporal SDK. The project is
building a worker SDK, not only a client that submits a request and reads a
result. Read the documents in this order when you are new to the repository:

1. [Writing workflows](guides/workflows.md) introduces the public OCaml API
   and marks which examples are synthetic-only versus native-ready.
2. [Verified progress](progress.md) records what has actually been implemented
   and tested at each milestone. The [live evidence audit](reference/live-acceptance-coverage.md)
   records the current tested source and CI boundary.
3. [Implementation roadmap](implementation-roadmap.md) lists work that is
   planned but not yet a supported feature.
4. [Runtime invariants](reference/runtime-invariants.md) explains the rules
   that workflow execution and replay must preserve.
5. [Native Core bridge](reference/core-bridge.md) explains ownership and the
   OCaml/C/Rust boundary.

The remaining reference documents are useful when changing one subsystem:

- [Native execution translation](reference/native-execution-translation.md)
  describes checked activation and command conversion.
- [Private JSON control protocol](reference/core-protocol.md) defines the
  bilateral envelope and its validation rules.
- [Native client JSON protocol](reference/client-protocol.md) documents typed
  workflow starts, asynchronous start tickets, exact-run waits, cancellation,
  termination, reset, typed signals, output-only and typed-input queries,
  visibility pages, typed workflow update admission and completion,
  asynchronous activity completion, and shutdown.
- [OCaml activity protocol adapter](reference/activity-protocol.md) documents
  remote activity tasks, completions, heartbeats, and opaque task-token
  ownership. The heartbeat schema is
  [`activity-heartbeat.schema.json`](schemas/bridge/activity-heartbeat.schema.json).
- [Native workflow execution](reference/native-worker-execution.md) documents
  the current native worker command slice and two-stage child resolution.
- [Native activity execution](reference/native-activity-execution.md) documents
  activity dispatch and completion retention.
- [Deterministic workflow time](reference/workflow-time.md) documents
  `Temporal.Workflow.now`, its exact timestamp representation, and its replay
  safety contract.
- [Workflow patching](reference/workflow-patching.md) documents public patch-in
  and deprecation, durable patch IDs, per-execution decisions and mode safety,
  and the initial plus lifecycle live gates verified by PR #348 and PR #356.
- [Worker versioning](reference/worker-versioning.md) documents typed legacy
  build-ID and deployment-based routing options, task-local selected deployment
  metadata, the closed OCaml/Rust JSON contract, and the current evidence
  boundary.
- [Interactive workflows](reference/interactive-workflows.md) documents the
  experimental typed signal, query, and update definitions, deterministic
  handler dispatcher, and the remaining native-delivery boundary.
- [Native workflow interactions](design/native-interactions.md) specifies the
  Core-to-Rust-to-OCaml mapping for signals, queries, and updates, including
  response timing, replay rules, validation, the implemented signal boundary,
  the immediate and suspended update boundaries, and the remaining live
  query/update recovery and broader acceptance work. Output-only and typed-input
  query acceptance, typed update admission/completion, and exact-run termination
  are recorded in the live two-binary evidence.
- [OCaml SDK logging](reference/observability.md) documents log sources, tags,
  levels, privacy, and Domain behavior.
- [Local Temporal stack](reference/local-temporal-stack.md) documents the
  PostgreSQL/Temporal Server Compose fixture and Make targets.
- [Two-OCaml-binary acceptance design](reference/two-ocaml-binary-e2e-acceptance.md)
  records the live driver/worker result test, its success-path evidence, and
  the scenarios that still need real-server coverage.
- [Live acceptance coverage](reference/live-acceptance-coverage.md) separates
  synthetic evidence from the verified real-server two-binary success path
  and planned scenario expansion.
- [Worker restart and replay acceptance design](reference/worker-restart-replay-acceptance.md)
  specifies the controlled worker-replacement scenario, its exact assertions,
  diagnostic evidence, and fresh-volume cleanup rules, and records the live
  verification evidence. It covers restart/replay only; sticky-cache eviction
  and crash recovery remain separate scenarios.
- [Internal replay worker bridge](reference/replay-bridge.md) documents the
  bounded Rust history feeder, strict JSON/base64 format, Core ownership, and
  the local evidence for the first replay-plumbing slice.
- [Live diagnostic artifacts](reference/live-diagnostic-artifacts.md) describes
  bounded CI retention, redaction, partial failure evidence and retrieval.
- [Worker restart/replay diagnostic contract](reference/worker-restart-replay-diagnostics.md)
  defines the payload-free normalized history, generation/replay records, and
  ordered controller lifecycle evidence used by both the offline contract gate
  and the live controller.
- [Parent/child restart and replay acceptance](reference/parent-child-restart-replay-acceptance.md)
  defines the bilateral exact-run authority, three history stages, private
  atomic checkpoint lifecycle, and evidence boundary for child recovery.
- [Feature coverage and implementation status](reference/feature-coverage.md)
  gives the short status reference and distinguishes live evidence, mock-only
  tests, partly live-tested native bridge support, and deferred features.
- [Quality and security gates](reference/quality-gates.md) documents pinned
  scanners and the checks run locally and in CI.
- [Installed package boundary](reference/package-boundary.md) documents which
  libraries are package-private and the installed-consumer regression that
  protects the public `Temporal` surface.
- [Public API compatibility](reference/api-stability.md) documents the
  pre-`0.1.0` compatibility policy and the installed-consumer type witness.
- [Precompiled OCaml SDK](reference/prebuilt-ocaml.md) describes compatible binary
  installations and downstream linking without recompiling the SDK.
- [Release preflight](reference/release-preflight.md) documents the clean-tree
  metadata and source-input gate, deterministic CI-only Cargo
  SBOM audit, and stable/prerelease tag-to-manifest consistency check.
- [Architecture specification](superpowers/specs/2026-07-11-ocaml-temporal-sdk-design.md)
  records the long-term design. APIs described there may be future work.

Files under `superpowers/plans/` are historical implementation plans. When a
plan and the source disagree, the source, tests, and progress record win.

## How the pieces fit together

An application writes ordinary OCaml functions and links the `temporal-sdk`
library into its own final executable. `Temporal.Worker` owns workflow and
activity registration and executes the current native task slice.
`Temporal.Client` starts an execution, can cancel that exact execution, and
waits for its exact workflow/run pair returned by Temporal; it does not execute
workflow code.

For an HTTP(S) endpoint, the public library creates one private supervisor per
SDK instance. That supervisor owns the Rust runtime, Temporal Core client, and
optional worker, and serializes lifecycle operations on one owner Domain. Rust
is a static library linked into the OCaml executable. There is no required
Rust sidecar or separate OCaml/Rust service to deploy.

The public facade reaches that native transport through a private OCaml
kernel. The kernel owns deterministic activation scheduling, execution-local
state, future callbacks, mailbox serialization, and the one-owner-Domain
supervisor. `Temporal_sdk_kernel` is a package-private module allow-list, while
the private `Backend` and `Native_worker` adapters translate public values into
kernel operations. Applications should use the installed `Temporal` facade
instead of depending on those implementation modules; the [three-layer
boundary decision](decisions/0009-three-layer-ocaml-boundary.md) and the
[installed-package boundary](reference/package-boundary.md) describe the
ownership and compatibility rules.

The private OCaml/Rust boundary uses strictly validated JSON semantic records.
This is an internal, inspectable representation chosen to keep the ownership
and validation contract small. Temporal Server does not receive these JSON
records: Rust converts them to and from the official Temporal Core protobuf and
gRPC APIs. JSON is also one possible **payload encoding** (`json/plain`), which
is a separate concern from the private bridge protocol. Payloads are otherwise
opaque bytes with encoding metadata.

## Current status at a glance

The [feature reference](reference/feature-coverage.md) and
[live evidence matrix](reference/live-acceptance-coverage.md) record the audit at
`beae10d0a58e`, including the successful September 19 CI job and exact source
links. Keep current support wording aligned with the v1 policy decision.

| Layer | Evidence | Important limit |
| --- | --- | --- |
| Workflow runtime | Focused tests for typed definitions, scheduling, futures, deterministic time/randomness, conditions and scope hooks; live timer/activity/child waits | Detailed ordering and direct scope cancellation remain focused-test evidence. |
| Activities | Remote retry/heartbeat/timeout/async paths and local activity success pass live | Callback concurrency, cooperative cancellation observation, local retry/recovery and bounded shutdown remain incomplete. |
| Children and recovery | Live success, failure, explicit cancellation, retry and start rejection; separate controllers prove exact parent/child replay and child failure after replay | Broader policies, races and repeated recovery combinations remain unqualified. |
| Client/interactions | Named exact-run controls, both query forms, typed updates, direct/external signals and external cancellation pass live, including completed-target signal rejection | Reset/visibility, suspended updates, deadlines and interaction replay/eviction need additional live cases. |
| Patching and versioning | Live patch-in/deprecation/removal histories; focused legacy/deployment routing support | Dedicated live routing, rollout and arbitrary-history compatibility remain open. |
| Native ownership and packaging | OCaml/C/Rust lifecycle, validation, package/installed-consumer, license and preflight gates | Public authentication, supported compatibility/release delivery and production operational qualification remain separate gates. |

The [generated inventory](reference/live-acceptance-inventory.md) lists the
current baseline definitions and live controller targets. The audited server
job ran on Linux OCaml 5.5 with Temporal 1.32.0 and PostgreSQL 18.6; the Linux
compiler matrix and native Windows/macOS jobs are build/focused-test evidence.
A signal or cancellation RPC acknowledgement does not prove handler execution
or terminal completion; the live driver checks those outcomes separately.

## Build and test commands

The Makefile is the supported command interface. The normal commands run in a
Docker Compose development container, so host language-tool installation is
optional for build and test work:

```sh
make build
make test-unit
make test-runtime
make verify
make quality
make license-check
make test-temporal-worker-restart
make test-temporal-integration
make test-temporal-workflow-patching
make test-temporal-parent-child-restart
```

`make verify` combines the OCaml version check, Dune build/lint, Rust format and
Clippy checks, Rust tests, bridge/install smoke tests, and the repository
quality contract. `make quality` is a separate host-tool gate and requires the
pinned `cargo-deny`, `cargo-machete`, and `typos` binaries. The license audit is
intentionally separate from the OCaml-version matrix and runs in containers.
If a local Docker VM has limited memory for native linkers, set
`DUNE_JOBS=1` (or another small value) on build/lint targets; the variable is
empty by default so CI keeps its normal parallelism.
For the real server smoke, the Makefile owns Compose project selection,
readiness checks, failure logs, and volume cleanup; do not run the fixture's
Compose file from the repository root.

### CI lanes

A code pull request runs representative compatibility lanes: Linux amd64 on
OCaml 5.2 and 5.5, Linux arm64 on OCaml 5.5, macOS ARM64 on OCaml 5.5, the
quality and dependency-license audits, and the OCaml 5.5 Temporal/PostgreSQL
smoke. The Windows x64 OCaml 5.5 native lane is enabled when native bridge,
build/toolchain, workflow, or composite-action configuration changes.
Documentation-only pull requests still run the standalone license audit; JSON
protocol schemas under `docs/schemas/` are instead treated as code. A pull
request that changes only the live acceptance fixture under
`test/integration/temporal/` runs the license audit and its live smoke rather
than the representative matrix. A push to `master` and the scheduled run are
the exhaustive gate: OCaml 5.2–5.5 on both Linux architectures, both OCaml 5.5
native desktop jobs, quality, license audit, and the live smoke.

When a GitHub Actions run is still `queued`, it has not produced verification
evidence. The representative local baseline is `make check OCAML_VERSION=5.2`,
which combines `make verify` with the package/OCaml license audit. Run
`make quality` separately for the pinned host scanners, and run
`make native-verify` on a matching Windows or macOS host for the native
compatibility path. The locked Cargo license scanner is intentionally a single
CI-only job; `make license-check` does not claim to replace that scanner. Use
`make test-temporal-integration` only when a real Temporal Server/PostgreSQL
result is required. These local results are useful interim evidence, but they
do not turn an unexecuted matrix, platform, or live-server job green; queued
required checks still need to finish when Actions becomes available.

`make test-temporal-worker-restart-contract` is the Docker-free contract gate:
it validates the normalized history/replay diagnostic contract, the ordered
restart-controller lifecycle contract, and their rejection paths. The umbrella
`make test-temporal-worker-restart` runs that contract gate and the live
two-generation replacement scenario. A green contract-only run must not be
used as evidence that a worker was restarted or that Temporal replay occurred;
the live target is required for that claim.

`make test-temporal-workflow-patching-contract` is likewise Docker-free. It
validates the checked-in patch-history, replay-diagnostic, and controller
fixtures plus fail-closed normalization and validation cases. The umbrella
`make test-temporal-workflow-patching` runs that contract before the real
two-scenario Compose controller. A green contract-only run is not evidence of
Temporal Server replay. The complete [PR #348 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29411260374) is the
corresponding real-server evidence for the original patch-in cases; the
complete [PR #356 CI run](https://github.com/mfow/ocaml-temporal/actions/runs/29469232271)
verifies the expanded lifecycle cases.

## Terms used in this project

- **Temporal Server** stores workflow history and dispatches workflow and
  activity work. It does not execute OCaml workflow functions.
- **Temporal Core** is Temporal's official Rust worker/client library. It
  manages server communication and durable worker state machines for an SDK.
- **SDK** is the whole OCaml-facing worker implementation: workflow runtime,
  activity dispatch, lifecycle, and the optional client start/cancel/wait
  surface.
- **Client** is the smaller `Temporal.Client` API for starting a workflow,
  cancelling one exact execution, and waiting for that execution. It is not the
  worker.
- **Worker** is a `Temporal.Worker` value that registers local OCaml workflow
  and activity functions and polls/completes tasks.
- **Workflow activation** is a validated batch of work that Core delivers to
  the OCaml runtime, such as starting a workflow, resolving an activity, firing
  a timer, cancelling, or removing a cached execution.
- **Command** is a validated instruction emitted by workflow code, such as
  scheduling an activity or starting a durable timer. Core sends the command
  to Temporal Server as part of the workflow completion.
- **Replay** means running workflow code again against recorded history. A
  replay must make the same decisions and produce the same commands.
- **Payload** is an opaque byte sequence plus metadata naming its encoding.
  Temporal does not require JSON for payloads.
- **Codec** converts a typed OCaml value to and from a payload. The built-in
  string codec uses `json/plain`; bytes and unit use binary encodings.
- **Future** is a workflow-owned value that becomes ready in a later
  activation. Waiting suspends the current workflow fiber, not an operating
  system thread or an unrelated application task.
- **Supervisor** is the private one-owner Domain that serializes native
  lifecycle and bridge operations for one SDK instance. Applications do not
  handle its native graph directly.
- **Bridge** is the small C-compatible boundary between the OCaml library and
  the Rust static library. It copies owned bytes and keeps protobuf details out
  of public OCaml code.

When a term is still unclear, start with the workflow guide and then follow the
subsystem reference linked above; avoid inferring live feature support from a
type or a synthetic test alone.

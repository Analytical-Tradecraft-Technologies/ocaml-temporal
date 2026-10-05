# OCaml Temporal SDK Design

**Status:** Approved target architecture; implementation is in progress
**Date:** 2026-07-11
**License:** Apache-2.0
**Target:** A reusable, publishable OCaml 5 SDK for authoring Temporal workflows

This document describes the intended completed SDK. It is not a list of
features available today. See the [progress log](../progress.md) for
verified current behavior and the [roadmap](../implementation-roadmap.md)
for remaining work.

## 1. Purpose

This project will let developers author Temporal workflows in modern OCaml while using Temporal's Rust Core SDK for server communication and worker machinery. The delivered worker is a native executable built by the OCaml application. Rust is linked into that executable as a private implementation detail; application authors do not operate a Rust worker, Go worker, sidecar, or HTTP service.

The SDK must make workflow code feel like ordinary OCaml. Workflow bodies and reusable helpers are normal functions. Temporal operations use typed values, explicit codecs, labelled arguments, immutable configuration values, typed futures, and `result`-based failure handling. OCaml 5 algebraic effects provide direct-style suspension internally, but effect constructors and continuations are not part of the public API.

The long-term target is feature parity with production Temporal SDKs. Delivery will proceed through verified, independently useful vertical slices without narrowing that target.

Parity is a behavioral goal, not an instruction to translate another SDK's
public API. Core worker correctness comes first. After that foundation is
working end to end, the project will study useful features from the other
official SDKs and express them using normal OCaml types and composition. A
feature may use an existing Temporal Core mechanism or be implemented in OCaml
from first principles, depending on which boundary gives the clearest,
safest, and most maintainable result.

## 2. Goals

1. Build and run the complete development and test environment through Makefile targets backed by Docker Compose.
2. Include PostgreSQL, a self-hosted Temporal Server, Temporal UI, an OCaml workflow worker, and cross-language test activity workers in the Compose stack.
3. Produce a single native worker executable built by OCaml and linked with a private Rust bridge.
4. Support durable, deterministic OCaml workflow execution, including replay after worker restart and cache eviction.
5. Provide typed activities, child workflows, timers, concurrent scheduling, racing, cancellation, signals, queries, updates, continue-as-new, versioning, external workflow operations, local activities, Nexus operations, search attributes, memo, and the remaining Core-supported workflow commands.
6. Make reusable orchestration helpers indistinguishable from ordinary OCaml functions wherever practical.
7. Interoperate with activities and workflows implemented by other Temporal SDKs.
8. Be fast enough for orchestration-heavy AI agents and horizontally scalable on Kubernetes.
9. Be suitable for publication as an open-source OPAM package with stable interfaces, documentation, examples, CI, reproducible builds, and a clear compatibility policy.

## 3. Non-goals

- Workflow code will not perform unrecorded network, filesystem, process, wall-clock, or random operations.
- The initial package will not replace Temporal Server or reimplement its wire service.
- The public OCaml API will not expose Tokio futures, Rust ownership, raw pointers, or Temporal Core's unstable C ABI.
- Rust activities are not required for OCaml workflow authors. Activities may be implemented in any compatible Temporal SDK. OCaml activity execution will still be added for parity.
- Native continuations will not be serialized. Durability comes from deterministic replay, matching Temporal's execution model.

## 4. Architectural Decision

### 4.1 Selected architecture

Use an OCaml language runtime over Temporal's Rust Core SDK:

```mermaid
flowchart LR
    App["OCaml application"] --> API["Public temporal OCaml library"]
    API --> Runtime["OCaml workflow runtime and effect scheduler"]
    Runtime --> Stub["Private OCaml/C stubs"]
    Stub --> Bridge["Versioned Rust static library"]
    Bridge --> Core["Pinned Temporal Core SDK"]
    Core --> Server["Temporal Server"]
    Server --> PG["PostgreSQL"]
```

The Rust bridge is a project-owned compatibility boundary. It wraps pinned Core crates and exports a small C ABI designed for OCaml. Core activation and completion messages cross that ABI as owned byte buffers. Opaque handles identify runtimes, clients, and workers. No Rust object layout becomes ABI.

The bridge provides blocking C entry points over Core's asynchronous operations. OCaml stubs release the OCaml runtime lock while a poll or completion waits in Rust, then reacquire it before returning an owned result. Rust/Tokio threads do not call arbitrary OCaml closures. This avoids foreign-thread callback hazards and keeps OCaml in control of its runtime.

Stateful native resources are owned as one graph by an OCaml supervisor actor per SDK instance. In a typical process that graph contains one Tokio/Core runtime, one configured cluster client, and one or a small number of task-queue workers. The actor runs on a dedicated Domain, accepts typed messages through a synchronized MPSC mailbox from any OCaml Domain, and returns one-shot `result` replies. It serializes handle creation, use, shutdown, and destruction; there is not a separate actor per native handle. Tokio remains responsible for network concurrency inside Rust, and each workflow execution continues to use its own deterministic effect scheduler.

The supervisor does not poll Rust on a timer. Rust signals an internal condition/event primitive whenever Core work becomes ready, and the supervisor's dedicated Domain/OS thread waits through a blocking C stub with its OCaml runtime lock released. Workflow effect continuations and general cooperative schedulers never execute this blocking wait. The call returns normally to OCaml to drain work. This provides callback-like wakeup latency without allowing arbitrary Tokio threads to enter the OCaml runtime; shutdown uses the same signal to unblock the wait safely.

Throughput optimization is deliberately deferred until correctness benchmarks
justify it. A later implementation may pipeline native transport, JSON
validation/codec work, and workflow execution on dedicated lanes, but the
single-owner ordering contract remains the source of truth: the public API
must not assume that those lanes are parallel or expose a second mutable owner.

### 4.2 Why not the Go SDK

The Go SDK includes a Go-specific cooperative workflow runtime. Embedding it would require adapting that runtime's workflow callbacks to OCaml or placing an additional recorded boundary around OCaml evaluation. Temporal Core instead exposes workflow activations and commands specifically so language runtimes can implement their own scheduler. That boundary maps directly to the problem this project must solve.

### 4.3 Why not HTTP or stdin as the primary path

A stateless OCaml evaluator reached through a recorded Local Activity can be replay-safe: the request includes prior decisions and ready results, and the response contains new commands or a final result. It remains a valid diagnostic and isolation fallback.

It is not the primary path because it adds serialization, process or network latency, Local Activity markers, deployment complexity, and repeated evaluation. The embedded Core design lets the OCaml runtime retain live continuations while cached and reconstruct them only when Temporal requests replay.

### 4.4 Why not a pure OCaml service client

A pure client would also require implementing worker polling, sticky queues, history processing, workflow state machines, retries, heartbeats, protocol evolution, and many correctness details already maintained by Temporal Core. It creates the largest parity and maintenance burden without improving the public OCaml workflow API.

## 5. Repository and Package Layout

The intended top-level layout is:

```text
.
├── Makefile
├── dune-project
├── temporal-sdk.opam
├── Cargo.toml
├── Cargo.lock
├── lib/
│   ├── public/          # Stable OCaml modules and .mli files
│   ├── runtime/         # Effect scheduler and activation interpreter
│   ├── protocol/        # Typed private JSON adapter values and validation
│   └── ffi/             # OCaml C stubs
├── rust/
│   └── bridge/          # Project-owned staticlib over Temporal Core
├── test/
│   ├── unit/
│   ├── replay/
│   ├── integration/
│   │   └── temporal/       # Compose, config, and scripts for the live test fixture
│   ├── compatibility/
│   └── load/
├── examples/
│   ├── hello_activity/
│   ├── concurrent_agent/
│   ├── child_workflows/
│   └── signals_queries_updates/
└── docs/
    ├── guides/
    ├── reference/
    ├── design/
    ├── decisions/
    └── progress.md
```

The experimental public OPAM package is named `temporal-sdk`. The Rust toolchain is a build dependency, not an application programming dependency. Cargo dependencies are locked, and the public OCaml package version controls which Core revision it embeds.

## 6. Public OCaml Programming Model

### 6.1 Ordinary functions first

Workflow logic is written as ordinary functions. Only the outer registration value contains Temporal metadata:

```ocaml
let research_agent input =
  let open Temporal.Result_syntax in
  let* completion = call_llm input.prompt in
  let* review = summarize_and_review input.document in
  Ok { completion; review }

let research_agent_workflow =
  Workflow.define
    ~name:"research_agent"
    ~input:Agent_input.codec
    ~output:Agent_result.codec
    research_agent
```

Helpers need no registration or special return wrapper beyond the `result` that makes operational failure explicit. Calling a helper does not create a child workflow or a separate history. A child exists only when code explicitly invokes `Child_workflow.start` or `Child_workflow.execute`.

### 6.2 Typed definitions and references

The principal public types are abstract:

```ocaml
type ('input, 'output) Workflow.t
type ('input, 'output) Activity.t
type ('value, 'error) Future.t
type 'value Codec.t
type Error.t
```

Workflow definitions may be registered locally or used as typed child references. Remote workflows and activities can be declared by name and codec without having a local implementation. Definitions carry safe defaults, while each invocation can override applicable options.

### 6.3 Activity and child execution

The same naming convention applies to both activities and children:

```ocaml
Activity.start   : ('i, 'o) Activity.t -> 'i -> ('o, Error.t) Future.t
Activity.execute : ('i, 'o) Activity.t -> 'i -> ('o, Error.t) result

Child_workflow.start   : id:string -> ('i, 'o) Workflow.t -> 'i
                         -> ('o, Error.t) Future.t
Child_workflow.execute : id:string -> ('i, 'o) Workflow.t -> 'i
                         -> ('o, Error.t) result
```

`start` schedules immediately and returns. `execute` is the convenience composition of `start` and `Future.await`.

### 6.4 Futures and concurrency

Futures are typed in both success and failure channels. Core combinators include:

- `Future.await`
- `Future.map` and `Future.map_error`
- `Future.both` and tuple variants
- `Future.all` for homogeneous collections
- `Future.race` for two heterogeneous success values
- `Future.first` for homogeneous collections
- `Future.cancel`
- readiness inspection that does not block

Scheduling multiple operations before awaiting naturally creates Temporal concurrency:

```ocaml
let enrich document =
  let open Temporal.Result_syntax in
  let summary = Activity.start Activities.summarize document in
  let entities = Activity.start Activities.extract_entities document in
  let* summary, entities = Future.await (Future.both summary entities) in
  Ok { summary; entities }
```

`Workflow.async` starts deterministic OCaml-only concurrent branches. Cancellation scopes provide structured concurrency. Scheduler order is deterministic: runnable continuations are processed FIFO by creation sequence, with activation job ordering preserved.

### 6.5 Higher-order composition

Partially applied `start` functions have the ordinary inferred shape `'input -> ('output, Error.t) Future.t`. That makes generic wrappers natural:

```ocaml
let race primary secondary input =
  let primary = primary input in
  let secondary = secondary input in
  Future.await (Future.race primary secondary)

let fastest_llm =
  race
    (Activity.start Activities.openai)
    (Activity.start Activities.anthropic)
```

Timeout, fallback, logging, compensation, batching, hedging, rate-limiting, and agent/tool orchestration can all be expressed as normal higher-order OCaml functions. The SDK will provide common combinators but will not force users into a framework-specific operation AST.

### 6.6 Explicit errors

Expected failures never require exceptions:

```ocaml
Future.await     : ('a, 'e) Future.t -> ('a, 'e) result
Activity.execute : ('i, 'o) Activity.t -> 'i -> ('o, Error.t) result
```

`Temporal.Result_syntax` supplies `let*` and `let+` for `result`. `Error.t` is structured and retains activity, child workflow, application, timeout, cancellation, termination, codec, update, Nexus, and bridge causes. Inspection uses stable accessors and views. `Future.map_error` supports domain-specific wrappers.

Business failures that are part of a successful workflow contract belong in the encoded output type. An outer `Error Error.t` means the Temporal execution itself should fail or cancel. Unexpected OCaml exceptions are caught only at the worker defect boundary and converted to a structured, non-retryable workflow task or workflow failure with diagnostics.

### 6.7 Signals, queries, and updates

Signals, queries, and updates are separately typed definitions with codecs:

```ocaml
let approve = Signal.define ~name:"approve" ~input:Approval.codec
let status = Query.define ~name:"status" ~output:Status.codec
let add_tool = Update.define ~name:"add_tool"
    ~input:Tool.codec ~output:Unit.codec
```

Handlers close over ordinary workflow state. Signal and update handlers may suspend where Temporal permits. Query handlers and update validators are enforced as non-suspending, read-only handler modes. The runtime rejects command-producing operations from those modes with structured errors.

## 7. OCaml 5 Suspension Model

OCaml 5 algebraic effects make direct-style suspension possible. Public functions such as `Future.await`, `Workflow.sleep`, and condition waits perform private effects when they cannot complete immediately. A deep effect handler owned by the workflow scheduler captures the one-shot continuation and records what must wake it.

The activation loop is:

1. Core returns a serialized `WorkflowActivation`.
2. The runtime locates or creates the execution keyed by namespace, workflow ID, and run ID.
3. Activation jobs resolve futures, deliver signals or updates, fire timers, request cancellation, or initialize the workflow.
4. Resolved wait conditions enqueue captured continuations.
5. The deterministic scheduler resumes runnable continuations until all fibers are complete or blocked.
6. Effects that schedule Temporal operations append commands and return typed future handles without blocking.
7. The runtime serializes a `WorkflowActivationCompletion` containing all commands and handler responses.
8. Core validates and sends that completion to Temporal Server.

Continuations remain only in worker memory. On an eviction activation, completion, or shutdown, the runtime removes the execution and every associated continuation. On replay, Core provides replay activations from history; the workflow starts again and reconstructs identical state and suspension points. No continuation serialization is required.

This model gives hot executions O(new activation work) behavior while retaining replay durability. Cache size and eviction are bounded and configurable.

## 8. Determinism

The runtime guarantees deterministic scheduling for SDK-managed effects but cannot make arbitrary OCaml code deterministic. The SDK therefore provides:

- Replay-safe `Workflow.now`, randomness, UUID, side effect, mutable side effect, and version/patch APIs.
- A workflow-safe logging API that suppresses duplicate replay logs by default.
- Runtime mode checks that reject Temporal operations outside workflows or from read-only handlers.
- Replay tests against stored histories.
- A determinism linter as the project matures, covering common wall-clock, random, filesystem, process, network, thread, unordered hash iteration, and unsafe global-state patterns.
- Documentation that distinguishes pure orchestration code from activities.

Hash-table iteration is not treated as a stable workflow order. APIs that turn maps into commands require deterministic ordering or sort keys internally where a comparison is available.

## 9. Payloads and Cross-language Interoperability

Every public boundary uses an explicit `'a Codec.t`. Temporal stores opaque
payload bytes and does not require JSON. The SDK provides a `json/plain` codec
for convenient interoperability with standard converters in other SDKs, as
well as binary, null, and eventually Protobuf codecs. Applications may use
another deterministic encoding when both sides agree on its metadata and byte
format.

Codec failures are structured errors with payload metadata and safe diagnostics. Raw payload access is available for dynamic workflows and forward-compatible integrations. Payload codecs and encryption/compression hooks operate outside deterministic user logic where required.

An optional PPX package may later derive codecs and typed definitions, but the base library never requires PPX.

## 10. Feature Parity Plan

Parity is organized by capability rather than by replacing the final objective with an MVP:

1. **Core runtime:** connection, worker creation, workflow polling/completion, workflow start/completion/failure, timers, remote activities, child workflows, futures, replay, eviction, and cancellation.
2. **Interactive workflows:** signals, queries, updates, update validators, conditions, handler concurrency, and handler completion policies.
3. **Advanced commands:** continue-as-new, external signal/cancel, side effects, mutable side effects, patches/versioning, search attributes, memo, child policies, retry/timeouts, and workflow information.
4. **Worker breadth:** OCaml activities, local activities, heartbeats, async completion, worker shutdown, interceptors, payload codecs, metrics, tracing, and worker tuning/versioning.
5. **Platform breadth:** client workflow operations, schedules, visibility/list/count, reset/terminate/cancel, update handles, Nexus operations, testing server controls, replay tooling, and task-queue priority/fairness.
6. **Publication hardening:** API compatibility gates, multi-platform artifacts, performance tuning, security review, exhaustive guides, and OPAM release automation.

Each capability lands with unit tests, an integration or replay test where
relevant, documentation, and a verified commit. Convenience APIs inspired by
other SDKs are added after their underlying Core operations and replay behavior
are proven. Their OCaml API is designed independently rather than copying the
source SDK's classes, exception model, or concurrency abstractions.

## 11. Docker Compose, Makefile, and Kubernetes

The development contract is Makefile-first. Expected targets include:

- `make build`
- `make test`
- `make test-unit`
- `make test-integration`
- `make test-replay`
- `make lint`
- `make fmt`
- `make up`
- `make down`
- `make logs`
- `make clean`
- `make docs`
- `make bench`
- `make verify`

Docker Compose includes PostgreSQL health checks, Temporal schema initialization and server, Temporal UI, the OCaml worker, and representative external activity workers. Integration tests wait on health, start workflows through a client, assert results and histories, restart the worker, and verify replayed completion.

The worker image uses a multi-stage build and a minimal non-root runtime image. Configuration comes from environment variables and mounted secrets, matching Kubernetes ConfigMap/Secret deployment. The worker is stateless apart from its bounded sticky execution cache, so replicas scale horizontally on the same task queue. Graceful termination stops polls, drains in-flight activations within a deadline, reports shutdown, and exits for Kubernetes termination handling.

## 12. Testing and Performance

### 12.1 Test layers

- Pure OCaml unit tests for codecs, error views, option builders, futures, scheduler ordering, and handler modes.
- Rust unit tests for ABI ownership, buffer lifetime, shutdown, and Core translation.
- C ABI smoke tests to detect symbol or calling-convention drift.
- Deterministic runtime tests using synthetic activations and expected completions.
- Replay tests using checked-in histories.
- Docker integration tests against Temporal Server and PostgreSQL.
- Cross-language tests where OCaml workflows call activities and children implemented in another SDK.
- Restart and eviction tests that prove continuation reconstruction.
- Failure tests for retries, timeouts, cancellation, malformed payloads, unavailable workers, and nondeterminism.
- Load tests for workflow activation throughput, scheduling fan-out, memory per cached workflow, replay cost, and bridge overhead.

### 12.2 Performance principles

- One native process and no per-activation network hop beyond Temporal polling.
- Protobuf bytes cross the FFI boundary once in each direction per activation.
- Hot executions resume captured continuations instead of re-running from the beginning.
- Commands are batched into one activation completion.
- Rust buffers have explicit ownership and a single release path.
- Public codecs avoid unnecessary intermediate JSON values when binary/protobuf codecs are selected.
- Worker poller counts, cache size, and concurrency are configurable.

Benchmarks establish regression budgets before optimization claims are made. Performance documentation reports hardware, workload, command, percentile latency, throughput, and memory rather than unsupported adjectives.

## 13. Observability and Operations

Temporal Core metrics and tracing are exposed through project-owned configuration. OCaml workflow logs include namespace, task queue, workflow ID, run ID, workflow type, and replay state. Sensitive payloads are never logged by default.

Worker startup reports SDK versions, embedded Core version, build ID, namespace, task queue, and enabled capabilities. Health endpoints or probes distinguish process liveness, Temporal connectivity, polling readiness, and graceful shutdown.

## 14. Open-source Readiness

The project is designed for publication, not merely internal use:

- Apache-2.0 licensing with source headers where required.
- `README.md`, `CHANGES.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, and release instructions.
- Complete public `.mli` files with `odoc` comments and runnable examples.
- Semantic versioning for the OCaml API and an explicit Core compatibility matrix.
- CI for supported OCaml 5 releases on Linux and macOS, amd64 and arm64 where runners permit.
- Formatting, linting, unit, replay, integration, documentation, package, and example-build gates.
- Reproducible `Cargo.lock`, pinned container images, and dependency/license review.
- OPAM metadata and package linting.
- A deprecation policy and migration guides for breaking releases.
- Issue and pull-request templates plus a public feature-parity tracker.

### 14.1 Dependency license policy

All production, development, build, generated-code, container-image, and transitive dependencies are subject to a release-blocking license audit.

The default allowlist is:

- MIT
- Apache-2.0
- BSD-2-Clause
- BSD-3-Clause
- ISC
- Zlib
- PostgreSQL
- similarly permissive licenses approved and recorded in a dependency review

The only standing copyleft exception is `LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception`, and only for the OCaml compiler/runtime or an explicitly reviewed OCaml ecosystem dependency. The linking exception must permit distribution of the final executable under the application's chosen terms. An unmodified LGPL license is not equivalent and is prohibited.

GPL, AGPL, ordinary LGPL, MPL, EPL, CDDL, SSPL, BUSL, Commons Clause, source-available licenses, non-commercial licenses, and dependencies with unknown or missing terms are prohibited. Test-only dependencies follow the same policy so the public repository and release process remain uncomplicated.

CI generates an inventory and fails on unapproved licenses. Rust dependencies are checked from `Cargo.lock` with `cargo-deny` or an equivalent lockfile-aware scanner. OPAM dependencies and their transitive closure are checked from the locked switch metadata. Container images and bundled artifacts are included in the software bill of materials. Every exception is recorded with the exact package, version, SPDX expression, scope, rationale, and approving project decision.

The initial compatibility floor is OCaml 5.2 or newer. CI includes the oldest supported compiler and the current stable compiler. The implementation may use compiler-version compatibility shims internally while keeping one public API.

## 15. Risks and Mitigations

### Core API changes

Temporal Core and its C bridge can change. The project pins Core, owns its Rust ABI, and updates through compatibility tests rather than exposing upstream ABI directly.

### Effect runtime correctness

Continuation bookkeeping, handler concurrency, and cancellation are subtle. The scheduler remains a small isolated module tested against synthetic activations, replay histories, and restart scenarios. Every continuation has one owner and one legal resume/discontinue transition.

### OCaml runtime blocking

Temporal polls may wait indefinitely. C stubs release the OCaml runtime lock around bridge calls. Shutdown handles unblock pending polls before worker destruction.

### Error API evolution

Adding public variant constructors can break exhaustive matches. `Error.t` remains abstract; stable views and accessors expose detail, with new categories introduced under semantic-versioning rules.

### Cross-language payload mismatch

Compatibility fixtures are generated and consumed by multiple SDK languages. Metadata and JSON/protobuf behavior are tested byte-for-byte where the Temporal format requires it.

### Scope pressure from full parity

The parity tracker maps every supported Core activation job, command, client operation, activity operation, and worker option to implementation, tests, and docs. Incremental releases may be partial, but missing capabilities remain visible and cannot be mistaken for completion.

## 16. Acceptance Criteria

The project goal is complete only when current evidence demonstrates all of the following:

1. A fresh checkout builds, tests, and runs through documented Makefile commands and Docker Compose.
2. Compose runs PostgreSQL, Temporal Server, Temporal UI, and the required workers with health checks.
3. A Dune-built OCaml native executable contains and uses the Rust/Core bridge without a sidecar.
4. OCaml workflow examples schedule activities and children concurrently, await one or all, sleep, cancel, and complete with typed results.
5. Worker restart and cache eviction replay workflows to the same commands and final results.
6. Ordinary OCaml helper and higher-order functions wrap activities and workflows without registration or framework-specific monads.
7. Expected failures are explicit `result` values throughout the public workflow API.
8. Cross-language activities and child workflows exchange compatible payloads.
9. Every capability claimed in the parity matrix has implementation, tests, and user documentation.
10. Benchmarks cover activation throughput, bridge overhead, replay, fan-out, and cached-workflow memory.
11. Public interfaces, guides, examples, contribution files, security policy, changelog, license, and OPAM metadata are ready for an external release.
12. A complete dependency and container SBOM passes the permissive-license policy, with only documented OCaml linking-exception entries.
13. The complete verification suite passes from a clean environment with no uncommitted generated artifacts.

## 17. Primary References

- [Temporal Core SDK architecture](https://github.com/temporalio/sdk-core/blob/95e97686a079dcfe6c42e3254b2f3f5e3d97408f/ARCHITECTURE.md)
- [Temporal Core SDK repository](https://github.com/temporalio/sdk-core)
- [OCaml 5 Effect module](https://ocaml.org/manual/5.4/api/Effect.html)
- [OCaml C interoperability and embedding](https://ocaml.org/manual/5.5/intfc.html)
- [Temporal workflow determinism](https://docs.temporal.io/workflow-definition)

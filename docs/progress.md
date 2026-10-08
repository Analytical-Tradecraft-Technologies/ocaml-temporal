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
[commit-pinned live evidence audit](reference/live-acceptance-coverage.md)
records the current tested source, named assertions and successful CI job for
the Temporal acceptance controllers.

## 2026-10-08: Memory, history, cache and fan-out benchmarks (#527, #528)

The shared benchmark harness now has an instrumented mode. Each repetition
records `Gc.quick_stat` allocation deltas per phase, and OCaml heap and live
bytes (`Gc.stat`) plus process RSS (procfs on Linux, `ps` elsewhere) before
load, after warmup, after measurement, after close and after compaction. It
also records recovery deltas that separate retained live data from allocator
capacity, and a cross-repetition retention trend. Three server-free suites
drive the production `Native_worker_execution` adapter through an in-memory
source that decodes pre-encoded bridge JSON exactly as the supervisor does:
`history-replay-memory` (synthetic sequential-activity histories, run at
1k/10k/50k equivalent events by `make bench-history`), `workflow-cache-memory`
(steady residency and `Cache_full` eviction with replayed reload, run by
`make bench-cache`), and `activity-fanout` (1,000-activity fan-out at one and
eight interleaved runs, run by `make bench-fanout`). An untimed attribution
pass splits adapter time into activation decode, completion encode and copy,
and workflow/adapter work.

Indicative local results are in
[the benchmark reference](reference/benchmark-harness.md#indicative-results-and-bottlenecks).
Replay costs about 25 µs and 80 KB of allocation per activation, independent
of history length. A run holds about 3.9 KB of live OCaml data, a resident
timer workflow about 3.3 KB, and no repetition-over-repetition retention was
observed. OCaml-side JSON decoding and encoding take about 64% of adapter time
with small payloads. No thresholds were added. Rust/serde, Core and live-server
fan-out are not yet measured.

Evidence: `dune build @test/benchmark/runtest` runs tiny smoke
configurations of all three suites and validates their memory report sections
with `check_report.exe`, including that the fan-out suite's derived
`fanouts_per_second` and `activities_per_second` equal samples per second
times the work in each sample. `test_benchmark_harness.exe` checks that the
`after_close` and `after_compact` snapshots, taken only after the workload's
owning scope has returned, exclude a released 32 MiB dummy workload, and that
the fan-out fixture bound (512 MiB of estimated base64-encoded results,
checked by division) rejects oversized configurations before building them.

## 2026-10-09: Query and suspended-update recovery after eviction and restart (#530)

`test/integration/interaction_recovery/regression.exe` joins
`LIVE_REGRESSION_EXECUTABLES`, so `make test-temporal-live-regressions` runs it
against the Compose stack. It runs three executions of one workflow whose
`add` update handler suspends on a signal and then starts a durable timer:

- **Eviction:** on a worker with a one-entry sticky cache, a filler workflow
  forces the run out of the cache after the update is accepted. Queries then
  answer from replayed state, and the release signal resumes the handler from
  history.
- **Restart:** the worker process that accepted the update is terminated, and a
  fresh process answers queries and completes the update. History attributes
  acceptance to the original process and completion to the replacement.
- **Control:** the same commands with no queries, rejected updates, eviction,
  or replacement.

A `probe` query reports per-process counts of workflow-body starts and
validator calls, proving that answers come from a replay and that replaying an
accepted update does not re-run its validator. Exact run histories, read with
the pinned CLI, show the update accepted but not completed while suspended,
then one acceptance and one completion with the original update ID. A handle
re-attached by update ID returns the same result. Apart from workflow-task
events, both recovered histories equal the control run's, compared by event
type and an allowlist of meaningful attributes (payloads, update and signal
names, update ID and outcome, timer duration).

Evidence: `make test-temporal-live-regressions` passed all six suites in the
Linux development image (OCaml 5.2) against a fresh Compose Temporal
1.32.0/PostgreSQL stack. Eight sequential and three concurrent runs of this
suite also passed against that stack with the driver and workers built on
macOS (OCaml 5.4.1). As a sensitivity check, forcing
`run_validator = true` for replayed updates made the suite fail at the first
post-eviction query, and giving only the recovered runs a different timer
duration failed the control comparison. No SDK defect was found.

## 2026-10-08: Successful activity completions require a payload (#954)

The activity completion protocol accepted `{"kind":"completed","result":null}`
and converted it to Core's `Success { result: None }`, which Core's
`validate_activity_completion` rejects as malformed at the pinned commit. The
bridge then returned `STATUS_WORKER` for a document it had accepted, and the
lease stayed held. The lifecycle stress in #953 found this; the OCaml executor
always sends a payload, so the public API could not reach it. The Rust
`ActivityCompletionResult::Completed` and OCaml `Activity_protocol.Completed`
now carry a required payload, both decoders refuse `null` with a protocol error
at `$.result.result` before any Core call, and both completion schemas require
the payload. A void result is an encoded payload whose data may be empty; the official SDKs
send a `binary/null` payload with empty data for one.
The Rust validator also checks the success payload's limits at decode, as it
already did for heartbeat details. The ABI version is unchanged: the OCaml
sender never produced the refused shape. `rust/core-bridge/tests/activity_protocol.rs`,
the new `rust/core-bridge/tests/activity_null_completion.rs` (worker ABI
against a loopback gRPC double: corrected completion and reject both retire
the lease exactly once after the refusal), and
`test/bridge/test_ocaml_activity_protocol.ml` cover it.

## 2026-10-08: Client handles by workflow ID and typed completed successors (#791, #837)

`Temporal.Client.get_handle client ~workflow ~id ?run_id ()` builds a typed
handle for a workflow the caller did not start, without contacting Temporal.
With `~run_id` it addresses that exact run; without it the handle addresses
the workflow's current run. Every request after start now accepts an empty
run ID in the private protocol (OCaml encoders, Rust validators, and the
bridge schemas), which the bridge forwards so Temporal resolves the latest run
for wait, signal, query, cancel, terminate, reset, update, and update polls.
A current-run update adopts the run Temporal reports as having accepted it,
and the update handle keeps polling that run. `Client.wait` on a current-run
handle follows continued-as-new, cron, and retry successors with exact-run
waits until a run closes without one, matching the official SDKs' default
result following; exact-run handles still never follow implicitly.
`Client.run_id` now returns `string option` (`None` for a current-run
handle). `Completed` became `Completed { output; successor }`, so a completed
cron or retry run's successor (already decoded by the bridge from
`new_execution_run_id`) is no longer discarded (#837). The already-started run
ID half of #837 was resolved by `Client.already_started` (#936). The bridge
ABI stays at version 4; [core-bridge.md](reference/core-bridge.md) records
why the selector is additive.

Evidence: `rust/core-bridge/tests/support/client_current_run.rs` drives every
run-addressed operation from its JSON document with an empty run ID through a
Core connection backed by the in-memory callback transport and checks the
empty run ID reaches Temporal, the wait response echoes it and keeps a
completed successor, a current-run update adopts the resolved run, and a
missing or changed run fails closed. `test/bridge/test_ocaml_client_protocol.ml`
and `test/bridge/test_ocaml_client_update_protocol.ml` check the OCaml encoders
and decoders; `test/bridge/test_client_terminal_adapter.ml` checks the
completed successor through the backend and the public client, and that a
current-run wait follows completed, failed, timed-out, and continued-as-new
links while an exact-run wait returns them; `test/unit/test_client_worker.ml`
covers `get_handle` validation and a current-run handle signalling and
terminating a reset successor on the mock. The live regression
`test/integration/client_request_ids` signals, queries, and waits on a
workflow by ID alone against Temporal Server.

## 2026-10-09: Deterministic race matrix (#520)

`test/runtime/test_race_matrix.ml` drives the private workflow runtime with
scripted Core activations, so each competing ordering is forced exactly. Its
35 rows cover these races:

- activity completion against workflow cancellation;
- an activity cancel request against the activity's own completion;
- a timer against cancellation;
- scope cancellation propagating to an activity or child, against that
  operation's completion or start failure;
- child completion and start failure against parent cancellation;
- signal and update handlers against root completion and cancellation;
- local-activity retry backoff against cancellation.

Rows use only job orders that the pinned Core can deliver, citing Core's
activation job-ordering contract. Each row asserts the rendered command
history of every activation, the trace of what workflow code observed, and
the fate of every delivered update (completed, rejected, abandoned, pending,
or unanswered).

Generic checks reject any history with a second terminal command, a command
after the terminal one, a task failure, or a late, repeated or out-of-order
update response. Core never activates a run after its terminal command, so
the harness also refuses any scripted step after one; a late job is covered
only by replaying into a fresh execution. Self-tests prove that each check
and this guard catch a synthetic violation. Each row runs twice on fresh executions, as Core redelivery would.
Rows whose updates were accepted also replay with validation disabled and a
validator that would now reject, and must reproduce the same history.

The matrix pins the current contract: immediate workflow cancellation (#514),
first terminal wins, and the SDK does not wait for unfinished handlers. It
found two orderings that drop handler work Core expects to run. A signal or
update in the same activation as `Cancel_workflow` is never invoked, so the
update receives no validation response. An update continuation that is ready
in the activation where the root completes first is discarded. Both choices
are replay-visible and interact with the #514/#489 cancellation decision, so
they are pinned and linked to #962 rather than changed here. The
[interaction reference](reference/interactive-workflows.md#handler-races-with-completion-and-cancellation)
lists the outcomes.

Evidence: `dune build` and `dune test test/runtime test/unit` pass on OCaml
5.4.1 (macOS ARM64). The matrix printed the same result in 20 consecutive
runs. Changing one pinned update fate made the test fail with both histories
printed. Caller-visible outcomes of abandoned and unanswered updates still
depend on server behavior, which this offline matrix does not exercise.

## 2026-10-08: Application-linked replay command example (#516)

`examples/replay` wraps `Temporal.Replay` in a copyable, offline command:
`replay_command.ml` parses `--workflow-id ID FILE...` (repeatable, so several
executions are checked in one run), replays each binary `History` file in its
own native graph, prints `PASS`/`FAIL` per history plus a summary, and exits
with the first failing history's status: 1 nondeterminism, 2 workflow task
failed, 3 invalid, 4 unsupported, 5 replay could not run, 64 usage and 66
unreadable file. `replay_history.ml` is the only application-specific part; it
links the example workflow. The command is a `test` stanza, outside the live
example gate, because it never connects to a server. JSON-history input stays
out of scope: the pinned Core's JSON form differs from the CLI's, so the
example documents the `temporal workflow show --output json` to protobuf
conversion, and those steps reproduce the checked-in history byte for byte.

Evidence: one run of the three example processes against the Compose stack
(Temporal Server 1.32.0, OCaml 5.4.1 on macOS arm64) was exported with the
pinned admin-tools CLI and converted as documented, giving
`examples/replay/histories/compose-message-ada-lovelace.pb` (SHA-256
`525da46b18f522772491ecdb0e6f82ffaf7e51f0285cf184eefdd167cb8adbb1`).
`dune test` replays it with the example executable, and
`test/replay_cli/test_replay_cli.ml` runs the real executables to check exit 0,
a removed timer (1), a defect (2), non-protobuf and empty files (3), a
duplicate registration (5), usage errors (64), a missing file (66), and that
a multi-history run reports every history before returning the first
failure's status. Unsupported histories (4) have no reliable trigger yet and
are covered only by the mapping.

## 2026-10-08: Actionable replay nondeterminism diagnostics (#529)

`Temporal.Replay.Nondeterminism` now carries a `mismatch` record besides
Core's complete `message`: the history's workflow ID, the workflow type from
the run's start job, the first unmatched recorded event's ID and type, the
command state machine Core matched it against (normally the command the
changed code produced), and Core's mismatch sentence unwrapped from its
failure envelope. Extraction is best effort over Core's text and fails
closed: anything Core does not state is `None`, and no OCaml source location
is invented. `failure_message` keeps its `nondeterminism (run RUN_ID): `
prefix and now renders that context with a reminder to guard intentional
changes with `Temporal.Workflow.patched`. Every rendered failure is one line
of at most about 3 KB: interpolated values are escaped and each is bounded
(256 bytes for identifiers, 1,024 for Core's reason) without splitting UTF-8,
while the record keeps the exact values. The workflow guide has a
troubleshooting example for locating an incompatible change and states that
a clean replay covers only recorded paths. No ABI, bridge, or Rust change was
needed; a live worker already reports the same Core text on the
`WorkflowTaskFailed` event and in Core's `WARN` log.

Evidence: `test/bridge/test_replay_diagnostics.ml` replays the corpus
`negative-timer-removed` control through the public API and asserts workflow
type `corpus.timer`, workflow ID `history-corpus-timer`, event 5
`TimerStarted`, command `Complete workflow`, Core's exact reason and a
one-line rendering without the recorded payload; the
`negative-patch-active-on-legacy` control asserts the patch ID in the reason
with the event and command left `None`. `test/bridge/test_public_replay.ml`
asserts the event and command of a removed timer, an activity in place of a
timer, and an added timer (event 16 `WorkflowExecutionCompleted` against a
`Timer` command), and renders a 60 KB workflow ID containing line breaks
and control bytes as one escaped, truncated line while the record keeps the
exact ID, with task failures and invalid input still classified
separately. The installed-consumer witness binds every new field.

## 2026-10-08: Seeded bridge lifecycle stress (#522)

`rust/core-bridge/tests/lifecycle_stress.rs` generates reproducible operation
sequences over two runtime slots and runs them against the C ABI. The
operations cover:

- runtime create, free, and GC-fallback dispose, including double release;
- the replay-worker lifecycle (feed, poll, complete, reject, finalize, drain,
  dispose);
- live client connect and disconnect;
- an activity worker against a gRPC double (poll, complete, reject, shutdown);
- stale and repeated completions, calls on released slots, and the panic
  probe.

A model and two ledgers check every step, and check again after teardown:

- each runtime is created once and cleaned up exactly once;
- a released slot is null and rejects every call;
- no lease is delivered twice or retired twice;
- shutdown reports exactly the leases still held;
- the server sees exactly one completion for every token that was leased,
  whether an explicit call, shutdown, free, or dispose retired it;
- a drained replay always finalizes.

A failing case is minimized and reported with its seed, case number,
reproduction command, and per-operation trace. CI keeps the report as an
artifact.

`test/bridge/test_ocaml_lifecycle_gc_stress.ml` runs seeded runtime cycles
through the OCaml bindings, with collections and compactions between calls.
Each cycle either closes its runtime explicitly or leaves it to the
custom-block finalizer. The test also runs real supervisor create and
shutdown cycles. `make test-lifecycle-stress` and
`make native-test-lifecycle-stress` run both tests with a configurable seed
and budget. The [lifecycle stress reference](reference/bridge-lifecycle-stress.md)
gives budgets and the instrumentation scope of each tool.

Evidence: the default Rust run (24 cases × 48 operations, seed `0x05222026`)
and the scripted regressions pass in about 8 seconds locally. A 300-case run
with seed `0x7e57` (14,400 operations) also passed, and so did the OCaml
stress at 1,000 cycles. Neither found a lifecycle defect.

One finding was outside the lifecycle scope. The semantic activity protocol
accepts a `completed` result with a `null` payload, but Temporal Core rejects
that completion, and the bridge reports `STATUS_WORKER` with the lease still
held. The OCaml executor always sends a payload, so the stress does the same.

The stable Rust test is not sanitizer-instrumented. The repository has no
scheduled stress job yet. Concurrent multi-Domain stress also remains under
#506.

## 2026-10-06: Rendered API documentation gate (#794)

`make docs` builds the odoc API documentation with odoc warnings fatal in the
dev profile, and CI runs it once in the OCaml 5.2/amd64 `verify` lane. odoc is
a CI-only tool installed from the exact closure in `scripts/docs-tools.locked`
by a separate `docs` stage of `Dockerfile.dev`; it is not an SDK dependency.
The existing defects were fixed in doc comments only: unterminated `[...]`
interval spans, ambiguous and unresolved references, field and constructor
comments in `Client` that were not doc comments, missing module synopses, and
a `Temporal` preamble that described Dune internals. A package landing page
(`lib/public/index.mld`) adds a quick start, and `@canonical` tags render the
private context, async-handle, and future types under their public names.
Verified locally with odoc 3.2.1 on OCaml 5.4.1; the quick-start snippets
type-check against the library.

## 2026-10-08: Public offline replay API (#515)

`Temporal.Replay` lets an application replay a recorded workflow history
against its registered workflow definitions without a server or credentials.
`History.of_protobuf ~workflow_id` accepts the binary `History` protobuf;
`replay` and `replay_all` return `Ok ()` or a typed `Nondeterminism`,
`Workflow_task_failed`, `Invalid_history`, `Unsupported_history`, or
`Replay_error`. No new engine or ABI symbol was added: each call drives the
existing private replay operations with the production workflow adapter, as
the cold-replay benchmark does, and shuts its native graph down on every path.
The only kernel change is an alias for the shared payload wrapper used to build
the replay document.

Evidence: `test/bridge/test_public_replay.ml`, using only the public API,
replays all five retained live task-failure histories with compatible code,
reports a removed timer and an activity-for-timer change as nondeterminism,
reports a defect and an unregistered type as task failures, reports
non-protobuf, truncated and event-free input as invalid, rejects invalid
registrations before native allocation, and completes 140 alternating
successful and nondeterministic replays (more than OCaml's simultaneous Domain
limit). The installed-consumer witness covers the new signatures. A replay CLI
(#516), richer mismatch context (#529), JSON-history input and a persistent
corpus (#503) remain open.

## 2026-10-08: Detect and report non-yielding workflow code (#493)

A workflow activation that runs workflow code past a configurable deadline
without returning is now detected, reported, and its workflow task failed.
The workflow lane publishes each activation (a fresh epoch, the activation,
and its workflow ID and type) in an atomic cell before any user code runs. A
watchdog Domain started by `Worker.run` samples that epoch every quarter of
the deadline, counting ticks rather than reading a clock, and asks the adapter
to abandon an epoch seen for the whole deadline. Lane and watchdog race on one
atomic claim per activation, so exactly one completes the native lease: the
watchdog submits the ordinary adapter failure completion through the
supervisor, and a lane that later returns drops its completion and its run
without a native call. The first detection is logged once
(`workflow_activation_deadline_exceeded`, with new `temporal.workflow_id` and
`temporal.run_id` tags) and exposed by the new lock-free
`Temporal.Worker.health` as a sticky `Stuck_workflow_activation`. The deadline
is `Worker.Options.make ?workflow_activation_deadline` (default two seconds,
the Python SDK's deadlock timeout; `` `Disabled `` for debugging). The stuck
code is never interrupted; recovery is an external process restart, and the
guide documents liveness-probe wiring.

Evidence: `test/runtime/test_native_worker_watchdog.ml` spins a workflow on a
test-controlled flag and checks one task failure per stuck lease, the dropped
late completion and subsequent eviction acknowledgement, sticky health, a real
watchdog Domain detecting within its bound and firing once, unaffected quick
activations, and public option validation. `dune build`, `@doc`, and the
runtime, unit, SDK-supervisor, bridge, observability, and API-witness suites
pass on OCaml 5.4.1. A live subprocess qualification against Temporal Server
and the bounded-shutdown interaction (#495) remain outstanding.

## 2026-10-07: Continue-as-new suggestion reasons (#792)

`Temporal.Workflow.Info.continue_as_new_reasons` reports why the server
suggested continuing as new, as the polymorphic variants
`` `History_size_too_large ``, `` `Too_many_history_events `` and
`` `Too_many_updates `` matching the pinned Core enum. The reasons already
crossed the bridge in activation metadata; now that the native adapter
translates each activation once (#846), it installs them in the execution's
task-local history snapshot next to `continue_as_new_suggested`, so they come
only from activation metadata, replay identically, and are cleared by an
activation without metadata. Core's unspecified zero value names no cause and
is dropped, so a suggestion may carry an empty list, as older servers send.
The bridge and OCaml decoders already reject unknown reason values, so no
protocol or ABI change was needed. `test/runtime/test_execution_info.ml`
checks order preservation, placeholder removal, the live and replayed paths,
and the reset on the next activation. This completes #792.

## 2026-10-06: Typed client RPC errors and query handler failures; bridge ABI v4 (#823)

Client RPC failures are classifiable without parsing messages. Every closed
`rpc` code becomes a `` `Bridge `` error with a stable PascalCase
`Error.error_type` (`NotFound`, `Unavailable`, `TerminationOutcomeUncertain`,
...) and the new `Client.rpc_status` variant. `non_retryable` is set exactly
for the permanent conditions (invalid argument, not found, already exists,
failed precondition, permission denied, unauthenticated, unimplemented, and
uncertain termination); the other codes follow Temporal Core's retryable set
plus deadline exceeded and cancelled. A failed workflow query handler, which
Temporal reports as `InvalidArgument` with a `QueryFailedFailure` detail, is
now a distinct query-only `query_failed` bridge document carrying the handler's
message (at most 4,096 bytes, NUL replaced) and becomes a non-retryable
`` `Workflow `` error with type `QueryFailed`, recognized by
`Client.is_query_failed`. The mock client reports an unknown workflow, a
mismatched run, and a signal to a closed run as the same typed `NotFound`.

The new error kind is rejected by a version 3 OCaml decoder, so the bridge ABI
moved from 3 to 4 (`ABI_VERSION`, the C header constant,
`Native_bridge.abi_version`, and the `ocaml_temporal_core_v4_` symbol prefix).

Evidence: Rust tests in `rust/core-bridge/tests/support/client_errors.rs`
cover every status code's mapping, the `QueryFailedFailure` type-URL check,
message bounding, and query, permanent-status, and signal failures through a
real Core connection over Core's callback transport; OCaml protocol, supervisor
adapter, and public classification tests
(`test/bridge/test_client_rpc_errors.ml`) cover decoding, operation-specific
rejection, and the full classification table. The live completed-query
regression now requires a missing and a refusing query handler to be typed
query failures with the handler message and a signal to the completed run to
be a non-retryable `` `Not_found ``; it passed locally against the Temporal
CLI 1.5.1 development server (Temporal Server 1.29.1) with Temporal Core
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`, and CI runs it against the Compose
stack. The live smoke driver's unknown-query and uncertain-termination checks
now use the typed classification.

## 2026-10-06: One translation and one completion encode per workflow task (#846)

The native workflow worker used to translate (and so canonically re-encode)
each activation twice, once for registry lookup and again inside
`Native_execution.activate`, and to encode each completion twice, once as
validation in `Native_execution` and again in the supervisor before the C
call. The worker now passes the private `translated_activation` (which keeps
its source activation) to `Native_execution.activate_translated`, and the
completion's single encoder pass produces an abstract
`Temporal_protocol.Encoded_workflow_completion.t` that the adapter retains
and the supervisor's `Complete_workflow`/`Complete_replay_workflow` copy into
C unchanged. Retained completions are now those immutable bytes, so the
payload deep copy is gone and a retry resubmits the identical string; an
adapter-built completion the encoder rejects fails closed without a native
call, as before. All validation is kept, and the wire bytes are unchanged.
The adapter's `complete_workflow` source operation also receives the typed
completion read-only (`~completion`), so test sources and the cold-replay
benchmark inspect commands without decoding the submitted JSON inside the
measured path; the production supervisor ignores it. Runtime tests check that submitted bytes equal the canonical encoding of the
completion, that a retryable rejection resubmits the physically identical
string without rerunning the workflow, and that the retained bytes do not
change when a typed payload buffer is mutated afterwards. On a scratch
harness (OCaml 5.4.1, Apple M4 Pro, release profile), ten activations each
carrying a 2 MiB activity result in and a 2 MiB activity input out took a
median of 6.39 s before and 3.19 s after, measured on the codec that was
current before #923.

## 2026-10-06: Bounded Tokio worker pool per runtime (#832)

Every client and worker built its Core runtime with Tokio's default of one
worker thread per core, so a client plus a worker on a 64-core host cost
about 135 threads. Runtime creation now resolves an explicit worker count:
`Client.create` and `Worker.create` accept `?io_threads` (1 to 256,
validated as a typed defect before anything is allocated), and the default is
the host's available parallelism capped at 4. The count reaches Tokio through
the new `ocaml_temporal_core_v3_runtime_new_with_worker_threads` symbol; the
existing `ocaml_temporal_core_v3_runtime_new` keeps its signature and uses the
default, so this additive symbol needs no ABI version change beyond v3. Rust integration tests read the pool
size back from Tokio's metrics for explicit counts, both range ends, and the
default, and prove an oversized count is rejected without a handle; the C ABI
harness covers both outcomes and an OCaml test covers bridge and public
validation for mock and native targets. The public argument is named
`io_threads` and documented only as an upper bound on network and
server-communication threads, so applications are not coupled to the private
Tokio executor; the Tokio mapping is recorded in `docs/reference/core-bridge.md`.
Sharing one runtime between instances remains future work.

## 2026-10-06: Every client RPC uses Core's retry layer (#820)

Only workflow start went through Core's retrying `Connection`; every other
client RPC called the raw `workflow_service()` stub, so `Client.wait` failed
about a second after the Temporal Server stopped. Wait, signal, query, cancel,
terminate, reset, update, update polling, and visibility listing now use the
retrying connection. Bounded RPCs fit Core's retry window and each attempt's
gRPC deadline inside their budgets; the control-RPC budget grew from one to
three seconds so that Core's 1 s +/-20% throttle wait before re-sending after
`resource_exhausted` fits. Terminate has no idempotency key, so it is re-sent
only after `resource_exhausted` and reports `unavailable` as
`termination_outcome_uncertain` instead of risking a misleading `not_found`
from a blind re-send. The history long poll allows thirty consecutive attempts
per poll. Callback transport tests in
`rust/core-bridge/tests/support/client_retry.rs` prove recovery after one
`unavailable` reply for each idempotent RPC with byte-identical re-sends, a
signal and a terminate delivered after one `resource_exhausted` inside the
control budget (the signal case fails with the old one-second budget), no
retry of `not_found`/`invalid_argument` or of ambiguous terminate statuses, an
uncertain result for an unavailable terminate, and a persistently unavailable
signal returning within its budget. See
[the Core bridge reference](reference/core-bridge.md#native-client-start-and-exact-run-wait).

## 2026-10-06: Workflow ID conflict policies for Client.start; bridge ABI v3 (#933)

`Temporal.Client.start` accepts `?id_conflict_policy` (`` `Fail ``,
`` `Use_existing ``, `` `Terminate_existing ``; default `` `Fail ``). The
policy is always sent as an explicit
`StartWorkflowExecutionRequest.workflow_id_conflict_policy`, never
`UNSPECIFIED`, and takes part in the pending request-ID equality check. Start
responses and outcomes now carry Temporal's `started` flag, exposed as
`Client.started`. A `` `Fail `` conflict returns a non-retryable `` `Workflow ``
error with type `WorkflowExecutionAlreadyStarted`, and
`Client.already_started` recovers the running execution from it.

The start request, response and outcome schemas changed incompatibly in both
directions, so the bridge ABI moved from 2 to 3: `ABI_VERSION`, the C header
constant, `Native_bridge.abi_version` and the `ocaml_temporal_core_v3_` symbol
prefix change together, and a stale Rust archive or OCaml object now fails at
link time or during startup negotiation rather than on every start.

Evidence: protocol round-trip and strictness tests in
`test/bridge/test_ocaml_client_protocol.ml`, Rust client-start protocol tests,
deterministic `mock://` backend tests for all three policies in
`test/unit/test_client_worker.ml`, and the Rust, C harness and OCaml ABI
negotiation tests for version 3. The live client request-ID regression
(`test/integration/client_request_ids/regression.ml`) covers `` `Fail `` (typed
already-started error naming the running run), `` `Use_existing `` (same run,
`started = false`), `` `Terminate_existing `` (previous run terminated, new run
started) and request-ID deduplication taking precedence over the policy. It
passed locally against a Temporal 1.32 development server with Temporal Core
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`; CI runs it against the Compose
stack's Temporal Server 1.32.0 with PostgreSQL 18.6.

## 2026-10-06: Fewer payload passes in the OCaml protocol codec (#846)

Payload bytes used to be serialized, reparsed, and base64 re-encoded several
times per crossing purely as self-checks. A parsed payload wrapper is now
decoded in place from its base64 string by a single table-driven pass that
checks canonical form directly (alphabet, terminal padding, zero unused bits)
instead of re-encoding the result. Building a wrapper no longer serializes and
reparses it. Outgoing payload-bearing documents keep the receiver's checks
without a second parse: the tree is validated as `parse_strict` would, the
serialized bytes go through the same raw-text preflight as Rust applies, and
the semantic decoder runs on the validated tree. Replay-history validation
decodes the parsed history wrapper in place. The wire format is unchanged.
On an Apple M4 Pro with OCaml 5.4.1, one 2 MiB-payload activation decode
went from 58 ms to 19 ms, and `encode_activation`/`encode_completion` went
from 150 ms to 11 ms. The new `payload-codec` benchmark sample went from
413 ms to 59 ms at p50. A task with ten 2 MiB results in and ten 2 MiB
arguments out, run through the runtime's current call pattern, went from 6.7 s
to 0.62 s. Protocol tests compare the new decoder with an independent
re-encoding reference for RFC 4648 vectors, every length from 0 to 300 bytes,
an exhaustive set of four-symbol groups, and malformed wrappers.

## 2026-10-06: Offline opam builds from the release source archive (#778)

opam's build sandbox denies network access, so `cargo build --locked` could
not fetch the locked crates or the pinned Temporal Core Git revision during
`opam install`. The release source archive is now produced by
`scripts/create-source-archive.sh`, which adds the `cargo vendor --locked`
output as `rust/vendor.tar`, the matching source replacement as
`rust/vendor-config.toml`, and the audited third-party notices. When both
files are present `scripts/build-rust-bridge.sh` unpacks the crates into the
Cargo target directory and runs Cargo with `--frozen` and
`CARGO_NET_OFFLINE=true`; Git checkouts keep the normal `--locked` build.
`test/smoke/test_rust_bridge_offline.sh` proves both modes and the rejection of
an incomplete archive with a stand-in Cargo. An archive built by the script
from this commit compiled with `dune build -p temporal-sdk` under a macOS
`sandbox-exec` policy matching opam's (network and writes outside the build
directory denied, nonexistent `CARGO_HOME`).

## 2026-10-06: Fixed-bug live regressions run in CI (#795)

The live regression executables for client request IDs (#545), update
admission outcomes (#546), completed-workflow queries (#548), local activity
retry cancellation (#567) and split workflow/activity workers (#805) were
neither compiled nor run by CI. They are now in the prebuilt smoke artifact,
and `make test-temporal-live-ci` runs them through
`make test-temporal-live-regressions` against a fresh Compose stack. That
target registers the fixtures' `default` namespace, copies the pinned
admin-tools CLI for the two suites that inspect or delete their own
executions, and bounds every process. A Docker-free contract, part of
`make test-quality-contract`, requires every `test/integration/*/regression.exe`
to be in the artifact list and run by a recipe reachable from the CI live
target, and its self-test proves that omissions are rejected. The suites
compiled locally with OCaml 5.4.1; their first live results come from the
pull request's Linux CI job.

## 2026-10-06: Connection failures name their cause; Core logs reach stderr (#833)

Every client connection failure used to read `Temporal client connection
failed`, and the runtime was created without a Core logger, so Core's own
records were discarded. The bridge now reports a closed cause (`dns`,
`refused`, `tls`, `timeout`, `unauthenticated`, ...) plus the bounded,
escaped local transport error chain. A failed `GetSystemInfo` adds only its
gRPC code, never server text, and Core's rejection of connection options is a
`configuration` error. Runtime creation installs a Core push logger that
formats one bounded, escaped line per record and enqueues it without
blocking. A per-runtime writer thread drains the bounded queue to stderr, so
a stalled stderr reader drops (and counts) records instead of stalling Core.
Runtime close waits at most 500 ms for that writer before detaching it.
Neither thread calls OCaml. `OCAML_TEMPORAL_CORE_LOG` selects the level
(default `warn`; `off` disables it). Rust ABI tests cover refused and DNS
causes and the message bound, cause classification, level parsing and
rejection, line formatting, non-blocking enqueue, drop reporting, and runtime
close with a blocked writer. An OCaml unit test proves the cause reaches
`Client.create`.

## 2026-10-06: Linear-time future settlement (#847)

Settling one future no longer scans every other pending registration. The
scheduler's teardown ledger, condition waiters, scope cancellation hooks, and
both runtime and derived future observers now use
`Temporal_base.Ordered_registry`, an intrusive doubly linked list with O(1)
append and removal whose traversals follow explicit registration order (no
hashing). Settling `n` pending futures, releasing `n` condition waiters, or
unlinking `n` completed scoped operations is therefore O(n) rather than
O(n²). Resume, teardown, hook, and callback order are unchanged.
`test/runtime/test_settle_scaling.ml` checks that order and a generous CPU
budget for 50,000-wide fan-outs, and `test/unit/test_ordered_registry.ml`
covers the registry contract; focused unit and runtime suites passed locally
on OCaml 5.4.1.

## 2026-10-06: Large client signal, query, and update inputs (#771)

The Rust bridge decoded signal, query, and update requests with the generic
object parser, which applies the 65,536-byte text limit to base64 payload
data. Any input over 49,152 raw bytes was therefore rejected before the RPC,
and OCaml reported it as a malformed client error. These requests now use the
same payload-aware decoder as workflow start: each payload byte field may hold
128 MiB inside the 192 MiB document limit, while identifiers, handler names,
and metadata keys keep the 65,536-byte text limit. Rust ABI tests submit
payloads at and above the old ceiling, at exactly 128 MiB, and one base64
quantum above it, plus identifiers and metadata keys at and above the text
limit, to an unconnected runtime and check which ones reach the lifecycle
guard. An OCaml test sends payloads from the real OCaml encoders through the
C stubs to the same guard.

## 2026-10-06: Stopping a worker from a signal handler (#830)

An OCaml signal handler may run on the thread blocked in `Worker.run`, so in a
single-Domain program the natural `SIGTERM` handler calling `Worker.shutdown`
was rejected as re-entrant and the worker never stopped. The new
`Worker.request_shutdown` is a single atomic write that both run lanes treat
as a stop at their next check; `run` returns `Ok ()` and the application then
calls `Worker.shutdown` to drain and release the worker. A re-entrant
`shutdown` still returns a defect but now posts the same request. A runtime
model test raises a real `SIGUSR1` against a loop on the main thread, a mock
worker test raises it from inside an activity callback, and the examples and
the Compose smoke worker now use the handler directly instead of a watcher
Domain.

## 2026-10-06: Retained completions fail closed (#843)

The workflow and activity adapters used to resubmit every retained completion
on the next poll or shutdown drain, regardless of how the earlier attempt
failed; the retryable classification only decided what happened when the
resubmission failed again. Each retained entry now records its first failure
that the source did not explicitly classify as retryable, and later polls and
drains return that error without calling the supervisor, so a non-retryable
failure followed by `Worker.shutdown` or a second `Worker.run` can no longer
submit the same completion twice. The workflow source signature gained the
same `error_is_retryable`/`exception_is_retryable` classifiers as the activity
source; production returns `false` because the bridge has no retryable
workflow-completion status. Runtime tests prove that a non-retryable typed
rejection, an unclassified exception, and a lost acknowledgement are each
submitted exactly once across poll, poll, and drain, block later tasks, and
are released only by `discard`, while explicitly retryable failures still
retry without rerunning user code.

## 2026-10-06: One unrepresentable activity task no longer stops the worker (#801)

An activity task the bridge cannot represent (a standalone activity with no
workflow, or a header key another SDK allowed) used to be failed back to Core
retryably and then reported as a fatal protocol status, so `Worker.run` ended
for the whole task queue, again after every redelivery. Rust now fails only
that task with a non-retryable `UnrepresentableActivityTask` application
failure carrying a static category, writes a bounded stderr diagnostic, and
returns `NOT_READY` so the worker keeps polling. When OCaml's decoder rejects
a document Rust accepted, a successful rejection is likewise reported as an
empty poll on live workers (a replay still fails). The activity poll lane
also drops an orphaned, repeated, or retired cancellation silently instead of
publishing a fatal lane error. A gRPC-double ABI test proves the rejected
task's failure, slot release, and delivery of the next task; ledger and OCaml
adapter tests cover the cancellation and decode-failure classifications.

## 2026-10-06: Retryable client capacity errors (#796)

The native client's two bounded registries, 64 in-flight starts and 64
distinct waited runs, now reject excess admissions with a dedicated ABI status
`15` (`RESOURCE_EXHAUSTED`) instead of reusing `INVALID_STATE`, which callers
could not tell apart from a closed client. The public adapter returns a
retryable `bridge` error with `error_type` `resource_exhausted`, recognized by
the new `Client.is_at_capacity`. Both bounds are documented on `Client.start`
and `Client.wait`. Rust callback-transport tests prove both bounds, ticket
reuse at capacity, and re-admission after a slot is freed; an OCaml bridge
test proves the public classification. The bounds remain fixed; making them
configurable is left for a follow-up.

## 2026-10-05: Activity timeout and duration bounds (#812)

`Activity.start`, `start_local`, and `start_handle` reject an explicit zero
schedule-to-close or start-to-close timeout with a ready typed defect before any
codec runs or command is emitted; Temporal treats zero as unset, which would
otherwise wedge the workflow on server-rejected tasks. `Duration.of_ms` raises
above the protobuf `Duration` maximum (315,576,000,000,999 ms), and the OCaml
decoder and Rust validator both reject zero close timeouts and durations past
protobuf's maximum second count. Shared invalid fixtures prove the bilateral
rejection; focused OCaml unit/bridge/runtime suites and the Rust test suite
passed locally on OCaml 5.4.1.

## 2026-09-20: Current start metadata and worker replacement (#512)

`Client.start` memo and registered search attributes now reach root and
continued OCaml workflows. `Workflow.start_metadata` returns an owned,
replay-stable snapshot of both maps and the server expiration timestamp.
Absent and empty maps remain distinct; exact nanoseconds and payload metadata
are retained. Mutating an input or observed payload cannot change later reads.
See [the start metadata contract](reference/workflow-start-metadata.md).

The live continuation exposed a server-applied first-task backoff even without
cron: the successful successor recorded `0.897940959s`. The bridge preserves
that duration for validated workflow/retry continuations. It still rejects
cron and nonzero root start delay, without a broad continuation bypass.

Local Linux arm64 validation used OCaml 5.5.1, Rust 1.94.1, Temporal Core
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`, Temporal Server/admin-tools 1.32.0,
and PostgreSQL 18.6. The 45 Rust workflow protocol tests, shared OCaml protocol
fixtures, native execution ownership/replay suite, `make lint-rust`, and
`make test-install` passed. `make test-temporal-start-metadata-live` passed
for memo-only, search-only, combined, continued combined, and an official
Temporal CLI start with both maps plus execution/run/task timeouts.

The live checker requires matching exact run identities; initial running
status with durable timer history; unchanged memo/indexed values and Keyword
type; root-to-successor linkage; a terminal history preserving the initial
prefix; expected output; and accepted task completions from both worker
generations. The five final run IDs were
`01a0be2d-9c34-7523-a9b6-aa3ceb76ec1f`,
`01a0be2d-9cf8-7c46-a208-d9795d26d473`,
`01a0be2d-9d6e-7066-ace6-08cf002273f1`,
`0b1f665e-6257-453c-80d4-15a4845f4cbe`, and
`01a0be2d-a35a-7be4-aa95-68ee7841d49e`. Local raw histories, visibility
records, and logs remain in `_build/start-metadata-evidence`. Both CI
workflows now run the isolated metadata gate first and retain its synthetic
evidence; hosted CI is a separate verification gate.

This establishes current metadata round trips and exact-history worker
replacement/replay. It does not close the future #499 start-policy matrix,
#501/PR #555 cache-full eviction qualification, the #503 shared replay corpus,
or #505 feature conformance. An initial one-slot-cache attempt encountered
#501's polling delay; the final fixture uses ordinary worker defaults and
forces reconstruction by replacing the entire process.

## 2026-07-21: Stable and prerelease tag consistency gate (#444)

The complete [PR #444 Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29827725596)
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

The complete [PR #438 Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29805397413)
passes the real Temporal/PostgreSQL integration smoke after isolating the
cache-eviction worker and configuring it with one Core cache slot. The current
`make test-temporal-worker-cache-eviction` gate uses a typed read-only query as
its synchronization barrier, requires the payload-free `cache_full` marker,
acknowledges Core's `RemoveFromCache` activation with an empty completion, and
then checks exact-run cancellation and teardown. The earlier [PR #322 run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29402103748)
remains historical evidence for the original gate; broader cache/recovery and
child-failure scenarios remain planned.

## 2026-07-19: Live external workflow cancellation acceptance (#431)

The complete [PR #431 Build run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29679213525)
passes the real Temporal/PostgreSQL integration smoke and all compatibility
jobs. Its two-binary scenario delivers an external signal, rejects a
mismatched run ID before acknowledging the exact-run request, then cancels the
separately started target and observes its cancelled result. This replaces the
earlier documentation boundary that treated live external workflow operations
as outstanding; missing or already-completed targets and replay interaction
remain separate live scenarios.

## 2026-07-21: Stable mismatched-run external cancellation diagnostic (#439)

The complete [PR #439 Build run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29824441578)
keeps the live external-cancellation scenario green while tightening its
wrong-run assertion. A mismatched exact-run cancellation now has documented
bridge evidence as a retryable workflow error (`non_retryable=false`) whose
message begins `Unable to cancel external workflow because not found`. The
workflow-ID-plus-run-ID identity check remains the acceptance boundary; missing
or already-completed targets and replay interaction are not implied by this
diagnostic.

## 2026-07-19: Live typed queries, workflow updates, and termination

The complete [PR #434 Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29684113836)
extends the parked-workflow query acceptance from output-only queries to
`Temporal.Client.query_with_input`. It also requires the exact missing-handler
rejection and preserves the local typed-input validation boundary. The [PR #428
Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29676120429)
verifies typed workflow-update admission, polling, handler state mutation, and
completion against the real Temporal/PostgreSQL stack; [PR #432](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29681119024)
adds the unregistered-handler rejection and proves the parked workflow remains
usable afterward. The [PR #433 Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29683521094)
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

The complete [PR #406 Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29557704643)
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

The Docker-free contract passes, and the complete [PR #356 GitHub Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29469232271) verifies the
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
run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29434016013) passed all
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
run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29411260374) passed all
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
run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29351689638) passed the
Temporal/PostgreSQL integration job. Its public driver started
`smoke.activity_long_backoff_retry`, retained the exact workflow/run handle,
observed the run complete, required
`SMOKE:BACKOFF:RETRIED:SMOKE`, and finished with the complete driver assertion
marker. The worker-side fixture rejects a second callback delivered in under
one second, so the result is evidence of a server-delivered, non-immediate retry
under a policy configured with a two-second initial and maximum backoff. It does
not measure or prove that the full configured delay elapsed.

This was the first live evidence for the eighteenth baseline result. The
complete [PR #439 run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29824441578)
retains the scenario in the later 26-start baseline; PR #289 remains historical
evidence for the preceding seventeen-result slice.

## 2026-07-14: Retry-after-restart acceptance extension

Status: verified in the Temporal/PostgreSQL integration job of the complete
[PR #298 GitHub Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29346853291).

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

Status: verified in the complete [PR #279 GitHub Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29329420364).

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

Status: verified in the complete [PR #253 GitHub Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29286560471)
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

Status: historical CI evidence from the full [PR #210 Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29221151859),
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

The squash-merged [PR #191](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/191),
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

Status: locally contract-checked after the squash-merged [PR #193](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/193), commit `48ed97f`. No live Temporal Server or GitHub Actions success claim is made here. The expanded Actions run was cancelled, and subsequent checks may remain queued while the repository quota is exhausted.

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

The merged tip is `1fa679c`: [#164](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/164)
(`4724830`) preserves configurable `JQ_BIN` paths containing spaces and adds
a regression invocation; [#165](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/165)
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
[`29191260073`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29191260073)
remains the latest successful two-binary acceptance evidence.

## 2026-07-13: Public client state, protocol evidence, and scheduler teardown (#159–#161)

The merged tip is `02f4627`: [#159](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/159)
(`980855f`) clarifies public client state and adds protocol coverage; [#160](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/160)
(`d5606ad`) separates local protocol evidence from live Temporal evidence; and
[#161](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/161) (`02f4627`) releases
settled futures during scheduler teardown with a weak-reference regression test.

PR #161 recorded this local verification: `DUNE_BUILD_DIR=/tmp/ocaml-temporal-dune-audit
CARGO_TARGET_DIR=/tmp/ocaml-temporal-cargo-target opam exec -- dune runtest --root .
test/runtime`, `sh scripts/check-format.sh`,
`sh test/smoke/test_quality_contract.sh .`, and `git diff --check`. The
repository's GitHub Actions checks remain queued while the Actions quota is
exhausted, so this entry makes no CI-success claim. The historical
five-execution live result in run
[`29191260073`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29191260073)
remains the latest successful two-binary acceptance evidence.

## 2026-07-13: Protocol, lifecycle, and two-binary acceptance contracts (#141–#152)

Status: the merged documentation and acceptance-contract milestones are now
present on `origin/master` at `c008c52`. The activity-protocol lifecycle and
evidence records are [#141](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/141)
(`cb2892c`), [#142](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/142)
(`d1d45c2`), and [#143](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/143)
(`ef3e171`); asynchronous completion, native worker ownership, and installed
package boundaries are [#144](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/144)
(`cfb760a`), [#145](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/145)
(`9a6992a`), and [#146](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/146)
(`127f3f6`). Native execution translation, dependency licensing, restart and
replay evidence, and the separation of control from operation JSON are
recorded by [#147](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/147)
(`6e860b2`), [#148](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/148)
(`b30f85f`), [#149](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/149)
(`54cf1a9`), and [#150](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/150)
(`3466ae9`). [#151](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/151)
(`ef0ed69`) adds assertions that the acceptance harness has separate
`smoke_driver` and `smoke_worker` OCaml binaries with distinct roles. [#152](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/152)
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
[`29191260073`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29191260073)
was the latest successful two-binary acceptance evidence at that time; the
later PR #210 entry above supersedes it.

## 2026-07-13: Documentation evidence and navigation refresh (#129–#140)

Status: documentation-only updates were merged in PRs [#129](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/129)
(`d37f863`), [#130](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/130)
(`857862b`), and [#131](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/131)
(`404a7c5`) for runtime invariants, merged lifecycle evidence, and feature
coverage; [#132](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/132)
(`1dfc13e`), [#133](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/133)
(`515f723`), and [#135](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/135)
(`a656f92`) for the two-binary acceptance boundary, queued-CI fallback, and
live-acceptance evidence; [#134](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/134)
(`229b548`), [#137](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/137)
(`97924c7`), and [#138](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/138)
(`65b6441`) for native activity, client protocol, and Core-bridge contracts;
and [#136](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/136)
(`b487eaf`), [#139](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/139)
(`6709fce`), and [#140](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/140)
(`9104f3d`) for observability, workflow guidance, and documentation
navigation.

These changes clarify the current implementation and evidence boundaries but
do not add runtime behavior or new live Temporal results. Applicable local
format, quality-contract, configuration, and focused documentation checks were
used for the documentation PRs; host-only `make quality` remains dependent on
the pinned scanner binaries being installed. GitHub Actions checks may remain
queued because of the repository quota and are not treated as passing evidence.
The historical five-execution live result in run
[`29191260073`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29191260073)
was the latest successful two-binary acceptance evidence for this historical
entry. The expanded assertions were later live-verified by PR #210, as recorded
in the current entry above.

## 2026-07-13: Scope, child-lifecycle, and ABI cancellation-validation coverage

Status: locally verified on the merged `origin/master` tip with focused OCaml,
Rust, and ABI tests. The work was merged in [PR #125](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/125)
as `8e56c24`, [PR #126](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/126) as
`43beafb`, and [PR #127](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/127) as
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
merged in [PR #120](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/120) as commit
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
The lifecycle tests were merged in [PR #118](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/118)
as commit `8b62593`, and the bridge ownership tests were merged in [PR #119](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/119)
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
merged in [PR #99](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/99) as commit
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
[PR #100](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/100) as commit `9baa00a`;
no new live acceptance result was produced by that documentation change.

The acceptance references now distinguish the historical five-execution green
run [`29191260073`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29191260073)
from the current-at-that-time seven-run cancellation/heartbeat implementation and its local protocol,
client, worker, and supervisor checks. The one-shot OCaml assertion driver and
the long-lived worker are described separately. GitHub Actions run
[`29193818312`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29193818312)
was cancelled, so the seven-run cancellation and heartbeat scenarios were
unverified against a live Temporal Server at the time. PR #210 later supplied
the green nine-scenario run recorded in the current entry above.

## 2026-07-12: GitHub Actions capacity observation

Status: historical observation for PR #100; this entry does not claim a
completed CI result. The PR #100 Actions attempt
[`29194514765`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29194514765)
did not produce a completed result in the observed window, and its push run for
merge commit `9baa00a`
[`29194534789`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29194534789)
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
[`29191260073`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29191260073)
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
[`29187733405`](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29187733405)
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
- [GitHub Actions run 29139710646](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29139710646)
  completed all eight compiler/architecture cells and the license job
  successfully.
- [GitHub Actions run 29139792049](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29139792049)
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
- [GitHub Actions run 29141377953](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29141377953)
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
- [GitHub Actions run 29142248581](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29142248581)
  passed the standalone dependency audit and all eight Linux OCaml 5.2 through
  5.5 amd64/arm64 jobs with the linked Rust bridge.

Native desktop evidence:

- Windows x64 builds the OCaml-owned executable with OCaml 5.5 and the pinned
  GNU Rust toolchain, then passes the Rust ABI tests, Clippy, rustfmt, OCaml
  tests, and the fresh installed-package consumer.
- macOS ARM64 performs the same native verification with OCaml 5.5 and the
  pinned Apple ARM Rust toolchain.
- [GitHub Actions run 29143621807](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29143621807)
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

## 2026-09-20: Preserve open workflows after task defects (#511)

Unexpected workflow/encoder/registration/SDK defects now produce Core failed
workflow tasks with no commands. Deliberate typed application failures retain
terminal semantics and retryability. The adapter discards unsafe state while
retaining the exact completion and its ownership until acknowledgement, including
an acknowledgement exception after source acceptance. The before-v1 contract and
operation-specific query/update behavior are documented in
[workflow failures](reference/workflow-failures.md).

Local verification used the cached development image
`ocaml-temporal-501-dev:latest` (OCaml 5.5.1, Rust 1.94.1, Dune 3.24.2), selected
with Make's `COMPOSE_RUN` override and single-job Dune/Cargo builds. The focused
runtime/bridge/observability/supervisor suites passed; Rust protocol and retry
policy tests passed (44 + 5), and the existing replay ABI suite passed all ten
tests including the five retained live histories. Scoped warning-denying Clippy,
Rust formatting, repository whitespace, quality contract, and Compose
configuration checks passed. The full cross-version/platform matrix remains the
hosted CI gate; no dependency pin changed.

The live `make test-temporal-task-failure-live` run captured in
`20260920T093940780862Z` passed against Core
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`, Temporal 1.32.0, and PostgreSQL 18.6.
The source baseline plus patch hash, exact original run IDs, executable hashes,
and image digests are in the checked-in
[regression manifest](../test/integration/temporal/task_failure/histories/manifest.json).
The broken worker recorded workflow-task failures for body, output encoder, and
missing registration while each execution remained running. A fresh corrected
executable replayed the committed timer prefix and completed the same three
client handles. Both deliberate business failures closed their original runs
with the expected retryability flags and no installed workflow retry policy.
No speculative timer reached durable history. Both workers stopped gracefully,
and the task-owned Compose stack and volume were removed; raw initial/terminal
histories, describe responses and process/server logs remain under
`_build/task-failure-live/20260920T093940780862Z/`.

CI runs this recovery gate before the existing seven live controllers and
uploads its bounded synthetic evidence immediately, even on failure. The
retained protobuf histories also replay offline through the existing private
Core ABI; this does not introduce a public replay API or establish the broader
transport-fault/mixed-deployment qualification required by later issues.

## 2026-10-06: Digest-pinned OCaml images and locked OPAM installs (#804)

`Dockerfile.dev` now selects one of four `ocaml/opam` stages (5.2 through 5.5),
each pinned to its multi-architecture manifest index digest, and the Docker and
native macOS/Windows artifact lanes install dependencies through
`scripts/opam-locked-deps.sh` instead of re-solving `temporal-sdk.opam`. The
script installs every non-compiler package at its locked version, applies only
the per-series replacements listed in `scripts/opam-lock-overrides.txt`
(currently `ocamlfind.1.9.9~preview` on OCaml 5.5), and fails on any drift. See
[Installing the locked OPAM closure](dependencies.md#installing-the-locked-opam-closure).
Local validation covered the repository contract scripts and a stub-OPAM test
of the installer; the image builds and native installs are validated by the
hosted CI matrix.

## 2026-10-06: Dispose acknowledges Core's follow-up evictions (#775)

Runtime close force-fails each workflow activation OCaml still holds and
tombstones its run ID. Core answers that failure with a same-run cache
eviction, which the workflow poll lane used to drop as a retired duplicate, so
the workflow poll never reported `ShutDown`: close waited out the 90 s drain
bound and released an unfinalized worker. The lane now acknowledges a retired
run's pure eviction with an empty completion, and dispose's own
force-completion acknowledges leased or queued evictions empty instead of
failing them. The ledger records each run's eviction bit when the poll lane
admits it, so an entry admitted but not yet enqueued at the disposal snapshot
is still acknowledged rather than failed.
`rust/core-bridge/tests/runtime_dispose_eviction.rs` drives a
leased activation through runtime close against a gRPC double and fails
within 30 s without the fix.

## 2026-10-06: Workflow and activity execution info (#792)

`Temporal.Workflow.info ()` and `Temporal.Workflow.is_replaying ()` expose the
run identity Core sends in the initialization job (workflow and run IDs, first
run ID, type, attempt, parent, start time), the worker task queue, and the
task-local replay flag, history length/size, and continue-as-new suggestion
that the native adapter now installs before every activation alongside the
existing clock and deployment metadata. `Temporal.Activity.Context.info`
exposes the start task's namespace, workflow identity, activity ID/type,
attempt, local flag, and timestamps. Both are abstract accessor modules so
fields can be added compatibly. No bridge protocol change was needed: every
value already crossed the private JSON boundary. The workflow namespace is not
exposed yet because activations do not carry it and the worker adapter does
not pass it to executions; asynchronous activity contexts do not expose info
yet. `test/runtime/test_execution_info.ml` drives live and replayed native
activations through the adapter and covers the detached, synthetic, and
standalone-activity paths; the native activity adapter test checks the
forwarded task identity.

## 2026-10-06: Remaining execution info (#792)

`Temporal.Workflow.Info.namespace` reports the worker namespace: Core
activations do not carry it, so the native worker passes its validated
namespace to `Native_worker_execution.create`, which copies it into every
execution context (and rejects a malformed value as typed configuration at
`$.namespace`). `Temporal.Activity.Async_context.info` gives asynchronous
callbacks the same `Activity.Info.t` as synchronous ones, and `Activity.Info`
now reports the schedule-to-close, start-to-close, and heartbeat timeouts.
Timeouts are rounded up to whole milliseconds so the conversion is total and
adds no task-rejection path for sub-millisecond values; asynchronous
definitions also skip the synchronous context's exact heartbeat-interval
check, and the protobuf maximum is clamped to the largest public
`Duration.t`. Server
continue-as-new suggestion reasons were not exposed yet at this point; the
2026-10-07 entry adds them.
`test/runtime/test_execution_info.ml`, the async adapter test, and the native
worker adapter test cover the new values.

## 2026-10-06: Thread-keyed workflow context (#765)

The current workflow context, the scheduler owner id that guards
`Future.await`, condition waits and scope operations, and the read-only query
marker were stored in `Domain.DLS`, which every system thread of a Domain
shares. Two workers whose `run` loops share a Domain could therefore record
commands into each other's execution or restore a stale context. These
bindings now use the private `Thread_binding` slot: a Domain-local atomic cell
holding an immutable map from `Thread.id` to the bound value. Reads take no
lock and skip the thread lookup when nothing is bound on the Domain; writes
use a compare-and-set retry that only sibling threads of the same Domain can
contend on. Each entry exists only inside its `with_value` extent and is
restored on the installing thread even on an exception, so single-thread
behavior is unchanged and no entry outlives its activation.
`test/runtime/test_thread_context_isolation.ml` forces interleaved activations
on two threads of one Domain, checks that helper threads do not inherit a
context, checks exception cleanup, and stress-tests the primitive with
yielding threads.

## 2026-10-06: In-process workflow test environment (#834)

`mock://` never ran workflow code, so applications had no supported way to
unit-test workflow logic without a server. The new public `Temporal.Testing`
module fills that gap. Its engine, the private
`Temporal_runtime.Test_environment`, starts ordinary `Execution.t` values (the
runtime a native worker uses) and replaces Temporal Server and Core with a
single-threaded simulator that interprets their commands: activities run when
scheduled, with retry policies and heartbeat-detail hand-off applied in
virtual time; timers fire on a virtual clock that jumps to the next event when
every execution is blocked; child workflows start in the same environment and
honor parent-close policies; external signals and cancellations, queries in
query-only activations, validated updates, workflow cancellation and
continue-as-new are routed as Core would. Identifiers and the randomness seed
are deterministic counters. A workflow task failure ends the run with its
error, and a workflow blocked with nothing scheduled is reported as a defect
instead of hanging. `mock://` deliberately stays a plumbing-only backend:
routing it through the engine would mean rewriting the client, backend and
worker modules for a weaker, untyped API, while `Testing` gives typed handles
directly. Activity timeouts, task retries, child cancellation types,
asynchronous activities, Nexus and visibility are not simulated.
`test/unit/test_testing.ml` covers time skipping, real and mocked activities,
retries, error propagation, child workflows, signals, queries, updates,
cancellation, continue-as-new, external signals, local activities and
reproducibility; `examples/testing` tests the example application's workflow.

## 2026-10-08: Versioned replay history corpus (#518)

The repository now has a versioned replay corpus, the seed for #503's
compatibility gate. `test/fixtures/history-corpus/manifest.json` (schema
[`manifest.schema.json`](schemas/history-corpus/manifest.schema.json)) indexes
21 replay cases over 17 histories. Each history records its provenance
(producing SDK commit, Core pin, digest-pinned Temporal Server image, capture
command and date), its SHA-256 checksums, the frozen definition set to replay
it against, and the expected verdict. Eleven histories were captured live for
this corpus (Temporal Server 1.32.0, Core `95e97686`, OCaml 5.4.1). Together
they cover activity success, a timer, activity retry, signal/update/query
interaction, a parent and its child, both runs of a continue-as-new, and
marker-free, active and deprecated patch histories. Five retained #511
workflow-task failure histories and the synthetic #694 initial-signals history
are copied byte for byte with their original provenance. The other entries
reuse these histories. Two are compatibility cases: a marker-free history on
the patched generation, and an active marker on the deprecated generation. Two
are negative controls: the timer workflow without its timer, and a patched
history on the pre-patch generation.

`test/history_corpus/test_history_corpus.ml` is an ordinary Dune test, so
`make test` and the native jobs gate it without Docker. The test checks the
manifest schema, checksums, references and orphaned files, and it requires
every promised feature tag to have coverage. It then replays every entry
through the private Core replay path (a fresh supervisor and the production
workflow adapter) against frozen public-API definitions. A `replays_ok` entry
must report the recorded run ID and workflow type and finalize naturally. A
negative control must produce Core's `[TMPRL1100]` nondeterminism eviction.
Mutation checks confirmed that a changed checksum, an orphaned file, an
unknown member, a missing feature, a wrong run ID, incompatible definitions and
a flipped expectation each fail the gate. `make history-corpus-capture` runs
`history_corpus_capture.exe` for three worker generations against the
Compose stack, exports each run with the pinned Temporal CLI, encodes the
JSON as protobuf, and stages it for append-only installation. A second capture
reproduced all eleven cases. See the
[history corpus reference](reference/history-corpus.md) for the rules and the
remaining scope (#524 runs the corpus across upgrades; #515 adds a public
runner).

## 2026-10-08: Replay corpus as the SDK/Core upgrade gate (#524)

The replay history corpus now runs through the public `Temporal.Replay` API,
the entry point applications use, instead of the private replay path.
`test/history_corpus/history_corpus_runner.ml` replaces
`test_history_corpus.ml` and `corpus_replay.ml` as the single runner. It is the
Dune test, so `make verify` and `make native-verify` run it on every PR, and
it is also `make test-history-corpus-upgrade`. That target writes a per-case
table and `_build/history-corpus/report.json`. The report records each case's
ID, expected and actual outcome, failure message and producing SDK commit and
Core revision. It also records the candidate SDK commit, the Core revision
read from `rust/Cargo.lock`, the OCaml version and the bridge ABI. The runner
exits 1 and lists every mismatched case ID. The Linux amd64 / OCaml 5.5.1 CI
leg uploads the report as an artifact, also after a failed verification.

The public API reports no run ID or workflow type on success. The manifest
validator therefore decodes each entry's `workflow_type` and
`original_execution_run_id` from the start event of the protobuf history that
is replayed (a minimal documented field walk, `corpus_history_identity.ml`),
checks the optional JSON copy against the same values, and the runner
registers only the entry's workflow type. `test_history_corpus_mismatch`
copies the corpus, breaks one expected pass and one negative control, and
requires exit 1 with exactly those IDs in standard error and in the report. A
second scenario swaps in another valid protobuf without a JSON copy and
requires validation to fail on that entry's identity. A manual mutation also
confirmed that a wrong `run_id` fails validation. The
[Core pin upgrade checklist](dependencies.md#temporal-core-pin-upgrades) now
requires a passing report for every Core bump and Dependabot Cargo PR. The
runner output and report state that a pass is forward-compatibility evidence
only, not rollback evidence (#508). The
whole corpus replays in about a second. Every current capture comes from Core
`95e97686`, so the first Core bump will be the first cross-revision replay.

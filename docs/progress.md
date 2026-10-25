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

## 2026-10-17: v1 policy and qualification docs reconciled with merged work (#489, #514)

This is a documentation-only correction; the v1 support policy's approval
status is unchanged and remains a maintainer decision on #489. Each statement
was checked against `origin/master` code and tests before editing:

- The policy's remote-activity row no longer says callbacks run serially under
  the polling lock. Since PR #752 activity callbacks run on a dedicated
  capacity-one Domain, so a blocked callback stops other activities but not
  unrelated workflow progress; #492 stays open for qualification.
- Asynchronous handles observe cancellation, pause and reset through the
  result of `Async_handle.heartbeat` (PR #1103, exercised live by the PR #1117
  example). They are still not notified of worker shutdown.
- The metrics row now lists the SDK-side `temporal_ocaml_` worker series
  (PR #1114), activity-scoped log correlation (PR #1051) and the bounded scrape
  connections (PR #1096). The endpoint still has no authentication or TLS.
- The transport-fault qualification and Core bridge references no longer list
  #490 (CI artifact retention) or #501 as open. The release checklist's gate
  statuses were rechecked and its date refreshed.
- The policy, README, workflow guide, feature coverage and the
  `Client.cancel` documentation now say plainly that workflow-level
  cooperative cancellation with durable cleanup is excluded from the proposed
  MVP v1 scope (#514): a cancellation request closes the run in the activation
  that delivers it, before workflow code can run cleanup.

## 2026-10-16: Runnable cooperative cancellation example (#494)

`examples/cancellation` is a self-checking example of cooperative activity
cleanup. Its activities hold a reservation in an in-memory ledger that stands
in for an external service. The ledger key is the workflow ID, run ID and
explicit activity ID, so every attempt of one scheduled activity uses the
same key while a new run that reuses a closed run's workflow ID gets its own;
acquire, release and commit are idempotent, so retries and redeliveries are safe. A
synchronous activity checks `Context.is_worker_shutting_down` and heartbeats
before each step; on any early exit it releases the reservation and then
returns the heartbeat's `` `Cancelled `` error, or a retryable error when its
worker is stopping, resuming later from its last heartbeat details. An
asynchronous activity's completer thread treats a non-retryable
`` `Cancelled `` result from `Async_handle.heartbeat` (#1103) as a requested
cancellation and a retryable one as a pause or reset, releases the
reservation, and then calls `Async_handle.cancel` or `Async_handle.fail`.

The driver runs a worker in the same process and checks the ledger history
for four scenarios: a workflow cancels the synchronous activity, and the
asynchronous one, under `Wait_cancellation_completed` and must see a
cancelled activity whose only attempt released its reservation because of
the cancellation; and the driver shuts the worker down while an activity
holds a synchronous activity's reservation, so that attempt must release it
because its worker is stopping, and a replacement worker's retry must
reacquire the key and commit it; finally a new run under the same workflow ID
must make and commit its own reservation. It prints a fixed five-line report.
Cleanup on worker shutdown is synchronous-only: an asynchronous handle is not
notified when its worker stops, shutdown closes it as a lost completion after
the grace period, an unfinished handle expires per #1043, and Temporal retries
the attempt according to its timeouts. The docs say so.

The executable is in `EXAMPLE_EXECUTABLES` and the CI smoke artifact, and
`run-examples-live.sh` runs it as a one-shot container on its own
`-cancellation` task queue and requires the exact report. The asynchronous
scenario is the first live use of the async heartbeat cancellation result.
The examples guide, the workflow guide, the feature coverage table and the
README describe the contract; the stale statement that asynchronous handles
are not notified of cancellation is corrected.

Evidence: `dune build --root . @install` and the example executables on OCaml
5.4.1 (macOS), `test/smoke/test_examples_live_contract.sh` (4 executables),
`scripts/check-live-acceptance-inventory.sh --check` and shellcheck of the
live controller. The live run needs the Compose stack and happens only in CI.
Re-qualification after the isolated activity dispatch of #492 remains.
## 2026-10-16: Start metadata survives a cache-full eviction (#512)

`make test-temporal-start-metadata-live` has a second phase. Before this,
the gate evicted runs only by replacing the worker, with the default cache
size. A fresh worker now runs the metadata workflow with
`max_cached_workflows = 1`, set through `Worker.Options`, on its own task
queue. The client starts three runs with different memo, search attribute
and execution-deadline combinations, one at a time. It waits for each run to
park before starting the next, so each new run's first task makes Core evict
the previous one with `RemoveFromCache` reason cache-full. It reads each
run's exact snapshot while the run is cached: the summary plus the
nanosecond expiration. It then queries, signals and completes the runs in
start order, which evicts the last cached run and rebuilds every run from
history. Each replayed snapshot must match the cached one, and each result
must be exact.

The cache worker installs the private observer seam and appends one
payload-free line per activation and acknowledged completion to
`cache-events.tsv`. The verifier requires each run to have a `cache_full`
completion followed by a replaying activation of the same run. It applies
the existing history and visibility checks to the three runs and requires
each snapshot's expiration to equal the start event's, to the nanosecond.
The evidence sits with the gate's other artifacts under
`_build/start-metadata-evidence`, and CI uploads it through the existing
`*.tsv`, `*.json` and `*.log` patterns.

Verified locally: `dune build --root . @install` and the
`start_metadata_driver` executable build; ShellCheck passes for the
controller and verifier; the verifier's eviction and timestamp checks were
exercised on synthetic records; and the Docker-free contract targets pass.
The live gate needs the Docker Compose stack and runs only in CI.
## 2026-10-16: Source-archive checks run with the switch on PATH

The master-only `opam-source-offline` job had failed on every master build
since #1080: `setup-ocaml` does not put the switch's `bin` directory on PATH
for plain `bash` steps, so opam's sandbox wrapper reported `Command not found:
dune` straight after the network probe. Both source-archive steps in
`build-pr.yml`, and the matching `source-archive-check` steps in
`release.yml`, now run under `opam exec --`, like the Makefile's native
targets. `scripts/check-opam-source-archive.sh` checks for `dune` before
probing or building and names that remedy, and
`test/smoke/test_opam_source_archive_check.sh` covers the missing-`dune` case
(it reproduces the master failure without the script change).

## 2026-10-16: SDK-side worker metrics (#500)

Core's Prometheus exporter (#1000) covers what Core sees. Worker states that
exist only in the OCaml SDK are now recorded into the same registry, under
`temporal_ocaml_` names with Core's `service_name` label and the runtime's
global tags: activity-lane occupancy and callback duration, admitted and
refused activity tasks (by a closed `reason`), asynchronous operations the
bridge refused because its client-call registry was full (#1104), expired
asynchronous leases (#1043), retained-completion retries, detached activity
lanes, and the outcome of every bounded worker shutdown (`clean`,
`abandoned_work`, `teardown_detached`, or the error outcomes) with the kind of
abandoned work. Every series carries only `namespace`, `task_queue` and at
most one closed-enumeration label; activity types are not a label because a
refused task's type is chosen by its scheduler. The names, types, units and
labels are in [observability](reference/observability.md#sdk-worker-metrics),
together with two example alerts.

The bridge path is additive to ABI version 4:
`ocaml_temporal_core_v4_shared_runtime_metric_meter`,
`..._metric_meter_register_json`, `..._metric_meter_record` and
`..._metric_meter_free` ([core bridge](reference/core-bridge.md#sdk-metric-meter-500)).
The meter holds Core's meter, not Core, so it may outlive the runtime and is
freed only by its OCaml finalizer; registration binds each series to its
labels once and enforces the cardinality bounds (name and label syntax,
reserved and global-tag label names, 8 labels, 256-byte values, 1,024 series);
recording is an allocation-free `[@@noalloc]` call that takes a read lock and
does no I/O. `Sdk_shared_runtime` creates the meter with an exporting runtime;
`Native_worker_metrics` registers a worker's series (counters exported at 0
from creation) and is a no-op when metrics are off. The activity adapter
records around both callback paths, at each SDK refusal, at the #1104
`Not_admitted` disposition and at lease expiry; the public worker records
completion retries, detached lanes and shutdown outcomes. A registration
failure leaves the worker running without SDK metrics after one
`worker_metrics_unavailable` warning.

Evidence: `rust/core-bridge/tests/sdk_metrics.rs` (every kind read back from a
real scrape, every registration bound, recording errors, a meter outliving its
runtime, exact totals from eight concurrent recorders);
`test/runtime/test_native_worker_metrics.ml` (adapter paths against a real
exporting runtime with a fake supervisor: occupancy during a callback,
admissions, refusals by reason, both `Not_admitted` operations, lease expiry,
worker-level outcomes, every documented family, the label allow-list, and no
execution identifier in the scrape); and the live
`test/integration/worker_metrics/regression.exe`, which now also requires the
SDK families, an admitted and timed callback, and one `clean` shutdown in a
scrape taken after `Worker.shutdown` (CI only, Compose). Locally:
`dune build --root . @install`, `dune runtest --root .`, the Rust test above,
`cargo clippy --locked --all-targets -- -D warnings` and `cargo fmt --check`.
## 2026-10-16: Rust toolchain 1.99.1 at every pin

The exact Rust compiler moved from 1.99.0 to 1.99.1 in one change across
`rust/rust-toolchain.toml`, the Makefile's `NATIVE_RUST_VERSION`, the
`build-pr`, `release` and `rust-bridge` workflows, the toolchain smoke gate
and its Makefile fixtures, and the dependency inventory. This supersedes the
Dependabot PR (#1110) that changed only the toolchain file. The
`rust:1.99-bookworm` digest used by `Dockerfile.dev` and `Dockerfile.rust-ci`
was refreshed to `sha256:e12566d5…de29`: the old index still shipped
`RUST_VERSION=1.99.0`, while every platform in the new index ships 1.99.1.

In that image, rustup installed 1.99.1 with Clippy and rustfmt, and
`cargo check --locked --all-targets`, `cargo clippy --locked --all-targets --
-D warnings` and `cargo fmt --all --check` were clean with no new lints.
`test/smoke/test_rust_toolchain.sh` built the bridge archive, and
`make test-quality-contract` and `scripts/check-dependency-inventory.sh`
passed.

## 2026-10-16: Live regression for a worker killed during a running activity (#519)

`test/integration/activity_worker_crash` adds the worker-removal case that the
#519 inventory left open: a worker process dies abruptly while an activity
callback is running. The driver spawns generation 1 (hosting the workflow and
the activity) as a separate process. The activity has a three-second heartbeat
timeout, a long start-to-close timeout and a one-second retry interval. Its
generation-1 callback heartbeats a fixed detail every 200 ms and never
returns. The driver kills generation 1 with `SIGKILL` only after two barriers,
with no fixed sleep: the callback's marker file, written after its first
accepted heartbeat, and `workflow describe` showing attempt 1 started on
generation 1 with recorded heartbeat details. It then requires that the
process died of that signal. With no worker polling, a third barrier waits
until `describe` shows attempt 2 scheduled with a heartbeat-timeout last
failure, so the server alone detected the lost attempt. Only then does
generation 2 start. Its callback reports `Activity.Info.attempt` and the
previous attempt's heartbeat detail. The run must return exactly
`completed:activity-worker-crash:attempt=2:generation=2:previous_heartbeat=generation-1-attempt-1`.
The exact-run history must hold one scheduled, started and completed activity
event and no activity failure or timeout. The started event must record
attempt 2, generation 2's identity and the heartbeat-timeout last failure.

The suite is in `LIVE_REGRESSION_EXECUTABLES`, the CI smoke artifact list and
`run-live-regressions.sh`. The controller passes it the official CLI and
`_build/live-regressions/activity_worker_crash/`, where it keeps both
`describe` snapshots, the raw history and each generation's log. The Build
workflow uploads that directory with the suite logs. `make
test-activity-worker-crash-live` runs it against a disposable server.
`docs/reference/live-acceptance-coverage.md` records it in the #519 inventory
and drops the remaining-gap note.

Verified locally: `dune build --root . @install`, the new executable, a
failure-path run against a closed port (the driver fails, kills generation 1
and prints its log), `make test-temporal-live-regressions-contract`,
`test_temporal_compose_config.sh`, `test_ci_artifacts.py` and shellcheck of the
controller. Docker was unavailable, so the live result comes only from the
Build workflow's Temporal/PostgreSQL job.
## 2026-10-16: Worker shutdown during an outage and a lost workflow-task completion (#504)

The transport interruption live regression has two new worker scenarios.

A lost workflow-task completion: a deterministic workflow runs one local
activity and then a timer. The local activity callback, not workflow code,
holds the first workflow task open until the worker proxy drops responses, so
the task completes with the marker and a `StartTimer` command only then. The scenario requires the run's history to record
`TimerStarted` while the acknowledgement is still being dropped, then
restores the proxy. Core's completion call fails; a retry gets `NotFound` for
the applied task token, and Core evicts the run either way. The timer's
workflow task replays the full history without recording anything twice. The
scenario requires the exact result, exactly one `MarkerRecorded`,
`TimerStarted`, `TimerFired` and `WorkflowExecutionCompleted`, one local
activity callback, a still-running `Worker.run`, and a fresh run
completing afterwards. It accepts a retried, failed or timed-out workflow task
and logs their counts.

Worker shutdown during an outage: an idle worker with its own proxy and
`Runtime.t` polls until the proxy refuses all traffic for 2 s. Then
`Worker.shutdown_with_report` (1 s grace period, 3 s teardown timeout) must
return `Ok` within 6 s, with both lanes stopped and nothing abandoned, and
`Worker.run` must return `Ok ()`. Core sends `ShutdownWorker` without retries
and ignores `Unavailable`, so the teardown may be `` `Completed `` or
`` `Detached ``. After `restore` the worker must detach from its runtime,
`Runtime.shutdown` must succeed, and the proxy must have no open connection.

[Transport fault qualification](reference/transport-fault-qualification.md)
documents both scenarios and narrows the remaining #504 scope to TLS faults
(#496) and the side-effecting idempotency example, both left for later.

Evidence: `dune build --root . @install` and the suite executable build
locally, and `test/smoke/test_live_regressions_contract.sh` and
`test/smoke/test_live_acceptance_inventory_contract.sh` pass. The suite needs
the Compose Temporal stack, so its live run is in CI through
`make test-temporal-live-regressions`; this entry does not claim a local live
pass.

## 2026-10-16: Signed release provenance and a consumer of the source archive (#510)

The Release workflow's `publish` job now signs SLSA build provenance for every
file it uploads (the source archive, the bridge and SDK archives,
`manifest.json`, the Cargo SBOM, `THIRD-PARTY-NOTICES.txt` and `SHA256SUMS`)
with `actions/attest-build-provenance` v4.2.2, pinned by commit and
MIT-licensed. The step runs after the assets are complete and checksummed and
before the draft release is created, so a signing failure publishes nothing.
Only that job holds `id-token: write` and `attestations: write`.
`test/smoke/test_quality_contract.sh` checks those permissions, the subject
glob, and that the attestation precedes `gh release create`. The release
checklist shows how a consumer verifies an asset with `gh attestation verify`,
pinned to the Release workflow, `master`, and the tagged commit.

PR #1080 already built the release source archive inside an opam build
sandbox, but nothing consumed the result as an installed package. The new
`scripts/check-source-archive-consumer.sh` reuses that sandboxed build: it runs
`dune install -p temporal-sdk` from the unpacked archive into a fresh prefix
and builds the archive's own `test/fixtures/install-consumer` in a separate
Dune root against that prefix only. It then runs the public API witness
(native and `(modes byte)`), the worker-environment probe, and installed
replay of a frozen history. It refuses a switch that already provides
`temporal-sdk`. Both `source-archive-check` in `release.yml` and the master
`opam-source-offline` job run it after the sandboxed build.
`test/smoke/test_source_archive_consumer_check.sh` covers the script's
decisions on every `dune runtest`, using stand-ins for dune and opam.

The release checklist now separates what the workflow automates from what
still needs a maintainer: the qualification sign-off, creating the protected
tag, dispatching the workflow (it uses only the workflow token and OIDC, with
no stored publish secret), and the prerelease rehearsal, which has not been
done. The Release workflow cannot run from a pull request, so the attestation
step is first exercised by that rehearsal.
## 2026-10-16: Concurrent live worker and client lifecycle stress (#506)

Before this change, the lifecycle stress drove live activity workers from one
thread only, and the concurrent stress used replay workers only. A new test
in the `lifecycle_stress` binary, `live_stress::concurrent_live_workers_and_clients`,
runs live activity workers and submitted client calls on several threads at
once against the binary's gRPC double. The double now records heartbeats per
token and answers every third with `cancel_requested`. It holds history long
polls open until the client resets them, and counts only requests that
arrived whole. The stress covers:

- polling, worker completions and rejections, and worker heartbeats followed
  by Core's Cancel task and a cancelled completion;
- handoff to async completion, then throttled async heartbeats and async
  completions submitted as client calls (#1025);
- signals, workflow cancellation requests, and never-answered waits, some
  released before their outcome (#1019);
- initiate-shutdown and shutdown with and without a held lease (#1028), and
  disconnect;
- graphs on owned Cores or on a shared Core that the coordinator releases
  mid-round, released on their own thread or by a reaper thread while their
  owner still awaits their calls.

After the rounds, the test checks the following:

- every graph was cleaned once and every Core dropped once;
- the client call registry is back at its baseline, through
  `test_registered_client_calls`, a new hidden test hook;
- every held long poll ended;
- every owed completion arrived exactly once, and an unconfirmed async
  completion arrived at most once;
- the server never saw more heartbeats than the test sent;
- no panic came from bridge code on any thread.

Shutdown and waiting frees must finish within 20 seconds, and calls of a
released graph must end within 30 seconds.

The default budget (3 rounds, 4 threads, up to 48 operations per graph) runs
in about 1.5 seconds in the ordinary `cargo test`. The
`LIFECYCLE_STRESS_LIVE_{ROUNDS,THREADS,STEPS,ROUND}` variables scale it.
`make stress-qualification` runs 200 rounds of 12 threads with up to 96
operations per graph. On an Apple
M-series laptop that budget (seed `0x7e57`) leased 47,950 tokens across 4,800
graphs and 1,805 Core runtimes in 224 seconds and passed, with no panic of
any kind. A run of 60 rounds of 12 threads (seed `0x1234abcd`) took 67 seconds.

The first large run failed on the double, not the bridge. A completion RPC
that the client aborted mid-body was decoded as a completion with an empty
token. The double now discards a request whose stream was reset or whose body
is shorter than its gRPC length prefix.

The upstream Core teardown panic in the Findings section of
[bridge lifecycle stress](reference/bridge-lifecycle-stress.md#findings) was
re-checked against the pinned Core `8fe478e`. The unchanged
`poll_task_awaiter.await.unwrap()` is now at `poll_buffer.rs:452`, and the
panic still occurs: once with each of two seeds in 25,600 replay graphs. It
remains contained on a Tokio worker thread, with every graph and Core
released exactly once.
## 2026-10-16: A full client-call registry no longer closes async handles (#1104)

`Native_worker_policy.async_operation_disposition` mapped
`Resource_exhausted` to `Retired`. On the async activity path that status
only means the bridge refused to admit the submitted call because its
pending client-call registry was full (`max_in_flight_calls`), before any
RPC existed; a server `ResourceExhausted` already maps to `Connection`.
Retiring closed the handle for good, so a later `complete` could not be
delivered and the attempt could only end by server timeout.

The policy now has a fourth disposition, `Not_admitted`, for
`Resource_exhausted`. The adapter reports it to the base handle as
`Not_submitted` with a retryable `` `Bridge `` error: the handle and lease
stay live, the key is released unless an earlier uncertain terminal request
is still owed, and in that case the adapter keeps that request's retained
bytes so the exact retry resends them. Heartbeats, `complete`, `fail` and
`cancel` share the classification. #1103's heartbeat throttle sends
unthrottled when its own token table is full, but every async call,
throttled heartbeats included, still passes the registry's admission check.
Lease expiry (#1043) is unchanged and still bounds a handle whose calls keep
being refused. `Activity.Async_handle` documents the retryable error.

Evidence on macOS with OCaml 5.4.1: the fake supervisor in
`test/runtime/test_native_async_activity.ml` now models the registry with a
slot count and refuses with `Resource_exhausted` when it is full. New tests
check that `complete`, `fail`, `cancel` and `heartbeat` each return a
retryable `` `Bridge `` error naming `resource_exhausted`, send nothing, and
keep the handle and lease live, and that the same call succeeds once a slot
frees. They also check that a refused retry keeps an earlier uncertain
completion owed and later resends its bytes, and that a lease whose calls
are refused still expires. `test_native_worker_policy.ml` pins the
classification. Restoring `Retired` fails the policy test and the
terminal-operation test, and clearing the retained bytes fails the
uncertainty test. `dune build @install` and the runtime, unit,
sdk_supervisor and bridge test aliases pass.

## 2026-10-15: Worker idle pacing keeps sub-tick precision on Windows (#1008)

The adaptive idle pacing from #994 slept with `Thread.delay`, which on
Windows passes whole milliseconds to `Sleep`: a sub-millisecond backoff step
returned at once and a longer one lasted until the next 15.625 ms timer tick.
Under the simulated Windows model a serial activity step took about 21 ms,
so #994's latency win did not apply there.

`Native_worker_idle.local_yield` now sleeps through a new additive ABI
version 4 symbol, `ocaml_temporal_core_v4_pacing_sleep_ns`, called from the
`Native_bridge.pacing_sleep` stub with the runtime lock released. It is
`std::thread::sleep`, which on Windows 10 1803 and later uses a per-timer
high-resolution waitable timer (`CREATE_WAITABLE_TIMER_HIGH_RESOLUTION`) and
falls back to `Sleep` elsewhere; on Unix-like targets it is `nanosleep`, as
`Thread.delay` was. The bridge adds no unsafe or platform-specific code.
Alternatives were rejected: `timeBeginPeriod(1)` changes the timer
resolution for the whole process and, on older Windows, the whole system,
behind the application's back; waiting on the native readiness event
instead of yielding is not possible, because the local yield runs exactly
when the sibling lane holds the single supervisor (busy, or in its own
readiness wait), and adapting the backoff to whole ticks would keep the
15.6 ms floor. The pacing algorithm and its decisions are unchanged, so idle
CPU, bounded shutdown (#495) and cancellation (#494) behave as before.

Evidence on macOS with OCaml 5.4.1:
- `test/runtime/test_native_worker_idle.ml` serial chain, per step:
  1.1 ms natively and under `IDLE_TEST_SLEEP_MODEL=windows` (fixture sleeps
  tick-bound, lane yields through the pacing sleep, as on Windows), against
  21 to 23 ms under the new `windows-thread-delay` model, which reproduces
  the old yields. The test now also fails if the mean step is not below one
  15.625 ms tick whenever the lanes use the pacing sleep, so the Windows x64
  job checks the real timer. An idle worker still makes 20 polls and
  10 native waits per second; an idle stop returns in about 90 ms.
- `rust/core-bridge/tests/pacing_sleep.rs` and
  `test/bridge/test_pacing_sleep.ml`: a pacing sleep lasts at least the
  requested time, non-positive requests return at once, the runtime lock is
  released, the header clamp matches Rust, and the median of 21 one-millisecond
  sleeps is below half a tick (1.5 ms on macOS). `Sleep(1)` at the default
  resolution would last a whole tick, so these fail on Windows if the
  high-resolution timer is not used.
- `test/bridge/abi_harness.c` calls the new symbol from C.
## 2026-10-15: Async activity heartbeats are throttled like Core's (#1025)

#1081 made `Async_handle` heartbeats and completions submitted client calls,
so they no longer block the worker's owner Domain, but every heartbeat was
still its own server round trip: a completer reporting progress per item
sent one RPC per item, while a running activity's heartbeats go through
Core's throttling heartbeat manager. The Rust bridge now throttles async
heartbeats per task token with Core's rules
(`activity_heartbeat_manager.rs` at the pinned revision), in the new
`heartbeat_throttle.rs`: the first heartbeat is sent at once and returns
that send's outcome; later ones within the interval only replace the
pending details and return at once; a trailing send carries the latest
details when the interval expires, and a quiet interval forgets the token.
The interval is computed as Core computes it, 80% of the heartbeat timeout
or the worker's `default_heartbeat_throttle_interval`, capped by
`max_heartbeat_throttle_interval`. The heartbeat timeout reaches the bridge
in a new async heartbeat document (`Encoded_async_activity_heartbeat`,
schema `activity-async-heartbeat.schema.json`) that adds the start task's
timeout to the heartbeat; the submitted-call selector 12 takes it, while
the unthrottled synchronous symbol keeps the plain document.

Each call now reports the activity state of the token's latest send. The
bridge returns the server's `cancel_requested`, `activity_reset`, or
`activity_paused` flag (in Core's precedence) as a fixed outcome word, and
`Async_handle.heartbeat` returns a `Cancelled` error for it, shaped like a
running activity's: non-retryable for a cancellation request, retryable with
type `ActivityPause` or `ActivityReset` otherwise. The heartbeat was still
recorded and the handle stays live (new private submission outcome
`Activity_state`); previously these flags were dropped. A flag seen by a
trailing send reaches the caller on its next heartbeat. Unlike Core, which
only logs a failed background send, a failed send ends the token's interval,
so the next heartbeat is sent at once and reports the server's own answer.
A terminal `complete`/`fail`/`cancel` evicts the token first (cancelling a
sleeping trailing send, and flushing pending details before a cancellation,
as Core does).

Each throttled token has one driver task on Core's Tokio runtime owning a
client; the runtime owner keeps every driver's handle, and disconnect and
close abort and join them after the submitted calls, before the connection
is dropped. At most the connection's `max_in_flight_calls` tokens are
tracked; beyond that heartbeats are sent unthrottled. Because a throttled
call can return before its details reach the server, the async lease's
heartbeat expiry (#1043) now allows twice the heartbeat timeout after the
last heartbeat call.

Evidence:

- `rust/core-bridge/tests/async_activity_heartbeat_throttle.rs` (gRPC
  double): first send immediate, coalescing into one trailing send with the
  latest details between 1.5 s and the 2 s heartbeat timeout, state released
  after a quiet interval and the next heartbeat sent at once; worker
  maximum/default intervals applied; cancellation reported by the send that
  saw it, on coalesced calls, and after a trailing send; a failed trailing
  send ending the window (`not_found` on the next call); eviction by
  completion (no trailing send) and cancellation (pending details flushed
  first); disconnect and close releasing every token and driver with no later
  send; at most `max_in_flight_calls` tokens tracked and 3 x 200 tokens
  leaving no state; the document requiring its timeout.
  `activity_protocol.rs` covers the document's round trip and validation;
  the existing submitted-call tests use the new document.
- `test/runtime/test_native_async_activity.ml`: the heartbeat carries the
  start task's exact timeout; cancel/pause/reset map to the documented
  errors, keep the handle live, renew the lease, and allow the following
  `cancel`; heartbeat expiry at twice the timeout.
  `test/bridge/test_ocaml_activity_protocol.ml` checks the canonical
  document, its validation, and the outcome words.
- Local (OCaml 5.4.1, macOS): `dune build @install`, `@test/runtime/runtest`,
  `@test/unit/runtest`, `@test/sdk_supervisor/runtest`,
  `@test/bridge/runtest`; scoped `cargo test` (lib, abi, activity_protocol,
  decoder_fuzz, lifecycle, runtime_cleanup, client_input_limits,
  client_bridge, worker_shutdown_outstanding, both async activity suites);
  `cargo clippy --locked --all-targets -- -D warnings`; `cargo fmt --check`.

## 2026-10-15: A running worker holds one or two OCaml Domains instead of three, and a Domain shortage in `run` is a typed error (#1042)

Every running `Worker` held three Domains: its supervisor, an activity-lane
Domain spawned even for a workflow-only worker, and an activation-watchdog
Domain. That capped a process at about 42 running workers on OCaml 5.2 (128
Domains), slowed every stop-the-world minor collection by about 8.5 µs per
idle Domain, and was undocumented. When the activity Domain could not be
spawned, `run` reported an untyped `native worker execution lane failed`
defect that dropped the cause.

- `Native_worker_loop.run` takes `~activity_lane`; a workflow-only worker
  spawns no activity Domain, and its lone workflow lane keeps the native-wait
  preference instead of alternating with local yields. A failed
  `Domain.spawn` returns the caller's `~lane_unavailable` error before either
  lane polls; the native worker maps it to a retryable `` `Bridge `` error
  (`domain_unavailable`) that names the Domain limit and keeps the cause, so
  the same worker can run again later. Any other escaped lane exception is
  still a defect, now with the exception text.
- The watchdog no longer owns a Domain. A system thread on the workflow
  lane's own Domain was rejected: it runs only when the lane hands over that
  Domain's runtime lock, which OCaml code does at periodic safepoints (a
  10 ms sampler ran about 16 times a second against a spinning loop on
  5.4.1) and lock-holding C code never does. Instead the sampling thread runs
  on the worker's supervisor owner Domain, which executes only SDK code and
  waits for Core with the lock released. `Sdk_supervisor.Make` gains a
  `Spawn_thread` request (`spawn_owner_thread`) that starts a companion thread
  there without exposing backend state. Because `Domain.join` waits for every
  thread on a Domain (verified on 5.4.1), the watchdog also exits once the
  worker begins its native release (`native_release_started`, set before
  every `Native.shutdown` or `initiate_shutdown`), so a stuck lane cannot
  block the supervisor's join.
- `Owner_unavailable` diagnostics for workers, clients and replays now name
  the Domain limit.
- The Domain budget per instance, the per-version limits (63 running
  activity workers or 127 workflow-only ones on threads of the main Domain,
  42 or 63 with one Domain per `run`, from 42 and 31 before), and why the
  supervisor and activity Domains stay are documented in `Temporal.Worker`
  and in the
  [native worker reference](reference/native-worker-execution.md#domain-budget).

Evidence: `test/runtime/test_worker_domain_budget.ml` runs under
`OCAMLRUNPARAM=d=1` (main Domain only) and checks that a workflow-only loop
and the watchdog work without spawning a Domain and that a failed activity
spawn is the typed error with no lane run; it skips the exhaustion part on
OCaml 5.2, which ignores `d`. `test_sdk_supervisor` checks that a companion
thread runs on the owner Domain, can use the supervisor, delays `shutdown`
until it exits, and is refused after shutdown. The watchdog tests cover the
exit on `host_closing` and a failed spawn. `dune build @install` plus the
runtime, unit, sdk_supervisor and bridge suites passed four times, and the
watchdog, budget, supervisor, loop and bounded-shutdown executables ten
times each, on OCaml 5.4.1 (macOS arm64). The live non-yielding-workflow
qualification is left to CI.

## 2026-10-14: Live-regression logs retained; coverage inventory, sizing guide and release checklist (#498, #504, #510, #519)

The Temporal/PostgreSQL job now uploads `_build/live-regressions/*.log` and the
official-SDK interoperability evidence as a `live-regressions-<run>-<attempt>`
artifact, in an `always()` step with the same seven-day retention and pinned
`upload-artifact` revision as the diagnostic bundles. Before, those logs were
only printed to the job output. The copied admin-tools CLI in the same
directory is excluded. Every step of that job now has its own timeout and the
job timeout (now 80 minutes) is at least their sum, so a hung step times out
and the evidence uploads after it still run; `test-temporal-config` enforces
this.

Releases now publish reviewed, versioned notes. The version PR commits
`docs/releases/<tag>.md` with non-empty `Changes`, `Breaking changes and
migration`, and `Residual limits` sections. `scripts/check-release-notes.sh`
gates `make release-tag-check` and the Release workflow's validate job, and the
publish job puts those notes ahead of its generated asset description instead
of replacing them. `test/smoke/test_release_notes_contract.sh` (part of
`make test-quality-contract`) covers the checker, the notes for the checked-in
version, and the workflow wiring. The first notes,
[`v0.1.0-rc.1`](releases/v0.1.0-rc.1.md), name the support policy's residual
limits.

[Live acceptance coverage](reference/live-acceptance-coverage.md#worker-crash-restart-shutdown-and-eviction-inventory)
now maps the three #519 boundaries (worker removal during pending work,
shutdown with outstanding work, eviction followed by continued progress) to the
crash-recovery, cache-eviction and parent/child restart controllers and the
`interaction_recovery` and `bounded_shutdown` regressions, with the commands
that reproduce each. It names the remaining gap: no suite kills a worker
while an activity callback is running.

The [worker sizing guide](guides/worker-sizing.md) (#498) covers the validated
`Worker.Options`, `io_threads` and shared runtimes, the sticky cache, small and
production starting points, and the metrics that show saturation. It states
that each worker runs one activity callback at a time (#777, #492). The
[release checklist](reference/release-checklist.md) (#510) lists the version,
gate, tag and verification steps and the sign-off roles, marks #506 to #510 and
#489 as open, and describes how to supersede a bad candidate or release
because the version-tag ruleset forbids moving or deleting a tag.

None of the four issues is complete. Verified with `actionlint` on the edited
workflows, YAML parses of them, `make test-quality-contract` and
`make test-temporal-config` (Docker-free contracts), and a relative-link check
of the edited Markdown.

## 2026-10-14: Malformed-history controls and feature coverage for the replay corpus (#503)

The [replay history corpus](reference/history-corpus.md) gains a third
expected verdict, `invalid_history`, and five malformed-history negative
controls derived from committed histories: a truncated protobuf, a gap in
event IDs, a start event without a task queue, a first workflow task Core
auto-fails, and a history replayed under another execution's workflow ID.
Each reuses a `replays_ok` entry's history unchanged on disk; the runner
applies the entry's named `history_mutation` in memory before replay
(`corpus_history_mutation.ml`), so the source entry and the edit are the
complete provenance and no derived binary is committed. Every control pins
its `Temporal.Replay` refusal reason with `expected_message`.

The runner's `required_features` list became a feature coverage table
(`corpus_coverage.ml`) mapping every replay-relevant supported feature to a
`replayed` case, a nondeterminism or invalid-history control, or an explicit
`not applicable` or `not yet captured` reason. The runner fails when a
feature tag has no row, a covered row has no case, a gap row gains a
`replays_ok` case without being promoted, or the table in the reference
document differs; the report carries the table with its case IDs. The known
gaps (cancellation, activity and workflow timeouts, workflow retries, local
activities, external signals and cancellation, start metadata,
search-attribute upserts, and time/random-dependent commands) still need live
captures. Two new `test_history_corpus_mismatch` scenarios check that a
control whose edit stops applying fails, and that unclassified and stale
coverage is reported.
## 2026-10-14: Installed consumer replays fixture histories (#497)

The installed-package consumer previously only type-checked
`Temporal.Replay` through `public_api.ml`. A new consumer executable,
`test/fixtures/install-consumer/installed_replay.ml`, now replays the frozen
history-corpus capture `live-2026-10-08-timer.pb` through the installed
package and asserts both verdicts: the recorded `corpus.timer` code replays
cleanly, and the same workflow type with its timer removed is reported as
`Nondeterminism` at the recorded `TimerStarted` event for workflow ID
`history-corpus-timer`. `test/bridge/test_install.sh` (`make test-install`)
and the relocated binary-only consumer (`test/bridge/test_prebuilt_library.sh`,
prepared by `scripts/ocaml-library-artifact.py prepare-consumer`) both copy
the history next to the consumer and run it. No new capture was made.

The oversized-history bound now has direct evidence.
`test/replay_cli/test_replay_cli.ml` creates a sparse file of
`Temporal.Replay.History.max_bytes + 1` bytes with `Unix.ftruncate` and
requires the example replay command to exit 3 with
`invalid history: larger than 134217728 bytes`. `test/bridge/test_public_replay.ml`
passes a string of the same length to `Replay.History.of_protobuf` and
requires the documented size rejection.

Issue #497 stays open until the v1 support policy (#489) is formally closed.
## 2026-10-14: Metrics scrape connections are capped (#500)

The bridge's public metrics accept loop spawned one relay per connection with
no limit, and a relay that had received its request could be held open
forever by a client that stopped reading or kept the connection alive. The
accept loop now takes one of 8 semaphore permits (`MAX_CONCURRENT_SCRAPES`)
before spawning a relay. A connection over the cap never connects to Core and
never touches the descriptor reserve; it receives a `503` from a short
rejection task (at most 16 pending, 1 s each) or is closed at once. Relays end
after 10 s without traffic and 60 s in total, in addition to the existing 5 s
request-head timeout, so slow and idle clients free their slot.

Evidence: `rust/core-bridge/tests/metrics_connection_cap.rs` saturates the cap
with idle and kept-alive clients, checks that idle over-cap bursts are closed
and further scrapes receive the busy `503` (never metrics) promptly, that the
bridge closes every holder, that real scrapes are served afterwards and that
the descriptor count returns to its baseline. The four metrics test binaries
passed four consecutive runs on macOS ARM, with `cargo clippy --locked
--all-targets -- -D warnings`, `cargo fmt --check` and
`dune build --root . @install @test/bridge/runtest`. Remaining #500 work:
OCaml-side metrics for SDK-only paths, a sample deployment that demonstrates
the declared failure signals, and OTLP export.
## 2026-10-13: Shutdown stops Core polling early, retires local activities without retry, reports accurately, and async activity calls leave the owner Domain (#1028, #1029, #1023, #1025)

Temporal Core kept long-polling the server for the whole grace period,
because it was told to stop only at the native release; every task it took
meanwhile waited for a lane that would never run it and was then failed, a
spurious workflow-task failure or a lost activity attempt on every rolling
deploy (#1028). A new bridge operation,
`ocaml_temporal_core_v4_worker_initiate_shutdown` (supervisor operation
`Initiate_worker_shutdown`), runs `PollLanes::initiate_shutdown` without
releasing anything, as the official SDKs' `initiate_shutdown` does: Core
stops polling and starts its own graceful-shutdown timer, while leased tasks
stay completable and heartbeatable. A stopping `Worker.run` sends it right
after its workflow lane returns (never after a lane failure), and the
bounded-shutdown thread sends it before the lanes phase; one atomic makes it
a single request. `run` starts its request on a detached thread and never
awaits it, so a supervisor mailbox occupied by a stuck activity-lane native
call cannot keep `run` past the grace deadline (Codex review).

Shutdown and disposal completed a retired local-activity token with a
retryable failure, which Core's local-activity manager retries in-process
with fresh tokens for about a minute, so teardown stalled past its timeout
and was reported detached (#1029). The bridge now classifies the token with
Core's own `TaskToken::is_local_activity_task` (the predicate Core uses to
route the completion) and reports a retired local activity as cancelled,
which Core resolves without retrying; its workflow task is failed by the same
drain, so the next worker re-executes it. Remote activities keep the
retryable failure.

The bounded-shutdown report miscounted (#1023). `callback_running` now reads
an atomic set around both synchronous and asynchronous callbacks, so an
abandoned `define_async` callback is reported as `activity_callback` and the
probe never waits on the delivery lock a heartbeat sweep holds. The run loop
detaches the activity lane at the deadline only while a callback runs (the
atomic probe, not the lane's per-poll busy flag, which CI showed could be
sampled mid-poll on an idle lane), joining an
idle lane for up to 150 ms more; the watchdog sleeps in 20 ms slices so its
stop is prompt. A worker with a grace period of zero or a few milliseconds
no longer logs `activity_lane_detached` or reports `lanes_stopped = false`
when nothing was running.

`Async_handle` completions and heartbeats were blocking RPCs on the worker's
single owner Domain, with the adapter's async lock held across them (#1025).
They are now submitted client calls (#807), selectors 11 and 12 of
`ocaml_temporal_core_v4_client_submit_json`, which run the RPC on a Rust task
while the caller's thread awaits it; a server `NotFound` is the fixed outcome
`not_found`, mapped back to `Invalid_state`, because an awaited
`Invalid_state` means a closed call. The adapter holds its async lock only to
claim a lease and record the outcome. Async heartbeats are still not
throttled; the `Async_handle` docs state their cost. Throttling with a
trailing flush, as Core does for synchronous heartbeats, needs a timer that
outlives the caller and is left for a follow-up.

Rust tests: `worker_shutdown_outstanding.rs` shows that after the initiation
the leased task still completes, Core polls no more although the slot was
freed (the assertion fails without the initiation), and the release fails
nothing; `worker_local_activity_retirement.rs` covers the local/remote
outcome; `async_activity_submitted_calls.rs` drives the submitted calls
against a gRPC double, including `NotFound`, validation before the RPC, and
a fast heartbeat completing while a slow one is still held by the server.
OCaml tests: `test_native_worker_bounded_shutdown.ml` (polling stopped by
both `run` and the shutdown thread during the grace period, a zero-grace
idle worker not detached, a stuck idle wait still bounded, no stop after a
lane failure, a blocked polling stop not holding `run`), `test_bounded_shutdown.ml` (the stop queued behind a blocked
supervisor operation keeps shutdown bounded), `test_native_worker_watchdog.ml`
(prompt stop) and `test_native_async_activity.ml` (an async callback reported
running; the lease probe and a new handoff proceed while an async RPC is in
flight). The new OCaml and Rust shutdown assertions were checked to fail
with each fix reverted. Verified with `dune build @install`, the
`test/runtime`, `test/unit`, `test/sdk_supervisor` and `test/bridge`
aliases on OCaml 5.4.1 (macOS), scoped `cargo test`, `cargo clippy
--all-targets -D warnings` and `cargo fmt --check`. The live regression the
issue suggests (a client scheduling work during the grace period against a
real server) was not run.
## 2026-10-13: CI builds the source archive inside an opam sandbox (#778)

The vendored release source archive (2026-10-06 entry) had only been proven
offline by hand; no CI job ran a sandboxed opam build from it.
`scripts/check-opam-source-archive.sh` now unpacks an archive and runs the
package's opam build command, `dune build -p temporal-sdk -j <jobs>`, through
opam's own build wrapper (`opam-init/hooks/sandbox.sh build`) or, on macOS
without one, an equivalent `sandbox-exec` policy. It first proves the sandbox
blocks a request to crates.io, clears every variable that would bypass the
archive's vendored sources, gives Cargo a fresh, empty `CARGO_HOME` so a home
warmed by `cargo vendor` cannot hide a source the archive omits, and requires
the resulting `temporal-sdk.install` to list the Rust bridge. The Release
workflow now creates the archive and audited notices once in a
`source-archive` job, and its macOS ARM64 `source-archive-check` job runs the
check on that artifact after confirming its SHA-256; `publish` needs both and
confirms the digest again before publishing the same file, so the archive
users download is the one that passed. Master builds also run an
`opam-source-offline` job in `build-pr.yml` on an archive it creates itself,
as an early warning.

`test/smoke/test_opam_source_archive_check.sh`, part of `dune runtest`, drives
the script with stand-in opam hook, curl and dune executables: a complete
archive builds through the hook's `build` mode with the bypass variables
cleared, a sandbox that lets the probe through is refused before building, and
archives without `rust/vendor.tar` or with a changed opam build command are
rejected; Cargo must see a cold home in place of the caller's warm one.
Removing the variable clearing or keeping the caller's `CARGO_HOME` makes it
fail. Locally on macOS
(opam 2.5.2, OCaml 5.4.1), an archive created by
`scripts/create-source-archive.sh` from this branch built successfully with the
optimized Rust bridge through both the real opam 2.5.2 sandbox wrapper and the
`sandbox-exec` fallback, with the network probe denied in each, and again
through the opam wrapper with the empty `CARGO_HOME`.
`dune build @test/smoke/runtest` and `test/smoke/test_quality_contract.sh`
pass. The new jobs have not run yet: the master job's first result comes from
the next master build, and the release jobs from the next release.

## 2026-10-13: Unknown update stages and visibility statuses degrade (#1078)

The client protocol rejected two server enums when a newer server sent a value
this SDK does not know, so an added update lifecycle stage made
`start_update` fail and an added execution status failed a whole
`list_visibility` page. Start-update now trusts an outcome whatever stage
accompanies it, and reads an unknown stage without an outcome as "not yet
accepted": the update is re-issued under the same ID until a known `Accepted`
stage or the typed `deadline_exceeded` error, so it never becomes a handle
without established durability. A `Completed` stage without an outcome still
fails closed. A visibility row with an unknown status is reported as
`unknown_<n>` with its canonical decimal wire value; an unspecified (`0`)
status is still a malformed row. The Rust encoder and OCaml decoder accept the
same vocabulary, including rejecting non-canonical spellings and
`unknown_<n>` for a named value, and the public `Client.visibility_execution`
status field documents it.

`rust/core-bridge/tests/support/client_update.rs` drives a scripted Core
connection that returns an outcome with an unknown stage and an unknown stage
before `Accepted`; `client_protocol.rs` unit tests cover status conversion and
the shared vocabulary; `test/bridge/test_ocaml_client_protocol.ml` decodes
`unknown_<n>` rows and rejects malformed spellings. Verified with the scoped
`cargo test --lib client_protocol`, `cargo clippy -D warnings`, `cargo fmt
--check`, `dune build @install` and the OCaml client protocol test on OCaml
5.4.1 (macOS).
## 2026-10-13: Released client calls free their capacity at once (#1019)

`Client_call.await` releases a call it leaves without a terminal read, for
example when an asynchronous exception or a decoder exception interrupts
`Client.wait`. Release unregistered the call's completion cell but left its
Tokio task running. The owner pruned only finished tasks and counted every
remaining task against the wait and in-flight limits. An exact-run wait has
no total deadline, so each abandoned wait on an open run kept a server long
poll and both slots until the run closed or the client shut down. After
`max_pending_waits` such abandonments, every new wait returned
`Resource_exhausted`.

The completion registry now holds each task's Tokio `AbortHandle`, attached
by the owner right after spawning. `release_call` closes the cell and aborts
the task from whichever thread releases it. This needs no runtime pointer.
At each submission the owner marks any call that has left the registry as
`Released` and aborts it again, which is idempotent. A released call holds no
start, wait or in-flight slot, even before its aborted task has finished.
Its join handle stays in the owner's task map, so disconnect and close still
join it. Because an abort finishes only once Tokio runs the cancellation,
submission joins the released tasks whenever the task map has reached the
in-flight ceiling, so the map never retains more than `max_in_flight_calls`
handles. The C header, the release stub comment and
`docs/reference/client-protocol.md` describe the
new release contract.

Four new tests in `rust/core-bridge/tests/support/client_calls.rs` use the
callback transport, whose history long polls never answer:

- releasing a pending wait drops its transport long poll with no further
  submit or disconnect;
- with the default limits, `max_pending_waits` new distinct waits are
  admitted right after one wait is released;
- with configured limits of two waits and two in-flight calls, releasing
  both waits admits two new ones at once;
- a burst of 500 submit-and-release cycles under a two-call ceiling never
  retains more than two task handles.

All four fail without the fix. Verified with the crate's library tests and
`tests/abi.rs`, `cargo clippy --lib --tests -D warnings` and
`cargo fmt --check` (macOS).

## 2026-10-13: A raising signal handler no longer leaks bridge borrows (#1018)

In OCaml 5, `caml_enter_blocking_section` runs pending signal handlers and
re-raises their exceptions. The bridge stubs entered it after taking a counted
runtime or shared-runtime borrow, after `runtime_close` had detached the
native pointer, or while holding a `malloc`'d input copy. A raising handler,
such as `Sys.Break` under `Sys.catch_break true`, therefore unwound the stub
and leaked that resource: the next close spun forever on the leaked borrow,
the GC finalizer could spin while holding the domain lock, or a detached Core
graph was never freed. Every stub in `lib/core_bridge/native_stubs.c` now
releases the lock through one helper that calls
`caml_enter_blocking_section_no_pending`, which every supported compiler
series (5.2 to 5.5) provides. A pending handler still runs, at the first OCaml
poll point after the stub has returned and released everything it owns.

`test/bridge/test_signal_borrow_release.ml` makes SIGUSR1 pending with a test
C helper that does not run OCaml handlers, then calls the production stubs
directly: the no-input and JSON runtime paths, client submit, shared-runtime
attach, and close. It checks that each stub returns and the raising handler
then runs exactly once, and that every runtime and the shared runtime close
afterwards. Without the fix the test fails at the first stub. It is skipped on
Windows, which has no SIGUSR1. Verified with `dune build @install` and
`dune build @test/bridge/runtest` on OCaml 5.4.1 (macOS).

## 2026-10-13: Teardown cleanup keeps the terminal command and eviction ack (#1021)

When a run completed or was evicted, the scheduler discontinued every parked
fiber inline, so user `Fun.protect ~finally` blocks and exception handlers ran
during teardown. If that cleanup raised (for example `Workflow.start_sleep`,
`Activity.start` or `upsert_search_attributes` after the run ended), or a
catch-all around an await turned the private shutdown exception into a
defect, the error reached `fail_task`. On the terminal path that discarded the
already-buffered `Complete_workflow`, `Fail_workflow` or cancellation command
and failed every replay of the task; on the eviction path it produced an
eviction completion with a task failure, which the Rust bridge rejects, so
`Worker.run` stopped with the eviction lease outstanding.

The scheduler now records exceptions that escape fibers after shutdown
separately (`Scheduler.take_teardown_failures`), as it does for exceptions
raised by the aborted fiber's own cleanup after continue-as-new or `terminate`
buffered the terminal command, and `run` reports only failures raised while
it was active and not aborting. The execution logs those teardown
failures as a warning with a count only, and a task-failing error reported by
workflow code once the run is terminal, has a buffered terminal command, or
is evicted is logged and dropped
instead of poisoning the task; after eviction every error takes that path. A
defect raised before the terminal command still discards the batch.
`Native_execution.activate_translated` always acknowledges a cache eviction
with an empty completion, even for a generation poisoned earlier, so the OCaml
and Rust eviction validators agree.

`test/runtime/test_handler_teardown.ml` covers signal and update handlers
whose `finally` starts a timer, upserts search attributes or raises, for both
completion and eviction; a catch-all root or handler that converts the
shutdown exception into a defect; a pre-terminal handler defect that must
still fail the task; native eviction acknowledgements for a failing
cleanup and for a poisoned generation; and a raising `finally` around
continue-as-new in the root and in a handler, and around a cancelled root. `test/runtime/test_scheduler.ml`
checks that a cleanup exception after shutdown is a teardown failure while an
active one is still `Failed`. Verified with the full `test/runtime` and
`test/unit` aliases on OCaml 5.4.1 (macOS).
## 2026-10-13: Runtime attachment and abandoned-worker cleanup contracts (#1044)

`runtime.mli` said a client or worker stays attached to a shared runtime
until its own `shutdown` has returned, but a worker whose bounded shutdown
returned with a `Detached` native teardown stays attached until that
background teardown finishes, as `worker.mli` already said, so a following
`Runtime.shutdown` returned an unexpected "still attached" defect. The
runtime interface, `Runtime.shutdown`, `Worker.Shutdown_report.teardown` and
the defect message now state the real rule: attachment ends when native
teardown completes.

The abandoned-worker finalizer comment claimed that keeping the worker
reachable from its cleanup thread stopped the supervisor's own finalizer from
closing the native graph before the drain finished. `Gc.finalise` runs the
finalizers of every value found unreachable in the same collection, so the
supervisor's cleanup could already be scheduled. The supervisor now has a
private `delegate_abandoned_cleanup` switch that turns its own finalizer into
a no-op; `Native_worker.create` sets it, making the worker's
drain-then-shutdown finalizer the only path that closes an abandoned worker's
graph. Every finalizer path that cannot spawn its cleanup or terminal-retry
thread, including the pending-retry path, now closes supervisor admission
itself (`Native_worker_policy.spawn_or_close_admission`), as the supervisor's
finalizer used to, so the owner Domain stops and the native custom-block
finalizer can still reclaim the graph and its shared-runtime lease.

`test/sdk_supervisor/test_sdk_supervisor.ml` abandons an owner and its
delegated supervisor together and checks that the backend closes exactly
once and only after the owner's slow drain; with the switch disabled the test
fails on every run. `test/runtime/test_native_worker_policy.ml` checks that a
failed spawn closes admission, a successful one does not, and a raising close
is contained. Verified with `dune build @install`, the `test/sdk_supervisor`,
`test/unit` and `test/runtime` aliases and `test_shared_runtime` on OCaml 5.4.1
(macOS).

## 2026-10-13: Child-start acknowledgement for abandoned children (#1040)

Temporal Server does not start a child whose parent has already closed, even
with `Parent_close_policy.Abandon`, and the public API had no way to wait for
the child's start: `Child_workflow.start` and `start_handle` resolved only on a
failed start or on the child's result. The documented "start an `Abandon`
child and return" pattern therefore lost the child silently whenever the
parent completed in the same workflow task.

`Child_workflow.started : 'output handle -> (string, Error.t) Future.t` now
resolves with the child's run ID when Core reports the start acknowledgement,
and with the same error as the result future when the child never starts
(start failure, or cancellation before the start). The runtime allocates both
futures with the child command, so the start stage is settled by the same
activation jobs and stays replay-safe. `child_workflow.mli`, the workflow
guide and the [operation policy reference](reference/durable-operation-policies.md)
now tell parents to await `started` before returning when the child must
outlive them, matching the Go, Java, Python and TypeScript SDKs.

`test/runtime/test_activation.ml` checks that a parent awaiting `started`
emits its completion only after the acknowledgement and receives the run ID
while the result stays pending, and that a start failure and a pre-start
cancellation settle the start stage with the result's error. The install
consumer fixture pins the new signature. Verified with `dune build @install`,
the `test/runtime` and `test/unit` aliases, and the install-consumer fixture on
OCaml 5.4.1 (macOS).

## 2026-10-13: Reference documents match the code and evidence (#1026)

Four reference statements had drifted from the code or their own evidence.
The live-evidence record's Pins row named Core `8fe478ee` for the
September 19 audited run, but the manifest at that baseline (`beae10d0a58e`)
pins `95e97686`. The row now names `95e97686`. The Core v1.1.0 upgrade's own
live evidence is cited separately: the successful Temporal/PostgreSQL job of
the Build run at the upgrade commit `acbda8da`. `native-interactions.md` now
describes the fail-closed signal policy from #811 (a missing handler or
undecodable signal fails the workflow task, not the run). `package-boundary.md`
lists six, not four, public types equal to package-private types, adding
`Runtime.t` and the constructor-bearing `Activity.Cancellation.reason`. The
child-start row of `native-execution-translation.md` now says that task queue
and parent-close policy are carried end to end. Each claim was checked against
`lib/runtime/execution.ml`, the public `.mli` files,
`lib/runtime/native_execution.ml`, `rust/core-bridge/src/workflow_protocol.rs`,
and the CI run and job logs. Documentation only; no behavior changed.
## 2026-10-13: Bytecode consumers link outside the SDK workspace (#1020)

The documented Dune `(modes byte)` link mode worked only inside the SDK's own
workspace, whose `dune-workspace` disables dynamically linked foreign
archives. The installed `temporal_core_bridge.cma` records the stub and Rust
archives as `-dllib` entries, but only the static `.a` files are installed.
An ordinary consumer project therefore linked a byte executable in
dynamic-stub mode, and `ocamlc` failed looking for
`dllocaml_temporal_core_bridge`. The bridge library is now archived with
`ocamlc -a -custom` and `-ccopt -L"$CAMLORIGIN"` (through `ocamlc_flags`,
because `ocamlopt` rejects `-custom`). Every bytecode link of the SDK,
including Dune `(modes byte)` and `ocamlfind ocamlc -linkpkg`, now builds a
custom runtime that embeds the static archives and finds them in the
relocatable install directory. Supporting bytecode was kept rather than
dropped: the custom runtime needs nothing the static archives do not already
provide. The install-consumer fixture now links `main` as `exe` and `byte`.
`test/bridge/test_install.sh` and the prebuilt-library consumer build and run
`main.bc` in a separate Dune root without the SDK's `dune-workspace`. The
README, workflow guide and Core bridge reference describe the supported modes;
the toplevel and native plugins remain unsupported.
## 2026-10-13: Per-series ocaml/opam pins Dependabot can refresh (#1027)

The daily Dependabot Docker job failed or proposed retargeting a compiler
stage to another series instead of refreshing the pinned `ocaml/opam`
digests. All four stages lived in `Dockerfile.dev`, which Dependabot treated
as a single dependency. Its `semver-minor`/`semver-major` ignore also had no
effect: candidate tags such as `debian-12-ocaml-5.3` are compared by their
trailing number (5.3), but the ignore ranges are derived from the whole current
tag, read as 12.5.2. Each series now pins its image on the single `FROM` line
of `docker/ocaml-<series>/Dockerfile`, which has its own Dependabot entry
ignoring `ocaml/opam` versions from the next series upwards. The current tag
stays eligible, so only its digest moves. `Dockerfile.dev` no longer names
`ocaml/opam`, and the Makefile and live-test scripts resolve a series to its
pinned reference with `scripts/ocaml-image.sh`.
`test_build_tool_dependencies.sh` checks that the pin files, the Dependabot
ranges and the CI matrix agree, and its mutation companion rejects a
retargetable entry, an undigested pin, an extra series and a second pin in
`Dockerfile.dev`. The dependency inventory reads the new pin files. Whether
Dependabot now opens per-tag digest PRs can only be confirmed by its first
scheduled run after merge.
## 2026-10-13: Replay rejects histories it cannot truly check (#1045, #1046, #1047)

Offline replay could report `PASS` without running any workflow code, or
replay a run under the wrong identity. Core auto-fails a run's first
workflow task itself when it cannot apply an event there, and evicts the run
with reason `Unspecified`. `Temporal.Replay` treated that reason as cache
housekeeping, so a history scrubbed of its `eventTime`s replayed as `Ok ()`,
even against incompatible code. Replay now classifies that eviction as
`Invalid_history`. As an independent guard, `Ok ()` also requires that the
registered workflow received the run's `Initialize_workflow` job.

The bridge's admission gate adds the checks that Core's `HistoryInfo` lacks.
A non-empty workflow ID recorded in the start event (Temporal Server 1.21+)
must equal the supplied one. The documentation wrongly said histories do not
record it, and the example CLI silently replayed `order-2.pb` as `order-1`.
The start event must have a task queue, because Core's replay poll unwraps it
and its panic stalled replay for 30 seconds before reporting `Replay_error`.
Event IDs must run from 1 without gaps, and every `WorkflowTaskStarted` event
needs a timestamp. Each rejection has its own static ABI message. The example
CLI now reads pipes, process substitution, `/dev/stdin`, and `-` (standard
input) without seeking. The README conversion recipe accepts legacy
PascalCase-enum JSON from the Web UI and `tctl`, ignores unknown fields, and
merges several pages. The docs explain that a single `GetWorkflowExecutionHistory`
response is only one page.

Evidence: new Rust unit tests in `tests/support/replay_bridge.rs` cover each
precondition and the ABI messages. `test/bridge/test_public_replay.ml`
derives invalid histories from a live fixture with a small protobuf wire
editor. It requires `Invalid_history` against both compatible and changed
code for scrubbed timestamps, an event-ID gap, a missing task queue, and a
first-task Core auto-fail (the last case failed with `Ok` before the fix). It
also checks the recorded-ID rules. `test/replay_cli` covers a mismatched ID,
piped standard input, and `/dev/stdin`. The recipe was checked by converting
the example's CLI JSON and a two-page PascalCase variant with unknown fields;
both produced the checked-in `.pb` byte for byte. Verified with
`dune build @install`; the `test/bridge` replay tests, `test/replay_cli`,
`examples/replay`, and `test/history_corpus` aliases; and the Rust
`replay_bridge`, `replay_abi`, and `application_failure_replay` tests, plus
`cargo clippy -D warnings` and `cargo fmt --check`, on OCaml 5.4.1 (macOS).
## 2026-10-14: Windows metrics scrape timeout is really 30 s (#1072)

The intermittent Windows failure of `test/bridge/test_runtime_metrics.ml`
("timed out after 30 s with 0 byte(s) received") was in the test's scrape
client, not the metrics endpoint. The helper bounded each read with
`Unix.setsockopt_float socket SO_RCVTIMEO 30.`. On Windows, OCaml passes that
option as a `struct timeval`, but Winsock reads `SO_RCVTIMEO` as a `DWORD`
of milliseconds, which is the `tv_sec` field. The bound was therefore 30 ms
(5 ms before #1058), so any scrape the loaded runner did not answer within a
few milliseconds failed with nothing received. Linux and macOS read the
`timeval` correctly, which is why only Windows failed. #1053's relay adds a
loopback hop, but the failure predates it.

The helper now waits with `Unix.select` against one deadline in seconds,
which behaves the same on every platform. A timeout raises a dedicated
exception naming the scrape (first scrape after `create`, after the
busy-address `create`, or after 64 reset connections), the waiting stage
(first response byte or rest of the response), the measured elapsed time and
the bytes received. A new case points the helper at a loopback listener that
never accepts and requires its 1 s timeout to last about 1 s. The old helper
fails that case on Windows after about 1 ms. Verified on OCaml 5.4.1 (macOS)
with `dune build @install` and `dune build @test/bridge/runtest`. The Windows
x64 CI job is the cross-platform gate. `test/integration/worker_metrics`
still uses `SO_RCVTIMEO`, which is correct there because that test runs only
on Linux under Docker Compose.

## 2026-10-13: Temporal.Testing follows server and native-worker semantics (#1035-#1039)

`Temporal.Testing` (`lib/runtime/test_environment.ml`) now matches a server
and the native worker in five places where tests could pass but production
would behave differently:

- **Final-activation commands (#1035).** When an activation's commands
  close the run, the engine drops activity and local-activity schedules,
  child starts (including `Abandon` children), child cancellations, and
  external signals and cancellations from that batch. The server records
  these commands but never dispatches them for a closed run.
- **Core job order (#1036).** Each activation's jobs are stable-sorted into
  Core's order: initialization; patches and seeds; signals and updates;
  other jobs; local-activity resolutions; queries. A signal that arrives
  with a timer now runs its handler before the root completes, so an
  acknowledged signal can no longer be lost.
- **Child retry policies (#1037).** A child that fails with a retry policy
  is retried as a new run of the same chain. The new run has `attempt + 1`
  and a `Retry` continuation, and its first activation waits out the
  backoff in virtual time. The child's `Workflow.info` reports the policy
  and the command's task queue. The parent sees a non-retryable
  `Child_workflow` error only once the retries end. Unlimited policies use
  the `max_activity_attempts` cap.
- **Native error shapes (#1038).** An activity's `Cancelled`-category
  error is retried and wrapped like any other failure. Only cancellations
  produced by the engine reach the workflow as `Cancelled`. Final activity
  failures are non-retryable and typed with the cause's application type.
  Dispatch and codec failures are typed `ocaml_temporal_native_activity`,
  as in `native_activity_execution.ml`. A child that *fails* with a
  `Cancelled` error reaches its parent as `Child_workflow` with no
  application type, as Core reports a canceled failure.
- **Smaller gaps (#1039).** Virtual-time additions saturate, so an
  overflowing timeout means no limit and `skip` cannot wrap the clock
  negative. Query-only activations no longer add to `history_length`. A
  mock replaces the ordinary registration wherever it appears in the list.
  The documented differences now say that workflow code is never replayed.

`test/unit/test_testing.ml` adds one regression test per issue, covering
each scenario above, including the coincident timer-and-signal case from
#1036. Disabling the job sort makes that test fail. Verified with
`dune build @install` and the `test/unit` test alias on OCaml 5.4.1 (macOS).
## 2026-10-13: Client reasons and visibility queries are checked for UTF-8 (#1014)

`Client.cancel`, `Client.terminate` and `Client.reset` used to check their
`~reason` only for length and NUL, and `Client.list_visibility` did the same
for its `~query`. On a native client, the owner Domain's JSON encoder later
rejected invalid UTF-8, and that rejection reached the caller as a retryable
`Bridge` protocol error. A deterministic caller mistake therefore looked
transient, and the same call succeeded on `mock://`. A shared `validate_text`
check now rejects oversized, NUL-bearing, or malformed UTF-8 free text at the
public boundary with a non-retryable defect, so both backends agree. Empty
reasons and queries remain valid. A unit test covers all four inputs on the
mock backend.
## 2026-10-13: Metrics endpoint survives accept errors (#1030)

The Prometheus endpoint no longer stops permanently and silently on the
first `accept()` error. Core's scrape server returns from its accept loop on
any error and Core exposes no way to serve its registry elsewhere, so the
bridge now binds the public listener itself and relays each connection to
Core's exporter, which listens only on an ephemeral loopback port.
Connection-scoped errors are skipped. Other errors, such as `EMFILE`, are
logged as `warn` records with the error kind and OS error number, and
retried with a backoff that doubles from 10 ms to 1 s. If Core's exporter
stops anyway (only possible through a resource error on its private
listener), scrapes receive `503 Service Unavailable` and one `error` record
is logged instead of the port closing silently. A relayed scrape needs a
third descriptor for Core's `accept()` and, on Linux, a fourth for the
follow-up `accept()` that Tokio makes until `WouldBlock`. Linux allocates the
new descriptor before it checks the queue. Relays therefore keep a reserve of
three spare descriptors. They release it just before the private `connect()` and
refill it after Core first responds. When the reserve cannot be refilled,
scrapes get a transient 503 instead of starving Core's accept loop. Bind
failures, the reported
address and release-time socket closure are unchanged. The bridge declares
`tracing` (MIT, already locked) for these records, enables Tokio's `net` and
`io-util` features (already compiled for Core), and its tests use `libc`.
The design is recorded in the
[Core bridge reference](reference/core-bridge.md), and
`Runtime.create` and the [observability guide](reference/observability.md)
document the behavior.

`rust/core-bridge/tests/metrics_accept_retry.rs` lowers the descriptor limit,
fills every descriptor and connects a scraper, so `accept()` fails with
`EMFILE`. After descriptors are released, new scrapes are served. On Linux,
where the kernel keeps the starved connection queued, that connection is
served too. `rust/core-bridge/tests/metrics_exporter.rs` stops Core's server
through a test hook and requires a 503 with the explanation on a port that
stays open, and still requires both sockets to close on release.
`rust/core-bridge/tests/metrics_core_accept_headroom.rs` keeps the process at
its descriptor limit, frees two or three descriptors per scrape, and requires
every answer to be a 200 or the transient 503 and Core's server never to
stop. Without the reserve, the first starved scrape kills Core's server.
Verified on macOS with OCaml 5.4.1:

- `cargo test --test metrics_exporter --test metrics_accept_retry --test
  metrics_core_accept_headroom`, run five times;
- `cargo clippy --all-targets -D warnings` and `cargo fmt --check`;
- `scripts/check-dependency-inventory.sh`;
- `dune build @install` and `dune build @test/bridge/runtest`.

## 2026-10-13: Server-valid inbound values no longer wedge workflows (#1015, #1016, #1017, #1049, #1050)

Several values that Temporal Server or another SDK legitimately sends were
rejected by the inbound Core-to-semantic conversion. History is immutable,
so each rejection failed every workflow task of the run (and every replay),
failed an activity task, or made `Client.wait` fail permanently. The Rust
conversion and the OCaml decoders now represent these values faithfully or
degrade them deterministically, while outbound validation stays strict:

- A negative application `next_retry_delay`, which the Go SDK sends when
  `time.Until(retryAt)` has passed, is carried as absent (#1049).
- Fields an SDK copies into an activity or child-workflow failure wrapper
  (including the Java SDK's empty child namespace) are bounded text instead
  of identifiers, and an absent wrapper type or execution reads as empty;
  `external_payloads` size details and empty payload metadata keys are
  dropped while the payload is kept (#1050).
- Unknown numbers in the advisory enums (continue-as-new suggestion reason,
  retry state, timeout type, application category, Nexus retry behavior,
  continuation initiator) read as `unspecified` (#1017).
- Start-event priority and activity-task priority keep their exact weight
  bits and only the 64-byte fairness-key bound applies; a start-event retry
  policy recorded with `maximum_attempts = 1` (which the server does not
  validate) is carried as recorded, and `Workflow.Info.retry_policy` returns
  `None` for a policy the public type cannot represent instead of raising
  (#1016).
- A Temporal Schedule action carrying the previous result or failure with no
  previous run ID is represented with an empty continuation run ID, and
  `Workflow.Info.continued_from_run_id` reports `None` (#1015).

The JSON schemas and `docs/reference/core-protocol.md` document the rules.
`rust/core-bridge/tests/workflow_protocol.rs` covers negative delays,
unknown enum numbers, peer-written payload details, SDK-written child
wrappers, schedule-started runs, and inbound priority (weights 5000, 0.0005,
NaN, a NUL fairness key) and retry policy (coefficient 0.5, empty error
type, absent and negative intervals), each round-tripping through the
semantic JSON while the same priorities remain rejected outbound.
`test/bridge/test_ocaml_activity_protocol.ml`,
`test/bridge/test_ocaml_workflow_protocol.ml` and
`test/runtime/test_execution_info.ml` cover the OCaml decoders and public
accessors. Verified with `dune build @install`, the `test/bridge`,
`test/runtime` and `test/unit` aliases on OCaml 5.4.1 (macOS), and the
focused `cargo test`, `cargo fmt --check` and `cargo clippy -D warnings`
runs.
## 2026-10-13: Continue-as-new encoder errors fail only the task (#1034)

`Codec.make` deliberately keeps the identity of an error that a custom encoder
returns, and the runtime turns a `Fail_workflow` into a workflow-task failure
only for `Bridge`, `Codec` and `Defect` errors. A continue-as-new successor
encoder that returned, for example, a `Workflow` error therefore emitted a real
`FailWorkflowExecution`. The run closed as failed, or started a retry run,
which contradicted the documented contract. `Workflow.continue_as_new` now
rewraps any other category as a `Codec` error. The new error names the
original category and message and keeps its details and failure type, so the
task fails with no commands and the run stays open, just as with the workflow
result encoder. A new runtime test drives a failing encoder with every
category and checks that only the task fails.
## 2026-10-13: Empty search-attribute upserts emit no command (#1033)

`Workflow.upsert_search_attributes []` used to buffer an
`UpsertWorkflowSearchAttributes` command with an empty map. Temporal Server
rejects that command with `BAD_SEARCH_ATTRIBUTES`, and replay emits it again,
so every retry of the workflow task failed and the run was stuck. The
workflow context store now treats an empty list as a no-op, as the Python SDK
does, after the existing sealed-execution check. This is replay-safe because a
rejected command is never recorded in history. The `Workflow` interface and
the workflows guide now state that values must be `json/plain` encodings of
the attribute's type, that a `binary/null` payload removes a key, and that
`Codec.bytes` or custom codecs are rejected by the server. A runtime test
checks that empty upserts around a non-empty one emit exactly one command.
## 2026-10-13: Activity retryability for unrunnable tasks, pause/reset completion, and async lease expiry (#1031, #1022, #1043)

An activity task for a type the worker does not register, or with an input
it cannot decode, was failed non-retryably, so during a rolling deploy or on
a queue shared by workers with different registrations the first attempt
failed the activity permanently (#1031). For a remote activity the adapter
now reports a retryable application failure: type `NotFoundError` with a
message listing the registered types for an unknown type (matching the Go,
Python, TypeScript and .NET SDKs), and the existing type for an undecodable
input. A local activity, which always runs on the scheduling worker, still
fails non-retryably, as does an unrepresentable heartbeat context.

A synchronous activity that acknowledged an operator pause or reset was
completed as cancelled, which the server rejects without a cancellation
request, leaving the attempt open until it timed out (#1022). When Core's
primary cancellation reason is `Paused` or `Reset` (Core reports those only
when no cancellation was requested), the attempt, a start interrupted before
admission, or a bare cancel task is now failed with a retryable
`ActivityPause` or `ActivityReset` application failure, as in the Python
SDK.

An async lease that application code never finished stayed admitted for the
worker's life, so every later shutdown waited the full grace period and
reported a lost completion (#1043). Each admitted lease now records its
attempt deadline (admission plus the shorter close timeout) and its heartbeat
deadline (heartbeat timeout after admission or the last async heartbeat),
which are never earlier than the server's; five seconds after either passes
the lease is closed without a native call and logged as
`activity_async_lease_expired`. Expiry is checked on admission, on every
drain, and by the shutdown probe, so a stale lease no longer holds shutdown.
The workflow guide's async example now retries the retryable "not active
yet" error on a system thread instead of a one-shot `Domain`, and the guide
and `Activity.Async_handle` document that every handle must be finished.

`test/runtime/test_native_activity_execution.ml` covers unknown types and
undecodable inputs for both localities and pause/reset for a running
attempt, a bare task and a start interrupted before admission;
`test/runtime/test_native_async_activity.ml` covers lease expiry by each
close timeout, the slack, heartbeat renewal, the no-timeout case, and expiry
on admission with an injected clock. Verified with `dune build @install` and
the `test/runtime` test alias on OCaml 5.4.1 (macOS). A live two-worker or
`temporal activity pause` test is not included.
## 2026-10-13: Binary payload metadata is carried losslessly (#1048)

Temporal payload metadata values are bytes, and the protocol layers already
carried them losslessly as base64, but the three runtime copies of
`runtime_payload` (workflow activations, workflow start input, and activity
input and heartbeat details) rejected any value that was not valid UTF-8 and
any key containing NUL. A Go or Java codec that stores a raw nonce or key ID
in metadata, or an OCaml `Client.start` memo with binary metadata, therefore
failed every workflow task of the run (history is immutable) or failed the
activity non-retryably, even when the codec never looked at that entry. The
runtime now copies inbound keys and values unchanged; an OCaml `string` is a
byte string. Outbound `protocol_payload` keeps the strict key rule but
accepts any value bytes, so a received payload can be returned or forwarded
unchanged.

`test/runtime/test_native_execution.ml` translates an activity result, a
failure detail, and a signal input and header carrying a `"\xff\x00"`
metadata value, and sends a binary value outbound while still rejecting a NUL
key. `test/runtime/test_native_activity_execution.ml` runs an activity whose
input carries binary metadata, and the unrepresentable-task and async-handle
regressions now use a second input value and a NUL key, the remaining
rejected shapes. Verified with `dune build @install` and the `test/runtime`,
`test/unit` and `test/bridge` test aliases on OCaml 5.4.1 (macOS).
## 2026-10-12: SIGPIPE containment (#1041)

A peer-closed connection can no longer kill a Linux worker or client with
SIGPIPE. Vectored socket writes (`writev`, used by h2, hyper and
tokio-rustls) raise the signal, and an OCaml host keeps its default,
terminating action because Rust's startup code that ignores it never runs in
a static library. The bridge does not change the process-wide disposition;
it blocks SIGPIPE in the signal mask of every thread it owns (Core's Tokio
workers and blocking pool through Core's thread-start hook, the cleanup
threads and the Core log writer) and, for the duration of each ABI call, on
the application's calling thread, consuming any SIGPIPE raised by the call
and restoring the caller's exact mask afterwards. The write then fails with
`EPIPE` through the normal error path. The bridge now declares `libc` (MIT OR
Apache-2.0, already locked) as a direct dependency, used only on Unix. The design is
recorded under SIGPIPE containment in the
[Core bridge reference](reference/core-bridge.md).

`rust/core-bridge/tests/sigpipe_containment.rs` restores the default
disposition and checks that Core's Tokio threads block the signal, that ABI
calls restore both unblocked and caller-blocked masks, and that a SIGPIPE
raised during a guarded call is consumed; on Linux it also drives `writev`
into reset connections on a blocked thread and inside a guarded call, and
resets 64 metrics scrapes mid-request before a final scrape succeeds.
`test/bridge/test_runtime_metrics.ml` repeats the aborted-scrape check from an
OCaml host with the default disposition. Verified with `cargo test --test
sigpipe_containment` (the Linux-only cases were also compiled and run on macOS
by temporarily widening their `cfg`), `cargo clippy --all-targets -D
warnings`, `cargo fmt --check`, `dune build @install` and
`dune build @test/bridge/runtest` on OCaml 5.4.1 (macOS). The in-process
transport-fault proxy still ignores SIGPIPE for its own sockets; a Linux
fault proxy in a separate process remains future work.
## 2026-10-13: Inbound failure types never raise (#1011)

An application failure type written by another SDK, worker or the server
history could contain a control character, invalid UTF-8, or more than the
65,536 encoded characters that `Error.make` allows; the Rust bridge's own
truncation marker even begins with a newline. The inbound type went through
`Error.make`, so `Client.wait`, `start_update` and `wait_update` raised
`Invalid_argument` instead of returning a typed result, and a workflow
awaiting such an activity or child failed every workflow task, including on
replay. `Failure_diagnostic.application_failure_type` now passes the type
through a deterministic sanitizer: malformed UTF-8 becomes U+FFFD, control
characters become visible `\uXXXX` escapes, and over-long text is cut at a
character boundary with a `...` marker. Valid types are unchanged and
outbound `Error.make` validation stays strict.

`test/bridge/test_failure_diagnostic.ml` covers control characters, NUL,
invalid UTF-8, a type exactly at the limit, an over-long type ending in the
bridge marker, multi-byte, quote and control-character cuts, and checks that
every result is accepted by `Error.make`;
`test/bridge/test_client_terminal_adapter.ml` drives a tab and newline type
through the client terminal conversion. Verified with `dune build @install`
and the focused bridge tests on OCaml 5.4.1 (macOS).

## 2026-10-13: Unset activity timeouts read as None (#1032)

Temporal reports an unset activity timeout as a zero duration. Core fills a
local activity's absent schedule-to-close with zero, the server reports an
unset heartbeat timeout as zero, and an unset remote schedule-to-close
inherits a zero workflow run timeout. The native activity adapter exposed
these as `Some 0ms` through `Activity.Info` and `Context.heartbeat_timeout`.
Deadline arithmetic therefore treated the attempt as already expired, and
heartbeat pacing at half the interval spun. The adapter now maps a zero
timeout to `None`, matching the Go and Python SDKs and `Temporal.Testing`.
`Context.heartbeat_timeout` also uses the same total conversion as `Info`, so
a sub-millisecond heartbeat interval scheduled by another SDK rounds up to the
next millisecond instead of failing a synchronous task non-retryably.

`test/runtime/test_native_activity_execution.ml` covers a local activity
without schedule-to-close, a remote activity with zero heartbeat and
schedule-to-close timeouts, and a sub-millisecond heartbeat interval through
both views. Verified with `dune build @install` and the `test/runtime` test
alias on OCaml 5.4.1 (macOS).

## 2026-10-13: Metrics scrape test no longer waits for connection close (#1058)

`test/bridge/test_runtime_metrics.ml` timed out intermittently on Windows
while reading a scrape until EOF with a 5 s receive timeout. Its `http_get`
helper now treats a response as complete once the headers and the declared
`Content-Length` body have arrived, falling back to EOF only without a
length, so the test no longer depends on when the exporter closes the
connection. The bound is 30 s, and a timeout now reports how many bytes
arrived, which separates a slow close from a missing response. The helper
was checked standalone against complete, partial and length-less responses
on OCaml 5.4.1 (macOS); the Windows CI job is the regression gate.

## 2026-10-13: Compile-checked API landing page (#1013)

The quick start on the rendered API landing page (`lib/public/index.mld`)
compiles again. Its client example now matches `Client.Completed` as the
inline record `Completed { output = message; _ }` introduced by #837/#957. The
shutdown comment no longer claims the client has no finalizer: it now says a
GC finalizer is only a last-resort safeguard. The page's module index gains
`Temporal.Runtime`, `Temporal.Replay` and `Temporal.Testing`, which the root
module exports.

odoc does not type-check code blocks, so `test/doc_snippets/` now guards the
page in every `dune runtest`. A standard-library-only extractor writes the
page's `{[ ... ]}` blocks, with line directives pointing back at the page,
into a library that is compiled against the in-tree `temporal-sdk` without
linking the Rust bridge. The same program fails if the `{!modules: ...}` index
differs from the `module X = ...` aliases in `lib/public/temporal.ml`. Run
against the unfixed page, the check reported the original error at
`index.mld` line 91. Verified on OCaml 5.4.1 (macOS) with `dune build @install`
and `dune build @test/doc_snippets/runtest @test/api_witness/runtest`.
## 2026-10-13: Option None inputs are sent as one argument (#1012)

The zero-argument convention from #819 is now decided by the input codec,
not by the encoded payload's bytes. Previously any input payload equal to
`{encoding=binary/null; data=""}` was sent as zero payloads, so
`Codec.option`'s `None` (byte-for-byte identical to `Codec.unit`) reached a
foreign handler with an optional parameter as no argument at all. Python then
raised a missing-argument `TypeError`, and the activity or workflow task
retried forever. Only `Codec.unit` is now sent as zero payloads. Every other
codec, including an option's `None`, sends exactly one payload, which matches
the Go, Java, and Python SDKs' single `binary/null` payload for a nil
argument.

The public codec carries a private `no_argument` marker that only
`Codec.unit` sets, exposed as `Codec.is_unit`.
`Codec_private.encode_arguments` applies the rule at every outbound site:
client start, signal, and update; activity, local-activity, and child-workflow
scheduling; continue-as-new; and external signals. The private runtime
commands and backend requests now carry the argument list, so no inner layer
inspects payload shapes. `Temporal_base.Payload.input_arguments` and
`is_unit_null` were removed. Decoding is unchanged and backward compatible:
OCaml receivers still map zero payloads to the unit payload, so an option
input from an older OCaml sender still decodes as `None`. Core does not
compare arguments on replay, so existing histories replay.

`test/runtime/test_input_arguments.ml` drives the public API for unit,
`None`, and `Some` across activities, local activities, child workflows,
continue-as-new, and external signals, and checks `Codec.is_unit`.
`test_native_execution` and `test_activation` cover exact forwarding of a
`binary/null` argument. A live cross-SDK fixture with a Python or Go handler
is not included, because this environment cannot run the Docker Compose
stack. Verified with `dune build @install` and the `test/runtime`,
`test/unit`, `test/bridge`, `test/sdk_supervisor`, and `test/api_witness`
test aliases on OCaml 5.4.1 (macOS).
## 2026-10-13: One log record per failed activity attempt (#1024)

An activity attempt that fails or is rejected now produces exactly one log
record. Previously every ordinary application `Error` was logged twice as an
`activity_task_rejected` warning: once by the activity adapter and again,
without correlation, by the public worker loop. The worker loop no longer logs
retired activity rejections; the adapter's correlated record is the only one.
The adapter also distinguishes the two causes: an `Error` returned by the
implementation, which Temporal routinely retries, is logged at `Info` as the
new `activity_task_failed` operation, while `activity_task_rejected` stays at
`Warning` for SDK-side refusals (unregistered type, codec or protocol errors,
or a raised callback). Both still return `Rejected` to the caller and submit
the same failure completion. `docs/reference/observability.md` documents the
new operation and drops the stale claim that an activity completion is encoded
twice (fixed by #995).

`test/runtime/test_native_activity_execution.ml` asserts that an application
failure yields exactly one correlated `activity_task_failed` record at `Info`
and no warning. Verified with `dune build @install` and the
`test/runtime` test alias on OCaml 5.4.1 (macOS).

## 2026-10-12: One encode per activity heartbeat (#1001)

Following #995, activity heartbeats no longer pass through the encoder twice.
The new private `Temporal_protocol.Encoded_activity_heartbeat` is the
heartbeat counterpart of `Encoded_activity_completion`: an abstract value only
its `encode` can build. `Native_activity_execution` encodes each synchronous
and asynchronous heartbeat once, which still rejects an invalid heartbeat
before any supervisor call, and `Record_activity_heartbeat` /
`Record_async_activity_heartbeat` carry that value so the supervisor copies
its bytes into C unchanged. `Protocol_adapter.encode_activity_heartbeat` is
gone. An async completion whose submission had an uncertain outcome keeps its
encoded bytes on its lease, keyed on the handle's own request key
(`Async_activity.operation_key`, now exported), so every retry the handle
allows resends that same string instead of re-preparing and re-encoding it. The ABI, Rust code and
JSON documents are unchanged.

The runtime fakes decode submitted heartbeats strictly and check they are
canonical, and never encode a submission. `test_async_completion_retry` now
requires the retried async completion to be physically the string submitted
first (`==`), and `test_async_retry_reuses_bytes_for_equal_request_key` does
the same for a `Fail` retried with an explicit `error_type` equal to its
category's default. New tests cover a single canonical async heartbeat document that
a later caller-buffer mutation cannot change, encoder-rejected synchronous and
asynchronous heartbeats that never reach the supervisor, and the supervisor's
heartbeat bytes and missing-worker rejection.
## 2026-10-12: Activity log correlation (#500)

Activity lease records now name the attempt they describe. The native activity
adapter copies each start task's workflow ID, run ID, workflow type, activity
ID, activity type, and attempt into the lease. Its `activity_task_completed`,
`activity_task_rejected`, `activity_async_handoff_accepted`, and
`activity_cancellation_delivered` records carry those values as
`temporal.workflow_id`, `temporal.run_id`, `temporal.workflow_type`,
`temporal.activity_id`, `temporal.activity_type`, and `temporal.attempt`. A
new `Debug` record, `activity_task_started`, is emitted before dispatch, so
application logs from the callback fall between two correlated SDK records.
`Temporal_base.Observability` gains the three activity tags with the shared
bounding rules (strings capped at 256 bytes; a negative attempt becomes zero).
The task token is never logged, and the identifiers stay out of metric labels.
A bare cancellation whose start the worker never saw carries no identity.

`test/runtime/test_native_activity_execution.ml` covers the started,
completed, rejected, and cancellation-delivered records, their ordering
around the callback, the absence of the token, and the bare-cancellation
case. `test/observability/test_tag_normalization.ml` covers the new tags.
Verified with `dune build`, and `dune test` for `test/runtime`,
`test/observability`, and `test/unit`, on OCaml 5.4.1 (macOS).

Still open for #500: OCaml-only executor and admission metrics, the sample
deployment that induces poll failure, saturation, and a shutdown timeout,
bounded exporter connections, and OTLP export.

## 2026-10-12: Decoder fuzzing, concurrent lifecycle stress, and scheduled qualification (#506)

`rust/core-bridge/tests/decoder_fuzz.rs` fuzzes the 20 Rust decoder targets
that the structured parser fuzz did not cover: activity tasks, completions,
and heartbeats; asynchronous activity calls; the ten client requests; the
asynchronous-start ticket read by poll and wait; client and worker
configuration; and replay history documents, including their inner protobuf
bytes. `test/bridge/test_ocaml_decoder_fuzz.ml` fuzzes 29 OCaml targets:
control envelopes, payloads, and payload objects; workflow activations and
completions; activity documents; every client response; all eight client
error decoders; and the submitted-call handle parser.
Both share the corpus under `test/bridge/fixtures/decoder-fuzz` and a set of
15 structural operators. Every input must produce no panic or escaped
exception, a typed rejection status with a bounded UTF-8 diagnostic, and an
allocation below a per-target bound measured by a counting allocator (Rust)
or `Gc.allocated_bytes` (OCaml). Accepted values must round-trip.

`rust/core-bridge/tests/concurrent_lifecycle_stress.rs` runs replay graphs on
several threads at once, attached to shared runtimes that the coordinator
releases mid-round. It requires every graph to be cleaned once, every Core
runtime to be dropped once, and no panic in bridge code on any thread.
`test/bridge/test_ocaml_concurrent_lifecycle_stress.ml` closes runtimes from
another Domain while their owners are inside native calls, closes a shared
runtime during concurrent attaches, and finalizes dropped runtimes while
other Domains collect and compact.

All four run in every PR with small budgets. The Rust tests are part of
`cargo test`. The two OCaml tests run under the `@test/bridge/bridge-stress`
alias, which `make test` and `make native-test` build after `dune runtest`.
Running the multi-Domain stress inside `dune runtest` starved timing-bound
runtime tests on the Windows x64 runner.
`make test-decoder-fuzz`, `make test-concurrent-stress`, and their `native-`
forms raise the budget. `make stress-qualification` runs every fuzzer and
stress with long budgets, and the new `.github/workflows/stress.yml` runs it
weekly and on dispatch, keeping reproductions as 30-day artifacts. Rust
producer CI now also retains `_build/decoder-fuzz` and
`_build/concurrent-stress` on failure.

Evidence on an Apple M-series laptop: 20,000 cases per Rust target with two
seeds (720,000 inputs), 20,000 cases per OCaml target (480,000 inputs),
1,200 Rust concurrent rounds of 16 threads with 4 graphs, and 300 OCaml rounds
of 8 Domains found no Rust bridge defect. The OCaml fuzz found that
`Native_bridge.parse_client_call` accepted signed, `_`-separated, and
`0x`/`0o`/`0b`/`0u` spellings that its contract excludes, so several
spellings aliased one handle; it now requires plain decimal digits, with
regression seeds and assertions. The concurrent stress now bounds setup and
the start gate by the round deadline, and a separate test injects a setup
hang and panic to prove they are reported promptly. The concurrent stress recorded an
upstream Temporal Core panic during runtime teardown (`poll_buffer.rs:428`,
unwrapping a cancelled join handle). Tokio contains it, and it has no
ABI-visible effect. The [lifecycle stress reference](reference/bridge-lifecycle-stress.md#findings)
tracks it for re-evaluation after the Core upgrade. A sanitizer-instrumented
Rust build and live-worker concurrency remain open under #506. See
[decoder fuzzing](reference/bridge-decoder-fuzz.md).
## 2026-10-12: Prometheus worker metrics and workflow log correlation (#500)

`Temporal.Runtime.create ?metrics` now exports Temporal Core's built-in
worker and client metrics. `Runtime.Metrics.prometheus ~bind_address ()`
validates an IP socket address and up to 16 bounded global tags (names that
collide with Core's labels are refused), and the endpoint is bound before
`create` returns; `Runtime.metrics_address` reports the bound address. Every
client and worker attached to the runtime reports there, under the
`temporal_`-prefixed names and bounded labels the official SDKs use, so
their dashboards apply unchanged. The design follows the official SDKs:
Core's own Prometheus exporter receives the meter at runtime construction, so
no Rust thread ever calls OCaml and no OCaml code runs on the scrape path.

The ABI stays at version 4. One additive export,
`shared_runtime_new_with_telemetry_json`, takes a strict telemetry document;
`shared_runtime_new` delegates to it with `{}`. The exporter guard is the
first field of `SharedCore`, so the scrape socket closes when the last Core
reference is released. Enabling `temporalio-common`'s `prometheus` feature
adds one locked package, `prometheus` 0.14.0 (Apache-2.0); the lock grows to
307 packages.

Log correlation: `workflow_activation_completed` and
`workflow_activation_rejected` now carry the workflow and run IDs, and a new
`temporal.replaying` tag marks those records and the workflow-source
`workflow_started`, `workflow_completed`, `workflow_failed` and
`workflow_cancelled` records, so replayed work is not mistaken for new
business progress. The [observability reference](reference/observability.md#worker-metrics)
documents the metric set, cardinality, failure isolation, an alerting
example, and the decision to defer tracing.

Evidence: `cargo test --locked` (including the new `metrics_exporter.rs`),
the new `test/unit/test_metrics_config.ml` and
`test/bridge/test_runtime_metrics.ml`, the extended
`test/observability/test_logging.ml`, and the new live regression
`test/integration/worker_metrics/regression.exe`, which passed against a
local Compose server: it scraped 20 metric families, found no label outside
the bounded set and neither execution ID in the scrape, and saw every
`workflow_activation_completed` record for the run carry its IDs. Still
open under #500: activity-scoped log correlation (activity ID, attempt),
OCaml-only executor/admission metrics, OTLP export, and the sample
deployment that induces poll failure, saturation and a shutdown timeout.
## 2026-10-12: Update callers when the run closes (#520)

A new live regression, `test/integration/update_run_close`, completes the
caller half of #520 that the offline race matrix (#963) could not observe:
an update caller must not hang when the run closes before the update
finishes. Against Temporal Server 1.32 it closes runs with an accepted,
suspended update by body completion, cancellation and termination. In each
case a `wait_update` already in flight, a later `wait_update` on the same
handle, and a `start_update` re-attaching by update ID all return the
server's non-retryable `Update` failure of type
`AcceptedUpdateCompletedWorkflow`. History shows the acceptance and no
completion. The run is closed only after the SDK's `temporal.sdk.bridge`
debug records show the in-flight caller's poll submitted and still
unanswered after a full one-second await slice, so that caller is provably
blocked on an outstanding poll RPC, although the server's receipt of the
poll is not observable. A fourth case terminates a run while an update is admitted but
not accepted. The driver makes admission observable, without timing, by
stopping the only worker first and waiting for the speculative workflow task
in `describe`. That caller, and a later one with the same update ID, receive
the non-retryable `Not_found` RPC error: Temporal aborts the request, Core
retries `aborted`, and the retry finds the run closed. Every caller returned
within 10 seconds (well under one second in practice). The SDK neither hung
nor misclassified a result, so no runtime change was needed. The orderings of
#962 remain open and are not produced live.

The suite is in `LIVE_REGRESSION_EXECUTABLES`, the CI smoke artifact list and
the `run-live-regressions.sh` dispatch, and has its own
`make test-update-run-close-live` target. `docs/reference/interactive-workflows.md`
records the outcome table.

Evidence: `opam exec --switch=ocaml-5.4.1 -- dune build --root . @install`
and `@doc`; `dune test` for `test/runtime` and `test/unit`; the suite run
three times against a local Compose Temporal/PostgreSQL stack with the pinned
admin-tools CLI; `sh scripts/check-format.sh`; `uvx typos@1.48.0`;
`scripts/check-dependency-inventory.sh`; and
`sh test/smoke/test_live_regressions_contract.sh .`.
## 2026-10-12: Benchmark record for adaptive idle pacing (#994)

`docs/reference/benchmark-harness.md` now records the live fan-out results
before and after #994's adaptive idle pacing (PR #996): one-execution p50
10.89 s to 5.94 s, eight-execution p50 107.9 s to 79.1 s, with the Windows
timer-granularity limitation tracked in #1008. Documentation only.

## 2026-10-11: Live fan-out benchmark and JSON bridge attribution (#528)

`make bench-fanout-live` runs a high fan-out workload against the Compose
Temporal stack: each execution starts N remote activities and fans them back
in with `Future.all`, with one or more executions per sample. The report has
the shared harness's latency, throughput, errors, OCaml memory and process RSS
with the Linux peak. An untimed attribution pass enables `Debug` on the
bridge and workflow log sources and aggregates each native bridge call's
duration, each workflow activation's processing time and new opt-in codec
records. `Observability.debug_timed` times the five OCaml protocol codec
passes only while the bridge source is at `Debug` and otherwise only checks
the level. `make bench-json-bridge-rust` times the Rust serde passes on
documents of the same shape.

At width 1,000 with 256-byte payloads on an M4 Pro Docker Desktop VM, a
one-execution fan-out took about 9 s and eight executions about 90 to 98 s,
with no errors and flat retained OCaml memory. JSON (OCaml about 77 ms, Rust
about 19 ms per fan-out) is about 1% of the live latency. The single remote
activity slot serializes activities, native completion calls (Core and the
server RPC) take about a quarter of the time, and an unmerged experiment
showed the 10 ms local lane yield in `Native_worker.wait_for_lane` to be the
dominant remaining cost. Activity completions are encoded twice (validation
and supervisor). The results and their limits are in
[benchmark-harness.md](reference/benchmark-harness.md#live-fan-out-and-bridge-attribution).
Verification: `dune build @install`, the benchmark and observability tests,
`@doc`, both live levels and the Rust benchmark, the format, spelling and
dependency-inventory checks.
## 2026-10-12: Idle-pacing tests measure decisions, not sleep granularity (#994 follow-up)

`test/runtime/test_native_worker_idle.ml` failed on the Windows x64 job of
unrelated PRs ("serial step requested 6.42 ms of yields on average", "timed
out waiting for both lanes to request a capped yield"). Switching the test to
the bridge's monotonic clock did not fix it. The cause is OCaml's Windows
`Thread.delay`, which passes whole milliseconds to `Sleep`: a sub-millisecond
backoff step returns at once and a longer one lasts until a 15.625 ms tick.
The lanes therefore take more backoff steps per round trip, so the total of
requested delays per step rose to about 6.5 ms. The pacing decisions were
unchanged, and no capped yield was requested during the chain. Idle lanes also
seldom reach the 10 ms cap there, because each new native wait restarts them.

The test's sleeps now go through one function that can simulate the Windows
sleep (`IDLE_TEST_SLEEP_MODEL=windows`). That reproduced both failures on
macOS: 6.2 to 6.9 ms of requested yields per step. The assertions now
measure the algorithm's decisions:
- the serial chain allows at most one capped yield per two steps. Adaptive
  pacing requests none on either sleep model; the fixed 10 ms yield
  requests 213 in 100 steps;
- the admission-race test no longer waits for capped yields. It confirms the
  sibling is queued while the raced wait holds the owner, which removes a
  transient-queue race, and only the raced wait has the long timeout. The
  pure unit test still checks the capped-lane restart deterministically.

`test/runtime/dune` also reruns the test under the Windows model, so this
interleaving is covered on every platform. The monotonic-clock change is
kept. Production code is unchanged.

With the Windows model, a simulated serial step takes about 21 ms with
adaptive pacing and 23 ms with the fixed yield. On Windows the #994 gain is
therefore limited by timer granularity.

## 2026-10-11: Temporal Core upgraded to v1.1.0, with a freshness check (#824)

The Core pin moves from `95e97686` (crate 0.5.0, 154 commits behind) to
`8fe478ee179ed754614d230f39586f3edb57da71`, the commit of upstream tags
`v1.1.0` and `core-v0.10.0`. The newest stable release was chosen over
`v1.0.0` because it also carries the sticky/normal poller balancing,
first-attempt-only workflow-task failure reporting and local-activity replay
grouping fixes. The lock shrinks from 322 to 306 packages: `backoff`,
`aws-lc-rs`/`aws-lc-sys`, `quinn` and the old `rand` 0.8 line leave the graph,
and `backon`, `ryu`, `serde_urlencoded`, `tracing-serde` and `wasm-streams`
join it, all permissively licensed. The Core licence file is byte-identical.

The C ABI, status values and every JSON document are unchanged (version 4).
Asynchronous activity completion and heartbeat now send their
`RespondActivityTask*`/`RecordActivityTaskHeartbeat` RPCs directly, because
Core's handle now applies the client data converter and accepts only a
high-level failure. New Core protobuf fields keep their previous meaning,
worker heartbeats keep omitting host environment details, and the two new
child-start failure causes reach OCaml as `unspecified`. Client connection is
now bounded at 10 s, because an endpoint that accepted and then stayed silent
held `Client.create` for Core's 30 s `GetSystemInfo` deadline and failed the
transport-interruption refuse scenario intermittently in CI. The
[Core bridge reference](reference/core-bridge.md#temporal-core-revision)
lists each adaptation and the Core behaviour changes kept as upstream
intends, notably worker-side payload-limit enforcement.

Code comments that describe Core behaviour were rechecked against the new
source: the fixed-poller split and `nonsticky_to_sticky_poll_ratio`, the
`PollRateLimiter` reciprocal, replay worker shutdown ordering, the one-slot
activity configuration, the nondeterminism text parsed by `Temporal.Replay`,
activation job ordering, and retry/throttle timing. Only the cache slot clamp
(#555) changed: Core's balancer now honours the cap itself, so the clamp is
kept for parity rather than as a workaround.

Nothing prompted the upgrade because Dependabot never moves a `rev =` pin.
The weekly `Temporal Core pin freshness` workflow now compares the pin with
the latest stable sdk-core release, opens or updates one tracking issue and
fails while an upgrade is due; `test/smoke/test_core_pin_freshness.sh` covers
its exit contract offline. [Dependencies](dependencies.md#temporal-core-pin-upgrades)
records the cadence.

Evidence (macOS ARM64, OCaml 5.4.1, Rust 1.97.1; CI uses the pinned
1.99.0): `cargo test --locked` (410 tests), clippy with `-D warnings` and `cargo fmt --check`; the bridge,
runtime, supervisor, unit, API witness, history corpus and smoke Dune suites;
`@doc`; the Cargo licence scan and its policy fixtures; the dependency
inventory; formatting and `typos`. `make test-history-corpus-upgrade` replayed
all 21 corpus cases, recorded at Core `95e97686`, with expected outcomes and
no history or expectation edits: the first cross-revision replay. Against
Temporal Server 1.32.0 and PostgreSQL 18.6, the activity-cancellation,
bounded-shutdown, client-request-ID, completed-query, concurrent-client,
split-task-type and worker-resource live regressions and the two-binary smoke
(including asynchronous activity completion) passed with native executables.
The CLI-dependent live regressions and Linux lanes run in CI.
## 2026-10-12: Adaptive idle pacing between worker lane polls (#994)

The live fan-out benchmark (#528) found that the fixed 10 ms local yield in
`Native_worker.wait_for_lane`, taken by an idle lane while its sibling is
busy or holds the native readiness wait, was about two thirds of a serial
1,000-activity fan-out. The new private `Native_worker_idle` module paces that
yield instead. While the sibling's `Wait_any` is in flight, the lane repolls
at once, once per wait: the poll queues behind the wait in the supervisor
mailbox and runs as soon as either lane has work. If that poll was served
just before the wait's mailbox admission, the lane sees the joined wait
still in flight and restarts its backoff at 0.1 ms for that wait rather than
resuming a capped sleep (PR review). Otherwise it backs off 0,
0.1 ms, doubling to the former 10 ms cap, restarting after every completed
task and after a readiness wake (for the sibling only, so the waiter cannot
re-enter a wait that returns at once). Each zero yield consumes one bounded
event, so an idle worker settles at the 10 ms cap and does not spin; the
100 ms native wait, the bounded shutdown deadlines (#495), the activity
completion retry backoff and the activation watchdog are unchanged. The
workflow lane's retry fallback keeps a fixed 10 ms delay. No Rust, C or
protocol change: the ABI stays at v4.

New runtime tests drive the real lane scheduler against a fake one-owner
supervisor, asserting poll order and requested yields rather than tight
wall-clock bounds so loaded CI runners cannot fail them. A task published on
either lane of an idle worker is taken by the poll the readiness wait
releases, with no extra empty poll or yield (about 0.14 ms locally). A forced
admission race, in which the sibling's joining poll is served before the
wait, requeues it after at most a 0.2 ms yield (the unfixed rule resumed a
10 ms sleep). A simulated serial activity chain with 0.5 ms round trips
requests 0.2 ms of yields per step (21 ms with the fixed 10 ms yield; bound
5 ms) and runs at about 1.07 ms per step. An idle worker made 20 polls and 10
native waits in one second, and a stop while idle returned in under 100 ms.
Unit tests cover the backoff schedule, the once-per-wait join, the
early-join restart, sibling-only readiness resets and invalid
configurations.

Live fan-out on the Compose Temporal 1.32.0/PostgreSQL 18.6 stack (M4 Pro,
Docker Desktop, OCaml 5.2.1 development container, #993's benchmark applied
locally, width 1,000, payload 256 B, one warmup): one execution, five
samples, p50 10.89 s before and 5.94 s after (90 to 162 activities/s); eight
executions, three samples, p50 107.9 s before and 79.1 s after (75 to 92
activities/s), with no errors. In the one-execution attribution pass, time
outside any native call or codec pass fell from about 5.5 s to about 0.3 s;
what remains is Core completion calls and genuine readiness waits for the
server.
## 2026-10-12: One encode per activity completion (#995)

Each activity completion was encoded twice: once in
`Native_activity_execution` to validate it, and again in the supervisor's
`Complete_activity` / `Complete_async_activity` handler to send it. The
adapter now runs the strict encoder once through the new private
`Temporal_protocol.Encoded_activity_completion`, the activity counterpart of
`Encoded_workflow_completion` (#846). Its abstract value is the only form a
completion can take to reach the supervisor, so an invalid completion is still
rejected before the bridge, and the supervisor copies its bytes into C
unchanged. A retained worker lease keeps only those immutable bytes in place of
a token-copied typed completion, so a retry through `poll`, `drain` or
`try_drain` resubmits the identical string. The async completion path encodes
once per submission. The protocol adapter's `encode_activity_completion` is
gone. The ABI and JSON documents are unchanged.

The fake supervisors in the runtime tests now decode the submitted bytes
strictly and check that they are the canonical encoding of the decoded value.
`test_completion_is_encoded_once` rejects one submission, raises a transient
exception on the next, and drains the third. It requires all three to be the
physically identical string and to equal the canonical encoding of the
accepted completion. The asynchronous heartbeat path still encodes a heartbeat
once to validate it and again in the supervisor. It is outside this change.
## 2026-10-12: Live qualification of non-yielding workflow recovery (#493)

PR #951 added the activation watchdog (detect, report, and fail the workflow
task) and PR #974 (#495) bounded shutdown with a stuck activation; both were
covered by focused tests only. The new `test/integration/non_yielding_workflow`
live regression closes the remaining acceptance criteria against Temporal
Server. Worker A (500 ms deadline) completes a control workflow, then runs a
workflow that spins without yielding. The fixture checks that:

- detection happens within the deadline plus one second of the code starting
  (about 0.64 s in five local runs), and `Worker.health` names the workflow
  type, workflow ID, run ID, a non-replaying activation, an elapsed time of at
  least the deadline, and `` `Task_failed ``;
- exactly one `workflow_activation_deadline_exceeded` record carries the same
  identifiers, and neither it nor the server's `WorkflowTaskFailed` event
  (identity `non-yielding-a`, message naming the activation deadline)
  contains the run's secret input;
- `shutdown_with_report` on A while the code still spins returns within the
  grace period plus the teardown timeout (about 1.3 s with a 1 s grace
  period) and reports one abandoned workflow activation, and A's `run` has not
  returned;
- worker B, the corrected build with a 3 s deadline, completes a second
  control workflow and a legitimately heavy 1 s activation while staying
  healthy, and Temporal's retry completes the original run with the
  corrected code. That run's history holds exactly one failed workflow task,
  task completions only from B, one completion, and no event the abandoned
  code could have produced;
- releasing the spinning code drops A's late completion exactly once and lets
  A's `run` return.

The suite is in `LIVE_REGRESSION_EXECUTABLES`, the CI smoke artifact list,
and the `run-live-regressions.sh` dispatch (it reads history through the
pinned CLI), and has a `make test-non-yielding-workflow-live` target. The
workflows guide now lists the blocking operations to avoid, how to size the
deadline for heavy activations and replay, the three diagnostic views, and how
a failed run recovers once corrected code serves its queue; its stale claim
that `shutdown` cannot finish while code is stuck now describes the bounded
report. The support policy, live coverage, and native worker references link
the regression.

Verification: the regression passed five of five runs on macOS (OCaml 5.4.1)
against the Compose Temporal/PostgreSQL stack, together with
`dune build @install`, `@doc`, the runtime, unit and smoke tests, the
live-regressions contract, the dependency inventory, `check-format.sh`, and
`typos`. There are no library, Rust, C, or ABI changes; ABI version 4 is
unchanged. Remaining limit, unchanged and documented: workflow tasks Core had
already received for a stuck worker wait for their workflow-task timeout.

## 2026-10-12: Official-SDK interoperability regression (#517)

A new live regression, `test/integration/sdk_interop`, drives an OCaml
workflow from the official Temporal Python SDK (`temporalio` 1.33.0, MIT).
The SDK runs only in the digest-pinned `python:3.14-slim-bookworm` image as
the `sdk-interop-python` Compose service, installed into a throwaway virtual
environment from a hash-locked closure of five packages
(`test/integration/sdk_interop/python/requirements.lock`, all MIT, BSD,
Apache-2.0 or PSF-2.0). `docs/dependencies.md` gains a CI-only
interoperability tooling table, and `scripts/check-dependency-inventory.sh`
and its contract now compare that table with the lock. `make license-check`
now also runs `make python-license-check`, which installs the lock with its
hashes in the pinned Python image and audits each installed distribution's
own `importlib.metadata` license fields and license files against the shared
SPDX allowlist, with two exact reviewed overrides (protobuf's free-text
license and typing-extensions' PSF-2.0) and an exact installed-equals-lock
check; `test/smoke/test_python_license_policy.sh` proves the rules on fake
distributions.

The OCaml regression process hosts the workflow worker and publishes its
queues; the Python driver hosts the activity the OCaml workflow calls, starts
the workflow with structured `json/plain` input, queries it, sends an
accepted update and three rejected ones (validator, `binary/plain` input,
wrong shape), signals a document with 2^53+1, a fraction, astral-plane text
and `null`, and checks the exact echoed result, the history payload
encodings, and the `temporal-ocaml` SDK name and version recorded in
`WorkflowTaskCompleted`. An activity `ApplicationError` returns through the
OCaml workflow as a `PricingFailed` failure with the original detail, and a
JSON string workflow input fails the workflow task while the run stays open.
The OCaml client then re-reads the same three exact runs. The suite is in
`LIVE_REGRESSION_EXECUTABLES`, and `run-live-regressions.sh` dispatches it to
both containers. It passed locally against the Compose Temporal Server 1.32.0
and PostgreSQL stack with OCaml 5.2:
`make test-temporal-live-regressions LIVE_REGRESSION_EXECUTABLES=test/integration/sdk_interop/regression.exe`.
Child workflows across SDKs, official-SDK workflows calling OCaml activities,
other official SDKs and custom codecs remain unqualified.
## 2026-10-11: Dune job limit reaches nested and long-lived Dune processes (#851)

The earlier #851 change passed `-j $(DUNE_JOBS)` to every Dune command in the
Makefile, but Dune processes started by scripts still used Dune's per-CPU
default. These are the `dune build @install` and installed-consumer builds in
`test/bridge/test_install.sh`, the prebuilt-consumer and vendored-build
checks, and the `dune exec` that starts each long-lived Compose worker. Dune
3.24 reads `DUNE_JOBS` from its environment, so the Makefile now exports the
resolved limit when it is non-empty. It unexports the variable when it is
empty, because Dune rejects an empty value. The fixture's `dev` Compose
service lists `DUNE_JOBS` without a value, so Compose copies it into every
`compose run` container and every service that extends `dev`, and leaves it
unset when the caller has none. The defaults are unchanged: 2 locally and
`auto` when `CI` is set. The README quickstart now gives
`DUNE_JOBS=1 CARGO_BUILD_JOBS=2` for a 4-6 GiB VM.

Dune reads `DUNE_JOBS` from its environment only from 3.22.0, and the SDK
supports Dune 3.18, so the export alone does not bound older releases. The
install, prebuilt-consumer and vendored-build scripts therefore start Dune
through an `opam_dune` helper, and `scripts/run-temporal-executable.sh` uses
the same branch. When `DUNE_JOBS` is non-empty, both add an explicit
`-j "$DUNE_JOBS"`.

`test/smoke/test_build_jobs_contract.sh` now runs a probe recipe to check that
recipes receive the local default, the CI default, and command-line and
environment overrides, and that an empty limit arrives unset. It also checks
that the `dev` service passes the variable through without an interpolated
default. Removing either the export or the unexport makes the contract fail.
For the scripts, the contract puts stub `opam` and `dune` commands on `PATH`.
It checks that every Dune start in the three test scripts goes through
`opam_dune`, and runs each helper and the worker launcher with and without
`DUNE_JOBS`. With a value, the recorded Dune arguments must include `-j`;
without one, they must not.
`docker compose config` showed the value reaching an extending worker
service, and `dune build --help` in the development image confirmed that
Dune 3.24.2 reads `DUNE_JOBS`.

An attempted local containerized `make build` with the default `-j 2`
reached a peak of 4.3 GiB in the dev container on a 9.7 GiB Docker Desktop VM.
The host disk then filled and the build stopped, so the full build and verify
were not completed locally.

## 2026-10-11: Configurable client admission limits (#796)

The native client's three admission limits were fixed constants in the Rust
bridge: 64 distinct waited runs, 64 in-flight start request IDs, and 4,096
submitted calls. A process serving more concurrent callers than that through
one client had to create several clients, each with its own runtime.
`Client.create` now takes `?max_pending_waits`, `?max_pending_starts`, and
`?max_in_flight_calls`. Each must be between 1 and 4,096, 4,096, and 65,536
respectively; any other value is a typed defect returned before anything is
allocated, for every target including `mock://`. Omitted values keep the
previous defaults.

The values travel as optional additive fields of the existing client connect
document, after `identity`, and the OCaml encoder omits every limit left at
its default, so a default client sends the pre-#796 document byte for byte
and ABI version 4 is unchanged. Rust decodes the fields strictly, validates
the same ranges again in `client_calls::ClientLimits::from_config` before any
network access, and stores them on the runtime when the connection succeeds.
The submitted-call path, the start-ticket path, and the retained wait path
read the per-connection limits instead of the constants, and the
`RESOURCE_EXHAUSTED` message names the configured value. Capacity errors
still surface as the retryable error `Client.is_at_capacity` recognizes.

Rust callback-transport tests enforce configured limits of two waits, two
starts on both start paths, and three calls in total, keep the defaults for
documents without limits, and reject zero, over-maximum, mistyped, and
unknown fields. An OCaml bridge test checks the exact encoded documents,
sender-side rejection, Rust's acceptance of a limited document, and typed
defects from `Client.create`; the capacity classification test covers the
configured-limit messages. The concurrent client live regression now also
refuses a third wait on a client created with `~max_pending_waits:2`.
## 2026-10-11: Workflow start policy in `Workflow.Info` (#512)

`Client.start` sets execution, run, and task timeouts and a workflow retry
policy (#499), and the bridge already carried each of them, with
continuation provenance, in the private `InitializeWorkflow` context, but
workflow code could read none of them. `Workflow.Info` now exposes
`execution_timeout`, `run_timeout`, and `task_timeout` as `Duration.t option`
(rounded up to whole milliseconds and clamped like the activity timeouts),
`retry_policy` as an `Activity.Retry_policy.t option` rebuilt from the
bridge-validated record with the exact coefficient bits, and
`continued_from_run_id` with `continuation_initiator`
(`` `Continue_as_new | `Retry | `Cron_schedule ``; the unspecified Core value
is `None`). The values are copied once into the run's immutable
`run_info` from the initialization job, so replay observes the same values.
`Temporal.Testing` reports the predecessor of its continue-as-new runs. No
Rust, C, or protocol change: the ABI stays at v4.

Runtime tests feed `InitializeWorkflow` jobs carrying every field, live and
replaying, for every continuation initiator, and check rounding, clamping,
and absent values. The new [start info live regression](../test/integration/workflow_start_info/README.md),
added to `LIVE_REGRESSION_EXECUTABLES`, passed against the Compose Temporal
1.32.0/PostgreSQL 18.6 stack. A root run, a retry run, and a continue-as-new
successor each reported exactly what the client sent, before and after worker
replacement forced a replay. Temporal carries the run and task timeouts,
retry policy, and memo over to a continue-as-new successor when the command
overrides none of them.

Still open under #512: cron and nonzero root start delay remain rejected,
the predecessor's failure and completion payloads are not public, and
cache-full eviction (#501/#555), the replay corpus (#503), and conformance
(#505) qualification are tracked separately.
## 2026-10-11: Patch lifecycle replay matrix (#531)

The history corpus now records the patch lifecycle beyond the single
`corpus.patch` workflow. Four frozen generations of each `corpus.lifecycle.*`
workflow type (legacy, patched, deprecated, removed) differ only in one gate
function. `corpus.lifecycle.multi` has three independent patches around an
activity, a timer and a child workflow; `corpus.lifecycle.loop` consults one
patch in each of three iterations; `corpus.lifecycle.handlers` puts patches in
a signal handler and an update handler that each run an activity; and
`corpus.lifecycle.handover` was started by a legacy worker and finished by a
fresh patched worker that reached the patch point for the first time.

The capture `live-2026-10-11` (Server 1.32.0, Core `95e97686`, OCaml 5.4.1,
clean tree at the recorded commit) adds 13 histories. They are replayed
against every other lifecycle generation as 39 derived cases, bringing the
corpus to 73 cases and 25 negative controls. Every prediction held: upgrades
replay in lifecycle order, premature deprecation and removal without
deprecation are nondeterministic, the deprecated and removed stages can roll
back one stage, and nothing written after the legacy stage replays on legacy
code. A negative control can now declare `expected_message`, a fragment of
Core's failure reason, and the runner fails it for any other mismatch. The
coverage gate requires the new patch features, and the mismatch test adds a
wrong-reason scenario. `HISTORY_CORPUS_GENERATIONS` lets a capture record only
new definition sets.

No SDK defect was found. Version routing remains deferred by the v1 support
policy, so no routing claim is made. Concurrent old and new workers on one
task queue are not exercised.

Evidence: `dune test test/history_corpus` passes all 73 cases and the three
runner negative-path scenarios.

## 2026-10-10: Concurrent calls on one client (#807)

One `Client.t` served every call on its supervisor Domain one at a time, and
that Domain waited for the network inside each call: a pending `Client.wait`
held it for 100 ms per turn, and a query, update admission, or update poll
for up to 30 s, so every other call on the client, including shutdown,
queued behind them. Every client RPC is now submit-and-complete. The owner
Domain only resolves the RPC deadline, encodes the request, and calls the new
`ocaml_temporal_core_v4_client_submit_json`, which validates it, registers a
completion cell, spawns one Tokio task, and returns an `<owner>.<call>`
handle. The caller then waits on its own Domain through
`ocaml_temporal_core_v4_client_await_call(owner, call, timeout_ms)`, which
takes no runtime pointer (the pair names a cell in a process-wide registry)
and runs with the OCaml lock released. A call is served only to the random
owner identity of the graph that submitted it; any other owner is answered
like an unknown call and cannot consume or observe it. A call counts
against the runtime's 4,096-call ceiling until its cell is retired (read,
released through the new `ocaml_temporal_core_v4_client_release_call`, or
closed with the runtime), not merely until its RPC ends, and the OCaml
await loop releases its call from a `Fun.protect` finalizer when it is
interrupted, so abandoned outcomes cannot grow the registry without bound. Tasks never call OCaml or touch the runtime graph; a
task that panics or is dropped settles its cell through a guard.

Disconnect and close (including the GC fallback) close every pending cell
first, so waiters get `Invalid_state`, mapped to the supervisor's typed
`Closed` and then to the public closed-client error (an uncertain start for
a start that may have been sent), and then abort and join the tasks before
the connection and Core are dropped. The #499 deadline rule is unchanged: an
expired request is completed by the owner and never reaches Rust. The
64-run wait and 64-request-ID start limits keep their slot sharing, and a
4,096-call ceiling bounds the task registry. Both symbols are additive, so
the ABI stays at version 4; the synchronous and ticket symbols remain for
direct bridge callers, and the eight bounded synchronous RPC methods now
share the submitted path's request decoding and futures. The generic
`Sdk_supervisor.Client_call` await loop and the `Native.Client_submit`
operation, `await_client_call`, and `call` are private.

Evidence: eight Rust tests over a callback gRPC transport (a pending wait and
query do not delay a signal and a start, which complete through their own
cells; disconnect wakes a blocked waiter with the closed status and drops
both transport futures; the GC close path releases calls; validation before
registration; distinct-run wait capacity; a panicking task settles its
cell; awaiting with another owner is refused as unknown and leaves the call
for its submitter; 4,096 finished but unread calls fill the ceiling until one
is released or read), a Rust ABI test and C harness assertions for the new symbols, four
deterministic supervisor tests over `Sdk_supervisor.Make` with a fake
completion source (an interrupted await releases its call exactly once
while a completed one is never released; a wait and a query pending on
other Domains do not delay
signals and starts, shutdown closes calls in flight, each completion reaches
only its caller, and a deadline that expires while queued completes without
submitting), a native supervisor test of submission, expired deadlines, and
closed admission, and the new `concurrent_client` live regression in
`make test-temporal-live-regressions`. Against the Compose server it kept 24
waits and a query stuck on a killed worker pending while ten signals and ten
starts on the same client finished with a worst latency of 10 ms (bound
1.5 s); terminating the workflows released every wait, the query ended at
its 8 s deadline, and shutting down a second client ended its in-flight
wait at once. The `client_request_ids` and `completed_queries` live
regressions also passed locally.

## 2026-10-10: Bounded worker shutdown with outstanding work (#495)

`Temporal.Worker.shutdown` now returns within the worker's grace period plus
a new `Worker.Options` `shutdown_teardown_timeout` (default 60 s), plus
250 ms of slack, whatever user code or the server does. The new
`Worker.shutdown_with_report` returns a `Worker.Shutdown_report.t` with the
elapsed time, whether both lanes stopped, the abandoned activity callbacks and
workflow activations, and whether native teardown completed or was left
`` `Detached `` in the background; `shutdown` is the same call without the
report. OCaml cannot preempt a callback, so work still running when the grace
period ends is abandoned, never awaited: `run` stops joining a stuck activity
Domain and returns, the bridge fails the abandoned task (retryably for an
activity) so Temporal retries it elsewhere, and the callback's late result
reaches a closed supervisor and is discarded. A non-yielding workflow
activation still holds the thread that called `run`; shutdown from another
thread returns within its bound and reports it.

One dedicated shutdown thread owns every step after admission: a `try_lock`
wait for the run mutex until the shared lanes deadline, non-blocking adapter
drains (a retryable retained completion is retried until the deadline, then
reported lost), and the single native release. The caller only waits for its
published outcome, at most the teardown timeout after the release began, so
an unreachable server yields a `` `Detached `` report while that thread still
completes the one release. Abandonment is an `Ok` report; a lost completion
or a native release failure is an `Error` after the graph is released, and
retired leases with nothing abandoned stay an `Error`, so no false success.
Admitted asynchronous activity leases are counted through their own lock
even while an abandoned callback holds the activity adapter, so one still
admitted at the deadline is reported as an `Error` instead of being retired
behind the abandoned callback's `Ok` report. Adapter discard after the
release is deferred while abandoned code holds an adapter lock and is
performed by the detached activity lane itself once its callback returns
(or by `run` after its workflow lane returns).
The orchestration lives in the new private `Native_worker_shutdown` module,
generic over injected effects. The old drain-retry admission reopening and
its policy helpers are removed. No Rust, C or ABI change; the bridge's own 90 s
drain and 30 s finalize bounds are unchanged.

Evidence: 16 focused tests over the real lane scheduler with fake adapters
(idle, cooperative and stuck activity callbacks, a stuck workflow activation,
retained completions retried, exhausted and permanently failing, an
unreachable server during release and during a drain, retired leases without
abandonment, an async lease outstanding or completed beside a stuck callback,
the detached lane performing a pending discard, a raising release, a past
deadline, and the unchanged join without detach); an adapter test showing an
admitted async lease stays counted while a synchronous callback holds the
adapter; three supervisor tests over `Sdk_supervisor.Make` with a fake
backend that blocks like an unreachable server, including a late operation
rejected as `Closed`; option and mock-report unit tests; and the new
`bounded_shutdown` live regression in `make test-temporal-live-regressions`,
which passed repeatedly against the Compose server: an activity worker whose
callback never heartbeats returned from shutdown after about 2.0 s (grace
2 s, teardown 20 s) reporting one abandoned callback with teardown completed,
its `run` had already returned, and Temporal ran attempt 2 to completion on a
second worker. The `activity_cancellation` and `split_worker_task_types` live
regressions also passed. Remaining limits: abandoned code keeps running (and
holding its Domain and memory) until it returns or the process exits, so a
process whose report shows abandoned work must be restarted; and a live
transport fault during shutdown is not yet part of the #504 suite.

## 2026-10-10: Exact-run wait reports retried runs by their real close event (#971)

An exact-run `Client.wait` on a run that its workflow retry policy retried
now returns `Failed { error; successor = Some _ }` with the failure that
caused the retry, instead of `Continued_as_new`. Temporal Server rewrites a
completed, failed, or timed-out close event that links a successor into a
synthetic continue-as-new event, in close-event-only history responses, for
any client that does not send the `supported-features: follows-next-run-id`
gRPC header. The bridge's close-event long poll now sends that header on
every request (Core's retry path copies it to each attempt), so a retried
failure is `Failed`, a retried timeout is `Timed_out`, and a cron completion
is `Completed`, each with its successor, while an explicit continue-as-new
is still `Continued_as_new`. The header is per request; the ABI stays at v4.

Evidence: a Rust unit test maps failed, timed-out, and completed close events
that carry `new_execution_run_id` to their own outcome with the successor; an
in-memory gRPC test checks that every history poll, including later pages and
current-run waits, carries the header; the OCaml terminal-adapter test now
requires each exact wait to keep its close event's kind and a retried
failure's error. The `client_policies` live regression no longer accepts
`Continued_as_new` for a retried run, and adds a retried run timeout
(`Timed_out` with a successor) and an explicit continue-as-new. Cron
schedules cannot be started from this client, so the cron `Completed` case is
unit-tested only.

## 2026-10-09: Cooperative activity cancellation and shutdown signal (#494)

A running synchronous activity can now observe cancellation and worker
shutdown. `Activity.Context.cancellation` returns the delivered
`Activity.Cancellation.t`, with Core's primary reason and every flagged
reason as a closed variant (`Requested`, `Timed_out`, `Not_found`,
`Worker_shutdown`, `Paused`, `Reset`); heartbeats return a non-retryable
`` `Cancelled `` error once one is delivered; and an activity that returns a
`` `Cancelled `` error after a delivered cancellation completes as cancelled
(otherwise it is an ordinary failure, and `Ok` still completes).
`Context.is_worker_shutting_down` reads the worker's `closed` and
`stop_requested` flags. The serial executor keeps its adapter lock for the
whole callback, so delivery rides on the heartbeat: each heartbeat ends with a
bounded, non-blocking sweep of the supervisor's ready activity tasks that
publishes the running attempt's cancellation to a single-assignment atomic
cell and defers every other task, in order, to later polls. A deferred start
whose cancellation was also deferred completes as cancelled without running,
and shutdown drain fails an undispatched deferred start retryably without
running user code. No Rust, C or ABI change.

Evidence: eight focused adapter tests (context cell lifecycle; delivery with
reasons and details; outcome precedence; cancellation before admission and
after completion; shutdown flag; drain of deferred starts; deferred poll
errors) and the new `activity_cancellation` live regression in
`make test-temporal-live-regressions`, which passed locally against the
Compose server: a workflow-cancelled heartbeating activity observed
`Requested` and the workflow saw a `` `Cancelled `` result under
`Wait_cancellation_completed`; a stalled single-attempt activity observed a
`Not_found` cancellation after its heartbeat timeout; and an activity on an
activity-only worker observed the shutdown flag while that worker shut down,
with its completion still accepted. Asynchronous handles are not yet
notified of cancellation, and bounded shutdown for callbacks that ignore the
signal remains #495.

## 2026-10-09: Client execution policies and RPC deadlines (#499)

`Temporal.Client.start` now takes the essential server-side execution
policies: `?execution_timeout`, `?run_timeout`, and `?task_timeout` as
`Duration.t`, a workflow `?retry_policy` (the validated
`Activity.Retry_policy.t`), and `?id_reuse_policy`
(`` `Allow_duplicate `` by default, `` `Allow_duplicate_failed_only ``, or
`` `Reject_duplicate ``) for a workflow ID whose latest run has closed, next to
the existing `?id_conflict_policy` for an open run. Start, signal, `query`,
`query_with_input`, `start_update`, cancel, terminate, reset, and
`list_visibility` accept a per-call `?rpc_timeout` between 1 ms and 60 s that
replaces the operation's built-in budget; omitted, the documented defaults are
unchanged. The `Client` module documentation separates RPC deadlines (one
client call) from workflow timeouts (recorded in history, ending the run as
`Timed_out`).

Validation happens before anything is sent, identically for the native and
`mock://` backends, and again in the OCaml protocol encoder and in Rust. A
zero timeout, a task timeout above 120 s or above the run timeout (the
execution timeout when there is no run timeout), and a run timeout above the
execution timeout are typed defects: the live server showed that Temporal
silently lowers a task timeout above the run timeout instead of rejecting it.
Negative or overflowing durations cannot be built as `Duration.t`. Every
reuse/conflict pairing is accepted: Temporal 1.32 accepts
`reject_duplicate` with `use_existing`. Temporal's deprecated
`TERMINATE_IF_RUNNING` is not offered.

The private start request gains optional `id_reuse_policy` (always sent by
OCaml), `execution_timeout_ms`, `run_timeout_ms`, `task_timeout_ms`,
`retry_policy`, and `rpc_timeout_ms` members. Every other bounded request gains
an optional `rpc_timeout_ms`. Rust converts the millisecond counts with
`try_from` and reuses the command retry-policy converter. The workflow
policies are part of pending-start equality; the RPC deadline is not, so a
reconciling retry may use another deadline. Because the transport reports its
own expiry of a per-attempt deadline as `cancelled`, a `cancelled` or
`deadline_exceeded` status after the caller's budget has elapsed is now
reported as `deadline_exceeded`. The deadline is fixed on a monotonic clock
(a private C stub, not a Rust symbol) when the public call begins, and the
owner Domain sends only the budget that remains when it dispatches the
request, so time queued behind earlier calls on the same client counts. A
request that expired while queued is never sent and fails with the typed
`deadline_exceeded` error; for a start this is a definite rejection, not an
uncertain outcome. A supervisor test holds the owner Domain with a long
operation and shows a queued 50 ms signal failing unsent while a queued
two-second signal is sent with only its remaining budget, and every client
operation on an unconnected supervisor returns `deadline_exceeded` for an
expired deadline instead of reaching the bridge. Visibility RPC failures are
now classified by `Client.rpc_status` too. An uncertain start now carries
`Error.error_type` `StartOutcomeUncertain`, recognized by the new
`Client.is_start_outcome_uncertain`. The mock applies the reuse policy to
closed runs. All new protocol members are optional and no response changed,
so the bridge ABI stays at v4.

Evidence:

- Rust unit tests: reuse-policy wire names, the `allow_duplicate` default and
  enum mapping, policy and timeout validation including the boundary values,
  `rpc_timeout_ms` bounds on every request, pending-start equality, and the
  expired-budget status mapping.
- Rust callback-transport tests: each policy reaching
  `StartWorkflowExecutionRequest` with exact durations and coefficient bits,
  invalid policies never reaching the transport, a 200 ms start deadline on a
  hung server ending as `unknown`, and a 300 ms signal deadline ending well
  inside the three-second default.
- OCaml protocol tests (`test/bridge/test_ocaml_client_protocol.ml`) and
  public mock tests (`test/unit/test_client_worker.ml`) for encoding,
  rejected values, reuse-policy semantics, and per-operation deadline
  validation.
- A new live regression, `test/integration/client_policies`, added to
  `LIVE_REGRESSION_EXECUTABLES`, passed every run against the Compose
  Temporal 1.32/PostgreSQL stack once the fixture was settled (six runs, the
  last two on the committed code), driven natively from macOS. It shows execution and run timeouts
  ending runs as `Timed_out`, the server recording the execution, run, and
  task timeouts, a 1 ms query deadline failing as `` `Deadline_exceeded ``
  while the workflow keeps running, the reuse policies refusing or allowing a
  closed ID, a workflow retry reaching attempt 2, and a 1 ms start deadline
  reconciled by request ID into exactly one run.

Remaining limits: cron schedules and delayed starts stay deferred. Exact-run
`wait` and `wait_update` take no caller deadline. On the pinned server an
exact-run wait reports a retried run's link as `Continued_as_new` rather
than as `Failed` with a successor; the live test accepts both forms.

## 2026-10-08: Deterministic transport fault regression (#504)

`test/integration/transport_interruption` adds a live regression. An
in-process TCP proxy sits between Temporal and one client, and a second proxy
sits between Temporal and one worker. The proxy can refuse connections or
forward requests while discarding the server's response bytes. A direct
client, which is never faulted, then reconciles the durable outcome. The
regression checks these behaviors:

- A refused connect returns a typed `Bridge` error.
- A start whose acknowledgement is lost reports the documented uncertain
  start. A probe with another request ID proves that the start was applied,
  and a retry with the same request ID returns the same run.
- A signal whose acknowledgement is lost returns a retryable status. A retry
  with the same request ID is applied once.
- An update whose response is delayed is re-sent with the same update ID and
  is applied once.
- An exact-run wait survives a 3 s outage.
- A terminate whose acknowledgement is lost reports
  `Termination_outcome_uncertain`, and the run is then observed as
  `Terminated`.
- A worker survives a poll outage.
- A lost activity-completion acknowledgement is retried without dispatching
  the callback a second time.
- Client shutdown while the server is unavailable returns at once.
- Both proxies end with no open connection.

The update and activity scenarios do not end their fault after a fixed delay.
Each waits, with a deadline, for proof that the fault happened: the update must
already be applied, or `ActivityTaskCompleted` must already be in the history
(read with the official CLI), and the proxy must have discarded response
bytes.

No production code changed. The existing classification already met each
assertion.

Evidence: three consecutive runs passed in 36-38 s with OCaml 5.4.1 on macOS
against the Compose Temporal 1.32.0/PostgreSQL stack. CI runs it through
`make test-temporal-live-regressions`. Some #504 scope remains open, as listed
in [transport fault qualification](reference/transport-fault-qualification.md#remaining-504-scope):
worker shutdown while the server is unavailable, faults over TLS, and the
external idempotency example.

## 2026-10-09: Validated worker resource and shutdown options (#498)

`Temporal.Worker.Options.make` now accepts the worker's resource and shutdown
settings as labelled optional arguments:

- `max_concurrent_workflow_tasks` and `workflow_task_pollers`, which is
  `Fixed n` or `Autoscaling { minimum; maximum; initial }`;
- `sticky_queue_schedule_to_start_timeout` and `graceful_shutdown_period`;
- `max_heartbeat_throttle_interval` and `default_heartbeat_throttle_interval`;
- `max_worker_activities_per_second` and
  `max_task_queue_activities_per_second`.

Times are `Duration.t` values bounded to one day. Rates must be positive
finite floats, and the worker rate must be at least one per day because Core
converts its reciprocal into a `Duration` and would otherwise panic during
worker construction. Counts must be between 1 and 1,000,000.

Every invalid value is a typed defect returned before anything is allocated.
That includes zero, negative, overflowing, NaN, infinite and subnormal
values, as well as these contradictions:

- a caching worker with fewer than two workflow task slots or two fixed
  pollers;
- unordered autoscaling bounds;
- an explicit default heartbeat interval above an explicit maximum.

One accessor per setting reports the effective value, with its default, and
the new `Worker.options` returns a worker's options. Omitted settings keep the
values the worker used before:

- 1,000 workflow tasks and 2 fixed pollers;
- a 30 s grace period;
- Core's 10 s sticky timeout and 60 s / 30 s heartbeat throttles;
- no rate limits.

Activity and local-activity slot counts remain pinned to one because the
OCaml activity executor is serial (#777), so they are deliberately not
options. The rate limits can only lower throughput. Core applies
`max_worker_activities_per_second` only to polled tasks, so an eagerly
dispatched activity bypasses it; the interface documents this.

The settings reach Core through an optional, closed `tuning` member of the
private worker document. OCaml omits it when nothing is set, so a default
worker's document is unchanged and the ABI stays at version 4. An archive
built before this change rejects only a tuned worker, with a typed
configuration error. An autoscaling maximum must equal
`max_concurrent_workflow_task_polls`. Core splits a fixed poller count
between the sticky and normal queues but applies autoscaling bounds to each
queue separately, so the bounds are documented as per-queue and the
two-poller cache rule applies only to fixed counts. The Rust mapping test
asserts the per-buffer poller behaviors.

Evidence:

- `test/unit/test_worker_options.ml` checks defaults, explicit values,
  boundary and contradiction defects, the reported options of a mock worker,
  and that fully tuned options pass native worker configuration (an
  unreachable port then fails at the client connection).
- `test/bridge/test_worker_tuning.ml` asserts the exact tuned, default and
  partial documents, every sender-side rejection, and that Rust accepts the
  tuned document through the C entry point.
- `rust/core-bridge/tests/support/worker_tuning.rs` decodes the same tuned
  document and checks every resulting `WorkerConfig` field. That includes
  autoscaling pollers, durations, rates, the cache-limited workflow slots and
  the pinned activity slots. It also covers the Rust-side rejections.
- `test/integration/worker_resource_options/regression.exe` is a new
  entry in `make test-temporal-live-regressions`. It runs a worker with a
  one-workflow cache, two slots and two pollers, 1 s sticky and 2 s grace
  timeouts, and one activity per second through six concurrent executions.
  Against the Compose Temporal 1.32.0/PostgreSQL stack (macOS arm64, OCaml
  5.4.1) every execution and a later recovery execution completed. The
  activity starts spanned 4.97 to 4.98 s over three runs. With the rate limit
  removed the same burst spanned 0.20 s and the check failed, as intended.

The grace period's effect on in-flight work is not live-qualified. `shutdown`
lets the running callback finish before native teardown, so the period only
bounds activity tasks Core still holds afterwards. The end-to-end shutdown
bound remains #495.

## 2026-10-09: Dependency inventory checked against its pinned sources (#783)

`docs/dependencies.md` had drifted from what is built: it still named the
`~dev` package version, PostgreSQL 16.13 and Temporal 1.31.0 service images,
319 Cargo dependencies, `base64` 0.22.1 and Tokio 1.52.3 as direct crates, and
older `taiki-e/install-action` and `msys2/setup-msys2` pins, and it omitted
`conf-protoc`, `conf-rust-2024`, `mtime`, and the `uuid` and `prost` direct
crates. The inventory was regenerated from the locked inputs, with every
licence re-read from package metadata (`opam show -f license` or the
opam-repository manifest, `cargo metadata`, crates.io, or the upstream
repository). It now records each OPAM package's role (runtime, test-only,
build, system check, or compiler), the per-compiler lock overrides, the odoc
closure, one table for the complete linking-exception scope (compiler,
build-only ocamlbuild, and the three CI-only odoc packages), the Rust and
Python images, a direct Rust dependency table, and every GitHub Action with
its pin. The stale note that odoc was rejected was replaced, since #794 adopted
it as a CI-only renderer.

`scripts/check-dependency-inventory.sh` (`make check-dependency-inventory`)
compares those tables with `temporal-sdk.opam(.locked)`,
`scripts/opam-lock-overrides.txt`, `scripts/docs-tools.locked`, every
digest-pinned image in the Dockerfiles, Compose file, and workflows,
`rust/Cargo.lock` package count and locked versions, the workspace and bridge
Cargo manifests (including crates the bridge declares directly; an
unattributable dependency table fails closed), `rust/rust-toolchain.toml`, the
quality-tool pins, and every external `uses:` reference. The
linking-exception table is compared with the locked versions of the compiler,
ocamlbuild, and the three CI-only odoc packages, so upgrading one fails until
that policy row is reviewed. `make license-check` runs it first, so the
standalone license job fails when Dependabot or a manual bump changes a pin
without its inventory row. It compares names, versions, and pins only;
licence columns stay a reviewed record behind the existing OPAM and Cargo
licence gates. `make test-dependency-inventory-contract`, also run in that
job, accepts the committed tree and a CRLF copy of the document and rejects
24 single-source drifts, each named by table.

Licence findings: no new or changed licence fell outside the policy. The
Python image's CPython licence is recorded as the PSF stack (SPDX
`Python-2.0`) because GitHub's detector reports `NOASSERTION` for it; the image
is CI-only. `actions/checkout` remains referenced by its `v7` tag rather than a
commit, unlike every other action.

## 2026-10-09: Bounded replay completions (#965)

`replay_worker_accepts_one_history_document` hung intermittently on Windows CI
until the job timeout. Running the unit tests in parallel loops on macOS
reproduced it 3 times in about 2,500 runs, all under parallel load. In all three, the test thread was
blocked in the final eviction acknowledgement, and Core's workflow processing
thread had already exited.

The cause is a race between Core's natural replay shutdown and that
acknowledgement. Core's completion reply sender can be stranded in a Tokio
channel whose receiver has dropped, and Core's `Workflows` keeps the sender
alive. The [replay bridge reference](reference/replay-bridge.md#bounded-core-waits)
explains the mechanism.

Changes:

- Replay completions, rejections, and disposal acknowledgements now await
  Core with a bound. An empty acknowledgement still pending 250 ms after the
  workflow lane observed Core's `ShutDown` is treated as accepted, which is
  what Core does for an empty completion that reaches a closed stream.
- The feeder send and the replay lane join are bounded at 60 seconds, so
  other stalls become typed `WORKER` failures, and hence `Replay_error`. The
  join bound wraps the whole join, so an acknowledgement inside it cannot
  extend it.
- After a lane join times out, the worker never reaches Core's unbounded
  finalizer. `finalize` refuses, and the next `dispose`, or
  `drop_runtime_graph`, deliberately leaks the Core worker with a counter and
  one stderr line. Running the finalizer could block forever, and detaching it
  would hang the Tokio runtime drop instead. The
  [reference](reference/replay-bridge.md#bounded-core-waits) documents the
  trade-off. Tests force both timeout paths with a 200 ms injected bound.
- The test runs under a 30-second deadline that reports the blocked step.

After the fix, 3,200 runs of the same loops passed with no hang. Temporary
instrumentation recorded two stranded acknowledgements in those runs, and
both were released by the new bound. Also verified with `cargo test --locked`,
clippy, rustfmt, and the bridge and history-corpus dune tests.

## 2026-10-07: Shared runtime across clients and workers (#832)

Following the maintainer decision on #832, several clients and workers can
now share one Core runtime and Tokio pool through the new public
`Temporal.Runtime` (`create ?io_threads`, `attached`, `shutdown`) and
`?runtime` on `Client.create` and `Worker.create`; without it every instance
still owns its own runtime. Rust moves Core into a reference-counted
`SharedCore` held by an opaque shared-runtime handle and by every attached
graph, through three additive v4 symbols (`shared_runtime_new`,
`runtime_new_attached`, `shared_runtime_free`/`_dispose`), so Core is
destroyed exactly once by its last holder and never under a live graph. On
the OCaml side each attachment is a lease that the instance's supervisor
releases after closing its graph; `Runtime.shutdown` returns a typed defect
while any lease is outstanding. Evidence: Rust integration tests for shared
Core identity, independent replay workers on one pool, release in both
orders, and exactly-once destruction including the GC fallback; the C ABI
harness under ASan/UBSan; and an OCaml test covering the bridge, the lease
ledger, two real supervisors on one runtime, public ordering errors for
`mock://` and refused native targets, and a Linux thread-count leak check.
The design and ownership rules are in `docs/reference/core-bridge.md`.

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
test proves the public classification. The bounds were fixed at the time;
the 2026-10-11 entry above makes them configurable.

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

## 2026-10-24: Corpus histories for timer cancellation and child options (#1134, #1135, #503)

Capture `live-2026-10-24` adds the frozen `corpus-v2-cancellation`
definition set and six live histories, recorded natively on macOS against
the pinned Compose server from a clean tree: a scoped 60 s timer cancelled
while pending (`TimerCanceled`), a scoped timer that fired before its scope
was cancelled (no cancel command), a bounded condition that timed out
(`TimerFired`), one satisfied by a signal (`TimerCanceled`), and a parent
and child where the start-child event records 60 s / 30 s / 7 s timeouts and
`AllowDuplicateFailedOnly`. The negative control
`negative-timer-cancel-dropped` replays the cancel-before-fire history
against code that never cancels and must report nondeterminism. New feature
rows `timer-cancellation`, `condition-timeout` and `child-start-options` are
`replayed`; the corpus test passes with 85 cases.

## 2026-10-24: Linux bubblewrap install of the source archive in CI (#1148)

`opam-source-sandbox-linux` in `build-pr.yml`, on every tier, and
`source-archive-check-linux` in `release.yml`, which `publish` now needs,
build the source archive under opam's bubblewrap sandbox on Ubuntu 24.04.
Each job then builds the installed-package consumer against the result.
They lift Ubuntu's AppArmor restriction on unprivileged user namespaces that
bubblewrap needs, and fail if opam has no sandbox hook. They set the new
`OCAML_TEMPORAL_OPAM_CHECK_READ_ONLY_CARGO_HOME` mode of
`check-opam-source-archive.sh`: Cargo's home is a cold directory outside
every writable path, and a write probe proves it is read-only inside the
sandbox before the build, as a user's default home is under `opam install`.
Locally, the mode refused a work directory under `/tmp`, which the macOS
wrapper leaves writable. The release checklist now lists the Linux job and no
longer makes the opam install a manual post-publication step.

## 2026-10-24: Scoped durable-timer cancellation (#1134, first slice)

`Workflow.start_sleep` and `Workflow.sleep` take `?scope`. The runtime timer
now returns a cancellation operation next to its future: while the timer is
pending it removes the resolver, emits one `Cancel_timer` and settles the
future with a `Cancelled` error. After the firing job (same or later
activation), after a repeated cancellation, or once a terminal command sealed
the context, it is `Ok ()` with no command; from another execution it is a
typed defect. Core's timer machine ignores cancels of fired timers and
delivers no firing job for a cancelled one, so no bridge or protocol change
was needed, and unscoped sleeps emit the same commands as before.
`Scope.with_scope` around `Future.race work (start_sleep ~scope timeout)`
therefore leaves no pending timer when the work wins.
`test/runtime/test_timer_cancellation.ml` covers cancel before fire,
direct-style `sleep ~scope`, cancel after fire in the same and a later
activation, the race cleanup, an already-cancelled scope, zero duration,
unchanged unscoped commands, and foreign-owner rejection. Still open under
#1134: `Condition.wait_until ~timeout`, a live scenario and corpus histories
for cancel-before/after-fire.

## 2026-10-24: Build and CI hygiene (#1150)

- The OPAM license-policy fixtures (`test/smoke/test_license_policy.sh`) now
  run in `make test-quality-contract`: in the Compose image on Linux and
  with the native OCaml toolchain on macOS. Windows skips them, because its
  opam cannot read the MSYS temporary fixture paths. They take the
  repository root as an argument, so they no longer depend on the working
  directory. A mutant allow-list that admits GPL fails them.
- `rust/dune` excludes `target` and `vendor` from Dune's source tree. With a
  populated `rust/target`, `dune rules` for the bridge archive lists no
  `rust/target` path; without the file it lists the probe.
- `make native-test` runs the sanitized C ABI harness on macOS (verified
  locally). Windows skips it, as `quality-gates.md` records, because MinGW
  ships no sanitizer runtimes.
- Every `actions/checkout` reference is pinned to
  `3d3c42e5aac5ba805825da76410c181273ba90b1` (v7). The quality contract now
  requires a full commit SHA for every remote action in every workflow,
  GitHub-owned ones included, and a mutable `@v7` in `stress.yml` fails it.

## 2026-10-24: Child workflow timeouts and ID reuse policy (#1135, first half)

`Child_workflow.start`, `start_handle` and `execute` take
`?execution_timeout`, `?run_timeout`, `?task_timeout` and `?id_reuse_policy`.
The timeout rules moved from `Client.start` into the private
`Workflow_timeouts_private.validate`, so the client and a parent reject the
same values with the same messages before any command. The runtime command,
the OCaml workflow protocol (encoder and decoder), the completion JSON schema
and the Rust `StartChildWorkflow` decoder carry the four fields as optional
keys omitted when unset, so existing child starts encode byte-identically.
Rust re-validates the timeout rules, maps them onto Core's
`StartChildWorkflowExecution` fields (an unset reuse policy stays
`Unspecified`), and the reverse Core conversion now accepts them, still
rejecting the deprecated terminate-if-running policy. Evidence: the Rust tests
`child_workflow_timeouts_and_reuse_policy_round_trip` and
`child_workflow_timeout_rules_are_enforced` pin the exact document, which the
OCaml `test_child_timeout_options` decodes, and
`test/runtime/test_child_start_options.ml` covers the public boundary.
`Temporal.Testing` records but does not enforce child timeouts. Still open
under #1135: memo and search attributes (blocked by #512), a live
run-timeout regression and a corpus history.

## 2026-10-24: Interface and reference corrections from the second audit (#1146)

Eight rows of the #1146 audit are corrected against the code:

- `Activity.Context.info` now says `Temporal.Testing` contexts carry task
  metadata, with virtual-clock times; only the `mock://` backend's do not.
  `Async_context.info` was already accurate, because `Temporal.Testing` does
  not run asynchronous activities.
- The `Temporal.Testing` differences now list that workflow timeouts are
  always `None` and that activity times come from the virtual clock.
- The start-metadata reference now says Testing records a child's retry
  policy.
- The workflow-failures reference now names `Client.start ?retry_policy`.
- The observability reference now describes the post-#790 validator rule
  and the handler rule after acceptance.
- `Update.Handler.make` now documents what a handler `Error` does.
- The interactive-workflows reference now names `Client.query ?rpc_timeout`.
- The runtime invariants now describe the one `Obj`-based store,
  `Workflow_context.Local`, and why the value restriction makes it sound.

Two rows are left to the issues that change the code: the detached Core
finalizer's unbounded drop (#1149) and the `backend.ml` details comment
(#1143, which removes that function).

## 2026-10-24: Release from the tag, not the master tip (#1144)

The Release workflow is now dispatched from the version tag
(`refs/tags/<tag>`), so `GITHUB_SHA`, every checkout and the signed provenance
name the tagged commit regardless of what merged to `master` after tagging.
Validation refuses any other dispatch ref and calls
`check-release-tag-commit.sh TAG SHA master`, whose new optional branch
argument fetches enough history (unshallowing a depth-1 checkout) to require
the tagged commit to be on `origin/master`. The draft and publish checks still
re-verify that the tag peels to the same commit. The contract test covers a
tag that stays releasable after `master` advances, the same check from a
depth-1 clone, an unmerged tagged commit, and an unfetchable branch; a
mutation that drops the ancestry check fails it. The quality contract pins the
validate job's ref and ancestry checks. The checklist, preflight guide and
attestation verification (`--source-ref refs/tags/<tag>`) are updated.

## 2026-10-24: Local activity cancellation from workflow code (#1156)

`Activity.start_local`, `execute_local` and the new `start_local_handle` take
`?scope`, and `start_local_handle` returns the ordinary activity handle whose
`Activity.cancel` emits `Request_cancel_local_activity` once. Both paths reuse
the remote-activity scope attachment (registered until the future settles),
so the recorded `cancellation_type` (default `Wait_cancellation_completed`)
now has an effect. A local activity in a language-owned `DoBackoff` timer is
cancelled with its timer and settles immediately, as the runtime already did.
`test/runtime/test_local_activity_scope.ml` covers a scope cancelling a
running local activity once, an explicit handle cancel followed by Core's
resolution and an idempotent late cancel, cancellation during backoff, and no
command after completion or for an already-cancelled scope. Live
qualification of local cancellation remains open.

## 2026-10-24: Replay-safe uuid4, random_float and random_bytes (#1155)

`Workflow.uuid4`, `random_float` and `random_bytes ~length` draw from the
run's seeded stream, like `random_int`, so replay sees the same values and a
reset continues from its recorded seed. The new draws see each stream word
through the SplitMix64 output finalizer, so even a small seed gives well-mixed
bits. `random_int` keeps the raw words, so earlier histories replay
unchanged. `test/runtime/test_workflow_random.ml` checks:

- same-seed equality and reset behavior;
- a pinned golden stream;
- the UUID v4 layout;
- the float and byte bounds;
- the defects outside a workflow and in update validators.

Capture `live-2026-10-24-random` adds a `corpus.random_parent` history,
whose child workflow ID is built from the draws, and its child. The negative
control `negative-random-reordered` draws in another order and must report
nondeterminism. All 88 cases pass. The workflow guide shows a `uuid4`
fan-out.

## 2026-10-24: Bounded Client.wait and wait_update (#1136)

`Client.wait ?timeout` and `Client.wait_update ?timeout` bound the caller's
wait from the moment of the call; on a current-run handle the one budget
covers the whole run chain, and on an update every poll. The deadline is
enforced in `Sdk_supervisor.Client_call.await`, the loop every submitted
client call is awaited through: no bounded native wait outlasts the
remaining budget, and on expiry the loop releases the call (freeing its
share of the `max_pending_waits` slot and aborting the Rust long poll, as
#1019 already did for abandoned calls) and passes the shared
expired-deadline failure through the call's own decoder. The caller
therefore gets the same retryable error with `rpc_status`
`Some `Deadline_exceeded` that any expired RPC deadline produces, and the
workflow and the handle are untouched, so a later wait observes the run. No
Rust or protocol change was needed. A zero timeout is a typed defect; a bound
too long for the monotonic clock is treated as unbounded. Evidence:
`test_concurrent_client_calls` checks with a fake clock that slices shrink
to the remaining budget, that expiry releases once and decodes the deadline
error, that an outcome published first is kept, and that an expired deadline
releases without polling; `test_client_worker` checks validation and that a
bounded mock wait is unchanged. A live regression against a stuck run
(`test/integration/concurrent_client`) remains open.
Codex review follow-up: an expired call is read once with a zero-length wait
before it is released, so an outcome published while the caller was paused
is returned rather than dropped, and a wait or update poll whose deadline has
already passed is not submitted at all. Like `?rpc_timeout`, the bound does
not interrupt the owner's network-free admission of a request.

## 2026-10-24: Bounded condition waits (#1134, second slice)

`Condition.wait_until ?timeout` and `wait_until_result ?timeout` bound a
workflow-local condition with a durable timer built from the scoped timer
cancellation of the first #1134 slice. The predicate handed to the
condition store also reports true once the timer has fired, so the
activation loop's usual idle recheck wakes the waiter; the application
predicate is then re-read so a satisfied state wins over a timeout delivered
in the same activation, and the wait's private scope is cancelled, which
emits `Cancel_timer` for a still-pending timer. A timeout is a typed
`` `Timeout `` error. No runtime or protocol change; unbounded waits record
nothing, as before. `test/runtime/test_condition_timeout.ml` covers
satisfaction cancelling the timer, timeout, satisfaction and firing in one
activation, zero and unbounded waits, a predicate error, and the detached
defect. The live scenario and corpus histories for cancel-before/after-fire
remain open under #1134.

## 2026-10-24: Per-workflow versioning behavior (#1159)

`Worker.workflow ?versioning_behavior:[ `Pinned | `Auto_upgrade ]` declares a
workflow type's own behavior on a deployment-versioned worker, overriding
the worker default. Every successful completion carries it in the new
optional `versioning_behavior` member of the completion document, validated
by OCaml and Rust, and Rust sends it as Core's `Success.versioning_behavior`.
Evictions and failed tasks carry none. `Worker.create` rejects it on a
worker without deployment-based versioning and `use_worker_versioning`. The
new `worker_versioning` live regression runs builds `v1` and `v2` of one
deployment with an `Auto_upgrade` default. After `v2` becomes current, the
auto-upgrade run moves to `v2`, while the pinned run stays on `v1` and its
history records `VERSIONING_BEHAVIOR_PINNED`. A mutant that drops the
declaration fails it because the pinned run moves. It passed locally against
the pinned Compose stack.

## 2026-10-24: Local activity cancellation live through the public API (#1156)

`test/integration/local_activity_cancellation` no longer reaches into the
private runtime adapter: each policy's execution schedules with
`Activity.start_local_handle` and cancels with `Activity.cancel`, and a new
fourth execution starts with `Activity.start_local ~scope` (local default
`Wait_cancellation_completed`) and cancels the scope from the signal
handler. Every execution cancels a committed 120-second retry timer, records
exactly one local-activity marker, completes, and replays on a fresh worker
with the same query result and no new attempt. It passed locally against the
pinned Compose stack. A corpus history remains open.

## 2026-10-24: Asynchronous handles observe worker shutdown (#1157)

`Activity.Async_handle.is_worker_shutting_down` and
`Activity.Async_context.is_worker_shutting_down` read the worker's shutdown
flag, and once it is set `Async_handle.heartbeat` returns a retryable
`` `Cancelled `` error of type `"WorkerShutdown"` instead of `Ok`. The
heartbeat is still recorded, and an activity-state error from the same call
takes precedence. A completer that fails the handle with that error hands
the attempt back. The `bounded_shutdown` live regression gained an
asynchronous scenario: worker C's completer hands attempt 1 back, C's
shutdown with a 30 s grace period returns in about 0.1 s with a clean
report, and worker D completes attempt 2. A mutant without the heartbeat
error fails it with a lost completion. It passed locally against the pinned
Compose stack. The cooperative-cleanup example's asynchronous completer
releases its reservation and fails retryably on shutdown.

## 2026-10-24: Bounded waits live (#1136)

The `concurrent_client` live regression gained a bounded-wait check on a
client with `max_pending_waits:1`: a 1 s `Client.wait ?timeout` on an open
run returns a retryable `Deadline_exceeded` error after about a second, the
released slot admits a wait on another run, a later bounded wait on the same
handle observes the run's termination, and shutdown during a bounded wait
ends it promptly with the closed-client error. It passed locally against the
pinned Compose stack.

## 2026-10-24: Client.fetch_history (#1137)

`Temporal.Client.fetch_history ?page_size ?rpc_timeout handle` returns the
complete recorded history of a run as a `Replay.History.t`, so a replay gate
needs only `Temporal.Client` and `Temporal.Replay`. It is the submitted call
selector `HISTORY` (13), additive within ABI version 4. Rust follows every
`GetWorkflowExecutionHistory` page within one budget (30 s by default) and
returns one binary `History` message, the concatenation of the re-encoded
pages, bounded by the payload safety limit. The
`test/integration/fetch_history` live regression replays an open run's
partial history and the complete history fetched with the default page size
and with one and three events per page. A variant that changes only the code
after the run's last signal must fail each complete fetch, so a fetch that
dropped later pages would be caught (verified with a mutant that stops after
the first page). It passed locally against the pinned Compose stack.
`Replay.History.of_json` for CLI exports remains open.

## 2026-10-24: Live child start options regression (#1135)

`test/integration/child_start_options` runs against the Compose
Temporal/PostgreSQL stack and is part of `make test-temporal-live-regressions`.
A child started with execution, run and task timeouts reports
`exec=60000 run=30000 task=7000` through `Workflow.info`, read through the
parent's query before and after a worker replacement that replays the
parent's start-child command. A child stalled behind a 3 s run timeout is
ended by Temporal: the parent sees a `child_workflow` error and an exact-run
`Client.wait` on the child reports `Timed_out`. After a child completes, the
same child ID is refused under `` `Reject_duplicate `` and reused under
`` `Allow_duplicate ``. It passed locally against the pinned Compose stack.
The parent's error is not yet a typed timeout kind (#1143), and child memo
and search attributes remain blocked by #512.

## 2026-10-25: The 65,536-byte lifecycle identifier limit is reachable (#1224)

OCaml validates and documents each client and worker identifier (target URL,
identity, namespace, task queue, build ID, deployment name) as at most
65,536 bytes. Rust nevertheless capped the whole lifecycle document at 64 KiB,
so a single maximal identity failed late with a `Configuration` "lifecycle
configuration exceeds" error after a supervisor and Core runtime had been
created. The cap is now five strings at the per-field limit, each fully
`\u00XX`-escaped, plus 64 KiB for fixed fields (about 2 MiB). It still bounds
parser allocation for a document only the SDK writes. The new
`test/bridge/test_lifecycle_config_limits.ml` connects with a 65,536-byte
identity, plain and made of control characters, to a refused endpoint and
gets a connection error. It also starts a replay worker whose namespace, task
queue and build ID are each 65,536 bytes. With the old cap it fails with
"plain identity: rejected by the lifecycle document cap".

## 2026-10-24: Typed failure origin and heartbeat details (#1143)

`Temporal.Error.origin` classifies an error received from Temporal by the
first layer of its cause chain past the activity, child-workflow and Nexus
operation wrappers: `` `Application ``, `` `Timeout t `` (with the timeout
type, also through `Error.timeout_type`), `` `Server ``, `` `Cancelled ``,
`` `Terminated `` or `` `Other ``; it is `None` for an error the SDK or
application created. Workflow code can therefore tell an activity timeout
from a business or server failure without parsing the message, and the
category stays `` `Activity `` or `` `Child_workflow ``. Heartbeat payloads of
timeout and reset layers are no longer merged into `details`; they are
returned by `Error.last_heartbeat_details`. The native workflow path and the
client's `wait` share the classification (`Failure_diagnostic`). Covered by
focused protocol tests and a native-worker test that resolves an activity
with a heartbeat timeout. `Error.make` accepts `?origin` and
`?last_heartbeat_details` for tests; neither is transmitted. A live
cross-SDK timeout check (#517) remains open.

## 2026-10-24: Live timer cancellation regression (#1134)

`test/integration/timer_cancellation` runs in
`make test-temporal-live-regressions` and passed locally against the pinned
Compose stack. A bounded `Condition.wait_until` satisfied by a signal records
`TIMER_STARTED` and `TIMER_CANCELED`; one left to expire records
`TIMER_FIRED` and returns `` `Timeout ``; a `Workflow.sleep ~scope`
cancelled from a signal handler records `TIMER_CANCELED` and returns
`` `Cancelled ``. Each outcome is read before and after a worker
replacement, so a fresh worker replays the cancel or the firing without a
nondeterminism error. History-corpus entries for these runs remain open
under #1134 (#503).

## 2026-10-24: Concurrent bounded waits and bounded update waits live (#1136)

The `concurrent_client` live regression now covers the last #1136 criteria.
On a client with `max_pending_waits:1`, waits of 1 s, 3 s and 6 s on the
same open run, started together, each expire at their own bound with a
retryable `Deadline_exceeded`, and once all three have expired a wait on a
second run is admitted, so no expired waiter kept a share of the shared
run's slot. A `hold` update accepted by the worker but released
only by a later signal makes a 1 s `Client.wait_update ?timeout` expire with
the same error, and later bounded waits on the same update handle return its
result. It passed locally against the pinned Compose stack.

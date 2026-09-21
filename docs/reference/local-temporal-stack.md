# Local Temporal and PostgreSQL Stack

This reference describes the live infrastructure and first end-to-end worker
acceptance available now. The private typed poll/completion operations and
bounded native readiness waits are composed into the public worker loop. The
Compose target runs a lower-level supervisor lifecycle check and then a
separate public OCaml worker and driver that assert real workflow results. The
workflow adapter still treats unsupported or incomplete commands as typed
errors rather than inventing defaults at the OCaml/Rust boundary.

The complete Compose fixture lives under `test/integration/temporal/`, including
its PostgreSQL/Temporal configuration and fixture-only helper scripts. The
repository root intentionally contains no Compose file. Use the Make commands
below: they select the fixture with explicit Compose file, project-directory,
and project-name arguments while preserving the repository root as the
development-container build context and source mount.

The live acceptance fixture has two different OCaml application roles. The
long-lived `smoke-worker` registers the fixture's workflows and mock activity,
polls Temporal, executes the registered OCaml code, and reports completions.
The short-lived `smoke-driver` is the acceptance test runner: it registers no
worker, starts known workflows through `Temporal.Client`, waits for each exact
workflow/run result, checks the expected values, and exits nonzero when an
assertion fails. The driver is therefore not a second worker and does not
execute workflow or activity code itself.

## Requirements and ports

Install Docker with Compose v2 and Make. PostgreSQL is reachable only by other
containers on the project network. Temporal's gRPC frontend is published at
`localhost:7233` by default; set `TEMPORAL_FRONTEND_PORT` when that host port is
already in use:

```sh
TEMPORAL_FRONTEND_PORT=17233 make temporal-start
```

The development credentials are the fixed user/password `temporal`. They are
appropriate only for this isolated local stack and must not be copied into a
shared or production deployment.

## Operator commands

```sh
make temporal-start
make temporal-health
make temporal-status
make temporal-logs
make temporal-stop
```

`temporal-start` waits for PostgreSQL, runs the one-shot schema migration,
waits for Temporal's frontend listener, and then executes the full health gate.
The health gate verifies the primary and visibility `schema_version` tables,
calls Temporal's gRPC cluster-health operation, and describes the
`temporal-sdk-test` namespace. Namespace creation is idempotent.

`temporal-stop` removes the containers and network without being the acceptance
teardown. The acceptance target always calls `temporal-clean` before and after
its run, so no PostgreSQL volume or workflow history is preserved between
acceptance runs. To remove local histories and all schema data explicitly (or
after an interactive stack stop):

```sh
make temporal-clean
```

## Clean integration acceptance

Run the infrastructure acceptance test with:

```sh
make test-temporal-integration
```

The one-shot driver is bounded to 300 seconds by default. The bound absorbs a
temporary PostgreSQL checkpoint or host I/O stall while still failing a lost
native request well before the CI job timeout. The Compose command sends
`TERM` at the bound and permits 10 seconds for graceful cleanup before `KILL`;
set `TEMPORAL_DRIVER_TIMEOUT_SECONDS` to override the bound for a slower local
machine.

The test intentionally removes this Compose project's PostgreSQL data volume
before and after the run. No PostgreSQL volume or workflow history is
preserved between acceptance runs. After database/frontend readiness, its
OCaml lifecycle
executable uses the private supervisor and C/Rust bridge to connect the
official Core client, construct and namespace-validate a workflow/remote-
activity worker, exercise invalid and repeated lifecycle transitions, and shut
the graph down deterministically. It then waits for `smoke-worker` to publish
readiness and runs `smoke-driver` as a one-shot test process. The
`temporal-start-worker` target force-recreates the worker container before
waiting, because readiness lives in that container's `/tmp` and must not be
inherited from a stopped container. Before
`Temporal.Worker.create`, the worker removes any prior readiness marker after
validating the readiness path and before validating later marker settings; its
finalizer removes the marker again.
This ordering prevents a reused container from reporting a previous run's
readiness while the current worker is still being constructed or has failed.
The baseline driver stages the workflows in the
[generated inventory](live-acceptance-inventory.md). It waits for per-run
worker-visible readiness markers before signal/update/cancellation/termination
operations and serializes timeout-retry fixtures after the shorter heartbeat
path. It requires exact successes or typed terminal failures, including local
activity success, asynchronous completion, continuation, child outcomes and
completed-target external signal rejection.
The [live evidence audit](live-acceptance-coverage.md) pins the tested source,
successful job, Compose images and exact assertion boundaries. Historical
PR #266 verified an earlier, smaller baseline; its scenario total does not
describe the current driver.
After the driver exits, the Makefile stops
the worker and requires the current run's exact `.worker-stopped` marker; the
driver's successful `client_shutdown` phase provides the corresponding client
teardown evidence.

This is a real workflow-result acceptance fixture, not only a lifecycle test.
The complete [PR #253 CI run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
has live evidence for fan-out, timer/activity, parent/child success and
failure/cancellation, ordinary, heartbeat-detail, and timeout-triggered
activity retry, delayed asynchronous activity completion, continue-as-new
successor following, typed workflow failure, exact-run cancellation, and
graceful shutdown. A separate `make test-temporal-worker-restart` target in
the same run also verifies worker replacement, replay, exact-run continuation,
history ordering, and volume cleanup. This baseline fixture does not establish
child start-failure, heartbeat-timeout-triggered retry, sticky-cache eviction,
or crash recovery. Separate gates now cover forced crash recovery and the
one-slot sticky-cache eviction contract; see the [live acceptance
matrix](live-acceptance-coverage.md) for their current evidence.
The workflow configuration runs this target on pull requests and pushes to
`master` in a standalone Ubuntu job labelled for OCaml 5.5; a queued or
cancelled Actions run is not live acceptance evidence. It is intentionally
absent from the multi-version build matrix because starting a real database and
Temporal cluster once provides the same infrastructure evidence.

## Workflow-patch replay acceptance

`make test-temporal-workflow-patching` uses the same isolated Compose topology
but has its own controller and worker services. It first invokes
`make test-temporal-workflow-patching-contract`, which is Docker-free and
checks the checked-in fixtures, history-normalization, and fail-closed
validation contract. It checks only that the acceptance-schema documents are
readable JSON objects; it does not run a JSON Schema validator against the
fixtures. The live portion starts a client-only OCaml driver plus distinct
legacy, active, deprecated, and removed worker processes.

The controller creates a marker-free history under a workflow definition with
no `Temporal.Workflow.patched` call, replaces that worker with a fresh
patch-aware process, and requires the old activity/result branch during replay.
It then creates an active marker under the patch-aware definition and replaces
that worker with deprecation-only code, which must preserve the false marker
while running the new behavior. Finally, a fresh deprecation worker creates a
true marker before code containing no patch API replays the same run. It validates server history before and after each
replacement, observes `is_replaying=true` from generation two, and removes the
project's PostgreSQL volume during teardown. The legacy snapshots must contain
zero patch markers; the active snapshots must contain exactly one false marker,
and the removal snapshots exactly one true marker. The complete [PR #348 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29411260374) records a
successful invocation of the two original scenarios; the complete [PR #356
run](https://github.com/mfow/ocaml-temporal/actions/runs/29469232271) records
the active-to-deprecated and deprecated-to-removed scenarios as well.

## Parent/child restart and replay acceptance

`make test-temporal-parent-child-restart` first runs a Docker-free bilateral
history/checkpoint contract, then starts the same fresh PostgreSQL/Temporal
topology with a client-only OCaml driver and two sequential instances of one
worker-only OCaml binary. The controller binds both exact runs, removes
generation one while parent and child are pending, requires generation-two
replay for both, and deletes the project volume. See the
[dedicated acceptance reference](parent-child-restart-replay-acceptance.md) for
the three history stages, private atomic checkpoint, and current live-evidence
status.

`make test-temporal-parent-child-failure-replay` runs the corresponding
Docker-free failure contract and then the live replacement scenario. The child
fails after generation-two replay and the parent must handle that typed child
failure; see [child failure after worker-replay acceptance](child-failure-replay-acceptance.md).

Running `docker compose` directly from the repository root is unsupported. The
root Make targets are the stable interface and deliberately hide the fixture's
test-only location and Compose project identity.

If startup fails, `make temporal-logs` prints the last 200 lines from
PostgreSQL, schema migration, and Temporal Server. Image pulls can be large;
check local storage before the first run. The integration target prints those
logs automatically before cleanup when a readiness command fails.

## Image and platform policy

The stack pins official PostgreSQL 16.13 Bookworm and Temporal 1.31.0 Server
and admin-tools OCI manifests. All three manifest indexes contain native Linux
`amd64` and `arm64` images. Version upgrades require rerunning the clean smoke,
the stop/start persistence path, manifest inspection, and the dependency and
license review.

The primary software licenses are permissive: Temporal Server and its official
tools are MIT, PostgreSQL uses the PostgreSQL License, and the Docker Official
Image packaging for PostgreSQL is MIT. See the
[dependency inventory](../dependencies.md) for exact references and scope.

## Kubernetes correspondence

The Compose services intentionally separate schema migration from the server,
matching the production responsibility split:

| Compose component | Kubernetes responsibility |
|---|---|
| `postgresql` | Managed PostgreSQL or a reviewed StatefulSet |
| `temporal-schema` | Versioned schema migration Job |
| `temporal` | Temporal Server Deployment and Service |
| Compose health checks | Startup/readiness probes and cluster monitoring |
| fixed environment values | ConfigMaps and Secrets |

Use Temporal's maintained Helm charts and production guidance rather than
translating this local Compose file mechanically.

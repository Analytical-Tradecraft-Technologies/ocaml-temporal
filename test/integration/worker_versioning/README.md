# Worker versioning regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
dune exec test/integration/worker_versioning/regression.exe -- \
  check http://127.0.0.1:7233 /path/to/temporal
```

The second argument is the official Temporal CLI, used to set the
deployment's current version and to read each run's history. The test covers
per-workflow versioning behavior (#1159) on a live server:

- Two worker processes run builds `v1` and `v2` of one worker deployment on
  one task queue (both unique per run). Each registers
  `worker-versioning.pinned` with `` ~versioning_behavior:`Pinned `` and
  `worker-versioning.auto` with `` ~versioning_behavior:`Auto_upgrade ``,
  under an `` `Auto_upgrade `` worker default, so only the pinned type's own
  declaration can keep its run on `v1`.
- With `v1` current, one run of each type starts. Each records the build
  (`Workflow.current_deployment_version`) of the workflow task that observes
  each `step` signal. After one step both report `v1`.
- `v2` starts and becomes current while the `v1` worker keeps running.
  Routing changes reach the task queue asynchronously, so the auto-upgrade
  run is stepped until a task runs on `v2` (within 60 s). The pinned run is
  given the same number of steps, and every one of them must still run on
  `v1`.
- After `finish`, both histories, read with the CLI, record
  `VERSIONING_BEHAVIOR_PINNED` and `VERSIONING_BEHAVIOR_AUTO_UPGRADE` on
  every completed workflow task.

The test terminates its workflows and reaps its workers, including after an
assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes.

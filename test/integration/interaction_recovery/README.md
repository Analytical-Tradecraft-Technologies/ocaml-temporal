# Query and suspended-update recovery regression

Issue #530. Against a disposable Temporal server with a `default` namespace:

```sh
make test-interaction-recovery-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/absolute/path/to/temporal
```

The driver spawns and reaps its own worker processes on unique task queues
and runs three executions of one workflow. Each workflow keeps a counter and
accepts an `add` update whose handler suspends on a `release` signal, then
starts a short durable timer before it applies the amount.

- **Cache eviction.** The worker has a one-entry sticky cache. After the
  update is accepted (and a negative update is rejected by its validator), a
  filler workflow's first task forces the target out of the cache. A query
  must then answer from the replayed state. A query to the filler evicts the
  target again, so the release signal resumes the suspended handler from
  history.
- **Worker restart.** The worker process that accepted the update is
  terminated and a fresh process takes over the task queue. Queries, the
  release, and completion all happen in the replacement.
- **Control.** The same start, update, release, and finish, with no queries,
  rejected updates, eviction, or replacement.

A `probe` query returns the counter together with two counters kept by the
worker process: how many times the workflow body started and how many times
the validator ran. They prove that the answers came from a replay (the body
started again, or started once in a fresh process) and that replaying an
accepted update did not re-run its validator, while live requests still do.

Every assertion uses server state: query answers, update outcomes, and each
exact run's history read through the official CLI. Histories must show the
update accepted but not completed while it is suspended, then exactly one
acceptance and one completion naming the original update ID on the original
run. A handle re-attached with the same update ID after recovery observes
the same result. In the restart run, history attributes the acceptance to the
original worker process and the completion to the replacement. Finally, the
durable events of both recovered runs, excluding workflow-task bookkeeping,
must equal the control run's, so queries and rejected updates did not change
later commands. Waits are bounded polls on these observations rather than
fixed sleeps.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace and the pinned
admin-tools CLI.

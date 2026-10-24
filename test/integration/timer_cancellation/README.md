# Timer cancellation regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
dune exec test/integration/timer_cancellation/regression.exe -- \
  check http://127.0.0.1:7233 /path/to/temporal
```

The second argument is the official Temporal CLI, used to read each run's
history. The test starts and reaps its own worker on a unique task queue and
checks durable-timer cancellation (#1134) on a live server:

- `satisfied`: `Condition.wait_until ~timeout` (60 s) is satisfied by a
  signal, so the wait succeeds and history records one `TIMER_STARTED` and
  one `TIMER_CANCELED`.
- `timeout`: the same wait with a 2 s bound is never satisfied, so it
  reports a `` `Timeout `` error and history records `TIMER_FIRED` and no
  cancellation.
- `scope`: `Workflow.sleep ~scope` (60 s) is cancelled by a signal handler
  cancelling the scope, so the sleep returns `` `Cancelled `` and history
  records `TIMER_CANCELED`.

Each workflow records its outcome and waits for a `finish` signal. The
outcome is read with a query, the worker process is replaced so a fresh
worker must replay the history (including the cancel or the firing), the
outcome is read again and must be identical, and the workflow then
completes with it. The test terminates every workflow it creates, including
after an assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes, passing the pinned admin-tools CLI.

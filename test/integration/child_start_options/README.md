# Child start options regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
dune exec test/integration/child_start_options/regression.exe -- \
  check http://127.0.0.1:7233
```

The test starts and reaps its own worker on a unique task queue. It checks
that the options a parent passes to `Child_workflow` (#1135) reach Temporal:

- A child started with a 60 s execution, 30 s run, and 7 s task timeout
  reads those values back through `Workflow.info`. The parent records the
  child's report and exposes it through a query; the worker process is then
  replaced, so the fresh worker must replay the parent's history (including
  the start-child command carrying the options), and the report must be read
  back identically.
- A child that never finishes, started with a 3 s run timeout, is ended by
  Temporal. The parent observes a failed child (an `Error.kind` of
  `child_workflow`; a typed timeout kind is #1143's subject), and an
  exact-run `Client.wait` on the child reports `Timed_out`.
- After a child completes, starting another child with the same workflow ID
  is refused under `` `Reject_duplicate `` (its `started` future fails) and
  allowed under `` `Allow_duplicate ``.

Each parent waits for a `finish` signal after recording its outcome, so the
outcome is read before and after the worker replacement and must then equal
the parent's completed result. The test terminates every workflow it
creates, including after an assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes.

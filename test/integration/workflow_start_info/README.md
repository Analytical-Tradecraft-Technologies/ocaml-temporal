# Workflow start info regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
dune exec test/integration/workflow_start_info/regression.exe -- \
  check http://127.0.0.1:7233
```

The test starts and reaps its own worker on a unique task queue. It checks
that workflow code reads back, through `Workflow.info` and
`Workflow.start_metadata`, the start policies `Client.start` sent (#512):

- A run started without options reports attempt 1, itself as the chain
  origin, no predecessor, no execution or run timeout, Temporal's recorded
  10 second default task timeout, no retry policy, and no memo.
- A run started with execution, run, and task timeouts, a retry policy, and
  a memo fails its first attempt. The retry run reports attempt 2, the failed
  run as predecessor and chain origin, the `` `Retry `` initiator, and every
  value the client set, with the retry policy's exact backoff coefficient.
- A run started with the same options continues as new. The successor
  reports the `` `Continue_as_new `` initiator and the same timeouts, retry
  policy, and memo, which Temporal carries over because the SDK's
  continue-as-new command overrides none of them.

For each reporting run the report is read with a query, the worker process
is replaced so the fresh worker must replay the run's history, and the
report is read again and must be identical. The run then completes with the
same report. The test terminates every workflow it creates, including after
an assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. Search-attribute and memo combinations, and starts from the
official Temporal CLI, are covered by `make test-temporal-start-metadata-live`.

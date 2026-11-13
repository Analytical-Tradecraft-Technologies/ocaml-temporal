# Cron schedule and start delay regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
_build/default/test/integration/cron_start_delay/regression.exe check http://127.0.0.1:7233 "$(command -v temporal)"
```

The second argument is the official Temporal CLI, used to start one cron
chain with `temporal workflow start --cron`. The test starts and reaps one
worker on a unique task queue and covers cron schedules and delayed starts
(#1158):

- `Client.start ~start_delay` (3 s): the run completes, workflow code reads a
  3,000 ms `Workflow.Info.first_workflow_task_backoff` and no cron schedule
  or predecessor, and its first `Workflow.now` is at least 3 s after
  `Workflow.Info.start_time`.
- `Client.signal_with_start ~start_delay` (3 s): the same, with the signal
  delivered in the delayed first activation.
- `Client.start ~cron_schedule:"@every 2s"`: the first run reports the
  schedule, a first-task backoff and no predecessor; its exact-run wait is
  `Completed` with the next run as successor; that second run reports the
  schedule, `` `Cron_schedule `` and the first run as its predecessor. A
  current-run `Client.wait ~timeout:5 s` on the chain ends with
  `Deadline_exceeded`, because the schedule keeps starting runs.
- A cron chain started by the official CLI, checked the same way.
- An invalid cron expression is rejected by Temporal with an
  `` `Invalid_argument `` RPC status.
- The histories of the delayed run and of both client cron runs, fetched
  with `Client.fetch_history`, replay against the workflow.

Each observation is printed to the log. The test terminates every chain it
starts, including after an assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace and the pinned CLI.

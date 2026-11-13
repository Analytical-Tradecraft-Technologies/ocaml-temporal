# Continue-as-new options regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
_build/default/test/integration/continue_as_new_options/regression.exe check http://127.0.0.1:7233 "$(command -v temporal)"
```

The second argument is the official Temporal CLI, used only to register the
`ContinueAsNewOptionsKeyword` search attribute in `default` when it is not
registered yet. The test starts and reaps one worker on each of two unique
task queues, A and B, and covers the successor options of
`Workflow.continue_as_new` (#1153):

- A run started on queue A with the memo `kept` continues as new with
  `~task_queue:B`, a 60 s run timeout, a 15 s task timeout, a five-attempt
  retry policy, the replacement memo `note` and the keyword search attribute.
- The successor is executed by the worker on queue B. A query reads back its
  `Workflow.Info` task queue, run and task timeouts and retry policy, and its
  `Workflow.start_metadata` memo and keyword; `Client.describe` reports queue
  B, the memo `note` alone and the keyword.
- After a signal the successor continues as new without options, and the
  next run reports the same queue, timeouts, retry policy, memo and keyword:
  each one is inherited.
- After another signal that run continues as new with a 2 s run timeout and a
  single-attempt retry policy. The last run waits forever, so it ends as
  `Timed_out` with no retry successor.
- The histories of the runs that continued with options, without options,
  and with only a run timeout and retry policy, fetched with
  `Client.fetch_history`, replay against the workflow.

The test terminates its workflow, including after an assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace and the pinned CLI.

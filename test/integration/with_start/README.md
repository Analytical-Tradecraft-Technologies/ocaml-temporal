# Signal-with-start and update-with-start regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
_build/default/test/integration/with_start/regression.exe check http://127.0.0.1:7233 "$(command -v temporal)"
```

The second argument is the official Temporal CLI, used only to read run
histories. The test starts and reaps its own worker on a unique task queue,
whose entity workflow records every signal and accepted update it receives and
completes with them, comma-separated, once signalled to finish. It covers
`Client.signal_with_start` and `Client.update_with_start` (#1151):

- `signal_with_start` on an absent workflow starts a run (`Client.started` is
  `true`) whose history records the signal before its first workflow task
  starts, so the first activation delivers it. While that run is open, a
  second call only signals it: same exact run ID, `started` is `false`. The run
  completes with both signals.
- After the run closed, the same call starts another run and again delivers the
  signal before its first task. `` `Terminate_existing `` then terminates that
  open run and starts and signals a new one. Two calls with the same request ID
  deliver their signal once.
- `update_with_start` starts a run and its accepted update completes; a second
  call on the open run is admitted there (same run ID, `started` is `false`).
  A validator rejection is returned beside the run handle, both on the open run
  and for an absent workflow, whose run was still started. A `` `Fail `` call
  on the open run is the typed already-started error naming that run.

The test terminates its workflows, including after an assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace and the pinned CLI.

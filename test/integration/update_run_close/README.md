# Update callers when the run closes

Issue #520. Against a disposable Temporal server with a `default` namespace:

```sh
make test-update-run-close-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/absolute/path/to/temporal
```

The driver spawns and reaps its own worker processes on unique task queues.
The workflow waits for a `finish` signal and registers a `hold` update whose
handler, once accepted, never finishes. Each case closes the run while the
update is unfinished and asserts the exact typed result of every caller and
that each returns within 10 seconds.

- **Accepted, then completed / cancelled / terminated.** After `start_update`
  returns an accepted handle, one `wait_update` is started on a separate
  thread and the run is closed by the `finish` signal, `Client.cancel`, or
  `Client.terminate`. `Client.wait` must report `Completed`, `Cancelled` or
  `Terminated`. The in-flight caller, a later `wait_update` on the same
  handle, and a `start_update` that re-attaches with the same update ID must
  all return the server's non-retryable `Update` failure of type
  `AcceptedUpdateCompletedWorkflow`. History must show the acceptance and no
  completion.
- **Admitted, then terminated.** The run's only worker is stopped after its
  first workflow task. A `start_update` on a separate thread is then admitted
  but cannot be accepted, which the driver observes as the speculative
  workflow task that `describe` reports. The run is terminated. That caller
  and a later `start_update` with the same update ID must return the
  non-retryable `Not_found` RPC error. History must have no update event.

[Update callers when the run closes](../../../docs/reference/interactive-workflows.md#update-callers-when-the-run-closes)
records the semantics and why each result has its shape.

Every assertion uses client results or server state read through the
official CLI; waits are bounded polls, never fixed sleeps. A process alarm
bounds the whole driver, so a caller that never returns fails the suite
instead of hanging it.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace and the pinned
admin-tools CLI.

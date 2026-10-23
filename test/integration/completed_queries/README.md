# Completed workflow query regression

Against a disposable Temporal development server:

```sh
make test-completed-queries-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233
```

The driver creates a unique task queue and a separate worker process, completes
a workflow, then queries its final workflow-local value repeatedly. It also
checks that an unknown query and a handler that returns an error are both
reported as the typed, non-retryable query failure (`Client.is_query_failed`,
keeping the handler message), that a signal to the completed run is a
permanent `` `Not_found `` RPC error, and that none of this invalidates later
queries.
It replaces the worker and repeats the successful queries, proving a fresh process can replay a
completed execution and answer from its reconstructed final state. All worker
processes are stopped and reaped by the fixture.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace.

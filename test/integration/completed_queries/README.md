# Completed workflow query regression

Against a disposable Temporal development server:

```sh
make test-completed-queries-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233
```

The driver creates a unique task queue and a separate worker process, completes
a workflow, then queries its final workflow-local value repeatedly. It also
checks an unknown query returns an error without invalidating later queries.
It replaces the worker and repeats the successful queries, proving a fresh process can replay a
completed execution and answer from its reconstructed final state. All worker
processes are stopped and reaped by the fixture.

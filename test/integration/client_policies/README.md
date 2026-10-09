# Client execution policies and RPC deadlines regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
make test-client-policies-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/path/to/temporal
```

The test starts and reaps its own worker on a unique task queue and covers
the policies `Client.start` exposes for #499:

- An execution timeout and a run timeout each end a blocked run as
  `Timed_out`. The official CLI's `workflow describe` output shows the
  recorded execution, run, and task timeouts, so the values reached the
  server.
- While that run is open, a query with a 1 ms `?rpc_timeout` fails with
  `Client.rpc_status` `` Some `Deadline_exceeded ``, and the same query with
  the default deadline then succeeds. The RPC deadline bounds the call, not
  the workflow.
- Workflow ID reuse: after a successful completion, `` `Reject_duplicate ``
  and `` `Allow_duplicate_failed_only `` refuse the ID with the typed
  already-started error naming the closed run, and the default reuses it.
  After a failure, `` `Reject_duplicate `` still refuses the ID and
  `` `Allow_duplicate_failed_only `` reuses it. `` `Use_existing `` with
  `` `Reject_duplicate `` attaches to an open run.
- A workflow retry policy retries a failing run once. An exact-run wait on
  the first run returns `Failed` with its attempt-1 error and the retry run as
  its successor; `Continued_as_new` fails the test (#971). The retry run
  reports attempt 2 and ends the chain.
- The same retry policy retries a run that hit its run timeout. The first
  run's exact-run wait returns `Timed_out` with the retry run as its
  successor, and the retry run's own timeout ends the chain.
- An explicit continue-as-new is still reported as `Continued_as_new`, and
  the successor run completes with the input it was continued with.
- Cron schedules cannot be started through this client, so a cron
  `Completed` successor is covered by the Rust and OCaml unit tests rather
  than here.
- A start with a 1 ms `?rpc_timeout` is accepted, reported as uncertain
  (`Client.is_start_outcome_uncertain`) when it expired after being sent, or
  rejected with `` `Deadline_exceeded `` when it expired before being sent.
  Retrying it with the same request ID returns a started run, and a separate
  start then finds exactly that run open. A signal with an expired deadline
  keeps its typed RPC status.

The test terminates every workflow it creates, including after an assertion
failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace and the pinned
admin-tools CLI.

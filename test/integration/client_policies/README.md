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
- A workflow retry policy retries a failing run once. The retry run reports
  attempt 2 and ends the chain. The pinned server reports the first run's
  link to this client as continue-as-new rather than as a failure with a
  successor, so the test accepts both forms.
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

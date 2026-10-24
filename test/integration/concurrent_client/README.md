# Concurrent client calls regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
make test-concurrent-client-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233
```

The test starts and reaps its own workers on a unique task queue and covers
#807, where one `Client.t` served every call on its supervisor Domain one at
a time and that Domain waited for the network inside each call:

- 24 `Client.wait` calls on eight open workflows stay pending on other
  threads, and a query with an 8 s `?rpc_timeout` is stuck on a workflow
  whose worker was killed after it answered once. Meanwhile ten signals and
  ten starts on the same client must each finish within 1.5 s. Before #807
  they queued behind 100 ms per pending wait per turn (about 2.4 s here) and
  behind the stuck query for up to its whole deadline.
- None of the long calls finishes early. Terminating the workflows releases
  every wait with `Terminated`, and the stuck query ends by itself with an
  error.
- Shutting down a second client while a `Client.wait` is in flight on it
  returns promptly, and the wait ends with the closed-client error instead of
  staying blocked.
- A third client created with `~max_pending_waits:2` (#796) keeps two waits
  pending and refuses a wait on a third distinct run at once with an error
  recognized by `Client.is_at_capacity`; shutting it down ends the two
  admitted waits.
- A fourth client created with `~max_pending_waits:1` checks
  `Client.wait ?timeout` (#1136): a 1 s bounded wait on an open run returns
  a retryable error with `Client.rpc_status` `` Some `Deadline_exceeded ``
  after about a second; the released slot then admits a wait on a second
  run instead of refusing it as at capacity; after termination a later
  bounded wait on the first handle reports `Terminated`; and shutting the
  client down during a 30 s bounded wait ends it promptly with the
  closed-client error.
- A fifth client created with `~max_pending_waits:1` starts waits of 1 s,
  3 s and 6 s on the same open run at once; they share that run's slot. Each
  expires at its own bound with `Deadline_exceeded` (the 6 s wait is still
  pending after the 3 s one expires). Once all three have expired, a wait on
  a second run is admitted rather than refused as at capacity, which shows no
  expired waiter kept a share of the first run's slot. Terminating the second
  run ends that wait with `Terminated`.
- A `hold` update that the worker accepts at once but completes only after a
  `release-hold` signal checks `Client.wait_update ?timeout`: a 1 s bounded
  wait on the accepted update expires with the same retryable error, and
  after the signal later bounded waits on the same update handle return its
  result.

The test terminates every workflow it creates, including after an assertion
failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace.

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

The test terminates every workflow it creates, including after an assertion
failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace.

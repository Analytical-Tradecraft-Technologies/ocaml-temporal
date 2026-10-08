# Transport interruption regression

This suite injects transport faults deterministically (#504). Two in-process
TCP proxies (`fault_proxy.ml`) sit in front of the server, one for a client
and one for a worker. A direct client reconciles the durable outcome after
each fault. The scenarios, their required typed results, and the remaining
#504 scope are described in
[transport fault qualification](../../../docs/reference/transport-fault-qualification.md).

Start a disposable Temporal server that has the `default` namespace. Then run
the suite with an official Temporal CLI path, which the activity scenario uses
to read its own history:

```sh
make test-transport-interruption-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/path/to/temporal
```

The suite runs its worker on a dedicated Domain and uses a unique task queue.
It terminates every workflow it creates, including after an assertion fails.
It fails if a proxied socket is still open after the client and the worker
have shut down. Every injection and observed result is logged as a
`transport-fault +<seconds>s` line.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. The suite usually takes under a minute.

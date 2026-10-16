# Transport interruption regression

This suite injects transport faults deterministically (#504). In-process TCP
proxies (`fault_proxy.ml`) sit in front of the server: one for a client, one
for the main worker, and one for a worker that is shut down during an outage. A direct client reconciles the durable outcome after
each fault. The scenarios, their required typed results, and the remaining
#504 scope are described in
[transport fault qualification](../../../docs/reference/transport-fault-qualification.md).

Start a disposable Temporal server that has the `default` namespace. Then run
the suite with an official Temporal CLI path, which the activity and
workflow-task scenarios use to read their own histories:

```sh
make test-transport-interruption-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/path/to/temporal
```

The suite runs its workers on dedicated Domains and uses unique task queues.
It terminates every workflow it creates, including after an assertion fails.
It fails if a proxied socket is still open after the client and the worker
have shut down. Every injection and observed result is logged as a
`transport-fault +<seconds>s` line.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. The suite usually takes about a minute.

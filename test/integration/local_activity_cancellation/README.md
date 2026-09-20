# Local activity retry cancellation

Against a disposable Temporal server, with the official CLI explicitly selected:

```sh
make test-local-activity-cancellation-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/path/to/temporal
```

The driver creates a unique queue and one execution for each cancellation
policy. A retryable local activity failure requests a 120-second delay. After
the driver observes the durable timer in history, a signal cancels the original
operation twice. The fixture requires the original future to resolve as
cancelled, the workflow to complete, exactly one local activity marker, and
cancellation of that exact timer. No timer may fire or workflow task fail.

It replaces the worker process and queries all three completed executions,
requiring Core to replay their histories and reconstruct the same cancelled
state. An activity-side attempt log must still contain exactly one invocation
per execution after replay. The driver terminates its remaining executions and
stops/reaps its workers on exit; an overall timeout bounds failed runs.

The fixture uses the private runtime scheduling handle because the public local
activity API currently exposes only a future. It does not introduce a public
cancellation API. The client, worker, signal, and query paths use the public SDK.
Deterministic runtime tests separately exercise cancellation before backoff
delivery, during backoff, after timer firing, and before a retry's later backoff
under all three policies in live and replay contexts, including repeated calls
and stale timer rejection.

Verified on 2026-09-21 with OCaml 5.4.1, pinned Core
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`, and Temporal CLI 1.9.1 / Server
1.32.0 using a native SQLite development server. All three histories contained
one `core_local_activity` marker, one retry timer, cancellation of that timer,
and workflow completion. Fresh-worker queries reconstructed the cancelled
result without another activity invocation. This is focused native evidence;
the Docker/PostgreSQL and platform matrices remain separate gates.

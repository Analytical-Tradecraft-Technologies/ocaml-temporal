# Client request ID regression

Start a disposable Temporal development server, then run:

```sh
make test-client-request-ids-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233
```

The test starts and reaps its own worker on a unique task queue. It checks
conflicting starts from two native clients, the three workflow ID conflict
policies (`` `Fail `` with its typed existing run, `` `Use_existing ``, and
`` `Terminate_existing ``, including request-ID deduplication taking
precedence), signals and updates from separate client processes, and
intentional deduplication of explicitly supplied IDs. It also addresses
workflows by ID alone (`Client.get_handle` without a run ID, #791): another
process signals, updates, and queries a running workflow by its workflow ID,
and a current-run handle signals and queries a workflow across its
continue-as-new while its `wait` follows the chain to the final run.
It terminates all created workflows, including after an assertion failure.
Use a test server with the `default` namespace; no other services are required.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace.

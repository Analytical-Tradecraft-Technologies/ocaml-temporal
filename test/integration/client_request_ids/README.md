# Client request ID regression

Start a disposable Temporal development server, then run:

```sh
make test-client-request-ids-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233
```

The test starts and reaps its own worker on a unique task queue. It checks
conflicting starts from two native clients, signals and updates from separate
client processes, and intentional deduplication of explicitly supplied IDs.
It terminates all created workflows, including after an assertion failure.
Use a test server with the `default` namespace; no other services are required.

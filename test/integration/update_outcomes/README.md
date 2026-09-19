# Update admission outcome regression

Against a disposable Temporal development server:

```sh
make test-update-outcomes-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233 \
  TEMPORAL_TEST_CLI=/absolute/path/to/temporal
```

The fixture creates and reaps its worker and uses one unique workflow in the
`default` namespace. It checks validator rejection details, unknown handlers,
input decoding rejection, and a signal-controlled suspended update.

It then deletes **only its own exact workflow execution** using the official
CLI. Once an uncached update handle proves the server record is unavailable,
a handle whose admission response already contained the successful outcome
must still return that result, repeatedly, without another server lookup.

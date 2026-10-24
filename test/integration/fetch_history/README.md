# Fetch history regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
_build/default/test/integration/fetch_history/regression.exe check http://127.0.0.1:7233
```

The test starts and reaps its own worker on a unique task queue and covers
`Client.fetch_history` (#1137), which lets an application replay a run with
only `Temporal.Client` and `Temporal.Replay`, with no CLI export or protobuf
conversion:

- The fixture records three 10 ms durable timers, waits for a `finish`
  signal, records three more and completes.
- While the run is open, after the first three timers, the fetched history
  replays against the fixture and also against a variant that changes only
  the code after `finish`.
- After completion, the history fetched with the server's default page size,
  with one event per page and with three events per page replays against
  the fixture, and each fails against that variant with `Nondeterminism`.
  This proves each fetch includes its last pages, which a replay alone
  cannot show because a truncated prefix still replays.
- A current-run handle from `Client.get_handle` fetches the same complete
  history.
- An unknown workflow ID is an error with `Client.rpc_status`
  `` Some `Not_found ``.

The test terminates its workflow, including after an assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace.

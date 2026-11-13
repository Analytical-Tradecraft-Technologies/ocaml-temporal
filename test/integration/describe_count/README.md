# Describe and count regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
_build/default/test/integration/describe_count/regression.exe check http://127.0.0.1:7233 "$(command -v temporal)"
```

The second argument is the official Temporal CLI, used only to register the
`DescribeCountKeyword` Keyword search attribute in `default` when it is not
registered yet. The test starts and reaps its own worker on a unique task
queue and covers `Client.describe` and `Client.count` (#1152):

- A run started with a memo and that search attribute is described while it
  is open, through its exact handle and a current-run handle from
  `Client.get_handle`: the identity, workflow type, task queue, `` `Running ``
  status, start time, a non-empty history, the memo (whole payloads) and the
  search attribute (payload data; the server adds a `type` metadata entry)
  must match what `Client.start` sent, with no close time or pending work.
- After a signal lets it complete, the run is `` `Completed `` with a close
  time no earlier than its start and a longer history.
- An unknown workflow ID and an unknown run of a known workflow are errors
  with `Client.rpc_status` `` Some `Not_found ``.
- Two more runs are seeded on the same queue and one is terminated. Once
  visibility has caught up, `Client.count` must equal the number of rows
  `Client.list_visibility` pages through for `TaskQueue = '<queue>'` (three)
  and for the queue's running runs (one).

The test terminates its workflows, including after an assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace and the pinned CLI.

# Temporal SDK examples

These examples form one small three-process application. They use only the
public `temporal-sdk` library, so they also show the boundary an application
uses after installing the package.

The commands below are a host-execution path for the examples. They assume an
OCaml/Dune toolchain is available on the host; the repository's Docker build
targets compile the executables but do not keep these long-lived application
processes running.

For the pinned local Temporal/PostgreSQL stack, start the server through the
supported Makefile target first:

```sh
make temporal-start
```

That target publishes the Temporal frontend at `127.0.0.1:7233` and creates
the `temporal-sdk-test` namespace if necessary. If the default host port is
already occupied, choose a different port for the stack and use the same port
in `TEMPORAL_ADDRESS` below:

```sh
TEMPORAL_FRONTEND_PORT=17233 make temporal-start
export TEMPORAL_ADDRESS=http://127.0.0.1:17233
```

The examples default to `default` for an externally managed Temporal Server.
When using `make temporal-start`, export the namespace created by that target
before launching any of the three processes:

```sh
export TEMPORAL_NAMESPACE=temporal-sdk-test
export TEMPORAL_TASK_QUEUE=ocaml-temporal-example
```

In separate terminals, run the programs in this order. The same environment
must be visible in all three terminals:

```sh
dune exec examples/activity_worker/activity_worker.exe
dune exec examples/workflow_worker/workflow_worker.exe
dune exec examples/client/client.exe -- "Ada Lovelace"
```

The `make build-examples` (or `make native-build` on a native host) target is
useful for
compilation checks, but it does not replace the three `dune exec` processes.
This example path is separate from `make test-temporal-integration`, whose
dedicated smoke worker and driver provide the repository's automated live
acceptance evidence.

The activity worker turns two requested message styles into text. The workflow
worker concurrently schedules those activities, records a short durable timer,
and returns the combined message. Both workers share one task queue, which is
safe because a worker polls only the task kinds it registers: the workflow
worker (`~activities:[]`) never takes activity tasks, and the activity worker
(`~workflows:[]`) never takes workflow tasks. A worker must register at least
one workflow or activity. The client starts one execution, waits for
its exact run, and prints the completed value.

All three programs read the same optional environment variables:

- `TEMPORAL_ADDRESS` (default: `http://127.0.0.1:7233`)
- `TEMPORAL_NAMESPACE` (default: `default`)
- `TEMPORAL_TASK_QUEUE` (default: `ocaml-temporal-example`)

The client also accepts an optional name argument and `TEMPORAL_WORKFLOW_ID`.
Set a unique workflow ID when deliberately re-running an execution in a shared
namespace. The worker programs handle `SIGINT` and `SIGTERM` by requesting the
public graceful shutdown operation before their processes exit.

The sample client waits for the exact run returned by `Client.start`; it does
not automatically follow a continued-as-new successor. The current sample
workflow does not continue as new, but a modified workflow that does will make
the client print the successor identity and exit with an error by design. See
the [workflow guide](../docs/guides/workflows.md#continue-a-run-with-fresh-history)
for the explicit `Client.follow` path. Stop the local infrastructure after the
example run with `make temporal-stop`; use `make temporal-clean` when the
PostgreSQL volume and its workflow history should also be removed.

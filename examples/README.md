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

`make test-temporal-examples-live` runs this exact application in CI: it
compiles the three executables, starts the Compose Temporal/PostgreSQL stack,
starts both workers as separate containers on a fresh task queue, runs the
client, and requires the documented output below. It also checks that a blank
name fails the workflow with `a name is required`, and that both workers exit
cleanly after `SIGTERM`. It always removes the stack afterwards. A new example
executable must be added to that target;
`test/smoke/test_examples_live_contract.sh` fails until it is.

```text
Workflow completed:
Hello, Ada Lovelace!
Next: review the Temporal result for Ada Lovelace.
```

`examples/testing/example_workflow_test.ml` unit-tests the same workflow
in-process with `Temporal.Testing`, once with the real activity and once with
a stub, and needs no Temporal Server. It runs with the other unit tests
(`dune test`).

The broader SDK acceptance evidence comes from `make test-temporal-integration`,
whose dedicated smoke worker and driver exercise many more Temporal features.

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

## Replay recorded histories before deploying

`examples/replay` is an offline compatibility check to run before deploying
changed workflow code. It replays recorded workflow histories against the
workflow code linked into the executable and reports whether that code still
produces the commands each original execution recorded. It needs no Temporal
Server, network access, or production credentials.

Replay runs the application's own workflow functions and codecs, so there is
no generic replay binary: each application builds a small executable that
links its workflow definitions. The example has two parts:

- [`replay_command.ml`](replay/replay_command.ml): the reusable command. It
  parses arguments, reads each history file, calls `Temporal.Replay.replay`,
  prints one line per history and returns the exit status. Copy it unchanged.
- [`replay_history.ml`](replay/replay_history.ml): the application-specific
  entry point. It passes the example workflow's registration to
  `Replay_command.main`. In your application, list every workflow, with the
  same signal, query and update handlers, that your production worker
  registers. Activities are not listed: replay never runs them, and their
  recorded results come from the history.

Run it on the checked-in history of one example execution:

```sh
dune exec examples/replay/replay_history.exe -- \
  --workflow-id compose-message-ada-lovelace \
  examples/replay/histories/compose-message-ada-lovelace.pb
```

```text
PASS examples/replay/histories/compose-message-ada-lovelace.pb workflow_id=compose-message-ada-lovelace
replayed 1 history: 1 passed, 0 failed
```

Pass several files to check several executions in one run. Each file uses the
most recent `--workflow-id` before it, because the History protobuf does not
record the workflow ID:

```sh
replay_history.exe --workflow-id order-1 order-1.pb \
  --workflow-id order-2 order-2.pb
```

Every history is replayed and printed, as `PASS FILE workflow_id=ID` or
`FAIL FILE workflow_id=ID: DIAGNOSTIC`, followed by a summary line. The
diagnostic starts with the failure kind from `Temporal.Replay.failure_message`.
The process exits with the status of the first failing history in
command-line order:

| Exit status | Meaning |
| --- | --- |
| 0 | Every history replayed cleanly. |
| 1 | Nondeterminism: the code produces different commands from the recorded history, for example a removed timer. Do not deploy it without a [patch marker](../docs/guides/workflows.md#introduce-a-new-branch-with-a-patch-marker). |
| 2 | Workflow task failed: the workflow raised, returned a defect, could not decode its input, or its type is not registered. |
| 3 | Invalid history: the file is empty, too large, or not a binary `History` protobuf. |
| 4 | Unsupported history: the history uses a feature this SDK cannot replay yet. |
| 5 | The replay could not run (an invalid registration list or an SDK runtime failure); the verdict is unknown. |
| 64 | Usage error. |
| 66 | A history file could not be read. |

Optional `--namespace` and `--task-queue` values are only reported to workflow
code through `Temporal.Workflow.info`. Pass the original worker's values when
the workflow reads them.

### Export a history

The command reads the binary encoding of Temporal's
`temporal.api.history.v1.History` message. The Temporal CLI exports the
protobuf **JSON** encoding, so convert it once after exporting:

```sh
temporal workflow show --namespace my-namespace \
  --workflow-id order-1 --output json > order-1.json
```

The conversion needs a protobuf library and the Temporal API definitions
(MIT-licensed, from `https://github.com/temporalio/api`). With `protoc` and
Python's `protobuf` package:

```sh
git clone --depth 1 https://github.com/temporalio/api.git temporal-api
protoc -I temporal-api --include_imports --descriptor_set_out=history.desc \
  temporal-api/temporal/api/history/v1/message.proto
python3 - history.desc order-1.json order-1.pb <<'EOF'
import sys
from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory

descriptor_path, json_path, protobuf_path = sys.argv[1:]
files = descriptor_pb2.FileDescriptorSet()
with open(descriptor_path, "rb") as source:
    files.ParseFromString(source.read())
pool = descriptor_pool.DescriptorPool()
for file in files.file:
    pool.Add(file)
History = message_factory.GetMessageClass(
    pool.FindMessageTypeByName("temporal.api.history.v1.History"))
with open(json_path) as source:
    history = json_format.Parse(source.read(), History())
with open(protobuf_path, "wb") as target:
    target.write(history.SerializeToString())
EOF
```

Any other protobuf JSON-to-binary converter with the same message definitions
works too. Programs that already call the `GetWorkflowExecutionHistory` gRPC
API receive the binary form directly. The export needs read access to the
namespace; replaying the exported file does not.

Exported histories contain workflow inputs, results and signal payloads.
Treat them as production data, and prefer synthetic or scrubbed executions for
histories committed to a repository.

### Run it in CI

Keep a directory of exported histories next to the workflow code and run the
replay executable in the pipeline that builds a release candidate, failing the
job on any non-zero status. Add a history whenever a workflow gains a new
code path worth protecting, and keep histories of executions that may still
be running when the new code is deployed. A passing replay covers only the
paths those histories took, and it compares command shapes, not payload
values.

This repository does the same: `dune test` replays the checked-in example
history with `replay_history.exe`, and
[`test/replay_cli`](../test/replay_cli/test_replay_cli.ml) runs the command
against deliberately changed workflow code to check the nondeterminism,
task-failure, invalid-input and usage exit statuses. The history was recorded
from one run of the three example processes above (task queue
`ocaml-temporal-example`, workflow ID `compose-message-ada-lovelace`) and
converted with the steps above; `compose-message-ada-lovelace.json` is the
CLI export it was converted from. If you change the example workflow's
commands, this check fails as intended: guard the change with a patch marker
instead of re-recording the history.

The replay example is an offline `test` stanza rather than a long-lived
`executable`, so it is deliberately outside the live example gate described
above.

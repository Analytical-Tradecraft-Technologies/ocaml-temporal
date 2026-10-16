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

The client starts the workflow with a stable request ID, a one-hour workflow
execution timeout, and a five-second deadline on the start call. If that
deadline expires, `Client.is_start_outcome_uncertain` recognizes the error
and the client retries once with the same request ID. Temporal then returns
the run if the first attempt created it, or creates it exactly once. This is
the safe way to reconcile an uncertain start: never retry with a new request
ID.

The `make build-examples` (or `make native-build` on a native host) target is
useful for
compilation checks, but it does not replace the three `dune exec` processes.

`make test-temporal-examples-live` runs this exact application in CI: it
compiles the example executables, starts the Compose Temporal/PostgreSQL stack,
starts both workers as separate containers on a fresh task queue, runs the
client, and requires the documented output below. It also checks that a blank
name fails the workflow with `a name is required`, and that both workers exit
cleanly after `SIGTERM`. It then runs the
[cooperative cancellation example](#cancel-activities-cooperatively) and
requires its exact report. It always removes the stack afterwards. A new
example executable must be added to that target;
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

## Cancel activities cooperatively

`examples/cancellation` shows how an activity stops early without leaving
external state behind. The worker never interrupts activity code: an activity
finds out that it should stop only when it checks, so a long-running activity
must heartbeat and look at the answer.

Each activity in [`cooperative_cleanup.ml`](cancellation/cooperative_cleanup.ml)
holds a reservation in a small in-memory ledger while it works. The ledger
stands in for an external service such as a database or a lock service. The
reservation key is the workflow ID, run ID and activity ID, so every attempt
of the same scheduled activity uses the same key. An idempotency key should
name the business operation: retries of one activity share it, while a new
run that reuses a closed run's workflow ID (which `Client.start` allows by
default) is a new operation and gets a new key. Leaving out the run ID would
let that new run find the old run's committed reservation and report success
without doing any work. When work must happen at most once across runs, key
it by a business identifier in the activity input instead. Every ledger
operation is idempotent: acquiring a held key keeps the hold, releasing a key that is not
held does nothing, and neither undoes a commit. That matters because Temporal
delivers an activity at least once. A retry, or a redelivery after a worker
crash, can repeat any step, and a redelivered attempt that finds its
reservation already committed reports success without doing the work again.

- **Synchronous activity** (`hold_reservation`, from
  `Activity.define_with_context`). Before each step it checks
  `Activity.Context.is_worker_shutting_down`, then calls
  `Activity.Context.heartbeat`. A heartbeat returns a `` `Cancelled `` error
  once Temporal has asked the attempt to stop, and
  `Activity.Context.cancellation` then tells it why. On every early exit the
  activity releases its reservation first and then returns:
  - the heartbeat's `` `Cancelled `` error for a requested cancellation, so
    Temporal records the attempt as cancelled;
  - a retryable error when its worker is stopping, so Temporal runs another
    attempt, which resumes from the step in the last heartbeat details;
  - the heartbeat's error for an operator pause or reset, which the worker
    reports as a retryable `ActivityPause` or `ActivityReset` failure.
- **Asynchronous activity** (`hold_reservation_async`, from
  `Activity.define_async`). The callback acquires the reservation and hands
  the attempt to a thread, which heartbeats with `Activity.Async_handle.heartbeat`.
  An asynchronous activity has no context to poll; the heartbeat result is its
  cancellation signal. A non-retryable `` `Cancelled `` error is a requested
  cancellation: the thread releases the reservation and then calls
  `Async_handle.cancel`. A retryable `` `Cancelled `` error is a pause or
  reset: it releases the reservation and calls `Async_handle.fail` with that
  error, because Temporal accepts a cancelled completion only after a
  cancellation request.

  Only the synchronous form can clean up when its worker stops. An
  asynchronous handle is not notified: a worker shutdown waits for it for the
  graceful shutdown period and then closes it as a lost completion, and a
  handle that is never finished expires once Temporal must have timed its
  attempt out (#1043). Its later calls return a non-retryable error, at which
  point this example's completer releases the reservation, and Temporal
  retries the attempt after its heartbeat or start-to-close timeout.

The `reserve-then-cancel` workflow starts one of these activities with
`Activity.start_handle ~cancellation_type:Wait_cancellation_completed`,
waits three seconds, and calls `Activity.cancel`. With that policy the
activity's future resolves only after the activity has acknowledged the
cancellation, which it does after its cleanup, so the workflow could safely
start compensating work at that point. Cancellation reaches an activity only
in the response to a heartbeat, so schedule a cancellable activity with a
`heartbeat_timeout`; here two seconds bounds how long the activity takes to
notice.

[`cancellation.ml`](cancellation/cancellation.ml) is a self-checking driver.
It runs a worker in the same process, so it can read the ledger, and runs
four scenarios:

1. The workflow cancels the synchronous activity. The ledger must show the
   only attempt acquiring the reservation and then releasing it because of
   the cancellation, and the workflow must see the activity as cancelled.
2. The same with the asynchronous activity.
3. The driver shuts the worker down while a `reserve-and-commit` activity
   holds its reservation. The attempt must release it because its worker is
   stopping and fail retryably. The driver then starts a replacement worker,
   whose retry must reacquire the same key and commit it. This uses the
   synchronous activity, the only form that observes its worker stopping.
4. The driver starts a new run under the closed run's workflow ID. Its
   activity must make and commit a reservation of its own, under the new run
   ID, and leave the old run's reservation unchanged.

With the server and environment from the start of this guide:

```sh
dune exec examples/cancellation/cancellation.exe
```

```text
Cancelled activity: released its reservation, then acknowledged the cancellation.
Cancelled asynchronous activity: released its reservation, then acknowledged the cancellation.
Worker shutdown: the activity released its reservation, and a replacement worker retried and committed it.
Reused workflow ID: the new run made and committed its own reservation.
Cooperative cancellation example passed.
```

It exits 1 with a description of the unexpected ledger history or workflow
result if any scenario behaves differently. It uses the task queue
`$TEMPORAL_TASK_QUEUE-cancellation`, so the other example workers never take
its tasks, and fresh workflow IDs, so it can be rerun in the same namespace.
It needs no worker running in another terminal.

An activity that never heartbeats never learns of a cancellation, and one
that ignores the shutdown flag keeps running after the worker's graceful
shutdown period. The worker then abandons it, fails its attempt retryably and
discards its result, so its cleanup must still be safe to repeat in the next
attempt. See the [`Activity.Cancellation`](../lib/public/activity.mli)
documentation for the full contract.

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
most recent `--workflow-id` before it:

```sh
replay_history.exe --workflow-id order-1 order-1.pb \
  --workflow-id order-2 order-2.pb
```

The workflow is replayed under that ID, so it must be the execution's own ID.
Histories from Temporal Server 1.21 or later record the ID in their start
event, and a history whose recorded ID differs from `--workflow-id` fails with
exit status 3 instead of replaying under the wrong identity; for example,
`--workflow-id order-1 order-1.pb order-2.pb` rejects `order-2.pb`. Older
histories do not record the ID, and then `--workflow-id` is used as given.

A `FILE` of `-` reads the history from standard input, and pipes, process
substitution and `/dev/stdin` work like regular files, so an exported history
can be streamed in without a temporary file (use `./-` for a file literally
named `-`):

```sh
cat order-1.pb | replay_history.exe --workflow-id order-1 -
```

Every history is replayed and printed, as `PASS FILE workflow_id=ID` or
`FAIL FILE workflow_id=ID: DIAGNOSTIC`, followed by a summary line. The
diagnostic starts with the failure kind from `Temporal.Replay.failure_message`.
Each record is exactly one line: in `FILE`, `ID`, the diagnostic and usage
errors, a backslash is printed as `\\`, newline, carriage return and tab as
`\n`, `\r` and `\t`, and any other control character as `\xHH`, so a path or
message containing line breaks cannot split a record or imitate another one.
Other text, including UTF-8, is printed unchanged. Output is written in binary
mode, so every line ends in a bare `\n` on Windows as well, and a Windows
path's `\` separators appear as `\\`. The process exits with the status of the first failing history in
command-line order:

| Exit status | Meaning |
| --- | --- |
| 0 | Every history replayed cleanly. |
| 1 | Nondeterminism: the code produces different commands from the recorded history, for example a removed timer. Do not deploy it without a [patch marker](../docs/guides/workflows.md#introduce-a-new-branch-with-a-patch-marker). |
| 2 | Workflow task failed: the workflow raised, returned a defect, could not decode its input, or its type is not registered. |
| 3 | Invalid history: the file is empty, too large, or not a binary `History` protobuf; it records a different workflow ID than `--workflow-id`; or Temporal Core rejects its events (for example a start event without a task queue, missing event IDs, or `WorkflowTaskStarted` events stripped of their `eventTime`). |
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
Python's `protobuf` package, the script below writes `order-1.pb` from one or
more JSON files:

```sh
git clone --depth 1 https://github.com/temporalio/api.git temporal-api
protoc -I temporal-api --include_imports --descriptor_set_out=history.desc \
  temporal-api/temporal/api/history/v1/message.proto
python3 - history.desc order-1.pb order-1.json <<'EOF'
import json
import re
import sys
from google.protobuf import descriptor, descriptor_pb2, descriptor_pool, json_format, message_factory

descriptor_path, protobuf_path, *json_paths = sys.argv[1:]
files = descriptor_pb2.FileDescriptorSet()
with open(descriptor_path, "rb") as source:
    files.ParseFromString(source.read())
pool = descriptor_pool.DescriptorPool()
for file in files.file:
    pool.Add(file)
History = message_factory.GetMessageClass(
    pool.FindMessageTypeByName("temporal.api.history.v1.History"))


def enum_name(value, enum):
    """Maps a legacy PascalCase name such as "WorkflowExecutionStarted" to the
    proto name "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED"; other values are kept."""
    if not isinstance(value, str) or value in enum.values_by_name:
        return value
    prefix = enum.values_by_number[0].name.removesuffix("UNSPECIFIED")
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", value).upper()
    return prefix + snake if prefix + snake in enum.values_by_name else value


def normalize(value, message):
    """Rewrites enum names throughout one JSON object of type `message`."""
    if not isinstance(value, dict):
        return value
    for field in message.fields:
        for key in {field.json_name, field.name} & value.keys():
            value[key] = normalize_field(value[key], field)
    return value


def normalize_field(value, field):
    if field.type == descriptor.FieldDescriptor.TYPE_ENUM:
        if isinstance(value, list):
            return [enum_name(item, field.enum_type) for item in value]
        return enum_name(value, field.enum_type)
    if field.type != descriptor.FieldDescriptor.TYPE_MESSAGE:
        return value
    if field.message_type.GetOptions().map_entry:
        entry = field.message_type.fields_by_name["value"]
        return {key: normalize_field(item, entry) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item, field.message_type) for item in value]
    return normalize(value, field.message_type)


# Each JSON file is the whole history or one page of it; the events of every
# page are appended in the order the files are given.
history = History()
for json_path in json_paths:
    with open(json_path) as source:
        page = normalize(json.load(source), History.DESCRIPTOR)
    history.MergeFrom(json_format.ParseDict(page, History(), ignore_unknown_fields=True))
with open(protobuf_path, "wb") as target:
    target.write(history.SerializeToString())
EOF
```

The script accepts the CLI's output unchanged. It also accepts the older
JSON shape written by the legacy Web UI and `tctl`, whose enum values are
PascalCase names such as `"WorkflowExecutionStarted"` (plain
`json_format.Parse` rejects them as invalid enum values), and it ignores
fields that the API definitions do not know. Any other protobuf JSON-to-binary
converter with the same message definitions and the same handling works too.

The replay must see the **complete** history. The CLI fetches every page
itself. A program that calls the `GetWorkflowExecutionHistory` gRPC API
receives the binary form directly, but each response holds only one page:
keep requesting with the returned `next_page_token` until it is empty, then
concatenate the serialized `history` of every response in order (serialized
`History` messages concatenate into one valid `History`), or pass every JSON
page to the script above. A history cut off after the first page passes
replay's input checks and can report `PASS` while checking only the tasks in
that page. The export needs read access to the namespace; replaying the
exported file does not.

Exported histories contain workflow inputs, results and signal payloads.
Treat them as production data, and prefer synthetic or scrubbed executions for
histories committed to a repository. Scrub payload contents only: keep every
event, its `eventId`, and its `eventTime`, which Temporal Core needs to replay
the history. A scrubber that deletes `eventTime` makes the history invalid
(exit status 3).

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

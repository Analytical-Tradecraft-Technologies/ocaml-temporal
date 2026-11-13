# Official-SDK interoperability regression

This suite (#517) checks that a client and worker written with an official
Temporal SDK interoperate with an OCaml workflow through the default
`json/plain` payload converter. The official SDK is the Temporal Python SDK,
`temporalio` 1.33.0 (MIT), pinned with its whole dependency closure by
SHA-256 digest in [`python/requirements.lock`](python/requirements.lock) and
inventoried in [dependencies](../../../docs/dependencies.md#ci-only-interoperability-tooling).
It runs only in the digest-pinned `python:3.15-slim-bookworm` image as the
`sdk-interop-python` Compose service; the OCaml development image never gains
Python.

Reproduce it against a fresh Compose Temporal/PostgreSQL stack with:

```sh
make test-temporal-live-regressions \
  LIVE_REGRESSION_EXECUTABLES=test/integration/sdk_interop/regression.exe
```

Add `TEMPORAL_COMPOSE_PROJECT=<name>` and a `TEMPORAL_FRONTEND_PORT=<port>`
environment variable to keep it apart from another local stack. CI runs it
with the other suites through `make test-temporal-live-regressions`, which
`make test-temporal-live-ci` invokes. Logs and evidence are written to
`_build/live-regressions/sdk_interop.log` and
`_build/live-regressions/sdk_interop/` (`ready.json`, `python-ready`,
`ocaml-with-start.json`, `python-report.json` and `driver-exit`).

## Topology

[`run-live-regressions.sh`](../temporal/scripts/run-live-regressions.sh)
starts `regression.exe check` in the development image. That process spawns
an OCaml worker for the `interop-order` and `interop-entity` workflows on a
unique task queue and publishes the queue names in `ready.json`. The
controller then runs the [Python driver](python/driver.py), which hosts the
`interop-price` activity and the `interop-py-entity` workflow in official-SDK
workers on two more queues and drives the OCaml workflows with the official
client. Once its workers poll, the driver publishes `python-ready`; the OCaml
process then drives the Python entity with its own client while the driver
runs its cases, and publishes those runs (or its failure) in
`ocaml-with-start.json`, which the driver checks from its side before it
exits. After the driver exits, the controller writes its status to
`driver-exit`, and the OCaml process reopens every exact run the driver
reported through the OCaml client. Both processes must succeed.

The two entity workflows implement one protocol (#1151): a `deliver` signal
and an `apply` update (whose validator rejects `"reject"` with type
`Rejected`, non-retryable, and a JSON detail) append a string, and a
no-argument `finish` signal completes the run with its label, the received
items in order, its run ID and the implementing SDK.

## Tested combinations

| Direction | Interaction | Asserted |
| --- | --- | --- |
| Python client to OCaml workflow | Start with a JSON object: unicode text, integer cents, a boolean, an explicit `null` and an absent optional field | Stored input is `json/plain` and unchanged; the result echoes every value, `null` for both optional spellings |
| OCaml workflow to Python activity | `Activity.execute ~task_queue` with a JSON request | The Python activity receives exactly the OCaml-encoded request; the scheduled input and the Python result are `json/plain` in history; the OCaml result contains the Python result unchanged |
| Python client to OCaml query | `status` while waiting for approval, and an unknown query | Decoded JSON state; the unknown query raises `WorkflowQueryFailedError` |
| Python client to OCaml update | One accepted update; a validator rejection; a `binary/plain` (Python `bytes`) input; a JSON object of the wrong shape | Accepted result `3`; the rejection is an `ApplicationError` of type `InvalidQuantity`, non-retryable, with its JSON detail; both incompatible inputs are rejected as `WorkflowUpdateFailedError`; rejected updates neither change state nor enter history |
| Python client to OCaml signal | A JSON document with 2^53+1, `0.1`, astral-plane text, `null`, booleans, and empty object and list | The workflow result returns it unchanged; the signal input is `json/plain` in history |
| Run identity and versions | Result names `Workflow.Info` run ID; history `WorkflowTaskCompleted.sdk_metadata` | The run ID equals the run the official client started; history records SDK `temporal-ocaml` at the `.release-version` package version |
| Python activity failure to Python client through OCaml | `ApplicationError` type `OutOfStock`, non-retryable, with a JSON detail | The OCaml workflow sees `OutOfStock` and fails with type `PricingFailed`, forwarding the raw details; Python decodes type, non-retryability and the original detail |
| Server timeout of a Python activity, seen by the OCaml workflow (#1143) | The Python activity heartbeats `{"progress": 42}` and outlives the 3 s start-to-close timeout the OCaml workflow set, with one attempt | History records the server's `START_TO_CLOSE` timeout and no completion or failure. The OCaml workflow branches on `Error.timeout_type` (not the message) and fails with type `PricingTimedOut`. Its details are `{"timeout": "start_to_close"}` followed by `Error.last_heartbeat_details`, so the Python client decodes the heartbeat its own activity recorded, kept apart from the failure details. The OCaml client decodes the same |
| Incompatible workflow input | A JSON string instead of the order object | The OCaml worker fails the workflow task and the run stays `RUNNING` (fail-closed decoding); the driver terminates it |
| Python client signal-with-start to OCaml workflow (#1151) | `start_workflow` with `start_signal` on an absent workflow, on its open run, with `TERMINATE_EXISTING`, and after the run closed | A new run whose history records the `json/plain` signal before the first workflow task starts; the open run is only signalled (same run ID); the replaced run is terminated after both signals; the closed workflow starts again. The OCaml results list exactly the delivered signals |
| Python client update-with-start to OCaml workflow (#1151) | `execute_update_with_start_workflow` with `USE_EXISTING` on an absent workflow and its open run, an OCaml validator rejection on the open run and on an absent workflow, and `FAIL` on the open run | Results `1` and `2` from one run; the rejection is an `ApplicationError` of type `Rejected` with its detail while `workflow_handle()` still names the open or newly started run; `FAIL` raises `WorkflowAlreadyStartedError` naming the open run; only the two accepted updates enter history |
| OCaml client signal-with-start to Python workflow (#1151) | `Client.signal_with_start` on an absent workflow, its open run, with `` `Terminate_existing ``, and after the run closed | `Client.started` and the exact run ID per case on the OCaml side; on the Python side, the OCaml-encoded `json/plain` start input and signals, each new run's signal recorded before its first workflow task, the replaced run terminated, and the same decoded results |
| OCaml client update-with-start to Python workflow (#1151) | `Client.update_with_start` with `` `Use_existing `` on an absent workflow and its open run, a Python validator rejection on the open run and on an absent workflow, and `` `Fail `` on the open run | Update results `1` and `2`; the Python rejection's type, non-retryability and detail returned beside the open or newly started run; `Client.already_started` naming the open run; Python sees only the accepted updates in history and decodes the same results |
| OCaml client re-check | The four exact runs reported by Python, and the five with-start runs of the OCaml entity | The OCaml client decodes the same completed result, the `PricingFailed` type and detail, the `PricingTimedOut` type with its timeout kind and heartbeat detail, the termination, and the with-start results Python decoded |

The driver report records the Python SDK, CPython, OCaml SDK and Temporal
Server versions, and every exact workflow and run ID.

Not covered: child workflows across SDKs, an official-SDK workflow calling an
OCaml activity, custom payload codecs or encryption, search attributes and
memo encodings, and other official SDKs. These are not claims of general
cross-SDK compatibility.

# Official-SDK interoperability regression

This suite (#517) checks that a client and worker written with an official
Temporal SDK interoperate with an OCaml workflow through the default
`json/plain` payload converter. The official SDK is the Temporal Python SDK,
`temporalio` 1.33.0 (MIT), pinned with its whole dependency closure by
SHA-256 digest in [`python/requirements.lock`](python/requirements.lock) and
inventoried in [dependencies](../../../docs/dependencies.md#ci-only-interoperability-tooling).
It runs only in the digest-pinned `python:3.14-slim-bookworm` image as the
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
`_build/live-regressions/sdk_interop/` (`ready.json`, `python-report.json` and
`driver-exit`).

## Topology

[`run-live-regressions.sh`](../temporal/scripts/run-live-regressions.sh)
starts `regression.exe check` in the development image. That process spawns
an OCaml worker for the `interop-order` workflow on a unique task queue and
publishes the queue names in `ready.json`. The controller then runs the
[Python driver](python/driver.py), which hosts the `interop-price` activity
in an official-SDK worker on a second queue and drives the OCaml workflow
with the official client. After the driver exits, the controller writes its
status to `driver-exit`, and the OCaml process reopens every exact run the
driver reported through the OCaml client. Both processes must succeed.

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
| Incompatible workflow input | A JSON string instead of the order object | The OCaml worker fails the workflow task and the run stays `RUNNING` (fail-closed decoding); the driver terminates it |
| OCaml client re-check | The three exact runs reported by Python | The OCaml client decodes the same completed result, the `PricingFailed` type and detail, and the termination |

The driver report records the Python SDK, CPython, OCaml SDK and Temporal
Server versions, and every exact workflow and run ID.

Not covered: child workflows across SDKs, an official-SDK workflow calling an
OCaml activity, custom payload codecs or encryption, search attributes and
memo encodings, and other official SDKs. These are not claims of general
cross-SDK compatibility.

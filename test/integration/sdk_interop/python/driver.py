"""Official Temporal Python SDK side of the OCaml interoperability regression.

The OCaml regression executable (``../regression.ml``) hosts the
``interop-order`` workflow and publishes its task queues in ``ready.json``.
This driver then uses only the official SDK's public client and worker API,
with its default data converter, to:

* host the ``interop-price`` activity that the OCaml workflow calls;
* start OCaml workflow runs with structured ``json/plain`` input;
* query, update (accepted, validator-rejected, wrongly encoded and wrongly
  shaped) and signal the OCaml workflow;
* decode the OCaml result, a terminal failure whose type and details crossed
  both SDKs, and the fail-closed outcome of an incompatible workflow input;
* read each exact run's durable history to assert payload encodings and the
  SDK identity that the OCaml worker recorded.

Any failed expectation raises, so the process exits non-zero. On success it
writes ``python-report.json`` with both SDK versions, the server version and
the exact workflow and run IDs, which the OCaml side then re-reads through its
own client. The script only runs inside the pinned Python container started by
``test/integration/temporal/scripts/run-live-regressions.sh``.
"""

import asyncio
import json
import os
import sys
import time
from datetime import timedelta
from pathlib import Path

import temporalio
from temporalio import activity
from temporalio.api.workflowservice.v1 import GetSystemInfoRequest
from temporalio.client import (
    Client,
    WorkflowFailureError,
    WorkflowHandle,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
)
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

# Seconds to wait for the OCaml side to publish its queues, and for a
# workflow to reach an observable state. They bound waits on another process;
# the controller's container timeout bounds the driver as a whole.
READY_TIMEOUT = 120.0
STATE_TIMEOUT = 60.0

# The signal document exercises every JSON value kind. 2**53 + 1 is not
# representable as a double, so a float-based conversion in either SDK would
# change it; the strings need UTF-8 and surrogate-pair escapes to survive.
APPROVAL = {
    "approver": "Åsa Ø'Brien \U0001f680",
    "ticket": 2**53 + 1,
    "ratio": 0.1,
    "tags": ["urgent", None, True, False],
    "nested": {"empty": {}, "list": []},
}

# Activity inputs received from the OCaml workflow, keyed by workflow ID, so
# the driver can compare what the OCaml encoder produced with what it sent.
RECEIVED: dict[str, dict] = {}


def require(condition: bool, message: str) -> None:
    """Fails the driver with ``message``; unlike ``assert`` it survives -O."""
    if not condition:
        raise RuntimeError(f"interop expectation failed: {message}")


@activity.defn(name="interop-price")
async def price(request: dict) -> dict:
    """Prices an order for the OCaml workflow in integer cents.

    An item with SKU ``OUT-OF-STOCK`` raises a non-retryable application
    failure with a structured detail, which the OCaml workflow forwards in
    its own terminal failure.
    """
    RECEIVED[activity.info().workflow_id] = request
    items = request["items"]
    for item in items:
        if item["sku"] == "OUT-OF-STOCK":
            raise ApplicationError(
                "out of stock",
                {"sku": item["sku"], "requested": item["qty"]},
                type="OutOfStock",
                non_retryable=True,
            )
    return {
        "currency": request["currency"],
        "lines": len(items),
        "total_cents": sum(item["qty"] * item["unit_cents"] for item in items),
        "rush_fee_cents": 500 if request["rush"] else 0,
        "priced_by": "temporalio-python",
    }


def read_ready(evidence: Path) -> dict:
    """Waits for the OCaml side's atomically renamed ``ready.json``."""
    path = evidence / "ready.json"
    deadline = time.monotonic() + READY_TIMEOUT
    while not path.exists():
        require(
            time.monotonic() < deadline, "the OCaml worker never published ready.json"
        )
        time.sleep(0.2)
    return json.loads(path.read_text(encoding="utf-8"))


async def await_status(handle: WorkflowHandle, expected: str) -> dict:
    """Polls the OCaml ``status`` query until it reports ``expected``."""
    deadline = time.monotonic() + STATE_TIMEOUT
    while True:
        status = await handle.query("status")
        if status["status"] == expected:
            return status
        require(
            time.monotonic() < deadline, f"status stayed {status!r}, wanted {expected}"
        )
        await asyncio.sleep(0.2)


def payload_json(payloads, label: str):
    """Checks one ``json/plain`` payload and returns its decoded value."""
    require(
        len(payloads.payloads) == 1, f"{label} has {len(payloads.payloads)} payloads"
    )
    payload = payloads.payloads[0]
    encoding = payload.metadata.get("encoding")
    require(encoding == b"json/plain", f"{label} encoding is {encoding!r}")
    return json.loads(payload.data)


async def rejected_update(
    handle: WorkflowHandle, update_id: str, argument
) -> BaseException:
    """Requires the OCaml worker to reject an update and returns its cause."""
    try:
        await handle.execute_update("add-item", argument, id=update_id)
    except WorkflowUpdateFailedError as error:
        require(error.cause is not None, f"update {update_id} rejection has no cause")
        return error.cause
    raise RuntimeError(f"interop expectation failed: update {update_id} was accepted")


async def success_run(client: Client, ready: dict, ocaml_version: str) -> dict:
    """Starts, interacts with and verifies the history of one successful run."""
    queue = ready["workflow_task_queue"]
    workflow_id = f"{queue}-success"
    items = [
        {"sku": "A-1", "qty": 2, "unit_cents": 1999},
        {"sku": "B-2", "qty": 1, "unit_cents": 250},
    ]
    # "note" is an explicit null and "coupon" is absent; OCaml decodes both.
    order = {
        "customer": "Zoë 東京",
        "items": items,
        "note": None,
        "rush": True,
        "pricing_task_queue": ready["pricing_task_queue"],
    }
    started = await client.start_workflow(
        "interop-order",
        order,
        id=workflow_id,
        task_queue=queue,
        execution_timeout=timedelta(minutes=5),
    )
    run_id = started.result_run_id
    require(bool(run_id), "start returned no run ID")
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    try:
        status = await await_status(handle, "awaiting-approval")
        require(
            status == {"status": "awaiting-approval", "items": 2}, f"status {status!r}"
        )
        try:
            await handle.query("missing")
            raise RuntimeError("interop expectation failed: unknown query succeeded")
        except WorkflowQueryFailedError:
            pass

        added = {"sku": "C-3", "qty": 4, "unit_cents": 125}
        count = await handle.execute_update("add-item", added, id="add-c3")
        require(count == 3, f"accepted update returned {count!r}")

        cause = await rejected_update(
            handle, "invalid-quantity", {"sku": "D-4", "qty": 0, "unit_cents": 1}
        )
        require(isinstance(cause, ApplicationError), f"validator cause {cause!r}")
        require(cause.type == "InvalidQuantity", f"validator type {cause.type!r}")
        require(cause.non_retryable, "validator rejection lost non_retryable")
        require(
            cause.message.startswith("quantity must be positive"),
            f"validator message {cause.message!r}",
        )
        require(
            cause.details == ({"sku": "D-4", "qty": 0},),
            f"validator details {cause.details!r}",
        )
        # bytes use the official SDK's binary/plain converter; the OCaml
        # json/plain codec must reject that encoding rather than guess.
        await rejected_update(handle, "binary-input", b"\x00not json")
        await rejected_update(
            handle, "wrong-shape", {"sku": "E-5", "qty": "two", "unit_cents": 1}
        )
        status = await handle.query("status")
        require(status["items"] == 3, f"rejected updates changed state: {status!r}")

        await handle.signal("approve", APPROVAL)
        result = await handle.result()
    except BaseException:
        await terminate_quietly(handle)
        raise

    pricing = {
        "currency": "EUR",
        "lines": 2,
        "total_cents": 2 * 1999 + 250,
        "rush_fee_cents": 500,
        "priced_by": "temporalio-python",
    }
    expected = {
        "workflow_id": workflow_id,
        "run_id": run_id,
        "customer": order["customer"],
        "note": None,
        "coupon": None,
        "rush": True,
        "items": items + [added],
        "pricing": pricing,
        "approval": APPROVAL,
        "sdk": "temporal-ocaml",
    }
    require(result == expected, f"workflow result {result!r} != {expected!r}")
    request = {"currency": "EUR", "rush": True, "items": items}
    require(
        RECEIVED.get(workflow_id) == request,
        f"activity received {RECEIVED.get(workflow_id)!r}",
    )
    await check_success_history(handle, ready, order, request, pricing, ocaml_version)
    return {"workflow_id": workflow_id, "run_id": run_id, "result": result}


async def check_success_history(handle, ready, order, request, pricing, ocaml_version):
    """Asserts the exact run's durable payload encodings and SDK identity."""
    events = (await handle.fetch_history()).events
    started = [
        e.workflow_execution_started_event_attributes
        for e in events
        if e.HasField("workflow_execution_started_event_attributes")
    ]
    require(len(started) == 1, "history has no single start event")
    require(
        payload_json(started[0].input, "workflow input") == order,
        "stored input changed",
    )

    scheduled = [
        e.activity_task_scheduled_event_attributes
        for e in events
        if e.HasField("activity_task_scheduled_event_attributes")
    ]
    require(len(scheduled) == 1, f"{len(scheduled)} activities were scheduled")
    require(scheduled[0].activity_type.name == "interop-price", "activity type changed")
    require(
        scheduled[0].task_queue.name == ready["pricing_task_queue"],
        f"activity queue {scheduled[0].task_queue.name!r}",
    )
    require(
        payload_json(scheduled[0].input, "activity input") == request,
        "OCaml-encoded activity input changed",
    )
    completed = [
        e.activity_task_completed_event_attributes
        for e in events
        if e.HasField("activity_task_completed_event_attributes")
    ]
    require(
        len(completed) == 1
        and payload_json(completed[0].result, "activity result") == pricing,
        "Python-encoded activity result changed",
    )

    signals = [
        e.workflow_execution_signaled_event_attributes
        for e in events
        if e.HasField("workflow_execution_signaled_event_attributes")
    ]
    require([s.signal_name for s in signals] == ["approve"], "signal history changed")
    require(
        payload_json(signals[0].input, "signal input") == APPROVAL,
        "signal input changed",
    )
    # Rejected updates are not durable; only the accepted one is recorded.
    accepted = [
        e.workflow_execution_update_accepted_event_attributes
        for e in events
        if e.HasField("workflow_execution_update_accepted_event_attributes")
    ]
    require(
        [a.accepted_request.meta.update_id for a in accepted] == ["add-c3"],
        "accepted update history changed",
    )

    metadata = [
        e.workflow_task_completed_event_attributes.sdk_metadata
        for e in events
        if e.HasField("workflow_task_completed_event_attributes")
    ]
    names = {m.sdk_name for m in metadata if m.sdk_name}
    versions = {m.sdk_version for m in metadata if m.sdk_version}
    require(names == {"temporal-ocaml"}, f"workflow tasks completed by {names!r}")
    require(versions == {ocaml_version}, f"OCaml SDK versions {versions!r}")

    closed = [
        e.workflow_execution_completed_event_attributes
        for e in events
        if e.HasField("workflow_execution_completed_event_attributes")
    ]
    require(len(closed) == 1, "history has no single completion event")
    payload_json(closed[0].result, "workflow result")


async def activity_failure_run(client: Client, ready: dict) -> dict:
    """Requires an official-SDK activity failure to return through OCaml."""
    queue = ready["workflow_task_queue"]
    workflow_id = f"{queue}-activity-failure"
    order = {
        "customer": "failure",
        "items": [{"sku": "OUT-OF-STOCK", "qty": 3, "unit_cents": 100}],
        "note": "call first",
        "coupon": "SPRING",
        "rush": False,
        "pricing_task_queue": ready["pricing_task_queue"],
    }
    started = await client.start_workflow(
        "interop-order",
        order,
        id=workflow_id,
        task_queue=queue,
        execution_timeout=timedelta(minutes=5),
    )
    handle = client.get_workflow_handle(workflow_id, run_id=started.result_run_id)
    try:
        await handle.result()
    except WorkflowFailureError as error:
        cause = error.cause
        require(isinstance(cause, ApplicationError), f"failure cause {cause!r}")
        require(cause.type == "PricingFailed", f"failure type {cause.type!r}")
        require(cause.non_retryable, "workflow failure lost non_retryable")
        require("OutOfStock" in cause.message, f"failure message {cause.message!r}")
        require(
            cause.details == ({"sku": "OUT-OF-STOCK", "requested": 3},),
            f"failure details {cause.details!r}",
        )
        return {
            "workflow_id": workflow_id,
            "run_id": started.result_run_id,
            "error_type": cause.type,
            "details": list(cause.details),
        }
    except BaseException:
        await terminate_quietly(handle)
        raise
    raise RuntimeError("interop expectation failed: failing order completed")


async def incompatible_input_run(client: Client, ready: dict) -> dict:
    """Requires undecodable workflow input to fail the task and leave the run open.

    A JSON string is valid ``json/plain`` but not an order object. The OCaml
    worker must fail its workflow task (the run stays open for a corrected
    worker) rather than complete or fail the execution; the driver then
    terminates its own run.
    """
    queue = ready["workflow_task_queue"]
    workflow_id = f"{queue}-incompatible-input"
    started = await client.start_workflow(
        "interop-order",
        "not an order",
        id=workflow_id,
        task_queue=queue,
        execution_timeout=timedelta(minutes=5),
    )
    handle = client.get_workflow_handle(workflow_id, run_id=started.result_run_id)
    try:
        deadline = time.monotonic() + STATE_TIMEOUT
        while True:
            events = (await handle.fetch_history()).events
            failed = [
                e.workflow_task_failed_event_attributes
                for e in events
                if e.HasField("workflow_task_failed_event_attributes")
            ]
            if failed:
                break
            require(
                time.monotonic() < deadline, "incompatible input never failed a task"
            )
            await asyncio.sleep(0.2)
        require(failed[0].failure.message != "", "workflow task failure has no message")
        description = await handle.describe()
        require(
            description.status is not None and description.status.name == "RUNNING",
            f"incompatible input closed the run: {description.status!r}",
        )
    finally:
        await terminate_quietly(handle)
    return {
        "workflow_id": workflow_id,
        "run_id": started.result_run_id,
        "task_failure": failed[0].failure.message,
    }


async def terminate_quietly(handle: WorkflowHandle) -> None:
    """Best-effort cleanup of this driver's own exact run."""
    try:
        await handle.terminate(reason="sdk interop regression cleanup")
    except Exception as error:  # noqa: BLE001 - the run may already be closed.
        print(f"cleanup of {handle.id}: {error}", file=sys.stderr, flush=True)


def write_report(evidence: Path, report: dict) -> None:
    """Publishes the report through a rename so it is never read half-written."""
    partial = evidence / "python-report.json.partial"
    partial.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    partial.replace(evidence / "python-report.json")


async def main() -> None:
    """Runs every interop case and writes the evidence report."""
    evidence = Path(os.environ["SDK_INTEROP_EVIDENCE_DIR"])
    ocaml_version = os.environ["OCAML_SDK_VERSION"]
    ready = read_ready(evidence)
    client = await Client.connect(
        os.environ["TEMPORAL_ADDRESS"], namespace=ready["namespace"]
    )
    system = await client.workflow_service.get_system_info(GetSystemInfoRequest())
    print(
        f"python temporalio {temporalio.__version__}, server {system.server_version}",
        flush=True,
    )
    async with Worker(
        client, task_queue=ready["pricing_task_queue"], activities=[price]
    ):
        success = await success_run(client, ready, ocaml_version)
        print(f"success run: {success['workflow_id']}/{success['run_id']}", flush=True)
        failure = await activity_failure_run(client, ready)
        print(
            f"activity failure run: {failure['workflow_id']}/{failure['run_id']}",
            flush=True,
        )
    incompatible = await incompatible_input_run(client, ready)
    print(
        f"incompatible input run: {incompatible['workflow_id']}/{incompatible['run_id']}",
        flush=True,
    )
    write_report(
        evidence,
        {
            "python_sdk_version": temporalio.__version__,
            "python_version": sys.version.split()[0],
            "ocaml_sdk_version": ocaml_version,
            "server_version": system.server_version,
            "runs": {
                "success": success,
                "activity_failure": failure,
                "incompatible_input": incompatible,
            },
        },
    )
    print("official Python SDK driver: ok", flush=True)


if __name__ == "__main__":
    asyncio.run(main())

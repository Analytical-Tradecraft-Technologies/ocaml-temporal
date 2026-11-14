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
  both SDKs, a server-side activity timeout that the OCaml workflow reports
  by its typed timeout kind and heartbeat details, and the fail-closed
  outcome of an incompatible workflow input;
* read each exact run's durable history to assert payload encodings and the
  SDK identity that the OCaml worker recorded;
* use signal-with-start and update-with-start (#1151) against an OCaml entity
  workflow, and host a Python entity with the same protocol that the OCaml
  client drives with the same two operations at the same time, then check
  the runs the OCaml side reports from this side.

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
from temporalio import activity, workflow
from temporalio.api.enums.v1 import TimeoutType
from temporalio.api.workflowservice.v1 import GetSystemInfoRequest
from temporalio.client import (
    Client,
    WithStartWorkflowOperation,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHandle,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
)
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError
from temporalio.worker import Worker

# Seconds to wait for the OCaml side to publish its queues, and for a
# workflow to reach an observable state. They bound waits on another process;
# the controller's container timeout bounds the driver as a whole.
READY_TIMEOUT = 120.0
STATE_TIMEOUT = 60.0

# The timeout case (#1143): the OCaml workflow gives the pricing activity this
# start-to-close timeout and a single attempt, and the SLOW activity sleeps far
# longer after heartbeating HEARTBEAT, which must come back separately from
# the failure details.
TIMEOUT_MS = 3000
SLOW_ACTIVITY_SECONDS = 10.0
HEARTBEAT = {"progress": 42}

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
    its own terminal failure. An item with SKU ``SLOW`` heartbeats
    ``HEARTBEAT`` and then outlives the OCaml-chosen start-to-close timeout,
    so the server times the attempt out (#1143); the sleep is cancelled when
    the worker shuts down.
    """
    RECEIVED[activity.info().workflow_id] = request
    items = request["items"]
    if any(item["sku"] == "SLOW" for item in items):
        activity.heartbeat(HEARTBEAT)
        await asyncio.sleep(SLOW_ACTIVITY_SECONDS)
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


@workflow.defn(name="interop-py-entity", sandboxed=False)
class PythonEntity:
    """The Python counterpart of the OCaml ``interop-entity`` (#1151).

    It records every ``deliver`` signal and accepted ``apply`` update in
    arrival order, waits for ``finish``, and completes with the same JSON
    document as the OCaml entity, naming this SDK. The OCaml client drives it
    with signal-with-start and update-with-start. It runs unsandboxed because
    this script is ``__main__``, which the sandbox cannot re-import; the
    workflow uses only deterministic SDK calls.
    """

    def __init__(self) -> None:
        """Starts with no items and unfinished."""
        self.items: list[str] = []
        self.finished = False

    @workflow.run
    async def run(self, label: str) -> dict:
        """Waits for ``finish`` and reports the label and received items."""
        await workflow.wait_condition(lambda: self.finished)
        return {
            "label": label,
            "items": self.items,
            "run_id": workflow.info().run_id,
            "sdk": "temporalio-python",
        }

    @workflow.signal(name="deliver")
    def deliver(self, item: str) -> None:
        """Appends the signalled item."""
        self.items.append(item)

    @workflow.signal(name="finish")
    def finish(self) -> None:
        """Lets the run complete; the OCaml ``Codec.unit`` sends no payload."""
        self.finished = True

    @workflow.update(name="apply")
    def apply(self, item: str) -> int:
        """Appends the item and returns the new item count."""
        self.items.append(item)
        return len(self.items)

    @apply.validator
    def validate_apply(self, item: str) -> None:
        """Rejects ``reject`` as the OCaml validator does: type ``Rejected``,
        non-retryable, with one JSON detail."""
        if item == "reject":
            raise ApplicationError(
                "REJECTED", {"item": item}, type="Rejected", non_retryable=True
            )


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


async def activity_timeout_run(client: Client, ready: dict) -> dict:
    """Requires a server-side timeout of an official-SDK activity to reach the
    OCaml workflow as a typed start-to-close timeout (#1143).

    The OCaml workflow branches on ``Error.timeout_type`` rather than on the
    message and fails with type ``PricingTimedOut``. Its details are the
    timeout kind followed by ``Error.last_heartbeat_details``, so the Python
    client must decode the heartbeat its own activity recorded, kept apart from
    the failure details. History must show the server's start-to-close timeout
    of the single attempt.
    """
    queue = ready["workflow_task_queue"]
    workflow_id = f"{queue}-activity-timeout"
    order = {
        "customer": "timeout",
        "items": [{"sku": "SLOW", "qty": 1, "unit_cents": 100}],
        "note": None,
        "rush": False,
        "pricing_task_queue": ready["pricing_task_queue"],
        "pricing_timeout_ms": TIMEOUT_MS,
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
        require(isinstance(cause, ApplicationError), f"timeout cause {cause!r}")
        require(cause.type == "PricingTimedOut", f"timeout failure type {cause.type!r}")
        require(cause.non_retryable, "timeout failure lost non_retryable")
        require(
            cause.details == ({"timeout": "start_to_close"}, HEARTBEAT),
            f"timeout failure details {cause.details!r}",
        )
        events = (await handle.fetch_history()).events
        timed_out = [
            e.activity_task_timed_out_event_attributes
            for e in events
            if e.HasField("activity_task_timed_out_event_attributes")
        ]
        require(len(timed_out) == 1, f"{len(timed_out)} activity timeouts in history")
        require(
            timed_out[0].failure.timeout_failure_info.timeout_type
            == TimeoutType.TIMEOUT_TYPE_START_TO_CLOSE,
            f"history timeout {timed_out[0].failure.timeout_failure_info!r}",
        )
        require(
            not any(
                e.HasField("activity_task_completed_event_attributes")
                or e.HasField("activity_task_failed_event_attributes")
                for e in events
            ),
            "timed-out activity also completed or failed",
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
    raise RuntimeError("interop expectation failed: timed-out order completed")


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


def history_of(events, field: str) -> list:
    """Returns the ``field`` attributes of every event that has them."""
    return [getattr(e, field) for e in events if e.HasField(field)]


async def require_signal_first(handle: WorkflowHandle, label: str, item: str):
    """Requires the run's first ``deliver`` signal, carrying ``item`` as
    ``json/plain``, to be recorded before its first workflow task started, so
    the run's first activation delivers it (#1151). Returns the history."""
    events = (await handle.fetch_history()).events
    require(
        bool(events)
        and events[0].HasField("workflow_execution_started_event_attributes"),
        f"{label}: the history does not begin with the start",
    )
    for event in events[1:]:
        if event.HasField("workflow_execution_signaled_event_attributes"):
            signal = event.workflow_execution_signaled_event_attributes
            require(signal.signal_name == "deliver", f"{label}: first signal {signal}")
            require(
                payload_json(signal.input, f"{label} signal input") == item,
                f"{label}: the first signal is not {item!r}",
            )
            return events
        require(
            not event.HasField("workflow_task_started_event_attributes"),
            f"{label}: the first workflow task started before the signal",
        )
    raise RuntimeError(f"interop expectation failed: {label}: no signal recorded")


def entity_operation(
    queue: str, workflow_id: str, policy: WorkflowIDConflictPolicy
) -> WithStartWorkflowOperation:
    """The start half of an update-with-start of the OCaml entity."""
    return WithStartWorkflowOperation(
        "interop-entity",
        "python",
        id=workflow_id,
        task_queue=queue,
        id_conflict_policy=policy,
        execution_timeout=timedelta(minutes=5),
    )


async def operation_run(
    client: Client, operation: WithStartWorkflowOperation
) -> WorkflowHandle:
    """Returns an exact-run handle for the run an update-with-start used."""
    started = await operation.workflow_handle()
    require(bool(started.result_run_id), "update-with-start returned no run ID")
    return client.get_workflow_handle(started.id, run_id=started.result_run_id)


async def finish_entity(handle: WorkflowHandle, items: list) -> dict:
    """Finishes a run of the OCaml entity, started by this client with the
    label ``python``, and requires it to report exactly ``items`` and its own
    run ID."""
    await handle.signal("finish")
    result = await handle.result()
    expected = {
        "label": "python",
        "items": items,
        "run_id": handle.run_id,
        "sdk": "temporal-ocaml",
    }
    require(result == expected, f"entity result {result!r} != {expected!r}")
    return result


def require_rejection(cause: BaseException | None, label: str) -> None:
    """Requires the entity validator's typed rejection of ``reject``."""
    require(isinstance(cause, ApplicationError), f"{label}: cause {cause!r}")
    require(cause.type == "Rejected", f"{label}: type {cause.type!r}")
    require(cause.non_retryable, f"{label}: rejection lost non_retryable")
    require(cause.message.startswith("REJECTED"), f"{label}: {cause.message!r}")
    require(cause.details == ({"item": "reject"},), f"{label}: {cause.details!r}")


async def with_start_runs(client: Client, ready: dict) -> dict:
    """Uses the official client's signal-with-start and update-with-start
    against the OCaml entity (#1151).

    Signal-with-start starts an absent workflow with the signal recorded
    before its first task, only signals the open run (same run ID), replaces
    it under ``TERMINATE_EXISTING``, and starts the closed workflow again.
    Update-with-start starts a run with an accepted update, admits a second
    one on the open run, reports the OCaml validator's rejection while still
    naming the run (open, or started by that very call), and raises
    ``WorkflowAlreadyStartedError`` naming the open run under ``FAIL``. Every
    run's result lists exactly what the OCaml workflow code received.
    """
    queue = ready["workflow_task_queue"]
    handles: list[WorkflowHandle] = []

    async def signal_with_start(
        workflow_id: str, item: str, policy: WorkflowIDConflictPolicy
    ) -> WorkflowHandle:
        started = await client.start_workflow(
            "interop-entity",
            "python",
            id=workflow_id,
            task_queue=queue,
            execution_timeout=timedelta(minutes=5),
            id_conflict_policy=policy,
            start_signal="deliver",
            start_signal_args=[item],
        )
        require(bool(started.result_run_id), "signal-with-start returned no run ID")
        handle = client.get_workflow_handle(workflow_id, run_id=started.result_run_id)
        handles.append(handle)
        return handle

    async def update_with_start(
        workflow_id: str, item: str, policy: WorkflowIDConflictPolicy
    ):
        operation = entity_operation(queue, workflow_id, policy)
        try:
            outcome = await client.execute_update_with_start_workflow(
                "apply", item, start_workflow_operation=operation, id=f"apply-{item}"
            )
        except WorkflowUpdateFailedError as error:
            outcome = error
        handle = await operation_run(client, operation)
        handles.append(handle)
        return handle, outcome

    use_existing = WorkflowIDConflictPolicy.USE_EXISTING
    try:
        signal_id = f"{queue}-python-signal-with-start"
        first = await signal_with_start(signal_id, "first", use_existing)
        await require_signal_first(first, "absent workflow", "first")
        again = await signal_with_start(signal_id, "second", use_existing)
        require(again.run_id == first.run_id, "the open run was not signalled")
        replacement = await signal_with_start(
            signal_id, "third", WorkflowIDConflictPolicy.TERMINATE_EXISTING
        )
        require(replacement.run_id != first.run_id, "TERMINATE_EXISTING kept the run")
        description = await first.describe()
        require(
            description.status == WorkflowExecutionStatus.TERMINATED,
            f"the replaced run is {description.status!r}",
        )
        signals = history_of(
            (await first.fetch_history()).events,
            "workflow_execution_signaled_event_attributes",
        )
        require(
            [payload_json(s.input, "signal input") for s in signals]
            == ["first", "second"],
            "the replaced run did not record both signals",
        )
        await require_signal_first(replacement, "terminate_existing", "third")
        replacement_result = await finish_entity(replacement, ["third"])
        restarted = await signal_with_start(signal_id, "fourth", use_existing)
        require(restarted.run_id != replacement.run_id, "a closed run was signalled")
        await require_signal_first(restarted, "closed workflow", "fourth")
        restarted_result = await finish_entity(restarted, ["fourth"])

        update_id = f"{queue}-python-update-with-start"
        updated, count = await update_with_start(update_id, "one", use_existing)
        require(count == 1, f"the first update returned {count!r}")
        attached, count = await update_with_start(update_id, "two", use_existing)
        require(attached.run_id == updated.run_id, "the open run was not updated")
        require(count == 2, f"the second update returned {count!r}")
        rejected, error = await update_with_start(update_id, "reject", use_existing)
        require(rejected.run_id == updated.run_id, "the rejection named another run")
        require(isinstance(error, WorkflowUpdateFailedError), f"accepted: {error!r}")
        require_rejection(error.cause, "rejection on the open run")
        refused = entity_operation(queue, update_id, WorkflowIDConflictPolicy.FAIL)
        try:
            await client.execute_update_with_start_workflow(
                "apply", "three", start_workflow_operation=refused, id="apply-three"
            )
            raise RuntimeError("interop expectation failed: FAIL used the open run")
        except WorkflowAlreadyStartedError as error:
            require(error.run_id == updated.run_id, f"FAIL named {error.run_id!r}")
        # The operation's handle carries the same refusal.
        try:
            await refused.workflow_handle()
            raise RuntimeError("interop expectation failed: FAIL returned a handle")
        except WorkflowAlreadyStartedError:
            pass
        updated_result = await finish_entity(updated, ["one", "two"])
        accepted = history_of(
            (await updated.fetch_history()).events,
            "workflow_execution_update_accepted_event_attributes",
        )
        require(
            [
                payload_json(a.accepted_request.input.args, "update input")
                for a in accepted
            ]
            == ["one", "two"],
            "rejected or refused updates entered history",
        )
        fresh, error = await update_with_start(
            f"{update_id}-rejected", "reject", use_existing
        )
        require(isinstance(error, WorkflowUpdateFailedError), f"accepted: {error!r}")
        require_rejection(error.cause, "rejection on a new run")
        fresh_result = await finish_entity(fresh, [])
    except BaseException:
        for handle in handles:
            await terminate_quietly(handle)
        raise

    def run(handle: WorkflowHandle, result: dict | None = None) -> dict:
        entry = {"workflow_id": handle.id, "run_id": handle.run_id}
        if result is not None:
            entry["result"] = result
        return entry

    return {
        "signal_with_start_terminated": run(first),
        "signal_with_start_replacement": run(replacement, replacement_result),
        "signal_with_start_restarted": run(restarted, restarted_result),
        "update_with_start": run(updated, updated_result),
        "update_with_start_rejected": run(fresh, fresh_result),
    }


async def check_ocaml_with_start(client: Client, evidence: Path, ready: dict) -> dict:
    """Checks, from this side, the Python entity runs that the OCaml client
    signal-/update-with-started (#1151).

    Waits for ``ocaml-with-start.json``, then requires every reported exact
    run to carry the OCaml-encoded ``json/plain`` start input; each started
    signal-with-start run to record its signal before its first workflow
    task; the replaced run to be terminated after both signals; the results
    this client decodes to equal those the OCaml client decoded; and only the
    two accepted updates, never the rejected one, to enter history.
    """
    path = evidence / "ocaml-with-start.json"
    deadline = time.monotonic() + STATE_TIMEOUT
    while not path.exists():
        require(time.monotonic() < deadline, "the OCaml client never reported")
        await asyncio.sleep(0.2)
    outcome = json.loads(path.read_text(encoding="utf-8"))
    require(outcome.get("ok") is True, f"OCaml client failed: {outcome.get('error')}")
    runs = outcome["runs"]

    async def reported(name: str):
        run = runs[name]
        require(
            run["workflow_id"].startswith(ready["workflow_task_queue"]),
            f"the OCaml side reported a foreign workflow ID {run['workflow_id']!r}",
        )
        handle = client.get_workflow_handle(run["workflow_id"], run_id=run["run_id"])
        events = (await handle.fetch_history()).events
        started = history_of(events, "workflow_execution_started_event_attributes")
        require(
            len(started) == 1
            and started[0].workflow_type.name == "interop-py-entity"
            and payload_json(started[0].input, f"{name} input") == "ocaml",
            f"{name}: the OCaml client's start changed",
        )
        return run, handle

    run, handle = await reported("signal_with_start_terminated")
    events = await require_signal_first(handle, "OCaml absent workflow", "first")
    signals = history_of(events, "workflow_execution_signaled_event_attributes")
    require(
        [payload_json(s.input, "signal input") for s in signals] == ["first", "second"],
        "the run the OCaml client replaced did not record both signals",
    )
    description = await handle.describe()
    require(
        description.status == WorkflowExecutionStatus.TERMINATED,
        f"the run the OCaml client replaced is {description.status!r}",
    )
    for name, items in [
        ("signal_with_start_replacement", ["third"]),
        ("signal_with_start_restarted", ["fourth"]),
        ("update_with_start", ["one", "two"]),
        ("update_with_start_rejected", []),
    ]:
        run, handle = await reported(name)
        if name.startswith("signal"):
            await require_signal_first(handle, f"OCaml {name}", items[0])
        result = await handle.result()
        expected = {
            "label": "ocaml",
            "items": items,
            "run_id": run["run_id"],
            "sdk": "temporalio-python",
        }
        require(
            result == run["result"] == expected,
            f"{name}: Python decoded {result!r}, OCaml {run['result']!r}",
        )
        accepted = history_of(
            (await handle.fetch_history()).events,
            "workflow_execution_update_accepted_event_attributes",
        )
        inputs = [
            payload_json(a.accepted_request.input.args, "update input")
            for a in accepted
        ]
        require(
            inputs == (items if name.startswith("update") else []),
            f"{name}: accepted update history changed",
        )
    return runs


def publish(path: Path, text: str) -> None:
    """Writes ``text`` through a rename so it is never read half-written."""
    partial = path.with_name(path.name + ".partial")
    partial.write_text(text, encoding="utf-8")
    partial.replace(path)


async def terminate_quietly(handle: WorkflowHandle) -> None:
    """Best-effort cleanup of this driver's own exact run."""
    try:
        await handle.terminate(reason="sdk interop regression cleanup")
    except Exception as error:  # noqa: BLE001 - the run may already be closed.
        print(f"cleanup of {handle.id}: {error}", file=sys.stderr, flush=True)


def write_report(evidence: Path, report: dict) -> None:
    """Publishes the report through a rename so it is never read half-written."""
    publish(
        evidence / "python-report.json", json.dumps(report, indent=2, sort_keys=True)
    )


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
    async with (
        Worker(client, task_queue=ready["pricing_task_queue"], activities=[price]),
        Worker(
            client,
            task_queue=ready["python_workflow_task_queue"],
            workflows=[PythonEntity],
        ),
    ):
        # The OCaml client starts its with-start cases against PythonEntity
        # now, concurrently with the cases below (#1151).
        publish(evidence / "python-ready", "ready\n")
        success = await success_run(client, ready, ocaml_version)
        print(f"success run: {success['workflow_id']}/{success['run_id']}", flush=True)
        failure = await activity_failure_run(client, ready)
        print(
            f"activity failure run: {failure['workflow_id']}/{failure['run_id']}",
            flush=True,
        )
        timeout = await activity_timeout_run(client, ready)
        print(
            f"activity timeout run: {timeout['workflow_id']}/{timeout['run_id']}",
            flush=True,
        )
        with_start = await with_start_runs(client, ready)
        print("signal-/update-with-start against the OCaml entity: ok", flush=True)
        ocaml_with_start = await check_ocaml_with_start(client, evidence, ready)
        print("OCaml client's with-start runs checked from Python: ok", flush=True)
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
                "activity_timeout": timeout,
                "incompatible_input": incompatible,
                "with_start": with_start,
                "ocaml_with_start": ocaml_with_start,
            },
        },
    )
    print("official Python SDK driver: ok", flush=True)


if __name__ == "__main__":
    asyncio.run(main())

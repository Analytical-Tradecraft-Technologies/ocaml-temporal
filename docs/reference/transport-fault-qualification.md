# Transport fault qualification

This document describes the deterministic transport fault harness for issue
#504 and the outcomes it verifies. It covers what a client or worker reports
when the network to Temporal fails, and how a caller reconciles an operation
that the server may have applied. The suite is
[`test/integration/transport_interruption`](../../test/integration/transport_interruption/README.md).

## Harness

The regression starts two in-process TCP proxies (`Fault_proxy`), one for a
client and one for a worker, both in front of the same Temporal frontend. A
third client connects directly to the server. It is never faulted, so it
reports the durable state. The proxy has three modes, and the driver switches
between them at exact points in a scenario:

| Mode | Effect on the SDK |
| --- | --- |
| `Forward` | Bytes are copied unchanged in both directions. |
| `Refuse` | Every live connection is closed, and each new connection is closed as soon as it is accepted: the server looks down. |
| `Drop_responses` | Client bytes reach Temporal, but every byte the server sends back is discarded. A request can be applied while its acknowledgement is lost. |

The proxy does not parse HTTP/2, so it cannot invent a server answer. Dropping
response bytes leaves that socket's HTTP/2 state corrupt. For that reason,
returning to `Forward` (`restore`) always closes every proxied connection
first, and the SDK reconnects from a clean state. The proxy runs on its own
Domain, so a test Domain blocked in a native call cannot starve it.

The proxy does not stop or restart a container. A fault starts at a known
request boundary instead of depending on how long a server restart happens to
take on the host. Container restarts are already covered by the worker restart
and crash suites.

Every injection and every observed result is printed as a
`transport-fault +<seconds>s ...` line. The line includes the elapsed time,
the proxy mode, the operation, its typed classification, and the request,
workflow, run or update ID used to reconcile it. A failure log therefore ties
each injection to the operation that observed it.

## Verified outcomes

All results are typed `Error.t` values. No scenario raises an exception for an
operational failure.

| Scenario | Fault | Required SDK result | Reconciliation and durable outcome |
| --- | --- | --- | --- |
| Client connect | `Refuse` during `Client.create` | Prompt `` `Bridge `` error | The same target connects after `restore`. |
| Start, acknowledgement lost | `Drop_responses` during `Client.start ~request_id` | After the 10 s start deadline: an uncertain-start error. It is `` `Bridge ``, non-retryable, has no `rpc_status` and no `already_started`, and names the request and workflow IDs | A start with another request ID fails with `already_started` naming the run, which proves the first start was applied. A retry with the original request ID returns that same run, with `started = true`. |
| Signal, acknowledgement lost | `Drop_responses` during `Client.signal ~request_id` | After the 3 s control deadline: a retryable transport status (`Cancelled`, `Deadline_exceeded`, `Unavailable` or `Unknown`) | A retry with the same request ID succeeds. The counter shows that the signal was applied once. |
| Update, response delayed | `Drop_responses` for 2 s, then `restore` | `start_update` succeeds. Core re-sends the same update ID on the new connection within the 30 s acceptance budget | `wait_update` and a direct query both show one application. |
| Exact-run wait | `Refuse` for 3 s while `Client.wait` is in flight | The wait does not fail | It returns `Completed` once a direct signal finishes the run. |
| Terminate, acknowledgement lost | `Drop_responses` during `Client.terminate` | `rpc_status = Termination_outcome_uncertain`, non-retryable | A direct `wait` observes `Terminated`. |
| Worker poll outage | `Refuse` for the worker while a workflow is started | `Worker.run` keeps running | The workflow completes after `restore`. |
| Activity completion, acknowledgement lost | `Drop_responses` for the worker while the activity completes | The completion is retained and re-sent after reconnecting. Core treats the server's `NotFound` for the already-applied completion as done | The workflow completes, and the activity callback was dispatched exactly once in that attempt. |
| Client shutdown while unavailable | `Refuse`, then `Client.shutdown` | `Ok ()` without waiting on the transport | None needed. |

After every proxied client and the worker are shut down, both proxies must
report zero open connections. A connection still open at that point is a
leaked native handle or socket.

## Delivery guarantees the tests rely on

- An uncertain client outcome is never proof that the server rejected the
  operation. Reconcile it by retrying with the same idempotency key
  (`request_id` for start and signal, `update_id` for update, and the
  defaulted key for cancel), or by observing the exact run with `wait` or a
  query. Terminate has no idempotency key, so it reports
  `Termination_outcome_uncertain` instead of retrying blindly.
- Retrying a retained activity completion does not run the callback again in
  the same in-memory attempt. The activity scenario counts callback
  dispatches to check this.
- Activities are delivered at least once. If a process dies, or an activity
  times out before Temporal records its completion, the server can legitimately
  dispatch the activity again, even when an earlier attempt already performed
  an external side effect. The SDK cannot make that side effect exactly-once.
  Carry a stable business idempotency key in the activity input, and make the
  external system deduplicate on that key atomically. Do not use the attempt
  number as the key.

## Running

CI runs the suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. The suite usually finishes in under a minute, and the controller
bounds it at 180 s. To run it locally against a disposable server that has the
`default` namespace:

```sh
make test-transport-interruption-live RUN='opam exec --' \
  TEMPORAL_CLIENT_TEST_URL=http://127.0.0.1:7233
```

## Remaining #504 scope

The following parts of #504 are not covered by this suite:

- Worker shutdown while the transport is unavailable. This depends on the
  bounded shutdown work in #495.
- Faults on authenticated (TLS) connections. This depends on #496.
- Workflow task completion loss as its own targeted scenario. It is exercised
  only indirectly here, through lost poll responses during the activity
  scenario.
- The worked side-effecting activity example with an external store that
  provides an atomic idempotency contract, and the failure-after-commit
  redelivery test across a worker restart.
- Retention of CI diagnostic artifacts (#490). Today the suite's evidence is
  its log under `_build/live-regressions/`.

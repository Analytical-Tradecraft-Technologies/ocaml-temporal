# OCaml SDK Logging

The OCaml SDK uses the [`logs`](https://ocaml.org/p/logs/latest) library for
application-configurable logging. The SDK creates sources and submits events,
but never installs a reporter or changes reporting levels. An application owns
those process-wide choices.

## Sources

Source names are stable filtering identifiers:

| Source | Purpose |
|---|---|
| `temporal.sdk.lifecycle` | Native runtime, worker adapter, and worker lifecycle events |
| `temporal.sdk.bridge` | Calls through the private OCaml/C/Rust bridge |
| `temporal.sdk.workflow` | Workflow execution and activation processing |

The workflow source is reserved for deterministic local workflow execution and
activation records. Native worker and adapter poll, completion, rejection, and
run/shutdown records currently use the lifecycle source; filter that source
when diagnosing worker operation.

Current events use these levels:

- `Debug` records bridge operation completions, expected not-ready states,
  workflow execution and activation processing, and worker/adapter
  poll/completion detail. `temporal.duration_ms` is currently attached to
  bridge-operation completion and local workflow activation records.
- `Info` records runtime initialization/closure, workflow lifecycle
  transitions, and worker run/shutdown transitions.
- `Warning` records recoverable abnormal conditions, such as work delivered to
  an execution after cache eviction, a rejected workflow/activity task or
  activation completion, or a worker shutdown that still has leased tasks to
  finish.
- `Error` records failed bridge operations and workflow failures.

At the bridge boundary, an empty non-blocking worker poll lane, an exact-run
client wait whose bounded 100 ms interval elapsed, or an asynchronous
start-ticket poll/wait that is still pending returns the typed `Not_ready`
status and emits a `Debug` bridge record with
`temporal.bridge_status=not_ready`. The public worker adapters also emit
`Debug` lifecycle records named `workflow_poll_not_ready` and
`activity_poll_not_ready` when they map an empty lane to normal `Not_ready`
progress. These are expected scheduling states, not failures; protocol,
lifecycle, configuration, and native bridge failures remain `Error` records.
This level split keeps a healthy worker or waiting client from producing
error-volume logs while retaining actionable diagnostics for conditions that
require intervention. These level assignments describe the SDK's current
reporting callsites; applications may use the generic `Observability.report`
helper for additional application events.

The SDK never logs at `App`, which is reserved for the application.

## Tags

Reporters receive typed structural tags independently from message prose:

| Tag | Type | Meaning |
|---|---|---|
| `temporal.operation` | string | Stable operation identifier |
| `temporal.duration_ms` | float | Finite non-negative elapsed milliseconds |
| `temporal.workflow_type` | string | Registered workflow type |
| `temporal.job_count` | int | Jobs supplied in one activation |
| `temporal.command_count` | int | Commands emitted by one activation |
| `temporal.bridge_status` | string | Stable lowercase bridge status |
| `temporal.error_kind` | string | Stable lowercase Temporal error category |

## Operation identifiers

The `temporal.operation` tag is the stable filtering key for an individual
SDK action. These are the identifiers emitted by the current callsites; the
same identifier may appear in more than one source when a bridge operation
also has a higher-level lifecycle record:

| Source | Current operation identifiers |
|---|---|
| `temporal.sdk.bridge` | `check_abi_version`, `echo`, `conformance_wait_ms`, `runtime_create`, `runtime_close`, `client_connect`, `client_disconnect`, `client_start_workflow_json`, `client_begin_start_workflow_json`, `client_poll_start_workflow_json`, `client_wait_start_workflow_json`, `client_wait_workflow_json`, `client_cancel_workflow_json`, `client_signal_workflow_json`, `client_query_workflow_json`, `client_update_workflow_json`, `client_poll_update_workflow_json`, `client_complete_async_activity_json`, `client_record_async_activity_heartbeat_json`, `worker_start`, `worker_try_poll_workflow`, `worker_wait_workflow`, `worker_complete_workflow_json`, `worker_reject_workflow_json`, `worker_try_poll_activity`, `worker_wait_activity`, `worker_wait_activity_completion_retry_backoff`, `worker_complete_activity_json`, `worker_reject_activity_json`, `worker_record_activity_heartbeat_json`, `worker_shutdown`, `replay_worker_start`, `replay_worker_feed_history`, `replay_worker_try_poll_workflow`, `replay_worker_wait_workflow`, `replay_worker_complete_workflow`, `replay_worker_reject_workflow`, `replay_worker_finish_input`, `replay_worker_finalize`, `replay_worker_dispose` |
| `temporal.sdk.lifecycle` | `runtime_create`, `runtime_close`, `workflow_task_rejected`, `activity_task_rejected`, `activity_completion_retry`, `worker_run_started`, `worker_run_finished`, `worker_terminal_cleanup`, `worker_terminal_cleanup_failed`, `worker_shutdown`, `worker_shutdown_failed`, `workflow_activation_completed`, `workflow_activation_rejected`, `workflow_poll_not_ready`, `activity_task_completed`, `activity_async_handoff_accepted`, `activity_poll_not_ready` |
| `temporal.sdk.workflow` | `execution_created`, `workflow_started`, `workflow_completed`, `workflow_failed`, `workflow_query_unhandled`, `workflow_query_completed`, `workflow_query_failed`, `workflow_update_unhandled`, `workflow_signal_unhandled`, `workflow_signal_received`, `workflow_signal_handled`, `workflow_cancelled`, `execution_evicted`, `activation_ignored`, `activate` |

The bridge source emits a completion record for every bridge call and a
second status record when the typed result is unsuccessful. That second record
uses the status-specific level policy above: normal `Not_ready` progress is
`Debug`, `Outstanding_tasks` during shutdown is `Warning`, and failures that
need intervention are `Error`. Applications that need a broad worker view
should filter `temporal.sdk.lifecycle`; applications diagnosing deterministic
workflow execution should filter `temporal.sdk.workflow`.

### Activity lease events

The activity adapter reports a few lifecycle operations whose names describe
lease transitions rather than user-level activity outcomes. Their distinction
matters when diagnosing retries:

| Operation | Level | Meaning |
|---|---|---|
| `activity_task_completed` | `Debug` | The native worker accepted a terminal completion for the leased task. The adapter retires that lease; it does not call the activity again. |
| `activity_task_rejected` | `Warning` | The adapter submitted a typed task-level rejection and the lease was retired. This is acknowledged task progress, so the worker loop continues; inspect `temporal.error_kind` for the stable rejection category. |
| `activity_completion_retry` | `Warning` | Submission of a terminal completion returned the explicitly retryable bridge status. The exact completion remains retained for a later drain, and the activity callback is not rerun. This event is a transient lease condition, not evidence that the activity ran twice. |
| `activity_async_handoff_accepted` | `Debug` | Core accepted `Will_complete_async`, moving the task from the worker lease to the namespace-bound asynchronous lease. This is not a terminal activity result; a later `Async_handle.complete`, `fail`, or `cancel` call must retire that lease. |

The `activity_async_handoff_accepted` record therefore proves only that the
worker-side handoff was accepted. It does not prove that a later asynchronous
completion or heartbeat was accepted. Those operations have their own bridge
records (`client_complete_async_activity_json` and
`client_record_async_activity_heartbeat_json`), while the public handle keeps
their typed result and retry semantics.

### Workflow execution and interaction events

Workflow-source events describe the in-memory execution state machine. They
are not server acknowledgements, and an event can be emitted by the synthetic
runtime or by the native worker adapter before a corresponding completion is
accepted by Temporal:

| Operation | Level | Meaning |
|---|---|---|
| `execution_created` | `Debug` | The SDK allocated the scheduler and workflow context for one execution. This is local state creation, not proof that Temporal accepted a start request. |
| `workflow_started` | `Info` | A `Start_workflow` activation was accepted and the workflow callback was queued. The callback is run at most once for that execution. |
| `workflow_completed` | `Info` | Workflow code returned successfully, its output was encoded, and the SDK buffered a terminal completion command. It does not by itself prove that the worker's native completion RPC succeeded. |
| `workflow_failed` | `Error` | The SDK buffered a terminal failure command for a typed workflow, codec, activation, or bridge error. Inspect `temporal.error_kind`; the event is local failure evidence, not a server-side failure classification. |
| `workflow_cancelled` | `Info` | A cancellation activation was received for a non-terminal execution and the SDK is emitting its terminal cancellation command. It does not mean that the server has already observed the completion. |
| `execution_evicted` | `Debug` | Core asked the SDK to remove an execution from its sticky cache. The execution context is shut down and no workflow commands are produced for that eviction activation. |
| `activation_ignored` | `Warning` | A later activation arrived for an execution already removed from the cache. The SDK intentionally ignores it and returns no commands; repeated occurrences indicate a stale or out-of-order delivery that needs investigation. |
| `activate` | `Debug` | One activation batch finished local processing. `temporal.job_count`, `temporal.command_count`, `temporal.workflow_type`, and `temporal.duration_ms` describe that batch, including a zero-command ignored batch. |

Interaction events distinguish admission from handler completion. A matching
signal emits `workflow_signal_received` when it is queued on the owning
scheduler and `workflow_signal_handled` only after the handler returns `Ok ()`.
An absent signal handler emits `workflow_signal_unhandled` and fails the
workflow; a handler error instead leads to `workflow_failed` without a
successful handled event. Queries are synchronous and do not fail the
workflow: `workflow_query_completed` means that the output was encoded,
`workflow_query_failed` records a typed handler or encoding error, and
`workflow_query_unhandled` means that no handler was registered. Each query
outcome is still returned to the caller as a query response.

The current update event set is deliberately smaller. A missing update handler
emits `workflow_update_unhandled` at `Error` level and returns a rejected
update response. Validator or implementation errors also return a rejection,
but do not currently have separate named workflow log operations; absence of
`workflow_update_unhandled` must therefore not be read as proof that an update
was accepted. Acceptance and completion remain protocol responses rather than
separate log operations so callers can correlate them by
`protocol_instance_id`.

### Native worker lifecycle and completion events

Lifecycle-source records describe the worker adapter's ownership and cleanup
boundaries. They help separate a completion that was accepted by the native
adapter from a public `Worker.run` or `Worker.shutdown` result:

| Operation | Level | Meaning |
|---|---|---|
| `workflow_activation_completed` | `Debug` | The native supervisor accepted a retained workflow completion and the adapter retired that completion. A terminal or eviction completion also removes the corresponding execution from the adapter registry; this is not a server-side workflow-result acknowledgement. |
| `workflow_activation_rejected` | `Warning` | The adapter submitted an SDK-generated failure completion for a malformed or otherwise rejected activation, and the native supervisor accepted that rejection. `temporal.error_kind` identifies the stable reason; a transport failure that leaves the completion pending does not emit this event. |
| `workflow_task_rejected` | `Warning` | The public worker observed an adapter rejection whose failure completion already retired the workflow lease. The worker loop treats that as progress and continues polling; a rejection that did not retire its lease is returned as a worker error instead. |
| `worker_run_started` | `Info` | One invocation of `Temporal.Worker.run` acquired the run ownership guard and began polling. It does not mean that a workflow or activity task is currently available. |
| `worker_run_finished` | `Info` | That polling invocation returned and released the run guard. It may have stopped because shutdown was requested or because the loop returned an error; inspect the public `result` rather than treating this event as success. |
| `worker_terminal_cleanup` | `Info` | A terminal cleanup attempt obtained a native shutdown result and the adapter then discarded its OCaml-owned maps. It can occur after an earlier public shutdown error because cleanup is the force-release boundary. |
| `worker_terminal_cleanup_failed` | `Error` | Native terminal cleanup returned an error or raised before its release result was proven. The worker retains the cleanup-pending state and may schedule another detached attempt; inspect `temporal.error_kind` when present. |
| `worker_shutdown` | `Info` | Public worker shutdown drained the adapters and the native supervisor returned `Ok`. Repeated shutdown calls are cached and do not represent new native work. |
| `worker_shutdown_failed` | `Error` | An unexpected exception escaped the public native-shutdown call before a typed result was returned. This is narrower than every typed shutdown error; the cleanup path is scheduled separately. |

The workflow and activity task-rejection events are lease outcomes, not
application-level retries. A `Warning` for an acknowledged rejection means the
worker can continue polling, while `activity_completion_retry` specifically
means that the exact activity completion remains retained for a safe later
submission. Neither event means that the callback was invoked twice.

Latency is measured around the local OCaml operation with the portable Unix
wall clock, expressed as fractional milliseconds, and clamped to zero if the
clock moves backwards. The SDK currently attaches this tag to bridge-operation
completion and local workflow activation records; worker and adapter lifecycle
records do not currently carry latency tags. It is diagnostic metadata only:
workflow code never uses it to choose commands or results. Future modules
should reuse the source and tag definitions in `Temporal_base.Observability`
instead of inventing near-duplicate names.

The shared tag constructor normalizes negative counts to zero. Negative,
`NaN`, and infinite durations also become zero. This defensive boundary keeps
reporter and metrics backends free from impossible values if a future internal
caller supplies malformed metadata; valid numeric values are preserved.

## Application setup and filtering

The default `logs` reporter discards records and new sources inherit the
process's current default level. A small application setup can use the base
formatter reporter and then make bridge detail more verbose:

```ocaml
let () =
  Logs.set_reporter (Logs.format_reporter ());
  Logs.set_level (Some Logs.Info);
  Logs.Src.list ()
  |> List.find_opt (fun source ->
         Logs.Src.name source = "temporal.sdk.bridge")
  |> Option.iter (fun source -> Logs.Src.set_level source (Some Logs.Debug))
```

Applications using reporters from multiple Domains must also configure the
reporter synchronization appropriate to their runtime, for example with
`Logs.set_reporter_mutex`. The SDK does not select that policy or add an
optional reporter package on the application's behalf.

## Privacy and failure isolation

Events contain operation names, counts, type names, stable error categories,
and latency. They do not contain payload bytes, workflow inputs or outputs,
credentials, Rust diagnostic strings, or arbitrary remote failure detail.
User-controlled string tags are capped at 256 bytes. Truncation preserves valid
UTF-8 character boundaries before adding the visible `...` suffix, and current
message prose is constant and bounded. The Rust bridge reduces Core/gRPC worker
and poll-lane failures to those constant categories before they reach C; the
OCaml worker adapter repeats the check before reporting or returning an error.
The private diagnostic text is discarded because this logging policy has no
path that is allowed to expose it.

Every SDK report passes through one exception shield. If an application
reporter or formatter raises, the SDK discards that record and returns the
same `result`, commands, or exception it would have produced without logging.
Logging therefore adds diagnostics without changing the public typed-error
model or deterministic command decisions.

Workflow-runtime reports also run with the Domain-local workflow context
temporarily masked. A reporter that calls a workflow API re-entrantly therefore
receives the normal outside-workflow behavior and cannot append deterministic
commands to the activation being reported.

## Verification

`test/observability/test_logging.ml` installs an in-memory reporter and checks
the contract structurally: exact source and tag names, representative
bridge/workflow severity assignments, finite non-negative latency tags on
bridge and activation records, and the absence of raw byte payloads or request
JSON in message text and rendered tags. It also verifies that a reporter
exception cannot change a bridge result or workflow command batch. Run it with
`dune exec ./test/observability/test_logging.exe` or use the broader Makefile
test target.

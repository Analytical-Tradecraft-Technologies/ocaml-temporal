# OCaml SDK Logging and Metrics

The OCaml SDK uses the [`logs`](https://ocaml.org/p/logs/latest) library for
application-configurable logging, and can export Temporal Core's worker and
client metrics to Prometheus ([worker metrics](#worker-metrics)). The SDK creates sources and submits events,
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
  bridge-operation completion, protocol codec timing and local workflow
  activation records.
- `Info` records runtime initialization/closure, workflow lifecycle
  transitions, and worker run/shutdown transitions.
- `Warning` records recoverable abnormal conditions, such as work delivered to
  an execution after cache eviction, a rejected workflow/activity task or
  activation completion, or a worker shutdown that still has leased tasks to
  finish.
- `Error` records failed bridge operations, workflow task failures, and
  terminal workflow failures.

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
reporting callsites. Applications can emit their own events through `Logs`;
the SDK's `Observability.report` helper is not exported by the public
`Temporal` module.

The SDK never logs at `App`, which is reserved for the application.

## Tags

Reporters receive typed structural tags independently from message prose:

| Tag | Type | Meaning |
|---|---|---|
| `temporal.operation` | string | Stable operation identifier |
| `temporal.duration_ms` | float | Finite non-negative elapsed milliseconds |
| `temporal.workflow_type` | string | Registered workflow type |
| `temporal.workflow_id` | string | Workflow ID; attached to diagnostics that name one execution (watchdog, workflow and activity lease outcomes) |
| `temporal.run_id` | string | Workflow run ID; attached to diagnostics that name one execution (watchdog, workflow and activity lease outcomes) |
| `temporal.replaying` | bool | Whether the reported activation was replaying history |
| `temporal.activity_id` | string | Activity ID; attached to activity lease records that name one attempt |
| `temporal.activity_type` | string | Registered activity type of that attempt |
| `temporal.attempt` | int | Activity attempt number reported by Core (one-based), distinguishing retries of one activity ID |
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
| `temporal.sdk.bridge` | `check_abi_version`, `echo`, `conformance_wait_ms`, `runtime_create`, `runtime_close`, `client_connect`, `client_disconnect`, `client_start_workflow_json`, `client_begin_start_workflow_json`, `client_poll_start_workflow_json`, `client_wait_start_workflow_json`, `client_wait_workflow_json`, `client_submit_json`, `client_await_call`, `client_release_call`, `client_cancel_workflow_json`, `client_reset_workflow_json`, `client_terminate_workflow_json`, `client_list_visibility_json`, `client_signal_workflow_json`, `client_query_workflow_json`, `client_update_workflow_json`, `client_poll_update_workflow_json`, `client_complete_async_activity_json`, `client_record_async_activity_heartbeat_json`, `worker_start`, `worker_try_poll_workflow`, `worker_wait_workflow`, `worker_complete_workflow_json`, `worker_reject_workflow_json`, `worker_try_poll_activity`, `worker_wait_activity`, `worker_wait_any`, `worker_wait_activity_completion_retry_backoff`, `worker_complete_activity_json`, `worker_reject_activity_json`, `worker_record_activity_heartbeat_json`, `worker_shutdown`, `replay_worker_start`, `replay_worker_feed_history`, `replay_worker_try_poll_workflow`, `replay_worker_wait_workflow`, `replay_worker_complete_workflow`, `replay_worker_reject_workflow`, `replay_worker_finish_input`, `replay_worker_finalize`, `replay_worker_dispose`; codec timing (Debug only): `workflow_activation_decode`, `workflow_completion_encode`, `activity_task_decode`, `activity_completion_encode` |
| `temporal.sdk.lifecycle` | `runtime_create`, `runtime_close`, `workflow_task_rejected`, `activity_task_failed`, `activity_task_rejected`, `activity_completion_retry`, `worker_run_started`, `worker_run_finished`, `worker_terminal_cleanup`, `worker_terminal_cleanup_failed`, `worker_shutdown`, `worker_shutdown_failed`, `activity_lane_detached`, `worker_shutdown_abandoned_work`, `worker_shutdown_teardown_detached`, `workflow_activation_completed`, `workflow_completion_diagnostic_failed`, `workflow_activation_rejected`, `workflow_poll_not_ready`, `activity_task_started`, `activity_cancellation_delivered`, `activity_task_completed`, `activity_async_handoff_accepted`, `activity_poll_not_ready` |
| `temporal.sdk.workflow` | `execution_created`, `workflow_started`, `workflow_completed`, `workflow_failed`, `workflow_task_failed`, `workflow_query_unhandled`, `workflow_query_completed`, `workflow_query_failed`, `workflow_update_unhandled`, `workflow_signal_unhandled`, `workflow_signal_received`, `workflow_signal_handled`, `workflow_cancelled`, `execution_evicted`, `activation_ignored`, `activate` |

The bridge source emits a completion record for every bridge call and a
second status record when the typed result is unsuccessful. That second record
uses the status-specific level policy above: normal `Not_ready` progress is
`Debug`, `Outstanding_tasks` during shutdown is `Warning`, and failures that
need intervention are `Error`. Applications that need a broad worker view
should filter `temporal.sdk.lifecycle`; applications diagnosing deterministic
workflow execution should filter `temporal.sdk.workflow`.

### Protocol codec timing

The OCaml half of the private JSON bridge can be timed inside a running
worker. While `temporal.sdk.bridge` is at `Debug`, each protocol codec pass
emits one `Debug` record, `codec operation completed`, carrying
`temporal.operation` and `temporal.duration_ms`:

| Operation | Pass |
|---|---|
| `workflow_activation_decode` | Copy of the native activation bytes and strict decoding, in the supervisor |
| `workflow_completion_encode` | The single canonical encoder pass over a workflow completion, in the worker adapter |
| `activity_task_decode` | Copy of the native activity-task bytes and strict decoding, in the supervisor |
| `activity_completion_encode` | The single canonical encoder pass over an activity completion, in the activity adapter; the supervisor submits those bytes without encoding again (#995) |

Below `Debug` these passes run without reading the clock or building tags
(`Observability.debug_timed`), so the instrumentation has no measurable cost
in a production configuration. The records exist for benchmark attribution
([#528]); see the live fan-out suite in
[benchmark-harness.md](benchmark-harness.md#live-fan-out-and-bridge-attribution).
Each activity completion is encoded exactly once, by
`activity_completion_encode`; a retried submission reuses those bytes.

[#528]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/528

### Activity lease events

The activity adapter reports a few lifecycle operations whose names describe
lease transitions rather than user-level activity outcomes. Their distinction
matters when diagnosing retries:

| Operation | Level | Meaning |
|---|---|---|
| `activity_task_started` | `Debug` | The adapter is about to dispatch a start task to its registered callback (or to reject it, for example an unregistered type). Emitted before any application code runs. |
| `activity_cancellation_delivered` | `Debug` | A Core cancellation for the running attempt was delivered to its context during a heartbeat. |
| `activity_task_completed` | `Debug` | The native worker accepted a terminal completion for the leased task. The adapter retires that lease; it does not call the activity again. |
| `activity_task_failed` | `Info` | The activity implementation returned an ordinary `Error`, which the adapter submitted as a failure completion, and the lease was retired. This is expected Temporal behaviour: the server applies the retry policy unless the error is non-retryable. `temporal.error_kind` carries the error's kind. |
| `activity_task_rejected` | `Warning` | The SDK refused the task itself (for example an unregistered activity type, an argument or result codec error, a malformed task, or a callback that raised) and submitted a non-retryable failure completion; the lease was retired. This is acknowledged task progress, so the worker loop continues; inspect `temporal.error_kind` for the stable rejection category. |
| `activity_completion_retry` | `Warning` | Submission of a terminal completion returned the explicitly retryable bridge status. The exact completion remains retained for a later drain, and the activity callback is not rerun. This event is a transient lease condition, not evidence that the activity ran twice. |
| `activity_async_handoff_accepted` | `Debug` | Core accepted `Will_complete_async`, moving the task from the worker lease to the namespace-bound asynchronous lease. This is not a terminal activity result; a later `Async_handle.complete`, `fail`, or `cancel` call must retire that lease. |

#### Activity correlation

The adapter's `activity_task_started`, `activity_cancellation_delivered`,
`activity_task_completed`, `activity_task_failed`, `activity_task_rejected`,
and `activity_async_handoff_accepted` records carry the attempt's identity (#500):
`temporal.workflow_id`, `temporal.run_id`, `temporal.workflow_type`,
`temporal.activity_id`, `temporal.activity_type`, and `temporal.attempt`.
The started record precedes the callback and the terminal record follows it,
so application logs written by the callback fall between two SDK records for
the same attempt. To tag those application logs directly, read the same
fields from `Temporal.Activity.Context.info`. The task token is never logged.

Two records do not carry this identity. A bare cancellation whose start this
worker never received completes with no workflow or activity ID, because
none is known. The worker loop's own `activity_completion_retry` record
carries only `temporal.error_kind`; use the adapter's correlated record for
the attempt. Each failed or rejected attempt produces exactly one
`activity_task_failed` or `activity_task_rejected` record, emitted by the
adapter; the worker loop does not log it again (#1024). Activity records are not
replay-tagged: activities do not replay.

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
| `workflow_failed` | `Error` | A non-task workflow error caused the SDK to buffer a terminal failure command. Inspect `temporal.error_kind`; the event is local failure evidence, not a server-side failure classification. |
| `workflow_task_failed` | `Error` | A codec, bridge, or defect error caused the SDK to discard the activation's commands and fail the workflow task. The run remains open for replay or recovery; no terminal workflow failure command is buffered. |
| `workflow_cancelled` | `Info` | A cancellation activation was received for a non-terminal execution and the SDK is emitting its terminal cancellation command. It does not mean that the server has already observed the completion. |
| `execution_evicted` | `Debug` | Core asked the SDK to remove an execution from its sticky cache. The execution context is shut down and no workflow commands are produced for that eviction activation. |
| `activation_ignored` | `Warning` | A later activation arrived for an execution already removed from the cache. The SDK intentionally ignores it and returns no commands; repeated occurrences indicate a stale or out-of-order delivery that needs investigation. |
| `activate` | `Debug` | One activation batch finished local processing. `temporal.job_count`, `temporal.command_count`, `temporal.workflow_type`, and `temporal.duration_ms` describe that batch, including a zero-command ignored batch. |

`workflow_started`, `workflow_completed`, `workflow_failed`, and
`workflow_cancelled` also carry `temporal.replaying` (#500). Core re-runs
workflow code when it rebuilds an evicted run from history, so the same
`workflow_completed` can be emitted again for a run whose completion
Temporal already recorded. Count business outcomes only from records with
`temporal.replaying=false`, or better, from the server-side workflow result;
these records are local evidence that a command was buffered.

Interaction events distinguish admission from handler completion. A matching
signal emits `workflow_signal_received` when it is queued on the owning
scheduler and `workflow_signal_handled` only after the handler returns `Ok ()`.
An absent signal handler emits `workflow_signal_unhandled` and fails the
workflow task, leaving the run open. An undecodable payload or more than one
payload emits `workflow_signal_received` and then fails the task through a
`Codec` error (`workflow_task_failed`), without `workflow_signal_handled`. This is the
deliberate fail-closed signal policy described in
[interactive workflows](interactive-workflows.md#unknown-and-undecodable-signals).
A signal handler error emits
`workflow_failed` for a terminal workflow error or `workflow_task_failed`
for a task failure, without a successful handled event. Queries are
synchronous and do not fail the workflow: `workflow_query_completed` means
that the output was encoded,
`workflow_query_failed` records a typed handler or encoding error, and
`workflow_query_unhandled` means that no handler was registered. Each query
outcome is still returned to the caller as a query response.

The current update event set is deliberately smaller. A missing update handler
emits `workflow_update_unhandled` at `Error` level and returns a rejected
update response. Ordinary validation or input errors reject the update without
a separate named workflow log operation. A bridge or defect error in a
validator or implementation can instead fail the workflow task and emit
`workflow_task_failed`; an accepted update's codec error can reject its update
response. Absence of `workflow_update_unhandled` must therefore not be read as
proof that an update was accepted. Acceptance and completion remain protocol
responses rather than separate log operations so callers can correlate them by
`protocol_instance_id`.

### Native worker lifecycle and completion events

Lifecycle-source records describe the worker adapter's ownership and cleanup
boundaries. They help separate a completion that was accepted by the native
adapter from a public `Worker.run` or `Worker.shutdown` result:

| Operation | Level | Meaning |
|---|---|---|
| `workflow_activation_completed` | `Debug` | The native supervisor accepted a retained workflow completion and the adapter retired that completion. A task failure or Core eviction removes the execution from the adapter registry; a terminal workflow completion remains available for queries until eviction. This is not a server-side workflow-result acknowledgement. Carries `temporal.workflow_id`, `temporal.run_id`, and `temporal.replaying` for correlation with the workflow history (#500). |
| `workflow_completion_diagnostic_failed` | `Warning` | A diagnostic callback raised after the supervisor accepted a workflow completion. The adapter contains the observer exception, so the completion stays accepted. |
| `workflow_activation_rejected` | `Warning` | The adapter submitted an SDK-generated failure completion for a malformed or otherwise rejected activation, and the native supervisor accepted that rejection. `temporal.error_kind` identifies the stable reason; a transport failure that leaves the completion pending does not emit this event. Carries `temporal.run_id`, and `temporal.workflow_id` while the run is cached (#500). |
| `workflow_activation_deadline_exceeded` | `Error` | Workflow source. The activation watchdog found an activation running workflow code for at least the configured deadline and released its lease with the adapter's failure completion (#493). Carries `temporal.workflow_type`, `temporal.workflow_id`, `temporal.run_id`, `temporal.duration_ms` (a lower bound), and a `temporal.error_kind` naming what was acknowledged: `workflow_task_failed` (an ordinary activation's workflow task was failed), `workflow_queries_failed` (a query-only activation's queries were answered with failures; the task was not failed), `eviction_acknowledged` (an eviction-only activation received its empty acknowledgement), or `completion_unacknowledged` (the replacement completion could not be delivered). Emitted once per worker lifetime; the worker is unhealthy and should be restarted. |
| `workflow_activation_late_completion_dropped` | `Warning` | The code of an activation the watchdog already failed finally returned; its completion was dropped without a native call and its run removed. |
| `workflow_task_rejected` | `Warning` | The public worker observed an adapter rejection whose failure completion already retired the workflow lease. The worker loop treats that as progress and continues polling; a rejection that did not retire its lease is returned as a worker error instead. |
| `worker_run_started` | `Info` | One invocation of `Temporal.Worker.run` acquired the run ownership guard and began polling. It does not mean that a workflow or activity task is currently available. |
| `worker_run_finished` | `Info` | That polling invocation returned and released the run guard. It may have stopped because shutdown was requested or because the loop returned an error; inspect the public `result` rather than treating this event as success. |
| `worker_terminal_cleanup` | `Info` | A detached cleanup retry received `Ok` from native shutdown. It then attempts to discard its OCaml-owned maps and clear cleanup-pending state. The original public shutdown path does not emit this event. |
| `worker_terminal_cleanup_failed` | `Error` | Native terminal cleanup returned `Error` or cleanup raised. A returned native error still proves the force-release boundary, so the adapter maps are discarded and cleanup-pending is cleared. An exception leaves cleanup-pending set for later retry or finalization; inspect `temporal.error_kind` when present. |
| `worker_shutdown` | `Info` | A bounded public worker shutdown finished its native teardown before returning (#495). It may still have abandoned work, which `worker_shutdown_abandoned_work` reports separately. Repeated shutdown calls are cached and do not represent new native work. |
| `worker_shutdown_failed` | `Error` | The native release returned an error, or an unexpected exception escaped it before a typed result was returned (`temporal.error_kind` is then `exception` and the cleanup path is scheduled separately). |
| `activity_lane_detached` | `Warning` | After a stop, an activity callback was still running when the grace period ended, so `Worker.run` returned without joining its activity Domain (#495). The callback keeps running and its result will be discarded. |
| `worker_shutdown_abandoned_work` | `Warning` | Bounded shutdown stopped waiting for a lane at the end of the grace period (#495). `temporal.error_kind` is `activity_callback`, `workflow_activation`, or `native_call` (a lane blocked in a native call). The abandoned task is failed so Temporal retries it; the process should be restarted. |
| `worker_shutdown_teardown_detached` | `Warning` | The teardown timeout elapsed before native teardown finished (#495). Shutdown returned; the teardown continues on its own SDK thread. |

Submission and acceptance are separate: the SDK emits
`workflow_activation_completed` only after Core accepted the completion,
never when it was merely encoded or submitted. A completion whose submission
failed retryably stays retained and emits nothing until a later submission is
accepted; `workflow_completed` (workflow source) is emitted earlier, when the
command is buffered, and therefore never by itself proves acceptance.

The workflow and activity task-rejection events are lease outcomes, not
application-level retries. A `Warning` for an acknowledged rejection means the
worker can continue polling, while `activity_completion_retry` specifically
means that the exact activity completion remains retained for a safe later
submission. Neither event means that the callback was invoked twice.

Latency is measured around the local OCaml operation with the portable Unix
wall clock, expressed as fractional milliseconds, and clamped to zero if the
clock moves backwards. The SDK currently attaches this tag to bridge-operation
completion, protocol codec timing and local workflow activation records; worker and adapter lifecycle
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
latency, the replay flag, and, on the execution-scoped records named above,
the workflow and run IDs and, for activity lease records, the activity ID and
attempt. Applications that treat workflow or activity IDs as sensitive should
filter `temporal.workflow_id` and `temporal.activity_id` in their reporter. Events do not contain
payload bytes, workflow inputs or outputs,
credentials, Rust diagnostic strings, or arbitrary remote failure detail.
User-controlled string tags are capped at 256 bytes. Truncation preserves valid
UTF-8 character boundaries before adding the visible `...` suffix, and current
message prose is constant and bounded. The Rust bridge reduces Core/gRPC worker
and poll-lane failures to those constant categories before they reach C; the
OCaml worker adapter repeats the check before reporting or returning an error.
The private diagnostic text is discarded because this logging policy has no
path that is allowed to expose it.

Client connection failures are the one deliberate exception (#833). A
`connection` bridge error from `Client.create` or `Worker.create` reads
`Temporal client connection failed (cause=<cause>): <detail>`. `<cause>` is a
closed category: `dns`, `refused`, `reset`, `timeout`, `tls`,
`unauthenticated`, `permission_denied`, `unavailable`, or `other`. `<detail>`
is the local transport error chain from the resolver, socket, HTTP/2, and TLS
layers, for example
`transport error: tcp connect error: Connection refused (os error 61)`. It is
capped at 512 bytes with a visible `...[truncated]` marker, and control
characters are escaped so it is always one line. When the server answers the
initial `GetSystemInfo` call with an error, only its gRPC code name is
included (`GetSystemInfo returned unauthenticated`); the server's status
message is never copied. Core's own rejection of connection options (URI,
headers, TLS settings) is a `configuration` error with a constant message.

## Temporal Core logs

Temporal Core, the Rust engine behind the native worker and client, emits its
own log records: poll and RPC retries, worker initialization and shutdown
progress, nondeterminism details, and similar. These are written to the
process's **stderr**, one line per record:

```text
ocaml-temporal core WARN temporalio_sdk_core::worker: <message> key=value ...
```

They do not go through `Logs`. Core emits them synchronously on its own Tokio
threads, and the bridge must never call OCaml from those threads, so the sink
is a fixed stderr writer rather than an OCaml reporter.

The `OCAML_TEMPORAL_CORE_LOG` environment variable selects the most verbose
level that is written. It is read once when each native runtime is created,
that is, by each `Client.create` or `Worker.create` against a real server:

| Value | Effect |
|---|---|
| unset or empty | `warn` (default) |
| `off` (or `none`) | Core records are discarded |
| `error`, `warn`, `info`, `debug`, `trace` | Temporal crates are logged at that level; third-party transport crates (tonic, hyper, h2, rustls) are capped at `warn` |

Values are case-insensitive. Any other value makes runtime creation fail with
a `configuration` bridge error that names the variable without echoing the
rejected value.

Each line is bounded to 2,048 bytes plus the newline; longer records end with
`...[truncated]`. Control characters, including newlines, are escaped, so a
record cannot forge further lines. Fields are sorted by key. A failed stderr
write is ignored, and a formatting panic is contained.

Stderr writes happen on a dedicated per-runtime writer thread, never on the
Core thread that emitted the record. Core only places the formatted line in
a bounded queue of 1,024 lines. If stderr is a pipe or socket whose reader is
slow or stalled, the queue fills and further records are **dropped** rather
than slowing Core. Once the writer catches up (or after one idle second, or at
shutdown) it writes one summary line:

```text
ocaml-temporal core WARN ocaml_temporal_core_bridge: N Core log records dropped because the stderr writer fell behind
```

Closing a client or worker waits at most 500 ms for queued lines to be
flushed. A writer still blocked on stderr after that is abandoned, so a stuck
stderr can never hang shutdown. Its last queued lines are written only if
stderr unblocks before the process exits. Logging therefore never affects
worker progress or SDK operation latency beyond that bounded close.

Unlike `Logs` events, Core records are not filtered for privacy. They can
include workflow IDs, run IDs, task queues, activity types, and failure
messages that come from workflow code or from the server, and `debug` or
`trace` records can describe activations in detail. Set
`OCAML_TEMPORAL_CORE_LOG=off` where that is unacceptable.

Every SDK report passes through one exception shield. If an application
reporter or formatter raises, the SDK discards that record and returns the
same `result`, commands, or exception it would have produced without logging.
Logging therefore adds diagnostics without changing the public typed-error
model or deterministic command decisions.

Workflow-runtime reports also run with the Domain-local workflow context
temporarily masked. A reporter that calls a workflow API re-entrantly therefore
receives the normal outside-workflow behavior and cannot append deterministic
commands to the activation being reported.

## Worker metrics

Temporal Core instruments every worker and client it runs. The SDK exports
those instruments, with Core's names and labels, through a Prometheus scrape
endpoint on a shared `Temporal.Runtime` (#500):

```ocaml
let ( let* ) = Result.bind

let start ~target_url ~namespace ~task_queue ~workflows ~activities =
  let* metrics =
    Temporal.Runtime.Metrics.prometheus ~bind_address:"0.0.0.0:9464"
      ~global_tags:[ ("deployment", "orders-blue") ] ()
  in
  let* runtime = Temporal.Runtime.create ~metrics () in
  let* worker =
    Temporal.Worker.create ~runtime ~target_url ~namespace ~task_queue
      ~workflows ~activities ()
  in
  (* Worker.run, then Worker.shutdown before Runtime.shutdown. *)
  Ok (worker, runtime)
```

Every client and worker created with `~runtime` reports to the same endpoint,
`http://<bind_address>/metrics`. Clients and workers that own their runtime
(no `~runtime`) export nothing. `Runtime.metrics_address` returns the bound
address, resolving a requested port `0`. The endpoint is bound before
`Runtime.create` returns: an address already in use is a `Bridge` error that
names the address and leaves nothing running. It stops when
`Runtime.shutdown` has destroyed the runtime.

This is the same mechanism and naming the official SDKs use (their runtime
telemetry options feed Core's Prometheus exporter), so existing Temporal SDK
dashboards and alerts apply unchanged. OTLP/OpenTelemetry push export,
custom metric prefixes, and application-defined metrics are not offered yet;
`Runtime.Metrics` is where they would be added.

### Metric set

Names carry the `temporal_` prefix. Durations are histograms in
milliseconds unless `durations_as_seconds` is set; `unit_suffix` appends the
unit and `counters_total_suffix` appends `_total` to counters, as in the
official SDKs.

| Area | Metric (type) | Use |
|---|---|---|
| Client RPCs | `temporal_request`, `temporal_request_failure` (counters); `temporal_request_latency` (histogram) | Server call volume, errors, and latency by `operation` and `status_code`. |
| Polling | `temporal_long_request`, `temporal_long_request_failure` (counters); `temporal_long_request_latency` (histogram); `temporal_num_pollers` (gauge) | Long-poll volume and poll or connection failures (`temporal_long_request_failure`); pollers by `poller_type`. |
| Poll outcomes | `temporal_workflow_task_queue_poll_succeed`, `temporal_workflow_task_queue_poll_empty`, `temporal_activity_task_received`, `temporal_activity_poll_no_task` (counters) | Task throughput and idle polls. |
| Saturation | `temporal_worker_task_slots_available`, `temporal_worker_task_slots_used` (gauges, by `worker_type`); `temporal_workflow_task_schedule_to_start_latency`, `temporal_activity_schedule_to_start_latency` (histograms) | A worker with no free slots and growing schedule-to-start latency is saturated. |
| Workflow tasks | `temporal_workflow_task_execution_latency`, `temporal_workflow_task_replay_latency` (histograms); `temporal_workflow_task_execution_failed` (counter, by `failure_reason`) | Task latency, replay cost, and task failures, including nondeterminism. |
| Workflow outcomes | `temporal_workflow_completed`, `temporal_workflow_failed`, `temporal_workflow_canceled`, `temporal_workflow_continue_as_new` (counters); `temporal_workflow_endtoend_latency` (histogram) | Terminal outcomes reported by the worker that completed them. |
| Activities | `temporal_activity_execution_latency`, `temporal_activity_succeed_endtoend_latency` (histograms); `temporal_activity_execution_failed` (counter); `temporal_local_activity_*` equivalents | Activity latency and failures. |
| Sticky cache | `temporal_sticky_cache_size` (gauge); `temporal_sticky_cache_hit`, `temporal_sticky_cache_miss`, `temporal_sticky_cache_total_forced_eviction` (counters) | Cache pressure; forced evictions cause replays. |
| Lifecycle | `temporal_worker_start` (counter) | Worker starts. |

Core creates a family on its first observation, so an idle worker exports a
subset. The live regression requires the 19 families that a worker which ran
one workflow and one activity must export.

### Labels and cardinality

Series carry only bounded labels: `service_name` (constant
`temporal-core-sdk`), `namespace`, `task_queue`, `workflow_type`,
`activity_type`, `poller_type`, `worker_type`, `eager`, `failure_reason`,
`operation`, `status_code`, the histogram `le`, and the application's global
tags. Cardinality is therefore the product of task queues, registered
workflow and activity types, and small closed enumerations. Workflow IDs, run
IDs, activity IDs, payloads, failure messages, and credentials are never
labels; use the correlated log records above to diagnose one execution.
Global tags are bounded (at most 16, 64-byte names, 256-byte values) and may
not reuse a Core label name. The live regression fails on any other label and
checks that the execution's IDs never appear in the scrape.

### Failure isolation and resource use

The scrape server is Core's own Prometheus exporter, running as one task on
the runtime's background I/O threads behind the SDK's own listener. The SDK
accepts scrape connections on `bind_address` and relays each one to Core's
exporter on a private loopback port, so an `accept()` error (for example
descriptor exhaustion, or a client that resets before it is accepted) is
logged and retried with a backoff of at most one second instead of stopping
the endpoint (#1030). If Core's exporter itself stops, scrapes receive
`503 Service Unavailable` and one `error` record is logged; recreating the
runtime restores the endpoint. It never runs OCaml code and is not on
the workflow or activity path: instruments are updated in memory, so a slow,
failing, or absent scraper cannot block, fail, or reorder workflow and
activity execution or change durable behavior. Memory is bounded by the
label cardinality above. Each scrape connection is served by a short-lived
task; Core imposes no connection limit, so bind the endpoint to a loopback or
private interface (or put a scraping agent in front of it) rather than a
public one. The endpoint has no authentication or TLS.

### Monitoring and alerting example

```yaml
groups:
  - name: temporal-worker
    rules:
      - alert: TemporalWorkerPollFailures
        expr: sum by (task_queue) (rate(temporal_long_request_failure[5m])) > 0
        for: 5m
      - alert: TemporalWorkerSaturated
        expr: >
          min by (task_queue) (temporal_worker_task_slots_available{worker_type="WorkflowWorker"}) == 0
          and on (task_queue)
          histogram_quantile(0.95, sum by (task_queue, le)
            (rate(temporal_workflow_task_schedule_to_start_latency_bucket[5m]))) > 1000
        for: 10m
      - alert: TemporalWorkflowTaskFailures
        expr: sum by (task_queue, failure_reason) (rate(temporal_workflow_task_execution_failed[5m])) > 0
```

A poll failure shows as `temporal_long_request_failure` (and Core's stderr
warnings); a saturated executor as zero available slots with rising
schedule-to-start latency; a stuck activation additionally as the
`workflow_activation_deadline_exceeded` record and `Worker.health`.
Shutdown outcomes, including a teardown timeout, are reported by
`Worker.shutdown_with_report` and the `worker_shutdown*` lifecycle records
rather than by a metric, because the endpoint stops with the runtime.

### Tracing

Distributed tracing (OpenTelemetry spans across client, workflow, and
activity) is deferred. Core's internal spans reach operators only as the
stderr log records described above.

## Verification

`test/observability/test_logging.ml` installs an in-memory reporter and checks
the contract structurally: exact source and tag names, representative
bridge/workflow severity assignments, finite non-negative latency tags on
bridge and activation records, and the absence of raw byte payloads or request
JSON in message text and rendered tags. It also verifies that a reporter
exception cannot change a bridge result or workflow command batch. Run it with
`dune exec ./test/observability/test_logging.exe` or use the broader Makefile
test target.

`rust/core-bridge/tests/connection_diagnostics.rs` connects to a closed
loopback port and to an unresolvable `.invalid` host and checks the `refused`
and `dns` causes, the transport detail, and the message bound. It also checks
cause classification, level parsing, the Core log filter, and that a Core
record becomes one escaped, bounded line.
`rust/core-bridge/tests/core_log_env.rs` checks that every accepted
`OCAML_TEMPORAL_CORE_LOG` value creates a runtime and that an invalid value is
a `configuration` error. `rust/core-bridge/tests/core_log_queue.rs` blocks
the writer with a gated sink and checks that enqueuing never blocks on a full
queue, that drops are counted and reported in one summary line, and that
runtime close completes while the writer is blocked.
`test/unit/test_client_worker.ml` checks that the cause reaches the public
`Client.create` error.

Metrics (#500): `test/unit/test_metrics_config.ml` covers option validation
(bind addresses, tag names, values, count, uniqueness, and that no rejected
value is echoed) and the exact bridge document.
`rust/core-bridge/tests/metrics_exporter.rs` proves through the C ABI that a
configured exporter answers scrapes on its resolved ephemeral port, that
attached graphs receive Core's meter, that invalid documents and a busy port
publish nothing, and that the socket is closed once the last Core reference
is released. `test/bridge/test_runtime_metrics.ml` repeats the lifecycle
through the public `Runtime` API. The live regression
`test/integration/worker_metrics/regression.exe` (part of
`make test-temporal-live-regressions`) runs a workflow and an activity on a
worker attached to an exporting runtime, scrapes the endpoint, requires the
documented families, rejects any unexpected label or execution identifier
in the scrape, checks the global tag, and checks that every
`workflow_activation_completed` record for the run carries its workflow ID,
run ID, and `temporal.replaying=false`. `test/observability/test_logging.ml`
checks the `temporal.replaying` tag on replayed and fresh executions.
`test/runtime/test_native_activity_execution.ml` checks that activity lease
records carry the attempt identity, bracket the callback's own logs, never
contain the task token, and carry no identity for a bare cancellation;
`test/observability/test_tag_normalization.ml` checks bounding of the
activity tags.

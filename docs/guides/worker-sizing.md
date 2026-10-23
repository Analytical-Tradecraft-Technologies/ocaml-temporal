# Sizing a worker

This guide explains the settings that bound a worker's resource use, how they
interact, and which metrics show whether a worker is sized correctly. The
contract for every option is the documentation of `Temporal.Worker.Options.make`
in [`lib/public/worker.mli`](../../lib/public/worker.mli); this page does not
replace it.

The package is experimental. The numbers below are starting points derived from
the option contracts and the defaults, not results from a load test. The
sustained-load release gate ([#507]) does not exist yet, so measure your own
workload before relying on a configuration.

## What a worker runs concurrently

Each `Worker.t` has two lanes:

- **Workflow lane.** Workflow activations run one at a time, on the thread that
  called `Worker.run`. `max_concurrent_workflow_tasks` bounds how many workflow
  tasks Temporal Core holds for that lane, not how many run in parallel.
- **Activity lane.** Activity callbacks run on the worker's own activity
  Domain, **one at a time**. The worker grants Temporal Core exactly one
  remote and one local activity slot, and these are deliberately not
  configurable ([#777]): a larger slot count would let the server start
  activity timeouts for tasks that could only wait in a queue. [#492], which
  tracks the isolation of activity execution from workflow polling, remains
  open; no option raises activity concurrency within one worker.

To raise activity throughput, run more activity workers, for example several
activity-only worker processes on the same task queue. The
[examples](../../examples/README.md) already split one workflow-only worker
from one activity-only worker; the `split_worker_task_types` live regression
(#805) covers that shape. A slow activity therefore never occupies the
workflow lane, but it does block every other activity on the same worker until
it returns.

Network I/O runs on separate background threads (below). Workflow and activity
code never runs on those threads.

Each worker also holds OCaml Domains of its own: the supervisor that owns its
native handles and, while `Worker.run` is active, an activity-lane Domain if
the worker registers activities. A workflow-only worker uses only its
supervisor Domain; the activation watchdog is a thread on that Domain, not a
Domain of its own. Each client and each replay holds a supervisor Domain too.
OCaml 5 limits how many Domains a process can run. A creation that cannot
spawn its supervisor fails with an error naming the limit, and `Worker.run`
returns a retryable `domain_unavailable` error if it cannot spawn the
activity lane, so prefer more processes over many workers in one process. See
[Domain budget](../reference/native-worker-execution.md#domain-budget) for
the cost per instance and the practical worker counts per OCaml version.

## The options

All of these are labelled arguments of `Worker.Options.make`, which validates
them before any native resource exists and returns a typed defect naming the
option on a bad value. `Worker.options` reports the effective values,
including defaults, and is safe to log.

| Option | Default | What it bounds |
| --- | --- | --- |
| `max_cached_workflows` | 1000 | Sticky workflow cache entries. `0` disables sticky caching, so every task replays its full history. |
| `max_concurrent_workflow_tasks` | 1000 | Workflow tasks Core holds at once. At least 1, and at least 2 while the cache is enabled. Core admits at most `max 2 max_cached_workflows` tasks, so a small cache also lowers this limit. |
| `workflow_task_pollers` | `Fixed 2` | Open workflow-task long polls. `Fixed n` is split between the sticky and normal queues, so a caching worker needs `n >= 2`. `Autoscaling { minimum; maximum; initial }` applies its bounds to each queue separately. |
| `sticky_queue_schedule_to_start_timeout` | 10 s | How long a task waits on this worker's sticky queue before the server offers it to any worker. No effect without the cache. |
| `workflow_activation_deadline` | `` `After `` 2 s | Watchdog for workflow code that stops yielding; see [Health](#health-and-shutdown). |
| `graceful_shutdown_period` | 30 s | How long running work may continue after shutdown begins. |
| `shutdown_teardown_timeout` | 60 s | How long shutdown then waits for native teardown. |
| `max_heartbeat_throttle_interval`, `default_heartbeat_throttle_interval` | 60 s, 30 s | How often activity heartbeats reach the server. |
| `max_worker_activities_per_second` | unset | Remote activity tasks this worker polls per second. Eagerly dispatched activities bypass it unless they set `~do_not_eagerly_execute:true`. |
| `max_task_queue_activities_per_second` | unset | Server-side dispatch limit for the whole task queue. Workers that set different values overwrite each other, and setting it disables eager activity execution. |

Counts are capped at 1,000,000 and durations at one day. The live
`worker_resource_options` regression (#498) runs a worker with a one-entry
cache, two task slots and pollers, and a rate limit of one activity per
second, saturates it, and checks that every execution still completes.

### The cache

The sticky cache keeps each cached run's workflow state in the worker process,
so a new task for that run does not replay its history. When the cache is
full, Core evicts a run and its next task replays from the start; this costs
CPU and server reads, not correctness. Size the cache to the number of runs
that receive tasks often enough for replay to matter, bounded by the memory
you can give the process. No per-run memory figure has been measured for the
SDK; the [benchmark harness](../reference/benchmark-harness.md) has
exploratory memory suites, but they are not a sizing table.

## Background I/O threads

`Worker.create` and `Client.create` take `?io_threads`, an upper bound on the
background threads that instance uses for network I/O and server
communication. When omitted it is the host's available parallelism capped at
4, and it must be between 1 and 256. Each worker and client owns its own
threads unless they share a `Temporal.Runtime.t`: pass `~runtime` to every
`Client.create` and `Worker.create` in the process, and set the bound once on
`Runtime.create ?io_threads`. Passing both `~runtime` and `~io_threads` is a
typed defect.

A shared runtime is also the only way to export metrics (below). In a process
with several workers and clients, prefer one shared runtime.

## Small worker

For development, CI, or a low-volume queue on a small container:

```ocaml
let ( let* ) = Result.bind

let small_worker ~target_url ~namespace ~task_queue ~workflows ~activities =
  let* options =
    Temporal.Worker.Options.make ~max_cached_workflows:100
      ~max_concurrent_workflow_tasks:10
      ~workflow_task_pollers:(Temporal.Worker.Options.Fixed 2) ()
  in
  Temporal.Worker.create ~options ~io_threads:1 ~target_url ~namespace
    ~task_queue ~workflows ~activities ()
```

- One I/O thread is enough for a handful of long polls.
- `Fixed 2` is the smallest poller setting a caching worker accepts.
- Keep the shutdown defaults unless your supervisor's termination deadline is
  shorter than their 90-second sum; shorten both together if it is.

## Production worker

For a production deployment, start from these rules and adjust from the
metrics:

1. **Split workflow and activity workers** into separate processes when
   activities are slow or numerous, and scale the activity processes
   horizontally, because each worker runs one activity at a time.
2. **Share one runtime per process** with a metrics exporter (below), and size
   its `io_threads` to the number of long polls the process keeps open. The
   default (parallelism capped at 4) suits most single-worker processes.
3. **Set the cache** to cover your active runs within the process memory
   limit. Keep `max_concurrent_workflow_tasks` at least 2 and no higher than
   `max 2 max_cached_workflows`, since Core ignores the excess.
4. **Use `Autoscaling` pollers** when task arrival is bursty, for example
   `Autoscaling { minimum = 1; maximum = 10; initial = 2 }`. Remember that a
   caching worker may keep up to `2 * maximum` polls open.
5. **Bound shutdown to your supervisor.** `Worker.shutdown` returns within
   `graceful_shutdown_period + shutdown_teardown_timeout` plus a fraction of a
   second. Make the supervisor's termination grace period (for example a
   Kubernetes `terminationGracePeriodSeconds`) longer than that sum, so the
   process is not killed mid-teardown. A killed process is still recovered by
   Temporal's timeouts, more slowly.
6. **Rate-limit activities** with `max_worker_activities_per_second` when a
   downstream system needs protection. Use the task-queue limit only when every
   worker on the queue sets the same value.

## Metrics to watch

Create the runtime with a Prometheus exporter and attach every worker and
client to it; workers that own their runtime export nothing. The
[observability reference](../reference/observability.md#worker-metrics) lists
the full metric set, labels and an alerting example. For sizing, watch:

| Signal | Metrics | Action |
| --- | --- | --- |
| Workflow lane saturated | `temporal_worker_task_slots_available{worker_type="WorkflowWorker"}` at 0, rising `temporal_workflow_task_schedule_to_start_latency` | Add workers, or check for slow workflow code (`temporal_workflow_task_execution_latency`) |
| Activity lane saturated | Rising `temporal_activity_schedule_to_start_latency`, activity slots at 0 | Add activity workers; one worker runs one activity at a time |
| Cache too small | Rising `temporal_sticky_cache_total_forced_eviction` and `temporal_sticky_cache_miss`, high `temporal_workflow_task_replay_latency` | Raise `max_cached_workflows` if memory allows |
| Idle pollers | High `temporal_workflow_task_queue_poll_empty` or `temporal_activity_poll_no_task` | Fewer pollers or workers are enough |
| Connectivity | `temporal_long_request_failure`, `temporal_request_failure` | Check the endpoint and network, not the sizing |
| Task failures | `temporal_workflow_task_execution_failed` by `failure_reason` | Nondeterminism or stuck code, not capacity |

## Health and shutdown

`Worker.health` reports `Stuck_workflow_activation` once the activation
watchdog has failed a workflow task whose code stopped yielding. OCaml code
cannot be interrupted, so the stuck code keeps the workflow lane busy and the
state is sticky: a liveness probe should fail on it, and the supervisor should
replace the process. `Worker.shutdown_with_report` reports what a bounded
shutdown abandoned (activity callbacks and workflow activations still running
when the grace period ended); Temporal retries their tasks, normally on another
worker. Activities are delivered at least once, so their side effects must be
idempotent.

[#492]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/492
[#507]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/507
[#777]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/777

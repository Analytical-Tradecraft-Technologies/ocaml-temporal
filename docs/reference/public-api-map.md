# Public API map

The supported OCaml API is the wrapped `Temporal` library. The source of truth
for its module list is [`lib/public/temporal.ml`](../../lib/public/temporal.ml):
that file is an explicit allow-list, not an automatic export of every
implementation file in `lib/public/`. The map below groups the same modules by
where application code normally uses them.

## Choose a module by execution context

| Context | Modules | Use them for |
| --- | --- | --- |
| Workflow code | `Temporal.Workflow`, `Temporal.Activity`, `Temporal.Child_workflow`, `Temporal.Future`, `Temporal.Condition`, `Temporal.Scope`, `Temporal.Workflow_context`, `Temporal.Time`, `Temporal.Duration` | Defining deterministic work, making replay-safe time and pseudo-random choices, scheduling Temporal operations, waiting for results, and keeping execution-local state |
| Application startup and shutdown | `Temporal.Client`, `Temporal.Worker`, `Temporal.Runtime`, `Temporal.Runtime_info` | Connecting to Temporal, registering executable definitions, running the worker, optionally sharing background I/O threads, and checking the linked bridge |
| Values crossing a Temporal boundary | `Temporal.Codec`, `Temporal.Payload`, `Temporal.Error`, `Temporal.Result_syntax` | Encoding typed values, inspecting opaque payloads, representing expected failures, and composing `result` values |
| Signals, queries, and updates | `Temporal.Signal`, `Temporal.Query`, `Temporal.Update`, `Temporal.Interaction` | Defining typed interactions, registering handlers, and testing deterministic local dispatch |
| Application tests | `Temporal.Testing` | Running registered workflows and activities in-process with a time-skipping virtual clock, stubbing activities and child workflows, and driving signals, queries, updates, and cancellation without a Temporal Server |

The same module can be used by a workflow helper and by registration code when
its contract allows it, but the execution context still matters. In
particular, workflow code must remain deterministic: it must not read the host
clock, perform I/O, use randomness, or mutate process-global state. See the
[workflow guide](../guides/workflows.md) and the [runtime invariants](runtime-invariants.md)
for those rules.

## Public module responsibilities

### Authoring and scheduling

- `Temporal.Workflow` defines local or remote workflows. `start_sleep` creates
  a durable timer and returns a future; `sleep` is its wait-and-return
  convenience form. `Workflow.now ()` reads the activation timestamp supplied
  by Temporal and never falls back to host wall-clock time. `random_int ~bound`
  makes a replay-safe pseudo-random choice from the execution-local stream.
  `current_deployment_version ()` reports the deployment and build identity
  selected for the current task, or `None` when no versioned task metadata is
  available; it is diagnostic metadata, not a replacement for replay-safe
  patching. `info ()` returns an abstract `Workflow.Info.t` with the run's
  identity (workflow and run IDs, first run ID, type, namespace, task queue,
  attempt, parent, start time) and the current activation's replay flag, history
  length and size, and continue-as-new suggestion, all taken from Temporal's
  activations; `is_replaying ()` is the shorthand for replay-aware logging.
  `upsert_search_attributes` merges encoded values into the
  execution's indexed search attributes; the update becomes visible after the
  workflow task is accepted. `continue_as_new` ends the current run and starts
  a successor. `patched ~id`
  introduces a new deterministic branch while allowing histories created
  before that marker to replay the old branch. `deprecate_patch ~id` is the
  later unit-returning lifecycle marker used while phasing that branch gate
  out; see [workflow patching](workflow-patching.md).
- `Temporal.Activity` defines local, remote, context-aware, and asynchronous
  activities. `start` schedules an activity without waiting; `execute` is the
  convenience composition of `start` and `Future.await`. Keep the handle from
  `start_handle` when the workflow may need to cancel one exact activity or
  inspect its future separately; `Retry_policy` and the cancellation policy
  control the durable command options described in the [operation policy
  reference](durable-operation-policies.md). `Priority` adds validated
  scheduling metadata: a lower positive priority key is preferred, while an
  optional fairness key and weight guide best-effort queue fairness. Activity
  callbacks are the boundary for external I/O. Their context/heartbeat rules
  are described in the [activity reference](native-activity-execution.md). An
  asynchronous callback
  returns `Completed`, `Failed`, or `Will_complete_async`; after the handoff,
  `Async_handle` provides terminal `complete`, `fail`, and `cancel` operations
  plus non-terminal `heartbeat`, while `Async_context` is only used to obtain
  that retained capability and, through `Async_context.info`, the same
  `Activity.Info.t` that a synchronous callback reads. The attempt-scoped
  `Context` instead supplies
  copied heartbeat details and timeout metadata for callbacks that complete
  during dispatch, and `Context.info` returns an abstract `Activity.Info.t`
  with the namespace, scheduling workflow, activity ID and type, attempt,
  local-activity flag, Core-reported scheduling timestamps, and effective
  schedule-to-close, start-to-close, and heartbeat timeouts (rounded up to
  whole milliseconds).
- `Temporal.Child_workflow` schedules a child workflow and exposes its typed
  future. Use its operation handle when the parent must cancel one exact child;
  child retry and cancellation policies are passed to the durable command; see
  the [operation policy reference](durable-operation-policies.md) for their
  parent-side behavior.
  Child scheduling and completion are durable workflow operations, not ordinary
  function calls; the [workflow guide](../guides/workflows.md) marks the
  authoring/native-support boundary.
- `Temporal.Future` combines and observes workflow-owned results. A future is
  tied to the execution that created it; `await` suspends the current workflow
  fiber rather than blocking an OS thread. `Temporal.Condition` waits on
  replay-safe OCaml state, while `Temporal.Scope` adds typed cancellation to
  observation of futures. Activities and child workflows started with
  `~scope` also register exactly-once server-cancellation hooks; timers and
  unscoped operations remain observation-only. See the
  [workflow-local cancellation scope reference](workflow-scopes.md).
- `Temporal.Workflow_context` provides execution-local values for workflow
  state. Use it instead of module-level mutable state when the value belongs
  to one workflow execution.

### Boundaries and failure values

- `Temporal.Codec` pairs an OCaml type with a payload encoding. The standard
  codecs are convenient defaults, not a requirement that all payloads be
  JSON; `Temporal.Payload` remains opaque bytes plus encoding metadata.
- `Temporal.Error.t` is the typed failure channel for expected operational
  failures. Use `Error.view`, `Error.kind`, and `Error.message` for stable
  inspection. Exceptions are for programmer defects or violated internal
  invariants, not normal Temporal outcomes.
- `Temporal.Result_syntax` supplies ordinary `result` composition. It does
  not introduce workflow effects or change error ownership.
- `Temporal.Time` and `Temporal.Duration` make timestamp and timer units
  explicit. Workflow time is integer seconds plus normalized nanoseconds;
  workflow timer durations are non-negative whole milliseconds. See the
  [workflow-time reference](workflow-time.md).

### Application lifecycle and interactions

- `Temporal.Client` starts typed workflow executions, optionally attaching
  validated `memo` and `search_attributes` payloads. A caller-owned
  `request_id` makes an uncertain start safe to retry as the same logical
  request. `?id_conflict_policy` (`` `Fail `` by default, `` `Use_existing ``,
  or `` `Terminate_existing ``) chooses what happens when the workflow ID
  already has a running execution: a typed already-started error whose
  existing run `Client.already_started` returns, a handle for the running
  execution (`Client.started` is `false`), or termination of that execution
  and a new run. The client retains the exact workflow/run identity, rebuilds a
  typed handle for a `Continued_as_new` successor with `Client.follow`,
  requests exact-run cancellation, reset, or termination. `Client.reset`
  stops an exact run at a workflow-task event boundary and returns a new
  execution identity; call `Client.follow` explicitly if you want to await
  that successor. The client also sends typed signals and output-only or
  exactly-one-input queries. `Client.query_with_input` encodes the typed query
  argument before transport; the client lists bounded visibility results and
  waits for typed terminal outcomes. `Client.start_update` waits until Temporal
  accepts one typed workflow update and returns an opaque handle;
  `Client.wait_update` polls that
  exact update until it has a typed outcome, while `Client.update_id` exposes
  the server-correlated update ID for diagnostics and retry bookkeeping.
  `Client.follow`
  only validates and combines the existing client, workflow definition, and
  successor identity; it does not start or implicitly follow a run. A
  successful cancel, terminate, or signal acknowledges the server request; it
  does not claim that workflow code has already processed an asynchronous
  request. A query returns the decoded output-only or typed-input handler
  result or a typed error. Call `Client.shutdown` when the client is no longer
  needed to release
  its native graph; shutdown is idempotent and retains a teardown failure so
  cleanup problems are not silently discarded. Shutdown does not wait for or
  fail because of a concurrent `Client.start`: an in-flight native start is
  aborted, and that caller receives a non-retryable `bridge` error saying
  Temporal did not prove whether the start was accepted, including its
  workflow and request IDs for reconciliation. One native client keeps at
  most 64 starts in flight and waits on at most 64 distinct runs at once; a
  call beyond either bound is rejected before reaching Temporal with a
  retryable `bridge` error that `Client.is_at_capacity` recognizes, and the
  client stays usable.
- `Client.create` and `Worker.create` accept `?io_threads`, an upper bound on
  the background threads each instance uses for network I/O and server
  communication; the SDK may use fewer (#832). The default is the host's
  available parallelism capped at 4; a value outside 1..256 is a typed defect
  returned before anything is allocated. The bound applies per client and per
  worker.
- `Temporal.Runtime` is an explicit, shareable background I/O runtime (#832).
  `Runtime.create ?io_threads ()` starts it, and passing it as `?runtime` to
  `Client.create` and `Worker.create` makes those instances share its threads
  instead of starting their own (`?runtime` together with `?io_threads` is a
  typed defect). Each attached client or worker stays attached until its own
  `shutdown` returns; `Runtime.attached` reports the count, and
  `Runtime.shutdown` returns a typed defect while it is non-zero, so
  instances must be shut down before their runtime. A shut-down runtime
  rejects new attachments with a typed defect. Without `?runtime`, behavior
  is unchanged.
- `Temporal.Worker` registers workflows, activities, and the signal, query, and
  update handlers attached to each workflow registration. It owns one
  supervisor graph, runs the poll loops, and performs idempotent shutdown.
  `Worker.request_shutdown` is the signal-handler-safe way to make `run`
  return; call `Worker.shutdown` afterwards to release the worker.
  `Temporal.Worker.Options` provides typed, immutable resource and worker
  routing settings, including legacy build-ID and deployment-based versioning;
  see the [worker versioning reference](worker-versioning.md). A
  successfully shut-down worker is not reusable: calling `Worker.run` again
  returns a typed `bridge` error ("worker is shut down") on both the mock and
  native backends without polling. Create a new worker for a new polling
  lifecycle. The interaction handler registration modes and their
  current native limitations are described in the [interactive-workflow reference](interactive-workflows.md).
  The final executable remains an OCaml application; Rust is a private linked
  implementation detail.
- `Temporal.Runtime_info` is for installation and diagnostics. Its ABI check
  confirms that the Rust bridge linked into the executable matches the OCaml
  package expectation; it is not a worker health probe.
- `Temporal.Signal`, `Temporal.Query`, and `Temporal.Update` define typed
  interaction names and codecs. Queries may be output-only or accept exactly
  one typed input; their handlers remain synchronous and read-only.
  `Temporal.Interaction` is the deterministic,
  synchronous local dispatcher for tests. Native interaction delivery has a
  narrower experimental boundary; see the [interactive-workflow reference](interactive-workflows.md).
- `Temporal.Testing` is the in-process workflow test environment. It drives
  the same private workflow runtime as a native worker against a
  deterministic server simulator, skipping virtual time whenever every
  workflow is blocked on a timer or activity retry. `mock_activity` and
  `mock_workflow` stub definitions by name; `start`, `signal`, `query`,
  `update`, `cancel`, `skip`, and `result` drive a workflow step by step. It is
  independent of `Client` and `Worker`, whose `mock://` target remains a
  plumbing-only backend that never runs workflow code. The simulated
  semantics and their limits are listed in the module documentation and the
  [workflow guide](../guides/workflows.md#test-workflows-in-process).

## What is not public

Source files such as `Backend`, `Native_worker`, the private
`Temporal_sdk_kernel` library, the runtime libraries, the mailbox, the
supervisor, and the Rust/C bridge may be needed to build the package, but they
are not supported application modules. Do not depend on
their records, constructors, JSON documents, Rust handles, protobuf values, or
generated interfaces. The installed-package rules and regression test are
described in the [package-boundary reference](package-boundary.md).

When a feature is missing from this map, first check whether it belongs in an
existing public module. Publishing a private implementation module is a
package-surface change that requires an explicit design decision, updated
boundary tests, and documentation of its ownership and compatibility
contract.

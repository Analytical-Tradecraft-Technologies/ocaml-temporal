# Workflow task and execution failures

Before stable v1, workflow workers change their default failure behavior:
repairable code and SDK defects fail the current **workflow task** and leave
its workflow execution open. A compatible corrected worker can replay and
complete the same workflow ID and run ID. Applications must no longer use
`Error.defect` as shorthand for a deliberate business failure.

| Origin | Completion behavior |
| --- | --- |
| Workflow body returns `Error.make ~category:Workflow ...` | Terminal workflow failure; preserves `non_retryable` and details |
| Workflow propagates an expected activity, child, cancellation, or timeout error | Existing terminal failure/cancellation semantics remain |
| Unexpected body/scheduler/signal exception, or a propagated `Defect`, `Bridge`, or `Codec` error | Failed workflow task, no commands |
| Workflow result encoder returns an error or raises | Failed workflow task, regardless of the encoder's error category |
| Missing workflow registration, malformed activation, invalid resolver state, adapter exception before submission | Failed workflow task, no commands |
| Query lookup, handler, or codec failure | Failed query answer for the original query ID; does not fail the workflow |
| Missing update handler, input rejection, validation rejection, or deliberate typed handler failure | Rejected update response; does not fail the workflow |
| Unexpected update exception/invariant failure, or codec failure after acceptance | Failed workflow task; discard speculative acceptance and commands |
| Eviction | Successful empty acknowledgement |

For example, a business rule that should close the execution returns:

```ocaml
Error
  (Temporal.Error.make ~category:`Workflow ~non_retryable:true
     ~message:"order rejected" ())
```

`non_retryable=false` permits Temporal's configured workflow retry policy to
start another run. It does not itself install a retry policy. The current
public `Client.start` has no workflow-retry-policy argument; absent a server
retry policy, either flag closes the original run. Explicit child retry
policies remain supported and are passed to Core unchanged. A defect's
`non_retryable` diagnostic field does **not** prevent workflow-task retries,
since it is no longer emitted as a workflow-execution failure. There is no
worker-wide opt-in to make unexpected defects terminal. A workflow that
intentionally converts an error to category `Workflow` makes that decision
explicitly in its own application code.

## Protocol and ownership

The private completion object retains `run_id` and `commands`. Optional
`task_failure` carries the existing structured failure type. Absent or null
means a successful command completion, preserving existing successful JSON.
A non-null value requires `commands=[]`; both OCaml and Rust validate this.
C continues transporting the same owned JSON buffer through the existing ABI.
Rust maps this to pinned Core `WorkflowActivationCompletion::fail`, with
`WorkflowWorkerUnhandledFailure`, rather than a successful completion holding
`FailWorkflowExecution`. Query-only and eviction completions reject this
failed status at the activation-aware Core boundary.

A failed task shuts down its unsafe OCaml generation immediately: pending
updates, continuations, local state, and every buffered command are discarded.
The run-map ownership record and copied completion remain until acknowledgement;
Core's subsequent eviction/initialization reconstructs a fresh execution.
Native conversion/decode rejection already uses Core task failure and keeps its
existing exact-document lease rules.

A completion exception can occur after Core accepted the value. The adapter
therefore retains the exact completion, never substitutes a failure command,
and never reruns workflow code. The supervisor's existing explicit retryability
classification decides whether retry is allowed. Terminal native shutdown
releases unresolved ownership before the OCaml pending map is discarded.

## Regression and live recovery gates

`make test-workflow-task-failure` runs the bilateral JSON/Core conversion tests,
OCaml runtime/adapter/observability/supervisor regressions, and Core retry-policy
tests. These cover command discard, fresh sequences on reconstruction, query
and update rejection, business retryability, failed-task acknowledgement retry,
and an accepted completion whose acknowledgement is lost. The existing Rust
`replay_abi` suite also replays five retained synthetic live histories with the
corrected timer/result commands and intentional business failures. It rejects
unexpected eviction reasons and requires natural replay finalization.

`make test-temporal-task-failure-live OCAML_VERSION=5.5` builds three separate
executables and runs the pinned Temporal/PostgreSQL Compose stack. Use a fresh
`TEMPORAL_COMPOSE_PROJECT` and, for concurrent local work, an unused
`TEMPORAL_FRONTEND_PORT`. The controller refuses an existing project's
containers or volumes. It stops and removes only its own processes and stack.
`TASK_FAILURE_DEV_IMAGE` may select an already built development image; use the
matching Make `COMPOSE_RUN` override to compile against that same image.

The broken worker raises after a committed timer and a speculative second
timer, raises from another workflow's output encoder, and omits a third
workflow's registration. The controller requires each exact run to remain
running with a durable workflow-task failure and no terminal event. It then
replaces the worker with the independently linked corrected executable. The
client keeps its original handles; all three original runs must complete.
The first two runs replay their committed timer prefix in the fresh process,
and no speculative second timer may appear. Two other workflows deliberately
return typed application errors, exercising both retryability flags without
an installed retry policy.

Each invocation retains raw initial/terminal histories, separate exact-run
`describe` responses, client identities, process/server logs, SDK commit and
diff hash, binary SHA-256 hashes, Core revision, pinned image configuration,
and teardown verdict in `_build/task-failure-live/<timestamp>/`. These fixture
histories contain only synthetic inputs. CI uploads that directory even when
the live gate fails. The live gate exercises fresh OCaml worker replay; the
retained-history regression separately exercises offline Core replay. These
bounded cases do not establish exhaustive mixed-deployment qualification.

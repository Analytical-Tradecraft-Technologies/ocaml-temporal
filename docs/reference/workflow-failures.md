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
| Signal with no registered handler, an undecodable signal payload, or more than one signal payload | Failed workflow task, no commands; the run waits for a compatible worker, reset, or termination ([fail-closed signal policy](interactive-workflows.md#unknown-and-undecodable-signals)) |
| Signal handler returns a typed non-task error such as `Workflow` | Terminal workflow failure |
| Workflow result encoder returns an error or raises | Failed workflow task, regardless of the encoder's error category |
| `Workflow.continue_as_new` input encoder returns an error | Failed workflow task, no continue-as-new command |
| Missing workflow registration, malformed activation, invalid resolver state, adapter exception before submission | Failed workflow task, no commands |
| Query lookup, handler, or codec failure | Failed query answer for the original query ID; does not fail the workflow |
| Missing update handler, input rejection, any validator error (returned in any category, or raised), or deliberate typed handler failure | Rejected update response; does not fail the workflow |
| Unexpected update handler exception/invariant failure after acceptance, or codec failure after acceptance | Failed workflow task; discard speculative acceptance and commands |
| Eviction | Successful empty acknowledgement |

For example, a business rule that should close the execution returns:

```ocaml
Error
  (Temporal.Error.make ~category:`Workflow ~non_retryable:true
     ~error_type:"OrderRejected" ~message:"order rejected" ())
```

The optional `~error_type` becomes the failure's Temporal
`ApplicationFailureInfo.type`, which parents, clients, the Temporal UI, and
code written with other Temporal SDKs use to identify a business failure.
Without it the SDK sends the lowercase category name (`Error.kind`, here
`"workflow"`), which was the only type available before `~error_type`
existed. Activity failures follow the same rule, and a retry policy's
`non_retryable_error_types` is matched against this type (see the [durable
operation policies](durable-operation-policies.md#retry-policy)). In the other
direction, an error received from an activity, a child workflow, an update, or
a client `wait` reports the type of the innermost application failure through
`Error.error_type`, even when Temporal nests it under an activity or
child-workflow wrapper; its category still describes the wrapper. Because
that type was written by another SDK, worker or history, it is not held to
`Error.make`'s rules: the SDK rewrites it deterministically so that it never
raises (#1011). Malformed UTF-8 becomes U+FFFD, each ASCII control character
becomes a visible `\uXXXX` escape (a Python type `"Invalid\tInput"` reads as
`"Invalid\u0009Input"`), and a type whose encoded length exceeds 65,536
characters is cut at a character boundary and ends in `...`. Valid types are
unchanged, and replay always yields the same text.

An inbound error also records where the failure originated (#1143), so
workflow code can tell a timeout from a business failure or a server failure
without parsing `Error.message`. `Error.origin` walks the cause chain from
the outside inward past the wrappers that only name the failed operation
(activity, child workflow, Nexus operation) and classifies the first other
layer. The category is unchanged, so an activity timeout is still
`` `Activity ``:

| First non-wrapper layer | `Error.origin` | Notes |
| --- | --- | --- |
| Application failure, any SDK | `` Some `Application `` | `Error.error_type` names it |
| Timeout | `` Some (`Timeout t) `` | `t` is `` `Start_to_close ``, `` `Schedule_to_start ``, `` `Schedule_to_close ``, `` `Heartbeat `` or `` `Unspecified ``; `Error.timeout_type` returns it. A child's run or execution timeout arrives as `` `Start_to_close `` |
| Server failure | `` Some `Server `` | For example a result over a namespace limit |
| Cancellation, termination | `` Some `Cancelled ``, `` Some `Terminated `` | |
| Reset, Nexus handler error, a kind a newer server adds, or wrappers only | `` Some `Other `` | |
| Not received from Temporal | `None` | Unless the application passed `~origin` to `Error.make`, which it may do to test such branches |

A timeout that ends an activity's retries still wraps the last attempt's
failure, so `Error.error_type` may name that application failure while
`Error.origin` is the timeout. `(Error.view e).details` holds only
application and cancellation details. The `last_heartbeat_details` that
Temporal records on a timeout or reset layer are returned separately by
`Error.last_heartbeat_details`, so an activity's error-details codec never
decodes a heartbeat payload. The same classification applies to failures a
client `wait` or `wait_update` decodes, and a `wait` that ends cancelled,
terminated or timed out reports `` `Cancelled ``, `` `Terminated `` or
`` `Timeout `Unspecified `` (Temporal does not record which workflow timeout
expired). `Temporal.Testing` does not simulate
activity timeouts (#1160); its virtual-deadline errors are created by the
SDK, so their origin is `None`.

Neither the origin nor the heartbeat details are transmitted: an error that
an activity or workflow returns is sent with only its category, message,
retryability, details and type. Otherwise the cause chain is not preserved as
structure. Inbound causes appear in the error message, and outbound OCaml
failures carry no `cause`. Every failure
the SDK emits uses the source `"ocaml-temporal"`.

`non_retryable=false` permits Temporal's configured workflow retry policy to
start another run. It does not itself install a retry policy: pass one with
`Client.start ?retry_policy` (#499) or a child's `?retry_policy`, which are
sent to Temporal unchanged. Absent a retry policy, either flag closes the
original run. A defect's
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
The run-map ownership record and retained completion bytes remain until acknowledgement;
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

The one-shot client publishes `completed.tsv` before shutting down. The
controller samples its container state before reading that marker, so a clean
exit during polling cannot hide completed results. It still requires identical
accepted/completed identities and exit zero; worker readiness markers continue
to require a live producer. `make test-task-failure-markers` exercises these
publication/exit cases without Docker.

Each invocation retains raw initial/terminal histories, separate exact-run
`describe` responses, client identities, process/server logs, SDK commit and
diff hash, binary SHA-256 hashes, Core revision, pinned image configuration,
and teardown verdict in `_build/task-failure-live/<timestamp>/`. These fixture
histories contain only synthetic inputs. CI uploads that directory even when
the live gate fails. The live gate exercises fresh OCaml worker replay; the
retained-history regression separately exercises offline Core replay. These
bounded cases do not establish exhaustive mixed-deployment qualification.

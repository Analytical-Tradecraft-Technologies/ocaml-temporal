# Durable operation retry and cancellation policies

`Temporal.Activity` and `Temporal.Child_workflow` expose retry and
cancellation options as part of the durable operation command. These options
are not mutable worker state and they are not a local retry loop: Temporal Core
records the command and owns the retry state machine. Workflow code must choose
them from deterministic values so replay emits the same command.

This page covers the public policy surface. The [activity execution
reference](native-activity-execution.md) and [native execution
translation](native-execution-translation.md) describe the lower-level bridge
and worker behavior; the [activity retry ADR](../decisions/0007-activity-retry-policy.md)
records the wire-format decision.

## Retry policy

`Temporal.Activity.Retry_policy.make` constructs one immutable policy. The
same policy type is accepted by activity and child-workflow operations:

```ocaml
let run_with_retry activity child input =
  let open Temporal.Result_syntax in
  let* retry_policy =
    Temporal.Activity.Retry_policy.make
      ~initial_interval:(Temporal.Duration.of_ms 100L)
      ~backoff_coefficient:2.0
      ~maximum_interval:(Temporal.Duration.of_ms 10_000L)
      ~maximum_attempts:3
      ~non_retryable_error_types:["InvalidInput"]
      ()
  in
  let* activity_result =
    Temporal.Activity.execute ~retry_policy activity input
  in
  Temporal.Child_workflow.execute ~retry_policy
    ~id:"follow-up" child activity_result
```

The constructor validates before a command is emitted and returns a typed
`Error.t` for invalid configuration. Its fields have these meanings:

- `initial_interval` and `maximum_interval` are positive whole-millisecond
  durations, and the maximum must not be smaller than the initial interval.
- `backoff_coefficient` is finite and at least `1.0`. Temporal applies it
  between attempts, subject to the maximum interval.
- `maximum_attempts = 0` means that no attempt-count limit is selected;
  positive values include the initial attempt.
- `non_retryable_error_types` is a copied list of Temporal application error
  type names. A matching application failure is not retried by this policy.
  Names must satisfy the same strict text constraints as other values crossing
  the native boundary.

`Retry_policy.create` is an alias for `make`. The accessor functions return
copies or immutable scalar values, so changing a caller-owned list after
construction cannot change a command already selected by workflow code.
Passing no `~retry_policy` leaves the option absent; it does not create an
OCaml policy or silently replace the service's default with a language-level
one.

Do not construct a policy from `Unix.gettimeofday`, `Random`, environment
state, or mutable process-global configuration in workflow code. If retry
configuration must be discovered from the outside world, obtain it in an
activity and pass the resulting value through a replay-safe workflow decision.

## Cancellation policy

Cancellation policy controls how the parent operation's future is settled
after its exact operation handle is cancelled. It is different from
`Temporal.Scope.cancel`: a scope always stops local observation, and when the
operation was started with `~scope`, it additionally runs that operation's
server-cancellation hook. It is also different from
`Temporal.Client.cancel`, which addresses an exact workflow/run from
application code.

### Activities

Pass `~cancellation_type` to `Activity.start`, `Activity.start_handle`, or
`Activity.execute`. The default is `Try_cancel`.

| `Temporal.Activity.cancellation_type` | Parent-side behavior |
| --- | --- |
| `Try_cancel` | Ask the activity worker to stop when possible and report according to Core's cancellation transition. |
| `Wait_cancellation_completed` | Wait for the activity worker's cancellation acknowledgement before the operation settles. |
| `Abandon` | Leave the activity running and settle the parent operation without waiting for that activity to stop. |

Use `start_handle` when the workflow needs to retain one exact activity and
possibly cancel it later:

```ocaml
let start_and_wait activity input =
  let handle = Temporal.Activity.start_handle activity input in
  Temporal.Future.await (Temporal.Activity.future handle)

let request_stop handle =
  Temporal.Activity.cancel handle
```

`Activity.cancel` is owner-checked and idempotent. A repeated call, or a call
after natural activity completion or a failed start, does not emit a second
operation. The activity cancellation command is identified by the private
sequence attached to the handle; callers do not supply a cancellation reason.

### Child workflows

Pass `~cancellation_type` to `Child_workflow.start`, `start_handle`, or
`execute`. The default is `Try_cancel`.

| `Temporal.Child_workflow.cancellation_type` | Parent-side behavior |
| --- | --- |
| `Try_cancel` | Request child cancellation and report the parent operation immediately. |
| `Wait_cancellation_completed` | Wait for the child cancellation to complete before settling the parent operation. |
| `Wait_cancellation_requested` | Wait until Core confirms that the cancellation request was accepted. |
| `Abandon` | Report the parent operation immediately without asking the child worker to stop. |

Use the child handle when the parent must cancel one exact child. A supplied
`?reason` is copied into the durable cancellation command and must be selected
from replay-safe data. Repeated cancellation is idempotent, including after a
natural child completion or a failed child start. The child's own retry policy
still belongs to the child-start command; cancelling the parent handle does
not convert a retryable child failure into an application retry loop.

### Routing and parent closure

`Child_workflow.start`, `start_handle`, and `execute` accept `~task_queue` to
route a child to a different worker, including a worker implemented by another
SDK. Omission inherits the parent queue. Empty, malformed UTF-8, NUL-containing,
and oversized queues are rejected before input encoding or command allocation.

`~parent_close_policy:Child_workflow.Parent_close_policy.Abandon` asks the
server to leave an accepted child running when its parent closes. `Terminate`
and `Request_cancel` select the other explicit policies; omission retains the
server default. Parent-close policy is independent of `~cancellation_type`,
which controls an explicit cancellation request made through a child handle or
scope. These options do not expose a child-start acknowledgement: callers that
close a parent immediately after scheduling must not assume the child has
already been accepted by the server.

The deterministic runtime and bilateral bridge tests cover all three policies,
queue validation, default compatibility, and lossless Core conversion. This
coverage does not claim a live cross-queue server acceptance test.

## Choosing the right mechanism

These mechanisms affect different owners:

| Goal | Mechanism | Does it emit a Temporal command? |
| --- | --- | --- |
| Stop a workflow fiber from observing a future | `Temporal.Scope.await` after `Temporal.Scope.cancel` | Resolves the private scope signal; scoped activities and children also emit their registered cancellation command |
| Stop or abandon one scheduled activity | Activity cancellation type plus `Activity.cancel` | Yes, according to the selected Core policy |
| Stop or abandon one child workflow | Child cancellation type plus `Child_workflow.cancel` | Yes, according to the selected Core policy |
| Cancel an exact client execution | `Temporal.Client.cancel` | Yes, through the native client path |
| Retry a failed activity or child according to history | `~retry_policy` | The command carries policy data; Core owns later attempts |

Cancelling a scope only requests server-side cancellation for activities and
children that were started with `~scope`; timers and unscoped operations are
not implicitly cancelled. To cancel an operation independently of a scope,
retain its activity or child handle and use its cancellation policy. If an
application needs to cancel a running workflow from outside workflow
execution, use the exact workflow/run handle through `Temporal.Client.cancel`.

## Current boundary and evidence

Policy values are copied across the OCaml/runtime/Rust boundary and validated
before Core conversion. The retry coefficient is preserved by its exact IEEE
754 bit representation in the private protocol; application code never sees
that wire detail. Activity policy construction and malformed-input handling
are covered by [`test_activity_retry_policy.ml`](../../test/unit/test_activity_retry_policy.ml),
the runtime [`test_activity_retry_policy.ml`](../../test/runtime/test_activity_retry_policy.ml),
and the bilateral protocol tests. The public scope distinction and ownership
rules are covered by [`test_scope.ml`](../../test/runtime/test_scope.ml).

Live acceptance proves selected activity retry and cancellation paths, but it
does not imply that every Core cancellation mode or child recovery scenario is
live-verified. Consult the current acceptance references before treating a
focused or historical run as coverage for a new policy combination.

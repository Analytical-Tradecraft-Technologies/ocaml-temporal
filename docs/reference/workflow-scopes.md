# Workflow-local cancellation scopes

`Temporal.Scope` is an experimental, workflow-local boundary for deciding
which future a workflow still wants to observe. Cancelling a scope always
wakes its waiters with a typed cancellation error. When a remote activity or child
workflow was started with the optional `~scope` argument, cancellation also
invokes that operation's server-side cancellation hook exactly once. Timers
and operations started without `~scope` remain observation-only and are
cleaned up by the normal Temporal/runtime lifecycle.

The local-activity start API does not expose `~scope`.

Use a scope when a workflow wants one cancellation decision to cover both its
local waiters and selected durable operations. Use an operation handle when
the Temporal operation must be cancelled independently of a scope:

| Need | Public API | Effect |
| --- | --- | --- |
| Stop waiting for a workflow-owned future | `Temporal.Scope.cancel` and `Temporal.Scope.await` | Resolves the private signal and wakes scope waiters |
| Cancel scoped activity or child workflow operations | `Temporal.Scope.cancel` after starting with `~scope` | Runs each registered server-cancellation hook once and emits the corresponding Core command |
| Request cancellation of one scheduled activity | `Temporal.Activity.start_handle` and `Temporal.Activity.cancel` | Emits the activity cancellation command using its configured policy |
| Request cancellation of one child workflow | `Temporal.Child_workflow.start_handle` and `Temporal.Child_workflow.cancel` | Emits the child cancellation command using its configured policy |
| Cancel an exact client execution | `Temporal.Client.cancel` | Sends an exact workflow/run cancellation request to Temporal Server |

## Observing a future with a scope

`Temporal.Scope.with_scope` is the usual entry point. It creates a scope,
passes it to the body, and requests cancellation during cleanup. Cleanup is
idempotent and also runs when the body raises an unexpected exception; the
exception is still propagated. If the body succeeds but a registered hook
fails during `with_scope`'s implicit cleanup cancellation, `with_scope` returns
that typed cleanup error instead of reporting the body value as an overall
success. A typed error or unexpected exception from the body remains primary
after cleanup is attempted.

An explicit `Scope.cancel` returns any hook failure only to that call. Once
cancellation has been requested, later calls are idempotent and return
`Ok ()`; they do not replay an earlier hook error. Consequently, if a body
explicitly cancels its scope, discards that error, and then returns `Ok value`,
`with_scope` also returns `Ok value`. A body should await every branch it
intends to observe before returning and should not discard explicit
cancellation errors.

The following pattern races a workflow operation against a durable deadline.
When the deadline wins, the operation is no longer observed by this body. If
the operation was started with `~scope`, the same cancellation also requests
server-side cancellation according to that operation's policy:

```ocaml
let await_until deadline operation =
  Temporal.Scope.with_scope (fun scope ->
    match
      Temporal.Scope.await scope
        (Temporal.Future.race operation
           (Temporal.Workflow.start_sleep deadline))
    with
    | Ok (Temporal.Future.Left value) -> Ok value
    | Ok (Temporal.Future.Right ()) ->
        Error
          (Temporal.Error.make
             ~category:`Timeout ~message:"workflow deadline elapsed" ())
    | Error error -> Error error)
```

`Temporal.Scope.await` first checks ownership and whether cancellation has
already been requested. If both the operation and the private scope signal
are ready, the operation wins because it is registered first. Otherwise the
first deterministic scheduler completion wins. A future error passes through
unchanged; a scope signal produces an error whose category is `Cancelled`.
Registered operation hooks run as part of `Scope.cancel`; the first hook error
is returned after all hooks have been attempted. The future must belong to the
same workflow execution as the scope.

Another runnable fiber in that same workflow execution can request the stop:

```ocaml
let stop_observation scope =
  Temporal.Scope.cancel scope
```

The call must run during the owning workflow scheduler's active turn. A
successful cancellation is idempotent, and it wakes scope waiters. A
cancellation request that arrives after an operation has already completed
does not rewrite that result; its hook is simply not registered as live work.

## Completed registrations

Activity and child hooks use `Scope.on_cancel ~until:terminal_future`. Both
successful and failed terminal results remove the hook, including a rejected
child start. Custom hooks can use the same optional argument with a future
owned by the scope's workflow; without it they live until cancellation.
Cancellation removes the corresponding terminal observer as well. A terminal
result already visible when cancellation runs suppresses the hook even if its
cleanup callback is still queued.

`Future.race` and `Future.first` detach their input observers after either a
successful or failed winner. Removing an observer also suppresses a callback
already queued for delivery, without cancelling the underlying operation.
Registration order and the scheduler's FIFO queue continue to decide ties.
Only outstanding work contributes registrations to a long-lived scope.

The deterministic [retention regression](../../test/runtime/test_scope_retention.ml)
keeps one scope alive for three batches of 10,000 completed operations and
measures live OCaml words after full major GC, before cancelling the scope.
It covers activities, child terminal/start failures, scoped awaits, runtime
and derived race inputs, duplicate inputs, and inline ready delivery. It uses
the scheduler after trace removal (#551), with no trace subtraction or cache
eviction. This establishes language-layer retention, not live server behavior.

## Lifecycle and ownership

- `Temporal.Scope.create ()` succeeds only while a workflow execution is active
  on the current Domain. Outside workflow execution it returns a typed defect.
- `Temporal.Scope.cancel`, `is_cancelled`, `check`, and `await` are all
  owner-checked. They must run on the scheduler that created the scope while
  that scheduler is processing workflow callbacks.
- Calling a scope operation from another Domain, between scheduler runs, or
  after workflow teardown returns a typed ownership defect. A retained scope
  is not a cross-Domain synchronization primitive.
- `Temporal.Scope.check` is a non-waiting guard: it returns `Ok ()` for an
  active scope and a typed `Cancelled` error after cancellation.
- `Temporal.Scope.is_cancelled` reports the state without scheduling work, but
  it has the same ownership checks as cancellation and awaiting.
- Scope cancellation itself is a private deterministic signal. Only activity
  and child operations started with `~scope` register hooks that enqueue the
  corresponding server-cancellation command. Timer futures and unscoped
  operations are not implicitly cancelled. Workflow completion, eviction,
  cancellation, and shutdown dispose the scope signal and its waiters.

These rules keep cancellation deterministic and prevent one workflow execution
from reading or mutating another execution's state. They are part of the
[runtime invariants](runtime-invariants.md); the focused ownership, cleanup,
idempotence, and server-cancellation cases live in
[`test/runtime/test_scope.ml`](../../test/runtime/test_scope.ml) and
[`test/runtime/test_scope_server_cancel.ml`](../../test/runtime/test_scope_server_cancel.ml).

For a complete public-module index, see the [public API map](public-api-map.md).
For server-facing activity and child cancellation behavior, see the
[durable operation policy reference](durable-operation-policies.md), the
[workflow guide](../guides/workflows.md), and the relevant protocol references.

## Evidence and callback boundary

The [audited evidence](live-acceptance-coverage.md#cancellation-and-non-live-evidence)
pins the scope, activity and child implementations and their focused tests.
The live child-cancellation fixture uses an explicit child handle; it does not
invoke scope cancellation, so it is not direct live evidence for `~scope`.

A successful hook buffers a command; it does not wait for a server terminal
outcome or preempt an OCaml activity callback. Activity task cancellation and
worker shutdown use separate lifecycle paths. The public activity context has
no cooperative callback cancellation probe at the audited baseline, and the
serialized adapter cannot drain a cancellation update while its callback is
still executing. See [native activity cancellation](native-activity-execution.md#cancellation).

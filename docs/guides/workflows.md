# Writing workflows in OCaml

The public API is the `Temporal` module. You write a workflow as an ordinary
OCaml function from a typed input to `('output, Temporal.Error.t) result`, then
register that function with a `Temporal.Workflow.t`. Activities use the same
function shape. There is no workflow base class, callback interface, or special
syntax to learn.

The one unusual part is waiting. A call such as `Temporal.Future.await` looks
like a normal function call, but when its value is not ready the SDK suspends
only the current workflow fiber. It does not block an operating-system thread,
and the workflow author never handles an effect constructor or a saved
continuation. OCaml 5 algebraic effects implement that suspension privately.

This guide describes the API that compiles today and labels its execution
boundary honestly:

| Target | What it is useful for today |
| --- | --- |
| `mock://...` | Fast deterministic unit tests for client/worker registration and dispatch. The pure runtime tests also exercise timers, activities, child scheduling, replay, cancellation, and future combinators without a server. |
| `http://...` or `https://...` | The OCaml-owned native client/worker path backed by Rust Temporal Core. The current native command slice handles activity, timer, terminal, cancellation, cache, and two-stage child-resolution paths. It is covered by focused bridge and adapter tests. |
| Live Compose acceptance | Real PostgreSQL and Temporal Server validation with two separate OCaml binaries: a public worker and a public client driver. It asserts a fan-out activity result, a timer-then-activity result, and a parent awaiting a timer-owning child workflow. |

The first two rows are different test boundaries, not different workflow
languages. The same typed definitions and direct-style functions are used in
both; `mock://` keeps tests local, while an HTTP(S) target uses the native
OCaml/Rust bridge. The live Compose target proves the listed success paths
through a real Temporal Server; it does not yet cover every failure, recovery,
or child-workflow scenario.

## The direct-style model

There are three values to keep distinct:

- A `result` is an ordinary OCaml value. `Ok value` means the operation
  succeeded and `Error error` means it produced an expected SDK or Temporal
  failure.
- A `Temporal.Future.t` is a workflow-owned value that may become a result in
  a later response from Temporal. Creating one schedules work; it does not
  wait.
- `Temporal.Future.await future` turns that future into a result. If the future
  is already ready, it returns immediately. Otherwise the private scheduler
  suspends the current workflow fiber and resumes it when Temporal supplies the
  matching completion.

This lets workflow code read from top to bottom while retaining Temporal's
durable execution model:

The `summarize` value below is the typed activity reference defined later in
this guide.

```ocaml
let summarize_document document =
  let open Temporal.Result_syntax in
  let* summary = Temporal.Activity.execute summarize document in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) in
  Ok summary
```

`Activity.execute` is the convenient form of “start this activity, then await
it”. If the activity is still running, the function above is suspended at that
line and the worker can run other workflow fibers. It is not equivalent to
holding a mutex or sleeping a native thread. The same rule applies to
`Workflow.sleep` and `Child_workflow.execute`.

`Temporal.Result_syntax` provides only the standard `result` operators. Its
`let*` makes it easy to stop on the first expected error; it does not add a
second monad or expose the effect scheduler.

### Future ownership and shutdown

Every `Temporal.Future.t` belongs to the workflow execution that created it.
It is a handle to that execution's deterministic completion, not a general
purpose promise that can be shared with application threads or retained as a
long-lived cache entry. Combinators such as `Future.map`, `Future.both`, and
`Future.all` preserve the same workflow ownership.

When an execution completes, is evicted, is cancelled, or shuts down, its
pending futures and captured continuations are disposed. Callback work that is
already queued for that owner becomes inert; it must not run workflow code
after the owner has ended. This is part of the scheduler's cleanup contract,
not a signal that a queued callback can safely be reused by another execution.

Drop future handles when their workflow ends. Do not call `Future.await`,
register more composition, or use a retained handle to coordinate unrelated
work after its owner has shut down. If code needs a result outside the
workflow, have the owning workflow return that result through its normal
completion path and keep the application-level value rather than the future
handle.

### Workflow-local conditions

`Temporal.Condition.wait_until` is the direct-style way to wait for
workflow-local state. It is deliberately different from a timer or a future
returned by an activity: it emits no Temporal command and creates no history
event. The SDK checks the predicate immediately. If it is false, only the
current workflow fiber is parked; the activation can continue running other
fibers and handlers.

```ocaml
let wait_for_approval approved_by_signal =
  let open Temporal.Result_syntax in
  let* () =
    Temporal.Condition.wait_until (fun () ->
      Option.is_some !approved_by_signal)
  in
  Ok (Option.get !approved_by_signal)
```

The predicate should be a quick, deterministic read of workflow state. A
signal handler or another runnable workflow fiber can mutate that state; after
the activation's queued work drains, the SDK rechecks waiting predicates in
registration order. A predicate that becomes true in that same activation
therefore resumes before the activation is returned to Temporal. If the state
has not changed, the waiter remains pending for a later activation. Do not
perform network I/O, read wall-clock time, sleep an OS thread, or call a
suspending SDK operation from a condition predicate; use an activity, child
workflow, timer, or future for those operations.

When a predicate can report an expected application failure, use
`Temporal.Condition.wait_until_result`:

```ocaml
let wait_for_valid_state state =
  Temporal.Condition.wait_until_result (fun () ->
    if valid_state !state then Ok true
    else Error (Temporal.Error.codec ~message:"state is invalid"))
```

Both forms return `(unit, Temporal.Error.t) result`. An exception raised by a
predicate is contained and returned as a typed non-retryable defect. A
condition is owned by one workflow execution and is removed when that
execution completes, is evicted, or shuts down; stale callbacks cannot wake a
finished workflow or retain its state.

Child-workflow code is valid in the synthetic runtime and the semantic command
translator. The native adapter also represents the complete two-stage
resolution lifecycle: a successful start acknowledgment records the child run
ID, and a later terminal resolution resumes the parent future. Focused tests
cover success, start failure, final-before-start, duplicate sequences, and
lease retirement. The live Compose fixture now also covers one parent calling
`Child_workflow.execute` and receiving a successful child result after the
child's durable timer. The live Compose fixture also covers propagated child
failure, child cancellation, child retry, and duplicate-ID child-start failure;
the complete [PR #351
run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013) additionally
verifies exact parent and child replay through worker replacement. Broader
child failure recovery remains a separate acceptance scenario.

## 1. Write a deterministic OCaml function

Start with ordinary functions and return expected failures as values:

```ocaml
let normalize name = String.trim name

let greeting input =
  let name = normalize input in
  if String.equal name "" then
    Error (Temporal.Error.defect ~message:"name must not be empty")
  else
    Ok ("Hello, " ^ name)

let greeting_workflow =
  Temporal.Workflow.define
    ~name:"greeting"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    greeting
```

`Workflow.define` pairs the stable Temporal workflow type name with input and
output codecs and the local implementation. `normalize` is just a helper
function; it needs no registration or special syntax. `Workflow.remote` makes
a typed reference to workflow code owned by another worker and has no local
implementation, so it cannot be registered in `Temporal.Worker.create`.
`Temporal.Result_syntax` supplies `let*` for sequencing these ordinary
`result` values; it does not introduce a second effect system.

The function above returns an `Error.t` value instead of raising an exception.

## Update indexed search attributes

Workflows can publish indexed search attributes by emitting one deterministic
merge command. The values are ordinary `Temporal.Payload.t` values, so the
same codecs used for activity and workflow inputs can be reused. Keys must be
unique, non-empty, valid UTF-8 strings no longer than 65,536 bytes; invalid
keys are programmer errors and are rejected before the command is buffered.

```ocaml
let classify_workflow () =
  let status = Temporal.Codec.encode Temporal.Codec.string "ready" in
  match status with
  | Error error -> Error error
  | Ok status ->
      Temporal.Workflow.upsert_search_attributes [ ("agent_status", status) ];
      Ok "classified"
```

The update is recorded in workflow history and becomes visible to Temporal's
visibility layer after the workflow task completion is accepted. Calling this
operation does not perform network I/O, read wall-clock time, or mutate
process-global state, which keeps replay deterministic. The current release
has focused OCaml and Rust conversion coverage; a live visibility acceptance
scenario is still tracked separately in the [feature coverage
matrix](../reference/feature-coverage.md).
That is the normal way to report an expected workflow failure. Pattern-match
when the caller needs to choose a recovery path, or use `let*` when the error
should finish the workflow:

```ocaml
let greet_or_explain input =
  match greeting input with
  | Ok value -> Ok value
  | Error error ->
      Ok ("The greeting could not be created: " ^ Temporal.Error.message error)
```

Exceptions are reserved for programmer defects and broken internal invariants.
The worker boundary catches an unexpected exception and reports a structured
failure; workflow code should not use exceptions as its ordinary branch or
retry mechanism.

Workflow code must be deterministic during replay. Temporal may run the same
function again from its recorded history, so a different decision on the
second run would produce a different workflow. In workflow code, prefer pure
OCaml values and SDK operations:

| Safe in a workflow | Put it in an activity (or wait for a replay-safe SDK API) |
| --- | --- |
| String/list calculations and immutable data | Network calls, filesystem access, or subprocesses |
| `Activity.start`, `Child_workflow.start`, timers, and future combinators | `Unix.gettimeofday`, `Random`, or another unrecorded clock/random source |
| IDs derived from workflow input and explicit constants | Reading mutable process-global state or environment to choose a command |
| Iteration over an ordered list | Iteration over a hash table or other unordered collection when order affects commands |

Activities are the place for external work such as calling an LLM. The
activity result is recorded by Temporal, so replay can use that recorded result
without calling the external service again.

### Introduce a new branch with a patch marker

When deployed workflow code must make a new command-producing decision, guard
the change with one stable patch ID:

```ocaml
if Temporal.Workflow.patched ~id:"agent.add-review-step.v1" then
  review_draft draft
else Ok draft
```

New executions take the `true` branch and record the marker. Replay of a
history that already contains the marker also takes that branch; replay of an
older history without it takes the `false` branch. A helper can call
`patched` like an ordinary OCaml function because the SDK keeps the decision
inside the current workflow execution.

Treat the ID as permanent history data: use a source-code constant, never
reuse it for a different change, and do not derive it from configuration or
other mutable state. After marker-free histories can no longer replay through
this point, replace the conditional with
`Temporal.Workflow.deprecate_patch ~id:"agent.add-review-step.v1"` and keep the
new behavior unconditional. Removing that deprecation call is a later step,
after every non-deprecated marker history has been drained, migrated, or
otherwise proven safe. See
[workflow patching](../reference/workflow-patching.md) for both migration gates,
the bridge contract, and the current live-evidence boundary.

## 2. Use typed codecs

Temporal stores values as payloads: bytes plus metadata naming the encoding.
Temporal does not require JSON and does not inspect the payload body. The
built-in codecs are:

- `Temporal.Codec.string`, using the interoperable `json/plain` encoding;
- `Temporal.Codec.bytes`, using `binary/plain`;
- `Temporal.Codec.unit`, using `binary/null`; and
- `Temporal.Codec.option codec`, which uses the nested codec for `Some` and
  `binary/null` for `None`. When the nested codec would itself produce a
  `binary/null` payload — as `unit` and a nested `option`'s own `None` do — the
  `Some` value is wrapped in a distinct `binary/x-ocaml-optional` envelope so that
  `Some ()`, `Some None`, and `None` remain distinguishable on decode. Ordinary
  values such as `Some "text"` keep their interoperable encoding untouched.

`Codec.option` is deliberately *injective*: decoding always recovers the exact
value that was encoded. For every option type that another-language SDK can
represent — `string option`, `int option`, and so on — this is also the fully
interoperable representation (`None` is the standard `binary/null` nil; `Some v`
is the inner codec's own encoding). The private `binary/x-ocaml-optional`
envelope appears only for `unit option` and nested `option`, types that have no
counterpart in most other SDKs; a non-OCaml worker or client that received one
would report an unknown-encoding error rather than silently reading it as `None`.

If you instead need an option that collapses onto a foreign *nullable* — for
example an OCaml activity result read by a Go worker, where you accept that a
present-but-empty value is indistinguishable from absent — do not reach for
`Codec.option`. Build that exact wire representation with `Codec.make`, which
lets you choose the encoding name and byte layout the non-OCaml counterpart
expects. Keeping the collapse out of `Codec.option` means the default anyone
picks stays round-trip-safe.

Payload metadata is object-like on the Temporal wire, so each metadata name
must occur at most once. The public and private codecs reject duplicate names
with a typed codec error before invoking application conversion code; this
keeps manually constructed payloads subject to the same invariant as payloads
received through the strict bridge protocol.

JSON here is a payload choice, not the private OCaml/Rust bridge protocol and
not the format sent to Temporal Server. The bridge's Rust side converts its
strict semantic JSON records to Temporal Core protobuf; see the [protocol
reference](../reference/core-protocol.md).

Codec operations return `result`, because a remote payload can be malformed or
encoded with the wrong name:

```ocaml
let encode_prompt prompt =
  Temporal.Codec.encode Temporal.Codec.string prompt

let decode_prompt payload =
  Temporal.Codec.decode Temporal.Codec.string payload
```

Define a custom deterministic codec when another encoding is more appropriate:

```ocaml
let positive_integer =
  Temporal.Codec.make
    ~encoding:"example/positive-integer"
    ~encode:(fun value ->
      if value > 0 then Ok (Bytes.of_string (string_of_int value))
      else Error (Temporal.Error.codec ~message:"integer must be positive"))
    ~decode:(fun bytes ->
      match int_of_string_opt (Bytes.to_string bytes) with
      | Some value when value > 0 -> Ok value
      | _ -> Error (Temporal.Error.codec ~message:"invalid positive integer"))
```

Changing a codec for an existing workflow is a compatibility change: old
history can contain payloads written by the previous codec.

## 3. Choose between an activity and a child workflow

Starting either operation returns a typed future, but the two operations
represent different Temporal resources. The `execute` forms are convenience
functions that start and immediately await that future:

| Use an activity when… | Use a child workflow when… |
| --- | --- |
| One task should perform external or nondeterministic work, such as an LLM call. | The work is itself a durable workflow with its own workflow type and history. |
| An activity worker, possibly written in another language, should execute the task. | You want to start another workflow execution explicitly and identify it with a durable child ID. |
| The parent needs one typed result from that task. | The parent wants a separate workflow boundary and may await that child result. |

Calling an ordinary helper function creates neither resource. A child exists
only when code explicitly calls `Child_workflow.start` or
`Child_workflow.execute`; an activity exists only when code explicitly calls
`Activity.start` or `Activity.execute`.

Define a local implementation when this worker should execute the operation:

```ocaml
let summarize_activity =
  Temporal.Activity.define
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun document -> Ok (String.trim document))
```

For work that must finish after the worker callback returns, use the explicit
asynchronous form. The callback receives a typed capability, returns
`Will_complete_async`, and hands the capability to the code that will finish
the activity. Later operations return `(unit, Error.t) result`; they do not
raise for an expired task token or a transport failure. Do not retain the
ordinary contextual activity value: it is intentionally invalid after the
callback returns.

```ocaml
let delayed_summary =
  Temporal.Activity.define_async
    ~name:"delayed_summary"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun context document ->
      let handle = Temporal.Activity.Async_context.handle context in
      ignore
        (Domain.spawn (fun () ->
             let value = String.trim document in
             match Temporal.Activity.Async_handle.complete handle value with
             | Ok () -> ()
             | Error error ->
                 Logs.err (fun report ->
                     report "delayed_summary completion failed: %s"
                       (Temporal.Error.message error))));
      Temporal.Activity.Will_complete_async handle)
```

The handle is tied to one activity attempt and one output codec. A completion
or failure can be submitted once; heartbeat calls keep the lease non-terminal.
The worker first acknowledges the asynchronous handoff to Temporal Core, then
the retained handle uses the namespace-bound client operation. If that native
operation reports a retryable transport failure, the private adapter retains
the byte-identical request for retry. A terminal bridge error closes the handle
so a later call fails predictably instead of leaving worker shutdown blocked.

Use `Activity.remote` or `Workflow.remote` when another worker owns the
implementation. A remote definition keeps the name and codecs needed to
encode and decode the call, but it cannot be registered as a local worker
implementation.

## 4. Schedule activities and wait directly

Use a local activity definition when this worker will execute the task, or a
remote activity reference when another worker owns it:

```ocaml
let summarize =
  Temporal.Activity.remote
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let summarize_document document =
  let open Temporal.Result_syntax in
  let summary_future = Temporal.Activity.start summarize document in
  let timer_future =
    Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 10L)
  in
  let* summary, () =
    Temporal.Future.await (Temporal.Future.both summary_future timer_future)
  in
  Ok summary

let summarize_workflow =
  Temporal.Workflow.define
    ~name:"summarize_document"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    summarize_document
```

For a valid call, `Activity.start` emits a command and returns a future
immediately. Starting the timer before waiting demonstrates the important
pattern: schedule independent work first, then await a combined future.
Temporal can process the activity and timer independently, while the workflow
still receives their results in a deterministic way. `Activity.execute` is the
short form for `start` followed by `Future.await` when no other work needs to
be started first.

Activity scheduling accepts labelled options for a stable activity ID, task
queue, timeout values, cancellation policy, eager-execution preference, and an
optional retry policy:

```ocaml
let policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 1_000L)
    ~backoff_coefficient:1.5
    ~maximum_interval:(Temporal.Duration.of_ms 60_000L)
    ~maximum_attempts:4 ()

let result =
  match policy with
  | Error error -> Error error
  | Ok retry_policy ->
      Temporal.Activity.execute ~retry_policy remote_activity input
```

The constructor validates positive intervals, a finite coefficient of at least
`1.0`, and a non-negative signed 32-bit attempt limit; `0` means unlimited
attempts. Invalid identifiers, payloads, or options produce a typed future
error before a history command is emitted. Omitting `~retry_policy` is
different from supplying one: the private protocol carries an explicit JSON
`null`, so Temporal's default policy remains distinguishable from a concrete
policy during replay. `Temporal.Workflow.start_sleep` creates a durable timer
without waiting; `Temporal.Workflow.sleep` is the start-and-wait form.

When a workflow must decide later whether to stop an activity, keep the opaque
handle returned by `Activity.start_handle` instead of discarding it:

```ocaml
let lookup =
  Temporal.Activity.start_handle
    ~cancellation_type:Temporal.Activity.Try_cancel
    summarize document

let maybe_stop should_stop =
  if should_stop then Temporal.Activity.cancel lookup else Ok ()

let result = Temporal.Future.await (Temporal.Activity.future lookup)
```

`Activity.cancel` identifies exactly one scheduled activity and returns a typed
`result`. It emits at most one deterministic `Request_cancel_activity` command;
repeated calls and calls after the activity has completed are idempotent. The
handle cannot be forged for another activity, and calls made outside its owning
workflow return a typed lifecycle defect. The command has no user-supplied
reason field, so cancellation is intentionally parameterless. Use the
activity's structured result or application payload to carry any domain-level
explanation. Invalid activity options or input encoding produce a ready failed
handle and emit no schedule or cancellation command.

## 5. Combine futures

A `Temporal.Future.t` belongs to the workflow execution that created it. It is
not a general-purpose operating-system promise. The common combinators are:

```ocaml
let await_both first second =
  Temporal.Future.both first second |> Temporal.Future.await

let await_all pending =
  Temporal.Future.all pending |> Temporal.Future.await

let await_fastest left right =
  Temporal.Future.race left right |> Temporal.Future.await
```

`both` and `all` wait for every input and preserve deterministic input ordering;
they do not cancel siblings implicitly. If several inputs fail, `all` returns
the first error in the input list after all siblings have settled. `race` can
combine different output types and returns `Left value` or `Right value`;
`first` is the homogeneous non-empty-list form. An error is a completion, so it
may win a race. The combinator itself does not cancel losing operations; they
continue according to their normal Temporal lifecycle. Once a winner is
selected, the aggregate releases its captured resolver and result immediately;
an unfinished loser therefore cannot keep the winner's value alive until the
loser completes or the workflow shuts down.

All inputs to a combinator must belong to the same workflow execution. A
future from another execution is not silently adopted: the result is a ready
structured defect. This ownership rule prevents one workflow's scheduler from
resuming another workflow's continuation.

When a future is not ready, `Future.await` suspends only the current workflow
fiber. Other runnable workflow fibers and the worker process can continue. The
effect machinery is private, so workflow authors write direct-style OCaml.
`Future.peek` and `Future.is_ready` are available when code needs to inspect a
future without waiting, but they do not make an incomplete operation complete.

### Cancellation scopes

`Temporal.Scope` adds a workflow-local, structured boundary around observation
of futures:

```ocaml
let run_with_deadline input =
  Temporal.Scope.with_scope (fun scope ->
    let result = Temporal.Activity.start summarize input in
    Temporal.Scope.await scope result)
```

`Scope.create` and `Scope.with_scope` return ordinary typed `result` values.
`Scope.cancel` is idempotent when repeated by the owning scheduler,
`Scope.check` is a non-blocking cancellation check, and `Scope.await` returns
an `Error.t` with category `Cancelled` when the scope has been cancelled before
the observed future completes. `Scope.is_cancelled` is also typed because
status reads must be serialized with the owning workflow Domain. Every scope
operation called between scheduler runs, from another OCaml Domain, or after
shutdown returns a typed ownership defect. This keeps signal delivery
deterministic and prevents a cancellation request or status read from racing
the queue that owns the scope.

Scope cancellation is cooperative for local observation and structured for
selected durable operations. Starting an activity or child workflow with
`~scope` registers a server-cancellation hook that runs exactly once when the
scope is cancelled, using that operation's configured cancellation policy.
Timers and operations started without `~scope` are not implicitly cancelled;
retain their handles when they must be controlled independently. Losing
futures in `Future.race` remain ordinary observations unless the caller
explicitly uses a scope around the later wait.

## 6. Child workflows: authoring versus native support

Child-workflow references use the same typed shape as activities:

```ocaml
let review =
  Temporal.Workflow.remote
    ~name:"review_document"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let start_review document =
  Temporal.Child_workflow.start
    ~id:"document-review"
    review
    document
```

The ID is durable Temporal identity. It must be non-empty, valid UTF-8, free of
NUL bytes, and within the bridge's bounded length. `Child_workflow.execute`
starts and waits in one call. Child retries are configured with the same
validated policy type used by activities; Temporal Core owns the retry state
machine, so replay does not run an OCaml retry loop:

```ocaml
let retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 1_000L)
    ~backoff_coefficient:2.0
    ~maximum_interval:(Temporal.Duration.of_ms 5_000L)
    ~maximum_attempts:3 ()

let run_review document =
  match retry_policy with
  | Error error -> Error error
  | Ok retry_policy ->
      Temporal.Child_workflow.execute ~retry_policy
        ~id:"document-review" review document
```

The policy's intervals are exact durations and its coefficient is carried as
lossless IEEE-754 bits through the private JSON protocol. Omitting
`~retry_policy` emits `null`, which selects Core's default child policy. The
current focused tests verify command construction and the bilateral Core
conversion; the live Compose fixture has not yet exercised a child retry.

When a workflow needs to keep the child operation alongside other work, retain
the opaque handle returned by `start_handle`:

```ocaml
let child =
  Temporal.Child_workflow.start_handle
    ~cancellation_type:Temporal.Child_workflow.Try_cancel
    ~id:"document-review" review document

let stop_child () =
  Temporal.Child_workflow.cancel ~reason:"review no longer needed" child

let result = Temporal.Future.await (Temporal.Child_workflow.future child)
```

`cancel` returns a typed result and is idempotent for one handle, including a
valid call made after the child naturally completes or its start fails; it does
not raise for an expected Temporal failure. The `Try_cancel` policy asks Core to
request cancellation and report the child result promptly. Use
`Wait_cancellation_completed` or `Wait_cancellation_requested` when the parent
must remain pending until Core observes that stage, or `Abandon` when no child
request should be sent. `Try_cancel` is the default for `start_handle`, `start`,
and `execute`, so an ordinary `cancel` call requests cancellation; select
`Abandon` explicitly when the child should keep running. The reason is
validated before it becomes a durable command, and the handle cannot be forged
for another child sequence.

The definitions and calls above compile, and the synthetic runtime tests cover
child scheduling and deterministic future resolution. The native protocol and
worker adapter also carry the child-start acknowledgment and terminal
resolution required to resume the parent. The adapter rejects final-before-
start, duplicate, and unknown sequences as typed bridge failures, preserving
the parent lease rather than acknowledging an unsafe completion. Focused tests
cover the complete lifecycle. The live Compose fixture covers the parent/child
success path, propagated failure, child cancellation, and child retry: the
parent calls `Child_workflow.execute`, the registered child waits on a durable
timer, and the driver asserts the parent's exact result. The complete [PR #289
CI run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719) also
live-verifies duplicate-ID child-start failure. The complete [PR #351
run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013) verifies
the exact parent and child replay path. The separate
[child-failure-after-replay acceptance](../reference/child-failure-replay-acceptance.md)
is live-verified by the complete [PR #361 CI run](https://github.com/mfow/ocaml-temporal/actions/runs/29475615866):
it requires both replay observations before the typed child failure and the
parent's `SMOKE:PARENT:CHILD:FAILURE_RECOVERED` result. Additional child
failure and recovery permutations remain separate acceptance work.

### Signal or cancel another workflow

Workflow code can address another execution without leaving the direct-style
OCaml API. Define the signal's input codec once, then use the returned future
like any other workflow operation:

```ocaml
let refresh_signal =
  Temporal.Signal.define ~name:"refresh" ~input:Temporal.Codec.string

let notify_refresh workflow_id run_id reason =
  Temporal.Workflow.signal_external_workflow
    ~workflow_id ~run_id ~signal:refresh_signal ~input:reason

let stop_other_workflow workflow_id run_id =
  Temporal.Workflow.cancel_external_workflow
    ~workflow_id ~run_id ~reason:"superseded by a newer execution"
```

Both operations emit a deterministic command and suspend the current workflow
until Core reports an acknowledgement or structured failure. They do not make
network calls from OCaml or block an Eio fiber or Domain. The exact run ID is
optional for targets that intentionally address the latest execution, but
supplying it is safer when a workflow must never affect a replacement run.
Input and headers are copied through the private JSON bridge; invalid names,
payloads, NUL bytes, and duplicate sequence resolutions become typed errors.
See the [external workflow protocol reference](../reference/core-protocol.md#external-workflow-operations)
for the wire shapes and Core target mapping.

### Continue a run with fresh history

Use `Temporal.Workflow.continue_as_new` when the current execution should end
and Temporal should start the same workflow type with a new input and a fresh
history. The operation is terminal: it never returns to the current workflow
fiber, so code after the call is unreachable. The input is encoded with the
definition's codec before the command is emitted.

```ocaml
let process_batch batch =
  if List.length batch >= 1_000 then
    Temporal.Workflow.continue_as_new process_batch_workflow []
  else
    process_items batch
```

The successor input must be deterministic workflow data. A codec failure is a
typed failure of the current run; it is not raised as an ordinary exception.
Calling this function outside a running workflow is programmer misuse and is
reported as `Invalid_argument`. The private completion protocol represents the
operation as a terminal `continue_as_new` command with a workflow type and an
ordered payload list. The bridge maps it to Temporal Core's
`ContinueAsNewWorkflowExecution` command, fills only explicit Core defaults,
and rejects unsupported non-default options instead of silently dropping them.

`Temporal.Client.wait` treats the current run as terminal and returns its
typed continued-as-new outcome with the successor execution reference; it does
not follow the successor automatically. The complete [PR #253 CI run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
live-verified the successor-following path in Compose; longer continuation
chains and other advanced history-management features remain separate work.

## 7. Compose ordinary helpers

Workflow starters and futures are ordinary values. Helpers can accept or return
them without registration or a special SDK base class:

```ocaml
let fan_out starters input =
  List.map (fun start -> start input) starters
  |> Temporal.Future.all

let fastest left right input =
  Temporal.Future.race (left input) (right input)

let run_helpers input =
  let starts =
    [ (fun value -> Temporal.Activity.start summarize value);
      (fun value ->
        Temporal.Activity.start summarize (value ^ ":backup")) ]
  in
  fan_out starts input |> Temporal.Future.await
```

These helpers still make their callers' Temporal boundaries visible: the
caller chooses when each operation starts and when to await it. Calling a
normal OCaml helper does not create a history event.

Helpers can also add ordinary application behavior around one operation. They
do not need a registration entry or a special return type:

```ocaml
let summarize_with_label document =
  let open Temporal.Result_syntax in
  let* summary = Temporal.Activity.execute summarize document in
  Ok ("summary: " ^ summary)

let run_two_summaries document =
  let first = Temporal.Activity.start summarize document in
  let second = Temporal.Activity.start summarize (document ^ ":backup") in
  Temporal.Future.all [ first; second ]
  |> Temporal.Future.await
```

`run_two_summaries` is still just an OCaml function. The two activity commands
are emitted when it is called, and the caller decides whether to await both,
race them, or map their result. This is the intended way to build a small
library of reusable orchestration helpers.

## 8. Register a worker

The worker registration boundary packs heterogeneous typed definitions while
keeping each implementation and its codecs together:

```ocaml
let summarize_activity =
  Temporal.Activity.define
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun input -> Ok input)

let worker_result =
  Temporal.Worker.create
    ~target_url:"http://127.0.0.1:7233"
    ~namespace:"default"
    ~task_queue:"summaries"
    ~workflows:[ Temporal.Worker.workflow summarize_workflow ]
    ~activities:[ Temporal.Worker.activity summarize_activity ]
    ()
  |> Result.bind Temporal.Worker.run
```

Use `http://` or `https://` for a real native worker. `mock://` is a private,
deterministic test backend and does not contact Temporal Server. Registration
rejects duplicate names and remote-only definitions before a native graph is
created. `Temporal.Worker.run` is a blocking lifecycle loop; call it from an
ordinary dedicated OCaml Domain or system thread rather than directly from a
cooperative Eio/Lwt scheduler fiber. `Temporal.Worker.shutdown` is idempotent
and drains retryable completions before releasing the native graph.

This restriction applies to the worker lifecycle call, not to
`Future.await` inside a workflow. The worker's native readiness wait releases
the OCaml runtime lock, but `Worker.run` still owns a blocking loop. Keep it on
the dedicated worker Domain and let workflow fibers use the private scheduler
for their durable waits.

The native path keeps Rust/Core and its protobufs private. The OCaml worker
receives checked descriptions of work, runs the typed function, and sends a
checked result back through the private supervisor.

## 9. Start, control, and wait from a client

`Temporal.Client` is useful when an application needs to submit an execution
cancel or signal one exact execution, and wait for its result without running
workflow code itself. It retains the exact workflow ID and server-issued run
ID:

```ocaml
let result =
  let open Temporal.Result_syntax in
  let* client =
    Temporal.Client.create
      ~target_url:"http://127.0.0.1:7233"
      ~namespace:"default"
      ()
  in
  let* handle =
    Temporal.Client.start client
      ~workflow:summarize_workflow
      ~task_queue:"summaries"
      ~id:"summary-1"
      ~input:"document"
      ()
  in
  Temporal.Client.wait handle
```

`Temporal.Client.wait` does not silently follow continue-as-new. It returns a
typed terminal outcome so the application can decide whether to follow the
successor. Pass a stable `~request_id` when retrying an uncertain start; reuse
that ID only for the same logical start. As with the worker, expected failures
are `result` values. Exceptions are reserved for programmer defects and are
contained at the worker boundary.

Client identifier fields are checked before transport selection: they must be
non-empty, valid UTF-8, NUL-free strings no longer than 65,536 bytes. Malformed
identifiers therefore return a typed defect consistently with either the
deterministic mock or the native JSON bridge. This includes keys in both the
`~memo` and `~search_attributes` collections.

When the result is `Continued_as_new successor`, use
`Temporal.Client.follow` to make an exact-run handle for that successor:

```ocaml
let open Temporal.Result_syntax in
match Temporal.Client.wait handle with
| Ok (Temporal.Client.Continued_as_new successor) ->
    let* successor_handle =
      Temporal.Client.follow client ~workflow:summarize_workflow successor
    in
    Temporal.Client.wait successor_handle
| Ok (Temporal.Client.Completed output) -> Ok (Temporal.Client.Completed output)
| Ok terminal -> Ok terminal
| Error error -> Error error
```

`follow` is local handle construction; it does not start a workflow, perform a
second server lookup, or silently choose the latest run. It validates the
successor's namespace, workflow, and run IDs, rejects a successor from a
different client namespace, retains the original client's ownership and the
supplied workflow's codecs, and returns a typed error if the client has already
been shut down or the identity is malformed. The caller therefore chooses
explicitly whether to observe one successor, build a loop over a chain, or
stop after the original run.

`Temporal.Client.cancel` sends a cancellation request for the exact run held by
the handle and returns after Temporal acknowledges that request. It does not
wait for workflow code to stop; call `Temporal.Client.wait` afterward to
observe the typed `Cancelled` terminal result. Supply a stable `~request_id`
when retrying an uncertain control operation, just as for a workflow start.

`Temporal.Client.signal` similarly targets the exact run and encodes its input
with the typed `Temporal.Signal` definition before sending the request. A
successful call acknowledges Temporal's signal RPC, not the later execution of
the worker-side handler. The handler must be registered on the worker through
the workflow definition; the [interactive workflow reference](../reference/interactive-workflows.md)
describes the typed definition and deterministic handler boundary. The client
signal bridge and mock lifecycle are focused-tested at this baseline. The
complete [PR #266 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29311239247) live-verifies
typed signal delivery and condition wake-up, and the expanded [PR #289 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719) retains those
assertions in the historical seventeen-result baseline. The [PR #302 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638) first
verified the long-backoff retry extension, and the complete [PR #439 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578) retains
the signal and retry paths in the current 26-start baseline. A successful
signal call still acknowledges Temporal's RPC
rather than the later execution of the worker-side handler.

## 10. Validate locally

From the repository root, the focused Make targets are:

```sh
make test-unit
make test-runtime
make verify
make test-temporal-integration
```

The first two use deterministic test seams and do not require a running
server. The integration target starts a fresh PostgreSQL and Temporal Server
Compose project, checks the schemas and frontend, runs the OCaml-owned Core
lifecycle executable, then runs a public worker and a separate public driver.
The worker executes registered workflows and activities. The driver is a
one-shot test runner, not a worker. Its current implementation stages 26
top-level workflow starts covering fan-out, timers, local and remote
activities, continue-as-new, the retry variants, delayed asynchronous
completion, child success/failure/cancellation/retry/start rejection, typed
workflow failure, direct cancellation and termination, queries, updates,
direct and workflow-to-workflow signals, and external cancellation. Readiness
markers guard control operations, and the start-to-close and heartbeat-timeout
retries remain serialized after the shorter heartbeat path. The complete [PR
#439 CI run](https://github.com/mfow/ocaml-temporal/actions/runs/29824441578)
passed all current terminal assertions against Temporal Server 1.31 and
PostgreSQL. The historical [PR #289 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719) verifies
the earlier seventeen-result baseline, and [PR #302](https://github.com/mfow/ocaml-temporal/actions/runs/29351689638)
first verifies its long-backoff extension. The
[PR #266 CI
run](https://github.com/mfow/ocaml-temporal/actions/runs/29311239247) remains focused
evidence for typed signal delivery and condition wake-up, while the historical
[PR #210 CI run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859)
remains evidence for the earlier nine-scenario slice. The implementation scope
and evidence boundary are described in the [acceptance design](../reference/two-ocaml-binary-e2e-acceptance.md)
and [live acceptance coverage](../reference/live-acceptance-coverage.md).

For the complete ownership and protocol rules, read the [runtime
invariants](../reference/runtime-invariants.md), [Core bridge reference](../reference/core-bridge.md),
and [native worker execution reference](../reference/native-worker-execution.md).

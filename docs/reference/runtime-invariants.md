# Runtime Invariants

These invariants define the correctness boundary of the OCaml workflow
runtime. Changes that weaken one require an architecture decision and replay
tests.

For introductory definitions of activation, command, replay, payload, future,
and bridge, read the [documentation guide](../README.md) first.

## Execution ownership

- One execution owns one scheduler, command sequence, pending-operation set,
  and continuation set.
- A workflow operation can access its context only while that execution's
  activation is running on the current domain.
- An inline query temporarily installs its owning execution context only for
  its synchronous handler, letting it inspect workflow-local state without
  running scheduler fibers. Query dispatch disables deterministic randomness,
  and the dynamic context binding is restored before its response is emitted.
  The query may read its own live `Temporal.Scope.is_cancelled` and `check`
  results through a separate Domain-local read marker. It cannot cancel the
  scope, await a future, read another execution's scope, or use a scope after
  shutdown.
- Futures from different schedulers cannot be combined.
- Terminal completion, failure, cancellation, eviction, and shutdown dispose
  all pending callbacks and captured continuations.
- Terminal completion retains final workflow-local state and inline query
  handlers until Core cache eviction. Queries can append their response to a
  sealed context, but never restart fibers or append durable commands. Eviction
  still releases the run and acknowledges its native lease with no command.
- Eviction emits no workflow command. A later replay creates a fresh execution
  rather than reusing native continuations.
- The focused runtime regression
  [`test_eviction_allows_fresh_replay_execution`](../../test/runtime/test_native_worker_execution.ml)
  checks both halves of that rule: the evicted generation is not invoked again,
  and a later start for the same run ID receives a new scheduler and can
  complete.

## Deterministic scheduling

- Runnable fibers receive monotonic sequence numbers and execute FIFO.
- Completed callbacks leave no execution-history list in the scheduler.
  Ordering tests record their own observations; production memory does not
  grow with the number of callbacks drained by a cached workflow. A full-GC
  regression checks retained live words across repeated callback batches.
- Spawn order is source execution order.
- Activation jobs are applied in their supplied list order.
- Initialization records the workflow start during that job pass, then queues
  the root fiber after the initial signal/update handlers. Those handlers are
  invoked in activation order before the root's first instruction, even when
  the root completes without suspending. A handler that suspends allows later
  handlers and the root to proceed; initialization does not wait for every
  handler to finish. Duplicate initialization, cancellation, eviction, and
  handler failure still prevent an invalid root invocation. The offline
  [initial-signals replay](../../test/integration/temporal/initial_signals/README.md)
  qualifies the ordering through Core and the production worker adapter.
- Core's `UpdateRandomSeed` replaces only the execution-local random stream at
  its position in that job pass, before resumed fibers run. The complete uint64
  seed survives the bridge as canonical decimal text; the runtime preserves
  its existing zero-seed fallback. Earlier workflow observations are retained.
  Strict activation validation rejects a malformed seed before any timer or
  other pending operation is consumed. The recorded
  [timer-reset regression](../../test/integration/temporal/reset_random_seed/README.md)
  checks the same stream through live execution and offline Core replay.
- Resolving a future appends its waiters in waiter-registration order.
- No hash-table traversal determines runnable or command ordering.
- Command sequence numbers are monotonic per execution and begin at one.
- Commands are returned in emission order.

## Futures and continuations

- A promise resolves at most once; unknown or duplicate external resolutions
  are bridge defects.
- A captured one-shot continuation is continued or discontinued exactly once.
- Derived futures retain scheduler identity, queueing, callback liveness, and
  suspension gates independently of the source result. A retained mapped summary
  must not keep its discarded activity payload alive. Ready ownership errors
  preserve a real suspension gate for later pending combinators. The activity
  weak-reference and live-heap probe in `test/runtime/test_future_retention.ml`
  checks collection while the owner and summary futures remain active.
- Awaiting a ready future does not perform an effect.
- A derived future evaluates its outside-owner fallback only when no result was
  observed after suspension. In particular, `Future.map_error` never invokes
  an error mapper for a successful result, even when the await resumed from a
  later activation.
- External signal/cancellation validation and encoding failures created inside
  a workflow retain its scheduler owner, callback liveness, and suspension gate.
  They emit no command or durable sequence, preserve their original typed error
  through joins, and obey the same ready-input ordering as successful futures.
  Calls outside a workflow remain inert; actual cross-workflow combinations
  still return an ownership defect.
- Awaiting a pending future outside its owning running scheduler returns a
  structured defect.
- `Future.both` and `Future.all` observe every input before settling and select
  the first error in input order; successful `all` values retain input order.
- `Future.race` and `Future.first` settle on the first completion, including an
  error, without cancelling losers. Already-ready inputs use registration order;
  pending inputs use deterministic callback order.
- A `Temporal.Condition` predicate is evaluated immediately and then only by
  the owning execution's activation drain. A false predicate owns one private
  scheduler future and one callback; a true predicate or typed predicate error
  creates no waiter. Notification snapshots waiters in registration order,
  removes each waiter before resolving it, and re-drains newly queued
  continuations so a state mutation in the same activation can release a
  condition without a synthetic timer. Deferred checks run before the scheduler
  releases its owner marker and callback liveness, so they can read workflow-local
  scope state just like the initial check. The notifier has no fiber effect
  handler: predicates must remain deterministic, non-blocking, and non-suspending.
  Context teardown deactivates every waiter
  before scheduler shutdown, so a late notification cannot retain or resume
  an ended workflow.
- A `Temporal.Scope` signal belongs to the same scheduler as the workflow
  futures it observes. Cancellation resolves that private signal and invokes
  each registered activity or child-workflow cancellation hook at most once;
  timers and unscoped operations remain observation-only. Every scope
  operation is owner-checked. While the scheduler is paused between runs,
  only `is_cancelled` and `check` may also be called by a synchronous query for
  the same live execution; a foreign or stale handle returns a typed defect
  rather than racing mutable state. Normal workflow teardown closes any
  still-pending signal and its callbacks. Repeating
  cancellation is idempotent; hook errors are aggregated as a typed first
  error after all hooks have been attempted. The owner check compares the
  currently running scheduler with the scheduler that created the scope, so a
  foreign scheduler cannot inspect or mutate the scope. A rejected foreign
  operation leaves the owner able to query and cancel its own scope, as
  covered by the cross-scheduler scope test.
- Combining futures from different executions returns a ready typed defect
  owned by the leading input rather than raising an operational exception.
- User callback exceptions are contained and reported as scheduler defects.
- Private scheduler shutdown and terminal-control exceptions pass through
  signal/update callback wrappers unchanged. Discontinuing an unfinished
  handler must release its continuation without turning teardown into a task
  defect or discarding the chosen terminal command. This does not drain
  unfinished handlers or imply that an accepted update completed.
- The implementation uses typed closures and GADTs, not `Obj.magic` or a
  heterogeneous untyped value store.

## Commands and terminal state

- Custom `Codec.make` encode/decode callbacks are invoked inside a codec
  exception boundary. An ordinary raised exception becomes a typed codec error
  without its potentially sensitive message; a returned error is preserved.
  Private terminal/shutdown control exceptions still unwind workflow fibers.
  Client start encoding failures submit no request, and a failed terminal
  decode does not discard the exact-run result needed for a later wait.
- Input payloads are encoded before scheduling a command.
- Activity outputs are decoded before resolving the typed public future.
- Child-workflow IDs are explicit, non-empty, valid UTF-8, and at most 65,536
  UTF-8 bytes. Invalid identity consumes neither a sequence nor a command. A
  child resolver is registered before its command is emitted. Core resolves a
  child in two stages: the start acknowledgment stores its non-empty run ID,
  while a later terminal resolution removes the resolver and decodes its
  payload. A start failure removes and resolves the future immediately. One
  terminal result may arrive without a start acknowledgment: when the
  workflow cancels a `Try_cancel` or `Abandon` child after
  `StartChildWorkflowExecutionInitiated` but before
  `ChildWorkflowExecutionStarted`, Core resolves it directly as cancelled.
  That `Cancelled` resolution is accepted only after the handle emitted
  `Cancel_child_workflow`, and completes the future with the typed
  cancellation error. Any other terminal result before start, a duplicate
  acknowledgment, or an unknown sequence is a non-retryable bridge defect; no
  event is silently dropped.
- Child task queues are optional validated identifiers. Explicit routing and
  parent-close policy survive runtime, JSON, and Core translation unchanged.
  Omission preserves existing server defaults and default command bytes. Parent
  closure and explicit child cancellation remain separate policy choices.
- Activities, child workflows, and timers share one monotonic command sequence.
- A terminated child resolves its pending future with a typed child-workflow
  error, including the termination cause and identity in its diagnostic. The
  parent can recover and schedule further work without rejecting its activation
  or stopping the worker. The native adapter regression covers this behavior
  with both live-mode and replay-mode activations and a subsequent run; the
  Rust regression obtains the termination from the pinned Core replay machine.
- When Core delegates a local activity retry delay, the original activity
  resolver and cancellation decision remain live while a separate workflow
  timer owns the delay. Cancelling during that delay removes the timer callback,
  emits its cancellation, and settles the original future as cancelled under
  every policy: the preceding attempt has already finished. A backoff delivered
  after cancellation settles the future without starting a timer. If the timer
  fires first, the next attempt is already scheduled and Core owns its
  cancellation according to the selected policy. Repeated cancellation never
  emits another command or revives a retry. The focused
  [runtime regression](../../test/runtime/test_local_activity_cancellation.ml)
  and [live history/replay fixture](../../test/integration/local_activity_cancellation/README.md)
  cover these ownership boundaries.
- Local-activity backoff commands are emitted in scheduler order, not during
  the activation job pass (#809). The backoff job and the retry timer's firing
  are validated in the job pass, but allocating the timer sequence and
  emitting `StartTimer`, and later re-emitting `ScheduleLocalActivity`, are
  queued scheduler work at that job's position, after fibers woken by earlier
  jobs. Core may merge jobs that a live worker received in separate
  activations (for example inside a heartbeating workflow task) into one
  activation on replay; queuing keeps sequence numbers and command order
  identical in both cases, as in the TypeScript and Python SDKs where the
  coroutine awaiting the activity handles its backoff. Cancellation while
  that work is queued settles the future and the queued work emits nothing.
  The [split/merged activation regression](../../test/runtime/test_local_activity_backoff_order.ml)
  compares the command streams.
- An activity retry policy is immutable once attached to a command. Its initial
  interval is positive, its maximum interval is at least the initial interval,
  its finite backoff coefficient is at least 1.0, and its maximum-attempt count
  is between zero and Int32.max_int; zero means unlimited attempts. The
  coefficient is serialized as canonical unsigned decimal IEEE-754 bits, not a
  JSON float, so OCaml, Rust, Core, and replay retain the same value.
- The schedule-activity object always contains a retry-policy member. JSON null
  means no explicit policy; an object means the validated policy above.
  Omission is malformed on both sides and cannot silently select a service
  default.
- Zero-duration sleep emits no timer.
- Positive sleep emits one timer and resumes only for its exact sequence.
- A workflow emits at most one terminal command.
- Continue-as-new is terminal: once its command is emitted, later jobs from
  the same activation (including timers or cancellation requests) are rejected
  or ignored according to their lifecycle state and cannot emit a follow-up
  command.
- Terminal command emission is retained while pending runtime state is torn
  down immediately.
- Unexpected code/codec defects and malformed bridge jobs fail the workflow
  task, discard all buffered commands and unsafe continuations, and preserve
  the open execution for replay. Deliberate typed application errors remain
  terminal workflow failures. See [workflow failures](workflow-failures.md).
- Completion ownership survives an uncertain acknowledgement. The adapter
  must retain the exact value and may not replace it or rerun workflow code.

## Replay

- Native continuations are cache optimizations and are never serialized.
- Replay reconstructs state by running the workflow again from its start.
- Identical definitions, inputs, and ordered activation jobs must produce
  identical command bytes.
- Workflow code must not use unrecorded wall time, randomness, I/O, process
  state, or nondeterministic iteration to affect commands.
- Patch decisions belong to one workflow execution. Core's `NotifyHasPatch`
  jobs are applied before workflow fibers run; absent a known marker,
  `Temporal.Workflow.patched` returns `not is_replaying`. The first answer for
  an ID is retained for the run: a notification seeds only IDs workflow code
  has not consulted, so it never flips a returned decision. A non-deprecated
  marker command is emitted on every call whose decision is `true` and never
  for a `false` replay decision. The unit-returning
  `Temporal.Workflow.deprecate_patch` retains the same private decision state
  and emits a deprecated marker under the same condition. The runtime does not
  deduplicate same-mode commands or share patch state between runs, but it
  rejects active and deprecated calls for one ID in one execution before
  emitting the second mode. Patch IDs are durable history keys, not deployment or process state.
- Replay-safe randomness is provided by `Temporal.Workflow.random_int`.
  Side-effect and replay-aware workflow logging APIs remain required before
  production release. The complete [PR #348 CI
  run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29411260374) verifies
  the two original live patch-in histories. The complete [PR #356 run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29469232271) additionally
  verifies active-to-deprecated and deprecated-to-removed replacement.

## Core boundary assumptions

- Core and every Cargo dependency are pinned and license-audited.
- Rust alone handles Temporal/Core protobuf. Strict JSON activation and
  completion documents cross the language boundary as owned buffers with one
  explicit free path; OCaml copies them into typed values before execution.
- Replay history uses the closed JSON document in
  `docs/schemas/bridge/replay-history.schema.json`. OCaml validates the
  envelope, canonical base64, and size limits before the supervisor sends it;
  Rust repeats those checks and then applies Core's protobuf/history
  invariants. A replay feeder has one bounded slot, and a completion or
  rejection is accepted only for the exact activation lease that was polled.
- The private supervisor validates native poll bytes before returning typed
  workflow or activity values to another Domain. It canonically encodes and
  reparses typed completions before entering C.
- An activity `Start` (remote or local) creates one Core completion debt. A `Cancel` poll
  is an update to that same token: it is handed to the OCaml activity adapter
  while the token remains tracked, but it never acquires a second completion
  lease. Only a cancellation that arrives after the start has completed is
  stale and may be discarded. This keeps cancellation delivery observable
  without allowing duplicate-token completion races. A cancellation the poll
  lane cannot attach to a live Start (unknown because the Start completed
  first, repeated, retired by disposal, malformed, or polled while draining)
  is dropped silently: it owns no completion debt, and a lane error for it
  would end `Worker.run` over a benign race (issue #801).
- If a Core activity task cannot be converted or encoded before it reaches the
  OCaml adapter, Rust fails only an unrepresentable `Start`, because that is
  the task that owns the completion debt. An unrepresentable `Cancel` is
  dropped as an update; failing it through the activity-completion API would
  consume the still-needed Start lease. The generated failure is a
  non-retryable application failure of type `UnrepresentableActivityTask`
  whose message carries only a static conversion category, because every
  redelivery of the same task would be rejected again. Once Core accepts it
  (which also releases the task's activity slot), the poll reports
  `Not_ready` and the worker keeps serving the queue; only a failed Core
  rejection is fatal (issue #801). Standalone activities (no workflow
  execution) are not representable and take this path.
- If OCaml cannot decode a successful poll, it returns the exact untouched
  Rust document to the private rejection ABI. Rust requires full semantic
  equality with retained handoff state before retiring the lease; changed IDs,
  tokens, or content cannot consume real outstanding work. Rejection cleanup
  for a retained Start removes ledger and semantic ownership together even
  when Core reports an error. A retained Cancel is different: it is only an
  update to the Start's shared token, so rejecting that document removes the
  one semantic update without retiring the Start's native completion debt.
  On a live worker a successful rejection is local task progress, so the
  supervisor reports an empty poll and keeps polling (issue #801); a workflow
  rejection then also applies the fixed 100 ms redelivery backoff. A replay
  keeps the OCaml protocol failure as its result, because skipping history it
  could not decode would report an unchecked replay as compatible. A failed
  rejection always keeps the original protocol failure primary.
- Rejecting a workflow activation fails its Core workflow task, except for a
  pure cache eviction: it owns no workflow task, so every rejection path
  (Rust conversion or encoding failure, OCaml decode failure, replay
  rejection) acknowledges it with an empty completion instead. Failing an
  eviction leaves it outstanding in release Core and panics debug Core
  (issue #814).
- Native `Not_ready` is represented as `Ok None`. ABI version 2 also exposes
  bounded `Wait_workflow`, `Wait_activity`, and combined `Wait_any` readiness
  operations. Only the
  owner-Domain supervisor may invoke them; the C boundary releases the OCaml
  runtime lock while Rust waits, and no workflow fiber or effect scheduler
  invokes or blocks on a native lock, condition variable, or timer.
- Rust panics, decode errors, and Core failures become explicit bridge errors.
- Foreign runtime threads never call arbitrary OCaml closures.
- Blocking FFI calls occur only while the OCaml runtime lock is released.
- Worker readiness waits are bounded to 100 ms and return `Not_ready` on a
  quiet lane, so a supervisor handler cannot strand a queued shutdown request.
  Only one idle execution lane enters a native wait at a time; the preferred
  lane alternates after each wait, with a one-yield fallback for staggered idle
  polls. The live worker's native wait is `Wait_any`, so work on either lane
  ends it and the token holder can never sleep through the sibling's task
  (#806). While its sibling is busy or already owns the wait, a lane yields
  locally for 10 ms, then retries its nonblocking poll. The poll reports a
  fatal Rust lane error even with no queued task.
- The workflow execution Domain and capacity-one activity execution Domain
  share the same serialized supervisor mailbox. The activity adapter retains
  exclusive ownership of an attempt and its completion retry. Worker shutdown
  joins that Domain before draining either adapter or releasing native handles;
  a callback that never returns therefore leaves shutdown waiting without an
  overall deadline (tracked by #495).
- A retained activity completion may be retried only after the OCaml source
  receives the explicit bridge `Retryable` status. The pinned Core completion
  implementation removes the activity lease before suppressing generic network
  failures, so `Connection`, `Not_ready`, and `Worker` never authorize a
  second completion attempt. The dedicated retry backoff is a 10 ms native
  timer with the OCaml runtime lock released; it is not a readiness signal.
- Both the workflow and activity adapters record the first completion failure
  that is not explicitly retryable (a typed error, an exception, or an async
  handle admission that fails after Core accepted `WillCompleteAsync`) on the
  retained entry. From then on neither a later `poll`, a second `Worker.run`,
  nor a shutdown drain submits that completion again; each returns the
  recorded error without a native call, and only terminal `discard` releases
  it (issue #843). No workflow completion failure is retryable: the bridge
  defines `Retryable` only for activity completion, and pinned Core reports
  only deterministic validation failures from
  `complete_workflow_activation`, so an identical resubmission could at best
  fail again and, after a lost acknowledgement, could complete a later
  activation of the same run.
- Namespace-bound async heartbeats and async complete/fail/cancel do not
  consume that Core completion lease, so they do not fail closed. An
  uncertain RPC `Connection` keeps the public handle and adapter async lease
  tracked and returns a retryable error; a terminal operation may then only
  be retried byte-for-byte, while an uncertain heartbeat is dropped so it
  never blocks a newer heartbeat or the terminal operation. A definite
  non-`NotFound` RPC rejection releases that request key (even after an
  earlier uncertain attempt of the same request) but keeps both the handle and
  lease live for a corrected or different operation. Worker drain must still
  report the outstanding obligation. Confirmed `NotFound` maps to
  `Invalid_state` and closes the handle. The adapter removes the lease exactly
  once: on an accepted terminal operation or a retiring error. This decision
  does not change Core worker completion retry policy.
- Adapter shutdown reopens admission only for an explicitly retryable activity
  drain. Workflow-drain errors and permanent activity errors invoke the
  supervisor's `Native.shutdown`/`runtime_close` path before leaving the private
  worker closed and the public wrapper terminal; runtime disposal force-retires
  any remaining native leases. A returned native `Error` is still
  release-complete by that contract, so OCaml adapter maps are discarded only
  after the result is observed. If native shutdown raises before returning, the
  maps remain retained, a terminal-cleanup-pending flag schedules a detached
  retry, and the worker finalizer remains a last-resort path. A shutdown
  defect from either execution lane's own system thread is different: it
  cannot wait for its own lane to finish, but no teardown has started, so it
  remains retryable for a later call from any other thread. Lane identity is
  the system thread (Domain plus `Thread.id`), so a sibling thread on a lane's
  Domain is an ordinary caller. The public wrapper checks this before
  acquiring its shutdown mutex to avoid a deadlock against a concurrent
  shutdown that holds that mutex while waiting for the loop. That rejected
  call, and `Worker.request_shutdown`, set a separate sticky stop-request
  atomic that both lanes observe like the shutdown flag but that never admits
  teardown, so the loop returns and a later `shutdown` (from any thread,
  including the one that ran the loop) performs the drain and native release.
  Setting it is the only lifecycle operation that is safe inside an OCaml
  signal handler: it takes no lock and makes no native call (#830).
- Each Rust poll lane owns one mutex-protected pending count. Producers hold
  that mutex while publishing a queue message and its wake notification;
  the supervisor holds it while receiving and decrementing. A wake is never
  considered proof of readiness without rechecking the protected predicate.
- Shutdown closes both readiness signals before waking Core polls, but queued
  messages always take precedence over terminal state and are drained before a
  readiness wait reports shutdown or a fatal lane error.
- Explicit worker shutdown never waits for OCaml after initiating Core
  shutdown: it completes every leased or queued task itself while joining both
  poll lanes, waits at most 90 s for the join and 30 s for Core finalization,
  and keeps the worker owned (by the graph or by the finalizer task) until
  `finalize_shutdown` returns. A force-completed lease is reported as
  `Outstanding_tasks` after the worker has been released (issue #769).
- Dispose force-fails ledger debt and queued tasks before joining the Core poll
  lanes so shutdown cannot wait for OCaml. Because a poll already in flight can
  publish a task after that first drain, dispose joins both lanes (with the
  same bounded, draining join as explicit shutdown) and performs a
  final no-producer drain before finalization; no task may remain only in a
  ready queue or ledger at the point the worker graph is released.

## Native activation translation

- `Temporal_runtime.Native_execution` is a pure OCaml boundary below the
  supervisor. It owns no Rust handle, performs no I/O, and does not block a
  workflow scheduler.
- A typed activation is revalidated with the canonical protocol encoder before
  any execution state is touched. Sequence numbers, identifiers, payloads,
  timestamps, ordering, and closed-object invariants therefore have one
  validation path for JSON input and programmatic OCaml values.
- A rejected activation job is side-effect free: malformed child-resolution
  JSON cannot allocate a sequence, consume a resolver, or resolve a future.
  Lifecycle checks then reject terminal-before-start, duplicate start, duplicate
  terminal, and unknown-sequence messages as bridge defects without replacing
  the state established by a valid message.
- Activation jobs and emitted commands retain source ordering. Every payload
  is copied; binary protocol metadata that cannot be represented by the
  runtime's string metadata map is rejected rather than lossy-decoded.
- Initialization, cancellation, replay metadata, and cache-eviction details
  remain available in the translated activation even where the first runtime
  kernel uses only a marker job. Eviction is acknowledged with an empty
  completion and never emits workflow commands.
- A valid value with no lossless representation is an explicit typed
  `unsupported` error. Activity commands carry every exposed Core field, and
  child-start commands carry the workflow identity and input payload. Rust
  injects the already validated worker namespace into each Core child-start
  command because Core copies it into child failure metadata. Child resolution
  retains start run IDs, terminal payloads, typed failure info, and cancellation
  state. Other options not yet exposed by the OCaml runtime remain explicit
  Core defaults; the adapter never fabricates a language-level option or
  silently drops a non-default value.
- Unknown or duplicate operation sequences are bridge defects. They fail the
  execution rather than being ignored, because ignoring them would make
  replay diverge from the history supplied by Core.

## Supervisor mailbox

- One mailbox owner Domain invokes every handler; producers never execute a
  handler while admitting or awaiting work.
- The bounded FIFO order is the total order of successful enqueue mutations
  under the mailbox mutex. One producer's program order is preserved;
  concurrent producers have no stronger order before those mutations.
- SDK shutdown is admitted through the mailbox's reserved terminal slot. Its
  FIFO append and `Open` to `Closing` transition happen under the same mutex;
  it may temporarily raise the waiting queue to `capacity + 1`, and no later
  normal request can be admitted ahead of it.
- Concurrent `submit_and_close` calls linearize at that same mutex. For an
  open mailbox with no handler failure, exactly one contender appends the
  terminal request and gets a pending reply; every other contender observes
  `Closed`. If the admitted terminal handler fails before a later contender
  reaches the mutex, that contender instead observes the terminal
  `Handler_raised` failure. The winning request remains after work already
  admitted, and normal posts submitted after the transition are rejected. The
  regression test releases multiple producer Domains through a barrier before
  the race and checks the single winner, losing results, late rejection, and
  FIFO drain.
- Queue and lifecycle state are data-race free. Every condition wait rechecks
  its protected predicate after waking.
- Normal close rejects new work and drains all admitted work before the owner
  stops. An unexpected handler exception rejects new work, discards queued
  posts, and settles the active and queued calls with the same failure.
- A terminal reply remains owned by its admitted queue entry until the owner
  settles it. Dropping the caller's pending capability cannot strand the owner
  or change the terminal result observed by `join`.
- Blocking mailbox entry points run only on ordinary producer Domains. Future
  Eio or workflow-effect adapters must offload them rather than blocking a
  cooperative scheduler Domain.
- A handler never calls `post`, `call`, or `join` on its own processor.
  `call` and `join` cannot complete while the sole owner is executing that
  handler, and `post` can block if the bounded queue is full. A handler may
  call `close`, which does not wait for the owner and preserves drain semantics.

## SDK instance supervisor

- Exactly one dedicated owner Domain creates, uses, and closes the complete
  runtime/client/worker graph for an SDK instance. Individual native handles do
  not receive their own actors.
- Backend state never appears in a producer-facing operation or result. Typed
  GADT operations may expose ordinary copied values but cannot return a raw
  native handle.
- Expected operation errors leave a running graph usable. An unexpected
  backend exception marks the graph terminal, attempts cleanup exactly once,
  and becomes the common mailbox failure for active, queued, and later calls.
- Shutdown is admitted in FIFO order, invalidates the graph before later work
  can use it, closes and joins the owner, and caches the exact terminal result.
  Repeated or concurrent shutdown invokes backend destruction at most once.
- A shutdown which races a completed or abandoned asynchronous start drains
  every pending ticket, aborts and joins each Tokio task, and only then
  releases the client/Core graph. A result already queued for an abandoned
  ticket is discarded with its receiver; it cannot cause a second task join or
  a second native free.
- Exact-run client waits retain their history future and pagination state
  across bounded owner turns. The runtime admits at most 64 distinct pending
  executions and at most 64 outstanding start tickets, retiring each on a
  terminal result or error. Admission beyond either bound returns
  `Resource_exhausted` without side effects and leaves the client usable; it
  never reuses `Invalid_state`, which callers treat as a closed graph. Disconnect and
  runtime shutdown cancel all retained futures before releasing Core; no
  background wait task outlives the owner.
- A backend shutdown result, including `Error`, means the graph has been
  consumed or invalidated. A retryable operation must not masquerade as
  terminal shutdown while it still owns live resources.
- Supervisor entry points may block and run only on ordinary producer Domains.
  Fiber runtimes must offload them; deterministic workflow schedulers must not
  invoke them directly.
- The abandoned-instance finalizer never calls a blocking supervisor entry
  point itself. It delegates normal shutdown to a dedicated system thread; an
  already completed explicit shutdown schedules no redundant cleanup.
- A handler that waits for native worker readiness uses the bounded bridge wait
  and returns to the mailbox between retries; it never performs an indefinite
  condition wait that could block the mailbox's reserved shutdown transition.

## Recent regression evidence

The lifecycle edge tests in `test/runtime/test_activation.ml` exercise the
terminal continue-as-new rule, including later activations and cancellation
remaining inert, and verify that cancelling a child after a failed start is a
typed no-op. `test/runtime/test_scope.ml` verifies repeated scope cancellation
does not emit a Temporal command. The bilateral Rust test
`rust/core-bridge/tests/workflow_protocol.rs` rejects a continue-as-new
completion that contains a follow-up timer, both during JSON encoding and Core
conversion. These tests are local runtime/protocol evidence; they do not claim
live Temporal Server coverage.

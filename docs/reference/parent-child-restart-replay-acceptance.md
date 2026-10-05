# Parent/child restart and replay acceptance

This acceptance scenario proves that one OCaml worker generation can be
removed while a parent and its long-running child are both suspended, and that
a fresh OCaml worker can replay and complete those same two executions. It is a
recovery test for the SDK worker path, not a user-facing persistence protocol.

The live gate is `make test-temporal-parent-child-restart`. Its Docker-free
contract half validates checked-in evidence fixtures and rejection cases. Its
live half creates a fresh PostgreSQL-backed Temporal namespace, runs two worker
generations and a separate client/assertion binary, and deletes the Compose
volume when it finishes. Passing only the contract half does not prove that a
Temporal worker replay occurred.

## Process roles and exact-run authority

The client binary starts one parent and waits on the exact handle returned by
`Temporal.Client.start`. It does not register or execute workflow code. The
worker binary registers only the parent and child workflow definitions. The
two Compose worker services execute that same binary in sequence; generation
one is stopped and removed before generation two starts.

The controller accepts the parent run ID only from the typed OCaml start
handle. It then reads that exact parent history and learns the child run ID
from its unique `ChildWorkflowExecutionStarted` event. It never uses a
latest-run lookup for either execution. Every later history request and replay
assertion supplies the exact workflow/run pair.

## Three history stages

The normalizer reduces Temporal CLI history to a closed, payload-free semantic
record described by
[`parent-child-restart-replay-history.schema.json`](../schemas/acceptance/parent-child-restart-replay-history.schema.json).
The validator relates both histories at three stages:

1. `initial` requires the parent to have durably started the exact child and
   completed its second workflow task without a terminal child event. The
   child must have completed its initial workflow task and started its durable
   timer without firing or terminating.
2. `post_removal` preserves both initial prefixes after generation one has
   stopped and its container has been removed. Temporal Server may append only
   `TimerFired` and `WorkflowTaskScheduled` to the child; no replacement worker
   may have started that task yet.
3. `terminal` preserves the post-removal prefixes, requires generation two to
   process the child's timer completion, correlates the exact child completion
   back to the parent's initiated and started event IDs, and requires both
   executions plus the exact client wait to complete successfully.

The ordered controller document also proves worker stop/removal, non-overlap,
fresh generation-two readiness, exact-run replay observations, and final
PostgreSQL volume deletion. Its closed shape is defined by
[`parent-child-restart-replay-controller.schema.json`](../schemas/acceptance/parent-child-restart-replay-controller.schema.json).

## Private checkpoint lifecycle

One test-only observer inside `Native_worker` receives already validated
activation metadata on the worker adapter's serialized path. It retains an
immutable state-machine value for exactly two fixed roles; it is not another
actor, Domain, native handle, or callback from a Rust thread.

Generation one knows the configured parent and child workflow IDs but learns
their server-assigned run IDs from their initial activations. It publishes
nothing after seeing only the parent. After both roles are known it constructs
one canonical document containing the two identities followed by parent and
child initial records.

Generation two is configured with both exact run IDs and first strictly reads
the complete generation-one document. Replay activations may arrive in either
role order, but the completed document always appends parent replay then child
replay. The format is defined by
[`parent-child-restart-replay-diagnostics.schema.json`](../schemas/acceptance/parent-child-restart-replay-diagnostics.schema.json).
Identifiers are valid UTF-8, contain no control bytes, and are bounded to 4096
bytes so four worst-case JSON-escaped identities fit the 65536-byte file
limit. History lengths use canonical positive signed-64 decimal strings.

The state machine is pure. The adapter computes a candidate state, encodes and
writes the complete document to a same-directory temporary file, flushes it,
atomically renames it over the old checkpoint, and only then commits the
candidate in memory. A failed write leaves the preceding state available for
redelivery. The temporary file and final file are owned by the worker process;
the live controller owns their removal. No payload bytes, task tokens,
continuations, native pointers, or Rust handles enter this diagnostic.

This publication order gives atomic visibility, not crash-durable `fsync`
semantics. That is sufficient for acceptance evidence because generation one
must finish the write before the controller stops it and generation two
strictly rejects a missing, partial, oversized, or non-canonical document.

## Evidence boundary

The complete [PR #351 CI
run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29434016013) passed all
nine jobs and live-verified this scenario against Temporal Server and
PostgreSQL. Local state-machine tests and contract fixtures remain supporting
validation evidence rather than substitutes for that run. The separate
[child-failure-after-replay acceptance](child-failure-replay-acceptance.md) is
live-verified by the complete [PR #361 CI
run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29475615866); it covers
typed child failure after both replacement-worker replay observations. Do not
generalize either acceptance run to arbitrary cache pressure, crashes, child
failure recovery, or a replay corpus.

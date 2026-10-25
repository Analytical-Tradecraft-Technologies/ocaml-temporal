# Workflow patching

`Temporal.Workflow.patched` and `Temporal.Workflow.deprecate_patch` implement
the first two workflow-patching lifecycle steps in the public OCaml API. A
workflow can retain its legacy branch for histories created before a behavior
change, then mark the patch as deprecated while incompatible histories drain.

This is not a general deployment-versioning system. Removing a deprecated
patch call is supported for histories that passed the documented lifecycle
gates. Legacy build-ID and deployment-based worker routing are available
separately through `Temporal.Worker.Options`; side effects, arbitrary historic
compatibility, and migration tooling remain separate work.

## Authoring contract

Give each behavioral change a stable, descriptive identifier:

```ocaml
let process_order order =
  let open Temporal.Result_syntax in
  if Temporal.Workflow.patched ~id:"orders.validate-address.v2" then
    let* () = validate_address order in
    ship order
  else ship order
```

For a newly started execution, the first call returns `true` and emits a
non-deprecated patch-marker command. During replay, it returns `true` only
when Temporal Core reports that the marker exists in that execution's history;
replay of an older history without the marker returns `false`. The first
answer for an ID is retained for the execution, so ordinary OCaml helper
functions can call `patched` without threading version state through their
arguments, and a later `NotifyHasPatch` job for an ID already consulted never
changes that answer. A notification that arrives before the first call seeds
the decision; once workflow code has branched on it, the decision is fixed for
the run. This matches the memoization in Temporal's official SDKs. A late
notification is still remembered: the next call for that ID keeps returning
`false` but emits the `SetPatchMarker` command Core needs to consume the
reported history marker, because Core treats an unmatched non-deprecated patch
marker as nondeterminism.

Patch IDs are durable history keys. They must be non-empty, valid UTF-8,
NUL-free, and no more than 65,536 bytes. Invalid IDs and calls outside workflow
execution raise `Invalid_argument` because they are programmer defects. Never
reuse an ID for a different change, derive it from mutable configuration, or
build it from nondeterministic data.

A call emits `SetPatchMarker` when its retained decision is `true`, or after
a late notification as described above. A `false` replay decision with no
such notification emits nothing: pinned Core creates a real
`RecordMarker` command for every `SetPatchMarker` it receives during replay and
keeps it queued unless a later history command displaces it, so emitting a
marker for the legacy branch could record a marker contradicting the branch the
run actually took (issue #788). Repeated `true` calls emit repeated same-mode
commands; Core skips IDs for which it has already created a marker command.
An emitted completion command is not itself history evidence; the live
acceptance below checks the normalized server history separately.

### Deprecate a patch

Only after marker-free executions can no longer replay across the changed
branch, replace the gate at the same logical point with a lifecycle-only call:

```ocaml
let process_order order =
  let open Temporal.Result_syntax in
  Temporal.Workflow.deprecate_patch ~id:"orders.validate-address.v2";
  let* () = validate_address order in
  ship order
```

`deprecate_patch` returns `unit`: it records lifecycle intent and must never be
used as a branch decision. It shares the same retained decision as
`patched`: a fresh execution, or replay of a history whose marker Core
reported, emits `SetPatchMarker { deprecated = true }` (repeated calls emit
repeated commands for Core to deduplicate), while replay of a marker-free
history emits nothing, matching Core's "call allowed" outcome. Do not call
`patched` and `deprecate_patch` for the same ID during one workflow execution.
Core retains the first marker command for an ID, so the OCaml runtime rejects
mixed modes with `Invalid_argument` before it can emit an ambiguous completion.

A history notification establishes only that the marker exists; it does not
lock the source-level mode. This lets replacement code replay an older
non-deprecated marker while calling only `deprecate_patch`. A fresh execution
of that replacement code emits a deprecated marker.

There are two separate safety gates. Before replacing `patched`, old worker
builds must no longer create marker-free executions and every execution that
could replay the old branch at this point must have drained, continued as new,
been reset or migrated, or been proved command-compatible with the new branch.
Core accepting a deprecation call without a marker does not make different
old/new branch commands deterministic. Later, before removing
`deprecate_patch`, histories containing the older non-deprecated marker must
also have drained or been otherwise accounted for. The live acceptance plan
therefore migrates active-marker histories, never marker-free legacy histories,
into the deprecation phase.

## Replay contract

The bridge preserves this order for each workflow task:

1. Rust converts Core's `NotifyHasPatch` job into the closed private JSON
   protocol.
2. OCaml validates the complete activation and installs the execution-local
   replay and patch-notification state before workflow fibers run.
3. `Temporal.Workflow.patched` fixes the execution-local branch decision on its
   first call and emits `SetPatchMarker { deprecated = false }` when that
   decision is `true` or a late notification reported the marker, while `Temporal.Workflow.deprecate_patch` retains the
   same private decision state and emits `SetPatchMarker { deprecated = true }`
   under the same condition without exposing a boolean.
4. Rust validates the completion and converts the marker back to Core's
   protobuf command.

The decision must not depend on environment variables, current deployment,
process-global mutable state, wall-clock time, or whether another workflow run
has seen the patch. Core gives query jobs their own activation. A query
activation mixed with a patch notification is rejected bilaterally rather than
accepted as a new bridge convention.

## Live replay acceptance

`make test-temporal-workflow-patching` is the dedicated real-server acceptance
target. It first runs the Docker-free
`make test-temporal-workflow-patching-contract`, then uses real PostgreSQL and
Temporal Server containers with separate public OCaml processes: a long-lived
worker executes workflow code, while a one-shot client driver starts exact runs
and checks their terminal results.

The controller executes three source-replacement scenarios for the same stable
test patch ID:

| Scenario | Required observations | What the result establishes |
| --- | --- | --- |
| Legacy history | A legacy worker runs a definition with no `patched` call and reaches a durable timer. The normalized initial and terminal histories contain zero patch markers. A fresh patch-aware worker receives that exact run with `is_replaying=true`, takes the legacy activity branch, and the driver receives the fixed legacy result. | A history created before the change retains its legacy behavior after worker replacement. |
| Active to deprecated | A patch-aware worker starts a run whose history contains exactly one non-deprecated marker. Generation two contains only `deprecate_patch`, receives the exact run with `is_replaying=true`, runs the new behavior unconditionally, and leaves the false marker and initial history prefix unchanged. | An active-marker history remains deterministic when the branch decision is replaced by the lifecycle-only deprecation call. |
| Deprecated to removed | A deprecation worker starts a run whose history contains exactly one marker with `deprecated=true`. Generation two is a separately compiled definition containing no patch API, receives the exact run with `is_replaying=true`, runs the same behavior, and leaves the true marker and initial prefix unchanged. | A deprecated-marker history remains deterministic after the patch call is removed from source. |

The initial and terminal history checks are both intentional. The controller
uses the server's normalized history and branch-specific activity/result
oracles instead of inferring a decision from a worker log or from the locally
buffered completion commands. The replacement workers must have distinct
containers and fresh native runtime graphs; a restarted process or a client
that manufactures a result is not sufficient evidence.

The Docker-free contract checks the checked-in normalized histories,
replay-diagnostic and controller fixtures, that the schema documents are
readable JSON objects, the history normalizer, and representative malformed
cases. It does not run a JSON Schema validator against those fixtures, build
workers, start containers, contact Temporal Server, or establish that a replay
occurred.
The complete [PR #348 CI
run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29411260374) records the
successful real-server invocation of the two original patch-in scenarios. The
complete [PR #356 run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29469232271)
also verifies the active-to-deprecated and deprecated-to-removed transitions.

## Lifecycle replay matrix

Issue [#531](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/531)
extends the evidence above with recorded histories in the
[replay history corpus](history-corpus.md), so every boundary below is
checked offline by `dune runtest` through the public `Temporal.Replay` API.
Four source generations of each `corpus.lifecycle.*` workflow differ only at
their patch gates: **legacy** (no patch call, legacy branch), **patched**
(`patched` chooses), **deprecated** (`deprecate_patch`, then the new branch)
and **removed** (no patch call, new branch). The legacy and new branches
schedule different activity types, so a branch change is visible to Core.

| Workflow | Patch placement |
| --- | --- |
| `corpus.lifecycle.multi` | Three independent patch IDs in one workflow: one swaps an activity type, one adds a durable timer, one swaps a child workflow type. |
| `corpus.lifecycle.loop` | One patch ID consulted in each of three loop iterations, after the previous iteration's activity resolved. |
| `corpus.lifecycle.handlers` | One patch inside a signal handler and another inside an update handler; each handler runs the chosen activity and the root waits for both. |
| `corpus.lifecycle.handover` | A run started and parked by a legacy worker, then released on a fresh patched worker that reaches the patch point for the first time. |

Each of the first three workflows has one history per stage and is replayed
against all four generations. The observed verdicts, identical for all three
workflows, are:

| History recorded by \ replayed on | legacy | patched | deprecated | removed |
| --- | --- | --- | --- | --- |
| legacy (no marker, legacy branch) | ok | ok | **nondeterminism** (activity type) | **nondeterminism** (activity type) |
| patched (active marker) | **nondeterminism** (unconsumed marker) | ok | ok | **nondeterminism** (unconsumed marker) |
| deprecated (deprecated marker) | **nondeterminism** (activity type) | ok | ok | ok |
| removed (no marker, new branch) | **nondeterminism** (activity type) | **nondeterminism** (activity type) | ok | ok |

Each negative control also declares the Core reason it must fail with (for
example `Non-deprecated patch marker encountered for change ...`), so a
removal is reported for the documented cause rather than any mismatch. What
the matrix establishes for this SDK and the pinned Core:

- **Upgrade order is safe.** Legacy histories replay on patched code, active
  markers on deprecated code, deprecated markers on removed code. This holds
  for several patches in one workflow, for a patch consulted repeatedly in a
  loop (the first answer is retained for the run), and for patches inside
  signal and update handlers.
- **Each gate is necessary.** Deprecating while marker-free legacy runs remain
  fails on the legacy-branch history (the first gate), and deleting a
  `patched` call without the deprecation step fails on every active-marker
  history (the second gate).
- **Rollback is stage-dependent.** Code at the deprecated stage can roll back
  to the patched generation (a deprecated marker satisfies `patched`), and the
  removed stage can roll back to the deprecated generation. Rolling back to
  the legacy generation fails for every history written by a later generation,
  and rolling back from removed to patched fails because `patched` reports
  `false` without a marker.

The handover history models one step of a rolling deploy: the run was created
by the old worker, which completed its first task, and finished on a new
worker. Because that run had not yet reached the patch point, `patched`
returned `true` on the new worker and recorded its marker after a
marker-free prefix. That history replays on the patched and deprecated
generations, but not on the legacy or removed ones. A run that reaches a
patch point on a new worker therefore cannot safely return to an old worker,
which is why an old worker must not keep polling the task queue once new
workers serve it. Concurrent old and new workers on one task queue, where the
server may hand any task to either build, are not exercised: their outcome
depends on task routing rather than SDK behavior, and the SDK makes no
routing claim (see [worker versioning](worker-versioning.md)).

The capture program `history_corpus_capture.exe` records these histories from
a disposable Temporal Server; the
[corpus reference](history-corpus.md#capturing-and-adding-histories) documents
the command and the provenance retained for the `live-2026-10-11` capture.

## Focused evidence and remaining boundary

Focused OCaml runtime tests cover patch-in decisions, deprecated marker
emission, replay with and without a notification (including the absence of a
marker for a `false` decision), late notifications that must not flip a
retained decision, repeated same-mode calls,
mixed-mode rejection, execution isolation, native completion translation, and
copying mutable source strings before they enter durable state. Shared fixtures
and Rust tests cover strict JSON validation, duplicate command preservation,
and round trips through the pinned Core protobuf types. The expanded live target
adds exact server-history and worker-replacement evidence for lifecycle
transitions once its complete CI run succeeds.

The [lifecycle replay matrix](#lifecycle-replay-matrix) adds recorded
histories for multiple patches, loops, signal and update handlers, removal,
rollback between stages and a mid-run upgrade.

Worker routing, including legacy build-ID and deployment-based versioning, is
configured separately through `Temporal.Worker.Options`; see
[worker versioning](worker-versioning.md). Version routing is deferred by the
[v1 support policy](v1-support-policy.md), so no corpus case or live test
claims that a particular build receives an execution. Side effects, arbitrary
historical compatibility, concurrent mixed-version workers on one task queue,
automated migration safety analysis, and histories that have not passed both
documented gates remain separate roadmap work. For the overall
evidence boundary, read [live acceptance coverage](live-acceptance-coverage.md).

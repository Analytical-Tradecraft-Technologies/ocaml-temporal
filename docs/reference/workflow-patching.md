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
replay of an older history without the marker returns `false`. The decision is
retained for the execution, so ordinary OCaml helper functions can call
`patched` without threading version state through their arguments.

Patch IDs are durable history keys. They must be non-empty, valid UTF-8,
NUL-free, and no more than 65,536 bytes. Invalid IDs and calls outside workflow
execution raise `Invalid_argument` because they are programmer defects. Never
reuse an ID for a different change, derive it from mutable configuration, or
build it from nondeterministic data.

Every call emits the idempotent `SetPatchMarker` command, including repeated
calls with the same ID. The OCaml runtime must not locally deduplicate those
commands: Core owns durable marker state and history-machine deduplication.
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
used as a branch decision. Repeated deprecation calls are allowed and emit
`SetPatchMarker { deprecated = true }` for Core to deduplicate. Do not call
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
3. `Temporal.Workflow.patched` selects the execution-local branch decision and
   emits `SetPatchMarker { deprecated = false }`, while
   `Temporal.Workflow.deprecate_patch` retains the same private decision state
   and emits `SetPatchMarker { deprecated = true }` without exposing a boolean.
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
run](https://github.com/mfow/ocaml-temporal/actions/runs/29411260374) records the
successful real-server invocation of the two original patch-in scenarios. The
complete [PR #356 run](https://github.com/mfow/ocaml-temporal/actions/runs/29469232271)
also verifies the active-to-deprecated and deprecated-to-removed transitions.

## Focused evidence and remaining boundary

Focused OCaml runtime tests cover patch-in decisions, deprecated marker
emission, replay with and without a notification, repeated same-mode calls,
mixed-mode rejection, execution isolation, native completion translation, and
copying mutable source strings before they enter durable state. Shared fixtures
and Rust tests cover strict JSON validation, duplicate command preservation,
and round trips through the pinned Core protobuf types. The expanded live target
adds exact server-history and worker-replacement evidence for lifecycle
transitions once its complete CI run succeeds.

Worker routing, including legacy build-ID and deployment-based versioning, is
configured separately through `Temporal.Worker.Options`; see
[worker versioning](worker-versioning.md). Side effects, arbitrary historical
compatibility, automated migration safety analysis, and histories that have not
passed both documented gates remain separate roadmap work. For the overall
evidence boundary, read [live acceptance coverage](live-acceptance-coverage.md).

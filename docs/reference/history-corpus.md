# Replay history corpus

The replay history corpus is a versioned set of recorded Temporal workflow
histories that every candidate SDK must replay, unchanged, against frozen
application code. It is the seed for issue
[#503](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/503)'s
compatibility gate: histories produced by an older SDK, Core pin or server are
replayed by the candidate without being regenerated, and a deliberately
incompatible definition must still be reported as nondeterministic.

| Part | Location |
| --- | --- |
| Histories and manifest | [`test/fixtures/history-corpus/`](../../test/fixtures/history-corpus/) |
| Manifest schema (v1) | [`docs/schemas/history-corpus/manifest.schema.json`](../schemas/history-corpus/manifest.schema.json) |
| Frozen definition sets | [`test/history_corpus/corpus_definitions.ml`](../../test/history_corpus/corpus_definitions.ml) |
| Gate and upgrade runner | [`test/history_corpus/history_corpus_runner.ml`](../../test/history_corpus/history_corpus_runner.ml), [`corpus_runner.ml`](../../test/history_corpus/corpus_runner.ml) |
| Runner negative-path test | [`test/history_corpus/test_history_corpus_mismatch.ml`](../../test/history_corpus/test_history_corpus_mismatch.ml) |
| Capture program and scripts | [`test/history_corpus/capture/`](../../test/history_corpus/capture/), [`test/history_corpus/scripts/`](../../test/history_corpus/scripts/) |

## What is checked, and where

`history_corpus_runner` is an ordinary Dune test, so `dune runtest`, `make test`
(and therefore `make verify` on every Linux matrix leg of every PR) and the
native Windows/macOS `make native-test` jobs all run it; no Temporal Server or
Docker is involved, and the whole corpus replays in about a second.
`make test-history-corpus` runs only the corpus tests. It fails, naming the
entry ID, when:

- the manifest violates the v1 schema (every object is closed), a capture or
  entry reference is dangling, a capture is unused, or a negative control does
  not name a `replays_ok` entry with the same history;
- a history file's SHA-256 differs from the manifest, a referenced file is
  missing, or a file under `histories/` is not referenced (orphan);
- the `WorkflowExecutionStarted` event decoded from an entry's protobuf
  history (the bytes that are replayed) does not record the entry's
  `workflow_type` and `run_id` (`original_execution_run_id`, the run's own
  ID), or the optional JSON copy disagrees with them;
- the [feature coverage table](#feature-coverage) is violated: a `replayed`
  feature has no `replays_ok` entry, a negative-control feature has no entry
  with that verdict, an entry carries a feature tag that has no row, a
  `not yet captured` feature has a `replays_ok` entry (promote the row), a
  `not applicable` feature is tagged on any entry, or the table in this
  document differs from the runner's;
- a `replays_ok` entry does not return `Ok ()` from `Temporal.Replay.replay`
  with only the entry's workflow type registered from its definition set
  (so a history recorded for another type cannot pass); or
- a `nondeterminism` entry does not return `Nondeterminism` for the entry's
  run ID, or an `invalid_history` entry does not return `Invalid_history`, or
  a negative control's public failure text does not contain its optional
  `expected_message`.

The SHA-256 implementation is a small test helper checked against the FIPS
180-4 vectors before any manifest is read, because the OCaml standard library
has no SHA-256 and a hashing dependency would only serve this test.

## Replay path

The runner replays through the public
[`Temporal.Replay`](../../lib/public/replay.mli) API (issue
[#515](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/515)),
the same entry point applications use for their own pre-deployment checks, so
an SDK or Core upgrade is judged by the behavior users observe. Each entry is
an independent `Temporal.Replay.replay` call: its own native replay graph and
owner Domain, released before the next entry runs, with the API's 30-second
no-progress bound. The input is the entry's binary
`temporal.api.history.v1.History` protobuf and workflow ID, passed to
`Temporal.Replay.History.of_protobuf` unchanged.

The public API returns no run ID or workflow type for a successful replay.
The #518 gate used a private replay path to check those; the runner instead
reads them from the start event of each entry's protobuf history, the exact
replay input, and registers only the entry's workflow type.
[`corpus_history_identity.ml`](../../test/history_corpus/corpus_history_identity.ml)
walks just the needed protobuf fields (field numbers from the Temporal API
protos at the pinned Core revision), because no Temporal protobuf decoder is
reachable from OCaml test code; this works for entries without a JSON copy. That keeps the identity checks
while one runner, on the public API, serves both `dune runtest` and the
upgrade command. The private replay path stays covered by
`bench_cold_replay` and the
[initial-signals regression](../../test/integration/temporal/initial_signals/README.md).

## SDK and Core upgrade gate

Issue [#524](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/524)
makes the corpus the replay-compatibility gate for upgrades. Histories are
never regenerated for a candidate: each is replayed as recorded by its
producing SDK commit and Core revision (the manifest's capture provenance).

```sh
make test-history-corpus-upgrade
```

builds the runner and replays every entry, printing a per-case table (case ID,
the Core revision that produced the history, expected and actual outcome,
pass/FAIL) and writing `_build/history-corpus/report.json`
(`HISTORY_CORPUS_REPORT` overrides the path). It exits non-zero on any mismatch
and prints `FAIL history corpus: N mismatched case(s): <IDs>`. The candidate SDK
commit is the host checkout's `HEAD` unless `HISTORY_CORPUS_SDK_COMMIT` is
set; the candidate Core revision is the `temporalio-sdk-core` Git source in
`rust/Cargo.lock`. The runner can also be invoked directly:

```sh
history_corpus_runner.exe MANIFEST [--report FILE] [--cargo-lock FILE] [--sdk-commit SHA]
```

The report (`schema` `ocaml-temporal/history-corpus-report/v1`) contains:

| Member | Meaning |
| --- | --- |
| `status` | `pass`, or `fail` on any mismatched case or manifest/coverage problem. |
| `scope` | The forward-compatibility-only limitation below, carried with the evidence. |
| `candidate` | `sdk_commit`, `core_revision`, `core_source`, `ocaml_version`, `native_bridge_abi` of the build under test. |
| `summary` | Counts of `cases`, `passed`, `failed` and `problems`. |
| `failing_cases` | IDs of mismatched cases in manifest order. |
| `problems` | Manifest validation or coverage failures not tied to one replay; when the manifest is invalid no case is replayed. |
| `feature_coverage` | One object per [feature coverage](#feature-coverage) row: `feature`, `status`, the satisfying `cases` and, for a row without cases, its `reason`. |
| `cases[]` | Per entry: `id`, `workflow_type`, `workflow_id`, `run_id`, `history`, `replay_definitions`, `expected`, `actual`, `result`, `message` (the public failure text or mismatch reason) and `produced_by` (`capture`, `kind`, `sdk_commit`, `core_revision`). |

A pass is **forward-compatibility** evidence only: the candidate replays
histories recorded by older SDK/Core builds. It does not show that the previous
release can replay histories written by the candidate, so it is not evidence
that a deployment can be rolled back after the candidate has processed
workflow tasks. The runner prints this limitation and the report records it as
`scope`; rollback qualification belongs to
[#508](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/508).

`actual` is `replays_ok`, `nondeterminism`, `workflow_task_failed`,
`invalid_history`, `unsupported_history`, `replay_error` (the replay could not
run) or `not_run` (unknown definition set, unregistered type or unreadable
history).

Where it runs:

- **Every PR, merge group and master build.** `make verify` runs the runner as
  part of `dune runtest` on every Linux matrix leg, and `make native-verify` on
  Windows and macOS. The Linux amd64 / OCaml 5.5.1 leg of
  [`build-pr.yml`](../../.github/workflows/build-pr.yml) also runs
  `make test-history-corpus-upgrade` (reusing that job's build) and uploads the
  report as the `history-corpus-report-linux-amd64-ocaml-5.5.1` artifact, also
  when verification failed. Because `rust/Cargo.lock` is a Dune dependency of
  the test, a lockfile change always reruns the corpus.
- **Temporal Core pin bumps and Dependabot Cargo PRs.** These must pass the
  runner without editing the manifest or histories; the PR cites the report
  artifact (candidate `core_revision` against each case's `produced_by`). See
  the [Core upgrade checklist](../dependencies.md#temporal-core-pin-upgrades).
- **Releases.** The release workflow reuses `build-pr.yml`, so the release
  candidate commit produces the same report.

`test_history_corpus_mismatch` keeps the negative path honest: it copies the
corpus to a temporary directory, points `compat-patch-active-on-deprecated` at
the pre-patch definitions (an expected pass that now reports nondeterminism)
and `negative-timer-removed` at the compatible definitions (a negative control
that now replays), runs the real runner, and requires exit status 1, both IDs
on standard error and in the report's `failing_cases`, and every other case
passing. A second scenario replaces `live-2026-10-08-activity`'s protobuf
with the timer history, updates its checksum and drops its JSON copy; the
runner must reject the manifest with exactly that entry's workflow type and
run ID mismatches and replay nothing.

## Manifest format

The manifest has three members. `schema` is the format identifier
`ocaml-temporal/history-corpus/v1`. `captures` maps a capture ID to the
provenance shared by histories produced together. `entries` lists replay
cases.

A capture records `kind` (`live` or `synthetic`), `captured_on`, the producing
`sdk_commit` and whether tracked files differed (`sdk_tree_dirty`), the pinned
`core_revision`, the digest-pinned `temporal_server_image` (`none` for a
synthetic history), the `capture_command`, worker toolchains, the protobuf
export tool and descriptor hash, and, for reused fixtures, the original
`source`.

An entry records a unique `id`, its `purpose`, `features` tags, the recorded
`workflow_type`, `workflow_id` and `run_id`, its `capture`, the producing
`generation`, the `history` paths and SHA-256 values (`protobuf` is the replay
input; `json` is the Temporal CLI export it was encoded from), the
`replay_definitions` set, and the `expected` verdict (`replays_ok`,
`nondeterminism` or `invalid_history`). A negative control (`nondeterminism`
or `invalid_history`) must set `negative_control_of` to the `replays_ok` entry
whose history it reuses, so the failure is attributable to the definitions,
or to the control's documented edit or workflow ID, rather than to a
different history. It may also set `expected_message`, a fragment of the
failure reason that the replay failure text must contain; the lifecycle
negative controls use it so that, for example, a removed patch is reported
as an unconsumed marker rather than as an unrelated command mismatch. An
`invalid_history` entry may set `history_mutation`, the deterministic edit
described in [malformed-history controls](#malformed-history-controls).

Several entries may share one history file; they must agree on its checksum.

## Initial corpus

| Capture | Entries | Provenance |
| --- | --- | --- |
| `live-2026-10-08` | `activity`, `timer`, `activity-retry`, `interaction` (signal, update, condition), `parent`, `child`, `continue-as-new-first`, `continue-as-new-second`, `patch-marker-free`, `patch-active`, `patch-deprecated` | Captured for this corpus with `capture-history-corpus.sh` against Temporal Server 1.32.0 (Compose stack, digest-pinned), Core `95e97686`, OCaml 5.4.1. The worker ran natively on macOS against the Compose server; the Make target runs the same program in the development container. |
| `task-failure-2026-09-20` | `task-failure-body`, `-encoder`, `-missing`, `-business-retryable`, `-business-permanent` | Copied byte for byte from the [#511 workflow-task failure gate](workflow-failures.md) (`make test-temporal-task-failure-live`), whose own manifest retains run IDs and binary hashes. |
| `initial-signals-synthetic` | `initial-signals` | The synthetic history from the [#694 regression](../../test/integration/temporal/initial_signals/README.md); not from a server. |
| `live-2026-10-24` | `timer-cancel-before-fire`, `timer-cancel-after-fire`, `condition-timeout`, `condition-satisfied`, `options-parent`, `options-child` | Captured for [#1134](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/1134) and [#1135](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/1135) with only the `corpus-v2-cancellation` generation, against Temporal Server 1.32.0 (Compose stack, digest-pinned), Core `8fe478ee`, OCaml 5.4.1, from a clean tree at the recorded `sdk_commit`. The worker ran natively on macOS. Scoped timers cancelled before and after firing, a bounded condition that times out and one satisfied by a signal, and a child started with execution/run/task timeouts and an ID reuse policy. |
| `live-2026-10-11` | `lifecycle-{multi,loop,handlers}-{legacy,patched,deprecated,removed}`, `lifecycle-handover` | Captured for [#531](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/531) with only the four `corpus-lifecycle-*` generations, against Temporal Server 1.32.0 (Compose stack, digest-pinned), Core `95e97686`, OCaml 5.4.1, from a clean tree at the recorded `sdk_commit`. The worker ran natively on macOS. The handover run was started by the legacy process and finished by a fresh patched process. |

Derived cases reuse those histories against other definition sets:
`compat-patch-marker-free-on-patched` (a pre-patch history replays on the
patched generation, where `Workflow.patched` reports `false`),
`compat-patch-active-on-deprecated` (an active marker replays after
`Workflow.deprecate_patch`), and two negative controls:
`negative-timer-removed` (the timer workflow without its timer) and
`negative-patch-active-on-legacy` (a patched history on the pre-patch
generation). Core reports both as `[TMPRL1100]` nondeterminism. A third,
`negative-timer-cancel-dropped`, replays the `live-2026-10-24` cancel-before-fire
history against a definition whose long timer is unscoped and never cancelled:
history records `TimerCanceled` with no matching command, so Core must report
nondeterminism, which shows the corpus checks the cancellation itself.

The `live-2026-10-11` histories are each replayed against every other
lifecycle generation: `compat-lifecycle-<workflow>-<history stage>-on-<code
stage>` entries must replay and `negative-lifecycle-...` entries must report
nondeterminism with their declared reason. The complete matrix and what it
establishes about patch upgrade, removal and rollback are in the
[patching reference](workflow-patching.md#lifecycle-replay-matrix).

The patch-replay, restart-replay and parent/child fixtures under
`test/integration/temporal/fixtures/` are normalized, payload-free
projections with placeholder run IDs. They are acceptance evidence, not
replayable histories, so the corpus records new patch and child histories
instead of reusing them.

Every history contains only synthetic payloads. The capture program sets fixed
client and worker identities, and the export script rejects a history whose
`identity` fields are not those synthetic values, so host names do not enter
the corpus. Histories are small (under 10 KiB each) and checked out without
line-ending conversion (see `.gitattributes`).

## Frozen definitions

[`corpus_definitions.ml`](../../test/history_corpus/corpus_definitions.ml)
defines named definition sets: `corpus-v1` (all current workflows with the
patched `corpus.patch`), `corpus-v1-patch-legacy`,
`corpus-v1-patch-deprecated`, `negative-timer-removed`,
`task-failure-corrected`, `initial-signals` and the four patch lifecycle
generations `corpus-lifecycle-legacy`, `-patched`, `-deprecated` and
`-removed`. The lifecycle generations share one body per workflow type and
differ only in a single gate function, so the stage is the only variable in
their replay matrix. The same values are registered
by the live capture worker and by the replay test, so capture and replay use
identical application code. They use only the public `Temporal` API.

Once an entry names a workflow type, its definition must not change: the corpus
proves that a candidate SDK replays histories recorded by this exact
application code. Model a behavior change as a new workflow type, a new patch,
or a new definition set, and add new entries for it.

## Capturing and adding histories

Capture from a disposable Compose stack:

```sh
make history-corpus-capture HISTORY_CORPUS_CAPTURE_ID=live-YYYY-MM-DD
```

The target starts the stack (`make temporal-start`), builds
`history_corpus_capture.exe` in the development container, runs the
`corpus-v1-patch-legacy`, `corpus-v1-patch-deprecated`, `corpus-v1` and
`corpus-lifecycle-legacy`, `-patched`, `-deprecated`, `-removed` worker
generations in that order (each process hosts one worker generation and a
client with fixed workflow IDs), exports every recorded run with
`temporal workflow show --output json` from the pinned admin-tools image, and
encodes it as protobuf with Python `protobuf` `json_format` and a descriptor
set compiled by `protoc` from the pinned Core protos. The export step runs on
the host and needs `python3` with the `protobuf` package, `protoc`, and `cargo`
(or `HISTORY_CORPUS_CORE_PROTOS`). Output is staged in
`.history-corpus-capture/` with a `manifest.fragment.json` for review.
`HISTORY_CORPUS_GENERATIONS` limits a capture to the named generations (in
order), so new definition sets can be captured without re-recording existing
cases; `corpus-lifecycle-legacy` must precede `corpus-lifecycle-patched`,
because it leaves the parked handover run that the patched generation
finishes.

To append the staged histories, add `HISTORY_CORPUS_INSTALL=test/fixtures/history-corpus`.
Installation is append-only: it refuses an existing capture ID, entry ID or
history file. Compatibility cases and negative controls that reuse a history
are added to the manifest by hand. `test/history_corpus/scripts/capture-history-corpus.sh`
documents the variables for running the same capture against another
disposable server.

## Addition, review and retention rules

- Never regenerate, rewrite or delete a committed history to make the gate
  pass. A failing entry is an SDK, Core or server compatibility change; fix it,
  or record an approved, intentional break in the PR and in
  [progress](../progress.md) before changing the entry's expected verdict.
- Add new behavior as new entries under a new capture ID. Keep payloads
  synthetic and identities fixed; do not add histories from real deployments.
- Keep each history small. Replay has a 30-second no-progress bound per
  entry, and the whole corpus replays in a few seconds.
- A new supported feature, a new feature tag, or a removed or demoted
  coverage row must change the runner's table
  ([`corpus_coverage.ml`](../../test/history_corpus/corpus_coverage.ml)) and
  the [feature coverage](#feature-coverage) table below together; the Dune
  test fails while they differ. A captured gap must be promoted to
  `replayed`.

## Malformed-history controls

Since [#1067](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/1067),
`Temporal.Replay` refuses histories it cannot truly check as
`Invalid_history` instead of replaying them or stalling. Five
`invalid_history` entries keep that refusal under the same upgrade gate as the
replay verdicts. Each reuses a committed `replays_ok` history (its
`negative_control_of`), so its checksum and recorded identity are checked as
usual, and the runner applies the entry's `history_mutation` to those bytes in
memory just before replay
([`corpus_history_mutation.ml`](../../test/history_corpus/corpus_history_mutation.ml)).
No derived history file is committed: the source entry and the named edit are
the complete, deterministic provenance of the replayed bytes, and an edit
whose target is absent fails the case instead of replaying the input
unchanged. The edits match the bridge's own
[public replay tests](../../test/bridge/test_public_replay.ml), which pin the
same rejections against one task-failure fixture.

| Entry | Source history | Edit | Required reason (`expected_message`) |
| --- | --- | --- | --- |
| `invalid-truncated-protobuf-activity-retry` | `live-2026-10-08-activity-retry` | `truncate-protobuf`: keep the first half of the bytes | `history failed strict validation` |
| `invalid-event-id-gap-timer` | `live-2026-10-08-timer` | `event-id-gap`: event 3 missing, later IDs one higher | `event IDs are not contiguous from 1` |
| `invalid-missing-task-queue-interaction` | `live-2026-10-08-interaction` | `drop-start-task-queue`: start event without `task_queue` | `start event has no task queue` |
| `invalid-first-task-rejected-continue-as-new` | `live-2026-10-08-continue-as-new-first` | `drop-first-workflow-task-scheduled-attributes`: Core cannot apply event 2 in the first workflow task and auto-fails it | `This history is invalid` (Core's first-task failure) |
| `invalid-workflow-id-mismatch-patch-active` | `live-2026-10-08-patch-active` | none; replayed under `history-corpus-patch-marker-free`, another corpus execution's ID | `records a different workflow ID than the one supplied` |

`test_history_corpus_mismatch` removes the gap control's edit and requires the
runner to fail exactly that case with actual outcome `replays_ok`, so a
control whose edit stops applying cannot pass.

## Feature coverage

Every feature the SDK supports, or exports for evaluation, that could affect
what a history records or how it replays has one row, drawn from the
[v1 support policy](v1-support-policy.md) and the
[feature inventory](feature-coverage.md). The runner holds the same table
([`corpus_coverage.ml`](../../test/history_corpus/corpus_coverage.ml)), checks
it against the manifest's feature tags on every run, compares it with this
table in the Dune test (`--coverage-doc`), and writes it, with the satisfying
case IDs, as the report's `feature_coverage` member. Statuses:

- `replayed`: at least one `replays_ok` entry carries the tag;
- `nondeterminism control` / `invalid-history control`: at least one entry
  with that verdict carries the tag;
- `not applicable`: replay cannot exercise the feature, and no entry may
  carry the tag;
- `not yet captured`: a known gap tracked by
  [#503](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/503).
  It needs a live capture against Temporal Server, and no `replays_ok` entry
  may carry the tag until the row is promoted.

`test_history_corpus_mismatch` tags an entry with an unclassified feature and
with a `not yet captured` feature and requires exactly those two problems.

| Feature | Status | Cases or reason |
| --- | --- | --- |
| `timer` | replayed | `live-2026-10-08-timer` and others |
| `condition` | replayed | `live-2026-10-08-interaction` |
| `child-workflow` | replayed | `live-2026-10-08-parent`, `live-2026-10-08-child` |
| `child-side` | replayed | `live-2026-10-08-child` |
| `continue-as-new` | replayed | `live-2026-10-08-continue-as-new-first`, `-second` |
| `continue-as-new-successor` | replayed | `live-2026-10-08-continue-as-new-second` |
| `timer-cancellation` | replayed | `live-2026-10-24-timer-cancel-before-fire`, `-timer-cancel-after-fire`, `-condition-satisfied` |
| `condition-timeout` | replayed | `live-2026-10-24-condition-timeout`, `-condition-satisfied` |
| `child-start-options` | replayed | `live-2026-10-24-options-parent`, `-options-child` |
| `activity` | replayed | `live-2026-10-08-activity` and others |
| `activity-retry` | replayed | `live-2026-10-08-activity-retry` |
| `signal` | replayed | `live-2026-10-08-interaction`, lifecycle handler cases |
| `signal-before-first-task` | replayed | `initial-signals` |
| `update` | replayed | `live-2026-10-08-interaction`, lifecycle handler cases |
| `workflow-failure` | replayed | `task-failure-business-*` |
| `non-retryable-failure` | replayed | `task-failure-business-permanent` |
| `workflow-task-failure-recovery` | replayed | `task-failure-body`, `-encoder`, `-missing` |
| `missing-registration` | replayed | `task-failure-missing` |
| `patch` | replayed | patch and lifecycle cases |
| `patch-marker-free` | replayed | `live-2026-10-08-patch-marker-free` and lifecycle cases |
| `patch-active` | replayed | `live-2026-10-08-patch-active` and lifecycle cases |
| `patch-deprecated` | replayed | `live-2026-10-08-patch-deprecated` and lifecycle cases |
| `patch-removed` | replayed | lifecycle cases |
| `patch-multiple` | replayed | `lifecycle-multi-*` |
| `patch-loop` | replayed | `lifecycle-loop-*` |
| `patch-signal-handler` | replayed | `lifecycle-handlers-*` |
| `patch-update-handler` | replayed | `lifecycle-handlers-*` |
| `patch-mid-run-upgrade` | replayed | `lifecycle-handover` |
| `patch-upgrade` | replayed | `compat-*` cases |
| `patch-rollback` | replayed | `compat-lifecycle-*` rollback cases |
| `negative-control` | nondeterminism control | `negative-*` cases |
| `malformed-history` | invalid-history control | the five `invalid-*` cases |
| `invalid-history-truncated-protobuf` | invalid-history control | `invalid-truncated-protobuf-activity-retry` |
| `invalid-history-event-id-gap` | invalid-history control | `invalid-event-id-gap-timer` |
| `invalid-history-missing-task-queue` | invalid-history control | `invalid-missing-task-queue-interaction` |
| `invalid-history-first-task-rejected` | invalid-history control | `invalid-first-task-rejected-continue-as-new` |
| `invalid-history-workflow-id-mismatch` | invalid-history control | `invalid-workflow-id-mismatch-patch-active` |
| `query` | not applicable | Queries record no history events, so replay never runs a query handler; the live completed-query regression covers them. |
| `asynchronous-activity-completion` | not applicable | Task-token completion records the same `ActivityTaskCompleted` event as a synchronous one, which the activity cases replay. |
| `payload-codecs` | not applicable | Replay compares command shapes, not payload values; codec compatibility is checked by the codec and interoperability tests. |
| `client-operations` | not applicable | Client calls, start options and RPC deadlines run outside workflow code; what they record is listed under the workflow-side features. |
| `worker-configuration` | not applicable | Worker resource, polling and shutdown options, metrics and logging record nothing in a history. |
| `in-process-testing` | not applicable | `Temporal.Testing` runs against a simulator and produces no server history. |
| `workflow-cancellation` | not yet captured | A cancellation request delivered to a running workflow, and a run completed as cancelled. |
| `activity-cancellation` | not yet captured | Activity cancellation by handle or scope. |
| `child-workflow-cancellation` | not yet captured | Child workflow cancellation by handle or scope. |
| `activity-timeout` | not yet captured | An activity attempt ending in a start-to-close, schedule-to-close or heartbeat timeout. |
| `workflow-timeout` | not yet captured | A run ended by its execution or run timeout. |
| `workflow-retry` | not yet captured | A workflow retry run whose retry policy, attempt and predecessor are read through `Workflow.Info`. |
| `local-activity` | not yet captured | A local activity and its marker, including a Core-directed backoff timer. |
| `external-signal` | not yet captured | Signalling another workflow from workflow code. |
| `external-cancellation` | not yet captured | Requesting cancellation of another workflow from workflow code. |
| `start-metadata` | not yet captured | A start memo and search attributes read through `Workflow.start_metadata`. |
| `search-attribute-upsert` | not yet captured | `Workflow.upsert_search_attributes`. |
| `deterministic-time-random` | not yet captured | Commands that depend on `Workflow.now` or `Workflow.random_int`. |
| `deployment-version` | not yet captured | A run on a deployment-versioned worker whose commands branch on `Workflow.current_deployment_version`. |

## Scope

#518 seeded the corpus and #524 runs it through the public replay API as the
SDK/Core upgrade gate. #503 added the malformed-history controls and the
feature coverage table; the `not yet captured` rows are its remaining gaps,
each needing a live capture against a Temporal Server. The lifecycle matrix covers
replay compatibility only; concurrent mixed-version workers and version
routing are outside the corpus. Every current capture was produced at Core
`95e97686`. The first Core pin bump, to `8fe478ee` (v1.1.0, #824), was the
first true cross-revision replay: every case kept its expected outcome, and
its report is the evidence retained on that pull request. Cancellation,
timeouts, workflow retries, local activities, external signals and
cancellation, start metadata and search-attribute histories are not yet
represented (see [feature coverage](#feature-coverage)).

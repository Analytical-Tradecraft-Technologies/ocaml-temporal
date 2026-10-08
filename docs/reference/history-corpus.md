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
- an entry's JSON history does not start with a `WorkflowExecutionStarted`
  event whose workflow type and `originalExecutionRunId` match the entry's
  `workflow_type` and `run_id`;
- a required feature (activity, timer, activity retry, signal, update,
  child workflow, continue-as-new, marker-free/active/deprecated patch,
  workflow failure, workflow-task failure recovery) has no `replays_ok` entry,
  or there is no nondeterminism negative control (queries are not a required
  feature: they record no history events, so replay never exercises a query
  handler, and query behaviour is covered by the live completed-query
  regression instead);
- a `replays_ok` entry does not return `Ok ()` from `Temporal.Replay.replay`
  with only the entry's workflow type registered from its definition set
  (so a history recorded for another type cannot pass); or
- a `nondeterminism` entry does not return `Nondeterminism` for the entry's
  run ID.

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
checks them offline against each entry's checksummed JSON history (see above)
and registers only the entry's workflow type. That keeps the identity checks
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
| `candidate` | `sdk_commit`, `core_revision`, `core_source`, `ocaml_version`, `native_bridge_abi` of the build under test. |
| `summary` | Counts of `cases`, `passed`, `failed` and `problems`. |
| `failing_cases` | IDs of mismatched cases in manifest order. |
| `problems` | Manifest validation or coverage failures not tied to one replay; when the manifest is invalid no case is replayed. |
| `cases[]` | Per entry: `id`, `workflow_type`, `workflow_id`, `run_id`, `history`, `replay_definitions`, `expected`, `actual`, `result`, `message` (the public failure text or mismatch reason) and `produced_by` (`capture`, `kind`, `sdk_commit`, `core_revision`). |

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
passing.

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
`replay_definitions` set, and the `expected` verdict (`replays_ok` or
`nondeterminism`). A `nondeterminism` entry must set `negative_control_of` to
the `replays_ok` entry whose history it reuses, so the failure is attributable
to the definitions rather than to a different history.

Several entries may share one history file; they must agree on its checksum.

## Initial corpus

| Capture | Entries | Provenance |
| --- | --- | --- |
| `live-2026-10-08` | `activity`, `timer`, `activity-retry`, `interaction` (signal, update, condition), `parent`, `child`, `continue-as-new-first`, `continue-as-new-second`, `patch-marker-free`, `patch-active`, `patch-deprecated` | Captured for this corpus with `capture-history-corpus.sh` against Temporal Server 1.32.0 (Compose stack, digest-pinned), Core `95e97686`, OCaml 5.4.1. The worker ran natively on macOS against the Compose server; the Make target runs the same program in the development container. |
| `task-failure-2026-09-20` | `task-failure-body`, `-encoder`, `-missing`, `-business-retryable`, `-business-permanent` | Copied byte for byte from the [#511 workflow-task failure gate](workflow-failures.md) (`make test-temporal-task-failure-live`), whose own manifest retains run IDs and binary hashes. |
| `initial-signals-synthetic` | `initial-signals` | The synthetic history from the [#694 regression](../../test/integration/temporal/initial_signals/README.md); not from a server. |

Derived cases reuse those histories against other definition sets:
`compat-patch-marker-free-on-patched` (a pre-patch history replays on the
patched generation, where `Workflow.patched` reports `false`),
`compat-patch-active-on-deprecated` (an active marker replays after
`Workflow.deprecate_patch`), and two negative controls:
`negative-timer-removed` (the timer workflow without its timer) and
`negative-patch-active-on-legacy` (a patched history on the pre-patch
generation). Core reports both as `[TMPRL1100]` nondeterminism.

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
`task-failure-corrected` and `initial-signals`. The same values are registered
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
`corpus-v1-patch-legacy`, `corpus-v1-patch-deprecated` and `corpus-v1` worker
generations in that order (each process hosts one worker generation and a
client with fixed workflow IDs), exports every recorded run with
`temporal workflow show --output json` from the pinned admin-tools image, and
encodes it as protobuf with Python `protobuf` `json_format` and a descriptor
set compiled by `protoc` from the pinned Core protos. The export step runs on
the host and needs `python3` with the `protobuf` package, `protoc`, and `cargo`
(or `HISTORY_CORPUS_CORE_PROTOS`). Output is staged in
`.history-corpus-capture/` with a `manifest.fragment.json` for review.

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
- A feature removed from `required_features` in the runner, or a new required
  feature, must be reflected in the coverage list above.

## Scope

#518 seeded the corpus and #524 runs it through the public replay API as the
SDK/Core upgrade gate. The corpus does not yet add malformed-history negative
controls beyond the bridge's and `Temporal.Replay`'s own validation tests, or
qualify every supported feature for #503. Every current capture was produced
at Core `95e97686`; the first Core pin bump is the first true cross-revision
replay, and its report is the evidence to retain. Cancellation,
timeouts, local activities, external signals and search-attribute histories
are not yet represented.

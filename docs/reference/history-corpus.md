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
| Docker-free gate | [`test/history_corpus/test_history_corpus.ml`](../../test/history_corpus/test_history_corpus.ml) |
| Capture program and scripts | [`test/history_corpus/capture/`](../../test/history_corpus/capture/), [`test/history_corpus/scripts/`](../../test/history_corpus/scripts/) |

## What is checked, and where

`test_history_corpus` is an ordinary Dune test, so `dune runtest`, `make test`
and the native Windows/macOS `make native-test` jobs all run it; no Temporal
Server or Docker is involved. `make test-history-corpus` runs only this test.
It fails, naming the entry ID, when:

- the manifest violates the v1 schema (every object is closed), a capture or
  entry reference is dangling, a capture is unused, or a negative control does
  not name a `replays_ok` entry with the same history;
- a history file's SHA-256 differs from the manifest, a referenced file is
  missing, or a file under `histories/` is not referenced (orphan);
- a required feature (activity, timer, activity retry, signal, update, query,
  child workflow, continue-as-new, marker-free/active/deprecated patch,
  workflow failure, workflow-task failure recovery) has no `replays_ok` entry,
  or there is no nondeterminism negative control;
- a `replays_ok` entry does not replay cleanly: Core must accept every
  completion, report no task failure or failure eviction, see exactly one
  `InitializeWorkflow` with the manifest's run ID and workflow type, accept
  exactly one terminal command, and let the replay worker finalize naturally
  within 30 seconds; or
- a `nondeterminism` entry does not produce a Core nondeterminism eviction.

The SHA-256 implementation is a small test helper checked against the FIPS
180-4 vectors before any manifest is read, because the OCaml standard library
has no SHA-256 and a hashing dependency would only serve this test.

## Replay path

Replay uses the existing private path, the same one exercised by
`bench_cold_replay` and the
[initial-signals regression](../../test/integration/temporal/initial_signals/README.md):
a fresh `Sdk_supervisor.Native` instance starts a workflow-only Core replay
worker, feeds one protobuf history through the
[replay bridge](replay-bridge.md), and the production
`Native_worker_execution` adapter runs the registered OCaml definitions. Each
entry gets its own supervisor, and every native resource is released before the
next entry runs. The public `Temporal.Workflow` definitions are converted to
private registrations by
[`corpus_replay.ml`](../../test/history_corpus/corpus_replay.ml), which mirrors
the package-private conversions used by `Temporal.Worker`.

The corpus does not depend on that runner. The histories are binary
`temporal.api.history.v1.History` protobufs plus a workflow ID, which is also
the input of the proposed public `Temporal.Replay` API (issue
[#515](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/515)).
Issue [#524](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/524)
can run the same manifest through either path, or across SDK/Core upgrades.

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
| `live-2026-10-08` | `activity`, `timer`, `activity-retry`, `interaction` (signal, update, query, condition), `parent`, `child`, `continue-as-new-first`, `continue-as-new-second`, `patch-marker-free`, `patch-active`, `patch-deprecated` | Captured for this corpus with `capture-history-corpus.sh` against Temporal Server 1.32.0 (Compose stack, digest-pinned), Core `95e97686`, OCaml 5.4.1. The worker ran natively on macOS against the Compose server; the Make target runs the same program in the development container. |
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
- Keep each history small. The test has a 30-second bound per entry, and the
  whole corpus replays in a few seconds.
- A feature removed from `required_features` in the test, or a new required
  feature, must be reflected in the coverage list above.

## Scope

This seeds the corpus required by #518. It does not run the corpus across older
SDK releases or Core upgrades (#524), provide a public replay runner (#515),
add malformed-history negative controls beyond the bridge's existing
validation tests, or qualify every supported feature for #503. Cancellation,
timeouts, local activities, external signals and search-attribute histories
are not yet represented.

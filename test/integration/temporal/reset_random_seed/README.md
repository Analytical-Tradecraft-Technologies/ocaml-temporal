# Timer reset and deterministic randomness

Regression for [#570](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/570).
`reset_definition.ml` draws a random integer, waits on a one-second timer,
draws again, and starts a second timer whose duration depends on that draw.
Its result contains both integers. Resetting after the first timer must keep
the first draw and replace the stream used for the second draw.

## Recorded evidence

Captured on 2026-09-21 with native OCaml 5.4.1, Temporal CLI 1.9.1 and its
development server 1.32.0, and the repository's pinned Core revision
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`.

| Observation | Value |
| --- | --- |
| Original run | `01a0bff9-f290-7781-a0de-dbd4fc5d16a3` |
| Original result | `727942:941156` |
| Reset point | Workflow task completed, event 9, after timer 1 fired |
| Reset run | `5ac6e66a-8a2f-4424-bf89-5b4ff2821a4e` |
| Reset result | `727942:493861` |
| Core replacement seed | `18178923120523618679` |

`reset-history.json` is the completed reset history exported by the CLI.
Nonsemantic identity and reset-description text is normalized to fixture
labels. Run IDs, timestamps, payloads, timer durations, and event order are
unchanged. Event 9 is now `WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW`, carrying
the new run ID from which Core derives the replacement seed.
`reset-history.replay.json` contains the same history encoded as the pinned
Temporal API `History` protobuf in the private replay envelope. The conversion
used `protoc --include_imports --descriptor_set_out` on the pinned
`temporal/api/history/v1/message.proto`, followed by Python protobuf's
`json_format.ParseDict` and `SerializeToString`; it added no SDK dependency.

`test_reset_replay.ml` runs without a server. It feeds the recorded history
through the native supervisor and Core replay worker, executes the same
workflow through the production OCaml worker adapter, requires exactly one
reset seed notification, checks the exact result, and requires natural replay
finalization. Machine failures, rejected tasks, incomplete replay, or cleanup
alone cannot pass. The test runs in the ordinary `make native-test` / Dune
suite, including CI. The focused runtime test separately covers ordered seed
replacement, zero, the unsigned upper half, maximum uint64, and rejection
before a pending timer is consumed.

## Repeat the live capture

Build the fixture with `make native-lint` in the native development setup.
Start a disposable local Temporal development server on port 17233, then run:

```sh
TEMPORAL_RUN_LIVE=1 TEMPORAL_ADDRESS=http://127.0.0.1:17233 \
  _build/default/test/integration/temporal/reset_random_seed/reset_worker.exe
```

In another terminal, use the official Temporal CLI:

```sh
temporal workflow execute --address 127.0.0.1:17233 \
  --workflow-id reset-random-seed-fixture --type reset-random-seed \
  --task-queue reset-random-seed --input '"fixture"' --output json
temporal workflow reset --address 127.0.0.1:17233 \
  --workflow-id reset-random-seed-fixture --event-id 9 \
  --reason 'Reset random seed regression' --output json
temporal workflow show --address 127.0.0.1:17233 \
  --workflow-id reset-random-seed-fixture --output json
```

Wait for the reset run to complete before recording its history. Verify that
the first result integer is unchanged, the second belongs to the replacement
stream, and there is no worker-failure task after the deliberate reset event.
Stop the fixture worker with SIGTERM, then stop the disposable server.

This is a bounded native development-server regression. It does not qualify
all reset options, signals/updates, PostgreSQL deployment behavior, or recovery
under faults. Reset remains experimental and outside production recovery
guidance until those broader gates pass.

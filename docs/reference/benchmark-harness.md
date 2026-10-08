# Reproducible benchmark harness

These exploratory, no-server suites share one versioned JSON report shape. The
original minimal suite creates a fresh in-memory workflow execution, applies
one synthetic `Start_workflow` activation that completes immediately, checks
the terminal command, and shuts down. It does **not** measure Temporal Core,
the C/Rust bridge, worker polling, a Temporal Server, or network latency. The
activation suites for [#526] add distinct warm-cache and cold-replay
boundaries. The [memory and allocation suites](#memory-and-allocation-suites)
for [#527] and the [fan-out suite](#fan-out-and-json-bridge-overhead) for
[#528] extend the same report with memory sections. None of these suites alone
qualifies the sustained-load gate in [#507].

From a checkout with Docker Compose and Python 3 available, run the original
minimal suite:

```sh
make bench BENCH_WARMUP=100 BENCH_SAMPLES=1000 BENCH_REPETITIONS=3 \
  BENCH_SEED=1 BENCH_HOST_LABEL='machine/CPU/RAM description'
```

Run the two activation scenarios independently or in sequence:

```sh
make bench-activation-warm BENCH_WARMUP=100 BENCH_SAMPLES=1000 \
  BENCH_REPETITIONS=3 BENCH_HOST_LABEL='machine/CPU/RAM description'
make bench-activation-cold BENCH_COLD_WARMUP=1 BENCH_COLD_SAMPLES=10 \
  BENCH_COLD_REPETITIONS=3 BENCH_HOST_LABEL='machine/CPU/RAM description'
# Runs the two commands above with their defaults:
make bench-activation
```

Each command builds and runs the OCaml executable with Dune's `release`
profile in the development container. No Temporal Server or credentials are
needed. Reports default to `_build/benchmarks/local-minimal-activation.json`,
`_build/benchmarks/ocaml-warm-cache-activation.json`, and
`_build/benchmarks/core-cold-replay.json`. Set `BENCH_REPORT` for `make bench`,
`BENCH_WARM_REPORT` for `make bench-activation-warm`, or `BENCH_COLD_REPORT`
for `make bench-activation-cold` to retain a report outside the ignored build
tree. Cold replay reads the checked-in
`test/integration/temporal/initial_signals/history.replay.json`; use
`BENCH_REPLAY_HISTORY` to supply another compatible retained history and
inspect the reported document identity.

The **warm-cache** workload starts a timer-loop execution in the first warmup
sample of each repetition. Each measured sample applies one synthetic
`Fire_timer` activation to that same retained execution, then checks the next
`Start_timer` sequence. Measured latency includes OCaml activation processing
and command validation, but excludes execution construction, shutdown, Core,
the native bridge, polling, server, and network time. The one-time construction
is included in the first warmup attempt only.

The **cold-replay** workload uses a fresh pinned-Core replay worker for every
attempt. Its default history contains two initial signals and a terminal
result. Each attempt starts a native graph, feeds the history, processes and
acknowledges replay activations through the normal OCaml worker adapter,
requires natural replay finalization, verifies the result, and shuts down.
History file reading and worker configuration construction occur before timed
phases. Measured latency includes Core, FFI, supervisor, OCaml processing,
finalization, and cleanup. It excludes Temporal client, server, and network
time. One cold sample is **one complete history replay**, whereas one warm
sample is **one timer-firing activation**; their throughput rates are different
units and should not be compared as if they were interchangeable.

The **payload-codec** suite for [#846] measures OCaml-side payload throughput
through the private workflow protocol:

```sh
make bench-payload-codec BENCH_PAYLOAD_WARMUP=5 BENCH_PAYLOAD_SAMPLES=50 \
  BENCH_HOST_LABEL='machine/CPU/RAM description'
```

One sample decodes one activation carrying a 2 MiB activity result
(about 2.8 MB of JSON), re-encodes the decoded value as the runtime's
activation validation does, then encodes and decodes one completion that
schedules an activity with a 2 MiB argument, and checks both payloads. The
document is built before the timed phases. Rust, Core, FFI, the supervisor,
and the network are excluded. The report defaults to
`_build/benchmarks/payload-codec.json`; set `BENCH_PAYLOAD_REPORT` to keep it
elsewhere.

All these baselines admit work synchronously; every suite except fan-out
does so at concurrency one. Pending-attempt
backlog at the harness admission boundary is zero by construction; internal
Core queues are not measured. `saturation_observation` is `not_exercised`.
These fields limit interpretation; they are not evidence of load capacity or
server throughput. Separate open-loop and sustained-load qualification remains
under [#507].

The command exits nonzero for build failures, invalid configuration, or any
failed sample. A valid JSON report is retained even when a sample fails, with
attempt counts, errors, and the first three error messages. Report
`schema_version: 1` contains source commit and dirty-tree flag, SDK version,
OCaml compiler version, Dune profile, pinned Core revision, server version
(`none`), base image reference, built development image ID, machine/container
identity, host label, warmup and measurement counts, repetitions, seed,
workload configuration, phase duration, per-attempt latency in microseconds,
p50/p95/p99 (nearest-rank), successful attempts per second, and error count.
The seed is recorded for the shared harness interface; these deterministic
workloads do not use randomness. Each repetition warms up separately and the
warm-cache execution is fresh for each repetition. Latency and throughput use
a monotonic elapsed clock. The per-attempt array supports later analysis; the
overall phase duration is authoritative for successful attempts per second.
Failed attempts and undrained backlog are never counted as successful
throughput. The minimal suite still times local execution creation,
activation, validation, and shutdown in each attempt.

The UTC timestamp is wall-clock metadata only. If Docker cannot resolve the
built image ID, the report explicitly records `unavailable`; the base image
reference alone may be a mutable tag and is not a substitute for a digest.
Compare reports only when source/configuration, compiler, machine, fixture,
and measurement boundary are compatible. No threshold is inferred from one
local run. [#507] still requires representative live workloads, agreed
budgets, sustained-load and fault-recovery evidence, and stable release-gate
metrics.

## Server-free worker adapter

The memory and fan-out suites drive the **production** OCaml worker adapter
(`Native_worker_execution.Make`) through `test/benchmark/benchmark_worker.ml`,
an in-memory source that replaces the native supervisor. Activation documents
are encoded as canonical bridge JSON before the baseline snapshot, so the
fixture is not counted as load. On poll, the source copies the document to a
string and strictly decodes it, as the supervisor does with bytes received
from Rust; on completion it copies the single encoded completion into a fresh
buffer, as the supervisor does before the native call. Everything in between
(translation, registry lookup, deterministic execution, futures, command
validation and the completion encoder pass) is unmodified production code.
Temporal Core, Rust/serde JSON work, the C stubs, the supervisor's owner
Domain and mailbox, polling, activity execution and the server are excluded.
Each report lists these in `memory_scope.unmeasured_native_components`.

## Memory and allocation suites

Instrumented reports (`Benchmark_harness.run_instrumented`) keep every field
above and add, per repetition:

- `allocation.{warmup,measurement}`: `Gc.quick_stat` deltas (allocated bytes
  in total and per attempt, minor/promoted/major words, minor and major
  collections, compactions) on the benchmark Domain.
- `memory`: snapshots `before_load` (after `Gc.compact`, before the workload
  is built), `after_warmup` (steady state), `after_measurement` (under load or
  churn), `after_close` (workload released) and `after_compact`. The last two are taken
  after the function that owns the workload has closed it and returned, so no
  harness closure still references the workload or its state. Each has the
  OCaml heap, top heap and **live** bytes from `Gc.stat`, which forces a full
  major collection, and process RSS. RSS comes from `/proc/self/status`
  (`VmRSS`, with peak `VmHWM`) on Linux, otherwise from `ps`, without a peak
  (`null`); `rss_source` names the counter.
- `memory.recovery`: live bytes above baseline under load, `retained_live_bytes`
  after close, heap above baseline after compaction, RSS released by
  compaction, and RSS still above baseline after compaction.
- `observations`: untimed, suite-specific measurements taken after the
  measurement snapshot.

`memory_trend` lists `retained_live_bytes` for each repetition. Each
repetition builds a fresh workload, so a strictly rising sequence is
reproducible evidence of retention; a flat sequence attributes the remainder
to fixed state. That fixed remainder includes the harness's own per-sample
latency records, which are still reachable for the report: about 72 bytes
per measured sample. Live bytes that return to baseline while RSS stays high
are allocator capacity rather than reachable data; RSS released between
`after_close` and `after_compact` was free OCaml heap.

```sh
# Replay at 1k, 10k and 50k equivalent history events (one report each).
make bench-history BENCH_HISTORY_SIZES='1000 10000 50000' \
  BENCH_HOST_LABEL='machine/CPU/RAM description'
# Workflow cache: every run resident, then eviction churn.
make bench-cache BENCH_CACHE_RUNS=1000 BENCH_CACHE_CHURN_CAPACITY=250
# Both of the above.
make bench-memory
```

**History replay** (`history-replay-memory`, `bench_history_replay.ml`). One
sample replays a whole synthetic history of sequential activities: a
replaying `Initialize_workflow`, one replaying `Resolve_activity` per step,
each checked against the next `Schedule_activity` and the final
`Complete_workflow`, then a `Workflow_execution_ending` eviction. A step stands
for six history events, plus five fixed start and end events, so
`BENCH_HISTORY_EVENTS` selects the equivalent history length (bounded to
200,000). `BENCH_HISTORY_PAYLOAD_BYTES` (default 128) sizes each argument and
result. Observations record the live bytes held by the run just before its
terminal step and after eviction, plus a JSON attribution pass (see below).
Reports go to `_build/benchmarks/history-replay-<events>.json`.

**Workflow cache** (`workflow-cache-memory`, `bench_workflow_cache.ml`).
`BENCH_CACHE_RUNS` runs are advanced round-robin, one timer activation per
sample. Each run generation fires `BENCH_CACHE_DEPTH` (default 8) timers,
completes, is evicted and restarts. At most `BENCH_CACHE_CAPACITY` runs are
resident. Touching a non-resident run first evicts the oldest resident run
(`Cache_full`), then reloads the touched run by replaying its initialization
and every timer it had fired, as Core does after a cache miss. Under
round-robin access, oldest-first eviction is LRU, so a capacity below the run
count makes every sample a miss (`churn`); a capacity equal to it never evicts
for space (`steady`). Observations count evictions, reloads and replayed
activations, and measure the cache's own footprint by evicting all resident
runs between two live-size readings. Reports go to
`_build/benchmarks/workflow-cache-{steady,churn}.json`.

## Fan-out and JSON bridge overhead

```sh
# Two admitted concurrency levels (one report each).
make bench-fanout BENCH_FANOUT_LEVELS='1 8' BENCH_FANOUT_WIDTH=1000 \
  BENCH_FANOUT_BATCH=10 BENCH_FANOUT_PAYLOAD_BYTES=256
```

**Activity fan-out** (`activity-fanout`, `bench_activity_fanout.ml`). One
sample runs `BENCH_FANOUT_CONCURRENCY` workflow runs to completion. Each run
schedules `BENCH_FANOUT_WIDTH` activities in its first activation and awaits
them all with `Future_store.all`. Results arrive `BENCH_FANOUT_BATCH` per
activation, interleaved round-robin across the concurrent runs; the last batch
completes the run and an eviction follows. Arguments and results are JSON
strings of `BENCH_FANOUT_PAYLOAD_BYTES` encoded bytes. Sample latency is the
end-to-end fan-out completion time for every concurrent run.
`throughput_successes_per_second` counts samples (groups of
`BENCH_FANOUT_CONCURRENCY` fan-outs), not fan-outs. Each phase therefore also
reports `fanouts_per_second` (samples per second times the concurrency) and
`activities_per_second` (times `activities_per_sample`).
`admitted_concurrency` is the number of concurrent runs, with
`admission_model: closed_loop_interleaved_runs`.

All result activations are encoded before the baseline snapshot and stay
resident, so the suite rejects a configuration before building them unless
`BENCH_FANOUT_WIDTH * BENCH_FANOUT_CONCURRENCY` is at most 200,000 and the
estimated fixture size is at most 512 MiB. The estimate charges each activity
result its base64-expanded payload (4 bytes per started 3 bytes) plus a
512-byte envelope allowance, above the measured envelope, and is checked by
division so it cannot overflow.

The untimed `json_bridge_attribution` pass, also used by the history suite,
reruns the workload with extra timers. It splits each adapter poll into
strict activation decoding (including the native-bytes copy), the completion
encoder pass, the completion byte copy, and the remainder: translation,
workflow code, futures, registry and command validation. Encoding happens
inside the adapter, so the source times a second encoder pass over the same
typed completion and subtracts it from the poll time; the duplicate is never
submitted. The attribution is limited to OCaml. The Rust half of the JSON
bridge (serde encoding of activations and decoding of completions), the C
copies, Core and scheduling between Domains are not measured, so the shares
below are a lower bound on total bridge cost.

## Indicative results and bottlenecks

These are single local runs, not baselines or thresholds. They were measured
on an Apple M4 Pro (14 cores, 48 GB) with macOS 27.0.1, running natively
outside the container, with OCaml 5.4.1 and the Dune `release` profile, on
this change's branch from `master` at `4cf43eab`. Medians of three
repetitions are shown. RSS came from `ps`, so no peak is shown.

| Workload | p50 per sample | Allocated per sample | JSON bridge share (decode / encode) |
| --- | --- | --- | --- |
| Replay, 995 events (167 activations) | 4.2 ms | 13.1 MB | 64% (29% / 35%) |
| Replay, 9,995 events (1,667 activations) | 41.8 ms | 133 MB | 64% (29% / 35%) |
| Replay, 49,997 events (8,334 activations) | 209 ms | 666 MB | 64% (29% / 35%) |
| Cache steady, 1,000 runs resident | 9 µs | 41 KB | not measured |
| Cache churn, 1,000 runs / 250 resident | 52 µs | 201 KB | not measured |
| Fan-out 1,000, 1 run, 256 B payloads | 27.9 ms | 66 MB | 65% (25% / 38%) |
| Fan-out 1,000, 8 runs, 256 B payloads | 229 ms | 531 MB | 65% (24% / 40%) |
| Fan-out 1,000, 1 run, 16 KiB payloads | 501 ms | 564 MB | 46% (33% / 14%) |
| Fan-out 100, 1 run, 256 B payloads | 2.8 ms | 6.7 MB | 62% (23% / 36%) |

Observations from these runs:

- **Replay cost is linear in history length**: about 25 µs and 80 KB of
  allocation per replayed activation at every length. The run itself holds
  about 3.9 KB of live OCaml data just before its terminal step, whatever the
  history length, and live bytes return exactly to the pre-replay value after
  eviction. The OCaml layer keeps no history-proportional state; the live
  growth with history length is the pre-encoded fixture, which is part of the
  baseline.
- **Cache footprint** is about 3.3 KB of live OCaml data per resident timer
  workflow (3.29 MB for 1,000 runs). Churn costs about 5.8 times as much per
  sample as steady residency because each miss evicts one run and replays
  about 4.4 activations. Retained live bytes after close were flat across
  repetitions in both modes (716 KB steady, 692 KB churn, almost all harness
  latency records for 10,000 samples), so no suspected leak was observed.
- **Allocator capacity versus retention**: compaction released 1.7 MB of RSS
  after the 10k replay and 9.5 MB after the c1 fan-out, returning RSS close to
  baseline while live bytes had already recovered. With 16 KiB payloads, RSS
  reached about 500 MB against a 22 MB live heap and stayed high after
  compaction. Live bytes had recovered, so this is allocator capacity, not
  reachable data. It is consistent with the macOS allocator keeping the
  multi-megabyte JSON strings that OCaml 5 allocates outside its pooled major
  heap.
- **Fan-out scales linearly with concurrency**: 27.9 µs per activity at one
  run and 28.7 µs at eight interleaved runs. There is no contention in this
  single-Domain path, so the interleaving itself costs nothing measurable.
- **The bottleneck is OCaml-side JSON**: strict activation decoding plus the
  completion encoder take about 64% of adapter time with small payloads.
  Allocation (66 KB per activity, 72 major collections per 20 fan-out
  samples) is dominated by the same codec. Completions total 858 KB of JSON
  per 1,000-activity fan-out, almost all in the one that schedules the
  activities, and encoding them takes about 11 ms.
  With 16 KiB payloads, base64 and copying dominate the remainder, decode
  grows to 33%, and the end-to-end cost is about 0.5 ms per activity.
  Optimizing the codec, or a binary protocol, is out of scope here; the
  numbers locate the cost for a later decision.

[#507]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/507
[#526]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/526
[#527]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/527
[#528]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/528
[#846]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/846

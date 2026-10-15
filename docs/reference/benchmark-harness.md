# Reproducible benchmark harness

These exploratory, no-server suites share one versioned JSON report shape. The
original minimal suite creates a fresh in-memory workflow execution, applies
one synthetic `Start_workflow` activation that completes immediately, checks
the terminal command, and shuts down. It does **not** measure Temporal Core,
the C/Rust bridge, worker polling, a Temporal Server, or network latency. The
activation suites for [#526] add distinct warm-cache and cold-replay
boundaries. The [memory and allocation suites](#memory-and-allocation-suites)
for [#527] and the [fan-out suite](#fan-out-and-json-bridge-overhead) for
[#528] extend the same report with memory sections. The
[live fan-out suite](#live-fan-out-and-bridge-attribution) is the one suite
that does run against a Temporal Server. None of these suites alone
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
below are a lower bound on total bridge cost. The
[live suite](#live-fan-out-and-bridge-attribution) measures those parts.

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

## Live fan-out and bridge attribution

The suites above exclude Temporal Core, the native bridge and the server. The
live suite for [#528] runs the same fan-out shape end to end against the
Compose Temporal stack, and attributes its time to the JSON bridge, native
calls and the rest:

```sh
# Starts (or reuses) the Compose stack, then one report per level. The stack
# keeps running for reruns: `make temporal-clean` removes it.
make bench-fanout-live BENCH_LIVE_FANOUT_LEVELS='1 8' BENCH_FANOUT_WIDTH=1000 \
  BENCH_FANOUT_PAYLOAD_BYTES=256 BENCH_LIVE_FANOUT_SAMPLES=5 \
  BENCH_LIVE_FANOUT_REPETITIONS=2 BENCH_HOST_LABEL='machine/CPU/RAM description'
# The Rust half of the JSON bridge, on documents of the same shape.
make bench-json-bridge-rust BENCH_FANOUT_WIDTH=1000 BENCH_FANOUT_BATCH=10 \
  BENCH_FANOUT_PAYLOAD_BYTES=256
```

**Live activity fan-out** (`live-activity-fanout`, `bench_live_fanout.ml`).
One process creates a client and one worker (on its own Domain) on a fresh
task queue. One sample starts `BENCH_FANOUT_CONCURRENCY` executions through
`Client.start` and waits for each. Every execution starts
`BENCH_FANOUT_WIDTH` remote activities with `Activity.start`, fans them back
in with `Future.all` and returns their count, which the sample checks. Each
activity echoes a JSON string of `BENCH_FANOUT_PAYLOAD_BYTES` encoded bytes.
Sample latency therefore includes the client, Temporal Server, PostgreSQL,
Core, the Rust bridge, the C stubs, the supervisor, the OCaml adapter,
workflow code and activity execution. Reports go to
`_build/benchmarks/live-activity-fanout-c<level>.json` and record the server
image (with digest) as `server_version`. `BENCH_LIVE_TARGET_URL` (default
`http://temporal:7233`, inside the Compose network) and
`BENCH_LIVE_NAMESPACE` (default `temporal-sdk-test`) select the server. Width
is bounded to 5,000 and arguments plus results to 32 MiB per execution, well
inside Temporal's default history limits; concurrency is bounded to 64, the
client's start capacity. Memory snapshots are those of the shared harness;
RSS and the Linux `VmHWM` peak cover the whole benchmark process (client,
worker, Core and Rust), not the server or database.

The untimed `live_bridge_attribution` pass reruns
`BENCH_LIVE_ATTRIBUTION_SAMPLES` samples with the SDK's bridge and workflow
log sources at `Debug` and aggregates three kinds of records (see
[observability](observability.md#protocol-codec-timing)):

- every bridge call's native duration, split into calls that returned work
  (`ok`), empty polls (`not_ready`) and errors. A poll that returned work
  covers taking the item from Core's lane, Rust's conversion from protobuf,
  serde encoding with its validation reparse, and the C copy; a completion
  call covers the C copy, serde decoding, Core and, because the bridge waits
  for Core's result, Core's RPC to the server;
- the OCaml codec passes timed by `Observability.debug_timed`;
- each workflow activation's local processing (`activate`).

Durations are per sample. Lanes overlap (the workflow lane, the activity
Domain and the supervisor run concurrently), so the categories are compared
with the pass's wall time rather than summed to it, and the rest of the wall
time is not attributed by a record. The pass is slower than the timed phases
because every record costs a clock read and a reporter call; its wall time is
reported next to the measured latency.

The Rust benchmark (`rust/core-bridge/benches/json_bridge.rs`, Cargo's
optimized bench profile) times `encode_activation` of one activation of
`BENCH_FANOUT_BATCH` activity results, `decode_completion` plus Core
conversion of the completion that schedules `BENCH_FANOUT_WIDTH` activities,
and `decode_completion` of one activity completion, and estimates their sum
per fan-out. It builds its documents from the semantic JSON protocol, so
Core's protobuf-to-semantic conversion and the activity-task encoding are not
in that estimate; the live ready-poll durations bound them instead.

### Indicative live results

Single local runs, not baselines or thresholds. Host: Apple M4 Pro (14
cores, 48 GB), macOS 27, Docker Desktop VM with 14 CPUs and 9.7 GiB
(`Linux 7.0.14-linuxkit aarch64`). The benchmark ran in the development
container (OCaml 5.4.1, the digest-pinned `ocaml-5.4` stage of `Dockerfile.dev`,
Dune `release` profile, Core `95e97686`) against
`temporalio/server:1.32.0` with PostgreSQL 18.6 in the same VM, on this
change's branch from `master` after #990. Width 1,000, payload 256 B, two
repetitions of one warmup and five measured samples, one attribution sample
per repetition. Server, database and benchmark share the VM's CPUs, so
absolute times are specific to this machine and Docker configuration.

| Executions per sample | p50 per sample (rep 1 / rep 2) | Activities/s | Peak RSS (`VmHWM`) | Errors |
| --- | --- | --- | --- | --- |
| 1 | 9.16 s / 8.97 s | 109 | 80 MB / 99 MB | 0 |
| 8 | 98.3 s / 90.0 s | 80 / 87 | 140 MB / 152 MB | 0 |

Attribution pass, first repetition, per sample (one execution: 272
workflow activations, 1,000 activity tasks; eight executions: 6,113
activations, 8,000 tasks):

| Category | 1 execution | Share of wall | 8 executions | Share of wall |
| --- | --- | --- | --- | --- |
| Instrumented wall time | 9.10 s | | 95.2 s | |
| OCaml JSON codec (all five passes) | 77 ms | 0.8% | 777 ms | 0.8% |
| Native polls that returned work | 108 ms | 1.2% | 1.49 s | 1.6% |
| Native completion calls (Core and server RPC) | 2.03 s | 22% | 28.9 s | 30% |
| Native readiness waits (`worker_wait_any`) | 0.86 s | 9% | 15.4 s | 16% |
| Workflow activation processing | 5.5 ms | 0.06% | 82 ms | 0.09% |
| Not inside any native call or codec pass | about 6.0 s | 66% | about 48.6 s | 51% |

The OCaml codec splits as follows for one execution: activity-task decoding
34 µs per task (33.6 ms), activation decoding 40 µs per activation
(10.9 ms), workflow completion encoding 16.6 ms (most of it the one
1,000-command completion), and the two activity completion encodes 8.5 ms and
7.3 ms. Each activity completion takes 1.5 ms (2.1 ms with eight executions)
inside its native call, and each workflow completion 1.8 ms (2.0 ms).

The Rust benchmark, run natively on the same Mac (not in the container), for
width 1,000 and batches of 10:

| Payload | Activation encode (10 results) | Fan-out completion decode + conversion | Activity completion decode | Estimated Rust codec per fan-out |
| --- | --- | --- | --- | --- |
| 256 B | 116 µs | 4.49 ms + 0.25 ms | 2.6 µs | 18.9 ms |
| 16 KiB | 884 µs | 30.5 ms + 0.54 ms | 29 µs | 148 ms |

Findings:

- **JSON is not the live bottleneck.** With 256-byte payloads, OCaml codec
  work is about 77 ms and the Rust codec estimate about 19 ms per
  1,000-activity fan-out: together about 1% of a 9-second fan-out. The
  server-free suite's 65% JSON share describes the adapter alone; once Core
  and the server are in the path it is small. Larger payloads raise it (the
  Rust estimate grows eightfold at 16 KiB) but this run did not measure them
  live.
- **Throughput is bounded by one activity at a time.** The worker has one
  remote activity slot by design (see `Worker.Options`), so 1,000 activities
  are 1,000 sequential poll, execute and complete cycles: about 9 ms each with
  one execution and 11 to 12 ms each with eight. Eight executions add
  activation and workflow-completion traffic (6,113 instead of 272
  activations per 8,000 activities) that competes for the same supervisor,
  so per-activity cost rises rather than amortizing.
- **The largest share is outside native calls.** About 6 s of the 9.1 s
  passes in no native call or codec pass. An unmerged experiment that
  reduced the 10 ms local yield in `Native_worker.wait_for_lane` (used when
  the sibling lane holds the native wait token) to 1 ms cut the one-execution
  p50 from 9.16 s to 3.97 s (three samples, otherwise identical), so that
  yield is the dominant cost of this workload. [#994] then replaced the fixed
  yield with adaptive idle pacing (see "Adaptive idle pacing" below).
- **Native completions are the next cost**: 1.5 to 2.5 ms per activity
  completion, which includes Core's response RPC to the server. Ready polls,
  conversion and serde encoding cost about 80 to 130 µs per item.
- **Memory**: peak RSS stays at 80 to 100 MB for one execution and 140 to
  150 MB for eight. Retained OCaml live bytes after close were about 20 KB in
  both repetitions at both levels, with no rising trend. RSS remained 19 to
  39 MB above the pre-load baseline after compaction, consistent with native
  allocator capacity rather than retained OCaml data; the suite cannot
  separate Core's own retention from allocator behavior.
- **Activity completions were encoded twice** when these numbers were taken:
  once by the activity adapter's validation pass and again by the supervisor,
  about 8 µs each with these payloads. #995 since made it one encode in the
  adapter, timed as `activity_completion_encode`.

### Adaptive idle pacing (#994)

[#994] replaced the fixed 10 ms local yield with adaptive pacing: a lane
repolls at once while its sibling's native wait is in flight, and otherwise
backs off from 0 through 0.1 ms, doubling up to the old 10 ms cap, resetting
whenever a task completes. The same workload (width 1,000, 256 B payloads,
Compose Temporal 1.32.0, OCaml 5.2.1 container) measured before and after
the change, as recorded for PR #996:

| Executions per sample | Samples | p50 before | p50 after | Activities/s | Errors |
| --- | --- | --- | --- | --- | --- |
| 1 | 5 | 10.89 s | 5.94 s | 90 to 162 | 0 |
| 8 | 3 | 107.9 s | 79.1 s | 75 to 92 | 0 |

In the one-execution attribution pass the time outside any native call or
codec pass fell from about 5.5 s to about 0.3 s; what remains is Core
completion calls and real server waits. Empty polls rose from about 2.4k to
6.4k per sample at about 1.7 µs each. The eight-execution samples are noisy
(67, 79 and 115 s). These are single-machine numbers.

On Windows the gain was initially small: `Thread.delay` rounds
sub-millisecond requests to an immediate return and longer ones to the
15.6 ms timer tick, so a simulated serial step took about 21 ms with adaptive
pacing against about 23 ms with the fixed yield. [#1008] moved the local
yield to the bridge's high-resolution pacing sleep; under the same simulated
Windows model a serial step now takes about 1.1 ms, as on macOS
(`IDLE_TEST_SLEEP_MODEL=windows-thread-delay` reproduces the old 21 to 23 ms).

[#507]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/507
[#526]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/526
[#527]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/527
[#528]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/528
[#846]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/846
[#994]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/994
[#1008]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/1008

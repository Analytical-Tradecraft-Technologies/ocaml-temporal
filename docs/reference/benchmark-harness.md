# Reproducible benchmark harness

These exploratory, no-server suites share one versioned JSON report shape. The
original minimal suite creates a fresh in-memory workflow execution, applies
one synthetic `Start_workflow` activation that completes immediately, checks
the terminal command, and shuts down. It does **not** measure Temporal Core,
the C/Rust bridge, worker polling, a Temporal Server, or network latency. The
activation suites for [#526] add distinct warm-cache and cold-replay
boundaries. The later memory and fan-out suites in [#527] and [#528] should
reuse the report shape. None of these suites alone qualifies the sustained-load
gate in [#507].

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

All three baselines admit work synchronously at concurrency one. Pending-attempt
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

[#507]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/507
[#526]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/526
[#527]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/527
[#528]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/528
[#846]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/846

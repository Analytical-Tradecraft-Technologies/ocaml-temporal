# Reproducible benchmark harness

The first suite is an exploratory, no-server benchmark for the OCaml execution
layer. It creates a fresh in-memory workflow execution, applies one synthetic
`Start_workflow` activation that completes immediately, checks the terminal
command, and shuts the execution down. It does **not** measure Temporal Core,
the C/Rust bridge, worker polling, a Temporal Server, or network latency. The
later activation, memory, and fan-out suites in [#526], [#527], and [#528]
should reuse the versioned report shape while identifying their own measured
boundaries. This suite does not qualify the sustained-load gate in [#507].

From a checkout with Docker Compose and Python 3 available, run:

```sh
make bench BENCH_WARMUP=100 BENCH_SAMPLES=1000 BENCH_REPETITIONS=3 \
  BENCH_SEED=1 BENCH_HOST_LABEL='machine/CPU/RAM description'
```

The command builds and runs the OCaml executable with Dune's `release` profile
in the development container;
it does not start a Temporal Server or require credentials. By default, the
report is `_build/benchmarks/local-minimal-activation.json`. Set
`BENCH_REPORT=/path/to/report.json` to retain it outside the ignored build tree.
The command exits nonzero for build failures, invalid configuration, or any
failed workflow sample. A valid JSON report is retained even when a sample
fails, with attempted sample counts, errors, and the first three error messages.

Report `schema_version: 1` contains the source commit and dirty-tree flag, SDK
version, OCaml compiler version, Dune profile, pinned Core revision, server version (`none`),
machine/container identity, host label, exact warmup and measurement counts,
repetitions, seed, workload configuration, phase duration, per-attempt latency
in microseconds, p50/p95/p99 (nearest-rank), successful operations per second,
and error count. The seed is recorded for the shared harness interface; the
minimal workload does not use randomness. Each repetition warms up separately.
Latency and throughput use the process wall clock and include local execution
creation, activation, validation, and shutdown. The per-attempt array supports
later analysis; the overall phase duration is authoritative for throughput.

Compare reports only when their source/configuration, compiler, machine, and
measurement boundary are compatible. No threshold is inferred from one local
run. [#507] still requires representative live workloads, agreed budgets,
sustained-load and fault-recovery evidence, and stable release-gate metrics.

[#507]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/507
[#526]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/526
[#527]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/527
[#528]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/528

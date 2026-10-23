# Bridge lifecycle stress

Issue [#522](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/522)
adds seeded, reproducible operation-sequence stress for the native lifecycle.
It complements the fixed lifecycle regressions (`lifecycle.rs`,
`runtime_cleanup*.rs`, `worker_shutdown_outstanding.rs`,
`runtime_dispose_eviction.rs`, `replay_abi.rs`), the
[structured parser fuzz](bridge-parser-fuzz.md), and the C ABI sanitizer
harness. Two tests make up the stress:

- `rust/core-bridge/tests/lifecycle_stress.rs` drives the C ABI directly.
- `test/bridge/test_ocaml_lifecycle_gc_stress.ml` drives the OCaml bindings
  under GC pressure.

Neither test needs a Temporal server.

## Rust operation-sequence stress

Each case generates a sequence of abstract operations over two runtime slots.
The sequence depends only on the seed and the case number. The operations are:

| Area | Operations |
| --- | --- |
| Runtime | create; explicit `runtime_free`; GC-fallback `runtime_dispose`; a repeated free or dispose of a released slot; null slot pointers |
| Replay worker | start; feed a complete or open history; feed a malformed history; finish input; bounded wait; poll; complete; reject; finalize; drain; dispose |
| Live client and worker | connect; refused connect; activity-only worker start; bounded wait; poll; complete; reject; shutdown; disconnect |
| Misuse | completion or rejection of a retired or never-leased lease; every call on a released (null) slot; the ABI panic probe |

About three quarters of the operations are chosen from the generator's own
view of each slot's intended state. Examples are a start after a create, a
wait followed by a poll on a running worker, and the completion of a held
lease. The other quarter are drawn from the whole operation table, so calls in
the wrong state, after release, and on stale leases are mixed in.

Every operation is valid in every state. An operation that does not apply to
the current state still runs and must return the status the ABI documents for
that state. Two kinds of call are never made, because the ABI contract forbids
them: overwriting a live runtime slot and passing a dangling pointer. After a
release, the slot holds the null that `runtime_free` or `runtime_dispose`
wrote back, and every later call uses that null.

Replay workers run without a client. Live workers connect to a plaintext
HTTP/2 gRPC double on loopback. The double gives a new, uniquely tokenized
activity task to every activity poll and records each completion,
failure, or cancellation it receives for a token. Native scheduling is not
seeded: whether a poll finds a task depends on Core's timing. For such racy
calls the model accepts every documented status and updates itself from the
observed one.

### What the model and ledgers check

The checks below run after every operation and again after the case tears
down.

- **Handles.** Creations from `runtime_new` must match the model exactly. The
  process-local cleanup counter (`test_runtime_cleanup_counts`) must never
  exceed the releases the model made, which would mean a double release. It
  must also cover every waiting `runtime_free`. After teardown it must
  converge so that every runtime has been cleaned exactly once.
- **Released slots.** A released slot is null. Every runtime-scoped call on
  it returns `STATUS_INVALID_ARGUMENT`, and a repeated free or dispose of it
  returns `STATUS_OK`.
- **Leases.** A replay activation must belong to a history that was fed and
  not yet evicted, and its run must not already be leased. An activity token
  must never be delivered twice. Completing or rejecting a retired lease must
  be refused (`STATUS_PROTOCOL` for replay completions and for rejections,
  `STATUS_WORKER` for activity completions). Worker shutdown must return
  `STATUS_OUTSTANDING_TASKS` exactly when the test still holds a lease.
- **Server completions.** The double must never see a second completion for
  a token. After teardown, every token the bridge leased to the test must
  have exactly one completion. This holds whichever path retired the token:
  explicit completion, rejection, shutdown, `runtime_free`, or the GC-fallback
  dispose. Tokens the bridge received but never delivered are allowed one
  completion at most, because a poll response cancelled in flight never
  reaches Core.
- **Replay drain.** A replay may finalize only after its input is closed, no
  lease is held, and every fed history has been evicted. A drain closes the
  input and completes every activation, after which finalization must
  succeed.
- **Results.** Every result has a stored status equal to the returned one, at
  most one owned buffer, and a UTF-8 diagnostic. It can be released twice.
  Only the panic probe may return `STATUS_PANIC`.

At the end of the run, the test prints the count of every
`(operation, outcome)` pair it observed. With the default seed and budget it
also requires a fixed set of important outcomes to appear at least once, so a
change to the generator cannot quietly reduce the stress to misuse-only calls.
That set includes leased and completed activations and tasks, shutdown with
an outstanding lease, `runtime_free` and `runtime_dispose` with a held lease,
and a drained, finalized replay. Only the generated cases count: the
hand-written regressions and the reruns made while minimizing a failure record
into their own collectors, so they cannot satisfy this check.

### Budgets and reproduction

The ordinary `cargo test --manifest-path rust/Cargo.toml --locked` run, and
therefore every Rust CI job, executes 24 cases of 48 operations with seed
`0x05222026`. This takes about 8 seconds on an Apple M-series laptop. Most of
that time is the 100 ms bounded readiness waits that return `NOT_READY`. Each
case is also bounded: a native call that does not return within 60 seconds
is reported as a hang.

Four environment variables control the run:

| Variable | Meaning | Default | Range |
| --- | --- | --- | --- |
| `LIFECYCLE_STRESS_SEED` | Base seed, decimal or `0x` hexadecimal | `0x05222026` | any 64-bit value |
| `LIFECYCLE_STRESS_CASES` | Number of cases | 24 | 1–1,000,000 |
| `LIFECYCLE_STRESS_STEPS` | Operations per case | 48 | 1–2,000 |
| `LIFECYCLE_STRESS_CASE` | Run only this case number | unset | any |

`make test-lifecycle-stress` runs both stress tests in the development
container. `make native-test-lifecycle-stress` uses the host toolchain. Both
accept the same variables. A longer run looks like this:

```sh
make native-test-lifecycle-stress LIFECYCLE_STRESS_CASES=2000 LIFECYCLE_STRESS_SEED=0x1234abcd
```

A local run of 300 cases with seed `0x7e57` (14,400 operations) took 102
seconds and passed. Scale `LIFECYCLE_STRESS_CASES` to fit the time available,
at roughly 0.34 seconds per case.

When a case fails, the test deletes chunks of operations while a failure in
the same category persists, for at most 256 extra runs. It then panics with a
report, and writes the same report to
`_build/lifecycle-stress/<seed>-<case>.txt` (override the directory with
`LIFECYCLE_STRESS_OUTPUT_DIR`). The report contains:

- the seed, case number, and step count;
- the exact `make native-test-lifecycle-stress` command that regenerates the
  case;
- the original failure and operation list;
- the minimized failure, operation list, and per-operation trace with each
  observed status.

Rust producer CI keeps this directory as a 14-day `lifecycle-stress-*`
artifact when a job fails.

Because native timing is not seeded, the minimized sequence can fail for a
slightly different reason than the original, and a regenerated case can pass
on a rerun. Rerun the reported case a few times before treating it as fixed.
The minimized operation list uses Rust syntax, so it can be pasted directly
into the `scripted_regressions` test as a permanent regression. That test
already pins several sequences:

- activity leases abandoned at the GC-fallback dispose, followed by a double
  close;
- shutdown with an outstanding lease, followed by stale completions on a
  restarted worker;
- replay leases held at `runtime_free` and at replay dispose;
- a replay drained after a rejection.

A native hang is not minimized. The runtime involved stays wedged inside the
bridge and is deliberately leaked, so the run stops and reports the trace up
to the hang.

To add an operation, such as the shared-runtime symbols proposed in PR #939,
follow the checklist at the top of `lifecycle_stress.rs`.

## OCaml GC and finalizer stress

`test_ocaml_lifecycle_gc_stress.ml` runs in the ordinary `dune runtest`. It
performs 48 seeded runtime cycles through `Native_bridge`. Each cycle creates
a runtime, may start a replay worker, may finish its input, and may wait on
it. The cycle then does one of three things:

- drops the runtime, so the custom-block finalizer releases it through
  `runtime_dispose`;
- disposes the replay worker, closes the runtime, and closes it again;
- closes the runtime with its children still running, checks that every later
  call is rejected with `Invalid_argument`, and closes it again.

Minor collections, full major collections, and compactions run between the
calls. This moves the runtime custom block while the C stubs read it, and
finalizes the runtimes the cycles dropped. A few real `Sdk_supervisor.Native`
supervisors are also created, checked, and shut down twice, with collections
in between.

`LIFECYCLE_STRESS_SEED` and `LIFECYCLE_STRESS_OCAML_CYCLES` (1–100,000, default
48) set the seed and the length. The OCaml test accepts the same unsigned
64-bit seeds as the Rust stress and splits the value into its low and high
32-bit halves to seed `Random.State`, so no bits are lost. The Make targets pass both, and Dune reruns
the test whenever either value changes.

## Concurrent stress (#506)

The stresses above run every native call from one thread. Issue
[#506](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/506)
adds two tests in which several threads or Domains change native state at the
same time. Each still respects the single-owner rule: only the owning thread
or Domain makes ordinary calls on a graph.

### Rust: threads and shared runtimes

`rust/core-bridge/tests/concurrent_lifecycle_stress.rs` runs seeded rounds.
In each round:

1. The coordinator creates two shared runtimes with two Tokio threads each.
2. Every worker thread concurrently attaches its graphs to those handles
   (the ABI permits concurrent attaches). One graph in four builds its own
   Core instead.
3. A start gate releases the threads together. At a seeded moment the
   coordinator releases each shared handle, through `shared_runtime_free` or
   the GC-fallback `shared_runtime_dispose`, while the threads keep working.

Each graph follows one of six seeded scripts: release idle; start a replay,
feed a history, pump, then `runtime_free` or `runtime_dispose` (sometimes with
a lease held); drain every activation and finalize; restart a disposed replay
worker; or dispose a replay worker early. A pump is one bounded wait, one
poll, and the completion of any leased activation.

The test checks:

- every call returns a documented status, and never `STATUS_PANIC`,
  `STATUS_INTERNAL`, or `STATUS_INVALID_ARGUMENT` on a live handle;
- a released slot is null;
- a round finishes within 180 seconds, or it is reported as a hang. The
  deadline starts before the threads are spawned and bounds setup, the start
  gate, and the run; no wait is unbounded. A thread that panics anywhere
  reports at once through `catch_unwind`. If setup fails or times out, the
  gate is aborted, the other threads release their graphs and report within
  a 30-second grace, and the shared runtimes are released only once every
  thread has reported (a hung thread might still be attaching). A second
  test, `setup_faults_are_reported_within_the_deadline`, injects a setup hang
  and a setup panic and checks that each is reported promptly and leaks
  nothing; the ordinary stress never injects a fault. The two tests share a
  mutex, and each one waits until exactly its own Core runtimes have been
  dropped before releasing it. Each round counts the Cores it creates,
  including those of graphs that a failed setup releases again. A shared
  runtime released through the GC-fallback `shared_runtime_dispose` drops its
  Core later, on a cleanup thread. Before this wait, such a late drop from the
  fault-injection test could land inside the other test's measurement, which
  macOS CI on PR #1003 reported as one drop too many;
- after the last round, within 60 seconds, every graph has been cleaned
  exactly once and every Core runtime, owned or shared, has been dropped
  exactly once, whichever holder released it last;
- no panic, on any thread, has a location in `rust/core-bridge/src`. A panic
  hook records every panic, including those on Core's Tokio threads that the
  ABI's `catch_unwind` never sees. Panics inside Temporal Core are reported
  in the summary rather than failed; see [Findings](#findings).

The default budget (3 rounds, 4 threads, 3 graphs each) runs in about half a
second, and it must observe a completion, a finalization, a
`runtime_dispose`, and a `runtime_free`.

### Rust: concurrent live workers and clients

The replay rounds above need no server, so they never exercise a live worker
or a client call. The test `live_stress::concurrent_live_workers_and_clients`
in `lifecycle_stress.rs` (source `rust/core-bridge/tests/support/live_stress.rs`)
does, against the same gRPC double as the sequential cases. The double is
extended for it:

- it records every `RecordActivityTaskHeartbeat` per token and answers it
  with `cancel_requested` for every third token;
- it holds every `GetWorkflowExecutionHistory` long poll (a submitted
  `wait`) open until the client resets the stream or the connection closes,
  and counts both the held and the ended polls;
- it counts a completion or heartbeat only when its request arrived whole,
  because a request that the client reset mid-body is never acted on by a
  real server.

In each round the coordinator creates one shared Core runtime and a reaper
thread, then starts the worker threads. Each thread owns two graphs. One graph
in three builds its own Core; the others attach to the shared runtime, which
the coordinator frees or disposes at a seeded moment once every attach has
finished. Each graph then runs a seeded number of operations chosen from its
modelled state:

| Area | Operations |
| --- | --- |
| Worker | connect; start an activity worker; bounded wait and poll; complete, reject, or heartbeat the held lease; complete it as cancelled after Core delivers a Cancel; hand it off with `will_complete_async`; `worker_initiate_shutdown`; `worker_shutdown` with or without a held lease; `client_disconnect`, which is refused while the worker runs |
| Async activity (#1025) | submit throttled async heartbeats (500 ms heartbeat timeout) and async completions for handed-off tokens through any of the thread's graphs that runs a worker, or through one without a worker, which must refuse them |
| Client calls (#807, #1019) | submit signals, workflow cancellation requests, and waits the double never answers; await them from the submitting thread; release some before their outcome |
| Release | `runtime_free` or `runtime_dispose` on the owning thread, or a handoff to the reaper thread, which releases the graph while its former owner keeps awaiting the graph's calls |

Every call must return a documented status, never `STATUS_PANIC`,
`STATUS_INTERNAL`, or `STATUS_INVALID_ARGUMENT`. Native timing decides some
outcomes, so the model accepts every documented status of a racy call and
updates itself from the observed one. Beyond the status of each call, the
test checks the following:

- **Leases.** A Start task is delivered once across all threads and only
  while the worker's single activity slot is free. A Cancel task must update
  the lease the graph still holds, once. The bridge drops a Cancel whose
  Start was retired, so a stale Cancel is a failure.
- **Calls.** An await after release, disconnect, or a release on the owning
  thread reports the closed failure at once, because each of those
  unregisters the call before it returns. A call whose graph the reaper is
  releasing must reach a terminal outcome within 30 seconds. A wait never
  completes successfully. An async heartbeat answers `cancel_requested` only
  for a token the double cancels. A disconnect leaves no heartbeat throttle
  state (`test_heartbeat_throttle_counts`).
- **Shutdown.** `worker_shutdown` returns `STATUS_OUTSTANDING_TASKS` exactly
  when the test holds a lease. It and every waiting `runtime_free` finish
  within 20 seconds, and a round within 180 seconds.
- **Ledgers after the last round.** Every graph is cleaned exactly once and
  every Core runtime, owned or shared, is dropped exactly once. The
  process-wide client call registry returns to its baseline
  (`test_registered_client_calls`, a test-only hook added for this check).
  Every held history long poll has ended, so released and closed calls
  aborted their RPCs instead of leaving them detached. Each leased token has
  exactly one server completion when one is owed: worker completion,
  rejection, force-completion at shutdown or release, or an async completion
  read as accepted. A token handed off without a confirmed async completion
  has at most one. The server never saw more heartbeats for a token than the
  test recorded or submitted, so throttling coalesces heartbeats but never
  invents them.
- **Panics.** The test installs a panic hook that records panics on every
  thread, including Core's Tokio threads. No panic may come from
  `rust/core-bridge/src`, and each thread's body runs under `catch_unwind`, so
  a panic in a stress thread is reported rather than lost. Panics inside
  Temporal Core are reported in the summary; see [Findings](#findings).

The default budget is 3 rounds of 4 threads with up to 48 operations per
graph, about 120 leased tokens. It runs in about 1.5 seconds and must reach a
fixed set of outcomes, including a Cancel for a held lease, a cancelled
completion, an async heartbeat answered `cancel_requested`, an accepted
async completion, a released wait, `STATUS_OUTSTANDING_TASKS` from shutdown,
and both release paths on both threads. A failure writes the seed, round,
rerun command, and each failing thread's trace to
`_build/lifecycle-stress/live-<seed>-round<round>.txt`. Thread scheduling is
not seeded, so a rerun with `LIFECYCLE_STRESS_LIVE_ROUND` regenerates the
operation choices but not the interleaving.

`make stress-qualification` runs 200 live rounds of 12 threads with up to 96
operations per graph. Locally (seed `0x7e57`) that leased 47,950 tokens
across 4,800 graphs and 1,805 Core runtimes in 224 seconds and passed.

### OCaml: Domains and the C borrow gates

This test is not part of `dune runtest`: it runs under the
`@test/bridge/concurrent-stress` alias, which `make test` and
`make native-test` build only after `dune runtest` has finished (see
[decoder fuzzing](bridge-decoder-fuzz.md) for why).

`test/bridge/test_ocaml_concurrent_lifecycle_stress.ml` runs seeded rounds of
worker Domains through `Native_bridge`. In each round:

- the main Domain creates a shared runtime, and each worker Domain attaches
  graphs to it or creates its own runtime while the main Domain closes the
  shared runtime at a seeded moment;
- each worker Domain makes replay calls (start, finish, wait, poll, dispose,
  close) on the runtimes it owns;
- each worker Domain publishes one runtime, and its neighbour may close that
  runtime while the owner is inside, or about to enter, a blocking call;
- every Domain runs minor and major collections and compactions between
  calls, and drops some runtimes for the custom-block finalizer.

Every result must be `Ok` or a documented typed error, never `Panic`,
`Internal`, `Abi_mismatch`, or an unknown status. A runtime that the owner
has seen released must keep refusing calls with `Invalid_argument`. An attach
after the shared close must be refused.

A second Domain making ordinary (non-close) calls on the same runtime is
outside the contract. The C gate admits it, but the Rust graph would then be
mutated from two threads. The supervisor never does this, so the stress does
not either.

### Budgets

| Variable | Meaning | Default | Range |
| --- | --- | --- | --- |
| `CONCURRENT_STRESS_SEED` | Rust base seed | `0x0506c0c0` | any 64-bit value |
| `CONCURRENT_STRESS_ROUNDS` | Rust rounds | 3 | 1–10,000 |
| `CONCURRENT_STRESS_THREADS` | Rust worker threads per round | 4 | 2–64 |
| `CONCURRENT_STRESS_GRAPHS` | Rust graphs per thread | 3 | 1–32 |
| `CONCURRENT_STRESS_OCAML_ROUNDS` | OCaml rounds | 6 | 1–10,000 |
| `CONCURRENT_STRESS_OCAML_DOMAINS` | OCaml worker Domains | 4 | 2–32 |
| `LIFECYCLE_STRESS_LIVE_ROUNDS` | Live rounds | 3 | 1–10,000 |
| `LIFECYCLE_STRESS_LIVE_THREADS` | Live worker threads per round | 4 | 2–32 |
| `LIFECYCLE_STRESS_LIVE_STEPS` | Upper bound of operations per live graph | 48 | 2–2,000 |
| `LIFECYCLE_STRESS_LIVE_ROUND` | Run only this live round | unset | any |

The OCaml test and the live stress take their seed from
`LIFECYCLE_STRESS_SEED`. The live variables belong to
`make test-lifecycle-stress` and `make native-test-lifecycle-stress`, which
run the `lifecycle_stress` binary.

```sh
make native-test-concurrent-stress CONCURRENT_STRESS_ROUNDS=300 CONCURRENT_STRESS_THREADS=16 CONCURRENT_STRESS_GRAPHS=4 CONCURRENT_STRESS_OCAML_ROUNDS=300 CONCURRENT_STRESS_OCAML_DOMAINS=8
```

On an Apple M-series laptop, 400 Rust rounds of 16 threads with 4 graphs each
(25,600 graphs, 7,276 Core runtimes) took about 67 seconds, and 300 OCaml
rounds of 8 Domains took a few seconds. Thread and Domain scheduling is not
seeded, so a failure may not reproduce exactly. A Rust failure writes the
seed, round, rerun command, and each failing thread's call trace to
`_build/concurrent-stress/<seed>-round<round>.txt` (override with
`CONCURRENT_STRESS_OUTPUT_DIR`). Rust producer CI keeps that directory as a
14-day artifact when a job fails.

### Findings

Long concurrent runs occasionally record one panic inside Temporal Core:
`poll_task_awaiter.await.unwrap()` on `JoinError::Cancelled`. It happens when a
Core runtime shuts down while a worker's poll task is still running, so Tokio
cancels the inner task and the outer task unwraps the cancellation. Tokio
contains the panic inside its worker thread. It does not cross the ABI,
change any status, leak a graph or a Core runtime, or break the cleanup
counters. Release builds keep `panic = "unwind"`, so the containment holds
there too. It is an upstream teardown-order defect, not a bridge memory or
ownership violation.

It was first recorded at `poll_buffer.rs:428`. The Core upgrade in PR #989 did
not fix it. Re-checked on 2026-10-16 against the pinned Core `8fe478e`, the
unchanged `unwrap` is now at
`crates/sdk-core/src/pollers/poll_buffer.rs:452`, and it still occurs:

- The replay concurrent stress with 400 rounds of 16 threads and 4 graphs
  (25,600 graphs, about 7,300 Core runtimes) recorded it exactly once with
  each of seeds `0x0506c0c0` and `0x77`, as
  `JoinError::Cancelled` on a `tokio-rt-worker` thread. Both runs passed:
  every graph and Core runtime was released exactly once and no bridge code
  panicked.
- The live stress recorded no occurrence in 1,440 graphs (60 rounds of 12
  threads, seed `0x1234abcd`) or in 4,800 graphs at the qualification budget
  (seed `0x7e57`).
- With the default budgets neither stress records it.

The containment therefore still holds. Both concurrent stresses report any
recurrence in their summary (`contained_upstream_panics`) and fail on a panic
located in bridge code. It stays tracked here, not upstream, and needs
re-evaluation after the next Core upgrade.

## What each tool observes

| Component | Rust stress | Rust concurrent stress | Rust live concurrent stress | OCaml GC stress | OCaml multi-Domain stress | `test/bridge/test_abi.sh` |
| --- | --- | --- | --- | --- | --- | --- |
| C ABI status, result, and slot contracts | Yes, called from Rust | Yes, from several threads | Yes, from several threads | Indirectly | Indirectly | Yes, called from C |
| Rust bridge (`abi.rs`, `worker_bridge.rs`, `replay_bridge.rs`) | Yes: panics, debug assertions, ledgers | Yes: panics on any thread, cleanup counters | Yes: panics on any thread, cleanup counters, server ledgers | Through the C stubs | Through the C stubs | Linked, not instrumented |
| Live workers, submitted client calls, async heartbeat throttle (`client_calls.rs`, `heartbeat_throttle.rs`) | Live workers only, one thread | No | Yes, with call registry and throttle checks | No | No | No |
| Shared Core runtimes and cross-thread release order | No | Yes | Yes, with a reaper thread | No | Yes, through the C gate | No |
| Temporal Core worker and replay state machines | Yes, as linked | As linked | Activity worker and client, as linked | As linked | As linked | As linked |
| C stubs (`native_stubs.c`) and the OCaml custom block | No | No | No | Yes | Yes, with cross-Domain close | No |
| OCaml GC and finalizer interaction | No | No | No | Yes | Yes, with collections on other Domains | No |
| AddressSanitizer and UndefinedBehaviorSanitizer | No | No | No | No | No | C harness code only |

The stable Rust test profile gives debug assertions, overflow checks, and
panic detection. It does not give sanitizer or coverage instrumentation, and
the pinned toolchain has no sanitizer support. A use-after-free inside Rust
or Core would therefore show up only as a wrong status, a panic, a hang, or
a ledger mismatch, not as an ASan report.

The ABI's pointer-to-pointer release design makes a dangling handle
unreachable for a caller that follows the contract. The stress checks that
design, by verifying that the slot is cleared and every later call is
rejected. It does not check what happens to a caller that copies a handle and
uses the copy after release, which is undefined behavior by contract.

The cleanup counters observe whole runtimes. Child objects (client, worker,
replay worker) are counted only through the runtime that owns them and
through the server completion ledger. Thread and file-descriptor counts are
not measured, because those depend on the platform.

## Scope and follow-up

Lifecycle cases are single-threaded per runtime, which matches the ABI's
single-owner rule; the [concurrent stress](#concurrent-stress-506) adds
cross-thread and cross-Domain release, close, and finalizer races. The
scheduled workflow `.github/workflows/stress.yml` runs every stress and
fuzzer weekly with large budgets; see
[scheduled qualification](bridge-decoder-fuzz.md#scheduled-qualification).

Parent issue
[#506](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/506)
keeps these items:

- a sanitizer-instrumented build of the Rust bridge (nightly
  `-Zsanitizer=address` with `-Zbuild-std`), which the pinned stable
  toolchain cannot provide and which needs a decision about a nightly
  toolchain;
- final qualification, which follows #492.

Live-worker and client paths under concurrency are covered by the
[live concurrent stress](#rust-concurrent-live-workers-and-clients) against
the gRPC double. It does not cover live workflow workers, whose tasks need
real histories from a server, or a real Temporal server's timing and
failures; the Docker Compose acceptance suites cover those.

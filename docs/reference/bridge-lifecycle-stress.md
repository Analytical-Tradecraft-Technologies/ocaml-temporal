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

## What each tool observes

| Component | Rust stress | OCaml stress | `test/bridge/test_abi.sh` |
| --- | --- | --- | --- |
| C ABI status, result, and slot contracts | Yes, called from Rust | Indirectly | Yes, called from C |
| Rust bridge (`abi.rs`, `worker_bridge.rs`, `replay_bridge.rs`) | Yes: panics, debug assertions, ledgers | Through the C stubs | Linked, not instrumented |
| Temporal Core worker and replay state machines | Yes, as linked | As linked | As linked |
| C stubs (`native_stubs.c`) and the OCaml custom block | No | Yes | No |
| OCaml GC and finalizer interaction | No | Yes | No |
| AddressSanitizer and UndefinedBehaviorSanitizer | No | No | C harness code only |

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

Cases are single-threaded per runtime, which matches the ABI's single-owner
rule. The two slots interleave on one test thread. Parent issue
[#506](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/506)
keeps four items:

- scheduled long runs;
- concurrent execution, cancellation, and shutdown across Domains and
  runtimes;
- a sanitizer-instrumented build of the Rust bridge (nightly
  `-Zsanitizer=address` with `-Zbuild-std`);
- the final production runtime paths.

The repository has no scheduled stress job yet. The nightly master build
reuses the cached Rust bridge and does not rerun Rust tests on a cache hit.
Until a scheduled job exists, run the longer budget above by hand before
releases or after lifecycle changes.

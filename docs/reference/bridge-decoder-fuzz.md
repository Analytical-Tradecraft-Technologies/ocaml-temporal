# Bridge decoder fuzzing

Issue [#506](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/506)
extends the [structured parser fuzz](bridge-parser-fuzz.md) to every JSON
decoder on both sides of the bridge. Two harnesses share one committed corpus
and one set of mutation operators:

- `rust/core-bridge/tests/decoder_fuzz.rs` covers every Rust decoder that the
  OCaml side reaches through the C ABI.
- `test/bridge/test_ocaml_decoder_fuzz.ml` covers every OCaml decoder that
  reads a document the Rust bridge returns.

The Rust harness runs in the ordinary `cargo test`. The OCaml harness is not
part of `dune runtest`. It runs under the `@test/bridge/decoder-fuzz` alias,
which `make test` and `make native-test` build through
`@test/bridge/bridge-stress` only after `dune runtest` has finished. Both use
a small fixed budget there. A [scheduled workflow](#scheduled-qualification)
runs them with large budgets.

The OCaml multi-Domain lifecycle stress shares that arrangement. Under
`dune runtest`, Dune ran it alongside the timing-bound runtime tests. It starts
several Core runtimes and Tokio pools at once, and on the small Windows x64
runner that pushed `test_native_activity_execution`,
`test_native_worker_loop`, and `test_native_worker_bounded_shutdown` past their
bounds. Running the stress aliases in a separate step after `dune runtest`
keeps the same PR coverage without the overlap. To run them alone:

```sh
dune build @test/bridge/bridge-stress    # both OCaml harnesses, default budgets
make native-test-decoder-fuzz            # Rust and OCaml decoder fuzz
```

## Targets

| Side | Target | Entry point | Accepted means | Typed rejection |
| --- | --- | --- | --- | --- |
| Rust | `activity-task`, `activity-completion`, `activity-heartbeat` | `activity_protocol::decode_*` | `Ok`, and the value re-encodes and decodes | `Err` |
| Rust | `async-complete`, `async-heartbeat` | asynchronous-activity ABI calls | `STATUS_INVALID_STATE` (no client) | `STATUS_PROTOCOL` |
| Rust | `client-start`, `-cancel`, `-terminate`, `-reset`, `-signal`, `-query`, `-update`, `-poll-update`, `-visibility`, `-wait` | client ABI calls on a runtime without a client | `STATUS_INVALID_STATE` | `STATUS_PROTOCOL` |
| Rust | `start-ticket-poll`, `start-ticket-wait` | `client_poll_start_workflow_json`, `client_wait_start_workflow_json` (`client_protocol::decode_start_ticket`) on a runtime without a client | `STATUS_INVALID_STATE` | `STATUS_PROTOCOL` |
| Rust | `client-config` | `client_connect_json` | `STATUS_CONNECTION` from the refusing port `127.0.0.1:1` | `STATUS_CONFIGURATION` |
| Rust | `worker-config` | `replay_worker_start_json` (a real replay worker, disposed after each acceptance) | `STATUS_OK` | `STATUS_CONFIGURATION`, `STATUS_WORKER` |
| Rust | `replay-history` | `replay_worker_feed_history_json` after `finish_input` | `STATUS_INVALID_STATE` (feeder closed, after Core's `HistoryInfo` gate) | `STATUS_PROTOCOL` |
| OCaml | `control-envelope`, `control-payload`, `control-payload-object` | `Control_protocol.decode`, `decode_payload`, `decode_payload_object` (also used directly to validate replay histories) | `Ok`, and the value re-encodes and decodes | `Error` |
| OCaml | `workflow-activation`, `workflow-completion` | `Workflow_protocol.decode_*` | as above | `Error` |
| OCaml | `activity-task`, `activity-completion`, `activity-heartbeat` | `Activity_protocol.decode_*` | as above | `Error` |
| OCaml | every client response and error body: start, start outcome, ticket, cancel, terminate, signal, reset, query, visibility, update, poll update, and wait responses, and all eight error decoders (generic, start, wait, cancel, reset, signal, query, update) | `Client_protocol.decode_*` | `Ok` (start outcomes also re-encode) | `Error` |
| OCaml | `client-call` | `Native_bridge.parse_client_call`, the `<owner>.<call>` handle Rust returns from a submitted call | `Some`, and both parts are plain decimal digits | `None` |

The Rust `worker-config` target uses the replay worker because a live worker
checks for a client before semantic validation; both share one decoder and
validator. The live `worker_start_json` and the telemetry configuration
proposed in PR #1000 are not targets yet. Rust client responses and activity
tasks are encoded, not decoded, by Rust; the OCaml harness fuzzes their
decoders.

### Properties checked for every input

- **No panic or escaped exception.** A Rust unwind in a pure decoder, an ABI
  `STATUS_PANIC`, or any OCaml exception (including `Stack_overflow` and
  `Out_of_memory`) fails the case.
- **Typed rejection.** An ABI call returns the target's accepted status or one
  of its listed rejection statuses. The stored result status must equal the
  returned one, a failure carries no value bytes, and its diagnostic must be
  non-empty UTF-8 of at most 4,096 bytes, so a rejection cannot echo a large
  input.
- **Bounded allocation.** The Rust harness installs a counting global
  allocator and measures the peak live heap during each call. It must stay
  below 2 MiB (8 MiB for the two configuration targets, which construct Core
  objects) plus 32 bytes per input byte. The OCaml harness bounds the total
  bytes allocated by one decode (`Gc.allocated_bytes`) to 4 MiB plus 512
  bytes per input byte.
- **Round trip.** Where an encoder exists, an accepted value re-encodes and the
  encoding decodes again.

A target also fails if no generated input reaches the rejection path. With the
default seed and budget, Rust targets must also reach the accepted path.

## Findings

`Native_bridge.parse_client_call` documented that both parts of a call
handle are positive decimal integers, but it parsed each part with
`int_of_string_opt`, which also accepts a sign, `_` separators, and `0x`,
`0o`, `0b`, and `0u` prefixes. `+1.2`, `0x1.2`, and `1.0b10` were therefore
accepted, and several spellings could alias one handle. Each part must now
be nonempty ASCII digits before conversion. The spellings are kept as
`client-call/regressions/` seeds, and `test/bridge/test_ocaml_bridge.ml`
asserts them. Rust only ever issues plain decimal handles, so no released
behaviour depended on the old parse.

## Corpus

Each target's seeds live under `test/bridge/fixtures/decoder-fuzz/<target>/`
(OCaml client bodies under `decoder-fuzz/ocaml-client/<kind>/`). The OCaml
control and workflow targets reuse `test/bridge/fixtures/protocol` and
`workflow-protocol`. In each directory:

- every `valid/` document must be accepted;
- every `invalid/` and `regressions/` document must be rejected.

New seeds need no code change. The two Rust replay-history valid seeds are
built at run time from `tests/support/replay_fixture.rs`, so a change to that
protobuf fixture cannot leave a stale base64 file behind. Targets with a
shared corpus whose decoders accept different documents (the OCaml control,
workflow, and operation-specific error targets) mutate the seeds they accept
and do not enforce the directory contract.

When a fix lands for a fuzz finding, add the minimized reproduction to the
target's `regressions/` directory.

## Mutation operators

Each generated case applies one operator to a valid seed. The first 16 (Rust)
or 15 (OCaml) cases visit every operator once; later cases choose at random.

| Operator | Effect |
| --- | --- |
| Truncation, deletion | Cut the document or remove up to 32 bytes |
| Byte replacement | Insert a structural byte (`{}[]",:\` and so on, NUL, DEL) |
| Invalid UTF-8 | Insert `FF`, an overlong `C0 80`, an encoded surrogate, or a lone continuation byte |
| Duplicate member | Repeat the first key with a `null` value |
| Oversized string | Replace a string with 65,536–65,538 bytes, straddling the text limit |
| Deep nesting | Nest arrays 127–130 levels deep, straddling the 128 limit; the OCaml harness also tries 20,000 levels to check the preflight scan |
| Numeric boundary | Replace a number with `-0`, `1e309`, 2^32, 2^63, −2^63−1, 2^64, `00`, and similar |
| Type confusion | Replace a string or number with `null`, `true`, `[]`, `{}`, `""`, `0`, or `[null]` |
| Hostile escape | Insert a lone surrogate, `\u0000`, a supplementary-plane pair, or a bidirectional override |
| Base64 corruption | Replace one character of a base64 string with `=`, `!`, `-`, `_`, space, `A`, or `/` |
| Inner bytes (Rust) | Decode a base64 field, flip a bit, insert an 11-byte varint or a huge length prefix, truncate, or duplicate a slice, then re-encode canonically. For replay histories this fuzzes Temporal's protobuf decoder and Core's history invariant gate |
| Crossover | Join a prefix of one seed to a suffix of another |
| Slice duplication | Repeat up to 64 bytes in place |
| Benign substitution | Replace a string with a plausible identifier or a number with a small integer, so semantic validation sees well-formed documents |
| Havoc | Stack two to four of the operators above |

## Client configuration and the network

A client configuration that passes validation reaches Core's `connect`. The
harness only sends such a document when its URL names `127.0.0.1:1`, the
refusing loopback port used by the seeds. A mutation that names another host
or port is counted as `skipped` and never sent, so the fuzz performs no DNS
lookup and no real connection. On Windows a refused connection costs about
two seconds of TCP retransmission, so every connect-reaching document is
skipped there and only rejections run.

## Budgets and reproduction

| Variable | Meaning | Default | Range |
| --- | --- | --- | --- |
| `DECODER_FUZZ_SEED` | Base seed, decimal or `0x` hexadecimal; each target derives its own stream from it | `0x0506d3c0` | any 64-bit value |
| `DECODER_FUZZ_CASES` | Generated cases per Rust target | 96 | 16–50,000 |
| `DECODER_FUZZ_OCAML_CASES` | Generated cases per OCaml target | 96 | 15–50,000 |
| `DECODER_FUZZ_TARGET` | Run only the named target | unset | a target name |
| `DECODER_FUZZ_REPLAY` | Run one retained Rust input, relative to the repository root | unset | a file of at most 128 KiB |

```sh
make test-decoder-fuzz            # Docker development container
make native-test-decoder-fuzz DECODER_FUZZ_CASES=20000 DECODER_FUZZ_OCAML_CASES=20000 DECODER_FUZZ_SEED=0x1234
```

The default budget runs all 20 Rust targets in under a second and all 29
OCaml targets in a few seconds. On an Apple M-series laptop 20,000 cases per
Rust target (360,000 inputs) took about 80 seconds. The largest per-call peak
seen in the Rust harness was about 0.74 MiB; the largest OCaml allocation for
one decode was about 25 MB, for a 65 KiB string (well under its allowance).

Before each Rust call the harness writes the exact input to
`_build/decoder-fuzz/active-<target>.bin` and removes it afterwards, so an
abort leaves the input behind. A property violation is reduced by bounded
chunk deletion and written to `_build/decoder-fuzz/<target>-<seed>-<case>.bin`.
The panic message gives the replay command, for example:

```sh
make native-test-decoder-fuzz DECODER_FUZZ_TARGET=client-start DECODER_FUZZ_REPLAY=_build/decoder-fuzz/client-start-000000000506d3c0-000042.bin
```

The OCaml harness writes a minimized `ocaml-<target>-<label>.bin` into its
working directory (`_build/default/test/bridge`, or `DECODER_FUZZ_OUTPUT_DIR`)
and prints the bytes as an escaped OCaml string. A corpus seed that is
classified against its directory is reported with the bridge's last
diagnostic. Rust producer CI keeps `_build/decoder-fuzz` as a 14-day artifact
when a job fails.

## Scheduled qualification

`.github/workflows/stress.yml` runs `make stress-qualification` every Monday at
03:30 UTC and on manual dispatch. It runs, in the Docker development
container on Linux amd64 with OCaml 5.5:

- the structured parser fuzz at 20,000 cases per parser;
- both decoder fuzzers at 20,000 cases per target;
- the lifecycle stress at 1,000 Rust cases and 2,000 OCaml cycles, and its
  concurrent live workers and clients at 200 rounds of 12 threads with up to
  96 operations per graph;
- the concurrent stress at 300 rounds of 16 threads with 4 graphs each, and
  300 OCaml rounds of 8 Domains.

The seed is the dispatch input, or the run number in hexadecimal, and is
written to the job summary. Every reproduction directory is kept as a 30-day
artifact when the job fails. The job has a 120-minute ceiling. The workflow
is not a required check and does not run on pull requests.

## What this does not prove

This is deterministic structured mutation fuzzing, not coverage-guided
libFuzzer or AFL fuzzing. The pinned stable toolchain has no sanitizer or
coverage instrumentation, so a memory error inside Rust or Core shows up only
as a panic, a wrong status, a hang, or an allocation-bound failure. The C stubs
and the OCaml runtime are not exercised by the Rust harness; the OCaml harness
calls the pure OCaml decoders without the C stubs. Inputs are capped at
128 KiB, so the 192 MiB document and 128 MiB payload ceilings are covered by
the fixed protocol tests, not by fuzzing. A clean finite run is evidence, not
proof of memory safety.

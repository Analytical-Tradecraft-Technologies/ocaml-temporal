# Structured bridge parser fuzzing

Issue [#521](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/521)
adds a bounded mutation harness for four independently callable Rust parsers:
the generic envelope, the canonical-base64 payload wrapper, workflow
activation, and workflow completion. The corpus selects committed valid and
invalid documents under `test/bridge/fixtures/{protocol,workflow-protocol}`;
the same seeds are used for the ordinary CI smoke and longer runs.

The harness first asserts valid seeds decode and known-invalid seeds reject.
It then makes reproducible truncation, deletion, byte replacement,
invalid-UTF-8, duplicate-key, base64 or identifier corruption,
oversized-string, and deep-nesting mutations. An accepted document must encode
and decode again. Each parser reports its accepted and rejected counts. A run
fails if either path is absent. It does not assume that every mutation must
reject; some are valid after mutation.

The ordinary `cargo test --manifest-path rust/Cargo.toml --locked` includes
128 generated cases per parser. `make test-parser-fuzz` runs just those targets
inside the repository's development container. For a longer bounded run with
the identical targets and corpus:

```sh
make test-parser-fuzz STRUCTURED_FUZZ_CASES=10000 STRUCTURED_FUZZ_SEED=0x521506a1
```

`make native-test-parser-fuzz` uses the host toolchain. Each input is capped at
96 KiB, and `STRUCTURED_FUZZ_CASES` must be 11–50,000 per parser so every
mutation operator runs at least once. The base64 operator selects a fixture
with encoded data; the envelope target corrupts its correlation ID instead.
An oversized mutation replaces the workflow `run_id` or payload `encoding`
field; the envelope parser receives an oversized string field. The harness
requires those mutations to reject. The envelope and workflow targets exercise
the 65,536-byte string boundary; the payload target rejects the invalid
encoding value after parsing it. Nested mutations exercise the 128-level
boundary. This avoids allocating a 192 MiB document or a 128 MiB payload in
every CI matrix cell. The fixed protocol tests separately assert those
configured transport ceilings. The Rust producer CI job has a 45-minute
timeout in addition to the per-target case cap.

Before every decode, the harness writes the exact bytes to
`_build/structured-fuzz/active-<parser>.bin`. It removes that file after a
normal result, leaving it for an abort. A Rust unwind or unexpected corpus
result writes a reproduction to `<parser>-<seed>-<case>.bin`; an unwind is also
reduced by bounded chunk deletion while preserving the panic. Rust producer
CI retains this directory as a 14-day artifact on failure. To replay a retained
file against the same parser, place it inside the checkout and run, for
example:

```sh
make native-test-parser-fuzz STRUCTURED_FUZZ_TARGET=fuzz_activation STRUCTURED_FUZZ_REPLAY=_build/structured-fuzz/activation-00000000521506a1-000042.bin
```

The replay variable accepts any file within the 96 KiB cap; the selected
target receives its exact bytes. Omit `STRUCTURED_FUZZ_TARGET` to run all four.
Record the tested commit, seed, case number, target, and retained file when
reporting a defect. Keep a minimized reproducer as a regression fixture after
fixing the defect.

This is **deterministic structured mutation fuzzing**, not coverage-guided
libFuzzer/AFL fuzzing. The normal Rust test profile provides debug assertions
and panic detection but no sanitizer or coverage instrumentation. It does not
instrument the C stubs, OCaml runtime, or the pinned Temporal Core dependency
as a whole. The separate `test/bridge/test_abi.sh` harness uses ASan/UBSan for
its C ABI scope. The broader lifecycle and long-run qualification remains
under [#506](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/506).

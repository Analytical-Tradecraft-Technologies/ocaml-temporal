# Repository guidance for coding agents

## Project purpose

This repository is building a community-maintained, unofficial Temporal SDK
for OCaml 5. It is an SDK with workflow workers and deterministic workflow
execution, not only a service client for starting workflows and retrieving
results. The intended public artifact is a reusable OCaml library suitable for
open-source distribution.

The final worker executable is owned and built by the OCaml application. The
public OCaml library calls private C stubs, which call this project's Rust
static library, which in turn uses the official Temporal Core implementation.
Keep Rust and its ownership model behind the private boundary; do not invert
the architecture by making Rust own the OCaml application.

## Architecture and API constraints

- Keep workflow authoring idiomatic, direct-style OCaml. Public helper
  functions should compose like ordinary OCaml functions, including helpers
  that wrap one or more activities or workflows.
- Implement the worker, replay, and essential Temporal command path before
  adding higher-level conveniences inspired by other SDKs. Feature parity is a
  behavioral target, not a requirement to translate another language's API.
  Study those SDKs after the supporting core capability is verified, then
  design the OCaml surface with modules, labelled arguments, variants,
  [result], higher-order functions, and other idiomatic OCaml tools. Reuse
  Temporal Core where it owns durable state-machine behavior; reimplement a
  language-layer feature in OCaml when that boundary is clearer and safer.
- Use private OCaml 5 algebraic effects to suspend direct-style workflow code.
  Do not expose effect constructors, continuations, Rust futures, Tokio, raw
  pointers, or Core implementation details in the public API.
- Represent expected operational failures with typed `result` values.
  Exceptions are reserved for programmer defects and violated internal
  invariants, not routine Temporal failures or control flow.
- Preserve Temporal determinism and replay safety. Workflow code must not
  perform nondeterministic I/O, wall-clock reads, randomness, or process-global
  mutation outside replay-safe SDK operations.
- One OCaml supervisor actor per SDK instance owns the complete Rust
  runtime/client/worker handle graph. It runs on a dedicated Domain, accepts
  typed mailbox messages from other Domains, and serializes lifecycle changes.
  Do not create one actor per native handle.
- Rust/Tokio owns network concurrency. It signals readiness through the native
  event mechanism; the supervisor waits through a C stub with the OCaml runtime
  lock released. Never block a workflow effect scheduler or let arbitrary Rust
  threads call OCaml closures.
- Treat memory and lifecycle correctness as more important than optimization.
  Every cross-language allocation and opaque handle must have one documented
  owner, one release path, panic containment, deterministic cleanup, and tests
  for error and shutdown paths. Optimize only after preserving those rules.
- Keep production code and tests in separate files. Rust integration tests
  belong under `rust/core-bridge/tests/`; OCaml tests belong under `test/`.

Read the design specification, implementation roadmap, runtime invariants, and
Core bridge ownership reference under `docs/` before changing the corresponding
subsystem. Record material design decisions and verified progress there as the
implementation evolves.

## Documentation and comments

Write source documentation for every function, method, class or module, and
data structure, including internal code and test helpers. The detail should be
proportional to the construct, but sufficient for a contributor encountering
the project for the first time to understand its purpose and contract.

Useful documentation explains intent and context that the implementation alone
does not make obvious. As applicable, document invariants, ownership and
lifetime rules, threading or Domain assumptions, determinism requirements,
failure behavior, protocol semantics, units, ordering guarantees, and why a
particular approach is necessary. Within a function, add focused comments where
non-obvious control flow, safety reasoning, cleanup ordering, or boundary
conversion would otherwise slow down review.

Do not add comments that merely restate identifiers, types, or individual
statements in prose. Keep comments synchronized with behavior when code changes;
stale commentary is a correctness defect, especially at the OCaml/C/Rust
boundary.

## Dependencies and licensing

- Project code is Apache-2.0. New dependencies must use permissive licenses
  such as Apache-2.0, MIT, or BSD and must pass the complete locked dependency
  audit. Copyleft, source-available, missing, unknown, or ambiguous dependency
  licenses are not acceptable.
- Respect the narrowly documented OCaml linking-exception treatment in
  `docs/dependencies.md`; do not generalize it to unrelated packages.
- Pin Temporal Core by immutable Git commit and preserve the locked Cargo
  graph. Upgrades require updated license, ABI, replay, and compatibility
  evidence.
- Keep the Cargo license scanner in its standalone GitHub Actions job. If
  Python is needed for CI-only tooling, use the pinned official Python
  container rather than installing Python into every development image.

## Build, test, and delivery workflow

- Makefile targets are the supported command interface. Docker Compose is the
  default local environment; it currently avoids requiring host OCaml tooling.
  The native `make native-verify` target exists for Windows and macOS
  compatibility jobs.
- Before committing a meaningful milestone, run the relevant focused tests and
  the broadest practical Makefile verification locally. Do not spend time
  reproducing every OCaml version locally: validate one representative version,
  push, and use the GitHub Actions matrix as the cross-version/platform gate.
- Keep the dependency-license audit as one independent CI job rather than
  repeating it for every OCaml version. Master CI covers OCaml 5.2 through 5.5
  on Linux amd64/arm64 and 5.5 on Windows x64/macOS ARM; releases cover all
  four compiler series on all four platforms. PRs use the smaller matrix in
  `scripts/ci-matrix.py`. Rust producers are shared across compiler versions.
- End-to-end Temporal tests must eventually use Docker Compose with Temporal
  Server and PostgreSQL. Native Windows/macOS jobs test the library and bridge
  directly and should not run a Linux Docker Compose stack.
- Use GitHub Dependabot for Cargo dependencies only; Dependabot does not support
  OPAM dependency updates.
- Keep CI and local checks efficient and high-signal. Add scanners only when
  they are maintained, permissively licensed, and provide value beyond existing
  compiler, test, formatting, and lint gates.
- Immediately before merging a pull request, refresh its conversation comments,
  reviews, and unresolved review threads. Address and resolve actionable
  feedback, and rerun applicable CI after code changes.

## AI disclosure

If an agent changes any source code and its model name and version are not
already listed under `AI disclosure` in `README.md`, it must add them there.
List only the model name and version; do not include reasoning or thinking
levels.

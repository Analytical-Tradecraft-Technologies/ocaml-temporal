# Contributing

Thank you for helping build this community-maintained, unofficial Temporal
SDK for OCaml 5. This guide is the human entry point: how to build, test and
submit a change, and what reviewers check. The binding engineering rules are
in [`AGENTS.md`](AGENTS.md) (mirrored in `CLAUDE.md`); this guide summarizes
them but does not replace them.

> **Status: draft pending maintainer adoption (#1140).** The workflow below
> describes how the repository already works. The
> [code of conduct](CODE_OF_CONDUCT.md) and the security reporting route
> (#509) take effect only once the maintainer adopts them.

By participating you agree to follow the [code of conduct](CODE_OF_CONDUCT.md).
Do not report security vulnerabilities in public issues; see
[Reporting a security problem](#reporting-a-security-problem).

## Before you start

- **Read the architecture first.** The public OCaml library calls private C
  stubs, which call this project's Rust static library, which uses the
  official Temporal Core. The OCaml application owns the worker executable.
  Start with the [documentation guide](docs/README.md), the
  [architecture specification](docs/design/sdk-architecture.md), the
  [runtime invariants](docs/reference/runtime-invariants.md) and the
  [Core bridge reference](docs/reference/core-bridge.md) before changing the
  corresponding subsystem.
- **Agree on scope for features.** For a new public API, a new Temporal
  feature or a cross-language change, open or comment on an issue before
  writing code. Large features get a design note under `docs/design/` (or a
  decision record under `docs/decisions/`) before implementation. Feature
  parity with other SDKs is a behavioral target, not a request to translate
  their API: the OCaml surface uses modules, labelled arguments, variants,
  `result` and ordinary higher-order functions.
- **Look for open work.** Issues labelled `good first issue` or
  `help wanted` are good starting points. Issues in the
  [v1 MVP milestone](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/milestone/1)
  carry a `priority: P1`/`P2`/`P3` label and a **Dependencies and
  implementation order** section; work an issue only after the issues it is
  **Blocked by** are closed. `post-mvp1` marks backlog for after the first
  release.

## Development environment

Docker with Compose v2 and GNU Make are the only host requirements for the
default path; the Makefile is the supported command interface. Give the Docker
VM at least 8 GiB of memory and 20 GB of free disk (see the
[README quick start](README.md#quick-start)).

```sh
make build             # OCaml library and the pinned Rust bridge
make test-unit         # focused OCaml tests
make test-runtime      # deterministic runtime tests
make test-rust         # Rust bridge tests
make verify            # version check, lint and every Dune/Rust/bridge test
make quality           # pinned cargo-deny, cargo-machete and typos (host tools)
make license-check     # locked dependency license audit
```

Native development on macOS or Windows uses `make native-verify` with a local
OCaml 5.2–5.5 switch and the Rust toolchain from `rust/rust-toolchain.toml`.
Live tests against a real Temporal Server and PostgreSQL run through Docker
Compose targets such as `make test-temporal-integration`; see the
[local stack reference](docs/reference/local-temporal-stack.md).

Before you push a meaningful change, run the focused tests for what you
touched and then the broadest practical gate (normally `make verify`). You do
not need every OCaml version locally: validate one, and let the CI matrix
cover the other compiler versions and platforms. The
[quality gates reference](docs/reference/quality-gates.md) lists every CI job
and its local equivalent.

## Engineering rules reviewers check

- **Determinism and replay safety.** Workflow code must not perform
  nondeterministic I/O, wall-clock reads, randomness or process-global
  mutation outside replay-safe SDK operations. Changes to workflow execution
  need replay coverage.
- **Typed failures.** Expected operational failures are typed `result` values.
  Exceptions are reserved for programmer defects and violated invariants.
- **Private boundary.** Effect constructors, continuations, Rust futures,
  Tokio, raw pointers and Core details never appear in the public API.
- **Ownership.** Every cross-language allocation and opaque handle has one
  documented owner, one release path, panic containment, deterministic cleanup
  and tests for its error and shutdown paths.
- **Tests in separate files.** OCaml tests live under `test/`; Rust
  integration tests live under `rust/core-bridge/tests/`.
- **Documentation on everything.** Every function, module and data structure,
  including internal helpers and test helpers, has a doc comment that explains
  intent, contracts, ownership, threading or determinism assumptions as
  applicable. Do not restate the code. Keep comments in sync with behavior.
  Update the relevant reference under `docs/` and add a dated entry to
  [`docs/progress.md`](docs/progress.md) for verified milestones.

## Dependencies and licensing

Project code is [Apache-2.0](LICENSE). A new dependency must use a permissive
license (Apache-2.0, MIT, BSD or similar) and pass the locked dependency
audit; copyleft, source-available, missing or ambiguous licenses are not
accepted. The one narrow exception for the OCaml linking exception is
documented in the [dependency inventory](docs/dependencies.md), which also
records every dependency. Temporal Core is pinned to an immutable commit;
upgrading it needs license, ABI, replay and compatibility evidence.

## AI-assisted contributions

AI tools may be used. If an AI model changed source code in your contribution
and its name and version are not yet listed under
[AI disclosure](README.md#ai-disclosure) in `README.md`, add it there in the
same pull request (model name and version only). You remain responsible for
reviewing, testing and understanding everything you submit.

## Commits and pull requests

- Branch from an up-to-date `master` and keep one logical change per pull
  request. Pull requests that change the same append-only files (for example
  `docs/progress.md` or a Dune `(names ...)` list) conflict easily; rebase or
  merge `master` when that happens.
- Commit subjects follow the existing style: `type(scope): summary`, for
  example `fix(bridge): ...`, `feat(client): ...`, `docs: ...`,
  `test(live): ...`, `chore(deps): ...`.
- Fill in the [pull request template](.github/pull_request_template.md): what
  changed, which issue it fixes (`Fixes #N` only when every requirement is
  met, otherwise `Refs #N`), and exactly which tests you ran.
- Pull requests merge through the **merge queue** with squash merges once the
  required checks pass. Required CI takes roughly half an hour; the queue
  re-runs it on the merged result.
- Address every review comment, including automated reviewers (Codex,
  Datadog), by fixing it or replying with a reasoned rebuttal, and resolve the
  review threads; the ruleset requires resolved threads.
- Changes under `.github/` and to `LICENSE` need the code owner's approval
  (see `.github/CODEOWNERS`).

## Reporting bugs and requesting features

Use the [issue templates](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/new/choose).
A useful bug report names the SDK version or commit, the OCaml version, the
platform, the Temporal Server version, the `make` target or program that
reproduces it, the exact workflow and run IDs, and logs with payloads
redacted.

## Reporting a security problem

Do not open a public issue for a suspected vulnerability. Until the security
policy from #509 is adopted, use GitHub's private vulnerability reporting on
this repository's **Security** tab if it is enabled, or contact the maintainer
listed in `.github/CODEOWNERS` privately.

## License of contributions

Unless you state otherwise, any contribution you submit for inclusion is
licensed under the [Apache License 2.0](LICENSE), as described in its
section 5, without additional terms.

# Quality and security gates

The normal `make verify` path already treats OCaml compiler warnings, Rust
Clippy warnings, rustfmt drift, repository formatting errors, and test failures
as errors. The independent quality gate adds three checks that find different
defect classes without repeating that work in every compiler and architecture
job.

## Running the gate

Install these exact native binaries on the development host:

```text
cargo-deny 0.20.2
cargo-machete 0.9.2
typos 1.48.0
```

Then run:

```sh
make quality
```

`make quality-tool-version-check` fails before scanning when any executable is
missing or has a different version. The subordinate `make quality-rust` and
`make quality-spelling` targets are useful for focused local diagnosis but
retain the same version gate. These checks are native because their upstream
projects publish small binaries for supported development platforms; they are
not SDK dependencies and are intentionally absent from the Compose builder.

Every third-party action reference checked into the build workflows uses an
immutable full commit SHA. GitHub-owned `actions/*` actions may use readable
major-version references because the effective Actions policy admits them
separately. The effective policy can be enforced at repository, organization,
or enterprise level; this source contract neither identifies nor changes that
external enforcement point. Non-GitHub actions must also be admitted there by
their exact owner, repository, and commit SHA. Consequently, pinning the
workflow source is necessary but cannot by itself restore a workflow rejected
at startup. Hosted evidence requires both nonzero job creation for the current
commit and successful jobs after the effective policy is updated.

In particular,
[`taiki-e/install-action`](https://github.com/taiki-e/install-action) is pinned
with checksum validation enabled and fallback installation disabled, and the
native lanes pin
[`ocaml/setup-ocaml`](https://github.com/ocaml/setup-ocaml) rather than following
its mutable `v3` reference. The quality action installs the same exact tool
versions and then invokes `make quality`. The job runs once for each code pull
request and once for each push to `master` or scheduled run; it is not repeated
for every OCaml matrix cell. Path filtering skips it for documentation-only
pull requests, while the standalone license-audit job remains independent.

## CI matrix policy

Pull requests use a representative compatibility sample: Linux amd64 on OCaml
5.2 and 5.5, Linux arm64 and macOS ARM64 on OCaml 5.5, and one live
Temporal/PostgreSQL smoke on Linux amd64 with OCaml 5.5. The Windows x64 OCaml
5.5 job is added before merge when changes affect the native bridge,
build/toolchain, workflow, or composite-action configuration. This keeps the
oldest supported compiler, current compiler, both Linux architectures, and a
non-Linux native link in the normal PR gate while avoiding the redundant
intermediate Linux combinations.

Pushes to `master` and scheduled runs remain the exhaustive compatibility
gate: OCaml 5.2–5.5 on both Linux architectures plus both OCaml 5.5 native
desktop jobs. A 5.3 or 5.4 regression that does not affect the representative
lanes is therefore caught immediately after merge by the subsequent master run;
the reduced PR matrix is not presented as a replacement for that evidence.

Markdown and legal-documentation-only pull requests run the independent
license audit only. JSON schemas under `docs/schemas/` are protocol contracts
and therefore take the code path despite their documentation location. A pull
request that changes only the live acceptance fixture under
`test/integration/temporal/` runs the license audit and live smoke without the
representative matrix; the smoke is the direct verification for that fixture.

## CI jobs and local equivalents

The workflow has separate jobs because the checks have different toolchain and
runtime requirements. The Makefile command for each job is shown here so a
queued Actions run does not make the local verification boundary ambiguous:

| CI job | Workflow command | Local command | What the local result proves |
| --- | --- | --- | --- |
| `verify` | `make verify OCAML_VERSION=<matrix version>` | `make verify OCAML_VERSION=5.2` (or another locally available image) | Docker-backed OCaml build/lint, Rust tests, bridge/install tests, and repository quality contracts. PRs use the representative cells; master and scheduled runs use the exhaustive matrix. |
| `quality` | `make quality` | `make quality` | The pinned native `cargo-deny`, `cargo-machete`, and `typos` scans. The exact binaries must be installed on the host. |
| `license-audit` | `make license-check OCAML_VERSION=5.2`, plus the two isolated Python Cargo-license checks | `make license-check OCAML_VERSION=5.2` | The package/OCaml dependency license policy. The locked Cargo license scanner remains a single CI-only step and is not repeated in the OCaml matrix. |
| `native` (master) / `native-macos`, `native-windows` (PR) | `make native-verify` | `make native-verify` on a matching native host | The OCaml 5.5 and Rust native link, format, lint, install, and test path. macOS ARM64 runs for every code PR; Windows x64 runs for native-boundary PRs and unconditionally on master/scheduled runs. |
| `temporal-integration` | `make test-temporal-integration`, `make test-temporal-worker-restart`, `make test-temporal-worker-crash-recovery`, `make test-temporal-worker-cache-eviction`, `make test-temporal-workflow-patching`, `make test-temporal-parent-child-restart`, and `make test-temporal-parent-child-failure-replay` | Run the corresponding target when Docker and network access are available | Seven sequential live controllers reuse one job's checkout/build cache while each owns a fresh Temporal/PostgreSQL lifecycle. The 45-minute CI ceiling includes the bilateral parent/child recovery gates; contract-only results are not live evidence. |

`make check OCAML_VERSION=5.2` is a convenient Docker-backed local baseline:
it combines `make verify` and `make license-check`. It does not run
`make quality`, the native compatibility path, or the real-server smoke; run
those separately when their respective evidence is needed.

## When Actions remain queued

A check whose GitHub Actions status remains `queued` has not run, so it is not
verification and must not be treated as a pass. If repository quota or runner
availability leaves a check queued indefinitely, use the documented local
gates as interim evidence. For a normal Docker-backed checkout, the closest
non-live local baseline is:

```sh
make check OCAML_VERSION=5.2
make quality                 # requires the exact pinned host binaries
```

On Windows or macOS, also run `make native-verify` to exercise the native
compatibility path. Run `make test-temporal-integration` only when a live
Temporal Server/PostgreSQL result is specifically required. These commands
exercise the available local gates without weakening required CI or turning an
unexecuted matrix, platform, or live-server job green; required GitHub checks
still need to complete successfully when Actions is available again.

## Rust dependency advisories and sources

[`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny) checks the complete
locked, all-feature Cargo graph against the current RustSec advisory database.
Security advisories fail by default. Unmaintained transitive crates do not
fail the gate because this project cannot replace dependencies inside the
immutable Temporal Core graph; an unmaintained direct workspace dependency
does fail. Unsound advisories fail when they affect a direct workspace
dependency.

The source policy in `deny.toml` admits crates.io and the exact Temporal Core
repository only. Every Git dependency must use a `rev` specification, so a
new dependency cannot follow a mutable branch or tag. The Cargo manifest and
lockfile continue to pin Temporal Core's precise commit; the source gate is an
additional structural safeguard rather than a replacement for that invariant.

Cargo-deny's licence mode is not run. The repository-owned scanner understands
the six project-reviewed Temporal workspace packages and is intentionally the
only Cargo licence gate.

## Unused Rust dependencies

[`cargo-machete`](https://github.com/bnjbvr/cargo-machete) scans workspace
manifests and source references for direct dependencies that no longer appear
to be used. The gate enables Cargo metadata to handle renamed dependencies and
workspace inheritance accurately. The tool is deliberately approximate, so
any future false-positive exception must name the dependency and explain the
generated-code or build-time use in Cargo metadata; blanket ignores are not
acceptable.

## Cross-language spelling

[`typos`](https://github.com/crate-ci/typos) scans OCaml, Rust, C, shell,
Markdown, JSON, YAML, and TOML together. This catches mistakes in the extensive
API and ownership documentation that compiler-only checks cannot see. Add a
narrow dictionary exception only for an intentional project term or external
proper name; do not suppress a file or directory merely to silence a genuine
documentation defect.

## Evaluated OCaml alternatives

No separate OCaml semantic linter was added. The maintained compiler and Dune
checks already cover type errors, exhaustiveness, unused declarations, and
configured warnings. The OCaml Platform's
[`odoc`](https://github.com/ocaml/odoc) would add valuable documentation-link
validation, but version 3.2.1 has an ordinary `tyxml` dependency licensed under
LGPL with the OCaml linking exception. That dependency is outside this
repository's exact approved exceptions, so adding odoc would violate policy.
The decision can be revisited if a future permissive dependency closure is
available.

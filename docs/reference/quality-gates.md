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
request or merge group and once per scheduled run; it is not repeated for
every OCaml matrix cell. Documentation-only changes skip the code jobs,
including the standalone license audit.

## CI matrix policy

PRs and merge groups use a representative compatibility sample: Linux amd64
on OCaml 5.2 and 5.5, Linux arm64, macOS ARM64, and Windows x64 on OCaml 5.5,
and a live Temporal/PostgreSQL smoke on Linux amd64 with OCaml 5.5.
All these jobs and the quality/license jobs share a single changed-code gate.

Markdown and legal-documentation-only changes skip the code jobs. GitHub
workflows, composite actions, and protocol schemas under `docs/schemas/`
always count as code. Other paths, including unknown inputs and live test
fixtures, also trigger all code jobs. The required path-detection job always
runs; a failed diff fails that job. PRs compare against their merge base to
exclude unrelated master changes, and merge groups include every queued
change relative to the group's base. Renames include both old and new paths.
The separate release metadata/SBOM workflow still runs without compiling code.

Scheduled runs retain the exhaustive OCaml 5.2–5.5 Linux matrix plus both
native platforms. Intermediate-compiler regressions outside the PR sample
are caught by the next scheduled run. Pushes to `master` do not run CI.

## Rust artifact sharing

The free native GitHub runners produce four independent Rust bundles. Linux
uses a pinned Rust image on Debian 12, matching the C-library baseline of all
OCaml development images; the amd64 and arm64 builds run on their matching
native runners. Windows retains the GNU/MinGW target, and macOS retains ARM64.
There is no cross compilation or paid runner requirement.

| Bundle | Producer | Consumers in the same run |
| --- | --- | --- |
| Linux amd64 | `rust-amd64`, `ubuntu-24.04` | All Linux amd64 OCaml lanes and the live Temporal smoke |
| Linux arm64 | `rust-arm64`, `ubuntu-24.04-arm` | All Linux arm64 OCaml lanes |
| macOS ARM64 | Existing native macOS job, `macos-15` | That job's OCaml/C build and tests |
| Windows x64 GNU | Existing native Windows job, `windows-latest` | That job's OCaml/C build and tests |

On a cache miss the producer checks the pinned toolchain, Rust formatting,
Clippy with warnings denied, and the complete locked Rust test suite before
packaging its static library, dynamic library, and native link metadata.
Windows additionally bundles the required MinGW import archives; the consumer
regenerates their search path after relocation. Producers disable incremental
state and use line-table debug information, retaining file/line backtraces,
debug assertions, and the development optimization level while reducing
artifact size. Normal local source builds keep their existing Cargo profiles.

Only the finished bundle is cached, never Cargo's registry or target tree.
Keys include platform, the complete Rust source/header/test/lockfile tree,
bridge build and validation scripts, Makefile, workflows, and image definitions.
Native desktop keys additionally include the runner image version, actual Rust
compiler, C compiler, and Protocol Buffers compiler. There are no fallback
keys. Every consumer checks the expected key, platform, required files, and
SHA-256 checksums before linking. Explicit prebuilt mode fails if the bundle is
invalid; it never falls back to compiling Rust. Dune tracks the bundle path and
key as environment dependencies; bundles are immutable for the lifetime of a
build directory.

The existing required OCaml and live-smoke checks explicitly fail when their
Rust producers fail. Their job conditions override GitHub's default dependency
skipping, because a skipped required job would otherwise count as successful.
Documentation-only PRs retain the existing path-based skips.

Only successful Rust producers on scheduled `master` runs save shared
caches. PR and merge-queue jobs can restore these default-branch caches without
write credentials. OCaml-only changes therefore reuse the same Rust bundle
across PR, queue, and scheduled runs even though their commit IDs differ. A change to
Rust or its build inputs needs a fresh producer on each event until a scheduled run
seeds the new key. This deliberately does not promote PR artifacts into
master: GitHub scopes PR caches to their merge ref. Cache eviction or a new
native runner image also causes a normal rebuild. Live dependency-advisory and
license checks continue independently on every applicable workflow run.

Linux producers upload their bundles as one-day Actions artifacts for fanout;
native desktop jobs consume their bundles in place and avoid another upload.
Each OCaml lane still compiles the private C stubs, examples, OCaml libraries,
and tests and runs the installation/public API checks. Linux also retains the
sanitized C ABI harness. Every live worker/driver inherits the same bundle,
including controllers with separate Dune build directories. PR path filters,
existing check names, the exhaustive scheduled matrix, and the seven live smoke
controllers remain unchanged. A cold scheduled run compiles Rust four times
instead of eleven; cache hits eliminate those Rust compilations too. Artifact
transfer, OCaml/C compilation, linking, and tests still consume runner time.

For local Linux production and consumption on the same architecture:

```sh
make rust-bridge RUST_BRIDGE_KEY=local-validation
TEMPORAL_RUST_BRIDGE_DIR=/workspace/_build/rust-bridge \
TEMPORAL_RUST_BRIDGE_KEY=local-validation make verify OCAML_VERSION=5.2
```

`make native-rust-bridge RUST_BRIDGE_KEY=local-validation` is the equivalent
producer on a configured native host. Set `TEMPORAL_RUST_BRIDGE_DIR` to the
absolute host bundle path when invoking `make native-verify`. The local key is
caller-owned: use the same source checkout and never reuse it after changing
Rust inputs. CI calculates its keys automatically. Without these environment
variables, `make verify` and `make native-verify` retain the full source-build
and Rust-test path. The focused `make test-quality-contract` gate includes
artifact relocation, missing/corrupted bundle, and key/platform rejection
tests with Cargo unavailable to consumers.

## CI jobs and local equivalents

The workflow has separate jobs because the checks have different toolchain and
runtime requirements. The Makefile command for each job is shown here so a
queued Actions run does not make the local verification boundary ambiguous:

| CI job | Workflow command | Local command | What the local result proves |
| --- | --- | --- | --- |
| `verify` | `make verify OCAML_VERSION=<matrix version>` with the verified bridge bundle | `make verify OCAML_VERSION=5.2` (or another locally available image) | Docker-backed OCaml build/lint, bridge/install tests, and repository quality contracts. CI gets Rust validation from its producer; the default local command also runs Rust tests. PRs use the representative cells; scheduled runs use the exhaustive matrix. |
| `quality` | `make quality` | `make quality` | The pinned native `cargo-deny`, `cargo-machete`, and `typos` scans. The exact binaries must be installed on the host. |
| `license-audit` | `make license-check OCAML_VERSION=5.2`, plus the two isolated Python Cargo-license checks | `make license-check OCAML_VERSION=5.2` | The package/OCaml dependency license policy. The locked Cargo license scanner remains a single CI-only step and is not repeated in the OCaml matrix. |
| `native` (scheduled) / `native-macos`, `native-windows` (PR) | `make native-verify` | `make native-verify` on a matching native host | The OCaml 5.5 and Rust native link, format, lint, install, and test path. macOS ARM64 runs for every code PR; Windows x64 uses the same code gate; scheduled runs cover both platforms. |
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

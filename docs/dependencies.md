# Dependency and License Inventory

All project and build dependencies are checked before a milestone commit.
`make license-check` reads `temporal-sdk.opam.locked` and the per-compiler
replacements in `scripts/opam-lock-overrides.txt`, asks OPAM for each package's
exact license metadata, and rejects missing or unapproved values. The
standalone GitHub Actions license job streams locked metadata emitted by
`make cargo-metadata` into the repository scanner running in a separate
official Python container. `make cargo-metadata` only emits the locked Cargo
metadata; it does not run the scanner. Cargo license scanning deliberately
does not run in the compiler/architecture matrix or as a Makefile target.
The Makefile's `quality` target is a separate contributor/CI gate for RustSec,
unused-dependency, and spelling checks.

## Keeping this inventory current

The tables in this document are compared with the files that actually pin each
dependency by `make check-dependency-inventory`
(`scripts/check-dependency-inventory.sh`). `make license-check` runs it first,
so the standalone license job, and `make check` locally, fail when a lock file,
image digest, action pin, or tool version changes without the matching row
here. It uses only POSIX tools and needs neither OPAM, Cargo, nor Docker.

| Inventory table | Compared with |
|---|---|
| [Locked OCaml closure](#locked-ocaml-closure) | Every package and version in `temporal-sdk.opam.locked`, plus the `temporal-sdk.opam` version |
| [Per-compiler lock overrides](#per-compiler-lock-overrides) | `scripts/opam-lock-overrides.txt` |
| [Linking-exception scope](#linking-exception-scope) | The locked versions of `ocaml`, `ocaml-base-compiler`, and `ocamlbuild` in `temporal-sdk.opam.locked`, and of `tyxml`, `re`, and `camlp-streams` in `scripts/docs-tools.locked` |
| [CI-only documentation tooling](#ci-only-documentation-tooling) | `scripts/docs-tools.locked` |
| [Builder image tooling](#builder-image-tooling), [toolchain and CI images](#toolchain-and-ci-images), and [local integration service images](#local-integration-service-images) | Every digest-pinned image reference in `Dockerfile.dev`, `Dockerfile.rust-ci`, `test/integration/temporal/compose.yaml`, and the GitHub workflows and composite actions |
| [Locked Cargo closure](#locked-cargo-closure) | The `[[package]]` count in `rust/Cargo.lock` and the Temporal Core revision in `rust/Cargo.toml` |
| [Direct Rust dependencies](#direct-rust-dependencies) | Every crate in `[workspace.dependencies]` in `rust/Cargo.toml` or declared by the bridge (inherited or directly), every locked version of each crate in `rust/Cargo.lock`, and the bridge's normal and dev dependency sections; any other bridge dependency table fails the check |
| [CI-only quality tools](#ci-only-quality-tools) | The `taiki-e/install-action` tool list and the Makefile `QUALITY_*_VERSION` defaults |
| [GitHub Actions](#github-actions) | Every external `uses:` reference, its pinned ref, and its release comment |

The Rust toolchain version stated below is also compared with
`rust/rust-toolchain.toml`. The checker compares names, versions, and pins; it
does not re-derive license columns. Those are reviewed from each package's own
metadata (`opam show <package> -f license`, `cargo metadata`, or the upstream
repository) when a row is added or changed, and the OPAM and Cargo license
gates above remain the enforcement for everything the SDK builds or ships.
`make test-dependency-inventory-contract` proves the checker rejects drift in
each compared source.

## Policy

For OPAM packages, the exact accepted license values are MIT, Apache-2.0,
BSD-2-Clause, BSD-3-Clause, ISC, Zlib, and PostgreSQL. An OCaml compiler/runtime
package may use
`LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception`. Other copyleft,
source-available, non-commercial, missing, and unknown terms are rejected.

`ocamlbuild.0.16.1` is an exact build-only exception for
`LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception`. It enters the locked
closure solely because `logs.0.10.0` uses it to build; neither ocamlbuild nor
its code is linked into or redistributed with the SDK. Other ocamlbuild
versions and ordinary LGPL packages remain prohibited.

`ocaml-options-vanilla.1` is an exact reviewed exception for OPAM's historical
`CC0-1.0+` metadata. CC0 is permissive; the exception is package- and
version-specific rather than a general substring match.

The `base-*` entries below are virtual packages shipped by the OCaml compiler
distribution. Their OPAM records contain no independent source or license, so
the checker recognizes only these exact package names at version `base` and
attributes them to the reviewed compiler distribution.

## Locked OCaml closure

Roles: *runtime* packages are linked into an application that uses the SDK;
*test-only* packages are declared `with-test` and used only by the SDK's own
tests; *build* packages run while building the SDK or a dependency and are not
linked; *system check* packages only verify that an external tool is present;
*compiler* packages are provided by the pinned image or `setup-ocaml`.

| Package | Exact version | License | Role | Linked into release | Redistributed | Review note |
|---|---:|---|---|---|---|---|
| temporal-sdk | 0.1.0~rc.1 | Apache-2.0 | project | yes | yes | Project source and binary |
| logs | 0.10.0 | ISC | runtime | yes | no | Maintained application-configurable logging infrastructure; the SDK installs no reporter |
| yojson | 3.0.0 | BSD-3-Clause | runtime | yes | no | Implements the optional cross-language `json/plain` codec; Temporal itself does not require JSON |
| mtime | 2.1.0 | ISC | test-only | no | no | Monotonic clock for the repository benchmark harness (`with-test`); not a library dependency |
| dune | 3.24.2 | MIT | build | no | no | Build system only |
| ocamlbuild | 0.16.1 | LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception | build | no | no | Exact reviewed build-only linking-exception dependency of `logs` |
| ocamlfind | 1.9.8 | MIT | build | no | no | Build-time library discovery required by `topkg` |
| topkg | 1.1.1 | ISC | build | no | no | Build-time packaging tool required by `logs` and `mtime` |
| conf-protoc | 4.4.0 | BSD-3-Clause | system check | no | no | Runs `protoc --version`; the Protocol Buffers compiler builds Temporal Core's generated crates |
| conf-rust-2024 | 1 | MIT | system check | no | no | Checks for a Rust toolchain that supports the 2024 edition used by the bridge |
| ocaml | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler/runtime | yes | no | Approved OCaml linking exception |
| ocaml-base-compiler | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler | no | no | Approved OCaml linking exception |
| ocaml-config | 3 | ISC | compiler | no | no | Compiler configuration package |
| ocaml-options-vanilla | 1 | CC0-1.0+ | compiler | no | no | Exact reviewed permissive metadata exception |
| base-bigarray | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-domains | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-nnp | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-threads | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-unix | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |

The lock is solved for OCaml 5.2.1; the compiler rows show that solve, while
the other supported series use their own pinned compiler packages (see
[Installing the locked OPAM closure](#installing-the-locked-opam-closure)).

Protocol conformance tests use a small repository-owned standard-library
harness. Alcotest itself is ISC, but its complete OPAM test closure includes
ordinary LGPL packages outside the compiler/runtime exception allowed by this
project, so it is intentionally neither used nor declared as a package test
dependency.

## Linking-exception scope

The OCaml linking exception is accepted only in the exact places below. Each
row is either part of the compiler itself, never linked, or confined to a
disposable CI image; none of them extends to any other package.

| Package | Exact version | License | Scope | Enforced by | Rationale |
|---|---:|---|---|---|---|
| ocaml | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler/runtime | `scripts/check-licenses.sh` exact package name | The OCaml compiler and runtime every OCaml program links |
| ocaml-base-compiler | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler | `scripts/check-licenses.sh` exact package name | The compiler distribution that provides `ocaml` |
| ocamlbuild | 0.16.1 | LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception | build only | `scripts/check-licenses.sh` exact name and version | Required to build `logs.0.10.0`; not linked or redistributed |
| tyxml | 4.6.0 | LGPL-2.1-only WITH OCaml-LGPL-linking-exception | CI-only documentation tool | `scripts/docs-tools.locked` and the `docs` image stage | Part of the odoc closure; renders HTML in a disposable CI image |
| re | 1.14.0 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | CI-only documentation tool | `scripts/docs-tools.locked` and the `docs` image stage | Part of the odoc closure; renders HTML in a disposable CI image |
| camlp-streams | 5.0.1 | LGPL-2.1-only WITH OCaml-LGPL-linking-exception | CI-only documentation tool | `scripts/docs-tools.locked` and the `docs` image stage | Part of the odoc closure; renders HTML in a disposable CI image |

`make license-check` accepts only the exact compiler/runtime names and the
exact ocamlbuild version hard-coded by the policy; adding an SDK row here also
requires a matching exact-name and version checker change. The three CI-only
rows are not admitted by `make license-check` at all: they are never SDK
dependencies, and the reasoning that permits them as documentation tools must
not be cited to admit them, or any other LGPL package, into the SDK closure.

The compiler rows show the OCaml 5.2.1 solve of the lock; every supported
series uses its own compiler under the same exception, and
`scripts/check-licenses.sh` also names `ocaml-compiler-libs` should it enter
the lock. `make check-dependency-inventory` compares each row's version with
`temporal-sdk.opam.locked` (compiler and ocamlbuild) or
`scripts/docs-tools.locked` (the CI-only rows), so upgrading any of these
packages fails until this table is reviewed.

## CI-only documentation tooling

`make docs` renders the API documentation with odoc in the `docs` stage of
`Dockerfile.dev`. That stage installs the exact closure in
`scripts/docs-tools.locked` on top of the locked SDK closure and fails if opam
installs any package that the manifest does not list. None of these packages
is an SDK dependency: they are absent from `temporal-sdk.opam` and its lock,
are not linked into or redistributed with any SDK artifact, and are not
needed to build or install the SDK. `make license-check` therefore does not
audit them; changes to the tool lock must update this table instead, which
`make check-dependency-inventory` enforces. The reasoning, and the limit that
it must not be generalized to SDK dependencies, is recorded in
[quality and security gates](reference/quality-gates.md#rendered-api-documentation).
Packages the docs stage reuses from the SDK lock (`dune`, `ocamlfind`,
`ocamlbuild`, and `topkg`) are listed only in the locked OCaml closure.

| Package | Exact version | License | Role in the odoc closure |
|---|---:|---|---|
| odoc | 3.2.1 | ISC | Documentation compiler and HTML renderer |
| odoc-parser | 3.2.1 | ISC | Doc-comment parser used by odoc |
| astring | 0.8.5 | ISC | String utilities used by odoc |
| cmdliner | 2.1.1 | ISC | Command-line parsing for odoc |
| cppo | 1.8.0 | BSD-3-Clause | Build-time preprocessor |
| crunch | 4.0.0 | ISC | Embeds odoc's HTML support files at build time |
| fmt | 0.11.0 | ISC | Formatting utilities |
| fpath | 0.7.3 | ISC | File-path utilities |
| ptime | 1.2.0 | ISC | Time values used by crunch |
| uutf | 1.0.4 | ISC | UTF-8 handling used by tyxml |
| seq | base | compiler virtual package | No independent source; part of OCaml distribution |
| tyxml | 4.6.0 | LGPL-2.1-only WITH OCaml-LGPL-linking-exception | HTML generation (see [linking-exception scope](#linking-exception-scope)) |
| re | 1.14.0 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | Regular expressions used by tyxml (see [linking-exception scope](#linking-exception-scope)) |
| camlp-streams | 5.0.1 | LGPL-2.1-only WITH OCaml-LGPL-linking-exception | Stream compatibility library used by odoc-parser (see [linking-exception scope](#linking-exception-scope)) |

## Builder image tooling

The development image starts from one `ocaml/opam:debian-12-ocaml-<series>`
stage per supported compiler series (5.2 through 5.5, matching
`scripts/ci-matrix.py`). Each stage in `Dockerfile.dev` is pinned to the
multi-architecture manifest index digest below; those `FROM` lines are the
single source of truth. The Makefile selects the stage `ocaml-<series>` from
`OCAML_VERSION`, and the Compose file and live-test scripts default to
`ocaml-5.2`. BuildKit resolves only the selected stage. Dependabot's `docker`
ecosystem refreshes the digests in place; it is configured to ignore minor and
major `ocaml/opam` tag updates so a stage is never retargeted to another
compiler series. An explicit `OCAML_IMAGE=<image reference>` is still accepted
for local experiments but is not used by CI or release lanes.

| Image | Stage | Manifest index digest |
|---|---|---|
| `ocaml/opam:debian-12-ocaml-5.2` | `ocaml-5.2` | `sha256:b06c7348b6ed3e83b5f66267254504a8f066ec9be9c020c30a8e28ce10ab3ef6` |
| `ocaml/opam:debian-12-ocaml-5.3` | `ocaml-5.3` | `sha256:bbaac53e502f6602013d8967c3a54cfcb898b556f453ab72e8e23966c3c681df` |
| `ocaml/opam:debian-12-ocaml-5.4` | `ocaml-5.4` | `sha256:ba37cf7a29709fa2f19124fda8f4cabdea3156a648276fc98c9d528998ae2e59` |
| `ocaml/opam:debian-12-ocaml-5.5` | `ocaml-5.5` | `sha256:57f87030c6082e3f46f59988fbf12a84947945baf2974d4567a9dcee85780090` |

Each index contains native `linux/amd64` and `linux/arm64` images. The image
build recipes are MIT (`ocurrent/docker-base-images`); the OCaml and OPAM
software they contain is covered by the locked closure above.
Operating-system and ambient base-image tools are not linked into or
redistributed with the future worker artifact. Release containers will use a
separate minimal runtime stage and will receive their own package/SBOM audit
before publication.

### Installing the locked OPAM closure

Every lane that produces an OCaml artifact (the Docker image used by the Linux
matrix and the native macOS/Windows jobs) installs dependencies with
`scripts/opam-locked-deps.sh install`, not by re-solving `temporal-sdk.opam`.
The script installs each locked package at its exact version and then runs
`scripts/opam-locked-deps.sh check`, which fails the build with a `MISMATCH`
line for any package whose installed version differs. The audited versions are
therefore the versions the published `.cmxa` archives are compiled against.

`temporal-sdk.opam.locked` is solved for OCaml 5.2.1, but the artifact matrix
also builds OCaml 5.3, 5.4, and 5.5. `opam install --locked` cannot serve that
matrix: it would pin the compiler too, and the lock's `pin-depends` would
rebuild 5.2.1 from source. The script therefore leaves compiler-provided
packages (`ocaml`, `ocaml-base-compiler`, `ocaml-config`,
`ocaml-options-*`, and `base-*`) to the pinned image or setup-ocaml and only
reports them; the exact compiler patch level is asserted by the Makefile's
version checks. Every other package is held to the lock on every compiler.

#### Per-compiler lock overrides

When OPAM metadata makes a locked version uninstallable for one compiler
series, `scripts/opam-lock-overrides.txt` may name a replacement version for
that series only. Overrides cannot add packages or replace compiler packages,
and `make license-check` audits them alongside the lock.

| OCaml series | Package | Replacement version | License | Reason |
|---|---|---:|---|---|
| 5.5 | ocamlfind | 1.9.9~preview | MIT | `ocamlfind.1.9.8` declares `ocaml < 5.5.0~`; build-only tool reached through topkg and not linked into the SDK |

## Toolchain and CI images

The development image copies the Rust toolchain `1.99.0`, Cargo, Clippy, and
rustfmt from the official multi-architecture Rust image below, and
`Dockerfile.rust-ci` builds the shared Linux Rust producer from the same
digest. Rust is dual-licensed Apache-2.0 OR MIT. Debian's `protobuf-compiler`
and `libprotobuf-dev` packages are installed as build-only tools required by
Temporal Core's generated protobuf crates and standard Google protobuf
definitions; Protocol Buffers is BSD-3-Clause. Neither tool is intended for
the eventual minimal runtime image.

The Cargo license policy script runs in a separate official Python image in
the standalone GitHub Actions audit job, and the release preflight uses the
same image to generate and audit the Cargo SBOM. These containers have no
network access and mount the source read-only. Python is not installed in the development or
eventual runtime image, and nothing from the Python image is linked into or
redistributed with the SDK.

| Image | Manifest index digest | Used by | Native platforms | License |
|---|---|---|---|---|
| `rust:1.99-bookworm` | `sha256:59037199c44290f2befcdd58dcc540164763fc296950255aaefeef096a1866b0` | `Dockerfile.dev` Rust toolchain stage and `Dockerfile.rust-ci` | Linux amd64 and arm64/v8 | Rust is Apache-2.0 OR MIT; image packaging (`rust-lang/docker-rust`) is Apache-2.0 OR MIT |
| `python:3.14-slim-bookworm` | `sha256:4ff4b92a68355dbdb52584ab3391dff8d371a61d4e063468bfd0130e3189c6d9` | Cargo license audit and release-preflight Cargo SBOM generation and audit (CI-only) | Linux amd64, arm64/v8, and additional official architectures | CPython's PSF license stack (SPDX `Python-2.0`); image packaging (`docker-library/python`) is MIT |

`ocamlformat` is deliberately absent. Version 0.28.1 is MIT licensed, but its
build closure includes ordinary GPL packages (`menhir` and `fix`), which this
project's all-dependencies policy prohibits. `make lint` and `make fmt`
currently enforce repository-owned whitespace rules instead.

## Local integration service images

The Compose acceptance substrate uses the following exact OCI manifest
indexes. These images are development and integration services; they are not
linked into the SDK or redistributed in its OCaml package. Dependabot's
`docker-compose` ecosystem refreshes them, grouping the two Temporal images.

| Image | Manifest digest | Native platforms | Primary software license | Review |
|---|---|---|---|---|
| `postgres:18.6-bookworm` | `sha256:3725f4e2499eef5134592b3b4ab79a543ed7f8e533b05b5b637af926630f6650` | Linux amd64, arm64/v8, and additional official architectures | PostgreSQL License; Docker image packaging is MIT | [PostgreSQL license](https://www.postgresql.org/about/licence/), [official image source](https://github.com/docker-library/postgres) |
| `temporalio/server:1.32.0` | `sha256:c3e752127759616bb1615e0f9ba0e21635aeb5fdeb922de4f371c350955f46ae` | Linux amd64 and arm64 | MIT | [Temporal source and license](https://github.com/temporalio/temporal), [official Compose sample](https://github.com/temporalio/samples-server/tree/main/compose) |
| `temporalio/admin-tools:1.32.0` | `sha256:a9f84fb9a374b2374fe2e67c8efc0468ff3f1c66c8a0b14597ec86e349e62bca` | Linux amd64 and arm64 | MIT | Schema and CLI tooling from the official Temporal release/sample |

The pinned manifest indexes were inspected directly before adoption. Both
Temporal indexes expose native `linux/amd64` and `linux/arm64` manifests; the
PostgreSQL index includes those architectures and others. Temporal's archived
Compose repository marks `auto-setup` as deprecated, so this project follows
the maintained `samples-server` split between Server and admin-tools.
Committed history fixtures record the image references that produced them as
provenance; those records are not pins and are not part of this table.

The service containers are not release worker images. The future minimal OCaml
worker image still requires its own complete SBOM and redistribution audit
before publication.

## Locked Cargo closure

`rust/Cargo.lock` locks 322 packages: the project bridge and 321 dependencies
rooted at Temporal Core commit `95e97686a079dcfe6c42e3254b2f3f5e3d97408f`.
Some crates are locked at more than one version because different packages in
the graph require incompatible releases. The client, common, and SDK-Core
dependencies disable their default features: `temporalio-client` enables
`core-based-sdk` and `tls-ring`, `temporalio-sdk-core` enables `tls-ring`, and
`temporalio-common` uses no additional features. `temporalio-protos` uses its
default feature set. The project bridge is Apache-2.0 and
emits `staticlib` and `cdylib` artifacts for the native boundary plus an
internal `rlib` for Rust integration tests.

The whole locked graph, not only the direct dependencies below, is audited by
the standalone Cargo license job (`scripts/check-cargo-licenses.py` over
`cargo metadata --locked`), by `cargo-deny` for advisories and sources, and by
the third-party notices generator described below.

### Direct Rust dependencies

The bridge declares these workspace dependencies. "Locked versions" lists every
version of that crate in `rust/Cargo.lock`; where two appear, the bridge uses
the newest version that satisfies its requirement and the older one is reached
only through Temporal Core's graph. Every direct crate is also a dependency of
the Temporal Core closure at the same locked version, so these declarations
change package ownership metadata but add no package to the graph. Licenses are
the `license` field of `cargo metadata` for the locked version.

| Crate | Requirement | Locked versions | Kind | License | Purpose |
|---|---|---|---|---|---|
| base64 | 0.23.0 | 0.22.1, 0.23.1 | normal | MIT OR Apache-2.0 | Payload bytes in the private JSON control protocol |
| flate2 | 1.1.10 | 1.1.10 | dev | MIT OR Apache-2.0 | Decodes gzip-compressed gRPC requests in the test server double |
| h2 | 0.4.19 | 0.4.19 | dev | MIT | Plaintext HTTP/2 gRPC test server double |
| http | 1.5.0 | 1.5.0 | dev | MIT OR Apache-2.0 | HTTP types for the test server double |
| prost | 0.14.4 | 0.14.4 | normal | Apache-2.0 | Protobuf encoding and decoding of Temporal API messages and replay histories |
| serde | 1.0.228 | 1.0.229 | normal | MIT OR Apache-2.0 | Private control protocol data model |
| serde_json | 1.0.150 | 1.0.151 | normal | MIT OR Apache-2.0 | Private JSON control protocol |
| prost-wkt-types | 0.7.1 | 0.7.2 | normal | Apache-2.0 AND BSD-3-Clause | Exact protobuf timestamps and durations in the semantic adapter |
| temporalio-client | Core revision | 0.5.0 | normal | MIT (reviewed `LICENSE.txt`) | Temporal Core client |
| temporalio-common | Core revision | 0.5.0 | normal | MIT (reviewed `LICENSE.txt`) | Temporal Core shared types |
| temporalio-sdk-core | Core revision | 0.5.0 | normal | MIT (reviewed `LICENSE.txt`) | Temporal Core worker state machines |
| temporalio-protos | Core revision | 0.5.0 | normal | MIT (reviewed `LICENSE.txt`) | Temporal API protobuf types |
| tokio | 1.52.3 | 1.53.2 | normal, dev | MIT | Core's existing executor, channels, and task handles for the guarded poll lanes; tests also enable `net` |
| uuid | 1.23.4 | 1.27.0 | normal | Apache-2.0 OR MIT | Random v4 tickets for pending workflow-start operations |

The guarded poll lanes' hand-off channels are intentionally unbounded because
Core's outstanding-task permits provide the bound and a bounded send could
deadlock shutdown. Temporal Core already selected and used the locked Tokio
runtime, so the bridge does not create a second executor.

Dependabot checks the Cargo workspace under `/rust` every calendar day at
07:00 Australia/Sydney and targets `master`, with a three-day release cooldown
and a maximum of ten open Cargo version-update PRs. Its explicit
`allow: dependency-type: all` policy includes direct and transitive Cargo
dependencies; without that rule, routine version updates cover only direct
dependencies (security updates can still cover vulnerable lockfile entries).
Temporal Core packages remain grouped. Cargo resolution continues to enforce
upstream version requirements; this policy does not override those constraints.
Docker, Docker Compose, Rust toolchains, and GitHub Actions have separate daily
entries. Versions embedded in arbitrary scripts, Makefiles, and action inputs
still require explicit maintenance. OCaml and OPAM are intentionally absent
because GitHub Dependabot does not support that ecosystem; the locked OPAM
closure and its per-compiler overrides continue to be reviewed and updated
manually. A Dependabot PR that changes any pinned input listed in
[Keeping this inventory current](#keeping-this-inventory-current) fails the
license job until the matching table row is updated.

See the [Dependabot allow reference](https://docs.github.com/en/code-security/reference/supply-chain-security/dependabot-options-reference#allow)
for the direct/indirect update behavior.

### Temporal Core pin upgrades

A change to the Temporal Core revision, whether a hand-edited pin bump or a
grouped Dependabot `temporal-core` PR, and any other Dependabot Cargo PR must
carry this evidence before merge:

1. Updated license evidence: the standalone Cargo license job passes, and the
   locked package count, Core revision, and direct-dependency rows above are
   updated (`make check-dependency-inventory` fails until they are).
2. ABI evidence: the bridge ABI and Rust integration tests pass
   (`make verify`, `make native-verify`).
3. Replay evidence: the [replay history corpus](reference/history-corpus.md)
   passes `make test-history-corpus-upgrade` without regenerating, editing or
   deleting any committed history or expected outcome. CI runs the corpus on
   every leg and uploads the `history-corpus-report-linux-amd64-ocaml-5.5.1`
   report; the PR description links it and states the candidate
   `core_revision` and the revisions that produced the corpus. A mismatch lists
   the failing case IDs and blocks the upgrade until it is fixed or recorded as
   an approved, intentional compatibility break (see the corpus retention
   rules). A pass is forward-compatibility evidence only; it does not show
   that the previous pin can replay histories the new one writes, so it is not
   rollback evidence.
4. Compatibility evidence: the Temporal/PostgreSQL live job passes against the
   new pin.

The Cargo scanner parses SPDX `AND`, `OR`, `WITH`, and parentheses rather than
matching substrings. For an `OR`, it prints the exact approved branch selected;
every `AND` branch must be approved. It also understands Cargo's historical
slash-as-OR spelling. GPL, LGPL, AGPL, MPL, missing, malformed, and unknown
licenses fail policy fixtures. Approved permissive identifiers found in the
closure include MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, Zlib,
Unicode-3.0, 0BSD, MIT-0, CC0-1.0, Unlicense, and CDLA-Permissive-2.0. The
Apache LLVM exception is an exact approved exception.

Six first-party packages inherit the upstream workspace `LICENSE.txt` rather
than publishing a Cargo `license` expression: `temporalio-client`,
`temporalio-common`, `temporalio-common-wasm`, `temporalio-macros`,
`temporalio-protos`, and `temporalio-sdk-core`. The scanner permits this only
for those exact package names, the immutable Core git revision, and a file
named `LICENSE.txt`; the reviewed upstream license is MIT. See
[ADR 0001](decisions/0001-temporal-core-c-boundary.md).

That conclusion lives once, as `concluded_license` in
`scripts/check-cargo-licenses.py`, and the SBOM generator and the notices
generator below import it rather than repeating the package list. Following
SPDX 2.3, the Cargo SBOM records what each package declares separately from
what this project concludes: the six Core packages have `licenseDeclared`
`NOASSERTION` (Cargo metadata names only a file), `licenseConcluded` `MIT`, and
a `licenseComments` entry naming the reviewed `LICENSE.txt`. Every other
package's Cargo expression is used for both fields, with the historical
`MIT/Apache-2.0` slash rewritten as SPDX `OR`. The SBOM audit rejects a package
whose concluded license is missing or `NOASSERTION`.

## Third-party notices in release archives

Every release archive contains the Rust bridge, which statically links the
locked Cargo graph, and the MIT, BSD, ISC, Apache-2.0, Unicode-3.0, and similar
licenses of those packages require their texts and attributions to accompany
binary redistribution. `scripts/generate-third-party-notices.py` produces
`THIRD-PARTY-NOTICES.txt` from `cargo metadata --locked` without any new tool
dependency: it copies each package's top-level `LICENSE*`, `LICENCE*`,
`COPYING*`, `COPYRIGHT*`, `NOTICE*`, and `UNLICENSE*` files (and one level of a
`LICENSES` directory) from the package sources Cargo already downloaded, plus
the metadata `license_file` (the Core workspace `LICENSE.txt`). The document
contains this project's `LICENSE`, an inventory of every non-workspace package
grouped by concluded license, and each distinct text once with the packages
that use it. Output depends only on the Cargo graph: no checkout path,
timestamp, or environment value is recorded, and CRLF and trailing whitespace
are normalized.

Some published crates declare a license in Cargo metadata but omit the file
(currently the OpenTelemetry, `prost-wkt`, `pbjson`, `tonic-prost`, `jni`,
`objc2-*`, `r-efi`, `valuable`, and `winapi-*-gnu` crates). MIT and BSD
require the actual copyright notice to accompany binaries, so the generator
never fills in a licence template for them. Instead each such package must
have a reviewed entry for its exact version in
`scripts/license-texts/crates/manifest.json`, which points at upstream texts
vendored byte for byte under `scripts/license-texts/crates/`. An entry records
the package names and versions, the concluded license expression it was
reviewed against (it must still equal the package's concluded license), each
vendored file's SHA-256 and upstream URL pinned to a full commit hash, and the
evidence for choosing that commit. Optional `standard_texts` add a text from
`scripts/license-texts/` (only Apache-2.0 is kept) when upstream merely names a
license whose terms need no holder line, as for the `objc2` crates; the MIT and
BSD templates are deliberately absent. An entry without any upstream file is
accepted only with a `maintainer_exception` explaining the maintainer's
approval. Generation also rejects any emitted text, including a crate's own
file, whose copyright line still contains a template placeholder such as
`<year> <copyright holders>` (the Apache-2.0 appendix's
`[yyyy] [name of copyright owner]` instruction is part of that licence and is
allowed), and the `--audit` mode rejects such a line in a finished document.

A package with no licence file and no reviewed entry, an unconcluded license,
or a missing declared `license_file` fails generation with every affected
package listed; no partial file is written. The list covers the whole locked
graph, including platform-specific and build-only packages, so it is a superset
of what any single platform links.

To add a reviewed notice when a Cargo update introduces a file-less package or
version:

1. Run the generator against fresh `cargo metadata --locked` output; it names
   every package that needs review.
2. Read the crate's `.cargo_vcs_info.json` (in the downloaded registry source)
   for the commit and path it was published from, or, for an older crate
   without one, find the commit that set the published version.
3. Locate the `LICENSE*`, `COPYING*`, `NOTICE*`, or equivalent file at that
   commit (crate directory first, then repository root) with
   `gh api repos/<owner>/<repo>/contents/<path>?ref=<commit>`, and save its
   exact bytes under `scripts/license-texts/crates/<source>/`. Include any
   upstream `NOTICE` file, and for an `AND` expression the text of every part
   (for example the protobuf `LICENSE` for `prost-wkt-types`' BSD-3-Clause
   schemas).
4. Add or extend the manifest entry with the file's SHA-256, its
   `https://github.com/<owner>/<repo>/blob/<commit>/<path>` URL, and the
   evidence. Never write a copyright holder that upstream does not state; if
   upstream publishes no licence text, record the evidence and ask a
   maintainer to decide on a `maintainer_exception`.
5. Re-run the generator and `--audit`, and
   `python3 -m unittest discover -s test/smoke -p 'test_*artifact*.py'`.

Entries for versions no longer in the lock file are harmless and can be
removed in the same change that drops the package.

OCaml packages need no entry: the SDK archive contains only the installed
`temporal-sdk` package, while the OCaml runtime, `logs`, and `yojson` are
linked by the application from its own OPAM switch and are not redistributed
(see the locked OCaml closure table above).

The Release workflow generates the file on the publish runner, audits it
against the same metadata (every package listed exactly once, every referenced
text present), and passes it to `scripts/package-release-bridges.py`. Packaging
rejects a notices file that is not generated or does not embed the project
`LICENSE`, adds `LICENSE` and `THIRD-PARTY-NOTICES.txt` at the root of every
bridge and SDK archive, re-reads each archive to confirm both files, and
publishes the notices as a separate checksummed asset recorded in
`manifest.json`.

The release source archive also redistributes the locked Cargo graph in source
form: `scripts/create-source-archive.sh` vendors exactly the `Cargo.lock`
packages (each with its own licence files) so opam can build offline (see
[Offline opam source builds](reference/package-boundary.md#offline-opam-source-builds)),
and copies the same audited `THIRD-PARTY-NOTICES.txt` to the archive root,
which supplies the reviewed texts for crates that publish none. Vendoring adds
no package and changes no version, so the existing Cargo licence audit covers
it.

## CI-only quality tools

The independent quality job installs checksum-verified release artifacts with
`taiki-e/install-action`, pinned in the workflow by immutable commit (see
[GitHub Actions](#github-actions)) and configured with no installation
fallback. It installs these exact tools without adding them to the SDK's
runtime or build dependency graph:

| Tool | Version | License | Distinct purpose |
|---|---:|---|---|
| cargo-deny | 0.20.2 | MIT OR Apache-2.0 | RustSec advisory and Cargo source-provenance checks |
| cargo-machete | 0.9.2 | MIT | Fast detection of unused direct Rust dependencies |
| typos | 1.48.0 | MIT OR Apache-2.0 | Low-noise spelling checks across source, documentation, and configuration |

The versions are also enforced by `make quality` for contributors who install
the binaries locally. Cargo-deny's license check is deliberately disabled:
the repository's existing scanner has stricter reviewed exceptions for the
pinned Temporal Core workspace and remains the single Cargo licence authority
in the standalone dependency-audit job.

No additional OCaml semantic analyzer was selected. Dune and the OCaml
compiler already fail build warnings, and odoc is used only as the CI-only
documentation renderer described above; its LGPL closure is confined to the
`docs` image and does not extend this project's compiler and `ocamlbuild`
exceptions to SDK tooling or dependencies. `ocamlformat` remains excluded for
the separate copyleft closure documented above. The language-neutral typo gate
still checks OCaml identifiers, comments, and interfaces without weakening the
policy.

## GitHub Actions

Workflows and the composite `rust-bridge` action use these external actions.
They run only on CI runners and are not linked into or redistributed with the
SDK. Dependabot's `github-actions` ecosystem refreshes them. "Release" is the
version comment recorded beside a commit pin.

| Action | Pinned ref | Release | License | Used for |
|---|---|---|---|---|
| `actions/checkout` | `v7` | v7 | MIT | Repository checkout |
| `actions/cache/restore` | `caa296126883cff596d87d8935842f9db880ef25` | v5 | MIT | Restoring verified Rust bridge bundles |
| `actions/cache/save` | `caa296126883cff596d87d8935842f9db880ef25` | v5 | MIT | Saving verified Rust bridge bundles |
| `actions/upload-artifact` | `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a` | v7 | MIT | Passing bridges, reports, and release inputs between jobs |
| `actions/download-artifact` | `3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c` | v8 | MIT | Consuming those artifacts |
| `msys2/setup-msys2` | `ec48f7c5447b3140e2b088413ae3a55687bccb6e` | v2 | MIT | MinGW build tools for the Windows Rust producer |
| `ocaml/setup-ocaml` | `93303b622b2522e4411e295f9e77411a24912ac7` | v3.9.0 | MIT | Native macOS and Windows OCaml compilers |
| `taiki-e/install-action` | `183e4297cca2404691e9380e1307288dced5c82a` | v2.87.25 | Apache-2.0 OR MIT | Installing the pinned CI-only quality tools |

`actions/checkout` is referenced by its major-version tag rather than an
immutable commit; every other action is pinned by commit.

## Standalone Windows Rust producer

The Rust producer uses `msys2/setup-msys2` (pinned in
[GitHub Actions](#github-actions)) to install GNU/MinGW build tools without
installing OCaml. It selects MINGW64 to match the existing Windows
GNU ABI; the MSYS2 project deprecates this environment in favor of UCRT64,
so a future CRT migration must update and validate both producers and consumers.
MSYS2/GCC are build tools, not new SDK library dependencies. The distributed
Rust bridge retains the existing locked dependency and native import-library
license checks. The action must also be allowed by the infra repository policy.

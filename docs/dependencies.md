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

| Package | Exact version | License | Scope | Linked into release | Redistributed | Review note |
|---|---:|---|---|---|---|---|
| temporal-sdk | ~dev | Apache-2.0 | project | yes | yes | Project source and binary |
| dune | 3.24.2 | MIT | build | no | no | Build system only |
| logs | 0.10.0 | ISC | runtime | yes | no | Maintained application-configurable logging infrastructure; the SDK installs no reporter |
| ocamlbuild | 0.16.1 | LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception | build | no | no | Exact reviewed build-only linking-exception dependency of `logs` |
| ocamlfind | 1.9.8 | MIT | build | no | no | Build-time library discovery required by `logs` |
| topkg | 1.1.1 | ISC | build | no | no | Build-time packaging tool required by `logs` |
| yojson | 3.0.0 | BSD-3-Clause | runtime | yes | no | Implements the optional cross-language `json/plain` codec; Temporal itself does not require JSON |
| ocaml | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler/runtime | yes | no | Approved OCaml linking exception |
| ocaml-base-compiler | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler | no | no | Approved OCaml linking exception |
| ocaml-config | 3 | ISC | build | no | no | Compiler configuration package |
| ocaml-options-vanilla | 1 | CC0-1.0+ | build | no | no | Exact reviewed permissive metadata exception |
| base-bigarray | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-domains | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-nnp | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-threads | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-unix | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |

Protocol conformance tests use a small repository-owned standard-library
harness. Alcotest itself is ISC, but its complete OPAM test closure includes
ordinary LGPL packages outside the compiler/runtime exception allowed by this
project, so it is intentionally neither used nor declared as a package test
dependency.

## Reviewed OCaml linking exceptions

| Package | Exact version | License | Scope | Rationale |
|---|---:|---|---|---|
| ocamlbuild | 0.16.1 | LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception | build only | Required to build `logs.0.10.0`; not linked or redistributed |

Only the exact compiler/runtime names and the exact ocamlbuild version
hard-coded by the policy are accepted. Adding a row here also requires a
matching exact-name and version checker change.

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

| Stage | Image tag | Manifest index digest |
|---|---|---|
| `ocaml-5.2` | `ocaml/opam:debian-12-ocaml-5.2` | `sha256:b06c7348b6ed3e83b5f66267254504a8f066ec9be9c020c30a8e28ce10ab3ef6` |
| `ocaml-5.3` | `ocaml/opam:debian-12-ocaml-5.3` | `sha256:bbaac53e502f6602013d8967c3a54cfcb898b556f453ab72e8e23966c3c681df` |
| `ocaml-5.4` | `ocaml/opam:debian-12-ocaml-5.4` | `sha256:ba37cf7a29709fa2f19124fda8f4cabdea3156a648276fc98c9d528998ae2e59` |
| `ocaml-5.5` | `ocaml/opam:debian-12-ocaml-5.5` | `sha256:57f87030c6082e3f46f59988fbf12a84947945baf2974d4567a9dcee85780090` |

Each index contains native `linux/amd64` and `linux/arm64` images.
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

When OPAM metadata makes a locked version uninstallable for one compiler
series, `scripts/opam-lock-overrides.txt` may name a replacement version for
that series only. Overrides cannot add packages or replace compiler packages,
and `make license-check` audits them alongside the lock. The only current
entry is `ocamlfind.1.9.9~preview` for OCaml 5.5, because `ocamlfind.1.9.8`
declares `ocaml < 5.5.0~`; ocamlfind (MIT) is a build-only tool reached
through topkg and is not linked into the SDK.

The image copies Rust 1.98.1, Cargo, Clippy, and rustfmt from the official
multi-architecture `rust:1.98-bookworm` image at manifest digest
`sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e`.
That manifest contains native `linux/amd64` and `linux/arm64/v8` images. Rust
is dual-licensed Apache-2.0 OR MIT. Debian's `protobuf-compiler` and
`libprotobuf-dev` packages are installed as build-only tools required by
Temporal Core's generated protobuf crates and standard Google protobuf
definitions; Protocol Buffers is BSD-3-Clause. Neither tool is intended for
the eventual minimal runtime image. The Cargo license policy script runs in a
separate official `python:3.14-slim-bookworm` image pinned at manifest digest
`sha256:4ff4b92a68355dbdb52584ab3391dff8d371a61d4e063468bfd0130e3189c6d9`
in the standalone GitHub Actions audit job. Its scanner container has no
network access and mounts the source read-only. Python is not installed in the
development or eventual runtime image.

`ocamlformat` is deliberately absent. Version 0.28.1 is MIT licensed, but its
build closure includes ordinary GPL packages (`menhir` and `fix`), which this
project's all-dependencies policy prohibits. `make lint` and `make fmt`
currently enforce repository-owned whitespace rules instead.

## Local integration service images

The Compose acceptance substrate uses the following exact OCI manifest
indexes. These images are development and integration services; they are not
linked into the SDK or redistributed in its OCaml package.

| Image | Manifest digest | Native platforms | Primary software license | Review |
|---|---|---|---|---|
| `postgres:16.13-bookworm` | `sha256:472efd9a66f2b2f1a5aeb18b28de74332e6ef88c2b93a1a5d812fb6db67a5f60` | Linux amd64, arm64/v8, and additional official architectures | PostgreSQL License; Docker image packaging is MIT | [PostgreSQL license](https://www.postgresql.org/about/licence/), [official image source](https://github.com/docker-library/postgres) |
| `temporalio/server:1.31.0` | `sha256:b021b3b58c3f169634cdbb0451fcc0e69e8190b40454323362c7c52bbd4ff7b9` | Linux amd64 and arm64 | MIT | [Temporal source and license](https://github.com/temporalio/temporal), [official Compose sample](https://github.com/temporalio/samples-server/tree/main/compose) |
| `temporalio/admin-tools:1.31.0` | `sha256:3e68adcd54195a7c1222e99f2dbc32a4fdbf44ad69e3bb48e21e85c4bf417c2e` | Linux amd64 and arm64 | MIT | Schema and CLI tooling from the official Temporal release/sample |

The pinned manifest indexes were inspected directly before adoption. Both
Temporal indexes expose native `linux/amd64` and `linux/arm64` manifests; the
PostgreSQL index includes those architectures and others. Temporal's archived
Compose repository marks `auto-setup` as deprecated, so this project follows
the maintained `samples-server` split between Server and admin-tools.

The service containers are not release worker images. The future minimal OCaml
worker image still requires its own complete SBOM and redistribution audit
before publication.

## Locked Cargo closure

`rust/Cargo.lock` locks 319 dependencies rooted at Temporal Core commit
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`; metadata contains 320 packages
including the project bridge itself. The client, common, and SDK-Core
dependencies disable their default features: `temporalio-client` enables
`core-based-sdk` and `tls-ring`, `temporalio-sdk-core` enables `tls-ring`, and
`temporalio-common` uses no additional features. `temporalio-protos` uses its
default feature set. The project bridge is Apache-2.0 and
emits `staticlib` and `cdylib` artifacts for the native boundary plus an
internal `rlib` for Rust integration tests.

The bridge declares `serde` 1.0.228 (MIT OR Apache-2.0), `serde_json` 1.0.150
(MIT OR Apache-2.0), and `base64` 0.22.1 (MIT OR Apache-2.0) directly for its
private control protocol. The semantic adapter additionally declares the
first-party `temporalio-protos` package at the same immutable Core revision and
`prost-wkt-types` 0.7.1 (Apache-2.0) for exact protobuf timestamps and
durations. The guarded poll lanes directly declare Tokio 1.52.3 (MIT) for its
executor, channels, and task handles; their hand-off channels are intentionally
unbounded because Core's outstanding-task permits provide the bound and a
bounded send could deadlock shutdown. Temporal Core already selected and used
that exact locked runtime, so the bridge does not create a second executor.
Every package was already present at the exact locked version in the
Temporal Core closure, so these declarations change package ownership metadata
but add no package to the 319-dependency graph.

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
manually.

See the [Dependabot allow reference](https://docs.github.com/en/code-security/reference/supply-chain-security/dependabot-options-reference#allow)
for the direct/indirect update behavior.

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

## CI-only quality tools

The independent quality job installs checksum-verified release artifacts with
`taiki-e/install-action` 2.83.1, pinned in the workflow by immutable commit.
The action is MIT OR Apache-2.0 and is configured with no installation
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
compiler already fail build warnings, while the mature documentation compiler
`odoc` 3.2.1 currently depends on `tyxml` under
`LGPL-2.1-only WITH OCaml-LGPL-linking-exception`. This project's policy does
not extend its narrowly approved compiler and `ocamlbuild` exceptions to
ordinary tooling packages. `ocamlformat` remains excluded for the separate
copyleft closure documented above. The language-neutral typo gate still checks
OCaml identifiers, comments, and interfaces without weakening the policy.

## Standalone Windows Rust producer

The Rust producer uses `msys2/setup-msys2` (MIT), pinned to
`66cd2cce69caa17b53920067426061ca1de3a884`, to install GNU/MinGW build tools
without installing OCaml. It selects MINGW64 to match the existing Windows
GNU ABI; the MSYS2 project deprecates this environment in favor of UCRT64,
so a future CRT migration must update and validate both producers and consumers.
MSYS2/GCC are build tools, not new SDK library dependencies. The distributed
Rust bridge retains the existing locked dependency and native import-library
license checks. The action must also be allowed by the infra repository policy.

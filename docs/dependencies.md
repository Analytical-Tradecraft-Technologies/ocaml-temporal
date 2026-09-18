# Dependency and License Inventory

All project and build dependencies are checked before a milestone commit.
`make license-check` reads `temporal-sdk.opam.locked`, asks OPAM for each package's
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
| dune | 3.24.0 | MIT | build | no | no | Build system only |
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

The development image is based on `ocaml/opam:debian-12-ocaml-5.2` and uses
OPAM to install the locked closure. Operating-system and ambient base-image
tools are not linked into or redistributed with the future worker artifact.
Release containers will use a separate minimal runtime stage and will receive
their own package/SBOM audit before publication.

The image copies Rust 1.98.1, Cargo, Clippy, and rustfmt from the official
multi-architecture `rust:1.98.1-bookworm` image at manifest digest
`sha256:9a73a5088750b4c95158ab26629c854c3d6fc4b173cb7bc8079ad252d8ed7bfa`.
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

Dependabot checks the Cargo workspace under `/rust` every Monday and targets
updates at `master`. OCaml and OPAM are intentionally absent because GitHub
Dependabot does not support that ecosystem; the locked OPAM closure continues
to be reviewed and updated manually.

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

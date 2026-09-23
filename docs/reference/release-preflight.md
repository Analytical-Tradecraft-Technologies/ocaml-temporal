# Release preflight

The initial prerelease is `v0.1.0-rc.1`. OPAM records it as `0.1.0~rc.1`
in `.release-version`, `temporal-sdk.opam`, and `temporal-sdk.opam.locked`.
Those three files must agree with the version tag supplied to the release
workflow; changing the Run workflow field alone does not change package metadata.

## Build coverage and artifact reuse

All event types call the graph in `.github/workflows/build-pr.yml`:

| Platform | PR and merge queue | Master and nightly | Release |
| --- | --- | --- | --- |
| Linux x64 | 5.2.1, 5.5.1 | All four | All four |
| Linux ARM64 | 5.5.1 | All four | All four |
| macOS ARM64 | 5.5.1 | 5.5.1 | All four |
| Windows x64 GNU/MinGW | 5.5.1 | 5.5.1 | All four |

The supported exact versions are **5.2.1, 5.3.0, 5.4.1, and 5.5.1**.
Status-check labels retain their existing minor-series names for branch
protection. A compiler version probe verifies the full requested patch.
`scripts/ci-matrix.py` is the matrix source of truth.

Four Rust producer jobs run independently of OCaml. Linux producers use the
pinned Debian 12 Rust image; desktop producers install the pinned Rust toolchain
without installing OCaml. Windows uses the MIT-licensed `msys2/setup-msys2`
action and the GNU/MinGW toolchain. Each producer runs Rust formatting, Clippy,
and the locked Rust tests before packaging static/dynamic libraries and native
link dependencies. An exact cache hit reuses the verified bundle; its key
includes sources, tests, build machinery, profile, and native environment.
Release builds use a separate optimized profile for tests and packaging.

OCaml jobs download their platform's Rust bundle, validate its identity and
checksums, and compile the OCaml library and C stubs for the selected compiler.
Local source builds still compile Rust when no bundle is provided. An invalid
explicit bundle fails instead of falling back to a Rust build.

The Linux x64/5.5.1 OCaml job also compiles every live smoke executable once.
Other compiler jobs build the installed library, examples, and unit tests;
their lint targets use `@install` rather than Dune's all-executables default.
One downstream smoke job verifies their commit, compiler, platform, file list,
and SHA-256 digests, then runs all existing Temporal/PostgreSQL controllers.
It does not copy Dune build state or compile worker/driver programs again.
Local smoke commands retain the normal source-build behavior.

Rust bundles, compiled smoke executables, and the audited Cargo SBOM are retained
for 90 days through `RUST_BRIDGE_ARTIFACT_RETENTION_DAYS`, managed in infra.
Diagnostic uploads retain their existing shorter lifetimes. Actions artifact
downloads require GitHub authentication; published public release assets do not.

## Publish a prerelease

After merging the workflow, open **Actions → Release → Run workflow**, select
`master`, and enter **Version tag**, initially `v0.1.0-rc.1`. Everything after
that manual dispatch is automated:

1. Require master, matching version metadata, a clean source checkout, and an
   unused tag.
2. Build/test all four Rust platforms and all sixteen OCaml combinations, plus
   the single live smoke job, quality/security scans, and dependency audits.
3. Validate all bridge bundles, archive the exact source commit, audit the Cargo
   SPDX SBOM, and generate the release manifest and asset checksums.
4. Atomically create the Git tag at the exact tested commit. A concurrent or
   existing tag fails publication instead of moving or reusing that tag.
5. Upload assets to a draft release and publish it as a prerelease when the tag
   has a prerelease suffix. No manual tagging or asset upload is required.

The release contains four compiler-independent Rust bridge archives, source,
`manifest.json`, the Cargo SBOM, and `SHA256SUMS`. Applications compile the OCaml
library and C stubs using their own compiler; these are not precompiled OCaml
package distributions. Linux assets target Debian 12/glibc, not musl/Alpine.

For the next version, first update the three version files in a PR and then
enter the matching new tag at dispatch. Published tags are immutable. If an
upload fails after tag creation, leave the draft unpublished and inspect it;
a rerun deliberately fails on the existing tag rather than replacing assets.
The source SHA and checksums in the manifest identify what was built, but are
not a signed provenance attestation.

## Local gate

Run `make release-preflight` from a clean checkout. This deliberately performs
metadata and source checks only, so it does not require Docker, OCaml tooling,
or a Rust build. It verifies:

- the working tree has no staged, unstaged, or untracked files;
- the opam manifests, Dune project, README, license, and pinned Temporal Core
  revision agree on identity, ownership, licensing, release metadata, and the
  canonical GitHub repository location; and
- generated build trees are not tracked and the sorted Git source manifest can
  be fingerprinted reproducibly.

The clean-tree requirement is intentional: a preflight result must describe
the exact inputs that would be archived or built, not a mixture of committed
files and local output. The target also runs the stale-owner rejection fixture;
that fixture stays out of the ordinary quality target so contributors can run
the latter from a dirty worktree.

## CI SBOM

`.github/workflows/release-preflight.yml` runs this complete target on pull
requests, merge groups, and manual dispatches. It obtains the locked Cargo graph
with `cargo metadata --locked`, then invokes the project-owned standard-library
SBOM generator inside the pinned official Python image with network access
disabled. The generated SPDX 2.3 document is deterministic: package IDs are
derived from Cargo IDs, package order is stable, and its creation timestamp is
fixed. A second isolated invocation validates the document before the job
finishes. The SBOM is a CI artifact/input check and is not committed to the
repository. It covers the locked Cargo package graph only; it is not yet the
complete OCaml package or runtime-container SBOM.

The preflight workflow does not publish or tag by itself. The manually
dispatched Release workflow requires it alongside the full build/test graph
before publishing.

## Tag consistency gate

Before creating a release tag, run `make release-tag-check RELEASE_TAG=vX.Y.Z`.
For a prerelease, use either the familiar `v1.0.0-beta.1` spelling or the
equivalent OPAM-native `v1.0.0~beta.1` spelling. The checker records the
package version as `1.0.0~beta.1` in both cases, because OPAM's tilde ordering
keeps the beta below the eventual `1.0.0` release.
The check accepts only a three-part numeric tag with an optional prerelease
suffix and verifies the normalized version against `.release-version`,
`temporal-sdk.opam`, and `temporal-sdk.opam.locked`. A development checkout
using `~dev` therefore cannot accidentally be published under a release-looking
tag. The same
contract is exercised without Docker by
`test/smoke/test_release_tag_contract.sh`.

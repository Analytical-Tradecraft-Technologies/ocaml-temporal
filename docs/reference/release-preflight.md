# Release preflight

The initial prerelease is `v0.1.0-rc.1`. OPAM records it as `0.1.0~rc.1`
in `.release-version`, `temporal-sdk.opam`, and `temporal-sdk.opam.locked`.
Those three files must agree with the version tag supplied to the release
workflow; changing the Run workflow field alone does not change package metadata.
The Rust bridge reports the SDK to Temporal as `temporal-ocaml` with the same
version in SemVer spelling (`0.1.0-rc.1`), recorded as `SDK_VERSION` in
`rust/core-bridge/src/abi.rs`; the preflight gate and a Rust unit test reject a
mismatch with `.release-version`.

Every repository link, including JSON schema `$id` values, uses the canonical
`https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal` URL. The
preflight gate rejects any tracked file that still uses the pre-transfer
location, because GitHub's transfer redirect is not permanent and published
schema identifiers cannot change later.

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
Release builds package Cargo's `[profile.release]` and run their Rust tests
with the same optimization level and without debug assertions or overflow
checks. Source and opam installations build the same Cargo release profile,
because Dune's release profile (selected by `opam install` and `dune build -p`)
selects it; only Dune's dev profile uses Cargo's unoptimized dev profile.

OCaml jobs download their platform's Rust bundle, validate its identity and
checksums, and compile the OCaml library and C stubs for the selected compiler.
Each job then packages that installed SDK and verifies an independent consumer
against its relocated, source-free installation before uploading it. Downstream
applications can [link the compiled SDK directly](prebuilt-ocaml.md).
Local source builds still compile Rust when no bundle is provided. An invalid
explicit bundle fails instead of falling back to a Rust build.

The Linux x64/5.5.1 OCaml job also compiles every live smoke executable once.
Other compiler jobs build the installed library, examples, and unit tests;
their lint targets use `@install` rather than Dune's all-executables default.
One downstream smoke job verifies their commit, compiler, platform, file list,
and SHA-256 digests, then runs all existing Temporal/PostgreSQL controllers.
It does not copy Dune build state or compile worker/driver programs again.
Local smoke commands retain the normal source-build behavior.

Rust bundles, compiled OCaml SDKs, smoke executables, and the audited Cargo SBOM are retained
for 90 days through `RUST_BRIDGE_ARTIFACT_RETENTION_DAYS`, managed in infra.
Diagnostic uploads retain their existing shorter lifetimes. Actions artifact
downloads require GitHub authentication; published public release assets do not.

## Publish a prerelease

The release tag is a maintainer approval boundary. First review the candidate
commit, the support decision, and the required qualification evidence. An
authorized maintainer then creates `v0.1.0-rc.1` at that exact `master` commit
and pushes the protected tag over HTTPS. For example, from a clean checkout:

```sh
git fetch origin master
git tag -a v0.1.0-rc.1 <approved-master-sha> -m 'OCaml Temporal SDK v0.1.0-rc.1'
git push origin refs/tags/v0.1.0-rc.1
```

Open **Actions → Release → Run workflow**, select `master`, and enter the same
**Version tag**. Dispatch must still resolve to the approved SHA. The workflow:

1. Require master, matching version metadata, a clean source checkout, and a
   remote tag that peels to the exact dispatch commit. A missing, retargeted,
   or unreachable tag fails before the release build.
2. Build/test all four Rust platforms and all sixteen OCaml combinations, plus
   the single live smoke job, quality/security scans, and dependency audits.
3. Generate and audit `THIRD-PARTY-NOTICES.txt` from the locked Cargo graph,
   validate all bridge and OCaml SDK bundles, add `LICENSE` and the notices to
   every archive, archive the exact source commit with its vendored Cargo
   sources and notices, audit the Cargo SPDX SBOM,
   and generate the release manifest and asset checksums.
4. Fetch and verify the remote tag again before creating the draft and before
   publishing it. The workflow never creates or moves the protected tag.
5. Upload assets to a draft release and publish it as a prerelease when the tag
   has a prerelease suffix. No manual asset upload is required.

The release contains four compiler-independent Rust bridge archives, sixteen
compiled OCaml SDK archives, an offline-buildable source archive for
[`opam install`](package-boundary.md#offline-opam-source-builds), `manifest.json`, the Cargo SBOM, the
third-party notices, and `SHA256SUMS`. Each bridge and SDK archive carries
`LICENSE` and `THIRD-PARTY-NOTICES.txt` at its root, so redistributing any one
archive keeps the licence texts of the statically linked Rust packages with it
(see [third-party notices](../dependencies.md#third-party-notices-in-release-archives)).
Compatible applications link the OCaml SDK, C stubs and Rust bridge
directly without rebuilding them. Each SDK records its exact compiler and compiled
dependency identities; incompatible environments must use matching dependencies
or build from source. Linux assets target Debian 12/glibc, not musl/Alpine.

For the next version, first update the three version files and `SDK_VERSION` in a PR and obtain
maintainer approval of the candidate and its evidence. Create the matching
protected tag at the approved master commit, then dispatch the workflow. The
tag must be protected against updates and deletion as well as unauthorized
creation before dispatch: the workflow detects a mismatched commit at each
check, but cannot prevent a ref change between the final check and publication,
or after publication. If an upload fails, leave the draft unpublished and
inspect it. A rerun may finish if no draft release exists yet; it will not
silently replace an existing draft or release. If a published release is found
defective, preserve its tag and assets for audit,
mark it superseded in its notes, and issue a corrected version under a new tag.
The source SHA and checksums in the manifest identify what was built, but are
not a signed provenance attestation.

## Local gate

Run `make release-preflight` from a clean checkout. This deliberately performs
metadata and source checks only, so it does not require Docker, OCaml tooling,
or a Rust build. It verifies:

- the working tree has no staged, unstaged, or untracked files;
- the opam manifests, Dune project, README, license, and pinned Temporal Core
  revision agree on identity, ownership, licensing, release metadata, and the
  canonical GitHub repository location;
- the README and package metadata carry the maturity label that matches the
  version (see below); and
- generated build trees are not tracked and the Git source manifest can be
  fingerprinted reproducibly.

The clean-tree requirement is intentional: a preflight result must describe
the exact inputs that would be archived or built, not a mixture of committed
files and local output. The target also runs the preflight fixture contract
(`test/smoke/test_release_preflight_contract.sh`), which covers stale-owner
rejection, manifest content sensitivity, and both maturity directions; it stays
out of the ordinary quality target so contributors can run the latter from a
dirty worktree.

### Maturity label

The maturity expectation is derived from `.release-version`, not hard-wired:

- A **prerelease** (an OPAM `~` suffix such as `0.1.0~rc.1`) or any **`0.x`**
  version is experimental. `README.md` must say so (it must match
  `experimental` or `pre-0.1.0`, case-insensitively), and `dune-project`,
  `temporal-sdk.opam`, and `temporal-sdk.opam.locked` must each contain the
  word `experimental` (the synopsis, description, and `experimental` tag).
- A **stable** version (`1.0.0` and later, without `~`) must not be labelled
  experimental: none of those four files may contain `experimental`, and the
  README may not describe the package as pre-`0.1.0`. Preparing a stable
  release therefore includes rewording the README status section and removing
  the experimental synopsis, description, and tag from the package metadata.

`test/smoke/test_repository.ml` applies the same rule to the exact package
metadata strings, and the Release workflow titles its notes "Experimental OCaml
Temporal SDK" only for a prerelease or `v0.x` tag.

### Source manifest

The preflight prints `source manifest sha256:` followed by the SHA-256 of the
NUL-terminated `git ls-tree -r -z --full-tree HEAD` listing. Each record holds
one tracked entry's mode, object type, Git object ID, and path, so the digest
changes whenever any file's contents, executable bit, symlink target, or path
changes, and it is identical for two checkouts of the same tree. Because the
clean-tree checks pass first, HEAD's tree is exactly what would be archived.
Git's canonical tree order and `-z` (which bypasses `core.quotePath` escaping)
make the digest independent of locale, user Git configuration, mtimes, umask,
commit metadata, and checkout location. Per-file identity relies on Git's
object IDs (collision-detecting SHA-1, or SHA-256 in a SHA-256 repository).
The digest is reproducibility evidence for review, not a signed attestation.

## Toolchain bounds

`temporal-sdk.opam` and `dune-project` bound OCaml to `>= 5.2 & < 5.6`,
matching the CI-tested 5.2–5.5 series; raise the upper bound only together with
the CI matrix. The opam `conf-rust-2024` dependency only proves edition 2024
support (Rust 1.85), while the workspace `rust-version` in `rust/Cargo.toml` is
newer. `scripts/build-rust-bridge.sh` therefore compares the compiler Cargo
will use (`$RUSTC`, default `rustc`) against that `rust-version` before
running Cargo, and fails with the required and found versions instead of a
dependency-resolution error deep inside Cargo.
`test/smoke/test_rust_version_gate.sh` exercises this with stand-in compilers.
The package declares no `available:` platform restriction yet.

## CI SBOM

`.github/workflows/release-preflight.yml` runs this complete target on pull
requests, merge groups, and manual dispatches. It obtains the locked Cargo graph
with `cargo metadata --locked`, then invokes the project-owned standard-library
SBOM generator inside the pinned official Python image with network access
disabled. The generated SPDX 2.3 document is deterministic: package IDs are
derived from Cargo IDs, package order is stable, and its creation timestamp is
fixed. Each package records its Cargo `licenseDeclared` and the
`licenseConcluded` shared with the Cargo licence scanner, so the pinned Temporal
Core crates, which declare only a licence file, are concluded `MIT` rather than
`NOASSERTION`. A second isolated invocation validates the document, including
that every package has a concluded licence, before the job finishes.

The third-party notices are generated by the Release workflow's publish job
rather than here, because they need the downloaded package sources; the
generator, its audit, and the archive checks are covered offline by
`make test-ci-artifacts`. The SBOM is a CI artifact/input check and is not committed to the
repository. It covers the locked Cargo package graph only; it is not yet the
complete OCaml package or runtime-container SBOM.

The preflight workflow does not publish or tag by itself. The manually
dispatched Release workflow requires it alongside the full build/test graph
before publishing.

## Tag consistency gate

Before creating a release tag, run `make release-tag-check RELEASE_TAG=vX.Y.Z`.
For a prerelease, the Git tag uses the SemVer hyphen spelling
`v1.0.0-beta.1`, while `.release-version` and both opam files use the OPAM
spelling `1.0.0~beta.1`, because OPAM's tilde ordering keeps the beta below
the eventual `1.0.0` release. The checker converts exactly one character: the
first hyphen after `vMAJOR.MINOR.PATCH` becomes `~`, and the prerelease suffix
is otherwise compared unchanged. Git refnames cannot contain `~`, so a tag
input such as `v1.0.0~beta.1` can never exist; the checker and the release
asset packager reject it with an error that names the hyphen spelling.
The check accepts only a three-part numeric tag with an optional prerelease
suffix and verifies the normalized version against `.release-version`,
`temporal-sdk.opam`, and `temporal-sdk.opam.locked`. A development checkout
using `~dev` therefore cannot accidentally be published under a release-looking
tag. The same
contract is exercised without Docker by
`test/smoke/test_release_tag_contract.sh`.

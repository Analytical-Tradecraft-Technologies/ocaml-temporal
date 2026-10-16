# Release checklist and bad-release remediation

This is the maintainer checklist for publishing a version, from a release
candidate such as `v0.1.0-rc.1` to the eventual `v1.0.0`, and the procedure for
a release that turns out to be bad. It is built on the existing machinery:

- [`.github/workflows/release.yml`](../../.github/workflows/release.yml), the
  manually dispatched Release workflow;
- [release preflight](release-preflight.md), which describes that workflow, the
  `make release-preflight` and `make release-tag-check` gates, and the asset
  set;
- the [v1 support policy](v1-support-policy.md), which defines the candidate
  evidence and who is responsible for it;
- the repository's tag rulesets (below).

No release has been published yet. Rehearsing this procedure end to end, as a
prerelease, is [#510], which is open.

## Automated and maintainer-owned steps

The Release workflow automates everything that can be checked from the
repository. These steps still need a maintainer, and the workflow cannot
replace them:

| Step | Automated by the Release workflow | Needs a maintainer |
| --- | --- | --- |
| Version metadata, release notes, tag-to-commit match | Yes, before any build and again before the draft and before publishing | Writing the notes and the version PR |
| Full release build and test graph, licence audit, Cargo SBOM | Yes | No |
| Source archive builds inside the opam sandbox | Yes, `source-archive-check` | No |
| Consumer built against the package installed from that archive, outside the checkout | Yes, `source-archive-check` ([#510]) | No |
| `SHA256SUMS` over every asset | Yes | No |
| Signed SLSA build provenance for every asset | Yes, `actions/attest-build-provenance` in `publish` ([#510]) | No |
| Qualification evidence and sign-off | No | The release owner, in the tracking issue |
| Creating the protected version tag | No | A member of the maintainer team |
| Publish credentials | The job uses the workflow's own `GITHUB_TOKEN` and OIDC token; no stored secret | Dispatching the workflow; a future opam-repository submission needs a maintainer's own account |
| Prerelease rehearsal: dispatch a real `-rc` tag, then download and verify it as in step 4 | No | A maintainer; not done yet ([#510]) |
| Bad-release remediation | No | A maintainer, as described below |

## Tag rules

Two repository rulesets apply to every `refs/tags/v*` ref:

| Ruleset | Rule | Bypass |
| --- | --- | --- |
| Immutable version tags | Blocks **update** and **deletion** | None. No maintainer or App can move or delete a version tag once it exists. |
| Maintainers and release App create version tags | Restricts **creation** | The maintainer team |

A version tag is therefore permanent from the moment it is pushed, whether or
not a release is ever published from it. Every remediation below follows from
that: a version number that has been tagged can never be reused for different
contents, which is also the [support policy's](v1-support-policy.md#prerelease-compatibility)
rule.

The Release workflow never creates or moves a tag. It checks, with
`scripts/check-release-tag-commit.sh`, that the remote tag peels to the
dispatched commit before the build, before creating the draft, and before
publishing it.

## Roles and sign-off

The roles come from the [support policy](v1-support-policy.md#candidate-qualification-and-responsibility):

- The **repository maintainer** approves the release scope and the support
  policy.
- The **release owner** records the exact commit and tag, names the reviewers,
  collects the evidence below, and decides whether the version is published.
  Only a member of the maintainer team can create the tag, so the person
  pushing it signs off by doing so.
- **Reviewers** named by the release owner check the API/runtime changes and
  the live evidence for the exact commit.
- The **application operator** owns any deployment. Publishing a version is
  not evidence that a deployment is fit for production.

Record the sign-off (commit, tag, reviewers, linked evidence and residual
limits) in the tracking issue for the release before pushing the tag.

## Checklist

### 1. Prepare the version

- [ ] In one PR, set the version in `.release-version`, `temporal-sdk.opam`,
      `temporal-sdk.opam.locked` (OPAM spelling, for example `1.0.0~rc.1`), and
      `SDK_VERSION` in `rust/core-bridge/src/abi.rs` (SemVer spelling,
      `1.0.0-rc.1`).
- [ ] For a stable version (`1.0.0` or later, no prerelease suffix), remove the
      experimental wording from `README.md`, `dune-project` and both opam
      files; the [maturity rule](release-preflight.md#maturity-label) rejects a
      stable version still labelled experimental, and a prerelease that is not.
- [ ] Add the release notes to the same PR as `docs/releases/<tag>.md` (for
      example `docs/releases/v1.0.0-rc.1.md`), with three non-empty sections:
      `## Changes`, `## Breaking changes and migration`, and
      `## Residual limits`, which must name every limit the
      [support policy](v1-support-policy.md#feature-matrix) requires.
      `scripts/check-release-notes.sh` rejects a missing file, a missing or
      empty section, and a `TODO`, `TBD` or `FIXME` placeholder. The
      [v0.1.0-rc.1 notes](../releases/v0.1.0-rc.1.md) are the first example.
      Reviewers approve the notes with the version change.
- [ ] Merge the PR through the merge queue, so the PR and merge-group checks
      (including `release-preflight.yml`) pass on it.

### 2. Gate the exact candidate commit

Every item is evidence for the commit that will be tagged, not an older one.

- [ ] From a clean checkout of that commit, `make release-preflight` and
      `make release-tag-check RELEASE_TAG=<tag>` pass.
- [ ] The master Build workflow (`build.yml`) is green for that commit,
      including the Temporal/PostgreSQL live job. Retain the run URL; its
      `live-diagnostics-*` and `live-regressions-*` artifacts expire after
      seven days, so download what the sign-off cites.
- [ ] The open qualification gates below are closed, or the release owner
      records why this version ships without them. A prerelease may ship with
      open gates if its notes say so; `v1.0.0` may not.
- [ ] The release owner has recorded the sign-off described above.

### 3. Tag and publish

- [ ] A maintainer creates and pushes the annotated tag at the approved commit,
      as in [Publish a prerelease](release-preflight.md#publish-a-prerelease).
      Double-check the SHA first: the tag cannot be corrected afterwards.
- [ ] Dispatch **Actions → Release → Run workflow** on `master` with the same
      tag. The workflow validates the request, including the release notes,
      runs the full `release` tier of
      `build-pr.yml` (sixteen OCaml combinations, four Rust producers, the
      live job and the licence audit) and `release-preflight.yml`, builds the
      source archive in the opam sandbox and a consumer against the package
      installed from it, then assembles and checks the assets, signs their
      build provenance, uploads them to a draft, and publishes it (as a
      prerelease when the tag has a `-` suffix). The published notes
      are `docs/releases/<tag>.md` followed by a generated `## Release assets`
      section; nothing else is written by hand on the release page.

### 4. Verify the published release

- [ ] Download the assets and run `sha256sum --check SHA256SUMS`.
- [ ] Check that `manifest.json` names the tagged commit.
- [ ] Verify the provenance attestation of every asset, as any consumer can:

  ```sh
  repo=Analytical-Tradecraft-Technologies/ocaml-temporal
  tag=v0.1.0-rc.1
  gh release download "$tag" --repo "$repo" --dir "ocaml-temporal-$tag"
  cd "ocaml-temporal-$tag"
  sha256sum --check SHA256SUMS
  # The commit the protected tag peels to; the workflow ran from master at it.
  commit=$(git ls-remote "https://github.com/$repo" "refs/tags/$tag^{}" | cut -f 1)
  for asset in *; do
    gh attestation verify "$asset" --repo "$repo" \
      --signer-workflow "$repo/.github/workflows/release.yml" \
      --source-ref refs/heads/master --source-digest "$commit" \
      --deny-self-hosted-runners
  done
  ```

  `gh attestation verify` (signed in with `gh auth login`) looks up the
  attestations for the file's SHA-256, checks the Sigstore signature and
  certificate, and requires that this repository's Release workflow produced
  the file on a GitHub-hosted runner from that commit. A modified file, or one
  built anywhere else, fails. To verify later without network access, save the
  bundle once with `gh attestation download <asset> --repo "$repo"` and pass
  it with `--bundle`. The repository's **Actions → Attestations** page lists
  the same attestations.
- [ ] Install the source archive in a fresh environment with
      [`opam install`](package-boundary.md#offline-opam-source-builds) and
      build a consumer against it; link one prebuilt SDK archive as described
      in [prebuilt OCaml](prebuilt-ocaml.md).
- [ ] Record the release URL and this verification in the tracking issue.

## Qualification gates

These issues are blockers of [#510] or required by the support policy. Status
is as of 2026-10-17; check each issue before relying on it.

| Gate | Issue | Status |
| --- | --- | --- |
| Bridge fuzzing and lifecycle stress | [#506] | Open. Fixed-budget runs are in every build, and `stress.yml` reruns them weekly with large budgets. |
| Benchmark and sustained-load gates | [#507] | Open. The [benchmark harness](benchmark-harness.md) is exploratory and is not a load gate. |
| Worker upgrade and rollback | [#508] | Open. [The rehearsal](worker-upgrade-rollback.md) is a proposed procedure, not an accepted upgrade. |
| Security reporting and maintenance | [#509] | Open. There is no `SECURITY.md` yet. |
| Release pipeline rehearsal | [#510] | Open. The source-archive consumer build and signed provenance are automated; a maintainer's prerelease rehearsal is still required. |
| v1 support and compatibility policy | [#489] | Open. |
| Documentation reconciled with CI evidence | [#491] | Closed. |
| Test instrumentation removed from the production worker | [#502] | Closed. |

## Bad-release remediation

Because version tags cannot be moved or deleted, a bad version is never fixed
in place. It is superseded by a new version number.

### The Release workflow failed before publishing

The workflow uploads assets to a draft and publishes only after every check
passes, so nothing is public yet.

- **Transient failure** (runner loss, a flaky live test) with no draft
  created: rerun the workflow for the same tag. The tag still points at the
  approved commit.
- **A draft exists**: leave it unpublished and inspect it. A rerun will not
  replace an existing draft or release. Once the cause is understood, a
  maintainer may delete the unpublished draft (deleting a draft release does
  not touch the tag) and rerun.
- **The commit itself is bad** (a real test failure, wrong metadata): the tag
  cannot be retargeted, so that version number is spent. Do not publish it.
  Fix the problem on `master`, bump to the next number (for example
  `v0.1.0-rc.2`), and start the checklist again. Note the abandoned tag in the
  tracking issue so nobody dispatches it later.

### A published release is bad

- [ ] Do not delete the release, its assets or its tag. Keep them for audit,
      as [release preflight](release-preflight.md#publish-a-prerelease)
      requires; anyone who already downloaded an asset can still check it
      against `SHA256SUMS`.
- [ ] Edit the published release's notes on the release page to start with a
      warning: what is wrong, which versions are affected, and which version
      replaces it. Its `docs/releases/<tag>.md` is history and stays as
      published; the replacement's own notes describe the defect and the fix.
- [ ] Publish the replacement under a new version: the next release candidate
      (`v0.1.0-rc.2`) for a bad candidate, or the next patch
      (`v1.0.1`) for a bad stable release, following the full checklist.
- [ ] Link the replacement from the bad release's notes, and record both in
      the tracking issue.
- [ ] If the defect is a security issue, follow the private reporting
      procedure that [#509] will establish before describing it publicly.

GitHub has no way to yank a release; the warning in the notes and the newer
version are the remediation. A prerelease is never marked as GitHub's latest
release, so a bad candidate is not offered to users who follow "latest".
The project does not yet publish to opam-repository. If it does, a bad version
there should also be superseded rather than removed.

[#489]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/489
[#491]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/491
[#502]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/502
[#506]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/506
[#507]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/507
[#508]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/508
[#509]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/509
[#510]: https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/510

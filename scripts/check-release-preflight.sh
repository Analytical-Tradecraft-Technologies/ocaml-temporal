#!/bin/sh
set -eu

# This gate checks release inputs without compiling or contacting Docker.  A
# release must be reproducible from a clean Git tree, so it deliberately
# rejects local edits and generated build output before inspecting metadata.
root=${1:-.}
cd "$root"

fail() {
  echo "release preflight: $*" >&2
  exit 1
}

git rev-parse --git-dir >/dev/null 2>&1 || fail "run from a Git checkout"
git diff --quiet || fail "working tree has unstaged changes"
git diff --cached --quiet || fail "index has staged changes"
[ -z "$(git status --porcelain --untracked-files=all)" ] ||
  fail "working tree has untracked files"

[ -f .release-version ] || fail "missing .release-version"
version=$(sed 's/\r$//' .release-version)
[ -n "$version" ] || fail ".release-version is empty"
[ "$(printf '%s\n' "$version" | wc -l | tr -d ' ')" -eq 1 ] ||
  fail ".release-version must contain one line"
case "$version" in
  *[[:space:]]*) fail ".release-version must not contain whitespace" ;;
esac

[ -f temporal-sdk.opam ] || fail "missing temporal-sdk.opam"
[ -f temporal-sdk.opam.locked ] || fail "missing temporal-sdk.opam.locked"
[ -f dune-project ] || fail "missing dune-project"
[ -f README.md ] || fail "missing README.md"
[ -s LICENSE ] || fail "LICENSE is missing or empty"
[ -f rust/Cargo.lock ] || fail "rust/Cargo.lock is missing"

repository_url=https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal
repository_git=git+https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal.git

opam_field() {
  sed -n "s/^$1:[[:space:]]*\"\([^\"]*\)\"[[:space:]]*$/\1/p" temporal-sdk.opam | head -n 1
}
locked_field() {
  sed -n "s/^$1:[[:space:]]*\"\([^\"]*\)\"[[:space:]]*$/\1/p" temporal-sdk.opam.locked | head -n 1
}
opam_name=$(opam_field name)
opam_version=$(opam_field version)
[ "$opam_name" = temporal-sdk ] || fail "opam name must be temporal-sdk"
[ "$opam_version" = "$version" ] || fail "opam version does not match .release-version"
[ "$(opam_field maintainer)" = "Michael Fowlie" ] || fail "unexpected opam maintainer"
[ "$(opam_field authors)" = "Michael Fowlie" ] || fail "unexpected opam author"
[ "$(opam_field license)" = Apache-2.0 ] || fail "opam license must be Apache-2.0"
[ "$(opam_field homepage)" = "$repository_url" ] || fail "unexpected opam homepage"
[ "$(opam_field bug-reports)" = "$repository_url/issues" ] || fail "unexpected opam bug-reports URL"
[ "$(opam_field dev-repo)" = "$repository_git" ] || fail "unexpected opam dev-repo"
grep -F 'x-maintenance-intent: [ "(latest)" ]' temporal-sdk.opam >/dev/null ||
  fail "opam maintenance intent is missing"

[ "$(locked_field name)" = "$opam_name" ] || fail "locked opam name differs"
[ "$(locked_field version)" = "$opam_version" ] || fail "locked opam version differs"
[ "$(locked_field homepage)" = "$repository_url" ] || fail "locked opam homepage differs"
[ "$(locked_field bug-reports)" = "$repository_url/issues" ] || fail "locked opam bug-reports URL differs"
[ "$(locked_field dev-repo)" = "$repository_git" ] || fail "locked opam dev-repo differs"
grep -F '(name temporal-sdk)' dune-project >/dev/null || fail "dune package name differs"
grep -F '(source (github Analytical-Tradecraft-Technologies/ocaml-temporal))' dune-project >/dev/null ||
  fail "dune source repository differs"
grep -F '(authors "Michael Fowlie")' dune-project >/dev/null || fail "dune author differs"
grep -F '(maintainers "Michael Fowlie")' dune-project >/dev/null || fail "dune maintainer differs"
grep -F '(maintenance_intent "(latest)")' dune-project >/dev/null || fail "dune maintenance intent is missing"
grep -F 'Community-maintained and unofficial. Not affiliated with or endorsed by Temporal Technologies, Inc.' README.md >/dev/null ||
  fail "README disclaimer is missing"
grep -F "$repository_url" README.md >/dev/null || fail "README repository link is missing"
grep -Eiq 'experimental|pre-0\.1\.0' README.md || fail "README must identify the package as experimental"
grep -F 'Apache License' LICENSE >/dev/null || fail "LICENSE is not Apache-2.0"
grep -F "repository = \"$repository_url\"" rust/Cargo.toml >/dev/null ||
  fail "Cargo repository metadata differs"
grep -F "NAMESPACE = \"$repository_url/sbom/cargo\"" scripts/generate-cargo-sbom.py >/dev/null ||
  fail "Cargo SBOM namespace differs"

# Generated build trees are never valid release inputs.  Checking tracked
# paths catches accidental commits even when a clean checkout hides ignored
# local output.
if git ls-files | grep -E '(^|/)(_build|target)(/|$)|(^|/)\.DS_Store$' >/dev/null; then
  fail "generated build output is tracked"
fi

# Temporal Core is intentionally pinned by immutable revision.  A moving
# branch or tag would make two clean release checkouts resolve different code.
grep -E 'git = "https://github.com/temporalio/sdk-core.git"' rust/Cargo.toml >/dev/null ||
  fail "Temporal Core source is not the approved repository"
grep -E 'rev = "[0-9a-f]{40}"' rust/Cargo.toml >/dev/null ||
  fail "Temporal Core must be pinned by a 40-character revision"

[ -n "$(git ls-files)" ] || fail "Git source manifest is empty"
manifest_hash=$(git ls-files | LC_ALL=C sort | shasum -a 256 | awk '{print $1}')
printf '%s\n' "release preflight: ok"
printf '%s\n' "source manifest sha256: $manifest_hash"

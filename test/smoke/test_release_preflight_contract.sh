#!/bin/sh
set -eu

# The release checker itself is the source-only contract.  This wrapper keeps
# its Makefile invocation explicit and verifies the script remains portable
# POSIX shell before CI uses it on a clean checkout.
root=${1:-.}
cd "$root"
sh -n scripts/check-release-preflight.sh
sh test/smoke/test_cargo_sbom_contract.sh .
if [ -n "$(git status --porcelain --untracked-files=all)" ]; then
  echo "release preflight contract requires a clean checkout" >&2
  exit 1
fi
checkout_hash=$(sh scripts/check-release-preflight.sh . |
  sed -n 's/^source manifest sha256: //p')
[ -n "$checkout_hash" ] || { echo "release preflight printed no source manifest" >&2; exit 1; }

# A release preflight must reject package metadata that points to the former
# repository location, even when the checkout is otherwise clean. Build a
# committed fixture so the release gate reaches metadata validation instead of
# failing first on its clean-tree requirement.
fixture_root=$(mktemp -d "${TMPDIR:-/tmp}/temporal-release-preflight.XXXXXX")
fixture=$fixture_root/repository
host_git_config=$fixture_root/gitconfig
git_trace=$fixture_root/git-trace
mkdir "$fixture"
trap 'rm -rf "$fixture_root"' EXIT HUP INT TERM

# Exercise the fixture under a hostile but valid user configuration that makes
# every eligible Git command start detached automatic maintenance. Ephemeral
# fixture cleanup must never race a process that Git started in the fixture.
git config --file "$host_git_config" maintenance.auto true
git config --file "$host_git_config" maintenance.autoDetach true
git config --file "$host_git_config" maintenance.commit-graph.auto -1
: > "$git_trace"
export GIT_CONFIG_GLOBAL="$host_git_config"
export GIT_TRACE2_EVENT="$git_trace"
git archive --format=tar HEAD | tar -x -C "$fixture"
(
  cd "$fixture"
  git init -q
  # This repository is discarded immediately after its two commits. Disable
  # detached automatic maintenance so no Git process can outlive the fixture
  # and race the strict EXIT-trap cleanup.
  git config maintenance.auto false
  git config user.email release-contract@example.invalid
  git config user.name 'Release contract'
  git add -A
  git -c commit.gpgSign=false commit -q -m 'release fixture'
  base_commit=$(git rev-parse HEAD)
  sed 's#Analytical-Tradecraft-Technologies/ocaml-temporal#mfow/ocaml-temporal#g' \
    temporal-sdk.opam > temporal-sdk.opam.tmp
  mv temporal-sdk.opam.tmp temporal-sdk.opam
  git add temporal-sdk.opam
  git -c commit.gpgSign=false commit -q -m 'stale package metadata'
  if sh scripts/check-release-preflight.sh . >/dev/null 2>&1; then
    echo "release preflight accepted stale package repository metadata" >&2
    exit 1
  fi
  # Documentation and schema identifiers must not use the former location
  # either, even when package metadata is correct.
  git checkout -q HEAD~1 -- temporal-sdk.opam
  former_owner=mfow
  printf '\n[CI](https://github.com/%s/ocaml-temporal/actions)\n' \
    "$former_owner" >> docs/README.md
  git add temporal-sdk.opam docs/README.md
  git -c commit.gpgSign=false commit -q -m 'stale documentation link'
  if sh scripts/check-release-preflight.sh . >/dev/null 2>&1; then
    echo "release preflight accepted a stale repository URL in documentation" >&2
    exit 1
  fi

  # Stores the committed fixture's source manifest digest in $manifest. It is
  # called directly rather than through $(...) so a preflight rejection or a
  # missing digest exits this fixture subshell instead of yielding an empty
  # value that could make a "digest changed" assertion pass vacuously.
  read_manifest_hash() {
    manifest=$(sh scripts/check-release-preflight.sh . |
      sed -n 's/^source manifest sha256: //p')
    if [ -z "$manifest" ]; then
      echo "release preflight rejected the fixture or printed no source manifest" >&2
      exit 1
    fi
  }
  # Commits every fixture change so preflight sees a clean tree.
  commit_fixture() {
    git add -A
    git -c commit.gpgSign=false commit -q -m "$1"
  }

  # The manifest must fingerprint contents, not only path names (#827). The
  # fixture's first commit has the same tree as the checkout under test, so
  # its digest must be reproducible across the two clones.
  git reset -q --hard "$base_commit"
  read_manifest_hash
  base_hash=$manifest
  if [ "$base_hash" != "$checkout_hash" ]; then
    echo "source manifest differs between two checkouts of the same tree" >&2
    exit 1
  fi
  printf '\nContent-only change.\n' >> docs/README.md
  commit_fixture 'content change'
  read_manifest_hash
  if [ "$manifest" = "$base_hash" ]; then
    echo "source manifest did not change when file contents changed" >&2
    exit 1
  fi
  # Restoring the content in a new commit restores the digest: it depends on
  # the tree, not on commit metadata or history.
  git checkout -q "$base_commit" -- docs/README.md
  commit_fixture 'restore content'
  read_manifest_hash
  if [ "$manifest" != "$base_hash" ]; then
    echo "source manifest depends on commit history rather than tree contents" >&2
    exit 1
  fi
  # Only the executable bit changes here. Setting it in both the worktree and
  # the index keeps the tree clean whether or not core.fileMode is honoured.
  chmod +x LICENSE
  git update-index --chmod=+x LICENSE
  git -c commit.gpgSign=false commit -q -m 'mode change'
  read_manifest_hash
  if [ "$manifest" = "$base_hash" ]; then
    echo "source manifest did not change when a file mode changed" >&2
    exit 1
  fi

  # Maturity labelling follows the version (#827). A prerelease that drops
  # its experimental labels is rejected.
  git reset -q --hard "$base_commit"
  # Rewrites every maturity label in the README and package metadata.
  strip_experimental_labels() {
    for file in README.md dune-project temporal-sdk.opam temporal-sdk.opam.locked; do
      sed -e 's/[Ee]xperimental//g' -e 's/pre-`\{0,1\}0\.1\.0/stable/g' \
        "$file" > "$file.tmp"
      mv "$file.tmp" "$file"
    done
  }
  strip_experimental_labels
  commit_fixture 'unlabelled prerelease'
  if sh scripts/check-release-preflight.sh . >/dev/null 2>&1; then
    echo "release preflight accepted a prerelease not labelled experimental" >&2
    exit 1
  fi

  # A stable version that is still labelled experimental is rejected, and the
  # same version passes once the labels are removed.
  git reset -q --hard "$base_commit"
  current_version=$(cat .release-version)
  current_sdk_version=$(printf '%s\n' "$current_version" | sed 's/~/-/')
  for file in .release-version temporal-sdk.opam temporal-sdk.opam.locked; do
    sed "s/$current_version/1.0.0/" "$file" > "$file.tmp"
    mv "$file.tmp" "$file"
  done
  sed "s/SDK_VERSION: &str = \"$current_sdk_version\"/SDK_VERSION: \&str = \"1.0.0\"/" \
    rust/core-bridge/src/abi.rs > rust/core-bridge/src/abi.rs.tmp
  mv rust/core-bridge/src/abi.rs.tmp rust/core-bridge/src/abi.rs
  commit_fixture 'stable version still labelled experimental'
  if sh scripts/check-release-preflight.sh . >/dev/null 2>&1; then
    echo "release preflight accepted a stable release labelled experimental" >&2
    exit 1
  fi
  strip_experimental_labels
  commit_fixture 'stable labels'
  if ! sh scripts/check-release-preflight.sh . >/dev/null; then
    echo "release preflight rejected a correctly labelled stable release" >&2
    exit 1
  fi
)

if grep -F '"maintenance","run","--auto"' "$git_trace" >/dev/null; then
  echo "release preflight fixture started automatic Git maintenance" >&2
  exit 1
fi

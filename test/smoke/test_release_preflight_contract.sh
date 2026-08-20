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
sh scripts/check-release-preflight.sh .

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
  sed 's#Analytical-Tradecraft-Technologies/ocaml-temporal#mfow/ocaml-temporal#g' \
    temporal-sdk.opam > temporal-sdk.opam.tmp
  mv temporal-sdk.opam.tmp temporal-sdk.opam
  git add temporal-sdk.opam
  git -c commit.gpgSign=false commit -q -m 'stale package metadata'
  if sh scripts/check-release-preflight.sh . >/dev/null 2>&1; then
    echo "release preflight accepted stale package repository metadata" >&2
    exit 1
  fi
)

if grep -F '"maintenance","run","--auto"' "$git_trace" >/dev/null; then
  echo "release preflight fixture started automatic Git maintenance" >&2
  exit 1
fi

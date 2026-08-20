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
fixture=$(mktemp -d "${TMPDIR:-/tmp}/temporal-release-preflight.XXXXXX")
trap 'rm -rf "$fixture"' EXIT HUP INT TERM
git archive --format=tar HEAD | tar -x -C "$fixture"
(
  cd "$fixture"
  git init -q
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

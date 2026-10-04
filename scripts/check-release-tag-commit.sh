#!/bin/sh
set -eu

# A maintainer creates the protected release tag before dispatch. Resolve the
# remote ref afresh so an annotated tag is compared by its commit, not its tag
# object, and a missing or retargeted tag fails closed before publication.
if [ "$#" -ne 2 ]; then
  echo 'usage: check-release-tag-commit.sh TAG EXPECTED_COMMIT' >&2
  exit 2
fi
tag=$1
expected=$2
git check-ref-format "refs/tags/$tag"
expected_commit=$(git rev-parse --verify "$expected^{commit}")
if ! git fetch --no-tags --depth=1 origin "refs/tags/$tag"; then
  echo "Release tag $tag is missing or could not be fetched from origin." >&2
  exit 1
fi
actual_commit=$(git rev-parse --verify 'FETCH_HEAD^{commit}')
if [ "$actual_commit" != "$expected_commit" ]; then
  echo "Release tag $tag points to $actual_commit, expected $expected_commit." >&2
  exit 1
fi

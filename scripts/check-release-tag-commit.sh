#!/bin/sh
set -eu

# A maintainer creates the protected release tag before dispatch. Resolve the
# remote ref afresh so an annotated tag is compared by its commit, not its tag
# object, and a missing or retargeted tag fails closed before publication.
#
# With BRANCH, the expected commit must also be an ancestor of (or equal to)
# that branch on origin. The Release workflow runs from the tag itself (#1144),
# so later merges to master no longer invalidate an approved tag; the ancestor
# check keeps a tag on an unmerged commit from being released.
if [ "$#" -ne 2 ] && [ "$#" -ne 3 ]; then
  echo 'usage: check-release-tag-commit.sh TAG EXPECTED_COMMIT [BRANCH]' >&2
  exit 2
fi
tag=$1
expected=$2
branch=${3:-}
# git check-ref-format rejects "~" silently; name the OPAM/Git spelling mix-up
# so a maintainer who typed the OPAM version knows which tag to use instead.
case "$tag" in
  *~*)
    echo "Release tag $tag cannot contain '~' because Git refnames forbid it; use the hyphen spelling (for example v1.0.0-beta.1)." >&2
    exit 1
    ;;
esac
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

if [ -n "$branch" ]; then
  git check-ref-format "refs/heads/$branch"
  # The ancestry walk needs the history between the tag and the branch tip,
  # which a depth-1 CI checkout (or the depth-1 tag fetch above) lacks.
  # The arguments were copied above, so the positional parameters carry the
  # optional flag; "$@" expands it without word splitting or globbing.
  if [ "$(git rev-parse --is-shallow-repository)" = true ]; then
    set -- --unshallow
  else
    set --
  fi
  if ! git fetch --no-tags "$@" origin "refs/heads/$branch"; then
    echo "Release branch $branch could not be fetched from origin." >&2
    exit 1
  fi
  branch_commit=$(git rev-parse --verify 'FETCH_HEAD^{commit}')
  if ! git merge-base --is-ancestor "$expected_commit" "$branch_commit"; then
    echo "Release tag $tag points to $expected_commit, which is not on origin/$branch." >&2
    exit 1
  fi
fi

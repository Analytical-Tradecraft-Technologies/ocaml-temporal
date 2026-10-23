#!/bin/sh
set -eu

# Exercise the release ref check against a real remote, including an annotated
# tag whose object ID differs from the source commit and a network failure.
root=$(cd "${1:-.}" && pwd)
script="$root/scripts/check-release-tag-commit.sh"
# OPAM's Cygwin shell and Git for Windows interpret absolute /tmp differently.
# Keep paths passed to both tools relative to the same working directory.
cd "$root"
fixture=$(mktemp -d './.temporal-release-ref.XXXXXX')
trap 'rm -rf "$fixture"' EXIT HUP INT TERM
git init --bare "$fixture/origin.git" >/dev/null
git init -b master "$fixture/checkout" >/dev/null
# Neither short-lived repository may start detached maintenance while the
# fixture's EXIT trap removes its Git directories.
git --git-dir="$fixture/origin.git" config maintenance.auto false
git --git-dir="$fixture/origin.git" config gc.auto 0
git -C "$fixture/checkout" config maintenance.auto false
git -C "$fixture/checkout" config gc.auto 0
git -C "$fixture/checkout" config user.name 'Release test'
git -C "$fixture/checkout" config user.email 'release-test@example.invalid'
git -C "$fixture/checkout" config commit.gpgsign false
git -C "$fixture/checkout" config tag.gpgsign false
printf 'first\n' > "$fixture/checkout/source.txt"
git -C "$fixture/checkout" add source.txt
git -C "$fixture/checkout" commit -m first >/dev/null
git -C "$fixture/checkout" remote add origin ../origin.git
git -C "$fixture/checkout" push origin HEAD:master >/dev/null
first=$(git -C "$fixture/checkout" rev-parse HEAD)

# Both supported Git tag representations must peel to the same tested source.
git -C "$fixture/checkout" tag v1.0.0-light
git -C "$fixture/checkout" tag -a v1.0.0-annotated -m 'Release candidate'
git -C "$fixture/checkout" push origin refs/tags/v1.0.0-light refs/tags/v1.0.0-annotated >/dev/null
(cd "$fixture/checkout" && sh "$script" v1.0.0-light "$first")
(cd "$fixture/checkout" && sh "$script" v1.0.0-annotated "$first")

# The OPAM tilde spelling can never name a Git tag; it fails with guidance
# before any fetch is attempted.
if error=$(cd "$fixture/checkout" && sh "$script" 'v1.0.0~beta.1' "$first" 2>&1); then
  echo "release accepted a '~' tag that Git cannot represent" >&2
  exit 1
fi
case "$error" in
  *"cannot contain '~'"*) ;;
  *)
    echo "release gave an unclear '~' tag rejection: $error" >&2
    exit 1
    ;;
esac

# A missing tag or a tag on another commit must stop publication.
if (cd "$fixture/checkout" && sh "$script" v1.0.0-missing "$first" >/dev/null 2>&1); then
  echo 'release accepted a missing tag' >&2
  exit 1
fi
printf 'second\n' >> "$fixture/checkout/source.txt"
git -C "$fixture/checkout" commit -am second >/dev/null
second=$(git -C "$fixture/checkout" rev-parse HEAD)
git -C "$fixture/checkout" tag v1.0.0-wrong
git -C "$fixture/checkout" push origin refs/tags/v1.0.0-wrong >/dev/null
if (cd "$fixture/checkout" && sh "$script" v1.0.0-wrong "$first" >/dev/null 2>&1); then
  echo 'release accepted a tag on the wrong commit' >&2
  exit 1
fi
(cd "$fixture/checkout" && sh "$script" v1.0.0-wrong "$second")

# Releasing from the tag (#1144): an approved tag stays releasable after
# unrelated commits land on master, as long as its commit is on master.
# The earlier depth-1 tag fetches left this checkout shallow, which defeats
# Git's fast-forward check; the push is a fast-forward of master all the same.
git -C "$fixture/checkout" push --force origin HEAD:master >/dev/null
(cd "$fixture/checkout" && sh "$script" v1.0.0-annotated "$first" master)
(cd "$fixture/checkout" && sh "$script" v1.0.0-wrong "$second" master)
# The same check from a depth-1 clone of the tag, as the CI checkout is, must
# fetch enough history to prove ancestry rather than fail or pass blindly.
git clone --quiet --depth=1 --branch v1.0.0-annotated \
  "file://$PWD/$fixture/origin.git" "$fixture/shallow" 2>/dev/null
git -C "$fixture/shallow" config maintenance.auto false
git -C "$fixture/shallow" config gc.auto 0
(cd "$fixture/shallow" && sh "$script" v1.0.0-annotated "$first" master)
# A tag on a commit that never reached master must not be released, even
# though it peels to the expected commit; nor may an unfetchable branch.
git -C "$fixture/checkout" checkout --quiet -b unmerged
printf 'unmerged\n' >> "$fixture/checkout/source.txt"
git -C "$fixture/checkout" commit -qam unmerged
unmerged=$(git -C "$fixture/checkout" rev-parse HEAD)
git -C "$fixture/checkout" tag v1.0.0-unmerged
git -C "$fixture/checkout" push origin refs/tags/v1.0.0-unmerged >/dev/null
(cd "$fixture/checkout" && sh "$script" v1.0.0-unmerged "$unmerged")
if error=$(cd "$fixture/checkout" && sh "$script" v1.0.0-unmerged "$unmerged" master 2>&1); then
  echo 'release accepted a tag whose commit is not on master' >&2
  exit 1
fi
case "$error" in
  *"is not on origin/master"*) ;;
  *)
    echo "release gave an unclear unmerged-tag rejection: $error" >&2
    exit 1
    ;;
esac
if (cd "$fixture/checkout" && sh "$script" v1.0.0-annotated "$first" missing-branch >/dev/null 2>&1); then
  echo 'release accepted a missing release branch' >&2
  exit 1
fi
git -C "$fixture/checkout" checkout --quiet master

# A tag moved after the first check must be rejected even when the checkout
# still has its old local tag; publication checks the remote a second time.
git -C "$fixture/origin.git" update-ref refs/tags/v1.0.0-light "$second"
if (cd "$fixture/checkout" && sh "$script" v1.0.0-light "$first" >/dev/null 2>&1); then
  echo 'release accepted a moved remote tag' >&2
  exit 1
fi

# A cached local tag cannot substitute for a reachable remote.
git -C "$fixture/checkout" remote set-url origin ../unreachable.git
if (cd "$fixture/checkout" && sh "$script" v1.0.0-light "$first" >/dev/null 2>&1); then
  echo 'release accepted an unreachable remote tag' >&2
  exit 1
fi

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

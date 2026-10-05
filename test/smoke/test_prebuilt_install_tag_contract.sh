#!/bin/sh
set -eu

# The prebuilt-SDK guide tells consumers how to obtain the expected source
# commit for `ocaml-library-artifact.py unpack --commit`. Releases use
# annotated tags, whose `git/ref/tags` API object is the tag object rather
# than the commit, so the installer's provenance check would reject every
# release. GitHub cannot be queried from CI here, so this contract pins the
# documented command to the commits endpoint that peels tags, and checks with
# a real annotated tag that the documented offline form peels as well.
root=$(cd "${1:-.}" && pwd)
guide="$root/docs/reference/prebuilt-ocaml.md"

# Windows checkouts may materialize CRLF; compare the semantic text only.
resolution=$(tr -d '\015' < "$guide" | grep '^expected_commit=' || true)
if [ "$(printf '%s\n' "$resolution" | grep -c .)" -ne 1 ]; then
  echo "prebuilt-ocaml.md must document exactly one expected_commit resolution" >&2
  exit 1
fi
expected_resolution='expected_commit=$(gh api "repos/$repository/commits/refs/tags/$version" --jq '"'"'.sha'"'"')'
if [ "$resolution" != "$expected_resolution" ]; then
  echo "prebuilt-ocaml.md resolves the release tag without peeling it to a commit:" >&2
  echo "  $resolution" >&2
  exit 1
fi
# The guide's offline alternative must keep its peeling suffix.
tr -d '\015' < "$guide" | grep -F -q 'git rev-parse "$version^{commit}"' || {
  echo "prebuilt-ocaml.md lost its peeled offline tag resolution" >&2
  exit 1
}

# Demonstrate the distinction the guide relies on with both tag kinds.
cd "$root"
fixture=$(mktemp -d './.temporal-prebuilt-tag.XXXXXX')
trap 'rm -rf "$fixture"' EXIT HUP INT TERM
git init -q "$fixture/checkout"
# Prevent detached maintenance from racing the EXIT-trap removal.
git -C "$fixture/checkout" config maintenance.auto false
git -C "$fixture/checkout" config gc.auto 0
git -C "$fixture/checkout" config user.name 'Prebuilt tag test'
git -C "$fixture/checkout" config user.email 'prebuilt-tag-test@example.invalid'
git -C "$fixture/checkout" config commit.gpgsign false
git -C "$fixture/checkout" config tag.gpgsign false
git -C "$fixture/checkout" commit -q --allow-empty -m release
commit=$(git -C "$fixture/checkout" rev-parse HEAD)
git -C "$fixture/checkout" tag -a v1.0.0-annotated -m 'Release candidate'
git -C "$fixture/checkout" tag v1.0.0-light
if [ "$(git -C "$fixture/checkout" rev-parse refs/tags/v1.0.0-annotated)" = "$commit" ]; then
  echo "annotated tag fixture did not create a distinct tag object" >&2
  exit 1
fi
for version in v1.0.0-annotated v1.0.0-light; do
  peeled=$(git -C "$fixture/checkout" rev-parse "$version^{commit}")
  if [ "$peeled" != "$commit" ]; then
    echo "$version peeled to $peeled, expected $commit" >&2
    exit 1
  fi
done

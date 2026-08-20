#!/bin/sh
set -eu

# Exercise both the accepted release shape and failure modes that could
# otherwise publish a development manifest under a plausible-looking tag.
root=${1:-.}
script="$root/scripts/check-release-tag.sh"
fixture=$(mktemp -d "${TMPDIR:-/tmp}/temporal-release-tag.XXXXXX")
trap 'rm -rf "$fixture"' EXIT HUP INT TERM

for path in .release-version temporal-sdk.opam temporal-sdk.opam.locked; do
  cp "$root/$path" "$fixture/$path"
  sed 's/~dev/0.1.0/g' "$fixture/$path" > "$fixture/$path.tmp"
  mv "$fixture/$path.tmp" "$fixture/$path"
done

sh "$script" "$fixture" v0.1.0

# Prerelease tags are valid release candidates.  A familiar SemVer hyphen is
# normalized to OPAM's tilde ordering, so the package beta remains older than
# the eventual final release. A tag written with OPAM's tilde is accepted too.
for path in .release-version temporal-sdk.opam temporal-sdk.opam.locked; do
  cp "$root/$path" "$fixture/$path"
  sed 's/~dev/1.0.0~beta.1/g' "$fixture/$path" > "$fixture/$path.tmp"
  mv "$fixture/$path.tmp" "$fixture/$path"
done
sh "$script" "$fixture" v1.0.0-beta.1
sh "$script" "$fixture" v1.0.0~beta.1

if sh "$script" "$fixture" 0.1.0 >/dev/null 2>&1; then
  echo "release tag contract accepted a tag without v prefix" >&2
  exit 1
fi
if sh "$script" "$fixture" v0.1 >/dev/null 2>&1; then
  echo "release tag contract accepted a two-component version" >&2
  exit 1
fi
if sh "$script" "$fixture" v1.0.0- >/dev/null 2>&1; then
  echo "release tag contract accepted an empty prerelease suffix" >&2
  exit 1
fi
if sh "$script" "$root" v0.1.0 >/dev/null 2>&1; then
  echo "release tag contract accepted the development manifest" >&2
  exit 1
fi

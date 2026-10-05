#!/bin/sh
set -eu

# Exercise both the accepted release shape and failure modes that could
# otherwise publish a development manifest under a plausible-looking tag.
root=${1:-.}
script="$root/scripts/check-release-tag.sh"
fixture=$(mktemp -d "${TMPDIR:-/tmp}/temporal-release-tag.XXXXXX")
injection_marker_name="release-tag-injected-$$"
injection_marker="$root/$injection_marker_name"
trap 'rm -rf "$fixture"; rm -f "$injection_marker"' EXIT HUP INT TERM

# Fixtures must not depend on the repository still having its initial ~dev
# version: the same rejection tests also run on concrete release candidates.
set_fixture_version() {
  printf '%s\n' "$1" > "$fixture/.release-version"
  for path in temporal-sdk.opam temporal-sdk.opam.locked; do
    sed "s/^version:.*/version: \"$1\"/" "$root/$path" > "$fixture/$path"
  done
}
set_fixture_version 0.1.0

sh "$script" "$fixture" v0.1.0

# Prerelease tags are valid release candidates. The Git tag's SemVer hyphen is
# normalized to OPAM's tilde ordering, so the package beta remains older than
# the eventual final release. Assert the exact normalized version reported.
set_fixture_version 1.0.0~beta.1
output=$(sh "$script" "$fixture" v1.0.0-beta.1)
if [ "$output" != 'release tag check: ok (v1.0.0-beta.1 -> temporal-sdk 1.0.0~beta.1)' ]; then
  echo "release tag contract normalized v1.0.0-beta.1 unexpectedly: $output" >&2
  exit 1
fi

# Only the separator after the numeric core is converted; later hyphens are
# part of the prerelease identifier and must survive unchanged.
set_fixture_version 1.0.0~rc-2.1
output=$(sh "$script" "$fixture" v1.0.0-rc-2.1)
if [ "$output" != 'release tag check: ok (v1.0.0-rc-2.1 -> temporal-sdk 1.0.0~rc-2.1)' ]; then
  echo "release tag contract normalized v1.0.0-rc-2.1 unexpectedly: $output" >&2
  exit 1
fi
if sh "$script" "$fixture" v1.0.0-rc.2.1 >/dev/null 2>&1; then
  echo "release tag contract normalized a later hyphen in the prerelease" >&2
  exit 1
fi

# Git refnames cannot contain "~", so the OPAM spelling can never be a tag.
# Reject it explicitly, with guidance, even when the manifests match it.
set_fixture_version 1.0.0~beta.1
if git check-ref-format "refs/tags/v1.0.0~beta.1"; then
  echo "release tag contract assumes Git rejects '~' in tags" >&2
  exit 1
fi
if error=$(sh "$script" "$fixture" 'v1.0.0~beta.1' 2>&1); then
  echo "release tag contract accepted a '~' tag that Git cannot represent" >&2
  exit 1
fi
case "$error" in
  *"cannot contain '~'"*v1.0.0-beta.1*) ;;
  *)
    echo "release tag contract gave an unclear '~' rejection: $error" >&2
    exit 1
    ;;
esac

# A prerelease tag must not match final manifests, nor a final tag a prerelease.
if sh "$script" "$fixture" v1.0.0 >/dev/null 2>&1; then
  echo "release tag contract accepted a final tag for a prerelease manifest" >&2
  exit 1
fi
if sh "$script" "$fixture" v1.0.0-beta.2 >/dev/null 2>&1; then
  echo "release tag contract accepted a different prerelease suffix" >&2
  exit 1
fi

# Git accepts some shell metacharacters in ref names. A release tag must never
# become shell source when passed through the public Make target.
injection_tag=$(printf 'v1.0.0";touch>%s;#' "$injection_marker_name")
if ! git check-ref-format "refs/tags/$injection_tag"; then
  echo "release tag contract fixture is not a Git-valid tag" >&2
  exit 1
fi
if make --no-print-directory -C "$root" release-tag-check \
    RELEASE_TAG="$injection_tag" >/dev/null 2>&1; then
  echo "release tag contract accepted a shell metacharacter tag" >&2
  exit 1
fi
if [ -e "$injection_marker" ]; then
  echo "release tag contract executed untrusted tag content" >&2
  exit 1
fi

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
set_fixture_version '~dev'
if sh "$script" "$fixture" v0.1.0 >/dev/null 2>&1; then
  echo "release tag contract accepted the development manifest" >&2
  exit 1
fi

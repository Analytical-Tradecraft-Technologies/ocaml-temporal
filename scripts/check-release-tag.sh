#!/bin/sh
set -eu

# A release tag is an immutable claim about the package inputs. Keep this
# check independent of opam, Dune, Docker, and Rust so it can run before any
# expensive build and fail before a mismatched archive is published.
root=${1:-.}
tag=${2:-${RELEASE_TAG:-}}
cd "$root"

fail() {
  echo "release tag check: $*" >&2
  exit 1
}

[ -n "$tag" ] || fail "provide a tag as the second argument or RELEASE_TAG"
case "$tag" in
  v*) ;;
  *) fail "tag must start with v (got $tag)" ;;
esac

# Accept three numeric components plus an optional prerelease suffix. Git tags
# may use the familiar SemVer hyphen (v1.0.0-beta.1), but OPAM deliberately
# uses a tilde for prereleases so that a beta sorts before the final 1.0.0.
# Normalize the former to the latter before comparing package metadata.
tag_version=${tag#v}
core=$tag_version
prerelease=
separator=
case "$tag_version" in
  *-*)
    core=${tag_version%%-*}
    prerelease=${tag_version#*-}
    separator=-
    ;;
  *~*)
    core=${tag_version%%~*}
    prerelease=${tag_version#*~}
    separator='~'
    ;;
esac
case "$separator" in
  -|~)
    case "$prerelease" in
      '' | .* | *. | *..* | *[!A-Za-z0-9.-]* | *~*)
        fail "tag has an invalid prerelease suffix (got $tag)" ;;
    esac
    ;;
esac
case "$core" in
  *[!0-9.]* | .* | *. | *..*) fail "tag must be vMAJOR.MINOR.PATCH (got $tag)" ;;
esac
old_ifs=$IFS
IFS=.
set -- $core
IFS=$old_ifs
[ "$#" -eq 3 ] || fail "tag must be vMAJOR.MINOR.PATCH (got $tag)"
for component in "$@"; do
  [ -n "$component" ] || fail "tag contains an empty version component"
done

case "$separator" in
  -) version="$core~$prerelease" ;;
  *) version="$tag_version" ;;
esac

[ -f .release-version ] || fail "missing .release-version"
release_version=$(sed 's/\r$//' .release-version)
[ "$release_version" = "$version" ] ||
  fail ".release-version is $release_version, but tag is $version"
[ "$release_version" != "~dev" ] || fail "development version cannot be tagged"

[ -f temporal-sdk.opam ] || fail "missing temporal-sdk.opam"
[ -f temporal-sdk.opam.locked ] || fail "missing temporal-sdk.opam.locked"

opam_field() {
  sed -n "s/^$1:[[:space:]]*\"\([^\"]*\)\"[[:space:]]*$/\1/p" \
    temporal-sdk.opam | head -n 1
}
locked_field() {
  sed -n "s/^$1:[[:space:]]*\"\([^\"]*\)\"[[:space:]]*$/\1/p" \
    temporal-sdk.opam.locked | head -n 1
}
[ "$(opam_field name)" = temporal-sdk ] || fail "opam package name differs"
[ "$(opam_field version)" = "$version" ] || fail "opam version differs"
[ "$(locked_field name)" = temporal-sdk ] || fail "locked package name differs"
[ "$(locked_field version)" = "$version" ] || fail "locked package version differs"

printf '%s\n' "release tag check: ok ($tag -> temporal-sdk $version)"

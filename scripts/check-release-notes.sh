#!/bin/sh
set -eu

# Requires the reviewed, versioned release notes for one tag before the Release
# workflow builds or publishes it. The notes are committed with the version
# change as docs/releases/<tag>.md and published ahead of the workflow's
# generated asset description, so the release states its changes, breaking
# changes and migration steps, and the residual limits that
# docs/reference/v1-support-policy.md requires. The check is structural: it
# proves each required section exists and has content, not that the content
# is correct, which remains the release owner's sign-off.
#
# Usage: check-release-notes.sh ROOT TAG (TAG defaults to $RELEASE_TAG).
root=${1:-.}
tag=${2:-${RELEASE_TAG:-}}

fail() {
  echo "release notes check: $*" >&2
  exit 1
}

[ -n "$tag" ] || fail "provide a tag as the second argument or RELEASE_TAG"
# The tag becomes a path component; check-release-tag.sh validates its full
# shape, and this guard keeps an unvalidated tag from leaving docs/releases.
case "$tag" in
  v[0-9]*) ;;
  *) fail "tag must start with v and a digit (got $tag)" ;;
esac
case "$tag" in
  */* | *..* | *[!A-Za-z0-9.-]*) fail "tag is not a plain version tag (got $tag)" ;;
esac

notes="docs/releases/$tag.md"
[ -f "$root/$notes" ] ||
  fail "missing $notes; commit the reviewed release notes in the version PR"

# Counts the non-blank lines of one "## " section, up to the next "## ".
# A trailing carriage return is ignored, so notes checked out with CRLF line
# endings (the Windows default with core.autocrlf) read the same.
section_lines() {
  awk -v heading="## $1" '
    { sub(/\r$/, "") }
    $0 == heading { inside = 1; found = 1; next }
    inside && /^## / { inside = 0 }
    inside && $0 !~ /^[[:space:]]*$/ { lines++ }
    END { if (!found) print -1; else print lines + 0 }' "$root/$notes"
}

for section in 'Changes' 'Breaking changes and migration' 'Residual limits'; do
  count=$(section_lines "$section")
  [ "$count" -ge 0 ] || fail "$notes has no '## $section' section"
  [ "$count" -gt 0 ] || fail "$notes has an empty '## $section' section"
done

# Placeholders must be resolved before a release is published.
if grep -Eqiw 'TODO|TBD|FIXME' "$root/$notes"; then
  fail "$notes still contains a TODO, TBD or FIXME placeholder"
fi

printf '%s\n' "release notes check: ok ($notes)"

#!/bin/sh
set -eu

# Docker-free contract for versioned release notes (#510). The Release
# workflow must refuse a tag whose reviewed notes are missing or incomplete,
# and must publish those notes rather than only its generated asset text.
root=${1:-.}
script="$root/scripts/check-release-notes.sh"
workflow="$root/.github/workflows/release.yml"
fixture=$(mktemp -d "${TMPDIR:-/tmp}/temporal-release-notes.XXXXXX")
trap 'rm -rf "$fixture"' EXIT HUP INT TERM

fail() {
  echo "release notes contract: $*" >&2
  exit 1
}

# Writes complete notes for one tag into the fixture tree.
write_notes() {
  mkdir -p "$fixture/docs/releases"
  cat >"$fixture/docs/releases/$1.md" <<'EOF'
# v1.0.0

## Changes

- Something changed.

## Breaking changes and migration

None.

## Residual limits

- Activities run one at a time per worker.
EOF
}

# Expects the checker to reject the fixture for TAG with a message naming WHY.
expect_rejected() {
  tag=$1
  why=$2
  if error=$(sh "$script" "$fixture" "$tag" 2>&1); then
    fail "accepted notes that should fail: $why"
  fi
  case "$error" in
    *"$why"*) ;;
    *) fail "rejection did not mention '$why': $error" ;;
  esac
}

write_notes v1.0.0
output=$(sh "$script" "$fixture" v1.0.0)
[ "$output" = 'release notes check: ok (docs/releases/v1.0.0.md)' ] ||
  fail "unexpected success output: $output"

# The same notes with CRLF line endings, as a Windows checkout with
# core.autocrlf produces, must also pass.
write_notes v1.0.2
sed 's/$/\r/' "$fixture/docs/releases/v1.0.2.md" >"$fixture/crlf.tmp"
mv "$fixture/crlf.tmp" "$fixture/docs/releases/v1.0.2.md"
grep -q "$(printf '\r')" "$fixture/docs/releases/v1.0.2.md" ||
  fail "the CRLF fixture has no carriage returns"
output=$(sh "$script" "$fixture" v1.0.2) ||
  fail "rejected notes with CRLF line endings"
[ "$output" = 'release notes check: ok (docs/releases/v1.0.2.md)' ] ||
  fail "unexpected CRLF success output: $output"

expect_rejected v1.0.1 'missing docs/releases/v1.0.1.md'
expect_rejected '../v1.0.0' 'tag must start with v'
expect_rejected 'v1.0.0/x' 'not a plain version tag'

sed '/^## Residual limits$/,$d' "$fixture/docs/releases/v1.0.0.md" \
  >"$fixture/notes.tmp"
mv "$fixture/notes.tmp" "$fixture/docs/releases/v1.0.0.md"
expect_rejected v1.0.0 "no '## Residual limits' section"

write_notes v1.0.0
printf '\n## Residual limits\n' >"$fixture/tail.tmp"
sed '/^## Residual limits$/,$d' "$fixture/docs/releases/v1.0.0.md" \
  | cat - "$fixture/tail.tmp" >"$fixture/notes.tmp"
mv "$fixture/notes.tmp" "$fixture/docs/releases/v1.0.0.md"
expect_rejected v1.0.0 "empty '## Residual limits' section"

write_notes v1.0.0
printf '\nTODO: describe the shutdown limit.\n' >>"$fixture/docs/releases/v1.0.0.md"
expect_rejected v1.0.0 'placeholder'

# The checked-in version must have its notes, so a tag can be cut from master.
version=$(sed 's/\r$//' "$root/.release-version")
case "$version" in
  *~dev) ;;
  *)
    tag="v$(printf '%s\n' "$version" | sed 's/~/-/')"
    sh "$script" "$root" "$tag" >/dev/null ||
      fail "the checked-in version $version has no complete docs/releases/$tag.md"
    ;;
esac

# The workflow checks the notes before building and publishes them first.
validate=$(sed -n '/^  validate:/,/^  build:/p' "$workflow")
printf '%s\n' "$validate" |
  grep -Fqx '          sh scripts/check-release-notes.sh . "$RELEASE_TAG"' ||
  fail "the validate job must check the release notes"
publish=$(sed -n '/^  publish:/,$p' "$workflow")
printf '%s\n' "$publish" |
  grep -Fqx '          cp "docs/releases/$RELEASE_TAG.md" "$RUNNER_TEMP/release-notes.md"' ||
  fail "the publish job must start the release notes with docs/releases/<tag>.md"
printf '%s\n' "$publish" |
  grep -Fqx '          cat >> "$RUNNER_TEMP/release-notes.md" <<EOF' ||
  fail "the publish job must append, not overwrite, its generated notes"
if printf '%s\n' "$publish" | grep -Fq 'cat > "$RUNNER_TEMP/release-notes.md"'; then
  fail "the publish job overwrites the reviewed release notes"
fi
printf '%s\n' "$publish" |
  grep -Fq -- '--notes-file "$RUNNER_TEMP/release-notes.md"' ||
  fail "the release must be created from the assembled notes file"

echo 'release notes contract: ok'

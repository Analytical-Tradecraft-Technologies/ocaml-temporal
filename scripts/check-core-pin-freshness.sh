#!/bin/sh
# Compares the Temporal Core pin in rust/Cargo.toml with the commit of the
# latest stable temporalio/sdk-core release (issue #824).
#
# Dependabot never proposes Core upgrades because Core is a `rev =` git
# dependency, so this check is what notices that upstream has released past
# the pin. It only reads public GitHub metadata through the `gh` CLI (which
# needs GH_TOKEN in CI) and never changes the pin: an upgrade still needs the
# licence, ABI, replay and compatibility evidence listed under "Temporal Core
# pin upgrades" in docs/dependencies.md.
#
# Usage: check-core-pin-freshness.sh [source-root]
#
# Prints a short Markdown report on stdout. Exit status:
#   0  the pin is the latest release commit or already contains it;
#   1  the latest release is not contained in the pin (upgrade due);
#   2  the pin or the upstream metadata could not be read.
# CORE_UPSTREAM_REPO overrides the upstream repository (tests use it).
set -eu

root=${1:-.}
repo=${CORE_UPSTREAM_REPO:-temporalio/sdk-core}

# Every Core crate shares one revision; check-dependency-inventory.sh enforces
# that, so the first `rev =` is the pin.
pin=$(sed -n 's/^temporalio-sdk-core = .*rev = "\([0-9a-f]*\)".*/\1/p' "$root/rust/Cargo.toml" | head -n 1)
case "$pin" in
  ????????????????????????????????????????) ;;
  *)
    echo "could not read a 40-hex Temporal Core rev from $root/rust/Cargo.toml" >&2
    exit 2
    ;;
esac

# `releases/latest` excludes drafts and prereleases. Upstream publishes paired
# tags for one commit (for example v1.1.0 and core-v0.10.0), so the release is
# compared by commit rather than by tag name.
if ! tag=$(gh api "repos/$repo/releases/latest" --jq '.tag_name'); then
  echo "could not read the latest $repo release" >&2
  exit 2
fi
if ! release=$(gh api "repos/$repo/commits/$tag" --jq '.sha'); then
  echo "could not resolve $repo release $tag to a commit" >&2
  exit 2
fi
# The compare API reports how the release commit relates to the pin:
# `identical`, `behind` (release is an ancestor of the pin), `ahead` (pin is
# an ancestor of the release) or `diverged`.
if ! comparison=$(gh api "repos/$repo/compare/$pin...$release" \
  --jq '.status + " " + (.ahead_by | tostring) + " " + (.behind_by | tostring)'); then
  echo "could not compare $repo $pin with $release" >&2
  exit 2
fi
# The compare API answers "<status> <ahead_by> <behind_by>"; split it with read
# rather than unquoted word splitting so no field can glob-expand.
read -r status ahead_by _behind_by <<COMPARISON
$comparison
COMPARISON

short_pin=$(printf '%.12s' "$pin")
short_release=$(printf '%.12s' "$release")
echo "## Temporal Core pin freshness"
echo
echo "- Pin: \`$pin\` (rust/Cargo.toml)"
echo "- Latest $repo release: \`$tag\` at \`$release\`"
case "$status" in
  identical | behind)
    echo "- Status: current (\`$status\`); the pin contains the latest release."
    exit 0
    ;;
  ahead | diverged)
    echo "- Status: upgrade due; the pin is $ahead_by commit(s) behind \`$tag\` (\`$status\`)."
    echo
    echo "Compare: https://github.com/$repo/compare/$short_pin...$short_release"
    echo
    echo "Follow the \"Temporal Core pin upgrades\" checklist in docs/dependencies.md."
    exit 1
    ;;
  *)
    echo "unexpected compare status '$status' for $repo $pin...$release" >&2
    exit 2
    ;;
esac

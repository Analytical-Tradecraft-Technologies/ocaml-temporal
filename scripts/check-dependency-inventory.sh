#!/bin/sh
set -eu

# Compare the tables in docs/dependencies.md with the files that actually pin
# each dependency, so a lock, digest, action, or tool bump cannot leave the
# licence inventory silently stale (#783). Each check derives the expected
# rows from the source, extracts the matching columns of one documented table,
# and fails with a diff naming the table when the two sets differ.
#
# Only names, versions, and pins are compared; licence columns are reviewed
# from package metadata when a row changes and enforced by the OPAM and Cargo
# licence gates. Only POSIX tools are used, so the check needs no OPAM, Cargo,
# or Docker and runs in milliseconds.
#
# Usage: check-dependency-inventory.sh [repository-root]
root=${1:-$(CDPATH="" cd -- "$(dirname "$0")/.." && pwd)}
doc="$root/docs/dependencies.md"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
failed=0

# Print the data rows of every Markdown table under one exact heading line, up
# to the next heading of any level. Cells are trimmed, stripped of backticks,
# and joined with tabs; the header row and its separator are skipped. CRLF
# checkouts are normalized so the check is independent of Git's line endings.
doc_table() {
  awk -v heading="$1" '
    { sub(/\r$/, "") }
    $0 == heading { active = 1; next }
    active && /^#/ { exit }
    active && /^\|/ {
      if (!in_table) { in_table = 1; next }
      if ($0 ~ /^\|[-:| ]+\|$/) next
      line = $0
      sub(/^\|[ ]*/, "", line)
      sub(/[ ]*\|$/, "", line)
      count = split(line, cells, /[ ]*\|[ ]*/)
      out = ""
      for (i = 1; i <= count; i++) {
        gsub(/`/, "", cells[i])
        out = out (i > 1 ? "\t" : "") cells[i]
      }
      print out
      next
    }
    { in_table = 0 }
  ' "$doc"
}

# Select tab-separated columns (awk field numbers) from standard input.
columns() {
  awk -F '\t' -v list="$1" '
    BEGIN { n = split(list, wanted, ",") }
    {
      out = ""
      for (i = 1; i <= n; i++) out = out (i > 1 ? "\t" : "") $(wanted[i])
      print out
    }
  '
}

# Compare the expected rows (from source files) with the documented rows as
# sorted sets. Both files are written by the caller under $scratch; a
# difference is reported with the table name and the fix to apply.
compare() {
  name=$1
  LC_ALL=C sort -u "$scratch/expected" > "$scratch/expected.sorted"
  LC_ALL=C sort -u "$scratch/documented" > "$scratch/documented.sorted"
  if [ ! -s "$scratch/expected.sorted" ]; then
    echo "dependency inventory: no source rows extracted for '$name'; update the extractor" >&2
    failed=1
  elif ! cmp -s "$scratch/expected.sorted" "$scratch/documented.sorted"; then
    echo "dependency inventory drift in '$name' (< source, > docs/dependencies.md):" >&2
    diff "$scratch/expected.sorted" "$scratch/documented.sorted" >&2 || true
    failed=1
  fi
}

# Require one exact text fragment in the inventory for a scalar fact.
require_text() {
  name=$1
  needle=$2
  if ! grep -F -- "$needle" "$doc" >/dev/null; then
    echo "dependency inventory drift in '$name': docs/dependencies.md must contain: $needle" >&2
    failed=1
  fi
}

# OPAM: the project version plus every exact `{= "version"}` constraint in
# the depends block of the solved lock.
{
  awk -F '"' '/^version: "/ { print "temporal-sdk\t" $2 }' "$root/temporal-sdk.opam"
  awk '
    /^depends: \[/ { active = 1; next }
    active && /^\]/ { exit }
    active {
      line = $0
      if (match(line, /"[^"]+" \{= "[^"]+"\}/)) {
        split(substr(line, RSTART, RLENGTH), parts, "\"")
        print parts[2] "\t" parts[4]
      }
    }
  ' "$root/temporal-sdk.opam.locked"
} > "$scratch/expected"
doc_table '## Locked OCaml closure' | columns 1,2 > "$scratch/documented"
compare 'Locked OCaml closure'

sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$root/scripts/opam-lock-overrides.txt" \
  | awk '{ print $1 "\t" $2 "\t" $3 }' > "$scratch/expected"
doc_table '#### Per-compiler lock overrides' | columns 1,2,3 > "$scratch/documented"
compare 'Per-compiler lock overrides'

sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$root/scripts/docs-tools.locked" \
  | awk '{ print $1 "\t" $2 }' > "$scratch/expected"
doc_table '## CI-only documentation tooling' | columns 1,2 > "$scratch/documented"
compare 'CI-only documentation tooling'

# Container images: every digest-pinned reference that a build, Compose
# service, or workflow pulls. Recorded provenance in test fixtures is not a
# pin and is deliberately excluded. Documented rows put the image in column 1
# and its digest in column 2 (the builder table's stage sits in column 2).
for source in "$root/Dockerfile.dev" "$root/Dockerfile.rust-ci" \
  "$root/test/integration/temporal/compose.yaml" \
  "$root"/.github/workflows/*.yml "$root"/.github/actions/*/action.yml; do
  [ -f "$source" ] || continue
  grep -oE '[A-Za-z0-9./_-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}' "$source" || true
done | awk '{ sub(/@/, "\t"); print }' > "$scratch/expected"
{
  doc_table '## Builder image tooling' | columns 1,3
  doc_table '## Toolchain and CI images' | columns 1,2
  doc_table '## Local integration service images' | columns 1,2
} > "$scratch/documented"
compare 'Builder, toolchain and CI, and local integration service images'

channel=$(sed -n 's/^channel = "\(.*\)"$/\1/p' "$root/rust/rust-toolchain.toml")
require_text 'Rust toolchain' "Rust toolchain \`$channel\`"

# Cargo: the lock's package count (the bridge plus its dependencies) and the
# single Temporal Core revision every git dependency must share.
packages=$(grep -c '^\[\[package\]\]$' "$root/rust/Cargo.lock")
require_text 'Locked Cargo closure' "\`rust/Cargo.lock\` locks $packages packages: the project bridge and $((packages - 1)) dependencies"
revisions=$(sed -n 's/.*rev = "\([0-9a-f]*\)".*/\1/p' "$root/rust/Cargo.toml" | LC_ALL=C sort -u)
if [ "$(printf '%s\n' "$revisions" | wc -l | tr -d ' ')" != 1 ] || [ -z "$revisions" ]; then
  echo "dependency inventory: rust/Cargo.toml must pin exactly one Temporal Core revision" >&2
  failed=1
else
  require_text 'Locked Cargo closure' "Temporal Core commit \`$revisions\`"
fi

# Direct Rust dependencies: workspace requirement (or "Core revision" for the
# git-pinned Core packages), every locked version in lock order, and whether
# the bridge uses the crate as a normal and/or dev dependency.
awk '
  /^\[\[package\]\]$/ { name = ""; next }
  /^name = "/ { split($0, q, "\""); name = q[2]; next }
  /^version = "/ && name != "" {
    split($0, q, "\"")
    locked[name] = (name in locked) ? locked[name] ", " q[2] : q[2]
  }
  END { for (n in locked) print n "\t" locked[n] }
' "$root/rust/Cargo.lock" > "$scratch/locked"
awk '
  /^\[/ { section = $0; next }
  /^[A-Za-z0-9_-]+([.=]|[ ]*=)/ {
    name = $0
    sub(/[ .=].*/, "", name)
    if (section == "[dependencies]") normal[name] = 1
    if (section == "[dev-dependencies]") dev[name] = 1
  }
  END {
    for (n in normal) kind[n] = "normal"
    for (n in dev) kind[n] = (n in kind) ? kind[n] ", dev" : "dev"
    for (n in kind) print n "\t" kind[n]
  }
' "$root/rust/core-bridge/Cargo.toml" > "$scratch/kinds"
awk '
  /^\[/ { active = ($0 == "[workspace.dependencies]"); next }
  active && /^[A-Za-z0-9_-]+[ ]*=/ {
    name = $0
    sub(/[ ]*=.*/, "", name)
    requirement = ""
    if ($0 ~ /rev = "/) requirement = "Core revision"
    else if (match($0, /version = "[^"]+"/)) {
      requirement = substr($0, RSTART + 11, RLENGTH - 12)
    } else if (match($0, /= "[^"]+"/)) {
      requirement = substr($0, RSTART + 3, RLENGTH - 4)
    }
    print name "\t" requirement
  }
' "$root/rust/Cargo.toml" > "$scratch/requirements"
awk -F '\t' '
  FILENAME == ARGV[1] { locked[$1] = $2; next }
  FILENAME == ARGV[2] { kind[$1] = $2; next }
  {
    print $1 "\t" $2 "\t" (($1 in locked) ? locked[$1] : "not locked") "\t" \
      (($1 in kind) ? kind[$1] : "unused by the bridge")
  }
' "$scratch/locked" "$scratch/kinds" "$scratch/requirements" > "$scratch/expected"
doc_table '### Direct Rust dependencies' | columns 1,2,3,4 > "$scratch/documented"
compare 'Direct Rust dependencies'

# CI-only quality tools: the install-action tool list is the CI source; the
# Makefile defaults that `make quality` enforces must name the same versions.
grep -h 'tool: ' "$root"/.github/workflows/*.yml \
  | sed 's/.*tool: //' | tr ',' '\n' | awk '{ sub(/@/, "\t"); print }' \
  > "$scratch/expected"
doc_table '## CI-only quality tools' | columns 1,2 > "$scratch/documented"
compare 'CI-only quality tools'
awk '
  $1 == "QUALITY_CARGO_DENY_VERSION" && $2 == "?=" { print "cargo-deny\t" $3 }
  $1 == "QUALITY_CARGO_MACHETE_VERSION" && $2 == "?=" { print "cargo-machete\t" $3 }
  $1 == "QUALITY_TYPOS_VERSION" && $2 == "?=" { print "typos\t" $3 }
' "$root/Makefile" > "$scratch/expected"
compare 'CI-only quality tools (Makefile QUALITY_*_VERSION)'

# GitHub Actions: every external `uses:` reference with its ref and the
# release named in the trailing comment (the ref itself for a bare tag).
grep -h 'uses:' "$root"/.github/workflows/*.yml "$root"/.github/actions/*/action.yml \
  | sed 's/^[ -]*uses:[ ]*//' | grep -v '^\./' \
  | awk '{
      split($1, ref, "@")
      release = ref[2]
      if ($2 == "#" && $3 != "") release = $3
      print ref[1] "\t" ref[2] "\t" release
    }' > "$scratch/expected"
doc_table '## GitHub Actions' | columns 1,2,3 > "$scratch/documented"
compare 'GitHub Actions'

if [ "$failed" -ne 0 ]; then
  echo "update docs/dependencies.md (and review each changed licence) to match the pinned sources" >&2
  exit 1
fi
echo "dependency inventory matches the locked and pinned sources"

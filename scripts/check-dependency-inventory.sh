#!/bin/sh
set -eu

# Compare the tables in docs/dependencies.md with the files that actually pin
# each dependency, so adding or removing a dependency cannot leave the licence
# inventory silently incomplete (#783). Each check derives the expected rows
# from the source, extracts the matching columns of one documented table, and
# fails with a diff naming the table when the two sets differ.
#
# Only dependency names (and the bridge's dependency kinds) are compared.
# Versions, refs, tags, and digests live solely in the pinning files, so a
# version-only bump such as a Dependabot PR needs no inventory edit. The
# exceptions are the Temporal Core revision, whose upgrade needs recorded
# evidence, and the documentation-tool lock, which no licence gate audits. Licence columns are reviewed from package metadata when a row is
# added, and every locked version stays enforced by the OPAM, Cargo, and
# Python licence gates. Only POSIX tools are used, so the check needs no OPAM,
# Cargo, or Docker and runs in milliseconds.
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

# OPAM: the project package plus every exact `{= "version"}` constraint in the
# depends block of the solved lock. Only the package names are inventoried.
{
  echo "temporal-sdk"
  awk '
    /^depends: \[/ { active = 1; next }
    active && /^\]/ { exit }
    active {
      line = $0
      if (match(line, /"[^"]+" \{= "[^"]+"\}/)) {
        split(substr(line, RSTART, RLENGTH), parts, "\"")
        print parts[2]
      }
    }
  ' "$root/temporal-sdk.opam.locked"
} > "$scratch/expected"
cp "$scratch/expected" "$scratch/opam-closure"
doc_table '## Locked OCaml closure' | columns 1 > "$scratch/documented"
compare 'Locked OCaml closure'

# Overrides are identified by compiler series and package; the replacement
# version lives only in the overrides file.
sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$root/scripts/opam-lock-overrides.txt" \
  | awk '{ print $1 "\t" $2 }' > "$scratch/expected"
doc_table '#### Per-compiler lock overrides' | columns 1,2 > "$scratch/documented"
compare 'Per-compiler lock overrides'

# Documentation tools keep their exact versions: no licence gate reads
# scripts/docs-tools.locked, so this table is the only licence review of that
# closure and must change with every tool-lock edit. Dependabot does not manage
# the file.
sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "$root/scripts/docs-tools.locked" \
  | awk '{ print $1 "\t" $2 }' > "$scratch/expected"
cut -f1 "$scratch/expected" > "$scratch/docs-tools"
doc_table '## CI-only documentation tooling' | columns 1,2 > "$scratch/documented"
compare 'CI-only documentation tooling'

# CI-only official Temporal Python SDK closure for the interoperability
# regression (#517): the package of every `name==version` requirement line of
# the hash-locked file that test/integration/sdk_interop/python/run-driver.sh
# installs.
awk -F '==' '
  { sub(/\r$/, "") }
  /^[A-Za-z0-9._-]+==/ { print $1 }
' "$root/test/integration/sdk_interop/python/requirements.lock" > "$scratch/expected"
doc_table '## CI-only interoperability tooling' | columns 1 > "$scratch/documented"
compare 'CI-only interoperability tooling'

# Linking-exception scope: the exact packages admitted under the OCaml
# linking exception. The names are the reviewed policy (check-licenses.sh for
# the compiler and ocamlbuild, the odoc review for the docs tools), so adding
# or dropping one fails until the policy table is reviewed. A package missing
# from its lock yields no source row and so shows as a stale documented row.
{
  awk '$1 == "ocaml" || $1 == "ocaml-base-compiler" || $1 == "ocamlbuild"' \
    "$scratch/opam-closure"
  awk '$1 == "tyxml" || $1 == "re" || $1 == "camlp-streams"' \
    "$scratch/docs-tools"
} > "$scratch/expected"
doc_table '## Linking-exception scope' | columns 1 > "$scratch/documented"
compare 'Linking-exception scope'

# Container images: the repository of every digest-pinned reference that a
# build, Compose service, or workflow pulls. Tags and digests are refreshed by
# Dependabot in the source files and are not repeated here; a documented image
# keeps its tag for readability, so both sides drop everything after the first
# `:`. Recorded provenance in test fixtures is not a pin and is deliberately
# excluded.
for source in "$root/Dockerfile.dev" "$root"/docker/ocaml-*/Dockerfile \
  "$root/Dockerfile.rust-ci" \
  "$root/test/integration/temporal/compose.yaml" \
  "$root"/.github/workflows/*.yml "$root"/.github/actions/*/action.yml; do
  [ -f "$source" ] || continue
  grep -oE '[A-Za-z0-9./_-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}' "$source" || true
done | sed 's/:.*//' > "$scratch/expected"
{
  doc_table '## Builder image tooling' | columns 1
  doc_table '## Toolchain and CI images' | columns 1
  doc_table '## Local integration service images' | columns 1
} | sed 's/:.*//' > "$scratch/documented"
compare 'Builder, toolchain and CI, and local integration service images'

# Cargo: the single Temporal Core revision every git dependency must share.
# Unlike ordinary version bumps, a Core upgrade requires licence, ABI, replay,
# and compatibility evidence (see "Temporal Core pin upgrades"), so the
# documented commit must change with it.
revisions=$(sed -n 's/.*rev = "\([0-9a-f]*\)".*/\1/p' "$root/rust/Cargo.toml" | LC_ALL=C sort -u)
if [ "$(printf '%s\n' "$revisions" | wc -l | tr -d ' ')" != 1 ] || [ -z "$revisions" ]; then
  echo "dependency inventory: rust/Cargo.toml must pin exactly one Temporal Core revision" >&2
  failed=1
else
  require_text 'Locked Cargo closure' "Temporal Core commit \`$revisions\`"
fi

# Direct Rust dependencies: every crate in the workspace dependency table or
# declared by the bridge, with whether the bridge uses it as a normal and/or
# dev dependency. Requirements and locked versions stay in the Cargo files.
awk '
  /^\[/ { active = ($0 == "[workspace.dependencies]"); next }
  active && /^[A-Za-z0-9_-]+[ ]*=/ { line = $0; sub(/[ .=].*/, "", line); print line }
' "$root/rust/Cargo.toml" > "$scratch/workspace"
# One line per bridge declaration: crate and kind. Dependency tables this
# extractor cannot attribute (build, target-specific, or
# `[dependencies.<crate>]` sub-tables) fail closed rather than escaping.
if ! awk '
  /^\[/ {
    section = $0
    if (section ~ /dependencies/ && section != "[dependencies]" \
        && section != "[dev-dependencies]") {
      print "unsupported bridge dependency table " section > "/dev/stderr"
      unsupported = 1
    }
    next
  }
  /^[A-Za-z0-9_-]+([.=]|[ ]*=)/ {
    kind = ""
    if (section == "[dependencies]") kind = "normal"
    if (section == "[dev-dependencies]") kind = "dev"
    if (kind == "") next
    line = $0
    sub(/[ .=].*/, "", line)
    print line "\t" kind
  }
  END { if (unsupported) exit 1 }
' "$root/rust/core-bridge/Cargo.toml" > "$scratch/declarations"; then
  echo "dependency inventory: rust/core-bridge/Cargo.toml uses a dependency table the inventory cannot attribute; extend scripts/check-dependency-inventory.sh" >&2
  failed=1
fi
# Union of the workspace table and the bridge declarations. Kinds accumulate
# in manifest order ([dependencies] precedes [dev-dependencies], giving
# "normal, dev").
awk -F '\t' '
  FILENAME == ARGV[1] { names[$1] = 1; next }
  {
    names[$1] = 1
    kind[$1] = ($1 in kind) ? kind[$1] ", " $2 : $2
  }
  END {
    for (n in names) print n "\t" ((n in kind) ? kind[n] : "unused by the bridge")
  }
' "$scratch/workspace" "$scratch/declarations" > "$scratch/expected"
doc_table '### Direct Rust dependencies' | columns 1,2 > "$scratch/documented"
compare 'Direct Rust dependencies'

# CI-only quality tools: the install-action tool list is the CI source; the
# Makefile defaults that `make quality` enforces must name the same tools.
# Matching versions between the two is the Makefile's own concern.
grep -h 'tool: ' "$root"/.github/workflows/*.yml \
  | sed 's/.*tool: //' | tr ',' '\n' | sed 's/@.*//' \
  > "$scratch/expected"
doc_table '## CI-only quality tools' | columns 1 > "$scratch/documented"
compare 'CI-only quality tools'
awk '
  $1 == "QUALITY_CARGO_DENY_VERSION" && $2 == "?=" { print "cargo-deny" }
  $1 == "QUALITY_CARGO_MACHETE_VERSION" && $2 == "?=" { print "cargo-machete" }
  $1 == "QUALITY_TYPOS_VERSION" && $2 == "?=" { print "typos" }
' "$root/Makefile" > "$scratch/expected"
compare 'CI-only quality tools (Makefile QUALITY_*_VERSION)'

# GitHub Actions: every external `uses:` reference. Its pinned ref and release
# comment are maintained by Dependabot in the workflow files.
grep -h 'uses:' "$root"/.github/workflows/*.yml "$root"/.github/actions/*/action.yml \
  | sed 's/^[ -]*uses:[ ]*//' | grep -v '^\./' \
  | awk '{ split($1, ref, "@"); print ref[1] "\t" ref[2] }' > "$scratch/refs"
cut -f1 "$scratch/refs" > "$scratch/expected"
doc_table '## GitHub Actions' | columns 1 > "$scratch/documented"
compare 'GitHub Actions'
# One action, one pin (#1227): two refs for the same action mean one copy was
# left behind by an update, typically in the composite action that Dependabot
# did not scan.
duplicated=$(LC_ALL=C sort -u "$scratch/refs" | cut -f1 | uniq -d)
if [ -n "$duplicated" ]; then
  printf 'dependency inventory: actions pinned to more than one ref: %s\n' \
    "$(printf '%s' "$duplicated" | tr '\n' ' ')" >&2
  failed=1
fi

if [ "$failed" -ne 0 ]; then
  echo "update docs/dependencies.md (and review the licence of each added dependency) to match the pinned sources" >&2
  exit 1
fi
echo "dependency inventory matches the locked and pinned sources"

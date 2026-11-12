#!/bin/sh
# Fails when a reference document quotes a Temporal Core pin or Rust
# toolchain that differs from the tree (#1142).
#
# A statement about the *current* pin carries an invisible marker right after
# the quoted value, so historical mentions (upgrade notes, progress entries,
# corpus capture records) stay free text:
#
#   `8fe478ee179ed754614d230f39586f3edb57da71`<!-- pin:core -->
#   `1.99.1`<!-- pin:rust -->
#
# A `pin:core` value must be a prefix of at least eight hex digits of the
# `rev` of the Core crates in rust/Cargo.toml; a `pin:rust` value must equal
# the channel in rust/rust-toolchain.toml. Every Markdown file under docs/ and
# README.md is scanned, and each document listed in the required-marker loops
# near the end must carry at least one marker of that kind, so deleting a
# marker cannot silently disable the check.
#
# Usage: check-doc-pins.sh [source-root]
# Exit status: 0 when every marker matches, 1 on drift or a missing marker,
# 2 when a pin cannot be read from the tree.
set -eu

root=${1:-.}

# Every Core crate shares one revision (check-dependency-inventory.sh), so the
# first temporalio-sdk-core rev is the pin.
core=$(sed -n 's/^temporalio-sdk-core = .*rev = "\([0-9a-f]*\)".*/\1/p' "$root/rust/Cargo.toml" | head -n 1)
case "$core" in
  ????????????????????????????????????????) ;;
  *) echo "could not read a 40-hex Temporal Core rev from $root/rust/Cargo.toml" >&2; exit 2 ;;
esac
# The trailing `.*` tolerates a CRLF checkout on the native Windows lane.
rust=$(sed -n 's/^channel = "\([^"]*\)".*/\1/p' "$root/rust/rust-toolchain.toml" | head -n 1)
if [ -z "$rust" ]; then
  echo "could not read the Rust channel from $root/rust/rust-toolchain.toml" >&2
  exit 2
fi

status=0
# One document path per line. The loops below read it with `while read`, never
# by unquoted word splitting, so a path with spaces or glob characters stays
# one path.
documents=$(cd "$root" && { find docs -name '*.md' -type f; echo README.md; } | LC_ALL=C sort)
tab=$(printf '\t')

# markers KIND FILE prints "LINE<TAB>VALUE" for each `VALUE`<!-- pin:KIND -->.
markers() {
  awk -v kind="$1" '
    {
      line = $0
      needle = "`<!-- pin:" kind " -->"
      while ((at = index(line, needle)) > 0) {
        head = substr(line, 1, at - 1)
        start = 0
        for (i = length(head); i > 0; i--) if (substr(head, i, 1) == "`") { start = i; break }
        print NR "\t" (start ? substr(head, start + 1) : "")
        line = substr(line, at + length(needle))
      }
    }' "$2"
}

while IFS= read -r document; do
  path="$root/$document"
  [ -f "$path" ] || continue
  while IFS="$tab" read -r line value; do
    [ -n "$line" ] || continue
    case "$value" in
      ????????*) ;;
      *) echo "$document:$line: pin:core value '$value' is shorter than 8 hex digits" >&2; status=1; continue ;;
    esac
    case "$value" in
      *[!0-9a-f]*) echo "$document:$line: pin:core value '$value' is not lowercase hex" >&2; status=1; continue ;;
    esac
    case "$core" in
      "$value"*) ;;
      *) echo "$document:$line: quotes Core pin $value but rust/Cargo.toml pins $core" >&2; status=1 ;;
    esac
  done <<EOF
$(markers core "$path")
EOF
  while IFS="$tab" read -r line value; do
    [ -n "$line" ] || continue
    if [ "$value" != "$rust" ]; then
      echo "$document:$line: quotes Rust toolchain $value but rust/rust-toolchain.toml selects $rust" >&2
      status=1
    fi
  done <<EOF
$(markers rust "$path")
EOF
done <<EOF
$documents
EOF

# Documents that state the current pin and must keep a marker for it.
for document in \
  docs/reference/live-acceptance-coverage.md \
  docs/reference/v1-support-policy.md \
  docs/reference/core-bridge.md \
  docs/dependencies.md
do
  if [ -z "$(markers core "$root/$document" 2>/dev/null)" ]; then
    echo "$document: no \`...\`<!-- pin:core --> marker for the current Core pin" >&2
    status=1
  fi
done
for document in docs/reference/live-acceptance-coverage.md; do
  if [ -z "$(markers rust "$root/$document" 2>/dev/null)" ]; then
    echo "$document: no \`...\`<!-- pin:rust --> marker for the current Rust toolchain" >&2
    status=1
  fi
done

if [ "$status" -eq 0 ]; then
  echo "documentation pins: ok (Core $core, Rust $rust)"
fi
exit "$status"

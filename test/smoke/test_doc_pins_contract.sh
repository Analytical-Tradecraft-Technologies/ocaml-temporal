#!/bin/sh
set -eu

# Exercises scripts/check-doc-pins.sh (#1142) on a copy of the documentation
# and pin files, so each kind of drift is shown to fail without touching the
# checkout.
root=${1:-$(CDPATH='' cd -- "$(dirname "$0")/../.." && pwd)}
checker="$root/scripts/check-doc-pins.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
trap 'exit 1' HUP INT TERM

fail() {
  echo "doc pins contract: $*" >&2
  exit 1
}

# fresh: rebuilds the fixture tree from the checkout.
fresh() {
  rm -rf "$work/tree"
  mkdir -p "$work/tree/rust"
  cp "$root/rust/Cargo.toml" "$root/rust/rust-toolchain.toml" "$work/tree/rust/"
  cp -R "$root/docs" "$work/tree/docs"
  cp "$root/README.md" "$work/tree/README.md"
}

# expect_status STATUS LABEL [PATTERN]: runs the checker on the fixture tree,
# requiring its exit status and, when given, a diagnostic matching PATTERN.
expect_status() {
  actual=0
  sh "$checker" "$work/tree" >"$work/out" 2>"$work/err" || actual=$?
  [ "$actual" -eq "$1" ] || { cat "$work/err" >&2; fail "$2: expected status $1, got $actual"; }
  if [ "$#" -ge 3 ] && ! grep -q -- "$3" "$work/err"; then
    cat "$work/err" >&2
    fail "$2: diagnostic does not mention $3"
  fi
}

# The checkout itself is consistent.
fresh
expect_status 0 checkout
grep -q 'documentation pins: ok' "$work/out" || fail "checkout: no success line"

# The current pin, read the same way as the checker, so the fixtures below
# keep working after a consistent Core upgrade.
core=$(sed -n 's/^temporalio-sdk-core = .*rev = "\([0-9a-f]*\)".*/\1/p' "$root/rust/Cargo.toml" | head -n 1)
[ "${#core}" -eq 40 ] || fail "could not read the Core rev from rust/Cargo.toml"
# other: a valid 40-hex revision that is guaranteed to differ from the pin.
case "$core" in
  0*) other=1111111111111111111111111111111111111111 ;;
  *) other=0000000000000000000000000000000000000000 ;;
esac
short=$(printf '%s' "$core" | cut -c 1-7)
upper=$(printf '%s' "$core" | cut -c 1-8 | tr 'a-f' 'A-F')
stale=$(printf '%s' "$other" | cut -c 1-8)

# A Core pin bump without a documentation re-audit is drift.
fresh
sed "s/rev = \"[0-9a-f]\{40\}\"/rev = \"$other\"/" \
  "$root/rust/Cargo.toml" > "$work/tree/rust/Cargo.toml"
grep -q "rev = \"$other\"" "$work/tree/rust/Cargo.toml" || fail "core bump: fixture did not change the rev"
expect_status 1 'core bump' 'live-acceptance-coverage.md:.*quotes Core pin'

# A toolchain bump is drift for the Rust marker.
fresh
sed 's/^channel = ".*"/channel = "9.99.9"/' "$root/rust/rust-toolchain.toml" \
  > "$work/tree/rust/rust-toolchain.toml"
expect_status 1 'rust bump' 'quotes Rust toolchain'

# A CRLF toolchain file (native Windows checkout) still reads correctly.
fresh
awk '{ printf "%s\r\n", $0 }' "$root/rust/rust-toolchain.toml" > "$work/tree/rust/rust-toolchain.toml"
expect_status 0 'crlf toolchain'

# Deleting the marker from a required document cannot disable the check.
fresh
sed 's/<!-- pin:core -->//g' "$root/docs/reference/core-bridge.md" \
  > "$work/tree/docs/reference/core-bridge.md"
expect_status 1 'missing marker' 'core-bridge.md: no'

# A marked stale or malformed value anywhere under docs/ fails; unmarked
# historical mentions do not.
for value in "$stale" "$short" "$upper"; do
  fresh
  printf '\nOld pin `%s`<!-- pin:core --> here.\n' "$value" >> "$work/tree/docs/progress.md"
  expect_status 1 "marked $value" 'docs/progress.md:'
done
fresh
printf '\nThe previous pin was `95e97686a079dcfe6c42e3254b2f3f5e3d97408f`.\n' >> "$work/tree/docs/progress.md"
expect_status 0 'unmarked history'

# An unreadable pin is a usage error, not a pass.
fresh
printf '[workspace]\n' > "$work/tree/rust/Cargo.toml"
expect_status 2 'no core rev'

echo 'doc pins contract: ok'

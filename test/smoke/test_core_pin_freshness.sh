#!/bin/sh
# Offline contract for scripts/check-core-pin-freshness.sh (issue #824).
#
# A stub `gh` on PATH answers the three GitHub API calls from fixed values, so
# the test needs no network or token. It checks the exit-status contract the
# scheduled workflow relies on: 0 when the pin contains the latest release,
# 1 when an upgrade is due, and 2 when the pin or upstream metadata is
# unreadable (which must never be mistaken for "current").
#
# Usage: test_core_pin_freshness.sh <source-root>
set -eu

source_root=$1
script=$source_root/scripts/check-core-pin-freshness.sh
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

pin=0123456789abcdef0123456789abcdef01234567
release=89abcdef0123456789abcdef0123456789abcdef

mkdir -p "$scratch/bin" "$scratch/root/rust"
# The stub mirrors only the `gh api <path> --jq <filter>` shape the script
# uses. STUB_COMPARE is the compare status line; STUB_FAIL names a path
# fragment whose request fails, as an API or authentication error would.
cat > "$scratch/bin/gh" <<'EOF'
#!/bin/sh
[ "$1" = api ] || exit 64
case "$2" in
  *"${STUB_FAIL:-never-matches}"*) echo "stub failure for $2" >&2; exit 1 ;;
  repos/upstream/sdk/releases/latest) echo v9.9.9 ;;
  repos/upstream/sdk/commits/v9.9.9) echo "$STUB_RELEASE" ;;
  repos/upstream/sdk/compare/*) echo "$STUB_COMPARE" ;;
  *) echo "unexpected stub request $2" >&2; exit 64 ;;
esac
EOF
chmod +x "$scratch/bin/gh"

printf 'temporalio-sdk-core = { git = "https://example.invalid/sdk.git", rev = "%s" }\n' \
  "$pin" > "$scratch/root/rust/Cargo.toml"

# Runs the script with one compare answer; prints its exit status and keeps
# its report in $scratch/out for content checks.
run() {
  set +e
  PATH="$scratch/bin:$PATH" CORE_UPSTREAM_REPO=upstream/sdk STUB_RELEASE="$release" \
    STUB_COMPARE="$1" STUB_FAIL="${2:-never-matches}" \
    sh "$script" "$scratch/root" > "$scratch/out" 2> "$scratch/err"
  echo "$?"
  set -e
}

expect() {
  if [ "$1" != "$2" ]; then
    echo "core pin freshness: $3: expected exit $2, got $1" >&2
    cat "$scratch/out" "$scratch/err" >&2
    exit 1
  fi
}

expect "$(run 'identical 0 0')" 0 'pin equal to the release'
grep -Fq "current (\`identical\`)" "$scratch/out"
expect "$(run 'behind 0 3')" 0 'pin newer than the release'
expect "$(run 'ahead 7 0')" 1 'release ahead of the pin'
grep -Fq "7 commit(s) behind \`v9.9.9\`" "$scratch/out"
grep -Fq 'Temporal Core pin upgrades' "$scratch/out"
expect "$(run 'diverged 2 1')" 1 'release not contained in the pin'
expect "$(run 'unknown 0 0')" 2 'unknown compare status'
expect "$(run 'identical 0 0' releases/latest)" 2 'release lookup failure'
expect "$(run 'identical 0 0' compare/)" 2 'compare failure'

printf 'temporalio-sdk-core = { path = "../sdk-core" }\n' > "$scratch/root/rust/Cargo.toml"
expect "$(run 'identical 0 0')" 2 'pin without a git revision'

echo 'PASS core pin freshness'

#!/bin/sh
# Literal Make expressions such as $(MAKE) are searched for in single quotes.
# shellcheck disable=SC2016
set -eu

# Docker-free guard for the live regression suites (#795). Every
# test/integration/<suite>/regression.exe must be shipped in the CI smoke
# artifact and executed by a recipe reachable from test-temporal-live-ci,
# either directly or through LIVE_REGRESSION_EXECUTABLES and its controller.
# A new suite therefore fails here instead of compiling (or not) and silently
# never running. This is a source check only; it never claims a live result.
#
# Without LIVE_REGRESSIONS_CONTRACT_FIXTURE, the script also proves that it
# rejects representative omissions in a scratch copy of the checked inputs.
root=${1:-$(CDPATH="" cd -- "$(dirname "$0")/../.." && pwd)}
script=$(CDPATH="" cd -- "$(dirname "$0")" && pwd)/$(basename "$0")
makefile="$root/Makefile"
artifact_list="$root/scripts/ci-smoke-executables.txt"
controller="$root/test/integration/temporal/scripts/run-live-regressions.sh"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-live-regressions.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

fail() {
  echo "live regressions contract: $*" >&2
  exit 1
}

# Prints one Make recipe body: tab-indented and comment lines after the rule
# line, ignoring CRLF checkouts. The rule's prerequisites are not included.
recipe() {
  awk -v target="$1:" '
    { sub(/\r$/, "") }
    index($0, target) == 1 { active = 1; next }
    active && /^[^\t#]/ { exit }
    active { print }
  ' "$makefile"
}

# One suite per directory; its dune file must declare `(name regression)`.
for dune_file in "$root"/test/integration/*/dune; do
  [ -f "$dune_file" ] || continue
  directory=${dune_file%/dune}
  directory=${directory##*/}
  if tr -d '\r' <"$dune_file" | grep -F '(name regression)' >/dev/null; then
    printf 'test/integration/%s/regression.exe\n' "$directory"
  fi
done | LC_ALL=C sort -u >"$scratch/declared"
[ -s "$scratch/declared" ] || fail "no regression executables were found"

# LIVE_REGRESSION_EXECUTABLES is one Make assignment line of space-separated paths.
sed -n 's/^LIVE_REGRESSION_EXECUTABLES[[:space:]]*:=[[:space:]]*//p' "$makefile" \
  | tr -d '\r' | tr ' ' '\n' | sed '/^$/d' | LC_ALL=C sort -u >"$scratch/listed"
[ -s "$scratch/listed" ] || fail "LIVE_REGRESSION_EXECUTABLES is empty or missing"

# Collect every recipe reachable from the CI live entry point. Any defined
# target named in a reachable recipe counts as reachable: this covers both
# `$(MAKE) target` and helper calls such as `run_bounded 180 target`.
awk '{ sub(/\r$/, "") } /^[A-Za-z0-9_][A-Za-z0-9_.-]*:/ { sub(/:.*/, ""); print }' \
  "$makefile" | LC_ALL=C sort -u >"$scratch/defined"
printf '%s\n' test-temporal-live-ci >"$scratch/reachable"
: >"$scratch/visited"
: >"$scratch/recipes"
while :; do
  next=$(LC_ALL=C comm -23 "$scratch/reachable" "$scratch/visited" | sed -n 1p)
  [ -n "$next" ] || break
  printf '%s\n' "$next" >>"$scratch/visited"
  LC_ALL=C sort -u -o "$scratch/visited" "$scratch/visited"
  recipe "$next" >"$scratch/recipe"
  cat "$scratch/recipe" >>"$scratch/recipes"
  tr -c 'A-Za-z0-9_.-' '\n' <"$scratch/recipe" \
    | grep -Fx -f "$scratch/defined" >>"$scratch/reachable" || true
  LC_ALL=C sort -u -o "$scratch/reachable" "$scratch/reachable"
done
grep -Fx test-temporal-live-regressions "$scratch/reachable" >/dev/null \
  || fail "test-temporal-live-ci must run test-temporal-live-regressions"

# The CI target must build (or, with prebuilt artifacts, check) the same list,
# hand it to the controller, and always remove the Compose stack.
recipe test-temporal-live-regressions >"$scratch/live"
grep -F 'build-temporal-executables.sh $(LIVE_REGRESSION_EXECUTABLES)' "$scratch/live" >/dev/null \
  || fail "test-temporal-live-regressions must build LIVE_REGRESSION_EXECUTABLES"
grep -F 'scripts/run-live-regressions.sh $(LIVE_REGRESSION_EXECUTABLES)' "$scratch/live" >/dev/null \
  || fail "test-temporal-live-regressions must run every LIVE_REGRESSION_EXECUTABLES entry"
grep -F '$(MAKE) temporal-clean' "$scratch/live" >/dev/null \
  || fail "test-temporal-live-regressions must remove the Compose stack"
[ -f "$controller" ] || fail "missing live regression controller $controller"

# Listed suites must exist, ship in the artifact, and have an invocation in
# the controller, which rejects unknown executables at run time.
while IFS= read -r executable; do
  grep -Fx -- "$executable" "$scratch/declared" >/dev/null \
    || fail "$executable in LIVE_REGRESSION_EXECUTABLES has no regression stanza"
  tr -d '\r' <"$controller" | grep -F -- "$executable)" >/dev/null \
    || tr -d '\r' <"$controller" | grep -F -- "$executable|" >/dev/null \
    || fail "$executable has no invocation in run-live-regressions.sh"
done <"$scratch/listed"

# Every declared suite is in the smoke artifact and executed in CI.
while IFS= read -r executable; do
  tr -d '\r' <"$artifact_list" | grep -Fx -- "$executable" >/dev/null \
    || fail "$executable is missing from scripts/ci-smoke-executables.txt"
  grep -Fx -- "$executable" "$scratch/listed" >/dev/null \
    || grep -F -- "$executable" "$scratch/recipes" >/dev/null \
    || fail "$executable is not run by any recipe reachable from test-temporal-live-ci"
done <"$scratch/declared"

if [ -n "${LIVE_REGRESSIONS_CONTRACT_FIXTURE:-}" ]; then
  exit 0
fi

# Self-test: each mutation of a scratch copy must be rejected for the stated
# reason, so the checks above cannot silently degrade into accepting anything.
fixture="$scratch/fixture"
reset_fixture() {
  rm -rf "$fixture"
  mkdir -p "$fixture/scripts" "$fixture/test/integration/temporal/scripts"
  cp "$makefile" "$fixture/Makefile"
  cp "$artifact_list" "$fixture/scripts/ci-smoke-executables.txt"
  cp "$controller" "$fixture/test/integration/temporal/scripts/run-live-regressions.sh"
  for dune_file in "$root"/test/integration/*/dune; do
    [ -f "$dune_file" ] || continue
    directory=${dune_file%/dune}
    mkdir -p "$fixture/test/integration/${directory##*/}"
    cp "$dune_file" "$fixture/test/integration/${directory##*/}/dune"
  done
}
expect_rejected() {
  expected=$1
  if LIVE_REGRESSIONS_CONTRACT_FIXTURE=1 sh "$script" "$fixture" >"$scratch/log" 2>&1; then
    fail "self-test accepted a fixture that should fail with: $expected"
  fi
  grep -F -- "$expected" "$scratch/log" >/dev/null || {
    cat "$scratch/log" >&2
    fail "self-test failed for an unexpected reason (wanted: $expected)"
  }
}

reset_fixture
LIVE_REGRESSIONS_CONTRACT_FIXTURE=1 sh "$script" "$fixture" >/dev/null \
  || fail "self-test rejected an unmodified copy"

reset_fixture
first=$(sed -n 1p "$scratch/listed")
grep -Fvx -- "$first" "$artifact_list" >"$fixture/scripts/ci-smoke-executables.txt" || true
expect_rejected "$first is missing from scripts/ci-smoke-executables.txt"

reset_fixture
grep -Fv -- '$(MAKE) test-temporal-live-regressions' "$makefile" >"$fixture/Makefile" || true
expect_rejected "test-temporal-live-ci must run test-temporal-live-regressions"

reset_fixture
mkdir -p "$fixture/test/integration/new_suite"
printf '(executable\n (name regression))\n' >"$fixture/test/integration/new_suite/dune"
printf '%s\n' test/integration/new_suite/regression.exe >>"$fixture/scripts/ci-smoke-executables.txt"
expect_rejected "test/integration/new_suite/regression.exe is not run by any recipe"

reset_fixture
sed 's#^LIVE_REGRESSION_EXECUTABLES := #&test/integration/new_suite/regression.exe #' \
  "$makefile" >"$fixture/Makefile"
mkdir -p "$fixture/test/integration/new_suite"
printf '(executable\n (name regression))\n' >"$fixture/test/integration/new_suite/dune"
printf '%s\n' test/integration/new_suite/regression.exe >>"$fixture/scripts/ci-smoke-executables.txt"
expect_rejected "test/integration/new_suite/regression.exe has no invocation in run-live-regressions.sh"

echo "live regressions contract: ok ($(wc -l <"$scratch/declared" | tr -d ' ') regression executables)"

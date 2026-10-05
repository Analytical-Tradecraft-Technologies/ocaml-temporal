#!/bin/sh
# Literal Make expressions such as $(MAKE) are searched for in single quotes.
# shellcheck disable=SC2016
set -eu

# Docker-free guard for the live example gate (#798). Every executable declared
# under examples/ must be compiled by EXAMPLE_EXECUTABLES, shipped in the CI
# smoke artifact, and started by the live controller, and that controller must
# remain part of the Linux live CI recipe. A newly added example therefore
# fails here instead of being compiled but silently never executed. This is a
# source check only; it never claims that a live run passed.
root=${1:-$(CDPATH="" cd -- "$(dirname "$0")/../.." && pwd)}
makefile="$root/Makefile"
artifact_list="$root/scripts/ci-smoke-executables.txt"
controller="$root/test/integration/temporal/scripts/run-examples-live.sh"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-examples-contract.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

fail() {
  echo "examples live contract: $*" >&2
  exit 1
}

# Lists examples/<directory>/<name>.exe for each `(executable (name ...))`
# stanza, whether the name is on the opening line or a later one. Every
# executable stanza must yield exactly one name, and the plural `executables`
# form is rejected, so an unrecognised layout fails instead of silently
# dropping an example from every later comparison.
for dune_file in "$root"/examples/*/dune; do
  [ -f "$dune_file" ] || continue
  directory=${dune_file%/dune}
  directory=${directory##*/}
  awk -v directory="$directory" -v file="$dune_file" '
    function close_stanza() {
      if (executable && names != 1) {
        printf "examples live contract: %s: executable stanza has %d names\n", file, names > "/dev/stderr"
        failed = 1
      }
      executable = 0
      names = 0
    }
    { sub(/\r$/, "") }
    /^\(executables([[:space:]]|\)|$)/ {
      printf "examples live contract: %s: unsupported (executables ...) stanza\n", file > "/dev/stderr"
      failed = 1
    }
    /^\(/ { close_stanza() }
    /^\(executable([[:space:]]|$)/ { executable = 1 }
    executable && match($0, /\(name [A-Za-z0-9_]+\)/) {
      name = substr($0, RSTART + 6, RLENGTH - 7)
      names++
      print "examples/" directory "/" name ".exe"
    }
    END {
      close_stanza()
      if (failed) exit 1
    }
  ' "$dune_file" || fail "unrecognised executable layout in $dune_file"
done >"$scratch/declared.unsorted"
LC_ALL=C sort -u "$scratch/declared.unsorted" >"$scratch/declared"
[ -s "$scratch/declared" ] || fail "no example executables were found"

# EXAMPLE_EXECUTABLES is one Make assignment line of space-separated paths.
sed -n 's/^EXAMPLE_EXECUTABLES[[:space:]]*:=[[:space:]]*//p' "$makefile" \
  | tr -d '\r' | tr ' ' '\n' | sed '/^$/d' | LC_ALL=C sort -u >"$scratch/compiled"
if ! cmp -s "$scratch/declared" "$scratch/compiled"; then
  diff "$scratch/declared" "$scratch/compiled" >&2 || true
  fail "EXAMPLE_EXECUTABLES must list exactly the declared example executables"
fi

[ -f "$controller" ] || fail "missing live controller $controller"
while IFS= read -r executable; do
  tr -d '\r' <"$artifact_list" | grep -Fx -- "$executable" >/dev/null \
    || fail "$executable is missing from scripts/ci-smoke-executables.txt"
  grep -F -- "=$executable" "$controller" >/dev/null \
    || fail "$executable is not started by run-examples-live.sh"
done <"$scratch/declared"

# The live target must build the list, run the controller, and be invoked by
# the CI live recipe rather than existing only as an optional local command.
recipe() {
  awk -v target="$1:" '
    { sub(/\r$/, "") }
    index($0, target) == 1 { active = 1; next }
    active && /^[^\t#]/ { exit }
    active { print }
  ' "$makefile"
}
recipe test-temporal-examples-live >"$scratch/live"
grep -F 'build-temporal-executables.sh $(EXAMPLE_EXECUTABLES)' "$scratch/live" >/dev/null \
  || fail "test-temporal-examples-live must build EXAMPLE_EXECUTABLES"
grep -F 'scripts/run-examples-live.sh' "$scratch/live" >/dev/null \
  || fail "test-temporal-examples-live must run run-examples-live.sh"
grep -F '$(MAKE) temporal-clean' "$scratch/live" >/dev/null \
  || fail "test-temporal-examples-live must remove the Compose stack"
recipe test-temporal-live-ci | grep -F '$(MAKE) test-temporal-examples-live' >/dev/null \
  || fail "test-temporal-live-ci must run test-temporal-examples-live"

echo "examples live contract: ok ($(wc -l <"$scratch/declared" | tr -d ' ') executables)"

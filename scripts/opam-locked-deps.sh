#!/bin/sh
# Install, or verify, the exact OPAM dependency closure recorded in
# temporal-sdk.opam.locked for the compiler that is already active.
#
# Usage:
#   sh scripts/opam-locked-deps.sh install [extra `opam install` options...]
#   sh scripts/opam-locked-deps.sh check
#   sh scripts/opam-locked-deps.sh plan
#
# The lock file was solved for one compiler (its `ocaml` entry), but every
# lane that produces a release artifact uses its own compiler series: the
# digest-pinned ocaml/opam images on Linux and setup-ocaml on macOS/Windows.
# `opam install --locked` cannot express that split: it would also pin the
# compiler, and the lock's `pin-depends` would rebuild it from source. This
# script therefore treats packages in two classes:
#
# * Compiler-provided packages (`ocaml`, the compiler implementation and its
#   `base-*`/`ocaml-config`/`ocaml-options-*` companions) are owned by the
#   base image or setup-ocaml and are never installed here. `check` reports
#   them; the compiler version itself is asserted by the Makefile version
#   checks against the CI matrix.
# * Every other locked package is installed at exactly its locked version.
#   The only permitted deviation is an entry in scripts/opam-lock-overrides.txt
#   for a named compiler series, used when OPAM metadata declares the locked
#   version unavailable for that compiler. Overrides may only replace the
#   version of a package that is already locked; they cannot add packages.
#
# `install` always finishes with `check`, so drift fails the build that would
# otherwise publish an artifact compiled against unaudited versions. Output is
# normalized for CRLF because native Windows OPAM emits it under Cygwin.
set -eu

root=${OPAM_LOCK_ROOT:-.}
lock=$root/temporal-sdk.opam.locked
overrides=$root/scripts/opam-lock-overrides.txt

fail() {
  echo "opam-locked-deps: $*" >&2
  exit 1
}

# Succeed for packages that the selected compiler installation owns.
compiler_package() {
  case "$1" in
    ocaml|ocaml-base-compiler|ocaml-variants|ocaml-config|ocaml-options-*|base-*) return 0 ;;
  esac
  return 1
}

# Print the installed version of one package, or nothing when OPAM does not
# report it as installed. `opam var` prints an undefined marker or fails for
# missing packages depending on the OPAM release; both normalize to empty.
installed_version() {
  value=$(opam var "$1:version" 2>/dev/null | tr -d '\r') || value=
  case "$value" in
    ''|'#undefined'|*' '*) ;;
    *) printf '%s\n' "$value" ;;
  esac
}

[ -f "$lock" ] || fail "missing $lock"
[ -f "$overrides" ] || fail "missing $overrides"

# Locked `name version` pairs from the depends block. The lock uses OPAM's
# generated one-dependency-per-line form, which the licence audit also reads.
locked=$(tr -d '\r' <"$lock" | sed -n 's/^  "\([^"]*\)" {= "\([^"]*\)"}$/\1 \2/p')
[ -n "$locked" ] || fail "no exact dependency pins found in $lock"
locked_ocaml=$(printf '%s\n' "$locked" | awk '$1 == "ocaml" { print $2 }')
[ -n "$locked_ocaml" ] || fail "$lock does not pin the ocaml package"

active_ocaml=$(installed_version ocaml)
[ -n "$active_ocaml" ] || fail "no active OCaml compiler in the current OPAM switch"
active_series=$(printf '%s\n' "$active_ocaml" | cut -d. -f1,2)

# Validate every override line before using any of them, so a typo or an
# attempt to introduce an unlocked package fails regardless of compiler.
override_lines=$(tr -d '\r' <"$overrides" | sed -e 's/#.*//' -e '/^[[:space:]]*$/d')
if [ -n "$override_lines" ]; then
  printf '%s\n' "$override_lines" | while read -r series package version extra; do
    [ -n "$version" ] && [ -z "${extra:-}" ] ||
      fail "malformed override line: $series $package $version ${extra:-}"
    case "$series" in
      [0-9]*.[0-9]*) ;;
      *) fail "override series must be MAJOR.MINOR: $series" ;;
    esac
    compiler_package "$package" &&
      fail "override must not replace compiler-provided package $package"
    printf '%s\n' "$locked" | awk -v p="$package" '$1 == p { found = 1 } END { exit !found }' ||
      fail "override names $package, which is not in $lock"
  done
fi

# Print the planned `name.version` for each non-compiler locked package.
plan() {
  printf '%s\n' "$locked" | while read -r package version; do
    compiler_package "$package" && continue
    override=$(printf '%s\n' "$override_lines" |
      awk -v s="$active_series" -v p="$package" '$1 == s && $2 == p { print $3 }')
    printf '%s.%s\n' "$package" "${override:-$version}"
  done
}

# Compare the active switch against the plan and the compiler-owned entries.
check() {
  mismatches=0
  for entry in $(plan); do
    package=${entry%%.*}
    expected=${entry#*.}
    actual=$(installed_version "$package")
    if [ "$actual" = "$expected" ]; then
      echo "LOCKED   $package $actual"
    else
      echo "MISMATCH $package installed=${actual:-none} expected=$expected" >&2
      mismatches=1
    fi
  done
  # The exact compiler patch level is asserted separately by the Makefile's
  # version checks against the CI matrix; report these for build logs only.
  printf '%s\n' "$locked" | while read -r package version; do
    compiler_package "$package" || continue
    echo "COMPILER $package installed=$(installed_version "$package") locked=$version"
  done
  [ "$mismatches" -eq 0 ] ||
    fail "installed OPAM packages differ from $lock for OCaml $active_ocaml"
  echo "opam-locked-deps: OCaml $active_ocaml matches $lock (solved for OCaml $locked_ocaml)"
}

command=${1:-}
[ "$#" -eq 0 ] || shift
case "$command" in
  plan) plan ;;
  check) check ;;
  install)
    # Word splitting is intended: package specs never contain whitespace.
    # shellcheck disable=SC2046
    opam install --yes "$@" $(plan)
    check
    ;;
  *) fail "usage: $0 install [opam options...] | check | plan" ;;
esac

#!/bin/sh
# Install, or verify, the exact OPAM closure of one CI-only tool lock on top of
# the locked SDK closure (temporal-sdk.opam.locked).
#
# Usage:
#   sh scripts/opam-tool-lock.sh install <tool-lock> [extra `opam install` options...]
#   sh scripts/opam-tool-lock.sh check <tool-lock>
#
# A tool lock lists `<package> <exact version>` lines, with `#` comments. Two
# exist: scripts/docs-tools.locked (odoc, for `make docs`) and
# scripts/eio-example.locked (the Eio integration example, examples/eio). Their
# packages are never SDK dependencies, so they are installed only in the
# disposable Docker image stage or CI lane that needs them.
#
# `install` installs every listed package at its exact version, then compares
# the switch before and after. Every package that opam added, or moved to
# another version, must be listed in the tool lock at that version, and every
# package it removed or moved must be listed by name. opam also installs
# transitive dependencies, so an unlisted package, from a lock omission or new
# upstream metadata, fails here instead of slipping past the licence review in
# docs/dependencies.md, and so does a locked SDK package that opam upgraded or
# removed. On a fresh switch the added set therefore equals the tool lock; a
# switch that already holds some listed packages (a cached CI switch, or a
# developer's own) only adds the rest. Both commands then require every
# listed package at its exact version and finish with
# `scripts/opam-locked-deps.sh check`, so a tool install can never silently
# move an SDK dependency. Output is normalized for CRLF because native Windows
# OPAM emits it under Cygwin.
set -eu

root=${OPAM_LOCK_ROOT:-.}

fail() {
  echo "opam-tool-lock: $*" >&2
  exit 1
}

[ "$#" -ge 2 ] || fail "usage: $0 install <tool-lock> [opam options...] | check <tool-lock>"
command=$1
lock=$2
shift 2
[ -f "$lock" ] || fail "missing $lock"

# Print the lock's `name version` pairs, sorted, without comments or blanks.
entries() {
  tr -d '\r' <"$lock" | sed -e 's/#.*//' -e '/^[[:space:]]*$/d' |
    awk '{ print $1 " " $2 }' | LC_ALL=C sort
}

# Print every installed package of the active switch as `name version`, sorted.
# Colour is disabled explicitly: opam colours names and versions with ANSI
# escapes whenever OPAMCOLOR or the CI environment asks for it (the macOS lane
# does), and coloured pairs never compare equal to the lock's plain lines.
installed() {
  opam list --installed --short --columns=name,version --color=never | tr -d '\r' |
    awk '{ print $1 " " $2 }' | LC_ALL=C sort
}

[ -n "$(entries)" ] || fail "no packages listed in $lock"

scratch=$(mktemp -d "${TMPDIR:-/tmp}/opam-tool-lock.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
entries >"$scratch/expected"

case "$command" in
  install)
    installed >"$scratch/before"
    # xargs appends the `name.version` specs after the caller's opam options;
    # package specs never contain whitespace.
    awk '{ print $1 "." $2 }' "$scratch/expected" | xargs opam install --yes "$@"
    installed >"$scratch/after"
    # Added or moved packages (`name version` after) must be listed exactly.
    LC_ALL=C comm -13 "$scratch/before" "$scratch/after" >"$scratch/added"
    LC_ALL=C comm -23 "$scratch/added" "$scratch/expected" >"$scratch/unlisted"
    # Removed or moved packages (`name version` before) must be listed by name.
    LC_ALL=C comm -23 "$scratch/before" "$scratch/after" | awk '{ print $1 }' \
      | LC_ALL=C sort >"$scratch/removed"
    awk '{ print $1 }' "$scratch/expected" | LC_ALL=C sort >"$scratch/names"
    LC_ALL=C comm -23 "$scratch/removed" "$scratch/names" >>"$scratch/unlisted"
    if [ -s "$scratch/unlisted" ]; then
      echo "opam-tool-lock: opam changed packages that $lock does not list:" >&2
      cat "$scratch/unlisted" >&2
      exit 1
    fi
    ;;
  check) ;;
  *) fail "unknown command $command" ;;
esac

installed >"$scratch/installed"
LC_ALL=C comm -23 "$scratch/expected" "$scratch/installed" >"$scratch/missing"
if [ -s "$scratch/missing" ]; then
  echo "opam-tool-lock: not installed at the version in $lock:" >&2
  cat "$scratch/missing" >&2
  exit 1
fi

echo "opam-tool-lock: the active switch matches $lock"
sh "$root/scripts/opam-locked-deps.sh" check

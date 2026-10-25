#!/bin/sh
# Describe the compiler ABI and compiled dependency identities, without paths
# tied to an OPAM switch. Invoke inside `opam exec --` on producer and consumer.
set -eu
export LC_ALL=C
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM
ocamlc -config >"$temporary/config"
tr -d '\r' <"$temporary/config" | awk '
  /^(version|architecture|model|system|os_type|ccomp_type|word_size|int_size|flambda|flat_float_array|with_frame_pointers|tsan|host|target|native_runtime_id|bytecode_runtime_id|.*_magic_number):/ { print }
'
# Installed dune-package metadata follows the producer's Dune format. Record
# the tool version as well so the tested downstream environment is reproducible.
dune --version >"$temporary/dune-version"
printf 'dune: %s\n' "$(tr -d '\r' <"$temporary/dune-version")"
# These are the complete external runtime libraries of temporal-sdk. Retain
# OCaml interface AND implementation CRCs: equal OPAM versions alone do not
# guarantee native-code compatibility (cross-module optimization matters).
for package in stdlib unix threads logs yojson; do
  directory=$(ocamlfind query "$package" | tr -d '\r')
  version=$(ocamlfind query -format '%v' "$package" | tr -d '\r')
  printf 'dependency: %s %s\n' "$package" "$version"
  ocamlobjinfo "$directory/$package.cmxa" >"$temporary/objects"
  tr -d '\r' <"$temporary/objects" | awk '
    /^Name:|^CRC of implementation:/ { print }
    /^[ \t]+[0-9a-f]+[ \t]+[A-Za-z_]/ { print $1 " " $2 }
  '
done
# Host identity (#1147), as `host-` lines that the installer compares by
# minimum version rather than exactly: a newer C library or macOS on the
# consumer is fine, an older one fails at link time. The C compiler is
# recorded for diagnosis only.
case "$(uname -s)" in
  Linux)
    # "glibc 2.36" on glibc systems; empty on musl, which has no such query.
    libc=$(getconf GNU_LIBC_VERSION 2>/dev/null || true)
    printf 'host-libc: %s\n' "${libc:-unknown}"
    ;;
  Darwin)
    macos=$(sw_vers -productVersion | tr -d '\r')
    printf 'host-macos: %s\n' "$macos"
    # Objects target MACOSX_DEPLOYMENT_TARGET when set, else the running OS.
    printf 'host-macos-deployment-target: %s\n' "${MACOSX_DEPLOYMENT_TARGET:-$macos}"
    ;;
esac
cc_version=$(cc --version 2>/dev/null | head -n 1 | tr -d '\r' || true)
printf 'host-cc: %s\n' "${cc_version:-unknown}"

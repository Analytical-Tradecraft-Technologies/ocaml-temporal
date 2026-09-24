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

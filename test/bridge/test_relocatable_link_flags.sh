#!/bin/sh
# Exercise the real OCaml linker, including Dune's distinction between -ccopt
# and -cclib. No Windows host or Rust build is needed for the MinGW path format.
# Invoke inside opam exec so the compiler remains selected after changing cwd.
set -eu
root=$(cd "$1" && pwd)
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM
cd "$temporary"

# A native OCaml archive doubles as a tiny import-library fixture. The final
# linker must find it even though this program does not call its symbols.
printf '%s\n' '(** Unused native archive for the relocation regression. *)' \
  'let value = 7' > imported.ml
ocamlopt -c imported.ml
ocamlopt -a -o imported.cmxa imported.cmx
mkdir -p bundle/import-libs producer
cp imported.a bundle/import-libs/libwinapi_fixture.a
cd producer
TEMPORAL_OCAML_IMPORT_DIR=rust-imports TEMPORAL_OCAML_LINK_FLAGS=ocaml-flags.sexp \
  sh "$root/scripts/render-rust-link-flags.sh" MINGW64_NT-test \
  ../bundle flags.sexp '-lwinapi_fixture'
printf '%s\n' '(lang dune 3.18)' > dune-project
cat > dune <<'DUNE'
(library
 (name sdk)
 (library_flags (:include ocaml-flags.sexp))
 (c_library_flags (:include flags.sexp)))
DUNE
printf '%s\n' '(** Public value for the independent binary consumer. *)' \
  'let value = 42' > sdk.ml
dune build --root . sdk.cmxa
mkdir '../relocated sdk'
cp _build/default/sdk.cmxa _build/default/sdk.a \
  _build/default/.sdk.objs/byte/sdk.cmi \
  _build/default/.sdk.objs/native/sdk.cmx '../relocated sdk/'
cp -R rust-imports '../relocated sdk/'
cd ..
rm -rf producer bundle
printf '%s\n' '(** Link only the relocated compiled library. *)' \
  'let () = assert (Sdk.value = 42)' > main.ml
ocamlopt -I 'relocated sdk' sdk.cmxa main.ml -o main.exe
./main.exe
printf '%s\n' 'Dune/OCaml native import relocation passed, including paths with spaces.'

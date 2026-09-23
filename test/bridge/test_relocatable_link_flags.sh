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
mkdir -p producer/bundle/import-libs
cp imported.a producer/bundle/import-libs/libwinapi_fixture.a
cp "$root/scripts/render-rust-link-flags.sh" producer/
cd producer
cp "$root/lib/core_bridge/install-rust-imports.inc" .
printf '%s\n' '(lang dune 3.18)' '(using directory-targets 0.1)' \
  '(package (name temporal-sdk))' > dune-project
cat > dune <<'DUNE'
(rule
 (targets flags.sexp ocaml-flags.sexp (dir rust-imports))
 (deps render-rust-link-flags.sh (source_tree bundle))
 (action
  (setenv TEMPORAL_OCAML_IMPORT_DIR rust-imports
   (setenv TEMPORAL_OCAML_LINK_FLAGS ocaml-flags.sexp
    (run sh ./render-rust-link-flags.sh MINGW64_NT-test
     bundle flags.sexp -lwinapi_fixture)))))
(include install-rust-imports.inc)
(library
 (name temporal_core_bridge)
 (package temporal-sdk)
 (library_flags (:include ocaml-flags.sexp))
 (c_library_flags (:include flags.sexp)))
DUNE
printf '%s\n' '(** Public value for the independent binary consumer. *)' \
  'let value = 42' > temporal_core_bridge.ml
dune build --root . @install
# Exercise the SDK's actual install declaration with a generated directory.
# A directory symlink works on Unix but fails in Windows' file-copy fallback.
test ! -L _build/install/default/lib/temporal-sdk/__private__/temporal_core_bridge/rust-imports
prefix="$temporary/relocated sdk"
native_prefix=$prefix
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) native_prefix=$(cygpath -w "$prefix") ;;
esac
dune install --root . --prefix "$native_prefix"
installed="$prefix/lib/temporal-sdk/__private__/temporal_core_bridge"
test -s "$installed/rust-imports/libwinapi_fixture.a"
cd ..
rm -rf producer
printf '%s\n' '(** Link only the relocated compiled library. *)' \
  'let () = assert (Temporal_core_bridge.value = 42)' > main.ml
native_installed="$native_prefix/lib/temporal-sdk/__private__/temporal_core_bridge"
ocamlopt -I "$native_installed" -I "$native_installed/.public_cmi" \
  temporal_core_bridge.cmxa main.ml -o main.exe
./main.exe
printf '%s\n' 'Dune/OCaml native import installation and relocation passed, including paths with spaces.'

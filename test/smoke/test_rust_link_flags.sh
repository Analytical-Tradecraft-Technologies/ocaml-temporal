#!/bin/sh
set -eu

workspace_root=$1
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM

search_dir="$temporary_root/registry with space/winapi/lib"
build_dir=$temporary_root/target/debug/build/winapi-x86_64-pc-windows-gnu-test
mkdir -p "$search_dir" "$build_dir"
: >"$search_dir/libwinapi_ntdll.a"
printf 'cargo:rustc-link-search=native=%s\n' "$search_dir" >"$build_dir/output"

output=$temporary_root/flags.sexp
sh "$workspace_root/scripts/render-rust-link-flags.sh" \
  'MINGW64_NT-test' \
  "$temporary_root/target" \
  "$output" \
  '-lwinapi_ntdll -lbcrypt'

expected_dir=$search_dir
if command -v cygpath >/dev/null 2>&1; then
  expected_dir=$(cygpath -m "$expected_dir")
fi
expected=$(printf '("-L%s" -lwinapi_ntdll -lbcrypt)\n' "$expected_dir")
actual=$(cat "$output")
if [ "$actual" != "$expected" ]; then
  printf 'unexpected Windows Rust link flags\nexpected: %s\nactual:   %s\n' \
    "$expected" "$actual" >&2
  exit 1
fi

non_windows_output=$temporary_root/non-windows-flags.sexp
sh "$workspace_root/scripts/render-rust-link-flags.sh" \
  'Linux' \
  "$temporary_root/target" \
  "$non_windows_output" \
  '-lpthread -ldl'
printf '(-lpthread -ldl)\n' >"$temporary_root/expected-non-windows.sexp"
cmp "$temporary_root/expected-non-windows.sexp" "$non_windows_output"

# Relocate the complete Windows bundle and remove Cargo's registry. The new
# linker flags must point to the downloaded import libraries, including when
# the consumer's checkout path contains spaces.
sh "$workspace_root/scripts/render-rust-link-flags.sh" \
  'MINGW64_NT-test' "$temporary_root/target" "$output" \
  '-lwinapi_ntdll -lbcrypt' "$temporary_root/bundle/import-libs"
mv "$temporary_root/bundle" "$temporary_root/relocated bundle"
rm -rf "$search_dir" "$temporary_root/target"
sh "$workspace_root/scripts/render-rust-link-flags.sh" \
  'MINGW64_NT-test' "$temporary_root/relocated bundle" "$output" \
  '-lwinapi_ntdll -lbcrypt'
expected_dir="$temporary_root/relocated bundle/import-libs"
if command -v cygpath >/dev/null 2>&1; then
  expected_dir=$(cygpath -m "$expected_dir")
fi
test "$(cat "$output")" = "$(printf '("-L%s" -lwinapi_ntdll -lbcrypt)' "$expected_dir")"

# Installed OCaml archives must retain no producer checkout path. Their import
# archives travel beside the cmxa; the compiler expands CAMLORIGIN at final link.
TEMPORAL_OCAML_IMPORT_DIR="$temporary_root/installed/rust-imports" \
  TEMPORAL_OCAML_LINK_FLAGS="$temporary_root/ocaml-flags.sexp" \
  sh "$workspace_root/scripts/render-rust-link-flags.sh" \
  'MINGW64_NT-test' "$temporary_root/relocated bundle" "$output" \
  '-lwinapi_ntdll -lbcrypt'
test "$(cat "$output")" = '(-lwinapi_ntdll -lbcrypt)'
test "$(cat "$temporary_root/ocaml-flags.sexp")" = '(-ccopt "-L\"$CAMLORIGIN/rust-imports\"")'
cmp "$temporary_root/installed/rust-imports/libwinapi_ntdll.a" \
  "$temporary_root/relocated bundle/import-libs/libwinapi_ntdll.a"
TEMPORAL_OCAML_IMPORT_DIR="$temporary_root/linux/rust-imports" \
  TEMPORAL_OCAML_LINK_FLAGS="$temporary_root/ocaml-flags.sexp" \
  sh "$workspace_root/scripts/render-rust-link-flags.sh" \
  Linux "$temporary_root/relocated bundle" "$output" '-lpthread -ldl'
test -s "$temporary_root/linux/rust-imports/README"
test "$(cat "$output")" = '(-lpthread -ldl)'
test "$(cat "$temporary_root/ocaml-flags.sexp")" = '()'
rm "$temporary_root/relocated bundle/import-libs/libwinapi_ntdll.a"
if sh "$workspace_root/scripts/render-rust-link-flags.sh" \
  'MINGW64_NT-test' "$temporary_root/relocated bundle" "$output" \
  '-lwinapi_ntdll -lbcrypt' 2>/dev/null; then
  echo 'accepted missing Windows import library' >&2; exit 1
fi

#!/bin/sh
set -eu

operating_system=$1
target_root=$2
output=$3
native_link_flags=$4
bundle_imports=${5:-}

# Checks the exact import-library set requested by rustc. Bundled libraries and
# Cargo registry libraries follow the same validation before reaching Dune.
complete_imports () {
  for flag in $native_link_flags; do
    case "$flag" in
      -lwinapi_*) [ -f "$1/lib${flag#-l}.a" ] || return 1 ;;
    esac
  done
}

# Converts the path printed by a Windows Cargo build script into a spelling
# understood by both MSYS shell tools and native Windows programs. Unix paths
# are left unchanged so this function is also testable on non-Windows hosts.
windows_path () {
  path=$1
  case "$path" in
    [A-Za-z]:[\\/]* )
      if ! command -v cygpath >/dev/null 2>&1; then
        echo "Cargo returned a Windows path but cygpath is unavailable: $path" >&2
        exit 1
      fi
      cygpath -m "$path"
      ;;
    * ) printf '%s\n' "$path" ;;
  esac
}

# Cargo's winapi import libraries are not installed into the MinGW toolchain's
# default search path. rustc knows their directory from the dependency's build
# script, but --print=native-static-libs reports only -l names. Preserve the
# corresponding -L directory for the foreign OCaml linker.
windows_search_dir () {
  if [ -d "$target_root/import-libs" ]; then
    complete_imports "$target_root/import-libs" || {
      echo 'incomplete bundled Windows import libraries' >&2; exit 1;
    }
    windows_path "$target_root/import-libs"
    return
  fi
  selected=
  for metadata in "$target_root"/debug/build/winapi-x86_64-pc-windows-gnu-*/output; do
    [ -f "$metadata" ] || continue
    while IFS= read -r candidate; do
      [ -n "$candidate" ] || continue
      candidate=$(windows_path "$candidate")

      complete_imports "$candidate" || continue

      if [ -n "$selected" ] && [ "$selected" != "$candidate" ]; then
        echo "multiple Cargo winapi library directories matched the Rust link flags" >&2
        exit 1
      fi
      selected=$candidate
    done <<EOF
$(sed -n 's/^cargo:rustc-link-search=native=//p' "$metadata")
EOF
  done

  if [ -z "$selected" ]; then
    echo "could not find Cargo's complete winapi import-library directory" >&2
    exit 1
  fi
  printf '%s\n' "$selected"
}

case "$operating_system" in
  MINGW* | MSYS* | CYGWIN*)
    search_dir=$(windows_search_dir)
    if [ -n "$bundle_imports" ]; then
      mkdir -p "$bundle_imports"
      for flag in $native_link_flags; do
        case "$flag" in
          -lwinapi_*) cp "$search_dir/lib${flag#-l}.a" "$bundle_imports/" ;;
        esac
      done
    fi
    # Native Windows Dune/linkers cannot resolve Cygwin /cygdrive paths.
    if command -v cygpath >/dev/null 2>&1; then
      search_dir=$(cygpath -m "$search_dir")
    fi
    # Dune reads this as an S-expression. Quote and escape the generated path
    # so installations below a directory containing spaces remain valid.
    escaped_search_dir=$(printf '%s' "$search_dir" | sed 's/\\/\\\\/g; s/"/\\"/g')
    printf '("-L%s" %s)\n' "$escaped_search_dir" "$native_link_flags" >"$output"
    ;;
  *)
    printf '(%s)\n' "$native_link_flags" >"$output"
    ;;
esac

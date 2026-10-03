#!/bin/sh
# Copy Dune's complete install tree, dereferencing its build-tree symlinks.
# This operation never builds: the CI verification job must finish first.
set -eu
destination=$1
test ! -e "$destination" || { echo "stage already exists: $destination" >&2; exit 1; }
mkdir -p "$destination"
destination=$(cd "$destination" && pwd)
native_destination=$destination
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) native_destination=$(cygpath -w "$destination") ;;
esac
opam exec -- dune install temporal-sdk --prefix "$native_destination"
opam exec -- sh scripts/ocaml-library-environment.sh >"$destination/environment.txt"

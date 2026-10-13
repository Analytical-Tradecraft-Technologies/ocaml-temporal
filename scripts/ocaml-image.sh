#!/bin/sh
# Resolve the OCaml development base image for Dockerfile.dev.
#
# Usage: scripts/ocaml-image.sh SELECTOR
#
# SELECTOR is a compiler series (`5.4`), a series name (`ocaml-5.4`), or an
# explicit image reference. A series or series name prints the digest-pinned
# `ocaml/opam` reference recorded on the single FROM line of
# docker/ocaml-<series>/Dockerfile, which is the only source of truth for that
# series (#1027). Any other selector containing `:`, `/` or `@` is an explicit
# reference for local experiments and is printed unchanged; CI and release
# lanes only pass series names. The script prints exactly one line on success
# and exits non-zero, printing nothing on stdout, for an unknown series or a
# malformed pin file, so callers can treat empty output as failure.
set -eu

if [ "$#" -ne 1 ] || [ -z "$1" ]; then
  echo "usage: $0 <series | ocaml-<series> | image-reference>" >&2
  exit 2
fi

root=$(CDPATH='' cd -- "$(dirname "$0")/.." && pwd)
selector=$1

case "$selector" in
  ocaml-[0-9]*.[0-9]*) series=${selector#ocaml-} ;;
  [0-9]*.[0-9]*) series=$selector ;;
  *:* | */* | *@*)
    printf '%s\n' "$selector"
    exit 0
    ;;
  *)
    echo "ocaml-image: unrecognized selector '$selector'" >&2
    exit 1
    ;;
esac

# Accept a full compiler version such as 5.4.1 by keeping major.minor.
series=$(printf '%s' "$series" | cut -d. -f1,2)
pin="$root/docker/ocaml-$series/Dockerfile"
if [ ! -f "$pin" ]; then
  echo "ocaml-image: no pinned base image for OCaml $series ($pin)" >&2
  exit 1
fi

reference=$(awk '$1 == "FROM" { print $2 }' "$pin")
expected="ocaml/opam:debian-12-ocaml-$series@sha256:"
case "$reference" in
  "$expected"*) ;;
  *)
    echo "ocaml-image: $pin must contain exactly one FROM $expected<digest> line" >&2
    exit 1
    ;;
esac
if [ "$(printf '%s\n' "$reference" | wc -l | tr -d ' ')" != 1 ] ||
  ! printf '%s\n' "$reference" | grep -Eq '@sha256:[0-9a-f]{64}$'; then
  echo "ocaml-image: $pin must contain exactly one FROM $expected<digest> line" >&2
  exit 1
fi
printf '%s\n' "$reference"

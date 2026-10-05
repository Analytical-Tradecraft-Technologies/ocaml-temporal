#!/bin/sh
set -eu

failed=0

# Return success only for an approved SPDX expression. Linking and metadata
# exceptions are deliberately restricted to the exact packages and versions
# reviewed in docs/dependencies.md.
allowed_license() {
  package=$1
  version=$2
  license=$3

  case "$license" in
    MIT|Apache-2.0|BSD-2-Clause|BSD-3-Clause|ISC|Zlib|PostgreSQL)
      return 0
      ;;
    'LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception')
      case "$package" in
        ocaml|ocaml-base-compiler|ocaml-compiler-libs) return 0 ;;
      esac
      ;;
    'LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception')
      [ "$package" = "ocamlbuild" ] && [ "$version" = "0.16.1" ] && return 0
      ;;
    'CC0-1.0+')
      [ "$package" = "ocaml-options-vanilla" ] && [ "$version" = "1" ] && return 0
      ;;
  esac
  return 1
}

# Check one package's complete OPAM license field. OPAM may return a quoted list
# or a comma-separated value, so normalize either form before applying the
# exact allowlist. Missing licenses fail except for named compiler virtual
# packages, which have no independent source distribution.
check_license_value() {
  package=$1
  version=$2
  value=$3

  if [ -z "$value" ]; then
    case "$package:$version" in
      base-bigarray:base|base-domains:base|base-nnp:base|base-threads:base|base-unix:base)
        echo "ALLOW $package $version compiler-virtual-package"
        return
        ;;
    esac
    echo "DENY  $package $version missing-license" >&2
    failed=1
    return
  fi

  expressions=$(printf '%s\n' "$value" | awk -F '"' '{ for (i = 2; i <= NF; i += 2) print $i }')
  if [ -z "$expressions" ]; then
    expressions=$(printf '%s\n' "$value" | awk -F ',' '{ for (i = 1; i <= NF; i++) { gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i); print $i } }')
  fi
  old_ifs=$IFS
  IFS='
'
  for license in $expressions; do
    IFS=$old_ifs
    if allowed_license "$package" "$version" "$license"; then
      echo "ALLOW $package $version $license"
    else
      echo "DENY  $package $version $license" >&2
      failed=1
    fi
    IFS='
'
  done
  IFS=$old_ifs
}

# Read package name, version, and license metadata from one OPAM manifest, then
# pass the normalized values to the policy checker. Fixture manifests may omit
# name or version, in which case the filename and "fixture" identify them.
check_opam_file() {
  file=$1
  case "$file" in
    */*) opam_file=$file ;;
    *) opam_file=./$file ;;
  esac
  package=$(opam show --just-file "$opam_file" --field=name 2>/dev/null || true)
  version=$(opam show --just-file "$opam_file" --field=version 2>/dev/null || true)
  [ -n "$package" ] || package=$(basename "$file" .opam)
  [ -n "$version" ] || version=fixture
  value=$(opam show --just-file "$opam_file" --field=license 2>/dev/null || true)
  check_license_value "$package" "$version" "$value"
}

if [ "$#" -gt 0 ]; then
  for file in "$@"; do
    check_opam_file "$file"
  done
else
  check_opam_file temporal-sdk.opam
  dependencies=$(sed -n 's/^  "\([^"]*\)" {= "\([^"]*\)".*/\1 \2/p' temporal-sdk.opam.locked)
  # Per-compiler replacements installed by scripts/opam-locked-deps.sh are
  # part of some release artifact's build closure, so audit them identically.
  overrides=$(sed -e 's/#.*//' -e '/^[[:space:]]*$/d' scripts/opam-lock-overrides.txt |
    awk '{ print $2, $3 }')
  [ -z "$overrides" ] || dependencies="$dependencies
$overrides"
  while read -r package version; do
    [ -n "$package" ] || continue
    value=$(opam show "$package.$version" --field=license 2>/dev/null || true)
    check_license_value "$package" "$version" "$value"
  done <<EOF
$dependencies
EOF
fi

exit "$failed"

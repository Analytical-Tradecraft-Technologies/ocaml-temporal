#!/bin/sh
set -eu

root=${1:-.}
checker="$root/test/fixtures/install-consumer/test_build_tool_dependencies.sh"
fixture_root=$(mktemp -d "/tmp/temporal-sdk-build-dependency-pins.XXXXXX")

cleanup() {
  case "$(basename "$fixture_root")" in
    temporal-sdk-build-dependency-pins.*) rm -rf -- "$fixture_root" ;;
    *) echo "refusing to remove unexpected fixture path: $fixture_root" >&2 ;;
  esac
}
trap cleanup EXIT HUP INT TERM

prepare_fixture() {
  sed -n 'p' "$root/dune-project" >"$fixture_root/dune-project"
  sed -n 'p' "$root/temporal-sdk.opam" >"$fixture_root/temporal-sdk.opam"
  sed -n 'p' "$root/temporal-sdk.opam.locked" \
    >"$fixture_root/temporal-sdk.opam.locked"
}

replace_pin_with_unrelated_sentinel() {
  dependency=$1
  version=$2
  sentinel=$3
  next_lock="$fixture_root/temporal-sdk.opam.locked.next"

  awk -v dependency="$dependency" -v version="$version" -v sentinel="$sentinel" '
    $0 == "  \"" dependency "\" {= \"" version "\"}" {
      print "  \"" dependency "\""
      print "  \"" sentinel "\" {= \"" version "\"}"
      replaced = 1
      next
    }
    { print }
    END { if (!replaced) exit 2 }
  ' "$fixture_root/temporal-sdk.opam.locked" >"$next_lock"
  mv "$next_lock" "$fixture_root/temporal-sdk.opam.locked"
}

assert_rejects_detached_pin() {
  dependency=$1
  version=$2
  sentinel=$3
  log="$fixture_root/check.log"

  prepare_fixture
  replace_pin_with_unrelated_sentinel "$dependency" "$version" "$sentinel"
  if sh "$checker" "$fixture_root" >"$log" 2>&1; then
    echo "install consumer metadata mutation: detached $dependency pin unexpectedly passed" >&2
    exit 1
  fi
  if ! grep -F "does not pin $dependency to version $version" "$log" >/dev/null; then
    cat "$log" >&2
    echo "install consumer metadata mutation: $dependency failed for the wrong reason" >&2
    exit 1
  fi
}

assert_rejects_detached_pin \
  conf-protoc 4.4.0 zz-conf-protoc-pin-sentinel
assert_rejects_detached_pin \
  conf-rust-2024 1 zz-conf-rust-pin-sentinel

printf '%s\n' "install consumer metadata pin mutations: ok"

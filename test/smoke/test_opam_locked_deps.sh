#!/bin/sh
set -eu

# Exercise scripts/opam-locked-deps.sh against a stub `opam` executable, so
# the lock-enforcement policy is tested without network access or a real
# switch. The stub answers `opam var <package>:version` from a fixture table
# and records `opam install` arguments.
source_root=$(cd "${1:-.}" && pwd)
script=$source_root/scripts/opam-locked-deps.sh
# Keep the fixture below the workspace: native Windows tools under Cygwin
# cannot resolve Cygwin's /tmp, and the stub is a POSIX script either way.
fixture=$(mktemp -d "./temporal-sdk-opam-lock.XXXXXX")
fixture=$(cd "$fixture" && pwd)
cleanup() {
  case "$(basename "$fixture")" in
    temporal-sdk-opam-lock.*) rm -rf -- "$fixture" ;;
  esac
}
trap cleanup EXIT HUP INT TERM

fail() {
  echo "opam-locked-deps contract: $*" >&2
  exit 1
}

mkdir -p "$fixture/bin" "$fixture/root/scripts"
cp "$source_root/temporal-sdk.opam.locked" "$fixture/root/"
cp "$source_root/scripts/opam-lock-overrides.txt" "$fixture/root/scripts/"

# Stub opam: installed versions live in $fixture/installed as `name version`.
cat >"$fixture/bin/opam" <<'EOF'
#!/bin/sh
case "$1" in
  var)
    package=${2%%:*}
    version=$(awk -v p="$package" '$1 == p { print $2 }' "$OPAM_STUB_INSTALLED")
    [ -n "$version" ] || { echo '#undefined'; exit 0; }
    printf '%s\r\n' "$version"
    ;;
  install)
    shift
    printf '%s\n' "$*" >"$OPAM_STUB_INSTALL_LOG"
    ;;
  *) echo "unexpected opam command: $*" >&2; exit 2 ;;
esac
EOF
chmod +x "$fixture/bin/opam"
export PATH="$fixture/bin:$PATH"
export OPAM_LOCK_ROOT="$fixture/root"
export OPAM_STUB_INSTALLED="$fixture/installed"
export OPAM_STUB_INSTALL_LOG="$fixture/install.log"

# Populate the stub switch with the exact lock, optionally replacing versions.
# Arguments are `name=version` replacements applied after the locked values.
install_switch() {
  sed -n 's/^  "\([^"]*\)" {= "\([^"]*\)"}$/\1 \2/p' \
    "$fixture/root/temporal-sdk.opam.locked" >"$OPAM_STUB_INSTALLED"
  for replacement in "$@"; do
    name=${replacement%%=*}
    awk -v p="$name" -v v="${replacement#*=}" \
      '$1 == p { $2 = v; seen = 1 } { print } END { if (!seen) print p, v }' \
      "$OPAM_STUB_INSTALLED" >"$OPAM_STUB_INSTALLED.next"
    mv "$OPAM_STUB_INSTALLED.next" "$OPAM_STUB_INSTALLED"
  done
}

# The locked compiler installs every non-compiler package at its lock version.
install_switch
plan=$(sh "$script" plan)
for expected in dune.3.24.2 logs.0.10.0 yojson.3.0.0 ocamlfind.1.9.8; do
  printf '%s\n' "$plan" | grep -Fx "$expected" >/dev/null ||
    fail "5.2 plan omits $expected"
done
if printf '%s\n' "$plan" | grep -E '^(ocaml|ocaml-base-compiler|ocaml-config|ocaml-options-vanilla|base-[a-z]+)\.' >/dev/null; then
  fail "plan installs compiler-provided packages"
fi
sh "$script" check >/dev/null || fail "an exactly locked switch was rejected"

# Installation forwards caller options and the plan, then verifies the switch.
sh "$script" install --no-depexts >/dev/null
case " $(cat "$OPAM_STUB_INSTALL_LOG") " in
  ' --yes --no-depexts '*' dune.3.24.2 '*' yojson.3.0.0 '*) ;;
  *) fail "install did not forward options and exact package versions" ;;
esac

# A runtime library that drifts from the audited lock must fail loudly.
install_switch yojson=3.0.1
if sh "$script" check >"$fixture/drift.log" 2>&1; then
  fail "a drifted yojson version was accepted"
fi
grep -F 'MISMATCH yojson installed=3.0.1 expected=3.0.0' "$fixture/drift.log" >/dev/null ||
  fail "drift diagnostic does not name the package and versions"

# Another compiler series keeps the compiler it was given and applies only its
# own documented overrides.
install_switch ocaml=5.5.1 ocaml-base-compiler=5.5.1 'ocamlfind=1.9.9~preview'
sh "$script" plan | grep -Fx 'ocamlfind.1.9.9~preview' >/dev/null ||
  fail "5.5 plan does not apply its ocamlfind override"
sh "$script" check >/dev/null || fail "a locked 5.5 switch with its override was rejected"
install_switch ocaml=5.3.0 ocaml-base-compiler=5.3.0 'ocamlfind=1.9.9~preview'
if sh "$script" check >/dev/null 2>&1; then
  fail "a 5.5-only override was accepted for OCaml 5.3"
fi

# Overrides cannot introduce unlocked packages or replace the compiler.
install_switch
for bad in '5.2 unlocked-package 1.0' '5.2 ocaml 5.2.0' '5.2 dune'; do
  printf '%s\n' "$bad" >"$fixture/root/scripts/opam-lock-overrides.txt"
  if sh "$script" plan >/dev/null 2>&1; then
    fail "invalid override was accepted: $bad"
  fi
done

echo "opam-locked-deps contract: ok"

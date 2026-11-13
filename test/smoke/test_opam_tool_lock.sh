#!/bin/sh
set -eu

# Exercise scripts/opam-tool-lock.sh against a stub `opam` executable, so the
# CI-only tool-lock policy (the docs and Eio example stages, #1141) is tested
# without network access or a real switch. The stub keeps its switch as
# `name version` lines, answers `opam var`, `opam list` and `opam install`
# from it, and can be told to add or move packages behind the caller's back,
# as transitive dependencies or new upstream metadata would.
source_root=$(cd "${1:-.}" && pwd)
script=$source_root/scripts/opam-tool-lock.sh
# Keep the fixture below the workspace: native Windows tools under Cygwin
# cannot resolve Cygwin's /tmp, and the stub is a POSIX script either way.
fixture=$(mktemp -d "./temporal-sdk-opam-tool-lock.XXXXXX")
fixture=$(cd "$fixture" && pwd)
cleanup() {
  case "$(basename "$fixture")" in
    temporal-sdk-opam-tool-lock.*) rm -rf -- "$fixture" ;;
  esac
}
trap cleanup EXIT HUP INT TERM

fail() {
  echo "opam-tool-lock contract: $*" >&2
  exit 1
}

mkdir -p "$fixture/bin" "$fixture/root/scripts"
cp "$source_root/temporal-sdk.opam.locked" "$fixture/root/"
cp "$source_root/scripts/opam-lock-overrides.txt" \
  "$source_root/scripts/opam-locked-deps.sh" "$fixture/root/scripts/"
tool_lock=$source_root/scripts/eio-example.locked

# Stub opam. `install` applies each `name.version` argument, then the
# `name version` lines of $OPAM_STUB_EXTRA, which model packages that a real
# install would add or move without being asked.
cat >"$fixture/bin/opam" <<'EOF'
#!/bin/sh
# Replace or add one `name version` line in the stub switch.
set_package() {
  awk -v p="$1" -v v="$2" \
    '$1 == p { $2 = v; seen = 1 } { print } END { if (!seen) print p, v }' \
    "$OPAM_STUB_INSTALLED" >"$OPAM_STUB_INSTALLED.next"
  mv "$OPAM_STUB_INSTALLED.next" "$OPAM_STUB_INSTALLED"
}
case "$1" in
  var)
    package=${2%%:*}
    version=$(awk -v p="$package" '$1 == p { print $2 }' "$OPAM_STUB_INSTALLED")
    [ -n "$version" ] || { echo '#undefined'; exit 0; }
    printf '%s\r\n' "$version"
    ;;
  list)
    awk '{ printf "%-20s %s\r\n", $1, $2 }' "$OPAM_STUB_INSTALLED"
    ;;
  install)
    shift
    for spec in "$@"; do
      case "$spec" in
        -*) ;;
        *) set_package "${spec%%.*}" "${spec#*.}" ;;
      esac
    done
    if [ -n "${OPAM_STUB_EXTRA:-}" ]; then
      printf '%s\n' "$OPAM_STUB_EXTRA" | while read -r name version; do
        set_package "$name" "$version"
      done
    fi
    ;;
  *) echo "unexpected opam command: $*" >&2; exit 2 ;;
esac
EOF
chmod +x "$fixture/bin/opam"
export PATH="$fixture/bin:$PATH"
export OPAM_LOCK_ROOT="$fixture/root"
export OPAM_STUB_INSTALLED="$fixture/installed"

# Populate the stub switch with exactly the SDK lock, plus any `name version`
# lines given as arguments.
sdk_switch() {
  sed -n 's/^  "\([^"]*\)" {= "\([^"]*\)"}$/\1 \2/p' \
    "$fixture/root/temporal-sdk.opam.locked" >"$OPAM_STUB_INSTALLED"
  for line in "$@"; do
    printf '%s\n' "$line" >>"$OPAM_STUB_INSTALLED"
  done
}

# Require a failing install whose diagnostics name $2.
expect_install_failure() {
  label=$1
  needle=$2
  if sh "$script" install "$tool_lock" --no-depexts >"$fixture/out" 2>&1; then
    fail "accepted $label"
  fi
  grep -F -- "$needle" "$fixture/out" >/dev/null ||
    fail "$label: diagnostics do not name '$needle'"
}

# A fresh SDK switch gains exactly the tool lock.
sdk_switch
OPAM_STUB_EXTRA='' sh "$script" install "$tool_lock" --no-depexts >/dev/null ||
  fail "a fresh switch was rejected"
sh "$script" check "$tool_lock" >/dev/null || fail "the installed tool lock was rejected"

# A switch that already holds some listed packages, one at another version,
# installs the rest and moves that one to its locked version.
sdk_switch 'eio 1.3' 'fmt 0.11.0'
OPAM_STUB_EXTRA='' sh "$script" install "$tool_lock" --no-depexts >/dev/null ||
  fail "a switch with some listed packages was rejected"

# An unlisted transitive dependency fails and is named.
sdk_switch
OPAM_STUB_EXTRA='uring 2.7.0' expect_install_failure 'an unlisted package' 'uring 2.7.0'

# A locked SDK package that the install moves fails, by either name or version.
sdk_switch
OPAM_STUB_EXTRA='dune 3.25.0' expect_install_failure 'a moved SDK package' 'dune'

# A listed package that the install leaves at another version fails.
sdk_switch
OPAM_STUB_EXTRA='eio 1.5' expect_install_failure 'a listed package at another version' 'eio 1.5'

# `check` alone rejects a switch without the tool lock.
sdk_switch
if sh "$script" check "$tool_lock" >/dev/null 2>&1; then
  fail "check accepted a switch without the tool lock"
fi

echo "opam-tool-lock contract: ok"

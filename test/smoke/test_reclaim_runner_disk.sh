#!/bin/sh
set -eu

# Docker-free check of scripts/reclaim-runner-disk.sh. Stub df, sudo and docker
# commands on PATH record what the script would delete, so both the skip and
# the cleanup branch run on any POSIX host without touching the real disk.

source_root=${1:-.}
script=$source_root/scripts/reclaim-runner-disk.sh
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
mkdir "$scratch/bin"

fail() {
  echo "reclaim-runner-disk contract: $*" >&2
  exit 1
}

# df reports FAKE_AVAILABLE_KIB before cleanup and FAKE_AFTER_KIB once the
# sudo stub has recorded a deletion, mimicking the space it frees.
cat >"$scratch/bin/df" <<'STUB'
#!/bin/sh
available=$FAKE_AVAILABLE_KIB
if [ -s "$FAKE_LOG" ] && grep -q '^sudo ' "$FAKE_LOG"; then
  available=$FAKE_AFTER_KIB
fi
echo 'Filesystem 1024-blocks Used Available Capacity Mounted on'
echo "/dev/root 152043520 1 $available 50% /"
STUB
cat >"$scratch/bin/sudo" <<'STUB'
#!/bin/sh
echo "sudo $*" >>"$FAKE_LOG"
STUB
cat >"$scratch/bin/docker" <<'STUB'
#!/bin/sh
echo "docker $*" >>"$FAKE_LOG"
STUB
chmod +x "$scratch/bin/df" "$scratch/bin/sudo" "$scratch/bin/docker"

gib=1048576
# Runs the script with the given free space (KiB) and arguments, leaving the
# recorded commands in $scratch/log and the output in $scratch/out.
run() {
  available=$1
  after=$2
  shift 2
  : >"$scratch/log"
  PATH="$scratch/bin:$PATH" FAKE_LOG="$scratch/log" \
    FAKE_AVAILABLE_KIB=$available FAKE_AFTER_KIB=$after \
    sh "$script" "$@" >"$scratch/out" 2>&1
}

# Enough space: nothing is deleted, with or without --prune-docker.
run $((86 * gib)) $((86 * gib)) 40 --prune-docker || fail "skip branch failed: $(cat "$scratch/out")"
[ ! -s "$scratch/log" ] || fail "deleted with 86 GiB free: $(cat "$scratch/log")"
grep -F 'keeping the preinstalled toolchains' "$scratch/out" >/dev/null \
  || fail "skip branch did not explain itself"
run $((40 * gib)) $((40 * gib)) 40 || fail "boundary run failed"
[ ! -s "$scratch/log" ] || fail "deleted with exactly the needed space free"

# Too little space: delete the toolchains, and prune Docker only on request.
run $((14 * gib)) $((60 * gib)) 40 || fail "cleanup branch failed: $(cat "$scratch/out")"
grep -Fx 'sudo rm -rf /usr/local/lib/android /usr/share/dotnet /opt/ghc /usr/local/.ghcup' \
  "$scratch/log" >/dev/null || fail "toolchains were not deleted: $(cat "$scratch/log")"
! grep -F 'docker' "$scratch/log" >/dev/null || fail "pruned Docker without --prune-docker"
run $((14 * gib)) $((60 * gib)) 40 --prune-docker || fail "prune run failed"
grep -Fx 'docker system prune --all --force --volumes' "$scratch/log" >/dev/null \
  || fail "--prune-docker did not prune: $(cat "$scratch/log")"
! grep -F '::warning::' "$scratch/out" >/dev/null || fail "warned after a sufficient cleanup"

# Still short after cleanup: warn, but leave the decision to the job.
run $((14 * gib)) $((20 * gib)) 40 || fail "a short cleanup must not fail the step"
grep -F '::warning::only 20 GiB free' "$scratch/out" >/dev/null \
  || fail "no warning when cleanup could not free enough: $(cat "$scratch/out")"

# Malformed invocations are usage errors, not silent skips.
for args in '' 'forty' '40 --prune' '40 --prune-docker extra'; do
  # Word splitting of the fixture arguments is intended.
  # shellcheck disable=SC2086
  if run $((86 * gib)) $((86 * gib)) $args; then
    fail "accepted invalid arguments: '$args'"
  fi
done

echo "reclaim-runner-disk contract: ok"

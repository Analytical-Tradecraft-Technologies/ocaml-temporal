#!/bin/sh
# Frees root-filesystem space on a disposable GitHub-hosted Ubuntu runner, but
# only when the space a job needs is not already free.
#
# Usage: reclaim-runner-disk.sh MIN_FREE_GIB [--prune-docker]
#
# MIN_FREE_GIB is the free space, in GiB, that the calling job needs on `/`.
# When at least that much is available the script only reports `df` and
# exits. Otherwise it deletes the preinstalled Android, .NET and Haskell
# toolchains and, with --prune-docker, every preinstalled Docker image and
# volume.
#
# Hosted ubuntu-24.04 images once left about 14 GB free, so CI deleted those
# toolchains on every run. Since late 2026 the image has a 145 GB disk with
# 75-86 GB free, and the unconditional deletion only cost 2-5 minutes on the
# merge-queue critical path in each of the Rust, OCaml and live smoke jobs.
# Checking first keeps the safety net if a future image shrinks again.
#
# The deletion is destructive and must only run on a disposable hosted
# runner: callers guard the step with runner.environment == 'github-hosted'.
# test/smoke/test_reclaim_runner_disk.sh checks both branches with stubbed
# df, sudo and docker commands.
set -eu

usage() {
  echo "usage: $0 MIN_FREE_GIB [--prune-docker]" >&2
  exit 2
}

[ "$#" -ge 1 ] && [ "$#" -le 2 ] || usage
min_free_gib=$1
case $min_free_gib in
  '' | *[!0-9]*) usage ;;
esac
prune_docker=false
if [ "$#" -eq 2 ]; then
  [ "$2" = --prune-docker ] || usage
  prune_docker=true
fi

# Free space on `/` in 1024-byte blocks. POSIX `df -P` keeps each filesystem
# on one line, so the fourth field of the second line is "Available".
available_kib() {
  df -Pk / | awk 'NR == 2 { print $4 }'
}

df -h /
available=$(available_kib)
case $available in
  '' | *[!0-9]*)
    echo "cannot read the free space on / from df" >&2
    exit 1
    ;;
esac
needed=$((min_free_gib * 1024 * 1024))
if [ "$available" -ge "$needed" ]; then
  echo "$((available / 1048576)) GiB free on / (need $min_free_gib GiB); keeping the preinstalled toolchains"
  exit 0
fi

echo "$((available / 1048576)) GiB free on / (need $min_free_gib GiB); deleting preinstalled toolchains"
sudo rm -rf /usr/local/lib/android /usr/share/dotnet /opt/ghc /usr/local/.ghcup
if [ "$prune_docker" = true ]; then
  docker system prune --all --force --volumes
fi
df -h /
available=$(available_kib)
if [ "$available" -lt "$needed" ]; then
  # The job may still fit, so report rather than fail; a later
  # "No space left on device" then points straight at this warning.
  echo "::warning::only $((available / 1048576)) GiB free on / after cleanup (need $min_free_gib GiB)"
fi

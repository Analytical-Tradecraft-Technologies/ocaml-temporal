#!/bin/sh
# Start the diagnostic deadline before opam/Dune compilation. Reserve a bounded
# part of the existing process budget for reporting and client shutdown.
set -eu
budget=${SMOKE_CACHE_EVICTION_TIMEOUT_SECONDS:-900}
case "$budget" in
  ''|0*|*[!0-9]*|??????*) echo "cache eviction timeout must be an integer >= 2" >&2; exit 2 ;;
esac
if [ "$budget" -lt 2 ] || [ "$budget" -gt 86400 ]; then
  echo "cache eviction timeout must be between 2 and 86400 seconds" >&2
  exit 2
fi
reserve=$((budget / 10))
[ "$reserve" -ge 1 ] || reserve=1
[ "$reserve" -le 30 ] || reserve=30
SMOKE_CACHE_EVICTION_DEADLINE_EPOCH=$(($(date +%s) + budget - reserve))
export SMOKE_CACHE_EVICTION_DEADLINE_EPOCH
exec timeout --signal=TERM --kill-after=10s "$budget" "$@"

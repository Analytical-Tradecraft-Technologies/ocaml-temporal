#!/bin/sh
set -eu

# This script runs inside the official admin-tools image. A successful TCP
# probe alone is not enough: the CLI health RPC proves that the gRPC frontend
# can serve requests. Namespace description proves persistence is usable,
# but the search-attribute API must also observe the newly registered namespace.
address=${TEMPORAL_ADDRESS:-temporal:7233}
namespace=${TEMPORAL_NAMESPACE:-temporal-sdk-test}
max_attempts=${TEMPORAL_HEALTH_MAX_ATTEMPTS:-60}
sleep_seconds=${TEMPORAL_HEALTH_SLEEP_SECONDS:-2}

attempt=1
while ! temporal operator cluster health --address "$address"; do
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "Temporal frontend did not become healthy after $max_attempts attempts" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep "$sleep_seconds"
done

if ! temporal operator namespace describe --namespace "$namespace" --address "$address" >/dev/null 2>&1; then
  temporal operator namespace create \
    --namespace "$namespace" \
    --retention 1d \
    --address "$address"
fi

temporal operator namespace describe \
  --namespace "$namespace" \
  --address "$address" \
  >/dev/null

# Registration can precede the frontend namespace cache becoming usable. Probe
# the same read-only API used by metadata acceptance; retry only the observed
# namespace-not-found response so permission/configuration errors remain fatal.
attempt=1
while true; do
  if readiness_error=$(temporal operator search-attribute list \
    --namespace "$namespace" --address "$address" -o json 2>&1); then
    break
  fi
  case "$readiness_error" in
    *"Namespace $namespace is not found."*) ;;
    *) printf '%s\n' "$readiness_error" >&2; exit 1 ;;
  esac
  if [ "$attempt" -ge "$max_attempts" ]; then
    printf 'Temporal namespace %s search-attribute readiness timed out after %s attempts\n' \
      "$namespace" "$max_attempts" >&2
    printf '%s\n' "$readiness_error" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep "$sleep_seconds"
done

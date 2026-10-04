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

# Only frontend availability and the CLI connection deadline are transient.
# Authentication, configuration, and other RPC failures must fail immediately.
is_transient_cli_error() {
  case "$1" in
    *"code = Unavailable"*|*"code = DeadlineExceeded"*) return 0 ;;
    "Error: failed connecting to Temporal server at $address: context deadline exceeded") return 0 ;;
  esac
  return 1
}

# Every namespace probe shares one bounded wait and retains its last error.
# The caller supplies the operation name and resets attempt for a new phase.
retry_namespace_probe() {
  if [ "$attempt" -ge "$max_attempts" ]; then
    printf 'Temporal namespace %s %s timed out after %s attempts\n' \
      "$namespace" "$1" "$max_attempts" >&2
    printf '%s\n' "$2" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep "$sleep_seconds"
}

attempt=1
while ! temporal operator cluster health --address "$address"; do
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "Temporal frontend did not become healthy after $max_attempts attempts" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep "$sleep_seconds"
done

# A failed describe is not necessarily a missing namespace: the frontend may
# briefly return Unavailable or a CLI connection deadline even after its health
# check succeeds. Create only on an explicit NotFound response, then wait for
# describe to observe it. A transient create failure may have committed on the
# server, so re-describe before another create attempt.
attempt=1
created=0
while true; do
  if describe_error=$(temporal operator namespace describe \
    --namespace "$namespace" --address "$address" 2>&1); then
    break
  fi
  case "$describe_error" in
    *"Namespace $namespace is not found."*)
      if [ "$created" -eq 0 ]; then
        if create_error=$(temporal operator namespace create \
          --namespace "$namespace" \
          --retention 1d \
          --address "$address" 2>&1); then
          created=1
          continue
        fi
        if is_transient_cli_error "$create_error"; then
          retry_namespace_probe create "$create_error"
          continue
        fi
        case "$create_error" in
          *"code = AlreadyExists"*|*"Namespace already exists"*|*"Namespace $namespace already exists"*)
            # A racing registration or an earlier timed-out create is usable
            # only after the describe probe confirms it.
            created=1
            continue ;;
          *) printf '%s\n' "$create_error" >&2; exit 1 ;;
        esac
      fi
      ;;
    *)
      if ! is_transient_cli_error "$describe_error"; then
        printf '%s\n' "$describe_error" >&2
        exit 1
      fi ;;
  esac
  retry_namespace_probe describe "$describe_error"
done

# Registration can precede the frontend namespace cache becoming usable. Probe
# the same read-only API used by metadata acceptance; namespace propagation and
# transient frontend failures share the bounded retry above.
attempt=1
while true; do
  if readiness_error=$(temporal operator search-attribute list \
    --namespace "$namespace" --address "$address" -o json 2>&1); then
    break
  fi
  case "$readiness_error" in
    *"Namespace $namespace is not found."*) ;;
    *)
      if ! is_transient_cli_error "$readiness_error"; then
        printf '%s\n' "$readiness_error" >&2
        exit 1
      fi ;;
  esac
  retry_namespace_probe 'search-attribute readiness' "$readiness_error"
done

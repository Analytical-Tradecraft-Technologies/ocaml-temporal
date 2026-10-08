#!/bin/sh
set -eu

# Runs the fixed-bug, feature and recovery live regressions (#499, #530,
# #545, #546, #548, #567, #805) against the already-started Compose
# Temporal/PostgreSQL stack (#795). The caller owns the server lifecycle;
# each regression executable owns its worker processes, uses a unique task
# queue, and terminates the workflow executions it creates.
#
# Arguments are repository-relative executables. Each must already exist under
# _build/default: the Makefile target compiles them locally, and CI unpacks the
# verified smoke artifact (TEMPORAL_PREBUILT_SMOKE=1). Running the binaries
# directly avoids a `dune exec` rebuild in CI and a held Dune lock locally.
#
# Every executable needs an explicit invocation below. An unknown executable is
# a configuration error rather than a silently skipped suite, and
# test/smoke/test_live_regressions_contract.sh requires the same membership.
root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal"
compose_file="$fixture/compose.yaml"
project=${TEMPORAL_COMPOSE_PROJECT:-ocaml-temporal-integration}
# Seconds for one regression process, including its fresh-worker replays. The
# local-activity fixture also arms its own 120-second alarm; this outer bound
# guarantees no regression can hang the shared CI job.
limit=${LIVE_REGRESSION_TIMEOUT_SECONDS:-180}
address=http://temporal:7233
evidence="$root/_build/live-regressions"
# The same bind-mounted file seen from the development container.
container_cli=/workspace/_build/live-regressions/temporal

[ "$#" -gt 0 ] || { echo "usage: $0 test/integration/<suite>/regression.exe..." >&2; exit 2; }
for executable in "$@"; do
  case "$executable" in
    test/integration/*.exe) ;;
    *) echo "invalid live regression executable: $executable" >&2; exit 2 ;;
  esac
  case "$executable" in *..*) echo "invalid live regression path: $executable" >&2; exit 2 ;; esac
  test -x "$root/_build/default/$executable" || {
    echo "missing compiled live regression executable: $executable" >&2
    exit 1
  }
done

# Applies one normalized Compose invocation, matching the other controllers so
# the existing development image and network are reused rather than rebuilt.
compose() {
  OCAML_IMAGE=${OCAML_IMAGE:-ocaml-5.2} \
    HOST_UID=${HOST_UID:-$(id -u)} HOST_GID=${HOST_GID:-$(id -g)} \
    docker compose --project-directory "$fixture" --file "$compose_file" \
      --project-name "$project" --profile temporal "$@"
}

# Runs one bounded command in the development image on the Compose network as
# the invoking user, so bind-mounted outputs keep host ownership. HOME points
# at a writable directory because the numeric user has no passwd entry, and
# OPAMROOT stays pinned to the image's switch because the image entrypoint
# (`opam exec --`) would otherwise look for an uninitialised root under HOME.
in_dev() {
  seconds=$1
  shift
  compose run --rm --no-deps -T \
    --user "${HOST_UID:-$(id -u)}:${HOST_GID:-$(id -g)}" --env HOME=/tmp \
    --env OPAMROOT=/home/opam/.opam \
    dev timeout --signal=TERM --kill-after=10s "$seconds" "$@"
}

rm -rf "$evidence"
mkdir -p "$evidence"

# Four fixtures predate the dedicated SDK namespace and use `default`, which
# the plain server image does not create (temporal-start registers only
# temporal-sdk-test, the split-worker fixture's default). Reuse the bounded health/namespace
# probe so registration is idempotent and propagation is awaited.
compose run --rm --no-deps -T --env TEMPORAL_NAMESPACE=default \
  --entrypoint /bin/sh temporal-admin-tools /scripts/check-temporal-stack.sh

# Three fixtures inspect or delete only their own executions through the
# official CLI. Copy the pinned admin-tools binary into the ignored build tree
# instead of installing another CLI into the development image, then prove it
# runs there before any regression depends on it.
# The single quotes are deliberate: the substitution runs in the container.
# shellcheck disable=SC2016
compose run --rm --no-deps -T --entrypoint /bin/sh temporal-admin-tools \
  -c 'exec cat "$(command -v temporal)"' >"$evidence/temporal.partial"
mv "$evidence/temporal.partial" "$evidence/temporal"
chmod 0755 "$evidence/temporal"
in_dev 60 "$container_cli" --version

failed=''
for executable in "$@"; do
  case "$executable" in
    test/integration/client_request_ids/regression.exe|\
    test/integration/completed_queries/regression.exe|\
    test/integration/split_worker_task_types/regression.exe)
      cli_argument='' ;;
    test/integration/client_policies/regression.exe|\
    test/integration/interaction_recovery/regression.exe|\
    test/integration/local_activity_cancellation/regression.exe|\
    test/integration/update_outcomes/regression.exe)
      cli_argument=$container_cli ;;
    *)
      echo "no live regression invocation is defined for $executable" >&2
      exit 2 ;;
  esac
  suite=${executable#test/integration/}
  suite=${suite%%/*}
  log="$evidence/$suite.log"
  printf 'live regression %s: begin\n' "$suite"
  status=0
  # cli_argument is either empty or one path without whitespace.
  # shellcheck disable=SC2086
  in_dev "$limit" "/workspace/_build/default/$executable" check "$address" $cli_argument \
    >"$log" 2>&1 || status=$?
  cat "$log"
  case "$status" in
    0) outcome=ok ;;
    124|137) outcome=timeout ;;
    *) outcome=failed ;;
  esac
  printf 'live regression %s: %s\n' "$suite" "$outcome"
  # Run every suite so one failure reports all regressions in a single job.
  [ "$status" -eq 0 ] || failed="$failed $suite"
done

if [ -n "$failed" ]; then
  echo "live regressions failed:$failed" >&2
  exit 1
fi
printf 'Live regressions passed. Logs: %s\n' "$evidence"

#!/bin/sh
set -eu

# Run the real stack setup with a fake CLI to control namespace propagation
# independently of runner speed, Docker, or a live Temporal server.
root=${1:-$(CDPATH='' cd -- "$(dirname "$0")/../.." && pwd)}
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT HUP INT TERM
mkdir "$fixture/bin"
cat > "$fixture/bin/temporal" <<'CLI'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$FIXTURE/calls"
case "$*" in
  'operator cluster health '*) exit 0 ;;
  'operator namespace describe '*)
    count=$(cat "$FIXTURE/describe-count")
    count=$((count + 1))
    printf '%s\n' "$count" > "$FIXTURE/describe-count"
    case "$DESCRIBE_SCENARIO" in
      transient) if [ "$count" -eq 1 ]; then
        echo 'rpc error: code = Unavailable desc = frontend restarting' >&2
        exit 1
      fi ;;
      cli_deadline_once) if [ "$count" -eq 1 ]; then
        echo "Error: failed connecting to Temporal server at $TEMPORAL_ADDRESS: context deadline exceeded" >&2
        exit 1
      fi ;;
      cli_deadline_permanent)
        echo "Error: failed connecting to Temporal server at $TEMPORAL_ADDRESS: context deadline exceeded" >&2
        exit 1 ;;
      cli_auth) echo 'Error: authentication failed: invalid API key' >&2; exit 1 ;;
      cli_bad_config) echo 'Error: invalid Temporal server configuration' >&2; exit 1 ;;
      pending) if [ "$count" -eq 2 ]; then
        echo 'Error: Namespace readiness-test is not found.' >&2
        exit 1
      fi ;;
      permanent) echo 'rpc error: code = Unavailable desc = frontend restarting' >&2; exit 1 ;;
      denied) echo 'rpc error: code = PermissionDenied desc = forbidden' >&2; exit 1 ;;
      wrong_namespace) echo 'Error: Namespace another-test is not found.' >&2; exit 1 ;;
    esac
    if test -f "$FIXTURE/registered"; then exit 0; fi
    echo 'Error: Namespace readiness-test is not found.' >&2
    exit 1 ;;
  'operator namespace create '*)
    count=$(cat "$FIXTURE/create-count")
    count=$((count + 1))
    printf '%s\n' "$count" > "$FIXTURE/create-count"
    if test -f "$FIXTURE/registered"; then
      echo 'Namespace readiness-test already exists.' >&2
      exit 1
    fi
    case "$CREATE_SCENARIO" in
      unavailable_once) if [ "$count" -eq 1 ]; then
        echo 'rpc error: code = Unavailable desc = frontend restarting' >&2
        exit 1
      fi ;;
      deadline_once) if [ "$count" -eq 1 ]; then
        echo 'rpc error: code = DeadlineExceeded desc = frontend not ready' >&2
        exit 1
      fi ;;
      cli_deadline_once) if [ "$count" -eq 1 ]; then
        echo "Error: failed connecting to Temporal server at $TEMPORAL_ADDRESS: context deadline exceeded" >&2
        exit 1
      fi ;;
      unavailable_permanent)
        echo 'rpc error: code = Unavailable desc = frontend restarting' >&2
        exit 1 ;;
      ambiguous_commit_once) if [ "$count" -eq 1 ]; then
        touch "$FIXTURE/registered"
        echo 'rpc error: code = Unavailable desc = response lost' >&2
        exit 1
      fi ;;
      denied) echo 'rpc error: code = PermissionDenied desc = forbidden' >&2; exit 1 ;;
      bad_config) echo 'Error: invalid Temporal server configuration' >&2; exit 1 ;;
    esac
    touch "$FIXTURE/registered"
    exit 0 ;;
  'operator search-attribute list '*) ;;
  *) exit 99 ;;
esac
count=$(cat "$FIXTURE/count")
count=$((count + 1))
printf '%s\n' "$count" > "$FIXTURE/count"
case "$SCENARIO" in
  ready) exit 0 ;;
  transient) if [ "$count" -ge 3 ]; then exit 0; fi ;;
  permanent) ;;
  unavailable_once) if [ "$count" -eq 1 ]; then
    echo 'rpc error: code = Unavailable desc = frontend restarting' >&2
    exit 1
  fi; exit 0 ;;
  deadline_once) if [ "$count" -eq 1 ]; then
    echo 'rpc error: code = DeadlineExceeded desc = frontend not ready' >&2
    exit 1
  fi; exit 0 ;;
  cli_deadline_once) if [ "$count" -eq 1 ]; then
    echo "Error: failed connecting to Temporal server at $TEMPORAL_ADDRESS: context deadline exceeded" >&2
    exit 1
  fi; exit 0 ;;
  unavailable_permanent)
    echo 'rpc error: code = Unavailable desc = frontend restarting' >&2
    exit 1 ;;
  denied) echo 'PermissionDenied: search attributes forbidden' >&2; exit 1 ;;
  bad_config) echo 'Error: invalid Temporal server configuration' >&2; exit 1 ;;
  wrong_namespace) echo 'Error: Namespace another-test is not found.' >&2; exit 1 ;;
esac
echo 'Error: unable to list search attributes: Namespace readiness-test is not found.' >&2
exit 1
CLI
chmod +x "$fixture/bin/temporal"

# Check success/failure, exact probe counts, and retained diagnostics. Optional
# arguments control describe/create failures and whether the namespace exists
# before setup. A nondefault namespace/address protects argument forwarding.
run_case() {
  scenario=$1 expected_status=$2 expected_count=$3
  describe_scenario=${4:-normal}
  pre_registered=${5:-no}
  expected_describes=${6:-2}
  expected_creates=${7:-1}
  create_scenario=${8:-normal}
  rm -f "$fixture/registered" "$fixture/calls"
  if [ "$pre_registered" = yes ]; then touch "$fixture/registered"; fi
  echo 0 > "$fixture/count"
  echo 0 > "$fixture/describe-count"
  echo 0 > "$fixture/create-count"
  status=0
  PATH="$fixture/bin:$PATH" FIXTURE="$fixture" SCENARIO="$scenario" \
    DESCRIBE_SCENARIO="$describe_scenario" CREATE_SCENARIO="$create_scenario" \
    TEMPORAL_ADDRESS=custom:7233 TEMPORAL_NAMESPACE=readiness-test \
    TEMPORAL_HEALTH_MAX_ATTEMPTS=3 TEMPORAL_HEALTH_SLEEP_SECONDS=0 \
    sh "$root/test/integration/temporal/scripts/check-temporal-stack.sh" \
    > "$fixture/output" 2>&1 || status=$?
  if [ "$status" -ne "$expected_status" ] ||
     [ "$(cat "$fixture/count")" -ne "$expected_count" ] ||
     [ "$(cat "$fixture/describe-count")" -ne "$expected_describes" ] ||
     [ "$(cat "$fixture/create-count")" -ne "$expected_creates" ]; then
    cat "$fixture/output" >&2
    echo "unexpected readiness result for $scenario/$describe_scenario: status=$status" >&2
    exit 1
  fi
  if [ "$expected_count" -gt 0 ]; then
    grep -F 'operator search-attribute list --namespace readiness-test --address custom:7233 -o json' "$fixture/calls" >/dev/null
  fi
}

run_case ready 0 1
run_case transient 0 3
run_case unavailable_once 0 2
run_case deadline_once 0 2
run_case cli_deadline_once 0 2
run_case permanent 1 3
grep -F 'readiness timed out after 3 attempts' "$fixture/output" >/dev/null
grep -F 'Namespace readiness-test is not found.' "$fixture/output" >/dev/null
run_case denied 1 1
grep -F 'PermissionDenied: search attributes forbidden' "$fixture/output" >/dev/null
run_case bad_config 1 1
grep -F 'invalid Temporal server configuration' "$fixture/output" >/dev/null
run_case wrong_namespace 1 1
grep -F 'Namespace another-test is not found.' "$fixture/output" >/dev/null
run_case unavailable_permanent 1 3
grep -F 'search-attribute readiness timed out after 3 attempts' "$fixture/output" >/dev/null
grep -F 'code = Unavailable' "$fixture/output" >/dev/null
run_case ready 0 1 normal yes 1 0
run_case ready 0 1 transient yes 2 0
run_case ready 0 1 cli_deadline_once yes 2 0
run_case ready 0 1 cli_deadline_once no 3 1
run_case ready 1 0 cli_deadline_permanent yes 3 0
grep -F 'describe timed out after 3 attempts' "$fixture/output" >/dev/null
grep -F 'failed connecting to Temporal server at custom:7233: context deadline exceeded' "$fixture/output" >/dev/null
run_case ready 1 0 cli_auth yes 1 0
grep -F 'authentication failed: invalid API key' "$fixture/output" >/dev/null
run_case ready 1 0 cli_bad_config yes 1 0
grep -F 'invalid Temporal server configuration' "$fixture/output" >/dev/null
run_case ready 0 1 pending no 3 1
run_case ready 1 0 permanent yes 3 0
grep -F 'describe timed out after 3 attempts' "$fixture/output" >/dev/null
run_case ready 1 0 denied yes 1 0
grep -F 'PermissionDenied' "$fixture/output" >/dev/null
run_case ready 1 0 wrong_namespace yes 1 0
grep -F 'Namespace another-test is not found.' "$fixture/output" >/dev/null
run_case ready 0 1 normal no 3 2 unavailable_once
run_case ready 0 1 normal no 3 2 deadline_once
run_case ready 0 1 normal no 3 2 cli_deadline_once
run_case ready 0 1 normal no 2 1 ambiguous_commit_once
run_case ready 0 1 pending no 3 2 ambiguous_commit_once
run_case ready 1 0 normal no 3 3 unavailable_permanent
grep -F 'create timed out after 3 attempts' "$fixture/output" >/dev/null
grep -F 'code = Unavailable' "$fixture/output" >/dev/null
run_case ready 1 0 normal no 1 1 denied
grep -F 'PermissionDenied' "$fixture/output" >/dev/null
run_case ready 1 0 normal no 1 1 bad_config
grep -F 'invalid Temporal server configuration' "$fixture/output" >/dev/null
printf 'Temporal namespace readiness tests: ok\n'

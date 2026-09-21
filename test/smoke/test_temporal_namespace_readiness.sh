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
  'operator namespace describe '*) test -f "$FIXTURE/registered"; exit $? ;;
  'operator namespace create '*) touch "$FIXTURE/registered"; exit 0 ;;
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
  denied) echo 'PermissionDenied: search attributes forbidden' >&2; exit 1 ;;
  wrong_namespace) echo 'Error: Namespace another-test is not found.' >&2; exit 1 ;;
esac
echo 'Error: unable to list search attributes: Namespace readiness-test is not found.' >&2
exit 1
CLI
chmod +x "$fixture/bin/temporal"

# Check success/failure, exact probe counts, and retained diagnostics. Using a
# nondefault namespace/address also protects argument forwarding to the probe.
run_case() {
  scenario=$1 expected_status=$2 expected_count=$3
  rm -f "$fixture/registered" "$fixture/calls"
  echo 0 > "$fixture/count"
  status=0
  PATH="$fixture/bin:$PATH" FIXTURE="$fixture" SCENARIO="$scenario" \
    TEMPORAL_ADDRESS=custom:7233 TEMPORAL_NAMESPACE=readiness-test \
    TEMPORAL_HEALTH_MAX_ATTEMPTS=3 TEMPORAL_HEALTH_SLEEP_SECONDS=0 \
    sh "$root/test/integration/temporal/scripts/check-temporal-stack.sh" \
    > "$fixture/output" 2>&1 || status=$?
  if [ "$status" -ne "$expected_status" ] || [ "$(cat "$fixture/count")" -ne "$expected_count" ]; then
    cat "$fixture/output" >&2
    echo "unexpected readiness result for $scenario: status=$status" >&2
    exit 1
  fi
  grep -F 'operator search-attribute list --namespace readiness-test --address custom:7233 -o json' "$fixture/calls" >/dev/null
}

run_case ready 0 1
run_case transient 0 3
run_case permanent 1 3
grep -F 'readiness timed out after 3 attempts' "$fixture/output" >/dev/null
grep -F 'Namespace readiness-test is not found.' "$fixture/output" >/dev/null
run_case denied 1 1
grep -F 'PermissionDenied: search attributes forbidden' "$fixture/output" >/dev/null
run_case wrong_namespace 1 1
grep -F 'Namespace another-test is not found.' "$fixture/output" >/dev/null
printf 'Temporal namespace readiness tests: ok\n'

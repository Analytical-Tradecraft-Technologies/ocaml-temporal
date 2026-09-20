#!/bin/sh
set -eu

# Reproduce Windows checkout line endings without Windows, while retaining
# failures for genuine source/document membership changes. The small fixture
# uses the supported source grammar independently of the repository scenarios.
root=${1:-.}
script="$root/scripts/check-live-acceptance-inventory.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
fixture="$scratch/fixture"
mkdir -p "$fixture/.github/workflows" "$fixture/docs/reference" \
  "$fixture/test/integration/temporal/driver" \
  "$fixture/test/integration/temporal/common"

cat > "$fixture/.github/workflows/build.yml" <<'WORKFLOW'
jobs:
  temporal-integration:
    steps:
      - run: make test-temporal-first
      - run: make test-temporal-second
  unrelated:
    steps:
      - run: make test-temporal-not-live
WORKFLOW
cat > "$fixture/Makefile" <<'MAKEFILE'
test-temporal-live-ci:
	$(MAKE) test-temporal-live-ci-contract
	$(MAKE) test-temporal-first
	$(MAKE) test-temporal-second
other-target:
	$(MAKE) test-temporal-not-live
MAKEFILE
cat > "$fixture/test/integration/temporal/driver/smoke_driver.ml" <<'DRIVER'
let starts = [
  start ~workflow:Definitions.first;
  start ~workflow:Definitions.second;
  start ~workflow:Definitions.first;
]
DRIVER
cat > "$fixture/test/integration/temporal/common/smoke_definitions.ml" <<'DEFINITIONS'
let first =
  Temporal.Workflow.define ~name:"smoke.first"
let second =
  Temporal.Workflow.remote ~name:"smoke.second"
let unused =
  Temporal.Workflow.define ~name:"smoke.unused"
DEFINITIONS

inventory="$fixture/docs/reference/live-acceptance-inventory.md"
workflow="$fixture/.github/workflows/build.yml"
driver="$fixture/test/integration/temporal/driver/smoke_driver.ml"
sh "$script" "$fixture" --write
cp "$inventory" "$scratch/expected-lf.md"

# Keep errors attributable to the intended assertion, not any incidental shell
# or missing-file failure. The checker must reject the requested mutation.
assert_rejected() {
  expected=$1
  if sh "$script" "$fixture" --check > "$scratch/rejection.log" 2>&1; then
    echo "live inventory contract accepted a source/document mutation" >&2
    exit 1
  fi
  if ! grep -F "$expected" "$scratch/rejection.log" >/dev/null; then
    cat "$scratch/rejection.log" >&2
    exit 1
  fi
}

for endings in lf crlf; do
  if [ "$endings" = crlf ]; then
    for path in .github/workflows/build.yml Makefile \
      test/integration/temporal/driver/smoke_driver.ml \
      test/integration/temporal/common/smoke_definitions.ml \
      docs/reference/live-acceptance-inventory.md; do
      awk '{ sub(/\r$/, ""); printf "%s\r\n", $0 }' "$fixture/$path" > "$scratch/crlf"
      mv "$scratch/crlf" "$fixture/$path"
    done
  fi
  cp "$inventory" "$scratch/checked-out.md"
  sh "$script" "$fixture" --check
  cmp "$inventory" "$scratch/checked-out.md"

  # Both workflow layouts must give the same membership on either platform.
  cp "$workflow" "$scratch/direct-workflow"
  sed '/make test-temporal-second/d; s/make test-temporal-first/make test-temporal-live-ci/' \
    "$scratch/direct-workflow" > "$workflow"
  sh "$script" "$fixture" --check
  cp "$scratch/direct-workflow" "$workflow"

  sed 's/test-temporal-second/test-temporal-added/' "$scratch/direct-workflow" > "$workflow"
  assert_rejected 'live inventory drift'
  cp "$scratch/direct-workflow" "$workflow"

  cp "$driver" "$scratch/original-driver"
  sed '/Definitions.second/d' "$scratch/original-driver" > "$driver"
  assert_rejected 'live inventory drift'
  sed 's/Definitions.second/Definitions.missing/' "$scratch/original-driver" > "$driver"
  assert_rejected 'cannot resolve baseline workflow definition: missing'
  cp "$scratch/original-driver" "$driver"

  # Normalization must not conceal changes within a documented workflow name.
  sed 's/smoke.second/smoke.changed/' "$scratch/checked-out.md" > "$inventory"
  assert_rejected 'live inventory drift'
  cp "$scratch/checked-out.md" "$inventory"

  # Regenerating either checkout format writes the same canonical LF content.
  sh "$script" "$fixture" --write
  cmp "$inventory" "$scratch/expected-lf.md"
  sh "$script" "$fixture" --check
done

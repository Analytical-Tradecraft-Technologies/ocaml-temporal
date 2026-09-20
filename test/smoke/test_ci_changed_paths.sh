#!/usr/bin/env bash
# Run the workflow's classifier against real Git histories without any builds.
set -euo pipefail
source_root=$(cd "${1:-.}" && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
awk '
  /^        run: \|$/ { copying = 1; next }
  copying && /^  [a-zA-Z0-9_-]+:/ { exit }
  copying { sub(/^          /, ""); print }
' "$source_root/.github/workflows/build-pr.yml" > "$fixture/classify.sh"
mkdir "$fixture/repo"
cd "$fixture/repo"
git init -q
git config commit.gpgsign false
git config core.autocrlf false
git config user.name 'CI fixture'
git config user.email 'ci-fixture@example.invalid'
printf 'base\n' > README.md
git add .
git commit -qm base
base=$(git rev-parse HEAD)

# Run the extracted step and require the expected single output.
check() {
  local expected=$1
  export BASE_SHA=$2 HEAD_SHA=$3
  export RUNNER_TEMP=$fixture GITHUB_OUTPUT=$fixture/output
  : > "$GITHUB_OUTPUT"
  bash "$fixture/classify.sh"
  test "$(cat "$GITHUB_OUTPUT")" = "code=$expected"
}

# Isolate each path to prove both exclusions and the fail-closed default.
for path in README.md docs/guide.md LICENSE NOTICE.md \
  lib/public/workflow.ml test/integration/temporal/driver/main.ml \
  docs/schemas/protocol.json docs/schemas/fixture.md \
  .github/workflows/build-pr.yml .github/actions/example/README.md \
  Makefile temporal-sdk.opam.locked rust/Cargo.lock new-build-input \
  'source with spaces.ml' $'source\nnewline.ml'; do
  git checkout -q --detach "$base"
  mkdir -p "$(dirname "$path")"
  printf 'changed\n' > "$path"
  git add .
  git commit -qm path
  case "$path" in
    README.md|docs/guide.md|LICENSE|NOTICE.md) expected=false ;;
    *) expected=true ;;
  esac
  check "$expected" "$base" "$(git rev-parse HEAD)"
done

# A code change on the base branch after the PR fork must not count as a PR edit.
git checkout -q --detach "$base"
printf 'docs\n' > README.md
git commit -qam docs
docs_head=$(git rev-parse HEAD)
git checkout -q --detach "$base"
printf 'code\n' > source.ml
git add .
git commit -qm code
code_head=$(git rev-parse HEAD)
check false "$code_head" "$docs_head"

# A merge-group comparison covers all changes relative to its supplied base.
git merge -q --no-edit "$docs_head"
check true "$base" "$(git rev-parse HEAD)"
check false "$code_head" "$(git rev-parse HEAD)"

# Disabling rename detection exposes the removed code path even when its new
# name is documentation. Deletion alone must also keep the code gate enabled.
git mv source.ml archived.md
git commit -qm rename
check true "$code_head" "$(git rev-parse HEAD)"
git checkout -q --detach "$code_head"
git rm -q source.ml
git commit -qm deletion
check true "$code_head" "$(git rev-parse HEAD)"
check false "$base" "$base"
if check false invalid-ref "$base" 2>/dev/null; then
  echo 'invalid comparisons must fail' >&2
  exit 1
fi
printf 'CI changed-path histories: passed\n'

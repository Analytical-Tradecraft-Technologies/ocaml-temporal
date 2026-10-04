#!/usr/bin/env bash
# Run the workflow's required-check gate against real Git histories without builds.
set -euo pipefail
source_root=$(cd "${1:-.}" && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
awk '
  /^        run: \|$/ { copying = 1; next }
  copying && (/^  [a-zA-Z0-9_-]+:/ || /^      - name:/) { exit }
  copying { sub(/^          /, ""); print }
' "$source_root/.github/workflows/build-pr.yml" > "$fixture/gate.sh"
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

# Run the extracted step and require the enabled output.
check() {
  export BASE_SHA=$1 HEAD_SHA=$2 GITHUB_OUTPUT=$fixture/output
  : > "$GITHUB_OUTPUT"
  bash "$fixture/gate.sh"
  test "$(cat "$GITHUB_OUTPUT")" = 'code=true'
}

# Every changed path, including documentation, must enable concrete required
# matrix checks. Exercise unusual filenames and former exclusions as well.
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
  check "$base" "$(git rev-parse HEAD)"
done

# The PR comparison accepts a docs-only change even when master also changed.
git checkout -q --detach "$base"
printf 'docs\n' > README.md
git commit -qam docs
docs_head=$(git rev-parse HEAD)
git checkout -q --detach "$base"
printf 'code\n' > source.ml
git add .
git commit -qm code
code_head=$(git rev-parse HEAD)
check "$code_head" "$docs_head"

# A merge-group comparison covers all changes relative to its supplied base.
git merge -q --no-edit "$docs_head"
check "$base" "$(git rev-parse HEAD)"
check "$code_head" "$(git rev-parse HEAD)"

# Renames, deletions, and even an empty comparison still run required checks.
git mv source.ml archived.md
git commit -qm rename
check "$code_head" "$(git rev-parse HEAD)"
git checkout -q --detach "$code_head"
git rm -q source.ml
git commit -qm deletion
check "$code_head" "$(git rev-parse HEAD)"
check "$base" "$base"
# Master and release calls have no PR comparison and must run the same graph.
check '' ''
export BASE_SHA=invalid-ref HEAD_SHA=$base
if bash "$fixture/gate.sh" 2>/dev/null; then
  echo 'invalid comparisons must fail' >&2
  exit 1
fi
printf 'CI changed-path histories: passed\n'

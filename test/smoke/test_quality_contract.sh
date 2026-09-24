#!/bin/sh
set -eu

# Dune intentionally ignores hidden source directories, so this small native
# contract test covers the GitHub workflow that the OCaml repository test
# cannot safely read from its sandbox.
source_root=${1:-.}
master_workflow=$source_root/.github/workflows/build.yml
pr_workflow=$source_root/.github/workflows/build-pr.yml
release_workflow=$source_root/.github/workflows/release-preflight.yml
makefile=$source_root/Makefile

# GitHub's Windows checkout can materialize tracked text with CRLF endings.
# The contract intentionally makes exact-line assertions, so normalize only
# the input representation before checking the workflow's semantic content.
master_workflow_text=$(tr -d '\015' < "$master_workflow")
pr_workflow_text=$(tr -d '\015' < "$pr_workflow")
release_workflow_text=$(tr -d '\015' < "$release_workflow")
makefile_text=$(tr -d '\015' < "$makefile")

# GitHub-owned actions are admitted separately by the effective Actions
# policy and may use readable major-version references. Every other remote
# action crosses a third-party supply-chain boundary, so both build workflows
# must select an immutable full commit. Local actions are repository source and
# therefore do not have an @ reference to validate.
#
# This is deliberately a strict source grammar rather than a partial YAML
# parser. Action entries must use an unquoted block mapping in the form
# `uses: owner/repository@ref`. Alternative YAML key spellings and complex
# mapping keys are rejected, preventing a valid YAML representation from
# becoming invisible to the reference check.
assert_third_party_actions_pinned() {
  workflow_name=$1
  workflow_text=$2
  third_party_refs=$(printf '%s\n' "$workflow_text" | awk \
    -v workflow_name="$workflow_name" '
    BEGIN {
      block_scalar_indent = -1
      invalid = 0
      single_quote = sprintf("%c", 39)
    }
    function reject(reason, source_line) {
      printf "%s: %s: %s\n", workflow_name, reason, source_line > "/dev/stderr"
      invalid = 1
    }
    function leading_spaces(source_line, without_indent) {
      without_indent = source_line
      sub(/^ */, "", without_indent)
      return length(source_line) - length(without_indent)
    }
    {
      line = $0
      sub(/\r$/, "", line)
      if (line ~ /\t/) {
        reject("tabs are unsupported in workflow YAML", line)
        next
      }

      indent = leading_spaces(line)
      if (block_scalar_indent >= 0) {
        if (line ~ /^ *$/) next
        if (indent > block_scalar_indent) next
        block_scalar_indent = -1
      }

      text = line
      sub(/^ */, "", text)
      if (text == "" || text ~ /^#/) next

      # An action reference is a scalar token, never an opaque block body.
      # Handle this key before the generic scalar tracker so canonical and
      # compact-step `uses: |` / `uses: >` entries cannot hide a mutable
      # reference on the following line.
      if (text ~ /^(-[[:space:]]+)?uses:[[:space:]]*[|>]/) {
        reject("action reference must be one unquoted token", line)
        next
      }

      # Shell bodies are opaque scalar content, not workflow mappings. Track
      # them by indentation so words such as `uses:` inside scripts cannot be
      # mistaken for action declarations.
      if (text ~ /^(- +)?[A-Za-z_][A-Za-z0-9_-]*: *[|>][-+]? *(#.*)?$/) {
        block_scalar_indent = indent
        next
      }

      # Quoted and explicit YAML mapping keys can encode `uses` through escape
      # sequences, so reject these key styles rather than pretending a source
      # regex can safely normalize them. Quoted scalar list values remain
      # valid because they do not end in a mapping-key colon.
      if (text ~ /^(- +)?"[^"]*" *:/ ||
          text ~ ("^(- +)?" single_quote "[^" single_quote "]*" single_quote " *:") ||
          text ~ /^(- +)?\? +/ ||
          text ~ /[{,] *"[^"]*" *:/ ||
          text ~ ("[{,] *" single_quote "[^" single_quote "]*" single_quote " *:")) {
        reject("quoted, explicit, and flow-style mapping keys are unsupported", line)
        next
      }

      if (text ~ /^uses: +/) {
        ref = text
        sub(/^uses: +/, "", ref)
        sub(/ +#.*/, "", ref)
        if (ref == "" || ref ~ /[[:space:]]/) {
          reject("action reference must be one unquoted token", line)
          next
        }
        if (ref !~ /^actions\// && ref !~ /^\.\//) print ref
        next
      }

      # Catch compact sequence entries, whitespace before the colon, and flow
      # mappings. Any remaining literal `uses` key is an unsupported spelling,
      # never an entry to silently ignore.
      if (text ~ /(^|[[:space:]{,&*])uses[[:space:]]*:/) {
        reject("action entries must use canonical unquoted `uses:` syntax", line)
      }
    }
    END {
      if (invalid) exit 1
    }
  ') || return 1
  if [ -z "$third_party_refs" ]; then
    return 0
  fi
  unpinned_refs=$(printf '%s\n' "$third_party_refs" |
    grep -Ev '@[0-9a-f]{40}$' || true)
  if [ -n "$unpinned_refs" ]; then
    printf '%s: third-party actions must use full commit SHAs:\n%s\n' \
      "$workflow_name" "$unpinned_refs" >&2
    return 1
  fi
}

assert_third_party_actions_pinned build.yml "$master_workflow_text"
assert_third_party_actions_pinned build-pr.yml "$pr_workflow_text"

# Exercise source spellings that YAML would normalize to the same `uses`
# mapping key. The workflow policy must either inspect them or reject them;
# silently ignoring a valid spelling would make the generic pin check porous.
assert_action_fixture_rejected() {
  fixture_name=$1
  fixture_text=$2
  if assert_third_party_actions_pinned "$fixture_name" "$fixture_text" \
    >/dev/null 2>&1; then
    echo "$fixture_name: unsafe action fixture was accepted" >&2
    return 1
  fi
}

assert_action_fixture_rejected compact-step \
  '    - uses: evil/example@v1'
assert_action_fixture_rejected whitespace-before-colon \
  '        uses : evil/example@v1'
assert_action_fixture_rejected double-quoted-key \
  '        "uses": evil/example@v1'
assert_action_fixture_rejected single-quoted-key \
  "        'uses': evil/example@v1"
assert_action_fixture_rejected escaped-quoted-key \
  '        "\u0075ses": evil/example@v1'
assert_action_fixture_rejected explicit-key \
  '? uses
: evil/example@v1'
assert_action_fixture_rejected flow-mapping \
  '    - {uses: evil/example@v1}'
assert_action_fixture_rejected anchored-key \
  '        &action uses: evil/example@v1'
assert_action_fixture_rejected uppercase-sha \
  '        uses: evil/example@AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
assert_action_fixture_rejected short-sha \
  '        uses: evil/example@0123456789abcdef'
assert_action_fixture_rejected mutable-tag \
  '        uses: evil/example@v1'
assert_action_fixture_rejected misleading-comment \
  '        uses: evil/example@v1 # @0123456789abcdef0123456789abcdef01234567'
assert_action_fixture_rejected literal-block-action \
  '        uses: |-
          evil/example@v1'
assert_action_fixture_rejected folded-block-action \
  '        uses: >-
          evil/example@v1'
assert_action_fixture_rejected compact-literal-block-action \
  '      - uses: |-
          evil/example@v1'
assert_action_fixture_rejected compact-folded-block-action \
  '      - uses: >-
          evil/example@v1'

assert_third_party_actions_pinned github-owned-fixture \
  '        uses: actions/checkout@v7'
assert_third_party_actions_pinned local-fixture \
  '        uses: ./.github/actions/example'
assert_third_party_actions_pinned pinned-third-party-fixture \
  '        uses: example/action@0123456789abcdef0123456789abcdef01234567'
assert_third_party_actions_pinned comment-fixture \
  '        # uses: evil/example@v1'
assert_third_party_actions_pinned block-scalar-fixture \
  '        run: |
          uses: evil/example@v1'

# Release preflight runs from a clean Actions checkout, so it is the right
# place to execute the stale-owner rejection fixture. Keep the fixture out of
# ordinary dirty-worktree quality checks, but assert both the Make target and
# workflow wiring so either half cannot silently turn into a no-op.
release_preflight_target=$(printf '%s\n' "$makefile_text" |
  sed -n '/^release-preflight:/,/^release-tag-check:/p' |
  sed 's/^[[:space:]]*//')
printf '%s\n' "$release_preflight_target" | grep -Fqx 'release-preflight:'
printf '%s\n' "$release_preflight_target" |
  grep -Fqx 'sh scripts/check-release-preflight.sh .'
printf '%s\n' "$release_preflight_target" |
  grep -Fqx 'sh test/smoke/test_release_preflight_contract.sh .'
printf '%s\n' "$release_workflow_text" |
  grep -Fqx '        run: make release-preflight'

# One shared graph owns scans, OCaml matrices and the expensive live stack.
for workflow in "$pr_workflow" "$release_workflow"; do
  grep -Fqx '  pull_request:' "$workflow"
  grep -Fqx '  merge_group:' "$workflow"
  if grep -Eq '^  push:|^    paths(-ignore)?:' "$workflow"; then exit 1; fi
done
for workflow in "$master_workflow" "$source_root/.github/workflows/release.yml"; do
  grep -Fqx '    uses: ./.github/workflows/build-pr.yml' "$workflow"
done
grep -Fqx '      tier: master' "$master_workflow"
grep -Fqx '  push:' "$master_workflow"
grep -Fqx '  workflow_dispatch:' "$master_workflow"
grep -Fqx '      tier: release' "$source_root/.github/workflows/release.yml"
for workflow in "$source_root"/.github/workflows/*.yml "$source_root"/.github/actions/*/action.yml; do
  assert_third_party_actions_pinned "$workflow" "$(cat "$workflow")"
done
# Source-only histories preserve fail-closed docs skipping for PR/merge queue.
bash "$source_root/test/smoke/test_ci_changed_paths.sh" "$source_root"
pr_quality=$(printf '%s\n' "$pr_workflow_text" | sed -n '/^  quality:/,/^  license-audit:/p')
printf '%s\n' "$pr_quality" | grep -Fqx "    if: needs.changes.outputs.code == 'true'"
printf '%s\n' "$pr_quality" | grep -Fqx '    name: Quality and security scans'
printf '%s\n' "$pr_quality" | grep -Fq 'cargo-deny@0.20.2,cargo-machete@0.9.2,typos@1.48.0'
printf '%s\n' "$pr_quality" | grep -Fqx '        run: make quality'
grep -Fqx '    name: Dependency license audit' "$pr_workflow"
grep -Fqx '        run: make license-check OCAML_VERSION=5.2' "$pr_workflow"
grep -Fqx '      matrix: ${{ fromJSON(needs.changes.outputs.linux) }}' "$pr_workflow"
# Native consumer failures stay visible even when their producer fails.
for platform in macos windows; do
  section=$(sed -n "/^  native-$platform:/,/^  [a-z][a-z-]*:/p" "$pr_workflow")
  printf '%s\n' "$section" | grep -Fq 'if: ${{ !cancelled()'
  printf '%s\n' "$section" | grep -Fqx "    needs: [changes, rust-$platform]"
  printf '%s\n' "$section" | grep -Fqx '        run: make native-verify'
done
pr_smoke=$(sed -n '/^  temporal-integration:/,/^  verify:/p' "$pr_workflow")
printf '%s\n' "$pr_smoke" | grep -Fqx '    needs: [changes, verify]'
printf '%s\n' "$pr_smoke" | grep -Fqx '    name: Temporal/PostgreSQL integration smoke (OCaml 5.5)'
printf '%s\n' "$pr_smoke" | grep -Fqx '      OCAML_VERSION: "5.5.1"'
printf '%s\n' "$pr_smoke" | grep -Fqx '      TEMPORAL_PREBUILT_SMOKE: "1"'
printf '%s\n' "$pr_smoke" | grep -Fqx '        run: make test-temporal-live-ci'
printf '%s\n' "$pr_smoke" | grep -Fqx '          retention-days: 7'

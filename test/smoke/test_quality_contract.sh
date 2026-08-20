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
      # Handle this key before the generic scalar tracker so `uses: |` and
      # `uses: >` cannot hide a mutable reference on the following line.
      if (text ~ /^uses:[[:space:]]*[|>]/) {
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

setup_ocaml_sha=15d660006c1d3110d77c34b7faa3bddefe8b82f0
master_setup_ocaml_count=$(printf '%s\n' "$master_workflow_text" |
  grep -Fc "ocaml/setup-ocaml@$setup_ocaml_sha # v3.7.0")
pr_setup_ocaml_count=$(printf '%s\n' "$pr_workflow_text" |
  grep -Fc "ocaml/setup-ocaml@$setup_ocaml_sha # v3.7.0")
test "$master_setup_ocaml_count" -eq 1
test "$pr_setup_ocaml_count" -eq 2

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

# The changed-path detector is a safety boundary: every non-document path
# must opt into the code and live-smoke jobs. Keep all three workflow outputs
# and the fail-closed defaults explicit so a refactor cannot silently skip
# ordinary source changes before they reach the representative matrix.
printf '%s\n' "$pr_workflow_text" |
  grep -Fqx "      code: \${{ steps.changed-paths.outputs.code }}"
printf '%s\n' "$pr_workflow_text" |
  grep -Fqx "      smoke: \${{ steps.changed-paths.outputs.smoke }}"
printf '%s\n' "$pr_workflow_text" |
  grep -Fqx "      native_windows: \${{ steps.changed-paths.outputs.native_windows }}"
printf '%s\n' "$pr_workflow_text" | grep -Fqx '          code=false'
printf '%s\n' "$pr_workflow_text" | grep -Fqx '          smoke=false'
printf '%s\n' "$pr_workflow_text" | grep -Fqx '          native_windows=false'
printf '%s\n' "$pr_workflow_text" | grep -Fqx '              *)'
printf '%s\n' "$pr_workflow_text" | grep -Fqx '                code=true'
printf '%s\n' "$pr_workflow_text" | grep -Fqx '                smoke=true'

# Anchor the job declaration at the workflow's two-space indentation so a
# future matrix step named "quality" cannot satisfy this contract by accident.
printf '%s\n' "$master_workflow_text" | grep -Fq '  quality:'
printf '%s\n' "$master_workflow_text" |
  grep -Fq 'name: Quality and security scans'
printf '%s\n' "$master_workflow_text" |
  grep -Fq 'taiki-e/install-action@2ca9b94c269419b7b0c711c09d0b21c4e1d51145'
printf '%s\n' "$master_workflow_text" |
  grep -Fq 'cargo-deny@0.20.2,cargo-machete@0.9.2,typos@1.48.0'
printf '%s\n' "$master_workflow_text" | grep -Fq 'run: make quality'

# PR quality uses the same pinned tools, but is conditional so Markdown-only
# changes can use the inexpensive independent license job.
pr_quality=$(printf '%s\n' "$pr_workflow_text" |
  sed -n '/^  quality:/,/^  license-audit:/p')
printf '%s\n' "$pr_quality" | grep -Fqx "    if: needs.changes.outputs.code == 'true'"
printf '%s\n' "$pr_quality" | grep -Fqx '    name: Quality and security scans'
printf '%s\n' "$pr_quality" |
  grep -Fq 'taiki-e/install-action@2ca9b94c269419b7b0c711c09d0b21c4e1d51145'
printf '%s\n' "$pr_quality" |
  grep -Fq 'cargo-deny@0.20.2,cargo-machete@0.9.2,typos@1.48.0'
printf '%s\n' "$pr_quality" | grep -Fqx '        run: make quality'

# Master and scheduled builds are the exhaustive compatibility gate. Scope
# every assertion to verify so a comment or another job cannot satisfy it.
master_verify=$(printf '%s\n' "$master_workflow_text" |
  sed -n '/^  verify:/,/^    steps:/p')
for version in 5.2 5.3 5.4 5.5; do
  printf '%s\n' "$master_verify" | grep -Fqx "          - \"$version\""
done
master_version_count=$(printf '%s\n' "$master_verify" | grep -Fc '          - "5.')
test "$master_version_count" -eq 4
printf '%s\n' "$master_verify" | grep -Fqx '          - ubuntu-24.04'
printf '%s\n' "$master_verify" | grep -Fqx '          - ubuntu-24.04-arm'
master_native=$(printf '%s\n' "$master_workflow_text" |
  sed -n '/^  native:/,$p')
printf '%s\n' "$master_native" | grep -Fqx '          - label: Windows x64'
printf '%s\n' "$master_native" | grep -Fqx '          - label: macOS ARM64'

# The PR matrix is deliberately an explicit three-lane include list rather
# than a Cartesian product: oldest/current amd64 and current ARM64. This keeps
# the compatibility floor, current release, and ARM build covered before
# merge, while avoiding five redundant Linux runner allocations.
pr_verify=$(printf '%s\n' "$pr_workflow_text" |
  sed -n '/^  verify:/,/^    steps:/p')
# Check the adjacent OCaml/runner fields rather than merely their presence.
# That preserves the intended floor/current pairing if the list is reordered
# or a future edit accidentally assigns the compatibility floor to ARM64.
printf '%s\n' "$pr_verify" | awk '
  $0 == "          - ocaml: \"5.2\"" {
    if ((getline runner) > 0 && runner == "            runner: ubuntu-24.04") {
      floor_amd64 = 1
    }
  }
  $0 == "          - ocaml: \"5.5\"" {
    if ((getline runner) > 0) {
      if (runner == "            runner: ubuntu-24.04") current_amd64 = 1
      if (runner == "            runner: ubuntu-24.04-arm") current_arm64 = 1
    }
  }
  END { exit !(floor_amd64 && current_amd64 && current_arm64) }
'
lane_count=$(printf '%s\n' "$pr_verify" | grep -Fc '          - ocaml:')
test "$lane_count" -eq 3

# The faster macOS native job runs for each code PR. Windows is intentionally
# conditional on bridge/build/workflow inputs, so preserve both the
# changed-path output and its separate job instead of accidentally turning it
# into a permanently skipped or always-expensive matrix cell.
printf '%s\n' "$pr_workflow_text" | grep -Fq '      native_windows:'
printf '%s\n' "$pr_workflow_text" | grep -Fq '          native_windows=false'
printf '%s\n' "$pr_workflow_text" | grep -Fq '                native_windows=true'
printf '%s\n' "$pr_workflow_text" | grep -Fq '  native-macos:'
printf '%s\n' "$pr_workflow_text" | grep -Fq '  native-windows:'
printf '%s\n' "$pr_workflow_text" |
  grep -Fq "if: needs.changes.outputs.native_windows == 'true'"

# The always-on macOS lane proves the representative desktop native link. The
# Windows lane is deliberately conditional, but when selected it must retain
# the matching compiler, architecture, and native verification command.
pr_native_macos=$(printf '%s\n' "$pr_workflow_text" |
  sed -n '/^  native-macos:/,/^  native-windows:/p')
printf '%s\n' "$pr_native_macos" | grep -Fqx "    if: needs.changes.outputs.code == 'true'"
printf '%s\n' "$pr_native_macos" | grep -Fqx '    name: OCaml 5.5 / macOS ARM64'
printf '%s\n' "$pr_native_macos" | grep -Fqx '    runs-on: macos-15'
printf '%s\n' "$pr_native_macos" | grep -Fqx '      NATIVE_ARCH: arm64'
printf '%s\n' "$pr_native_macos" | grep -Fqx '        run: make native-verify'
pr_native_windows=$(printf '%s\n' "$pr_workflow_text" |
  sed -n '/^  native-windows:/,$p')
printf '%s\n' "$pr_native_windows" |
  grep -Fqx "    if: needs.changes.outputs.native_windows == 'true'"
printf '%s\n' "$pr_native_windows" | grep -Fqx '    name: OCaml 5.5 / Windows x64'
printf '%s\n' "$pr_native_windows" | grep -Fqx '    runs-on: windows-latest'
printf '%s\n' "$pr_native_windows" | grep -Fqx '      NATIVE_ARCH: amd64'
printf '%s\n' "$pr_native_windows" | grep -Fqx '        run: make native-verify'

# License audit is intentionally unconditional. The live smoke is conditional
# on source or fixture changes and runs both acceptance scenarios at OCaml 5.5.
pr_license=$(printf '%s\n' "$pr_workflow_text" |
  sed -n '/^  license-audit:/,/^  temporal-integration:/p')
printf '%s\n' "$pr_license" | grep -Fqx '    name: Dependency license audit'
if printf '%s\n' "$pr_license" | grep -Fq '    if:'; then
  exit 1
fi
printf '%s\n' "$pr_license" |
  grep -Fqx '        run: make license-check OCAML_VERSION=5.2'
pr_smoke=$(printf '%s\n' "$pr_workflow_text" |
  sed -n '/^  temporal-integration:/,/^  verify:/p')
printf '%s\n' "$pr_smoke" | grep -Fqx "    if: needs.changes.outputs.smoke == 'true'"
printf '%s\n' "$pr_smoke" |
  grep -Fqx '    name: Temporal/PostgreSQL integration smoke (OCaml 5.5)'
printf '%s\n' "$pr_smoke" | grep -Fqx '    timeout-minutes: 45'
printf '%s\n' "$pr_smoke" | grep -Fqx '      OCAML_VERSION: "5.5"'
printf '%s\n' "$pr_smoke" | grep -Fqx '          make test-temporal-integration'
printf '%s\n' "$pr_smoke" | grep -Fqx '          make test-temporal-worker-restart'
printf '%s\n' "$pr_smoke" | grep -Fqx '          make test-temporal-worker-crash-recovery'
printf '%s\n' "$pr_smoke" | grep -Fqx '          make test-temporal-worker-cache-eviction'
printf '%s\n' "$pr_smoke" | grep -Fqx '          make test-temporal-workflow-patching'
printf '%s\n' "$pr_smoke" | grep -Fqx '          make test-temporal-parent-child-restart'

# The JSON schemas are protocol fixtures rather than prose. Preserve their
# code classification while keeping ordinary Markdown-only changes inexpensive.
# The cases are ordered, so assert the schema exception appears before the
# broad docs/ catch-all rather than merely checking that both snippets exist.
printf '%s\n' "$pr_workflow_text" | grep -Fq '              docs/schemas/*)'
printf '%s\n' "$pr_workflow_text" |
  grep -Fq '              *.md|*.markdown|LICENSE*|NOTICE*|docs/*)'
schema_case_line=$(printf '%s\n' "$pr_workflow_text" |
  awk '$0 == "              docs/schemas/*)" { print NR; exit }')
docs_case_line=$(printf '%s\n' "$pr_workflow_text" |
  awk '$0 == "              *.md|*.markdown|LICENSE*|NOTICE*|docs/*)" { print NR; exit }')
test -n "$schema_case_line"
test -n "$docs_case_line"
test "$schema_case_line" -lt "$docs_case_line"

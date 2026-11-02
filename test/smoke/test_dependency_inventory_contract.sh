#!/bin/sh
set -eu

# Contract for scripts/check-dependency-inventory.sh (#783). The checker must
# accept the repository as committed (including a CRLF checkout of the
# inventory) and must reject an added or removed dependency in every source it
# compares: each drift case below copies the inputs into a scratch tree, makes
# one realistic edit without touching docs/dependencies.md (or edits only the
# document), and requires a failure that names the affected table. Version,
# ref, tag, and digest bumps are recorded only in the pinning files, so the
# version cases require the checker to accept the edit unchanged.
root=${1:-.}
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

inputs='docs/dependencies.md
temporal-sdk.opam
temporal-sdk.opam.locked
scripts/opam-lock-overrides.txt
scripts/docs-tools.locked
scripts/check-dependency-inventory.sh
test/integration/sdk_interop/python/requirements.lock
Dockerfile.dev
Dockerfile.rust-ci
test/integration/temporal/compose.yaml
rust/rust-toolchain.toml
rust/Cargo.toml
rust/Cargo.lock
rust/core-bridge/Cargo.toml
Makefile'

# Build a fresh fixture tree containing exactly the checker's inputs.
fresh_fixture() {
  rm -rf "$scratch/fixture"
  printf '%s\n' "$inputs" | while IFS= read -r path; do
    mkdir -p "$scratch/fixture/$(dirname "$path")"
    cp "$root/$path" "$scratch/fixture/$path"
  done
  # One digest-pinned ocaml/opam base image per compiler series (#1027).
  for pin in "$root"/docker/ocaml-*/Dockerfile; do
    series_dir=$(basename "$(dirname "$pin")")
    mkdir -p "$scratch/fixture/docker/$series_dir"
    cp "$pin" "$scratch/fixture/docker/$series_dir/"
  done
  mkdir -p "$scratch/fixture/.github/workflows" "$scratch/fixture/.github/actions"
  cp "$root"/.github/workflows/*.yml "$scratch/fixture/.github/workflows/"
  for action in "$root"/.github/actions/*/; do
    name=$(basename "$action")
    mkdir -p "$scratch/fixture/.github/actions/$name"
    cp "$action/action.yml" "$scratch/fixture/.github/actions/$name/"
  done
}

# Run the checker against the fixture, capturing its diagnostics.
run_checker() {
  sh "$scratch/fixture/scripts/check-dependency-inventory.sh" "$scratch/fixture" \
    > "$scratch/out" 2>&1
}

# Apply one sed program to a fixture file and require that the edit changed
# it, so a stale fixture pattern cannot turn a case into a vacuous pass.
edit() {
  file="$scratch/fixture/$1"
  sed "$2" "$file" > "$file.new"
  if cmp -s "$file" "$file.new"; then
    echo "test fixture edit did not change $1: $2" >&2
    exit 1
  fi
  mv "$file.new" "$file"
}

# Insert one line before the first line of a fixture file that starts with a
# literal prefix. awk is used because sed newline escapes are not portable.
insert_before() {
  file="$scratch/fixture/$1"
  awk -v prefix="$2" -v text="$3" '
    !done && index($0, prefix) == 1 { print text; done = 1 }
    { print }
    END { if (!done) exit 1 }
  ' "$file" > "$file.new" || {
    echo "test fixture insertion found no '$2' in $1" >&2
    exit 1
  }
  mv "$file.new" "$file"
}

# Append text to a fixture file.
append() {
  printf '%s\n' "$2" >> "$scratch/fixture/$1"
}

# Require the checker to fail and to name the expected table.
expect_drift() {
  label=$1
  table=$2
  if run_checker; then
    echo "dependency inventory checker accepted drift: $label" >&2
    exit 1
  fi
  if ! grep -F -- "$table" "$scratch/out" >/dev/null; then
    echo "dependency inventory drift '$label' did not name '$table':" >&2
    cat "$scratch/out" >&2
    exit 1
  fi
}

# Require the checker to accept a version-only edit without an inventory change.
expect_clean() {
  if ! run_checker; then
    echo "dependency inventory checker rejected a version-only bump: $1" >&2
    cat "$scratch/out" >&2
    exit 1
  fi
}

fresh_fixture
if ! run_checker; then
  echo "dependency inventory checker rejected the committed repository:" >&2
  cat "$scratch/out" >&2
  exit 1
fi

# A Windows checkout may use CRLF; that alone is not drift.
awk '{ printf "%s\r\n", $0 }' "$scratch/fixture/docs/dependencies.md" > "$scratch/crlf"
mv "$scratch/crlf" "$scratch/fixture/docs/dependencies.md"
if ! run_checker; then
  echo "dependency inventory checker rejected a CRLF inventory:" >&2
  cat "$scratch/out" >&2
  exit 1
fi

# Version-only bumps: none of these may require an inventory edit.
fresh_fixture
edit temporal-sdk.opam.locked 's/"logs" {= "[^"]*"}/"logs" {= "9.9.9"}/'
edit temporal-sdk.opam 's/^version: ".*"/version: "9.9.9"/'
edit scripts/docs-tools.locked 's/^odoc .*/odoc 9.9.9/'
edit test/integration/sdk_interop/python/requirements.lock 's/^temporalio==[0-9.]*/temporalio==9.9.9/'
edit test/integration/temporal/compose.yaml 's/postgres:[0-9.]*-bookworm/postgres:99.9-bookworm/'
edit docker/ocaml-5.3/Dockerfile 's/ocaml-5.3@sha256:[0-9a-f]*/ocaml-5.3@sha256:0000000000000000000000000000000000000000000000000000000000000000/'
edit rust/rust-toolchain.toml 's/^channel = ".*"/channel = "9.9.9"/'
edit rust/Cargo.lock '/^name = "tokio"$/{n;s/^version = ".*"/version = "9.9.9"/;}'
edit rust/Cargo.toml 's/^uuid = { version = "[^"]*"/uuid = { version = "9.9.9"/'
edit .github/workflows/build-pr.yml 's/typos@[0-9.]*/typos@9.9.9/'
edit Makefile 's/^QUALITY_TYPOS_VERSION ?= .*/QUALITY_TYPOS_VERSION ?= 9.9.9/'
expect_clean 'OPAM, docs-tool, Python, image, toolchain, Cargo, and quality-tool versions'

fresh_fixture
append rust/Cargo.lock '
[[package]]
name = "inventory-drift-fixture"
version = "0.0.1"'
expect_clean 'new transitive Cargo package'

# Every GitHub Actions pin moves together, as a Dependabot update does.
fresh_fixture
for workflow in "$scratch"/fixture/.github/workflows/*.yml "$scratch"/fixture/.github/actions/*/action.yml; do
  sed 's/\(taiki-e\/install-action@\)[0-9a-f]* # v[0-9.]*/\10000000000000000000000000000000000000000 # v9.9.9/' \
    "$workflow" > "$workflow.new"
  mv "$workflow.new" "$workflow"
done
grep -q 'taiki-e/install-action@0000000000000000000000000000000000000000 # v9.9.9' \
  "$scratch/fixture/.github/workflows/build-pr.yml" || {
  echo "test fixture edit did not move the taiki-e/install-action pin" >&2
  exit 1
}
expect_clean 'action commit pin and release comment'

# Added or removed dependencies: each must name its table.
fresh_fixture
insert_before temporal-sdk.opam.locked '  "yojson" {= ' '  "zarith" {= "1.14"}'
expect_drift 'new locked OPAM package' 'Locked OCaml closure'

fresh_fixture
edit docs/dependencies.md '/^| yojson | /d'
expect_drift 'removed inventory row' 'Locked OCaml closure'

fresh_fixture
append scripts/opam-lock-overrides.txt '5.4 dune 9.9.9'
expect_drift 'new compiler override' 'Per-compiler lock overrides'

fresh_fixture
append scripts/docs-tools.locked 'logs 0.10.0'
expect_drift 'new documentation tool' 'CI-only documentation tooling'

fresh_fixture
insert_before test/integration/sdk_interop/python/requirements.lock 'typing-extensions==' 'grpcio==1.0.0'
expect_drift 'new interoperability package' 'CI-only interoperability tooling'

fresh_fixture
edit test/integration/temporal/compose.yaml 's/postgres:[0-9.]*-bookworm@/mysql:9.9@/'
expect_drift 'replaced Compose image' 'local integration service images'

fresh_fixture
edit rust/Cargo.toml 's/rev = "[0-9a-f]*"/rev = "0000000000000000000000000000000000000000"/g'
expect_drift 'Temporal Core revision' 'Locked Cargo closure'

fresh_fixture
insert_before rust/core-bridge/Cargo.toml 'flate2.workspace = true' 'base64.workspace = true'
expect_drift 'direct crate kind' 'Direct Rust dependencies'

fresh_fixture
insert_before rust/core-bridge/Cargo.toml 'base64.workspace = true' 'anyhow = "1"'
expect_drift 'normal crate declared directly by the bridge' 'Direct Rust dependencies'

fresh_fixture
insert_before rust/core-bridge/Cargo.toml 'flate2.workspace = true' 'pretty_assertions = { version = "1", default-features = false }'
expect_drift 'dev crate declared directly by the bridge' 'Direct Rust dependencies'

fresh_fixture
append rust/core-bridge/Cargo.toml '
[build-dependencies]
cc = "1"'
expect_drift 'unsupported bridge dependency table' 'cannot attribute'

fresh_fixture
edit docs/dependencies.md '/^| ocamlbuild | .* | build only | /d'
expect_drift 'linking-exception package without a policy row' 'Linking-exception scope'

fresh_fixture
edit docs/dependencies.md '/^| tyxml | .* | CI-only documentation tool | /d'
expect_drift 'docs-tool linking-exception row removed' 'Linking-exception scope'

fresh_fixture
edit .github/workflows/build-pr.yml 's/tool: \(.*\)typos@/tool: \1shellcheck@0.1.0,typos@/'
expect_drift 'new quality tool' 'CI-only quality tools'

fresh_fixture
edit Makefile 's/^QUALITY_TYPOS_VERSION ?= /QUALITY_TYPOS_PINNED ?= /'
expect_drift 'Makefile quality tool removed' 'QUALITY_*_VERSION'

fresh_fixture
edit .github/workflows/rust-bridge.yml 's/msys2\/setup-msys2@/msys2\/setup-msys3@/'
expect_drift 'new action' 'GitHub Actions'

fresh_fixture
edit .github/workflows/stress.yml 's/actions\/checkout@[0-9a-f]*/actions\/checkout@0000000000000000000000000000000000000000/'
expect_drift 'action pinned to two refs' 'more than one ref'

echo "dependency inventory contract passed"

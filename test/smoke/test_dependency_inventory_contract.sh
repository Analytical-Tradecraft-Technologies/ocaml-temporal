#!/bin/sh
set -eu

# Contract for scripts/check-dependency-inventory.sh (#783). The checker must
# accept the repository as committed (including a CRLF checkout of the
# inventory) and must reject drift in every source it compares: each case
# below copies the inputs into a scratch tree, makes one realistic edit
# without touching docs/dependencies.md (or edits only the document), and
# requires a failure that names the affected table.
root=${1:-.}
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

inputs='docs/dependencies.md
temporal-sdk.opam
temporal-sdk.opam.locked
scripts/opam-lock-overrides.txt
scripts/docs-tools.locked
scripts/check-dependency-inventory.sh
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

fresh_fixture
edit temporal-sdk.opam.locked 's/"logs" {= "[^"]*"}/"logs" {= "9.9.9"}/'
expect_drift 'locked OPAM version' 'Locked OCaml closure'

fresh_fixture
insert_before temporal-sdk.opam.locked '  "yojson" {= ' '  "zarith" {= "1.14"}'
expect_drift 'new locked OPAM package' 'Locked OCaml closure'

fresh_fixture
edit temporal-sdk.opam 's/^version: ".*"/version: "9.9.9"/'
expect_drift 'project version' 'Locked OCaml closure'

fresh_fixture
edit docs/dependencies.md '/^| yojson | /d'
expect_drift 'removed inventory row' 'Locked OCaml closure'

fresh_fixture
append scripts/opam-lock-overrides.txt '5.4 dune 9.9.9'
expect_drift 'new compiler override' 'Per-compiler lock overrides'

fresh_fixture
edit scripts/docs-tools.locked 's/^odoc .*/odoc 9.9.9/'
expect_drift 'documentation tool version' 'CI-only documentation tooling'

fresh_fixture
edit test/integration/temporal/compose.yaml 's/postgres:[0-9.]*-bookworm/postgres:99.9-bookworm/'
expect_drift 'Compose image tag' 'local integration service images'

fresh_fixture
edit Dockerfile.dev 's/ocaml-5.3@sha256:[0-9a-f]*/ocaml-5.3@sha256:0000000000000000000000000000000000000000000000000000000000000000/'
expect_drift 'builder image digest' 'local integration service images'

fresh_fixture
edit rust/rust-toolchain.toml 's/^channel = ".*"/channel = "9.9.9"/'
expect_drift 'Rust toolchain' 'Rust toolchain'

fresh_fixture
append rust/Cargo.lock '
[[package]]
name = "inventory-drift-fixture"
version = "0.0.1"'
expect_drift 'Cargo package count' 'Locked Cargo closure'

fresh_fixture
edit rust/Cargo.toml 's/rev = "[0-9a-f]*"/rev = "0000000000000000000000000000000000000000"/g'
expect_drift 'Temporal Core revision' 'Locked Cargo closure'

fresh_fixture
edit rust/Cargo.lock '/^name = "tokio"$/{n;s/^version = ".*"/version = "9.9.9"/;}'
expect_drift 'locked direct crate version' 'Direct Rust dependencies'

fresh_fixture
edit rust/Cargo.toml 's/^uuid = { version = "[^"]*"/uuid = { version = "9.9.9"/'
expect_drift 'direct crate requirement' 'Direct Rust dependencies'

fresh_fixture
insert_before rust/core-bridge/Cargo.toml 'flate2.workspace = true' 'base64.workspace = true'
expect_drift 'direct crate kind' 'Direct Rust dependencies'

fresh_fixture
edit .github/workflows/build-pr.yml 's/typos@[0-9.]*/typos@9.9.9/'
expect_drift 'quality tool version' 'CI-only quality tools'

fresh_fixture
edit Makefile 's/^QUALITY_TYPOS_VERSION ?= .*/QUALITY_TYPOS_VERSION ?= 9.9.9/'
expect_drift 'Makefile quality tool version' 'QUALITY_*_VERSION'

fresh_fixture
edit .github/workflows/build-pr.yml 's/\(taiki-e\/install-action@[0-9a-f]*\) # v[0-9.]*/\1 # v9.9.9/'
expect_drift 'action release comment' 'GitHub Actions'

fresh_fixture
edit .github/workflows/rust-bridge.yml 's/msys2\/setup-msys2@[0-9a-f]*/msys2\/setup-msys2@0000000000000000000000000000000000000000/'
expect_drift 'action commit pin' 'GitHub Actions'

echo "dependency inventory contract passed"

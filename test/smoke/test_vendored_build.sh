#!/bin/sh
# Builds and runs a scratch Dune workspace that vendors this SDK below
# [vendor/temporal] (#829). Git submodules, monorepos and [opam monorepo] all
# place the SDK in such a subdirectory, where Dune's workspace root is the
# consumer's project rather than the SDK. The test proves that the Rust bridge
# rule resolves its scripts, Rust sources and fallback Cargo target directory
# relative to the SDK, and that the linked bridge answers a real ABI call.
#
# Usage: test_vendored_build.sh SDK_ROOT
#
# The consumer deliberately has no [dune-workspace] on Unix, so it does not
# inherit the SDK's [disable_dynamically_linked_foreign_archives] setting.
# Windows consumers must set it themselves (see docs/reference/core-bridge.md);
# the test does so there to mirror the documented requirement.
#
# This compiles the full Rust bridge unless CARGO_TARGET_DIR points at a warm
# Cargo cache or TEMPORAL_RUST_BRIDGE_DIR supplies a prebuilt bundle, so it is
# an opt-in Make target rather than part of the default runtest alias.
set -eu

sdk_root=$(cd "$1" && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-vendored.XXXXXX")
# Remove the scratch workspace, including its Dune build and any fallback
# Cargo target directory, on every exit path.
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

vendor_root=$scratch/vendor/temporal
mkdir -p "$vendor_root" "$scratch/app"

# Copy the SDK as a consumer would receive it: tracked and unignored files only,
# never a stale _build, Cargo target directory or .git metadata. Git is used
# only when the SDK is itself the top level of its worktree; a source archive
# without Git, including one unpacked inside another project's worktree (for
# example an ignored vendor/ or duniverse/ directory), falls back to an
# explicit exclusion list.
git_top=$(git -C "$sdk_root" rev-parse --show-toplevel 2>/dev/null || true)
if [ -n "$git_top" ] && [ "$(cd "$git_top" && pwd -P)" = "$(cd "$sdk_root" && pwd -P)" ]; then
  (cd "$sdk_root" && git ls-files -z --cached --others --exclude-standard) |
    (cd "$sdk_root" && xargs -0 tar -cf - --) |
    tar -xf - -C "$vendor_root"
else
  (cd "$sdk_root" && tar -cf - --exclude=./_build --exclude=./.git \
    --exclude=./rust/target --exclude=./rust-target .) |
    tar -xf - -C "$vendor_root"
fi
# Deleted-but-tracked files are listed by Git yet absent; tar reports them and
# the copy above would fail. Assert the bridge inputs that matter are present.
test -f "$vendor_root/scripts/build-rust-bridge.sh"
test -f "$vendor_root/rust/Cargo.toml"

cat >"$scratch/dune-project" <<'EOF'
(lang dune 3.18)
EOF
cat >"$scratch/dune" <<'EOF'
(vendored_dirs vendor)
EOF

case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*)
    cat >"$scratch/dune-workspace" <<'EOF'
(lang dune 3.18)
(context
 (default
  (disable_dynamically_linked_foreign_archives true)))
EOF
    ;;
esac

cat >"$scratch/app/dune" <<'EOF'
(executable
 (name main)
 (libraries temporal-sdk))
EOF
cat >"$scratch/app/main.ml" <<'EOF'
(* Calls through the C stubs into the vendored Rust bridge, so a link that
   silently omitted the bridge archive cannot pass. *)
let () =
  match Temporal.Runtime_info.native_bridge_abi_version () with
  | Ok version -> Printf.printf "vendored bridge ABI %ld\n" version
  | Error _ ->
      prerr_endline "vendored bridge ABI query failed";
      exit 1
EOF

cd "$scratch"
opam exec -- dune build --root . ./app/main.exe
opam exec -- dune exec --root . ./app/main.exe

# Without an explicit CARGO_TARGET_DIR the fallback must stay inside the
# vendored SDK's build directory, never the consumer's build root.
if [ -z "${CARGO_TARGET_DIR:-}" ] && [ -z "${TEMPORAL_RUST_BRIDGE_DIR:-}" ]; then
  test ! -e _build/default/rust-target || {
    echo "Rust fallback target escaped the vendored SDK" >&2
    exit 1
  }
fi

echo "vendored consumer build: ok"

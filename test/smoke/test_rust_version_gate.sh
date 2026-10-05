#!/bin/sh
set -eu

# Verifies the Rust bridge build rejects a compiler older than the workspace
# rust-version before Cargo runs (#827). conf-rust-2024 only proves edition 2024
# support, so this gate is what turns an old distribution toolchain into an
# actionable error. Fake rustc/cargo executables keep the test independent of
# the host toolchain and prove Cargo is never reached on rejection.
workspace_root=$1
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM

fake_bin=$temporary_root/bin
mkdir "$fake_bin"
# A cargo stand-in that records any invocation, so the test can distinguish
# "rejected before Cargo" from "Cargo failed for another reason".
cat >"$fake_bin/cargo" <<'EOF'
#!/bin/sh
echo invoked >"$CARGO_MARKER"
exit 97
EOF
chmod +x "$fake_bin/cargo"

required=$(sed -n 's/^rust-version[[:space:]]*=[[:space:]]*"\([0-9][0-9.]*\)"[[:space:]]*$/\1/p' \
  "$workspace_root/rust/Cargo.toml" | head -n 1)
[ -n "$required" ] || { echo "rust/Cargo.toml has no rust-version" >&2; exit 1; }
required_major=${required%%.*}
required_minor=${required#*.}
required_minor=${required_minor%%.*}

# Writes a fake rustc that reports the banner given as $2 into file $1.
make_rustc() {
  printf '#!/bin/sh\necho "%s"\n' "$2" >"$1"
  chmod +x "$1"
}

# Runs the bridge build with the given fake rustc, capturing stderr and whether
# Cargo was reached. Prints the build's exit status.
run_build() {
  rm -f "$temporary_root/cargo-invoked"
  set +e
  env TEMPORAL_RUST_BRIDGE_DIR= PATH="$fake_bin:$PATH" RUSTC="$1" \
    CARGO_MARKER="$temporary_root/cargo-invoked" \
    CARGO_TARGET_DIR="$temporary_root/target" \
    sh "$workspace_root/scripts/build-rust-bridge.sh" "$workspace_root" \
    "$temporary_root/static.a" "$temporary_root/dynamic" \
    "$temporary_root/flags.sexp" >/dev/null 2>"$temporary_root/stderr"
  status=$?
  set -e
  printf '%s\n' "$status"
}

old_rustc=$fake_bin/rustc-old
make_rustc "$old_rustc" "rustc $required_major.$((required_minor - 1)).9 (0000000 2025-01-01)"
status=$(run_build "$old_rustc")
if [ "$status" -eq 0 ] || [ -e "$temporary_root/cargo-invoked" ]; then
  echo "bridge build did not reject an old rustc before running Cargo" >&2
  exit 1
fi
grep -F "requires Rust $required or newer" "$temporary_root/stderr" >/dev/null || {
  echo "old-rustc rejection does not name the required version:" >&2
  cat "$temporary_root/stderr" >&2
  exit 1
}

# The exact minimum and a newer nightly must both reach Cargo; the fake Cargo
# then fails, which is expected here.
for banner in "rustc $required.0 (0000000 2025-01-01)" \
  "rustc $required_major.$((required_minor + 1)).0-nightly (0000000 2025-01-01)"; do
  new_rustc=$fake_bin/rustc-new
  make_rustc "$new_rustc" "$banner"
  run_build "$new_rustc" >/dev/null
  if [ ! -e "$temporary_root/cargo-invoked" ]; then
    echo "bridge build rejected a compatible compiler ($banner):" >&2
    cat "$temporary_root/stderr" >&2
    exit 1
  fi
done

unparseable_rustc=$fake_bin/rustc-unparsable
make_rustc "$unparseable_rustc" "not a compiler"
status=$(run_build "$unparseable_rustc")
if [ "$status" -eq 0 ] || [ -e "$temporary_root/cargo-invoked" ]; then
  echo "bridge build accepted an unparsable rustc version" >&2
  exit 1
fi

#!/bin/sh
set -eu

# Seeds reach recipe shell words, so the Makefile must reject any seed that is
# not decimal digits or 0x followed by hexadecimal digits before a recipe runs
# (#1219). Each probe is a dry run (`make -n`), so an accepted seed only
# prints its command line, and the probes need neither Docker nor Cargo.

# A caller's own overrides must not reach the probes through MAKEFLAGS.
unset MAKEFLAGS MFLAGS MAKEOVERRIDES

source_root=${1:-.}
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM
marker="$temporary_root/injected"

# Runs a dry run of test-parser-fuzz with VARIABLE=VALUE on the command line
# and records its combined output in $output and its status in $status.
probe() {
  set +e
  output=$(make --no-print-directory -n -f "$source_root/Makefile" \
    -C "$source_root" test-parser-fuzz "$1=$2" 2>&1)
  status=$?
  set -e
}

# Fails unless VARIABLE=VALUE is refused by the Makefile's own check and
# nothing it carried ran.
expect_rejected() {
  probe "$1" "$2"
  if [ "$status" -eq 0 ]; then
    echo "$1 accepted an invalid seed: $2" >&2
    exit 1
  fi
  case "$output" in
    *"$1 must be decimal digits or 0x followed by hexadecimal digits"*) ;;
    *)
      echo "$1 failed without the seed diagnostic for $2: $output" >&2
      exit 1
      ;;
  esac
  if [ -e "$marker" ]; then
    echo "$1 ran shell carried by seed $2" >&2
    exit 1
  fi
}

# Fails unless VARIABLE=VALUE is accepted and quoted as given.
expect_accepted() {
  probe "$1" "$2"
  if [ "$status" -ne 0 ]; then
    echo "$1 rejected the valid seed $2: $output" >&2
    exit 1
  fi
}

for variable in STRUCTURED_FUZZ_SEED DECODER_FUZZ_SEED LIFECYCLE_STRESS_SEED \
  CONCURRENT_STRESS_SEED; do
  # The issue's reproduction: closing the quote passes a first-character check.
  expect_rejected "$variable" "1\"; touch '$marker'; \""
  # Make functions are refused unexpanded rather than evaluated.
  expect_rejected "$variable" "\$(shell touch '$marker')"
  expect_rejected "$variable" "1;touch $marker"
  expect_rejected "$variable" '1 2'
  expect_rejected "$variable" '0x'
  expect_rejected "$variable" '0xZZ'
  expect_rejected "$variable" 'abc'
  expect_rejected "$variable" ''
  expect_accepted "$variable" '12345'
  expect_accepted "$variable" '0x0506d3c0'
  expect_accepted "$variable" '0xABCDEF'
done

# A seed from the environment takes the same path as a command-line one.
set +e
output=$(env "STRUCTURED_FUZZ_SEED=1;touch $marker" make --no-print-directory \
  -n -f "$source_root/Makefile" -C "$source_root" test-parser-fuzz 2>&1)
status=$?
set -e
if [ "$status" -eq 0 ] || [ -e "$marker" ]; then
  echo "an environment seed bypassed the Makefile check: $output" >&2
  exit 1
fi

echo "Make seed validation: ok"

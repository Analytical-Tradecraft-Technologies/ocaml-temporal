#!/bin/sh
set -eu

# Contract for scripts/check-python-licenses.py (#517). It runs the scanner on
# small fake installed distributions under test/fixtures/python-licenses, so
# it needs neither the network nor a real install. The allowed tree must pass
# (an SPDX expression with its license file, an OR choice, an unambiguous
# classifier, and the reviewed protobuf entry). The denied tree must fail and
# name every disallowed, missing, malformed, ambiguous, contradicted or
# unlocked distribution, so no rule can silently become an allow.
root=${1:-$(CDPATH='' cd -- "$(dirname "$0")/../.." && pwd)}
scanner="$root/scripts/check-python-licenses.py"
fixtures="$root/test/fixtures/python-licenses"
python=${PYTHON:-python3}

if ! "$python" -I "$scanner" --lock "$fixtures/allowed/requirements.lock" \
  --path "$fixtures/allowed/site" >/dev/null; then
  echo "expected the allowed Python license fixture to pass" >&2
  exit 1
fi

if output=$("$python" -I "$scanner" --lock "$fixtures/denied/requirements.lock" \
  --path "$fixtures/denied/site" 2>&1); then
  echo "expected the denied Python license fixture to fail" >&2
  exit 1
fi

for package in gpl-expression missing-license copyleft-classifier free-text-only \
  ambiguous-classifier malformed-expression protobuf missing-license-file \
  unlocked-extra version-drift locked-missing; do
  if ! printf '%s\n' "$output" | grep -q "^DENY  $package "; then
    echo "expected a rejection for $package; scanner output:" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
done
if printf '%s\n' "$output" | grep -q '^ALLOW'; then
  echo "the denied fixture unexpectedly allowed a distribution" >&2
  exit 1
fi
echo "python license policy contract passed"

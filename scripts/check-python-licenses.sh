#!/bin/sh
set -eu

# License audit of the CI-only official Temporal Python SDK closure (#517).
# `make python-license-check` (run by `make license-check`) executes this in
# the digest-pinned Python image of the `sdk-interop-python` Compose service,
# never in the OCaml development image.
#
# It first runs the Docker-free policy contract on small fake distributions,
# then installs the hash-locked lock exactly as the interop driver does
# (--require-hashes --no-deps, wheels only) into a throwaway virtual
# environment created without pip, so the environment contains nothing but
# the locked distributions. scripts/check-python-licenses.py then reads each
# installed distribution's own license metadata and compares the installed
# set with the lock. Only the install step needs the network.
root=$(CDPATH='' cd -- "$(dirname "$0")/.." && pwd)
lock="$root/test/integration/sdk_interop/python/requirements.lock"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

sh "$root/test/smoke/test_python_license_policy.sh" "$root"

python3 -m venv --without-pip "$scratch/venv"
python3 -m pip --python "$scratch/venv/bin/python" install --quiet \
  --disable-pip-version-check --no-input --no-cache-dir --progress-bar off \
  --no-deps --only-binary ':all:' --require-hashes --requirement "$lock"
site=$("$scratch/venv/bin/python" -I -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
platform_site=$("$scratch/venv/bin/python" -I -c 'import sysconfig; print(sysconfig.get_path("platlib"))')
python3 -I "$root/scripts/check-python-licenses.py" --lock "$lock" \
  --path "$site" --path "$platform_site"

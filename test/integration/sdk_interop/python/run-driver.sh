#!/bin/sh
set -eu

# Entry point of the `sdk-interop-python` Compose service (#517). It runs in
# the pinned official Python image, never in the OCaml development image:
# the official Temporal Python SDK and its dependencies are installed into a
# throwaway virtual environment from the hash-locked requirements file next to
# this script, then driver.py runs in isolated mode (-I) so neither the
# working directory nor PYTHONPATH can inject modules.
#
# The lock pins every distribution by version and SHA-256 digest, and pip runs
# with --no-deps so nothing outside the reviewed lock can be installed; only
# prebuilt wheels are accepted, so no package build code runs here.
here=$(CDPATH='' cd -- "$(dirname "$0")" && pwd)
venv=${SDK_INTEROP_VENV:-/tmp/sdk-interop-venv}

python3 -m venv "$venv"
"$venv/bin/python" -m pip install --quiet --disable-pip-version-check \
  --no-input --no-cache-dir --progress-bar off --no-deps --only-binary ':all:' \
  --require-hashes --requirement "$here/requirements.lock"
exec "$venv/bin/python" -I "$here/driver.py"

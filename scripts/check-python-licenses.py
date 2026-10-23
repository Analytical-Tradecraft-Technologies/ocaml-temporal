#!/usr/bin/env python3
"""Fail closed when the installed CI-only Python closure has an unapproved license.

The official-SDK interoperability regression (#517) installs the hash-locked
`test/integration/sdk_interop/python/requirements.lock` into a throwaway
virtual environment. `scripts/check-python-licenses.sh` installs that lock the
same way and runs this script on the resulting site-packages directory, so the
audit reads the license metadata of the exact distributions the test runs,
not a hand-kept table.

For every installed distribution the script reads, through
`importlib.metadata`:

* `License-Expression` (PEP 639), parsed and evaluated with the SPDX parser
  and permissive allowlist shared with `scripts/check-cargo-licenses.py`;
* every `Classifier: License ::` entry, mapped to SPDX only where the
  classifier is unambiguous; a copyleft classifier always denies;
* the free-text `License` field, which is never interpreted on its own; and
* every `License-File` it declares, which must be present in the
  distribution.

A distribution passes when its expression (or, without one, its unambiguous
classifiers) is allowed and nothing in its metadata contradicts that. A
missing, unknown, malformed, copyleft or ambiguous license fails. The only
escape is `REVIEWED`, an explicit table keyed by exact name and version that
also pins the observed metadata value, so an upgrade or a metadata change
requires a new review. The installed set must equal the lock exactly: a
missing, extra or differently versioned distribution fails.

This script uses only the standard library and never accesses the network.
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from dataclasses import dataclass
from importlib.metadata import Distribution, distributions
from pathlib import Path
from types import ModuleType
from typing import TextIO


def load_license_policy() -> ModuleType:
    """Load the shared SPDX parser and allowlist from the Cargo scanner.

    The scanner's file name contains hyphens, so it is loaded by path, as
    `generate-cargo-sbom.py` does. It is registered before execution because
    its dataclasses resolve their module through `sys.modules`.
    """

    name = "check_cargo_licenses"
    if name in sys.modules:
        return sys.modules[name]
    path = Path(__file__).with_name("check-cargo-licenses.py")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load the license policy from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


POLICY = load_license_policy()

# Classifiers that name exactly one SPDX license. "BSD License" and "Apache
# Software License" omit the variant or version and are therefore ambiguous:
# a distribution relying on them alone needs a reviewed entry.
CLASSIFIER_LICENSES = {
    "License :: OSI Approved :: MIT License": "MIT",
    "License :: OSI Approved :: MIT No Attribution License (MIT-0)": "MIT-0",
    "License :: OSI Approved :: ISC License (ISCL)": "ISC",
    "License :: OSI Approved :: zlib/libpng License": "Zlib",
    "License :: OSI Approved :: The Unlicense (Unlicense)": "Unlicense",
}

# Fragments that make a license classifier copyleft or otherwise prohibited by
# the repository policy, whatever else the metadata declares.
COPYLEFT_MARKERS = (
    "GPL",
    "General Public License",
    "Affero",
    "Mozilla",
    "MPL",
    "Eclipse",
    "EUPL",
    "CDDL",
    "Common Public",
    "SSPL",
    "Server Side Public",
    "Non-Commercial",
)


@dataclass(frozen=True)
class Reviewed:
    """One exact, manually reviewed license conclusion.

    `field` and `observed` pin the metadata value that prompted the review,
    so the entry stops applying as soon as that value changes. `concluded` is
    the SPDX license this repository records for the distribution.
    """

    field: str
    observed: str
    concluded: str
    reason: str


# Reviewed conclusions, keyed by normalized name and exact version. Mirrored
# in docs/dependencies.md (CI-only interoperability tooling).
REVIEWED = {
    ("protobuf", "7.36.2"): Reviewed(
        field="License",
        observed="3-Clause BSD License",
        concluded="BSD-3-Clause",
        reason="the wheel declares only free text; upstream protocolbuffers/protobuf is BSD-3-Clause",
    ),
    ("typing-extensions", "4.16.0"): Reviewed(
        field="License-Expression",
        observed="PSF-2.0",
        concluded="PSF-2.0",
        reason="PSF-2.0 is permissive but outside the shared allowlist; CI-only and not redistributed",
    ),
}


def normalize_name(name: str) -> str:
    """Normalize a distribution name as PEP 503 does."""

    return re.sub(r"[-_.]+", "-", name).lower()


def read_lock(path: Path) -> dict[str, str]:
    """Return `{normalized name: version}` from `name==version` lock lines."""

    locked: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Za-z0-9._-]+)==([^\s\\;]+)", line)
        if match:
            locked[normalize_name(match.group(1))] = match.group(2)
    return locked


def license_classifiers(dist: Distribution) -> list[str]:
    """Return the distribution's `License ::` classifiers."""

    return [
        value
        for value in dist.metadata.get_all("Classifier") or []
        if value.startswith("License ::")
    ]


def missing_license_files(dist: Distribution) -> list[str]:
    """Return declared `License-File` entries that cannot be read.

    PEP 639 places them under `.dist-info/licenses/`; older wheel metadata
    put them directly in `.dist-info/`, so both locations are accepted. An
    empty file counts as missing.
    """

    declared = dist.metadata.get_all("License-File") or []
    return [
        name
        for name in declared
        if not (dist.read_text(f"licenses/{name}") or dist.read_text(name))
    ]


def evaluate_expression(expression: str) -> tuple[bool, str]:
    """Evaluate one SPDX expression with the shared parser and allowlist."""

    try:
        return POLICY.evaluate(POLICY.Parser(expression).parse())
    except POLICY.ExpressionError as error:
        return False, f"malformed expression: {error}"


def conclude(name: str, version: str, dist: Distribution) -> tuple[bool, str]:
    """Return whether one distribution is allowed, and why."""

    metadata = dist.metadata
    expression = (metadata.get("License-Expression") or "").strip()
    free_text = (metadata.get("License") or "").strip()
    classifiers = license_classifiers(dist)
    copyleft = [
        value
        for value in classifiers
        if any(marker in value for marker in COPYLEFT_MARKERS)
    ]
    if copyleft:
        return False, f"copyleft classifier {copyleft[0]!r}"
    missing = missing_license_files(dist)
    if missing:
        return False, f"declared license file missing: {', '.join(missing)}"

    reviewed = REVIEWED.get((name, version))
    if reviewed is not None:
        observed = (metadata.get(reviewed.field) or "").strip()
        if observed != reviewed.observed:
            return False, (
                f"reviewed {reviewed.field} changed from {reviewed.observed!r} "
                f"to {observed!r}; review again"
            )
        return True, f"{reviewed.concluded} via reviewed metadata ({reviewed.reason})"

    if expression:
        allowed, choice = evaluate_expression(expression)
        if not allowed:
            return False, f"{expression} ({choice})"
        mapped = {CLASSIFIER_LICENSES.get(value) for value in classifiers}
        if any(value is not None and value not in expression for value in mapped):
            return False, f"classifiers {classifiers!r} contradict {expression}"
        return True, f"{expression} via {choice}"

    mapped = {CLASSIFIER_LICENSES.get(value) for value in classifiers}
    if not classifiers and not free_text:
        return False, "missing-license"
    if not classifiers or None in mapped or len(mapped) != 1:
        return False, (
            f"ambiguous license: License={free_text[:60]!r}, "
            f"classifiers={classifiers!r}"
        )
    (spdx,) = mapped
    allowed, choice = evaluate_expression(spdx)
    if not allowed:
        return False, f"{spdx} ({choice})"
    return True, f"{spdx} via classifier"


def check(paths: list[str], locked: dict[str, str], output: TextIO) -> bool:
    """Audit every distribution found on `paths` against the policy and lock."""

    installed: dict[str, tuple[str, Distribution]] = {}
    passed = True
    # purelib and platlib are usually the same directory; scan each once.
    for dist in distributions(path=list(dict.fromkeys(paths))):
        name = normalize_name(dist.metadata.get("Name") or "<missing-name>")
        if name in installed:
            print(f"DENY  {name} duplicate installed distribution", file=output)
            passed = False
        installed[name] = (dist.version, dist)

    for name in sorted(set(locked) | set(installed)):
        if name not in installed:
            print(f"DENY  {name} {locked[name]} locked but not installed", file=output)
            passed = False
            continue
        version, dist = installed[name]
        if name not in locked:
            print(f"DENY  {name} {version} installed but not in the lock", file=output)
            passed = False
            continue
        if version != locked[name]:
            print(
                f"DENY  {name} {version} differs from locked {locked[name]}",
                file=output,
            )
            passed = False
            continue
        allowed, reason = conclude(name, version, dist)
        print(
            f"{'ALLOW' if allowed else 'DENY '} {name} {version} {reason}", file=output
        )
        passed = passed and allowed
    return passed


def main() -> int:
    """Parse arguments, audit the closure and return a process status."""

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--lock", required=True, type=Path, help="hash-locked requirements file"
    )
    parser.add_argument(
        "--path",
        required=True,
        action="append",
        help="site-packages directory holding the installed closure",
    )
    arguments = parser.parse_args()
    locked = read_lock(arguments.lock)
    if not locked:
        print(f"DENY  no name==version lines in {arguments.lock}", file=sys.stderr)
        return 1
    return 0 if check(arguments.path, locked, sys.stdout) else 1


if __name__ == "__main__":
    raise SystemExit(main())

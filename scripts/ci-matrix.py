#!/usr/bin/env python3
"""Select exact compilers while keeping required status-check labels stable."""

import json
import sys

# One supported patch per compiler series; artifact metadata uses the full value.
COMPILERS = ("5.2.1", "5.3.0", "5.4.1", "5.5.1")


def matrices(tier: str) -> dict[str, dict[str, list[dict[str, str]]]]:
    """Return bounded PR coverage, broader master coverage, or the full release."""
    if tier not in ("pr", "master", "release"):
        raise ValueError(f"unsupported build tier: {tier}")
    linux = [
        {"ocaml": version, "label": version.rsplit(".", 1)[0], "runner": runner}
        for runner in ("ubuntu-24.04", "ubuntu-24.04-arm")
        for version in COMPILERS
        if tier != "pr"
        or version == COMPILERS[-1]
        or (runner == "ubuntu-24.04" and version == COMPILERS[0])
    ]
    native = [
        {"ocaml": version, "label": version.rsplit(".", 1)[0]}
        for version in (COMPILERS if tier == "release" else COMPILERS[-1:])
    ]
    return {"linux": {"include": linux}, "native": {"include": native}}


if __name__ == "__main__":
    for name, matrix in matrices(sys.argv[1]).items():
        print(f"{name}={json.dumps(matrix, separators=(',', ':'))}")

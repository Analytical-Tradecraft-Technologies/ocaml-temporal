#!/usr/bin/env python3
"""Select exact compilers while keeping required status-check labels stable."""

import json
import sys

# One supported patch per compiler series; artifact metadata uses the full value.
COMPILERS = ("5.2.1", "5.3.0", "5.4.1", "5.5.1")

# The next compiler series, outside the supported `< 5.6` bound (#1164). Master
# builds run it in one non-blocking Linux amd64 lane from its pinned trunk or
# alpha image, so incompatibilities surface before its release. It never
# gates a pull request or a release; the bound-raise rule is in
# docs/reference/release-checklist.md.
NEXT_SERIES = "5.6"


def matrices(tier: str) -> dict[str, dict[str, list[dict[str, str]]]]:
    """Return bounded PR coverage, broader master coverage, or the full release.

    The ``next`` matrix holds the non-blocking next-compiler lane on master
    and is empty in the other tiers, whose workflows skip that job.
    """
    if tier not in ("pr", "master", "release"):
        raise ValueError(f"unsupported build tier: {tier}")
    following = (
        [{"ocaml": NEXT_SERIES, "label": NEXT_SERIES, "runner": "ubuntu-24.04"}]
        if tier == "master"
        else []
    )
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
    return {
        "linux": {"include": linux},
        "native": {"include": native},
        "next": {"include": following},
    }


if __name__ == "__main__":
    for name, matrix in matrices(sys.argv[1]).items():
        print(f"{name}={json.dumps(matrix, separators=(',', ':'))}")

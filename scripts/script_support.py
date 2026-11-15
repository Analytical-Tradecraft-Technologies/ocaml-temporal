"""Standard-library helpers shared by the repository's Python scripts.

The scripts beside this module have hyphenated file names and are run
directly (`python3 scripts/<name>.py`, or `python3 -I` in the pinned official
Python container), never installed as a package. Isolated mode leaves a
script's own directory off `sys.path`, so each user puts that directory first
before importing this module:

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from script_support import load_script

`ocaml-library-artifact.py` deliberately does not import this module: it is
copied alone into every prebuilt SDK bundle and must stay self-contained.
Scripts that share its artifact checks load it with `load_script` instead.

Only the standard library is used, and nothing here touches the network.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


def load_script(name: str) -> ModuleType:
    """Import the sibling script `<name>.py` without running its entry point.

    Hyphenated file names cannot be imported with `import`, so the file is
    loaded by path. The module is registered in `sys.modules`, under `name`
    with hyphens replaced by underscores, before it executes, because
    dataclasses resolve their defining module there. A later request returns
    the registered module, so every caller in one process shares a single
    instance: the licence tools, for example, all reach one conclusion from
    the one Cargo policy in `check-cargo-licenses.py`.
    """

    module_name = name.replace("-", "_")
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(f"{name}.py")
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load sibling script {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def read_json(path: Path, what: str = "JSON") -> Any:
    """Parse one UTF-8 JSON file into whatever value its root holds.

    An unreadable or malformed file becomes a `ValueError` naming the file as
    `what` (for example "reviewed notices"), so command-line tools can report
    it as one concise line instead of a traceback. The caller checks the
    shape of the value.
    """

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {what} {path}: {exc}") from exc


def load_json_object(path: Path) -> dict[str, Any]:
    """Read a JSON document, such as Cargo metadata or SPDX, whose root must be an object."""

    value = read_json(path)
    if not isinstance(value, dict):
        raise ValueError(f"JSON root in {path} must be an object")
    return value


def package_list(document: dict[str, Any], container: str) -> list[Any]:
    """Return the non-empty `packages` array of Cargo metadata or an SPDX document.

    `container` names the document in the error, such as "Cargo metadata" or
    "SBOM". The items are not inspected here: callers check each one with
    `package_fields` inside their own loop, so the first failing package is
    reported in the same order as their other per-package checks.
    """

    packages = document.get("packages")
    if not isinstance(packages, list) or not packages:
        raise ValueError(f"{container} contains no packages")
    return packages


def package_fields(
    package: object, container: str, fields: tuple[str, ...], missing: str
) -> tuple[str, ...]:
    """Return the named fields of one package entry, each a non-empty string.

    A non-object entry fails as "`container` package is not an object"; an
    absent, empty or non-string field fails with the caller's `missing`
    message, which lists the fields under the names its readers know.
    """

    if not isinstance(package, dict):
        raise ValueError(f"{container} package is not an object")
    values = tuple(package.get(field) for field in fields)
    if not all(isinstance(value, str) and value for value in values):
        raise ValueError(missing)
    return values


def add_generate_or_audit(
    parser: argparse.ArgumentParser, generate: str, generate_help: str, audit_help: str
) -> None:
    """Add the required choice between producing a document and auditing one.

    The SBOM and notices generators share this command-line contract: exactly
    one of the path option `generate` (which selects generation and names its
    input or output) and `--audit` (the existing document to check).
    """

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(generate, type=Path, help=generate_help)
    group.add_argument("--audit", type=Path, help=audit_help)

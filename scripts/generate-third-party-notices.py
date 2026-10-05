#!/usr/bin/env python3
"""Generate and audit the third-party licence notices shipped with releases.

Every release archive contains the Rust bridge, which statically links the
locked Cargo graph. The permissive licences of those packages (MIT, BSD, ISC,
Apache-2.0, Unicode-3.0, ...) require their licence texts and attributions to
accompany binary redistribution. This script copies those texts from the
package sources that `cargo metadata --locked` already downloaded, so it needs
no network access and no tool beyond the Python standard library.

The output is deterministic for a given Cargo graph: packages, texts, and text
identifiers are ordered by package identity and file content only, and no
checkout path, timestamp, or environment value is recorded. Generation fails,
listing every offending package, when a package has neither a licence file nor
a standard text for one of its licence choices; it never emits a partial file.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import posixpath
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


HEADER = "ocaml-temporal third-party notices"
# Matches the files crates conventionally ship to satisfy attribution
# requirements: LICENSE, LICENSE-MIT, LICENCE.md, COPYING, NOTICE, COPYRIGHT,
# UNLICENSE, and REUSE-style LICENSES directories.
LICENSE_FILE_PATTERN = re.compile(
    r"^(licen[cs]es?|copying|copyright|notice|unlicense)([-._].*)?$", re.IGNORECASE
)
STANDARD_TEXTS = Path(__file__).with_name("license-texts")
INVENTORY_LINE = re.compile(r"^  - (\S+) (\S+): (T[0-9]{4}(?:, T[0-9]{4})*)$")
TEXT_HEADER = re.compile(r"^---- (T[0-9]{4}) ----$")


def load_license_policy() -> Any:
    """Import the Cargo licence scanner so all tools share one conclusion.

    The scanner's file name contains hyphens, so it is loaded by path. The
    module is registered before execution because its dataclasses resolve
    their defining module through `sys.modules`.
    """

    name = "check_cargo_licenses"
    if name in sys.modules:
        return sys.modules[name]
    path = Path(__file__).with_name("check-cargo-licenses.py")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load Cargo license policy from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


LICENSE_POLICY = load_license_policy()


@dataclass(frozen=True)
class Notice:
    """One licence or attribution text attributed to one package.

    `label` names where the text came from (a file relative to the package
    root, or the standard text used as a fallback) and is shown to readers;
    `text` is the normalized content that is deduplicated across packages.
    """

    label: str
    text: str


@dataclass(frozen=True)
class PackageNotices:
    """The concluded licence and every notice text for one locked package."""

    name: str
    version: str
    license: str
    notices: tuple[Notice, ...]
    fallback_note: str | None


def normalize_text(raw: bytes) -> str:
    """Decode a licence file into stable text.

    Undecodable bytes are replaced rather than rejected so that a legacy
    Latin-1 file still yields the same output on every platform. Line endings
    are unified and trailing whitespace removed so a CRLF checkout of the same
    crate cannot change the generated file.
    """

    text = raw.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in text.split("\n")]
    while lines and not lines[-1]:
        lines.pop()
    while lines and not lines[0]:
        lines.pop(0)
    return "\n".join(lines) + "\n" if lines else ""


def package_root(package: dict[str, Any]) -> Path:
    """Return the directory containing a package's Cargo manifest."""

    manifest = package.get("manifest_path")
    if not isinstance(manifest, str) or not manifest:
        raise ValueError(f"{package_label(package)} has no manifest_path")
    return Path(manifest).parent


def package_label(package: dict[str, Any]) -> str:
    """Name a package in diagnostics without assuming metadata is valid."""

    return f"{package.get('name', '<missing-name>')} {package.get('version', '<missing-version>')}"


def collect_files(package: dict[str, Any]) -> list[Notice]:
    """Read the licence and attribution files a package distributes.

    Only conventional top-level names (and one level of a `LICENSES`
    directory) are considered; source headers are not scanned. A metadata
    `license_file` is always included and must exist, because Cargo publishes
    it as the package's authoritative licence (the pinned Core crates point at
    their workspace `LICENSE.txt` outside the package directory).
    """

    root = package_root(package)
    found: dict[str, Path] = {}
    if root.is_dir():
        for entry in root.iterdir():
            if not LICENSE_FILE_PATTERN.match(entry.name) or entry.is_symlink():
                continue
            if entry.is_file():
                found[entry.name] = entry
            elif entry.is_dir():
                for child in entry.iterdir():
                    if child.is_file() and not child.is_symlink():
                        found[f"{entry.name}/{child.name}"] = child
    license_file = package.get("license_file")
    if isinstance(license_file, str) and license_file:
        relative = posixpath.normpath(license_file.replace("\\", "/"))
        path = root / relative
        if not path.is_file():
            raise ValueError(
                f"declares license_file {license_file!r}, which is missing from the package source"
            )
        found.setdefault(relative, path)
    notices = []
    for label in sorted(found):
        text = normalize_text(found[label].read_bytes())
        if text:
            notices.append(Notice(label, text))
    return notices


class LicenseExpression(Protocol):
    """Structural view of a parsed SPDX expression node from the shared
    licence scanner (`check-cargo-licenses.py`'s `Node`): a leaf licence, or an
    `and`/`or`/`with` combination of a left and right operand."""

    kind: str
    value: str | None
    left: "LicenseExpression | None"
    right: "LicenseExpression | None"


def standard_choice(node: LicenseExpression, available: set[str]) -> list[str] | None:
    """Pick licence identifiers satisfiable with this repository's standard texts.

    An `OR` takes its first satisfiable branch, an `AND` needs both branches,
    and a `WITH` exception is never satisfied by a plain standard text. The
    choice depends only on the expression, so it is deterministic.
    """

    if node.kind == "license":
        return [node.value] if node.value in available else None
    if node.kind == "and":
        left = standard_choice(node.left, available)
        right = standard_choice(node.right, available)
        return None if left is None or right is None else left + right
    if node.kind == "or":
        return standard_choice(node.left, available) or standard_choice(node.right, available)
    return None


def standard_notices(
    package: dict[str, Any], expression: str, texts_dir: Path
) -> tuple[list[Notice], str] | None:
    """Reproduce standard licence texts for a package that ships none.

    Some crates declare a licence in Cargo metadata but omit the file from the
    published package. Their declared licence is honoured by reproducing its
    standard text and naming the copyright holders recorded in Cargo metadata.
    Returns `None` when no branch of the expression has a standard text.
    """

    available = {path.stem for path in texts_dir.glob("*.txt")}
    choice = standard_choice(LICENSE_POLICY.Parser(expression).parse(), available)
    if not choice:
        return None
    notices = [
        Notice(f"standard {identifier} text",
               normalize_text((texts_dir / f"{identifier}.txt").read_bytes()))
        for identifier in dict.fromkeys(choice)
    ]
    authors = package.get("authors") or []
    holders = ", ".join(author for author in authors if isinstance(author, str))
    repository = package.get("repository")
    note = (
        "No licence file is distributed with this package; the standard text of "
        f"{' AND '.join(dict.fromkeys(choice))} is reproduced. Copyright holders: "
        f"{holders or 'the package authors'}"
        + (f" ({repository})" if isinstance(repository, str) and repository else "")
        + "."
    )
    return notices, note


def third_party_packages(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """Return every locked package except this project's own workspace members.

    The inventory deliberately covers the whole Cargo graph, including
    platform-specific and build-only packages, so a single file is a superset
    of what any one platform links.
    """

    packages = metadata.get("packages")
    if not isinstance(packages, list) or not packages:
        raise ValueError("Cargo metadata contains no packages")
    members = metadata.get("workspace_members")
    if not isinstance(members, list):
        raise ValueError("Cargo metadata is missing workspace_members")
    selected = []
    for package in packages:
        if not isinstance(package, dict):
            raise ValueError("Cargo metadata package is not an object")
        name, version = package.get("name"), package.get("version")
        if not (isinstance(name, str) and name and isinstance(version, str) and version):
            raise ValueError("Cargo package is missing name or version")
        if package.get("id") not in members:
            selected.append(package)
    return sorted(selected, key=lambda item: (item["name"], item["version"], item.get("source") or ""))


def collect(metadata: dict[str, Any], texts_dir: Path = STANDARD_TEXTS) -> list[PackageNotices]:
    """Resolve the concluded licence and notice texts of every package.

    All failures are gathered before raising so one run reports every package
    that needs review.
    """

    results = []
    errors = []
    for package in third_party_packages(metadata):
        label = package_label(package)
        concluded = LICENSE_POLICY.concluded_license(package)
        if concluded is None:
            errors.append(f"{label}: no concluded licence (run scripts/check-cargo-licenses.py)")
            continue
        try:
            notices = collect_files(package)
        except (OSError, ValueError) as exc:
            errors.append(f"{label}: {exc}")
            continue
        fallback_note = None
        if not notices:
            standard = standard_notices(package, concluded, texts_dir)
            if standard is None:
                errors.append(
                    f"{label}: no licence file in the package and no standard text for {concluded}"
                )
                continue
            notices, fallback_note = standard
        results.append(
            PackageNotices(package["name"], package["version"], concluded, tuple(notices), fallback_note)
        )
    if errors:
        raise ValueError("missing third-party licence texts:\n  " + "\n  ".join(errors))
    return results


def render(packages: list[PackageNotices], project_license: str) -> str:
    """Render the notices document.

    Identical texts are printed once and referenced by `Tnnnn` identifiers.
    Identifiers are assigned in order of the first package (by name and
    version) that uses each text, then by file label and content digest, so
    they depend only on the Cargo graph.
    """

    users: dict[str, list[tuple[PackageNotices, str]]] = {}
    texts: dict[str, str] = {}
    for package in packages:
        for notice in package.notices:
            digest = hashlib.sha256(notice.text.encode("utf-8")).hexdigest()
            texts[digest] = notice.text
            users.setdefault(digest, []).append((package, notice.label))
    order = sorted(
        users,
        key=lambda digest: (
            users[digest][0][0].name, users[digest][0][0].version, users[digest][0][1], digest
        ),
    )
    identifiers = {digest: f"T{index:04d}" for index, digest in enumerate(order, start=1)}

    lines = [
        HEADER,
        "=" * len(HEADER),
        "",
        "Generated by scripts/generate-third-party-notices.py from the locked Cargo",
        "graph (rust/Cargo.lock). Do not edit this file by hand.",
        "",
        "Every ocaml-temporal release archive contains the Rust bridge, which",
        "statically links the Rust packages listed below. The list covers the whole",
        "locked Cargo graph, including platform-specific and build-only packages, so",
        "it is a superset of what any single platform links. OCaml dependencies",
        "(the OCaml runtime, logs, yojson) are not redistributed in these archives;",
        "applications obtain and link them from their own OPAM switch.",
        "",
        "Part 1. ocaml-temporal licence",
        "------------------------------",
        "",
        project_license.rstrip("\n"),
        "",
        "Part 2. Third-party packages by licence",
        "---------------------------------------",
        "",
        "Each package lists the identifiers of its licence and notice texts in Part 3.",
    ]
    by_license: dict[str, list[PackageNotices]] = {}
    for package in packages:
        by_license.setdefault(package.license, []).append(package)
    for expression in sorted(by_license):
        lines += ["", f"{expression}:"]
        for package in by_license[expression]:
            ids = sorted({
                identifiers[hashlib.sha256(notice.text.encode("utf-8")).hexdigest()]
                for notice in package.notices
            })
            lines.append(f"  - {package.name} {package.version}: {', '.join(ids)}")
            if package.fallback_note:
                lines.append(f"    {package.fallback_note}")
    lines += ["", "Part 3. Licence and notice texts", "--------------------------------"]
    for digest in order:
        lines += ["", f"---- {identifiers[digest]} ----"]
        sources = sorted({f"{package.name} {package.version} ({label})" for package, label in users[digest]})
        lines += [f"From: {source}" for source in sources]
        lines += ["", texts[digest].rstrip("\n")]
    return "\n".join(lines) + "\n"


def generate(metadata: dict[str, Any], project_license: str, texts_dir: Path = STANDARD_TEXTS) -> str:
    """Collect and render notices for one Cargo metadata document."""

    if not project_license.strip():
        raise ValueError("project licence text is empty")
    return render(collect(metadata, texts_dir), normalize_text(project_license.encode("utf-8")))


def audit(document: str, metadata: dict[str, Any], project_license: str) -> None:
    """Check that a notices file is complete for the locked Cargo graph.

    The audit needs only metadata, not package sources: it proves the project
    licence is present, every third-party package appears exactly once in the
    inventory, and every referenced text exists (and every text is referenced).
    """

    lines = document.split("\n")
    if not lines or lines[0] != HEADER:
        raise ValueError("notices file does not start with the expected header")
    if normalize_text(project_license.encode("utf-8")).rstrip("\n") not in document:
        raise ValueError("notices file does not contain the project licence")
    listed: dict[tuple[str, str], list[str]] = {}
    headers: set[str] = set()
    for line in lines:
        if match := INVENTORY_LINE.match(line):
            key = (match.group(1), match.group(2))
            if key in listed:
                raise ValueError(f"package listed twice: {key[0]} {key[1]}")
            listed[key] = match.group(3).split(", ")
        elif match := TEXT_HEADER.match(line):
            if match.group(1) in headers:
                raise ValueError(f"duplicate notice text {match.group(1)}")
            headers.add(match.group(1))
    expected = {(package["name"], package["version"]) for package in third_party_packages(metadata)}
    missing = sorted(expected - listed.keys())
    if missing:
        raise ValueError("packages missing from notices: " + ", ".join(" ".join(key) for key in missing))
    unexpected = sorted(listed.keys() - expected)
    if unexpected:
        raise ValueError("notices list packages outside the Cargo graph: "
                         + ", ".join(" ".join(key) for key in unexpected))
    referenced = {identifier for ids in listed.values() for identifier in ids}
    if referenced != headers:
        raise ValueError("notice text references and texts differ: "
                         + ", ".join(sorted(referenced ^ headers)))


def load_json(path: Path) -> dict[str, Any]:
    """Load Cargo metadata and turn malformed input into a concise CLI error."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON root in {path} must be an object")
    return value


def main() -> int:
    """Generate (`--output`) or audit (`--audit`) a notices file."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True,
                        help="output of cargo metadata --locked --format-version 1")
    parser.add_argument("--project-license", type=Path, required=True,
                        help="this project's LICENSE file")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--output", type=Path, help="notices file to write")
    group.add_argument("--audit", type=Path, help="notices file to check for completeness")
    args = parser.parse_args()
    try:
        metadata = load_json(args.metadata)
        project_license = args.project_license.read_text(encoding="utf-8")
        if args.output is not None:
            document = generate(metadata, project_license)
            args.output.write_bytes(document.encode("utf-8"))
            print(f"third-party notices: wrote {args.output}")
        else:
            audit(args.audit.read_bytes().decode("utf-8"), metadata, project_license)
            print("third-party notices audit: ok")
    except (OSError, ValueError) as exc:
        print(f"third-party notices: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

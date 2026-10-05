"""Exercise the third-party notices generator against small offline fixtures.

The fixtures model the shapes found in the real locked Cargo graph: crates
shipping their own licence files, shared identical texts, the pinned Core
crates whose licence is a workspace file outside the package directory, and
crates that publish no licence file and therefore need a reviewed upstream
notice. No network or Cargo is needed.
"""

import copy
import hashlib
import json
from pathlib import Path
import random
import shutil
import subprocess
import sys
import tempfile
import unittest

from test_ci_artifacts import NOTICES, ROOT

PROJECT_LICENSE = "Apache License\nVersion 2.0, January 2004\n\nProject terms.\n"
APACHE_TEXT = "Apache License\nShared crate copy of the Apache text.\n"
CORE_SOURCE = NOTICES.LICENSE_POLICY.CORE_SOURCE_PREFIX + "core"
DELTA_MIT = "Copyright (c) 2020 Delta Author\n\nPermission is hereby granted.\n"
TEMPLATE_LINE = "Copyright (c) <year> <copyright holders>"
UPSTREAM = "https://example.invalid/delta/blob/" + "d" * 40 + "/LICENSE-MIT"


def write(path, text):
    """Create one fixture file and its parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def package(root, name, version, license, files=(), **extra):
    """Describe one Cargo package rooted under `root/name-version`.

    `files` maps file names below the package directory to their contents.
    Extra keyword arguments override Cargo metadata fields directly.
    """
    directory = root / f"{name}-{version}"
    directory.mkdir(parents=True, exist_ok=True)
    for relative, text in dict(files).items():
        write(directory / relative, text)
    entry = {
        "id": f"registry+https://example.invalid/index#{name}@{version}",
        "name": name,
        "version": version,
        "license": license,
        "license_file": None,
        "manifest_path": str(directory / "Cargo.toml"),
        "source": "registry+https://example.invalid/index",
        "authors": [],
        "repository": None,
    }
    entry.update(extra)
    return entry


def fixture_metadata(root):
    """Build a Cargo metadata document covering every supported notice shape."""
    core_root = root / "checkout"
    write(core_root / "LICENSE.txt", "The MIT License\n\nCopyright (c) Temporal\n")
    core_manifest = core_root / "crates/sdk-core/Cargo.toml"
    core_manifest.parent.mkdir(parents=True)
    packages = [
        package(root, "alpha", "1.0.0", "MIT", {"LICENSE-MIT": "MIT License\r\nCopyright alpha  \r\n"}),
        package(root, "beta", "2.0.0", "MIT/Apache-2.0", {"LICENSE-APACHE": APACHE_TEXT}),
        package(root, "gamma", "0.1.0", "Apache-2.0",
                {"LICENSE": APACHE_TEXT, "NOTICE": "Gamma includes work by Example Corp.\n",
                 "src/LICENSE-ignored": "not a top-level file\n"}),
        package(root, "delta", "0.3.0", "Zlib OR Apache-2.0 OR MIT",
                authors=["Delta Author <delta@example.invalid>"],
                repository="https://example.invalid/delta"),
        package(root, "epsilon", "0.2.0", "Zlib OR Apache-2.0 OR MIT"),
        dict(package(root, "temporalio-sdk-core", "0.5.0", None),
             id=CORE_SOURCE, source=CORE_SOURCE, license_file="../../LICENSE.txt",
             manifest_path=str(core_manifest)),
        dict(package(root, "bridge", "0.1.0", "Apache-2.0"), id="path+file:///bridge", source=None),
    ]
    return {"packages": packages, "workspace_members": ["path+file:///bridge"]}


def fixture_manifest():
    """Reviewed notices for the fixture packages that ship no licence file.

    `delta` uses a vendored upstream MIT notice; `epsilon` models a crate whose
    upstream only states its licence choice, so the standard Apache-2.0 text
    is added alongside the vendored statement.
    """
    return {
        "notices": [
            {
                "packages": [{"name": "delta", "version": "0.3.0"}],
                "license": "Zlib OR Apache-2.0 OR MIT",
                "files": [{"path": "delta/LICENSE-MIT", "upstream": UPSTREAM,
                           "sha256": hashlib.sha256(DELTA_MIT.encode()).hexdigest()}],
                "evidence": "Repository root LICENSE-MIT at the published commit.",
            },
            {
                "packages": [{"name": "epsilon", "version": "0.2.0"}],
                "license": "Zlib OR Apache-2.0 OR MIT",
                "files": [{"path": "epsilon/LICENSE.md",
                           "upstream": "https://example.invalid/e/blob/" + "e" * 40 + "/LICENSE.md",
                           "sha256": hashlib.sha256(b"Licensed under Zlib, Apache-2.0, or MIT.\n").hexdigest()}],
                "standard_texts": ["Apache-2.0"],
                "evidence": "Upstream names its licences without a copyright line; Apache-2.0 elected.",
            },
        ]
    }


def write_reviewed(root, manifest=None):
    """Lay out a reviewed-notice directory like `scripts/license-texts/crates`.

    The real standard Apache-2.0 text is copied into the parent directory so
    `standard_texts` resolve exactly as in the repository.
    """
    texts = root / "license-texts"
    reviewed = texts / "crates"
    write(reviewed / "delta/LICENSE-MIT", DELTA_MIT)
    write(reviewed / "epsilon/LICENSE.md", "Licensed under Zlib, Apache-2.0, or MIT.\n")
    shutil.copyfile(ROOT / "scripts/license-texts/Apache-2.0.txt", texts / "Apache-2.0.txt")
    write(reviewed / "manifest.json", json.dumps(manifest or fixture_manifest()))
    return reviewed


class GenerateTests(unittest.TestCase):
    """The generated document is complete, deduplicated, and reproducible."""

    def setUp(self):
        """Create one fixture tree in a disposable directory."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.metadata = fixture_metadata(self.root / "first")
        self.reviewed = write_reviewed(self.root / "first")
        self.document = NOTICES.generate(self.metadata, PROJECT_LICENSE, self.reviewed)

    def test_deterministic_and_path_independent(self):
        """Another checkout location and package order yield identical bytes."""
        other = fixture_metadata(self.root / "second/deeper")
        random.Random(787).shuffle(other["packages"])
        reviewed = write_reviewed(self.root / "second/deeper")
        self.assertEqual(NOTICES.generate(other, PROJECT_LICENSE, reviewed), self.document)
        self.assertNotIn(str(self.root), self.document)
        NOTICES.audit(self.document, self.metadata, PROJECT_LICENSE)

    def test_inventory_and_texts(self):
        """Every package except the workspace is listed under its concluded licence."""
        lines = self.document.splitlines()
        self.assertEqual(lines[0], NOTICES.HEADER)
        self.assertIn("Project terms.", self.document)
        self.assertNotIn(" bridge 0.1.0", self.document)
        self.assertRegex(self.document, r"\nMIT:\n(  - .*\n)*  - temporalio-sdk-core 0\.5\.0: T")
        self.assertIn("MIT OR Apache-2.0:\n  - beta 2.0.0: T", self.document)
        # The shared Apache copy is printed once and referenced by both crates.
        self.assertEqual(self.document.count("Shared crate copy of the Apache text."), 1)
        beta = next(line for line in lines if line.startswith("  - beta "))
        gamma = next(line for line in lines if line.startswith("  - gamma "))
        self.assertIn(beta.rsplit(": ", 1)[1], gamma)
        self.assertIn("Gamma includes work by Example Corp.", self.document)
        self.assertNotIn("not a top-level file", self.document)
        self.assertIn("From: temporalio-sdk-core 0.5.0 (../../LICENSE.txt)", self.document)
        # CRLF and trailing whitespace are normalized away.
        self.assertIn("MIT License\nCopyright alpha\n", self.document)
        self.assertNotIn("\r", self.document)

    def test_vendored_notice_for_crate_without_licence_file(self):
        """A file-less crate reproduces its reviewed upstream notice, not a template."""
        self.assertIn(
            "  - delta 0.3.0: T", self.document)
        self.assertIn(
            "    No licence file is distributed with this package; reviewed upstream "
            "licence texts are reproduced.", self.document)
        self.assertIn(f"From: delta 0.3.0 (upstream {UPSTREAM})", self.document)
        self.assertIn("Copyright (c) 2020 Delta Author", self.document)
        self.assertEqual(NOTICES.placeholder_lines(self.document), [])

    def test_standard_text_only_alongside_upstream_statement(self):
        """A standard text is added only when a reviewed entry elects it."""
        self.assertIn(
            "    No licence file is distributed with this package; reviewed upstream licence "
            "texts are reproduced, with the standard Apache-2.0 text that upstream only "
            "references.", self.document)
        self.assertIn("From: epsilon 0.2.0 (standard Apache-2.0 text)", self.document)
        self.assertNotIn("From: delta 0.3.0 (standard", self.document)
        standard = (ROOT / "scripts/license-texts/Apache-2.0.txt").read_text()
        self.assertIn(standard.strip(), self.document)


class PlaceholderTests(unittest.TestCase):
    """Licence-template placeholders are recognised without flagging real notices."""

    def test_detects_template_lines(self):
        """MIT/BSD template forms are placeholders; real notices and the Apache appendix are not."""
        for line in [TEMPLATE_LINE, "Copyright [year] [fullname]", "Copyright {year} {owner}",
                     "Copyright (C) <YEAR> <COPYRIGHT HOLDER>", "copyright <name of author>"]:
            with self.subTest(line=line):
                self.assertEqual(NOTICES.placeholder_lines(f"x\n  {line}\n"), [line])
        for line in ["Copyright (c) 2016 Alex Example <alex@example.invalid>",
                     "Copyright [yyyy] [name of copyright owner]",
                     "   Copyright {yyyy} {name of copyright owner}",
                     "Licensed to <the owner> on a line without the C-word"]:
            with self.subTest(line=line):
                self.assertEqual(NOTICES.placeholder_lines(line), [])

    def test_repository_ships_no_placeholder_templates(self):
        """Every standard and vendored text in the repository is free of placeholders."""
        texts = ROOT / "scripts/license-texts"
        for path in sorted(texts.rglob("*")):
            if path.is_file() and path.name != "manifest.json":
                with self.subTest(path=str(path.relative_to(ROOT))):
                    self.assertEqual(NOTICES.placeholder_lines(path.read_text(encoding="utf-8")), [])

    def test_repository_manifest_is_valid(self):
        """The checked-in reviewed manifest loads: hashes, paths, and sources all verify."""
        reviewed = NOTICES.load_reviewed(NOTICES.REVIEWED_NOTICES)
        self.assertIn(("valuable", "0.1.1"), reviewed)
        self.assertIn("Copyright (c) 2021 Valuable Contributors",
                      reviewed[("valuable", "0.1.1")].notices[0].text)


class FailureTests(unittest.TestCase):
    """Missing or unreviewed licence texts fail closed and name every package."""

    def setUp(self):
        """Create one fixture tree in a disposable directory."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.metadata = fixture_metadata(self.root)
        self.reviewed = write_reviewed(self.root)

    def generate_with(self, manifest):
        """Generate the fixture document against a modified reviewed manifest."""
        write(self.reviewed / "manifest.json", json.dumps(manifest))
        return NOTICES.generate(self.metadata, PROJECT_LICENSE, self.reviewed)

    def test_missing_reviewed_notice_is_reported_for_every_package(self):
        """No partial document is produced when any file-less package lacks a review."""
        self.metadata["packages"] += [
            package(self.root, "isc-only", "1.0.0", "ISC"),
            package(self.root, "llvm", "1.0.0", "Apache-2.0 WITH LLVM-exception"),
            package(self.root, "unreviewed", "1.0.0", None, {"LICENSE": "custom\n"}),
        ]
        with self.assertRaises(ValueError) as caught:
            NOTICES.generate(self.metadata, PROJECT_LICENSE, self.reviewed)
        message = str(caught.exception)
        self.assertIn("isc-only 1.0.0: ships no licence file and has no reviewed upstream notice", message)
        self.assertIn("llvm 1.0.0: ships no licence file", message)
        self.assertIn("unreviewed 1.0.0: no concluded licence", message)

    def test_reviews_are_version_exact(self):
        """A new version of a reviewed crate needs its own review."""
        delta = next(item for item in self.metadata["packages"] if item["name"] == "delta")
        delta["version"] = "0.3.1"
        delta["manifest_path"] = str(self.root / "delta-0.3.1/Cargo.toml")
        with self.assertRaisesRegex(ValueError, "delta 0.3.1: ships no licence file"):
            NOTICES.generate(self.metadata, PROJECT_LICENSE, self.reviewed)

    def test_placeholder_is_never_emitted(self):
        """A template copyright line fails generation and the audit wherever it appears."""
        shipped = package(self.root, "templated", "1.0.0", "MIT",
                          {"LICENSE": f"{TEMPLATE_LINE}\n\nPermission is hereby granted.\n"})
        metadata = dict(self.metadata, packages=self.metadata["packages"] + [shipped])
        with self.assertRaisesRegex(ValueError, "templated 1.0.0: LICENSE contains placeholder"):
            NOTICES.generate(metadata, PROJECT_LICENSE, self.reviewed)

        write(self.reviewed / "delta/LICENSE-MIT", f"{TEMPLATE_LINE}\n")
        manifest = fixture_manifest()
        manifest["notices"][0]["files"][0]["sha256"] = hashlib.sha256(
            f"{TEMPLATE_LINE}\n".encode()).hexdigest()
        with self.assertRaisesRegex(ValueError, "contains placeholder copyright line"):
            self.generate_with(manifest)

        write(self.reviewed / "delta/LICENSE-MIT", DELTA_MIT)
        document = self.generate_with(fixture_manifest())
        broken = document.replace("Copyright (c) 2020 Delta Author", TEMPLATE_LINE)
        with self.assertRaisesRegex(ValueError, "placeholder copyright lines"):
            NOTICES.audit(broken, self.metadata, PROJECT_LICENSE)

    def test_invalid_reviewed_entries(self):
        """Each manifest invariant is enforced and every broken entry is reported."""
        cases = {
            "does not match its reviewed sha256": ("files", 0, "sha256", "0" * 64),
            "must be pinned to a full commit hash": (
                "files", 0, "upstream", "https://example.invalid/delta/blob/main/LICENSE-MIT"),
            "vendored file '../escape' is missing": ("files", 0, "path", "../escape"),
            "evidence must be a non-empty string": (None, None, "evidence", ""),
            "no standard text for 'MIT'": (None, None, "standard_texts", ["MIT"]),
        }
        for message, (field, index, key, value) in cases.items():
            with self.subTest(message=message):
                manifest = fixture_manifest()
                entry = manifest["notices"][0]
                target = entry[field][index] if field else entry
                target[key] = value
                with self.assertRaisesRegex(ValueError, message):
                    self.generate_with(manifest)

        manifest = fixture_manifest()
        manifest["notices"][1]["packages"].append({"name": "delta", "version": "0.3.0"})
        with self.assertRaisesRegex(ValueError, "delta 0.3.0 is reviewed twice"):
            self.generate_with(manifest)

        manifest = fixture_manifest()
        manifest["notices"][0]["license"] = "MIT"
        with self.assertRaisesRegex(ValueError, "reviewed notice covers MIT, but the package declares"):
            self.generate_with(manifest)

    def test_missing_upstream_text_needs_maintainer_exception(self):
        """An entry with no upstream file is rejected unless a maintainer approved it."""
        manifest = fixture_manifest()
        manifest["notices"][1]["files"] = []
        with self.assertRaisesRegex(ValueError, "maintainer_exception must be a non-empty string"):
            self.generate_with(manifest)
        approved = copy.deepcopy(manifest)
        approved["notices"][1]["maintainer_exception"] = "Approved in issue #1 by a maintainer."
        document = self.generate_with(approved)
        self.assertIn("a maintainer-approved exception applies because upstream publishes no "
                      "licence text", document)

    def test_missing_declared_license_file(self):
        """A metadata licence_file that is absent from the source is fatal."""
        (self.root / "checkout/LICENSE.txt").unlink()
        with self.assertRaisesRegex(ValueError, "temporalio-sdk-core 0.5.0: declares license_file"):
            NOTICES.generate(self.metadata, PROJECT_LICENSE, self.reviewed)

    def test_audit_rejects_incomplete_documents(self):
        """The audit detects dropped packages, texts, licences, and headers."""
        document = NOTICES.generate(self.metadata, PROJECT_LICENSE, self.reviewed)
        cases = {
            "packages missing": "\n".join(
                line for line in document.split("\n") if not line.startswith("  - alpha ")),
            "references and texts differ": document.replace("---- T0001 ----", "---- T9999 ----"),
            "project licence": document.replace("Project terms.", "Other terms."),
            "expected header": "x" + document,
        }
        for message, broken in cases.items():
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    NOTICES.audit(broken, self.metadata, PROJECT_LICENSE)
        extra = dict(self.metadata, packages=self.metadata["packages"]
                     + [package(self.root, "late", "1.0.0", "MIT", {"LICENSE": "late\n"})])
        with self.assertRaisesRegex(ValueError, "packages missing from notices: late 1.0.0"):
            NOTICES.audit(document, extra, PROJECT_LICENSE)

    def test_cli_writes_nothing_on_failure(self):
        """The command exits non-zero and leaves no output for an incomplete graph."""
        self.metadata["packages"].append(package(self.root, "isc-only", "1.0.0", "ISC"))
        metadata = self.root / "metadata.json"
        metadata.write_text(json.dumps(self.metadata))
        license_path = self.root / "LICENSE"
        license_path.write_text(PROJECT_LICENSE)
        output = self.root / "THIRD-PARTY-NOTICES.txt"
        command = [sys.executable, str(ROOT / "scripts/generate-third-party-notices.py"),
                   "--metadata", str(metadata), "--project-license", str(license_path),
                   "--reviewed-notices", str(self.reviewed)]
        result = subprocess.run(command + ["--output", str(output)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("isc-only 1.0.0", result.stderr)
        self.assertFalse(output.exists())
        self.metadata["packages"].pop()
        metadata.write_text(json.dumps(self.metadata))
        subprocess.run(command + ["--output", str(output)], check=True, capture_output=True)
        subprocess.run(command + ["--audit", str(output)], check=True, capture_output=True)


if __name__ == "__main__":
    unittest.main()

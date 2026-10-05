"""Exercise the third-party notices generator against small offline fixtures.

The fixtures model the shapes found in the real locked Cargo graph: crates
shipping their own licence files, shared identical texts, the pinned Core
crates whose licence is a workspace file outside the package directory, and
crates that publish no licence file at all. No network or Cargo is needed.
"""

import json
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest

from test_ci_artifacts import NOTICES, ROOT

PROJECT_LICENSE = "Apache License\nVersion 2.0, January 2004\n\nProject terms.\n"
APACHE_TEXT = "Apache License\nShared crate copy of the Apache text.\n"
CORE_SOURCE = NOTICES.LICENSE_POLICY.CORE_SOURCE_PREFIX + "core"


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
        dict(package(root, "temporalio-sdk-core", "0.5.0", None),
             id=CORE_SOURCE, source=CORE_SOURCE, license_file="../../LICENSE.txt",
             manifest_path=str(core_manifest)),
        dict(package(root, "bridge", "0.1.0", "Apache-2.0"), id="path+file:///bridge", source=None),
    ]
    return {"packages": packages, "workspace_members": ["path+file:///bridge"]}


class GenerateTests(unittest.TestCase):
    """The generated document is complete, deduplicated, and reproducible."""

    def setUp(self):
        """Create one fixture tree in a disposable directory."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.metadata = fixture_metadata(self.root / "first")
        self.document = NOTICES.generate(self.metadata, PROJECT_LICENSE)

    def test_deterministic_and_path_independent(self):
        """Another checkout location and package order yield identical bytes."""
        other = fixture_metadata(self.root / "second/deeper")
        random.Random(787).shuffle(other["packages"])
        self.assertEqual(NOTICES.generate(other, PROJECT_LICENSE), self.document)
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

    def test_standard_text_for_crate_without_licence_file(self):
        """A file-less crate gets the first standard text its expression permits."""
        self.assertIn(
            "    No licence file is distributed with this package; the standard text of "
            "Apache-2.0 is reproduced. Copyright holders: Delta Author "
            "<delta@example.invalid> (https://example.invalid/delta).",
            self.document,
        )
        self.assertIn("From: delta 0.3.0 (standard Apache-2.0 text)", self.document)
        standard = (ROOT / "scripts/license-texts/Apache-2.0.txt").read_text()
        self.assertIn(standard.strip(), self.document)


class FailureTests(unittest.TestCase):
    """Missing licence texts fail closed and name every affected package."""

    def setUp(self):
        """Create one fixture tree in a disposable directory."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.metadata = fixture_metadata(self.root)

    def test_missing_texts_are_all_reported(self):
        """No partial document is produced when any package lacks a text."""
        self.metadata["packages"] += [
            package(self.root, "isc-only", "1.0.0", "ISC"),
            package(self.root, "llvm", "1.0.0", "Apache-2.0 WITH LLVM-exception"),
            package(self.root, "unreviewed", "1.0.0", None, {"LICENSE": "custom\n"}),
        ]
        with self.assertRaises(ValueError) as caught:
            NOTICES.generate(self.metadata, PROJECT_LICENSE)
        message = str(caught.exception)
        self.assertIn("isc-only 1.0.0: no licence file in the package and no standard text for ISC", message)
        self.assertIn("llvm 1.0.0: no licence file", message)
        self.assertIn("unreviewed 1.0.0: no concluded licence", message)

    def test_missing_declared_license_file(self):
        """A metadata licence_file that is absent from the source is fatal."""
        (self.root / "checkout/LICENSE.txt").unlink()
        with self.assertRaisesRegex(ValueError, "temporalio-sdk-core 0.5.0: declares license_file"):
            NOTICES.generate(self.metadata, PROJECT_LICENSE)

    def test_audit_rejects_incomplete_documents(self):
        """The audit detects dropped packages, texts, licences, and headers."""
        document = NOTICES.generate(self.metadata, PROJECT_LICENSE)
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
                   "--metadata", str(metadata), "--project-license", str(license_path)]
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

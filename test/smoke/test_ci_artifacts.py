"""Regression tests for matrix coverage and fail-closed artifact transport."""

import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


def load_script(name):
    """Import a command-line helper without invoking its entry point."""
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MATRIX = load_script("ci-matrix")
SMOKE = load_script("smoke-artifact")
RELEASE = load_script("package-release-bridges")
COMMIT = "a" * 40
VERSION = "5.5.1"


class MatrixTests(unittest.TestCase):
    """Keep full release coverage and the smaller presubmit/master budgets."""

    def test_counts_and_required_lanes(self):
        """All tiers retain the branch-protection lanes and unique identities."""
        for tier, count in (("pr", 5), ("master", 10), ("release", 16)):
            with self.subTest(tier=tier):
                matrix = MATRIX.matrices(tier)
                linux = matrix["linux"]["include"]
                native = matrix["native"]["include"]
                self.assertEqual(len(linux) + 2 * len(native), count)
                self.assertEqual(len(linux), len({(x["ocaml"], x["runner"]) for x in linux}))
                for runner in ("ubuntu-24.04", "ubuntu-24.04-arm"):
                    self.assertIn({"ocaml": VERSION, "label": "5.5", "runner": runner}, linux)
                self.assertIn({"ocaml": "5.2.1", "label": "5.2", "runner": "ubuntu-24.04"}, linux)
                for row in linux + native:
                    self.assertIn(row["ocaml"], MATRIX.COMPILERS)
                if tier == "release":
                    self.assertEqual({row["ocaml"] for row in native}, set(MATRIX.COMPILERS))

    def test_unknown_tier_fails(self):
        """Typos must not silently select a cheap, incomplete release matrix."""
        with self.assertRaises(ValueError):
            MATRIX.matrices("unknown")


class SmokeTests(unittest.TestCase):
    """Exercise byte integrity, provenance, and extraction safety without Docker."""

    def setUp(self):
        """Create one synthetic ELF64 executable in a disposable build tree."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "scripts").mkdir()
        self.name = "test/integration/worker.exe"
        (self.root / "scripts/ci-smoke-executables.txt").write_text(self.name + "\n")
        self.binary = self.root / "_build/default" / self.name
        self.binary.parent.mkdir(parents=True)
        self.content = b"\x7fELF\x02\x01" + bytes(12) + b"\x3e\x00" + b"compiled fixture"
        self.binary.write_bytes(self.content)
        self.artifact = self.root / "artifact"
        SMOKE.pack(self.root, self.artifact, COMMIT, VERSION)
        self.binary.unlink()

    def test_round_trip(self):
        """Restored files retain executable permissions and exact contents."""
        SMOKE.unpack(self.root, self.artifact, COMMIT, VERSION)
        self.assertEqual(self.binary.read_bytes(), self.content)
        self.assertTrue(os.access(self.binary, os.X_OK))

    def test_wrong_provenance(self):
        """Neither another commit nor another patch version may be consumed."""
        for commit, version in (("b" * 40, VERSION), (COMMIT, "5.5.0")):
            with self.assertRaisesRegex(ValueError, "provenance"):
                SMOKE.unpack(self.root, self.artifact, commit, version)
        self.assertFalse(self.binary.exists())

    def test_corrupt_archive(self):
        """A transport digest mismatch is fatal, not an Actions warning."""
        with (self.artifact / "executables.tar.gz").open("ab") as stream:
            stream.write(b"corrupt")
        with self.assertRaisesRegex(ValueError, "checksum"):
            SMOKE.unpack(self.root, self.artifact, COMMIT, VERSION)

    def test_unexpected_members_and_missing_binary(self):
        """Even a newly checksummed tar cannot introduce links or traversal."""
        for name, symlink in (("../outside", False), (self.name, True), ("", False)):
            with self.subTest(name=name, symlink=symlink):
                archive = self.artifact / "executables.tar.gz"
                with tarfile.open(archive, "w:gz") as tar:
                    if name:
                        member = tarfile.TarInfo(name)
                        if symlink:
                            member.type, member.linkname = tarfile.SYMTYPE, "../../outside"
                            tar.addfile(member)
                        else:
                            member.size = 1
                            tar.addfile(member, io.BytesIO(b"x"))
                path = self.artifact / "manifest.json"
                manifest = json.loads(path.read_text())
                manifest["archive_sha256"] = SMOKE.digest(archive.read_bytes())
                path.write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    SMOKE.unpack(self.root, self.artifact, COMMIT, VERSION)
                self.assertFalse(self.binary.exists())

    def test_compose_executables_in_compile_list(self):
        """Every Compose worker/driver is present in the one-time compile list."""
        import re
        compose = (ROOT / "test/integration/temporal/compose.yaml").read_text()
        referenced = set(re.findall(r"      - (test/integration/\S+\.exe)", compose))
        self.assertTrue(referenced <= set(SMOKE.targets(ROOT)))
        self.assertNotIn("      - dune\n", compose)

    def test_prebuilt_helpers_never_compile(self):
        """Missing artifacts fail; a valid executable receives its arguments."""
        tools = self.root / "tools"
        tools.mkdir()
        opam = tools / "opam"
        opam.write_text('#!/bin/sh\nprintf "unexpected compiler invocation\\n"\nexit 0\n')
        opam.chmod(0o755)
        env = {**os.environ, "TEMPORAL_PREBUILT_SMOKE": "1", "PATH": f"{tools}:{os.environ['PATH']}"}
        command = ["sh", str(ROOT / "scripts/run-temporal-executable.sh"), "--build-dir=_build/local", self.name]
        self.assertNotEqual(subprocess.run(command, cwd=self.root, env=env, capture_output=True).returncode, 0)
        self.binary.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
        self.binary.chmod(0o755)
        result = subprocess.run(command + ["with spaces", "--flag"], cwd=self.root, env=env, capture_output=True, text=True, check=True)
        self.assertEqual(result.stdout, "with spaces\n--flag\n")
        build = ["sh", str(ROOT / "scripts/build-temporal-executables.sh"), self.name]
        subprocess.run(build, cwd=self.root, env=env, check=True)
        self.binary.unlink()
        self.assertNotEqual(subprocess.run(build, cwd=self.root, env=env, capture_output=True).returncode, 0)


class ReleaseTests(unittest.TestCase):
    """Require complete verified native platform bundles before publication."""

    def setUp(self):
        """Prepare small, checksummed fixtures for all four platform bundles."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for platform in RELEASE.PLATFORMS:
            bundle = self.root / f"rust-bridge-{platform}"
            bundle.mkdir()
            for name in RELEASE.REQUIRED_FILES:
                (bundle / name).write_text("fixture\n")
            (bundle / "platform").write_text(platform + "\n")
            (bundle / "key").write_text(f"rust-bridge-v2-{platform}-release-{'a' * 64}-{'b' * 64}\n")
            if platform == "windows-amd64":
                (bundle / "import-libs").mkdir()
                (bundle / "import-libs/libexample.a").write_bytes(b"import library")
            self.checksums(bundle)

    def checksums(self, bundle):
        """Refresh checksums to distinguish validation errors from corruption."""
        (bundle / "SHA256SUMS").write_text("".join(
            f"{RELEASE.sha256(path)}  {path.relative_to(bundle)}\n"
            for path in sorted(bundle.rglob("*")) if path.is_file() and path.name != "SHA256SUMS"
        ))

    def test_complete_release(self):
        """Publication packages four versioned archives with exact provenance."""
        output = self.root / "assets"
        RELEASE.package_bridges(self.root, output, "v0.1.0-rc.1", COMMIT)
        manifest = json.loads((output / "manifest.json").read_text())
        self.assertEqual(manifest["commit"], COMMIT)
        self.assertEqual(set(manifest["bridges"]), set(RELEASE.PLATFORMS))
        for platform, entry in manifest["bridges"].items():
            self.assertEqual(RELEASE.sha256(output / entry["asset"]), entry["sha256"])

    def test_corrupt_or_nonrelease_bundle(self):
        """Library corruption and debug profiles cannot be shipped as releases."""
        bundle = self.root / "rust-bridge-linux-arm64"
        (bundle / "bridge.dynamic").write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "checksum"):
            RELEASE.validate_bundle(bundle, "linux-arm64")
        self.checksums(bundle)
        path = bundle / "key"
        path.write_text(path.read_text().replace("-release-", "-ci-"))
        self.checksums(bundle)
        with self.assertRaisesRegex(ValueError, "release bridge"):
            RELEASE.validate_bundle(bundle, "linux-arm64")

    def test_missing_windows_import_libraries(self):
        """A GNU archive without its import-library closure cannot be linked."""
        bundle = self.root / "rust-bridge-windows-amd64"
        (bundle / "import-libs/libexample.a").unlink()
        self.checksums(bundle)
        with self.assertRaisesRegex(ValueError, "import libraries"):
            RELEASE.validate_bundle(bundle, "windows-amd64")


if __name__ == "__main__":
    unittest.main()

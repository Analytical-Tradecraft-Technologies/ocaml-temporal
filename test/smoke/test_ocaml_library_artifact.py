"""Verify binary SDK transport, ABI rejection, and complete release coverage."""

import io
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest

from test_ci_artifacts import COMMIT, MATRIX, RELEASE, VERSION, load_script

ARTIFACT = load_script("ocaml-library-artifact")
PLATFORM = "linux-arm64"


def abi(ocaml=VERSION, platform=PLATFORM):
    """Provide distinct compiler and dependency identities for small fixtures."""
    system = {"linux-arm64": "linux", "linux-amd64": "linux",
              "macos-arm64": "macosx", "windows-amd64": "mingw64"}[platform]
    arch = "arm64" if platform.endswith("arm64") else "amd64"
    return (f"version: {ocaml}\narchitecture: {arch}\nsystem: {system}\n"
            "cmi_magic_number: fixture\ndune: 3.24.2\n"
            + "".join(f"dependency: {name} fixture\nCRC of implementation: abc123\n"
                      for name in ("stdlib", "unix", "threads", "logs", "yojson")))


def bridge_key(platform):
    """Match the independently validated Rust release fixture identities."""
    return f"rust-bridge-v2-{platform}-release-{'a' * 64}-{'b' * 64}"


def stage_sdk(stage, ocaml=VERSION, platform=PLATFORM):
    """Stage a synthetic installed SDK, including one source file to omit."""
    for name in ARTIFACT.REQUIRED | {ARTIFACT.PACKAGE + "temporal.ml"}:
        path = stage / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"compiled fixture")
    (stage / "environment.txt").write_text(abi(ocaml, platform))
    if platform == "windows-amd64":
        path = stage / ARTIFACT.PACKAGE / "__private__/temporal_core_bridge/rust-imports/libwinapi_ntdll.a"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"import library")


class LibraryArtifactTests(unittest.TestCase):
    """Accept only relocatable, intact libraries built for the consumer ABI."""

    def setUp(self):
        """Prepare an artifact independently from its eventual install prefix."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.stage = self.root / "stage"
        self.bundle = self.root / "bundle"
        self.prefix = self.root / "relocated sdk"
        stage_sdk(self.stage)
        ARTIFACT.pack(self.stage, self.bundle, COMMIT, VERSION, PLATFORM, "ci", "fixture-key")
        self.env = self.root / "environment.txt"
        self.env.write_text(abi())

    def unpack(self, **overrides):
        """Supply expected provenance from the caller rather than the bundle."""
        args = dict(directory=self.bundle, prefix=self.prefix, env=self.env,
                    commit=COMMIT, ocaml=VERSION, platform=PLATFORM, profile="ci")
        ARTIFACT.unpack(**(args | overrides))

    def test_source_free_relocation(self):
        """The original install tree is not needed to consume compiled files."""
        shutil.rmtree(self.stage)
        self.unpack()
        for name in ARTIFACT.REQUIRED:
            self.assertEqual((self.prefix / name).read_bytes(), b"compiled fixture")
        self.assertFalse(list(self.prefix.rglob("*.ml")))
        with self.assertRaisesRegex(ValueError, "prefix already exists"):
            self.unpack()

    def test_provenance_mismatch(self):
        """Wrong commit, patch, platform and profile all fail before installation."""
        for override in ({"commit": "b" * 40}, {"ocaml": "5.5.0"},
                         {"platform": "linux-amd64"}, {"profile": "release"}):
            with self.subTest(override=override), self.assertRaisesRegex(ValueError, "provenance"):
                self.unpack(**override)
        self.assertFalse(self.prefix.exists())

    def test_dependency_abi_mismatch_and_crlf(self):
        """Equal package versions cannot hide different native implementation CRCs."""
        self.env.write_text(abi().replace("abc123", "def456"))
        with self.assertRaisesRegex(ValueError, "dependency ABI mismatch"):
            self.unpack()
        self.assertFalse(self.prefix.exists())
        self.env.write_bytes(abi().replace("\n", "\r\n").encode())
        self.unpack()

    def test_corruption(self):
        """An archive transport digest mismatch cannot become a working prefix."""
        with (self.bundle / "library.tar.gz").open("ab") as stream:
            stream.write(b"corrupt")
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.unpack()
        self.assertFalse(self.prefix.exists())

    def test_unsafe_or_incomplete_archive(self):
        """Rechecksummed archives still cannot introduce links, paths or duplicates."""
        name = next(iter(ARTIFACT.REQUIRED))
        for names, symlink in ((["../outside"], False), ([name], True),
                               ([name, name], False), ([], False)):
            with self.subTest(names=names, symlink=symlink):
                archive = self.bundle / "library.tar.gz"
                with tarfile.open(archive, "w:gz") as tar:
                    for entry in names:
                        member = tarfile.TarInfo(entry)
                        if symlink:
                            member.type, member.linkname = tarfile.SYMTYPE, "../../outside"
                            tar.addfile(member)
                        else:
                            member.size = len(b"compiled fixture")
                            tar.addfile(member, io.BytesIO(b"compiled fixture"))
                manifest_path = self.bundle / "manifest.json"
                manifest = json.loads(manifest_path.read_text())
                manifest["archive_sha256"] = ARTIFACT.sha256(archive)
                manifest_path.write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    self.unpack()
                self.assertFalse(self.prefix.exists())

    def test_missing_libraries_or_mislabeled_compiler(self):
        """Packaging fails if the installation cannot represent its claimed SDK."""
        (self.stage / ARTIFACT.PACKAGE / "temporal.cmxa").unlink()
        with self.assertRaisesRegex(ValueError, "missing installed"):
            ARTIFACT.pack(self.stage, self.root / "bad", COMMIT, VERSION, PLATFORM, "ci", "key")
        with self.assertRaisesRegex(ValueError, "version/platform"):
            ARTIFACT.check_environment(abi(), "5.5.0", PLATFORM)
        with self.assertRaisesRegex(ValueError, "MinGW import"):
            ARTIFACT.check_files(dict.fromkeys(ARTIFACT.REQUIRED), "windows-amd64")


class LibraryReleaseTests(unittest.TestCase):
    """Publish only the complete tested compiler matrix with matching Rust input."""

    def setUp(self):
        """Build tiny SDK fixtures for all sixteen supported combinations."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.output = self.root / "assets"
        self.output.mkdir()
        self.tag = "v0.1.0-rc.1"
        (self.output / "manifest.json").write_text(json.dumps({
            "tag": self.tag, "commit": COMMIT, "profile": "release",
            "bridges": {platform: {"key": bridge_key(platform)} for platform in ARTIFACT.PLATFORMS},
        }))
        for platform in ARTIFACT.PLATFORMS:
            for ocaml in MATRIX.COMPILERS:
                stage = self.root / f"stage-{platform}-{ocaml}"
                stage_sdk(stage, ocaml, platform)
                bundle = self.root / f"ocaml-sdk-{platform}-ocaml-{ocaml}"
                ARTIFACT.pack(stage, bundle, COMMIT, ocaml, platform, "release", bridge_key(platform))

    def test_complete_release(self):
        """Every compiler/platform yields a checksummed archive with install tools."""
        RELEASE.package_libraries(self.root, self.output, self.tag, COMMIT)
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(len(manifest["ocaml_libraries"]), 16)
        for entry in manifest["ocaml_libraries"].values():
            archive = self.output / entry["asset"]
            self.assertEqual(ARTIFACT.sha256(archive), entry["sha256"])
            with tarfile.open(archive) as tar:
                self.assertEqual(set(tar.getnames()), {"manifest.json", "library.tar.gz", *ARTIFACT.TOOLS})

    def test_missing_combination(self):
        """A release cannot silently omit an older compiler or desktop platform."""
        shutil.rmtree(self.root / "ocaml-sdk-windows-amd64-ocaml-5.2.1")
        with self.assertRaises(FileNotFoundError):
            RELEASE.package_libraries(self.root, self.output, self.tag, COMMIT)
        self.assertNotIn("ocaml_libraries", json.loads((self.output / "manifest.json").read_text()))

    def test_different_rust_input(self):
        """An SDK built against another bridge must not accompany this release."""
        path = self.root / "ocaml-sdk-linux-arm64-ocaml-5.5.1/manifest.json"
        manifest = json.loads(path.read_text())
        manifest["rust_bridge_key"] = "another-bridge"
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "Rust bridge differ"):
            RELEASE.package_libraries(self.root, self.output, self.tag, COMMIT)


if __name__ == "__main__":
    unittest.main()

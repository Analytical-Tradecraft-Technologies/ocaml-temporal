#!/usr/bin/env python3
"""Package and consume a compiled SDK without copying Dune's build cache.

Bundles contain the installed OCaml archives/interfaces, private C/Rust native
archives and metadata. Exact compiler/compiled-dependency identities are checked
before an installation is exposed. A caller supplies provenance independently.
"""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import tarfile
import tempfile

PLATFORMS = ("linux-amd64", "linux-arm64", "macos-arm64", "windows-amd64")
TOOLS = ("ocaml-library-artifact.py", "ocaml-library-environment.sh")
PACKAGE = "lib/temporal-sdk/"
REQUIRED = {
    PACKAGE + "META", PACKAGE + "dune-package", PACKAGE + "temporal.cmi",
    PACKAGE + "temporal.cmxa", PACKAGE + "temporal.a",
    PACKAGE + "__private__/temporal_core_bridge/libtemporal_native_stubs.a",
    PACKAGE + "__private__/temporal_core_bridge/libocaml_temporal_core_bridge.a",
}


def sha256(path):
    """Stream large native archives rather than retaining them in memory."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def safe_name(name):
    """Accept only normalized relative file names below the install prefix."""
    path = PurePosixPath(name)
    return (bool(name) and not path.is_absolute() and path.as_posix() == name
            and all(part not in (".", "..") for part in path.parts)
            and re.fullmatch(r"[A-Za-z0-9_./+~-]+", name) is not None
            and path.parts[0] in ("lib", "doc", "share"))


def environment(path):
    """Normalize shell line endings while retaining every ABI fingerprint."""
    value = path.read_text().replace("\r", "")
    for marker in ("version: ", "architecture: ", "system: ", "cmi_magic_number: ", "dune: ",
                   "dependency: stdlib ", "dependency: unix ", "dependency: threads ",
                   "dependency: logs ", "dependency: yojson ", "CRC of implementation:"):
        if marker not in value:
            raise ValueError(f"incomplete compiler/dependency environment: {marker}")
    return value


def check_files(files, platform):
    """Require linkable SDK archives and the platform's native import closure."""
    if (not REQUIRED <= files.keys()
            or any(not safe_name(name) or name.endswith(".ml") for name in files)):
        raise ValueError("invalid or missing installed OCaml/native library files")
    if platform == "windows-amd64" and not any(
            name.startswith(PACKAGE + "__private__/temporal_core_bridge/rust-imports/libwinapi_")
            and name.endswith(".a") for name in files):
        raise ValueError("Windows SDK is missing its MinGW import libraries")


def check_environment(value, ocaml, platform):
    """Reject a mislabeled patch, CPU, or operating system before packaging."""
    config = dict(line.split(": ", 1) for line in value.splitlines() if ": " in line)
    expected_arch = "arm64" if platform.endswith("arm64") else "amd64"
    expected_system = {"linux-amd64": "linux", "linux-arm64": "linux",
                       "macos-arm64": "macosx", "windows-amd64": "mingw64"}[platform]
    if (config.get("version") != ocaml or config.get("architecture") != expected_arch
            or config.get("system") != expected_system):
        raise ValueError("compiler version/platform differs from artifact identity")


def pack(stage, output, commit, ocaml, platform, profile, bridge_key):
    """Archive an already-tested installation, excluding implementation sources."""
    abi = environment(stage / "environment.txt")
    check_environment(abi, ocaml, platform)
    files = {}
    for path in sorted(stage.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"install stage contains a symlink: {path}")
        if not path.is_file() or path.name == "environment.txt" or path.suffix == ".ml":
            continue
        name = path.relative_to(stage).as_posix()
        if not safe_name(name):
            raise ValueError(f"unexpected install path: {name}")
        files[name] = sha256(path)
    check_files(files, platform)
    output.mkdir(parents=True, exist_ok=False)
    archive = output / "library.tar.gz"
    with tarfile.open(archive, "w:gz", compresslevel=1) as tar:
        for name in files:
            tar.add(stage / name, arcname=name, recursive=False)
    manifest = {
        "schema": 1, "commit": commit, "ocaml": ocaml, "platform": platform,
        "profile": profile, "rust_bridge_key": bridge_key,
        "environment": abi, "files": files, "archive_sha256": sha256(archive),
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for name in TOOLS:
        shutil.copyfile(Path(__file__).with_name(name), output / name)


def validate(directory, commit, ocaml, platform, profile):
    """Check complete provenance and every archive member without extracting."""
    manifest = json.loads((directory / "manifest.json").read_text())
    expected = {"schema": 1, "commit": commit, "ocaml": ocaml,
                "platform": platform, "profile": profile}
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise ValueError("OCaml library artifact provenance mismatch")
    check_environment(manifest["environment"], ocaml, platform)
    files = manifest["files"]
    check_files(files, platform)
    archive = directory / "library.tar.gz"
    if sha256(archive) != manifest["archive_sha256"]:
        raise ValueError("OCaml library archive checksum mismatch")
    found = set()
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar:
            if member.name not in files or member.name in found or not member.isfile():
                raise ValueError(f"unexpected OCaml library archive member: {member.name}")
            stream = tar.extractfile(member)
            if hashlib.file_digest(stream, "sha256").hexdigest() != files[member.name]:
                raise ValueError(f"OCaml library file checksum mismatch: {member.name}")
            found.add(member.name)
    if found != files.keys():
        raise ValueError("missing OCaml library archive member")
    return manifest


def unpack(directory, prefix, env, commit, ocaml, platform, profile):
    """Expose a relocated installation only after ABI and byte checks pass."""
    manifest = validate(directory, commit, ocaml, platform, profile)
    if environment(env) != manifest["environment"]:
        raise ValueError("OCaml compiler or compiled dependency ABI mismatch; use matching dependencies or build from source")
    if prefix.exists() or prefix.is_symlink():
        raise ValueError("installation prefix already exists")
    prefix.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=prefix.parent) as temporary:
        stage = Path(temporary) / "sdk"
        stage.mkdir()
        with tarfile.open(directory / "library.tar.gz", "r:gz") as tar:
            for member in tar:
                path = stage / member.name
                path.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(member) as source, path.open("wb") as target:
                    shutil.copyfileobj(source, target)
                path.chmod(0o755 if member.mode & 0o111 else 0o644)
        stage.rename(prefix)


def prepare_consumer(root, destination):
    """Create an independent application fixture with no SDK build inputs."""
    destination.mkdir(parents=True, exist_ok=False)
    for name in ("dune", "dune-project", "main.ml", "public_api.ml", "worker_environment.ml"):
        shutil.copyfile(root / "test/fixtures/install-consumer" / name, destination / name)
    shutil.copyfile(root / "scripts/ocaml-library-environment.sh", destination / "environment.sh")
    shutil.copyfile(root / "test/bridge/test_prebuilt_library.sh", destination / "test.sh")


def main():
    """Expose packaging, installation and the independent consumer fixture."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("pack", "unpack", "prepare-consumer"))
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--stage", type=Path)
    parser.add_argument("--prefix", type=Path)
    parser.add_argument("--environment", type=Path)
    parser.add_argument("--commit")
    parser.add_argument("--ocaml")
    parser.add_argument("--platform", choices=PLATFORMS)
    parser.add_argument("--profile", choices=("ci", "release"), default="ci")
    parser.add_argument("--bridge-key")
    args = parser.parse_args()
    if args.operation == "prepare-consumer":
        prepare_consumer(Path.cwd(), args.directory)
        return
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit or "") or not re.fullmatch(r"5\.\d+\.\d+", args.ocaml or "") or not args.platform:
        parser.error("exact commit, OCaml patch and platform are required")
    if args.operation == "pack":
        if not args.stage or not args.bridge_key:
            parser.error("stage and Rust bridge key are required")
        pack(args.stage, args.directory, args.commit, args.ocaml, args.platform, args.profile, args.bridge_key)
    else:
        if not args.prefix or not args.environment:
            parser.error("prefix and current compiler/dependency environment are required")
        unpack(args.directory, args.prefix, args.environment, args.commit, args.ocaml, args.platform, args.profile)


if __name__ == "__main__":
    main()

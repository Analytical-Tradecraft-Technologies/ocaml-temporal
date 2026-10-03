#!/usr/bin/env python3
"""Transfer compiled Linux x64 smoke programs without transferring Dune state.

Only the executable allowlist is accepted. Provenance and the archive digest
are checked before extraction; links, traversal, extra or missing members fail.
"""

import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import tarfile


# This file also supplies Make's compile targets, keeping producer and consumer
# coverage identical as live scenarios acquire new executables.
def targets(root: Path) -> list[str]:
    """Read unique repository-relative executable paths from the shared list."""
    names = (root / "scripts/ci-smoke-executables.txt").read_text().splitlines()
    if not names or len(names) != len(set(names)):
        raise ValueError("empty or duplicate smoke executable list")
    for name in names:
        if not re.fullmatch(r"test/integration/[a-zA-Z0-9_/]+\.exe", name):
            raise ValueError(f"invalid executable path: {name}")
    return names


def digest(data: bytes) -> str:
    """Return the transport digest for an archive or executable."""
    return hashlib.sha256(data).hexdigest()


def pack(root: Path, directory: Path, commit: str, ocaml: str) -> None:
    """Archive final Linux x64 executables and record exact build provenance."""
    directory.mkdir(parents=True, exist_ok=True)
    files = {}
    archive = directory / "executables.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name in targets(root):
            data = (root / "_build/default" / name).read_bytes()
            # ELF64 little-endian x86_64: don't publish an ARM/local test build
            # under the artifact identity used by the Linux amd64 smoke job.
            if data[:6] != b"\x7fELF\x02\x01" or data[18:20] != b"\x3e\x00":
                raise ValueError(f"not a Linux x64 executable: {name}")
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), 0o755
            tar.addfile(info, io.BytesIO(data))
            files[name] = digest(data)
    manifest = {
        "schema": 1, "commit": commit, "ocaml": ocaml,
        "platform": "linux-amd64", "files": files,
        "archive_sha256": digest(archive.read_bytes()),
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def unpack(root: Path, directory: Path, commit: str, ocaml: str) -> None:
    """Validate all bytes and destinations before making any executable usable."""
    manifest = json.loads((directory / "manifest.json").read_text())
    expected = {"schema": 1, "commit": commit, "ocaml": ocaml, "platform": "linux-amd64"}
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise ValueError("smoke artifact provenance mismatch")
    names = targets(root)
    if set(manifest["files"]) != set(names):
        raise ValueError("smoke executable list mismatch")
    data = (directory / "executables.tar.gz").read_bytes()
    if digest(data) != manifest["archive_sha256"]:
        raise ValueError("smoke archive checksum mismatch")
    verified = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        for member in tar:
            if member.name not in names or member.name in verified or not member.isfile():
                raise ValueError(f"unexpected smoke archive member: {member.name}")
            stream = tar.extractfile(member)
            assert stream is not None
            content = stream.read()
            if digest(content) != manifest["files"][member.name]:
                raise ValueError(f"smoke executable checksum mismatch: {member.name}")
            verified[member.name] = content
    if set(verified) != set(names):
        raise ValueError("missing smoke executable")
    destination = root / "_build/default"
    for name in names:
        path = destination / name
        # Checkout is trusted, but never follow a pre-existing symlink in the
        # extraction tree (including _build itself) on a reused local runner.
        if any(parent.is_symlink() for parent in (path, *path.parents) if parent != root.parent):
            raise ValueError(f"symlink in smoke destination: {path}")
    for name, content in verified.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        path.chmod(0o755)


def main() -> None:
    """Dispatch the producer/consumer operation with explicit tested identity."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("pack", "unpack"))
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--ocaml", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        parser.error("expected a complete commit SHA")
    if args.ocaml != "5.5.1":
        parser.error("smoke lane requires OCaml 5.5.1")
    globals()[args.operation](Path.cwd(), args.directory, args.commit, args.ocaml)


if __name__ == "__main__":
    main()

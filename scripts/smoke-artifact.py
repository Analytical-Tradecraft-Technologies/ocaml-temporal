#!/usr/bin/env python3
"""Transfer compiled Linux x64 smoke programs without transferring Dune state.

Only the executable allowlist is accepted. Provenance and the archive digest
are checked before extraction; links, traversal, extra or missing members fail.

The archive is a tar stream compressed by the `zstd` command (multi-threaded,
level 3, 1 GiB long-distance window). The 44 executables are about 218 MB each
and each embeds the same statically linked Rust bridge, so the long window
stores that shared code roughly once: 8.2 GB packs to about 0.9 GB in about 25
seconds on a standard runner. Python's default gzip level 9 took 16 minutes for
a 1.9 GB archive and was most of the smoke-build job. GitHub-hosted Ubuntu
runners ship `zstd`; a local run of `make test-ci-artifacts` needs it too.
"""

import argparse
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
from typing import Iterator

ARCHIVE = "executables.tar.zst"
# The decompressor must be given the same window (--long=30, 1 GiB) to accept
# the archive; it uses about that much memory while unpacking.
ZSTD_COMPRESS = ("zstd", "-q", "-T0", "-3", "--long=30", "-f", "-o")
ZSTD_DECOMPRESS = ("zstd", "-q", "-d", "--long=30", "-c")
CHUNK = 8 * 1024 * 1024


# This file also supplies Make's compile targets, keeping producer and consumer
# coverage identical as live scenarios acquire new executables.
def targets(root: Path) -> list[str]:
    """Read unique repository-relative executable paths from the shared list."""
    names = (root / "scripts/ci-smoke-executables.txt").read_text().splitlines()
    if not names or len(names) != len(set(names)):
        raise ValueError("empty or duplicate smoke executable list")
    for name in names:
        if not re.fullmatch(r"(?:test/integration|examples)/[a-zA-Z0-9_/]+\.exe", name):
            raise ValueError(f"invalid executable path: {name}")
    return names


def digest(data: bytes) -> str:
    """Return the transport digest for an in-memory executable."""
    return hashlib.sha256(data).hexdigest()


def file_digest(path: Path) -> str:
    """Return the transport digest of a file without loading it whole."""
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK):
            hasher.update(chunk)
    return hasher.hexdigest()


def _zstd(arguments: tuple[str, ...], **kwargs) -> subprocess.Popen:
    """Start the zstd command, naming the missing tool instead of a bare OSError."""
    try:
        return subprocess.Popen(arguments, **kwargs)
    except FileNotFoundError as error:
        raise RuntimeError("the zstd command is required to pack or unpack smoke executables") from error


@contextlib.contextmanager
def archive_writer(archive: Path) -> Iterator[tarfile.TarFile]:
    """Yield a streaming tar writer whose output zstd compresses into `archive`.

    The tar stream is never materialized: members flow through the pipe as
    they are added. A zstd failure raises after the writer is closed.
    """
    process = _zstd((*ZSTD_COMPRESS, str(archive)), stdin=subprocess.PIPE)
    assert process.stdin is not None
    try:
        with tarfile.open(fileobj=process.stdin, mode="w|") as tar:
            yield tar
    finally:
        process.stdin.close()
        status = process.wait()
    if status != 0:
        raise RuntimeError(f"zstd failed to write {archive} (exit {status})")


def pack(root: Path, directory: Path, commit: str, ocaml: str) -> None:
    """Archive final Linux x64 executables and record exact build provenance."""
    directory.mkdir(parents=True, exist_ok=True)
    files = {}
    archive = directory / ARCHIVE
    with archive_writer(archive) as tar:
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
        "schema": 2, "commit": commit, "ocaml": ocaml,
        "platform": "linux-amd64", "files": files,
        "archive_sha256": file_digest(archive),
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def _reject_symlinks(root: Path, path: Path) -> None:
    """Fail if any component of `path` below the checkout root is a symlink.

    Checkout is trusted, but never follow a pre-existing symlink in the
    extraction tree (including _build itself) on a reused local runner. Only
    components below the checkout root are checked: the root's own ancestors
    may legitimately be symlinks, such as macOS /var and /tmp.
    """
    current = root
    for part in path.relative_to(root).parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"symlink in smoke destination: {path}")


def unpack(root: Path, directory: Path, commit: str, ocaml: str) -> None:
    """Validate all bytes and destinations before making any executable usable.

    Members are verified one at a time into a staging directory, so memory
    stays at one executable rather than the whole 8 GB set; only when every
    member has matched its manifest digest are they moved into _build/default.
    A failure removes the staging directory and leaves no executable behind.
    """
    manifest = json.loads((directory / "manifest.json").read_text())
    expected = {"schema": 2, "commit": commit, "ocaml": ocaml, "platform": "linux-amd64"}
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise ValueError("smoke artifact provenance mismatch")
    names = targets(root)
    if set(manifest["files"]) != set(names):
        raise ValueError("smoke executable list mismatch")
    archive = directory / ARCHIVE
    if file_digest(archive) != manifest["archive_sha256"]:
        raise ValueError("smoke archive checksum mismatch")
    destination = root / "_build/default"
    staging = root / "_build/.smoke-unpack"
    for name in names:
        _reject_symlinks(root, destination / name)
    _reject_symlinks(root, staging)
    shutil.rmtree(staging, ignore_errors=True)
    try:
        verified = set()
        process = _zstd((*ZSTD_DECOMPRESS, str(archive)), stdout=subprocess.PIPE)
        assert process.stdout is not None
        try:
            try:
                with tarfile.open(fileobj=process.stdout, mode="r|") as tar:
                    for member in tar:
                        if member.name not in names or member.name in verified or not member.isfile():
                            raise ValueError(f"unexpected smoke archive member: {member.name}")
                        stream = tar.extractfile(member)
                        assert stream is not None
                        content = stream.read()
                        if digest(content) != manifest["files"][member.name]:
                            raise ValueError(f"smoke executable checksum mismatch: {member.name}")
                        path = staging / member.name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(content)
                        path.chmod(0o755)
                        verified.add(member.name)
            except tarfile.TarError as error:
                # A digest-valid archive that zstd or tarfile still rejects
                # (for example, a hand-built bad stream) is a transport error.
                raise ValueError(f"smoke archive is not a valid tar stream: {error}") from error
            # Drain the tar padding after the end marker so zstd can exit.
            while process.stdout.read(CHUNK):
                pass
        finally:
            process.stdout.close()
            status = process.wait()
        if status != 0:
            raise ValueError(f"smoke archive does not decompress (zstd exit {status})")
        if verified != set(names):
            raise ValueError("missing smoke executable")
        for name in names:
            path = destination / name
            path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staging / name, path)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


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

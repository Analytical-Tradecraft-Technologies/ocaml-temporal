#!/usr/bin/env python3
"""Validate platform bundles and prepare portable, versioned release archives."""

import argparse
import hashlib
import importlib.util
import json
import re
import tarfile
from pathlib import Path


# Rust-only bundles remain useful for compilers outside the published SDK matrix.
PLATFORMS = {
    "linux-amd64": "x86_64-unknown-linux-gnu; Debian 12/glibc",
    "linux-arm64": "aarch64-unknown-linux-gnu; Debian 12/glibc",
    "macos-arm64": "aarch64-apple-darwin",
    "windows-amd64": "x86_64-pc-windows-gnu; GNU/MinGW",
}
REQUIRED_FILES = {
    "libocaml_temporal_core_bridge.a", "bridge.dynamic", "native-static-libs",
    "key", "platform",
}


def sha256(path):
    """Hash a library without loading the entire static archive into memory."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate_bundle(bundle, platform):
    """Reject incomplete, corrupt, wrong-platform, or unoptimized bundles."""
    if (bundle / "platform").read_text().strip() != platform:
        raise ValueError(f"wrong platform in {bundle}")
    key = (bundle / "key").read_text().strip()
    if not re.fullmatch(rf"rust-bridge-v2-{platform}-release-[0-9a-f]{{64}}-[0-9a-f]{{64}}", key):
        raise ValueError(f"not a release bridge identity: {key}")
    verified = set()
    for line in (bundle / "SHA256SUMS").read_text().splitlines():
        digest, filename = line.split(maxsplit=1)
        filename = filename.removeprefix("*")
        if filename in verified or not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", filename):
            raise ValueError(f"invalid or duplicate bundle path: {filename}")
        if any(part in (".", "..") for part in Path(filename).parts):
            raise ValueError(f"invalid bundle path: {filename}")
        path = bundle / filename
        if not path.resolve().is_relative_to(bundle.resolve()) or path.is_symlink():
            raise ValueError(f"invalid bundle path: {filename}")
        if not re.fullmatch(r"[0-9a-f]{64}", digest) or sha256(path) != digest:
            raise ValueError(f"checksum mismatch: {filename}")
        if path.stat().st_size == 0:
            raise ValueError(f"empty bundle file: {filename}")
        verified.add(filename)
    if not REQUIRED_FILES <= verified:
        raise ValueError(f"missing checksummed libraries or metadata in {bundle}")
    if platform == "windows-amd64" and not any(name.startswith("import-libs/") for name in verified):
        raise ValueError("Windows bridge is missing its MinGW import libraries")
    if any(path.is_symlink() for path in bundle.rglob("*")):
        raise ValueError(f"symlink in bundle: {bundle}")
    actual = {str(path.relative_to(bundle)) for path in bundle.rglob("*") if path.is_file()}
    if actual != verified | {"SHA256SUMS"}:
        raise ValueError(f"unchecksummed files in {bundle}")
    return key


def package_bridges(bundles, output, tag, commit):
    """Require all supported platforms before creating the release manifest."""
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[-~][A-Za-z0-9.-]+)?", tag):
        raise ValueError("invalid release tag")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("release source must be an exact commit")
    checked = {
        platform: validate_bundle(bundles / f"rust-bridge-{platform}", platform)
        for platform in PLATFORMS
    }
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"tag": tag, "commit": commit, "profile": "release", "bridges": {}}
    for platform, key in checked.items():
        archive = output / f"ocaml-temporal-bridge-{tag}-{platform}.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(bundles / f"rust-bridge-{platform}", arcname="bridge")
        manifest["bridges"][platform] = {
            "asset": archive.name, "sha256": sha256(archive), "key": key,
            "target": PLATFORMS[platform],
        }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def package_libraries(bundles, output, tag, commit):
    """Require all sixteen tested SDKs and their matching released Rust inputs."""
    spec = importlib.util.spec_from_file_location("ocaml_artifact", Path(__file__).with_name("ocaml-library-artifact.py"))
    artifact = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(artifact)
    spec = importlib.util.spec_from_file_location("ci_matrix", Path(__file__).with_name("ci-matrix.py"))
    matrix = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(matrix)
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    libraries = {}
    for platform in PLATFORMS:
        for ocaml in matrix.COMPILERS:
            identity = f"{platform}-ocaml-{ocaml}"
            bundle = bundles / f"ocaml-sdk-{identity}"
            metadata = artifact.validate(bundle, commit, ocaml, platform, "release")
            if metadata["rust_bridge_key"] != manifest["bridges"][platform]["key"]:
                raise ValueError(f"SDK and released Rust bridge differ: {identity}")
            archive = output / f"ocaml-temporal-sdk-{tag}-{identity}.tar.gz"
            with tarfile.open(archive, "w:gz", compresslevel=1) as tar:
                for name in ("library.tar.gz", "manifest.json", *artifact.TOOLS):
                    tar.add(bundle / name, arcname=name, recursive=False)
            libraries[identity] = {"asset": archive.name, "sha256": sha256(archive),
                                   "ocaml": ocaml, "platform": platform}
    manifest["ocaml_libraries"] = libraries
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


def main():
    """Read paths and the independently supplied release identity from CI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundles", required=True, type=Path)
    parser.add_argument("--ocaml-bundles", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    package_bridges(args.bundles, args.output, args.tag, args.commit)
    package_libraries(args.ocaml_bundles, args.output, args.tag, args.commit)


if __name__ == "__main__":
    main()

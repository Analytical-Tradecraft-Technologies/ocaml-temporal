#!/usr/bin/env python3
"""Validate platform bundles and prepare portable, versioned release archives."""

import argparse
import hashlib
import importlib.util
import json
import re
import sys
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
# Licence files placed at the top level of every release archive, beside the
# bridge directory or SDK bundle members, so redistributing any one archive
# carries the project licence and the statically linked third-party notices.
LICENSE_NAME = "LICENSE"
NOTICES_NAME = "THIRD-PARTY-NOTICES.txt"


def load_script(name):
    """Import a sibling helper whose file name contains hyphens."""
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), Path(__file__).with_name(f"{name}.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


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


def legal_files(license_path, notices_path):
    """Validate the licence inputs and return them keyed by archive name.

    The notices file must come from scripts/generate-third-party-notices.py
    (whose own audit proves completeness against the Cargo graph) and must
    embed this project's licence, so a stale or unrelated file is rejected.
    """
    notices = load_script("generate-third-party-notices")
    license_text = license_path.read_text(encoding="utf-8")
    if "Apache License" not in license_text:
        raise ValueError(f"project licence is not Apache-2.0: {license_path}")
    document = notices_path.read_bytes().decode("utf-8")
    if not document.startswith(notices.HEADER + "\n"):
        raise ValueError(f"not a generated third-party notices file: {notices_path}")
    if notices.normalize_text(license_text.encode("utf-8")).rstrip("\n") not in document:
        raise ValueError("third-party notices do not embed the project licence")
    return {LICENSE_NAME: license_path, NOTICES_NAME: notices_path}


def add_legal_files(tar, legal):
    """Add the licence files at the archive root in a fixed order."""
    for name in (LICENSE_NAME, NOTICES_NAME):
        tar.add(legal[name], arcname=name, recursive=False)


def verify_legal_files(archive, legal):
    """Re-read an archive and require byte-identical licence files at its root."""
    with tarfile.open(archive, "r:gz") as tar:
        for name, path in legal.items():
            try:
                member = tar.getmember(name)
            except KeyError:
                raise ValueError(f"{archive.name} is missing {name}") from None
            stream = tar.extractfile(member)
            if stream is None or stream.read() != path.read_bytes():
                raise ValueError(f"{archive.name} contains a different {name}")


def package_bridges(bundles, output, tag, commit, legal):
    """Require all supported platforms before creating the release manifest."""
    # Git refnames cannot contain "~", so prerelease tags use the SemVer hyphen
    # (v1.0.0-beta.1) even though OPAM records the version as 1.0.0~beta.1.
    if "~" in tag:
        raise ValueError("invalid release tag: Git tags cannot contain '~'; use the hyphen spelling")
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?", tag):
        raise ValueError("invalid release tag")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("release source must be an exact commit")
    checked = {
        platform: validate_bundle(bundles / f"rust-bridge-{platform}", platform)
        for platform in PLATFORMS
    }
    output.mkdir(parents=True, exist_ok=True)
    notices = output / f"ocaml-temporal-{tag}-third-party-notices.txt"
    notices.write_bytes(legal[NOTICES_NAME].read_bytes())
    manifest = {"tag": tag, "commit": commit, "profile": "release",
                "notices": {"asset": notices.name, "sha256": sha256(notices)}, "bridges": {}}
    for platform, key in checked.items():
        archive = output / f"ocaml-temporal-bridge-{tag}-{platform}.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(bundles / f"rust-bridge-{platform}", arcname="bridge")
            add_legal_files(tar, legal)
        verify_legal_files(archive, legal)
        manifest["bridges"][platform] = {
            "asset": archive.name, "sha256": sha256(archive), "key": key,
            "target": PLATFORMS[platform],
        }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def package_libraries(bundles, output, tag, commit, legal):
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
                add_legal_files(tar, legal)
            verify_legal_files(archive, legal)
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
    parser.add_argument("--license", required=True, type=Path,
                        help="this project's LICENSE file")
    parser.add_argument("--notices", required=True, type=Path,
                        help="audited output of scripts/generate-third-party-notices.py")
    args = parser.parse_args()
    legal = legal_files(args.license, args.notices)
    package_bridges(args.bundles, args.output, args.tag, args.commit, legal)
    package_libraries(args.ocaml_bundles, args.output, args.tag, args.commit, legal)


if __name__ == "__main__":
    main()

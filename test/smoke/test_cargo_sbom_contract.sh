#!/bin/sh
set -eu

# Cargo includes the checkout directory in path-package IDs. This contract
# proves that two clones of the same package graph receive the same document
# and package identities, while distinct graph contents remain distinguishable.
root=${1:-.}
cd "$root"
python3 - scripts/generate-cargo-sbom.py <<'PY'
import copy
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("sbom", sys.argv[1])
assert spec and spec.loader
sbom = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sbom)

def metadata(root):
    return {
        "workspace_root": root + "/rust",
        "packages": [
            {
                "id": "path+file://" + root + "/rust#fixture-0.1.0",
                "name": "fixture",
                "version": "0.1.0",
                "manifest_path": root + "/rust/core-bridge/Cargo.toml",
                "source": None,
                "license": "MIT/Apache-2.0",
            }
        ],
    }

left = sbom.make_document(metadata("/tmp/first-clone"))
right = sbom.make_document(metadata("/home/runner/work/ocaml-temporal"))
assert left == right
assert left["dataLicense"] == "CC0-1.0"
assert left["packages"][0]["filesAnalyzed"] is False
assert left["relationships"] == [{
    "spdxElementId": "SPDXRef-DOCUMENT",
    "relatedSpdxElement": left["packages"][0]["SPDXID"],
    "relationshipType": "DESCRIBES",
}]
assert left["packages"][0]["licenseDeclared"] == "MIT OR Apache-2.0"
assert left["packages"][0]["licenseConcluded"] == "MIT OR Apache-2.0"
sbom.audit_document(left)

# The pinned Core crates declare only a licence file (#787). The SBOM keeps
# that declaration honest while recording the scanner's reviewed conclusion;
# an unreviewed file-only package stays unconcluded and fails the audit.
core_source = sbom.LICENSE_POLICY.CORE_SOURCE_PREFIX + "temporalio-sdk-core@0.5.0"
core_metadata = metadata("/tmp/first-clone")
core_metadata["packages"].append({
    "id": core_source,
    "name": "temporalio-sdk-core",
    "version": "0.5.0",
    "manifest_path": "/cargo/checkouts/sdk-core/crates/sdk-core/Cargo.toml",
    "source": core_source,
    "license": None,
    "license_file": "../../LICENSE.txt",
})
core = sbom.make_document(core_metadata)
core_package = next(p for p in core["packages"] if p["name"] == "temporalio-sdk-core")
assert core_package["licenseConcluded"] == "MIT", core_package
assert core_package["licenseDeclared"] == "NOASSERTION", core_package
assert "LICENSE.txt" in core_package["licenseComments"]
sbom.audit_document(core)

unreviewed_metadata = copy.deepcopy(core_metadata)
unreviewed_metadata["packages"][1]["name"] = "unreviewed"
unreviewed = sbom.make_document(unreviewed_metadata)
unreviewed_package = next(p for p in unreviewed["packages"] if p["name"] == "unreviewed")
assert unreviewed_package["licenseConcluded"] == "NOASSERTION"
assert "licenseComments" not in unreviewed_package
try:
    sbom.audit_document(unreviewed)
except ValueError as exc:
    assert "no concluded license" in str(exc), str(exc)
else:
    raise AssertionError("SBOM audit accepted an unconcluded package")

changed_metadata = metadata("/tmp/first-clone")
changed_metadata["packages"][0]["version"] = "0.2.0"
changed_metadata["packages"][0]["id"] = "path+file:///tmp/first-clone/rust#fixture-0.2.0"
changed = sbom.make_document(changed_metadata)
assert changed["documentNamespace"] != left["documentNamespace"]
sbom.audit_document(changed)

multiple_metadata = metadata("/tmp/first-clone")
multiple_metadata["packages"].append(dict(
    multiple_metadata["packages"][0],
    id="path+file:///tmp/first-clone/rust#other-0.1.0",
    name="other",
))
multiple = sbom.make_document(multiple_metadata)
assert len(multiple["relationships"]) == 2
assert [rel["relatedSpdxElement"] for rel in multiple["relationships"]] == [
    package["SPDXID"] for package in multiple["packages"]
]
sbom.audit_document(multiple)

def expect_rejected(mutator, message):
    invalid = copy.deepcopy(left)
    mutator(invalid)
    try:
        sbom.audit_document(invalid)
    except ValueError as exc:
        assert message in str(exc), str(exc)
    else:
        raise AssertionError(f"SBOM audit accepted invalid {message}")

expect_rejected(lambda doc: doc.pop("dataLicense"), "data license")
expect_rejected(lambda doc: doc["packages"][0].pop("filesAnalyzed"), "filesAnalyzed")
expect_rejected(lambda doc: doc.pop("relationships"), "DESCRIBES")
expect_rejected(lambda doc: doc["packages"][0].pop("licenseDeclared"), "licenseDeclared")
expect_rejected(lambda doc: doc.update(documentNamespace=left["documentNamespace"] + "-stale"), "namespace")

other = dict(metadata("/home/runner/work/ocaml-temporal")["packages"][0], name="other")
assert sbom.package_spdx_id(other, "/home/runner/work/ocaml-temporal/rust") != \
    right["packages"][0]["SPDXID"]
PY

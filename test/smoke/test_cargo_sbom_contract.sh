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
sbom.audit_document(left)

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
expect_rejected(lambda doc: doc.update(documentNamespace=left["documentNamespace"] + "-stale"), "namespace")

other = dict(metadata("/home/runner/work/ocaml-temporal")["packages"][0], name="other")
assert sbom.package_spdx_id(other, "/home/runner/work/ocaml-temporal/rust") != \
    right["packages"][0]["SPDXID"]
PY

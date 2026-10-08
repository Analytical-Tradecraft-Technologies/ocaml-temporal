#!/usr/bin/env python3
"""Exports captured corpus runs and stages them as replay-corpus entries.

Input is the capture directory written by ``capture-history-corpus.sh``: one
``executions.<generation>.json`` file per worker generation, produced by
``history_corpus_capture.exe``. For every recorded run this script:

1. fetches the exact run's history with the official Temporal CLI
   (``temporal workflow show --output json``), unmodified;
2. rejects any history whose ``identity`` fields are not the capture
   program's fixed synthetic identities, so host names cannot enter the
   corpus;
3. encodes the same JSON as a binary ``temporal.api.history.v1.History``
   protobuf with ``google.protobuf.json_format.Parse`` and a descriptor set
   compiled by ``protoc`` from the pinned Temporal Core protos; and
4. writes ``manifest.fragment.json``: one capture provenance record plus one
   entry per run, in the corpus manifest's schema.

With ``--install CORPUS_DIR`` the staged histories and fragment are added to
the committed corpus. Installation is append-only: an existing entry ID,
capture ID, or history file is never overwritten, because historical
fixtures must not be regenerated in place (see
docs/reference/history-corpus.md).

This is maintainer tooling only. It needs Python 3 with the ``protobuf``
package and ``protoc`` on the host; no CI job or Docker-free test runs it.
"""

import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parents[3]

# Feature tags and purposes for each capture case. A case missing here is a
# programming error in the capture program, not something to guess.
CASES = {
    "activity": (["activity"], "One activity scheduled, started and completed."),
    "timer": (["timer"], "One durable timer started and fired."),
    "activity-retry": (
        ["activity", "activity-retry"],
        "Activity fails its first attempt and succeeds on the policy retry.",
    ),
    "interaction": (
        ["signal", "update", "query", "condition"],
        "Signal and accepted update satisfy a parked condition; a query ran "
        "against the same run (queries leave no history events).",
    ),
    "parent": (["child-workflow"], "Parent starts one child and returns its result."),
    "child": (["child-workflow", "child-side"], "Child run started by the parent."),
    "continue-as-new-first": (
        ["continue-as-new"],
        "First run ends with WorkflowExecutionContinuedAsNew.",
    ),
    "continue-as-new-second": (
        ["continue-as-new", "continue-as-new-successor"],
        "Successor run started by continue-as-new completes.",
    ),
    "patch-active": (
        ["patch", "patch-active"],
        "Patched generation records an active patch marker.",
    ),
    "patch-marker-free": (
        ["patch", "patch-marker-free"],
        "Pre-patch generation; history has no patch marker.",
    ),
    "patch-deprecated": (
        ["patch", "patch-deprecated"],
        "Deprecated generation records a deprecated patch marker.",
    ),
}

IDENTITY_PATTERN = re.compile(r"^history-corpus-[a-z0-9-]+$")


def fail(message):
    """Stops the export without leaving a partially staged corpus."""
    print(f"history corpus export failed: {message}", file=sys.stderr)
    sys.exit(1)


def sha256(path):
    """Returns the lowercase hex SHA-256 of a file."""
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def run(args, **kwargs):
    """Runs a command, failing with its stderr on a non-zero exit."""
    result = subprocess.run(args, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        fail(f"{shlex.join(args)} exited {result.returncode}: {result.stderr.strip()}")
    return result.stdout


def core_revision():
    """Reads the pinned Temporal Core revision from the Cargo workspace."""
    text = (REPO / "rust" / "Cargo.toml").read_text()
    match = re.search(r'temporalio-protos = \{[^}]*rev = "([0-9a-f]{40})"', text)
    if not match:
        fail("could not read the pinned temporalio-protos revision")
    return match.group(1)


def core_protos_dir(explicit):
    """Locates the pinned Core proto tree, via cargo metadata unless given."""
    if explicit:
        return pathlib.Path(explicit)
    metadata = json.loads(
        run(
            [
                "cargo",
                "metadata",
                "--format-version",
                "1",
                "--locked",
                "--manifest-path",
                str(REPO / "rust" / "Cargo.toml"),
            ]
        )
    )
    for package in metadata["packages"]:
        if package["name"] == "temporalio-protos":
            return pathlib.Path(package["manifest_path"]).parent / "protos"
    fail("temporalio-protos is not in the Cargo graph")


def descriptor_set(protos, output):
    """Compiles the History message and its imports into a descriptor set."""
    run(
        [
            "protoc",
            "-I",
            str(protos / "api_upstream"),
            "-I",
            str(protos),
            "--include_imports",
            f"--descriptor_set_out={output}",
            str(protos / "api_upstream/temporal/api/history/v1/message.proto"),
        ]
    )


def history_class(descriptor_path):
    """Builds a dynamic History message class from the descriptor set."""
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    files = descriptor_pb2.FileDescriptorSet()
    files.ParseFromString(pathlib.Path(descriptor_path).read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for file in files.file:
        pool.Add(file)
    return message_factory.GetMessageClass(
        pool.FindMessageTypeByName("temporal.api.history.v1.History")
    )


def check_identities(value, where):
    """Requires every identity in the history to be a fixed capture identity."""
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "identity" and not IDENTITY_PATTERN.match(str(item)):
                fail(f"{where}: non-synthetic identity {item!r}")
            check_identities(item, where)
    elif isinstance(value, list):
        for item in value:
            check_identities(item, where)


def compose_digest(service):
    """Reads one digest-pinned image reference from the Compose file."""
    text = (REPO / "test/integration/temporal/compose.yaml").read_text()
    match = re.search(rf"\n  {service}:\n(?:    .*\n)*?    image: (\S+)", text)
    if not match:
        fail(f"could not read the {service} image from compose.yaml")
    return match.group(1)


def provenance(args, descriptor, toolchains):
    """Describes the producing source, Core, server and export toolchain."""
    import google.protobuf

    commit = run(["git", "-C", str(REPO), "rev-parse", "HEAD"]).strip()
    dirty = run(["git", "-C", str(REPO), "status", "--porcelain", "--untracked-files=no"])
    return {
        "kind": "live",
        "captured_on": datetime.date.today().isoformat(),
        "sdk_commit": commit,
        "sdk_tree_dirty": bool(dirty.strip()),
        "core_revision": core_revision(),
        "temporal_server_image": compose_digest("temporal"),
        "temporal_cli_image": compose_digest("temporal-admin-tools"),
        "worker_toolchains": sorted(toolchains),
        "capture_command": args.capture_command,
        "protobuf_export": {
            "tool": f"Python protobuf {google.protobuf.__version__} json_format",
            "descriptor_sha256": sha256(descriptor),
            "descriptor_source": "protoc --include_imports over the pinned "
            "Core temporal/api/history/v1/message.proto",
        },
    }


def export(args):
    """Exports every recorded run and writes the staged fragment."""
    capture = pathlib.Path(args.capture_dir)
    staged = capture / "histories"
    staged.mkdir(parents=True, exist_ok=True)
    descriptor = capture / "history.desc"
    descriptor_set(core_protos_dir(args.core_protos), descriptor)
    History = history_class(descriptor)
    from google.protobuf import json_format

    entries = []
    toolchains = set()
    records = sorted(capture.glob("executions.*.json"))
    if not records:
        fail(f"no executions.*.json files in {capture}")
    for record_file in records:
        record = json.loads(record_file.read_text())
        generation = record["generation"]
        toolchains.add(f"OCaml {record['ocaml_version']} ({record['os_type']})")
        for execution in record["executions"]:
            case = execution["case"]
            if case not in CASES:
                fail(f"unknown capture case {case!r}")
            command = [
                "workflow",
                "show",
                "--namespace",
                args.namespace,
                "--workflow-id",
                execution["workflow_id"],
                "--output",
                "json",
            ]
            if execution["run_id"]:
                command += ["--run-id", execution["run_id"]]
            # The CLI prefix is shell text (Compose commands carry quoted
            # paths and environment assignments); the arguments are passed
            # positionally so no history identifier is re-parsed by the shell.
            text = run(["sh", "-c", args.temporal_cli + ' "$@"', "temporal-cli"] + command)
            document = json.loads(text)
            events = document.get("events") or fail(f"{case}: history has no events")
            started = events[0].get("workflowExecutionStartedEventAttributes")
            if started is None:
                fail(f"{case}: first event is not WorkflowExecutionStarted")
            if started["workflowType"]["name"] != execution["workflow_type"]:
                fail(f"{case}: unexpected workflow type")
            check_identities(document, case)
            name = f"{args.capture_id}-{case}"
            json_path = staged / f"{name}.json"
            pb_path = staged / f"{name}.pb"
            json_path.write_text(text)
            message = json_format.Parse(text, History())
            pb_path.write_bytes(message.SerializeToString())
            features, purpose = CASES[case]
            entries.append(
                {
                    "id": name,
                    "purpose": purpose,
                    "features": features,
                    "workflow_type": execution["workflow_type"],
                    "workflow_id": execution["workflow_id"],
                    "run_id": started["originalExecutionRunId"],
                    "capture": args.capture_id,
                    "generation": generation,
                    "history": {
                        "protobuf": f"histories/{name}.pb",
                        "protobuf_sha256": sha256(pb_path),
                        "json": f"histories/{name}.json",
                        "json_sha256": sha256(json_path),
                    },
                    "replay_definitions": generation,
                    "expected": "replays_ok",
                }
            )
    fragment = {
        "captures": {args.capture_id: provenance(args, descriptor, toolchains)},
        "entries": entries,
    }
    (capture / "manifest.fragment.json").write_text(json.dumps(fragment, indent=2) + "\n")
    print(f"staged {len(entries)} histories in {capture}")
    return fragment


def install(fragment, capture, corpus):
    """Appends the staged fragment to the committed corpus, never replacing."""
    corpus = pathlib.Path(corpus)
    manifest_path = corpus / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    ids = {entry["id"] for entry in manifest["entries"]}
    for capture_id in fragment["captures"]:
        if capture_id in manifest["captures"]:
            fail(f"capture {capture_id} already exists; corpus entries are immutable")
    for entry in fragment["entries"]:
        if entry["id"] in ids:
            fail(f"entry {entry['id']} already exists; corpus entries are immutable")
        for key in ("protobuf", "json"):
            if (corpus / entry["history"][key]).exists():
                fail(f"{entry['history'][key]} already exists")
    for entry in fragment["entries"]:
        for key in ("protobuf", "json"):
            target = corpus / entry["history"][key]
            target.write_bytes((pathlib.Path(capture) / entry["history"][key]).read_bytes())
    manifest["captures"].update(fragment["captures"])
    manifest["entries"].extend(fragment["entries"])
    # Write through a temporary file so an interrupted install cannot leave a
    # truncated manifest beside already copied histories.
    with tempfile.NamedTemporaryFile("w", dir=corpus, delete=False) as handle:
        handle.write(json.dumps(manifest, indent=2) + "\n")
    os.replace(handle.name, manifest_path)
    print(f"installed {len(fragment['entries'])} entries into {manifest_path}")


def main():
    """Parses arguments, exports, and optionally installs."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--capture-dir", required=True)
    parser.add_argument("--capture-id", required=True,
                        help="stable capture identifier, e.g. live-2026-10-08")
    parser.add_argument("--temporal-cli", required=True,
                        help="shell command prefix that runs the official Temporal CLI")
    parser.add_argument("--namespace", default="temporal-sdk-test")
    parser.add_argument("--core-protos", help="pinned Core protos directory")
    parser.add_argument("--capture-command", required=True)
    parser.add_argument("--install", metavar="CORPUS_DIR")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", args.capture_id):
        fail("--capture-id must be lowercase letters, digits and hyphens")
    fragment = export(args)
    if args.install:
        install(fragment, args.capture_dir, args.install)


if __name__ == "__main__":
    main()

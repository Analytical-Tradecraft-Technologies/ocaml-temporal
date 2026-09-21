#!/usr/bin/env python3
"""Replace defective OCaml code and retain exact-run server recovery evidence.

The supported Make target builds the three binaries first. This controller
owns only its explicitly selected Compose project and uniquely named processes;
all histories, identities, binary/source hashes and logs survive stack teardown.
"""

import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[4]
FIXTURE = ROOT / "test/integration/temporal"
CASES = ("body", "encoder", "missing", "business-retryable", "business-permanent")
RECOVERABLE = CASES[:3]
PROJECT = os.environ.get("TEMPORAL_COMPOSE_PROJECT", "ocaml-temporal-task-failure")
IMAGE = os.environ.get("TASK_FAILURE_DEV_IMAGE", f"{PROJECT}-dev")
STAMP = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
ARTIFACTS = ROOT / "_build/task-failure-live" / STAMP
COMPOSE = ["docker", "compose", "--project-directory", str(FIXTURE), "-f",
           str(FIXTURE / "compose.yaml"), "-p", PROJECT, "--profile", "temporal"]
BINARY_DIR = "_build/default/test/integration/temporal/task_failure"


def diagnostic_text(value):
    """Decode timeout output, which can remain bytes even in subprocess text mode."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


def command(args, timeout=120):
    """Keep successful stdout parseable; retain failed commands before cleanup."""
    try:
        return subprocess.run(args, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, check=True, timeout=timeout).stdout
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as error:
        diagnostic = {
            "argv": [str(arg) for arg in args], "cwd": str(ROOT),
            "error": f"{type(error).__name__}: {error}",
            "returncode": getattr(error, "returncode", None), "timeout_seconds": timeout,
            "stdout": diagnostic_text(getattr(error, "stdout", None)),
            "stderr": diagnostic_text(getattr(error, "stderr", None)),
        }
        # Append so a teardown failure cannot overwrite the original startup
        # failure. Also print to Actions when no containers ever produced logs.
        report = json.dumps(diagnostic, ensure_ascii=True)
        print(f"Task-failure command diagnostic: {report}", file=sys.stderr, flush=True)
        try:
            ARTIFACTS.mkdir(parents=True, exist_ok=True)
            with (ARTIFACTS / "command-failures.jsonl").open("a", encoding="utf-8") as output:
                output.write(report + "\n")
        except OSError as logging_error:
            print(f"Could not retain command diagnostic: {logging_error}", file=sys.stderr)
        raise


def save_json(name, value):
    """Retain readable immutable evidence under this invocation's directory."""
    (ARTIFACTS / name).write_text(json.dumps(value, indent=2) + "\n")


def sha256(path):
    """Bind large native executables without loading the whole file at once."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def wait_for(check, label, seconds=180):
    """Poll a concrete predicate with a deadline; time alone is never success."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(1)
    raise AssertionError(f"timed out waiting for {label}")


def docker_name(role):
    """Unique process names permit cleanup without touching another run."""
    return f"{PROJECT}-task-failure-{STAMP.lower()}-{role}"


def launch(role, executable):
    """Launch an already built OCaml process; no concurrent Dune lock/build."""
    name = docker_name(role)
    command(["docker", "run", "--detach", "--name", name, "--init", "--user",
             f"{os.getuid()}:{os.getgid()}", "--network", f"{PROJECT}_temporal-network",
             "-v", f"{ROOT}:/workspace", "-w", "/workspace",
             "-e", "TEMPORAL_TASK_FAILURE_LIVE=1", "-e", "TEMPORAL_ADDRESS=http://temporal:7233",
             "-e", "TEMPORAL_NAMESPACE=temporal-sdk-test", "-e", "OCAMLRUNPARAM=b", "-e",
             f"TASK_FAILURE_ARTIFACT_DIR=/workspace/{ARTIFACTS.relative_to(ROOT)}",
             IMAGE, f"{BINARY_DIR}/{executable}.exe"])
    return name


def running(name):
    """Fail promptly if a producer died before publishing its marker."""
    state = json.loads(command(["docker", "inspect", name]))[0]["State"]
    assert state["Running"], f"{name} exited before marker: {state}"
    return True


def marker(name, producer):
    """A ready file is accepted only while its exact producer still runs."""
    return running(producer) and (ARTIFACTS / name).is_file()


def stop_worker(generation, container):
    """Require public shutdown, zero exit, and removal before replacement."""
    command(["docker", "stop", "--time", "30", container], timeout=45)
    state = json.loads(command(["docker", "inspect", container]))[0]["State"]
    assert state["ExitCode"] == 0, state
    assert (ARTIFACTS / f"{generation}.stopped").read_text() == "stopped\n"
    logs = subprocess.run(["docker", "logs", container], text=True, capture_output=True, check=True)
    (ARTIFACTS / f"{generation}.log").write_text(logs.stdout + logs.stderr)
    command(["docker", "rm", container])


def describe(name, workflow_id, run_id, stage):
    """Bind the returned identity separately because raw histories can omit it."""
    value = json.loads(command(COMPOSE + ["run", "--rm", "--no-deps", "temporal-admin-tools",
        "temporal", "workflow", "describe", "--workflow-id", workflow_id,
        "--run-id", run_id, "--namespace", "temporal-sdk-test", "--output", "json"]))
    save_json(f"{name}.{stage}.describe.json", value)
    info = value.get("workflowExecutionInfo", value.get("workflow_execution_info"))
    execution = info["execution"]
    assert execution.get("workflowId", execution.get("workflow_id")) == workflow_id
    assert execution.get("runId", execution.get("run_id")) == run_id
    return info


def history(name, workflow_id, run_id, stage):
    """Retain unmodified payload-bearing JSON from this exact server run."""
    value = json.loads(command(COMPOSE + ["run", "--rm", "--no-deps", "temporal-admin-tools",
        "temporal", "workflow", "show", "--workflow-id", workflow_id,
        "--run-id", run_id, "--namespace", "temporal-sdk-test", "--output", "json"]))
    save_json(f"{name}.{stage}.history.json", value)
    events = value["events"]
    ids = [int(event["eventId"]) for event in events]
    assert ids == list(range(1, len(events) + 1)), "history has missing/duplicate events"
    return events


def event_type(event):
    """Accept protobuf JSON's CLI and enum spellings without fuzzy matching."""
    raw = event["eventType"].removeprefix("EVENT_TYPE_")
    return raw.replace("_", "").lower()


def validate(name, events, stage):
    """Check durable task/execution outcomes and absence of speculative commands."""
    types = [event_type(event) for event in events]
    if name in RECOVERABLE:
        assert "workflowexecutionfailed" not in types, "repairable defect closed execution"
        assert "workflowtaskfailed" in types, "no durable task failure"
        assert types.count("timerstarted") == (0 if name == "missing" else 1), "partial timer leaked"
        assert types.count("timerfired") == (0 if name == "missing" else 1)
        failures = [event["workflowTaskFailedEventAttributes"] for event in events
                    if event_type(event) == "workflowtaskfailed"]
        assert all("unhandledfailure" in item["cause"].replace("_", "").lower()
                   for item in failures), "unexpected task-failure cause"
        terminals = [item for item in types if item.startswith("workflowexecution") and item != "workflowexecutionstarted"]
        assert terminals == ([] if stage == "initial" else ["workflowexecutioncompleted"]), terminals
    else:
        assert types[-1] == "workflowexecutionfailed"
        assert "workflowtaskfailed" not in types
        failure = events[-1]["workflowExecutionFailedEventAttributes"]["failure"]
        assert failure["message"] == "intentional business failure"
        assert failure["applicationFailureInfo"].get("nonRetryable", False) == (name == "business-permanent")
        # No top-level retry policy is supplied: a retryable failure still closes
        # this run. Explicit bounded child retry policies have separate Core gates.
        assert not events[0]["workflowExecutionStartedEventAttributes"].get("retryPolicy")


def main():
    """Own the stack lifecycle and write a final verdict only after all checks."""
    assert re.fullmatch(r"[a-z0-9][a-z0-9_-]*", PROJECT), "invalid Compose project"
    # Refuse to destroy or reuse an existing stack, even with the same label.
    assert not command(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}"]).strip(), "project already in use"
    assert not command(["docker", "volume", "ls", "-q", "--filter", f"label=com.docker.compose.project={PROJECT}"]).strip(), "project has existing data"
    ARTIFACTS.mkdir(parents=True)
    print(f"Task-failure artifacts: {ARTIFACTS}", flush=True)
    binaries = {name: sha256(ROOT / BINARY_DIR / f"{name}.exe")
                for name in ("broken_worker", "corrected_worker", "recovery_driver")}
    source_diff = subprocess.run(["git", "diff", "HEAD", "--binary"], cwd=ROOT,
                                 check=True, stdout=subprocess.PIPE).stdout
    compose_config = json.loads(command(COMPOSE + ["config", "--format", "json"]))
    metadata = {"sdk_commit": command(["git", "rev-parse", "HEAD"]).strip(),
                "tracked_source_diff_sha256": hashlib.sha256(source_diff).hexdigest(),
                "core_revision": re.search(r'rev = "([a-f0-9]+)"', (ROOT / "rust/Cargo.toml").read_text())[1],
                "service_images": {name: service.get("image") for name, service in compose_config["services"].items() if service.get("image")},
                "development_image_id": command(["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"]).strip(),
                "compose_sha256": sha256(FIXTURE / "compose.yaml"),
                "binary_sha256": binaries, "project": PROJECT, "result": "running"}
    save_json("manifest.json", metadata)
    processes = []
    try:
        command(COMPOSE + ["up", "--detach", "--wait", "postgresql", "temporal"], timeout=300)
        command(["make", "temporal-health", f"TEMPORAL_COMPOSE_PROJECT={PROJECT}"], timeout=180)
        broken = launch("broken", "broken_worker")
        processes.append(broken)
        wait_for(lambda: marker("broken.ready", broken), "broken worker")
        driver = launch("driver", "recovery_driver")
        processes.append(driver)
        wait_for(lambda: marker("accepted.tsv", driver), "exact client handles")
        rows = [line.split("\t") for line in (ARTIFACTS / "accepted.tsv").read_text().splitlines()]
        assert [row[0] for row in rows] == list(CASES)
        assert len({row[2] for row in rows}) == len(CASES)
        initial = {}
        for name, workflow_id, run_id in rows:
            def initial_ready():
                """Repeat only history absence; any invalid terminal fails immediately."""
                running(broken)
                running(driver)
                events = history(name, workflow_id, run_id, "initial")
                types = [event_type(event) for event in events]
                required = "workflowtaskfailed" if name in RECOVERABLE else "workflowexecutionfailed"
                if name in RECOVERABLE:
                    assert "workflowexecutionfailed" not in types
                if required not in types:
                    return False
                validate(name, events, "initial")
                initial[name] = events
                return True
            wait_for(initial_ready, f"{name} durable initial failure")
            info = describe(name, workflow_id, run_id, "initial")
            expected_status = "running" if name in RECOVERABLE else "failed"
            assert info["status"].lower().endswith(expected_status), info["status"]
        assert not (ARTIFACTS / "completed.tsv").exists()
        stop_worker("broken", broken)
        processes.remove(broken)
        corrected = launch("corrected", "corrected_worker")
        processes.append(corrected)
        wait_for(lambda: marker("corrected.ready", corrected), "corrected worker")
        wait_for(lambda: (ARTIFACTS / "completed.tsv").is_file() or
                 (running(driver) and False), "same-handle results", seconds=240)
        assert (ARTIFACTS / "accepted.tsv").read_bytes() == (ARTIFACTS / "completed.tsv").read_bytes()
        assert command(["docker", "wait", driver]).strip() == "0"
        for name, workflow_id, run_id in rows:
            info = describe(name, workflow_id, run_id, "terminal")
            events = history(name, workflow_id, run_id, "terminal")
            validate(name, events, "terminal")
            assert events[:len(initial[name])] == initial[name], "initial history was rewritten"
            expected_status = "completed" if name in RECOVERABLE else "failed"
            assert info["status"].lower().endswith(expected_status), info["status"]
        stop_worker("corrected", corrected)
        processes.remove(corrected)
        metadata.update(result="passed", executions=rows,
                        replay="fresh corrected process replays the committed timer prefix on each original run")
    except BaseException as error:
        metadata.update(result="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        for container in processes:
            logs = subprocess.run(["docker", "logs", container], text=True, capture_output=True)
            (ARTIFACTS / f"{container}.log").write_text(logs.stdout + logs.stderr)
            subprocess.run(["docker", "rm", "--force", container], capture_output=True, check=False)
        logs = subprocess.run(COMPOSE + ["logs", "--no-color", "--tail", "200"], text=True, capture_output=True)
        (ARTIFACTS / "server.log").write_text(logs.stdout + logs.stderr)
        command(COMPOSE + ["down", "--volumes", "--remove-orphans"], timeout=120)
        assert not command(["docker", "volume", "ls", "-q", "--filter", f"label=com.docker.compose.project={PROJECT}"]).strip()
        assert not command(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}"]).strip()
        assert not command(["docker", "ps", "-aq", "--filter", f"name={PROJECT}-task-failure-{STAMP.lower()}-"]).strip()
        metadata.update(project_volumes_remaining=0, project_containers_remaining=0,
                        fixture_processes_remaining=0)
        save_json("manifest.json", metadata)
    print("Task-failure recovery and intentional execution failures passed", flush=True)


if __name__ == "__main__":
    main()

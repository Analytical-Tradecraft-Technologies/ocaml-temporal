"""Exercise the live controller's diagnostics with real failing subprocesses."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "integration/temporal/scripts/run-task-failure-live.py"
SPEC = importlib.util.spec_from_file_location("task_failure_controller", SCRIPT)
CONTROLLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTROLLER)


class CommandLoggingTests(unittest.TestCase):
    """Diagnostics survive startup/cleanup errors without changing command results."""

    def setUp(self):
        """Give each test an isolated artifact directory and captured CI stderr."""
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.artifacts = Path(self.directory.name) / "artifacts"
        patcher = patch.object(CONTROLLER, "ARTIFACTS", self.artifacts)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.stderr = io.StringIO()
        redirect = contextlib.redirect_stderr(self.stderr)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)

    def records(self):
        """Read retained records in failure order."""
        return [json.loads(line) for line in
                (self.artifacts / "command-failures.jsonl").read_text().splitlines()]

    def test_success_keeps_stdout_clean(self):
        """Stderr must not contaminate successful JSON consumed by the controller."""
        output = CONTROLLER.command([sys.executable, "-c",
                                     "import sys; print('{}'); print('warning', file=sys.stderr)"])
        self.assertEqual(json.loads(output), {})
        self.assertEqual(self.stderr.getvalue(), "")
        self.assertFalse(self.artifacts.exists())

    def test_failure_output_and_cleanup_failure_are_retained(self):
        """A later failure appends instead of erasing the startup diagnostic."""
        for message in ("startup", "cleanup"):
            args = [sys.executable, "-c", "import sys; print('progress'); "
                    f"print('{message}', file=sys.stderr); sys.exit(7)"]
            with self.assertRaises(subprocess.CalledProcessError) as raised:
                CONTROLLER.command(args)
            self.assertEqual(raised.exception.returncode, 7)
            record = self.records()[-1]
            self.assertEqual(record["argv"], args)
            self.assertEqual(record["returncode"], 7)
            self.assertEqual(record["stdout"], "progress\n")
            self.assertEqual(record["stderr"], message + "\n")
            self.assertIn(message, self.stderr.getvalue())
        self.assertEqual(len(self.records()), 2)

    def test_timeout_retains_partial_output(self):
        """TimeoutExpired byte streams remain readable in artifacts and CI."""
        with self.assertRaises(subprocess.TimeoutExpired):
            CONTROLLER.command([sys.executable, "-c", "import sys,time; "
                                "print('started', flush=True); "
                                "print('waiting', file=sys.stderr, flush=True); time.sleep(30)"], timeout=1)
        record = self.records()[0]
        self.assertEqual(record["stdout"], "started\n")
        self.assertEqual(record["stderr"], "waiting\n")
        self.assertEqual(record["timeout_seconds"], 1)

    def test_missing_executable_is_reported(self):
        """A process that never launches still leaves actionable evidence."""
        with self.assertRaises(FileNotFoundError):
            CONTROLLER.command([str(Path(self.directory.name) / "missing")])
        self.assertIn("FileNotFoundError", self.records()[0]["error"])

    def test_artifact_write_failure_preserves_original_error(self):
        """Disk errors do not replace the command failure or suppress CI output."""
        self.artifacts.write_text("not a directory")
        with self.assertRaises(subprocess.CalledProcessError) as raised:
            CONTROLLER.command([sys.executable, "-c", "import sys; sys.exit(9)"])
        self.assertEqual(raised.exception.returncode, 9)
        self.assertIn("Could not retain command diagnostic", self.stderr.getvalue())


DEADLINE = ("Error: failed connecting to Temporal server at temporal:7233: "
            "context deadline exceeded")
RETRY_LIBRARY = (Path(__file__).resolve().parents[1]
                 / "integration/temporal/scripts/temporal-cli-retry.sh")
# Stderr samples shared by the Python and shell classifiers (#1285): each
# pairs the text with whether a read-only query may be retried after it.
CLASSIFICATION_SAMPLES = (
    ("rpc error: code = Unavailable desc = frontend restarting", True),
    ("rpc error: code = DeadlineExceeded desc = frontend not ready", True),
    (DEADLINE, True),
    (DEADLINE + "\n  Connection checks for temporal:7233\n"
     "    ? gRPC failure could not be diagnosed before its deadline", True),
    (" Container ocaml-temporal-integration-temporal-admin-tools-run Creating\n"
     + DEADLINE, True),
    ("Error: authentication failed: invalid API key\n" + DEADLINE, False),
    ("Error: workflow not found for ID: missing", False),
    ("rpc error: code = NotFound desc = workflow execution not found", False),
    ("rpc error: code = PermissionDenied desc = forbidden", False),
    ("", False),
)


class ReadOnlyRetryTests(unittest.TestCase):
    """Read-only admin queries retry only transport failures, a bounded number of times."""

    def setUp(self):
        """Isolate artifacts, CI stderr and the fake CLI's attempt counter."""
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.counter = Path(self.directory.name) / "attempts"
        self.counter.write_text("0")
        patcher = patch.object(CONTROLLER, "ARTIFACTS", Path(self.directory.name) / "artifacts")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.stderr = io.StringIO()
        redirect = contextlib.redirect_stderr(self.stderr)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)
        self.sleeps = []

    def fake_cli(self, failures, message):
        """Return argv that fails [failures] times with [message], then prints JSON."""
        code = (
            "import pathlib, sys\n"
            f"counter = pathlib.Path({str(self.counter)!r})\n"
            "count = int(counter.read_text()) + 1\n"
            "counter.write_text(str(count))\n"
            f"if count <= {failures}:\n"
            "    print('partial')\n"
            f"    print({message!r}, file=sys.stderr)\n"
            "    sys.exit(1)\n"
            "print('{\"ok\": true}')\n")
        return [sys.executable, "-c", code]

    def attempts(self):
        """Number of times the fake CLI ran."""
        return int(self.counter.read_text())

    def test_transient_failure_is_retried_and_stdout_is_final(self):
        """Only the successful attempt's stdout reaches the JSON parser."""
        output = CONTROLLER.read_only_command(
            self.fake_cli(2, DEADLINE), sleep=self.sleeps.append)
        self.assertEqual(json.loads(output), {"ok": True})
        self.assertEqual(self.attempts(), 3)
        self.assertEqual(self.sleeps, [CONTROLLER.CLI_RETRY_SLEEP_SECONDS] * 2)
        self.assertIn("attempt 2 of 5", self.stderr.getvalue())

    def test_non_transport_failure_is_not_retried(self):
        """NotFound and authentication errors fail on the first attempt."""
        for message in ("rpc error: code = NotFound desc = missing",
                        "Error: authentication failed: invalid API key\n" + DEADLINE):
            self.counter.write_text("0")
            with self.assertRaises(subprocess.CalledProcessError):
                CONTROLLER.read_only_command(self.fake_cli(1, message), sleep=self.sleeps.append)
            self.assertEqual(self.attempts(), 1)
        self.assertEqual(self.sleeps, [])

    def test_retries_are_bounded_and_each_failure_is_retained(self):
        """A persistent transport failure re-raises after the last attempt."""
        with self.assertRaises(subprocess.CalledProcessError) as raised:
            CONTROLLER.read_only_command(
                self.fake_cli(99, "rpc error: code = Unavailable desc = down"),
                sleep=self.sleeps.append)
        self.assertEqual(raised.exception.returncode, 1)
        self.assertEqual(self.attempts(), CONTROLLER.CLI_RETRY_ATTEMPTS)
        self.assertEqual(len(self.sleeps), CONTROLLER.CLI_RETRY_ATTEMPTS - 1)
        records = (CONTROLLER.ARTIFACTS / "command-failures.jsonl").read_text().splitlines()
        self.assertEqual(len(records), CONTROLLER.CLI_RETRY_ATTEMPTS)

    def test_timeout_is_not_retried(self):
        """A hung CLI is bounded by the per-command timeout, not repeated."""
        with self.assertRaises(subprocess.TimeoutExpired):
            CONTROLLER.read_only_command([sys.executable, "-c", "import time; time.sleep(30)"],
                                         timeout=1, sleep=self.sleeps.append)
        self.assertEqual(self.sleeps, [])

    def test_python_and_shell_classifiers_agree(self):
        """Both live-test languages classify the same samples identically."""
        for text, transient in CLASSIFICATION_SAMPLES:
            with self.subTest(text=text):
                self.assertEqual(CONTROLLER.transient_cli_error(text), transient)
                shell = subprocess.run(
                    ["sh", "-c", '. "$1"; temporal_cli_transient_error "$2"', "sh",
                     str(RETRY_LIBRARY), text], check=False)
                self.assertEqual(shell.returncode == 0, transient)

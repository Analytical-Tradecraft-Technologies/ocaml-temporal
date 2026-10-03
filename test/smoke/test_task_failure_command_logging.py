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

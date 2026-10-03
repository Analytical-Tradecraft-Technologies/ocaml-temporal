"""Exercise completion publication and process exit without starting Docker."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "integration/temporal/scripts/run-task-failure-live.py"
SPEC = importlib.util.spec_from_file_location("task_failure_markers_controller", SCRIPT)
CONTROLLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTROLLER)


class MarkerTests(unittest.TestCase):
    """A terminal marker survives clean exit; readiness still requires liveness."""

    def setUp(self):
        """Isolate the real marker files and replace only Docker inspection."""
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.artifacts = Path(self.directory.name)
        patcher = patch.object(CONTROLLER, "ARTIFACTS", self.artifacts)
        patcher.start()
        self.addCleanup(patcher.stop)

    def inspect(self, *, running, exit_code=0, publish=False):
        """Publish during inspection to reproduce the CI check/exit interleaving."""
        def command(args):
            """Return the sampled container state after any final publication."""
            self.assertEqual(args, ["docker", "inspect", "driver"])
            if publish:
                (self.artifacts / "completed.tsv").write_text("exact results\n")
            return json.dumps([{"State": {"Running": running, "ExitCode": exit_code}}])
        return patch.object(CONTROLLER, "command", side_effect=command)

    def test_publication_during_inspection_survives_clean_exit(self):
        """The driver may publish and exit after the controller starts polling."""
        with self.inspect(running=False, publish=True):
            self.assertTrue(CONTROLLER.completion_marker("completed.tsv", "driver"))

    def test_running_without_results_keeps_waiting(self):
        """An active driver with no terminal marker is not yet successful."""
        with self.inspect(running=True):
            self.assertFalse(CONTROLLER.completion_marker("completed.tsv", "driver"))

    def test_running_with_results_is_ready_for_exit_check(self):
        """Publication can precede shutdown; the caller still waits for exit zero."""
        with self.inspect(running=True, publish=True):
            self.assertTrue(CONTROLLER.completion_marker("completed.tsv", "driver"))

    def test_clean_exit_without_results_fails(self):
        """Exit zero cannot replace the required exact-handle results."""
        with self.inspect(running=False), self.assertRaises(AssertionError):
            CONTROLLER.completion_marker("completed.tsv", "driver")

    def test_failed_exit_with_results_fails(self):
        """A driver error after publishing must not turn the live gate green."""
        (self.artifacts / "completed.tsv").write_text("exact results\n")
        with self.inspect(running=False, exit_code=7), self.assertRaises(AssertionError):
            CONTROLLER.completion_marker("completed.tsv", "driver")

    def test_failed_exit_without_results_fails(self):
        """A failed producer is rejected promptly rather than timing out."""
        with self.inspect(running=False, exit_code=7), self.assertRaises(AssertionError):
            CONTROLLER.completion_marker("completed.tsv", "driver")

    def test_readiness_requires_a_live_producer(self):
        """The completion exception must not accept a dead worker's ready file."""
        (self.artifacts / "ready").write_text("ready\n")
        with self.inspect(running=False), self.assertRaises(AssertionError):
            CONTROLLER.marker("ready", "driver")

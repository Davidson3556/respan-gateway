"""Runner checks use a mocked Cargo process and never contact providers."""

import contextlib
import importlib.util
import io
from pathlib import Path
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location(
    "live_runner", Path(__file__).resolve().parents[1] / "test_live.py"
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class LiveRunnerTests(unittest.TestCase):
    def run_mock(self, output):
        process = mock.Mock(returncode=0)
        process.communicate.return_value = (output, None)
        captured = io.StringIO()
        with (
            mock.patch("sys.argv", ["test_live.py", "anthropic"]),
            mock.patch.dict("os.environ", {"ANTHROPIC_API_KEY": "fake-secret-key"}),
            mock.patch.object(runner.subprocess, "Popen", return_value=process),
            contextlib.redirect_stdout(captured),
        ):
            result = runner.main()
        return result, captured.getvalue()

    def test_zero_selected_tests_cannot_report_success(self):
        result, _ = self.run_mock("running 0 tests\ntest result: ok. 0 passed;\n")
        self.assertEqual(result, 1)

    def test_one_selected_test_can_report_success(self):
        result, _ = self.run_mock("running 1 test\ntest result: ok. 1 passed;\n")
        self.assertEqual(result, 0)

    def test_provider_output_redacts_credentials(self):
        _, output = self.run_mock("running 1 test\nprovider echoed fake-secret-key\n")
        self.assertNotIn("fake-secret-key", output)
        self.assertIn("<redacted>", output)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Check safe refusal diagnostics without exposing scanner reports or inputs."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("release_content.py")
SPEC = importlib.util.spec_from_file_location("release_content_diagnostics", SCRIPT)
content = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(content)
CANARY = "private-diagnostic-canary-never-log-this"


class RefusalDiagnosticsTests(unittest.TestCase):
    def report(self, error, phase="binary-scan"):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            content.report_refusal(error, phase)
        text = output.getvalue()
        self.assertNotIn(CANARY, text)
        self.assertIn("release content validation failed", text)
        return text

    def test_tool_failures_do_not_log_command_output_or_stderr(self):
        errors = [
            (subprocess.TimeoutExpired([CANARY], 1, output=CANARY, stderr=CANARY), "tool-timeout"),
            (subprocess.CalledProcessError(7, [CANARY], output=CANARY, stderr=CANARY), "tool-failed"),
            (OSError(13, CANARY, CANARY), "io-failed"),
            (KeyError(CANARY), "metadata-field-missing"),
            (TypeError(CANARY), "metadata-type-invalid"),
            (ValueError(CANARY), "validation-refused"),
        ]
        for error, code in errors:
            with self.subTest(code=code):
                self.assertIn("code=" + code + ";", self.report(error))

    def test_unknown_phase_cannot_inject_log_content(self):
        self.assertIn("phase=unknown;", self.report(ValueError(CANARY), CANARY))

    def test_scanner_refusal_categories_preserve_fail_closed_behavior(self):
        finding = {"RuleID": "generic-api-key", "Match": CANARY, "Secret": CANARY}
        cases = [
            (7, b"", CANARY.encode(), "scanner-exit-invalid"),
            (0, b"[]", CANARY.encode(), "scanner-stderr-nonempty"),
            (0, b"{}", b"", "scanner-report-type-invalid"),
            (0, json.dumps([finding]).encode(), b"", "scanner-report-status-mismatch"),
            (1, b"[]", b"", "scanner-report-status-mismatch"),
            (1, b"[{}]", b"", "scanner-finding-shape-invalid"),
            (0, ("{" + CANARY).encode(), b"", "json-report-invalid"),
        ]
        for status, output, stderr, code in cases:
            with self.subTest(code=code):
                with self.assertRaises(ValueError) as caught:
                    content.scanner_findings(status, output, stderr)
                self.assertIn("code=" + code + ";", self.report(caught.exception))

    def test_valid_scanner_reports_are_unchanged(self):
        finding = {"RuleID": "generic-api-key", "Match": CANARY, "Secret": CANARY}
        self.assertEqual(content.scanner_findings(0, b"[]", b""), [])
        self.assertEqual(content.scanner_findings(1, json.dumps([finding]).encode(), b""), [finding])

    def test_unapproved_content_stays_a_refusal(self):
        for message, code in [
            ("binary secret scan found unapproved content", "elf-content-refused"),
            ("non-ELF binary secret scan found prohibited content", "non-elf-content-refused"),
            ("artifact path secret scan found prohibited content", "artifact-path-refused"),
            ("binary finding lacks verified Go build metadata", "go-metadata-unavailable"),
        ]:
            with self.subTest(code=code):
                self.assertIn("code=" + code + ";", self.report(ValueError(message)))

    def test_actual_cli_reports_a_safe_phase_and_keeps_nonzero_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / CANARY
            private.write_text(CANARY)
            scan = root / "scan"
            scan.mkdir()
            (scan / CANARY).symlink_to(private)
            result = subprocess.run([sys.executable, "-B", str(SCRIPT), "binary-scan", str(scan)],
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stdout, "")
            self.assertIn("phase=binary-scan; code=symlink-input-refused;", result.stderr)
            self.assertNotIn(CANARY, result.stderr)


if __name__ == "__main__":
    unittest.main()

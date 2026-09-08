"""WP-33350: prove config-supplied error_file_path cannot escape the base dir (CWE-73)."""

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Load path_safety directly so we don't import tap __init__ (ijson/singer deps).
_path_safety = _load_module(
    "tap_workday_raas.path_safety", _ROOT / "tap_workday_raas" / "path_safety.py")
resolve_safe_error_file_path = _path_safety.resolve_safe_error_file_path


class TestResolveSafeErrorFilePath(unittest.TestCase):
    def test_normal_relative_filename_is_accepted_inside_base(self):
        with tempfile.TemporaryDirectory() as base:
            resolved = resolve_safe_error_file_path("error.json", base)
            self.assertIsNotNone(resolved)
            self.assertEqual(resolved, os.path.join(os.path.realpath(base), "error.json"))

    def test_parent_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as base:
            self.assertIsNone(
                resolve_safe_error_file_path("../../etc/passwd", base))

    def test_absolute_path_is_rejected(self):
        with tempfile.TemporaryDirectory() as base:
            self.assertIsNone(
                resolve_safe_error_file_path("/etc/passwd", base))

    def test_none_and_empty_are_rejected(self):
        with tempfile.TemporaryDirectory() as base:
            self.assertIsNone(resolve_safe_error_file_path(None, base))
            self.assertIsNone(resolve_safe_error_file_path("", base))

    def test_base_dir_itself_is_rejected(self):
        with tempfile.TemporaryDirectory() as base:
            self.assertIsNone(resolve_safe_error_file_path(".", base))

    def test_traversal_that_lands_back_inside_is_accepted(self):
        with tempfile.TemporaryDirectory() as base:
            resolved = resolve_safe_error_file_path("sub/../error.json", base)
            self.assertEqual(
                resolved, os.path.join(os.path.realpath(base), "error.json"))


if __name__ == "__main__":
    unittest.main()

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path


_ERROR_LOGGING_PATH = (
    Path(__file__).resolve().parents[2] / "tap_workday_raas" / "error_logging.py"
)
_spec = importlib.util.spec_from_file_location(
    "tap_workday_raas.error_logging", _ERROR_LOGGING_PATH
)
_error_logging = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_error_logging)

write_error_file = _error_logging.write_error_file


class TestErrorLogging(unittest.TestCase):
    def test_writes_relative_error_file_within_working_directory(self):
        with tempfile.TemporaryDirectory() as working_directory:
            original_directory = os.getcwd()
            try:
                os.chdir(working_directory)
                Path("errors").mkdir()
                write_error_file("errors/tapError.json", {"message": "failed"})

                with Path("errors/tapError.json").open(encoding="utf-8") as error_file:
                    self.assertEqual({"message": "failed"}, json.load(error_file))
            finally:
                os.chdir(original_directory)

    def test_rejects_parent_directory_traversal(self):
        with tempfile.TemporaryDirectory() as working_directory:
            original_directory = os.getcwd()
            try:
                os.chdir(working_directory)
                with self.assertRaises(ValueError):
                    write_error_file("../tapError.json", {"message": "failed"})
            finally:
                os.chdir(original_directory)

    def test_rejects_absolute_path(self):
        with tempfile.TemporaryDirectory() as working_directory:
            absolute_path = Path(working_directory) / "tapError.json"
            with self.assertRaises(ValueError):
                write_error_file(absolute_path, {"message": "failed"})

    @unittest.skipUnless(hasattr(os, "O_NOFOLLOW"), "O_NOFOLLOW is unavailable")
    def test_rejects_symlink_destination(self):
        with tempfile.TemporaryDirectory() as working_directory:
            original_directory = os.getcwd()
            try:
                os.chdir(working_directory)
                protected_file = Path("protected.json")
                protected_file.write_text("unchanged", encoding="utf-8")
                Path("tapError.json").symlink_to(protected_file)

                with self.assertRaises(ValueError):
                    write_error_file("tapError.json", {"message": "failed"})
                self.assertEqual("unchanged", protected_file.read_text(encoding="utf-8"))
            finally:
                os.chdir(original_directory)


if __name__ == "__main__":
    unittest.main()

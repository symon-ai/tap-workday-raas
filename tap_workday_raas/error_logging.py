import json
import os
from pathlib import Path


_OPEN_SUPPORTS_DIR_FD = os.open in os.supports_dir_fd


def safe_error_file_path(error_file_path):
    """Return an error log path confined to the current working directory."""
    candidate = Path(error_file_path)
    if candidate.is_absolute() or ".." in candidate.parts or not candidate.name:
        raise ValueError("error_file_path must be within the working directory")
    return candidate


def write_error_file(error_file_path, error_info):
    """Write error details through directories securely opened from the cwd."""
    candidate = safe_error_file_path(error_file_path)
    if (
        not hasattr(os, "O_NOFOLLOW")
        or not hasattr(os, "O_DIRECTORY")
        or not _OPEN_SUPPORTS_DIR_FD
        or not hasattr(os, "fchmod")
    ):
        raise RuntimeError("secure error file creation is unsupported on this platform")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory_descriptor = os.open(".", directory_flags)
    try:
        for part in candidate.parts[:-1]:
            try:
                next_descriptor = os.open(
                    part, directory_flags, dir_fd=directory_descriptor
                )
            except OSError as err:
                raise ValueError(
                    "error_file_path must not contain symbolic links"
                ) from err
            os.close(directory_descriptor)
            directory_descriptor = next_descriptor

        flags = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW
        try:
            descriptor = os.open(
                candidate.name, flags, 0o600, dir_fd=directory_descriptor
            )
        except OSError as err:
            raise ValueError(
                "error_file_path must not contain symbolic links"
            ) from err
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as error_file:
                descriptor = None
                error_file.truncate()
                json.dump(error_info, error_file)
        finally:
            if descriptor is not None:
                os.close(descriptor)
    finally:
        os.close(directory_descriptor)

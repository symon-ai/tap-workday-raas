import json
import os
from pathlib import Path


def safe_error_file_path(error_file_path):
    """Return an error log path confined to the current working directory."""
    candidate = Path(error_file_path)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("error_file_path must be within the working directory")

    working_directory = Path.cwd().resolve()
    current_path = working_directory
    for part in candidate.parts:
        current_path = current_path / part
        if current_path.is_symlink():
            raise ValueError("error_file_path must not contain symbolic links")

    resolved_path = (working_directory / candidate).resolve()
    try:
        resolved_path.relative_to(working_directory)
    except ValueError as err:
        raise ValueError("error_file_path must be within the working directory") from err
    return resolved_path


def write_error_file(error_file_path, error_info):
    """Write error details without following a caller-controlled file symlink."""
    resolved_path = safe_error_file_path(error_file_path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(resolved_path, flags, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as error_file:
        json.dump(error_info, error_file)

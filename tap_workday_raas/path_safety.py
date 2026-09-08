"""Centralized validation for user/config-supplied file paths.

Guards against CWE-73 (external control of file name or path): the tap accepts
an ``error_file_path`` from config, and that tainted value must not be allowed
to escape the directory the ELT platform controls when the tap writes its
error file.
"""

import os


def resolve_safe_error_file_path(configured_path, base_dir=None):
    """Resolve a config-supplied error file path, constrained to base_dir.

    The returned path is guaranteed to be an absolute path located strictly
    inside ``base_dir`` (default: the current working directory). Any value
    that would escape that directory -- absolute paths, ``..`` traversal, or
    paths that otherwise resolve outside the base -- is rejected by returning
    ``None`` so the caller can skip the write. This never raises, so a bad
    path cannot crash the tap; the caller falls through to marker-based
    logging instead.
    """
    if not configured_path or not isinstance(configured_path, str):
        return None

    if base_dir is None:
        base_dir = os.getcwd()

    try:
        base = os.path.realpath(base_dir)
        candidate = os.path.realpath(os.path.join(base, configured_path))
    except (OSError, ValueError):
        return None

    # Must resolve to a file strictly inside base (not base itself).
    if candidate == base:
        return None
    try:
        if os.path.commonpath([base, candidate]) != base:
            return None
    except ValueError:
        # Raised e.g. for paths on different drives (Windows) -> unsafe.
        return None

    return candidate

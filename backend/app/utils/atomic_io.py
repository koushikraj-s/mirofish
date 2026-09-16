"""Dependency-free atomic JSON file I/O.

No atomic-write helper exists anywhere else in this codebase (every other
JSON write in the project -- `ProjectManager`, `TaskManager`, etc. -- writes
directly with `open(path, 'w')`). This module exists specifically because
`settings_store.py` persists live credentials and cannot risk a reader
observing a half-written file: a crash, `kill -9`, or a concurrent read
landing mid-`write()` must never see a truncated or corrupt JSON document.

The technique is the standard POSIX one: write the new content to a
temporary file in the *same directory* as the target (so the final rename
is on the same filesystem and therefore atomic), `flush()` + `fsync()` the
temp file so its bytes are actually on disk before the rename, then
`os.replace()` it over the destination. `os.replace` is atomic on POSIX and
on Windows (unlike `os.rename`, which raises on Windows if the destination
exists) -- a reader opening the destination path at any point either sees
the fully-old file or the fully-new file, never a mix.
"""

from __future__ import annotations

import json
import os
from typing import Any


def atomic_write_json(path: str, data: Any, *, mode: int = 0o600) -> None:
    """Atomically write *data* as JSON to *path*.

    Creates the parent directory if needed. On any failure while writing
    the temporary file, the temporary file is removed and the exception is
    re-raised -- the destination file (if it already existed) is left
    untouched.
    """

    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp"

    # O_CREAT|O_TRUNC|O_WRONLY with an explicit mode, rather than a plain
    # open(), so the temp file never briefly exists with looser
    # umask-derived permissions before we get a chance to chmod it -- it is
    # created with `mode` from the first syscall (modulo umask, corrected
    # below).
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
            json.dump(data, tmp_file, ensure_ascii=False, indent=2, sort_keys=True)
            tmp_file.write("\n")
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
    except BaseException:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise

    # os.open()'s mode is masked by the process umask, so force the exact
    # permissions the caller asked for (credentials.json must be 0600)
    # before the rename makes them visible at the final path.
    os.chmod(tmp_path, mode)
    os.replace(tmp_path, path)


def read_json_tolerant(path: str, default: Any = None) -> Any:
    """Read and parse JSON from *path*, tolerating a missing, empty,
    truncated, or otherwise corrupt file by returning *default* instead of
    raising.

    This is deliberately permissive: a partially-written or hand-edited
    settings file must never crash the app on boot or on the next read --
    at worst the runtime overrides it would have provided are silently
    dropped, falling back to `.env`/class defaults (see
    `settings_store.load_overrides`).
    """

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except (FileNotFoundError, OSError):
        return default

    if not content.strip():
        return default

    try:
        return json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return default

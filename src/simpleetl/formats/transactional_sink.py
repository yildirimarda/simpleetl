"""
Transactional sink contract for exactly-once writes.

Filesystem sinks: write to a temporary path in the same directory,
then atomically rename (os.rename) to the final destination.
JDBC sinks: create a staging table, load data, then swap
(staging table renamed to target, original target dropped) inside
a single database transaction.
"""

import logging
import os
import uuid
from typing import Any

from ..core.filesystem import get_filesystem, is_cloud_path

logger = logging.getLogger(__name__)


def _make_temp_path(destination: str) -> str:
    directory = os.path.dirname(os.path.abspath(destination)) or "."
    basename = os.path.basename(destination)
    return os.path.join(directory, f".tmp_{uuid.uuid4().hex}_{basename}")


def _atomic_rename(source: str, destination: str) -> None:
    if is_cloud_path(destination):
        fs = get_filesystem(destination)
        # Most fsspec backends support mv; fall back to copy+delete
        # when mv is unavailable.  The copy path is not atomic but
        # is the best-effort fallback for remote filesystems.
        try:
            fs.mv(source, destination, recursive=False)
        except Exception:
            # Fallback for backends without atomic mv
            with fs.open(source, "rb") as f_in:
                data = f_in.read()
            with fs.open(destination, "wb") as f_out:
                f_out.write(data)
            fs.rm(source)
    else:
        os.rename(source, destination)


def execute_atomic(
    writer,
    data: Any,
    destination: str,
    **kwargs: Any,
) -> None:
    """Execute writer.write atomically using temp-file + rename for
    filesystem sinks, or staging-table + swap for JDBC sinks.
    """
    if hasattr(writer, "_write_atomic"):
        writer._write_atomic(data, destination, **kwargs)
        return

    # Default filesystem atomic path
    temp_path = _make_temp_path(destination)
    try:
        # Call the internal non-transactional implementation to avoid recursion.
        if hasattr(writer, "_do_write"):
            writer._do_write(data, temp_path, **kwargs)
        else:
            writer.write(data, temp_path, **kwargs)
        _atomic_rename(temp_path, destination)
    except Exception:
        # Clean up temp file on any failure
        try:
            if is_cloud_path(temp_path):
                fs = get_filesystem(temp_path)
                if fs.exists(temp_path):
                    fs.rm(temp_path)
            elif os.path.exists(temp_path):
                os.unlink(temp_path)
        except Exception:
            pass
        raise

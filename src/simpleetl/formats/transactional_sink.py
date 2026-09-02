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

from fsspec.core import split_protocol

from ..core.filesystem import get_filesystem, is_cloud_path

logger = logging.getLogger(__name__)


def _make_temp_path(destination: str) -> str:
    protocol, path_part = split_protocol(destination)
    # Split path_part into directory and basename
    if "/" not in path_part:
        basename = path_part
        directory = ""
    else:
        directory, basename = path_part.rsplit("/", 1)
    temp_basename = (
        f".tmp_{uuid.uuid4().hex}_{basename}"
        if basename
        else f".tmp_{uuid.uuid4().hex}"
    )
    if protocol is not None:
        if directory:
            return f"{protocol}://{directory}/{temp_basename}"
        # For root-level paths like s3://bucket/file, directory is the bucket
        return (
            f"{protocol}://{directory}/{temp_basename}"
            if directory
            else f"{protocol}://{temp_basename}"
        )
    # Local path
    local_dir = os.path.dirname(os.path.abspath(path_part)) or "."
    return os.path.join(local_dir, temp_basename)


def _atomic_rename(source: str, destination: str, filesystem=None) -> None:
    if is_cloud_path(destination):
        fs = filesystem if filesystem is not None else get_filesystem(destination)
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
    filesystem = kwargs.pop("filesystem", None)
    temp_path = _make_temp_path(destination)
    try:
        # Call the internal non-transactional implementation to avoid recursion.
        if hasattr(writer, "_do_write"):
            writer._do_write(data, temp_path, filesystem=filesystem, **kwargs)
        else:
            writer.write(data, temp_path, filesystem=filesystem, **kwargs)
        _atomic_rename(temp_path, destination, filesystem=filesystem)
    except Exception:
        # Clean up temp file on any failure
        try:
            if is_cloud_path(temp_path):
                fs = filesystem if filesystem is not None else get_filesystem(temp_path)
                if fs.exists(temp_path):
                    fs.rm(temp_path)
            elif os.path.exists(temp_path):
                os.unlink(temp_path)
        except Exception:
            pass
        raise

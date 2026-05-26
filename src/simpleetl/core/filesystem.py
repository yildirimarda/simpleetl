"""
Unified filesystem abstraction for local and cloud storage.

Supports S3, GCS, ABFS, and local filesystem paths via fsspec.
"""

import os
from typing import Optional, Tuple

import fsspec

# Cloud storage URL prefixes
CLOUD_PREFIXES = {
    "s3": "s3://",
    "gs": "gs://",
    "gcs": "gcs://",
    "abfs": "abfs://",
    "abfss": "abfss://",
}

# Mapping from fsspec protocol names to cloud storage types
PROTOCOL_MAP = {
    "s3": "s3",
    "s3a": "s3",
    "gs": "gs",
    "gcs": "gs",
    "abfs": "abfs",
    "abfss": "abfs",
}


def is_cloud_path(path: str) -> bool:
    """
    Check if a path points to cloud storage.

    Args:
        path: File path or URL to check.

    Returns:
        True if the path is a cloud storage path.
    """
    for prefix in CLOUD_PREFIXES.values():
        if path.startswith(prefix):
            return True
    return False


def split_path(path: str) -> Tuple[str, str]:
    """
    Split a cloud storage path into bucket/container and prefix.

    For local paths, returns ('', path).

    Args:
        path: File path or cloud URL.

    Returns:
        Tuple of (bucket_or_container, prefix_or_remaining_path).

    Examples:
        >>> split_path('s3://my-bucket/data/file.csv')
        ('my-bucket', 'data/file.csv')
        >>> split_path('gs://my-bucket/data/file.csv')
        ('my-bucket', 'data/file.csv')
        >>> split_path('abfss://container@account.dfs.core.windows.net/data/file.csv')
        ('container@account.dfs.core.windows.net', 'data/file.csv')
        >>> split_path('/local/path/file.csv')
        ('', '/local/path/file.csv')
    """
    for prefix in CLOUD_PREFIXES.values():
        if path.startswith(prefix):
            rest = path[len(prefix):]
            parts = rest.split("/", 1)
            bucket = parts[0]
            prefix_path = parts[1] if len(parts) > 1 else ""
            return (bucket, prefix_path)
    return ("", path)


def get_cloud_type(path: str) -> Optional[str]:
    """
    Determine the cloud storage type from a path.

    Args:
        path: File path or cloud URL.

    Returns:
        Cloud type string ('s3', 'gs', 'abfs') or None if local.
    """
    for key, prefix in CLOUD_PREFIXES.items():
        if path.startswith(prefix):
            return PROTOCOL_MAP.get(key)
    return None


def get_filesystem(
    path: str,
    **kwargs,
) -> fsspec.AbstractFileSystem:
    """
    Return an appropriate fsspec filesystem for the given path.

    Automatically detects the filesystem type from the path prefix.
    For S3 paths, credentials are resolved from environment variables
    (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN) or
    IAM roles. For GCS, uses GOOGLE_APPLICATION_CREDENTIALS. For ABFS,
    uses AZURE_STORAGE_ACCOUNT_KEY or managed identity / DefaultAzureCredential.

    Args:
        path: File path or cloud URL.
        **kwargs: Additional arguments passed to the fsspec filesystem
            constructor (e.g., key, token, account_name, account_key).

    Returns:
        An fsspec filesystem instance.

    Examples:
        >>> fs = get_filesystem('s3://my-bucket/data/')
        >>> fs = get_filesystem('gs://my-bucket/data/')
        >>> fs = get_filesystem('abfss://container@account.dfs.core.windows.net/data/')
        >>> fs = get_filesystem('/local/path/')
    """
    protocol = _detect_protocol(path)
    if protocol:
        return fsspec.filesystem(protocol, **kwargs)
    return fsspec.filesystem("file", **kwargs)


def _detect_protocol(path: str) -> Optional[str]:
    """
    Detect the fsspec protocol from a path.

    Args:
        path: File path or cloud URL.

    Returns:
        Protocol string for fsspec, or None for local paths.
    """
    for key, prefix in CLOUD_PREFIXES.items():
        if path.startswith(prefix):
            return PROTOCOL_MAP.get(key)
    return None


def get_pa_filesystem(path: str, filesystem=None):
    """Convert an fsspec filesystem to a pyarrow filesystem.

    Args:
        path: File path or cloud URL.
        filesystem: Optional fsspec filesystem instance.

    Returns:
        A pyarrow filesystem instance.
    """
    import pyarrow.fs as pafs

    if filesystem is not None:
        return pafs.PyFileSystem(pafs.FSSpecHandler(filesystem))

    protocol = _detect_protocol(path)
    if protocol == "s3":
        return pafs.S3FileSystem()
    if protocol in ("gs", "gcs"):
        return pafs.GcsFileSystem()
    if protocol == "abfs":
        return pafs.AzureFileSystem()
    return pafs.LocalFileSystem()


def get_file_mode(path: str, mode: str) -> str:
    """
    Get the appropriate file mode for reading/writing based on format.

    Ensures binary mode is used for formats that require it (parquet,
    avro, orc) and text mode for others.

    Args:
        path: File path or cloud URL.
        mode: Base mode string ('r', 'w', 'rb', 'wb').

    Returns:
        Adjusted mode string.
    """
    binary_extensions = {".parquet", ".avro", ".orc", ".xlsx", ".xls"}
    _, ext = os.path.splitext(path)
    ext = ext.lower()

    if ext in binary_extensions and "b" not in mode:
        return mode + "b"
    return mode

"""
Error classification and custom exceptions for the ETL framework.

Provides a hierarchy of ETL-specific exceptions, error classification
(transient vs permanent), and a classification utility.
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class ErrorClassification(enum.Enum):
    """Classification of errors for retry and handling decisions."""
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNKNOWN = "unknown"


class ETLError(Exception):
    """
    Base exception for all ETL-related errors.

    Attributes:
        job_name: Name of the ETL job where the error occurred.
        phase: The ETL phase (extract, transform, load).
        record_info: Optional dictionary with record-level context.
        cause: The underlying exception that caused this error.
    """

    def __init__(
        self,
        message: str,
        job_name: str = "",
        phase: str = "",
        record_info: Optional[Dict[str, Any]] = None,
        cause: Optional[BaseException] = None,
    ):
        super().__init__(message)
        self.job_name = job_name
        self.phase = phase
        self.record_info = record_info or {}
        self.cause = cause
        self.timestamp = datetime.now(timezone.utc)

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.job_name:
            parts.append(f"job={self.job_name}")
        if self.phase:
            parts.append(f"phase={self.phase}")
        if self.cause:
            parts.append(f"caused_by={self.cause!r}")
        return " | ".join(parts)


class ExtractError(ETLError):
    """Raised when data extraction fails."""

    def __init__(self, message: str, **kwargs):
        kwargs.setdefault("phase", "extract")
        super().__init__(message, **kwargs)


class TransformError(ETLError):
    """Raised when data transformation fails."""

    def __init__(self, message: str, **kwargs):
        kwargs.setdefault("phase", "transform")
        super().__init__(message, **kwargs)


class LoadError(ETLError):
    """Raised when data loading fails."""

    def __init__(self, message: str, **kwargs):
        kwargs.setdefault("phase", "load")
        super().__init__(message, **kwargs)


class PartialFailureError(ETLError):
    """
    Raised when some records fail but others succeed during processing.

    Attributes:
        failed_records: List of (record_index, error) tuples.
        success_count: Number of records that were processed successfully.
        failure_count: Number of records that failed.
    """

    def __init__(
        self,
        message: str,
        failed_records: list[tuple[int, str]],
        success_count: int = 0,
        **kwargs,
    ):
        super().__init__(message, **kwargs)
        self.failed_records = failed_records
        self.success_count = success_count
        self.failure_count = len(failed_records)

    def __str__(self) -> str:
        base = super().__str__()
        return (
            f"{base} | successes={self.success_count} "
            f"failures={self.failure_count}"
        )


def classify_error(exception: BaseException) -> ErrorClassification:
    """
    Classify an error as TRANSIENT, PERMANENT, or UNKNOWN.

    Rules:
        - Connection errors (timeout, refused, reset) -> TRANSIENT
        - Authentication / permission errors -> PERMANENT
        - Schema / validation errors -> PERMANENT
        - File not found -> PERMANENT
        - Memory / overflow errors -> TRANSIENT
        - Everything else -> UNKNOWN

    Args:
        exception: The exception to classify.

    Returns:
        The error classification.
    """
    import errno
    import socket

    # Transient error types
    transient_types = (
        ConnectionError,
        TimeoutError,
        socket.timeout,
        socket.error,
        OSError,
        IOError,
        BlockingIOError,
        InterruptedError,
        BrokenPipeError,
        ConnectionResetError,
        ConnectionRefusedError,
        ConnectionAbortedError,
    )

    # Permanent error types
    permanent_types = (
        FileNotFoundError,
        PermissionError,
        IsADirectoryError,
        NotADirectoryError,
        ValueError,
        TypeError,
        KeyError,
        IndexError,
        AttributeError,
    )

    # Check permanent types first (some are subclasses of transient types,
    # e.g., FileNotFoundError is a subclass of OSError)
    if isinstance(exception, permanent_types):
        return ErrorClassification.PERMANENT

    # Check the exception itself for transient types
    if isinstance(exception, transient_types):
        # Some OSError subtypes with specific errnos are permanent
        if isinstance(exception, OSError):
            if exception.errno in (
                errno.ENOENT,  # No such file or directory
                errno.EACCES,  # Permission denied
                errno.EPERM,   # Operation not permitted
                errno.EISDIR,  # Is a directory
                errno.ENOTDIR, # Not a directory
                errno.EINVAL,  # Invalid argument
                errno.ENAMETOOLONG,  # File name too long
            ):
                return ErrorClassification.PERMANENT
        return ErrorClassification.TRANSIENT

    # Check chained causes
    cause = exception.__cause__
    if cause is not None:
        return classify_error(cause)

    # Check context (implicit chaining)
    context = exception.__context__
    if context is not None and not exception.__suppress_context__:
        return classify_error(context)

    return ErrorClassification.UNKNOWN

"""
Structured logging configuration for SimpleETL.
"""

import logging
import logging.handlers
import json
import sys
from datetime import datetime, timezone
from typing import Any
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
            if record.stack_info:
                log_entry['stack_trace'] = record.stack_info

        # Add extra fields (skip standard LogRecord attributes)
        _skip = {
            'args', 'exc_info', 'message', 'msg', 'created', 'relativeCreated',
            'exc_text', 'stack_info', 'lineno', 'funcName', 'created', 'msecs',
            'process', 'processName', 'thread', 'threadName', 'taskName',
            'name', 'levelno', 'levelname', 'pathname', 'filename', 'module',
        }
        for key, value in record.__dict__.items():
            if key not in _skip:
                log_entry[key] = value

        return json.dumps(log_entry, default=self._json_serializer)

    def _json_serializer(self, obj: Any) -> Any:
        """JSON serializer for non-serializable objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Path):
            return str(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class StructuredLogger:
    """Structured logger with JSON formatting."""

    def __init__(self, name: str = 'simpleetl', level: str = 'INFO'):
        """Initialize structured logger."""
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))

        # Avoid duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Setup logging handlers."""
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(JSONFormatter())
        self.logger.addHandler(console_handler)

        # File handler (if logs directory exists)
        logs_dir = Path('logs')
        if logs_dir.exists():
            file_handler = logging.handlers.RotatingFileHandler(
                logs_dir / 'etl.log',
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5
            )
            file_handler.setFormatter(JSONFormatter())
            self.logger.addHandler(file_handler)

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self.logger.debug(message, extra=kwargs)

    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self.logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self.logger.warning(message, extra=kwargs)

    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self.logger.error(message, extra=kwargs)

    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        self.logger.critical(message, extra=kwargs)

    def log_job_start(self, job_name: str, job_id: str, **kwargs) -> None:
        """Log job start event."""
        self.info(
            f"Starting job: {job_name}",
            event='job_start',
            job_name=job_name,
            job_id=job_id,
            **kwargs
        )

    def log_job_complete(self, job_name: str, job_id: str, duration: float, **kwargs) -> None:
        """Log job completion event."""
        self.info(
            f"Completed job: {job_name} in {duration:.2f}s",
            event='job_complete',
            job_name=job_name,
            job_id=job_id,
            duration=duration,
            **kwargs
        )

    def log_job_error(self, job_name: str, job_id: str, error: str, **kwargs) -> None:
        """Log job error event."""
        self.error(
            f"Job failed: {job_name}",
            event='job_error',
            job_name=job_name,
            job_id=job_id,
            error=error,
            **kwargs
        )

    def log_data_read(self, source: str, record_count: int, **kwargs) -> None:
        """Log data read event."""
        self.info(
            f"Read {record_count} records from {source}",
            event='data_read',
            source=source,
            record_count=record_count,
            **kwargs
        )

    def log_data_write(self, destination: str, record_count: int, **kwargs) -> None:
        """Log data write event."""
        self.info(
            f"Wrote {record_count} records to {destination}",
            event='data_write',
            destination=destination,
            record_count=record_count,
            **kwargs
        )


# Global logger instance
logger = StructuredLogger()


def get_logger(name: str = 'simpleetl') -> StructuredLogger:
    """Get a structured logger instance."""
    return StructuredLogger(name)
"""
Dead Letter Queue (DLQ) for failed ETL records.

Collects records that failed during extraction, transformation, or loading,
along with error context, and writes them to a file for later reprocessing.
"""

from __future__ import annotations

import csv
import json
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union


@dataclass
class DLQEntry:
    """
    A single entry in the Dead Letter Queue.

    Attributes:
        record_data: The raw record data that failed.
        error: The error message or exception string.
        phase: The ETL phase where the failure occurred.
        timestamp: When the failure was recorded.
        record_index: The index of the record in the source data.
        error_type: The class name of the exception.
        metadata: Additional context about the failure.
    """

    record_data: Any
    error: str
    phase: str = ""
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    record_index: int = -1
    error_type: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize entry to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DLQEntry:
        """Deserialize entry from dictionary."""
        return cls(
            **{k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        )


class DeadLetterQueue:
    """
    Collects failed records with error context for later reprocessing.

    Thread-safe collection that can write failed records to JSONL or CSV
    files and read them back for reprocessing.
    """

    def __init__(self):
        self._entries: List[DLQEntry] = []
        self._lock = threading.Lock()

    def add_entry(
        self,
        record_data: Any,
        error: str | BaseException,
        phase: str = "",
        record_index: int = -1,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DLQEntry:
        """
        Add a failed record to the DLQ.

        Args:
            record_data: The raw record data that failed.
            error: The error message or exception.
            phase: The ETL phase where the failure occurred.
            record_index: The index of the record in the source data.
            metadata: Additional context.

        Returns:
            The created DLQEntry.
        """
        error_str = str(error) if isinstance(error, BaseException) else str(error)
        error_type = (
            type(error).__name__ if isinstance(error, BaseException) else ""
        )
        entry = DLQEntry(
            record_data=record_data,
            error=error_str,
            phase=phase,
            record_index=record_index,
            error_type=error_type,
            metadata=metadata or {},
        )
        with self._lock:
            self._entries.append(entry)
        return entry

    @property
    def entries(self) -> List[DLQEntry]:
        """Return a copy of all DLQ entries."""
        with self._lock:
            return list(self._entries)

    @property
    def count(self) -> int:
        """Return the number of entries in the DLQ."""
        with self._lock:
            return len(self._entries)

    def clear(self) -> None:
        """Remove all entries from the DLQ."""
        with self._lock:
            self._entries.clear()

    def write_to_dlq(
        self,
        destination: Union[str, Path],
        format: str = "jsonl",
    ) -> int:
        """
        Write all DLQ entries to a file.

        Args:
            destination: Path to the output file.
            format: Output format, either 'jsonl' or 'csv'.

        Returns:
            The number of entries written.

        Raises:
            ValueError: If an unsupported format is specified.
        """
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)

        entries = self.entries

        if format == "jsonl":
            self._write_jsonl(path, entries)
        elif format == "csv":
            self._write_csv(path, entries)
        else:
            raise ValueError(
                f"Unsupported DLQ format: {format}. Supported: jsonl, csv"
            )

        return len(entries)

    @staticmethod
    def _write_jsonl(path: Path, entries: Sequence[DLQEntry]) -> None:
        """Write entries as JSON Lines."""
        with open(path, "w") as f:
            for entry in entries:
                json.dump(entry.to_dict(), f, default=str)
                f.write("\n")

    @staticmethod
    def _write_csv(path: Path, entries: Sequence[DLQEntry]) -> None:
        """Write entries as CSV."""
        if not entries:
            with open(path, "w") as f:
                f.write("")
            return

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "record_index", "phase", "error_type", "error", "timestamp",
            "record_data", "metadata",
        ])
        for entry in entries:
            writer.writerow([
                entry.record_index,
                entry.phase,
                entry.error_type,
                entry.error,
                entry.timestamp,
                json.dumps(entry.record_data, default=str),
                json.dumps(entry.metadata, default=str),
            ])

        with open(path, "w", newline="") as f:
            f.write(output.getvalue())

    def read_from_dlq(
        self, source: Union[str, Path], format: str = "jsonl"
    ) -> List[DLQEntry]:
        """
        Read DLQ entries from a file for reprocessing.

        Args:
            source: Path to the DLQ file.
            format: Input format, either 'jsonl' or 'csv'.

        Returns:
            List of DLQEntry objects read from the file.

        Raises:
            FileNotFoundError: If the source file does not exist.
            ValueError: If an unsupported format is specified.
        """
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"DLQ file not found: {path}")

        if format == "jsonl":
            entries = self._read_jsonl(path)
        elif format == "csv":
            entries = self._read_csv(path)
        else:
            raise ValueError(
                f"Unsupported DLQ format: {format}. Supported: jsonl, csv"
            )

        with self._lock:
            self._entries.extend(entries)

        return entries

    @staticmethod
    def _read_jsonl(path: Path) -> List[DLQEntry]:
        """Read entries from a JSON Lines file."""
        entries = []
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                entries.append(DLQEntry.from_dict(data))
        return entries

    @staticmethod
    def _read_csv(path: Path) -> List[DLQEntry]:
        """Read entries from a CSV file."""
        entries = []
        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                record_data = (
                    json.loads(row["record_data"])
                    if row.get("record_data")
                    else None
                )
                metadata = (
                    json.loads(row["metadata"])
                    if row.get("metadata")
                    else {}
                )
                entries.append(
                    DLQEntry(
                        record_data=record_data,
                        error=row.get("error", ""),
                        phase=row.get("phase", ""),
                        timestamp=row.get("timestamp", ""),
                        record_index=int(row.get("record_index", -1)),
                        error_type=row.get("error_type", ""),
                        metadata=metadata,
                    )
                )
        return entries

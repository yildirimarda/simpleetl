"""
Checkpoint management for ETL job progress tracking and resume.

Provides file-based and in-memory checkpoint stores so jobs can
resume from the last successful point after a failure.
"""

from __future__ import annotations

import json
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class Checkpoint:
    """
    Represents a snapshot of ETL job progress.

    Attributes:
        job_id: Unique identifier for the job run.
        job_name: Human-readable job name.
        phase: Current ETL phase (extract, transform, load).
        records_processed: Number of records processed so far.
        watermark: Optional watermark value for incremental loads.
        metadata: Additional metadata for the checkpoint.
        timestamp: When the checkpoint was created.
    """

    job_id: str
    job_name: str = ""
    phase: str = ""
    records_processed: int = 0
    watermark: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize checkpoint to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Checkpoint:
        """Deserialize checkpoint from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class CheckpointStore(ABC):
    """Abstract base class for checkpoint storage backends."""

    @abstractmethod
    def save(self, checkpoint: Checkpoint) -> None:
        """Persist a checkpoint."""
        ...

    @abstractmethod
    def load(self, job_id: str) -> Optional[Checkpoint]:
        """Load the most recent checkpoint for a job."""
        ...

    @abstractmethod
    def delete(self, job_id: str) -> None:
        """Delete all checkpoints for a job."""
        ...


class InMemoryCheckpointStore(CheckpointStore):
    """In-memory checkpoint store for testing and short-lived jobs."""

    def __init__(self):
        self._store: Dict[str, Checkpoint] = {}
        self._lock = threading.Lock()

    def save(self, checkpoint: Checkpoint) -> None:
        with self._lock:
            self._store[checkpoint.job_id] = checkpoint

    def load(self, job_id: str) -> Optional[Checkpoint]:
        with self._lock:
            return self._store.get(job_id)

    def delete(self, job_id: str) -> None:
        with self._lock:
            self._store.pop(job_id, None)

    def clear(self) -> None:
        """Clear all checkpoints."""
        with self._lock:
            self._store.clear()


class FileCheckpointStore(CheckpointStore):
    """
    File-based checkpoint store using JSON files.

    Each checkpoint is stored as a JSON file in the checkpoint directory.
    """

    def __init__(self, checkpoint_dir: str | Path = ".checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _get_path(self, job_id: str) -> Path:
        """Get the file path for a job's checkpoint."""
        return self.checkpoint_dir / f"{job_id}.json"

    def save(self, checkpoint: Checkpoint) -> None:
        path = self._get_path(checkpoint.job_id)
        with self._lock:
            with open(path, "w") as f:
                json.dump(checkpoint.to_dict(), f, indent=2, default=str)

    def load(self, job_id: str) -> Optional[Checkpoint]:
        path = self._get_path(job_id)
        with self._lock:
            if not path.exists():
                return None
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                return Checkpoint.from_dict(data)
            except (json.JSONDecodeError, KeyError, TypeError):
                return None

    def delete(self, job_id: str) -> None:
        path = self._get_path(job_id)
        with self._lock:
            if path.exists():
                path.unlink()


class CheckpointManager:
    """
    Manages checkpoint creation, loading, and deletion.

    Acts as a convenience wrapper around a CheckpointStore, providing
    higher-level operations for ETL jobs.

    Args:
        store: The checkpoint store to use. Defaults to FileCheckpointStore.
        job_id: Optional job ID. Generated automatically if not provided.
        job_name: Optional job name for checkpoint metadata.
    """

    def __init__(
        self,
        store: Optional[CheckpointStore] = None,
        job_id: Optional[str] = None,
        job_name: str = "",
    ):
        self.store = store or FileCheckpointStore()
        self.job_id = job_id or str(uuid.uuid4())
        self.job_name = job_name

    def save_checkpoint(
        self,
        phase: str,
        records_processed: int,
        watermark: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Checkpoint:
        """
        Save a checkpoint for the current job.

        Args:
            phase: Current ETL phase.
            records_processed: Number of records processed so far.
            watermark: Optional watermark for incremental loads.
            metadata: Additional metadata.

        Returns:
            The created Checkpoint.
        """
        checkpoint = Checkpoint(
            job_id=self.job_id,
            job_name=self.job_name,
            phase=phase,
            records_processed=records_processed,
            watermark=watermark,
            metadata=metadata or {},
        )
        self.store.save(checkpoint)
        return checkpoint

    def load_checkpoint(self) -> Optional[Checkpoint]:
        """
        Load the most recent checkpoint for this job.

        Returns:
            The checkpoint if found, None otherwise.
        """
        return self.store.load(self.job_id)

    def delete_checkpoint(self) -> None:
        """Delete the checkpoint for this job."""
        self.store.delete(self.job_id)

    def should_resume(self) -> bool:
        """
        Check if the job should resume from a checkpoint.

        Returns:
            True if a checkpoint exists for this job.
        """
        return self.store.load(self.job_id) is not None

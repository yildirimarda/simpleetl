"""
Incremental/delta loading support for ETL jobs.

Provides watermark-based tracking of the last processed record
so that subsequent job runs only extract new or changed data.
"""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import sqlalchemy

from .config import ETLJobConfig

logger = __import__("logging").getLogger(__name__)


@dataclass
class Watermark:
    """Represents a high-water mark for incremental loading.

    Attributes:
        job_name: Name of the ETL job.
        source: Data source identifier (table name, file path, etc.).
        column: The column used for incremental tracking.
        value: The last processed watermark value.
        updated_at: Timestamp when the watermark was last updated.
    """

    job_name: str
    source: str
    column: str
    value: Any
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class WatermarkStore(ABC):
    """Abstract base class for watermark persistence."""

    @abstractmethod
    def get(self, job_name: str, source: str) -> Optional[Watermark]:
        """Retrieve a watermark for the given job and source.

        Args:
            job_name: Name of the ETL job.
            source: Data source identifier.

        Returns:
            Watermark instance or None if not found.
        """
        pass

    @abstractmethod
    def set(self, watermark: Watermark) -> None:
        """Persist a watermark.

        Args:
            watermark: Watermark instance to persist.
        """
        pass

    @abstractmethod
    def delete(self, job_name: str, source: str) -> None:
        """Delete a watermark.

        Args:
            job_name: Name of the ETL job.
            source: Data source identifier.
        """
        pass


class FileWatermarkStore(WatermarkStore):
    """JSON file-based watermark storage.

    Stores watermarks in a JSON file on the local filesystem.
    Suitable for single-node deployments and testing.
    """

    def __init__(self, file_path: str = ".watermarks.json") -> None:
        """Initialize the file watermark store.

        Args:
            file_path: Path to the JSON file for storing watermarks.
        """
        self.file_path = Path(file_path)
        self._ensure_file()

    def _ensure_file(self) -> None:
        """Create the watermark file if it does not exist."""
        if not self.file_path.exists():
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            self._write_all({})

    def _read_all(self) -> Dict[str, Dict[str, Any]]:
        """Read all watermarks from the file.

        Returns:
            Dictionary of watermark data keyed by job_name:source.
        """
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    def _write_all(self, data: Dict[str, Dict[str, Any]]) -> None:
        """Write all watermarks to the file.

        Args:
            data: Dictionary of watermark data.
        """
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _make_key(self, job_name: str, source: str) -> str:
        """Create a composite key for the watermark.

        Args:
            job_name: Name of the ETL job.
            source: Data source identifier.

        Returns:
            Composite key string.
        """
        return f"{job_name}:{source}"

    def get(self, job_name: str, source: str) -> Optional[Watermark]:
        """Retrieve a watermark from the file store."""
        data = self._read_all()
        key = self._make_key(job_name, source)
        if key not in data:
            return None
        entry = data[key]
        return Watermark(
            job_name=entry["job_name"],
            source=entry["source"],
            column=entry["column"],
            value=entry["value"],
            updated_at=entry["updated_at"],
        )

    def set(self, watermark: Watermark) -> None:
        """Persist a watermark to the file store."""
        data = self._read_all()
        key = self._make_key(watermark.job_name, watermark.source)
        data[key] = {
            "job_name": watermark.job_name,
            "source": watermark.source,
            "column": watermark.column,
            "value": watermark.value,
            "updated_at": watermark.updated_at,
        }
        self._write_all(data)

    def delete(self, job_name: str, source: str) -> None:
        """Delete a watermark from the file store."""
        data = self._read_all()
        key = self._make_key(job_name, source)
        data.pop(key, None)
        self._write_all(data)


class DatabaseWatermarkStore(WatermarkStore):
    """Database table-based watermark storage.

    Stores watermarks in a database table for distributed deployments.
    The table is created automatically if it does not exist.
    """

    WATERMARK_TABLE = "etl_watermarks"

    def __init__(self, connection: str | sqlalchemy.engine.Engine) -> None:
        """Initialize the database watermark store.

        Args:
            connection: Database connection string or SQLAlchemy engine.
        """
        if isinstance(connection, str):
            self.engine = sqlalchemy.create_engine(connection)
        else:
            self.engine = connection
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the watermarks table if it does not exist."""
        with self.engine.connect() as conn:
            inspector = sqlalchemy.inspect(self.engine)
            if not inspector.has_table(self.WATERMARK_TABLE):
                conn.execute(
                    sqlalchemy.text(
                        f"CREATE TABLE {self.WATERMARK_TABLE} ("
                        "  job_name VARCHAR(255) NOT NULL, "
                        "  source VARCHAR(255) NOT NULL, "
                        "  column_name VARCHAR(255) NOT NULL, "
                        "  value VARCHAR(1024), "
                        "  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                        "  PRIMARY KEY (job_name, source)"
                        ")"
                    )
                )
                conn.commit()

    def get(self, job_name: str, source: str) -> Optional[Watermark]:
        """Retrieve a watermark from the database store."""
        with self.engine.connect() as conn:
            result = conn.execute(
                sqlalchemy.text(
                    f"SELECT job_name, source, column_name, value, updated_at "
                    f"FROM {self.WATERMARK_TABLE} "
                    f"WHERE job_name = :job_name AND source = :source"
                ),
                {"job_name": job_name, "source": source},
            ).fetchone()

        if result is None:
            return None

        return Watermark(
            job_name=result[0],
            source=result[1],
            column=result[2],
            value=result[3],
            updated_at=str(result[4]),
        )

    def set(self, watermark: Watermark) -> None:
        """Persist a watermark to the database store using UPSERT."""
        dialect = self.engine.dialect.name

        if dialect == "postgresql":
            query = sqlalchemy.text(
                f"INSERT INTO {self.WATERMARK_TABLE} "
                f"(job_name, source, column_name, value, updated_at) "
                f"VALUES (:job_name, :source, :column_name, :value, :updated_at) "
                f"ON CONFLICT (job_name, source) DO UPDATE SET "
                f"column_name = EXCLUDED.column_name, "
                f"value = EXCLUDED.value, "
                f"updated_at = EXCLUDED.updated_at"
            )
        elif dialect == "mysql":
            query = sqlalchemy.text(
                f"INSERT INTO {self.WATERMARK_TABLE} "
                f"(job_name, source, column_name, value, updated_at) "
                f"VALUES (:job_name, :source, :column_name, :value, :updated_at) "
                f"ON DUPLICATE KEY UPDATE "
                f"column_name = VALUES(column_name), "
                f"value = VALUES(value), "
                f"updated_at = VALUES(updated_at)"
            )
        else:
            # SQLite and others: try update first, then insert
            with self.engine.connect() as conn:
                update_result = conn.execute(
                    sqlalchemy.text(
                        f"UPDATE {self.WATERMARK_TABLE} "
                        f"SET column_name = :column_name, value = :value, "
                        f"updated_at = :updated_at "
                        f"WHERE job_name = :job_name AND source = :source"
                    ),
                    {
                        "job_name": watermark.job_name,
                        "source": watermark.source,
                        "column_name": watermark.column,
                        "value": str(watermark.value),
                        "updated_at": watermark.updated_at,
                    },
                )
                conn.commit()
                if update_result.rowcount == 0:
                    conn.execute(
                        sqlalchemy.text(
                            f"INSERT INTO {self.WATERMARK_TABLE} "
                            f"(job_name, source, column_name, value, updated_at) "
                            f"VALUES (:job_name, :source, :column_name, :value, :updated_at)"
                        ),
                        {
                            "job_name": watermark.job_name,
                            "source": watermark.source,
                            "column_name": watermark.column,
                            "value": str(watermark.value),
                            "updated_at": watermark.updated_at,
                        },
                    )
                    conn.commit()
            return

        with self.engine.connect() as conn:
            conn.execute(
                query,
                {
                    "job_name": watermark.job_name,
                    "source": watermark.source,
                    "column_name": watermark.column,
                    "value": str(watermark.value),
                    "updated_at": watermark.updated_at,
                },
            )
            conn.commit()

    def delete(self, job_name: str, source: str) -> None:
        """Delete a watermark from the database store."""
        with self.engine.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    f"DELETE FROM {self.WATERMARK_TABLE} "
                    f"WHERE job_name = :job_name AND source = :source"
                ),
                {"job_name": job_name, "source": source},
            )
            conn.commit()


class WatermarkManager:
    """High-level manager for watermark operations.

    Provides a convenient interface for getting and setting watermarks
    using a configured store backend.
    """

    def __init__(self, store: WatermarkStore) -> None:
        """Initialize the watermark manager.

        Args:
            store: The watermark store backend to use.
        """
        self.store = store

    def get_watermark(self, job_name: str, source: str) -> Optional[Watermark]:
        """Get the watermark for a job and source.

        Args:
            job_name: Name of the ETL job.
            source: Data source identifier.

        Returns:
            Watermark instance or None if not found.
        """
        return self.store.get(job_name, source)

    def set_watermark(
        self, job_name: str, source: str, column: str, value: Any
    ) -> Watermark:
        """Set the watermark for a job and source.

        Args:
            job_name: Name of the ETL job.
            source: Data source identifier.
            column: The column used for incremental tracking.
            value: The new watermark value.

        Returns:
            The created Watermark instance.
        """
        watermark = Watermark(
            job_name=job_name,
            source=source,
            column=column,
            value=value,
        )
        self.store.set(watermark)
        logger.info(
            "Watermark set for job '%s', source '%s': %s = %s",
            job_name,
            source,
            column,
            value,
        )
        return watermark

    def reset_watermark(self, job_name: str, source: str) -> None:
        """Reset (delete) the watermark for a job and source.

        Args:
            job_name: Name of the ETL job.
            source: Data source identifier.
        """
        self.store.delete(job_name, source)
        logger.info("Watermark reset for job '%s', source '%s'", job_name, source)

    @staticmethod
    def from_config(config: ETLJobConfig) -> "WatermarkManager":
        """Create a WatermarkManager from an ETLJobConfig.

        Args:
            config: ETLJobConfig with incremental settings.

        Returns:
            Configured WatermarkManager instance.
        """
        store_type = getattr(config, "watermark_store", "file")
        if store_type == "database":
            db_url = None
            if hasattr(config, "database") and config.database:
                db_url = config.database.url
            if db_url is None:
                db_url = config.params.get("database_url")
            if db_url is None:
                raise ValueError(
                    "Database URL must be configured for database watermark store. "
                    "Set 'database.url' or 'database_url' in config params."
                )
            store: WatermarkStore = DatabaseWatermarkStore(db_url)
        else:
            store = FileWatermarkStore()
        return WatermarkManager(store)
